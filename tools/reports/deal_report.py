#!/usr/bin/env python3
"""
Deal Report Generator - Single Project Feasibility Assessment

Generates professional PDF reports for individual interconnection projects.

Usage:
    from reports import generate_deal_report

    pdf_path = generate_deal_report(
        project_id="J1234",
        client_name="Acme Capital"
    )

CLI:
    python -m reports.deal_report J1234 --client "Acme Capital" -o report.pdf
"""

import argparse
import base64
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# PDF generation
try:
    from weasyprint import HTML, CSS
    WEASYPRINT_AVAILABLE = True
except ImportError:
    WEASYPRINT_AVAILABLE = False

# Data modules
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# Import from parent tools directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from .styles import get_deal_report_css, RECOMMENDATION_COLORS

# Output directory
OUTPUT_DIR = Path(__file__).parent.parent / 'output' / 'reports' / 'deals'


def generate_deal_report(
    project_id: str,
    df: pd.DataFrame = None,
    client_name: str = "Confidential",
    region: str = None,
    output_path: str = None,
    include_market_data: bool = True,
    include_charts: bool = True,
) -> str:
    """
    Generate a PDF feasibility report for a single project.

    Args:
        project_id: Queue ID to analyze
        df: DataFrame with queue data (optional, will load if not provided)
        client_name: Client name for report header
        region: ISO/RTO region (auto-detected if not provided)
        output_path: Output file path (optional, auto-generated if not provided)
        include_market_data: Include revenue/transmission analysis
        include_charts: Generate and embed charts

    Returns:
        Path to generated PDF file
    """
    if not WEASYPRINT_AVAILABLE:
        raise RuntimeError(
            "WeasyPrint is required for PDF generation. "
            "Install with: pip install weasyprint"
        )

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load data if not provided
    if df is None or df.empty:
        df = _load_queue_data()
        if df.empty:
            raise ValueError("No queue data available. Run data refresh first.")

    # Import analysis modules
    from scoring import FeasibilityScorer
    from real_data import RealDataEstimator

    # Find the project row directly (handle queue_id vs id column issue)
    print(f"[1/4] Scoring project {project_id}...")

    # Look for project by queue_id column
    project_row = None
    for col in ['queue_id', 'Queue_ID', 'queue', 'id', 'ID']:
        if col in df.columns:
            matches = df[df[col].astype(str) == str(project_id)]
            if not matches.empty:
                project_row = matches.iloc[0]
                break

    if project_row is None:
        raise ValueError(f"Project not found: {project_id}")

    # Score the project by passing row directly
    scorer = FeasibilityScorer(df)
    score_result = scorer.score_project(row=project_row)

    if 'error' in score_result:
        raise ValueError(f"Could not score project: {score_result['error']}")

    # Extract project info
    proj = score_result['project']
    breakdown = score_result['breakdown']

    # Auto-detect region if not provided
    if region is None:
        region = proj.get('region', proj.get('iso', 'Unknown'))

    # Build basic info dict
    basic = {
        'name': proj.get('name', 'Unknown'),
        'developer': proj.get('developer', 'Unknown'),
        'type': proj.get('type', 'Unknown'),
        'capacity_mw': proj.get('capacity_mw', 0),
        'state': proj.get('state', 'Unknown'),
        'county': proj.get('county', ''),
        'poi': proj.get('poi', 'Unknown'),
        'queue_date': proj.get('queue_date', 'Unknown'),
        'months_in_queue': proj.get('months_in_queue', 0),
        'status': proj.get('status', 'Active'),
        'study_phase': proj.get('study_phase', 'Unknown'),
    }

    # Get cost/timeline estimates
    print(f"[2/4] Computing estimates from historical data...")
    estimator = RealDataEstimator()
    estimates = estimator.estimate_project(
        region=region,
        project_type=basic['type'],
        capacity_mw=basic['capacity_mw'],
        months_in_queue=basic['months_in_queue']
    )

    cost_data = estimates['cost']
    timeline_data = estimates['timeline']
    completion_data = estimates['completion']

    # Get developer cross-RTO data
    print(f"[3/4] Analyzing developer track record...")
    cross_rto = _get_developer_cross_rto(df, basic['developer'])

    # Get market data (optional)
    market_data = {}
    if include_market_data:
        market_data = _get_market_data(
            region=region,
            capacity_mw=basic['capacity_mw'],
            technology=basic['type'],
            state=basic['state'],
            poi=basic.get('poi')
        )

    # Generate charts (optional)
    chart_images = {}
    if include_charts:
        chart_images = _generate_charts(
            region=region,
            basic=basic,
            cost_data=cost_data,
            timeline_data=timeline_data,
            breakdown=breakdown
        )

    # Build HTML
    print(f"[4/4] Building report...")
    html_content = _build_html(
        project_id=project_id,
        region=region,
        client_name=client_name,
        basic=basic,
        score_result=score_result,
        breakdown=breakdown,
        cost_data=cost_data,
        timeline_data=timeline_data,
        completion_data=completion_data,
        cross_rto=cross_rto,
        market_data=market_data,
        chart_images=chart_images,
        estimator=estimator,
    )

    # Generate PDF
    if output_path is None:
        safe_id = project_id.replace('/', '_').replace('\\', '_')
        output_path = OUTPUT_DIR / f"deal_report_{safe_id}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    else:
        output_path = Path(output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    HTML(string=html_content).write_pdf(
        str(output_path),
        stylesheets=[CSS(string=get_deal_report_css())]
    )

    print(f"Report generated: {output_path}")
    return str(output_path)


def _load_queue_data() -> pd.DataFrame:
    """Load queue data from available sources."""
    import sqlite3

    # Try loading from SQLite database (most comprehensive)
    try:
        db_path = Path(__file__).parent.parent / '.data' / 'queue.db'
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            df = pd.read_sql('SELECT * FROM projects', conn)
            conn.close()
            if not df.empty:
                return df
    except Exception:
        pass

    # Fallback to market_intel
    try:
        from market_intel import MarketData
        market = MarketData()
        return market.get_latest_data()
    except Exception:
        pass

    # Fallback to unified_data
    try:
        from unified_data import UnifiedQueue
        uq = UnifiedQueue()
        return uq.load_unified()
    except Exception:
        pass

    return pd.DataFrame()


def _get_developer_cross_rto(df: pd.DataFrame, developer: str) -> Dict[str, Any]:
    """Analyze developer presence across ISOs."""
    if not developer or developer == 'Unknown' or df.empty:
        return {
            'total_projects': 0,
            'total_capacity_mw': 0,
            'isos': [],
            'assessment': 'Unknown track record',
        }

    # Normalize developer name for matching
    dev_lower = developer.lower().strip()

    # Find matching projects
    dev_col = None
    for col in ['developer', 'Developer', 'applicant', 'Applicant', 'owner']:
        if col in df.columns:
            dev_col = col
            break

    if dev_col is None:
        return {
            'total_projects': 0,
            'total_capacity_mw': 0,
            'isos': [],
            'assessment': 'Developer data not available',
        }

    # Match developer (case-insensitive, partial match)
    mask = df[dev_col].fillna('').str.lower().str.contains(dev_lower, regex=False)
    dev_projects = df[mask]

    if dev_projects.empty:
        return {
            'total_projects': 1,  # At least the current project
            'total_capacity_mw': 0,
            'isos': [],
            'assessment': 'Single-project developer (limited track record)',
        }

    # Calculate metrics
    total_projects = len(dev_projects)

    # Get capacity
    cap_col = None
    for col in ['capacity_mw', 'Capacity_MW', 'mw', 'MW', 'capacity']:
        if col in df.columns:
            cap_col = col
            break

    total_capacity = 0
    if cap_col:
        total_capacity = dev_projects[cap_col].fillna(0).sum()

    # Get ISOs
    iso_col = None
    for col in ['iso', 'ISO', 'region', 'Region', 'rto']:
        if col in df.columns:
            iso_col = col
            break

    isos = []
    if iso_col:
        isos = dev_projects[iso_col].dropna().unique().tolist()

    # Assessment
    if total_projects >= 10:
        assessment = f"Established developer ({total_projects} projects, {total_capacity/1000:.1f} GW)"
    elif total_projects >= 5:
        assessment = f"Experienced developer ({total_projects} projects across {len(isos)} ISOs)"
    elif total_projects >= 2:
        assessment = f"Growing developer ({total_projects} projects)"
    else:
        assessment = "Single-project developer (limited track record)"

    return {
        'total_projects': total_projects,
        'total_capacity_mw': total_capacity,
        'isos': isos,
        'assessment': assessment,
    }


def _get_market_data(
    region: str,
    capacity_mw: float,
    technology: str,
    state: str,
    poi: str = None
) -> Dict[str, Any]:
    """Get market data (revenue, transmission, PPA, permits)."""
    market_data = {}

    # Revenue estimates
    try:
        from lmp_data import RevenueEstimator
        rev = RevenueEstimator()
        revenue = rev.estimate_annual_revenue(
            region=region,
            capacity_mw=capacity_mw,
            technology=technology,
            state=state,
            poi=poi
        )
        market_data['revenue'] = revenue
    except Exception:
        pass

    # Capacity value
    try:
        from capacity_data import CapacityValue
        cap = CapacityValue()
        capacity = cap.calculate_capacity_value(
            region=region,
            capacity_mw=capacity_mw,
            technology=technology
        )
        market_data['capacity'] = capacity
    except Exception:
        pass

    # Combined revenue
    energy_rev = market_data.get('revenue', {}).get('annual_revenue', 0)
    cap_rev = market_data.get('capacity', {}).get('annual_capacity_value', 0)
    market_data['total_annual_revenue'] = energy_rev + cap_rev

    # Transmission/congestion risk
    try:
        from transmission_data import ConstraintAnalysis
        tx = ConstraintAnalysis()
        transmission = tx.assess_poi_risk(
            region=region,
            poi=poi,
            state=state
        )
        market_data['transmission'] = transmission
    except Exception:
        pass

    # PPA comparison
    try:
        from ppa_data import PPABenchmarks
        ppa = PPABenchmarks()
        ppa_data = ppa.compare_merchant_vs_ppa(
            region=region,
            technology=technology,
            capacity_mw=capacity_mw
        )
        market_data['ppa'] = ppa_data
    except Exception:
        pass

    return market_data


def _generate_charts(
    region: str,
    basic: Dict,
    cost_data: Dict,
    timeline_data: Dict,
    breakdown: Dict
) -> Dict[str, str]:
    """Generate charts and return base64 encoded images."""
    chart_images = {}

    try:
        import charts_altair as charts
        from historical_data import HistoricalData

        hd = HistoricalData()
        charts_dir = Path(__file__).parent.parent / 'charts'

        this_project = {
            'capacity_mw': basic.get('capacity_mw', 0),
            'cost_low': cost_data['per_kw']['p25'],
            'cost_median': cost_data['per_kw']['p50'],
            'cost_high': cost_data['per_kw']['p75'],
            'type': basic.get('type', 'unknown'),
            'region': region,
        }

        # Cost scatter chart
        region_costs = hd.ic_costs_by_region.get(region)
        if region_costs is None or (hasattr(region_costs, 'empty') and region_costs.empty):
            region_costs = hd.ic_costs_df
        if region_costs is not None and len(region_costs) > 0:
            charts.cost_scatter(region_costs, this_project, f'{region} IC Cost Comparison')
            chart_images['cost_scatter'] = _embed_chart(charts_dir / 'cost_scatter_altair.png')

        # Risk bars chart
        score_data = {
            'queue_position': breakdown.get('queue_position', 0),
            'study_progress': breakdown.get('study_progress', 0),
            'developer_track_record': breakdown.get('developer_track_record', 0),
            'poi_congestion': breakdown.get('poi_congestion', 0),
            'project_characteristics': breakdown.get('project_characteristics', 0),
        }
        max_scores = {
            'queue_position': 25,
            'study_progress': 25,
            'developer_track_record': 20,
            'poi_congestion': 15,
            'project_characteristics': 15,
        }
        charts.risk_bars(score_data, max_scores, title='Risk Profile')
        chart_images['risk_bars'] = _embed_chart(charts_dir / 'risk_bars_altair.png')

    except Exception as e:
        print(f"  Warning: Chart generation error: {e}")

    return chart_images


def _embed_chart(path: Path) -> str:
    """Convert chart image to base64 for embedding."""
    try:
        if path.exists():
            with open(path, 'rb') as f:
                data = base64.b64encode(f.read()).decode('utf-8')
            return f"data:image/png;base64,{data}"
    except Exception:
        pass
    return ""


def _build_html(
    project_id: str,
    region: str,
    client_name: str,
    basic: Dict,
    score_result: Dict,
    breakdown: Dict,
    cost_data: Dict,
    timeline_data: Dict,
    completion_data: Dict,
    cross_rto: Dict,
    market_data: Dict,
    chart_images: Dict,
    estimator,
) -> str:
    """Build HTML content for PDF."""

    # Extract values
    score = score_result['total_score']
    grade = score_result['grade']
    rec = score_result['recommendation']
    confidence = score_result.get('confidence', 'Medium')
    red_flags = score_result.get('red_flags', [])
    green_flags = score_result.get('green_flags', [])

    rec_color = RECOMMENDATION_COLORS.get(rec, '#6b7280')
    rec_class = f"rec-{rec.lower().replace('-', '')}"

    # Format estimates
    cost_range = estimator.format_cost_range(cost_data)
    timeline_range = estimator.format_timeline_range(timeline_data)
    completion_rate = estimator.format_completion_rate(completion_data)

    # COD dates
    from dateutil.relativedelta import relativedelta
    now = datetime.now()

    def quarter(months):
        dt = now + relativedelta(months=int(months))
        return f"Q{(dt.month - 1) // 3 + 1} {dt.year}"

    # Traffic light helper
    def traffic_light(score_val, max_val):
        pct = score_val / max_val if max_val > 0 else 0
        if pct >= 0.7:
            return '<span class="traffic traffic-green"></span>'
        elif pct >= 0.4:
            return '<span class="traffic traffic-yellow"></span>'
        else:
            return '<span class="traffic traffic-red"></span>'

    # Risk level helper
    def risk_level(score_val, max_val):
        pct = score_val / max_val if max_val > 0 else 0
        if pct >= 0.7:
            return ('LOW', 'badge-low')
        elif pct >= 0.4:
            return ('MEDIUM', 'badge-medium')
        else:
            return ('HIGH', 'badge-high')

    # Build HTML sections
    html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Interconnection Feasibility Assessment - {basic['name']}</title>
</head>
<body>
    <!-- Header -->
    <div class="header">
        <h1>Interconnection Feasibility Assessment</h1>
        <div class="header-meta">
            <strong>{basic['name']}</strong> | Queue ID: {project_id} | {region}<br>
            Prepared for: {client_name} | {datetime.now().strftime('%B %d, %Y')}
        </div>
    </div>

    <!-- Executive Summary -->
    <div class="section">
        <h2>Executive Summary</h2>
        <div class="exec-summary">
            <div class="score-card">
                <div class="score-value" style="color: {rec_color};">{score:.0f}</div>
                <div class="score-label">/ 100</div>
                <div class="recommendation-badge {rec_class}">{rec}</div>
                <div class="grade-badge">Grade: {grade} | Confidence: {confidence}</div>
            </div>
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value">{cost_range}</div>
                    <div class="kpi-label">Estimated IC Cost</div>
                    <div class="kpi-detail">P25-P75 range</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{completion_rate}</div>
                    <div class="kpi-label">Completion Probability</div>
                    <div class="kpi-detail">Historical rate</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{quarter(timeline_data['remaining_p50'])}</div>
                    <div class="kpi-label">Target COD (P50)</div>
                    <div class="kpi-detail">{timeline_data['remaining_p50']:.0f} months</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{cost_data['n_comparables']}</div>
                    <div class="kpi-label">Comparable Projects</div>
                    <div class="kpi-detail">{region} historical</div>
                </div>
            </div>
        </div>
        <div class="key-risk">
            <strong>Key Risk:</strong> {red_flags[0] if red_flags else 'No critical risks identified'}<br>
            <strong>Developer:</strong> {cross_rto.get('assessment', 'Unknown track record')}
        </div>
    </div>

    <!-- Project Overview -->
    <div class="section">
        <h2>Project Overview</h2>
        <div class="two-col">
            <table class="data-table">
                <tr><th>Queue ID</th><td>{project_id}</td></tr>
                <tr><th>Project Name</th><td>{basic['name']}</td></tr>
                <tr><th>Developer</th><td>{basic['developer']}</td></tr>
                <tr><th>Project Type</th><td>{basic['type']}</td></tr>
            </table>
            <table class="data-table">
                <tr><th>Capacity</th><td>{basic['capacity_mw']:,.0f} MW</td></tr>
                <tr><th>State</th><td>{basic['state']}</td></tr>
                <tr><th>Queue Date</th><td>{basic['queue_date']}</td></tr>
                <tr><th>Time in Queue</th><td>{basic['months_in_queue']:.0f} months</td></tr>
            </table>
        </div>
    </div>

    <!-- Score Breakdown -->
    <div class="section">
        <h2>Feasibility Score Breakdown</h2>
        <div class="two-col">
            <div>
                <table class="score-table">
                    <thead>
                        <tr>
                            <th>Component</th>
                            <th>Score</th>
                            <th>Max</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>Queue Position</td>
                            <td>{breakdown['queue_position']:.1f}</td>
                            <td>25</td>
                            <td>{traffic_light(breakdown['queue_position'], 25)}</td>
                        </tr>
                        <tr>
                            <td>Study Progress</td>
                            <td>{breakdown['study_progress']:.1f}</td>
                            <td>25</td>
                            <td>{traffic_light(breakdown['study_progress'], 25)}</td>
                        </tr>
                        <tr>
                            <td>Developer Track Record</td>
                            <td>{breakdown['developer_track_record']:.1f}</td>
                            <td>20</td>
                            <td>{traffic_light(breakdown['developer_track_record'], 20)}</td>
                        </tr>
                        <tr>
                            <td>POI Congestion</td>
                            <td>{breakdown['poi_congestion']:.1f}</td>
                            <td>15</td>
                            <td>{traffic_light(breakdown['poi_congestion'], 15)}</td>
                        </tr>
                        <tr>
                            <td>Project Characteristics</td>
                            <td>{breakdown['project_characteristics']:.1f}</td>
                            <td>15</td>
                            <td>{traffic_light(breakdown['project_characteristics'], 15)}</td>
                        </tr>
                        <tr class="total-row">
                            <td><strong>Total</strong></td>
                            <td><strong>{score:.0f}</strong></td>
                            <td><strong>100</strong></td>
                            <td></td>
                        </tr>
                    </tbody>
                </table>
            </div>
            <div class="chart-container">
                {f'<img src="{chart_images["risk_bars"]}" alt="Risk Profile">' if chart_images.get('risk_bars') else '<div class="chart-placeholder">Chart not available</div>'}
            </div>
        </div>
    </div>

    <!-- Cost Analysis -->
    <div class="section">
        <h2>Interconnection Cost Analysis</h2>
        <div class="two-col">
            <div>
                <table class="data-table">
                    <thead>
                        <tr><th>Percentile</th><th>Total Cost</th><th>$/kW</th></tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>P25 (Low)</td>
                            <td>${cost_data['total_millions']['p25']:.0f}M</td>
                            <td>${cost_data['per_kw']['p25']:.0f}/kW</td>
                        </tr>
                        <tr class="highlight-row">
                            <td><strong>P50 (Median)</strong></td>
                            <td><strong>${cost_data['total_millions']['p50']:.0f}M</strong></td>
                            <td><strong>${cost_data['per_kw']['p50']:.0f}/kW</strong></td>
                        </tr>
                        <tr>
                            <td>P75 (High)</td>
                            <td>${cost_data['total_millions']['p75']:.0f}M</td>
                            <td>${cost_data['per_kw']['p75']:.0f}/kW</td>
                        </tr>
                    </tbody>
                </table>
                <div class="note">
                    <strong>Confidence:</strong> {cost_data['confidence']}<br>
                    <strong>Based on:</strong> {cost_data['n_comparables']} comparable {region} projects
                </div>
            </div>
            <div class="chart-container">
                {f'<img src="{chart_images["cost_scatter"]}" alt="Cost Comparison">' if chart_images.get('cost_scatter') else '<div class="chart-placeholder">Chart not available</div>'}
            </div>
        </div>
    </div>

    <!-- Timeline Analysis -->
    <div class="section">
        <h2>Timeline Analysis</h2>
        <table class="data-table" style="width: 70%;">
            <thead>
                <tr><th>Percentile</th><th>Remaining</th><th>Target COD</th></tr>
            </thead>
            <tbody>
                <tr>
                    <td>P25 (Fast)</td>
                    <td>{timeline_data['remaining_p25']:.0f} months</td>
                    <td>{quarter(timeline_data['remaining_p25'])}</td>
                </tr>
                <tr class="highlight-row">
                    <td><strong>P50 (Typical)</strong></td>
                    <td><strong>{timeline_data['remaining_p50']:.0f} months</strong></td>
                    <td><strong>{quarter(timeline_data['remaining_p50'])}</strong></td>
                </tr>
                <tr>
                    <td>P75 (Slow)</td>
                    <td>{timeline_data['remaining_p75']:.0f} months</td>
                    <td>{quarter(timeline_data['remaining_p75'])}</td>
                </tr>
            </tbody>
        </table>
        <div class="note">
            <strong>Completion Rate:</strong> {completion_rate} |
            <strong>Region rate:</strong> {completion_data['region_rate']*100:.1f}% |
            <strong>Type rate:</strong> {completion_data['type_rate']*100:.1f}%
        </div>
    </div>

    <!-- Developer Analysis -->
    <div class="section">
        <h2>Developer Analysis</h2>
        <table class="data-table" style="width: 60%;">
            <tr><th>Developer</th><td>{basic['developer']}</td></tr>
            <tr><th>Total Projects</th><td>{cross_rto.get('total_projects', 0)}</td></tr>
            <tr><th>Total Capacity</th><td>{cross_rto.get('total_capacity_mw', 0)/1000:.1f} GW</td></tr>
            <tr><th>ISOs Present</th><td>{', '.join(cross_rto.get('isos', [])) or 'N/A'}</td></tr>
            <tr><th>Assessment</th><td>{cross_rto.get('assessment', 'Unknown')}</td></tr>
        </table>
    </div>

    <!-- Risk Assessment -->
    <div class="section">
        <h2>Risk Assessment</h2>
        <table class="data-table" style="margin-bottom: 20px;">
            <thead>
                <tr><th>Category</th><th>Risk Level</th><th>Driver</th></tr>
            </thead>
            <tbody>
                {_build_risk_matrix_rows(breakdown, cost_data, cross_rto, basic)}
            </tbody>
        </table>
        <div class="two-col">
            <div>
                <h3 style="color: #dc2626;">Red Flags</h3>
                <ul class="flag-list">
                    {''.join(f'<li class="red-flag">{flag}</li>' for flag in red_flags) or '<li class="no-flag">No red flags identified</li>'}
                </ul>
            </div>
            <div>
                <h3 style="color: #16a34a;">Green Flags</h3>
                <ul class="flag-list">
                    {''.join(f'<li class="green-flag">{flag}</li>' for flag in green_flags) or '<li class="no-flag">No notable strengths identified</li>'}
                </ul>
            </div>
        </div>
    </div>

    {_build_market_data_section(market_data, region, basic.get('type', 'Unknown'))}

    <!-- Recommendation -->
    <div class="section">
        <h2>Recommendation</h2>
        <div class="recommendation-box" style="border-color: {rec_color};">
            <div class="recommendation-badge {rec_class}" style="font-size: 16px;">{rec}</div>
            <p class="rec-text">
                {_get_recommendation_text(rec, score, cross_rto)}
            </p>
        </div>
    </div>

    <!-- Due Diligence Checklist -->
    <div class="section">
        <h2>Due Diligence Checklist</h2>
        <ul class="checklist">
            <li>Obtain and review interconnection study documents</li>
            <li>Validate cost estimate against actual study documents</li>
            <li>Confirm current study phase with {region}</li>
            <li>Research developer ownership and financial backing</li>
            <li>Review transmission constraints at POI</li>
            <li>Verify developer financial capability for interconnection costs</li>
            <li>Assess regulatory and permitting status</li>
            {''.join(f'<li class="investigate">Investigate: {flag}</li>' for flag in red_flags[:3])}
        </ul>
    </div>

    <!-- Footer -->
    <div class="footer">
        <div class="disclaimer">
            <strong>Disclaimer:</strong> This assessment combines automated data extraction, scoring models, and benchmark-based estimates.
            All findings should be validated through manual review of source documents.
        </div>
        <div class="generated">
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} |
            Score: {score:.0f}/100 |
            Recommendation: {rec}
        </div>
    </div>
</body>
</html>'''

    return html


def _build_risk_matrix_rows(breakdown: Dict, cost_data: Dict, cross_rto: Dict, basic: Dict) -> str:
    """Build risk matrix table rows."""
    rows = []

    def risk_badge(level):
        colors = {'LOW': 'badge-low', 'MEDIUM': 'badge-medium', 'HIGH': 'badge-high'}
        return f'<span class="badge {colors.get(level, "")}">{level}</span>'

    def score_to_risk(score_val, max_val):
        pct = score_val / max_val if max_val > 0 else 0
        if pct >= 0.7:
            return 'LOW'
        elif pct >= 0.4:
            return 'MEDIUM'
        return 'HIGH'

    # Technical
    tech_level = score_to_risk(breakdown['study_progress'], 25)
    rows.append(f'<tr><td>Technical</td><td>{risk_badge(tech_level)}</td><td>Study progress</td></tr>')

    # Cost
    conf = cost_data.get('confidence', 'Medium').lower()
    cost_level = 'LOW' if 'high' in conf else ('MEDIUM' if 'medium' in conf else 'HIGH')
    rows.append(f'<tr><td>Cost</td><td>{risk_badge(cost_level)}</td><td>{cost_data.get("n_comparables", 0)} comparables</td></tr>')

    # Timeline
    time_level = score_to_risk(breakdown['study_progress'], 25)
    rows.append(f'<tr><td>Timeline</td><td>{risk_badge(time_level)}</td><td>{basic.get("months_in_queue", 0):.0f} months in queue</td></tr>')

    # Developer
    dev_projects = cross_rto.get('total_projects', 0)
    dev_level = 'LOW' if dev_projects >= 5 else ('MEDIUM' if dev_projects >= 2 else 'HIGH')
    rows.append(f'<tr><td>Developer</td><td>{risk_badge(dev_level)}</td><td>{dev_projects} projects</td></tr>')

    # POI
    poi_level = score_to_risk(breakdown['poi_congestion'], 15)
    rows.append(f'<tr><td>Queue/POI</td><td>{risk_badge(poi_level)}</td><td>POI congestion</td></tr>')

    return '\n'.join(rows)


def _build_market_data_section(market_data: Dict, region: str, project_type: str) -> str:
    """Build market data section HTML."""
    if not market_data:
        return ""

    sections = []

    # Revenue analysis
    revenue = market_data.get('revenue', {})
    capacity = market_data.get('capacity', {})
    total_rev = market_data.get('total_annual_revenue', 0)

    if revenue or capacity:
        energy_rev = revenue.get('annual_revenue', 0)
        cap_rev = capacity.get('annual_capacity_value', 0)

        sections.append(f'''
    <div class="section">
        <h2>Market & Revenue Analysis</h2>
        <div class="two-col">
            <div class="market-card">
                <h4>Estimated Annual Revenue</h4>
                <table class="data-table">
                    <tr><th>Energy Revenue</th><td>${energy_rev/1e6:.1f}M/yr</td></tr>
                    <tr><th>Capacity Revenue</th><td>${cap_rev/1e6:.1f}M/yr</td></tr>
                    <tr class="highlight-row"><th><strong>Total</strong></th><td><strong>${total_rev/1e6:.1f}M/yr</strong></td></tr>
                </table>
            </div>
            <div class="market-card">
                <h4>Revenue Assumptions</h4>
                <table class="data-table">
                    <tr><th>Effective LMP</th><td>${revenue.get('effective_price_mwh', 0):.2f}/MWh</td></tr>
                    <tr><th>Capacity Factor</th><td>{revenue.get('capacity_factor', 0)*100:.0f}%</td></tr>
                    <tr><th>ELCC</th><td>{capacity.get('elcc', 0)*100:.0f}%</td></tr>
                </table>
            </div>
        </div>
    </div>''')

    # Transmission risk
    transmission = market_data.get('transmission', {})
    if transmission:
        risk_rating = transmission.get('risk_rating', 'Unknown')
        sections.append(f'''
    <div class="section no-break">
        <h2>Transmission & Congestion Risk</h2>
        <table class="data-table" style="width: 60%;">
            <tr><th>Risk Rating</th><td><span class="badge badge-{risk_rating.lower() if risk_rating in ['LOW', 'MEDIUM', 'HIGH'] else 'medium'}">{risk_rating}</span></td></tr>
            <tr><th>Zone</th><td>{transmission.get('zone_id', 'N/A')}</td></tr>
            <tr><th>Congestion Level</th><td>{transmission.get('congestion_level', 'N/A')}</td></tr>
        </table>
    </div>''')

    return '\n'.join(sections)


def _get_recommendation_text(rec: str, score: float, cross_rto: Dict) -> str:
    """Get recommendation explanation text."""
    if rec == 'GO':
        return f"Proceed with standard due diligence. This project scores {score:.0f}/100 with strong fundamentals across scoring categories."
    elif rec == 'CONDITIONAL':
        return f"Enhanced due diligence required. This project scores {score:.0f}/100. Address flagged items before proceeding. Verify developer financial capacity and obtain actual study documents."
    else:
        return f"Pass or require significant risk mitigation. This project scores {score:.0f}/100 with multiple risk factors present. Consider only if risk factors can be contractually mitigated."


# CLI interface
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Deal Report PDF")
    parser.add_argument('project_id', help='Queue ID to analyze')
    parser.add_argument('--client', '-c', default='Confidential', help='Client name')
    parser.add_argument('--output', '-o', help='Output PDF path')
    parser.add_argument('--region', '-r', help='ISO/RTO region')
    parser.add_argument('--no-market-data', action='store_true', help='Skip market data analysis')
    parser.add_argument('--no-charts', action='store_true', help='Skip chart generation')

    args = parser.parse_args()

    try:
        pdf_path = generate_deal_report(
            project_id=args.project_id,
            client_name=args.client,
            region=args.region,
            output_path=args.output,
            include_market_data=not args.no_market_data,
            include_charts=not args.no_charts,
        )
        print(f"\nReport generated: {pdf_path}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
