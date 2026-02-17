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
import html
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

    # Extract project info from both scorer and original row
    proj = score_result['project']
    breakdown = score_result['breakdown']

    # Helper to get value from row with column name flexibility
    def get_row_value(row, keys, default=None):
        for key in keys:
            if key in row.index and pd.notna(row[key]):
                return row[key]
        return default

    # Auto-detect region if not provided
    if region is None:
        region = get_row_value(project_row, ['region', 'iso', 'Region', 'ISO'], 'Unknown')

    # Parse queue date
    queue_date_raw = get_row_value(project_row, ['queue_date_std', 'queue_date', 'Queue Date'])
    if queue_date_raw:
        try:
            # Handle Excel serial number
            if isinstance(queue_date_raw, (int, float)) and queue_date_raw > 30000:
                from datetime import datetime, timedelta
                queue_date = (datetime(1899, 12, 30) + timedelta(days=int(queue_date_raw))).strftime('%Y-%m-%d')
            else:
                queue_date = str(queue_date_raw)
        except:
            queue_date = str(queue_date_raw)
    else:
        queue_date = 'Unknown'

    # Calculate months in queue
    months_in_queue = 0
    try:
        from datetime import datetime
        if queue_date and queue_date != 'Unknown':
            qd = pd.to_datetime(queue_date)
            months_in_queue = max(0, (datetime.now() - qd).days // 30)
    except:
        pass

    # Build basic info dict - prefer original row data over scorer's limited dict
    # Use html.escape() for string values to prevent encoding issues in PDF
    def safe_str(val):
        return html.escape(str(val)) if val else 'Unknown'

    basic = {
        'name': safe_str(proj.get('name', get_row_value(project_row, ['name', 'project_name', 'Name'], 'Unknown'))),
        'developer': safe_str(proj.get('developer', get_row_value(project_row, ['developer', 'Developer'], 'Unknown'))),
        'type': safe_str(proj.get('type', get_row_value(project_row, ['type', 'type_std', 'Type'], 'Unknown'))),
        'capacity_mw': proj.get('capacity_mw', get_row_value(project_row, ['capacity_mw', 'Capacity_MW'], 0)) or 0,
        'state': safe_str(proj.get('state', get_row_value(project_row, ['state', 'State'], 'Unknown'))),
        'county': safe_str(get_row_value(project_row, ['county', 'County'], '')),
        'poi': safe_str(proj.get('poi', get_row_value(project_row, ['poi', 'POI'], 'Unknown'))),
        'queue_date': queue_date,
        'months_in_queue': months_in_queue,
        'status': safe_str(get_row_value(project_row, ['status', 'status_std', 'Status'], 'Active')),
        'study_phase': safe_str(get_row_value(project_row, ['study_phase', 'phase'], 'Unknown')),
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

    # Recommendation class for styling
    rec_class = rec.lower().replace('-', '')

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

    # Generate SVG gauge
    def svg_gauge(score_val, max_val=100):
        pct = min(score_val / max_val, 1.0) if max_val > 0 else 0
        radius = 40
        circumference = 2 * 3.14159 * radius
        stroke_len = pct * circumference

        color_class = rec_class
        return f'''<svg width="100" height="100" viewBox="0 0 100 100">
            <circle cx="50" cy="50" r="{radius}" class="gauge-bg"/>
            <circle cx="50" cy="50" r="{radius}" class="gauge-fill {color_class}"
                stroke-dasharray="{stroke_len} {circumference}"
                stroke-dashoffset="0"/>
        </svg>'''

    # Indicator helper (circle badges)
    def indicator(score_val, max_val):
        pct = score_val / max_val if max_val > 0 else 0
        if pct >= 0.7:
            return '<span class="indicator indicator-green">&#10003;</span>'
        elif pct >= 0.4:
            return '<span class="indicator indicator-yellow">!</span>'
        else:
            return '<span class="indicator indicator-red">&#10007;</span>'

    # Investment thesis
    thesis = _generate_thesis(rec, score, basic, cost_data, timeline_data, completion_data, cross_rto)

    # Build HTML
    html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Interconnection Feasibility Assessment - {basic['name']}</title>
</head>
<body>
    <!-- Premium Header -->
    <div class="header">
        <div class="header-badge">FEASIBILITY ASSESSMENT</div>
        <h1>{basic['name']}</h1>
        <div class="header-project">{basic['capacity_mw']:,.0f} MW {basic['type']} | {basic['state']} | {region}</div>
        <div class="header-meta">
            <span>Queue ID: {project_id}</span>
            <span>Developer: {basic['developer']}</span>
            <span>Prepared for: {client_name}</span>
            <span>{datetime.now().strftime('%B %d, %Y')}</span>
        </div>
    </div>

    <!-- Executive Summary -->
    <div class="section">
        <h2>Executive Summary</h2>

        <!-- Investment Thesis -->
        <div class="thesis-box">
            <div class="thesis-title">Investment Thesis</div>
            <div class="thesis-content">{thesis}</div>
        </div>

        <div class="exec-grid">
            <!-- Score Card with Gauge -->
            <div class="score-card {rec_class}">
                <div class="score-gauge">
                    {svg_gauge(score)}
                    <div class="score-center">
                        <div class="score-number">{score:.0f}</div>
                        <div class="score-max">/ 100</div>
                    </div>
                </div>
                <div class="verdict-pill {rec_class}">{rec}</div>
                <div class="score-meta">Grade {grade} | {confidence} Confidence</div>
            </div>

            <!-- KPI Grid -->
            <div class="kpi-grid">
                <div class="kpi-card cost">
                    <div class="kpi-label">Estimated IC Cost</div>
                    <div class="kpi-value">{cost_range}</div>
                    <div class="kpi-detail">P25-P75 range | {cost_data['n_comparables']} comparables</div>
                </div>
                <div class="kpi-card prob">
                    <div class="kpi-label">Completion Probability</div>
                    <div class="kpi-value">{completion_rate}</div>
                    <div class="kpi-detail">Based on {region} historical data</div>
                </div>
                <div class="kpi-card cod">
                    <div class="kpi-label">Target COD (P50)</div>
                    <div class="kpi-value">{quarter(timeline_data['remaining_p50'])}</div>
                    <div class="kpi-detail">{timeline_data['remaining_p50']:.0f} months remaining</div>
                </div>
                <div class="kpi-card comp">
                    <div class="kpi-label">Time in Queue</div>
                    <div class="kpi-value">{basic['months_in_queue']:.0f} mo</div>
                    <div class="kpi-detail">Since {basic['queue_date']}</div>
                </div>
            </div>
        </div>

        <!-- Risk Alert -->
        <div class="risk-alert">
            <div class="risk-alert-item">
                <div class="risk-alert-label">Key Risk</div>
                <div class="risk-alert-value">{red_flags[0] if red_flags else 'No critical risks identified'}</div>
            </div>
            <div class="risk-alert-item">
                <div class="risk-alert-label">Developer Profile</div>
                <div class="risk-alert-value">{cross_rto.get('assessment', 'Unknown track record')}</div>
            </div>
        </div>
    </div>

    <!-- Project Overview -->
    <div class="section">
        <h2>Project Overview</h2>
        <table class="data-table">
            <tr><th style="width:20%;">Queue ID</th><td style="width:30%;">{project_id}</td><th style="width:20%;">Capacity</th><td style="width:30%;">{basic['capacity_mw']:,.0f} MW</td></tr>
            <tr><th>Project Name</th><td colspan="3">{basic['name']}</td></tr>
            <tr><th>Developer</th><td>{basic['developer']}</td><th>State/County</th><td>{basic['state']}{f", {basic['county']}" if basic.get('county') else ''}</td></tr>
            <tr><th>Project Type</th><td>{basic['type']}</td><th>Queue Date</th><td>{basic['queue_date']}</td></tr>
            <tr><th>POI / Substation</th><td>{basic['poi']}</td><th>Current Status</th><td>{basic.get('status', 'Active')}</td></tr>
        </table>
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
                            <td>{indicator(breakdown['queue_position'], 25)}</td>
                        </tr>
                        <tr>
                            <td>Study Progress</td>
                            <td>{breakdown['study_progress']:.1f}</td>
                            <td>25</td>
                            <td>{indicator(breakdown['study_progress'], 25)}</td>
                        </tr>
                        <tr>
                            <td>Developer Track Record</td>
                            <td>{breakdown['developer_track_record']:.1f}</td>
                            <td>20</td>
                            <td>{indicator(breakdown['developer_track_record'], 20)}</td>
                        </tr>
                        <tr>
                            <td>POI Congestion</td>
                            <td>{breakdown['poi_congestion']:.1f}</td>
                            <td>15</td>
                            <td>{indicator(breakdown['poi_congestion'], 15)}</td>
                        </tr>
                        <tr>
                            <td>Project Characteristics</td>
                            <td>{breakdown['project_characteristics']:.1f}</td>
                            <td>15</td>
                            <td>{indicator(breakdown['project_characteristics'], 15)}</td>
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
                {f'<img src="{chart_images["risk_bars"]}" alt="Risk Profile">' if chart_images.get('risk_bars') else '<div class="chart-placeholder">Risk visualization not available</div>'}
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
                            <td>${cost_data['total_millions']['p25']:.1f}M</td>
                            <td>${cost_data['per_kw']['p25']:.0f}/kW</td>
                        </tr>
                        <tr class="highlight-row">
                            <td><strong>P50 (Median)</strong></td>
                            <td><strong>${cost_data['total_millions']['p50']:.1f}M</strong></td>
                            <td><strong>${cost_data['per_kw']['p50']:.0f}/kW</strong></td>
                        </tr>
                        <tr>
                            <td>P75 (High)</td>
                            <td>${cost_data['total_millions']['p75']:.1f}M</td>
                            <td>${cost_data['per_kw']['p75']:.0f}/kW</td>
                        </tr>
                    </tbody>
                </table>
                <div class="note">
                    <strong>Estimate Confidence:</strong> {cost_data['confidence']}<br>
                    <strong>Comparable Projects:</strong> {cost_data['n_comparables']} similar {region} projects analyzed
                </div>
            </div>
            <div class="chart-container">
                {f'<img src="{chart_images["cost_scatter"]}" alt="Cost Comparison">' if chart_images.get('cost_scatter') else '<div class="chart-placeholder">Cost comparison chart not available</div>'}
            </div>
        </div>
    </div>

    <!-- Timeline Analysis -->
    <div class="section">
        <h2>Timeline to Commercial Operation</h2>
        <table class="data-table" style="width: 75%;">
            <thead>
                <tr><th>Scenario</th><th>Remaining Time</th><th>Target COD</th></tr>
            </thead>
            <tbody>
                <tr>
                    <td>Optimistic (P25)</td>
                    <td>{timeline_data['remaining_p25']:.0f} months</td>
                    <td>{quarter(timeline_data['remaining_p25'])}</td>
                </tr>
                <tr class="highlight-row">
                    <td><strong>Base Case (P50)</strong></td>
                    <td><strong>{timeline_data['remaining_p50']:.0f} months</strong></td>
                    <td><strong>{quarter(timeline_data['remaining_p50'])}</strong></td>
                </tr>
                <tr>
                    <td>Conservative (P75)</td>
                    <td>{timeline_data['remaining_p75']:.0f} months</td>
                    <td>{quarter(timeline_data['remaining_p75'])}</td>
                </tr>
            </tbody>
        </table>
        <div class="note">
            <strong>Historical Completion Rate:</strong> {completion_rate} |
            <strong>Region:</strong> {completion_data['region_rate']*100:.1f}% |
            <strong>Technology:</strong> {completion_data['type_rate']*100:.1f}%
        </div>
    </div>

    <!-- Developer Analysis -->
    <div class="section no-break">
        <h2>Developer Analysis</h2>
        <table class="data-table" style="width: 65%;">
            <tr><th style="width: 35%;">Developer</th><td>{basic['developer']}</td></tr>
            <tr><th>Active Projects (All ISOs)</th><td>{cross_rto.get('total_projects', 0)}</td></tr>
            <tr><th>Total Portfolio Capacity</th><td>{cross_rto.get('total_capacity_mw', 0)/1000:.2f} GW</td></tr>
            <tr><th>ISOs with Presence</th><td>{', '.join(cross_rto.get('isos', [])) or 'N/A'}</td></tr>
            <tr><th>Developer Assessment</th><td><strong>{cross_rto.get('assessment', 'Unknown')}</strong></td></tr>
        </table>
    </div>

    <!-- Risk Assessment -->
    <div class="section">
        <h2>Risk Assessment</h2>
        <table class="data-table" style="margin-bottom: 20px;">
            <thead>
                <tr><th style="width:25%;">Risk Category</th><th style="width:20%;">Level</th><th>Key Driver</th></tr>
            </thead>
            <tbody>
                {_build_risk_matrix_rows(breakdown, cost_data, cross_rto, basic)}
            </tbody>
        </table>

        <div class="flags-grid">
            <div class="flags-col">
                <div class="flags-header red">Red Flags</div>
                <ul class="flag-list">
                    {''.join(f'<li class="red-flag">{flag}</li>' for flag in red_flags) or '<li class="no-flag">No critical red flags identified</li>'}
                </ul>
            </div>
            <div class="flags-col">
                <div class="flags-header green">Green Flags</div>
                <ul class="flag-list">
                    {''.join(f'<li class="green-flag">{flag}</li>' for flag in green_flags) or '<li class="no-flag">No notable strengths identified</li>'}
                </ul>
            </div>
        </div>
    </div>

    {_build_market_data_section(market_data, region, basic.get('type', 'Unknown'))}

    <!-- Recommendation -->
    <div class="section">
        <h2>Investment Recommendation</h2>
        <div class="recommendation-box {rec_class}">
            <div class="recommendation-verdict">{rec}</div>
            <div class="recommendation-text">
                {_get_recommendation_text(rec, score, cross_rto, basic, cost_data)}
            </div>
        </div>
    </div>

    <!-- Due Diligence Checklist -->
    <div class="section no-break">
        <h2>Due Diligence Checklist</h2>
        <ul class="checklist">
            <li>Obtain and review interconnection study documents (SIS/Facilities Study)</li>
            <li>Validate IC cost estimate against actual study documents</li>
            <li>Confirm current study phase status with {region}</li>
            <li>Research developer ownership structure and financial backing</li>
            <li>Review transmission constraints and upgrade requirements at POI</li>
            <li>Verify developer financial capability for IC cost exposure</li>
            <li>Assess regulatory timeline and permitting status</li>
            {''.join(f'<li class="priority">PRIORITY: Investigate {flag}</li>' for flag in red_flags[:3])}
        </ul>
    </div>

    <!-- Footer -->
    <div class="footer">
        <div class="footer-disclaimer">
            <strong>Disclaimer:</strong> This assessment combines automated data extraction, proprietary scoring models, and benchmark-based estimates derived from historical interconnection data.
            All findings should be validated through independent review of primary source documents including ISO interconnection agreements and study reports.
            This report does not constitute investment advice.
        </div>
        <div class="footer-generated">
            Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} | Feasibility Score: {score:.0f}/100 | Recommendation: {rec}
        </div>
    </div>
</body>
</html>'''

    return html


def _generate_thesis(rec: str, score: float, basic: Dict, cost_data: Dict, timeline_data: Dict, completion_data: Dict, cross_rto: Dict) -> str:
    """Generate investment thesis summary."""
    capacity = basic.get('capacity_mw', 0)
    project_type = basic.get('type', 'renewable')
    developer_projects = cross_rto.get('total_projects', 0)

    if rec == 'GO':
        return (
            f"This {capacity:,.0f} MW {project_type} project presents a compelling acquisition opportunity with a feasibility score of {score:.0f}/100. "
            f"Key strengths include {'an established developer with ' + str(developer_projects) + ' active projects' if developer_projects >= 5 else 'favorable queue positioning'} "
            f"and an estimated IC cost of {cost_data['per_kw']['p50']:.0f}/kW (P50). "
            f"Historical completion rates for comparable projects suggest a {completion_data['combined_rate']*100:.0f}% probability of reaching COD. "
            f"Standard due diligence is recommended."
        )
    elif rec == 'CONDITIONAL':
        return (
            f"This {capacity:,.0f} MW {project_type} project merits consideration with enhanced due diligence. "
            f"At {score:.0f}/100, the project shows potential but presents identified risks requiring mitigation. "
            f"IC cost estimates range from ${cost_data['total_millions']['p25']:.0f}M to ${cost_data['total_millions']['p75']:.0f}M, "
            f"suggesting material execution risk. Developer track record analysis and verification of study documents should be prioritized "
            f"before proceeding."
        )
    else:
        return (
            f"This {capacity:,.0f} MW {project_type} project scores {score:.0f}/100, indicating significant execution risk. "
            f"Multiple risk factors are present that may impact project viability. "
            f"If proceeding, substantial risk mitigation through contractual protections and enhanced due diligence would be required. "
            f"Consider only if strategic value justifies elevated risk profile."
        )


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


def _get_recommendation_text(rec: str, score: float, cross_rto: Dict, basic: Dict = None, cost_data: Dict = None) -> str:
    """Get recommendation explanation text with PE-focused language."""
    basic = basic or {}
    cost_data = cost_data or {}

    capacity = basic.get('capacity_mw', 0)
    cost_p50 = cost_data.get('total_millions', {}).get('p50', 0)
    developer_projects = cross_rto.get('total_projects', 0)

    if rec == 'GO':
        return (
            f"<strong>Proceed with standard acquisition due diligence.</strong> "
            f"This project scores {score:.0f}/100 demonstrating strong fundamentals across all evaluation criteria. "
            f"Estimated capital requirement of ${cost_p50:.0f}M for interconnection is within market norms. "
            f"{'Developer track record supports execution confidence. ' if developer_projects >= 5 else ''}"
            f"Recommend proceeding to detailed technical and commercial review."
        )
    elif rec == 'CONDITIONAL':
        return (
            f"<strong>Enhanced due diligence required before proceeding.</strong> "
            f"Project scores {score:.0f}/100 with identified risk factors requiring investigation. "
            f"Key diligence items: (1) obtain and validate interconnection study documents, "
            f"(2) verify developer financial capacity for ${cost_p50:.0f}M IC exposure, "
            f"(3) assess contractual mechanisms to mitigate flagged risks. "
            f"May warrant adjusted valuation or enhanced deal protections."
        )
    else:
        return (
            f"<strong>Pass or require substantial risk mitigation.</strong> "
            f"Project scores {score:.0f}/100 with multiple material risk factors. "
            f"Execution risk appears elevated based on current information. "
            f"If strategic rationale exists, consider only with significant contractual protections, "
            f"adjusted pricing reflecting risk, or milestone-based earn-out structure."
        )


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
