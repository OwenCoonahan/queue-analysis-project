#!/usr/bin/env python3
"""
HTML Report Generator

Generates professional HTML reports with embedded charts and styling.

Usage:
    python3 html_report.py 1738 --client "KPMG" -o report.html
"""

import argparse
import base64
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from analyze import QueueData, QueueAnalyzer
from scoring import FeasibilityScorer
from real_data import RealDataEstimator
from scrapers import comprehensive_developer_research, NYISODocumentFetcher

# Import chart and historical data modules
try:
    import charts_altair as charts
    from historical_data import HistoricalData
    CHARTS_AVAILABLE = True
except ImportError:
    CHARTS_AVAILABLE = False

# Import market data modules
try:
    from lmp_data import RevenueEstimator
    from capacity_data import CapacityValue
    from transmission_data import ConstraintAnalysis
    from ppa_data import PPABenchmarks
    from permitting_data import PermitAnalysis
    MARKET_DATA_AVAILABLE = True
except ImportError:
    MARKET_DATA_AVAILABLE = False


def embed_image(image_path: str) -> str:
    """Convert image to base64 for embedding in HTML."""
    try:
        with open(image_path, 'rb') as f:
            data = base64.b64encode(f.read()).decode('utf-8')
        return f"data:image/png;base64,{data}"
    except Exception:
        return ""


def generate_html_report(
    project_id: str,
    df,
    client_name: str = "[CLIENT]",
    region: str = "NYISO",
    output_dir: str = "output"
) -> str:
    """Generate comprehensive HTML report."""

    # Initialize components
    scorer = FeasibilityScorer(df)
    real_estimator = RealDataEstimator()

    # Get score
    print(f"[1/5] Scoring project {project_id}...")
    score_result = scorer.score_project(project_id=project_id)

    if 'error' in score_result:
        return f"<html><body>Error: {score_result['error']}</body></html>"

    # Get project info
    proj = score_result['project']
    breakdown = score_result['breakdown']

    # Basic info
    basic = {
        'name': proj.get('name', 'Unknown'),
        'developer': proj.get('developer', 'Unknown'),
        'type': proj.get('type', 'Unknown'),
        'capacity_mw': proj.get('capacity_mw', 0),
        'state': proj.get('state', 'Unknown'),
        'poi': proj.get('poi', 'Unknown'),
        'queue_date': proj.get('queue_date', 'Unknown'),
        'months_in_queue': proj.get('months_in_queue', 0),
    }

    # Get real data estimates
    print(f"[2/5] Computing estimates from historical data...")
    real_estimates = real_estimator.estimate_project(
        region=region,
        project_type=basic['type'],
        capacity_mw=basic['capacity_mw'],
        months_in_queue=basic['months_in_queue']
    )

    cost_data = real_estimates['cost']
    timeline_data = real_estimates['timeline']
    completion_data = real_estimates['completion']

    # Format estimates
    cost_range = real_estimator.format_cost_range(cost_data)
    timeline_range = real_estimator.format_timeline_range(timeline_data)
    completion_rate = real_estimator.format_completion_rate(completion_data)

    # Get external research
    print(f"[3/7] Running external research...")
    external = comprehensive_developer_research(basic['developer'])
    cross_rto = external.get('cross_rto', {})

    # Get document guidance
    print(f"[4/7] Getting document guidance...")
    doc_fetcher = NYISODocumentFetcher()
    doc_info = doc_fetcher.get_document_links(project_id)

    # Get market data (revenue, transmission, PPA, permits)
    market_data = {}
    if MARKET_DATA_AVAILABLE:
        print(f"[5/7] Computing market data...")
        market_data = _get_market_data(
            region=region,
            capacity_mw=basic['capacity_mw'],
            technology=basic['type'],
            state=basic['state'],
            poi=basic['poi']
        )

    # Generate charts
    chart_images = {}
    if CHARTS_AVAILABLE:
        print(f"[6/7] Generating charts...")
        chart_images = _generate_charts(
            project_id, region, basic, cost_data, timeline_data,
            breakdown, cross_rto, output_dir
        )

    # Build HTML
    print(f"[7/7] Building HTML report...")
    html = _build_html(
        project_id=project_id,
        region=region,
        client_name=client_name,
        basic=basic,
        score_result=score_result,
        breakdown=breakdown,
        cost_data=cost_data,
        timeline_data=timeline_data,
        completion_data=completion_data,
        cost_range=cost_range,
        timeline_range=timeline_range,
        completion_rate=completion_rate,
        external=external,
        cross_rto=cross_rto,
        doc_info=doc_info,
        chart_images=chart_images,
        market_data=market_data,
    )

    return html


def _get_market_data(
    region: str,
    capacity_mw: float,
    technology: str,
    state: str,
    poi: str = None
) -> Dict[str, Any]:
    """Get market data from all modules."""
    market_data = {}

    try:
        # Revenue estimates (LMP + Capacity)
        rev = RevenueEstimator()
        revenue = rev.estimate_annual_revenue(
            region=region,
            capacity_mw=capacity_mw,
            technology=technology,
            state=state,
            poi=poi
        )
        market_data['revenue'] = revenue

        # Capacity value
        cap = CapacityValue()
        capacity = cap.calculate_capacity_value(
            region=region,
            capacity_mw=capacity_mw,
            technology=technology
        )
        market_data['capacity'] = capacity

        # Combined annual revenue
        energy_rev = revenue.get('annual_revenue', 0)
        cap_rev = capacity.get('annual_capacity_value', 0)
        market_data['total_annual_revenue'] = energy_rev + cap_rev

        # Transmission/congestion risk
        tx = ConstraintAnalysis()
        transmission = tx.assess_poi_risk(
            region=region,
            poi=poi,
            state=state
        )
        market_data['transmission'] = transmission

        # PPA comparison
        ppa = PPABenchmarks()
        ppa_data = ppa.compare_merchant_vs_ppa(
            region=region,
            technology=technology,
            capacity_mw=capacity_mw
        )
        market_data['ppa'] = ppa_data

        # Permitting risk
        permit = PermitAnalysis()
        permits = permit.assess_permit_risk(
            state=state,
            technology=technology,
            capacity_mw=capacity_mw
        )
        market_data['permits'] = permits

    except Exception as e:
        market_data['error'] = str(e)

    return market_data


def _generate_charts(
    project_id: str,
    region: str,
    basic: Dict,
    cost_data: Dict,
    timeline_data: Dict,
    breakdown: Dict,
    cross_rto: Dict,
    output_dir: str
) -> Dict[str, str]:
    """Generate charts using Altair and return embedded image data."""
    chart_images = {}
    charts_dir = Path(output_dir) / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    # Altair saves to tools/charts/ by default
    altair_charts_dir = Path(__file__).parent / "charts"

    try:
        hd = HistoricalData()

        this_project = {
            'capacity_mw': basic.get('capacity_mw', 0),
            'cost_low': cost_data['per_kw']['p25'],
            'cost_median': cost_data['per_kw']['p50'],
            'cost_high': cost_data['per_kw']['p75'],
            'type': basic.get('type', 'unknown'),
            'region': region,
            'timeline_low': timeline_data['remaining_p25'],
            'timeline_likely': timeline_data['remaining_p50'],
            'timeline_high': timeline_data['remaining_p75'],
        }

        # 1. Cost Scatter (Altair)
        region_costs = hd.ic_costs_by_region.get(region)
        if region_costs is None or (hasattr(region_costs, 'empty') and region_costs.empty):
            region_costs = hd.ic_costs_df
        if region_costs is not None and len(region_costs) > 0:
            charts.cost_scatter(region_costs, this_project, f'{region} Interconnection Cost Comparison')
            path = altair_charts_dir / 'cost_scatter_altair.png'
            chart_images['cost_scatter'] = embed_image(str(path))

        # 2. Queue Outcomes Donut (Altair - replaces funnel)
        project_type = basic.get('type', 'Solar')
        funnel_data = hd.get_completion_funnel(region, project_type, year_range=(2000, 2024))
        if funnel_data and 'error' not in funnel_data:
            outcomes = {
                'Active': int(funnel_data.get('active_in_queue', 0)),
                'Withdrawn': int(funnel_data.get('withdrawn', 0)),
                'Completed': int(funnel_data.get('completed', 0)),
            }
            charts.queue_outcomes(outcomes, 'Active', f'{region} {project_type} Queue Outcomes')
            path = altair_charts_dir / 'queue_outcomes_altair.png'
            chart_images['completion_funnel'] = embed_image(str(path))
            chart_images['funnel_data'] = funnel_data

        # 3. Risk Bars (Altair - cleaner than radar)
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
        charts.risk_bars(score_data, max_scores, title='Project Risk Profile')
        path = altair_charts_dir / 'risk_bars_altair.png'
        chart_images['risk_radar'] = embed_image(str(path))

        # 4. Timeline Comparison (Altair)
        if hd.queued_up_df is not None:
            operational = hd.queued_up_df[hd.queued_up_df['q_status'] == 'operational'].copy()
            if 'q_date_parsed' in operational.columns and 'on_date_parsed' in operational.columns:
                operational = operational[
                    operational['q_date_parsed'].notna() &
                    operational['on_date_parsed'].notna()
                ]
                operational['months_to_cod'] = (
                    operational['on_date_parsed'] - operational['q_date_parsed']
                ).dt.days / 30
                if len(operational) > 10:
                    charts.timeline_comparison(operational, this_project, 'Time to COD by Region')
                    path = altair_charts_dir / 'timeline_altair.png'
                    chart_images['timeline_boxplot'] = embed_image(str(path))

    except Exception as e:
        print(f"Warning: Chart generation error: {e}")
        import traceback
        traceback.print_exc()

    return chart_images


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
    cost_range: str,
    timeline_range: str,
    completion_rate: str,
    external: Dict,
    cross_rto: Dict,
    doc_info: Dict,
    chart_images: Dict,
    market_data: Dict = None,
) -> str:
    """Build the complete HTML report."""

    # Recommendation styling
    rec = score_result['recommendation']
    rec_color = {'GO': '#28a745', 'CONDITIONAL': '#ffc107', 'NO-GO': '#dc3545'}.get(rec, '#6c757d')
    grade = score_result['grade']
    score = score_result['total_score']

    # Risk level helper
    def risk_badge(level):
        colors = {'Low': '#28a745', 'Medium': '#ffc107', 'High': '#dc3545'}
        return f'<span class="badge" style="background:{colors.get(level, "#6c757d")}">{level}</span>'

    # COD dates
    from dateutil.relativedelta import relativedelta
    now = datetime.now()
    def quarter(months):
        dt = now + relativedelta(months=months)
        return f"Q{(dt.month - 1) // 3 + 1} {dt.year}"

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Interconnection Feasibility Assessment - {basic['name']}</title>
    <style>
        :root {{
            --primary: #2c3e50;
            --secondary: #3498db;
            --success: #28a745;
            --warning: #ffc107;
            --danger: #dc3545;
            --light: #f8f9fa;
            --dark: #343a40;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            line-height: 1.6;
            color: #333;
            background: #f5f5f5;
        }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
        .header {{
            background: linear-gradient(135deg, var(--primary) 0%, #34495e 100%);
            color: white;
            padding: 40px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        .header h1 {{ font-size: 2rem; margin-bottom: 10px; }}
        .header .meta {{ opacity: 0.9; font-size: 0.95rem; }}
        .card {{
            background: white;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            padding: 25px;
            margin-bottom: 25px;
        }}
        .card h2 {{
            color: var(--primary);
            border-bottom: 2px solid var(--secondary);
            padding-bottom: 10px;
            margin-bottom: 20px;
            font-size: 1.4rem;
        }}
        .grid {{ display: grid; gap: 25px; }}
        .grid-2 {{ grid-template-columns: repeat(2, 1fr); }}
        .grid-3 {{ grid-template-columns: repeat(3, 1fr); }}
        @media (max-width: 768px) {{
            .grid-2, .grid-3 {{ grid-template-columns: 1fr; }}
        }}
        .summary-box {{
            background: var(--light);
            border-left: 4px solid {rec_color};
            padding: 20px;
            border-radius: 0 8px 8px 0;
        }}
        .score-display {{
            text-align: center;
            padding: 30px;
        }}
        .score-circle {{
            width: 120px;
            height: 120px;
            border-radius: 50%;
            background: conic-gradient({rec_color} {score*3.6}deg, #e9ecef {score*3.6}deg);
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0 auto 15px;
        }}
        .score-inner {{
            width: 100px;
            height: 100px;
            border-radius: 50%;
            background: white;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
        }}
        .score-number {{ font-size: 2rem; font-weight: bold; color: var(--primary); }}
        .score-label {{ font-size: 0.9rem; color: #666; }}
        .recommendation {{
            display: inline-block;
            padding: 8px 20px;
            border-radius: 20px;
            font-weight: bold;
            color: white;
            background: {rec_color};
        }}
        table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
        th, td {{ padding: 12px 15px; text-align: left; border-bottom: 1px solid #eee; }}
        th {{ background: var(--light); font-weight: 600; color: var(--primary); }}
        tr:hover {{ background: #fafafa; }}
        .badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85rem;
            font-weight: 500;
            color: white;
        }}
        .chart-container {{ text-align: center; margin: 20px 0; }}
        .chart-container img {{ max-width: 100%; height: auto; border-radius: 8px; }}
        .metric-card {{
            text-align: center;
            padding: 20px;
            background: var(--light);
            border-radius: 8px;
        }}
        .metric-value {{ font-size: 1.8rem; font-weight: bold; color: var(--primary); }}
        .metric-label {{ font-size: 0.9rem; color: #666; margin-top: 5px; }}
        .flag-list {{ list-style: none; padding: 0; }}
        .flag-list li {{ padding: 8px 0; border-bottom: 1px solid #eee; }}
        .flag-list li:last-child {{ border-bottom: none; }}
        .red-flag {{ color: var(--danger); }}
        .green-flag {{ color: var(--success); }}
        .checklist {{ list-style: none; padding: 0; }}
        .checklist li {{ padding: 10px 0; border-bottom: 1px solid #eee; }}
        .checklist li:before {{ content: "\\2610"; margin-right: 10px; color: var(--secondary); }}
        .footer {{
            text-align: center;
            padding: 20px;
            color: #666;
            font-size: 0.85rem;
        }}
        .progress-bar {{
            height: 8px;
            background: #e9ecef;
            border-radius: 4px;
            overflow: hidden;
            margin-top: 5px;
        }}
        .progress-fill {{ height: 100%; border-radius: 4px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Interconnection Feasibility Assessment</h1>
            <div class="meta">
                <strong>{basic['name']}</strong> | Queue ID: {project_id} | {region}<br>
                Prepared for: {client_name} | {datetime.now().strftime('%B %d, %Y')}
            </div>
        </div>

        <!-- Verdict Banner -->
        <div class="card" style="background: linear-gradient(135deg, {rec_color}22 0%, {rec_color}11 100%); border-left: 5px solid {rec_color};">
            <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap;">
                <div>
                    <h2 style="border:none; margin:0; padding:0;">VERDICT: <span style="color:{rec_color}">{rec}</span></h2>
                    <div style="color:#666; margin-top:5px;">Confidence: <strong>{score_result.get('confidence', 'Medium')}</strong></div>
                </div>
                <div style="text-align:right;">
                    <div style="font-size:2.5rem; font-weight:bold; color:{rec_color}">{score:.0f}<span style="font-size:1rem; color:#666">/100</span></div>
                    <div style="color:#666;">Grade: {grade}</div>
                </div>
            </div>
        </div>

        <!-- Executive Summary -->
        <div class="card">
            <h2>Executive Summary</h2>
            <div class="grid grid-3">
                <div class="score-display">
                    <div class="score-circle">
                        <div class="score-inner">
                            <span class="score-number">{score:.0f}</span>
                            <span class="score-label">/ 100</span>
                        </div>
                    </div>
                    <div class="recommendation">{rec}</div>
                    <div style="margin-top:10px;color:#666">Confidence: {score_result.get('confidence', 'Medium')}</div>
                </div>
                <div>
                    <div class="metric-card" style="margin-bottom:15px">
                        <div class="metric-value">{cost_range}</div>
                        <div class="metric-label">Estimated Cost (P25-P75)</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{completion_rate}</div>
                        <div class="metric-label">Completion Probability</div>
                    </div>
                </div>
                <div>
                    <div class="metric-card" style="margin-bottom:15px">
                        <div class="metric-value">{timeline_range}</div>
                        <div class="metric-label">Target COD Range</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{cost_data['n_comparables']}</div>
                        <div class="metric-label">Comparable Projects</div>
                    </div>
                </div>
            </div>
            <div class="summary-box" style="margin-top:20px">
                <strong>Key Risk:</strong> {score_result['red_flags'][0] if score_result['red_flags'] else 'No critical risks identified'}<br>
                <strong>Developer:</strong> {cross_rto.get('assessment', 'Unknown track record')}
            </div>
        </div>

        <!-- Project Overview -->
        <div class="card">
            <h2>Project Overview</h2>
            <div class="grid grid-2">
                <table>
                    <tr><th>Queue ID</th><td>{project_id}</td></tr>
                    <tr><th>Project Name</th><td>{basic['name']}</td></tr>
                    <tr><th>Developer</th><td>{basic['developer']}</td></tr>
                    <tr><th>Project Type</th><td>{basic['type']}</td></tr>
                </table>
                <table>
                    <tr><th>Capacity</th><td>{basic['capacity_mw']:,.0f} MW</td></tr>
                    <tr><th>State</th><td>{basic['state']}</td></tr>
                    <tr><th>Queue Date</th><td>{basic['queue_date']}</td></tr>
                    <tr><th>Time in Queue</th><td>{basic['months_in_queue']} months</td></tr>
                </table>
            </div>
        </div>

        <!-- Score Breakdown -->
        <div class="card">
            <h2>Feasibility Score Breakdown</h2>
            <div class="grid grid-2">
                <div>
                    <table>
                        <tr>
                            <th>Component</th>
                            <th>Score</th>
                            <th>Max</th>
                        </tr>
                        <tr>
                            <td>Queue Position</td>
                            <td>{breakdown['queue_position']:.1f}</td>
                            <td>25</td>
                        </tr>
                        <tr>
                            <td>Study Progress</td>
                            <td>{breakdown['study_progress']:.1f}</td>
                            <td>25</td>
                        </tr>
                        <tr>
                            <td>Developer Track Record</td>
                            <td>{breakdown['developer_track_record']:.1f}</td>
                            <td>20</td>
                        </tr>
                        <tr>
                            <td>POI Congestion</td>
                            <td>{breakdown['poi_congestion']:.1f}</td>
                            <td>15</td>
                        </tr>
                        <tr>
                            <td>Project Characteristics</td>
                            <td>{breakdown['project_characteristics']:.1f}</td>
                            <td>15</td>
                        </tr>
                    </table>
                </div>
                <div class="chart-container">
                    {f'<img src="{chart_images["risk_radar"]}" alt="Risk Radar">' if chart_images.get('risk_radar') else '<p>Chart not available</p>'}
                </div>
            </div>
        </div>

        <!-- Cost Analysis -->
        <div class="card">
            <h2>Cost Analysis</h2>
            <div class="grid grid-2">
                <div>
                    <table>
                        <tr><th>Percentile</th><th>Total Cost</th><th>$/kW</th></tr>
                        <tr>
                            <td>P25 (Low)</td>
                            <td>${cost_data['total_millions']['p25']:.0f}M</td>
                            <td>${cost_data['per_kw']['p25']:.0f}/kW</td>
                        </tr>
                        <tr>
                            <td><strong>P50 (Median)</strong></td>
                            <td><strong>${cost_data['total_millions']['p50']:.0f}M</strong></td>
                            <td><strong>${cost_data['per_kw']['p50']:.0f}/kW</strong></td>
                        </tr>
                        <tr>
                            <td>P75 (High)</td>
                            <td>${cost_data['total_millions']['p75']:.0f}M</td>
                            <td>${cost_data['per_kw']['p75']:.0f}/kW</td>
                        </tr>
                    </table>
                    <div style="margin-top:15px;padding:15px;background:#f8f9fa;border-radius:8px">
                        <strong>Confidence:</strong> {cost_data['confidence']}<br>
                        <strong>Based on:</strong> {cost_data['n_comparables']} comparable {region} projects
                    </div>
                </div>
                <div class="chart-container">
                    {f'<img src="{chart_images["cost_scatter"]}" alt="Cost Comparison">' if chart_images.get('cost_scatter') else '<p>Chart not available</p>'}
                </div>
            </div>
        </div>

        <!-- Timeline Analysis -->
        <div class="card">
            <h2>Timeline Analysis</h2>
            <div class="grid grid-2">
                <div>
                    <table>
                        <tr><th>Percentile</th><th>Remaining</th><th>Target COD</th></tr>
                        <tr>
                            <td>P25 (Fast)</td>
                            <td>{timeline_data['remaining_p25']} months</td>
                            <td>{quarter(timeline_data['remaining_p25'])}</td>
                        </tr>
                        <tr>
                            <td><strong>P50 (Typical)</strong></td>
                            <td><strong>{timeline_data['remaining_p50']} months</strong></td>
                            <td><strong>{quarter(timeline_data['remaining_p50'])}</strong></td>
                        </tr>
                        <tr>
                            <td>P75 (Slow)</td>
                            <td>{timeline_data['remaining_p75']} months</td>
                            <td>{quarter(timeline_data['remaining_p75'])}</td>
                        </tr>
                    </table>
                    <div style="margin-top:15px;padding:15px;background:#f8f9fa;border-radius:8px">
                        <strong>Completion Rate:</strong> {completion_rate}<br>
                        <strong>Region rate:</strong> {completion_data['region_rate']*100:.1f}% (n={completion_data['region_n']})<br>
                        <strong>Type rate:</strong> {completion_data['type_rate']*100:.1f}% (n={completion_data['type_n']})
                    </div>
                </div>
                <div class="chart-container">
                    {f'<img src="{chart_images["timeline_boxplot"]}" alt="Timeline Distribution">' if chart_images.get('timeline_boxplot') else '<p>Chart not available</p>'}
                </div>
            </div>
        </div>

        <!-- Completion Funnel -->
        {_build_funnel_section(chart_images, region, basic.get('type', 'Unknown'))}

        <!-- Risk Assessment -->
        <div class="card">
            <h2>Risk Assessment</h2>

            <!-- Risk Matrix Table -->
            <table style="margin-bottom:25px">
                <tr>
                    <th>Category</th>
                    <th>Risk Level</th>
                    <th>Driver</th>
                </tr>
                {_build_risk_matrix_rows(breakdown, cost_data, cross_rto, basic)}
            </table>

            <div class="grid grid-2">
                <div>
                    <h3 style="color:var(--danger);margin-bottom:15px">Red Flags (HIGH RISK)</h3>
                    <ul class="flag-list">
                        {''.join(f'<li class="red-flag">&#x26A0; {flag}</li>' for flag in score_result.get('red_flags', [])) or '<li>No red flags identified</li>'}
                    </ul>
                </div>
                <div>
                    <h3 style="color:var(--success);margin-bottom:15px">Green Flags (STRENGTHS)</h3>
                    <ul class="flag-list">
                        {''.join(f'<li class="green-flag">&#x2713; {flag}</li>' for flag in score_result.get('green_flags', [])) or '<li>No notable strengths identified</li>'}
                    </ul>
                </div>
            </div>
        </div>

        <!-- Conditional Recommendations -->
        <div class="card">
            <h2>Proceed If (Conditions)</h2>
            {_build_html_conditional_recommendations(score_result, cost_data, timeline_data, cross_rto, basic)}
        </div>

        <!-- Market Data Section -->
        {_build_market_data_section(market_data, region, basic.get('type', 'Unknown'))}

        <!-- Due Diligence Checklist -->
        <div class="card">
            <h2>Due Diligence Checklist</h2>
            <ul class="checklist">
                <li>Obtain and review interconnection study documents</li>
                <li>Validate cost estimate against actual study documents</li>
                <li>Confirm current study phase with {region}</li>
                <li>Research developer ownership and financial backing</li>
                <li>Review transmission constraints at POI</li>
                <li>Verify developer financial capability for interconnection costs</li>
                <li>Assess regulatory and permitting status</li>
                {''.join(f'<li>Investigate: {flag}</li>' for flag in score_result.get('red_flags', [])[:3])}
            </ul>
        </div>

        <!-- Footer -->
        <div class="footer">
            <strong>Disclaimer:</strong> This assessment combines automated data extraction, scoring models, and benchmark-based estimates.
            All findings should be validated through manual review of source documents.<br><br>
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Score: {score:.0f}/100 | Recommendation: {rec}
        </div>
    </div>
</body>
</html>'''

    return html


def _build_funnel_section(chart_images: Dict, region: str, project_type: str) -> str:
    """Build the completion funnel section if data available."""
    if not chart_images.get('completion_funnel'):
        return ""

    funnel = chart_images.get('funnel_data', {})
    return f'''
        <div class="card">
            <h2>Historical Queue Outcomes</h2>
            <div class="grid grid-2">
                <div style="padding:20px">
                    <h3 style="margin-bottom:20px">{region} {project_type} Projects</h3>
                    <div class="metric-card" style="margin-bottom:15px">
                        <div class="metric-value">{funnel.get('total_entered', 0)}</div>
                        <div class="metric-label">Projects Entered Queue</div>
                    </div>
                    <div class="grid grid-2" style="gap:10px">
                        <div class="metric-card">
                            <div class="metric-value" style="color:var(--success)">{funnel.get('completion_rate_pct', 0):.1f}%</div>
                            <div class="metric-label">Completed</div>
                        </div>
                        <div class="metric-card">
                            <div class="metric-value" style="color:var(--danger)">{funnel.get('withdrawal_rate_pct', 0):.1f}%</div>
                            <div class="metric-label">Withdrawn</div>
                        </div>
                    </div>
                </div>
                <div class="chart-container">
                    <img src="{chart_images['completion_funnel']}" alt="Completion Funnel">
                </div>
            </div>
        </div>
    '''


def _risk_badge_html(level: str) -> str:
    """Return HTML badge for risk level."""
    colors = {
        'LOW': '#28a745',
        'MEDIUM': '#ffc107',
        'HIGH': '#dc3545'
    }
    color = colors.get(level.upper(), '#6c757d')
    return f'<span class="badge" style="background:{color}">{level}</span>'


def _risk_level_from_score(score: float, max_score: float) -> str:
    """Get risk level from score."""
    pct = score / max_score
    if pct >= 0.7:
        return "LOW"
    elif pct >= 0.4:
        return "MEDIUM"
    else:
        return "HIGH"


def _build_risk_matrix_rows(breakdown: Dict, cost_data: Dict, cross_rto: Dict, basic: Dict) -> str:
    """Build risk matrix table rows."""
    rows = []

    # Technical risk
    tech_level = _risk_level_from_score(breakdown['study_progress'], 25)
    rows.append(f'''<tr>
        <td><strong>Technical</strong></td>
        <td>{_risk_badge_html(tech_level)}</td>
        <td>Study progress</td>
    </tr>''')

    # Cost risk
    confidence = cost_data.get('confidence', 'Medium').lower()
    if 'high' in confidence:
        cost_level = 'LOW'
    elif 'medium' in confidence:
        cost_level = 'MEDIUM'
    else:
        cost_level = 'HIGH'
    rows.append(f'''<tr>
        <td><strong>Cost</strong></td>
        <td>{_risk_badge_html(cost_level)}</td>
        <td>Based on {cost_data.get('n_comparables', 0)} comparables</td>
    </tr>''')

    # Timeline risk
    time_level = _risk_level_from_score(breakdown['study_progress'], 25)
    rows.append(f'''<tr>
        <td><strong>Timeline</strong></td>
        <td>{_risk_badge_html(time_level)}</td>
        <td>{basic.get('months_in_queue', 0)} months in queue</td>
    </tr>''')

    # Developer risk
    dev_projects = cross_rto.get('total_projects', 0)
    if dev_projects >= 5:
        dev_level = 'LOW'
    elif dev_projects >= 2:
        dev_level = 'MEDIUM'
    else:
        dev_level = 'HIGH'
    rows.append(f'''<tr>
        <td><strong>Developer</strong></td>
        <td>{_risk_badge_html(dev_level)}</td>
        <td>{dev_projects} projects across RTOs</td>
    </tr>''')

    # Queue/POI risk
    poi_level = _risk_level_from_score(breakdown['poi_congestion'], 15)
    rows.append(f'''<tr>
        <td><strong>Queue/POI</strong></td>
        <td>{_risk_badge_html(poi_level)}</td>
        <td>POI congestion level</td>
    </tr>''')

    return '\n'.join(rows)


def _build_html_conditional_recommendations(score_result: Dict, cost_data: Dict, timeline_data: Dict,
                                             cross_rto: Dict, basic: Dict) -> str:
    """Build HTML conditional recommendations."""
    rec = score_result['recommendation']
    items = []

    if rec == 'GO':
        items.append('<li><strong>Standard due diligence</strong> - No special conditions required</li>')
    else:
        # Cost adjustment
        cost_high = cost_data['total_millions']['p75']
        cost_median = cost_data['total_millions']['p50']
        items.append(f'<li><strong>Cost adjustment:</strong> Model ${cost_median:.0f}M-${cost_high:.0f}M interconnection cost in valuation</li>')

        # Timeline adjustment
        from dateutil.relativedelta import relativedelta
        now = datetime.now()
        likely_date = now + relativedelta(months=int(timeline_data['remaining_p50']))
        pessimistic_date = now + relativedelta(months=int(timeline_data['remaining_p75']))
        likely_q = f"Q{(likely_date.month - 1) // 3 + 1} {likely_date.year}"
        pessimistic_q = f"Q{(pessimistic_date.month - 1) // 3 + 1} {pessimistic_date.year}"
        items.append(f'<li><strong>Timeline adjustment:</strong> Model {likely_q} to {pessimistic_q} COD</li>')

        # Developer verification
        if cross_rto.get('total_projects', 0) < 3:
            items.append(f'<li><strong>Developer verification:</strong> Confirm financial capacity for ${cost_median:.0f}M+ interconnection investment</li>')

        # Contractual protection
        if cost_data.get('confidence', 'Medium').lower() in ['low', 'very low', 'medium']:
            items.append('<li><strong>Contractual protection:</strong> Negotiate cost cap or escrow for interconnection overruns</li>')

        # Monitoring
        monitoring_items = []
        if score_result['breakdown']['study_progress'] < 15:
            monitoring_items.append("study completion milestones")
        if score_result['breakdown']['poi_congestion'] < 10:
            monitoring_items.append("competing project withdrawals")
        if monitoring_items:
            items.append(f'<li><strong>Monitoring:</strong> Track quarterly for: {", ".join(monitoring_items)}</li>')

    return f'<ol style="padding-left:20px; line-height:2;">{"".join(items)}</ol>'


def _build_market_data_section(market_data: Dict, region: str, project_type: str) -> str:
    """Build market data section with revenue, transmission, PPA, and permits."""
    if not market_data or 'error' in market_data:
        return ""

    sections = []

    # Revenue Analysis Section
    revenue = market_data.get('revenue', {})
    capacity = market_data.get('capacity', {})
    total_rev = market_data.get('total_annual_revenue', 0)

    if revenue or capacity:
        energy_rev = revenue.get('annual_revenue', 0)
        cap_rev = capacity.get('annual_capacity_value', 0)
        elcc = capacity.get('elcc', 0)
        eff_price = revenue.get('effective_price_mwh', 0)

        sections.append(f'''
        <div class="card">
            <h2>Revenue Analysis</h2>
            <div class="grid grid-3">
                <div class="metric-card">
                    <div class="metric-value">${total_rev/1e6:.1f}M</div>
                    <div class="metric-label">Est. Annual Revenue</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">${energy_rev/1e6:.1f}M</div>
                    <div class="metric-label">Energy Revenue</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">${cap_rev/1e6:.1f}M</div>
                    <div class="metric-label">Capacity Revenue</div>
                </div>
            </div>
            <div class="grid grid-2" style="margin-top:20px">
                <div>
                    <table>
                        <tr><th colspan="2">Energy Revenue Details</th></tr>
                        <tr><td>Effective LMP</td><td>${eff_price:.2f}/MWh</td></tr>
                        <tr><td>Capacity Factor</td><td>{revenue.get('capacity_factor', 0)*100:.0f}%</td></tr>
                        <tr><td>Revenue Low</td><td>${revenue.get('revenue_low', 0)/1e6:.1f}M</td></tr>
                        <tr><td>Revenue High</td><td>${revenue.get('revenue_high', 0)/1e6:.1f}M</td></tr>
                    </table>
                </div>
                <div>
                    <table>
                        <tr><th colspan="2">Capacity Value Details</th></tr>
                        <tr><td>ELCC</td><td>{elcc*100:.0f}%</td></tr>
                        <tr><td>Accredited Capacity</td><td>{capacity.get('accredited_mw', 0):.0f} MW</td></tr>
                        <tr><td>Capacity Price</td><td>${capacity.get('price_mw_day', 0):.2f}/MW-day</td></tr>
                        <tr><td>Market</td><td>{capacity.get('market', 'N/A')}</td></tr>
                    </table>
                </div>
            </div>
        </div>
        ''')

    # Transmission Risk Section
    transmission = market_data.get('transmission', {})
    if transmission:
        risk_rating = transmission.get('risk_rating', 'Unknown')
        risk_colors = {'LOW': '#28a745', 'MODERATE': '#ffc107', 'HIGH': '#dc3545', 'VERY HIGH': '#dc3545'}
        risk_color = risk_colors.get(risk_rating, '#6c757d')

        constraints = transmission.get('relevant_constraints', [])
        upgrades = transmission.get('planned_upgrades', [])

        constraint_html = ''.join(f'<li class="red-flag">{c.get("name", "Unknown")}: {c.get("description", "")}</li>' for c in constraints[:3]) if constraints else '<li>No major constraints identified</li>'
        upgrade_html = ''.join(f'<li class="green-flag">{u.get("name", "Unknown")} (COD: {u.get("expected_cod", "TBD")})</li>' for u in upgrades[:3]) if upgrades else '<li>No planned upgrades in queue</li>'

        sections.append(f'''
        <div class="card">
            <h2>Transmission & Congestion Risk</h2>
            <div class="grid grid-2">
                <div>
                    <div class="metric-card" style="border-left: 4px solid {risk_color}">
                        <div class="metric-value" style="color:{risk_color}">{risk_rating}</div>
                        <div class="metric-label">Congestion Risk Rating</div>
                    </div>
                    <table style="margin-top:15px">
                        <tr><td>Zone</td><td>{transmission.get('zone_id', 'N/A')}</td></tr>
                        <tr><td>Congestion Level</td><td>{transmission.get('congestion_level', 'N/A')}</td></tr>
                        <tr><td>Risk Score</td><td>{transmission.get('risk_score', 0)}/100</td></tr>
                    </table>
                </div>
                <div>
                    <h3 style="margin-bottom:10px">Known Constraints</h3>
                    <ul class="flag-list">{constraint_html}</ul>
                    <h3 style="margin-top:15px;margin-bottom:10px">Planned Upgrades</h3>
                    <ul class="flag-list">{upgrade_html}</ul>
                </div>
            </div>
        </div>
        ''')

    # PPA vs Merchant Section
    ppa = market_data.get('ppa', {})
    if ppa:
        benchmark = ppa.get('ppa_benchmark', {})
        merchant = ppa.get('merchant_price', 0)
        ppa_p50 = benchmark.get('price_p50', 0) if isinstance(benchmark, dict) else 0
        premium_pct = ppa.get('ppa_premium_pct', 0)
        recommendation = ppa.get('recommendation', 'PPA provides revenue certainty; merchant offers upside potential.')

        sections.append(f'''
        <div class="card">
            <h2>PPA vs Merchant Analysis</h2>
            <div class="grid grid-3">
                <div class="metric-card">
                    <div class="metric-value">${ppa_p50:.1f}</div>
                    <div class="metric-label">PPA Benchmark ($/MWh)</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">${merchant:.1f}</div>
                    <div class="metric-label">Merchant Price ($/MWh)</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">{premium_pct:+.1f}%</div>
                    <div class="metric-label">PPA Premium</div>
                </div>
            </div>
            <div class="summary-box" style="margin-top:20px">
                <strong>Analysis:</strong> {recommendation}
            </div>
        </div>
        ''')

    # Permitting Section
    permits = market_data.get('permits', {})
    if permits:
        permit_risk = permits.get('risk_rating', 'Unknown')
        permit_colors = {'LOW': '#28a745', 'MEDIUM': '#ffc107', 'HIGH': '#dc3545'}
        permit_color = permit_colors.get(permit_risk, '#6c757d')

        timeline = permits.get('estimated_timeline_months', 0)
        success_rate = permits.get('success_rate', 0)
        required = permits.get('required_permits', [])
        issues = permits.get('known_issues', [])

        required_html = ''.join(f'<li>{p}</li>' for p in required[:5]) if required else '<li>Standard permits required</li>'
        issues_html = ''.join(f'<li class="red-flag">{i}</li>' for i in issues[:3]) if issues else '<li>No significant issues identified</li>'

        sections.append(f'''
        <div class="card">
            <h2>Permitting Assessment</h2>
            <div class="grid grid-3">
                <div class="metric-card" style="border-left: 4px solid {permit_color}">
                    <div class="metric-value" style="color:{permit_color}">{permit_risk}</div>
                    <div class="metric-label">Permit Risk Rating</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">{timeline:.0f} mo</div>
                    <div class="metric-label">Est. Timeline</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">{success_rate*100:.0f}%</div>
                    <div class="metric-label">Historical Success Rate</div>
                </div>
            </div>
            <div class="grid grid-2" style="margin-top:20px">
                <div>
                    <h3 style="margin-bottom:10px">Required Permits</h3>
                    <ul class="flag-list">{required_html}</ul>
                </div>
                <div>
                    <h3 style="margin-bottom:10px">Known Issues</h3>
                    <ul class="flag-list">{issues_html}</ul>
                </div>
            </div>
        </div>
        ''')

    return '\n'.join(sections)


def main():
    parser = argparse.ArgumentParser(description="Generate HTML Feasibility Report")
    parser.add_argument('project_id', help='Queue ID to analyze')
    parser.add_argument('--file', '-f', help='Local data file')
    parser.add_argument('--client', default='[CLIENT]', help='Client name')
    parser.add_argument('--output', '-o', default='output/report.html', help='Output file')
    parser.add_argument('--region', default='NYISO', help='Region')

    args = parser.parse_args()

    # Load data
    loader = QueueData()
    if args.file:
        df = loader.load_file(args.file)
    else:
        df = loader.load_nyiso()

    if df.empty:
        print("Error: No data loaded")
        return 1

    # Ensure output directory exists
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Generate report
    print(f"\n{'='*60}")
    print("GENERATING HTML REPORT")
    print(f"{'='*60}\n")

    html = generate_html_report(
        project_id=args.project_id,
        df=df,
        client_name=args.client,
        region=args.region,
        output_dir=str(output_path.parent)
    )

    # Save report
    with open(args.output, 'w') as f:
        f.write(html)

    print(f"\n{'='*60}")
    print(f"Report saved to: {args.output}")
    print(f"{'='*60}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
