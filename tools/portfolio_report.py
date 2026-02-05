#!/usr/bin/env python3
"""
Portfolio Report Generator for PE Firms

Generates comprehensive market-wide PDF reports with:
- Executive Summary with key metrics
- Pipeline Overview (technology mix, regional distribution)
- Risk Analytics (completion probability, expected MW)
- Competitive Landscape (developer concentration)
- Valuation Context (IC cost benchmarks)

Usage:
    python3 portfolio_report.py --output report.pdf --client "ABC Capital"
"""

import argparse
import base64
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import math

# Try to import dependencies
try:
    from weasyprint import HTML, CSS
    WEASYPRINT_AVAILABLE = True
except ImportError:
    WEASYPRINT_AVAILABLE = False
    print("Warning: WeasyPrint not installed. Run: pip install weasyprint")

try:
    import pandas as pd
    import numpy as np
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# Import our modules
from pe_analytics import PEAnalytics, analyze_portfolio, COMPLETION_RATES_BY_TYPE
from market_intel import MarketData

# Try to import chart modules
try:
    import charts_altair as charts
    CHARTS_AVAILABLE = True
except ImportError:
    CHARTS_AVAILABLE = False

# Output directory
OUTPUT_DIR = Path(__file__).parent / 'output'
OUTPUT_DIR.mkdir(exist_ok=True)

CHART_DIR = Path(__file__).parent / 'charts'
CHART_DIR.mkdir(exist_ok=True)


def generate_portfolio_report(
    df: pd.DataFrame = None,
    client_name: str = "Confidential",
    output_path: str = None,
    include_charts: bool = True
) -> str:
    """
    Generate a comprehensive portfolio report for PE firms.

    Args:
        df: DataFrame with queue data (optional, will fetch if not provided)
        client_name: Client name for report
        output_path: Output file path (optional)
        include_charts: Whether to generate and embed charts

    Returns:
        Path to generated PDF file
    """
    if not WEASYPRINT_AVAILABLE:
        raise RuntimeError("WeasyPrint is required for PDF export. Install with: pip install weasyprint")

    # Load data if not provided
    if df is None or df.empty:
        print("Loading queue data...")
        market = MarketData()
        df = market.get_latest_data()

        if df.empty:
            raise ValueError("No queue data available. Run market_intel.py to fetch data first.")

    # Run PE analytics
    print("Running PE analytics...")
    analytics = PEAnalytics(df)
    results = analyze_portfolio(df)

    # Generate charts
    chart_images = {}
    if include_charts and CHARTS_AVAILABLE:
        print("Generating charts...")
        chart_images = _generate_all_charts(results)

    # Build HTML
    print("Building report...")
    html_content = _build_portfolio_html(
        results=results,
        client_name=client_name,
        chart_images=chart_images,
        df=df
    )

    # Convert to PDF
    if output_path is None:
        output_path = OUTPUT_DIR / f"portfolio_report_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    else:
        output_path = Path(output_path)

    print(f"Generating PDF: {output_path}")
    HTML(string=html_content).write_pdf(
        str(output_path),
        stylesheets=[CSS(string=_get_portfolio_css())]
    )

    return str(output_path)


def _generate_all_charts(results: Dict) -> Dict[str, str]:
    """Generate all charts and return base64 encoded images."""
    images = {}

    try:
        # 1. Technology mix donut
        tech_data = results.get('technology_breakdown', {})
        if tech_data:
            tech_mw = {k: v.get('mw', 0) for k, v in tech_data.items()}
            charts.technology_mix_donut(tech_mw)
            images['tech_mix'] = _embed_chart('tech_mix_donut.png')

        # 2. Completion by technology
        comp_tech = results.get('completion_by_technology', {})
        if comp_tech:
            charts.completion_by_technology_bars(comp_tech)
            images['completion_by_tech'] = _embed_chart('completion_by_tech.png')

        # 3. Developer market share
        dev_data = results.get('developer_market_share', {})
        if dev_data:
            charts.developer_market_share_bars(dev_data)
            images['developer_share'] = _embed_chart('developer_share.png')

        # 4. Expected vs Nominal MW
        if tech_data:
            charts.expected_vs_nominal_mw(tech_data)
            images['expected_vs_nominal'] = _embed_chart('expected_vs_nominal.png')

        # 5. Time in queue histogram
        time_data = results.get('time_in_queue', {})
        if time_data and 'buckets' in time_data:
            charts.time_in_queue_histogram(time_data['buckets'])
            images['time_in_queue'] = _embed_chart('time_in_queue_hist.png')

        # 6. Queue vintage trend
        vintage = results.get('queue_vintage', {})
        if vintage:
            charts.queue_vintage_trend(vintage)
            images['vintage_trend'] = _embed_chart('queue_vintage_trend.png')

        # 7. Regional breakdown
        regional = results.get('regional_breakdown', {})
        if regional:
            charts.regional_breakdown_bars(regional)
            images['regional'] = _embed_chart('regional_breakdown.png')

        # 8. IC cost benchmarks
        ic_costs = results.get('ic_cost_benchmarks', {})
        if ic_costs:
            charts.ic_cost_benchmark_chart(ic_costs)
            images['ic_costs'] = _embed_chart('ic_cost_benchmarks.png')

        # 9. Pipeline funnel (if phase data available)
        phase_data = results.get('completion_by_phase', {})
        if phase_data:
            charts.pipeline_funnel(phase_data)
            images['pipeline_funnel'] = _embed_chart('pipeline_funnel.png')

    except Exception as e:
        print(f"  Warning: Could not generate some charts: {e}")

    return images


def _embed_chart(filename: str) -> str:
    """Convert chart image to base64 for embedding."""
    try:
        path = CHART_DIR / filename
        if path.exists():
            with open(path, 'rb') as f:
                data = base64.b64encode(f.read()).decode('utf-8')
            return f"data:image/png;base64,{data}"
    except Exception:
        pass
    return ""


def _build_portfolio_html(
    results: Dict,
    client_name: str,
    chart_images: Dict[str, str],
    df: pd.DataFrame
) -> str:
    """Build HTML content for portfolio report - presentation style (one element per slide)."""

    summary = results.get('summary', {})
    tech = results.get('technology_breakdown', {})
    regional = results.get('regional_breakdown', {})
    dev_conc = results.get('developer_concentration', {})
    dev_share = results.get('developer_market_share', {})
    time_data = results.get('time_in_queue', {})
    vintage = results.get('queue_vintage', {})
    ic_costs = results.get('ic_cost_benchmarks', {})

    # New PE-focused analytics
    regional_attract = results.get('regional_attractiveness', {})
    dev_tiers = results.get('developer_quality_tiers', {})
    tier_summary = results.get('developer_tier_summary', {})
    invest_recs = results.get('investment_recommendations', {})

    # Format key metrics
    total_projects = summary.get('total_projects', 0)
    total_mw = summary.get('total_mw', 0)
    total_gw = total_mw / 1000
    expected_mw = summary.get('expected_mw', 0)
    expected_gw = expected_mw / 1000
    discount_rate = summary.get('discount_rate', 0)
    hhi = dev_conc.get('hhi', 0)
    market_conc = dev_conc.get('interpretation', 'Unknown')
    top_5_share = dev_conc.get('top_5_share', 0)
    median_queue_time = time_data.get('percentiles', {}).get('p50', 0) if time_data else 0
    iso_count = summary.get('iso_count', 0)

    # Data coverage metrics
    dev_data_coverage = dev_conc.get('data_coverage', 0)
    dev_count_analyzed = dev_conc.get('developers_analyzed', 0)
    queue_date_projects = time_data.get('total_projects', 0) if time_data else 0
    queue_date_coverage = queue_date_projects / total_projects if total_projects > 0 else 0

    # Get list of ISOs in data
    isos_in_data = list(regional.keys()) if regional else []

    # Create chart HTML sections with consistent sizing
    def chart_section(img_key: str, size: str = "normal") -> str:
        container_class = {
            "small": "chart-container-small",
            "normal": "chart-container",
            "large": "chart-container-large"
        }.get(size, "chart-container")

        if img_key in chart_images and chart_images[img_key]:
            return f'<div class="{container_class}"><img src="{chart_images[img_key]}" class="chart-img" alt="{img_key}"></div>'
        return f'<div class="{container_class}"><div class="chart-placeholder">Chart not available</div></div>'

    # Technology breakdown table (top 6 only to fit)
    tech_items = sorted(tech.items(), key=lambda x: x[1].get('mw', 0), reverse=True)[:6]
    tech_rows = ""
    for t, data in tech_items:
        mw = data.get('mw', 0)
        share = data.get('share', 0) * 100
        rate = data.get('completion_rate', 0) * 100
        expected = data.get('expected_mw', 0)
        tech_rows += f"""
        <tr>
            <td>{t}</td>
            <td>{mw/1000:,.1f} GW</td>
            <td>{share:.1f}%</td>
            <td>{rate:.0f}%</td>
            <td>{expected/1000:,.1f} GW</td>
        </tr>
        """

    # Regional breakdown table
    regional_items = sorted(regional.items(), key=lambda x: x[1].get('mw', 0), reverse=True)
    regional_rows = ""
    for r, data in regional_items:
        mw = data.get('mw', 0)
        count = data.get('project_count', 0)
        avg_size = data.get('avg_project_size', 0)
        regional_rows += f"""
        <tr>
            <td>{r}</td>
            <td>{mw/1000:,.1f} GW</td>
            <td>{count:,}</td>
            <td>{avg_size:.0f} MW</td>
        </tr>
        """

    # Top developers table (top 8 to fit)
    dev_rows = ""
    dev_items = list(dev_share.items())[:8]
    for i, (dev, data) in enumerate(dev_items, 1):
        mw = data.get('mw', 0)
        share = data.get('market_share', 0) * 100
        count = data.get('project_count', 0)
        display_name = dev[:30] + '...' if len(dev) > 30 else dev
        dev_rows += f"""
        <tr>
            <td>{i}</td>
            <td>{display_name}</td>
            <td>{mw/1000:,.1f} GW</td>
            <td>{share:.1f}%</td>
            <td>{count:,}</td>
        </tr>
        """

    # IC cost benchmark table
    ic_rows = ""
    for region, costs in sorted(ic_costs.items(), key=lambda x: x[1].get('p50', 0)):
        p25 = costs.get('p25', 0)
        p50 = costs.get('p50', 0)
        p75 = costs.get('p75', 0)
        ic_rows += f"""
        <tr>
            <td>{region}</td>
            <td>${p25:,.0f}</td>
            <td>${p50:,.0f}</td>
            <td>${p75:,.0f}</td>
        </tr>
        """

    # Benchmark completion rates (compact - just key techs)
    key_techs = ['Storage', 'Solar', 'Wind', 'Solar+Storage', 'Natural Gas']
    benchmark_rows = ""
    for tech_type in key_techs:
        rate = COMPLETION_RATES_BY_TYPE.get(tech_type, 0)
        if rate > 0:
            benchmark_rows += f"""
            <tr>
                <td>{tech_type}</td>
                <td>{rate*100:.0f}%</td>
            </tr>
            """

    # Regional attractiveness table
    regional_attract_rows = ""
    if regional_attract:
        sorted_regions = sorted(regional_attract.items(),
                               key=lambda x: x[1].get('composite_score', 0), reverse=True)
        for iso, data in sorted_regions:
            score = data.get('composite_score', 0)
            grade = data.get('grade', 'N/A')
            rank = data.get('rank', 0)
            completion = data.get('completion_rate', 0)
            timeline = data.get('timeline_months', 0)
            ic_cost = data.get('ic_cost_per_kw', 0)

            # Color coding for grade
            grade_color = {'A': '#10b981', 'B': '#3b82f6', 'C': '#f59e0b', 'D': '#ef4444'}.get(grade, '#6b7280')

            regional_attract_rows += f"""
            <tr>
                <td>#{rank}</td>
                <td>{iso}</td>
                <td style="color: {grade_color}; font-weight: 700;">{grade}</td>
                <td>{score:.0f}</td>
                <td>{completion:.0%}</td>
                <td>{timeline:.0f} mo</td>
                <td>${ic_cost}/kW</td>
            </tr>
            """

    # Developer quality tiers table
    dev_tier_rows = ""
    if dev_tiers:
        for i, (dev, data) in enumerate(list(dev_tiers.items())[:10], 1):
            tier = data.get('tier', 'C')
            mw = data.get('mw', 0)
            count = data.get('project_count', 0)
            avg_size = data.get('avg_project_size', 0)
            tech_div = data.get('tech_diversity', 0)
            display_name = dev[:28] + '...' if len(str(dev)) > 28 else dev

            tier_color = {'A': '#10b981', 'B': '#3b82f6', 'C': '#f59e0b'}.get(tier, '#6b7280')

            dev_tier_rows += f"""
            <tr>
                <td style="color: {tier_color}; font-weight: 700;">{tier}</td>
                <td>{display_name}</td>
                <td>{mw/1000:,.1f} GW</td>
                <td>{count}</td>
                <td>{avg_size:.0f} MW</td>
                <td>{tech_div}</td>
            </tr>
            """

    # Developer tier summary
    tier_a_pct = tier_summary.get('A', {}).get('pct_of_pipeline', 0) if tier_summary else 0
    tier_b_pct = tier_summary.get('B', {}).get('pct_of_pipeline', 0) if tier_summary else 0
    tier_c_pct = tier_summary.get('C', {}).get('pct_of_pipeline', 0) if tier_summary else 0
    tier_a_count = tier_summary.get('A', {}).get('count', 0) if tier_summary else 0
    tier_b_count = tier_summary.get('B', {}).get('count', 0) if tier_summary else 0
    tier_c_count = tier_summary.get('C', {}).get('count', 0) if tier_summary else 0

    # Investment recommendations
    top_regions_list = invest_recs.get('top_regions', []) if invest_recs else []
    avoid_regions_list = invest_recs.get('avoid_regions', []) if invest_recs else []
    target_techs = invest_recs.get('target_technologies', []) if invest_recs else []
    key_risks = invest_recs.get('key_risks', []) if invest_recs else []
    market_timing = invest_recs.get('market_timing', {}) if invest_recs else {}
    dev_strategy = invest_recs.get('developer_strategy', {}) if invest_recs else {}

    html = f'''
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Interconnection Queue Portfolio Analysis</title>
    </head>
    <body>
        <!-- SLIDE 1: Cover Page -->
        <div class="cover-page">
            <div class="cover-content">
                <h1>Interconnection Queue<br>Portfolio Analysis</h1>
                <div class="cover-subtitle">US Market Intelligence Report</div>
                <div class="cover-meta">
                    <div class="cover-stat">
                        <span class="cover-value">{total_gw:,.0f} GW</span>
                        <span class="cover-label">Total Pipeline</span>
                    </div>
                    <div class="cover-stat">
                        <span class="cover-value">{total_projects:,}</span>
                        <span class="cover-label">Active Projects</span>
                    </div>
                    <div class="cover-stat">
                        <span class="cover-value">{iso_count}</span>
                        <span class="cover-label">ISO/RTOs</span>
                    </div>
                </div>
                <div class="cover-footer">
                    <div>Prepared for: {client_name}</div>
                    <div>{datetime.now().strftime('%B %d, %Y')}</div>
                </div>
            </div>
        </div>

        <!-- SLIDE 2: Executive Summary -->
        <div class="slide">
            <h1 class="slide-title">Executive Summary</h1>

            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value">{total_gw:,.0f} GW</div>
                    <div class="kpi-label">Total Pipeline</div>
                    <div class="kpi-detail">{total_projects:,} projects</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{expected_gw:,.0f} GW</div>
                    <div class="kpi-label">Risk-Adjusted MW</div>
                    <div class="kpi-detail">{discount_rate:.0%} discount</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{median_queue_time:.0f} mo</div>
                    <div class="kpi-label">Median Queue Time</div>
                    <div class="kpi-detail">P50 duration</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{hhi:,.0f}</div>
                    <div class="kpi-label">Developer HHI</div>
                    <div class="kpi-detail">{market_conc}</div>
                </div>
            </div>

            <div class="insights-box">
                <h3>Key Insights</h3>
                <ul>
                    <li><strong>Massive Pipeline:</strong> {total_gw:,.0f} GW across {total_projects:,} projects represents significant development activity.</li>
                    <li><strong>High Attrition Expected:</strong> Only {expected_gw:,.0f} GW ({100-discount_rate*100:.0f}%) expected to reach COD based on historical completion rates.</li>
                    <li><strong>Market Structure:</strong> {market_conc} market with top 5 developers controlling {top_5_share:.0%} of capacity.</li>
                    <li><strong>Timeline Risk:</strong> Median queue time of {median_queue_time:.0f} months reflects study backlogs and grid constraints.</li>
                </ul>
            </div>
        </div>

        <!-- SLIDE 3: Technology Mix -->
        <div class="slide">
            <h1 class="slide-title">Technology Mix</h1>
            <p class="slide-subtitle">Pipeline capacity by generation type</p>

            {chart_section('tech_mix', 'large')}

            <table class="data-table compact">
                <thead>
                    <tr>
                        <th>Technology</th>
                        <th>Pipeline</th>
                        <th>Share</th>
                        <th>Completion Rate</th>
                        <th>Expected</th>
                    </tr>
                </thead>
                <tbody>
                    {tech_rows}
                </tbody>
            </table>
        </div>

        <!-- SLIDE 4: Regional Distribution -->
        <div class="slide">
            <h1 class="slide-title">Regional Distribution</h1>
            <p class="slide-subtitle">Pipeline capacity by ISO/RTO</p>

            {chart_section('regional', 'large')}

            <table class="data-table compact">
                <thead>
                    <tr>
                        <th>Region</th>
                        <th>Pipeline</th>
                        <th>Projects</th>
                        <th>Avg Size</th>
                    </tr>
                </thead>
                <tbody>
                    {regional_rows}
                </tbody>
            </table>
        </div>

        <!-- SLIDE 5: Risk Analytics -->
        <div class="slide">
            <h1 class="slide-title">Risk Analytics</h1>
            <p class="slide-subtitle">Completion probability and expected MW</p>

            <div class="risk-callout">
                <div class="callout-icon">!</div>
                <div class="callout-content">
                    <strong>Key Risk Metric:</strong> Only {100-discount_rate*100:.0f}% of nominal capacity expected to reach COD.
                </div>
            </div>

            <div class="chart-row">
                {chart_section('completion_by_tech', 'small')}
                {chart_section('expected_vs_nominal', 'small')}
            </div>

            <table class="data-table compact" style="width: 60%; margin: 0 auto;">
                <thead>
                    <tr>
                        <th>Technology</th>
                        <th>Historical Completion Rate</th>
                    </tr>
                </thead>
                <tbody>
                    {benchmark_rows}
                </tbody>
            </table>
            <div class="source-note" style="text-align: center;">Source: LBL "Queued Up" 2024</div>
        </div>

        <!-- SLIDE 6: Time in Queue -->
        <div class="slide">
            <h1 class="slide-title">Time in Queue Analysis</h1>
            <p class="slide-subtitle">Project duration from queue entry to present ({queue_date_projects:,} projects with date data)</p>

            {chart_section('time_in_queue', 'large')}

            <div class="metric-box">
                <h3>Queue Duration Metrics</h3>
                <div class="metric-row">
                    <div class="metric-item">
                        <span class="metric-value">{time_data.get('percentiles', {}).get('p25', 0):.0f}</span>
                        <span class="metric-label">P25 (months)</span>
                    </div>
                    <div class="metric-item">
                        <span class="metric-value">{median_queue_time:.0f}</span>
                        <span class="metric-label">Median (months)</span>
                    </div>
                    <div class="metric-item">
                        <span class="metric-value">{time_data.get('percentiles', {}).get('p75', 0):.0f}</span>
                        <span class="metric-label">P75 (months)</span>
                    </div>
                    <div class="metric-item">
                        <span class="metric-value">{time_data.get('percentiles', {}).get('p90', 0):.0f}</span>
                        <span class="metric-label">P90 (months)</span>
                    </div>
                </div>
                <div class="metric-note">Based on {queue_date_coverage:.0%} of projects with queue entry date available</div>
            </div>
        </div>

        <!-- SLIDE 7: Competitive Landscape -->
        <div class="slide">
            <h1 class="slide-title">Competitive Landscape</h1>
            <p class="slide-subtitle">Developer concentration and market share (projects with developer data only)</p>

            <div class="metric-box">
                <h3>Market Concentration</h3>
                <div class="metric-row">
                    <div class="metric-item">
                        <span class="metric-value">{hhi:,.0f}</span>
                        <span class="metric-label">HHI Index</span>
                    </div>
                    <div class="metric-item">
                        <span class="metric-value">{top_5_share:.0%}</span>
                        <span class="metric-label">Top 5 Share</span>
                    </div>
                    <div class="metric-item">
                        <span class="metric-value">{market_conc}</span>
                        <span class="metric-label">Market Structure</span>
                    </div>
                    <div class="metric-item">
                        <span class="metric-value">{dev_data_coverage:.0%}</span>
                        <span class="metric-label">Data Coverage</span>
                    </div>
                </div>
                <div class="metric-note">HHI: &lt;1500 = Competitive | 1500-2500 = Moderate | &gt;2500 = Concentrated | Based on {dev_count_analyzed:,} developers with reported data</div>
            </div>

            {chart_section('developer_share', 'normal')}
        </div>

        <!-- SLIDE 8: Top Developers -->
        <div class="slide">
            <h1 class="slide-title">Top Developers</h1>
            <p class="slide-subtitle">Leading developers by pipeline capacity (among projects with developer data)</p>

            <table class="data-table" style="margin-top: 30px;">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Developer</th>
                        <th>Pipeline</th>
                        <th>Share</th>
                        <th>Projects</th>
                    </tr>
                </thead>
                <tbody>
                    {dev_rows}
                </tbody>
            </table>

            <div class="source-note" style="margin-top: 20px;">
                Note: Developer names may not be standardized across ISOs. Some ISOs do not report developer information.
            </div>
        </div>

        <!-- SLIDE 9: IC Cost Benchmarks -->
        <div class="slide">
            <h1 class="slide-title">Interconnection Cost Benchmarks</h1>
            <p class="slide-subtitle">Regional cost ranges ($/kW)</p>

            {chart_section('ic_costs', 'large')}

            <table class="data-table compact" style="width: 70%; margin: 0 auto;">
                <thead>
                    <tr>
                        <th>Region</th>
                        <th>P25 (Low)</th>
                        <th>P50 (Median)</th>
                        <th>P75 (High)</th>
                    </tr>
                </thead>
                <tbody>
                    {ic_rows}
                </tbody>
            </table>
            <div class="source-note" style="text-align: center;">Source: LBL "Queued Up" 2024</div>
        </div>

        <!-- SLIDE 10: Queue Growth Trend -->
        <div class="slide">
            <h1 class="slide-title">Queue Growth Trend</h1>
            <p class="slide-subtitle">Historical queue entry volume by year</p>

            {chart_section('vintage_trend', 'large')}

            <div class="notes-box">
                <h3>Valuation Considerations</h3>
                <ul>
                    <li><strong>Development Premium:</strong> Pre-IA projects: $5-15/kW | Post-IA: $20-50/kW</li>
                    <li><strong>IC Cost Risk:</strong> 5-20% of total project cost with wide regional variation</li>
                    <li><strong>Timeline Risk:</strong> Factor {median_queue_time:.0f} month median queue time into DCF models</li>
                </ul>
            </div>
        </div>

        <!-- SLIDE 11: Regional Attractiveness Rankings -->
        <div class="slide">
            <h1 class="slide-title">Regional Attractiveness Rankings</h1>
            <p class="slide-subtitle">Composite scoring based on completion rate, timeline, and IC cost</p>

            <table class="data-table" style="margin-top: 20px;">
                <thead>
                    <tr>
                        <th>Rank</th>
                        <th>Region</th>
                        <th>Grade</th>
                        <th>Score</th>
                        <th>Completion</th>
                        <th>Timeline</th>
                        <th>IC Cost</th>
                    </tr>
                </thead>
                <tbody>
                    {regional_attract_rows}
                </tbody>
            </table>

            <div class="metric-box" style="margin-top: 20px;">
                <h3>Scoring Methodology</h3>
                <div style="display: flex; gap: 20px; font-size: 9px;">
                    <div><strong>Completion Rate:</strong> 30% weight - Historical success rate from LBL data</div>
                    <div><strong>Timeline:</strong> 25% weight - Median months from queue entry to COD</div>
                    <div><strong>IC Cost:</strong> 25% weight - Median interconnection cost per kW</div>
                    <div><strong>Queue Health:</strong> 20% weight - Average project size as quality indicator</div>
                </div>
                <div class="metric-note" style="margin-top: 10px;">
                    Grade A: Score ≥70 | Grade B: 55-69 | Grade C: 40-54 | Grade D: &lt;40
                </div>
            </div>
        </div>

        <!-- SLIDE 12: Developer Quality Tiers -->
        <div class="slide">
            <h1 class="slide-title">Developer Quality Tiers</h1>
            <p class="slide-subtitle">Developer classification based on portfolio size, diversity, and track record</p>

            <div style="display: flex; gap: 15px; margin-bottom: 15px;">
                <div class="kpi-card" style="background: linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%); border-color: #10b981;">
                    <div class="kpi-value" style="color: #047857;">Tier A</div>
                    <div class="kpi-label">{tier_a_count} developers</div>
                    <div class="kpi-detail">{tier_a_pct:.0%} of pipeline</div>
                </div>
                <div class="kpi-card" style="background: linear-gradient(135deg, #dbeafe 0%, #bfdbfe 100%); border-color: #3b82f6;">
                    <div class="kpi-value" style="color: #1d4ed8;">Tier B</div>
                    <div class="kpi-label">{tier_b_count} developers</div>
                    <div class="kpi-detail">{tier_b_pct:.0%} of pipeline</div>
                </div>
                <div class="kpi-card" style="background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%); border-color: #f59e0b;">
                    <div class="kpi-value" style="color: #b45309;">Tier C</div>
                    <div class="kpi-label">{tier_c_count} developers</div>
                    <div class="kpi-detail">{tier_c_pct:.0%} of pipeline</div>
                </div>
            </div>

            <table class="data-table compact">
                <thead>
                    <tr>
                        <th>Tier</th>
                        <th>Developer</th>
                        <th>Pipeline</th>
                        <th>Projects</th>
                        <th>Avg Size</th>
                        <th>Tech Diversity</th>
                    </tr>
                </thead>
                <tbody>
                    {dev_tier_rows}
                </tbody>
            </table>

            <div class="source-note" style="margin-top: 10px;">
                Tier A: Large, diversified portfolios (&gt;5 GW, multiple technologies/regions) |
                Tier B: Medium portfolios, focused strategy |
                Tier C: Small/single-project developers
            </div>
        </div>

        <!-- SLIDE 13: Investment Recommendations -->
        <div class="slide">
            <h1 class="slide-title">Investment Recommendations</h1>
            <p class="slide-subtitle">Actionable insights for portfolio strategy</p>

            <div style="display: flex; gap: 20px;">
                <div style="flex: 1;">
                    <div class="notes-box" style="background: #f0fdf4; border-color: #86efac;">
                        <h3 style="color: #166534; margin: 0 0 10px 0;">Target Regions</h3>
                        <ul style="margin: 0; padding-left: 18px; font-size: 9px;">
                            {"".join([f'<li><strong>{r["region"]}</strong> (Grade {r["grade"]}): {r["rationale"]}</li>' for r in top_regions_list]) if top_regions_list else '<li>No data available</li>'}
                        </ul>
                    </div>

                    <div class="notes-box" style="background: #fef2f2; border-color: #fecaca; margin-top: 12px;">
                        <h3 style="color: #991b1b; margin: 0 0 10px 0;">Regions to Avoid</h3>
                        <ul style="margin: 0; padding-left: 18px; font-size: 9px;">
                            {"".join([f'<li><strong>{r["region"]}</strong> (Grade {r["grade"]}): {r["rationale"]}</li>' for r in avoid_regions_list]) if avoid_regions_list else '<li>No significant concerns identified</li>'}
                        </ul>
                    </div>
                </div>

                <div style="flex: 1;">
                    <div class="notes-box" style="background: #eff6ff; border-color: #bfdbfe;">
                        <h3 style="color: #1e40af; margin: 0 0 10px 0;">Target Technologies</h3>
                        <ul style="margin: 0; padding-left: 18px; font-size: 9px;">
                            {"".join([f'<li><strong>{t["technology"]}</strong>: {t["completion_rate"]:.0%} completion rate, {t["pipeline_mw"]/1000:,.0f} GW pipeline</li>' for t in target_techs]) if target_techs else '<li>No data available</li>'}
                        </ul>
                    </div>

                    <div class="metric-box" style="margin-top: 12px;">
                        <h3 style="margin: 0 0 10px 0;">Developer Strategy</h3>
                        <p style="font-size: 9px; margin: 0;">
                            <strong>Recommendation:</strong> {dev_strategy.get('recommendation', 'Partner with Tier A developers or acquire from Tier B/C')}<br>
                            Tier A developers control {tier_a_pct:.0%} of pipeline capacity with proven execution track records.
                        </p>
                    </div>
                </div>
            </div>

            <div class="risk-callout" style="margin-top: 15px;">
                <div class="callout-icon">!</div>
                <div class="callout-content">
                    <strong>Key Risks:</strong>
                    {"".join([f' • {r["risk"]} ({r["severity"]}): {r["mitigation"]}' for r in key_risks[:2]]) if key_risks else ' No specific risks identified'}
                </div>
            </div>

            <div class="source-note" style="margin-top: 10px;">
                <strong>Market Timing:</strong> {market_timing.get('signal', 'FERC Order 2023 implementation may accelerate timelines')}
            </div>
        </div>

        <!-- SLIDE 14: Methodology -->
        <div class="slide">
            <h1 class="slide-title">Methodology & Data Sources</h1>

            <div style="display: flex; gap: 25px;">
                <div style="flex: 1;">
                    <h3 style="margin-top: 0;">Data Sources</h3>
                    <ul class="source-list">
                        <li><strong>Queue Data:</strong> ISO/RTO public interconnection queues</li>
                        <li><strong>Completion Rates:</strong> LBL "Queued Up" 2024</li>
                        <li><strong>Cost Benchmarks:</strong> LBL historical analysis</li>
                        <li><strong>Refresh:</strong> Weekly queue updates</li>
                    </ul>

                    <h3>Key Metrics</h3>
                    <ul class="source-list">
                        <li><strong>Expected MW:</strong> Nominal × Completion probability</li>
                        <li><strong>HHI:</strong> Sum of squared market shares</li>
                        <li><strong>Time in Queue:</strong> Months from entry to present</li>
                    </ul>
                </div>
                <div style="flex: 1;">
                    <h3 style="margin-top: 0;">Data Coverage</h3>
                    <ul class="source-list">
                        <li><strong>ISOs in this report:</strong> {', '.join(isos_in_data)}</li>
                        <li><strong>Developer data:</strong> {dev_data_coverage:.0%} of pipeline has developer info</li>
                        <li><strong>Queue date:</strong> {queue_date_coverage:.0%} of projects have entry date</li>
                    </ul>

                    <h3>Limitations</h3>
                    <ul class="source-list">
                        <li>Developer names not standardized across ISOs</li>
                        <li>Completion rates are historical averages</li>
                        <li>Cost benchmarks based on LBL regional ranges</li>
                        <li>Some ISOs do not report developer or queue date</li>
                    </ul>
                </div>
            </div>

            <div style="position: absolute; bottom: 30px; left: 0; right: 0; text-align: center; font-size: 8px; color: #6b7280; border-top: 1px solid #e5e7eb; padding-top: 12px;">
                <strong>Disclaimer:</strong> This report is for informational purposes only and does not constitute investment advice.<br>
                Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Data: {datetime.now().strftime('%B %Y')} | {total_projects:,} projects | {total_gw:,.0f} GW
            </div>
        </div>
    </body>
    </html>
    '''

    return html


def _get_portfolio_css() -> str:
    """Get CSS styles for portfolio report - presentation style (one element per slide)."""
    return '''
    @page {
        size: letter;
        margin: 0.5in 0.6in;
        @bottom-center {
            content: counter(page);
            font-size: 9px;
            color: #666;
        }
    }

    body {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        font-size: 10px;
        line-height: 1.4;
        color: #1f2937;
    }

    /* Cover Page */
    .cover-page {
        height: 9.5in;
        display: flex;
        align-items: center;
        justify-content: center;
        background: linear-gradient(135deg, #1e3a5f 0%, #2c5282 50%, #1e3a5f 100%);
        margin: -0.5in -0.6in;
        padding: 0.6in;
        page-break-after: always;
    }

    .cover-content {
        text-align: center;
        color: white;
    }

    .cover-content h1 {
        font-size: 36px;
        font-weight: 700;
        margin: 0 0 15px 0;
        line-height: 1.2;
    }

    .cover-subtitle {
        font-size: 18px;
        opacity: 0.9;
        margin-bottom: 50px;
    }

    .cover-meta {
        display: flex;
        justify-content: center;
        gap: 40px;
        margin-bottom: 60px;
    }

    .cover-stat {
        text-align: center;
    }

    .cover-value {
        display: block;
        font-size: 32px;
        font-weight: 700;
    }

    .cover-label {
        display: block;
        font-size: 11px;
        opacity: 0.8;
        margin-top: 5px;
    }

    .cover-footer {
        font-size: 12px;
        opacity: 0.9;
    }

    /* Slide/Section - Presentation Style */
    .slide {
        page-break-before: always;
        page-break-after: always;
        page-break-inside: avoid;
        min-height: 8.5in;
        max-height: 9in;
        overflow: hidden;
        position: relative;
    }

    .slide:first-of-type {
        page-break-before: auto;
    }

    .slide-title {
        font-size: 20px;
        color: #1e3a5f;
        border-bottom: 3px solid #3182ce;
        padding-bottom: 10px;
        margin: 0 0 20px 0;
    }

    .slide-subtitle {
        font-size: 14px;
        color: #475569;
        margin: 0 0 15px 0;
    }

    /* Chart Container - Fixed Size for Consistency */
    .chart-container {
        width: 100%;
        height: 320px;
        display: flex;
        align-items: center;
        justify-content: center;
        margin: 15px 0;
    }

    .chart-container-large {
        width: 100%;
        height: 400px;
        display: flex;
        align-items: center;
        justify-content: center;
        margin: 15px 0;
    }

    .chart-container-small {
        width: 48%;
        height: 260px;
        display: flex;
        align-items: center;
        justify-content: center;
    }

    .chart-img {
        max-width: 100%;
        max-height: 100%;
        width: auto;
        height: auto;
        object-fit: contain;
        display: block;
        margin: 0 auto;
    }

    .chart-placeholder {
        background: #f1f5f9;
        border: 2px dashed #cbd5e1;
        border-radius: 8px;
        padding: 40px;
        text-align: center;
        color: #94a3b8;
        width: 100%;
        height: 100%;
        display: flex;
        align-items: center;
        justify-content: center;
    }

    /* Side by Side Charts */
    .chart-row {
        display: flex;
        gap: 20px;
        justify-content: space-between;
        margin: 15px 0;
    }

    /* KPI Grid - Executive Summary */
    .kpi-grid {
        display: flex;
        gap: 12px;
        margin-bottom: 20px;
    }

    .kpi-card {
        flex: 1;
        background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
        border-radius: 8px;
        padding: 18px 12px;
        text-align: center;
        border: 1px solid #e2e8f0;
    }

    .kpi-value {
        font-size: 28px;
        font-weight: 700;
        color: #1e3a5f;
    }

    .kpi-label {
        font-size: 10px;
        color: #64748b;
        margin-top: 6px;
    }

    .kpi-detail {
        font-size: 9px;
        color: #94a3b8;
        margin-top: 3px;
    }

    /* Key Insights Box */
    .insights-box {
        background: #f8fafc;
        border-left: 4px solid #3182ce;
        padding: 18px 22px;
        border-radius: 0 8px 8px 0;
        margin-top: 15px;
    }

    .insights-box h3 {
        margin: 0 0 12px 0;
        color: #1e3a5f;
        font-size: 13px;
    }

    .insights-box ul {
        margin: 0;
        padding-left: 18px;
    }

    .insights-box li {
        margin-bottom: 10px;
        font-size: 10px;
        line-height: 1.5;
    }

    /* Tables - Compact */
    table {
        width: 100%;
        border-collapse: collapse;
        font-size: 9px;
    }

    .data-table th, .data-table td {
        padding: 6px 8px;
        text-align: left;
        border-bottom: 1px solid #e5e7eb;
    }

    .data-table th {
        background: #f1f5f9;
        font-weight: 600;
        color: #475569;
    }

    .data-table tbody tr:nth-child(even) {
        background: #f8fafc;
    }

    .data-table.compact th, .data-table.compact td {
        padding: 4px 6px;
    }

    /* Risk Callout */
    .risk-callout {
        display: flex;
        align-items: center;
        background: #fef3c7;
        border: 1px solid #f59e0b;
        border-radius: 8px;
        padding: 12px 15px;
        margin-bottom: 15px;
    }

    .callout-icon {
        width: 28px;
        height: 28px;
        background: #f59e0b;
        border-radius: 50%;
        color: white;
        font-weight: bold;
        font-size: 16px;
        display: flex;
        align-items: center;
        justify-content: center;
        margin-right: 12px;
        flex-shrink: 0;
    }

    .callout-content {
        flex: 1;
        font-size: 10px;
    }

    /* Metric Box */
    .metric-box {
        background: #f8fafc;
        border-radius: 8px;
        padding: 18px;
        margin-bottom: 15px;
    }

    .metric-box h3 {
        margin: 0 0 12px 0;
        font-size: 13px;
    }

    .metric-row {
        display: flex;
        gap: 25px;
    }

    .metric-item {
        text-align: center;
    }

    .metric-value {
        display: block;
        font-size: 22px;
        font-weight: 700;
        color: #1e3a5f;
    }

    .metric-label {
        display: block;
        font-size: 9px;
        color: #64748b;
        margin-top: 3px;
    }

    .metric-note {
        font-size: 8px;
        color: #94a3b8;
        margin-top: 10px;
    }

    /* Valuation Notes */
    .notes-box {
        background: #f0fdf4;
        border: 1px solid #86efac;
        border-radius: 8px;
        padding: 15px 18px;
    }

    .notes-box h3 {
        margin: 0 0 10px 0;
        color: #166534;
        font-size: 12px;
    }

    .notes-box ul {
        margin: 0;
        padding-left: 18px;
    }

    .notes-box li {
        margin-bottom: 8px;
        font-size: 9px;
        line-height: 1.5;
    }

    /* Source Notes */
    .source-note {
        font-size: 8px;
        color: #94a3b8;
        font-style: italic;
        margin-top: 8px;
    }

    .source-list {
        padding-left: 18px;
    }

    .source-list li {
        margin-bottom: 6px;
        font-size: 9px;
    }

    /* Footer - Fixed at bottom */
    .slide-footer {
        position: absolute;
        bottom: 0;
        left: 0;
        right: 0;
        padding-top: 10px;
        border-top: 1px solid #e5e7eb;
        font-size: 8px;
        color: #6b7280;
        text-align: center;
    }

    /* Prevent orphans/widows */
    p, li {
        orphans: 3;
        widows: 3;
    }

    /* No break inside elements */
    .kpi-card, .metric-box, .insights-box, .risk-callout, .notes-box, .data-table {
        page-break-inside: avoid;
    }
    '''


# CLI interface
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate PE Portfolio Report")
    parser.add_argument('--client', '-c', default='Confidential', help='Client name')
    parser.add_argument('--output', '-o', help='Output PDF path')
    parser.add_argument('--no-charts', action='store_true', help='Skip chart generation')

    args = parser.parse_args()

    try:
        pdf_path = generate_portfolio_report(
            client_name=args.client,
            output_path=args.output,
            include_charts=not args.no_charts
        )
        print(f"\nReport generated: {pdf_path}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
