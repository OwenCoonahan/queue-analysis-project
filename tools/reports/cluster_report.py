#!/usr/bin/env python3
"""
Cluster Report Generator - Portfolio/Group Analysis

Generates PDF reports for groups of interconnection projects.

Supports filtering by:
- List of project IDs
- POI/Substation
- Developer
- ISO/Region
- Technology type

Usage:
    from reports import generate_cluster_report

    # By project IDs
    pdf_path = generate_cluster_report(
        project_ids=["J1234", "J1235", "J1236"],
        cluster_name="XYZ Substation Portfolio"
    )

    # By POI
    pdf_path = generate_cluster_report(
        filter_by="poi",
        filter_value="Athens 345kV",
        cluster_name="Athens Substation Projects"
    )

    # By developer
    pdf_path = generate_cluster_report(
        filter_by="developer",
        filter_value="NextEra",
        cluster_name="NextEra Portfolio Analysis"
    )

CLI:
    python -m reports.cluster_report --ids J1234,J1235,J1236 --name "Portfolio"
    python -m reports.cluster_report --developer "NextEra" --name "NextEra Portfolio"
    python -m reports.cluster_report --poi "Athens 345kV" --name "Athens POI"
"""

import argparse
import base64
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List

# PDF generation
try:
    from weasyprint import HTML, CSS
    WEASYPRINT_AVAILABLE = True
except ImportError:
    WEASYPRINT_AVAILABLE = False

# Data modules
try:
    import pandas as pd
    import numpy as np
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# Import from parent tools directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from .styles import get_cluster_report_css, RECOMMENDATION_COLORS

# Output directory
OUTPUT_DIR = Path(__file__).parent.parent / 'output' / 'reports' / 'clusters'


def generate_cluster_report(
    project_ids: List[str] = None,
    filter_by: str = None,
    filter_value: str = None,
    df: pd.DataFrame = None,
    cluster_name: str = "Project Cluster",
    client_name: str = "Confidential",
    output_path: str = None,
    include_charts: bool = True,
) -> str:
    """
    Generate a PDF report for a cluster of projects.

    Args:
        project_ids: List of queue IDs (if filtering by specific projects)
        filter_by: Filter type ('poi', 'developer', 'iso', 'type', 'state')
        filter_value: Value to filter by
        df: DataFrame with queue data (optional, will load if not provided)
        cluster_name: Name for this cluster/portfolio
        client_name: Client name for report header
        output_path: Output file path (optional)
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

    # Filter projects
    cluster_df = _filter_projects(df, project_ids, filter_by, filter_value)

    if cluster_df.empty:
        raise ValueError(f"No projects found matching filter criteria.")

    print(f"Found {len(cluster_df)} projects in cluster")

    # Import analysis modules
    from scoring import FeasibilityScorer

    # Score all projects
    print(f"[1/3] Scoring {len(cluster_df)} projects...")
    scorer = FeasibilityScorer(df)  # Use full df for context
    scored_projects = []

    for idx, row in cluster_df.iterrows():
        project_id = _get_project_id(row)
        if project_id:
            try:
                score_result = scorer.score_project(project_id=project_id)
                if 'error' not in score_result:
                    scored_projects.append({
                        'id': project_id,
                        'score_result': score_result,
                        'row': row.to_dict(),
                    })
            except Exception:
                pass

    if not scored_projects:
        raise ValueError("Could not score any projects in cluster.")

    print(f"  Scored {len(scored_projects)} projects successfully")

    # Compute cluster analytics
    print(f"[2/3] Computing cluster analytics...")
    analytics = _compute_cluster_analytics(scored_projects, cluster_df)

    # Generate charts
    chart_images = {}
    if include_charts:
        chart_images = _generate_cluster_charts(analytics, cluster_name)

    # Build HTML
    print(f"[3/3] Building report...")
    html_content = _build_cluster_html(
        cluster_name=cluster_name,
        client_name=client_name,
        scored_projects=scored_projects,
        analytics=analytics,
        chart_images=chart_images,
        filter_info={'by': filter_by, 'value': filter_value} if filter_by else None,
    )

    # Generate PDF
    if output_path is None:
        safe_name = cluster_name.replace(' ', '_').replace('/', '_')[:30]
        output_path = OUTPUT_DIR / f"cluster_report_{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    else:
        output_path = Path(output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    HTML(string=html_content).write_pdf(
        str(output_path),
        stylesheets=[CSS(string=get_cluster_report_css())]
    )

    print(f"Report generated: {output_path}")
    return str(output_path)


def _load_queue_data() -> pd.DataFrame:
    """Load queue data from available sources."""
    try:
        from market_intel import MarketData
        market = MarketData()
        return market.get_latest_data()
    except Exception:
        pass

    try:
        from unified_data import UnifiedQueue
        uq = UnifiedQueue()
        return uq.load_unified()
    except Exception:
        pass

    return pd.DataFrame()


def _get_project_id(row) -> Optional[str]:
    """Extract project ID from row."""
    for col in ['queue_id', 'Queue_ID', 'project_id', 'id', 'ID', 'queue_number']:
        if col in row.index and pd.notna(row[col]):
            return str(row[col])
    return None


def _filter_projects(
    df: pd.DataFrame,
    project_ids: List[str] = None,
    filter_by: str = None,
    filter_value: str = None
) -> pd.DataFrame:
    """Filter DataFrame to get cluster projects."""

    if project_ids:
        # Filter by specific IDs
        id_col = None
        for col in ['queue_id', 'Queue_ID', 'project_id', 'id', 'ID', 'queue_number']:
            if col in df.columns:
                id_col = col
                break

        if id_col:
            return df[df[id_col].astype(str).isin([str(i) for i in project_ids])]
        return pd.DataFrame()

    if filter_by and filter_value:
        filter_value_lower = filter_value.lower().strip()

        # Map filter_by to column names
        col_mappings = {
            'poi': ['poi', 'POI', 'point_of_interconnection', 'substation'],
            'developer': ['developer', 'Developer', 'applicant', 'Applicant', 'owner'],
            'iso': ['iso', 'ISO', 'region', 'Region', 'rto'],
            'type': ['type', 'Type', 'fuel_type', 'technology', 'gen_type'],
            'state': ['state', 'State', 'st'],
        }

        cols_to_check = col_mappings.get(filter_by.lower(), [filter_by])

        for col in cols_to_check:
            if col in df.columns:
                mask = df[col].fillna('').astype(str).str.lower().str.contains(
                    filter_value_lower, regex=False
                )
                filtered = df[mask]
                if not filtered.empty:
                    return filtered

    return pd.DataFrame()


def _compute_cluster_analytics(scored_projects: List[Dict], cluster_df: pd.DataFrame) -> Dict:
    """Compute aggregate analytics for the cluster."""

    # Extract scores
    scores = [p['score_result']['total_score'] for p in scored_projects]
    recommendations = [p['score_result']['recommendation'] for p in scored_projects]

    # Get capacities
    capacities = []
    for p in scored_projects:
        cap = p['score_result'].get('project', {}).get('capacity_mw', 0)
        if cap:
            capacities.append(cap)

    total_mw = sum(capacities)

    # Recommendation breakdown
    rec_counts = {
        'GO': recommendations.count('GO'),
        'CONDITIONAL': recommendations.count('CONDITIONAL'),
        'NO-GO': recommendations.count('NO-GO'),
    }

    # Score statistics
    score_stats = {
        'mean': np.mean(scores) if scores else 0,
        'median': np.median(scores) if scores else 0,
        'min': min(scores) if scores else 0,
        'max': max(scores) if scores else 0,
        'std': np.std(scores) if scores else 0,
    }

    # Technology breakdown
    tech_breakdown = {}
    for p in scored_projects:
        tech = p['score_result'].get('project', {}).get('type', 'Unknown')
        cap = p['score_result'].get('project', {}).get('capacity_mw', 0)
        if tech not in tech_breakdown:
            tech_breakdown[tech] = {'count': 0, 'mw': 0}
        tech_breakdown[tech]['count'] += 1
        tech_breakdown[tech]['mw'] += cap

    # ISO breakdown
    iso_breakdown = {}
    for p in scored_projects:
        iso = p['score_result'].get('project', {}).get('region',
               p['score_result'].get('project', {}).get('iso', 'Unknown'))
        cap = p['score_result'].get('project', {}).get('capacity_mw', 0)
        if iso not in iso_breakdown:
            iso_breakdown[iso] = {'count': 0, 'mw': 0}
        iso_breakdown[iso]['count'] += 1
        iso_breakdown[iso]['mw'] += cap

    # Risk summary by component
    risk_summary = {
        'queue_position': [],
        'study_progress': [],
        'developer_track_record': [],
        'poi_congestion': [],
        'project_characteristics': [],
    }

    for p in scored_projects:
        breakdown = p['score_result'].get('breakdown', {})
        for key in risk_summary:
            if key in breakdown:
                risk_summary[key].append(breakdown[key])

    risk_averages = {k: np.mean(v) if v else 0 for k, v in risk_summary.items()}

    return {
        'total_projects': len(scored_projects),
        'total_mw': total_mw,
        'total_gw': total_mw / 1000,
        'score_stats': score_stats,
        'rec_counts': rec_counts,
        'tech_breakdown': tech_breakdown,
        'iso_breakdown': iso_breakdown,
        'risk_averages': risk_averages,
    }


def _generate_cluster_charts(analytics: Dict, cluster_name: str) -> Dict[str, str]:
    """Generate cluster-level charts."""
    chart_images = {}

    try:
        import charts_altair as charts
        charts_dir = Path(__file__).parent.parent / 'charts'

        # Technology mix
        tech = analytics.get('tech_breakdown', {})
        if tech:
            tech_mw = {k: v['mw'] for k, v in tech.items() if v['mw'] > 0}
            if tech_mw:
                charts.technology_mix_donut(tech_mw)
                chart_images['tech_mix'] = _embed_chart(charts_dir / 'tech_mix_donut.png')

        # Regional breakdown
        iso = analytics.get('iso_breakdown', {})
        if iso:
            charts.regional_breakdown_bars(iso)
            chart_images['regional'] = _embed_chart(charts_dir / 'regional_breakdown.png')

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


def _build_cluster_html(
    cluster_name: str,
    client_name: str,
    scored_projects: List[Dict],
    analytics: Dict,
    chart_images: Dict,
    filter_info: Dict = None,
) -> str:
    """Build HTML content for cluster report."""

    total_projects = analytics['total_projects']
    total_gw = analytics['total_gw']
    score_stats = analytics['score_stats']
    rec_counts = analytics['rec_counts']
    tech_breakdown = analytics['tech_breakdown']
    iso_breakdown = analytics['iso_breakdown']

    # Determine primary ISO
    if iso_breakdown:
        primary_iso = max(iso_breakdown.items(), key=lambda x: x[1]['mw'])[0]
    else:
        primary_iso = 'Multiple'

    # Build project table rows
    project_rows = ""
    sorted_projects = sorted(scored_projects, key=lambda x: x['score_result']['total_score'], reverse=True)

    for p in sorted_projects:
        proj = p['score_result']['project']
        score = p['score_result']['total_score']
        rec = p['score_result']['recommendation']
        rec_class = f"score-{rec.lower().replace('-', '')}"

        project_rows += f'''
            <tr>
                <td>{p['id']}</td>
                <td>{proj.get('name', 'Unknown')[:35]}</td>
                <td>{proj.get('developer', 'Unknown')[:25]}</td>
                <td>{proj.get('type', 'Unknown')}</td>
                <td>{proj.get('capacity_mw', 0):,.0f}</td>
                <td class="score-cell {rec_class}">{score:.0f}</td>
                <td class="score-cell {rec_class}">{rec}</td>
            </tr>'''

    # Build technology breakdown rows
    tech_rows = ""
    for tech, data in sorted(tech_breakdown.items(), key=lambda x: x[1]['mw'], reverse=True):
        tech_rows += f'''
            <tr>
                <td>{tech}</td>
                <td>{data['count']}</td>
                <td>{data['mw']/1000:.1f} GW</td>
                <td>{data['mw']/analytics['total_mw']*100:.0f}%</td>
            </tr>'''

    # Build risk heatmap cells
    risk_cells = ""
    for p in sorted_projects[:20]:  # Limit to 20 for space
        score = p['score_result']['total_score']
        if score >= 70:
            risk_class = 'risk-low'
        elif score >= 50:
            risk_class = 'risk-medium'
        else:
            risk_class = 'risk-high'

        risk_cells += f'''
            <div class="risk-cell {risk_class}">
                <div class="risk-cell-id">{p['id'][:10]}</div>
                <div class="risk-cell-score">{score:.0f}</div>
            </div>'''

    html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Cluster Report - {cluster_name}</title>
</head>
<body>
    <!-- Cover Page -->
    <div class="cover-page">
        <div class="cover-content">
            <h1>{cluster_name}</h1>
            <div class="cover-subtitle">Portfolio Analysis Report</div>
            <div class="cover-stats">
                <div class="cover-stat">
                    <span class="cover-stat-value">{total_gw:.1f} GW</span>
                    <span class="cover-stat-label">Total Pipeline</span>
                </div>
                <div class="cover-stat">
                    <span class="cover-stat-value">{total_projects}</span>
                    <span class="cover-stat-label">Projects</span>
                </div>
                <div class="cover-stat">
                    <span class="cover-stat-value">{score_stats['mean']:.0f}</span>
                    <span class="cover-stat-label">Avg Score</span>
                </div>
            </div>
            <div class="cover-footer">
                <div>Prepared for: {client_name}</div>
                <div>{datetime.now().strftime('%B %d, %Y')}</div>
            </div>
        </div>
    </div>

    <!-- Executive Summary -->
    <div class="slide">
        <h1 class="slide-title">Executive Summary</h1>

        <div class="summary-grid">
            <div class="summary-card">
                <div class="summary-value">{total_gw:.1f} GW</div>
                <div class="summary-label">Total Pipeline</div>
                <div class="summary-detail">{total_projects} projects</div>
            </div>
            <div class="summary-card">
                <div class="summary-value">{score_stats['mean']:.0f}</div>
                <div class="summary-label">Average Score</div>
                <div class="summary-detail">Range: {score_stats['min']:.0f} - {score_stats['max']:.0f}</div>
            </div>
            <div class="summary-card" style="border-left: 4px solid #22c55e;">
                <div class="summary-value" style="color: #22c55e;">{rec_counts['GO']}</div>
                <div class="summary-label">GO Projects</div>
                <div class="summary-detail">{rec_counts['GO']/total_projects*100:.0f}% of portfolio</div>
            </div>
            <div class="summary-card" style="border-left: 4px solid #f59e0b;">
                <div class="summary-value" style="color: #f59e0b;">{rec_counts['CONDITIONAL']}</div>
                <div class="summary-label">CONDITIONAL</div>
                <div class="summary-detail">{rec_counts['CONDITIONAL']/total_projects*100:.0f}% of portfolio</div>
            </div>
            <div class="summary-card" style="border-left: 4px solid #ef4444;">
                <div class="summary-value" style="color: #ef4444;">{rec_counts['NO-GO']}</div>
                <div class="summary-label">NO-GO</div>
                <div class="summary-detail">{rec_counts['NO-GO']/total_projects*100:.0f}% of portfolio</div>
            </div>
        </div>

        <div class="cluster-note">
            <strong>Portfolio Summary:</strong> This cluster contains {total_projects} projects totaling {total_gw:.1f} GW.
            {rec_counts['GO']} projects ({rec_counts['GO']/total_projects*100:.0f}%) are recommended to proceed,
            {rec_counts['CONDITIONAL']} ({rec_counts['CONDITIONAL']/total_projects*100:.0f}%) require enhanced due diligence,
            and {rec_counts['NO-GO']} ({rec_counts['NO-GO']/total_projects*100:.0f}%) should be avoided or require significant risk mitigation.
        </div>
    </div>

    <!-- Project List -->
    <div class="slide">
        <h1 class="slide-title">Project Scorecard</h1>
        <p class="slide-subtitle">All projects ranked by feasibility score</p>

        <table class="project-table">
            <thead>
                <tr>
                    <th>Queue ID</th>
                    <th>Project Name</th>
                    <th>Developer</th>
                    <th>Type</th>
                    <th>MW</th>
                    <th>Score</th>
                    <th>Verdict</th>
                </tr>
            </thead>
            <tbody>
                {project_rows}
            </tbody>
        </table>
    </div>

    <!-- Technology Mix -->
    <div class="slide">
        <h1 class="slide-title">Technology Mix</h1>
        <p class="slide-subtitle">Pipeline by generation type</p>

        <div class="chart-row">
            <div class="chart-half">
                {f'<img src="{chart_images["tech_mix"]}" alt="Technology Mix">' if chart_images.get('tech_mix') else '<div style="padding:40px;text-align:center;color:#9ca3af;">Chart not available</div>'}
            </div>
            <div class="chart-half">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>Technology</th>
                            <th>Projects</th>
                            <th>Capacity</th>
                            <th>Share</th>
                        </tr>
                    </thead>
                    <tbody>
                        {tech_rows}
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- Risk Overview -->
    <div class="slide">
        <h1 class="slide-title">Risk Overview</h1>
        <p class="slide-subtitle">Project scores at a glance (green = GO, yellow = CONDITIONAL, red = NO-GO)</p>

        <div class="risk-heatmap">
            {risk_cells}
        </div>

        <div class="cluster-note" style="margin-top: 30px;">
            <strong>Score Distribution:</strong>
            Mean: {score_stats['mean']:.0f} |
            Median: {score_stats['median']:.0f} |
            Std Dev: {score_stats['std']:.1f} |
            Range: {score_stats['min']:.0f} - {score_stats['max']:.0f}
        </div>
    </div>

    <!-- Recommendations -->
    <div class="slide">
        <h1 class="slide-title">Recommendations</h1>

        <div style="margin-bottom: 25px;">
            <h3 style="color: #22c55e; margin-bottom: 10px;">GO Projects ({rec_counts['GO']})</h3>
            <p style="font-size: 10px; color: #4b5563;">
                These projects have strong fundamentals and are recommended to proceed with standard due diligence.
            </p>
            <table class="data-table" style="font-size: 9px;">
                <tr><th>Queue ID</th><th>Name</th><th>Score</th><th>MW</th></tr>
                {''.join(f"<tr><td>{p['id']}</td><td>{p['score_result']['project'].get('name', '')[:30]}</td><td>{p['score_result']['total_score']:.0f}</td><td>{p['score_result']['project'].get('capacity_mw', 0):,.0f}</td></tr>" for p in sorted_projects if p['score_result']['recommendation'] == 'GO')[:5]}
            </table>
        </div>

        <div style="margin-bottom: 25px;">
            <h3 style="color: #f59e0b; margin-bottom: 10px;">CONDITIONAL Projects ({rec_counts['CONDITIONAL']})</h3>
            <p style="font-size: 10px; color: #4b5563;">
                These projects require enhanced due diligence. Address flagged risks before proceeding.
            </p>
            <table class="data-table" style="font-size: 9px;">
                <tr><th>Queue ID</th><th>Name</th><th>Score</th><th>Key Risk</th></tr>
                {''.join(f"<tr><td>{p['id']}</td><td>{p['score_result']['project'].get('name', '')[:25]}</td><td>{p['score_result']['total_score']:.0f}</td><td>{p['score_result'].get('red_flags', ['N/A'])[0][:40] if p['score_result'].get('red_flags') else 'N/A'}</td></tr>" for p in sorted_projects if p['score_result']['recommendation'] == 'CONDITIONAL')[:5]}
            </table>
        </div>

        <div>
            <h3 style="color: #ef4444; margin-bottom: 10px;">NO-GO Projects ({rec_counts['NO-GO']})</h3>
            <p style="font-size: 10px; color: #4b5563;">
                These projects have multiple risk factors. Pass or require significant contractual protection.
            </p>
            <table class="data-table" style="font-size: 9px;">
                <tr><th>Queue ID</th><th>Name</th><th>Score</th><th>Key Risk</th></tr>
                {''.join(f"<tr><td>{p['id']}</td><td>{p['score_result']['project'].get('name', '')[:25]}</td><td>{p['score_result']['total_score']:.0f}</td><td>{p['score_result'].get('red_flags', ['N/A'])[0][:40] if p['score_result'].get('red_flags') else 'N/A'}</td></tr>" for p in sorted_projects if p['score_result']['recommendation'] == 'NO-GO')[:5]}
            </table>
        </div>
    </div>

    <!-- Footer Page -->
    <div class="slide">
        <h1 class="slide-title">Methodology & Notes</h1>

        <div style="display: flex; gap: 30px;">
            <div style="flex: 1;">
                <h3>Scoring Methodology</h3>
                <p style="font-size: 10px; line-height: 1.6;">
                    Projects are scored on a 100-point scale across five components:
                </p>
                <ul style="font-size: 9px; padding-left: 18px;">
                    <li><strong>Queue Position (25 pts):</strong> Position relative to other projects at POI</li>
                    <li><strong>Study Progress (25 pts):</strong> Advancement through interconnection studies</li>
                    <li><strong>Developer Track Record (20 pts):</strong> Developer experience and portfolio size</li>
                    <li><strong>POI Congestion (15 pts):</strong> Competition and congestion at interconnection point</li>
                    <li><strong>Project Characteristics (15 pts):</strong> Size, technology, and other factors</li>
                </ul>
            </div>
            <div style="flex: 1;">
                <h3>Recommendations</h3>
                <ul style="font-size: 9px; padding-left: 18px;">
                    <li><strong style="color: #22c55e;">GO (70+):</strong> Proceed with standard due diligence</li>
                    <li><strong style="color: #f59e0b;">CONDITIONAL (50-69):</strong> Enhanced due diligence required</li>
                    <li><strong style="color: #ef4444;">NO-GO (&lt;50):</strong> Pass or require significant risk mitigation</li>
                </ul>

                <h3 style="margin-top: 20px;">Data Sources</h3>
                <ul style="font-size: 9px; padding-left: 18px;">
                    <li>ISO/RTO public interconnection queues</li>
                    <li>LBL "Queued Up" historical data</li>
                    <li>Industry cost and timeline benchmarks</li>
                </ul>
            </div>
        </div>

        <div class="slide-footer">
            <strong>Disclaimer:</strong> This assessment uses automated scoring models and benchmark data.
            All recommendations should be validated through detailed due diligence.<br>
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | {total_projects} projects | {total_gw:.1f} GW
        </div>
    </div>
</body>
</html>'''

    return html


# CLI interface
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Cluster Report PDF")
    parser.add_argument('--ids', help='Comma-separated list of queue IDs')
    parser.add_argument('--developer', help='Filter by developer name')
    parser.add_argument('--poi', help='Filter by POI/substation')
    parser.add_argument('--iso', help='Filter by ISO/region')
    parser.add_argument('--type', help='Filter by technology type')
    parser.add_argument('--name', '-n', default='Project Cluster', help='Cluster name')
    parser.add_argument('--client', '-c', default='Confidential', help='Client name')
    parser.add_argument('--output', '-o', help='Output PDF path')
    parser.add_argument('--no-charts', action='store_true', help='Skip chart generation')

    args = parser.parse_args()

    # Determine filter type
    project_ids = None
    filter_by = None
    filter_value = None

    if args.ids:
        project_ids = [i.strip() for i in args.ids.split(',')]
    elif args.developer:
        filter_by = 'developer'
        filter_value = args.developer
    elif args.poi:
        filter_by = 'poi'
        filter_value = args.poi
    elif args.iso:
        filter_by = 'iso'
        filter_value = args.iso
    elif args.type:
        filter_by = 'type'
        filter_value = args.type
    else:
        print("Error: Must specify --ids, --developer, --poi, --iso, or --type")
        sys.exit(1)

    try:
        pdf_path = generate_cluster_report(
            project_ids=project_ids,
            filter_by=filter_by,
            filter_value=filter_value,
            cluster_name=args.name,
            client_name=args.client,
            output_path=args.output,
            include_charts=not args.no_charts,
        )
        print(f"\nReport generated: {pdf_path}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
