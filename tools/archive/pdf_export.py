#!/usr/bin/env python3
"""
PDF Export Module for Queue Analysis Reports

Generates professional PDF reports from analysis data.

Usage:
    from pdf_export import generate_pdf_report

    generate_pdf_report(
        project_id="1738",
        output_path="report.pdf",
        client_name="KPMG"
    )
"""

import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import base64

# Try to import WeasyPrint
try:
    from weasyprint import HTML, CSS
    WEASYPRINT_AVAILABLE = True
except ImportError:
    WEASYPRINT_AVAILABLE = False
    print("Warning: WeasyPrint not installed. Run: pip install weasyprint")

# Output directory
OUTPUT_DIR = Path(__file__).parent / 'output'
OUTPUT_DIR.mkdir(exist_ok=True)


def generate_pdf_report(
    project_id: str,
    df,
    client_name: str = "[CLIENT]",
    region: str = "NYISO",
    output_path: str = None
) -> str:
    """
    Generate a PDF report for a project.

    Args:
        project_id: Queue ID to analyze
        df: DataFrame with queue data
        client_name: Client name for report
        region: RTO/ISO region
        output_path: Output file path (optional)

    Returns:
        Path to generated PDF file
    """
    if not WEASYPRINT_AVAILABLE:
        raise RuntimeError("WeasyPrint is required for PDF export. Install with: pip install weasyprint")

    # Import analysis modules
    from scoring import FeasibilityScorer
    from real_data import RealDataEstimator

    # Score the project
    scorer = FeasibilityScorer(df)
    score_result = scorer.score_project(project_id=project_id)

    if 'error' in score_result:
        raise ValueError(f"Could not score project: {score_result['error']}")

    # Get estimates
    estimator = RealDataEstimator()
    proj = score_result['project']

    estimates = estimator.estimate_project(
        region=region,
        project_type=proj.get('type', 'Unknown'),
        capacity_mw=proj.get('capacity_mw', 0),
        months_in_queue=proj.get('months_in_queue', 0)
    )

    # Generate HTML content
    html_content = _build_pdf_html(
        project_id=project_id,
        region=region,
        client_name=client_name,
        score_result=score_result,
        estimates=estimates,
        estimator=estimator
    )

    # Convert to PDF
    if output_path is None:
        output_path = OUTPUT_DIR / f"report_{project_id}_{datetime.now().strftime('%Y%m%d')}.pdf"
    else:
        output_path = Path(output_path)

    # Generate PDF
    HTML(string=html_content).write_pdf(
        str(output_path),
        stylesheets=[CSS(string=_get_pdf_css())]
    )

    return str(output_path)


def _build_pdf_html(
    project_id: str,
    region: str,
    client_name: str,
    score_result: Dict,
    estimates: Dict,
    estimator
) -> str:
    """Build HTML content for PDF."""

    proj = score_result['project']
    breakdown = score_result['breakdown']
    cost = estimates['cost']
    timeline = estimates['timeline']
    completion = estimates['completion']

    # Recommendation colors
    rec = score_result['recommendation']
    rec_color = {'GO': '#22c55e', 'CONDITIONAL': '#f59e0b', 'NO-GO': '#ef4444'}.get(rec, '#6b7280')

    # Score gauge SVG
    score = score_result['total_score']
    score_color = '#22c55e' if score >= 70 else ('#f59e0b' if score >= 50 else '#ef4444')

    # Calculate arc for gauge
    import math
    angle = (score / 100) * 360
    large_arc = 1 if angle > 180 else 0
    rad = math.radians(angle - 90)
    end_x = 50 + 40 * math.cos(rad)
    end_y = 50 + 40 * math.sin(rad)

    gauge_svg = f'''
    <svg viewBox="0 0 100 100" width="120" height="120">
        <circle cx="50" cy="50" r="40" fill="none" stroke="#e5e7eb" stroke-width="8"/>
        <path d="M 50 10 A 40 40 0 {large_arc} 1 {end_x:.1f} {end_y:.1f}"
              fill="none" stroke="{score_color}" stroke-width="8" stroke-linecap="round"/>
        <text x="50" y="50" text-anchor="middle" dominant-baseline="middle"
              font-size="24" font-weight="bold" fill="{score_color}">{score:.0f}</text>
        <text x="50" y="68" text-anchor="middle" font-size="10" fill="#6b7280">/100</text>
    </svg>
    '''

    # Traffic light indicators
    def traffic_light(value, max_val):
        pct = value / max_val if max_val > 0 else 0
        if pct >= 0.75:
            return '<span class="traffic green"></span>'
        elif pct >= 0.5:
            return '<span class="traffic yellow"></span>'
        else:
            return '<span class="traffic red"></span>'

    # COD dates
    from dateutil.relativedelta import relativedelta
    now = datetime.now()
    def quarter(months):
        dt = now + relativedelta(months=int(months))
        return f"Q{(dt.month - 1) // 3 + 1} {dt.year}"

    html = f'''
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Interconnection Feasibility Assessment - {proj.get('name', 'Unknown')}</title>
    </head>
    <body>
        <!-- Header -->
        <div class="header">
            <div class="header-content">
                <h1>Interconnection Feasibility Assessment</h1>
                <div class="header-meta">
                    <strong>{proj.get('name', 'Unknown')}</strong> | Queue ID: {project_id} | {region}
                </div>
                <div class="header-meta">
                    Prepared for: {client_name} | {datetime.now().strftime('%B %d, %Y')}
                </div>
            </div>
        </div>

        <!-- Executive Summary -->
        <div class="section">
            <h2>Executive Summary</h2>
            <div class="summary-grid">
                <div class="summary-card">
                    <div class="gauge-container">
                        {gauge_svg}
                    </div>
                    <div class="recommendation" style="background: {rec_color};">{rec}</div>
                    <div class="grade">Grade: {score_result['grade']}</div>
                </div>
                <div class="summary-metrics">
                    <div class="metric-row">
                        <div class="metric-box">
                            <div class="metric-value">{estimator.format_cost_range(cost)}</div>
                            <div class="metric-label">Estimated Cost</div>
                        </div>
                        <div class="metric-box">
                            <div class="metric-value">{estimator.format_completion_rate(completion)}</div>
                            <div class="metric-label">Completion Probability</div>
                        </div>
                    </div>
                    <div class="metric-row">
                        <div class="metric-box">
                            <div class="metric-value">{quarter(timeline['remaining_p50'])}</div>
                            <div class="metric-label">Target COD (P50)</div>
                        </div>
                        <div class="metric-box">
                            <div class="metric-value">{cost['n_comparables']}</div>
                            <div class="metric-label">Comparable Projects</div>
                        </div>
                    </div>
                </div>
            </div>
            <div class="key-risk">
                <strong>Key Risk:</strong> {score_result['red_flags'][0] if score_result['red_flags'] else 'No critical risks identified'}
            </div>
        </div>

        <!-- Project Overview -->
        <div class="section">
            <h2>Project Overview</h2>
            <table class="data-table">
                <tr>
                    <th>Queue ID</th><td>{project_id}</td>
                    <th>Project Name</th><td>{proj.get('name', 'Unknown')}</td>
                </tr>
                <tr>
                    <th>Developer</th><td>{proj.get('developer', 'Unknown')}</td>
                    <th>Project Type</th><td>{proj.get('type', 'Unknown')}</td>
                </tr>
                <tr>
                    <th>Capacity</th><td>{proj.get('capacity_mw', 0):,.0f} MW</td>
                    <th>State</th><td>{proj.get('state', 'Unknown')}</td>
                </tr>
                <tr>
                    <th>Queue Date</th><td>{proj.get('queue_date', 'Unknown')}</td>
                    <th>Time in Queue</th><td>{proj.get('months_in_queue', 0)} months</td>
                </tr>
            </table>
        </div>

        <!-- Score Breakdown -->
        <div class="section">
            <h2>Feasibility Score Breakdown</h2>
            <table class="score-table">
                <tr>
                    <th>Component</th>
                    <th>Score</th>
                    <th>Max</th>
                    <th>Status</th>
                </tr>
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
                    <td><strong>{score_result['total_score']:.0f}</strong></td>
                    <td><strong>100</strong></td>
                    <td></td>
                </tr>
            </table>
        </div>

        <!-- Cost Analysis -->
        <div class="section">
            <h2>Cost Analysis</h2>
            <table class="data-table">
                <tr>
                    <th>Percentile</th>
                    <th>Total Cost</th>
                    <th>$/kW</th>
                </tr>
                <tr>
                    <td>P25 (Low)</td>
                    <td>${cost['total_millions']['p25']:.0f}M</td>
                    <td>${cost['per_kw']['p25']:.0f}/kW</td>
                </tr>
                <tr class="highlight-row">
                    <td><strong>P50 (Median)</strong></td>
                    <td><strong>${cost['total_millions']['p50']:.0f}M</strong></td>
                    <td><strong>${cost['per_kw']['p50']:.0f}/kW</strong></td>
                </tr>
                <tr>
                    <td>P75 (High)</td>
                    <td>${cost['total_millions']['p75']:.0f}M</td>
                    <td>${cost['per_kw']['p75']:.0f}/kW</td>
                </tr>
            </table>
            <div class="note">
                <strong>Confidence:</strong> {cost['confidence']} |
                <strong>Based on:</strong> {cost['n_comparables']} comparable {region} projects
            </div>
        </div>

        <!-- Timeline Analysis -->
        <div class="section">
            <h2>Timeline Analysis</h2>
            <table class="data-table">
                <tr>
                    <th>Percentile</th>
                    <th>Remaining</th>
                    <th>Target COD</th>
                </tr>
                <tr>
                    <td>P25 (Fast)</td>
                    <td>{timeline['remaining_p25']} months</td>
                    <td>{quarter(timeline['remaining_p25'])}</td>
                </tr>
                <tr class="highlight-row">
                    <td><strong>P50 (Typical)</strong></td>
                    <td><strong>{timeline['remaining_p50']} months</strong></td>
                    <td><strong>{quarter(timeline['remaining_p50'])}</strong></td>
                </tr>
                <tr>
                    <td>P75 (Slow)</td>
                    <td>{timeline['remaining_p75']} months</td>
                    <td>{quarter(timeline['remaining_p75'])}</td>
                </tr>
            </table>
            <div class="note">
                <strong>Completion Rate:</strong> {estimator.format_completion_rate(completion)} |
                <strong>Region rate:</strong> {completion['region_rate']*100:.1f}% |
                <strong>Type rate:</strong> {completion['type_rate']*100:.1f}%
            </div>
        </div>

        <!-- Risk Assessment -->
        <div class="section">
            <h2>Risk Assessment</h2>
            <div class="risk-grid">
                <div class="risk-column">
                    <h3 class="red-header">Red Flags</h3>
                    <ul class="flag-list">
                        {''.join(f'<li class="red-flag">{flag}</li>' for flag in score_result.get('red_flags', [])) or '<li class="no-flag">No red flags identified</li>'}
                    </ul>
                </div>
                <div class="risk-column">
                    <h3 class="green-header">Green Flags</h3>
                    <ul class="flag-list">
                        {''.join(f'<li class="green-flag">{flag}</li>' for flag in score_result.get('green_flags', [])) or '<li class="no-flag">No notable strengths identified</li>'}
                    </ul>
                </div>
            </div>
        </div>

        <!-- Recommendation -->
        <div class="section">
            <h2>Recommendation</h2>
            <div class="recommendation-box" style="border-color: {rec_color};">
                <div class="rec-badge" style="background: {rec_color};">{rec}</div>
                <p class="rec-text">
                    {'Proceed with standard due diligence. This project shows strong fundamentals across scoring categories.' if rec == 'GO' else
                     'Enhanced due diligence required. Address flagged items before proceeding.' if rec == 'CONDITIONAL' else
                     'Pass or require significant risk mitigation. Multiple risk factors present.'}
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
                {''.join(f'<li class="investigate">Investigate: {flag}</li>' for flag in score_result.get('red_flags', [])[:3])}
            </ul>
        </div>

        <!-- Footer -->
        <div class="footer">
            <p>
                <strong>Disclaimer:</strong> This assessment combines automated data extraction, scoring models, and benchmark-based estimates.
                All findings should be validated through manual review of source documents.
            </p>
            <p class="generated">
                Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} |
                Score: {score_result['total_score']:.0f}/100 |
                Recommendation: {rec}
            </p>
        </div>
    </body>
    </html>
    '''

    return html


def _get_pdf_css() -> str:
    """Get CSS styles for PDF."""
    return '''
    @page {
        size: letter;
        margin: 0.75in;
        @bottom-center {
            content: counter(page);
            font-size: 10px;
            color: #666;
        }
    }

    body {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        font-size: 11px;
        line-height: 1.5;
        color: #1f2937;
    }

    .header {
        background: linear-gradient(135deg, #1e3a5f 0%, #2c5282 100%);
        color: white;
        padding: 25px;
        margin: -0.75in -0.75in 20px -0.75in;
        width: calc(100% + 1.5in);
    }

    .header h1 {
        font-size: 22px;
        margin: 0 0 8px 0;
        font-weight: 600;
    }

    .header-meta {
        font-size: 11px;
        opacity: 0.9;
    }

    .section {
        margin-bottom: 20px;
        page-break-inside: avoid;
    }

    h2 {
        font-size: 14px;
        color: #1e3a5f;
        border-bottom: 2px solid #3182ce;
        padding-bottom: 5px;
        margin: 0 0 12px 0;
    }

    h3 {
        font-size: 12px;
        margin: 0 0 8px 0;
    }

    .summary-grid {
        display: flex;
        gap: 20px;
        margin-bottom: 15px;
    }

    .summary-card {
        text-align: center;
        padding: 15px;
        background: #f8fafc;
        border-radius: 8px;
        min-width: 140px;
    }

    .gauge-container {
        margin-bottom: 10px;
    }

    .recommendation {
        display: inline-block;
        padding: 6px 16px;
        border-radius: 15px;
        color: white;
        font-weight: bold;
        font-size: 12px;
    }

    .grade {
        margin-top: 8px;
        font-size: 11px;
        color: #6b7280;
    }

    .summary-metrics {
        flex: 1;
    }

    .metric-row {
        display: flex;
        gap: 15px;
        margin-bottom: 10px;
    }

    .metric-box {
        flex: 1;
        padding: 12px;
        background: #f8fafc;
        border-radius: 8px;
        text-align: center;
    }

    .metric-value {
        font-size: 16px;
        font-weight: bold;
        color: #1e3a5f;
    }

    .metric-label {
        font-size: 9px;
        color: #6b7280;
        margin-top: 3px;
    }

    .key-risk {
        padding: 10px 15px;
        background: #fef3c7;
        border-left: 4px solid #f59e0b;
        border-radius: 0 8px 8px 0;
        font-size: 11px;
    }

    table {
        width: 100%;
        border-collapse: collapse;
        margin: 10px 0;
    }

    .data-table th, .data-table td {
        padding: 8px 10px;
        text-align: left;
        border-bottom: 1px solid #e5e7eb;
    }

    .data-table th {
        background: #f1f5f9;
        font-weight: 600;
        color: #475569;
        width: 20%;
    }

    .score-table th, .score-table td {
        padding: 8px 10px;
        text-align: center;
        border-bottom: 1px solid #e5e7eb;
    }

    .score-table th {
        background: #f1f5f9;
        font-weight: 600;
    }

    .score-table td:first-child {
        text-align: left;
    }

    .total-row {
        background: #f8fafc;
    }

    .highlight-row {
        background: #f0fdf4;
    }

    .traffic {
        display: inline-block;
        width: 12px;
        height: 12px;
        border-radius: 50%;
    }

    .traffic.green { background: #22c55e; }
    .traffic.yellow { background: #f59e0b; }
    .traffic.red { background: #ef4444; }

    .note {
        padding: 10px;
        background: #f8fafc;
        border-radius: 6px;
        font-size: 10px;
        color: #64748b;
    }

    .risk-grid {
        display: flex;
        gap: 20px;
    }

    .risk-column {
        flex: 1;
    }

    .red-header { color: #dc2626; }
    .green-header { color: #16a34a; }

    .flag-list {
        list-style: none;
        padding: 0;
        margin: 0;
    }

    .flag-list li {
        padding: 6px 0;
        border-bottom: 1px solid #e5e7eb;
        font-size: 10px;
    }

    .red-flag::before { content: "⚠ "; color: #dc2626; }
    .green-flag::before { content: "✓ "; color: #16a34a; }
    .no-flag { color: #9ca3af; font-style: italic; }

    .recommendation-box {
        padding: 15px;
        border: 2px solid;
        border-radius: 8px;
        text-align: center;
    }

    .rec-badge {
        display: inline-block;
        padding: 8px 24px;
        border-radius: 20px;
        color: white;
        font-weight: bold;
        font-size: 14px;
        margin-bottom: 10px;
    }

    .rec-text {
        margin: 0;
        color: #4b5563;
    }

    .checklist {
        list-style: none;
        padding: 0;
    }

    .checklist li {
        padding: 8px 0;
        border-bottom: 1px solid #e5e7eb;
    }

    .checklist li::before {
        content: "☐ ";
        color: #3182ce;
    }

    .checklist .investigate {
        color: #dc2626;
    }

    .footer {
        margin-top: 30px;
        padding-top: 15px;
        border-top: 1px solid #e5e7eb;
        font-size: 9px;
        color: #6b7280;
    }

    .footer .generated {
        margin-top: 10px;
        text-align: center;
    }
    '''


# CLI interface
if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Generate PDF Feasibility Report")
    parser.add_argument('project_id', help='Queue ID to analyze')
    parser.add_argument('--file', '-f', help='Local data file')
    parser.add_argument('--client', default='[CLIENT]', help='Client name')
    parser.add_argument('--output', '-o', help='Output PDF path')
    parser.add_argument('--region', default='NYISO', help='Region')

    args = parser.parse_args()

    # Import data loader
    from analyze import QueueData

    # Load data
    loader = QueueData()
    if args.file:
        df = loader.load_file(args.file)
    else:
        df = loader.load_nyiso()

    if df.empty:
        print("Error: No data loaded")
        sys.exit(1)

    try:
        pdf_path = generate_pdf_report(
            project_id=args.project_id,
            df=df,
            client_name=args.client,
            region=args.region,
            output_path=args.output
        )
        print(f"PDF report generated: {pdf_path}")
    except Exception as e:
        print(f"Error generating PDF: {e}")
        sys.exit(1)
