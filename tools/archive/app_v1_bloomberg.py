#!/usr/bin/env python3
"""
Interconnection Feasibility Assessment Dashboard

A Streamlit app for PE firms to analyze interconnection queue projects.

Run with: streamlit run app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
from pathlib import Path
from datetime import datetime

# Add archive to path for legacy modules
sys.path.insert(0, str(Path(__file__).parent / 'archive'))

from analyze import QueueData, QueueAnalyzer
from scoring import FeasibilityScorer
from unified_data import UnifiedQueue, RegionalBenchmarks

# Import centralized report manager
try:
    from report_manager import ReportManager, create_project_feasibility_report
    REPORT_MANAGER_AVAILABLE = True
except ImportError:
    REPORT_MANAGER_AVAILABLE = False

# Try to import Playwright for PDF generation
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


def generate_pdf_from_data(
    project_id: str,
    project,
    score_result: dict,
    breakdown: dict,
    costs: dict,
    timeline: dict,
    client_name: str
) -> bytes:
    """Generate comprehensive PDF report using Playwright."""
    if not PLAYWRIGHT_AVAILABLE:
        raise RuntimeError("Playwright is required for PDF export. Install with: pip install playwright && playwright install chromium")

    # Helper to spell out project types
    def spell_out_type(type_code):
        type_map = {
            'S': 'Solar', 'W': 'Wind', 'ES': 'Battery Storage', 'B': 'Battery Storage',
            'BESS': 'Battery Storage', 'NG': 'Natural Gas', 'Gas': 'Natural Gas',
            'L': 'Load (Data Center)', 'AC': 'AC Transmission', 'DC': 'DC Transmission',
            'H': 'Hydro', 'N': 'Nuclear',
        }
        if pd.isna(type_code):
            return 'Unknown'
        return type_map.get(str(type_code).strip(), str(type_code))

    # Build HTML content
    rec = score_result['recommendation']
    score = score_result['total_score']
    grade = score_result.get('grade', 'C')

    # Color scheme
    colors = {
        'GO': {'primary': '#059669', 'bg': '#ecfdf5', 'text': '#065f46'},
        'CONDITIONAL': {'primary': '#d97706', 'bg': '#fffbeb', 'text': '#92400e'},
        'NO-GO': {'primary': '#dc2626', 'bg': '#fef2f2', 'text': '#991b1b'}
    }
    rec_colors = colors.get(rec, colors['CONDITIONAL'])

    # Get project details
    project_name = project.get('Project Name', 'Unknown')
    developer = project.get('Developer/Interconnection Customer', 'Unknown')
    project_type_raw = project.get('Type/ Fuel', 'Unknown')
    project_type = spell_out_type(project_type_raw)  # Spelled out
    capacity = project.get('SP (MW)', 0)
    if pd.isna(capacity):
        capacity = 0
    state = project.get('State', 'Unknown')
    poi = project.get('Points of Interconnection', 'Unknown')
    queue_date = project.get('Date of IR', 'Unknown')

    # Calculate queue age
    months_in_queue = 0
    queue_year = None
    try:
        if pd.notna(queue_date):
            q_date = pd.to_datetime(queue_date)
            months_in_queue = (datetime.now() - q_date).days // 30
            queue_year = q_date.year
    except:
        pass

    # Determine if stale project
    is_stale = months_in_queue > 60 and breakdown['study_progress'] < 12.5
    is_old = months_in_queue > 60
    years_in_queue = months_in_queue / 12

    # Queue age color and status
    if months_in_queue <= 24:
        age_color = '#059669'  # Green
        age_status = 'Recent'
    elif months_in_queue <= 60:
        age_color = '#d97706'  # Yellow
        age_status = 'Moderate'
    else:
        age_color = '#dc2626'  # Red
        age_status = 'Extended'

    # Completion rate context - be specific about what rate means
    completion_rate = timeline['completion_rate']
    completion_rate_pct = completion_rate * 100
    type_rates = {'Solar': 8.6, 'Wind': 17.4, 'Battery Storage': 1.8, 'Natural Gas': 27.8}
    type_specific_rate = type_rates.get(project_type, completion_rate_pct)

    # Recommendation rationale
    rationale = []
    if rec == 'GO':
        rationale.append("Strong fundamentals support proceeding with standard due diligence.")
        if breakdown['study_progress'] > 18:
            rationale.append("Advanced study phase reduces execution risk.")
    elif rec == 'CONDITIONAL':
        rationale.append("Project has potential but requires enhanced due diligence on key risks.")
        if breakdown['study_progress'] < 15:
            rationale.append("Early study phase creates uncertainty.")
    else:
        rationale.append("Significant risks identified. Recommend pass or major risk mitigation.")

    # Stale project warning text (compact to fit page 1)
    stale_warning_html = ''
    if is_stale:
        stale_warning_html = f'''
        <div style="background:#fef2f2;border:1px solid #dc2626;border-radius:5px;padding:8px;margin:8px 0;">
            <strong style="color:#dc2626;font-size:9px;">⚠️ STALE PROJECT WARNING:</strong>
            <span style="font-size:8px;color:#991b1b;">{years_in_queue:.1f} years in queue with limited study progress. Verify current status with NYISO.</span>
        </div>
        '''
    elif is_old:
        stale_warning_html = f'''
        <div style="background:#fffbeb;border:1px solid #d97706;border-radius:5px;padding:8px;margin:8px 0;">
            <strong style="color:#d97706;font-size:9px;">⚠️ Extended Queue Time:</strong>
            <span style="font-size:8px;color:#92400e;">{years_in_queue:.1f} years in queue. Standard benchmarks may not apply.</span>
        </div>
        '''

    # Score bar chart HTML generator (compact)
    def make_score_bar(name, score_val, max_val, description):
        pct = (score_val / max_val) * 100
        if pct >= 70:
            bar_color = '#059669'
        elif pct >= 40:
            bar_color = '#d97706'
        else:
            bar_color = '#dc2626'
        return f'''
        <div style="margin-bottom:5px;">
            <div style="display:flex;justify-content:space-between;font-size:8px;margin-bottom:1px;">
                <span><strong>{name}</strong></span>
                <span>{score_val:.1f}/{max_val} ({pct:.0f}%)</span>
            </div>
            <div style="background:#e5e7eb;height:8px;border-radius:4px;overflow:hidden;">
                <div style="background:{bar_color};height:100%;width:{pct}%;border-radius:4px;"></div>
            </div>
        </div>
        '''

    score_bars_html = ''.join([
        make_score_bar('Queue Position', breakdown['queue_position'], 25, ''),
        make_score_bar('Study Progress', breakdown['study_progress'], 25, ''),
        make_score_bar('Developer Track Record', breakdown['developer_track_record'], 20, ''),
        make_score_bar('POI Congestion', breakdown['poi_congestion'], 15, ''),
        make_score_bar('Project Characteristics', breakdown['project_characteristics'], 15, ''),
    ])

    html_content = f'''
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Interconnection Feasibility Assessment - {project_id}</title>
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}

            body {{
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
                font-size: 10px;
                line-height: 1.4;
                color: #1f2937;
                background: #fff;
            }}

            .page {{
                width: 100%;
                padding: 25px 35px;
                page-break-after: always;
            }}

            .page:last-child {{
                page-break-after: avoid;
            }}

            /* Header - more compact */
            .header {{
                display: flex;
                justify-content: space-between;
                align-items: flex-start;
                padding-bottom: 8px;
                border-bottom: 2px solid #1e3a5f;
                margin-bottom: 10px;
            }}

            .header-left h1 {{
                font-size: 18px;
                font-weight: 700;
                color: #1e3a5f;
            }}

            .header-left .subtitle {{
                font-size: 10px;
                color: #6b7280;
                margin-top: 2px;
            }}

            .header-right {{
                text-align: right;
                font-size: 9px;
                color: #6b7280;
            }}

            .header-right .client {{
                font-weight: 600;
                color: #1f2937;
            }}

            /* Verdict Box - more compact */
            .verdict-box {{
                background: {rec_colors['bg']};
                border: 2px solid {rec_colors['primary']};
                border-radius: 8px;
                padding: 10px 15px;
                display: flex;
                align-items: center;
                gap: 15px;
                margin-bottom: 10px;
            }}

            .verdict-score {{
                text-align: center;
                min-width: 70px;
            }}

            .verdict-score .number {{
                font-size: 36px;
                font-weight: 700;
                color: {rec_colors['primary']};
                line-height: 1;
            }}

            .verdict-score .label {{
                font-size: 9px;
                color: {rec_colors['text']};
                text-transform: uppercase;
                letter-spacing: 0.5px;
                margin-top: 3px;
            }}

            .verdict-rec {{
                background: {rec_colors['primary']};
                color: white;
                padding: 6px 18px;
                border-radius: 5px;
                font-size: 14px;
                font-weight: 700;
                letter-spacing: 1px;
            }}

            .verdict-rationale {{
                flex: 1;
            }}

            .verdict-rationale p {{
                color: {rec_colors['text']};
                font-size: 9px;
                margin-bottom: 3px;
            }}

            /* Section - more compact */
            .section {{
                margin-bottom: 12px;
            }}

            .section-title {{
                font-size: 11px;
                font-weight: 700;
                color: #1e3a5f;
                text-transform: uppercase;
                letter-spacing: 0.3px;
                padding-bottom: 5px;
                border-bottom: 1px solid #e5e7eb;
                margin-bottom: 8px;
            }}

            /* Metrics Grid - more compact */
            .metrics-grid {{
                display: grid;
                grid-template-columns: repeat(5, 1fr);
                gap: 8px;
                margin-bottom: 12px;
            }}

            .metric-card {{
                background: #f8fafc;
                border: 1px solid #e2e8f0;
                border-radius: 6px;
                padding: 8px;
                text-align: center;
            }}

            .metric-card .value {{
                font-size: 16px;
                font-weight: 700;
                color: #1e3a5f;
            }}

            .metric-card .label {{
                font-size: 7px;
                color: #64748b;
                text-transform: uppercase;
                letter-spacing: 0.3px;
                margin-top: 2px;
            }}

            .metric-card.highlight {{
                background: #eff6ff;
                border-color: #3b82f6;
            }}

            /* Tables - more compact */
            table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 9px;
            }}

            th, td {{
                padding: 6px 8px;
                text-align: left;
                border-bottom: 1px solid #e5e7eb;
            }}

            th {{
                background: #f8fafc;
                font-weight: 600;
                color: #374151;
                text-transform: uppercase;
                font-size: 8px;
                letter-spacing: 0.3px;
            }}

            tr:last-child td {{
                border-bottom: none;
            }}

            .table-highlight {{
                background: #fffbeb;
                font-weight: 600;
            }}

            /* Two Column Layout */
            .two-col {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 10px;
            }}

            /* Flags - more compact */
            .flag {{
                padding: 4px 8px;
                margin: 3px 0;
                border-radius: 4px;
                font-size: 8px;
            }}

            .flag-red {{
                background: #fef2f2;
                border-left: 3px solid #dc2626;
                color: #991b1b;
            }}

            .flag-green {{
                background: #f0fdf4;
                border-left: 3px solid #16a34a;
                color: #166534;
            }}

            .flag-yellow {{
                background: #fffbeb;
                border-left: 3px solid #d97706;
                color: #92400e;
            }}

            /* Context Box - more compact */
            .context-box {{
                background: #f0f9ff;
                border: 1px solid #0ea5e9;
                border-radius: 6px;
                padding: 8px;
                margin: 8px 0;
            }}

            .context-box .title {{
                font-weight: 600;
                color: #0369a1;
                font-size: 9px;
                text-transform: uppercase;
                letter-spacing: 0.3px;
                margin-bottom: 5px;
            }}

            .context-box p {{
                font-size: 9px;
                color: #0c4a6e;
            }}

            /* Checklist - more compact */
            .checklist {{
                list-style: none;
            }}

            .checklist li {{
                padding: 5px 0;
                padding-left: 20px;
                position: relative;
                font-size: 9px;
                border-bottom: 1px solid #f3f4f6;
            }}

            .checklist li:before {{
                content: "☐";
                position: absolute;
                left: 0;
                color: #9ca3af;
            }}

            /* Footer */
            .footer {{
                margin-top: 15px;
                padding-top: 10px;
                border-top: 1px solid #e5e7eb;
                font-size: 8px;
                color: #9ca3af;
                display: flex;
                justify-content: space-between;
            }}

            .footer-left {{
                font-weight: 600;
                color: #6b7280;
            }}

            /* Page break helpers */
            .page-break {{
                page-break-before: always;
            }}

            /* Prevent orphaned footer on new page */
            .footer {{
                page-break-inside: avoid;
                page-break-before: avoid;
            }}

            /* Keep sections together */
            .section {{
                page-break-inside: avoid;
            }}

            /* Ensure last page doesn't overflow */
            .page:last-child {{
                page-break-after: avoid !important;
                page-break-inside: avoid;
            }}
        </style>
    </head>
    <body>
        <!-- PAGE 1: Executive Summary -->
        <div class="page">
            <div class="header">
                <div class="header-left">
                    <h1>Interconnection Feasibility Assessment</h1>
                    <div class="subtitle">Queue Position Analysis & Due Diligence Report</div>
                </div>
                <div class="header-right">
                    <div class="client">Prepared for: {client_name}</div>
                    <div>{datetime.now().strftime('%B %d, %Y')}</div>
                    <div>Queue ID: {project_id} | NYISO</div>
                </div>
            </div>

            <div class="verdict-box">
                <div class="verdict-score">
                    <div class="number">{score:.0f}</div>
                    <div class="label">Feasibility Score</div>
                </div>
                <div class="verdict-rec">{rec}</div>
                <div class="verdict-rationale">
                    {''.join(f'<p>• {r}</p>' for r in rationale)}
                </div>
            </div>

            {stale_warning_html}

            <div class="metrics-grid">
                <div class="metric-card highlight">
                    <div class="value">{score:.0f}/100</div>
                    <div class="label">Overall Score</div>
                </div>
                <div class="metric-card">
                    <div class="value">${costs['med_total']:.1f}M</div>
                    <div class="label">Est. IC Cost (P50)</div>
                </div>
                <div class="metric-card">
                    <div class="value">{timeline['likely_date']}</div>
                    <div class="label">Target COD</div>
                </div>
                <div class="metric-card">
                    <div class="value">{type_specific_rate:.0f}%</div>
                    <div class="label">{project_type} Completion</div>
                </div>
                <div class="metric-card">
                    <div class="value">{capacity:.0f} MW</div>
                    <div class="label">Capacity</div>
                </div>
            </div>

            <div class="section">
                <div class="section-title">Project Details</div>
                <table>
                    <tr>
                        <th style="width:20%">Project Name</th>
                        <td style="width:30%">{project_name}</td>
                        <th style="width:20%">Queue ID</th>
                        <td style="width:30%">{project_id}</td>
                    </tr>
                    <tr>
                        <th>Developer</th>
                        <td>{developer}</td>
                        <th>Project Type</th>
                        <td>{project_type}</td>
                    </tr>
                    <tr>
                        <th>Capacity</th>
                        <td>{capacity:.0f} MW</td>
                        <th>State</th>
                        <td>{state}</td>
                    </tr>
                    <tr>
                        <th>Queue Date</th>
                        <td>{queue_date}</td>
                        <th>Time in Queue</th>
                        <td>{months_in_queue} months</td>
                    </tr>
                    <tr>
                        <th>POI</th>
                        <td colspan="3">{poi}</td>
                    </tr>
                </table>
            </div>

            <div class="context-box">
                <div class="title">Critical Market Context</div>
                <p><strong>NYISO has the lowest project completion rate of any major RTO at just 6.2%</strong> (vs. 12.2% national average).
                This means 94 out of every 100 NYISO queue projects never reach commercial operation.
                Projects that do succeed typically have: (1) advanced study phase, (2) experienced developers, and (3) manageable POI congestion.</p>
            </div>

            <div class="two-col">
                <div class="section">
                    <div class="section-title">Risk Factors</div>
                    {''.join(f'<div class="flag flag-red">{flag}</div>' for flag in score_result.get('red_flags', [])) or '<div class="flag flag-green">No critical risk factors identified</div>'}
                </div>
                <div class="section">
                    <div class="section-title">Positive Indicators</div>
                    {''.join(f'<div class="flag flag-green">{flag}</div>' for flag in score_result.get('green_flags', [])) or '<div class="flag flag-yellow">No notable positive indicators</div>'}
                </div>
            </div>

            <div class="footer">
                <div class="footer-left">CONFIDENTIAL - For authorized recipient only</div>
                <div>Page 1 of 2</div>
            </div>
        </div>

        <!-- PAGE 2: Detailed Analysis -->
        <div class="page">
            <div class="header">
                <div class="header-left">
                    <h1>Detailed Analysis</h1>
                    <div class="subtitle">{project_name} | {project_id}</div>
                </div>
                <div class="header-right">
                    <div>{datetime.now().strftime('%B %d, %Y')}</div>
                </div>
            </div>

            <div class="section">
                <div class="section-title">Score Breakdown</div>
                <div style="margin-bottom:15px;">
                    {score_bars_html}
                </div>
                <table>
                    <tr>
                        <th>Scoring Component</th>
                        <th style="width:80px;text-align:center">Score</th>
                        <th style="width:80px;text-align:center">Max</th>
                        <th style="width:80px;text-align:center">%</th>
                        <th>Assessment</th>
                    </tr>
                    <tr>
                        <td><strong>Queue Position</strong><br><span style="color:#6b7280;font-size:9px">Position relative to other projects at POI</span></td>
                        <td style="text-align:center">{breakdown['queue_position']:.1f}</td>
                        <td style="text-align:center">25</td>
                        <td style="text-align:center">{breakdown['queue_position']/25*100:.0f}%</td>
                        <td>{'Strong position' if breakdown['queue_position'] > 20 else 'Moderate position' if breakdown['queue_position'] > 12 else 'Late position - higher risk'}</td>
                    </tr>
                    <tr>
                        <td><strong>Study Progress</strong><br><span style="color:#6b7280;font-size:9px">Advancement through interconnection studies</span></td>
                        <td style="text-align:center">{breakdown['study_progress']:.1f}</td>
                        <td style="text-align:center">25</td>
                        <td style="text-align:center">{breakdown['study_progress']/25*100:.0f}%</td>
                        <td>{'IA signed - low risk' if breakdown['study_progress'] > 22 else 'Mid-study phase' if breakdown['study_progress'] > 12 else 'Early phase - high uncertainty'}</td>
                    </tr>
                    <tr>
                        <td><strong>Developer Track Record</strong><br><span style="color:#6b7280;font-size:9px">Historical project completion success</span></td>
                        <td style="text-align:center">{breakdown['developer_track_record']:.1f}</td>
                        <td style="text-align:center">20</td>
                        <td style="text-align:center">{breakdown['developer_track_record']/20*100:.0f}%</td>
                        <td>{'Experienced developer' if breakdown['developer_track_record'] > 14 else 'Moderate experience' if breakdown['developer_track_record'] > 10 else 'Limited track record'}</td>
                    </tr>
                    <tr>
                        <td><strong>POI Congestion</strong><br><span style="color:#6b7280;font-size:9px">Competition at point of interconnection</span></td>
                        <td style="text-align:center">{breakdown['poi_congestion']:.1f}</td>
                        <td style="text-align:center">15</td>
                        <td style="text-align:center">{breakdown['poi_congestion']/15*100:.0f}%</td>
                        <td>{'Low congestion' if breakdown['poi_congestion'] > 12 else 'Moderate congestion' if breakdown['poi_congestion'] > 7 else 'High congestion - delays likely'}</td>
                    </tr>
                    <tr>
                        <td><strong>Project Characteristics</strong><br><span style="color:#6b7280;font-size:9px">Type, size, and technology factors</span></td>
                        <td style="text-align:center">{breakdown['project_characteristics']:.1f}</td>
                        <td style="text-align:center">15</td>
                        <td style="text-align:center">{breakdown['project_characteristics']/15*100:.0f}%</td>
                        <td>{'Favorable characteristics' if breakdown['project_characteristics'] > 10 else 'Standard characteristics'}</td>
                    </tr>
                    <tr class="table-highlight">
                        <td><strong>TOTAL SCORE</strong></td>
                        <td style="text-align:center"><strong>{score:.0f}</strong></td>
                        <td style="text-align:center"><strong>100</strong></td>
                        <td style="text-align:center"><strong>{score:.0f}%</strong></td>
                        <td><strong>{rec}</strong></td>
                    </tr>
                </table>
            </div>

            <div class="two-col">
                <div class="section">
                    <div class="section-title">Interconnection Cost Estimate</div>
                    <table>
                        <tr>
                            <th>Scenario</th>
                            <th style="text-align:right">Total Cost</th>
                            <th style="text-align:right">$/kW</th>
                        </tr>
                        <tr>
                            <td>Low (P25)</td>
                            <td style="text-align:right">${costs['low_total']:.1f}M</td>
                            <td style="text-align:right">${costs['low_per_kw']:.0f}/kW</td>
                        </tr>
                        <tr class="table-highlight">
                            <td><strong>Base Case (P50)</strong></td>
                            <td style="text-align:right"><strong>${costs['med_total']:.1f}M</strong></td>
                            <td style="text-align:right"><strong>${costs['med_per_kw']:.0f}/kW</strong></td>
                        </tr>
                        <tr>
                            <td>High (P75)</td>
                            <td style="text-align:right">${costs['high_total']:.1f}M</td>
                            <td style="text-align:right">${costs['high_per_kw']:.0f}/kW</td>
                        </tr>
                    </table>
                    <p style="font-size:9px;color:#6b7280;margin-top:10px;">
                        Based on historical cost data from 1,400+ completed interconnection studies.
                        Actual costs from study documents may differ significantly.
                    </p>
                </div>
                <div class="section">
                    <div class="section-title">Timeline Estimate</div>
                    <table>
                        <tr>
                            <th>Scenario</th>
                            <th style="text-align:right">Months</th>
                            <th style="text-align:right">Est. COD</th>
                        </tr>
                        <tr>
                            <td>Optimistic</td>
                            <td style="text-align:right">{timeline['optimistic']} mo</td>
                            <td style="text-align:right">{timeline['optimistic_date']}</td>
                        </tr>
                        <tr class="table-highlight">
                            <td><strong>Base Case</strong></td>
                            <td style="text-align:right"><strong>{timeline['likely']} mo</strong></td>
                            <td style="text-align:right"><strong>{timeline['likely_date']}</strong></td>
                        </tr>
                        <tr>
                            <td>Pessimistic</td>
                            <td style="text-align:right">{timeline['pessimistic']} mo</td>
                            <td style="text-align:right">{timeline['pessimistic_date']}</td>
                        </tr>
                    </table>
                    <p style="font-size:9px;color:#6b7280;margin-top:10px;">
                        Median time from queue entry to COD is 48+ months nationally.
                        NYISO projects typically experience longer timelines.
                    </p>
                </div>
            </div>

            <div class="section">
                <div class="section-title">Due Diligence Checklist</div>
                <div class="two-col">
                    <ul class="checklist">
                        <li>Obtain and review Feasibility Study results</li>
                        <li>Obtain and review System Impact Study (if complete)</li>
                        <li>Obtain and review Facilities Study (if complete)</li>
                        <li>Verify Interconnection Agreement status</li>
                        <li>Review network upgrade cost allocation</li>
                        <li>Confirm milestone payment schedule</li>
                    </ul>
                    <ul class="checklist">
                        <li>Verify site control documentation</li>
                        <li>Review developer's financial capacity</li>
                        <li>Check for affected system study requirements</li>
                        <li>Verify permitting status and timeline</li>
                        <li>Assess offtake/PPA status</li>
                        <li>Review any cluster study participation</li>
                    </ul>
                </div>
            </div>

            <div class="context-box">
                <div class="title">Data Sources & Methodology</div>
                <p>This assessment uses data from: NYISO Interconnection Queue (current), Lawrence Berkeley National Laboratory "Queued Up" dataset (36,441 historical projects),
                and regional cost studies from PJM, NYISO, MISO, SPP, and ISO-NE (3,400+ projects with cost data). Completion rates are project-count weighted.
                Cost estimates use median values from historical studies. All figures are estimates and should be validated against actual interconnection study documents.</p>
            </div>

            <div class="footer">
                <div class="footer-left">CONFIDENTIAL - For authorized recipient only</div>
                <div>Page 2 of 2</div>
            </div>
        </div>
    </body>
    </html>
    '''

    # Generate PDF
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.set_content(html_content)
        pdf_bytes = page.pdf(
            format='Letter',
            margin={'top': '0.5in', 'right': '0.5in', 'bottom': '0.5in', 'left': '0.5in'},
            print_background=True
        )
        browser.close()

    return pdf_bytes


# Keep old function signature for compatibility
def generate_pdf_from_data_legacy(
    project_id: str,
    project,
    score_result: dict,
    breakdown: dict,
    costs: dict,
    timeline: dict,
    client_name: str
) -> bytes:
    """Legacy PDF generation - redirects to new function."""
    return generate_pdf_from_data(project_id, project, score_result, breakdown, costs, timeline, client_name)


def generate_html_report(
    project_id: str,
    project,
    score_result: dict,
    breakdown: dict,
    costs: dict,
    timeline: dict,
    client_name: str
) -> str:
    """Generate HTML report (for cloud deployment where Playwright isn't available)."""

    # Helper to spell out project types
    def spell_out_type(type_code):
        type_map = {
            'S': 'Solar', 'W': 'Wind', 'ES': 'Battery Storage', 'B': 'Battery Storage',
            'BESS': 'Battery Storage', 'NG': 'Natural Gas', 'Gas': 'Natural Gas',
            'L': 'Load (Data Center)', 'AC': 'AC Transmission', 'DC': 'DC Transmission',
            'H': 'Hydro', 'N': 'Nuclear',
        }
        if pd.isna(type_code):
            return 'Unknown'
        return type_map.get(str(type_code).strip(), str(type_code))

    rec = score_result['recommendation']
    score = score_result['total_score']

    colors = {
        'GO': {'primary': '#059669', 'bg': '#ecfdf5', 'text': '#065f46'},
        'CONDITIONAL': {'primary': '#d97706', 'bg': '#fffbeb', 'text': '#92400e'},
        'NO-GO': {'primary': '#dc2626', 'bg': '#fef2f2', 'text': '#991b1b'}
    }
    rec_colors = colors.get(rec, colors['CONDITIONAL'])

    project_name = project.get('Project Name', 'Unknown')
    developer = project.get('Developer/Interconnection Customer', 'Unknown')
    project_type = spell_out_type(project.get('Type/ Fuel', 'Unknown'))
    capacity = project.get('SP (MW)', 0) or 0
    state = project.get('State', 'Unknown')
    poi = project.get('Points of Interconnection', 'Unknown')
    queue_date = project.get('Date of IR', 'Unknown')

    return f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Feasibility Report - {project_id}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 900px; margin: 0 auto; padding: 40px; color: #1f2937; }}
        h1 {{ color: #1e3a5f; border-bottom: 3px solid #1e3a5f; padding-bottom: 10px; }}
        .verdict {{ background: {rec_colors['bg']}; border: 2px solid {rec_colors['primary']}; border-radius: 10px; padding: 20px; margin: 20px 0; display: flex; align-items: center; gap: 20px; }}
        .score {{ font-size: 48px; font-weight: bold; color: {rec_colors['primary']}; }}
        .rec-badge {{ background: {rec_colors['primary']}; color: white; padding: 8px 20px; border-radius: 5px; font-weight: bold; font-size: 18px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
        th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #e5e7eb; }}
        th {{ background: #f8fafc; font-weight: 600; }}
        .section {{ margin: 25px 0; }}
        .section-title {{ font-size: 14px; font-weight: 700; color: #1e3a5f; text-transform: uppercase; border-bottom: 2px solid #e5e7eb; padding-bottom: 8px; margin-bottom: 15px; }}
        .flag {{ padding: 8px 12px; margin: 5px 0; border-radius: 5px; }}
        .flag-red {{ background: #fef2f2; border-left: 4px solid #dc2626; color: #991b1b; }}
        .flag-green {{ background: #f0fdf4; border-left: 4px solid #16a34a; color: #166534; }}
        .context {{ background: #f0f9ff; border: 1px solid #0ea5e9; border-radius: 8px; padding: 15px; margin: 20px 0; }}
        .footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #e5e7eb; color: #6b7280; font-size: 12px; }}
        @media print {{ body {{ padding: 20px; }} }}
    </style>
</head>
<body>
    <h1>Interconnection Feasibility Assessment</h1>
    <p><strong>Prepared for:</strong> {client_name} | <strong>Date:</strong> {datetime.now().strftime('%B %d, %Y')} | <strong>Queue ID:</strong> {project_id}</p>

    <div class="verdict">
        <div class="score">{score:.0f}</div>
        <div>
            <div class="rec-badge">{rec}</div>
            <p style="margin-top:10px;color:{rec_colors['text']}">Feasibility Score out of 100</p>
        </div>
    </div>

    <div class="section">
        <div class="section-title">Project Details</div>
        <table>
            <tr><th>Project Name</th><td>{project_name}</td><th>Developer</th><td>{developer}</td></tr>
            <tr><th>Type</th><td>{project_type}</td><th>Capacity</th><td>{capacity:.0f} MW</td></tr>
            <tr><th>State</th><td>{state}</td><th>Queue Date</th><td>{queue_date}</td></tr>
            <tr><th>POI</th><td colspan="3">{poi}</td></tr>
        </table>
    </div>

    <div class="section">
        <div class="section-title">Key Metrics</div>
        <table>
            <tr><th>Metric</th><th>Value</th></tr>
            <tr><td>Estimated IC Cost (P50)</td><td><strong>${costs['med_total']:.1f}M</strong> (${costs['med_per_kw']:.0f}/kW)</td></tr>
            <tr><td>Cost Range</td><td>${costs['low_total']:.1f}M - ${costs['high_total']:.1f}M</td></tr>
            <tr><td>Target COD</td><td><strong>{timeline['likely_date']}</strong></td></tr>
            <tr><td>Timeline Range</td><td>{timeline['optimistic_date']} - {timeline['pessimistic_date']}</td></tr>
            <tr><td>Completion Probability</td><td>{timeline['completion_rate']*100:.0f}%</td></tr>
        </table>
    </div>

    <div class="section">
        <div class="section-title">Score Breakdown</div>
        <table>
            <tr><th>Component</th><th>Score</th><th>Max</th><th>%</th></tr>
            <tr><td>Queue Position</td><td>{breakdown['queue_position']:.1f}</td><td>25</td><td>{breakdown['queue_position']/25*100:.0f}%</td></tr>
            <tr><td>Study Progress</td><td>{breakdown['study_progress']:.1f}</td><td>25</td><td>{breakdown['study_progress']/25*100:.0f}%</td></tr>
            <tr><td>Developer Track Record</td><td>{breakdown['developer_track_record']:.1f}</td><td>20</td><td>{breakdown['developer_track_record']/20*100:.0f}%</td></tr>
            <tr><td>POI Congestion</td><td>{breakdown['poi_congestion']:.1f}</td><td>15</td><td>{breakdown['poi_congestion']/15*100:.0f}%</td></tr>
            <tr><td>Project Characteristics</td><td>{breakdown['project_characteristics']:.1f}</td><td>15</td><td>{breakdown['project_characteristics']/15*100:.0f}%</td></tr>
            <tr style="background:#fffbeb;font-weight:bold"><td>TOTAL</td><td>{score:.0f}</td><td>100</td><td>{score:.0f}%</td></tr>
        </table>
    </div>

    <div class="section">
        <div class="section-title">Risk Assessment</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;">
            <div>
                <strong>Red Flags</strong>
                {''.join(f'<div class="flag flag-red">{f}</div>' for f in score_result.get('red_flags', [])) or '<div class="flag flag-green">No critical risks identified</div>'}
            </div>
            <div>
                <strong>Green Flags</strong>
                {''.join(f'<div class="flag flag-green">{f}</div>' for f in score_result.get('green_flags', [])) or '<div class="flag">No notable positives</div>'}
            </div>
        </div>
    </div>

    <div class="context">
        <strong>Market Context:</strong> NYISO has the lowest project completion rate of any major RTO at 6.2% (vs 12.2% national average).
        Cost and timeline estimates based on 3,400+ historical interconnection studies from LBL Berkeley Lab.
    </div>

    <div class="footer">
        <p>CONFIDENTIAL - For authorized recipient only</p>
        <p>Generated by Queue Analysis Platform | Data sources: NYISO Queue, LBL Queued Up Dataset</p>
    </div>
</body>
</html>'''


# Page config
st.set_page_config(
    page_title="Interconnection Feasibility Assessment",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Bloomberg Terminal Style CSS
st.markdown("""
<style>
    /* Import monospace font */
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&display=swap');

    /* Global styles */
    * {
        font-family: 'JetBrains Mono', 'SF Mono', 'Fira Code', 'Consolas', monospace !important;
    }

    .stApp {
        background-color: #0a0a0f;
    }

    /* Main content area */
    .main .block-container {
        padding-top: 2rem;
        max-width: 100%;
    }

    /* Headers */
    .main-header {
        font-size: 1.8rem;
        font-weight: 700;
        color: #FF6B00;
        margin-bottom: 0;
        text-transform: uppercase;
        letter-spacing: 2px;
        border-bottom: 2px solid #FF6B00;
        padding-bottom: 10px;
    }
    .sub-header {
        font-size: 0.9rem;
        color: #00D4FF;
        margin-top: 5px;
        letter-spacing: 1px;
    }

    /* Section headers */
    h1, h2, h3 {
        color: #FF6B00 !important;
        border-bottom: 1px solid #2a2a3a;
        padding-bottom: 8px;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-size: 1.1rem !important;
    }

    /* Metrics styling */
    [data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
        font-weight: 700 !important;
        color: #00FF88 !important;
    }

    [data-testid="stMetricLabel"] {
        color: #888 !important;
        text-transform: uppercase;
        font-size: 0.7rem !important;
        letter-spacing: 1px;
    }

    [data-testid="stMetricDelta"] {
        font-size: 0.75rem !important;
    }

    /* Positive/negative deltas */
    [data-testid="stMetricDelta"] svg {
        display: none;
    }

    /* Cards and containers */
    .stExpander, .stDataFrame {
        border: 1px solid #2a2a3a !important;
        border-radius: 0 !important;
        background-color: #12121a !important;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #0d0d12 !important;
        border-right: 1px solid #FF6B00;
    }

    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 {
        color: #FF6B00 !important;
    }

    /* Input fields */
    .stTextInput input, .stSelectbox select {
        background-color: #1a1a2e !important;
        border: 1px solid #2a2a3a !important;
        color: #e0e0e0 !important;
        border-radius: 0 !important;
    }

    .stTextInput input:focus, .stSelectbox select:focus {
        border-color: #FF6B00 !important;
        box-shadow: 0 0 5px rgba(255, 107, 0, 0.3) !important;
    }

    /* Selectbox */
    [data-testid="stSelectbox"] > div > div {
        background-color: #1a1a2e !important;
        border: 1px solid #2a2a3a !important;
        border-radius: 0 !important;
    }

    /* Buttons */
    .stButton > button {
        background-color: #FF6B00 !important;
        color: #000 !important;
        border: none !important;
        border-radius: 0 !important;
        font-weight: 700 !important;
        text-transform: uppercase !important;
        letter-spacing: 1px !important;
    }

    .stButton > button:hover {
        background-color: #FF8533 !important;
        box-shadow: 0 0 10px rgba(255, 107, 0, 0.5) !important;
    }

    /* Download button */
    .stDownloadButton > button {
        background-color: #00D4FF !important;
        color: #000 !important;
    }

    /* Dataframes */
    .stDataFrame {
        border: 1px solid #2a2a3a !important;
    }

    .stDataFrame [data-testid="stDataFrameResizable"] {
        background-color: #12121a !important;
    }

    /* Tables */
    table {
        border-collapse: collapse !important;
        background-color: #12121a !important;
    }

    th {
        background-color: #1a1a2e !important;
        color: #FF6B00 !important;
        border: 1px solid #2a2a3a !important;
        text-transform: uppercase !important;
        font-size: 0.75rem !important;
        letter-spacing: 1px !important;
    }

    td {
        border: 1px solid #2a2a3a !important;
        color: #e0e0e0 !important;
    }

    /* Risk flags - terminal style */
    .red-flag {
        background: #1a0a0a;
        border-left: 3px solid #FF3333;
        padding: 8px 12px;
        margin: 4px 0;
        color: #FF6666;
        font-size: 0.85rem;
    }

    .green-flag {
        background: #0a1a0a;
        border-left: 3px solid #00FF88;
        padding: 8px 12px;
        margin: 4px 0;
        color: #66FF99;
        font-size: 0.85rem;
    }

    /* Status badges */
    .score-go {
        color: #00FF88;
        font-weight: bold;
        text-shadow: 0 0 10px rgba(0, 255, 136, 0.5);
    }
    .score-conditional {
        color: #FFD700;
        font-weight: bold;
        text-shadow: 0 0 10px rgba(255, 215, 0, 0.5);
    }
    .score-nogo {
        color: #FF3333;
        font-weight: bold;
        text-shadow: 0 0 10px rgba(255, 51, 51, 0.5);
    }

    /* Dividers */
    hr {
        border-color: #2a2a3a !important;
    }

    /* Checkboxes */
    .stCheckbox label {
        color: #888 !important;
    }

    .stCheckbox label:hover {
        color: #00D4FF !important;
    }

    /* Charts background */
    .js-plotly-plot .plotly {
        background-color: #12121a !important;
    }

    /* Info/Warning/Error boxes */
    .stAlert {
        background-color: #1a1a2e !important;
        border-radius: 0 !important;
        border-left: 3px solid #FF6B00 !important;
    }

    /* Success message */
    .stSuccess {
        background-color: #0a1a0a !important;
        border-left-color: #00FF88 !important;
    }

    /* Warning message */
    .stWarning {
        background-color: #1a1a0a !important;
        border-left-color: #FFD700 !important;
    }

    /* Bar chart colors */
    .stBarChart {
        background-color: #12121a !important;
    }

    /* Spinner */
    .stSpinner > div {
        border-top-color: #FF6B00 !important;
    }

    /* Caption */
    .stCaption {
        color: #555 !important;
        font-size: 0.7rem !important;
    }

    /* Bloomberg-style terminal header bar */
    .terminal-header {
        background: linear-gradient(90deg, #FF6B00 0%, #FF8533 100%);
        color: #000;
        padding: 8px 15px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 2px;
        margin-bottom: 15px;
        font-size: 0.85rem;
    }

    /* Data grid styling */
    .data-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 1px;
        background-color: #2a2a3a;
        border: 1px solid #2a2a3a;
    }

    .data-cell {
        background-color: #12121a;
        padding: 15px;
        text-align: center;
    }

    .data-cell-label {
        color: #666;
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 5px;
    }

    .data-cell-value {
        color: #00FF88;
        font-size: 1.5rem;
        font-weight: 700;
    }

    .data-cell-value.negative {
        color: #FF3333;
    }

    .data-cell-value.warning {
        color: #FFD700;
    }

    /* Verdict banner */
    .verdict-banner {
        background: linear-gradient(90deg, #12121a 0%, #1a1a2e 100%);
        border: 2px solid;
        padding: 20px;
        text-align: center;
        margin-bottom: 20px;
    }

    .verdict-go {
        border-color: #00FF88;
        box-shadow: 0 0 20px rgba(0, 255, 136, 0.2);
    }

    .verdict-conditional {
        border-color: #FFD700;
        box-shadow: 0 0 20px rgba(255, 215, 0, 0.2);
    }

    .verdict-nogo {
        border-color: #FF3333;
        box-shadow: 0 0 20px rgba(255, 51, 51, 0.2);
    }

    .verdict-text {
        font-size: 2rem;
        font-weight: 700;
        letter-spacing: 5px;
    }
</style>
""", unsafe_allow_html=True)

# Cost benchmarks ($/kW) from LBL/PJM/NYISO cost data audit (Jan 2026)
# Format: (P25_low, P50_median, P75_high)
# CRITICAL: Using MEDIAN values, not means (means are heavily skewed by outliers)
# Source: PJM (1,127 projects), NYISO (294 projects) cost studies
COST_BENCHMARKS = {
    'S': (30, 100, 250), 'Solar': (30, 100, 250),      # PJM median $82, NYISO median $125
    'W': (15, 60, 150), 'Wind': (15, 60, 150),         # PJM median $20, NYISO median $83
    'ES': (25, 60, 200), 'Storage': (25, 60, 200),     # PJM/NYISO median ~$60
    'NG': (5, 40, 100), 'Gas': (5, 40, 100),           # PJM median $9, NYISO median $73
    'L': (40, 120, 350), 'Load': (40, 120, 350),       # Limited data, estimated
    'AC': (40, 100, 250), 'DC': (40, 100, 250),        # Transmission projects
    'default': (30, 80, 200)                           # Overall median ~$60-80
}

# Note: Withdrawn projects have MUCH higher costs (median $150/kW, mean $500+/kW)
# This is a key risk indicator - high IC costs correlate with withdrawal

TIMELINE_BENCHMARKS = {
    'S': (24, 42, 72), 'Solar': (24, 42, 72),
    'W': (30, 54, 84), 'Wind': (30, 54, 84),
    'ES': (18, 36, 60), 'Storage': (18, 36, 60),
    'NG': (24, 48, 72), 'Gas': (24, 48, 72),
    'L': (24, 48, 84), 'Load': (24, 48, 84),
    'default': (30, 48, 72)
}

# Completion rates from LBL Berkeley Lab data audit (Jan 2026)
# Based on 36,441 projects - PROJECT COUNT weighted (not capacity)
# CRITICAL: These are real data, not estimates
COMPLETION_RATES = {
    'S': 0.086, 'Solar': 0.086,      # 8.6% (1,165 of 13,564 projects)
    'W': 0.174, 'Wind': 0.174,       # 17.4% (1,098 of 6,317 projects)
    'ES': 0.018, 'Storage': 0.018,   # 1.8% (99 of 5,536) - VERY LOW!
    'NG': 0.278, 'Gas': 0.278,       # 27.8% (924 of 3,326 projects)
    'L': 0.15, 'Load': 0.15,         # Estimated - limited data
    'default': 0.122                 # 12.2% overall (4,432 of 36,441)
}

# Regional completion rate adjustments
# NYISO has the LOWEST completion rate of all major RTOs!
REGIONAL_FACTORS = {
    'ISO-NE': 1.40,   # 17.1% completion (best)
    'ERCOT': 1.22,    # 14.9%
    'PJM': 1.20,      # 14.7%
    'SPP': 0.95,      # 11.6%
    'MISO': 0.81,     # 9.9%
    'CAISO': 0.66,    # 8.0%
    'NYISO': 0.51,    # 6.2% (LOWEST!)
}


@st.cache_data(ttl=3600)
def load_queue_data(region: str = 'NYISO'):
    """Load and cache queue data for specified region."""
    cache_dir = Path(__file__).parent / '.cache'

    if region == 'NYISO':
        loader = QueueData()
        df = loader.load_nyiso()
        return df, get_column_mapping('NYISO')

    elif region == 'CAISO':
        filepath = cache_dir / 'caiso_queue_direct.xlsx'
        if filepath.exists():
            df = pd.read_excel(filepath, sheet_name='Grid GenerationQueue', header=3)
            df = df.dropna(subset=['Project Name'])

            # Normalize column names to match NYISO format
            df = df.rename(columns={
                'Queue Position': 'Queue Pos.',
                'Fuel-1': 'Type/ Fuel',
                'Net MWs to Grid': 'SP (MW)',
                'Queue Date': 'Date of IR',
                'Station or Transmission Line': 'Points of Interconnection',
                'Current\nOn-line Date': 'Proposed COD',
            })

            return df, get_column_mapping('NYISO')
        return pd.DataFrame(), {}

    elif region == 'ERCOT':
        filepath = cache_dir / 'ercot_gis_report.xlsx'
        if filepath.exists():
            # Find header row
            df_raw = pd.read_excel(filepath, sheet_name='Project Details - Large Gen', header=None, nrows=50)
            header_row = None
            for i, row in df_raw.iterrows():
                row_str = ' '.join([str(v) for v in row.values if pd.notna(v)])
                if 'INR' in row_str and 'County' in row_str:
                    header_row = i
                    break
            if header_row:
                df = pd.read_excel(filepath, sheet_name='Project Details - Large Gen', header=header_row)
                df = df.dropna(how='all')
                df = df.reset_index(drop=True)  # Reset index after filtering

                # Normalize column names to match NYISO format
                df = df.rename(columns={
                    'INR': 'Queue Pos.',
                    'Interconnecting Entity': 'Developer/Interconnection Customer',
                    'Capacity (MW)': 'SP (MW)',
                    'Fuel': 'Type/ Fuel',
                    'Queue Date': 'Date of IR',
                    'GIM Study Phase': 'Availability of Studies',
                    'Projected COD': 'Proposed COD',
                    'POI Location': 'Points of Interconnection',
                })

                return df, get_column_mapping('NYISO')  # Use NYISO mapping since columns are normalized
        return pd.DataFrame(), {}

    elif region in ['MISO', 'SPP', 'ISO-NE']:
        # Load from LBL data (Berkeley Lab Queued Up)
        filepath = cache_dir / 'lbl_queued_up.xlsx'
        if filepath.exists():
            df = pd.read_excel(filepath, sheet_name='03. Complete Queue Data', header=1)
            # Filter to active projects in this region
            df = df[(df['region'] == region) & (df['q_status'] == 'active')].copy()
            df = df.reset_index(drop=True)  # Reset index after filtering

            # Convert Excel serial dates to proper datetime (with overflow protection)
            def safe_excel_to_datetime(series):
                """Convert Excel serial dates, handling edge cases."""
                def convert_val(val):
                    if pd.isna(val):
                        return pd.NaT
                    try:
                        # Only convert numeric values in valid Excel date range
                        num_val = float(val)
                        if 1 < num_val < 100000:  # Valid Excel date range
                            return pd.Timestamp('1899-12-30') + pd.Timedelta(days=num_val)
                        return pd.NaT
                    except (ValueError, TypeError, OverflowError):
                        # Try parsing as string date
                        try:
                            return pd.to_datetime(val, errors='coerce')
                        except:
                            return pd.NaT
                return series.apply(convert_val)

            for date_col in ['q_date', 'prop_date', 'on_date', 'wd_date', 'ia_date']:
                if date_col in df.columns:
                    df[date_col] = safe_excel_to_datetime(df[date_col])

            # Use q_id as project name fallback where project_name is missing
            if 'project_name' in df.columns and 'q_id' in df.columns:
                df['project_name'] = df['project_name'].fillna(df['q_id'])

            # Normalize column names to match NYISO format (what the UI expects)
            df = df.rename(columns={
                'q_id': 'Queue Pos.',
                'project_name': 'Project Name',
                'entity': 'Developer/Interconnection Customer',
                'mw1': 'SP (MW)',
                'type_clean': 'Type/ Fuel',
                'state': 'State',
                'county': 'County',
                'poi_name': 'Points of Interconnection',
                'q_date': 'Date of IR',
                'IA_status_clean': 'Availability of Studies',
                'prop_date': 'Proposed COD',
                'utility': 'Utility',
            })

            return df, get_column_mapping('NYISO')  # Use NYISO mapping since columns are normalized
        return pd.DataFrame(), {}

    else:
        # Fallback to NYISO
        loader = QueueData()
        df = loader.load_nyiso()
        return df, get_column_mapping('NYISO')


def get_column_mapping(region: str) -> dict:
    """Get column name mapping for each ISO."""
    mappings = {
        'NYISO': {
            'queue_id': 'Queue Pos.',
            'name': 'Project Name',
            'developer': 'Developer/Interconnection Customer',
            'capacity': 'SP (MW)',
            'type': 'Type/ Fuel',
            'state': 'State',
            'county': 'County',
            'poi': 'Points of Interconnection',
            'queue_date': 'Date of IR',
            'study_phase': 'Availability of Studies',
            'proposed_cod': 'Proposed COD',
            'utility': 'Utility',
        },
        'CAISO': {
            'queue_id': 'Queue Position',
            'name': 'Project Name',
            'developer': None,  # Not available
            'capacity': 'Net MWs to Grid',
            'type': 'Fuel-1',
            'state': 'State',
            'county': 'County',
            'poi': 'Station or Transmission Line',
            'queue_date': 'Queue Date',
            'study_phase': 'Study\nProcess',
            'proposed_cod': 'Current\nOn-line Date',
            'utility': 'Utility',
        },
        'ERCOT': {
            'queue_id': 'INR',
            'name': 'Project Name',
            'developer': 'Interconnecting Entity',
            'capacity': 'Capacity (MW)',
            'type': 'Fuel',
            'state': 'State',
            'county': 'County',
            'poi': 'POI Location',
            'queue_date': 'Queue Date',
            'study_phase': 'GIM Study Phase',
            'proposed_cod': 'Projected COD',
            'utility': None,
        },
        'LBL': {
            # LBL Queued Up data (used for MISO, SPP, ISO-NE)
            'queue_id': 'q_id',
            'name': 'project_name',
            'developer': 'entity',
            'capacity': 'mw1',
            'type': 'type_clean',
            'state': 'state',
            'county': 'county',
            'poi': 'poi_name',
            'queue_date': 'q_date',
            'study_phase': 'IA_status_clean',
            'proposed_cod': 'prop_date',
            'utility': 'utility',
        },
    }
    return mappings.get(region, mappings['NYISO'])


@st.cache_data(ttl=3600)
def load_historical_costs():
    """Load historical cost data."""
    cache_dir = Path(__file__).parent / '.cache'
    costs = []

    files = {
        'NYISO': 'nyiso_interconnection_cost_data.xlsx',
        'PJM': 'pjm_costs_2022_clean_data.xlsx',
        'MISO': 'miso_costs_2021_clean_data.xlsx',
        'SPP': 'spp_costs_2023_clean_data.xlsx',
        'ISO-NE': 'isone_interconnection_cost_data.xlsx'
    }

    for iso, filename in files.items():
        filepath = cache_dir / filename
        if filepath.exists():
            try:
                df = pd.read_excel(filepath, sheet_name='data')
                df['ISO'] = iso
                costs.append(df)
            except:
                pass

    if costs:
        return pd.concat(costs, ignore_index=True)
    return pd.DataFrame()


def estimate_costs(capacity_mw, project_type, region='NYISO'):
    """Estimate interconnection costs."""
    base = COST_BENCHMARKS.get(project_type, COST_BENCHMARKS['default'])
    multiplier = 1.3 if region == 'NYISO' else 1.0

    # Size adjustment
    if capacity_mw and not pd.isna(capacity_mw):
        if capacity_mw > 500:
            multiplier *= 1.3
        elif capacity_mw < 50:
            multiplier *= 1.2

    low = base[0] * multiplier
    med = base[1] * multiplier
    high = base[2] * multiplier

    cap = capacity_mw if capacity_mw and not pd.isna(capacity_mw) else 100

    return {
        'low_per_kw': low,
        'med_per_kw': med,
        'high_per_kw': high,
        'low_total': (low * cap * 1000) / 1_000_000,
        'med_total': (med * cap * 1000) / 1_000_000,
        'high_total': (high * cap * 1000) / 1_000_000
    }


def estimate_timeline(project_type, months_in_queue=0, study_progress=None):
    """
    Estimate timeline to COD based on study progress.

    Study progress score (0-25) maps to remaining time:
    - 20-25: IA signed/advanced → 6-18 months
    - 15-20: Facilities study → 12-30 months
    - 10-15: System impact study → 24-48 months
    - 5-10:  Feasibility study → 36-60 months
    - 0-5:   Early phase → 48-72 months
    """
    completion_rate = COMPLETION_RATES.get(project_type, COMPLETION_RATES['default'])

    # Determine remaining time based on study progress score
    if study_progress is not None:
        if study_progress >= 20:
            # IA signed or very advanced - close to COD
            remaining = (6, 12, 18)
        elif study_progress >= 15:
            # Facilities study phase
            remaining = (12, 24, 36)
        elif study_progress >= 10:
            # System impact study phase
            remaining = (24, 36, 48)
        elif study_progress >= 5:
            # Feasibility study phase
            remaining = (36, 48, 60)
        else:
            # Early phase or unknown
            remaining = (48, 60, 72)
    else:
        # Fallback to old logic if no study progress provided
        base = TIMELINE_BENCHMARKS.get(project_type, TIMELINE_BENCHMARKS['default'])
        remaining = (
            max(6, base[0] - months_in_queue),
            max(12, base[1] - months_in_queue),
            max(18, base[2] - months_in_queue)
        )

    now = datetime.now()

    def add_months(months):
        year = now.year + (now.month + months - 1) // 12
        month = (now.month + months - 1) % 12 + 1
        return f"Q{(month-1)//3 + 1} {year}"

    return {
        'optimistic': remaining[0],
        'likely': remaining[1],
        'pessimistic': remaining[2],
        'optimistic_date': add_months(remaining[0]),
        'likely_date': add_months(remaining[1]),
        'pessimistic_date': add_months(remaining[2]),
        'completion_rate': completion_rate,
        'study_based': study_progress is not None
    }


# =============================================================================
# CHART FUNCTIONS
# =============================================================================

# Bloomberg terminal color scheme
BLOOMBERG_COLORS = {
    'bg': '#0a0a0f',
    'bg_secondary': '#12121a',
    'grid': '#2a2a3a',
    'text': '#e0e0e0',
    'text_dim': '#666666',
    'orange': '#FF6B00',
    'green': '#00FF88',
    'red': '#FF3333',
    'yellow': '#FFD700',
    'cyan': '#00D4FF',
    'blue': '#3366FF',
}


def get_bloomberg_layout(title=None):
    """Return common Bloomberg-style layout settings."""
    layout = dict(
        paper_bgcolor=BLOOMBERG_COLORS['bg_secondary'],
        plot_bgcolor=BLOOMBERG_COLORS['bg_secondary'],
        font=dict(family='JetBrains Mono, monospace', color=BLOOMBERG_COLORS['text'], size=11),
        xaxis=dict(
            gridcolor=BLOOMBERG_COLORS['grid'],
            linecolor=BLOOMBERG_COLORS['grid'],
            tickfont=dict(color=BLOOMBERG_COLORS['text_dim']),
            title_font=dict(color=BLOOMBERG_COLORS['text_dim'])
        ),
        yaxis=dict(
            gridcolor=BLOOMBERG_COLORS['grid'],
            linecolor=BLOOMBERG_COLORS['grid'],
            tickfont=dict(color=BLOOMBERG_COLORS['text_dim']),
            title_font=dict(color=BLOOMBERG_COLORS['text_dim'])
        ),
        legend=dict(
            bgcolor='rgba(0,0,0,0)',
            font=dict(color=BLOOMBERG_COLORS['text_dim'])
        ),
        margin=dict(l=40, r=40, t=50, b=40)
    )
    if title:
        layout['title'] = dict(text=title, font=dict(color=BLOOMBERG_COLORS['orange'], size=14))
    return layout


def create_cost_scatter(historical_costs, project_cost_per_kw, project_type, capacity):
    """Create scatter plot of historical IC costs vs this project."""
    if historical_costs.empty:
        return None

    # Find cost column
    cost_col = None
    for col in historical_costs.columns:
        if 'total' in col.lower() and 'cost' in col.lower() and 'kw' in col.lower():
            cost_col = col
            break

    if not cost_col:
        return None

    # Filter valid data
    df = historical_costs[historical_costs[cost_col].notna() & (historical_costs[cost_col] > 0)].copy()
    if len(df) < 10:
        return None

    # Cap outliers for visualization
    df['cost_display'] = df[cost_col].clip(upper=500)

    # Find capacity column
    cap_col = None
    for col in df.columns:
        if 'nameplate' in col.lower() or ('mw' in col.lower() and 'cap' not in col.lower()):
            cap_col = col
            break

    if cap_col:
        df['capacity'] = pd.to_numeric(df[cap_col], errors='coerce').fillna(100)
    else:
        df['capacity'] = 100

    fig = go.Figure()

    # Historical projects
    fig.add_trace(go.Scatter(
        x=df['capacity'],
        y=df['cost_display'],
        mode='markers',
        marker=dict(size=8, color=BLOOMBERG_COLORS['cyan'], opacity=0.6,
                    line=dict(width=1, color=BLOOMBERG_COLORS['blue'])),
        name='Historical Projects',
        hovertemplate='%{x:.0f} MW<br>$%{y:.0f}/kW<extra></extra>'
    ))

    # This project
    fig.add_trace(go.Scatter(
        x=[capacity],
        y=[project_cost_per_kw],
        mode='markers',
        marker=dict(size=18, color=BLOOMBERG_COLORS['orange'], symbol='star',
                    line=dict(width=2, color='#FF8533')),
        name='This Project',
        hovertemplate=f'{capacity:.0f} MW<br>${project_cost_per_kw:.0f}/kW<extra></extra>'
    ))

    # Add percentile lines
    p25 = df[cost_col].quantile(0.25)
    p50 = df[cost_col].quantile(0.50)
    p75 = df[cost_col].quantile(0.75)

    fig.add_hline(y=p50, line_dash="dash", line_color=BLOOMBERG_COLORS['green'],
                  annotation_text=f"Median: ${p50:.0f}/kW",
                  annotation_font_color=BLOOMBERG_COLORS['green'])
    fig.add_hrect(y0=p25, y1=p75, fillcolor=BLOOMBERG_COLORS['green'], opacity=0.1, line_width=0)

    fig.update_layout(
        **get_bloomberg_layout(title="INTERCONNECTION COST COMPARISON"),
        xaxis_title="PROJECT CAPACITY (MW)",
        yaxis_title="IC COST ($/KW)",
        height=400,
        showlegend=True,
    )

    return fig


def create_completion_funnel(project_type):
    """Create funnel chart showing queue completion rates."""
    # Data from LBL audit (Jan 2026) - 36,441 projects analyzed
    # These are ACTUAL completion rates by project count
    funnel_data = {
        'S': {'Entered Queue': 100, 'Completed Studies': 25, 'Signed IA': 12, 'Reached COD': 9},   # 8.6% actual
        'W': {'Entered Queue': 100, 'Completed Studies': 40, 'Signed IA': 25, 'Reached COD': 17},  # 17.4% actual
        'ES': {'Entered Queue': 100, 'Completed Studies': 15, 'Signed IA': 5, 'Reached COD': 2},   # 1.8% actual!
        'NG': {'Entered Queue': 100, 'Completed Studies': 55, 'Signed IA': 40, 'Reached COD': 28}, # 27.8% actual
        'L': {'Entered Queue': 100, 'Completed Studies': 45, 'Signed IA': 25, 'Reached COD': 15},  # Estimated
    }

    data = funnel_data.get(project_type, funnel_data['S'])

    fig = go.Figure(go.Funnel(
        y=list(data.keys()),
        x=list(data.values()),
        textposition="inside",
        textinfo="value+percent initial",
        textfont=dict(color='white', size=12),
        marker=dict(color=[BLOOMBERG_COLORS['cyan'], BLOOMBERG_COLORS['blue'],
                          BLOOMBERG_COLORS['yellow'], BLOOMBERG_COLORS['green']]),
        connector=dict(line=dict(color=BLOOMBERG_COLORS['orange'], dash="dot", width=2))
    ))

    fig.update_layout(
        **get_bloomberg_layout(title=f"QUEUE COMPLETION FUNNEL ({project_type})"),
        height=350,
    )

    return fig


def create_timeline_boxplot(project_type, estimated_months):
    """Create boxplot showing timeline distribution."""
    # Historical timeline data by type
    timeline_data = {
        'S': {'min': 18, 'q1': 30, 'median': 42, 'q3': 60, 'max': 84},
        'W': {'min': 24, 'q1': 40, 'median': 54, 'q3': 72, 'max': 96},
        'ES': {'min': 12, 'q1': 24, 'median': 36, 'q3': 48, 'max': 72},
        'NG': {'min': 18, 'q1': 30, 'median': 48, 'q3': 60, 'max': 84},
        'L': {'min': 18, 'q1': 36, 'median': 48, 'q3': 72, 'max': 96},
    }

    data = timeline_data.get(project_type, timeline_data['S'])

    fig = go.Figure()

    # Box plot for historical
    fig.add_trace(go.Box(
        y=[data['min'], data['q1'], data['median'], data['q3'], data['max']],
        name='Historical',
        boxpoints=False,
        marker_color=BLOOMBERG_COLORS['cyan'],
        line_color=BLOOMBERG_COLORS['blue'],
        fillcolor='rgba(0, 212, 255, 0.3)'
    ))

    # This project's estimate
    fig.add_trace(go.Scatter(
        x=['Historical'],
        y=[estimated_months],
        mode='markers',
        marker=dict(size=15, color=BLOOMBERG_COLORS['orange'], symbol='diamond',
                    line=dict(width=2, color='#FF8533')),
        name=f'This Project ({estimated_months} mo)'
    ))

    fig.update_layout(
        **get_bloomberg_layout(title="TIME TO COD (MONTHS)"),
        yaxis_title="MONTHS FROM QUEUE ENTRY",
        height=300,
        showlegend=True
    )

    return fig


def create_risk_radar(breakdown):
    """Create radar chart of risk components."""
    categories = ['Queue Position', 'Study Progress', 'Developer', 'POI Congestion', 'Project Type']
    max_scores = [25, 25, 20, 15, 15]

    scores = [
        breakdown['queue_position'],
        breakdown['study_progress'],
        breakdown['developer_track_record'],
        breakdown['poi_congestion'],
        breakdown['project_characteristics']
    ]

    # Normalize to percentages
    percentages = [s/m * 100 for s, m in zip(scores, max_scores)]

    fig = go.Figure()

    fig.add_trace(go.Scatterpolar(
        r=percentages + [percentages[0]],  # Close the shape
        theta=categories + [categories[0]],
        fill='toself',
        fillcolor='rgba(255, 107, 0, 0.3)',
        line=dict(color=BLOOMBERG_COLORS['orange'], width=2),
        name='This Project'
    ))

    # Add 100% reference
    fig.add_trace(go.Scatterpolar(
        r=[100, 100, 100, 100, 100, 100],
        theta=categories + [categories[0]],
        fill='none',
        line=dict(color=BLOOMBERG_COLORS['grid'], width=1, dash='dash'),
        name='Maximum'
    ))

    fig.update_layout(
        **get_bloomberg_layout(title="RISK PROFILE RADAR"),
        polar=dict(
            bgcolor=BLOOMBERG_COLORS['bg_secondary'],
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                gridcolor=BLOOMBERG_COLORS['grid'],
                linecolor=BLOOMBERG_COLORS['grid'],
                tickfont=dict(color=BLOOMBERG_COLORS['text_dim'])
            ),
            angularaxis=dict(
                gridcolor=BLOOMBERG_COLORS['grid'],
                linecolor=BLOOMBERG_COLORS['grid'],
                tickfont=dict(color=BLOOMBERG_COLORS['text'])
            )
        ),
        height=400,
        showlegend=False
    )

    return fig


def create_developer_comparison():
    """Create bar chart comparing developer type outcomes."""
    dev_data = pd.DataFrame({
        'Developer Type': ['Hyperscaler', 'Major Utility', 'Experienced IPP', 'Single-Project SPV', 'Unknown'],
        'Completion Rate': [65, 55, 40, 22, 18],
        'Avg Time to COD': [36, 42, 48, 54, 60]
    })

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Bar(
            x=dev_data['Developer Type'],
            y=dev_data['Completion Rate'],
            name='Completion Rate (%)',
            marker_color=BLOOMBERG_COLORS['cyan'],
            marker_line_color=BLOOMBERG_COLORS['blue'],
            marker_line_width=1
        ),
        secondary_y=False
    )

    fig.add_trace(
        go.Scatter(
            x=dev_data['Developer Type'],
            y=dev_data['Avg Time to COD'],
            name='Avg Time to COD (mo)',
            mode='lines+markers',
            marker=dict(size=10, color=BLOOMBERG_COLORS['orange']),
            line=dict(color=BLOOMBERG_COLORS['orange'], width=2)
        ),
        secondary_y=True
    )

    fig.update_layout(
        **get_bloomberg_layout(title="OUTCOMES BY DEVELOPER TYPE"),
        height=350,
    )
    fig.update_yaxes(title_text="COMPLETION RATE (%)", secondary_y=False,
                     gridcolor=BLOOMBERG_COLORS['grid'], tickfont=dict(color=BLOOMBERG_COLORS['text_dim']))
    fig.update_yaxes(title_text="AVG MONTHS TO COD", secondary_y=True,
                     gridcolor=BLOOMBERG_COLORS['grid'], tickfont=dict(color=BLOOMBERG_COLORS['text_dim']))

    return fig


def main():
    # Header
    st.markdown('<p class="main-header">⚡ Interconnection Feasibility Assessment</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Queue Analysis & Due Diligence Platform</p>', unsafe_allow_html=True)
    st.divider()

    # ISO/Region selector at the top of sidebar
    st.sidebar.header("🌐 Market Selection")
    available_regions = ['NYISO', 'CAISO', 'ERCOT', 'MISO', 'SPP', 'ISO-NE']
    selected_region = st.sidebar.selectbox(
        "Select ISO/RTO",
        available_regions,
        index=0,
        help="Select the grid market to analyze. MISO/SPP/ISO-NE use LBL data."
    )

    # Show regional context
    benchmarks = RegionalBenchmarks()
    regional_completion = benchmarks.get_completion_rate(selected_region)
    regional_timeline = benchmarks.get_median_timeline(selected_region)

    st.sidebar.caption(f"📊 {selected_region} completion rate: {regional_completion*100:.1f}%")
    st.sidebar.caption(f"⏱️ Median timeline: {regional_timeline} months")
    st.sidebar.divider()

    # Load data for selected region
    with st.spinner(f"Loading {selected_region} queue data..."):
        df, col_map = load_queue_data(selected_region)
        historical_costs = load_historical_costs()

    if df.empty:
        st.error(f"Failed to load {selected_region} queue data. Please check your connection.")
        return

    # Store column mapping in session state for use throughout app
    st.session_state['col_map'] = col_map
    st.session_state['region'] = selected_region

    # Helper: Spell out project types
    def spell_out_type(type_code):
        """Convert type codes to full names."""
        type_map = {
            'S': 'Solar',
            'W': 'Wind',
            'ES': 'Battery Storage',
            'B': 'Battery Storage',
            'BESS': 'Battery Storage',
            'NG': 'Natural Gas',
            'Gas': 'Natural Gas',
            'L': 'Load (Data Center)',
            'AC': 'AC Transmission',
            'DC': 'DC Transmission',
            'H': 'Hydro',
            'N': 'Nuclear',
        }
        if pd.isna(type_code):
            return 'Unknown'
        type_str = str(type_code).strip()
        return type_map.get(type_str, type_str)

    # Helper: Extract year from date
    def get_queue_year(date_val):
        if pd.isna(date_val):
            return None
        try:
            return pd.to_datetime(date_val).year
        except:
            return None

    # Add queue year column
    df['_queue_year'] = df['Date of IR'].apply(get_queue_year)

    # Saved Reports Browser
    if REPORT_MANAGER_AVAILABLE:
        st.sidebar.header("📁 Saved Reports")
        rm = ReportManager()
        saved_reports = rm.list_reports(report_type='project', limit=5)

        if saved_reports:
            for r in saved_reports:
                rec_badge = "🟢" if "GO" in r.get('title', '') else "🟡" if "CONDITIONAL" in r.get('title', '') else "🔴"
                with st.sidebar.expander(f"{rec_badge} {r['title'][:35]}..."):
                    st.caption(f"Client: {r['client']}")
                    st.caption(f"Created: {r['created_at'][:10]}")
                    if r.get('files'):
                        for f in r['files']:
                            file_path = Path(r['report_dir']) / f
                            if file_path.exists():
                                with open(file_path, 'rb') as fp:
                                    st.download_button(
                                        f"📥 {f}",
                                        fp.read(),
                                        file_name=f,
                                        key=f"app_{r['report_id']}_{f}"
                                    )
        else:
            st.sidebar.caption("No saved reports yet")

        st.sidebar.divider()

    # Sidebar - Project Selection
    st.sidebar.header("🎯 Project Selection")

    # Client name
    client_name = st.sidebar.text_input("Client Name", value="PE Fund")

    # Filter options
    st.sidebar.subheader("Filters")

    # Queue Year Filter (NEW)
    year_options = ["All Years", "2024+", "2023+", "2022+", "2020+", "Before 2020"]
    selected_year_filter = st.sidebar.selectbox("Queue Year", year_options, index=1)  # Default to 2023+

    # Project Type Filter
    type_col = 'Type/ Fuel'
    types = ['All Types'] + sorted(df[type_col].dropna().unique().tolist())
    selected_type = st.sidebar.selectbox("Project Type", types)

    # Show only active projects checkbox
    show_active_only = st.sidebar.checkbox("Show only active projects", value=True)

    # Search filter
    search_term = st.sidebar.text_input("🔍 Search (name or developer)", "")

    # Apply filters to dataframe
    filtered_df = df.copy()

    # Year filter
    current_year = datetime.now().year
    if selected_year_filter == "2024+":
        filtered_df = filtered_df[filtered_df['_queue_year'] >= 2024]
    elif selected_year_filter == "2023+":
        filtered_df = filtered_df[filtered_df['_queue_year'] >= 2023]
    elif selected_year_filter == "2022+":
        filtered_df = filtered_df[filtered_df['_queue_year'] >= 2022]
    elif selected_year_filter == "2020+":
        filtered_df = filtered_df[filtered_df['_queue_year'] >= 2020]
    elif selected_year_filter == "Before 2020":
        filtered_df = filtered_df[filtered_df['_queue_year'] < 2020]

    # Type filter
    if selected_type != 'All Types':
        filtered_df = filtered_df[filtered_df[type_col] == selected_type]

    # Search filter
    if search_term:
        search_lower = search_term.lower()
        name_match = filtered_df['Project Name'].astype(str).str.lower().str.contains(search_lower, na=False)
        dev_match = filtered_df['Developer/Interconnection Customer'].astype(str).str.lower().str.contains(search_lower, na=False)
        filtered_df = filtered_df[name_match | dev_match]

    # Active projects filter
    if show_active_only:
        # Check for status-related columns in NYISO data
        status_cols = ['Status', 'Project Status', 'S', 'Availability of Studies']
        status_col_found = None
        for col in status_cols:
            if col in filtered_df.columns:
                status_col_found = col
                break

        if status_col_found:
            # Filter out withdrawn/completed projects
            inactive_keywords = ['withdrawn', 'cancelled', 'canceled', 'completed', 'operational', 'in service']
            status_lower = filtered_df[status_col_found].astype(str).str.lower()
            is_inactive = status_lower.apply(lambda x: any(kw in x for kw in inactive_keywords))
            filtered_df = filtered_df[~is_inactive]

    # Sort by queue date (NEWEST FIRST)
    filtered_df = filtered_df.sort_values('Date of IR', ascending=False, na_position='last')

    # Project selector with improved format
    def format_project_option(r):
        queue_pos = r['Queue Pos.']
        if pd.isna(queue_pos):
            queue_pos = "N/A"
        elif isinstance(queue_pos, float):
            queue_pos = str(int(queue_pos))
        else:
            queue_pos = str(queue_pos)

        name = r['Project Name']
        if pd.isna(name):
            name = "Unknown"
        else:
            name = str(name)[:35]

        year = r['_queue_year']
        year_str = f"[{int(year)}]" if pd.notna(year) else "[????]"

        # Age indicator
        if pd.notna(year):
            age = current_year - int(year)
            if age <= 2:
                indicator = "🟢"  # Recent
            elif age <= 5:
                indicator = "🟡"  # Moderate
            else:
                indicator = "🔴"  # Old/stale
        else:
            indicator = "⚪"

        return f"{indicator} {year_str} {queue_pos} - {name}"

    if len(filtered_df) == 0:
        st.warning("No projects match the selected filters. Try adjusting the year range or search term.")
        return

    project_options = filtered_df.apply(format_project_option, axis=1).tolist()

    selected_idx = st.sidebar.selectbox(
        "Select Project",
        range(len(project_options)),
        format_func=lambda x: project_options[x]
    )

    # Show filter summary
    st.sidebar.caption(f"Showing {len(filtered_df)} projects")

    # Get selected project
    project = filtered_df.iloc[selected_idx]
    queue_pos = project['Queue Pos.']
    if pd.isna(queue_pos):
        project_id = "0000"
    elif isinstance(queue_pos, float):
        project_id = str(int(queue_pos)).zfill(4)
    else:
        project_id = str(queue_pos)

    # Calculate queue age for warnings
    queue_date = project['Date of IR']
    months_in_queue = 0
    queue_year = None
    if pd.notna(queue_date):
        try:
            qd = pd.to_datetime(queue_date)
            months_in_queue = (datetime.now() - qd).days // 30
            queue_year = qd.year
        except:
            pass

    # Score the project
    scorer = FeasibilityScorer(df)
    score_result = scorer.score_project(project_id=project_id)

    if 'error' in score_result:
        st.error(f"Error scoring project: {score_result['error']}")
        return

    # Extract data
    proj = score_result['project']
    breakdown = score_result['breakdown']

    # STALE PROJECT WARNING
    is_stale = months_in_queue > 60 and breakdown['study_progress'] < 12.5  # 5+ years and < 50% study progress
    is_old = months_in_queue > 60

    if is_stale:
        st.error(f"""
        ⚠️ **STALE PROJECT WARNING**

        This project entered the queue in **{queue_year}** ({months_in_queue // 12} years ago) with limited study progress.
        Projects this old with low advancement are often stalled or abandoned.

        **Recommendation:** Verify current status directly with NYISO before proceeding with analysis.
        """)
    elif is_old:
        st.warning(f"""
        ⚠️ **Old Project Notice**

        This project entered the queue in **{queue_year}** ({months_in_queue // 12} years ago).
        Timeline and cost estimates may not be reliable for projects with extended queue times.
        """)

    # Calculate estimates - safely handle NaN values
    sp_mw = project['SP (MW)']
    capacity = float(sp_mw) if pd.notna(sp_mw) else 100.0

    proj_type_val = project[type_col]
    project_type = str(proj_type_val) if pd.notna(proj_type_val) else 'default'
    project_type_full = spell_out_type(proj_type_val)  # Full name for display

    costs = estimate_costs(capacity, project_type)

    # Pass study_progress score to get study-phase-based timeline estimates
    study_progress_score = breakdown['study_progress']
    timeline = estimate_timeline(project_type, months_in_queue, study_progress=study_progress_score)

    # Add stale warning flag for display purposes
    timeline['stale_warning'] = is_stale

    # ==================== MAIN CONTENT ====================

    # Executive Summary
    st.header("📋 Executive Summary")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        rec = score_result['recommendation']
        rec_color = {'GO': '🟢', 'CONDITIONAL': '🟡', 'NO-GO': '🔴'}.get(rec, '⚪')
        st.metric("Recommendation", f"{rec_color} {rec}")

    with col2:
        st.metric("Feasibility Score", f"{score_result['total_score']:.0f}/100",
                  delta=f"Grade {score_result['grade']}")

    with col3:
        st.metric("Estimated Cost", f"${costs['med_total']:.0f}M",
                  delta=f"${costs['low_total']:.0f}M - ${costs['high_total']:.0f}M")

    with col4:
        st.metric("Est. COD", timeline['likely_date'],
                  delta=f"{timeline['likely']} months")

    st.divider()

    # Project Overview
    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("📁 Project Overview")

        overview_data = {
            "Queue ID": project_id,
            "Project Name": project['Project Name'],
            "Developer": project['Developer/Interconnection Customer'],
            "Type": project_type,
            "Capacity (MW)": f"{capacity:,.0f}" if pd.notna(capacity) else "N/A",
            "County, State": f"{project['County']}, {project['State']}",
            "POI": project['Points of Interconnection'],
            "Queue Date": str(queue_date)[:10] if pd.notna(queue_date) else "N/A",
            "Months in Queue": months_in_queue,
            "Studies Available": project['Availability of Studies'] if pd.notna(project['Availability of Studies']) else "None listed"
        }

        for k, v in overview_data.items():
            st.markdown(f"**{k}:** {v}")

    with col2:
        st.subheader("📊 Score Breakdown")

        # Score chart
        score_data = pd.DataFrame({
            'Component': ['Queue Position', 'Study Progress', 'Developer', 'POI Congestion', 'Project Type'],
            'Score': [
                breakdown['queue_position'],
                breakdown['study_progress'],
                breakdown['developer_track_record'],
                breakdown['poi_congestion'],
                breakdown['project_characteristics']
            ],
            'Max': [25, 25, 20, 15, 15]
        })
        score_data['Percentage'] = score_data['Score'] / score_data['Max'] * 100

        # Create Plotly bar chart with Bloomberg styling
        score_fig = go.Figure()

        # Color bars based on percentage
        bar_colors = [
            BLOOMBERG_COLORS['green'] if p >= 70 else BLOOMBERG_COLORS['yellow'] if p >= 40 else BLOOMBERG_COLORS['red']
            for p in score_data['Percentage']
        ]

        score_fig.add_trace(go.Bar(
            x=score_data['Component'],
            y=score_data['Score'],
            marker_color=bar_colors,
            marker_line_color=BLOOMBERG_COLORS['grid'],
            marker_line_width=1,
            text=[f"{s:.1f}" for s in score_data['Score']],
            textposition='outside',
            textfont=dict(color=BLOOMBERG_COLORS['text'])
        ))

        score_fig.update_layout(
            paper_bgcolor=BLOOMBERG_COLORS['bg_secondary'],
            plot_bgcolor=BLOOMBERG_COLORS['bg_secondary'],
            font=dict(family='JetBrains Mono, monospace', color=BLOOMBERG_COLORS['text'], size=11),
            height=300,
            showlegend=False,
            xaxis=dict(
                tickangle=-45,
                tickfont=dict(size=10, color=BLOOMBERG_COLORS['text_dim']),
                gridcolor=BLOOMBERG_COLORS['grid'],
                linecolor=BLOOMBERG_COLORS['grid']
            ),
            yaxis=dict(
                range=[0, 30],
                gridcolor=BLOOMBERG_COLORS['grid'],
                linecolor=BLOOMBERG_COLORS['grid'],
                tickfont=dict(color=BLOOMBERG_COLORS['text_dim'])
            ),
            margin=dict(l=40, r=20, t=20, b=80)
        )

        st.plotly_chart(score_fig, use_container_width=True)

        # Detailed breakdown
        for _, row in score_data.iterrows():
            pct = row['Percentage']
            color = '🟢' if pct >= 70 else '🟡' if pct >= 40 else '🔴'
            st.markdown(f"{color} **{row['Component']}:** {row['Score']:.1f}/{row['Max']} ({pct:.0f}%)")

    st.divider()

    # Cost & Timeline Analysis
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("💰 Cost Analysis")

        cost_df = pd.DataFrame({
            'Scenario': ['Low', 'Median', 'High'],
            'Total ($M)': [costs['low_total'], costs['med_total'], costs['high_total']],
            '$/kW': [costs['low_per_kw'], costs['med_per_kw'], costs['high_per_kw']]
        })

        st.dataframe(cost_df, use_container_width=True, hide_index=True)

        st.markdown(f"""
        **Cost Drivers:**
        - Project Type: {project_type}
        - Capacity: {capacity:,.0f} MW
        - Region: NYISO (1.3x multiplier)
        """)

        if capacity and capacity > 500:
            st.warning("⚠️ Large project likely to trigger significant network upgrades")

    with col2:
        st.subheader("📅 Timeline Analysis")

        timeline_df = pd.DataFrame({
            'Scenario': ['Optimistic', 'Likely', 'Pessimistic'],
            'Months': [timeline['optimistic'], timeline['likely'], timeline['pessimistic']],
            'Target Date': [timeline['optimistic_date'], timeline['likely_date'], timeline['pessimistic_date']]
        })

        st.dataframe(timeline_df, use_container_width=True, hide_index=True)

        st.metric("Historical Completion Rate", f"{timeline['completion_rate']*100:.0f}%",
                  help="Percentage of similar projects that reach COD")

    st.divider()

    # Risk Assessment
    st.subheader("⚠️ Risk Assessment")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Red Flags**")
        if score_result['red_flags']:
            for flag in score_result['red_flags']:
                st.markdown(f'<div class="red-flag">🚩 {flag}</div>', unsafe_allow_html=True)
        else:
            st.success("No critical red flags identified")

    with col2:
        st.markdown("**Green Flags**")
        if score_result['green_flags']:
            for flag in score_result['green_flags']:
                st.markdown(f'<div class="green-flag">✅ {flag}</div>', unsafe_allow_html=True)
        else:
            st.info("No notable strengths identified")

    # Risk Matrix
    st.markdown("**Risk Matrix**")

    risk_matrix = pd.DataFrame({
        'Category': ['Technical', 'Cost', 'Timeline', 'Developer', 'Queue/POI'],
        'Level': [
            'High' if breakdown['study_progress'] < 10 else 'Medium' if breakdown['study_progress'] < 18 else 'Low',
            'High' if capacity and capacity > 500 else 'Medium',
            'High' if breakdown['study_progress'] < 10 else 'Medium' if breakdown['study_progress'] < 18 else 'Low',
            'High' if breakdown['developer_track_record'] < 8 else 'Medium' if breakdown['developer_track_record'] < 14 else 'Low',
            'Low' if breakdown['poi_congestion'] > 12 else 'Medium' if breakdown['poi_congestion'] > 6 else 'High'
        ],
        'Driver': [
            'Study progress',
            'Project size/complexity',
            'Study phase',
            'Developer track record',
            'POI competition'
        ]
    })

    st.dataframe(risk_matrix, use_container_width=True, hide_index=True)

    st.divider()

    # Due Diligence Checklist
    st.subheader("📝 Due Diligence Checklist")

    checklist_items = [
        "Obtain and review interconnection study documents",
        "Validate cost estimate against actual study documents",
        "Confirm current study phase with NYISO",
        "Research developer ownership/backing",
        "Verify developer financial capability for IC costs",
        "Review transmission constraints in POI area",
        "Assess regulatory/permitting status",
        "Check for any affected system studies"
    ]

    for item in checklist_items:
        st.checkbox(item, key=item)

    st.divider()

    # ==========================================================================
    # VISUAL ANALYTICS SECTION
    # ==========================================================================
    st.header("📊 Visual Analytics")

    # Row 1: Cost Scatter + Completion Funnel
    col1, col2 = st.columns(2)

    with col1:
        cost_scatter = create_cost_scatter(
            historical_costs,
            costs['med_per_kw'],
            project_type,
            capacity
        )
        if cost_scatter:
            st.plotly_chart(cost_scatter, use_container_width=True)
        else:
            st.info("Cost comparison chart requires historical data")

    with col2:
        funnel = create_completion_funnel(project_type)
        st.plotly_chart(funnel, use_container_width=True)

    # Row 2: Risk Radar + Timeline
    col1, col2 = st.columns(2)

    with col1:
        radar = create_risk_radar(breakdown)
        st.plotly_chart(radar, use_container_width=True)

    with col2:
        timeline_box = create_timeline_boxplot(project_type, timeline['likely'])
        st.plotly_chart(timeline_box, use_container_width=True)

    # Row 3: Developer Comparison (full width)
    st.subheader("Developer Context")
    dev_chart = create_developer_comparison()
    st.plotly_chart(dev_chart, use_container_width=True)

    # Historical stats summary
    if not historical_costs.empty:
        st.subheader("📈 Historical Data Summary")

        cost_col = None
        for col in historical_costs.columns:
            if 'total' in col.lower() and 'cost' in col.lower() and 'kw' in col.lower():
                cost_col = col
                break

        if cost_col:
            valid_costs = historical_costs[historical_costs[cost_col].notna() & (historical_costs[cost_col] > 0)]

            if len(valid_costs) > 0:
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Historical Projects", f"{len(valid_costs):,}")

                with col2:
                    median_cost = valid_costs[cost_col].median()
                    st.metric("Median IC Cost", f"${median_cost:.0f}/kW")

                with col3:
                    p25 = valid_costs[cost_col].quantile(0.25)
                    p75 = valid_costs[cost_col].quantile(0.75)
                    st.metric("IQR Range", f"${p25:.0f} - ${p75:.0f}/kW")

                with col4:
                    this_pct = (valid_costs[cost_col] <= costs['med_per_kw']).mean() * 100
                    st.metric("This Project Percentile", f"{this_pct:.0f}%")

    st.divider()

    # Export Section
    st.subheader("📤 Export Report")

    col1, col2 = st.columns(2)

    with col1:
        if PLAYWRIGHT_AVAILABLE:
            if st.button("📄 Generate PDF Report", type="primary"):
                with st.spinner("Generating PDF..."):
                    try:
                        pdf_bytes = generate_pdf_from_data(
                            project_id=project_id,
                            project=project,
                            score_result=score_result,
                            breakdown=breakdown,
                            costs=costs,
                            timeline=timeline,
                            client_name=client_name
                        )

                        # Save to centralized report storage
                        saved_report_id = None
                        if REPORT_MANAGER_AVAILABLE:
                            rm = ReportManager()
                            report_entry = create_project_feasibility_report(
                                rm=rm,
                                project_id=project_id,
                                project_name=str(project['Project Name'])[:50],
                                client=client_name,
                                score=score_result['total_score'],
                                recommendation=score_result['recommendation'],
                                parameters={
                                    'capacity_mw': capacity,
                                    'project_type': project_type,
                                    'cost_estimate_m': costs['med_total'],
                                    'timeline_months': timeline['likely'],
                                    'region': selected_region
                                }
                            )
                            # Save PDF to report directory
                            rm.save_content_to_report(
                                report_entry['report_id'],
                                'project',
                                pdf_bytes.decode('latin-1') if isinstance(pdf_bytes, bytes) else pdf_bytes,
                                'report.pdf'
                            )
                            # Actually write bytes properly
                            pdf_path = report_entry['report_dir'] / 'report.pdf'
                            with open(pdf_path, 'wb') as f:
                                f.write(pdf_bytes)
                            saved_report_id = report_entry['report_id']

                        st.download_button(
                            label="📥 Download PDF",
                            data=pdf_bytes,
                            file_name=f"feasibility_report_{project_id}.pdf",
                            mime="application/pdf"
                        )
                        if saved_report_id:
                            st.success(f"Report saved! ID: {saved_report_id[:25]}...")
                        else:
                            st.success("PDF generated! Click above to download.")
                    except Exception as e:
                        st.error(f"PDF generation failed: {e}")
        else:
            # HTML report fallback for cloud deployment
            if st.button("📄 Generate HTML Report", type="primary"):
                html_content = generate_html_report(
                    project_id=project_id,
                    project=project,
                    score_result=score_result,
                    breakdown=breakdown,
                    costs=costs,
                    timeline=timeline,
                    client_name=client_name
                )

                # Save to centralized report storage
                saved_report_id = None
                if REPORT_MANAGER_AVAILABLE:
                    rm = ReportManager()
                    report_entry = create_project_feasibility_report(
                        rm=rm,
                        project_id=project_id,
                        project_name=str(project['Project Name'])[:50],
                        client=client_name,
                        score=score_result['total_score'],
                        recommendation=score_result['recommendation'],
                        parameters={
                            'capacity_mw': capacity,
                            'project_type': project_type,
                            'cost_estimate_m': costs['med_total'],
                            'timeline_months': timeline['likely'],
                            'region': selected_region
                        }
                    )
                    rm.save_content_to_report(
                        report_entry['report_id'],
                        'project',
                        html_content,
                        'report.html'
                    )
                    saved_report_id = report_entry['report_id']

                st.download_button(
                    label="📥 Download HTML Report",
                    data=html_content,
                    file_name=f"feasibility_report_{project_id}.html",
                    mime="text/html"
                )
                if saved_report_id:
                    st.success(f"Report saved! ID: {saved_report_id[:25]}...")
                st.info("💡 Open the HTML file in your browser and use Print → Save as PDF")

    with col2:
        # Generate markdown report
        report_md = f"""# INTERCONNECTION FEASIBILITY ASSESSMENT

**Project:** {project['Project Name']}
**Queue ID:** {project_id}
**Date:** {datetime.now().strftime('%Y-%m-%d')}
**Prepared For:** {client_name}

---

## EXECUTIVE SUMMARY

| Attribute | Assessment |
|-----------|------------|
| **Recommendation** | **{score_result['recommendation']}** |
| **Feasibility Score** | {score_result['total_score']:.0f}/100 (Grade: {score_result['grade']}) |
| **Estimated Cost** | ${costs['low_total']:.0f}M - ${costs['high_total']:.0f}M |
| **Estimated COD** | {timeline['likely_date']} |
| **Completion Probability** | {timeline['completion_rate']*100:.0f}% |

## PROJECT OVERVIEW

- **Developer:** {project['Developer/Interconnection Customer']}
- **Type:** {project_type}
- **Capacity:** {capacity:,.0f} MW
- **Location:** {project['County']}, {project['State']}
- **POI:** {project['Points of Interconnection']}

## SCORE BREAKDOWN

| Component | Score | Max |
|-----------|-------|-----|
| Queue Position | {breakdown['queue_position']:.1f} | 25 |
| Study Progress | {breakdown['study_progress']:.1f} | 25 |
| Developer | {breakdown['developer_track_record']:.1f} | 20 |
| POI Congestion | {breakdown['poi_congestion']:.1f} | 15 |
| Project Type | {breakdown['project_characteristics']:.1f} | 15 |
| **TOTAL** | **{score_result['total_score']:.0f}** | **100** |

## RISK FLAGS

**Red Flags:** {', '.join(score_result['red_flags']) if score_result['red_flags'] else 'None'}

**Green Flags:** {', '.join(score_result['green_flags']) if score_result['green_flags'] else 'None'}

---

*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*
"""

        st.download_button(
            "📥 Download Markdown",
            report_md,
            file_name=f"feasibility_report_{project_id}.md",
            mime="text/markdown"
        )

    # Footer
    st.divider()
    st.caption(f"Report generated {datetime.now().strftime('%Y-%m-%d %H:%M')} | Data source: NYISO Interconnection Queue")


if __name__ == "__main__":
    main()
