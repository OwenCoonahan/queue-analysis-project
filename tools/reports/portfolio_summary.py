#!/usr/bin/env python3
"""
Portfolio Summary PDF Generator — 1-page overview of investable queue projects.

Generates a professional summary PDF for investor outreach using WeasyPrint.
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path

TOOLS_DIR = Path(__file__).parent.parent
DATA_DIR = TOOLS_DIR / '.data'
MASTER_DB = DATA_DIR / 'master.db'
OUTPUT_DIR = TOOLS_DIR / 'output'


def generate_portfolio_summary(db_path: Path = MASTER_DB, output_dir: Path = OUTPUT_DIR):
    """Generate 1-page portfolio summary PDF."""
    conn = sqlite3.connect(str(db_path))

    # Aggregate stats
    total = conn.execute("SELECT COUNT(*) FROM projects WHERE investable = 1").fetchone()[0]
    avg_score = conn.execute("SELECT ROUND(AVG(investability_score),1) FROM projects WHERE investable = 1").fetchone()[0]
    avg_itc = conn.execute("SELECT ROUND(AVG(itc_rate)*100,1) FROM projects WHERE investable = 1").fetchone()[0]
    total_mw = conn.execute("SELECT ROUND(SUM(capacity_mw),1) FROM projects WHERE investable = 1").fetchone()[0]
    total_credit = conn.execute("SELECT ROUND(SUM(estimated_credit_value),0) FROM projects WHERE investable = 1 AND estimated_credit_value > 0").fetchone()[0]
    n_states = conn.execute("SELECT COUNT(DISTINCT state) FROM projects WHERE investable = 1").fetchone()[0]

    # By state
    state_rows = conn.execute("""
        SELECT state, COUNT(*) as n, ROUND(AVG(capacity_mw),1) as avg_mw,
               ROUND(AVG(investability_score),1) as avg_score, ROUND(AVG(itc_rate)*100,0) as avg_itc
        FROM projects WHERE investable = 1 GROUP BY state ORDER BY n DESC
    """).fetchall()

    # By stage
    stage_rows = conn.execute("""
        SELECT construction_stage, COUNT(*) as n
        FROM projects WHERE investable = 1 GROUP BY construction_stage ORDER BY n DESC
    """).fetchall()

    # By size bucket
    size_rows = conn.execute("""
        SELECT CASE WHEN capacity_mw < 2 THEN 'Sub-2 MW' WHEN capacity_mw < 5 THEN '2-5 MW'
               WHEN capacity_mw <= 10 THEN '5-10 MW' ELSE '10+ MW' END as bucket, COUNT(*) as n
        FROM projects WHERE investable = 1 GROUP BY bucket ORDER BY n DESC
    """).fetchall()

    # By technology
    tech_rows = conn.execute("""
        SELECT type_std, COUNT(*) as n
        FROM projects WHERE investable = 1 GROUP BY type_std ORDER BY n DESC
    """).fetchall()

    # Top 10 projects
    top10 = conn.execute("""
        SELECT name, state, ROUND(capacity_mw,1) as mw, construction_stage,
               ROUND(itc_rate*100,0) as itc_pct, investability_score
        FROM projects WHERE investable = 1
        ORDER BY investability_score DESC LIMIT 10
    """).fetchall()

    conn.close()

    # Build state table
    state_html = ""
    for s in state_rows:
        state_html += f"<tr><td>{s[0]}</td><td style='text-align:center'>{s[1]}</td><td style='text-align:center'>{s[2]} MW</td><td style='text-align:center'>{s[3]}</td><td style='text-align:center'>{int(s[4])}%</td></tr>"

    # Build stage table
    stage_labels = {'construction': 'Construction', 'late': 'Late Stage', 'mid': 'Mid Stage', 'early': 'Early Stage'}
    stage_html = ""
    for s in stage_rows:
        stage_html += f"<tr><td>{stage_labels.get(s[0], s[0])}</td><td style='text-align:center'>{s[1]}</td></tr>"

    # Build size table
    size_html = ""
    for s in size_rows:
        size_html += f"<tr><td>{s[0]}</td><td style='text-align:center'>{s[1]}</td></tr>"

    # Build tech table
    tech_html = ""
    for t in tech_rows:
        tech_html += f"<tr><td>{t[0] or 'Unknown'}</td><td style='text-align:center'>{t[1]}</td></tr>"

    # Top 10 table
    top10_html = ""
    for p in top10:
        name = p[0] if p[0] and len(p[0]) < 40 else (p[0][:37] + '...' if p[0] else '--')
        stage_label = stage_labels.get(p[3], p[3] or '--')
        top10_html += f"<tr><td>{name}</td><td style='text-align:center'>{p[1]}</td><td style='text-align:center'>{p[2]}</td><td style='text-align:center'>{stage_label}</td><td style='text-align:center'>{int(p[4])}%</td><td style='text-align:center;font-weight:700'>{p[5]}</td></tr>"

    total_credit_m = f"${total_credit / 1_000_000:,.0f}M" if total_credit else '--'

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
@page {{
    size: letter;
    margin: 0.4in 0.5in 0.45in 0.5in;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
    font-family: -apple-system, "Helvetica Neue", Arial, sans-serif;
    font-size: 8.5px;
    line-height: 1.35;
    color: #1a1a1a;
}}
.header {{
    background: #1a1a1a;
    color: white;
    padding: 16px 22px 14px;
    margin: -0.4in -0.5in 0 -0.5in;
    width: calc(100% + 1.0in);
}}
.header h1 {{
    font-size: 16px;
    font-weight: 700;
    margin: 0;
    line-height: 1.2;
}}
.header-sub {{
    font-size: 10px;
    opacity: 0.8;
    margin-top: 2px;
}}
.header-brand {{
    font-size: 7px;
    letter-spacing: 2px;
    text-transform: uppercase;
    opacity: 0.5;
    margin-bottom: 3px;
}}

/* Key metrics strip */
.metrics {{
    display: flex;
    gap: 0;
    margin: 12px 0 10px;
    border: 1px solid #ddd;
}}
.metric {{
    flex: 1;
    text-align: center;
    padding: 8px 4px;
    border-right: 1px solid #ddd;
}}
.metric:last-child {{ border-right: none; }}
.metric-val {{
    font-size: 18px;
    font-weight: 800;
    color: #1a1a1a;
    line-height: 1;
}}
.metric-val.green {{ color: #2d7a2d; }}
.metric-label {{
    font-size: 6.5px;
    color: #888;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-top: 2px;
}}

/* Section */
.section-title {{
    font-size: 7.5px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: #1a1a1a;
    border-bottom: 2px solid #1a1a1a;
    padding-bottom: 2px;
    margin-bottom: 5px;
    margin-top: 10px;
}}

/* Tables */
table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 8px;
}}
th {{
    background: #f5f5f5;
    font-weight: 700;
    font-size: 7px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    padding: 3px 4px;
    text-align: left;
    border-bottom: 1px solid #ddd;
}}
td {{
    padding: 2.5px 4px;
    border-bottom: 1px solid #f0f0f0;
}}
.col-layout {{
    display: flex;
    gap: 14px;
}}
.col-left {{ flex: 1; }}
.col-right {{ flex: 1; }}
.col-small {{ flex: 0.7; }}

/* Preview table */
.preview-table td {{ font-size: 7.5px; }}
.preview-table th {{ font-size: 6.5px; }}

/* Footer */
.footer {{
    position: fixed;
    bottom: 0;
    left: 0;
    right: 0;
    padding: 5px 0.5in;
    border-top: 1px solid #ddd;
    font-size: 7px;
    color: #999;
    display: flex;
    justify-content: space-between;
}}

/* CTA */
.cta {{
    background: #f0f7f0;
    border: 1px solid #c5ddc5;
    padding: 8px 14px;
    margin-top: 10px;
    text-align: center;
    font-size: 9px;
    color: #2d5a2d;
}}
.cta strong {{ font-size: 10px; }}
</style>
</head>
<body>

<div class="header">
    <div class="header-brand">Prospector Labs &bull; Glass Energy Platform</div>
    <h1>ITC-Eligible Solar &amp; Storage Projects</h1>
    <div class="header-sub">Pre-Screened for Independent Investors &bull; {datetime.now().strftime('%B %Y')}</div>
</div>

<div class="metrics">
    <div class="metric">
        <div class="metric-val">{total}</div>
        <div class="metric-label">Queue Projects</div>
    </div>
    <div class="metric">
        <div class="metric-val">{total_mw}</div>
        <div class="metric-label">Total MW</div>
    </div>
    <div class="metric">
        <div class="metric-val">{n_states}</div>
        <div class="metric-label">States</div>
    </div>
    <div class="metric">
        <div class="metric-val green">{avg_itc}%</div>
        <div class="metric-label">Avg ITC Rate</div>
    </div>
    <div class="metric">
        <div class="metric-val green">{total_credit_m}</div>
        <div class="metric-label">Est. Credit Value</div>
    </div>
    <div class="metric">
        <div class="metric-val">{avg_score}</div>
        <div class="metric-label">Avg Score</div>
    </div>
</div>

<p style="font-size:8.5px;color:#444;margin-bottom:8px">
    Curated pipeline of <strong>{total} utility-scale projects</strong> (1&ndash;10 MW) passing 6-component investability screening:
    ITC eligibility, appropriate size, advanced development stage, independent developer needing capital, credit bonus stacking, and data completeness.
    Each project has a full deal sheet with tax credit breakdown, developer track record, and milestone data.
</p>

<div class="col-layout">
    <div class="col-left">
        <div class="section-title">By State</div>
        <table>
            <tr><th>State</th><th style="text-align:center">Projects</th><th style="text-align:center">Avg Size</th><th style="text-align:center">Avg Score</th><th style="text-align:center">Avg ITC</th></tr>
            {state_html}
        </table>

        <div class="section-title">By Development Stage</div>
        <table>
            <tr><th>Stage</th><th style="text-align:center">Projects</th></tr>
            {stage_html}
        </table>
    </div>
    <div class="col-right">
        <div class="section-title">By Size</div>
        <table>
            <tr><th>Size Bucket</th><th style="text-align:center">Projects</th></tr>
            {size_html}
        </table>

        <div class="section-title">By Technology</div>
        <table>
            <tr><th>Technology</th><th style="text-align:center">Projects</th></tr>
            {tech_html}
        </table>

        <div class="section-title">By ITC Rate</div>
        <table>
            <tr><th>ITC Rate</th><th style="text-align:center">What It Means</th></tr>
            <tr><td>30%</td><td style="text-align:center">Base ITC only</td></tr>
            <tr><td>40%</td><td style="text-align:center">+ Energy Community OR Low-Income</td></tr>
            <tr><td>50%</td><td style="text-align:center">+ Both EC &amp; LI bonuses</td></tr>
            <tr><td>60%</td><td style="text-align:center">+ Domestic Content (max stack)</td></tr>
        </table>
    </div>
</div>

<div class="section-title">Top 10 Projects Preview</div>
<table class="preview-table">
    <tr><th>Project Name</th><th style="text-align:center">State</th><th style="text-align:center">MW</th><th style="text-align:center">Stage</th><th style="text-align:center">ITC</th><th style="text-align:center">Score</th></tr>
    {top10_html}
</table>

<div class="cta">
    <strong>Full deal sheets available for all {total} projects.</strong><br>
    Contact: owen@prospectorlabs.com &bull; API access: prospectorlabs.com/api
</div>

<div class="footer">
    <span>Generated {datetime.now().strftime('%Y-%m-%d')} &bull; Prospector Labs &bull; prospectorlabs.com</span>
    <span>Not investment advice. Data from ISO interconnection queues, EIA, IRS. Refreshed daily.</span>
</div>

</body>
</html>"""

    output_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = output_dir / 'portfolio_summary_queue.pdf'

    try:
        from weasyprint import HTML
        HTML(string=html).write_pdf(str(pdf_path))
        print(f"Generated: {pdf_path}")
        return pdf_path
    except ImportError:
        html_path = pdf_path.with_suffix('.html')
        html_path.write_text(html, encoding='utf-8')
        print(f"WeasyPrint not available. Saved HTML: {html_path}")
        return html_path


def generate_dg_summary(db_path: Path = DATA_DIR / 'dg.db', output_dir: Path = OUTPUT_DIR):
    """Generate 1-page DG investable summary PDF."""
    conn = sqlite3.connect(str(db_path))

    total = conn.execute("SELECT COUNT(*) FROM projects WHERE dg_investable = 1").fetchone()[0]
    total_mw = conn.execute("SELECT ROUND(SUM(capacity_kw)/1000.0, 1) FROM projects WHERE dg_investable = 1").fetchone()[0]
    avg_score = conn.execute("SELECT ROUND(AVG(dg_investability_score),1) FROM projects WHERE dg_investable = 1").fetchone()[0]

    # By state
    state_rows = conn.execute("""
        SELECT state, COUNT(*) as n, ROUND(AVG(capacity_kw),1) as avg_kw,
               ROUND(AVG(dg_investability_score),1) as avg_score
        FROM projects WHERE dg_investable = 1 GROUP BY state ORDER BY n DESC
    """).fetchall()

    # By stage
    stage_rows = conn.execute("""
        SELECT dg_stage, COUNT(*) as n
        FROM projects WHERE dg_investable = 1 GROUP BY dg_stage ORDER BY n DESC
    """).fetchall()

    # By size bucket
    size_rows = conn.execute("""
        SELECT CASE WHEN capacity_kw < 25 THEN 'Sub-25 kW' WHEN capacity_kw < 100 THEN '25-100 kW'
               WHEN capacity_kw < 500 THEN '100-500 kW' WHEN capacity_kw <= 1000 THEN '500 kW-1 MW'
               ELSE '1+ MW' END as bucket, COUNT(*) as n
        FROM projects WHERE dg_investable = 1 GROUP BY bucket ORDER BY n DESC
    """).fetchall()

    # By source
    source_rows = conn.execute("""
        SELECT source, COUNT(*) as n
        FROM projects WHERE dg_investable = 1 GROUP BY source ORDER BY n DESC LIMIT 8
    """).fetchall()

    # Top 10 — prefer construction/inspection stage with names, spread across states
    top10 = conn.execute("""
        SELECT COALESCE(NULLIF(name, ''), queue_id) as display_name,
               state, ROUND(capacity_kw,1) as kw, dg_stage,
               dg_investability_score, source
        FROM projects WHERE dg_investable = 1
        ORDER BY
            CASE dg_stage WHEN 'construction' THEN 1 WHEN 'inspection' THEN 2 ELSE 3 END,
            dg_investability_score DESC
        LIMIT 10
    """).fetchall()

    conn.close()

    # Build tables
    state_html = ""
    for s in state_rows:
        state_html += f"<tr><td>{s[0]}</td><td style='text-align:center'>{s[1]:,}</td><td style='text-align:center'>{s[2]} kW</td><td style='text-align:center'>{s[3]}</td></tr>"

    stage_labels = {'approved': 'Approved', 'construction': 'Construction', 'inspection': 'Inspection', 'applied': 'Applied', 'operational': 'Operational'}
    stage_html = ""
    for s in stage_rows:
        stage_html += f"<tr><td>{stage_labels.get(s[0], s[0] or '--')}</td><td style='text-align:center'>{s[1]:,}</td></tr>"

    size_html = ""
    for s in size_rows:
        size_html += f"<tr><td>{s[0]}</td><td style='text-align:center'>{s[1]:,}</td></tr>"

    source_names = {
        'ny_dps_sir': 'NY DPS SIR', 'nj_dg': 'NJ Clean Energy', 'il_shines': 'IL Shines',
        'ameren_il': 'Ameren IL', 'ma_smart': 'MA SMART', 'ny_sun': 'NY-SUN',
        'ca_dg_stats': 'CA DG Stats', 'sdge_dg': 'SDG&E', 'ct_rsip': 'CT RSIP',
    }
    source_html = ""
    for s in source_rows:
        source_html += f"<tr><td>{source_names.get(s[0], s[0])}</td><td style='text-align:center'>{s[1]:,}</td></tr>"

    top10_html = ""
    for p in top10:
        name = p[0] if p[0] and len(p[0]) < 35 else (p[0][:32] + '...' if p[0] else '--')
        src = source_names.get(p[5], p[5] or '--')
        top10_html += f"<tr><td>{name}</td><td style='text-align:center'>{p[1]}</td><td style='text-align:center'>{p[2]}</td><td style='text-align:center'>{stage_labels.get(p[3], p[3] or '--')}</td><td style='text-align:center;font-weight:700'>{p[4]}</td><td style='text-align:center'>{src}</td></tr>"

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
@page {{
    size: letter;
    margin: 0.4in 0.5in 0.45in 0.5in;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
    font-family: -apple-system, "Helvetica Neue", Arial, sans-serif;
    font-size: 8.5px;
    line-height: 1.35;
    color: #1a1a1a;
}}
.header {{
    background: #1a3a5c;
    color: white;
    padding: 16px 22px 14px;
    margin: -0.4in -0.5in 0 -0.5in;
    width: calc(100% + 1.0in);
}}
.header h1 {{
    font-size: 16px;
    font-weight: 700;
    margin: 0;
    line-height: 1.2;
}}
.header-sub {{
    font-size: 10px;
    opacity: 0.8;
    margin-top: 2px;
}}
.header-brand {{
    font-size: 7px;
    letter-spacing: 2px;
    text-transform: uppercase;
    opacity: 0.5;
    margin-bottom: 3px;
}}
.metrics {{
    display: flex;
    gap: 0;
    margin: 12px 0 10px;
    border: 1px solid #ddd;
}}
.metric {{
    flex: 1;
    text-align: center;
    padding: 8px 4px;
    border-right: 1px solid #ddd;
}}
.metric:last-child {{ border-right: none; }}
.metric-val {{
    font-size: 18px;
    font-weight: 800;
    color: #1a1a1a;
    line-height: 1;
}}
.metric-val.blue {{ color: #1a3a5c; }}
.metric-label {{
    font-size: 6.5px;
    color: #888;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-top: 2px;
}}
.section-title {{
    font-size: 7.5px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: #1a1a1a;
    border-bottom: 2px solid #1a3a5c;
    padding-bottom: 2px;
    margin-bottom: 5px;
    margin-top: 10px;
}}
table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 8px;
}}
th {{
    background: #f5f5f5;
    font-weight: 700;
    font-size: 7px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    padding: 3px 4px;
    text-align: left;
    border-bottom: 1px solid #ddd;
}}
td {{
    padding: 2.5px 4px;
    border-bottom: 1px solid #f0f0f0;
}}
.col-layout {{
    display: flex;
    gap: 14px;
}}
.col-left {{ flex: 1; }}
.col-right {{ flex: 1; }}
.preview-table td {{ font-size: 7.5px; }}
.preview-table th {{ font-size: 6.5px; }}
.callout {{
    background: #f5f8fc;
    border-left: 3px solid #1a3a5c;
    padding: 6px 10px;
    margin: 8px 0;
    font-size: 8.5px;
    color: #2a4a6c;
}}
.cta {{
    background: #f0f4f8;
    border: 1px solid #c5d5e5;
    padding: 8px 14px;
    margin-top: 10px;
    text-align: center;
    font-size: 9px;
    color: #1a3a5c;
}}
.cta strong {{ font-size: 10px; }}
.footer {{
    position: fixed;
    bottom: 0;
    left: 0;
    right: 0;
    padding: 5px 0.5in;
    border-top: 1px solid #ddd;
    font-size: 7px;
    color: #999;
    display: flex;
    justify-content: space-between;
}}
</style>
</head>
<body>

<div class="header">
    <div class="header-brand">Prospector Labs &bull; Glass Energy Platform</div>
    <h1>Distributed Generation &mdash; Investable Project Pipeline</h1>
    <div class="header-sub">Sub-1 MW Solar &amp; Storage Portfolio Targets &bull; {datetime.now().strftime('%B %Y')}</div>
</div>

<div class="metrics">
    <div class="metric">
        <div class="metric-val blue">{total:,}</div>
        <div class="metric-label">DG Projects</div>
    </div>
    <div class="metric">
        <div class="metric-val">{total_mw:,}</div>
        <div class="metric-label">Total MW</div>
    </div>
    <div class="metric">
        <div class="metric-val">{len(state_rows)}</div>
        <div class="metric-label">States</div>
    </div>
    <div class="metric">
        <div class="metric-val">{avg_score}</div>
        <div class="metric-label">Avg Score</div>
    </div>
</div>

<div class="callout">
    <strong>Portfolio acquisition opportunity.</strong> DG projects typically don&rsquo;t have named developers &mdash;
    they&rsquo;re filed under utility programs (NY-SUN, NJ Clean Energy, IL Shines, MA SMART). The investment thesis:
    acquire portfolios of approved/construction-stage projects from program administrators or small installers
    who need capital to complete interconnection.
</div>

<div class="col-layout">
    <div class="col-left">
        <div class="section-title">By State</div>
        <table>
            <tr><th>State</th><th style="text-align:center">Projects</th><th style="text-align:center">Avg Size</th><th style="text-align:center">Avg Score</th></tr>
            {state_html}
        </table>

        <div class="section-title">By Development Stage</div>
        <table>
            <tr><th>Stage</th><th style="text-align:center">Projects</th></tr>
            {stage_html}
        </table>
    </div>
    <div class="col-right">
        <div class="section-title">By Size</div>
        <table>
            <tr><th>Size Bucket</th><th style="text-align:center">Projects</th></tr>
            {size_html}
        </table>

        <div class="section-title">By Source Program</div>
        <table>
            <tr><th>Program</th><th style="text-align:center">Projects</th></tr>
            {source_html}
        </table>
    </div>
</div>

<div class="section-title">Top 10 Projects Preview</div>
<table class="preview-table">
    <tr><th>Project</th><th style="text-align:center">State</th><th style="text-align:center">kW</th><th style="text-align:center">Stage</th><th style="text-align:center">Score</th><th style="text-align:center">Source</th></tr>
    {top10_html}
</table>

<div class="cta">
    <strong>Full dataset: {total:,} investable DG projects available via API or CSV export.</strong><br>
    Contact: owen@prospectorlabs.com &bull; API access: prospectorlabs.com/api
</div>

<div class="footer">
    <span>Generated {datetime.now().strftime('%Y-%m-%d')} &bull; Prospector Labs &bull; prospectorlabs.com</span>
    <span>Not investment advice. Data from state DG programs (NY-SUN, NJ CE, IL Shines, MA SMART).</span>
</div>

</body>
</html>"""

    output_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = output_dir / 'portfolio_summary_dg.pdf'

    try:
        from weasyprint import HTML
        HTML(string=html).write_pdf(str(pdf_path))
        print(f"Generated: {pdf_path}")
        return pdf_path
    except ImportError:
        html_path = pdf_path.with_suffix('.html')
        html_path.write_text(html, encoding='utf-8')
        print(f"WeasyPrint not available. Saved HTML: {html_path}")
        return html_path


if __name__ == '__main__':
    import sys
    if '--dg' in sys.argv:
        generate_dg_summary()
    elif '--both' in sys.argv:
        generate_portfolio_summary()
        generate_dg_summary()
    else:
        generate_portfolio_summary()
