#!/usr/bin/env python3
"""
Deal Sheet Generator — 1-page PDF deal sheets for investor scanning.

Generates concise, data-dense deal sheets from master.db enrichments.
Designed for ITC deal sourcing: shows project details, tax credit breakdown,
developer track record, and investability signals on a single page.

Usage:
    # Single project
    python -m reports.deal_sheet Q12345

    # Batch from investable pipeline
    python -m reports.deal_sheet --investable --min-score 70 --limit 5

    # Specific projects
    python -m reports.deal_sheet Q12345 Q67890 Q11111

    # Custom output
    python -m reports.deal_sheet Q12345 -o ~/Desktop/deal_sheet.pdf
"""

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Paths
TOOLS_DIR = Path(__file__).parent.parent
DATA_DIR = TOOLS_DIR / '.data'
MASTER_DB = DATA_DIR / 'master.db'
DEV_DB = DATA_DIR / 'developer.db'
DEFAULT_OUTPUT = TOOLS_DIR / 'output' / 'deal_sheets'


# ============================================================================
# Data Loading
# ============================================================================

def load_project(queue_id: str, db_path: Path = MASTER_DB) -> Optional[Dict]:
    """Load a project and all enrichment data from master.db."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT
            queue_id, name, region, state, county,
            type_std, type, capacity_mw, status_std, status,
            queue_date_std, queue_date, cod_std, cod,
            developer, developer_canonical, developer_tier,
            developer_tier_confidence, developer_needs_capital,
            construction_stage, construction_stage_confidence, construction_stage_method,
            ia_date, actual_cod, study_phase, backfeed_date, ia_status,
            feasibility_study_status, system_impact_study_status,
            facilities_study_status, test_energy_date, withdrawn_date,
            tax_credit_type, recommended_credit, base_credit_rate,
            effective_credit_rate, estimated_credit_value,
            energy_community_eligible, energy_community_bonus,
            energy_community_type,
            low_income_eligible, low_income_bonus, low_income_type,
            domestic_content_eligible, domestic_content_confidence,
            investable, investability_score, investability_json,
            poi, latitude, longitude,
            data_quality_flag
        FROM projects WHERE queue_id = ?
    """, (queue_id,)).fetchone()
    conn.close()

    if not row:
        return None
    return dict(row)


def load_developer(canonical_name: str, db_path: Path = DEV_DB) -> Optional[Dict]:
    """Load developer track record from developer.db."""
    if not db_path.exists():
        return None
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT canonical_name, display_name, tier, total_projects,
               active_projects, operational_projects, withdrawn_projects,
               completion_rate, total_capacity_mw, needs_capital,
               region_count, state_count, primary_technology
        FROM developers WHERE canonical_name = ?
    """, (canonical_name,)).fetchone()
    conn.close()
    if not row:
        return None
    return dict(row)


def find_investable_projects(
    min_score: int = 70,
    limit: int = 5,
    db_path: Path = MASTER_DB,
) -> List[str]:
    """Find top investable project IDs from master.db."""
    conn = sqlite3.connect(str(db_path))
    rows = conn.execute("""
        SELECT queue_id FROM projects
        WHERE investable = 1
        AND investability_score >= ?
        AND COALESCE(data_quality_flag, '') = ''
        ORDER BY investability_score DESC
        LIMIT ?
    """, (min_score, limit)).fetchall()
    conn.close()
    return [r[0] for r in rows]


# ============================================================================
# HTML Template
# ============================================================================

def _fmt(val, fallback='--'):
    """Format a value, returning fallback for None/empty."""
    if val is None or val == '':
        return fallback
    return str(val)


def _fmt_mw(val):
    if val is None:
        return '--'
    return f"{val:,.1f} MW"


def _fmt_pct(val):
    if val is None:
        return '--'
    if val < 1:
        return f"{val * 100:.0f}%"
    return f"{val:.0f}%"


def _fmt_money(val):
    if val is None or val == 0:
        return '--'
    if val >= 1_000_000:
        return f"${val / 1_000_000:,.1f}M"
    if val >= 1_000:
        return f"${val / 1_000:,.0f}K"
    return f"${val:,.0f}"


def _score_color(score):
    if score is None:
        return '#666'
    if score >= 80:
        return '#2d7a2d'
    if score >= 60:
        return '#b8860b'
    return '#a02020'


def _score_grade(score):
    if score is None:
        return '--'
    if score >= 80:
        return 'A'
    if score >= 65:
        return 'B'
    if score >= 50:
        return 'C'
    return 'D'


def _check(val, label):
    """Return a checkmark or X for a boolean signal."""
    if val is None:
        return f'<span style="color:#999">-- {label}</span>'
    if val:
        return f'<span style="color:#2d7a2d">&#10003; {label}</span>'
    return f'<span style="color:#a02020">&#10007; {label}</span>'


def _stage_label(stage):
    labels = {
        'early': 'Early Stage',
        'mid': 'Mid Stage',
        'late': 'Late Stage',
        'construction': 'Construction',
        'operational': 'Operational',
    }
    return labels.get(stage, _fmt(stage, '--'))


def render_deal_sheet(project: Dict, developer: Optional[Dict] = None) -> str:
    """Render a 1-page deal sheet as HTML."""

    queue_id = project['queue_id']
    name = _fmt(project.get('name'), queue_id)
    region = _fmt(project.get('region'))
    state = _fmt(project.get('state'))
    county = _fmt(project.get('county'))
    tech = _fmt(project.get('type_std') or project.get('type'))
    capacity_mw = project.get('capacity_mw')
    capacity = _fmt_mw(capacity_mw)
    status = _fmt(project.get('status_std') or project.get('status'))
    queue_date = _fmt(project.get('queue_date_std') or project.get('queue_date'))
    cod = _fmt(project.get('cod_std') or project.get('cod'))
    poi = _fmt(project.get('poi'))
    stage = project.get('construction_stage')
    stage_conf = project.get('construction_stage_confidence')
    lat = project.get('latitude')
    lon = project.get('longitude')

    # Tax credits — prefer investability_json for ITC rate (more accurate for deal sheets)
    inv_json = {}
    if project.get('investability_json'):
        try:
            inv_json = json.loads(project['investability_json'])
        except (json.JSONDecodeError, TypeError):
            pass

    ec_eligible = project.get('energy_community_eligible')
    ec_bonus = project.get('energy_community_bonus')
    ec_type = project.get('energy_community_type')
    li_eligible = project.get('low_income_eligible')
    li_bonus = project.get('low_income_bonus')
    li_type = project.get('low_income_type')
    dc_eligible = project.get('domestic_content_eligible')
    dc_confidence = project.get('domestic_content_confidence')

    # ITC rate from investability scoring (accounts for bonus stacking correctly)
    itc_rate = inv_json.get('itc_rate')
    base_credit_rate = project.get('base_credit_rate')
    est_value = project.get('estimated_credit_value')

    # Compute ITC base rate (30% for most solar/storage, 6% without prevailing wage)
    # and build up the breakdown
    if itc_rate and itc_rate > 0.05:
        # We have a meaningful ITC rate from investability scoring
        itc_base = 0.30  # standard ITC base with prevailing wage
        itc_ec = 0.10 if ec_eligible else 0
        itc_li = 0.10 if li_eligible else 0
        itc_dc = 0.10 if dc_eligible else 0
        effective_itc = itc_base + itc_ec + itc_li + itc_dc
        # Use the investability JSON rate if it differs (it accounts for edge cases)
        if abs(effective_itc - itc_rate) > 0.05:
            effective_itc = itc_rate
        credit_mode = 'itc'
    elif base_credit_rate and base_credit_rate < 0.05:
        # PTC project (rate is $/kWh, not a percentage)
        credit_mode = 'ptc'
        effective_itc = None
    else:
        credit_mode = 'itc'
        effective_itc = base_credit_rate
        itc_base = base_credit_rate or 0.30
        itc_ec = 0.10 if ec_eligible else 0
        itc_li = 0.10 if li_eligible else 0
        itc_dc = 0.10 if dc_eligible else 0
        effective_itc = itc_base + itc_ec + itc_li + itc_dc

    # Estimated credit value
    if est_value and est_value > 0:
        est_value_display = _fmt_money(est_value)
    elif effective_itc and capacity_mw:
        # Rough estimate: $1.2M/MW installed cost * ITC rate
        rough_value = capacity_mw * 1_200_000 * effective_itc
        est_value_display = _fmt_money(rough_value)
    else:
        est_value_display = '--'

    # Build credit breakdown rows
    credit_rows = ''
    if credit_mode == 'itc':
        credit_rows += f'<tr><td>Base ITC (prevailing wage)</td><td style="text-align:right">30%</td></tr>'
        if ec_eligible:
            credit_rows += f'<tr><td>Energy Community Bonus</td><td style="text-align:right">+10%</td></tr>'
        if li_eligible:
            credit_rows += f'<tr><td>Low-Income Community Bonus</td><td style="text-align:right">+10%</td></tr>'
        if dc_eligible:
            credit_rows += f'<tr><td>Domestic Content Bonus</td><td style="text-align:right">+10%</td></tr>'
        effective_display = f"{int(effective_itc * 100)}%" if effective_itc else '--'
    else:
        # PTC display
        ptc_base = base_credit_rate or 0
        credit_rows += f'<tr><td>Base PTC Rate</td><td style="text-align:right">${ptc_base:.4f}/kWh</td></tr>'
        if ec_eligible:
            credit_rows += f'<tr><td>Energy Community (+10%)</td><td style="text-align:right">+${ptc_base * 0.1:.4f}/kWh</td></tr>'
        if li_eligible:
            credit_rows += f'<tr><td>Low-Income Bonus (+10%)</td><td style="text-align:right">+${ptc_base * 0.1:.4f}/kWh</td></tr>'
        if dc_eligible:
            credit_rows += f'<tr><td>Domestic Content (+10%)</td><td style="text-align:right">+${ptc_base * 0.1:.4f}/kWh</td></tr>'
        ptc_mult = 1.0 + (0.1 if ec_eligible else 0) + (0.1 if li_eligible else 0) + (0.1 if dc_eligible else 0)
        effective_display = f"${ptc_base * ptc_mult:.4f}/kWh"

    # Investability
    score = project.get('investability_score')
    score_color = _score_color(score)
    grade = _score_grade(score)

    # Score component breakdown
    components = inv_json.get('components', {})
    score_rows = ''
    component_labels = {
        'itc_eligible': 'ITC Eligibility',
        'size_fit': 'Size Fit',
        'stage': 'Development Stage',
        'capital_need': 'Capital Need',
        'bonuses': 'Credit Bonuses',
        'completeness': 'Data Completeness',
    }
    for key, label in component_labels.items():
        comp = components.get(key, {})
        if comp:
            s = comp.get('score', 0)
            m = comp.get('max', 0)
            detail = comp.get('detail', '')
            bar_pct = (s / m * 100) if m > 0 else 0
            score_rows += f'''<tr>
                <td>{label}</td>
                <td style="text-align:center;font-size:8px;color:#888">{detail}</td>
                <td style="text-align:right;font-weight:600">{s}/{m}</td>
            </tr>'''

    # Developer
    dev_name = _fmt(project.get('developer_canonical') or project.get('developer'))
    dev_tier = _fmt(project.get('developer_tier'))
    needs_capital = project.get('developer_needs_capital')

    # Developer track record from developer.db
    dev_total = '--'
    dev_operational = '--'
    dev_active = '--'
    dev_withdrawn = '--'
    dev_comp_rate = '--'
    dev_capacity = '--'
    dev_regions = '--'
    dev_primary_tech = '--'
    if developer:
        dev_total = str(developer.get('total_projects') or 0)
        dev_operational = str(developer.get('operational_projects') or 0)
        dev_active = str(developer.get('active_projects') or 0)
        dev_withdrawn = str(developer.get('withdrawn_projects') or 0)
        dev_comp_rate = _fmt_pct(developer.get('completion_rate'))
        dev_capacity = _fmt_mw(developer.get('total_capacity_mw'))
        dev_regions = str(developer.get('region_count') or 0)
        dev_primary_tech = _fmt(developer.get('primary_technology'))

    # Risk flags
    risks = []
    dq = project.get('data_quality_flag')
    if dq:
        risks.append(f'Data quality flag: {dq}')
    if stage == 'early':
        risks.append('Early-stage project — higher risk of withdrawal')
    if cod and cod != '--':
        try:
            cod_year = int(str(cod)[:4])
            if cod_year < 2026:
                risks.append(f'COD date ({cod}) has passed — may be delayed or stalled')
        except (ValueError, TypeError):
            pass
    if not project.get('developer_canonical'):
        risks.append('Developer not identified')
    if developer and (developer.get('completion_rate') or 0) < 0.1 and (developer.get('total_projects') or 0) > 5:
        risks.append(f'Developer has low completion rate ({dev_comp_rate})')
    if not ec_eligible:
        risks.append('Not in an energy community — misses 10% ITC bonus')
    if not dc_eligible:
        risks.append('Domestic content not confirmed — 10% bonus at risk')
    if not risks:
        risks.append('No significant risk flags identified')

    risk_html = '\n'.join(f'<li>{r}</li>' for r in risks[:5])  # Cap at 5

    # Investability signals
    signals = []
    signals.append(_check(project.get('investable'), 'ITC eligible'))
    signals.append(_check(
        project.get('developer_tier') == 'independent',
        'Independent developer'
    ))
    signals.append(_check(ec_eligible, 'Energy community'))
    signals.append(_check(li_eligible, 'Low-income community'))
    signals.append(_check(dc_eligible, 'Domestic content'))
    signals.append(_check(
        stage in ('late', 'construction'),
        'Late-stage / construction'
    ))
    signals_html = '<br>'.join(signals)

    # Development milestones — show non-null milestone data as rows
    milestone_items = []
    study_phase_val = project.get('study_phase')
    ia_status_val = project.get('ia_status')
    ia_date_val = project.get('ia_date')
    backfeed_val = project.get('backfeed_date')
    test_energy_val = project.get('test_energy_date')
    actual_cod_val = project.get('actual_cod')
    feas_status = project.get('feasibility_study_status')
    sis_status = project.get('system_impact_study_status')
    fac_status = project.get('facilities_study_status')
    stage_method = project.get('construction_stage_method', '')

    if study_phase_val:
        milestone_items.append(('Study Phase', _fmt(study_phase_val)))
    if ia_status_val:
        milestone_items.append(('IA Status', _fmt(ia_status_val)))
    if ia_date_val:
        milestone_items.append(('IA Date', _fmt(ia_date_val)))

    # Study progression: show statuses if available
    study_steps = []
    if feas_status:
        study_steps.append(f"Feas: {feas_status}")
    if sis_status:
        study_steps.append(f"SIS: {sis_status}")
    if fac_status:
        study_steps.append(f"Fac: {fac_status}")
    if study_steps:
        milestone_items.append(('Study Progress', ' → '.join(study_steps)))

    if backfeed_val:
        milestone_items.append(('Backfeed Date', _fmt(backfeed_val)))
    if test_energy_val:
        milestone_items.append(('Test Energy Date', _fmt(test_energy_val)))
    if actual_cod_val:
        milestone_items.append(('Actual COD', _fmt(actual_cod_val)))
    if stage_method:
        milestone_items.append(('Classification', stage_method.replace('_', ' ').title()))

    if not milestone_items:
        milestone_rows_html = '<div class="data-row"><span class="data-label">No milestone data available</span><span class="data-value">--</span></div>'
    else:
        milestone_rows_html = '\n'.join(
            f'<div class="data-row"><span class="data-label">{label}</span><span class="data-value">{val}</span></div>'
            for label, val in milestone_items
        )

    # Location line
    loc_parts = [p for p in [region, f"{state}, {county}" if county and county != '--' else state] if p != '--']
    location_line = ' &bull; '.join(loc_parts)

    # Coordinates
    coord_display = f"{lat:.4f}, {lon:.4f}" if lat and lon else '--'

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
@page {{
    size: letter;
    margin: 0.45in 0.55in 0.5in 0.55in;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
    font-family: -apple-system, "Helvetica Neue", Arial, sans-serif;
    font-size: 9px;
    line-height: 1.35;
    color: #1a1a1a;
}}

/* Header */
.header {{
    background: #1a1a1a;
    color: white;
    padding: 14px 20px 12px;
    margin: -0.45in -0.55in 0 -0.55in;
    width: calc(100% + 1.1in);
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
}}
.header-left {{ flex: 1; }}
.header-brand {{
    font-size: 7px;
    letter-spacing: 2px;
    text-transform: uppercase;
    opacity: 0.6;
    margin-bottom: 3px;
}}
.header h1 {{
    font-size: 15px;
    font-weight: 600;
    margin: 0 0 2px 0;
    line-height: 1.2;
}}
.header-subtitle {{
    font-size: 10px;
    opacity: 0.8;
}}
.header-right {{ text-align: center; min-width: 72px; }}

/* Score Badge */
.score-badge {{
    width: 60px;
    height: 60px;
    border-radius: 50%;
    border: 3px solid {score_color};
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    background: rgba(255,255,255,0.1);
}}
.score-num {{
    font-size: 20px;
    font-weight: 700;
    line-height: 1;
    color: white;
}}
.score-grade {{
    font-size: 8px;
    font-weight: 600;
    color: {score_color};
    letter-spacing: 1px;
}}

/* Project bar */
.project-bar {{
    display: flex;
    gap: 12px;
    padding: 6px 0;
    border-bottom: 1px solid #ddd;
    margin-bottom: 8px;
    margin-top: 10px;
    font-size: 8.5px;
    color: #555;
    flex-wrap: wrap;
}}
.project-bar strong {{ color: #1a1a1a; }}

/* Two column layout */
.columns {{
    display: flex;
    gap: 16px;
    margin-bottom: 8px;
}}
.col-left {{ flex: 1; }}
.col-right {{ flex: 1; }}

/* Section headers */
.section-title {{
    font-size: 7.5px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: #1a1a1a;
    border-bottom: 2px solid #1a1a1a;
    padding-bottom: 2px;
    margin-bottom: 6px;
    margin-top: 10px;
}}
.section-title:first-child {{ margin-top: 0; }}

/* Data rows */
.data-row {{
    display: flex;
    justify-content: space-between;
    padding: 2.5px 0;
    border-bottom: 1px solid #f0f0f0;
    font-size: 8.5px;
}}
.data-row:last-child {{ border-bottom: none; }}
.data-label {{ color: #666; }}
.data-value {{ font-weight: 600; text-align: right; max-width: 55%; }}

/* Credit table */
.credit-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 8.5px;
    margin-bottom: 3px;
}}
.credit-table td {{
    padding: 2.5px 0;
    border-bottom: 1px solid #f0f0f0;
}}
.credit-table td:last-child {{
    text-align: right;
    font-weight: 600;
}}
.credit-total {{
    border-top: 2px solid #1a1a1a !important;
}}
.credit-total td {{ font-weight: 700; padding-top: 3px; }}
.credit-value {{
    background: #f0f7f0;
    padding: 4px 8px;
    text-align: center;
    font-size: 10px;
    font-weight: 700;
    color: #2d7a2d;
    margin-top: 3px;
}}

/* Score component table */
.score-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 8px;
}}
.score-table td {{
    padding: 2px 0;
    border-bottom: 1px solid #f0f0f0;
}}

/* Signals */
.signals {{
    font-size: 8.5px;
    line-height: 1.7;
}}

/* Risk section */
.risk-section {{
    margin-top: 8px;
    padding: 6px 10px;
    background: #fdf8f4;
    border-left: 3px solid #b8860b;
}}
.risk-section ul {{
    list-style: none;
    padding: 0;
}}
.risk-section li {{
    padding: 1.5px 0;
    font-size: 8px;
    color: #5a4a20;
}}
.risk-section li::before {{
    content: "\\25CF ";
    color: #b8860b;
    font-size: 6px;
}}

/* Footer */
.footer {{
    position: fixed;
    bottom: 0;
    left: 0;
    right: 0;
    padding: 5px 0.55in;
    border-top: 1px solid #ddd;
    font-size: 7px;
    color: #999;
    display: flex;
    justify-content: space-between;
}}

/* Developer section compact */
.dev-stats {{
    display: flex;
    gap: 6px;
    margin-top: 4px;
}}
.dev-stat {{
    text-align: center;
    flex: 1;
    padding: 3px 2px;
    background: #f8f8f8;
    border: 1px solid #eee;
}}
.dev-stat-val {{
    font-size: 13px;
    font-weight: 700;
    line-height: 1;
    color: #1a1a1a;
}}
.dev-stat-label {{
    font-size: 6.5px;
    color: #888;
    text-transform: uppercase;
    letter-spacing: 0.3px;
}}
</style>
</head>
<body>

<!-- HEADER -->
<div class="header">
    <div class="header-left">
        <div class="header-brand">Prospector Labs</div>
        <h1>Deal Sheet</h1>
        <div class="header-subtitle">{name}</div>
    </div>
    <div class="header-right">
        <div class="score-badge">
            <span class="score-num">{score or '--'}</span>
            <span class="score-grade">GRADE {grade}</span>
        </div>
    </div>
</div>

<!-- PROJECT SUMMARY BAR -->
<div class="project-bar">
    <span><strong>ID:</strong> {queue_id}</span>
    <span><strong>Region:</strong> {location_line}</span>
    <span><strong>Technology:</strong> {tech}</span>
    <span><strong>Capacity:</strong> {capacity}</span>
    <span><strong>Status:</strong> {status}</span>
    <span><strong>Coords:</strong> {coord_display}</span>
</div>

<!-- MAIN TWO-COLUMN LAYOUT -->
<div class="columns">
    <div class="col-left">

        <!-- INTERCONNECTION -->
        <div class="section-title">Interconnection</div>
        <div class="data-row"><span class="data-label">Queue Date</span><span class="data-value">{queue_date}</span></div>
        <div class="data-row"><span class="data-label">Expected COD</span><span class="data-value">{cod}</span></div>
        <div class="data-row"><span class="data-label">Point of Interconnection</span><span class="data-value">{poi}</span></div>
        <div class="data-row"><span class="data-label">Construction Stage</span><span class="data-value">{_stage_label(stage)}</span></div>
        <div class="data-row"><span class="data-label">Stage Confidence</span><span class="data-value">{_fmt_pct(stage_conf) if stage_conf else '--'}</span></div>

        <!-- DEVELOPMENT MILESTONES -->
        <div class="section-title">Development Milestones</div>
        {milestone_rows_html}

        <!-- DEVELOPER -->
        <div class="section-title">Developer</div>
        <div class="data-row"><span class="data-label">Name</span><span class="data-value">{dev_name}</span></div>
        <div class="data-row"><span class="data-label">Tier</span><span class="data-value">{dev_tier.replace('_', ' ').title()}</span></div>
        <div class="data-row"><span class="data-label">Needs Capital</span><span class="data-value">{'Yes' if needs_capital else 'No' if needs_capital is not None else '--'}</span></div>
        <div class="data-row"><span class="data-label">Portfolio</span><span class="data-value">{dev_capacity} across {dev_regions} regions</span></div>
        <div class="data-row"><span class="data-label">Primary Tech</span><span class="data-value">{dev_primary_tech}</span></div>

        <div class="dev-stats">
            <div class="dev-stat">
                <div class="dev-stat-val">{dev_total}</div>
                <div class="dev-stat-label">Total</div>
            </div>
            <div class="dev-stat">
                <div class="dev-stat-val">{dev_operational}</div>
                <div class="dev-stat-label">Built</div>
            </div>
            <div class="dev-stat">
                <div class="dev-stat-val">{dev_active}</div>
                <div class="dev-stat-label">Active</div>
            </div>
            <div class="dev-stat">
                <div class="dev-stat-val">{dev_withdrawn}</div>
                <div class="dev-stat-label">Withdrawn</div>
            </div>
            <div class="dev-stat">
                <div class="dev-stat-val">{dev_comp_rate}</div>
                <div class="dev-stat-label">Comp%</div>
            </div>
        </div>

        <!-- INVESTABILITY SIGNALS -->
        <div class="section-title">Investability Signals</div>
        <div class="signals">
            {signals_html}
        </div>

    </div>
    <div class="col-right">

        <!-- TAX CREDIT ANALYSIS -->
        <div class="section-title">Tax Credit Analysis</div>
        <table class="credit-table">
            {credit_rows}
            <tr class="credit-total"><td>Effective Rate</td><td>{effective_display}</td></tr>
        </table>
        <div class="credit-value">Est. Credit Value: {est_value_display}</div>

        <!-- BONUS ELIGIBILITY DETAIL -->
        <div class="section-title">Bonus Eligibility Detail</div>
        <div class="data-row"><span class="data-label">Energy Community</span><span class="data-value">{_fmt(ec_type) if ec_eligible else 'Not eligible'}</span></div>
        <div class="data-row"><span class="data-label">Low-Income</span><span class="data-value">{_fmt(li_type) if li_eligible else 'Not eligible'}</span></div>
        <div class="data-row"><span class="data-label">Domestic Content</span><span class="data-value">{_fmt(dc_confidence, 'Not confirmed').title() if dc_eligible else 'Not confirmed'}</span></div>

        <!-- SCORE BREAKDOWN -->
        <div class="section-title">Score Breakdown ({score or '--'}/100)</div>
        <table class="score-table">
            {score_rows}
        </table>

    </div>
</div>

<!-- RISK FLAGS -->
<div class="risk-section">
    <div class="section-title" style="border-color: #b8860b; color: #5a4a20; margin-bottom: 3px; margin-top: 0;">Risk Flags</div>
    <ul>{risk_html}</ul>
</div>

<!-- FOOTER -->
<div class="footer">
    <span>Generated {datetime.now().strftime('%Y-%m-%d')} &bull; Prospector Labs &bull; prospectorlabs.com</span>
    <span>Not investment advice. Data current as of last pipeline refresh.</span>
</div>

</body>
</html>"""
    return html


# ============================================================================
# PDF Generation
# ============================================================================

def generate_deal_sheet_pdf(
    queue_id: str,
    output_path: Optional[Path] = None,
    db_path: Path = MASTER_DB,
    dev_db_path: Path = DEV_DB,
) -> Optional[Path]:
    """Generate a 1-page deal sheet PDF for a project."""

    project = load_project(queue_id, db_path)
    if not project:
        print(f"  Project {queue_id} not found in database")
        return None

    developer = None
    dev_canonical = project.get('developer_canonical')
    if dev_canonical:
        developer = load_developer(dev_canonical, dev_db_path)

    html = render_deal_sheet(project, developer)

    if output_path is None:
        output_path = DEFAULT_OUTPUT / f"deal_sheet_{queue_id}.pdf"

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        from weasyprint import HTML
        HTML(string=html).write_pdf(str(output_path))
        print(f"  Generated: {output_path}")
        return output_path
    except ImportError:
        # Fallback: save as HTML
        html_path = output_path.with_suffix('.html')
        html_path.write_text(html, encoding='utf-8')
        print(f"  WeasyPrint not available. Saved HTML: {html_path}")
        return html_path


def generate_deal_sheets(
    project_ids: Optional[List[str]] = None,
    min_score: int = 70,
    limit: int = 5,
    output_dir: Optional[Path] = None,
    db_path: Path = MASTER_DB,
    dev_db_path: Path = DEV_DB,
) -> List[Path]:
    """Generate deal sheets for multiple projects."""

    if project_ids is None:
        project_ids = find_investable_projects(min_score, limit, db_path)

    if not project_ids:
        print("No projects found matching criteria")
        return []

    if output_dir is None:
        output_dir = DEFAULT_OUTPUT

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for qid in project_ids:
        out = output_dir / f"deal_sheet_{qid}.pdf"
        path = generate_deal_sheet_pdf(qid, out, db_path, dev_db_path)
        if path:
            results.append(path)

    print(f"\nGenerated {len(results)} deal sheets in {output_dir}")
    return results


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Generate 1-page deal sheet PDFs')
    parser.add_argument('project_ids', nargs='*', help='Queue IDs to generate sheets for')
    parser.add_argument('--investable', action='store_true',
                        help='Generate from investable pipeline')
    parser.add_argument('--min-score', type=int, default=70,
                        help='Minimum investability score (default: 70)')
    parser.add_argument('--limit', type=int, default=5,
                        help='Max projects to generate (default: 5)')
    parser.add_argument('-o', '--output', type=str,
                        help='Output path (file for single, directory for batch)')
    parser.add_argument('--db', type=str, help='Path to master.db')
    parser.add_argument('--dev-db', type=str, help='Path to developer.db')
    parser.add_argument('--html', action='store_true',
                        help='Output HTML instead of PDF')

    args = parser.parse_args()

    db = Path(args.db) if args.db else MASTER_DB
    dev_db = Path(args.dev_db) if args.dev_db else DEV_DB

    if args.project_ids and not args.investable:
        # Specific projects
        if len(args.project_ids) == 1 and args.output:
            # Single project, specific output
            path = generate_deal_sheet_pdf(
                args.project_ids[0],
                Path(args.output),
                db, dev_db,
            )
        else:
            # Multiple projects
            out_dir = Path(args.output) if args.output else DEFAULT_OUTPUT
            generate_deal_sheets(args.project_ids, output_dir=out_dir,
                                 db_path=db, dev_db_path=dev_db)
    elif args.investable or not args.project_ids:
        # From investable pipeline
        out_dir = Path(args.output) if args.output else DEFAULT_OUTPUT
        generate_deal_sheets(
            min_score=args.min_score,
            limit=args.limit,
            output_dir=out_dir,
            db_path=db,
            dev_db_path=dev_db,
        )
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
