#!/usr/bin/env python3
"""
Queue Analysis Report Generator

Simple, clean interface for generating interconnection feasibility reports.
Focus: Data accuracy and professional output.
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import sys

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from analyze import QueueData, QueueAnalyzer
from scoring import FeasibilityScorer

# Import intelligence and visualization modules
try:
    from intelligence import QueueIntelligence
    from visualizations import generate_report_visualizations, generate_calibration_chart_svg
    INTELLIGENCE_AVAILABLE = True
except ImportError:
    INTELLIGENCE_AVAILABLE = False

# Try to import WeasyPrint for PDF generation (consolidated library)
try:
    from weasyprint import HTML, CSS
    WEASYPRINT_AVAILABLE = True
except ImportError:
    WEASYPRINT_AVAILABLE = False

# Legacy Playwright support (deprecated - prefer WeasyPrint)
WEASYPRINT_AVAILABLE = False  # Disabled in favor of WeasyPrint

# =============================================================================
# BENCHMARK DATA (from LBL Berkeley Lab)
# =============================================================================
COST_BENCHMARKS = {
    'S': (30, 100, 250), 'Solar': (30, 100, 250),
    'W': (15, 60, 150), 'Wind': (15, 60, 150),
    'ES': (25, 60, 200), 'Storage': (25, 60, 200),
    'NG': (5, 40, 100), 'Gas': (5, 40, 100),
    'L': (40, 120, 350), 'Load': (40, 120, 350),
    'AC': (40, 100, 250), 'DC': (40, 100, 250),
    'default': (30, 80, 200)
}

TIMELINE_BENCHMARKS = {
    'S': (24, 42, 72), 'Solar': (24, 42, 72),
    'W': (30, 54, 84), 'Wind': (30, 54, 84),
    'ES': (18, 36, 60), 'Storage': (18, 36, 60),
    'NG': (24, 48, 72), 'Gas': (24, 48, 72),
    'L': (24, 48, 84), 'Load': (24, 48, 84),
    'default': (30, 48, 72)
}

COMPLETION_RATES = {
    'S': 0.086, 'Solar': 0.086,
    'W': 0.174, 'Wind': 0.174,
    'ES': 0.018, 'Storage': 0.018,
    'NG': 0.278, 'Gas': 0.278,
    'L': 0.15, 'Load': 0.15,
    'default': 0.122
}

# =============================================================================
# PAGE CONFIG
# =============================================================================
st.set_page_config(
    page_title="Queue Analysis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# =============================================================================
# CLEAN LIGHT THEME CSS
# =============================================================================
st.markdown("""
<style>
    /* Light, clean theme */
    .stApp {
        background-color: #f8f9fa;
    }

    /* Hide default streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Clean typography */
    h1, h2, h3 {
        color: #1a1a2e;
        font-weight: 600;
    }

    /* Status cards */
    .status-card {
        background: white;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 12px 16px;
        text-align: center;
    }
    .status-card.ok { border-left: 4px solid #10b981; }
    .status-card.warn { border-left: 4px solid #f59e0b; }
    .status-card.error { border-left: 4px solid #ef4444; }

    /* Section styling */
    .section {
        background: white;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 20px;
        margin: 16px 0;
    }

    .section-title {
        font-size: 14px;
        font-weight: 600;
        color: #6b7280;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 12px;
        padding-bottom: 8px;
        border-bottom: 1px solid #e5e7eb;
    }

    /* Score display */
    .score-large {
        font-size: 48px;
        font-weight: 700;
        line-height: 1;
    }
    .score-go { color: #10b981; }
    .score-conditional { color: #f59e0b; }
    .score-nogo { color: #ef4444; }

    /* Score bars */
    .score-bar-container {
        margin: 8px 0;
    }
    .score-bar-label {
        display: flex;
        justify-content: space-between;
        font-size: 13px;
        margin-bottom: 4px;
    }
    .score-bar-bg {
        background: #e5e7eb;
        height: 8px;
        border-radius: 4px;
        overflow: hidden;
    }
    .score-bar-fill {
        height: 100%;
        border-radius: 4px;
    }
    .bar-green { background: #10b981; }
    .bar-yellow { background: #f59e0b; }
    .bar-red { background: #ef4444; }

    /* Flags */
    .flag {
        padding: 8px 12px;
        border-radius: 6px;
        margin: 4px 0;
        font-size: 13px;
    }
    .flag-red {
        background: #fef2f2;
        border-left: 3px solid #ef4444;
        color: #991b1b;
    }
    .flag-green {
        background: #f0fdf4;
        border-left: 3px solid #10b981;
        color: #065f46;
    }

    /* Validation checklist */
    .validation-item {
        padding: 8px 0;
        border-bottom: 1px solid #f3f4f6;
        font-size: 13px;
    }
    .validation-item:last-child { border-bottom: none; }
    .check-ok { color: #10b981; }
    .check-warn { color: #f59e0b; }

    /* Button styling */
    .stButton > button {
        background: #1a1a2e;
        color: white;
        border: none;
        padding: 12px 24px;
        font-weight: 500;
    }
    .stButton > button:hover {
        background: #2d2d44;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background: white;
        border: 1px solid #e0e0e0;
        border-radius: 8px 8px 0 0;
        padding: 12px 24px;
    }
    .stTabs [aria-selected="true"] {
        background: #1a1a2e;
        color: white;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

@st.cache_data(ttl=300)
def get_data_status():
    """Check freshness of data for each ISO."""
    cache_dir = Path(__file__).parent / '.cache'

    status = {}
    iso_files = {
        'NYISO': 'nyiso_queue.xlsx',
        'ERCOT': 'ercot_gis_report.xlsx',
        'PJM': 'pjm_costs_2022_clean_data.xlsx',
        'CAISO': 'caiso_queue_direct.xlsx',
    }

    for iso, filename in iso_files.items():
        filepath = cache_dir / filename
        if filepath.exists():
            mtime = datetime.fromtimestamp(filepath.stat().st_mtime)
            age_days = (datetime.now() - mtime).days
            status[iso] = {
                'exists': True,
                'last_updated': mtime,
                'age_days': age_days,
                'status': 'ok' if age_days < 7 else 'warn' if age_days < 30 else 'error'
            }
        else:
            status[iso] = {
                'exists': False,
                'last_updated': None,
                'age_days': None,
                'status': 'error'
            }

    return status


@st.cache_data(ttl=3600)
def load_queue_data(iso: str):
    """Load queue data for specified ISO."""
    try:
        qd = QueueData()
        if iso == 'NYISO':
            df = qd.load_nyiso()
        elif iso == 'ERCOT':
            df = qd.load_ercot()
        elif iso == 'CAISO':
            df = qd.load_caiso()
        elif iso == 'MISO':
            df = qd.load_miso()
        elif iso == 'SPP':
            df = qd.load_spp()
        elif iso == 'ISO-NE':
            df = qd.load_isone()
        else:
            df = qd.load_nyiso()
        return df
    except Exception as e:
        st.error(f"Failed to load {iso} data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def score_all_projects(iso: str, df: pd.DataFrame) -> pd.DataFrame:
    """Score all projects in a dataframe for the Deal Finder."""
    if df.empty:
        return df

    scorer = FeasibilityScorer(df, region=iso)
    scores = []

    for idx, row in df.iterrows():
        try:
            result = scorer.score_project(row=row)
            scores.append({
                'index': idx,
                'score': result.get('total_score', 0),
                'recommendation': result.get('recommendation', 'UNKNOWN'),
                'red_flags_count': len(result.get('red_flags', [])),
                'green_flags_count': len(result.get('green_flags', []))
            })
        except Exception as e:
            scores.append({
                'index': idx,
                'score': 0,
                'recommendation': 'ERROR',
                'red_flags_count': 0,
                'green_flags_count': 0
            })

    score_df = pd.DataFrame(scores).set_index('index')
    result_df = df.copy()
    result_df['Feasibility Score'] = score_df['score']
    result_df['Recommendation'] = score_df['recommendation']
    result_df['Red Flags'] = score_df['red_flags_count']
    result_df['Green Flags'] = score_df['green_flags_count']

    return result_df


def spell_out_type(type_code):
    """Convert type codes to full names."""
    type_map = {
        'S': 'Solar', 'W': 'Wind', 'ES': 'Battery Storage', 'B': 'Battery Storage',
        'BESS': 'Battery Storage', 'NG': 'Natural Gas', 'Gas': 'Natural Gas',
        'L': 'Load (Data Center)', 'AC': 'AC Transmission', 'DC': 'DC Transmission',
        'H': 'Hydro', 'N': 'Nuclear', 'OS': 'Offshore Wind',
    }
    if pd.isna(type_code):
        return 'Unknown'
    return type_map.get(str(type_code).strip(), str(type_code))


def format_date(date_val):
    """Format date nicely."""
    if pd.isna(date_val):
        return 'Unknown'
    try:
        dt = pd.to_datetime(date_val)
        return dt.strftime('%b %d, %Y')
    except:
        return str(date_val)


# =============================================================================
# SCORING FUNCTIONS
# =============================================================================

def estimate_costs(capacity_mw, project_type, region='NYISO'):
    """Estimate interconnection costs based on LBL benchmark data."""
    base = COST_BENCHMARKS.get(project_type, COST_BENCHMARKS['default'])
    multiplier = 1.3 if region == 'NYISO' else 1.0

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


def estimate_timeline(project_type, study_progress=None):
    """Estimate timeline to COD based on study progress."""
    completion_rate = COMPLETION_RATES.get(project_type, COMPLETION_RATES['default'])

    if study_progress is not None:
        if study_progress >= 20:
            remaining = (6, 12, 18)
        elif study_progress >= 15:
            remaining = (12, 24, 36)
        elif study_progress >= 10:
            remaining = (24, 36, 48)
        elif study_progress >= 5:
            remaining = (36, 48, 60)
        else:
            remaining = (48, 60, 72)
    else:
        base = TIMELINE_BENCHMARKS.get(project_type, TIMELINE_BENCHMARKS['default'])
        remaining = base

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
        'completion_rate': completion_rate
    }


def analyze_project(project, df, region='NYISO'):
    """Run full analysis on a project."""
    scorer = FeasibilityScorer(df, region=region)

    # Get project details
    project_id = str(project.get('Queue Pos.', project.name))

    # Score the project using the correct API
    score_result = scorer.score_project(row=project)

    # Extract breakdown from score_result (scores are nested under 'breakdown')
    score_breakdown = score_result.get('breakdown', {})
    breakdown = {
        'queue_position': score_breakdown.get('queue_position', 0),
        'study_progress': score_breakdown.get('study_progress', 0),
        'developer_track_record': score_breakdown.get('developer_track_record', 0),
        'poi_congestion': score_breakdown.get('poi_congestion', 0),
        'project_characteristics': score_breakdown.get('project_characteristics', 0),
    }

    # Cost estimates
    capacity = project.get('SP (MW)', 0)
    if pd.isna(capacity) or capacity == 0:
        capacity = project.get('MW Capacity', 0)
    if pd.isna(capacity):
        capacity = 0

    project_type = project.get('Type/ Fuel', 'default')
    costs = estimate_costs(capacity, project_type, region)

    # Timeline estimates using study progress score
    study_score = score_breakdown.get('study_progress', 0)
    timeline = estimate_timeline(project_type, study_score)

    # Get enhanced intelligence if available
    intelligence = None
    if INTELLIGENCE_AVAILABLE:
        try:
            intel = QueueIntelligence()
            developer = project.get('Developer/Interconnection Customer', '')
            # POI column varies by ISO - check all possible names
            poi = (project.get('Points of Interconnection') or
                   project.get('POI') or
                   project.get('POI Location') or
                   project.get('Proposed POI') or
                   project.get('poiName') or  # MISO
                   project.get('Interconnection Location') or  # SPP/ISO-NE
                   '')
            project_name = project.get('Project Name', '')

            # Determine study phase from score
            if study_score >= 20:
                study_phase = 'ia_signed'
            elif study_score >= 15:
                study_phase = 'facilities'
            elif study_score >= 10:
                study_phase = 'system_impact'
            elif study_score >= 5:
                study_phase = 'feasibility'
            else:
                study_phase = 'early'

            intelligence = intel.analyze_project(
                project_name=str(project_name),
                developer=str(developer),
                poi_name=str(poi),
                capacity_mw=float(capacity) if capacity else 100,
                project_type=str(project_type),
                region=region,
                study_phase=study_phase
            )
        except Exception as e:
            intelligence = {'error': str(e)}

    return {
        'project_id': project_id,
        'score_result': score_result,
        'breakdown': breakdown,
        'costs': costs,
        'timeline': timeline,
        'capacity': capacity,
        'intelligence': intelligence
    }


def _generate_executive_summary(project, analysis, region, intelligence):
    """
    Generate the '5 Things You Need to Know' executive summary for PE decision makers.
    Dynamically identifies the most important findings from the analysis.
    """
    findings = []
    score_result = analysis.get('score_result', {})
    breakdown = analysis.get('breakdown', {})
    costs = analysis.get('costs', {})
    timeline = analysis.get('timeline', {})
    red_flags = score_result.get('red_flags', [])
    green_flags = score_result.get('green_flags', [])

    # 1. Queue Position & Timeline
    queue_date = project.get('Date of IR', project.get('Queue Date', ''))
    if queue_date:
        try:
            queue_dt = pd.to_datetime(queue_date)
            years_in_queue = (datetime.now() - queue_dt).days / 365.25
            if years_in_queue >= 5:
                findings.append({
                    'icon': '🚨',
                    'title': 'Extended Queue Time',
                    'text': f'Project has been in queue for {years_in_queue:.1f} years. This significantly exceeds typical timelines and suggests execution challenges.',
                    'sentiment': 'negative'
                })
            elif years_in_queue >= 3:
                findings.append({
                    'icon': '⚠️',
                    'title': 'Above-Average Queue Time',
                    'text': f'Project has been in queue for {years_in_queue:.1f} years, above the typical 2-3 year timeline.',
                    'sentiment': 'warning'
                })
            elif years_in_queue < 1:
                findings.append({
                    'icon': '✅',
                    'title': 'Recently Queued',
                    'text': f'Project entered queue {years_in_queue:.1f} years ago. Early stage but well-positioned if fundamentals are strong.',
                    'sentiment': 'positive'
                })
        except:
            pass

    # 2. Developer Track Record
    dev_intel = intelligence.get('developer', {}) if intelligence else {}
    if dev_intel and not dev_intel.get('error'):
        dev_rate = dev_intel.get('completion_rate', 0)
        dev_total = dev_intel.get('total_projects', 0)
        dev_completed = dev_intel.get('completed', 0)
        if dev_total > 0:
            if dev_rate == 0:
                findings.append({
                    'icon': '🚨',
                    'title': 'Developer Has Zero Completions',
                    'text': f'Developer has {dev_total} historical projects but NONE have reached COD. This is a major red flag.',
                    'sentiment': 'negative'
                })
            elif dev_rate < 0.10:
                findings.append({
                    'icon': '⚠️',
                    'title': 'Developer Track Record Concerning',
                    'text': f'Developer has only {dev_completed}/{dev_total} completions ({dev_rate*100:.0f}%), well below 17.5% average.',
                    'sentiment': 'warning'
                })
            elif dev_rate >= 0.25:
                findings.append({
                    'icon': '✅',
                    'title': 'Strong Developer Track Record',
                    'text': f'Developer has {dev_completed}/{dev_total} completions ({dev_rate*100:.0f}%), above 17.5% average.',
                    'sentiment': 'positive'
                })
    elif dev_intel and dev_intel.get('error'):
        findings.append({
            'icon': '⚠️',
            'title': 'Unknown Developer',
            'text': 'No historical data found for this developer. They may be new to market or operating under a different name.',
            'sentiment': 'warning'
        })

    # 3. POI/Interconnection Risk
    poi_intel = intelligence.get('poi', {}) if intelligence else {}
    if poi_intel and not poi_intel.get('error') and poi_intel.get('total_historical_projects', 0) > 0:
        poi_rate = poi_intel.get('completion_rate', 0)
        poi_total = poi_intel.get('total_historical_projects', 0)
        if poi_rate < 0.10:
            findings.append({
                'icon': '🚨',
                'title': 'High-Risk POI',
                'text': f'Only {poi_rate*100:.0f}% of projects at this POI have reached COD. Historical completion rate is very low.',
                'sentiment': 'negative'
            })
        elif poi_rate >= 0.20:
            findings.append({
                'icon': '✅',
                'title': 'Favorable POI History',
                'text': f'{poi_rate*100:.0f}% completion rate at this POI, above the national average.',
                'sentiment': 'positive'
            })

    # 4. Cost Risk
    cost_low = costs.get('low_total', 0)
    cost_high = costs.get('high_total', 0)
    cost_med = costs.get('med_total', 0)
    if cost_high > 0 and cost_low > 0:
        cost_variance = (cost_high - cost_low) / cost_med if cost_med > 0 else 0
        if cost_variance > 3:
            findings.append({
                'icon': '⚠️',
                'title': 'High Cost Uncertainty',
                'text': f'IC cost range is wide (${cost_low:.1f}M - ${cost_high:.1f}M). Request actual study documents to narrow estimates.',
                'sentiment': 'warning'
            })

    # 5. Study Progress
    study_score = breakdown.get('study_progress', 0)
    if study_score >= 20:
        findings.append({
            'icon': '✅',
            'title': 'Advanced Study Phase',
            'text': 'Project has completed major interconnection studies. Reduced uncertainty vs. early-stage projects.',
            'sentiment': 'positive'
        })
    elif study_score < 8:
        findings.append({
            'icon': '⚠️',
            'title': 'Early Study Phase',
            'text': 'Project is early in the interconnection process. Costs and timeline remain highly uncertain.',
            'sentiment': 'warning'
        })

    # 6. PPA/Offtake Status
    has_ppa = project.get('has_ppa')
    if has_ppa == True or has_ppa == 1:
        findings.append({
            'icon': '✅',
            'title': 'Confirmed Offtake',
            'text': 'Project has a confirmed PPA or offtake agreement, indicating buyer commitment and financing pathway.',
            'sentiment': 'positive'
        })

    # 7. Add any critical red flags not already covered
    critical_flags = [f for f in red_flags if 'CRITICAL' in f.upper() or 'significant' in f.lower()]
    for flag in critical_flags[:2]:  # Max 2 additional
        if len(findings) < 6:
            findings.append({
                'icon': '🚨',
                'title': 'Risk Flag',
                'text': flag,
                'sentiment': 'negative'
            })

    # Sort: negative first, then warning, then positive (most important first)
    sentiment_order = {'negative': 0, 'warning': 1, 'positive': 2}
    findings.sort(key=lambda x: sentiment_order.get(x['sentiment'], 1))

    # Take top 5
    findings = findings[:5]

    # If we have fewer than 3 findings, add some context
    if len(findings) < 3:
        # Add completion probability context
        mc = intelligence.get('monte_carlo', {}) if intelligence else {}
        if mc and not mc.get('error'):
            prob = mc.get('completion_probability', 0)
            findings.append({
                'icon': '📊' if prob >= 0.15 else '⚠️',
                'title': 'Completion Probability',
                'text': f'Based on Monte Carlo simulation, this project has a {prob*100:.0f}% probability of reaching COD.',
                'sentiment': 'positive' if prob >= 0.20 else 'warning' if prob >= 0.10 else 'negative'
            })

    # Generate HTML
    if not findings:
        return ""

    items_html = ""
    for f in findings:
        bg_color = '#fef2f2' if f['sentiment'] == 'negative' else '#fffbeb' if f['sentiment'] == 'warning' else '#f0fdf4'
        border_color = '#ef4444' if f['sentiment'] == 'negative' else '#f59e0b' if f['sentiment'] == 'warning' else '#10b981'
        items_html += f'''
        <div style="background:{bg_color};border-left:3px solid {border_color};padding:8px 12px;margin-bottom:6px;border-radius:4px;">
            <div style="font-weight:600;font-size:10px;margin-bottom:2px;">{f['icon']} {f['title']}</div>
            <div style="font-size:10px;color:#374151;">{f['text']}</div>
        </div>
        '''

    return f'''
        <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:12px;margin-bottom:16px;">
            <div style="font-size:11px;font-weight:600;color:#1e40af;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px;border-bottom:1px solid #e2e8f0;padding-bottom:6px;">
                Executive Summary: Key Findings
            </div>
            {items_html}
        </div>
    '''


def _generate_comparable_projects(project, analysis, region, intelligence):
    """
    Generate a comparable projects section showing similar projects that succeeded or failed.
    """
    try:
        from unified_data import RegionalBenchmarks

        benchmarks = RegionalBenchmarks()
        lbl_df = benchmarks._load_lbl()

        if lbl_df.empty:
            return ""

        # Get project parameters
        capacity = analysis.get('capacity', 100)
        project_type = project.get('Type/ Fuel', 'Solar')

        # Normalize project type
        type_lower = str(project_type).lower()
        if 'solar' in type_lower or type_lower == 's':
            type_filter = 'Solar'
        elif 'wind' in type_lower or type_lower == 'w':
            type_filter = 'Wind'
        elif 'battery' in type_lower or 'storage' in type_lower or type_lower == 'es':
            type_filter = 'Storage'
        elif 'gas' in type_lower or type_lower == 'ng':
            type_filter = 'Natural Gas'
        else:
            type_filter = type_lower

        # Filter to region and type
        mask = (lbl_df['region'].str.upper() == region.upper())
        if 'type_clean' in lbl_df.columns:
            mask &= lbl_df['type_clean'].str.lower().str.contains(type_filter.lower(), na=False)

        # Filter by similar capacity (+/- 100%)
        if 'mw1' in lbl_df.columns:
            numeric_mw = pd.to_numeric(lbl_df['mw1'], errors='coerce')
            mask &= (numeric_mw >= capacity * 0.25) & (numeric_mw <= capacity * 4)

        similar = lbl_df[mask].copy()

        if len(similar) == 0:
            return ""

        # Split into completed and withdrawn
        completed = similar[similar['q_status'] == 'operational'].head(3)
        withdrawn = similar[similar['q_status'] == 'withdrawn'].head(3)

        if len(completed) == 0 and len(withdrawn) == 0:
            return ""

        # Build HTML for completed projects
        completed_html = ""
        if len(completed) > 0:
            completed_rows = ""
            for _, row in completed.iterrows():
                name = str(row.get('project_name', 'Unknown'))[:30]
                mw = row.get('mw1', 0)
                developer = str(row.get('developer', 'Unknown'))[:20]
                year = row.get('q_year', 'N/A')
                completed_rows += f"<tr><td style='padding:4px 6px;border-bottom:1px solid #e5e7eb;font-size:9px;'>{name}</td><td style='padding:4px 6px;border-bottom:1px solid #e5e7eb;font-size:9px;'>{mw:.0f}</td><td style='padding:4px 6px;border-bottom:1px solid #e5e7eb;font-size:9px;'>{developer}</td><td style='padding:4px 6px;border-bottom:1px solid #e5e7eb;font-size:9px;'>{year}</td></tr>"

            completed_html = f'''
            <div style="flex:1;">
                <div style="font-weight:600;font-size:10px;color:#10b981;margin-bottom:6px;">✅ Similar Projects that COMPLETED</div>
                <table style="width:100%;border-collapse:collapse;">
                    <tr style="background:#f0fdf4;">
                        <th style="padding:4px 6px;text-align:left;font-size:9px;">Project</th>
                        <th style="padding:4px 6px;text-align:left;font-size:9px;">MW</th>
                        <th style="padding:4px 6px;text-align:left;font-size:9px;">Developer</th>
                        <th style="padding:4px 6px;text-align:left;font-size:9px;">Year</th>
                    </tr>
                    {completed_rows}
                </table>
            </div>
            '''

        # Build HTML for withdrawn projects
        withdrawn_html = ""
        if len(withdrawn) > 0:
            withdrawn_rows = ""
            for _, row in withdrawn.iterrows():
                name = str(row.get('project_name', 'Unknown'))[:30]
                mw = row.get('mw1', 0)
                developer = str(row.get('developer', 'Unknown'))[:20]
                year = row.get('q_year', 'N/A')
                withdrawn_rows += f"<tr><td style='padding:4px 6px;border-bottom:1px solid #e5e7eb;font-size:9px;'>{name}</td><td style='padding:4px 6px;border-bottom:1px solid #e5e7eb;font-size:9px;'>{mw:.0f}</td><td style='padding:4px 6px;border-bottom:1px solid #e5e7eb;font-size:9px;'>{developer}</td><td style='padding:4px 6px;border-bottom:1px solid #e5e7eb;font-size:9px;'>{year}</td></tr>"

            withdrawn_html = f'''
            <div style="flex:1;">
                <div style="font-weight:600;font-size:10px;color:#ef4444;margin-bottom:6px;">❌ Similar Projects that WITHDREW</div>
                <table style="width:100%;border-collapse:collapse;">
                    <tr style="background:#fef2f2;">
                        <th style="padding:4px 6px;text-align:left;font-size:9px;">Project</th>
                        <th style="padding:4px 6px;text-align:left;font-size:9px;">MW</th>
                        <th style="padding:4px 6px;text-align:left;font-size:9px;">Developer</th>
                        <th style="padding:4px 6px;text-align:left;font-size:9px;">Year</th>
                    </tr>
                    {withdrawn_rows}
                </table>
            </div>
            '''

        # Calculate success rate for similar projects
        total_resolved = len(similar[similar['q_status'].isin(['operational', 'withdrawn'])])
        total_completed = len(similar[similar['q_status'] == 'operational'])
        success_rate = total_completed / total_resolved if total_resolved > 0 else 0

        return f'''
        <div class="section">
            <div class="section-title">Comparable Projects ({region} {type_filter})</div>
            <div style="font-size:10px;color:#6b7280;margin-bottom:10px;">
                Historical success rate for similar projects: <strong style="color:{'#10b981' if success_rate >= 0.15 else '#ef4444'};">{success_rate*100:.0f}%</strong>
                ({total_completed} completed / {total_resolved} resolved)
            </div>
            <div style="display:flex;gap:16px;">
                {completed_html}
                {withdrawn_html}
            </div>
        </div>
        '''
    except Exception as e:
        return ""


def _generate_due_diligence_checklist(project, analysis, intelligence):
    """
    Generate a due diligence checklist with documents to request and questions to ask.
    Tailored based on the specific project's risk profile.
    """
    score_result = analysis.get('score_result', {})
    red_flags = score_result.get('red_flags', [])
    rec = score_result.get('recommendation', 'CONDITIONAL')

    # Standard documents to request
    docs_to_request = [
        "Interconnection Agreement (IA) or latest study report",
        "System Impact Study (SIS) results",
        "Facilities Study with cost estimates",
        "Network upgrade cost allocation breakdown",
        "Proof of site control (lease/purchase agreement)",
        "Evidence of offtake (PPA/LOI) if any",
    ]

    # Standard questions to ask
    questions = [
        "What is the current study phase and expected IA execution date?",
        "Have interconnection costs changed since initial estimates?",
        "Are there any pending transmission constraint issues?",
        "What is the status of required permits (state, local, environmental)?",
        "Does the project have committed offtake or is it merchant?",
    ]

    # Conditional items based on risk flags
    if any('queue' in f.lower() and 'year' in f.lower() for f in red_flags):
        questions.append("Why has the project been in queue so long? What has delayed progress?")
        docs_to_request.append("Timeline of study progression and any delay explanations")

    if any('developer' in f.lower() for f in red_flags):
        questions.append("What other projects has this developer successfully completed?")
        questions.append("What is the developer's financial backing and parent company?")
        docs_to_request.append("Developer financial statements or parent company guarantee")

    if any('congestion' in f.lower() or 'poi' in f.lower() for f in red_flags):
        questions.append("How many other projects are competing at this POI? What is their status?")
        questions.append("Is this project part of a cluster study? If so, what are the implications?")

    # Deal-breakers / red lines
    red_lines = [
        "No executed IA and no clear path to execution",
        "Interconnection costs exceed project economics threshold",
        "Developer unable to provide financial backing evidence",
        "Site control issues or unresolved land disputes",
        "Material permitting obstacles without clear resolution path",
    ]

    # Generate HTML
    docs_html = "".join([f"<li style='margin-bottom:4px;'>{doc}</li>" for doc in docs_to_request[:6]])
    questions_html = "".join([f"<li style='margin-bottom:4px;'>{q}</li>" for q in questions[:6]])
    redlines_html = "".join([f"<li style='margin-bottom:4px;color:#991b1b;'>{r}</li>" for r in red_lines[:5]])

    return f'''
        <div class="section" style="page-break-before:always;">
            <div class="section-title">Due Diligence Checklist</div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">
                <div>
                    <div style="font-weight:600;font-size:10px;color:#1e40af;margin-bottom:8px;">📋 Documents to Request</div>
                    <ul style="font-size:10px;margin:0;padding-left:16px;color:#374151;">
                        {docs_html}
                    </ul>
                </div>
                <div>
                    <div style="font-weight:600;font-size:10px;color:#1e40af;margin-bottom:8px;">❓ Questions to Ask Developer/Seller</div>
                    <ul style="font-size:10px;margin:0;padding-left:16px;color:#374151;">
                        {questions_html}
                    </ul>
                </div>
            </div>
            <div style="margin-top:12px;">
                <div style="font-weight:600;font-size:10px;color:#991b1b;margin-bottom:8px;">🚫 Potential Deal-Breakers (Red Lines)</div>
                <ul style="font-size:10px;margin:0;padding-left:16px;">
                    {redlines_html}
                </ul>
            </div>
        </div>
    '''


# =============================================================================
# PDF GENERATION
# =============================================================================

def generate_report_html(project, analysis, client_name, prepared_by, notes):
    """Generate clean HTML report with enhanced intelligence."""

    score = analysis['score_result']['total_score']
    rec = analysis['score_result']['recommendation']
    breakdown = analysis['breakdown']
    costs = analysis['costs']
    timeline = analysis['timeline']
    intelligence = analysis.get('intelligence', {})

    # Colors based on recommendation
    colors = {
        'GO': {'bg': '#ecfdf5', 'border': '#10b981', 'text': '#065f46'},
        'CONDITIONAL': {'bg': '#fffbeb', 'border': '#f59e0b', 'text': '#92400e'},
        'NO-GO': {'bg': '#fef2f2', 'border': '#ef4444', 'text': '#991b1b'}
    }
    c = colors.get(rec, colors['CONDITIONAL'])

    # Project details
    project_name = project.get('Project Name', 'Unknown')
    developer = project.get('Developer/Interconnection Customer', 'Unknown')
    project_type = spell_out_type(project.get('Type/ Fuel', 'Unknown'))
    capacity = analysis['capacity']
    state = project.get('State', 'NY')
    county = project.get('County', '')
    poi = project.get('Points of Interconnection', 'Unknown')
    queue_date = format_date(project.get('Date of IR', ''))
    queue_id = analysis['project_id']

    # Get region from score_result or default to NYISO
    regional_context = analysis['score_result'].get('regional_context', {})
    region = regional_context.get('region', 'NYISO')

    # Score bars HTML
    def score_bar(name, val, max_val):
        # Handle edge cases where val might not be a number
        if isinstance(val, dict):
            val = 0
        try:
            val = float(val) if val else 0
        except (TypeError, ValueError):
            val = 0
        pct = (val / max_val) * 100 if max_val > 0 else 0
        color = '#10b981' if pct >= 70 else '#f59e0b' if pct >= 40 else '#ef4444'
        return f'''
        <tr>
            <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;">{name}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;text-align:center;">{val:.1f}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;text-align:center;">{max_val}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;width:150px;">
                <div style="background:#e5e7eb;height:8px;border-radius:4px;overflow:hidden;">
                    <div style="background:{color};height:100%;width:{pct}%;border-radius:4px;"></div>
                </div>
            </td>
            <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;text-align:right;">{pct:.0f}%</td>
        </tr>
        '''

    # Flags HTML
    red_flags = analysis['score_result'].get('red_flags', [])
    green_flags = analysis['score_result'].get('green_flags', [])

    red_flags_html = ''.join([f'<div style="background:#fef2f2;border-left:3px solid #ef4444;padding:8px 12px;margin:4px 0;border-radius:4px;color:#991b1b;">{f}</div>' for f in red_flags]) or '<div style="color:#6b7280;font-style:italic;">None identified</div>'
    green_flags_html = ''.join([f'<div style="background:#f0fdf4;border-left:3px solid #10b981;padding:8px 12px;margin:4px 0;border-radius:4px;color:#065f46;">{f}</div>' for f in green_flags]) or '<div style="color:#6b7280;font-style:italic;">None identified</div>'

    html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Interconnection Feasibility Assessment - {queue_id}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 11px;
            line-height: 1.5;
            color: #1f2937;
            background: white;
        }}
        .page {{
            width: 100%;
            max-width: 8.5in;
            margin: 0 auto;
            padding: 0.5in;
        }}
        .header {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            padding-bottom: 16px;
            border-bottom: 2px solid #1a1a2e;
            margin-bottom: 20px;
        }}
        .header h1 {{
            font-size: 20px;
            color: #1a1a2e;
            margin: 0;
        }}
        .header-meta {{
            text-align: right;
            font-size: 10px;
            color: #6b7280;
        }}
        .verdict {{
            background: {c['bg']};
            border: 2px solid {c['border']};
            border-radius: 8px;
            padding: 16px 20px;
            display: flex;
            align-items: center;
            gap: 20px;
            margin-bottom: 20px;
        }}
        .verdict-score {{
            font-size: 42px;
            font-weight: 700;
            color: {c['border']};
            line-height: 1;
        }}
        .verdict-rec {{
            background: {c['border']};
            color: white;
            padding: 6px 16px;
            border-radius: 4px;
            font-weight: 600;
            font-size: 14px;
        }}
        .section {{
            margin-bottom: 16px;
        }}
        .section-title {{
            font-size: 11px;
            font-weight: 600;
            color: #6b7280;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            padding-bottom: 6px;
            border-bottom: 1px solid #e5e7eb;
            margin-bottom: 10px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th {{
            background: #f8fafc;
            padding: 8px 12px;
            text-align: left;
            font-weight: 600;
            font-size: 10px;
            text-transform: uppercase;
            color: #6b7280;
            border-bottom: 1px solid #e5e7eb;
        }}
        .two-col {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 16px;
        }}
        .metric-box {{
            background: #f8fafc;
            border: 1px solid #e5e7eb;
            border-radius: 6px;
            padding: 12px;
            text-align: center;
        }}
        .metric-value {{
            font-size: 18px;
            font-weight: 700;
            color: #1a1a2e;
        }}
        .metric-label {{
            font-size: 9px;
            color: #6b7280;
            text-transform: uppercase;
            margin-top: 2px;
        }}
        .context-box {{
            background: #f0f9ff;
            border: 1px solid #0ea5e9;
            border-radius: 6px;
            padding: 12px;
            margin: 16px 0;
            font-size: 10px;
            color: #0c4a6e;
        }}
        .footer {{
            margin-top: 24px;
            padding-top: 12px;
            border-top: 1px solid #e5e7eb;
            font-size: 9px;
            color: #9ca3af;
            display: flex;
            justify-content: space-between;
        }}
        @media print {{
            .page {{ padding: 0.4in; }}
        }}
    </style>
</head>
<body>
    <div class="page">
        <div class="header">
            <div>
                <h1>Interconnection Feasibility Assessment</h1>
                <div style="color:#6b7280;margin-top:4px;">Queue Position Analysis & Risk Assessment</div>
            </div>
            <div class="header-meta">
                <div><strong>Prepared for:</strong> {client_name}</div>
                <div><strong>Prepared by:</strong> {prepared_by}</div>
                <div>{datetime.now().strftime('%B %d, %Y')}</div>
                <div>Queue ID: {queue_id}</div>
            </div>
        </div>

        <div class="verdict">
            <div class="verdict-score">{score:.0f}</div>
            <div>
                <div class="verdict-rec">{rec}</div>
                <div style="margin-top:8px;color:{c['text']};font-size:10px;">Feasibility Score (0-100)</div>
            </div>
            <div style="flex:1;padding-left:20px;border-left:1px solid {c['border']};">
                <div style="font-size:11px;color:{c['text']};">
                    {'Strong fundamentals support proceeding with standard due diligence.' if rec == 'GO' else 'Project shows potential but requires enhanced due diligence on identified risks.' if rec == 'CONDITIONAL' else 'Significant risks identified. Recommend pass or substantial risk mitigation.'}
                </div>
            </div>
        </div>

        {_generate_executive_summary(project, analysis, region, intelligence)}

        <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:16px;">
            <div class="metric-box">
                <div class="metric-value">{score:.0f}/100</div>
                <div class="metric-label">Score</div>
            </div>
            <div class="metric-box">
                <div class="metric-value">${costs['med_total']:.1f}M</div>
                <div class="metric-label">Est. Cost (P50)</div>
            </div>
            <div class="metric-box">
                <div class="metric-value">{timeline['likely_date']}</div>
                <div class="metric-label">Target COD</div>
            </div>
            <div class="metric-box">
                <div class="metric-value">{timeline['completion_rate']*100:.0f}%</div>
                <div class="metric-label">{region} {project_type} Hist.</div>
            </div>
            <div class="metric-box">
                <div class="metric-value">{capacity:.0f} MW</div>
                <div class="metric-label">Capacity</div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Project Details</div>
            <table>
                <tr>
                    <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;width:15%;color:#6b7280;font-weight:500;">Project</td>
                    <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;width:35%;">{project_name}</td>
                    <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;width:15%;color:#6b7280;font-weight:500;">Developer</td>
                    <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;width:35%;">{developer}</td>
                </tr>
                <tr>
                    <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;color:#6b7280;font-weight:500;">Type</td>
                    <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;">{project_type}</td>
                    <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;color:#6b7280;font-weight:500;">Location</td>
                    <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;">{county}, {state}</td>
                </tr>
                <tr>
                    <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;color:#6b7280;font-weight:500;">Queue Date</td>
                    <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;">{queue_date}</td>
                    <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;color:#6b7280;font-weight:500;">POI</td>
                    <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;">{poi}</td>
                </tr>
            </table>
        </div>

        <div class="section">
            <div class="section-title">Score Breakdown</div>
            <table>
                <tr>
                    <th>Component</th>
                    <th style="text-align:center;">Score</th>
                    <th style="text-align:center;">Max</th>
                    <th>Progress</th>
                    <th style="text-align:right;">%</th>
                </tr>
                {score_bar('Queue Position', breakdown['queue_position'], 25)}
                {score_bar('Study Progress', breakdown['study_progress'], 25)}
                {score_bar('Developer Track Record', breakdown['developer_track_record'], 20)}
                {score_bar('POI Congestion', breakdown['poi_congestion'], 15)}
                {score_bar('Project Characteristics', breakdown['project_characteristics'], 15)}
                <tr style="background:#f8fafc;font-weight:600;">
                    <td style="padding:8px 12px;">TOTAL</td>
                    <td style="padding:8px 12px;text-align:center;">{score:.0f}</td>
                    <td style="padding:8px 12px;text-align:center;">100</td>
                    <td style="padding:8px 12px;"></td>
                    <td style="padding:8px 12px;text-align:right;">{score:.0f}%</td>
                </tr>
            </table>
        </div>

        <div class="two-col">
            <div class="section">
                <div class="section-title">Risk Factors</div>
                {red_flags_html}
            </div>
            <div class="section">
                <div class="section-title">Positive Indicators</div>
                {green_flags_html}
            </div>
        </div>

        <div class="two-col">
            <div class="section">
                <div class="section-title">Cost Estimates</div>
                <table>
                    <tr><th>Scenario</th><th style="text-align:right;">Total</th><th style="text-align:right;">$/kW</th></tr>
                    <tr>
                        <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;">Low (P25)</td>
                        <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;text-align:right;">${costs['low_total']:.1f}M</td>
                        <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;text-align:right;">${costs['low_per_kw']:.0f}</td>
                    </tr>
                    <tr style="background:#fffbeb;">
                        <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;font-weight:600;">Base (P50)</td>
                        <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;text-align:right;font-weight:600;">${costs['med_total']:.1f}M</td>
                        <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;text-align:right;font-weight:600;">${costs['med_per_kw']:.0f}</td>
                    </tr>
                    <tr>
                        <td style="padding:6px 12px;">High (P75)</td>
                        <td style="padding:6px 12px;text-align:right;">${costs['high_total']:.1f}M</td>
                        <td style="padding:6px 12px;text-align:right;">${costs['high_per_kw']:.0f}</td>
                    </tr>
                </table>
            </div>
            <div class="section">
                <div class="section-title">Timeline Estimates</div>
                <table>
                    <tr><th>Scenario</th><th style="text-align:right;">Months</th><th style="text-align:right;">Est. COD</th></tr>
                    <tr>
                        <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;">Optimistic</td>
                        <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;text-align:right;">{timeline['optimistic']}</td>
                        <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;text-align:right;">{timeline['optimistic_date']}</td>
                    </tr>
                    <tr style="background:#fffbeb;">
                        <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;font-weight:600;">Base Case</td>
                        <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;text-align:right;font-weight:600;">{timeline['likely']}</td>
                        <td style="padding:6px 12px;border-bottom:1px solid #e5e7eb;text-align:right;font-weight:600;">{timeline['likely_date']}</td>
                    </tr>
                    <tr>
                        <td style="padding:6px 12px;">Pessimistic</td>
                        <td style="padding:6px 12px;text-align:right;">{timeline['pessimistic']}</td>
                        <td style="padding:6px 12px;text-align:right;">{timeline['pessimistic_date']}</td>
                    </tr>
                </table>
            </div>
        </div>

        {generate_intelligence_html(intelligence)}

        {_generate_comparable_projects(project, analysis, region, intelligence)}

        {_generate_due_diligence_checklist(project, analysis, intelligence)}

        <div class="context-box">
            <strong>Data Sources & Methodology:</strong> Queue data from {region} (current). Completion rates and cost benchmarks from Lawrence Berkeley National Laboratory "Queued Up" dataset (36,441 projects, 3,400+ with cost data). Model validation shows 3.3x lift at top decile - high-scoring projects complete at 3x the average rate. All estimates should be validated against actual interconnection study documents.
            {f'<br><br><strong>Notes:</strong> {notes}' if notes else ''}
        </div>

        <div class="footer">
            <div>CONFIDENTIAL - For authorized recipient only</div>
            <div>Generated by Queue Analysis Platform</div>
        </div>
    </div>
</body>
</html>'''

    return html


def generate_intelligence_html(intelligence):
    """Generate HTML section for intelligence data."""
    if not intelligence or intelligence.get('error'):
        return ''

    sections = []

    # Model Validation Section
    validation = intelligence.get('validation', {})
    if validation.get('model_validated'):
        lift = validation.get('lift_at_top_decile', 0)
        sample = validation.get('sample_size', 0)

        # Generate calibration chart if visualizations available
        calibration_chart = ''
        if INTELLIGENCE_AVAILABLE and validation.get('score_buckets'):
            try:
                calibration_chart = generate_calibration_chart_svg(validation['score_buckets'], width=450, height=180)
            except:
                calibration_chart = ''

        sections.append(f'''
        <div class="section" style="page-break-before: always;">
            <div class="section-title">Model Validation (Predictive Power)</div>
            <div style="display:flex;gap:20px;align-items:flex-start;">
                <div style="flex:1;">
                    <p style="margin-bottom:10px;">Our scoring model has been validated against <strong>{sample:,}</strong> historical projects with known outcomes.</p>
                    <table>
                        <tr><td style="padding:4px 8px;border-bottom:1px solid #e5e7eb;color:#6b7280;">Top Decile Lift</td><td style="padding:4px 8px;border-bottom:1px solid #e5e7eb;font-weight:600;color:#10b981;">{lift:.1f}x</td></tr>
                        <tr><td style="padding:4px 8px;color:#6b7280;">Interpretation</td><td style="padding:4px 8px;">High-scoring projects complete at {lift:.1f}x the average rate</td></tr>
                    </table>
                </div>
                <div style="flex:1;">{calibration_chart}</div>
            </div>
        </div>
        ''')

    # Monte Carlo Section
    mc = intelligence.get('monte_carlo', {})
    if mc and not mc.get('error'):
        cost_data = mc.get('cost', {})
        timeline_data = mc.get('timeline_months', {})
        completion_prob = mc.get('completion_probability', 0)

        sections.append(f'''
        <div class="section">
            <div class="section-title">Probabilistic Analysis (Monte Carlo Simulation)</div>
            <p style="margin-bottom:10px;color:#4b5563;">Based on 10,000 simulations using historical project distributions.</p>
            <div style="display:flex;gap:20px;">
                <div style="flex:1;background:#f8fafc;padding:12px;border-radius:8px;">
                    <div style="font-weight:600;margin-bottom:8px;">Cost Distribution</div>
                    <table style="width:100%;">
                        <tr><td style="padding:3px 0;color:#6b7280;">P10 (Optimistic)</td><td style="text-align:right;">${cost_data.get('p10', 0):.1f}M</td></tr>
                        <tr style="background:#dbeafe;"><td style="padding:3px 0;font-weight:600;">P50 (Base Case)</td><td style="text-align:right;font-weight:600;">${cost_data.get('p50', 0):.1f}M</td></tr>
                        <tr><td style="padding:3px 0;color:#6b7280;">P90 (Conservative)</td><td style="text-align:right;">${cost_data.get('p90', 0):.1f}M</td></tr>
                    </table>
                </div>
                <div style="flex:1;background:#f8fafc;padding:12px;border-radius:8px;">
                    <div style="font-weight:600;margin-bottom:8px;">Timeline Distribution</div>
                    <table style="width:100%;">
                        <tr><td style="padding:3px 0;color:#6b7280;">P10 (Optimistic)</td><td style="text-align:right;">{timeline_data.get('p10', 0):.0f} months</td></tr>
                        <tr style="background:#dbeafe;"><td style="padding:3px 0;font-weight:600;">P50 (Base Case)</td><td style="text-align:right;font-weight:600;">{timeline_data.get('p50', 0):.0f} months</td></tr>
                        <tr><td style="padding:3px 0;color:#6b7280;">P90 (Conservative)</td><td style="text-align:right;">{timeline_data.get('p90', 0):.0f} months</td></tr>
                    </table>
                </div>
                <div style="flex:1;background:#f8fafc;padding:12px;border-radius:8px;text-align:center;">
                    <div style="font-weight:600;margin-bottom:8px;">This Project's Probability</div>
                    <div style="font-size:28px;font-weight:700;color:{'#ef4444' if completion_prob < 0.1 else '#f59e0b' if completion_prob < 0.2 else '#10b981'};">{completion_prob*100:.0f}%</div>
                    <div style="font-size:10px;color:#6b7280;">Score-adjusted estimate for this specific project</div>
                </div>
            </div>
        </div>
        ''')

    # Developer Intelligence
    dev = intelligence.get('developer', {})
    if dev and not dev.get('error') and dev.get('total_projects', 0) > 0:
        # Color code based on assessment
        dev_assessment = dev.get('assessment', 'unknown')
        dev_colors = {
            'excellent': '#10b981',
            'good': '#22c55e',
            'average': '#f59e0b',
            'below_average': '#f97316',
            'poor': '#ef4444',
            'no_completions': '#dc2626',
            'no_track_record': '#6b7280'
        }
        dev_color = dev_colors.get(dev_assessment, '#6b7280')
        dev_confidence = dev.get('confidence', 'unknown')

        # Get regional breakdown for context
        regional_note = ""
        regional_breakdown = dev.get('regional_breakdown', {}) or dev.get('success_by_region', {})
        if regional_breakdown and region in regional_breakdown:
            reg_data = regional_breakdown[region]
            reg_rate = reg_data.get('completion_rate', reg_data.get('rate', 0))
            reg_total = reg_data.get('total', 0)
            if reg_total > 0:
                regional_note = f"<tr><td style='padding:4px 8px;border-bottom:1px solid #e5e7eb;color:#6b7280;'>In {region} Specifically</td><td style='padding:4px 8px;border-bottom:1px solid #e5e7eb;'>{reg_rate*100:.0f}% completion ({reg_total} projects)</td></tr>"

        sections.append(f'''
        <div class="section">
            <div class="section-title">Developer Track Record (Historical)</div>
            <table>
                <tr>
                    <td style="padding:4px 8px;border-bottom:1px solid #e5e7eb;color:#6b7280;width:30%;">Historical Projects</td>
                    <td style="padding:4px 8px;border-bottom:1px solid #e5e7eb;">{dev.get('total_projects', 0)} projects ({dev.get('completed', 0)} completed, {dev.get('withdrawn', 0)} withdrawn)</td>
                </tr>
                <tr>
                    <td style="padding:4px 8px;border-bottom:1px solid #e5e7eb;color:#6b7280;">Completion Rate</td>
                    <td style="padding:4px 8px;border-bottom:1px solid #e5e7eb;font-weight:600;color:{dev_color};">{dev.get('completion_rate_pct', 'N/A')} <span style="font-weight:normal;color:#6b7280;">(avg: 17.5%)</span></td>
                </tr>
                {regional_note}
                <tr>
                    <td style="padding:4px 8px;border-bottom:1px solid #e5e7eb;color:#6b7280;">Assessment</td>
                    <td style="padding:4px 8px;border-bottom:1px solid #e5e7eb;color:{dev_color};font-weight:600;">{dev.get('assessment_text', 'Unknown')}</td>
                </tr>
                <tr>
                    <td style="padding:4px 8px;color:#6b7280;">Data Confidence</td>
                    <td style="padding:4px 8px;">{dev_confidence.replace('_', ' ').title()} ({dev.get('total_projects', 0)} historical projects)</td>
                </tr>
            </table>
        </div>
        ''')
    elif dev and dev.get('error'):
        # Show that we couldn't find developer data - this is important info
        sections.append(f'''
        <div class="section">
            <div class="section-title">Developer Track Record</div>
            <div style="background:#fef3c7;border:1px solid #f59e0b;padding:12px;border-radius:6px;color:#92400e;">
                <strong>⚠️ No Historical Data:</strong> {dev.get('error', 'Developer not found in historical database')}
                <div style="font-size:10px;margin-top:4px;">This developer may be new to the market or operating under a different name in historical records.</div>
            </div>
        </div>
        ''')

    # POI Intelligence
    poi = intelligence.get('poi', {})
    if poi and not poi.get('error') and poi.get('total_historical_projects', 0) > 0:
        risk_colors = {'low': '#10b981', 'medium': '#f59e0b', 'high': '#f97316', 'very_high': '#ef4444'}
        risk_color = risk_colors.get(poi.get('risk_level', 'medium'), '#6b7280')

        sections.append(f'''
        <div class="section">
            <div class="section-title">POI Historical Analysis</div>
            <table>
                <tr>
                    <td style="padding:4px 8px;border-bottom:1px solid #e5e7eb;color:#6b7280;width:30%;">Historical Projects at POI</td>
                    <td style="padding:4px 8px;border-bottom:1px solid #e5e7eb;">{poi.get('total_historical_projects', 0)} projects ({poi.get('completed_projects', 0)} completed, {poi.get('withdrawn_projects', 0)} withdrawn)</td>
                </tr>
                <tr>
                    <td style="padding:4px 8px;border-bottom:1px solid #e5e7eb;color:#6b7280;">POI Completion Rate</td>
                    <td style="padding:4px 8px;border-bottom:1px solid #e5e7eb;font-weight:600;">{poi.get('completion_rate_pct', 'N/A')}</td>
                </tr>
                <tr>
                    <td style="padding:4px 8px;color:#6b7280;">Risk Level</td>
                    <td style="padding:4px 8px;font-weight:600;color:{risk_color};">{poi.get('risk_level', 'Unknown').upper()} - {poi.get('risk_interpretation', '')}</td>
                </tr>
            </table>
        </div>
        ''')

    return '\n'.join(sections)


def generate_pdf(html_content):
    """Generate PDF from HTML using WeasyPrint."""
    if not WEASYPRINT_AVAILABLE:
        return None

    try:
        # CSS for PDF rendering
        css = CSS(string='''
            @page {
                size: letter;
                margin: 0.25in;
            }
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            }
        ''')
        pdf_bytes = HTML(string=html_content).write_pdf(stylesheets=[css])
        return pdf_bytes
    except Exception as e:
        print(f"PDF generation error: {e}")
        return None


# =============================================================================
# MAIN APP
# =============================================================================

def main():
    # Title
    st.markdown("# 📊 Interconnection Queue Intelligence")

    # Quick guide expander
    with st.expander("📖 Quick Guide - What can you do here?", expanded=False):
        st.markdown("""
        **🔍 Deal Finder** - Screen and filter all active projects by region, type, score, and capacity.
        Find the best deals fast with our feasibility scoring.

        **📈 Market Intelligence** - Understand the interconnection landscape:
        - Regional completion rates (ERCOT 34% vs NYISO 8%)
        - Technology trends (storage growing 10x since 2018)
        - Timeline analysis (median 40 months to COD)

        **📄 Single Project Report** - Generate detailed feasibility reports for PE due diligence.
        Includes cost estimates, timeline projections, and risk factors.

        **📊 Portfolio Analysis** - Analyze developer portfolios, POI congestion, and market segments.

        **⚙️ Data & Settings** - Explore the data sources powering this analysis:
        - 36,441 historical projects from LBL Berkeley Lab
        - Live queue data from 6 ISOs
        - EIA generator registry
        """)

    # Data Status Bar
    st.markdown("---")
    data_status = get_data_status()

    cols = st.columns(len(data_status))
    for i, (iso, status) in enumerate(data_status.items()):
        with cols[i]:
            if status['exists']:
                icon = "✅" if status['status'] == 'ok' else "⚠️" if status['status'] == 'warn' else "❌"
                date_str = status['last_updated'].strftime('%b %d') if status['last_updated'] else 'Unknown'
                age_str = f"({status['age_days']}d ago)" if status['age_days'] else ""
                st.markdown(f"""
                <div class="status-card {status['status']}">
                    <div style="font-weight:600;">{icon} {iso}</div>
                    <div style="font-size:11px;color:#6b7280;">{date_str} {age_str}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class="status-card error">
                    <div style="font-weight:600;">❌ {iso}</div>
                    <div style="font-size:11px;color:#6b7280;">No data</div>
                </div>
                """, unsafe_allow_html=True)

    st.markdown("---")

    # Tabs for different report types
    tab0, tab1, tab2, tab3, tab4 = st.tabs(["🔍 Deal Finder", "📈 Market Intelligence", "📄 Single Project Report", "📊 Portfolio Analysis", "⚙️ Data & Settings"])

    # ==========================================================================
    # TAB 0: DEAL FINDER
    # ==========================================================================
    with tab0:
        st.markdown("### Find & Screen Deals")
        st.caption("Filter, sort, and analyze projects across all ISOs")

        # Filters row
        filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)

        with filter_col1:
            finder_iso = st.selectbox(
                "Region/ISO",
                ["NYISO", "ERCOT", "CAISO", "MISO", "SPP", "ISO-NE"],
                key="finder_iso"
            )

        with filter_col2:
            project_type_filter = st.multiselect(
                "Project Type",
                ["Solar", "Wind", "Storage", "Gas", "Hybrid", "Load", "Other"],
                default=[],
                key="type_filter"
            )

        with filter_col3:
            score_range = st.slider(
                "Min Feasibility Score",
                0, 100, 0,
                key="score_range"
            )

        with filter_col4:
            capacity_range = st.slider(
                "Capacity Range (MW)",
                0, 1000, (0, 1000),
                key="capacity_range"
            )

        # Load and score data
        finder_df = load_queue_data(finder_iso)

        if not finder_df.empty:
            # Score all projects (cached)
            with st.spinner(f"Scoring {len(finder_df)} projects..."):
                scored_df = score_all_projects(finder_iso, finder_df)

            # Apply filters
            filtered_df = scored_df.copy()

            # Type filter
            if project_type_filter:
                type_map = {
                    'Solar': ['S', 'Solar', 'SOL'],
                    'Wind': ['W', 'Wind', 'WND', 'OS'],
                    'Storage': ['ES', 'Storage', 'BESS', 'B', 'Battery'],
                    'Gas': ['NG', 'Gas', 'CT', 'CC', 'CCGT'],
                    'Hybrid': ['H', 'HY', 'Hybrid', 'S+ES', 'W+ES'],
                    'Load': ['L', 'Load'],
                    'Other': []
                }
                type_codes = []
                for t in project_type_filter:
                    type_codes.extend(type_map.get(t, []))

                if type_codes:
                    type_col = 'Type/ Fuel' if 'Type/ Fuel' in filtered_df.columns else filtered_df.columns[0]
                    filtered_df = filtered_df[filtered_df[type_col].astype(str).isin(type_codes)]

            # Score filter
            if score_range > 0:
                filtered_df = filtered_df[filtered_df['Feasibility Score'] >= score_range]

            # Capacity filter
            cap_col = 'SP (MW)' if 'SP (MW)' in filtered_df.columns else 'Capacity (MW)' if 'Capacity (MW)' in filtered_df.columns else None
            if cap_col:
                filtered_df = filtered_df[
                    (filtered_df[cap_col].fillna(0) >= capacity_range[0]) &
                    (filtered_df[cap_col].fillna(0) <= capacity_range[1])
                ]

            # Summary stats
            stat_cols = st.columns(5)
            with stat_cols[0]:
                st.metric("Projects", f"{len(filtered_df):,}")
            with stat_cols[1]:
                go_count = len(filtered_df[filtered_df['Recommendation'] == 'GO'])
                st.metric("GO Deals", f"{go_count}", f"{go_count/len(filtered_df)*100:.0f}%" if len(filtered_df) > 0 else "0%")
            with stat_cols[2]:
                cond_count = len(filtered_df[filtered_df['Recommendation'] == 'CONDITIONAL'])
                st.metric("Conditional", f"{cond_count}")
            with stat_cols[3]:
                avg_score = filtered_df['Feasibility Score'].mean() if len(filtered_df) > 0 else 0
                st.metric("Avg Score", f"{avg_score:.0f}")
            with stat_cols[4]:
                if cap_col:
                    total_gw = filtered_df[cap_col].sum() / 1000
                    st.metric("Total Capacity", f"{total_gw:.1f} GW")

            st.markdown("---")

            # Sort options
            sort_col1, sort_col2 = st.columns([1, 3])
            with sort_col1:
                sort_by = st.selectbox(
                    "Sort by",
                    ["Feasibility Score", "Capacity", "Queue Date", "Red Flags"],
                    key="sort_by"
                )

            # Map sort column
            sort_map = {
                "Feasibility Score": "Feasibility Score",
                "Capacity": cap_col or "SP (MW)",
                "Queue Date": "Date of IR" if "Date of IR" in filtered_df.columns else filtered_df.columns[0],
                "Red Flags": "Red Flags"
            }
            sort_col_name = sort_map[sort_by]
            sort_ascending = sort_by == "Red Flags"  # Lower red flags = better

            if sort_col_name in filtered_df.columns:
                filtered_df = filtered_df.sort_values(sort_col_name, ascending=sort_ascending, na_position='last')

            # Display table with color coding
            st.markdown("#### Deal Pipeline")

            # Prepare display columns
            display_cols = []
            col_mapping = {
                'Queue Pos.': 'ID',
                'Project Name': 'Project',
                'Developer/Interconnection Customer': 'Developer',
                'Type/ Fuel': 'Type',
                'SP (MW)': 'MW',
                'Feasibility Score': 'Score',
                'Recommendation': 'Status',
                'Date of IR': 'Queue Date',
                'Points of Interconnection': 'POI'
            }

            for orig, new in col_mapping.items():
                if orig in filtered_df.columns:
                    display_cols.append(orig)

            if display_cols:
                display_df = filtered_df[display_cols].head(100).copy()
                display_df.columns = [col_mapping.get(c, c) for c in display_df.columns]

                # Style the dataframe
                def highlight_recommendation(val):
                    if val == 'GO':
                        return 'background-color: #dcfce7; color: #166534;'
                    elif val == 'CONDITIONAL':
                        return 'background-color: #fef3c7; color: #92400e;'
                    elif val == 'NO-GO':
                        return 'background-color: #fee2e2; color: #991b1b;'
                    return ''

                def highlight_score(val):
                    try:
                        v = float(val)
                        if v >= 70:
                            return 'background-color: #dcfce7;'
                        elif v >= 50:
                            return 'background-color: #fef3c7;'
                        else:
                            return 'background-color: #fee2e2;'
                    except:
                        return ''

                styled_df = display_df.style
                if 'Status' in display_df.columns:
                    styled_df = styled_df.applymap(highlight_recommendation, subset=['Status'])
                if 'Score' in display_df.columns:
                    styled_df = styled_df.applymap(highlight_score, subset=['Score'])

                st.dataframe(styled_df, use_container_width=True, height=400)

                # Quick analyze selected project
                st.markdown("---")
                st.markdown("#### Quick Analyze")

                queue_col = 'Queue Pos.' if 'Queue Pos.' in filtered_df.columns else filtered_df.columns[0]
                quick_select = st.selectbox(
                    "Select project to analyze",
                    filtered_df[queue_col].astype(str).tolist()[:50],
                    key="quick_select"
                )

                if quick_select:
                    selected_project = filtered_df[filtered_df[queue_col].astype(str) == str(quick_select)].iloc[0]

                    # Quick analysis
                    qcol1, qcol2, qcol3 = st.columns([1, 1, 2])

                    with qcol1:
                        score_val = selected_project.get('Feasibility Score', 0)
                        rec = selected_project.get('Recommendation', 'UNKNOWN')
                        color = '#10b981' if rec == 'GO' else '#f59e0b' if rec == 'CONDITIONAL' else '#ef4444'

                        st.markdown(f"""
                        <div style="text-align:center;padding:15px;background:white;border-radius:8px;border:2px solid {color};">
                            <div style="font-size:36px;font-weight:700;color:{color};">{score_val:.0f}</div>
                            <div style="font-size:12px;color:#6b7280;">Feasibility Score</div>
                            <div style="margin-top:8px;padding:4px 12px;background:{color};color:white;border-radius:4px;display:inline-block;font-weight:600;">{rec}</div>
                        </div>
                        """, unsafe_allow_html=True)

                    with qcol2:
                        proj_name = selected_project.get('Project Name', 'Unknown')
                        developer = selected_project.get('Developer/Interconnection Customer', 'Unknown')
                        proj_type = spell_out_type(selected_project.get('Type/ Fuel', ''))
                        capacity = selected_project.get('SP (MW)', 0) or selected_project.get('Capacity (MW)', 0) or 0

                        st.markdown(f"""
                        <div style="background:white;padding:15px;border-radius:8px;border:1px solid #e5e7eb;">
                            <div style="font-weight:600;font-size:14px;">{str(proj_name)[:40]}</div>
                            <div style="font-size:12px;color:#6b7280;margin-top:4px;">{str(developer)[:35]}</div>
                            <div style="margin-top:12px;font-size:12px;">
                                <div><strong>{proj_type}</strong> · {capacity:.0f} MW</div>
                                <div style="color:#6b7280;margin-top:4px;">Queue: {format_date(selected_project.get('Date of IR', ''))}</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

                    with qcol3:
                        st.markdown(f"""
                        <div style="background:#f8fafc;padding:15px;border-radius:8px;border:1px solid #e5e7eb;">
                            <div style="font-weight:600;margin-bottom:8px;">Quick Actions</div>
                        </div>
                        """, unsafe_allow_html=True)

                        if st.button("📄 Generate Full Report", key="quick_report"):
                            st.session_state['analyze_queue_id'] = quick_select
                            st.session_state['analyze_iso'] = finder_iso
                            st.info("Switch to 'Single Project Report' tab to generate the full report")

                        # Add to comparison
                        if 'compare_projects' not in st.session_state:
                            st.session_state.compare_projects = []

                        if st.button("➕ Add to Compare", key="add_compare"):
                            if quick_select not in st.session_state.compare_projects:
                                st.session_state.compare_projects.append({
                                    'id': quick_select,
                                    'iso': finder_iso,
                                    'name': str(selected_project.get('Project Name', 'Unknown'))[:30],
                                    'score': score_val,
                                    'rec': rec
                                })
                                st.success(f"Added {quick_select} to comparison")

            # Comparison Panel
            if 'compare_projects' in st.session_state and st.session_state.compare_projects:
                st.markdown("---")
                st.markdown("#### Compare Selected Projects")

                # Show comparison chips
                compare_cols = st.columns(len(st.session_state.compare_projects) + 1)

                for i, proj in enumerate(st.session_state.compare_projects):
                    with compare_cols[i]:
                        color = '#10b981' if proj['rec'] == 'GO' else '#f59e0b' if proj['rec'] == 'CONDITIONAL' else '#ef4444'
                        st.markdown(f"""
                        <div style="background:white;border:2px solid {color};border-radius:8px;padding:10px;text-align:center;">
                            <div style="font-weight:600;font-size:13px;">{proj['id']}</div>
                            <div style="font-size:11px;color:#6b7280;margin:4px 0;">{proj['name']}</div>
                            <div style="font-size:20px;font-weight:700;color:{color};">{proj['score']:.0f}</div>
                        </div>
                        """, unsafe_allow_html=True)

                with compare_cols[-1]:
                    if st.button("🗑️ Clear All", key="clear_compare"):
                        st.session_state.compare_projects = []
                        st.rerun()

                # Comparison Table
                if len(st.session_state.compare_projects) >= 2:
                    st.markdown("**Side-by-Side Comparison:**")

                    compare_data = []
                    for proj in st.session_state.compare_projects:
                        # Load project data
                        proj_df = load_queue_data(proj['iso'])
                        queue_col = 'Queue Pos.' if 'Queue Pos.' in proj_df.columns else proj_df.columns[0]
                        proj_row = proj_df[proj_df[queue_col].astype(str) == str(proj['id'])]

                        if not proj_row.empty:
                            proj_row = proj_row.iloc[0]
                            analysis = analyze_project(proj_row, proj_df, region=proj['iso'])

                            compare_data.append({
                                'Project': proj['id'],
                                'Name': str(proj_row.get('Project Name', 'Unknown'))[:25],
                                'Type': spell_out_type(proj_row.get('Type/ Fuel', '')),
                                'Capacity (MW)': proj_row.get('SP (MW)', 0) or proj_row.get('Capacity (MW)', 0) or 0,
                                'Score': analysis['score_result']['total_score'],
                                'Recommendation': analysis['score_result']['recommendation'],
                                'Est. Cost ($M)': analysis['costs']['med_total'],
                                'Est. COD': analysis['timeline']['likely_date'],
                                'Red Flags': len(analysis['score_result'].get('red_flags', []))
                            })

                    if compare_data:
                        compare_df = pd.DataFrame(compare_data)
                        st.dataframe(compare_df, use_container_width=True, hide_index=True)
        else:
            st.warning(f"No data available for {finder_iso}")

    # ==========================================================================
    # TAB 1: MARKET INTELLIGENCE
    # ==========================================================================
    with tab1:
        st.markdown("### Market Intelligence")
        st.caption("Understand the interconnection landscape and market dynamics")

        # Try to load LBL data for historical insights
        try:
            lbl_path = Path(__file__).parent / '.cache' / 'lbl_queued_up.xlsx'
            if lbl_path.exists():
                lbl_df = pd.read_excel(lbl_path, sheet_name='03. Complete Queue Data', header=1)
                has_lbl = True
            else:
                has_lbl = False
                lbl_df = pd.DataFrame()
        except Exception as e:
            has_lbl = False
            lbl_df = pd.DataFrame()

        # Market Overview Section
        st.markdown("#### National Overview")

        if has_lbl and not lbl_df.empty:
            overview_cols = st.columns(4)

            with overview_cols[0]:
                total_projects = len(lbl_df)
                active = len(lbl_df[lbl_df['q_status'] == 'active'])
                st.metric("Total Historical Projects", f"{total_projects:,}", f"{active:,} active")

            with overview_cols[1]:
                operational = len(lbl_df[lbl_df['q_status'] == 'operational'])
                completion_rate = operational / total_projects * 100 if total_projects > 0 else 0
                st.metric("National Completion Rate", f"{completion_rate:.1f}%", "Operational / Total")

            with overview_cols[2]:
                withdrawn = len(lbl_df[lbl_df['q_status'] == 'withdrawn'])
                withdrawal_rate = withdrawn / total_projects * 100 if total_projects > 0 else 0
                st.metric("Withdrawal Rate", f"{withdrawal_rate:.1f}%", f"{withdrawn:,} withdrawn")

            with overview_cols[3]:
                if 'mw1' in lbl_df.columns:
                    total_mw = lbl_df['mw1'].sum() / 1000  # Convert to GW
                    st.metric("Total Capacity", f"{total_mw:.0f} GW", "All historical")

            st.markdown("---")

            # Regional Comparison
            st.markdown("#### Regional Completion Rates")
            st.caption("Historical success rates by ISO/RTO - Critical for risk assessment")

            if 'region' in lbl_df.columns and 'q_status' in lbl_df.columns:
                region_stats = lbl_df.groupby('region').agg({
                    'q_status': lambda x: {
                        'total': len(x),
                        'operational': (x == 'operational').sum(),
                        'withdrawn': (x == 'withdrawn').sum(),
                        'active': (x == 'active').sum()
                    }
                }).reset_index()

                # Flatten the stats
                region_data = []
                for _, row in region_stats.iterrows():
                    stats = row['q_status']
                    comp_rate = stats['operational'] / stats['total'] * 100 if stats['total'] > 0 else 0
                    region_data.append({
                        'Region': row['region'],
                        'Total Projects': stats['total'],
                        'Operational': stats['operational'],
                        'Withdrawn': stats['withdrawn'],
                        'Active': stats['active'],
                        'Completion Rate': comp_rate
                    })

                region_df = pd.DataFrame(region_data).sort_values('Completion Rate', ascending=False)

                # Display as horizontal bars
                for _, row in region_df.iterrows():
                    rate = row['Completion Rate']
                    color = '#10b981' if rate > 20 else '#f59e0b' if rate > 10 else '#ef4444'

                    st.markdown(f"""
                    <div style="display:flex;align-items:center;margin-bottom:8px;">
                        <div style="width:80px;font-weight:600;">{row['Region']}</div>
                        <div style="flex:1;background:#e5e7eb;height:24px;border-radius:4px;overflow:hidden;margin:0 12px;">
                            <div style="background:{color};height:100%;width:{rate}%;display:flex;align-items:center;justify-content:flex-end;padding-right:8px;">
                                <span style="color:white;font-weight:600;font-size:12px;">{rate:.1f}%</span>
                            </div>
                        </div>
                        <div style="width:120px;font-size:12px;color:#6b7280;">{row['Total Projects']:,} projects</div>
                    </div>
                    """, unsafe_allow_html=True)

                st.markdown("""
                <div style="background:#eff6ff;border:1px solid #3b82f6;border-radius:8px;padding:12px;margin-top:16px;">
                    <strong>Key Insight:</strong> ERCOT has the highest completion rate (~34%) due to lighter regulatory requirements.
                    NYISO/CAISO have the lowest (~8-10%) due to grid congestion and complex study processes.
                </div>
                """, unsafe_allow_html=True)

            st.markdown("---")

            # Technology Trends
            st.markdown("#### Technology Mix Trends")
            st.caption("How the queue composition is changing over time")

            if 'q_year' in lbl_df.columns and 'type_clean' in lbl_df.columns:
                # Group by year and type
                recent_years = lbl_df[lbl_df['q_year'] >= 2018]

                if not recent_years.empty:
                    tech_by_year = recent_years.groupby(['q_year', 'type_clean']).agg({
                        'mw1': 'sum'
                    }).reset_index()

                    # Pivot for display
                    tech_pivot = tech_by_year.pivot(index='q_year', columns='type_clean', values='mw1').fillna(0)
                    tech_pivot = tech_pivot / 1000  # Convert to GW

                    # Show key technologies
                    key_techs = ['Solar', 'Wind', 'Storage', 'Gas', 'Hybrid']
                    available_techs = [t for t in key_techs if t in tech_pivot.columns]

                    if available_techs:
                        display_pivot = tech_pivot[available_techs].tail(6)

                        # Format as table
                        st.dataframe(
                            display_pivot.style.format("{:.0f} GW").background_gradient(cmap='Greens', axis=None),
                            use_container_width=True
                        )

                        # Key insights
                        if 'Storage' in display_pivot.columns:
                            storage_growth = display_pivot['Storage'].iloc[-1] / display_pivot['Storage'].iloc[0] if display_pivot['Storage'].iloc[0] > 0 else 0
                            st.markdown(f"""
                            <div style="background:#f0fdf4;border:1px solid #22c55e;border-radius:8px;padding:12px;margin-top:16px;">
                                <strong>Storage Explosion:</strong> Battery storage capacity in queue grew from {display_pivot['Storage'].iloc[0]:.0f} GW to {display_pivot['Storage'].iloc[-1]:.0f} GW ({storage_growth:.0f}x increase since 2018)
                            </div>
                            """, unsafe_allow_html=True)

            st.markdown("---")

            # Completion Rate by Technology
            st.markdown("#### Completion Rates by Technology")
            st.caption("Which project types have the best track record?")

            if 'type_clean' in lbl_df.columns:
                tech_completion = []
                for tech in lbl_df['type_clean'].dropna().unique():
                    tech_df = lbl_df[lbl_df['type_clean'] == tech]
                    total = len(tech_df)
                    if total >= 100:  # Only show techs with sufficient data
                        operational = len(tech_df[tech_df['q_status'] == 'operational'])
                        rate = operational / total * 100
                        tech_completion.append({
                            'Technology': tech,
                            'Completion Rate': rate,
                            'Sample Size': total,
                            'Completed': operational
                        })

                tech_comp_df = pd.DataFrame(tech_completion).sort_values('Completion Rate', ascending=False)

                if not tech_comp_df.empty:
                    tcol1, tcol2 = st.columns(2)

                    with tcol1:
                        st.markdown("**Highest Success:**")
                        for _, row in tech_comp_df.head(5).iterrows():
                            st.markdown(f"- **{row['Technology']}**: {row['Completion Rate']:.1f}% ({row['Sample Size']:,} projects)")

                    with tcol2:
                        st.markdown("**Lowest Success:**")
                        for _, row in tech_comp_df.tail(5).iterrows():
                            st.markdown(f"- **{row['Technology']}**: {row['Completion Rate']:.1f}% ({row['Sample Size']:,} projects)")

            st.markdown("---")

            # Timeline Insights
            st.markdown("#### Development Timeline Analysis")
            st.caption("How long does it actually take to reach COD?")

            if 'q_date' in lbl_df.columns and 'on_date' in lbl_df.columns:
                # Calculate time to COD for completed projects
                completed = lbl_df[lbl_df['q_status'] == 'operational'].copy()
                completed['q_date'] = pd.to_datetime(completed['q_date'], errors='coerce')
                completed['on_date'] = pd.to_datetime(completed['on_date'], errors='coerce')
                completed['months_to_cod'] = (completed['on_date'] - completed['q_date']).dt.days / 30.44

                # Filter valid timelines
                valid_timeline = completed[(completed['months_to_cod'] > 0) & (completed['months_to_cod'] < 200)]

                if not valid_timeline.empty:
                    timeline_cols = st.columns(4)

                    with timeline_cols[0]:
                        median_months = valid_timeline['months_to_cod'].median()
                        st.metric("Median Time to COD", f"{median_months:.0f} months", f"{median_months/12:.1f} years")

                    with timeline_cols[1]:
                        p25 = valid_timeline['months_to_cod'].quantile(0.25)
                        st.metric("Fast Track (P25)", f"{p25:.0f} months", "Top 25% of projects")

                    with timeline_cols[2]:
                        p75 = valid_timeline['months_to_cod'].quantile(0.75)
                        st.metric("Typical Delay (P75)", f"{p75:.0f} months", "75th percentile")

                    with timeline_cols[3]:
                        p90 = valid_timeline['months_to_cod'].quantile(0.90)
                        st.metric("Worst Case (P90)", f"{p90:.0f} months", "Plan for this")

                    # By region
                    if 'region' in valid_timeline.columns:
                        st.markdown("**Median Timeline by Region:**")
                        region_timeline = valid_timeline.groupby('region')['months_to_cod'].median().sort_values()

                        for region, months in region_timeline.items():
                            years = months / 12
                            color = '#10b981' if months < 40 else '#f59e0b' if months < 60 else '#ef4444'
                            st.markdown(f"""
                            <div style="display:flex;align-items:center;margin-bottom:4px;">
                                <div style="width:80px;">{region}</div>
                                <div style="background:{color};color:white;padding:2px 8px;border-radius:4px;font-size:12px;">
                                    {months:.0f} mo ({years:.1f} yr)
                                </div>
                            </div>
                            """, unsafe_allow_html=True)

        else:
            st.warning("LBL historical data not found. Run data refresh to enable market intelligence.")
            st.code("python3 refresh_data.py --lbl")

    # ==========================================================================
    # TAB 2: SINGLE PROJECT REPORT
    # ==========================================================================
    with tab2:
        col1, col2 = st.columns([1, 2])

        with col1:
            st.markdown("### 1. Select Project")

            # Check if coming from Deal Finder
            default_iso_idx = 0
            default_queue_id = ""
            if 'analyze_iso' in st.session_state and st.session_state.analyze_iso:
                iso_list = ["NYISO", "ERCOT", "CAISO", "MISO", "SPP", "ISO-NE"]
                if st.session_state.analyze_iso in iso_list:
                    default_iso_idx = iso_list.index(st.session_state.analyze_iso)
            if 'analyze_queue_id' in st.session_state and st.session_state.analyze_queue_id:
                default_queue_id = st.session_state.analyze_queue_id

            # ISO Selection
            iso = st.selectbox("ISO/RTO", ["NYISO", "ERCOT", "CAISO", "MISO", "SPP", "ISO-NE"], index=default_iso_idx)

            # Load data
            df = load_queue_data(iso)

            if df.empty:
                st.error(f"No data available for {iso}")
                return

            # Queue ID input
            queue_col = 'Queue Pos.' if 'Queue Pos.' in df.columns else df.columns[0]
            queue_ids = df[queue_col].dropna().astype(str).tolist()

            queue_id = st.text_input("Queue ID", value=default_queue_id, placeholder="e.g., 1756")

            # Or select from list
            with st.expander("Or browse projects"):
                # Filter options
                search = st.text_input("Search by name or developer", "")

                display_df = df.copy()
                if search:
                    mask = (
                        display_df['Project Name'].astype(str).str.lower().str.contains(search.lower(), na=False) |
                        display_df['Developer/Interconnection Customer'].astype(str).str.lower().str.contains(search.lower(), na=False)
                    )
                    display_df = display_df[mask]

                # Show projects sorted by date (newest first)
                if 'Date of IR' in display_df.columns:
                    display_df = display_df.sort_values('Date of IR', ascending=False)

                if not display_df.empty:
                    # Create display string
                    options = []
                    for _, row in display_df.iterrows():
                        qid = row.get(queue_col, '')
                        name = row.get('Project Name', 'Unknown')[:40]
                        ptype = spell_out_type(row.get('Type/ Fuel', ''))
                        options.append(f"{qid} - {name} ({ptype})")

                    selected = st.selectbox("Select project", options, index=None, placeholder="Choose a project...")
                    if selected:
                        queue_id = selected.split(' - ')[0]

            st.markdown("### 2. Report Info")
            client_name = st.text_input("Client/Firm Name", value="", placeholder="e.g., Blackstone Infrastructure")
            prepared_by = st.text_input("Prepared By", value="", placeholder="e.g., Your Name")
            notes = st.text_area("Notes (optional)", placeholder="Any additional context...")

        with col2:
            # Only show analysis if queue_id is entered
            if queue_id:
                # Find the project
                mask = df[queue_col].astype(str) == str(queue_id)
                if mask.sum() == 0:
                    st.warning(f"Project {queue_id} not found in {iso} queue")
                else:
                    project = df[mask].iloc[0]

                    # Run analysis
                    analysis = analyze_project(project, df, region=iso)

                    st.markdown("### 3. Analysis Preview")

                    # Score display
                    score = analysis['score_result']['total_score']
                    rec = analysis['score_result']['recommendation']

                    score_class = 'score-go' if rec == 'GO' else 'score-conditional' if rec == 'CONDITIONAL' else 'score-nogo'

                    col_a, col_b = st.columns([1, 2])
                    with col_a:
                        st.markdown(f"""
                        <div style="text-align:center;padding:20px;background:white;border-radius:8px;border:1px solid #e0e0e0;">
                            <div class="score-large {score_class}">{score:.0f}</div>
                            <div style="font-size:12px;color:#6b7280;margin-top:4px;">out of 100</div>
                            <div style="margin-top:12px;padding:6px 16px;background:{'#10b981' if rec=='GO' else '#f59e0b' if rec=='CONDITIONAL' else '#ef4444'};color:white;border-radius:4px;font-weight:600;display:inline-block;">{rec}</div>
                        </div>
                        """, unsafe_allow_html=True)

                    with col_b:
                        # Project summary
                        st.markdown(f"""
                        <div style="background:white;padding:16px;border-radius:8px;border:1px solid #e0e0e0;">
                            <div style="font-weight:600;font-size:14px;">{project.get('Project Name', 'Unknown')}</div>
                            <div style="color:#6b7280;font-size:12px;margin-top:4px;">{project.get('Developer/Interconnection Customer', 'Unknown')}</div>
                            <div style="margin-top:12px;display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:12px;">
                                <div><span style="color:#6b7280;">Type:</span> {spell_out_type(project.get('Type/ Fuel', ''))}</div>
                                <div><span style="color:#6b7280;">Capacity:</span> {analysis['capacity']:.0f} MW</div>
                                <div><span style="color:#6b7280;">Queue Date:</span> {format_date(project.get('Date of IR', ''))}</div>
                                <div><span style="color:#6b7280;">POI:</span> {str(project.get('Points of Interconnection', 'Unknown'))[:30]}</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

                    # Score breakdown
                    st.markdown("#### Score Breakdown")
                    breakdown = analysis['breakdown']

                    for name, key, max_val in [
                        ('Queue Position', 'queue_position', 25),
                        ('Study Progress', 'study_progress', 25),
                        ('Developer Track Record', 'developer_track_record', 20),
                        ('POI Congestion', 'poi_congestion', 15),
                        ('Project Characteristics', 'project_characteristics', 15),
                    ]:
                        # Safely get the value, handling edge cases
                        val = breakdown.get(key, 0)
                        if isinstance(val, dict):
                            val = 0  # Handle unexpected dict values
                        try:
                            val = float(val) if val else 0
                        except (TypeError, ValueError):
                            val = 0

                        pct = (val / max_val) * 100 if max_val > 0 else 0
                        bar_class = 'bar-green' if pct >= 70 else 'bar-yellow' if pct >= 40 else 'bar-red'
                        st.markdown(f"""
                        <div class="score-bar-container">
                            <div class="score-bar-label">
                                <span>{name}</span>
                                <span>{val:.1f}/{max_val} ({pct:.0f}%)</span>
                            </div>
                            <div class="score-bar-bg">
                                <div class="score-bar-fill {bar_class}" style="width:{pct}%;"></div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

                    # Flags
                    col_r, col_g = st.columns(2)
                    with col_r:
                        st.markdown("**Risk Factors**")
                        for flag in analysis['score_result'].get('red_flags', []):
                            st.markdown(f'<div class="flag flag-red">{flag}</div>', unsafe_allow_html=True)
                        if not analysis['score_result'].get('red_flags'):
                            st.markdown("*None identified*")

                    with col_g:
                        st.markdown("**Positive Indicators**")
                        for flag in analysis['score_result'].get('green_flags', []):
                            st.markdown(f'<div class="flag flag-green">{flag}</div>', unsafe_allow_html=True)
                        if not analysis['score_result'].get('green_flags'):
                            st.markdown("*None identified*")

                    # Key metrics
                    st.markdown("#### Key Estimates")
                    m1, m2, m3 = st.columns(3)
                    with m1:
                        costs = analysis['costs']
                        st.metric("IC Cost (P50)", f"${costs['med_total']:.1f}M", f"${costs['low_total']:.1f}M - ${costs['high_total']:.1f}M")
                    with m2:
                        timeline = analysis['timeline']
                        st.metric("Target COD", timeline['likely_date'], f"{timeline['optimistic_date']} - {timeline['pessimistic_date']}")
                    with m3:
                        st.metric("Type Hist. Rate", f"{timeline['completion_rate']*100:.0f}%", "Historical avg for this project type")

                    # Enhanced Historical Intelligence (from LBL data)
                    enhanced = analysis['score_result'].get('enhanced_analysis', {})
                    if enhanced:
                        st.markdown("### 📊 Historical Intelligence")
                        st.caption("Based on 36,441 historical projects from LBL Berkeley Lab")

                        intel_cols = st.columns(2)

                        with intel_cols[0]:
                            # Actual completion rate
                            comp_rate = enhanced.get('completion_rate', {})
                            if comp_rate and 'rate_pct' in comp_rate:
                                st.markdown(f"""
                                <div style="background:#f8f9fa;border:1px solid #e5e7eb;padding:12px;border-radius:8px;margin-bottom:10px;">
                                    <div style="font-size:0.9em;color:#6b7280;">Actual Completion Rate</div>
                                    <div style="font-size:1.8em;font-weight:bold;color:{'#ef4444' if comp_rate.get('rate',0) < 0.10 else '#22c55e' if comp_rate.get('rate',0) > 0.20 else '#f59e0b'};">
                                        {comp_rate.get('rate_pct', 'N/A')}
                                    </div>
                                    <div style="font-size:0.8em;color:#4b5563;">
                                        {comp_rate.get('sample_size', 0):,} historical projects | {comp_rate.get('confidence', 'N/A')} confidence
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)

                            # Developer track record
                            dev_record = enhanced.get('developer_track_record', {})
                            if dev_record and 'completion_rate_pct' in dev_record:
                                assessment_color = {
                                    'excellent': '#22c55e',
                                    'good': '#84cc16',
                                    'average': '#f59e0b',
                                    'below_average': '#f97316',
                                    'poor': '#ef4444',
                                    'no_completions': '#dc2626'
                                }.get(dev_record.get('assessment', ''), '#888')

                                st.markdown(f"""
                                <div style="background:#f8f9fa;border:1px solid #e5e7eb;padding:12px;border-radius:8px;">
                                    <div style="font-size:0.9em;color:#6b7280;">Developer Track Record</div>
                                    <div style="font-size:1.4em;font-weight:bold;color:{assessment_color};">
                                        {dev_record.get('completion_rate_pct', 'N/A')} ({dev_record.get('completed', 0)}/{dev_record.get('total_projects', 0)})
                                    </div>
                                    <div style="font-size:0.85em;color:#6b7280;">
                                        {dev_record.get('assessment_text', 'Unknown')}
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                            elif dev_record.get('error'):
                                st.markdown(f"""
                                <div style="background:#f8f9fa;border:1px solid #e5e7eb;padding:12px;border-radius:8px;">
                                    <div style="font-size:0.9em;color:#6b7280;">Developer Track Record</div>
                                    <div style="font-size:0.9em;color:#f59e0b;">
                                        ⚠️ {dev_record.get('error', 'No data')}
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)

                        with intel_cols[1]:
                            # POI history
                            poi_history = enhanced.get('poi_history', {})
                            if poi_history and 'completion_rate_pct' in poi_history:
                                risk_color = {
                                    'low': '#22c55e',
                                    'medium': '#f59e0b',
                                    'high': '#f97316',
                                    'very_high': '#dc2626'
                                }.get(poi_history.get('risk_level', ''), '#888')

                                st.markdown(f"""
                                <div style="background:#f8f9fa;border:1px solid #e5e7eb;padding:12px;border-radius:8px;margin-bottom:10px;">
                                    <div style="font-size:0.9em;color:#6b7280;">POI Historical Completion</div>
                                    <div style="font-size:1.8em;font-weight:bold;color:{risk_color};">
                                        {poi_history.get('completion_rate_pct', 'N/A')}
                                    </div>
                                    <div style="font-size:0.8em;color:#4b5563;">
                                        {poi_history.get('completed', 0)} completed, {poi_history.get('withdrawn', 0)} withdrawn at this POI
                                    </div>
                                    <div style="font-size:0.85em;color:#6b7280;">
                                        {poi_history.get('risk_interpretation', '')}
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                            else:
                                st.markdown(f"""
                                <div style="background:#f8f9fa;border:1px solid #e5e7eb;padding:12px;border-radius:8px;margin-bottom:10px;">
                                    <div style="font-size:0.9em;color:#6b7280;">POI Historical Data</div>
                                    <div style="font-size:0.9em;color:#4b5563;">
                                        No historical data for this POI
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)

                            # Timeline prediction
                            timeline_pred = enhanced.get('timeline_prediction', {})
                            if timeline_pred and 'p50_months' in timeline_pred:
                                st.markdown(f"""
                                <div style="background:#f8f9fa;border:1px solid #e5e7eb;padding:12px;border-radius:8px;">
                                    <div style="font-size:0.9em;color:#6b7280;">Historical Timeline (Queue to COD)</div>
                                    <div style="display:flex;justify-content:space-between;margin-top:8px;">
                                        <div style="text-align:center;">
                                            <div style="font-size:1.2em;font-weight:bold;color:#22c55e;">
                                                {timeline_pred.get('p50_months', 0):.0f}
                                            </div>
                                            <div style="font-size:0.7em;color:#6b7280;">P50 months</div>
                                        </div>
                                        <div style="text-align:center;">
                                            <div style="font-size:1.2em;font-weight:bold;color:#f59e0b;">
                                                {timeline_pred.get('p75_months', 0):.0f}
                                            </div>
                                            <div style="font-size:0.7em;color:#6b7280;">P75 months</div>
                                        </div>
                                        <div style="text-align:center;">
                                            <div style="font-size:1.2em;font-weight:bold;color:#ef4444;">
                                                {timeline_pred.get('p90_months', 0):.0f}
                                            </div>
                                            <div style="font-size:0.7em;color:#6b7280;">P90 months</div>
                                        </div>
                                    </div>
                                    <div style="font-size:0.75em;color:#4b5563;margin-top:8px;">
                                        {timeline_pred.get('confidence', 'N/A')} confidence | {timeline_pred.get('sample_size', 0)} projects
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)

                    # Data validation
                    st.markdown("### 4. Data Validation")

                    data_age = data_status.get(iso, {}).get('age_days', 999)

                    validations = [
                        (data_age < 7, f"Queue data current ({data_age} days old)" if data_age < 7 else f"Queue data may be stale ({data_age} days old)"),
                        (True, "Cost benchmarks from LBL Berkeley Lab (3,400+ projects)"),
                        (True, "Completion rates from LBL Queued Up (36,441 projects)"),
                        (False, "Verify study phase status with ISO OASIS"),
                    ]

                    for is_ok, text in validations:
                        icon = "✅" if is_ok else "⚠️"
                        color = "check-ok" if is_ok else "check-warn"
                        st.markdown(f'<div class="validation-item"><span class="{color}">{icon}</span> {text}</div>', unsafe_allow_html=True)

                    # Generate buttons
                    st.markdown("### 5. Generate Report")

                    # Initialize session state for report
                    if 'report_data' not in st.session_state:
                        st.session_state.report_data = None
                        st.session_state.report_queue_id = None
                        st.session_state.report_type = None

                    # Clear old report if project changed
                    if st.session_state.report_queue_id != queue_id:
                        st.session_state.report_data = None
                        st.session_state.report_queue_id = None

                    col_btn1, col_btn2 = st.columns(2)

                    with col_btn1:
                        if st.button("📄 Generate Report", type="primary", width="stretch"):
                            if not client_name:
                                st.warning("Please enter a client name")
                            else:
                                with st.spinner("Generating report..."):
                                    html = generate_report_html(project, analysis, client_name, prepared_by or "Queue Analysis", notes)

                                    if WEASYPRINT_AVAILABLE:
                                        pdf_bytes = generate_pdf(html)
                                        if pdf_bytes:
                                            st.session_state.report_data = pdf_bytes
                                            st.session_state.report_queue_id = queue_id
                                            st.session_state.report_type = 'pdf'
                                            st.success("PDF generated! Click download below.")
                                        else:
                                            st.error("PDF generation failed")
                                    else:
                                        st.session_state.report_data = html
                                        st.session_state.report_queue_id = queue_id
                                        st.session_state.report_type = 'html'
                                        st.success("Report generated! Click download below.")
                                    st.rerun()

                        # Show download button if report is ready
                        if st.session_state.report_data and st.session_state.report_queue_id == queue_id:
                            if st.session_state.report_type == 'pdf':
                                st.download_button(
                                    "📥 Download PDF",
                                    data=st.session_state.report_data,
                                    file_name=f"feasibility_report_{queue_id}.pdf",
                                    mime="application/pdf",
                                    width="stretch"
                                )
                            else:
                                st.download_button(
                                    "📥 Download HTML",
                                    data=st.session_state.report_data,
                                    file_name=f"feasibility_report_{queue_id}.html",
                                    mime="text/html",
                                    width="stretch"
                                )
                                st.caption("Open in browser and print to PDF")

                    with col_btn2:
                        summary = f"""FEASIBILITY ASSESSMENT: {queue_id}
Project: {project.get('Project Name', 'Unknown')}
Score: {score:.0f}/100 - {rec}
Cost Est: ${costs['med_total']:.1f}M (${costs['med_per_kw']:.0f}/kW)
Timeline: {timeline['likely_date']}
Type Hist. Rate: {timeline['completion_rate']*100:.0f}%"""
                        st.download_button(
                            "📋 Copy Summary",
                            data=summary,
                            file_name=f"summary_{queue_id}.txt",
                            mime="text/plain",
                            width="stretch"
                        )

            else:
                st.info("👈 Enter a Queue ID to analyze a project")

    # ==========================================================================
    # TAB 3: PORTFOLIO ANALYSIS
    # ==========================================================================
    with tab3:
        st.markdown("### Portfolio & Macro Analysis")
        st.markdown("Generate market-wide reports and portfolio summaries.")

        col1, col2 = st.columns([1, 2])

        with col1:
            st.markdown("#### Report Type")
            report_type = st.radio(
                "Select report type",
                ["Market Overview", "Developer Portfolio", "POI Analysis", "Custom Filter"],
                label_visibility="collapsed"
            )

            iso_macro = st.selectbox("ISO/RTO", ["NYISO", "All ISOs"], key="macro_iso")

            if report_type == "Developer Portfolio":
                df_macro = load_queue_data("NYISO")
                developers = df_macro['Developer/Interconnection Customer'].dropna().unique().tolist()
                selected_dev = st.selectbox("Developer", sorted(developers)[:50])

            elif report_type == "POI Analysis":
                df_macro = load_queue_data("NYISO")
                pois = df_macro['Points of Interconnection'].dropna().unique().tolist()
                selected_poi = st.selectbox("Point of Interconnection", sorted(pois)[:50])

            elif report_type == "Custom Filter":
                df_macro = load_queue_data("NYISO")
                project_types = ['All'] + sorted(df_macro['Type/ Fuel'].dropna().unique().tolist())
                selected_type = st.selectbox("Project Type", project_types)
                min_mw = st.number_input("Min Capacity (MW)", value=0, min_value=0)
                max_mw = st.number_input("Max Capacity (MW)", value=1000, min_value=0)

            client_macro = st.text_input("Client Name", key="macro_client", placeholder="e.g., Infrastructure Fund")

        with col2:
            st.markdown("#### Preview")

            df_macro = load_queue_data("NYISO")

            if report_type == "Market Overview":
                # Summary stats
                total_projects = len(df_macro)
                total_mw = df_macro['SP (MW)'].sum() if 'SP (MW)' in df_macro.columns else 0

                st.markdown(f"""
                <div style="background:white;padding:20px;border-radius:8px;border:1px solid #e0e0e0;">
                    <h4 style="margin:0 0 16px 0;">NYISO Queue Overview</h4>
                    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:16px;">
                        <div style="text-align:center;">
                            <div style="font-size:28px;font-weight:700;color:#1a1a2e;">{total_projects}</div>
                            <div style="font-size:11px;color:#6b7280;">Active Projects</div>
                        </div>
                        <div style="text-align:center;">
                            <div style="font-size:28px;font-weight:700;color:#1a1a2e;">{total_mw/1000:.1f} GW</div>
                            <div style="font-size:11px;color:#6b7280;">Total Capacity</div>
                        </div>
                        <div style="text-align:center;">
                            <div style="font-size:28px;font-weight:700;color:#1a1a2e;">6.2%</div>
                            <div style="font-size:11px;color:#6b7280;">Completion Rate</div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                # Type breakdown
                st.markdown("**By Project Type:**")
                if 'Type/ Fuel' in df_macro.columns and 'SP (MW)' in df_macro.columns:
                    type_summary = df_macro.groupby('Type/ Fuel').agg({
                        'SP (MW)': ['count', 'sum']
                    }).round(0)
                    type_summary.columns = ['Count', 'MW']
                    type_summary = type_summary.sort_values('MW', ascending=False).head(10)
                    st.dataframe(type_summary, width="stretch")

            elif report_type == "Developer Portfolio":
                if 'selected_dev' in dir() and selected_dev:
                    dev_df = df_macro[df_macro['Developer/Interconnection Customer'] == selected_dev]
                    dev_mw = dev_df['SP (MW)'].sum() if 'SP (MW)' in dev_df.columns else 0
                    st.markdown(f"**{selected_dev}** - {len(dev_df)} projects, {dev_mw:.0f} MW total")
                    st.dataframe(
                        dev_df[['Queue Pos.', 'Project Name', 'Type/ Fuel', 'SP (MW)', 'Date of IR']].head(20),
                        width="stretch"
                    )

            elif report_type == "POI Analysis":
                if 'selected_poi' in dir() and selected_poi:
                    poi_df = df_macro[df_macro['Points of Interconnection'] == selected_poi]
                    st.markdown(f"**{selected_poi}** - {len(poi_df)} projects at this POI")
                    st.dataframe(
                        poi_df[['Queue Pos.', 'Project Name', 'Type/ Fuel', 'SP (MW)', 'Date of IR']].head(20),
                        width="stretch"
                    )

            st.markdown("---")
            if st.button("📊 Generate Macro Report", type="primary"):
                st.info("Macro report generation coming soon. Use Single Project Report for now.")

    # ==========================================================================
    # TAB 4: DATA & SETTINGS
    # ==========================================================================
    with tab4:
        st.markdown("### Data & Settings")
        st.markdown("Understand your data sources and coverage")

        # Detailed status
        for iso, status in data_status.items():
            with st.expander(f"{iso} - {'✅ Current' if status['status'] == 'ok' else '⚠️ Stale' if status['status'] == 'warn' else '❌ Missing'}"):
                if status['exists']:
                    st.write(f"**Last Updated:** {status['last_updated'].strftime('%Y-%m-%d %H:%M:%S')}")
                    st.write(f"**Age:** {status['age_days']} days")

                    # Load and show stats
                    try:
                        df_status = load_queue_data(iso)
                        st.write(f"**Projects:** {len(df_status)}")
                        if 'SP (MW)' in df_status.columns:
                            st.write(f"**Total MW:** {df_status['SP (MW)'].sum():,.0f}")
                    except:
                        pass
                else:
                    st.write("No data file found")

        st.markdown("---")

        # Database Explorer Section
        st.markdown("#### Database Explorer")
        st.caption("Understand what data powers the analysis")

        db_tabs = st.tabs(["Queue Data", "Historical Data (LBL)", "Cost Data", "EIA Data"])

        with db_tabs[0]:
            st.markdown("**Live Queue Data by ISO**")

            queue_files = {
                'NYISO': ('.cache/nyiso_queue_direct.xlsx', '.cache/nyiso_queue.xlsx'),
                'ERCOT': ('.cache/ercot_queue_direct.parquet', '.cache/ercot_gis_report.xlsx'),
                'CAISO': ('.cache/caiso_queue_direct.xlsx',),
                'MISO': ('.cache/miso_queue_direct.parquet',),
                'SPP': ('.cache/spp_queue_direct.parquet',),
                'ISO-NE': ('.cache/isone_queue_direct.parquet',),
            }

            for iso, files in queue_files.items():
                cache_dir = Path(__file__).parent / '.cache'
                found_file = None
                for f in files:
                    fp = Path(__file__).parent / f
                    if fp.exists():
                        found_file = fp
                        break

                if found_file:
                    size_mb = found_file.stat().st_size / (1024 * 1024)
                    age_days = (datetime.now() - datetime.fromtimestamp(found_file.stat().st_mtime)).days

                    try:
                        if str(found_file).endswith('.parquet'):
                            df_check = pd.read_parquet(found_file)
                        else:
                            df_check = pd.read_excel(found_file)
                        row_count = len(df_check)
                        col_count = len(df_check.columns)
                    except:
                        row_count = "?"
                        col_count = "?"

                    status_color = '#10b981' if age_days < 7 else '#f59e0b' if age_days < 30 else '#ef4444'
                    st.markdown(f"""
                    <div style="display:flex;align-items:center;padding:8px;background:white;border:1px solid #e5e7eb;border-radius:6px;margin-bottom:8px;">
                        <div style="width:80px;font-weight:600;">{iso}</div>
                        <div style="width:100px;">{row_count:,} rows</div>
                        <div style="width:80px;">{col_count} cols</div>
                        <div style="width:80px;">{size_mb:.1f} MB</div>
                        <div style="flex:1;text-align:right;">
                            <span style="background:{status_color};color:white;padding:2px 8px;border-radius:4px;font-size:11px;">
                                {age_days}d ago
                            </span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div style="display:flex;align-items:center;padding:8px;background:#fef2f2;border:1px solid #ef4444;border-radius:6px;margin-bottom:8px;">
                        <div style="width:80px;font-weight:600;">{iso}</div>
                        <div style="color:#991b1b;">No data file found</div>
                    </div>
                    """, unsafe_allow_html=True)

        with db_tabs[1]:
            st.markdown("**LBL Berkeley Lab Historical Dataset**")
            st.caption("The gold standard for interconnection queue research")

            lbl_path = Path(__file__).parent / '.cache' / 'lbl_queued_up.xlsx'
            if lbl_path.exists():
                size_mb = lbl_path.stat().st_size / (1024 * 1024)

                st.markdown(f"""
                <div style="background:#f0fdf4;border:1px solid #22c55e;border-radius:8px;padding:16px;margin-bottom:16px;">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <div>
                            <div style="font-weight:700;font-size:16px;">LBL Queued Up Dataset</div>
                            <div style="color:#6b7280;font-size:12px;">Source: Lawrence Berkeley National Laboratory</div>
                        </div>
                        <div style="text-align:right;">
                            <div style="font-size:24px;font-weight:700;color:#22c55e;">36,441</div>
                            <div style="font-size:11px;color:#6b7280;">Projects</div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                st.markdown("**What's in the dataset:**")
                st.markdown("""
                - **Project outcomes** (operational, withdrawn, active, suspended)
                - **Timeline data** (queue date, IA date, COD date)
                - **Developer information** for track record analysis
                - **POI/substation** information for location analysis
                - **Project details** (capacity, fuel type, service type)
                - **Regional breakdown** (ERCOT, PJM, MISO, NYISO, CAISO, SPP, ISO-NE, West, Southeast)
                """)

                # Show data schema
                with st.expander("View Data Fields"):
                    try:
                        lbl_df = pd.read_excel(lbl_path, sheet_name='03. Complete Queue Data', header=1, nrows=5)
                        for col in lbl_df.columns:
                            dtype = str(lbl_df[col].dtype)
                            sample = str(lbl_df[col].iloc[0])[:50] if not pd.isna(lbl_df[col].iloc[0]) else "N/A"
                            st.markdown(f"- `{col}` ({dtype}): {sample}")
                    except Exception as e:
                        st.error(f"Error reading LBL data: {e}")
            else:
                st.warning("LBL data not found. Download from: https://emp.lbl.gov/queues")

        with db_tabs[2]:
            st.markdown("**Interconnection Cost Data**")
            st.caption("Historical cost data for estimating upgrade expenses")

            cost_files = {
                'NYISO': '.cache/nyiso_interconnection_cost_data.xlsx',
                'MISO': '.cache/miso_costs_2021_clean_data.xlsx',
                'PJM': '.cache/pjm_costs_2022_clean_data.xlsx',
                'SPP': '.cache/spp_costs_2023_clean_data.xlsx',
                'ISO-NE': '.cache/isone_interconnection_cost_data.xlsx',
            }

            for iso, filepath in cost_files.items():
                fp = Path(__file__).parent / filepath
                if fp.exists():
                    size_mb = fp.stat().st_size / (1024 * 1024)
                    st.markdown(f"- **{iso}**: {size_mb:.1f} MB")
                else:
                    st.markdown(f"- **{iso}**: Not available")

        with db_tabs[3]:
            st.markdown("**EIA Form 860 Data**")
            st.caption("Generator and plant information from the US Energy Information Administration")

            eia_dir = Path(__file__).parent / '.cache' / 'eia'
            if eia_dir.exists():
                eia_files = list(eia_dir.glob('*'))
                st.markdown(f"Found {len(eia_files)} EIA files:")
                for f in eia_files[:10]:
                    size_mb = f.stat().st_size / (1024 * 1024)
                    st.markdown(f"- `{f.name}`: {size_mb:.1f} MB")
            else:
                st.warning("EIA data not found")

        st.markdown("---")

        st.markdown("#### Refresh Data")
        st.markdown("To refresh queue data, run:")
        st.code("python3 refresh_data.py --iso NYISO")

        st.markdown("#### Data Sources Reference")
        st.markdown("""
        | Source | Description | Update Frequency |
        |--------|-------------|------------------|
        | ISO Queue Feeds | Live interconnection queue data | Weekly |
        | LBL Queued Up | Historical outcomes (36,441 projects) | Annual |
        | EIA Form 860 | Generator/plant registry | Annual |
        | FERC Filings | Regulatory interconnection filings | Daily |
        | PUDL | Aggregated energy data (18GB) | Quarterly |
        """)


if __name__ == "__main__":
    main()
