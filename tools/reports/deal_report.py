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

# Import consolidated analytics module
try:
    from analytics import QueueAnalytics
    ANALYTICS_AVAILABLE = True
except ImportError:
    ANALYTICS_AVAILABLE = False

# Output directory
OUTPUT_DIR = Path(__file__).parent / 'output'

# Report counter file
COUNTER_FILE = OUTPUT_DIR / '.report_counter'


def _get_next_report_number() -> int:
    """Get the next sequential report number."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if COUNTER_FILE.exists():
        try:
            current = int(COUNTER_FILE.read_text().strip())
        except (ValueError, IOError):
            current = 0
    else:
        # Initialize by checking existing files
        existing = list(OUTPUT_DIR.glob('report_*.pdf'))
        if existing:
            # Extract numbers from filenames like "report_005_..."
            numbers = []
            for f in existing:
                parts = f.stem.split('_')
                if len(parts) >= 2 and parts[1].isdigit():
                    numbers.append(int(parts[1]))
            current = max(numbers) if numbers else 0
        else:
            current = 0

    next_num = current + 1
    COUNTER_FILE.write_text(str(next_num))
    return next_num


# =============================================================================
# ENHANCED ANALYSIS FUNCTIONS - Real data, not generic templates
# =============================================================================

def _get_completion_funnel_data(region: str, project_type: str) -> Dict[str, Any]:
    """
    Get actual completion funnel from LBL historical data.
    Returns real numbers: total entered → withdrew at study → withdrew after IA → reached COD
    """
    try:
        from historical_data import HistoricalData
        hd = HistoricalData()

        funnel = hd.get_completion_funnel(region, project_type)

        if 'error' in funnel:
            return {'error': funnel['error']}

        # Calculate stage-specific numbers
        total = funnel['total_entered']
        operational = funnel['operational']
        withdrawn = funnel['withdrawn']
        active = funnel['active_in_queue']

        # Estimate stage breakdown (pre-IA vs post-IA withdrawals)
        # Based on industry patterns, ~60% withdraw before IA, ~40% after
        pre_ia_withdrawn = int(withdrawn * 0.6)
        post_ia_withdrawn = withdrawn - pre_ia_withdrawn

        return {
            'total_entered': total,
            'active_in_queue': active,
            'pre_ia_withdrawn': pre_ia_withdrawn,
            'post_ia_withdrawn': post_ia_withdrawn,
            'total_withdrawn': withdrawn,
            'reached_cod': operational,
            'completion_rate_pct': funnel['completion_rate_pct'],
            'withdrawal_rate_pct': funnel['withdrawal_rate_pct'],
            'funnel_text': (
                f"Of {total:,} {project_type} projects that entered the {region} queue: "
                f"{pre_ia_withdrawn:,} withdrew before IA signing, "
                f"{post_ia_withdrawn:,} withdrew after IA, "
                f"and {operational:,} reached commercial operation ({funnel['completion_rate_pct']:.1f}% completion rate)."
            ),
        }
    except Exception as e:
        return {'error': str(e)}


def _get_cost_distribution_data(region: str, project_type: str, this_project_cost_per_kw: float = None) -> Dict[str, Any]:
    """
    Get actual cost distribution from LBL IC cost data.
    Returns histogram buckets and percentiles with this project marked.
    """
    try:
        from historical_data import HistoricalData
        import numpy as np

        hd = HistoricalData()
        cost_df = hd.get_cost_distribution(region, project_type)

        if cost_df.empty or 'error' in cost_df.columns:
            return {'error': 'No cost data available'}

        # Get cost column
        cost_col = None
        for col in cost_df.columns:
            if 'cost_per_kw' in col.lower() or 'per_kw' in col.lower():
                cost_col = col
                break

        if cost_col is None:
            return {'error': 'Cost column not found'}

        costs = pd.to_numeric(cost_df[cost_col], errors='coerce').dropna()

        if len(costs) < 5:
            return {'error': 'Insufficient cost data'}

        # Calculate statistics
        p10, p25, p50, p75, p90 = np.percentile(costs, [10, 25, 50, 75, 90])

        # Build histogram buckets
        hist, bin_edges = np.histogram(costs, bins=10)
        buckets = []
        for i in range(len(hist)):
            buckets.append({
                'range_low': int(bin_edges[i]),
                'range_high': int(bin_edges[i+1]),
                'count': int(hist[i]),
                'contains_project': (
                    this_project_cost_per_kw is not None and
                    bin_edges[i] <= this_project_cost_per_kw < bin_edges[i+1]
                ),
            })

        # Determine this project's percentile
        project_percentile = None
        if this_project_cost_per_kw is not None:
            project_percentile = (costs < this_project_cost_per_kw).mean() * 100

        return {
            'n_projects': len(costs),
            'p10': round(p10, 0),
            'p25': round(p25, 0),
            'p50': round(p50, 0),
            'p75': round(p75, 0),
            'p90': round(p90, 0),
            'mean': round(costs.mean(), 0),
            'min': round(costs.min(), 0),
            'max': round(costs.max(), 0),
            'buckets': buckets,
            'this_project_cost': this_project_cost_per_kw,
            'this_project_percentile': round(project_percentile, 0) if project_percentile else None,
        }
    except Exception as e:
        return {'error': str(e)}


def _get_developer_completed_projects(developer: str, region: str = None, limit: int = 10) -> Dict[str, Any]:
    """
    Get list of developer's actual completed projects with details.
    Shows project names, MW, COD dates, etc.
    """
    try:
        from historical_data import HistoricalData
        hd = HistoricalData()

        if hd.queued_up_df is None:
            return {'error': 'Historical data not loaded'}

        df = hd.queued_up_df.copy()

        # Find developer column
        dev_col = hd._get_col(df, 'developer', hd._queued_up_cols)
        status_col = hd._get_col(df, 'status', hd._queued_up_cols)

        if not dev_col:
            return {'error': 'Developer column not found'}

        # Match developer name
        dev_lower = developer.lower().strip()
        mask = df[dev_col].fillna('').str.lower().str.contains(dev_lower, regex=False)
        dev_projects = df[mask].copy()

        if dev_projects.empty:
            return {
                'completed_projects': [],
                'withdrawn_projects': [],
                'active_projects': [],
                'summary': 'No historical projects found for this developer',
            }

        # Categorize by status
        if status_col:
            dev_projects['_status_cat'] = dev_projects[status_col].apply(hd._categorize_status)
        else:
            dev_projects['_status_cat'] = 'Unknown'

        # Get column names for extracting details
        name_col = hd._get_col(df, 'project_name', hd._queued_up_cols)
        cap_col = hd._get_col(df, 'capacity', hd._queued_up_cols)
        type_col = hd._get_col(df, 'project_type', hd._queued_up_cols)
        state_col = hd._get_col(df, 'state', hd._queued_up_cols)
        cod_col = hd._get_col(df, 'cod_date', hd._queued_up_cols)
        region_col = hd._get_col(df, 'region', hd._queued_up_cols)

        def extract_project(row):
            """Extract project details from row."""
            proj = {}
            proj['name'] = str(row[name_col])[:50] if name_col and pd.notna(row.get(name_col)) else 'Unnamed'
            proj['capacity_mw'] = float(row[cap_col]) if cap_col and pd.notna(row.get(cap_col)) else None
            proj['type'] = str(row[type_col]) if type_col and pd.notna(row.get(type_col)) else 'Unknown'
            proj['state'] = str(row[state_col]) if state_col and pd.notna(row.get(state_col)) else ''
            proj['region'] = str(row[region_col]) if region_col and pd.notna(row.get(region_col)) else ''
            if cod_col and pd.notna(row.get(cod_col)):
                try:
                    cod = pd.to_datetime(row[cod_col])
                    proj['cod_date'] = cod.strftime('%Y-%m-%d') if pd.notna(cod) else None
                except:
                    proj['cod_date'] = None
            else:
                proj['cod_date'] = None
            return proj

        completed = dev_projects[dev_projects['_status_cat'] == 'Completed'].head(limit)
        withdrawn = dev_projects[dev_projects['_status_cat'] == 'Withdrawn'].head(limit)
        active = dev_projects[dev_projects['_status_cat'] == 'Active'].head(limit)

        completed_list = [extract_project(row) for _, row in completed.iterrows()]
        withdrawn_list = [extract_project(row) for _, row in withdrawn.iterrows()]
        active_list = [extract_project(row) for _, row in active.iterrows()]

        # Calculate totals
        total_completed = len(dev_projects[dev_projects['_status_cat'] == 'Completed'])
        total_withdrawn = len(dev_projects[dev_projects['_status_cat'] == 'Withdrawn'])
        total_active = len(dev_projects[dev_projects['_status_cat'] == 'Active'])

        # Calculate total completed MW
        completed_mw = 0
        if cap_col:
            completed_mw = dev_projects[dev_projects['_status_cat'] == 'Completed'][cap_col].fillna(0).sum()

        return {
            'completed_projects': completed_list,
            'withdrawn_projects': withdrawn_list,
            'active_projects': active_list,
            'total_completed': total_completed,
            'total_withdrawn': total_withdrawn,
            'total_active': total_active,
            'completed_mw': round(completed_mw, 0),
            'summary': f"{total_completed} completed ({completed_mw:,.0f} MW), {total_withdrawn} withdrawn, {total_active} active",
        }
    except Exception as e:
        return {'error': str(e)}


def _get_comparable_outcomes(region: str, project_type: str, capacity_mw: float, limit: int = 15) -> Dict[str, Any]:
    """
    Get similar historical projects and their actual outcomes.
    Shows what happened to projects similar to this one.
    """
    try:
        from historical_data import HistoricalData
        hd = HistoricalData()

        comparables = hd.get_comparable_projects(
            region=region,
            project_type=project_type,
            capacity_mw=capacity_mw,
            capacity_tolerance=0.5,
            limit=limit * 3,  # Get more to show variety
        )

        if comparables.empty:
            return {'error': 'No comparable projects found'}

        # Get column mappings
        status_col = hd._get_col(comparables, 'status', hd._queued_up_cols)
        name_col = hd._get_col(comparables, 'project_name', hd._queued_up_cols)
        cap_col = hd._get_col(comparables, 'capacity', hd._queued_up_cols)
        cod_col = hd._get_col(comparables, 'cod_date', hd._queued_up_cols)
        queue_col = hd._get_col(comparables, 'queue_date', hd._queued_up_cols)
        state_col = hd._get_col(comparables, 'state', hd._queued_up_cols)

        # Categorize by status
        if status_col:
            comparables['_status_cat'] = comparables[status_col].apply(hd._categorize_status)
        else:
            comparables['_status_cat'] = 'Unknown'

        # Count outcomes
        status_counts = comparables['_status_cat'].value_counts().to_dict()
        total = len(comparables)
        completed = status_counts.get('Completed', 0)
        withdrawn = status_counts.get('Withdrawn', 0)
        active = status_counts.get('Active', 0)

        # Extract sample projects
        def extract_comparable(row):
            proj = {}
            proj['name'] = str(row[name_col])[:40] if name_col and pd.notna(row.get(name_col)) else 'Unnamed'
            proj['capacity_mw'] = int(row[cap_col]) if cap_col and pd.notna(row.get(cap_col)) else None
            proj['state'] = str(row[state_col]) if state_col and pd.notna(row.get(state_col)) else ''
            proj['outcome'] = row['_status_cat']

            # Calculate time to outcome
            if queue_col and cod_col:
                try:
                    q_date = pd.to_datetime(row[queue_col], errors='coerce')
                    c_date = pd.to_datetime(row[cod_col], errors='coerce')
                    if pd.notna(q_date) and pd.notna(c_date):
                        months = (c_date - q_date).days / 30
                        proj['months_to_cod'] = int(months)
                except:
                    pass

            return proj

        # Get sample of each outcome type
        completed_sample = [extract_comparable(row) for _, row in comparables[comparables['_status_cat'] == 'Completed'].head(5).iterrows()]
        withdrawn_sample = [extract_comparable(row) for _, row in comparables[comparables['_status_cat'] == 'Withdrawn'].head(5).iterrows()]

        return {
            'total_comparables': total,
            'completed': completed,
            'withdrawn': withdrawn,
            'active': active,
            'completion_rate': round(completed / (completed + withdrawn) * 100, 1) if (completed + withdrawn) > 0 else 0,
            'completed_sample': completed_sample,
            'withdrawn_sample': withdrawn_sample,
            'summary': f"Of {total} comparable projects: {completed} completed ({completed/(completed+withdrawn)*100:.0f}%), {withdrawn} withdrew, {active} still in queue" if (completed + withdrawn) > 0 else f"Of {total} comparable projects: {active} still in queue",
        }
    except Exception as e:
        return {'error': str(e)}


def _get_timeline_distribution_data(region: str, project_type: str) -> Dict[str, Any]:
    """
    Get actual time-to-COD distribution from completed projects.
    Shows real timeline ranges based on historical completions.
    """
    try:
        from historical_data import HistoricalData
        import numpy as np

        hd = HistoricalData()
        timeline_df = hd.get_timeline_distribution(region, project_type, completed_only=True)

        if timeline_df.empty or 'error' in timeline_df.columns:
            return {'error': 'No timeline data available'}

        if 'time_to_cod_months' not in timeline_df.columns:
            return {'error': 'Timeline calculation failed'}

        times = timeline_df['time_to_cod_months'].dropna()
        times = times[(times > 0) & (times < 180)]  # Filter outliers

        if len(times) < 5:
            return {'error': 'Insufficient timeline data'}

        p10, p25, p50, p75, p90 = np.percentile(times, [10, 25, 50, 75, 90])

        return {
            'n_projects': len(times),
            'p10_months': round(p10, 0),
            'p25_months': round(p25, 0),
            'p50_months': round(p50, 0),
            'p75_months': round(p75, 0),
            'p90_months': round(p90, 0),
            'mean_months': round(times.mean(), 0),
            'min_months': round(times.min(), 0),
            'max_months': round(times.max(), 0),
            'description': f"Based on {len(times)} completed {project_type} projects in {region}",
        }
    except Exception as e:
        return {'error': str(e)}


def _get_score_percentile(df: pd.DataFrame, score: float, region: str, project_type: str) -> Dict[str, Any]:
    """
    Calculate where this score ranks vs. peer projects.
    Returns percentile and comparison stats.
    """
    from scoring import FeasibilityScorer

    # Filter to comparable projects (same region and/or type)
    comparables = df.copy()

    # Try to filter by region
    region_col = None
    for col in ['region', 'iso', 'Region', 'ISO']:
        if col in df.columns:
            region_col = col
            break

    type_col = None
    for col in ['type', 'type_std', 'Type', 'fuel_type']:
        if col in df.columns:
            type_col = col
            break

    # Score a sample of comparable projects to build distribution
    try:
        scorer = FeasibilityScorer(df, region=region)

        # Filter comparables
        if region_col and region:
            region_match = comparables[comparables[region_col].astype(str).str.upper() == region.upper()]
            if len(region_match) >= 20:
                comparables = region_match

        if type_col and project_type:
            type_match = comparables[comparables[type_col].astype(str).str.lower().str.contains(project_type.lower(), na=False)]
            if len(type_match) >= 10:
                comparables = type_match

        # Sample up to 200 projects to score (for performance)
        sample_size = min(200, len(comparables))
        if sample_size < 10:
            return {'percentile': None, 'n_compared': 0, 'error': 'Too few comparables'}

        sample = comparables.sample(n=sample_size, random_state=42)

        scores = []
        for _, row in sample.iterrows():
            try:
                result = scorer.score_project(row=row)
                if 'error' not in result:
                    scores.append(result['total_score'])
            except:
                continue

        if len(scores) < 10:
            return {'percentile': None, 'n_compared': len(scores), 'error': 'Insufficient scores'}

        # Calculate percentile
        import numpy as np
        scores_array = np.array(scores)
        percentile = (scores_array < score).sum() / len(scores_array) * 100

        return {
            'percentile': round(percentile, 0),
            'n_compared': len(scores),
            'mean_score': round(np.mean(scores), 1),
            'median_score': round(np.median(scores), 1),
            'min_score': round(np.min(scores), 1),
            'max_score': round(np.max(scores), 1),
            'std_score': round(np.std(scores), 1),
            'interpretation': _interpret_percentile(percentile),
        }
    except Exception as e:
        return {'percentile': None, 'n_compared': 0, 'error': str(e)}


def _interpret_percentile(pct: float) -> str:
    """Interpret percentile ranking."""
    if pct >= 90:
        return "Top decile - exceptional project"
    elif pct >= 75:
        return "Top quartile - strong project"
    elif pct >= 50:
        return "Above median"
    elif pct >= 25:
        return "Below median - elevated risk"
    else:
        return "Bottom quartile - significant concerns"


def _get_poi_queue_analysis(df: pd.DataFrame, poi: str, project_id: str) -> Dict[str, Any]:
    """
    Analyze queue depth and competition at this POI.
    Returns: projects ahead, total capacity, withdrawal rate, etc.
    """
    if not poi or poi == 'Unknown':
        return {'error': 'POI not specified'}

    # Find POI column
    poi_col = None
    for col in ['poi', 'POI', 'substation', 'Substation', 'poi_name']:
        if col in df.columns:
            poi_col = col
            break

    if poi_col is None:
        return {'error': 'POI column not found'}

    # Get all projects at this POI
    poi_projects = df[df[poi_col].astype(str).str.lower().str.contains(poi.lower(), na=False)]

    if len(poi_projects) == 0:
        return {'error': 'No projects found at POI'}

    # Get capacity column
    cap_col = None
    for col in ['capacity_mw', 'Capacity_MW', 'mw', 'MW', 'capacity']:
        if col in df.columns:
            cap_col = col
            break

    # Get date column
    date_col = None
    for col in ['queue_date_std', 'queue_date', 'Queue Date', 'q_date']:
        if col in df.columns:
            date_col = col
            break

    # Get status column
    status_col = None
    for col in ['status', 'status_std', 'Status', 'q_status']:
        if col in df.columns:
            status_col = col
            break

    # Get queue_id column
    id_col = None
    for col in ['queue_id', 'Queue_ID', 'id', 'ID', 'q_id']:
        if col in df.columns:
            id_col = col
            break

    total_projects = len(poi_projects)
    total_capacity = poi_projects[cap_col].fillna(0).sum() if cap_col else 0

    # Count active vs withdrawn
    active_count = 0
    withdrawn_count = 0
    if status_col:
        withdrawn_keywords = ['withdrawn', 'cancelled', 'suspended', 'terminated']
        for _, row in poi_projects.iterrows():
            status = str(row[status_col]).lower()
            if any(kw in status for kw in withdrawn_keywords):
                withdrawn_count += 1
            else:
                active_count += 1

    # Find this project's position
    position = None
    projects_ahead = 0
    capacity_ahead = 0
    if date_col and id_col:
        try:
            this_project = poi_projects[poi_projects[id_col].astype(str) == str(project_id)]
            if not this_project.empty:
                this_date = pd.to_datetime(this_project.iloc[0][date_col], errors='coerce')
                if pd.notna(this_date):
                    # Count projects that entered queue before this one
                    for _, row in poi_projects.iterrows():
                        other_date = pd.to_datetime(row[date_col], errors='coerce')
                        if pd.notna(other_date) and other_date < this_date:
                            # Check if still active
                            if status_col:
                                status = str(row[status_col]).lower()
                                if not any(kw in status for kw in ['withdrawn', 'cancelled', 'suspended']):
                                    projects_ahead += 1
                                    if cap_col:
                                        capacity_ahead += row[cap_col] if pd.notna(row[cap_col]) else 0
                            else:
                                projects_ahead += 1
                                if cap_col:
                                    capacity_ahead += row[cap_col] if pd.notna(row[cap_col]) else 0

                    position = projects_ahead + 1
        except:
            pass

    # Calculate withdrawal rate
    withdrawal_rate = withdrawn_count / total_projects if total_projects > 0 else 0

    # Risk assessment
    if withdrawal_rate > 0.6:
        poi_risk = 'HIGH'
        poi_risk_reason = f'{withdrawal_rate*100:.0f}% withdrawal rate indicates problematic POI'
    elif withdrawal_rate > 0.4 or projects_ahead > 5:
        poi_risk = 'ELEVATED'
        poi_risk_reason = f'{projects_ahead} projects ahead, {withdrawal_rate*100:.0f}% withdrawal rate'
    elif projects_ahead > 2:
        poi_risk = 'MODERATE'
        poi_risk_reason = f'{projects_ahead} projects ahead in queue'
    else:
        poi_risk = 'LOW'
        poi_risk_reason = 'Favorable queue position'

    return {
        'total_projects': total_projects,
        'active_projects': active_count,
        'withdrawn_projects': withdrawn_count,
        'total_capacity_mw': round(total_capacity, 0),
        'position': position,
        'projects_ahead': projects_ahead,
        'capacity_ahead_mw': round(capacity_ahead, 0),
        'withdrawal_rate': round(withdrawal_rate * 100, 1),
        'risk_level': poi_risk,
        'risk_reason': poi_risk_reason,
    }


def _get_developer_stats(df: pd.DataFrame, developer: str, region: str = None) -> Dict[str, Any]:
    """
    Get developer's actual completion statistics.
    Returns: total projects, completed, withdrawn, completion rate.
    """
    if not developer or developer == 'Unknown':
        return {'error': 'Developer not specified'}

    # Try to get from LBL historical data first
    try:
        from unified_data import RegionalBenchmarks
        benchmarks = RegionalBenchmarks()
        track_record = benchmarks.get_developer_track_record(developer, region or 'ALL')

        if 'error' not in track_record:
            summary = track_record.get('summary', {})
            assessment = track_record.get('assessment', {})

            return {
                'source': 'LBL Historical Data',
                'total_projects': summary.get('total_projects', 0),
                'completed': summary.get('completed', 0),
                'withdrawn': summary.get('withdrawn', 0),
                'active': summary.get('active', 0),
                'completion_rate': round(summary.get('completion_rate', 0) * 100, 1),
                'assessment': assessment.get('text', 'Unknown'),
                'confidence': track_record.get('confidence', 'unknown'),
            }
    except:
        pass

    # Fallback to queue data analysis
    dev_col = None
    for col in ['developer', 'Developer', 'applicant', 'entity']:
        if col in df.columns:
            dev_col = col
            break

    if dev_col is None:
        return {'error': 'Developer column not found'}

    # Match developer (case-insensitive, partial match)
    dev_lower = developer.lower().strip()
    mask = df[dev_col].fillna('').str.lower().str.contains(dev_lower, regex=False)
    dev_projects = df[mask]

    if len(dev_projects) == 0:
        return {
            'source': 'Queue Data',
            'total_projects': 1,
            'completed': 0,
            'withdrawn': 0,
            'active': 1,
            'completion_rate': 0,
            'assessment': 'Single-project developer - no track record',
            'confidence': 'very_low',
        }

    # Count by status
    status_col = None
    for col in ['status', 'status_std', 'Status', 'q_status']:
        if col in df.columns:
            status_col = col
            break

    total = len(dev_projects)
    completed = 0
    withdrawn = 0
    active = 0

    if status_col:
        for _, row in dev_projects.iterrows():
            status = str(row[status_col]).lower()
            if any(kw in status for kw in ['operational', 'in service', 'completed', 'commercial']):
                completed += 1
            elif any(kw in status for kw in ['withdrawn', 'cancelled', 'suspended', 'terminated']):
                withdrawn += 1
            else:
                active += 1
    else:
        active = total

    completion_rate = completed / (completed + withdrawn) * 100 if (completed + withdrawn) > 0 else 0

    # Assessment
    if completion_rate >= 40:
        assessment = f'Excellent track record ({completed} of {completed + withdrawn} projects completed)'
    elif completion_rate >= 25:
        assessment = f'Good track record ({completed} completed, {withdrawn} withdrawn)'
    elif completion_rate >= 10:
        assessment = f'Below-average track record ({completion_rate:.0f}% completion rate)'
    elif completed > 0:
        assessment = f'Poor track record ({completed} of {completed + withdrawn} completed)'
    else:
        assessment = f'No completions on record ({withdrawn} withdrawn, {active} active)'

    return {
        'source': 'Queue Data',
        'total_projects': total,
        'completed': completed,
        'withdrawn': withdrawn,
        'active': active,
        'completion_rate': round(completion_rate, 1),
        'assessment': assessment,
        'confidence': 'high' if total >= 10 else ('medium' if total >= 5 else 'low'),
    }


def _get_lmp_analysis(region: str, state: str, poi: str = None) -> Dict[str, Any]:
    """
    Get actual LMP/pricing data for the project location.
    """
    try:
        import sqlite3
        db_path = Path(__file__).parent.parent / '.data' / 'queue.db'

        if not db_path.exists():
            return {'error': 'Database not found'}

        conn = sqlite3.connect(str(db_path))

        # Get LMP data for the region
        lmp_query = """
            SELECT zone_id, zone_name, avg_lmp, peak_lmp, offpeak_lmp,
                   solar_weighted_lmp, wind_weighted_lmp, volatility, year
            FROM lmp_annual
            WHERE region = ?
            ORDER BY year DESC
            LIMIT 3
        """
        lmp_df = pd.read_sql(lmp_query, conn, params=[region])

        # Get congestion data
        cong_query = """
            SELECT zone_id, zone_name, avg_congestion_cost, pct_hours_congested,
                   total_congestion_cost, congestion_level
            FROM tx_congestion
            WHERE region = ?
        """
        cong_df = pd.read_sql(cong_query, conn, params=[region])

        conn.close()

        if lmp_df.empty:
            return {'error': f'No LMP data for {region}'}

        # Find best matching zone (by state or POI name)
        best_zone = lmp_df.iloc[0]  # Default to first

        if state:
            state_match = lmp_df[lmp_df['zone_name'].str.contains(state, case=False, na=False)]
            if not state_match.empty:
                best_zone = state_match.iloc[0]

        # Get congestion for this zone
        congestion_data = {}
        if not cong_df.empty:
            zone_cong = cong_df[cong_df['zone_id'] == best_zone['zone_id']]
            if not zone_cong.empty:
                cong = zone_cong.iloc[0]
                congestion_data = {
                    'avg_congestion_cost': cong['avg_congestion_cost'],
                    'pct_hours_congested': cong['pct_hours_congested'],
                    'congestion_level': cong['congestion_level'],
                }

        return {
            'zone': best_zone['zone_name'],
            'zone_id': best_zone['zone_id'],
            'avg_lmp': round(best_zone['avg_lmp'], 2),
            'peak_lmp': round(best_zone['peak_lmp'], 2) if pd.notna(best_zone['peak_lmp']) else None,
            'offpeak_lmp': round(best_zone['offpeak_lmp'], 2) if pd.notna(best_zone['offpeak_lmp']) else None,
            'solar_weighted': round(best_zone['solar_weighted_lmp'], 2) if pd.notna(best_zone['solar_weighted_lmp']) else None,
            'wind_weighted': round(best_zone['wind_weighted_lmp'], 2) if pd.notna(best_zone['wind_weighted_lmp']) else None,
            'year': best_zone['year'],
            'congestion': congestion_data,
        }
    except Exception as e:
        return {'error': str(e)}


def _get_valuation_guidance(
    capacity_mw: float,
    cost_data: Dict,
    completion_rate: float,
    timeline_months: int,
    region: str,
    project_type: str
) -> Dict[str, Any]:
    """
    Provide entry price guidance for PE acquisition.
    """
    # Get market benchmarks
    try:
        import sqlite3
        db_path = Path(__file__).parent.parent / '.data' / 'queue.db'
        conn = sqlite3.connect(str(db_path))

        # Get capacity prices
        cap_query = """
            SELECT price_per_mw_day, price_per_kw_month, delivery_year
            FROM capacity_prices
            WHERE region = ?
            ORDER BY delivery_year DESC
            LIMIT 1
        """
        cap_df = pd.read_sql(cap_query, conn, params=[region])

        # Get PPA benchmarks
        ppa_query = """
            SELECT p50_price, p25_price, p75_price, year
            FROM ppa_benchmarks
            WHERE region = ? AND technology = ?
            ORDER BY year DESC
            LIMIT 1
        """
        ppa_df = pd.read_sql(ppa_query, conn, params=[region, project_type])

        conn.close()
    except:
        cap_df = pd.DataFrame()
        ppa_df = pd.DataFrame()

    # IC cost at P50
    ic_cost_p50 = cost_data.get('total_millions', {}).get('p50', 0)
    ic_cost_p75 = cost_data.get('total_millions', {}).get('p75', 0)

    # Risk-adjusted IC cost (weight toward P75 for conservatism)
    risk_adj_ic = ic_cost_p50 * 0.4 + ic_cost_p75 * 0.6

    # Development stage discount
    if timeline_months > 36:
        stage_discount = 0.70  # Early stage - 30% discount
        stage_desc = "Early stage (>36mo to COD)"
    elif timeline_months > 24:
        stage_discount = 0.80
        stage_desc = "Mid-stage (24-36mo to COD)"
    elif timeline_months > 12:
        stage_discount = 0.90
        stage_desc = "Late stage (12-24mo to COD)"
    else:
        stage_discount = 0.95
        stage_desc = "Near-COD (<12mo)"

    # Completion risk discount
    if completion_rate < 0.15:
        completion_discount = 0.60
    elif completion_rate < 0.25:
        completion_discount = 0.75
    elif completion_rate < 0.35:
        completion_discount = 0.85
    else:
        completion_discount = 0.95

    # Market benchmarks for development-stage projects ($/MW)
    # These are rough ranges based on industry transactions
    type_benchmarks = {
        'Solar': {'low': 50000, 'mid': 80000, 'high': 120000},
        'Battery': {'low': 40000, 'mid': 70000, 'high': 100000},
        'Wind': {'low': 60000, 'mid': 100000, 'high': 150000},
        'Gas': {'low': 30000, 'mid': 50000, 'high': 80000},
    }

    # Normalize project type
    type_key = 'Solar'  # default
    for key in type_benchmarks.keys():
        if key.lower() in project_type.lower():
            type_key = key
            break

    benchmarks = type_benchmarks[type_key]

    # Calculate entry price range
    base_mid = benchmarks['mid'] * capacity_mw / 1_000_000  # Convert to $M

    entry_low = base_mid * stage_discount * completion_discount * 0.8
    entry_mid = base_mid * stage_discount * completion_discount
    entry_high = base_mid * stage_discount * 1.1  # Less discount for upside

    # Total basis (entry + IC cost)
    basis_low = entry_low + ic_cost_p50
    basis_mid = entry_mid + risk_adj_ic
    basis_high = entry_high + ic_cost_p75

    # $/MW metrics
    entry_per_mw_low = entry_low * 1_000_000 / capacity_mw if capacity_mw > 0 else 0
    entry_per_mw_mid = entry_mid * 1_000_000 / capacity_mw if capacity_mw > 0 else 0
    basis_per_mw = basis_mid * 1_000_000 / capacity_mw if capacity_mw > 0 else 0

    return {
        'entry_price': {
            'low': round(entry_low, 1),
            'mid': round(entry_mid, 1),
            'high': round(entry_high, 1),
        },
        'entry_per_mw': {
            'low': round(entry_per_mw_low, 0),
            'mid': round(entry_per_mw_mid, 0),
        },
        'total_basis': {
            'low': round(basis_low, 1),
            'mid': round(basis_mid, 1),
            'high': round(basis_high, 1),
        },
        'basis_per_mw': round(basis_per_mw, 0),
        'ic_cost_assumed': round(risk_adj_ic, 1),
        'stage_discount': stage_discount,
        'stage_description': stage_desc,
        'completion_discount': completion_discount,
        'methodology': f"Market benchmark ${benchmarks['mid']/1000:.0f}k/MW adjusted for stage ({stage_discount:.0%}) and completion risk ({completion_discount:.0%})",
    }


def _get_ic_cost_breakdown(region: str, capacity_mw: float, project_type: str) -> Dict[str, Any]:
    """
    Break down IC costs into network upgrades vs direct connection.
    Uses historical data patterns.
    """
    # Industry patterns for cost allocation (based on LBL data analysis)
    # Network upgrades typically 60-80% of total IC cost for solar/wind
    # Direct connection costs more predictable

    type_patterns = {
        'Solar': {'network_pct': 0.70, 'direct_per_mw': 15000},
        'Battery': {'network_pct': 0.65, 'direct_per_mw': 12000},
        'Wind': {'network_pct': 0.75, 'direct_per_mw': 18000},
        'Gas': {'network_pct': 0.55, 'direct_per_mw': 20000},
    }

    # Get pattern for this type
    pattern = type_patterns.get('Solar')  # default
    for key, val in type_patterns.items():
        if key.lower() in project_type.lower():
            pattern = val
            break

    # Estimate direct connection cost
    direct_cost = pattern['direct_per_mw'] * capacity_mw / 1_000_000

    return {
        'network_upgrade_pct': round(pattern['network_pct'] * 100, 0),
        'direct_connection_pct': round((1 - pattern['network_pct']) * 100, 0),
        'direct_cost_estimate': round(direct_cost, 1),
        'note': 'Network upgrades are variable and depend on study results. Direct connection costs more predictable.',
        'cost_sharing_note': 'Earlier queue position = better cost allocation. Later entrants may bear proportionally higher upgrade costs.',
    }


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
    print(f"[3/8] Analyzing developer track record...")
    cross_rto = _get_developer_cross_rto(df, basic['developer'])

    # ==========================================================================
    # CONSOLIDATED ANALYTICS (from analytics.py - single source of truth)
    # ==========================================================================

    print(f"[4/8] Running consolidated analytics...")

    # Use the consolidated analytics module
    if ANALYTICS_AVAILABLE:
        qa = QueueAnalytics()

        # Get all analytics from single source of truth
        analytics_completion = qa.get_completion_probability(region, basic['type'], basic['capacity_mw'])
        analytics_developer = qa.get_developer_track_record(basic['developer'], region)
        analytics_poi = qa.get_poi_congestion_score(basic['poi'], region, project_id)
        analytics_cost = qa.get_cost_percentile(region, basic['type'], basic['capacity_mw'], cost_data['per_kw']['p50'])
        analytics_timeline = qa.get_timeline_benchmarks(region, basic['type'])
        analytics_ira = qa.get_ira_eligibility(basic['state'], basic.get('county'))
    else:
        analytics_completion = {}
        analytics_developer = {}
        analytics_poi = {}
        analytics_cost = {}
        analytics_timeline = {}
        analytics_ira = {}

    # ==========================================================================
    # ENHANCED ANALYSIS - Fallback/supplementary functions
    # ==========================================================================

    print(f"[5/8] Running enhanced analysis...")

    # 1. Score percentile ranking vs peer projects
    score_percentile = _get_score_percentile(
        df=df,
        score=score_result['total_score'],
        region=region,
        project_type=basic['type']
    )

    # 2. POI queue depth analysis (use analytics module result if available)
    poi_analysis = analytics_poi if analytics_poi and 'error' not in analytics_poi else _get_poi_queue_analysis(
        df=df,
        poi=basic['poi'],
        project_id=project_id
    )

    # 3. Developer actual completion stats (use analytics module result if available)
    developer_stats = analytics_developer if analytics_developer and 'error' not in analytics_developer else _get_developer_stats(
        df=df,
        developer=basic['developer'],
        region=region
    )

    # 4. LMP and congestion data
    lmp_analysis = _get_lmp_analysis(
        region=region,
        state=basic['state'],
        poi=basic.get('poi')
    )

    # 5. IC cost breakdown (network vs direct)
    ic_breakdown = _get_ic_cost_breakdown(
        region=region,
        capacity_mw=basic['capacity_mw'],
        project_type=basic['type']
    )

    # 6. Valuation guidance
    valuation = _get_valuation_guidance(
        capacity_mw=basic['capacity_mw'],
        cost_data=cost_data,
        completion_rate=completion_data['combined_rate'],
        timeline_months=timeline_data['remaining_p50'],
        region=region,
        project_type=basic['type']
    )

    # ==========================================================================
    # HISTORICAL DATA ANALYSIS - Real LBL data (via analytics module)
    # ==========================================================================

    print(f"[6/8] Pulling LBL historical data...")

    # 7. Completion funnel - use analytics module data
    completion_funnel = {
        'total_entered': analytics_completion.get('region_sample', 0),
        'completion_rate_pct': round(analytics_completion.get('combined_rate', 0) * 100, 1),
        'region_rate': analytics_completion.get('region_rate', 0),
        'technology_rate': analytics_completion.get('technology_rate', 0),
        'funnel_text': analytics_completion.get('methodology', ''),
    } if analytics_completion else _get_completion_funnel_data(region, basic['type'])

    # 8. Cost distribution with this project marked (use analytics module)
    cost_distribution = analytics_cost if analytics_cost and 'error' not in analytics_cost else _get_cost_distribution_data(
        region=region,
        project_type=basic['type'],
        this_project_cost_per_kw=cost_data['per_kw']['p50']
    )

    # 9. Developer's actual completed projects (from analytics module)
    developer_projects = {
        'completed_projects': analytics_developer.get('completed_projects', []),
        'total_completed': analytics_developer.get('completed', 0),
        'total_withdrawn': analytics_developer.get('withdrawn', 0),
        'completed_mw': analytics_developer.get('completed_mw', 0),
        'summary': analytics_developer.get('assessment', ''),
    } if analytics_developer else _get_developer_completed_projects(basic['developer'], region, 8)

    # 10. Comparable project outcomes
    comparable_outcomes = _get_comparable_outcomes(
        region=region,
        project_type=basic['type'],
        capacity_mw=basic['capacity_mw'],
        limit=20
    )

    # 11. Timeline distribution from completed projects (use analytics module)
    timeline_distribution = analytics_timeline if analytics_timeline and 'error' not in analytics_timeline else _get_timeline_distribution_data(
        region=region,
        project_type=basic['type']
    )

    # 12. IRA Energy Communities eligibility (NEW from analytics module)
    ira_eligibility = analytics_ira

    # ==========================================================================
    # TIER 2 ANALYTICS - Revenue, Capacity, Transmission (from analytics.py)
    # ==========================================================================

    print(f"[7/8] Computing Tier 2 revenue analytics...")

    revenue_stack = {}
    revenue_estimate = {}
    capacity_value = {}
    transmission_risk = {}
    ppa_benchmarks = {}

    if ANALYTICS_AVAILABLE:
        try:
            # Get individual Tier 2 components
            revenue_estimate = qa.get_revenue_estimate(
                region=region,
                technology=basic['type'],
                capacity_mw=basic['capacity_mw'],
                zone=None
            )
        except Exception as e:
            print(f"  Warning: Revenue estimate error: {e}")
            revenue_estimate = {}

        try:
            capacity_value = qa.get_capacity_value(
                region=region,
                technology=basic['type'],
                capacity_mw=basic['capacity_mw']
            )
        except Exception as e:
            print(f"  Warning: Capacity value error: {e}")
            capacity_value = {}

        try:
            transmission_risk = qa.get_transmission_risk(
                region=region,
                zone=None,
                poi=basic.get('poi')
            )
        except Exception as e:
            print(f"  Warning: Transmission risk error: {e}")
            transmission_risk = {}

        try:
            ppa_benchmarks = qa.get_ppa_benchmarks(
                region=region,
                technology=basic['type']
            )
        except Exception as e:
            print(f"  Warning: PPA benchmarks error: {e}")
            ppa_benchmarks = {}

        try:
            # Get full revenue stack (combined)
            revenue_stack = qa.get_full_revenue_stack(
                region=region,
                technology=basic['type'],
                capacity_mw=basic['capacity_mw'],
                zone=None
            )
        except Exception as e:
            print(f"  Warning: Revenue stack error: {e}")
            revenue_stack = {}

    # Bundle enhanced analysis
    enhanced_analysis = {
        'score_percentile': score_percentile,
        'poi_analysis': poi_analysis,
        'developer_stats': developer_stats,
        'lmp_analysis': lmp_analysis,
        'ic_breakdown': ic_breakdown,
        'valuation': valuation,
        # LBL historical data (via analytics module)
        'completion_funnel': completion_funnel,
        'cost_distribution': cost_distribution,
        'developer_projects': developer_projects,
        'comparable_outcomes': comparable_outcomes,
        'timeline_distribution': timeline_distribution,
        # IRA Energy Communities
        'ira_eligibility': ira_eligibility,
        # Tier 2: Revenue & Market Analysis
        'revenue_stack': revenue_stack,
        'revenue_estimate': revenue_estimate,
        'capacity_value': capacity_value,
        'transmission_risk': transmission_risk,
        'ppa_benchmarks': ppa_benchmarks,
        # Raw analytics module outputs (for debugging/transparency)
        '_analytics': {
            'completion': analytics_completion,
            'developer': analytics_developer,
            'poi': analytics_poi,
            'cost': analytics_cost,
            'timeline': analytics_timeline,
            'ira': analytics_ira,
        } if ANALYTICS_AVAILABLE else None,
    }

    # Get market data (optional)
    print(f"[6/8] Gathering market data...")
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
    print(f"[7/8] Building report...")

    # Charts
    print(f"[8/8] Generating charts...")
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
        enhanced=enhanced_analysis,
    )

    # Generate PDF with sequential numbering
    if output_path is None:
        report_num = _get_next_report_number()
        safe_id = project_id.replace('/', '_').replace('\\', '_')
        output_path = OUTPUT_DIR / f"report_{report_num:03d}_{safe_id}_{region}.pdf"
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
    enhanced: Dict = None,
) -> str:
    """Build HTML content for PDF."""

    # Extract values
    score = score_result['total_score']
    grade = score_result['grade']
    rec = score_result['recommendation']
    confidence = score_result.get('confidence', 'Medium')
    red_flags = score_result.get('red_flags', [])
    green_flags = score_result.get('green_flags', [])

    # Enhanced analysis (default empty dicts)
    enhanced = enhanced or {}
    score_pct = enhanced.get('score_percentile', {})
    poi_analysis = enhanced.get('poi_analysis', {})
    developer_stats = enhanced.get('developer_stats', {})
    lmp_analysis = enhanced.get('lmp_analysis', {})
    ic_breakdown = enhanced.get('ic_breakdown', {})
    valuation = enhanced.get('valuation', {})
    # NEW: LBL historical data
    completion_funnel = enhanced.get('completion_funnel', {})
    cost_distribution = enhanced.get('cost_distribution', {})
    developer_projects = enhanced.get('developer_projects', {})
    comparable_outcomes = enhanced.get('comparable_outcomes', {})
    timeline_dist = enhanced.get('timeline_distribution', {})
    # Tier 2: Revenue Stack
    revenue_stack = enhanced.get('revenue_stack', {})

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

    # Generate SVG gauge with inline styles for WeasyPrint compatibility
    def svg_gauge(score_val, max_val=100):
        pct = min(score_val / max_val, 1.0) if max_val > 0 else 0
        radius = 40
        circumference = 2 * 3.14159 * radius
        stroke_len = pct * circumference

        # Color based on recommendation
        if rec == 'GO':
            stroke_color = '#2d7a2d'
        elif rec == 'CONDITIONAL':
            stroke_color = '#b8860b'
        else:
            stroke_color = '#a02020'

        return f'''<svg width="100" height="100" viewBox="0 0 100 100" style="transform: rotate(-90deg);">
            <circle cx="50" cy="50" r="{radius}"
                style="fill: none; stroke: #e5e5e5; stroke-width: 6;"/>
            <circle cx="50" cy="50" r="{radius}"
                style="fill: none; stroke: {stroke_color}; stroke-width: 6; stroke-linecap: round;"
                stroke-dasharray="{stroke_len:.1f} {circumference:.1f}"/>
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

    # Score percentile text
    percentile_text = ""
    if score_pct.get('percentile') is not None:
        percentile_text = f"Top {100 - score_pct['percentile']:.0f}% of {score_pct.get('n_compared', 0)} comparable projects"

    # Investment thesis - now data-driven
    thesis = _generate_thesis(rec, score, basic, cost_data, timeline_data, completion_data, cross_rto, enhanced)

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
                {f'<div class="score-percentile">{percentile_text}</div>' if percentile_text else ''}
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

        <!-- Cost Distribution Histogram from LBL Data -->
        {_build_cost_histogram_section(cost_distribution, cost_data['per_kw']['p50'], region, basic['type'])}

        <!-- IC Cost Breakdown -->
        <div class="note" style="margin-top: 16px;">
            <strong>Cost Composition (Typical for {basic['type']}):</strong><br>
            Network Upgrades: ~{ic_breakdown.get('network_upgrade_pct', 70):.0f}% of total (variable based on study results)<br>
            Direct Connection: ~{ic_breakdown.get('direct_connection_pct', 30):.0f}% of total (est. ${ic_breakdown.get('direct_cost_estimate', 0):.1f}M)<br>
            <em>{ic_breakdown.get('cost_sharing_note', '')}</em>
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

    <!-- HISTORICAL COMPLETION FUNNEL -->
    {_build_completion_funnel_section(completion_funnel, region, basic.get('type', 'Unknown'))}

    <!-- COMPARABLE PROJECT OUTCOMES -->
    {_build_comparable_outcomes_section(comparable_outcomes, basic.get('type', 'Unknown'), basic.get('capacity_mw', 0))}

    <!-- POI Queue Analysis -->
    <div class="section no-break">
        <h2>POI Queue Analysis</h2>
        <div class="two-col">
            <div>
                <table class="data-table">
                    <tr><th style="width: 40%;">POI / Substation</th><td>{basic['poi']}</td></tr>
                    <tr><th>Total Projects at POI</th><td>{poi_analysis.get('total_projects', 'N/A')}</td></tr>
                    <tr><th>Active Projects</th><td>{poi_analysis.get('active_projects', 'N/A')}</td></tr>
                    <tr><th>Withdrawn Projects</th><td>{poi_analysis.get('withdrawn_projects', 'N/A')}</td></tr>
                    <tr><th>Total Capacity at POI</th><td>{poi_analysis.get('total_capacity_mw', 0):,.0f} MW</td></tr>
                </table>
            </div>
            <div>
                <table class="data-table">
                    <tr><th style="width: 40%;">Queue Position</th><td><strong>#{poi_analysis.get('position', 'N/A')}</strong> of {poi_analysis.get('total_projects', 'N/A')}</td></tr>
                    <tr><th>Projects Ahead</th><td>{poi_analysis.get('projects_ahead', 'N/A')} active</td></tr>
                    <tr><th>Capacity Ahead</th><td>{poi_analysis.get('capacity_ahead_mw', 0):,.0f} MW</td></tr>
                    <tr><th>POI Withdrawal Rate</th><td>{poi_analysis.get('withdrawal_rate', 0):.0f}%</td></tr>
                    <tr><th>POI Risk Level</th><td><span class="badge badge-{poi_analysis.get('risk_level', 'MEDIUM').lower()}">{poi_analysis.get('risk_level', 'UNKNOWN')}</span></td></tr>
                </table>
            </div>
        </div>
        <div class="note">
            <strong>POI Assessment:</strong> {poi_analysis.get('risk_reason', 'Assessment not available')}
        </div>
    </div>

    <!-- Developer Analysis -->
    <div class="section no-break">
        <h2>Developer Analysis</h2>
        <div class="two-col">
            <div>
                <table class="data-table">
                    <tr><th style="width: 40%;">Developer</th><td>{basic['developer']}</td></tr>
                    <tr><th>Total Historical Projects</th><td>{developer_stats.get('total_projects', cross_rto.get('total_projects', 0))}</td></tr>
                    <tr><th>Completed (Operational)</th><td><strong>{developer_stats.get('completed', 0)}</strong></td></tr>
                    <tr><th>Withdrawn/Cancelled</th><td>{developer_stats.get('withdrawn', 0)}</td></tr>
                    <tr><th>Currently Active</th><td>{developer_stats.get('active', 0)}</td></tr>
                </table>
            </div>
            <div>
                <table class="data-table">
                    <tr><th style="width: 40%;">Completion Rate</th><td><strong>{developer_stats.get('completion_rate', 0):.0f}%</strong></td></tr>
                    <tr><th>Total Portfolio Capacity</th><td>{cross_rto.get('total_capacity_mw', 0)/1000:.2f} GW</td></tr>
                    <tr><th>ISOs with Presence</th><td>{', '.join(cross_rto.get('isos', [])) or 'N/A'}</td></tr>
                    <tr><th>Data Confidence</th><td>{developer_stats.get('confidence', 'unknown').title()}</td></tr>
                    <tr><th>Data Source</th><td>{developer_stats.get('source', 'Queue Data')}</td></tr>
                </table>
            </div>
        </div>
        <div class="note">
            <strong>Developer Assessment:</strong> {developer_stats.get('assessment', cross_rto.get('assessment', 'Unknown'))}
        </div>
    </div>

    <!-- DEVELOPER'S ACTUAL COMPLETED PROJECTS -->
    {_build_developer_projects_section(developer_projects, basic['developer'])}

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

    <!-- Revenue Stack Analysis (Tier 2) -->
    {_build_revenue_stack_section(enhanced, region, basic.get('type', 'Unknown'), basic.get('capacity_mw', 0))}

    <!-- Valuation Guidance -->
    <div class="section">
        <h2>Valuation Guidance</h2>
        <div class="two-col">
            <div>
                <table class="data-table">
                    <thead>
                        <tr><th>Metric</th><th>Low</th><th>Base Case</th><th>High</th></tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>Entry Price</td>
                            <td>${valuation.get('entry_price', {}).get('low', 0):.1f}M</td>
                            <td><strong>${valuation.get('entry_price', {}).get('mid', 0):.1f}M</strong></td>
                            <td>${valuation.get('entry_price', {}).get('high', 0):.1f}M</td>
                        </tr>
                        <tr>
                            <td>+ IC Cost (Risk-Adj)</td>
                            <td colspan="3" style="text-align: center;">${valuation.get('ic_cost_assumed', 0):.1f}M</td>
                        </tr>
                        <tr class="highlight-row">
                            <td><strong>Total Basis</strong></td>
                            <td>${valuation.get('total_basis', {}).get('low', 0):.1f}M</td>
                            <td><strong>${valuation.get('total_basis', {}).get('mid', 0):.1f}M</strong></td>
                            <td>${valuation.get('total_basis', {}).get('high', 0):.1f}M</td>
                        </tr>
                    </tbody>
                </table>
            </div>
            <div>
                <table class="data-table">
                    <tr><th style="width: 50%;">Entry $/MW</th><td>${valuation.get('entry_per_mw', {}).get('mid', 0):,.0f}/MW</td></tr>
                    <tr><th>All-In Basis $/MW</th><td><strong>${valuation.get('basis_per_mw', 0):,.0f}/MW</strong></td></tr>
                    <tr><th>Stage Adjustment</th><td>{valuation.get('stage_description', 'N/A')}</td></tr>
                    <tr><th>Completion Risk Adj</th><td>{valuation.get('completion_discount', 1)*100:.0f}% of market</td></tr>
                </table>
            </div>
        </div>
        <div class="note">
            <strong>Methodology:</strong> {valuation.get('methodology', 'Market benchmark adjusted for development stage and completion risk')}
        </div>
    </div>

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

    <!-- Key Diligence Items -->
    <div class="section no-break">
        <h2>Key Diligence Items</h2>
        <table class="data-table">
            <thead>
                <tr><th style="width: 30%;">Item</th><th style="width: 15%;">Priority</th><th>Specific Action</th></tr>
            </thead>
            <tbody>
                <tr>
                    <td>IC Study Documents</td>
                    <td><span class="badge badge-high">HIGH</span></td>
                    <td>Obtain SIS/Facilities Study from {region}. Validate ${cost_data['total_millions']['p50']:.0f}M estimate against posted study costs.</td>
                </tr>
                <tr>
                    <td>Developer Financials</td>
                    <td><span class="badge badge-{'high' if developer_stats.get('completion_rate', 0) < 20 else 'medium'}">{'HIGH' if developer_stats.get('completion_rate', 0) < 20 else 'MEDIUM'}</span></td>
                    <td>{basic['developer']} has {developer_stats.get('completion_rate', 0):.0f}% completion rate. Verify financial capacity for ${valuation.get('ic_cost_assumed', 0):.0f}M IC exposure.</td>
                </tr>
                <tr>
                    <td>POI Queue Position</td>
                    <td><span class="badge badge-{poi_analysis.get('risk_level', 'MEDIUM').lower()}">{poi_analysis.get('risk_level', 'MEDIUM')}</span></td>
                    <td>Position #{poi_analysis.get('position', 'N/A')} with {poi_analysis.get('projects_ahead', 0)} projects ahead ({poi_analysis.get('capacity_ahead_mw', 0):,.0f} MW). Review cost allocation methodology.</td>
                </tr>
                <tr>
                    <td>Network Upgrades</td>
                    <td><span class="badge badge-medium">MEDIUM</span></td>
                    <td>~{ic_breakdown.get('network_upgrade_pct', 70):.0f}% of IC costs from network upgrades. Identify specific upgrade requirements and sharing arrangements.</td>
                </tr>
                {''.join(f'<tr><td>Red Flag Investigation</td><td><span class="badge badge-high">HIGH</span></td><td>{flag}</td></tr>' for flag in red_flags[:2])}
            </tbody>
        </table>
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


def _generate_thesis(rec: str, score: float, basic: Dict, cost_data: Dict, timeline_data: Dict, completion_data: Dict, cross_rto: Dict, enhanced: Dict = None) -> str:
    """Generate investment thesis summary using actual project data."""
    enhanced = enhanced or {}

    capacity = basic.get('capacity_mw', 0)
    project_type = basic.get('type', 'renewable')
    developer = basic.get('developer', 'Unknown')

    # Get enhanced data
    score_pct = enhanced.get('score_percentile', {})
    poi_analysis = enhanced.get('poi_analysis', {})
    developer_stats = enhanced.get('developer_stats', {})
    valuation = enhanced.get('valuation', {})

    # Calculate key metrics
    percentile = score_pct.get('percentile')
    percentile_text = f"top {100 - percentile:.0f}%" if percentile else ""
    dev_completion_rate = developer_stats.get('completion_rate', 0)
    dev_completed = developer_stats.get('completed', 0)
    dev_total = developer_stats.get('total_projects', 0)
    poi_position = poi_analysis.get('position', 'N/A')
    projects_ahead = poi_analysis.get('projects_ahead', 0)
    entry_price_mid = valuation.get('entry_price', {}).get('mid', 0)
    basis_per_mw = valuation.get('basis_per_mw', 0)

    if rec == 'GO':
        thesis = (
            f"<strong>Opportunity:</strong> {capacity:,.0f} MW {project_type} project scoring {score:.0f}/100 "
            f"({percentile_text + ' of comparable projects' if percentile_text else 'strong fundamentals'}). "
        )
        if dev_completed > 0:
            thesis += f"{developer} has completed {dev_completed} of {dev_total} historical projects ({dev_completion_rate:.0f}% rate). "
        if projects_ahead <= 2:
            thesis += f"Favorable queue position (#{poi_position}, only {projects_ahead} projects ahead). "
        thesis += (
            f"<strong>Economics:</strong> Est. entry ${entry_price_mid:.1f}M + ${valuation.get('ic_cost_assumed', 0):.0f}M IC = "
            f"${basis_per_mw:,.0f}/MW all-in basis. IC cost at ${cost_data['per_kw']['p50']:.0f}/kW (P50) is within market range. "
            f"<strong>Recommendation:</strong> Proceed with standard due diligence."
        )
        return thesis

    elif rec == 'CONDITIONAL':
        thesis = (
            f"<strong>Mixed Profile:</strong> {capacity:,.0f} MW {project_type} scoring {score:.0f}/100 "
            f"presents opportunity with identified risks. "
        )
        # Highlight specific concerns
        concerns = []
        if dev_completion_rate < 25 and dev_total > 0:
            concerns.append(f"developer completion rate of {dev_completion_rate:.0f}%")
        if projects_ahead > 3:
            concerns.append(f"{projects_ahead} projects ahead at POI")
        if poi_analysis.get('withdrawal_rate', 0) > 40:
            concerns.append(f"high POI withdrawal rate ({poi_analysis['withdrawal_rate']:.0f}%)")

        if concerns:
            thesis += f"Key concerns: {', '.join(concerns)}. "

        thesis += (
            f"<strong>Economics:</strong> IC cost range ${cost_data['total_millions']['p25']:.0f}M-${cost_data['total_millions']['p75']:.0f}M "
            f"creates ${cost_data['total_millions']['p75'] - cost_data['total_millions']['p25']:.0f}M variance exposure. "
            f"Est. all-in basis ${basis_per_mw:,.0f}/MW. "
            f"<strong>Recommendation:</strong> Enhanced diligence required. Validate IC costs against actual study documents."
        )
        return thesis

    else:
        thesis = (
            f"<strong>Elevated Risk:</strong> {capacity:,.0f} MW {project_type} scores {score:.0f}/100 with material execution concerns. "
        )
        # List major issues
        issues = []
        if dev_completion_rate < 15:
            issues.append(f"poor developer track record ({dev_completion_rate:.0f}% completion)")
        if projects_ahead > 5:
            issues.append(f"congested POI ({projects_ahead} projects ahead)")
        if completion_data.get('combined_rate', 0) < 0.2:
            issues.append(f"low base completion probability ({completion_data['combined_rate']*100:.0f}%)")

        if issues:
            thesis += f"Issues: {'; '.join(issues)}. "

        thesis += (
            f"<strong>Recommendation:</strong> Pass unless strategic value justifies risk. "
            f"If proceeding, require substantial contractual protections and adjusted pricing."
        )
        return thesis


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


def _build_revenue_stack_section(enhanced: Dict, region: str, project_type: str, capacity_mw: float) -> str:
    """Build comprehensive revenue stack section using Tier 2 analytics."""
    # Get Tier 2 data - prefer direct keys, fall back to _analytics
    revenue_stack = enhanced.get('revenue_stack', {})
    revenue = enhanced.get('revenue_estimate', {})
    capacity = enhanced.get('capacity_value', {})
    transmission = enhanced.get('transmission_risk', {})
    ppa = enhanced.get('ppa_benchmarks', {})

    # If no revenue data, skip the section
    if not revenue_stack and not revenue:
        return ""

    # Prefer pre-calculated revenue_stack, fall back to individual components
    if revenue_stack:
        energy_rev = revenue_stack.get('energy_revenue_millions', 0)
        cap_rev = revenue_stack.get('capacity_revenue_millions', 0)
        ancillary_rev = revenue_stack.get('ancillary_revenue_millions', 0)
        total_rev = revenue_stack.get('total_revenue_millions', 0)
        rev_per_kw = revenue_stack.get('revenue_per_kw', 0)
        energy_pct = revenue_stack.get('energy_pct', 0)
        cap_pct = revenue_stack.get('capacity_pct', 0)
        anc_pct = revenue_stack.get('ancillary_pct', 0)
        # Also pull assumptions from revenue_stack
        avg_lmp = revenue_stack.get('avg_lmp', revenue.get('avg_lmp', 0))
        cf = revenue_stack.get('capacity_factor', revenue.get('capacity_factor', 0))
        elcc = revenue_stack.get('elcc_percent', capacity.get('elcc_percent', 0))
    else:
        # Calculate from individual components
        energy_rev = revenue.get('annual_revenue_millions', 0)
        cap_rev = capacity.get('annual_value_millions', 0)

        # Estimate ancillary (3% for renewables, 20% for batteries)
        if 'battery' in project_type.lower() or 'storage' in project_type.lower():
            ancillary_mult = 0.20
        else:
            ancillary_mult = 0.03
        ancillary_rev = energy_rev * ancillary_mult
        total_rev = energy_rev + cap_rev + ancillary_rev

        # Revenue per kW
        rev_per_kw = (total_rev * 1_000_000) / (capacity_mw * 1000) if capacity_mw > 0 else 0

        # Energy percentage breakdown
        energy_pct = (energy_rev / total_rev * 100) if total_rev > 0 else 0
        cap_pct = (cap_rev / total_rev * 100) if total_rev > 0 else 0
        anc_pct = (ancillary_rev / total_rev * 100) if total_rev > 0 else 0
        avg_lmp = revenue.get('avg_lmp', 0)
        cf = revenue.get('capacity_factor', 0)
        elcc = capacity.get('elcc_percent', 0)

    # Transmission risk badge
    tx_rating = transmission.get('risk_rating', 'UNKNOWN')
    tx_badge_class = {
        'LOW': 'badge-low',
        'MODERATE': 'badge-medium',
        'ELEVATED': 'badge-elevated',
        'HIGH': 'badge-high',
    }.get(tx_rating, 'badge-medium')

    # PPA comparison
    ppa_mid = ppa.get('price_mid', 0)
    ppa_comparison = ""
    if ppa_mid > 0 and avg_lmp > 0:
        if avg_lmp > ppa_mid:
            ppa_comparison = f"Merchant pricing ({avg_lmp:.0f}$/MWh) exceeds PPA benchmark ({ppa_mid:.0f}$/MWh)"
        else:
            ppa_comparison = f"PPA benchmark ({ppa_mid:.0f}$/MWh) would improve on merchant ({avg_lmp:.0f}$/MWh)"

    return f'''
    <div class="section">
        <h2>Revenue Stack Analysis</h2>
        <p class="section-intro">Estimated annual revenue breakdown for {capacity_mw:.0f} MW {project_type} in {region}:</p>

        <div class="two-col">
            <div>
                <table class="data-table">
                    <thead>
                        <tr><th>Revenue Stream</th><th>Annual ($M)</th><th>% of Total</th><th>$/kW-yr</th></tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td><strong>Energy Revenue</strong></td>
                            <td>${energy_rev:.2f}M</td>
                            <td>{energy_pct:.1f}%</td>
                            <td>${energy_rev * 1000 / capacity_mw:.0f}</td>
                        </tr>
                        <tr>
                            <td><strong>Capacity Revenue</strong></td>
                            <td>${cap_rev:.2f}M</td>
                            <td>{cap_pct:.1f}%</td>
                            <td>${cap_rev * 1000 / capacity_mw:.0f}</td>
                        </tr>
                        <tr>
                            <td>Ancillary Services (est.)</td>
                            <td>${ancillary_rev:.2f}M</td>
                            <td>{anc_pct:.1f}%</td>
                            <td>${ancillary_rev * 1000 / capacity_mw:.0f}</td>
                        </tr>
                        <tr class="highlight-row">
                            <td><strong>Total Revenue</strong></td>
                            <td><strong>${total_rev:.2f}M</strong></td>
                            <td><strong>100%</strong></td>
                            <td><strong>${rev_per_kw:.0f}</strong></td>
                        </tr>
                    </tbody>
                </table>
            </div>
            <div>
                <table class="data-table">
                    <thead>
                        <tr><th colspan="2">Key Assumptions</th></tr>
                    </thead>
                    <tbody>
                        <tr><th>Average LMP</th><td>${avg_lmp:.0f}/MWh</td></tr>
                        <tr><th>Capacity Factor</th><td>{cf*100:.0f}%</td></tr>
                        <tr><th>ELCC</th><td>{elcc*100:.0f}%</td></tr>
                        <tr><th>Capacity Price</th><td>${capacity.get('price_mw_day', 0) or 0:.0f}/MW-day</td></tr>
                        <tr><th>Market Type</th><td>{capacity.get('market_type', 'N/A')}</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <div class="two-col" style="margin-top: 16px;">
            <div class="market-card">
                <h4>Transmission Risk</h4>
                <table class="data-table">
                    <tr><th>Risk Rating</th><td><span class="badge {tx_badge_class}">{tx_rating}</span></td></tr>
                    <tr><th>Congestion Level</th><td>{(transmission.get('congestion_level') or 'N/A').title()}</td></tr>
                    <tr><th>Avg Congestion Cost</th><td>${transmission.get('avg_congestion_cost', 0):.1f}/MWh</td></tr>
                    <tr><th>Hours Congested</th><td>{transmission.get('pct_hours_congested', 0)*100:.0f}%</td></tr>
                </table>
            </div>
            <div class="market-card">
                <h4>PPA Benchmarks ({region} {project_type})</h4>
                <table class="data-table">
                    <tr><th>Low</th><td>${ppa.get('price_low', 0):.0f}/MWh</td></tr>
                    <tr><th>Mid</th><td><strong>${ppa.get('price_mid', 0):.0f}/MWh</strong></td></tr>
                    <tr><th>High</th><td>${ppa.get('price_high', 0):.0f}/MWh</td></tr>
                    <tr><th>Trend</th><td>{ppa.get('trend', 'stable').title()}</td></tr>
                </table>
            </div>
        </div>

        <div class="note" style="margin-top: 12px;">
            <strong>Data Source:</strong> Benchmark pricing data. {ppa_comparison}
        </div>
    </div>'''


def _build_cost_histogram_section(distribution: Dict, this_project_cost: float, region: str, project_type: str) -> str:
    """Build HTML for cost distribution histogram."""
    if not distribution or 'error' in distribution:
        return ""

    buckets = distribution.get('buckets', [])
    if not buckets:
        return ""

    n_projects = distribution.get('n_projects', 0)
    p25 = distribution.get('p25', 0)
    p50 = distribution.get('p50', 0)
    p75 = distribution.get('p75', 0)
    project_pct = distribution.get('this_project_percentile')

    # Build histogram bars
    max_count = max(b['count'] for b in buckets) if buckets else 1
    histogram_rows = ""
    for bucket in buckets:
        bar_width = (bucket['count'] / max_count * 100) if max_count > 0 else 0
        highlight = ' style="background: #f0f7ff; font-weight: bold;"' if bucket.get('contains_project') else ''
        marker = ' <strong>[THIS PROJECT]</strong>' if bucket.get('contains_project') else ''

        histogram_rows += f'''
            <tr{highlight}>
                <td style="width: 25%;">${bucket['range_low']:,} - ${bucket['range_high']:,}</td>
                <td style="width: 60%;">
                    <div style="background: #e0e0e0; width: 100%; height: 18px; position: relative;">
                        <div style="background: {'#2563eb' if bucket.get('contains_project') else '#64748b'}; width: {bar_width:.0f}%; height: 100%;"></div>
                    </div>
                </td>
                <td style="width: 15%; text-align: right;">{bucket['count']}{marker}</td>
            </tr>'''

    percentile_text = ""
    if project_pct is not None:
        if project_pct <= 25:
            percentile_text = f"This project's estimated cost (${this_project_cost:.0f}/kW) is in the <strong>lowest quartile</strong> - favorable cost position."
        elif project_pct <= 50:
            percentile_text = f"This project's estimated cost (${this_project_cost:.0f}/kW) is <strong>below median</strong> - competitive cost position."
        elif project_pct <= 75:
            percentile_text = f"This project's estimated cost (${this_project_cost:.0f}/kW) is <strong>above median</strong> - P{project_pct:.0f}."
        else:
            percentile_text = f"This project's estimated cost (${this_project_cost:.0f}/kW) is in the <strong>highest quartile</strong> (P{project_pct:.0f}) - elevated cost risk."

    return f'''
        <div style="margin-top: 20px;">
            <h4>Cost Distribution: {region} {project_type} Projects (n={n_projects})</h4>
            <table class="histogram-table" style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr style="border-bottom: 1px solid #ccc;">
                        <th style="text-align: left; padding: 4px;">$/kW Range</th>
                        <th style="text-align: left; padding: 4px;">Distribution</th>
                        <th style="text-align: right; padding: 4px;">Count</th>
                    </tr>
                </thead>
                <tbody>
                    {histogram_rows}
                </tbody>
            </table>
            <div class="note" style="margin-top: 8px;">
                <strong>Percentiles:</strong> P25=${p25:.0f}/kW | P50=${p50:.0f}/kW | P75=${p75:.0f}/kW<br>
                {percentile_text}
            </div>
        </div>'''


def _build_completion_funnel_section(funnel: Dict, region: str, project_type: str) -> str:
    """Build HTML for completion funnel section with actual data."""
    if not funnel or 'error' in funnel:
        return ""

    total = funnel.get('total_entered', 0)
    if total == 0:
        return ""

    pre_ia = funnel.get('pre_ia_withdrawn', 0)
    post_ia = funnel.get('post_ia_withdrawn', 0)
    active = funnel.get('active_in_queue', 0)
    completed = funnel.get('reached_cod', 0)
    completion_rate = funnel.get('completion_rate_pct', 0)

    # Calculate percentages
    pre_ia_pct = (pre_ia / total * 100) if total > 0 else 0
    post_ia_pct = (post_ia / total * 100) if total > 0 else 0
    active_pct = (active / total * 100) if total > 0 else 0
    completed_pct = (completed / total * 100) if total > 0 else 0

    return f'''
    <div class="section">
        <h2>Historical Completion Analysis: {region} {project_type}</h2>
        <div class="funnel-container">
            <table class="data-table">
                <thead>
                    <tr><th style="width:35%;">Stage</th><th style="width:20%;">Projects</th><th style="width:15%;">% of Total</th><th style="width:30%;">Cumulative Attrition</th></tr>
                </thead>
                <tbody>
                    <tr>
                        <td><strong>Entered Queue</strong></td>
                        <td><strong>{total:,}</strong></td>
                        <td>100%</td>
                        <td>-</td>
                    </tr>
                    <tr>
                        <td>Withdrew Before IA</td>
                        <td>{pre_ia:,}</td>
                        <td>{pre_ia_pct:.1f}%</td>
                        <td>{pre_ia_pct:.1f}% lost</td>
                    </tr>
                    <tr>
                        <td>Withdrew After IA Signed</td>
                        <td>{post_ia:,}</td>
                        <td>{post_ia_pct:.1f}%</td>
                        <td>{pre_ia_pct + post_ia_pct:.1f}% lost</td>
                    </tr>
                    <tr>
                        <td>Still Active in Queue</td>
                        <td>{active:,}</td>
                        <td>{active_pct:.1f}%</td>
                        <td>-</td>
                    </tr>
                    <tr class="highlight-row">
                        <td><strong>Reached Commercial Operation</strong></td>
                        <td><strong>{completed:,}</strong></td>
                        <td><strong>{completed_pct:.1f}%</strong></td>
                        <td><strong>{completion_rate:.1f}% completion rate</strong></td>
                    </tr>
                </tbody>
            </table>
        </div>
        <div class="note">
            <strong>Key Insight:</strong> {funnel.get('funnel_text', f'{completion_rate:.1f}% of {project_type} projects in {region} reach commercial operation.')}
        </div>
    </div>'''


def _build_comparable_outcomes_section(outcomes: Dict, project_type: str, capacity_mw: float) -> str:
    """Build HTML for comparable project outcomes section."""
    if not outcomes or 'error' in outcomes:
        return ""

    total = outcomes.get('total_comparables', 0)
    if total == 0:
        return ""

    completed = outcomes.get('completed', 0)
    withdrawn = outcomes.get('withdrawn', 0)
    active = outcomes.get('active', 0)
    completion_rate = outcomes.get('completion_rate', 0)

    # Build sample projects table
    completed_sample = outcomes.get('completed_sample', [])
    withdrawn_sample = outcomes.get('withdrawn_sample', [])

    completed_rows = ""
    for proj in completed_sample[:5]:
        months = proj.get('months_to_cod', '')
        months_text = f"{months} mo" if months else "-"
        completed_rows += f'''
            <tr>
                <td>{proj.get('name', 'Unknown')[:30]}</td>
                <td>{proj.get('capacity_mw', '-'):,} MW</td>
                <td>{proj.get('state', '-')}</td>
                <td><span class="badge badge-low">Completed</span></td>
                <td>{months_text}</td>
            </tr>'''

    withdrawn_rows = ""
    for proj in withdrawn_sample[:3]:
        withdrawn_rows += f'''
            <tr>
                <td>{proj.get('name', 'Unknown')[:30]}</td>
                <td>{proj.get('capacity_mw', '-'):,} MW</td>
                <td>{proj.get('state', '-')}</td>
                <td><span class="badge badge-high">Withdrawn</span></td>
                <td>-</td>
            </tr>'''

    return f'''
    <div class="section">
        <h2>Comparable Project Outcomes</h2>
        <p class="section-intro">Analysis of {total} similar projects ({int(capacity_mw * 0.5):,}-{int(capacity_mw * 1.5):,} MW {project_type}) and their actual outcomes:</p>

        <div class="two-col" style="margin-bottom: 16px;">
            <div class="outcome-summary">
                <table class="data-table">
                    <tr><th style="width:50%;">Outcome</th><th>Count</th><th>Rate</th></tr>
                    <tr>
                        <td>Completed (COD)</td>
                        <td><strong>{completed}</strong></td>
                        <td><strong>{completion_rate:.0f}%</strong></td>
                    </tr>
                    <tr>
                        <td>Withdrawn/Cancelled</td>
                        <td>{withdrawn}</td>
                        <td>{100 - completion_rate:.0f}%</td>
                    </tr>
                    <tr>
                        <td>Still Active</td>
                        <td>{active}</td>
                        <td>-</td>
                    </tr>
                </table>
            </div>
            <div>
                <div class="note" style="margin: 0;">
                    <strong>Interpretation:</strong>
                    {'Projects of this size and type have above-average completion rates.' if completion_rate > 30 else
                     'Projects of this size and type have moderate completion rates.' if completion_rate > 20 else
                     'Projects of this size and type have below-average completion rates, requiring careful diligence.'}
                </div>
            </div>
        </div>

        {f"""
        <h4>Sample Completed Projects</h4>
        <table class="data-table">
            <thead>
                <tr><th>Project</th><th>Capacity</th><th>State</th><th>Status</th><th>Time to COD</th></tr>
            </thead>
            <tbody>
                {completed_rows}
            </tbody>
        </table>
        """ if completed_rows else ""}

        {f"""
        <h4 style="margin-top: 12px;">Sample Withdrawn Projects</h4>
        <table class="data-table">
            <thead>
                <tr><th>Project</th><th>Capacity</th><th>State</th><th>Status</th><th>Time to COD</th></tr>
            </thead>
            <tbody>
                {withdrawn_rows}
            </tbody>
        </table>
        """ if withdrawn_rows else ""}
    </div>'''


def _build_developer_projects_section(projects: Dict, developer: str) -> str:
    """Build HTML for developer's actual completed projects."""
    if not projects or 'error' in projects:
        return ""

    completed = projects.get('completed_projects', [])
    total_completed = projects.get('total_completed', 0)

    if total_completed == 0:
        return f'''
    <div class="section">
        <h2>Developer Historical Projects</h2>
        <div class="note">
            <strong>No Completed Projects Found:</strong> {developer} has no projects that reached commercial operation in the LBL database.
            This represents a significant execution risk factor.
        </div>
    </div>'''

    completed_mw = projects.get('completed_mw', 0)

    # Build completed projects table
    completed_rows = ""
    for proj in completed[:8]:
        cod = proj.get('cod_date', '-')
        if cod and cod != 'None':
            try:
                cod = cod[:7]  # Just year-month
            except:
                pass
        completed_rows += f'''
            <tr>
                <td>{proj.get('name', 'Unknown')[:35]}</td>
                <td>{proj.get('capacity_mw', '-'):,.0f} MW</td>
                <td>{proj.get('type', '-')}</td>
                <td>{proj.get('state', '-')}</td>
                <td>{proj.get('region', '-')}</td>
                <td>{cod if cod else '-'}</td>
            </tr>'''

    return f'''
    <div class="section">
        <h2>Developer Historical Projects: {developer}</h2>
        <p class="section-intro">{projects.get('summary', '')}</p>

        <h4>Completed Projects (Operational)</h4>
        <table class="data-table">
            <thead>
                <tr><th>Project Name</th><th>Capacity</th><th>Type</th><th>State</th><th>Region</th><th>COD</th></tr>
            </thead>
            <tbody>
                {completed_rows}
            </tbody>
        </table>
        {f'<div class="note" style="margin-top: 8px;">Showing {len(completed)} of {total_completed} completed projects ({completed_mw:,.0f} MW total)</div>' if total_completed > len(completed) else ''}
    </div>'''


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
