#!/usr/bin/env python3
"""
Enhanced Research Module

Automates additional research for interconnection feasibility assessments:
- Developer cross-reference across queues
- Historical cost benchmarking
- Timeline benchmarking
- Web search for developer background
- Location/transmission analysis

Usage:
    python3 research.py 1738                    # Full research on project
    python3 research.py 1738 --developer-only   # Just developer research
    python3 research.py --developer "Acme LLC"  # Search for developer
"""

import pandas as pd
import numpy as np
import requests
import re
import json
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field
import argparse
import sys
from pathlib import Path

from analyze import QueueData, QueueAnalyzer


# =============================================================================
# COST BENCHMARKS (from industry data / LBL studies)
# =============================================================================

# Interconnection costs by project type ($/kW)
# Source: LBL Queued Up reports, industry benchmarks
COST_BENCHMARKS = {
    # Type: (low_$/kW, median_$/kW, high_$/kW)
    'solar': (50, 150, 400),
    's': (50, 150, 400),
    'pv': (50, 150, 400),

    'wind': (80, 200, 500),
    'w': (80, 200, 500),
    'onshore': (80, 200, 500),

    'offshore': (200, 400, 800),
    'osw': (200, 400, 800),

    'battery': (30, 100, 250),
    'storage': (30, 100, 250),
    'bess': (30, 100, 250),
    'es': (30, 100, 250),

    'gas': (40, 120, 300),
    'ng': (40, 120, 300),
    'natural gas': (40, 120, 300),
    'cc': (40, 120, 300),
    'ct': (40, 120, 300),

    # Load interconnection (data centers, industrial)
    'load': (20, 80, 200),
    'l': (20, 80, 200),
    'datacenter': (20, 80, 200),
    'data center': (20, 80, 200),

    'default': (50, 150, 400),
}

# Regional cost multipliers
REGIONAL_MULTIPLIERS = {
    'NYISO': 1.3,  # Higher costs in NY
    'CAISO': 1.2,  # California premium
    'PJM': 1.0,    # Baseline
    'ERCOT': 0.9,  # Lower costs in TX
    'MISO': 0.95,
    'ISONE': 1.25,
    'SPP': 0.85,
    'default': 1.0,
}

# Size adjustment factors (larger projects often have economies of scale but also more complexity)
def size_cost_factor(capacity_mw: float) -> float:
    """Adjust cost estimate based on project size."""
    if capacity_mw < 50:
        return 1.2  # Small projects have higher per-MW costs
    elif capacity_mw < 200:
        return 1.0  # Reference size
    elif capacity_mw < 500:
        return 0.95  # Some economies of scale
    elif capacity_mw < 1000:
        return 1.1  # Very large = more network upgrades likely
    else:
        return 1.3  # Massive projects almost always trigger major upgrades


# =============================================================================
# TIMELINE BENCHMARKS
# =============================================================================

# Time to COD by project type (months from queue entry)
# Source: LBL Queued Up historical analysis
TIMELINE_BENCHMARKS = {
    # Type: (fast_months, typical_months, slow_months)
    'solar': (24, 42, 72),
    's': (24, 42, 72),

    'wind': (30, 54, 84),
    'w': (30, 54, 84),

    'offshore': (60, 84, 120),
    'osw': (60, 84, 120),

    'battery': (18, 36, 60),
    'storage': (18, 36, 60),
    'es': (18, 36, 60),

    'gas': (24, 48, 72),
    'ng': (24, 48, 72),

    'load': (24, 48, 84),
    'l': (24, 48, 84),

    'default': (30, 48, 72),
}

# Completion rates by type (historical)
COMPLETION_RATES = {
    'solar': 0.25,    # ~25% of solar projects reach COD
    's': 0.25,
    'wind': 0.20,     # ~20% for wind
    'w': 0.20,
    'offshore': 0.15, # Lower for offshore
    'osw': 0.15,
    'battery': 0.30,  # Storage has been completing better recently
    'storage': 0.30,
    'es': 0.30,
    'gas': 0.45,      # Gas has higher completion rates
    'ng': 0.45,
    'load': 0.50,     # Load projects (real demand) complete more often
    'l': 0.50,
    'default': 0.25,
}


# =============================================================================
# DEVELOPER RESEARCH
# =============================================================================

@dataclass
class DeveloperProfile:
    """Developer research results."""
    name: str
    projects_in_queue: int = 0
    total_capacity_mw: float = 0
    project_types: Dict[str, int] = field(default_factory=dict)
    states: List[str] = field(default_factory=list)
    other_projects: List[Dict] = field(default_factory=list)

    # From web search
    company_info: Optional[str] = None
    parent_company: Optional[str] = None
    financial_backing: Optional[str] = None
    news_mentions: List[str] = field(default_factory=list)

    # Risk indicators
    red_flags: List[str] = field(default_factory=list)
    green_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'projects_in_queue': self.projects_in_queue,
            'total_capacity_mw': self.total_capacity_mw,
            'project_types': self.project_types,
            'states': self.states,
            'other_projects': self.other_projects,
            'company_info': self.company_info,
            'parent_company': self.parent_company,
            'financial_backing': self.financial_backing,
            'news_mentions': self.news_mentions[:5],
            'red_flags': self.red_flags,
            'green_flags': self.green_flags,
        }


class DeveloperResearcher:
    """Research developer background from queue data and web."""

    # Known hyperscalers and major players (for identification)
    KNOWN_ENTITIES = {
        'microsoft': 'Hyperscaler (Microsoft)',
        'google': 'Hyperscaler (Google)',
        'amazon': 'Hyperscaler (Amazon/AWS)',
        'meta': 'Hyperscaler (Meta)',
        'facebook': 'Hyperscaler (Meta)',
        'apple': 'Hyperscaler (Apple)',
        'oracle': 'Major Tech (Oracle)',
        'qts': 'Major Data Center Developer',
        'equinix': 'Major Data Center Developer',
        'digital realty': 'Major Data Center Developer',
        'cyrusone': 'Major Data Center Developer',
        'vantage': 'Data Center Developer',
        'compass': 'Data Center Developer',
        'nextera': 'Major Utility/Developer',
        'aes': 'Major Power Company',
        'enel': 'Major Utility',
        'invenergy': 'Major Developer',
        'ørsted': 'Offshore Wind Major',
        'orsted': 'Offshore Wind Major',
        'equinor': 'Offshore Wind Major',
        'bp': 'Oil Major / Energy Transition',
        'shell': 'Oil Major / Energy Transition',
        'totalenergies': 'Oil Major / Energy Transition',
    }

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.analyzer = QueueAnalyzer(df)

    def research_developer(self, developer_name: str) -> DeveloperProfile:
        """
        Research a developer across queue data.
        """
        profile = DeveloperProfile(name=developer_name)

        if not developer_name or developer_name == '[Not available]':
            profile.red_flags.append("Developer name not available")
            return profile

        # Search for developer in queue
        dev_col = self.analyzer._get_col('developer')
        if not dev_col:
            return profile

        # Find all projects by this developer (fuzzy match)
        dev_lower = developer_name.lower()

        # Also search for partial matches (e.g., "Acme" matches "Acme Solar LLC")
        dev_words = set(dev_lower.replace('llc', '').replace('inc', '').replace(',', '').split())
        dev_words.discard('')

        matches = []
        for idx, row in self.df.iterrows():
            row_dev = str(row[dev_col]).lower()

            # Exact match
            if dev_lower in row_dev or row_dev in dev_lower:
                matches.append(row)
                continue

            # Word overlap match (for related entities)
            row_words = set(row_dev.replace('llc', '').replace('inc', '').replace(',', '').split())
            overlap = dev_words & row_words
            if len(overlap) >= 2 or (len(overlap) == 1 and len(dev_words) == 1):
                matches.append(row)

        if matches:
            matches_df = pd.DataFrame(matches)
            profile.projects_in_queue = len(matches_df)

            # Total capacity
            cap_col = self.analyzer._get_col('capacity')
            if cap_col:
                profile.total_capacity_mw = pd.to_numeric(matches_df[cap_col], errors='coerce').sum()

            # Project types
            type_col = self.analyzer._get_col('type')
            if type_col:
                profile.project_types = matches_df[type_col].value_counts().to_dict()

            # States
            state_col = self.analyzer._get_col('state')
            if state_col:
                profile.states = matches_df[state_col].dropna().unique().tolist()

            # Other projects (excluding current)
            name_col = self.analyzer._get_col('name')
            id_col = self.analyzer._get_col('id')
            for _, row in matches_df.head(10).iterrows():
                profile.other_projects.append({
                    'id': row.get(id_col) if id_col else None,
                    'name': row.get(name_col) if name_col else None,
                    'capacity': row.get(cap_col) if cap_col else None,
                    'type': row.get(type_col) if type_col else None,
                })

        # Check for known entities
        for keyword, entity_type in self.KNOWN_ENTITIES.items():
            if keyword in dev_lower:
                profile.parent_company = entity_type
                profile.green_flags.append(f"Associated with {entity_type}")
                break

        # Assess based on project count
        if profile.projects_in_queue >= 5:
            profile.green_flags.append(f"Experienced developer ({profile.projects_in_queue} projects)")
        elif profile.projects_in_queue == 1:
            profile.red_flags.append("Single project developer - limited track record")

        return profile

    def search_web_for_developer(self, developer_name: str) -> Dict[str, Any]:
        """
        Search the web for developer information.
        Returns company info, news, etc.
        """
        # This would integrate with web search
        # For now, return a placeholder that indicates manual research needed

        results = {
            'search_performed': True,
            'developer': developer_name,
            'findings': [],
            'requires_manual_review': True,
        }

        # Check for obvious patterns
        dev_lower = developer_name.lower()

        if 'llc' in dev_lower and len(developer_name.split()) <= 4:
            results['findings'].append("Appears to be single-purpose LLC (common for project finance)")

        if any(kw in dev_lower for kw in ['solar', 'wind', 'energy', 'power', 'storage']):
            results['findings'].append("Project-specific entity name suggests SPV structure")

        return results


# =============================================================================
# COST ESTIMATOR
# =============================================================================

class CostEstimator:
    """Estimate interconnection costs based on benchmarks."""

    def estimate_costs(self,
                       capacity_mw: float,
                       project_type: str,
                       region: str = 'default') -> Dict[str, Any]:
        """
        Estimate interconnection costs.

        Returns low/median/high estimates in $ millions.
        """
        # Get base costs per kW
        type_lower = str(project_type).lower().strip()
        base_costs = COST_BENCHMARKS.get(type_lower, COST_BENCHMARKS['default'])

        # Get regional multiplier
        region_mult = REGIONAL_MULTIPLIERS.get(region.upper(), REGIONAL_MULTIPLIERS['default'])

        # Get size factor
        size_factor = size_cost_factor(capacity_mw)

        # Calculate costs
        low_per_kw = base_costs[0] * region_mult * size_factor
        med_per_kw = base_costs[1] * region_mult * size_factor
        high_per_kw = base_costs[2] * region_mult * size_factor

        # Convert to total $ millions
        low_total = (low_per_kw * capacity_mw * 1000) / 1_000_000
        med_total = (med_per_kw * capacity_mw * 1000) / 1_000_000
        high_total = (high_per_kw * capacity_mw * 1000) / 1_000_000

        return {
            'capacity_mw': capacity_mw,
            'project_type': project_type,
            'region': region,
            'cost_per_kw': {
                'low': round(low_per_kw, 0),
                'median': round(med_per_kw, 0),
                'high': round(high_per_kw, 0),
            },
            'total_cost_millions': {
                'low': round(low_total, 1),
                'median': round(med_total, 1),
                'high': round(high_total, 1),
            },
            'range_display': f"${low_total:.0f}M - ${high_total:.0f}M",
            'methodology': 'Based on LBL/industry benchmarks, adjusted for region and size',
            'confidence': 'Low' if capacity_mw > 500 else 'Medium',
            'notes': self._get_cost_notes(capacity_mw, type_lower, region),
        }

    def _get_cost_notes(self, capacity_mw: float, project_type: str, region: str) -> List[str]:
        notes = []

        if capacity_mw > 500:
            notes.append("Large project likely to trigger significant network upgrades")

        if capacity_mw > 100 and project_type in ['l', 'load']:
            notes.append("Large load may require dedicated transmission infrastructure")

        if region.upper() == 'NYISO':
            notes.append("NYISO typically has higher interconnection costs due to constrained transmission")

        if project_type in ['osw', 'offshore']:
            notes.append("Offshore wind has significant additional costs for submarine cables and onshore POI upgrades")

        return notes


# =============================================================================
# TIMELINE ESTIMATOR
# =============================================================================

class TimelineEstimator:
    """Estimate project timelines based on benchmarks."""

    def estimate_timeline(self,
                          project_type: str,
                          months_in_queue: int = 0,
                          study_phase: str = 'unknown') -> Dict[str, Any]:
        """
        Estimate time to COD.

        Returns estimates in months from now.
        """
        # Get base timeline
        type_lower = str(project_type).lower().strip()
        base_timeline = TIMELINE_BENCHMARKS.get(type_lower, TIMELINE_BENCHMARKS['default'])

        # Adjust for time already spent in queue
        remaining_fast = max(6, base_timeline[0] - months_in_queue)
        remaining_typical = max(12, base_timeline[1] - months_in_queue)
        remaining_slow = max(18, base_timeline[2] - months_in_queue)

        # Adjust for study phase
        phase_lower = study_phase.lower()
        if 'ia' in phase_lower or 'agreement' in phase_lower:
            phase_factor = 0.3  # Near completion
        elif 'facilities' in phase_lower:
            phase_factor = 0.5
        elif 'impact' in phase_lower or 'sis' in phase_lower:
            phase_factor = 0.7
        elif 'feasibility' in phase_lower:
            phase_factor = 0.85
        else:
            phase_factor = 1.0  # Unknown, assume early

        # Apply phase factor
        remaining_fast = int(remaining_fast * phase_factor)
        remaining_typical = int(remaining_typical * phase_factor)
        remaining_slow = int(remaining_slow * phase_factor)

        # Get completion rate
        completion_rate = COMPLETION_RATES.get(type_lower, COMPLETION_RATES['default'])

        # Calculate dates
        now = datetime.now()

        def add_months(date, months):
            year = date.year + (date.month + months - 1) // 12
            month = (date.month + months - 1) % 12 + 1
            return f"Q{(month-1)//3 + 1} {year}"

        return {
            'project_type': project_type,
            'months_in_queue': months_in_queue,
            'study_phase': study_phase,
            'remaining_months': {
                'optimistic': remaining_fast,
                'likely': remaining_typical,
                'pessimistic': remaining_slow,
            },
            'estimated_cod': {
                'optimistic': add_months(now, remaining_fast),
                'likely': add_months(now, remaining_typical),
                'pessimistic': add_months(now, remaining_slow),
            },
            'range_display': f"{add_months(now, remaining_fast)} to {add_months(now, remaining_slow)}",
            'historical_completion_rate': f"{completion_rate*100:.0f}%",
            'notes': self._get_timeline_notes(type_lower, months_in_queue),
        }

    def _get_timeline_notes(self, project_type: str, months_in_queue: int) -> List[str]:
        notes = []

        if months_in_queue > 36:
            notes.append(f"Project has been in queue {months_in_queue} months - extended timeline may indicate issues")

        if project_type in ['osw', 'offshore']:
            notes.append("Offshore wind projects have extended timelines due to federal permitting and construction complexity")

        completion_rate = COMPLETION_RATES.get(project_type, COMPLETION_RATES['default'])
        if completion_rate < 0.25:
            notes.append(f"Only ~{completion_rate*100:.0f}% of {project_type} projects historically reach COD")

        return notes


# =============================================================================
# FULL RESEARCH REPORT
# =============================================================================

def research_project(df: pd.DataFrame, project_id: str, region: str = 'NYISO') -> Dict[str, Any]:
    """
    Perform full automated research on a project.

    Returns comprehensive research report.
    """
    analyzer = QueueAnalyzer(df)

    # Find the project
    results = analyzer.search(queue_id=project_id)
    if len(results) == 0:
        return {'error': f'Project not found: {project_id}'}

    project = results.iloc[0]

    # Extract key fields
    def get_field(patterns, default=None):
        for col in project.index:
            for pattern in patterns:
                if pattern.lower() in col.lower():
                    val = project[col]
                    if pd.notna(val):
                        return val
        return default

    project_name = get_field(['name'], 'Unknown')
    developer = get_field(['developer', 'customer', 'owner'], 'Unknown')
    capacity = get_field(['capacity', 'mw', 'sp (mw)'])
    project_type = get_field(['type', 'fuel'])
    state = get_field(['state'])
    poi = get_field(['poi', 'interconnection'])
    queue_date = get_field(['date', 'ir'])

    # Calculate months in queue
    months_in_queue = 0
    if queue_date:
        try:
            qd = pd.to_datetime(queue_date)
            months_in_queue = (datetime.now() - qd).days // 30
        except:
            pass

    # Parse capacity
    try:
        capacity_mw = float(capacity)
    except:
        capacity_mw = 100  # Default

    # Initialize researchers
    dev_researcher = DeveloperResearcher(df)
    cost_estimator = CostEstimator()
    timeline_estimator = TimelineEstimator()

    # Perform research
    print(f"Researching: {project_name} ({project_id})")

    # 1. Developer research
    print("  - Developer analysis...")
    developer_profile = dev_researcher.research_developer(str(developer))

    # 2. Web search for developer (placeholder)
    print("  - Web search...")
    web_results = dev_researcher.search_web_for_developer(str(developer))

    # 3. Cost estimate
    print("  - Cost estimation...")
    cost_estimate = cost_estimator.estimate_costs(
        capacity_mw=capacity_mw,
        project_type=str(project_type),
        region=region
    )

    # 4. Timeline estimate
    print("  - Timeline estimation...")
    timeline_estimate = timeline_estimator.estimate_timeline(
        project_type=str(project_type),
        months_in_queue=months_in_queue,
    )

    # 5. POI/Location analysis
    print("  - Location analysis...")
    poi_analysis = analyzer.analyze_poi(str(poi)) if poi else {}

    # Compile report
    report = {
        'project_id': project_id,
        'project_name': project_name,
        'basic_info': {
            'developer': developer,
            'capacity_mw': capacity_mw,
            'type': project_type,
            'state': state,
            'poi': poi,
            'queue_date': str(queue_date),
            'months_in_queue': months_in_queue,
        },
        'developer_research': developer_profile.to_dict(),
        'web_search': web_results,
        'cost_estimate': cost_estimate,
        'timeline_estimate': timeline_estimate,
        'poi_analysis': {k: v for k, v in poi_analysis.items() if k != 'projects'} if poi_analysis else {},
        'research_date': datetime.now().isoformat(),
    }

    return report


def print_research_report(report: Dict[str, Any]):
    """Pretty print research report."""
    if 'error' in report:
        print(f"Error: {report['error']}")
        return

    print("\n" + "=" * 70)
    print("AUTOMATED RESEARCH REPORT")
    print("=" * 70)

    # Basic info
    print(f"\nProject: {report['project_name']}")
    print(f"ID: {report['project_id']}")

    info = report['basic_info']
    print(f"\n--- Basic Info ---")
    print(f"Developer: {info['developer']}")
    print(f"Capacity: {info['capacity_mw']:,.0f} MW")
    print(f"Type: {info['type']}")
    print(f"State: {info['state']}")
    print(f"Months in Queue: {info['months_in_queue']}")

    # Developer research
    dev = report['developer_research']
    print(f"\n--- Developer Research ---")
    print(f"Projects in Queue: {dev['projects_in_queue']}")
    print(f"Total Capacity: {dev['total_capacity_mw']:,.0f} MW")
    if dev['parent_company']:
        print(f"Identified As: {dev['parent_company']}")
    if dev['green_flags']:
        print(f"Green Flags: {', '.join(dev['green_flags'])}")
    if dev['red_flags']:
        print(f"Red Flags: {', '.join(dev['red_flags'])}")

    if dev['other_projects']:
        print(f"\nOther Projects by Developer:")
        for p in dev['other_projects'][:5]:
            print(f"  - {p.get('name', 'Unknown')} ({p.get('capacity', '?')} MW, {p.get('type', '?')})")

    # Cost estimate
    cost = report['cost_estimate']
    print(f"\n--- Cost Estimate ---")
    print(f"Range: {cost['range_display']}")
    print(f"  Low: ${cost['total_cost_millions']['low']}M (${cost['cost_per_kw']['low']}/kW)")
    print(f"  Median: ${cost['total_cost_millions']['median']}M (${cost['cost_per_kw']['median']}/kW)")
    print(f"  High: ${cost['total_cost_millions']['high']}M (${cost['cost_per_kw']['high']}/kW)")
    print(f"Confidence: {cost['confidence']}")
    if cost['notes']:
        for note in cost['notes']:
            print(f"  Note: {note}")

    # Timeline estimate
    timeline = report['timeline_estimate']
    print(f"\n--- Timeline Estimate ---")
    print(f"Estimated COD: {timeline['range_display']}")
    print(f"  Optimistic: {timeline['estimated_cod']['optimistic']} ({timeline['remaining_months']['optimistic']} months)")
    print(f"  Likely: {timeline['estimated_cod']['likely']} ({timeline['remaining_months']['likely']} months)")
    print(f"  Pessimistic: {timeline['estimated_cod']['pessimistic']} ({timeline['remaining_months']['pessimistic']} months)")
    print(f"Historical Completion Rate: {timeline['historical_completion_rate']}")
    if timeline['notes']:
        for note in timeline['notes']:
            print(f"  Note: {note}")

    # POI Analysis
    if report['poi_analysis']:
        poi = report['poi_analysis']
        print(f"\n--- POI Analysis ---")
        print(f"Projects at POI: {poi.get('project_count', 'Unknown')}")
        if 'total_capacity_mw' in poi:
            print(f"Total Capacity at POI: {poi['total_capacity_mw']:,.0f} MW")

    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Automated Project Research")
    parser.add_argument('project_id', nargs='?', help='Project ID to research')
    parser.add_argument('--file', '-f', help='Local data file')
    parser.add_argument('--developer', help='Search for developer by name')
    parser.add_argument('--region', default='NYISO', help='Region for cost estimates')
    parser.add_argument('--json', action='store_true', help='Output as JSON')

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

    # Developer search
    if args.developer:
        researcher = DeveloperResearcher(df)
        profile = researcher.research_developer(args.developer)

        if args.json:
            print(json.dumps(profile.to_dict(), indent=2))
        else:
            print(f"\nDeveloper: {profile.name}")
            print(f"Projects: {profile.projects_in_queue}")
            print(f"Total Capacity: {profile.total_capacity_mw:,.0f} MW")
            if profile.green_flags:
                print(f"Green Flags: {', '.join(profile.green_flags)}")
            if profile.red_flags:
                print(f"Red Flags: {', '.join(profile.red_flags)}")
        return 0

    # Project research
    if args.project_id:
        report = research_project(df, args.project_id, args.region)

        if args.json:
            print(json.dumps(report, indent=2, default=str))
        else:
            print_research_report(report)
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
