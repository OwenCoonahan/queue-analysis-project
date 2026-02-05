#!/usr/bin/env python3
"""
SPP Completion Matcher

Matches SPP interconnection queue projects with EIA Form 860 operational plants
to identify completed projects that were marked as "Withdrawn" in the SPP feed.

The SPP direct data feed removes completed projects rather than marking them as complete.
This script uses EIA 860 data to identify which "withdrawn" projects actually completed.
"""

import sqlite3
from pathlib import Path
from difflib import SequenceMatcher
import re
from collections import defaultdict

TOOLS_DIR = Path(__file__).parent
V2_PATH = TOOLS_DIR / '.data' / 'queue_v2.db'
PUDL_PATH = TOOLS_DIR / '.cache' / 'pudl' / 'pudl.sqlite'


def normalize_name(name: str) -> str:
    """Normalize project/plant name for comparison."""
    if not name:
        return ''
    name = name.lower()
    # Remove common suffixes
    for suffix in ['llc', 'inc', 'corp', 'project', 'wind farm', 'solar farm',
                   'wind', 'solar', 'energy', 'power', 'plant', 'facility',
                   'generation', 'generating', 'station', 'center', ',', '.']:
        name = name.replace(suffix, '')
    # Remove extra whitespace
    name = ' '.join(name.split())
    return name.strip()


def normalize_county(county: str) -> str:
    """Normalize county name."""
    if not county:
        return ''
    county = county.lower()
    county = county.replace(' county', '').replace('county', '')
    return county.strip()


def similarity(a: str, b: str) -> float:
    """Calculate string similarity."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def capacity_match(cap1: float, cap2: float, tolerance: float = 0.15) -> bool:
    """Check if capacities match within tolerance."""
    if not cap1 or not cap2:
        return False
    diff = abs(cap1 - cap2) / max(cap1, cap2)
    return diff <= tolerance


def load_eia_plants():
    """Load operational renewable plants from EIA 860."""
    conn = sqlite3.connect(PUDL_PATH)
    conn.row_factory = sqlite3.Row

    query = """
    SELECT
        plant_id_eia,
        plant_name_eia,
        state,
        county,
        SUM(capacity_mw) as total_mw,
        MIN(generator_operating_date) as operating_date,
        fuel_type_code_pudl
    FROM out_eia__yearly_generators
    WHERE report_date = '2023-01-01'
      AND fuel_type_code_pudl IN ('wind', 'solar')
      AND operational_status = 'existing'
      AND state IN ('KS', 'OK', 'NE', 'NM', 'SD', 'ND', 'AR', 'LA', 'MT', 'WY', 'TX')
    GROUP BY plant_id_eia, plant_name_eia, state, county, fuel_type_code_pudl
    """

    plants = []
    for row in conn.execute(query):
        plants.append({
            'plant_id': row['plant_id_eia'],
            'name': row['plant_name_eia'],
            'name_norm': normalize_name(row['plant_name_eia']),
            'state': row['state'],
            'county': normalize_county(row['county']),
            'capacity_mw': row['total_mw'],
            'operating_date': row['operating_date'],
            'fuel_type': row['fuel_type_code_pudl']
        })

    conn.close()
    return plants


def load_spp_withdrawn():
    """Load SPP withdrawn renewable projects."""
    conn = sqlite3.connect(V2_PATH)
    conn.row_factory = sqlite3.Row

    query = """
    SELECT
        p.project_id,
        p.queue_id,
        p.project_name,
        l.state,
        l.county,
        p.capacity_mw,
        t.technology_code,
        p.queue_date,
        p.cod_proposed
    FROM fact_projects p
    JOIN dim_regions r ON p.region_id = r.region_id
    JOIN dim_statuses s ON p.status_id = s.status_id
    LEFT JOIN dim_locations l ON p.location_id = l.location_id
    LEFT JOIN dim_technologies t ON p.technology_id = t.technology_id
    WHERE r.region_code = 'SPP'
      AND s.status_category = 'Withdrawn'
      AND t.technology_code IN ('Wind', 'Solar')
    """

    projects = []
    for row in conn.execute(query):
        projects.append({
            'project_id': row['project_id'],
            'queue_id': row['queue_id'],
            'name': row['project_name'],
            'name_norm': normalize_name(row['project_name']),
            'state': row['state'],
            'county': normalize_county(row['county']) if row['county'] else '',
            'capacity_mw': row['capacity_mw'],
            'technology': row['technology_code'],
            'queue_date': row['queue_date'],
            'cod_proposed': row['cod_proposed']
        })

    conn.close()
    return projects


def find_matches(projects, plants):
    """Find matches between SPP projects and EIA plants."""
    matches = []

    # Build index by state+county for faster matching
    plants_by_location = defaultdict(list)
    for plant in plants:
        key = (plant['state'], plant['county'])
        plants_by_location[key].append(plant)

    for proj in projects:
        if not proj['state']:
            continue

        best_match = None
        best_score = 0.0
        match_reasons = []

        # Get candidate plants in same state
        candidates = []
        for (state, county), ps in plants_by_location.items():
            if state == proj['state']:
                candidates.extend(ps)

        for plant in candidates:
            score = 0.0
            reasons = []

            # Technology match
            tech_map = {'Wind': 'wind', 'Solar': 'solar'}
            if tech_map.get(proj['technology']) != plant['fuel_type']:
                continue

            # County match (strong signal)
            if proj['county'] and plant['county']:
                if proj['county'] == plant['county']:
                    score += 0.3
                    reasons.append('county_exact')
                elif similarity(proj['county'], plant['county']) > 0.8:
                    score += 0.2
                    reasons.append('county_similar')

            # Capacity match
            if capacity_match(proj['capacity_mw'], plant['capacity_mw'], 0.10):
                score += 0.3
                reasons.append('capacity_10pct')
            elif capacity_match(proj['capacity_mw'], plant['capacity_mw'], 0.20):
                score += 0.2
                reasons.append('capacity_20pct')
            elif capacity_match(proj['capacity_mw'], plant['capacity_mw'], 0.30):
                score += 0.1
                reasons.append('capacity_30pct')

            # Name similarity
            if proj['name_norm'] and plant['name_norm']:
                name_sim = similarity(proj['name_norm'], plant['name_norm'])
                if name_sim > 0.8:
                    score += 0.4
                    reasons.append(f'name_high_{name_sim:.2f}')
                elif name_sim > 0.6:
                    score += 0.2
                    reasons.append(f'name_med_{name_sim:.2f}')
                elif name_sim > 0.4:
                    score += 0.1
                    reasons.append(f'name_low_{name_sim:.2f}')

            if score > best_score and score >= 0.5:
                best_score = score
                best_match = plant
                match_reasons = reasons

        if best_match:
            matches.append({
                'project_id': proj['project_id'],
                'queue_id': proj['queue_id'],
                'project_name': proj['name'],
                'project_capacity': proj['capacity_mw'],
                'project_state': proj['state'],
                'project_county': proj['county'],
                'eia_plant_id': best_match['plant_id'],
                'eia_plant_name': best_match['name'],
                'eia_capacity': best_match['capacity_mw'],
                'eia_operating_date': best_match['operating_date'],
                'match_score': best_score,
                'match_reasons': ','.join(match_reasons)
            })

    return matches


def update_completed_status(matches, dry_run=True):
    """Update matched projects to Completed status."""
    conn = sqlite3.connect(V2_PATH)

    # Get Completed status_id
    cursor = conn.execute("""
        SELECT status_id FROM dim_statuses
        WHERE status_category = 'Completed'
        LIMIT 1
    """)
    row = cursor.fetchone()
    if not row:
        # Create Completed status if it doesn't exist
        conn.execute("""
            INSERT OR IGNORE INTO dim_statuses (status_code, status_name, status_category, status_order)
            VALUES ('operational', 'Operational', 'Completed', 10)
        """)
        cursor = conn.execute("""
            SELECT status_id FROM dim_statuses
            WHERE status_category = 'Completed'
            LIMIT 1
        """)
        row = cursor.fetchone()

    completed_status_id = row[0]

    updated = 0
    for match in matches:
        if match['match_score'] >= 0.6:  # High confidence threshold
            if not dry_run:
                conn.execute("""
                    UPDATE fact_projects
                    SET status_id = ?,
                        cod_actual = ?
                    WHERE project_id = ?
                """, (completed_status_id, match['eia_operating_date'], match['project_id']))
            updated += 1

    if not dry_run:
        conn.commit()
    conn.close()

    return updated


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Match SPP projects to EIA 860 plants')
    parser.add_argument('--apply', action='store_true', help='Apply updates (default: dry run)')
    parser.add_argument('--min-score', type=float, default=0.6, help='Minimum match score')
    parser.add_argument('--show-all', action='store_true', help='Show all matches, not just high confidence')
    args = parser.parse_args()

    print("Loading EIA 860 plants...")
    plants = load_eia_plants()
    print(f"  Loaded {len(plants)} operational renewable plants")

    print("\nLoading SPP withdrawn projects...")
    projects = load_spp_withdrawn()
    print(f"  Loaded {len(projects)} withdrawn renewable projects")

    print("\nFinding matches...")
    matches = find_matches(projects, plants)

    # Filter by score
    high_conf = [m for m in matches if m['match_score'] >= args.min_score]

    print(f"\nFound {len(matches)} total matches")
    print(f"  High confidence (>={args.min_score}): {len(high_conf)}")

    # Show matches
    if args.show_all:
        display_matches = matches
    else:
        display_matches = high_conf

    if display_matches:
        print(f"\n{'='*100}")
        print(f"{'Queue ID':<20} {'Project Name':<30} {'EIA Plant':<30} {'Score':<6} {'Reasons'}")
        print(f"{'='*100}")

        for m in sorted(display_matches, key=lambda x: -x['match_score'])[:50]:
            proj_name = (m['project_name'] or '')[:28]
            eia_name = (m['eia_plant_name'] or '')[:28]
            print(f"{m['queue_id']:<20} {proj_name:<30} {eia_name:<30} {m['match_score']:.2f}   {m['match_reasons']}")

    # Update if requested
    if args.apply:
        print(f"\nApplying updates...")
        updated = update_completed_status(high_conf, dry_run=False)
        print(f"  Updated {updated} projects to Completed status")
    else:
        print(f"\nDry run - would update {len(high_conf)} projects")
        print("  Run with --apply to update database")

    return matches


if __name__ == '__main__':
    main()
