#!/usr/bin/env python3
"""
Enrich master.db — Wind Turbine Cross-Reference + Utility Context
Dev5, 2026-03-23

PART 1: Match USWTDB wind turbines (grid.db) → master.db wind projects
  - Aggregates 75K individual turbines into ~1,782 project-level records
  - Match strategy: fuzzy project name + state
  - Adds: turbine_count, total_turbine_capacity_kw, primary_manufacturer,
          avg_hub_height_m, avg_rotor_diameter_m, turbine_year_range, uswtdb_eia_id

PART 2: Utility context enrichment from eia.db
  - Match project state+county → service_territories to find utility_id_eia
  - Pull utility_name, utility_total_sales_mwh, utility_total_customers

Usage:
    python3 enrich_wind_and_utility.py --all
    python3 enrich_wind_and_utility.py --wind           # Wind only
    python3 enrich_wind_and_utility.py --utility         # Utility only
    python3 enrich_wind_and_utility.py --dry-run         # Preview, no DB writes
"""

import logging
import re
import sqlite3
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / '.data'
MASTER_DB = DATA_DIR / 'master.db'
GRID_DB = DATA_DIR / 'grid.db'
EIA_DB = Path(__file__).parent.parent.parent.parent / 'prospector-platform' / 'pipelines' / 'databases' / 'eia.db'


# ─────────────────────────────────────────────────────────────
# Name normalization for fuzzy matching
# ─────────────────────────────────────────────────────────────

def normalize_name(name: Optional[str]) -> str:
    """Normalize a project name for fuzzy matching."""
    if not name:
        return ''
    name = name.lower().strip()
    # Remove common suffixes
    for suffix in [' llc', ' lp', ' inc', ', llc', ', lp', ', inc',
                   ' wind farm', ' wind energy', ' wind project',
                   ' wind power', ' energy center', ' wind', ' energy',
                   ' project', ' facility', ' station', ' plant',
                   ' phase i', ' phase ii', ' phase iii', ' phase 1', ' phase 2', ' phase 3',
                   ' i', ' ii', ' iii', ' iv', ' v']:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    # Remove punctuation and extra spaces
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def name_similarity(a: str, b: str) -> float:
    """Return similarity score 0-1 between two normalized names."""
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


# ─────────────────────────────────────────────────────────────
# PART 1: USWTDB Wind Turbine Cross-Reference
# ─────────────────────────────────────────────────────────────

def enrich_wind_turbines(dry_run: bool = False) -> Dict:
    """Match USWTDB wind turbines to master.db wind projects."""
    logger.info("=== PART 1: Wind Turbine Cross-Reference ===")

    if not GRID_DB.exists():
        logger.error(f"grid.db not found at {GRID_DB}")
        return {'error': 'grid.db not found'}

    # Load USWTDB project aggregates
    grid_conn = sqlite3.connect(GRID_DB)
    grid_conn.row_factory = sqlite3.Row
    uswtdb_projects = grid_conn.execute("""
        SELECT
            project_name, state, COUNT(*) as turbine_count,
            SUM(capacity_kw) as total_capacity_kw,
            MAX(project_capacity_mw) as project_capacity_mw,
            GROUP_CONCAT(DISTINCT manufacturer) as manufacturers,
            ROUND(AVG(hub_height_m), 1) as avg_hub_height_m,
            ROUND(AVG(rotor_diameter_m), 1) as avg_rotor_diameter_m,
            MIN(project_year) as year_min, MAX(project_year) as year_max,
            MAX(eia_id) as eia_id
        FROM wind_turbines
        WHERE project_name IS NOT NULL
        GROUP BY project_name, state
    """).fetchall()
    grid_conn.close()
    logger.info(f"  USWTDB: {len(uswtdb_projects):,} unique projects (aggregated from 75K turbines)")

    # Build lookup structures
    uswtdb_by_state = {}
    for p in uswtdb_projects:
        state = p['state']
        if state not in uswtdb_by_state:
            uswtdb_by_state[state] = []
        uswtdb_by_state[state].append(dict(p))

    # Load master.db wind projects
    master_conn = sqlite3.connect(MASTER_DB)
    master_conn.row_factory = sqlite3.Row
    wind_projects = master_conn.execute("""
        SELECT id, queue_id, name, state, region, capacity_mw, developer
        FROM projects
        WHERE type_std LIKE '%Wind%'
    """).fetchall()
    logger.info(f"  master.db: {len(wind_projects):,} wind projects")

    if not dry_run:
        cursor = master_conn.cursor()
        # Add columns
        new_cols = [
            ('turbine_count', 'INTEGER'),
            ('total_turbine_capacity_kw', 'REAL'),
            ('primary_manufacturer', 'TEXT'),
            ('avg_hub_height_m', 'REAL'),
            ('avg_rotor_diameter_m', 'REAL'),
            ('turbine_year_range', 'TEXT'),
            ('uswtdb_eia_id', 'INTEGER'),
            ('uswtdb_match_method', 'TEXT'),
            ('uswtdb_match_score', 'REAL'),
        ]
        for col_name, col_type in new_cols:
            try:
                cursor.execute(f'ALTER TABLE projects ADD COLUMN {col_name} {col_type}')
            except sqlite3.OperationalError:
                pass  # already exists

    stats = {'total_wind': len(wind_projects), 'matched': 0, 'unmatched': 0,
             'by_method': {'exact_name': 0, 'fuzzy_name': 0, 'capacity_match': 0}}

    for proj in wind_projects:
        state = proj['state']
        name = proj['name']
        capacity_mw = proj['capacity_mw']

        if not state or state not in uswtdb_by_state:
            stats['unmatched'] += 1
            continue

        candidates = uswtdb_by_state[state]
        best_match = None
        best_score = 0.0
        match_method = None

        norm_name = normalize_name(name)

        for cand in candidates:
            cand_norm = normalize_name(cand['project_name'])

            # Exact normalized name match
            if norm_name and cand_norm and norm_name == cand_norm:
                best_match = cand
                best_score = 1.0
                match_method = 'exact_name'
                break

            # Fuzzy name match
            if norm_name and cand_norm:
                sim = name_similarity(norm_name, cand_norm)
                if sim > 0.75 and sim > best_score:
                    best_match = cand
                    best_score = sim
                    match_method = 'fuzzy_name'

            # Capacity-based matching (if name match is weak, boost with capacity proximity)
            if capacity_mw and cand['project_capacity_mw']:
                cap_ratio = min(capacity_mw, cand['project_capacity_mw']) / max(capacity_mw, cand['project_capacity_mw'])
                if cap_ratio > 0.8 and sim > 0.5:
                    combined = sim * 0.7 + cap_ratio * 0.3
                    if combined > best_score:
                        best_match = cand
                        best_score = combined
                        match_method = 'capacity_match'

        if best_match and best_score >= 0.6:
            stats['matched'] += 1
            stats['by_method'][match_method] += 1

            # Primary manufacturer = most common
            mfgs = best_match['manufacturers']
            primary_mfg = mfgs.split(',')[0] if mfgs else None

            year_range = None
            if best_match['year_min'] and best_match['year_max']:
                if best_match['year_min'] == best_match['year_max']:
                    year_range = str(best_match['year_min'])
                else:
                    year_range = f"{best_match['year_min']}-{best_match['year_max']}"

            if not dry_run:
                cursor.execute("""
                    UPDATE projects SET
                        turbine_count = ?,
                        total_turbine_capacity_kw = ?,
                        primary_manufacturer = ?,
                        avg_hub_height_m = ?,
                        avg_rotor_diameter_m = ?,
                        turbine_year_range = ?,
                        uswtdb_eia_id = ?,
                        uswtdb_match_method = ?,
                        uswtdb_match_score = ?
                    WHERE id = ?
                """, (
                    best_match['turbine_count'],
                    best_match['total_capacity_kw'],
                    primary_mfg,
                    best_match['avg_hub_height_m'],
                    best_match['avg_rotor_diameter_m'],
                    year_range,
                    best_match['eia_id'],
                    match_method,
                    round(best_score, 3),
                    proj['id'],
                ))
        else:
            stats['unmatched'] += 1

    if not dry_run:
        master_conn.commit()

    master_conn.close()

    match_rate = 100 * stats['matched'] / stats['total_wind'] if stats['total_wind'] else 0
    logger.info(f"  Matched: {stats['matched']:,} / {stats['total_wind']:,} ({match_rate:.1f}%)")
    logger.info(f"  By method: {stats['by_method']}")
    logger.info(f"  Unmatched: {stats['unmatched']:,}")

    return stats


# ─────────────────────────────────────────────────────────────
# PART 2: Utility Context Enrichment
# ─────────────────────────────────────────────────────────────

def enrich_utility_context(dry_run: bool = False) -> Dict:
    """Match projects to utilities via state+county → service_territories."""
    logger.info("\n=== PART 2: Utility Context Enrichment ===")

    if not EIA_DB.exists():
        logger.error(f"eia.db not found at {EIA_DB}")
        return {'error': 'eia.db not found'}

    # Build utility lookup from service_territories (most recent year per state+county)
    eia_conn = sqlite3.connect(EIA_DB)
    eia_conn.row_factory = sqlite3.Row

    # Get the most recent utility for each state+county
    logger.info("  Building utility lookup from service_territories...")
    territories = eia_conn.execute("""
        SELECT state, county, utility_id_eia, MAX(report_date) as latest
        FROM service_territories
        WHERE state IS NOT NULL AND county IS NOT NULL
        GROUP BY state, county, utility_id_eia
        ORDER BY state, county, latest DESC
    """).fetchall()

    # Build state+county → utility_id_eia lookup (take the most recent)
    territory_lookup = {}
    for t in territories:
        key = (t['state'], t['county'].lower().strip())
        if key not in territory_lookup:
            territory_lookup[key] = t['utility_id_eia']

    logger.info(f"  Territory lookup: {len(territory_lookup):,} state+county combos")

    # Build utility_id → aggregate stats from utility_sales (most recent year)
    logger.info("  Building utility stats from utility_sales...")
    utility_stats = {}
    sales = eia_conn.execute("""
        SELECT utility_id_eia,
               SUM(customers) as total_customers,
               SUM(sales_mwh) as total_sales_mwh,
               SUM(sales_revenue) as total_revenue,
               MAX(report_date) as latest_year
        FROM utility_sales
        WHERE report_date = (SELECT MAX(report_date) FROM utility_sales)
        GROUP BY utility_id_eia
    """).fetchall()
    for s in sales:
        utility_stats[s['utility_id_eia']] = {
            'total_customers': int(s['total_customers']) if s['total_customers'] else None,
            'total_sales_mwh': int(s['total_sales_mwh']) if s['total_sales_mwh'] else None,
            'total_revenue': int(s['total_revenue']) if s['total_revenue'] else None,
        }
    logger.info(f"  Utility stats: {len(utility_stats):,} utilities")

    eia_conn.close()

    # Match master.db projects
    master_conn = sqlite3.connect(MASTER_DB)
    master_conn.row_factory = sqlite3.Row

    if not dry_run:
        cursor = master_conn.cursor()
        new_cols = [
            ('utility_id_eia', 'INTEGER'),
            ('utility_total_customers', 'INTEGER'),
            ('utility_total_sales_mwh', 'INTEGER'),
            ('utility_total_revenue', 'INTEGER'),
        ]
        for col_name, col_type in new_cols:
            try:
                cursor.execute(f'ALTER TABLE projects ADD COLUMN {col_name} {col_type}')
            except sqlite3.OperationalError:
                pass

    projects = master_conn.execute("""
        SELECT id, state, county FROM projects
        WHERE state IS NOT NULL AND county IS NOT NULL
    """).fetchall()
    logger.info(f"  Projects with state+county: {len(projects):,}")

    stats = {'total': len(projects), 'matched_territory': 0, 'matched_stats': 0, 'unmatched': 0}

    batch = []
    for proj in projects:
        state = proj['state']
        county = proj['county']
        if not state or not county:
            stats['unmatched'] += 1
            continue

        # Normalize county for lookup
        county_norm = county.lower().strip()
        # Try with and without "County" suffix
        utility_id = territory_lookup.get((state, county_norm))
        if not utility_id:
            utility_id = territory_lookup.get((state, county_norm + ' county'))
        if not utility_id:
            # Try removing "County" if present
            if county_norm.endswith(' county'):
                utility_id = territory_lookup.get((state, county_norm[:-7].strip()))

        if utility_id:
            stats['matched_territory'] += 1
            us = utility_stats.get(utility_id, {})
            if us:
                stats['matched_stats'] += 1

            if not dry_run:
                batch.append((
                    utility_id,
                    us.get('total_customers'),
                    us.get('total_sales_mwh'),
                    us.get('total_revenue'),
                    proj['id'],
                ))
        else:
            stats['unmatched'] += 1

    if not dry_run and batch:
        cursor.executemany("""
            UPDATE projects SET
                utility_id_eia = ?,
                utility_total_customers = ?,
                utility_total_sales_mwh = ?,
                utility_total_revenue = ?
            WHERE id = ?
        """, batch)
        master_conn.commit()

    master_conn.close()

    terr_rate = 100 * stats['matched_territory'] / stats['total'] if stats['total'] else 0
    stats_rate = 100 * stats['matched_stats'] / stats['total'] if stats['total'] else 0
    logger.info(f"  Territory matched: {stats['matched_territory']:,} / {stats['total']:,} ({terr_rate:.1f}%)")
    logger.info(f"  With sales stats: {stats['matched_stats']:,} ({stats_rate:.1f}%)")
    logger.info(f"  Unmatched: {stats['unmatched']:,}")

    return stats


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Enrich master.db — wind turbines + utility context")
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--wind', action='store_true')
    parser.add_argument('--utility', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    if not any([args.all, args.wind, args.utility]):
        args.all = True

    print(f"\n{'='*60}")
    print(f"Enrich master.db — Wind + Utility (Dev5)")
    print(f"  master.db: {MASTER_DB}")
    print(f"  grid.db:   {GRID_DB}")
    print(f"  eia.db:    {EIA_DB}")
    print(f"  Dry run:   {args.dry_run}")
    print(f"{'='*60}\n")

    results = {}

    if args.all or args.wind:
        results['wind_turbines'] = enrich_wind_turbines(dry_run=args.dry_run)

    if args.all or args.utility:
        results['utility_context'] = enrich_utility_context(dry_run=args.dry_run)

    print(f"\n{'='*60}")
    print("Results:")
    for name, stats in results.items():
        print(f"  {name}: {stats}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
