#!/usr/bin/env python3
"""
Enrich master.db — Grid Infrastructure Context
Dev5, 2026-03-23

Matches master.db projects to grid.db infrastructure via:
  1. POI name → substation fuzzy match (39K projects have POI)
  2. State+county → nearby wind generation from USWTDB
  3. Transmission owner → grid.db line owner stats

No lat/lon in master.db or substations, so distance-based matching
is not possible. POI name matching is the primary approach.

Adds columns:
  - poi_substation_match: matched substation name from grid.db
  - poi_substation_voltage_kv: voltage of matched substation
  - poi_substation_lines: number of connected transmission lines
  - poi_substation_owners: transmission owners at matched substation
  - poi_voltage_kv: voltage extracted from POI text
  - poi_match_score: fuzzy match confidence (0-1)
  - county_wind_capacity_mw: total USWTDB wind capacity in same state+county
  - county_wind_turbine_count: total USWTDB turbines in same state+county
  - county_wind_projects: number of USWTDB wind projects in same state+county

Usage:
    python3 enrich_grid_context.py --all
    python3 enrich_grid_context.py --poi         # POI matching only
    python3 enrich_grid_context.py --wind-nearby  # County wind context only
    python3 enrich_grid_context.py --dry-run      # Preview, no DB writes
"""

import logging
import re
import sqlite3
from collections import defaultdict
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


# ─────────────────────────────────────────────────────────────
# POI Parsing
# ─────────────────────────────────────────────────────────────

def extract_voltage_kv(text: str) -> Optional[float]:
    """Extract voltage in kV from POI text like '345kV' or '345 kV' or '345 KV'."""
    m = re.search(r'(\d+(?:\.\d+)?)\s*kv', text, re.IGNORECASE)
    if m:
        return float(m.group(1))
    return None


def parse_poi_substation_names(poi: str) -> List[str]:
    """Extract substation name(s) from POI string.

    Common POI formats:
      "59903 Bearkat 345kV"              → ["Bearkat"]
      "tap 345kV 23914 Tule Canyon - 23912 Ogallala C2" → ["Tule Canyon", "Ogallala"]
      "138 KV (CEHE Sub) Tap Burke (42410) - Eagle Nest (42430)" → ["CEHE", "Burke", "Eagle Nest"]
      "345kV 8718 Triada"                → ["Triada"]
    """
    if not poi:
        return []

    names = []

    # Remove voltage patterns to isolate names
    cleaned = re.sub(r'\d+(?:\.\d+)?\s*kv', '', poi, flags=re.IGNORECASE)
    # Remove "tap" prefix
    cleaned = re.sub(r'^\s*tap\s+', '', cleaned, flags=re.IGNORECASE)
    # Remove "ckt" / circuit references
    cleaned = re.sub(r'ckt\s*\d+', '', cleaned, flags=re.IGNORECASE)
    # Remove "Line" suffix (references to transmission lines, not substations)
    cleaned = re.sub(r'\s+line\s*$', '', cleaned, flags=re.IGNORECASE)

    # Split on separators: " - ", " – ", " to "
    parts = re.split(r'\s+[-–]\s+|\s+to\s+', cleaned)

    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Remove leading numeric IDs like "59903 " or "#8574 "
        name = re.sub(r'^[#]?\d+\s+', '', part).strip()
        # Remove trailing numeric IDs
        name = re.sub(r'\s+\d+$', '', name).strip()
        # Remove parenthesized IDs like "(42410)"
        name = re.sub(r'\(\d+\)', '', name).strip()
        # Remove "Sub" suffix
        name = re.sub(r'\s+sub\s*$', '', name, flags=re.IGNORECASE).strip()

        # Skip if too short or just numbers/punctuation
        if len(name) >= 2 and re.search(r'[a-zA-Z]', name):
            names.append(name)

    return names


def normalize_substation_name(name: str) -> str:
    """Normalize substation name for matching."""
    if not name:
        return ''
    name = name.upper().strip()
    # Remove common suffixes (longest first to avoid partial matches)
    for suffix in [' SWITCHING STATION', ' RECEIVING STATION', ' SW STATION',
                   ' SUBSTATION', ' SWITCHYARD', ' STATION', ' SUB', ' SS',
                   ' SWITCHING', ' SWITCHYD', ' RECV']:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    # Remove punctuation except spaces
    name = re.sub(r'[^A-Z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


# ─────────────────────────────────────────────────────────────
# PART 1: POI → Substation Matching
# ─────────────────────────────────────────────────────────────

def enrich_poi_substation(dry_run: bool = False) -> Dict:
    """Match project POI names to grid.db substations."""
    logger.info("=== PART 1: POI → Substation Matching ===")

    if not GRID_DB.exists():
        logger.error(f"grid.db not found at {GRID_DB}")
        return {'error': 'grid.db not found'}

    # Load substations (skip junk names)
    grid_conn = sqlite3.connect(GRID_DB)
    grid_conn.row_factory = sqlite3.Row
    substations = grid_conn.execute("""
        SELECT name, connected_lines, max_voltage_kv, owners
        FROM substations
        WHERE name NOT LIKE '#%'
          AND name NOT LIKE 'UNKNOWN%'
          AND name NOT LIKE 'TAP%'
          AND LENGTH(name) >= 3
    """).fetchall()
    grid_conn.close()
    logger.info(f"  Loaded {len(substations):,} usable substations from grid.db")

    # Build normalized lookup: norm_name → [substation_rows]
    sub_lookup = defaultdict(list)
    for s in substations:
        norm = normalize_substation_name(s['name'])
        if norm:
            sub_lookup[norm].append(dict(s))

    logger.info(f"  Unique normalized names: {len(sub_lookup):,}")

    # Load master.db projects with POI
    master_conn = sqlite3.connect(MASTER_DB)
    master_conn.row_factory = sqlite3.Row

    if not dry_run:
        cursor = master_conn.cursor()
        new_cols = [
            ('poi_substation_match', 'TEXT'),
            ('poi_substation_voltage_kv', 'REAL'),
            ('poi_substation_lines', 'INTEGER'),
            ('poi_substation_owners', 'TEXT'),
            ('poi_voltage_kv', 'REAL'),
            ('poi_match_score', 'REAL'),
        ]
        for col_name, col_type in new_cols:
            try:
                cursor.execute(f'ALTER TABLE projects ADD COLUMN {col_name} {col_type}')
            except sqlite3.OperationalError:
                pass

    projects = master_conn.execute("""
        SELECT id, poi, state FROM projects
        WHERE poi IS NOT NULL AND poi != ''
    """).fetchall()
    logger.info(f"  Projects with POI: {len(projects):,}")

    stats = {
        'total_with_poi': len(projects),
        'matched': 0,
        'unmatched': 0,
        'by_method': {'exact': 0, 'fuzzy': 0},
    }

    batch = []
    all_norm_names = list(sub_lookup.keys())

    for proj in projects:
        poi = proj['poi']
        poi_voltage = extract_voltage_kv(poi)
        poi_names = parse_poi_substation_names(poi)

        best_match = None
        best_score = 0.0
        match_method = None

        for poi_name in poi_names:
            norm_poi = normalize_substation_name(poi_name)
            if not norm_poi:
                continue

            # Exact match
            if norm_poi in sub_lookup:
                candidates = sub_lookup[norm_poi]
                # Prefer voltage match if available
                best_cand = candidates[0]
                if poi_voltage and len(candidates) > 1:
                    for c in candidates:
                        if c['max_voltage_kv'] and abs(c['max_voltage_kv'] - poi_voltage) < 1:
                            best_cand = c
                            break
                best_match = best_cand
                best_score = 1.0
                match_method = 'exact'
                break

            # Fuzzy match — only check names that share the first 3 chars
            prefix = norm_poi[:3]
            fuzzy_candidates = [n for n in all_norm_names if n[:3] == prefix]
            for cand_norm in fuzzy_candidates:
                sim = SequenceMatcher(None, norm_poi, cand_norm).ratio()
                if sim > 0.80 and sim > best_score:
                    cands = sub_lookup[cand_norm]
                    best_cand = cands[0]
                    if poi_voltage and len(cands) > 1:
                        for c in cands:
                            if c['max_voltage_kv'] and abs(c['max_voltage_kv'] - poi_voltage) < 1:
                                best_cand = c
                                break
                    best_match = best_cand
                    best_score = sim
                    match_method = 'fuzzy'

        if best_match and best_score >= 0.80:
            stats['matched'] += 1
            stats['by_method'][match_method] += 1

            # Truncate owners to first 3 for readability
            owners = best_match['owners']
            if owners and ',' in owners:
                owner_list = [o.strip() for o in owners.split(',')][:3]
                owners = ', '.join(owner_list)

            batch.append((
                best_match['name'],
                best_match['max_voltage_kv'],
                best_match['connected_lines'],
                owners,
                poi_voltage,
                round(best_score, 3),
                proj['id'],
            ))
        else:
            stats['unmatched'] += 1

    if not dry_run and batch:
        cursor.executemany("""
            UPDATE projects SET
                poi_substation_match = ?,
                poi_substation_voltage_kv = ?,
                poi_substation_lines = ?,
                poi_substation_owners = ?,
                poi_voltage_kv = ?,
                poi_match_score = ?
            WHERE id = ?
        """, batch)
        master_conn.commit()

    master_conn.close()

    match_rate = 100 * stats['matched'] / stats['total_with_poi'] if stats['total_with_poi'] else 0
    logger.info(f"  Matched: {stats['matched']:,} / {stats['total_with_poi']:,} ({match_rate:.1f}%)")
    logger.info(f"  By method: {stats['by_method']}")
    logger.info(f"  Unmatched: {stats['unmatched']:,}")

    return stats


# ─────────────────────────────────────────────────────────────
# PART 2: County-level Wind Generation Context
# ─────────────────────────────────────────────────────────────

def enrich_nearby_wind(dry_run: bool = False) -> Dict:
    """Add county-level wind generation context from USWTDB."""
    logger.info("\n=== PART 2: County Wind Generation Context ===")

    if not GRID_DB.exists():
        logger.error(f"grid.db not found at {GRID_DB}")
        return {'error': 'grid.db not found'}

    # Aggregate wind turbines by state+county
    grid_conn = sqlite3.connect(GRID_DB)
    grid_conn.row_factory = sqlite3.Row
    wind_by_county = {}
    rows = grid_conn.execute("""
        SELECT state, county,
               COUNT(*) as turbine_count,
               ROUND(SUM(capacity_kw) / 1000.0, 1) as total_capacity_mw,
               COUNT(DISTINCT project_name) as project_count
        FROM wind_turbines
        WHERE state IS NOT NULL AND county IS NOT NULL
        GROUP BY state, county
    """).fetchall()
    grid_conn.close()

    for r in rows:
        # Normalize county: USWTDB uses "Kern County" style
        county_norm = r['county'].lower().strip()
        if county_norm.endswith(' county'):
            county_norm = county_norm[:-7].strip()
        key = (r['state'], county_norm)
        wind_by_county[key] = {
            'turbine_count': r['turbine_count'],
            'total_capacity_mw': r['total_capacity_mw'],
            'project_count': r['project_count'],
        }
    logger.info(f"  Wind data: {len(wind_by_county):,} state+county combos with turbines")

    # Match to master.db
    master_conn = sqlite3.connect(MASTER_DB)
    master_conn.row_factory = sqlite3.Row

    if not dry_run:
        cursor = master_conn.cursor()
        new_cols = [
            ('county_wind_capacity_mw', 'REAL'),
            ('county_wind_turbine_count', 'INTEGER'),
            ('county_wind_projects', 'INTEGER'),
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

    stats = {'total': len(projects), 'matched': 0, 'unmatched': 0}
    batch = []

    for proj in projects:
        state = proj['state']
        county = proj['county']
        if not state or not county:
            stats['unmatched'] += 1
            continue

        county_norm = county.lower().strip()
        if county_norm.endswith(' county'):
            county_norm = county_norm[:-7].strip()

        wind = wind_by_county.get((state, county_norm))
        if wind:
            stats['matched'] += 1
            batch.append((
                wind['total_capacity_mw'],
                wind['turbine_count'],
                wind['project_count'],
                proj['id'],
            ))
        else:
            stats['unmatched'] += 1

    if not dry_run and batch:
        cursor.executemany("""
            UPDATE projects SET
                county_wind_capacity_mw = ?,
                county_wind_turbine_count = ?,
                county_wind_projects = ?
            WHERE id = ?
        """, batch)
        master_conn.commit()

    master_conn.close()

    match_rate = 100 * stats['matched'] / stats['total'] if stats['total'] else 0
    logger.info(f"  Matched: {stats['matched']:,} / {stats['total']:,} ({match_rate:.1f}%)")
    logger.info(f"  Unmatched: {stats['unmatched']:,}")

    return stats


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Enrich master.db — grid infrastructure context")
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--poi', action='store_true', help='POI → substation matching only')
    parser.add_argument('--wind-nearby', action='store_true', help='County wind context only')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    if not any([args.all, args.poi, args.wind_nearby]):
        args.all = True

    print(f"\n{'='*60}")
    print(f"Enrich master.db — Grid Infrastructure Context (Dev5)")
    print(f"  master.db: {MASTER_DB}")
    print(f"  grid.db:   {GRID_DB}")
    print(f"  Dry run:   {args.dry_run}")
    print(f"{'='*60}\n")

    results = {}

    if args.all or args.poi:
        results['poi_substation'] = enrich_poi_substation(dry_run=args.dry_run)

    if args.all or args.wind_nearby:
        results['county_wind'] = enrich_nearby_wind(dry_run=args.dry_run)

    print(f"\n{'='*60}")
    print("Results:")
    for name, stats in results.items():
        print(f"  {name}: {stats}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
