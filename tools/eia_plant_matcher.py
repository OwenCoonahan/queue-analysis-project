#!/usr/bin/env python3
"""
EIA Plant ID Matcher — Links master.db projects to EIA-860 generators.

This is the critical Step 1 in the enrichment chain (see PLAYBOOK.md).
Adding plant_id_eia to master.db projects unlocks:
  - FERC Form 1 financials (capex, opex)
  - EPA eGRID emissions (co2_rate)
  - EIA ownership data
  - USWTDB wind turbine details

Match strategy (tiered):
  1. Exact: plant_name + state + capacity within 20%  → high confidence
  2. Normalized: stripped name + state + same tech type → medium confidence
  3. Fuzzy: difflib similarity > 0.8 + state + type    → low confidence

Usage:
    python3 eia_plant_matcher.py                    # Run matching
    python3 eia_plant_matcher.py --dry-run           # Preview without writing
    python3 eia_plant_matcher.py --stats              # Show match statistics
    python3 eia_plant_matcher.py --verify             # Spot-check matches
"""

import sqlite3
import re
import argparse
from pathlib import Path
from difflib import SequenceMatcher
from collections import defaultdict

# Paths
TOOLS_DIR = Path(__file__).parent
MASTER_DB = TOOLS_DIR / '.data' / 'master.db'
EIA_DB = Path(__file__).parent.parent.parent.parent / 'prospector-platform' / 'pipelines' / 'databases' / 'eia.db'

# Map EIA technology_description → master.db type_std
EIA_TECH_TO_TYPE = {
    'Solar Photovoltaic': 'Solar',
    'Solar Thermal with Energy Storage': 'Solar',
    'Solar Thermal without Energy Storage': 'Solar',
    'Onshore Wind Turbine': 'Wind',
    'Offshore Wind Turbine': 'Wind',
    'Batteries': 'Storage',
    'Flywheels': 'Storage',
    'Natural Gas Fired Combustion Turbine': 'Gas',
    'Natural Gas Fired Combined Cycle': 'Gas',
    'Natural Gas Steam Turbine': 'Gas',
    'Natural Gas Internal Combustion Engine': 'Gas',
    'Natural Gas with Compressed Air Storage': 'Gas',
    'Other Natural Gas': 'Gas',
    'Conventional Hydroelectric': 'Hydro',
    'Hydroelectric Pumped Storage': 'Hydro',
    'Conventional Steam Coal': 'Coal',
    'Coal Integrated Gasification Combined Cycle': 'Coal',
    'Nuclear': 'Nuclear',
    'Petroleum Liquids': 'Oil',
    'Petroleum Coke': 'Oil',
    'Landfill Gas': 'Other',
    'Wood/Wood Waste Biomass': 'Other',
    'Other Waste Biomass': 'Other',
    'Municipal Solid Waste': 'Other',
    'Geothermal': 'Other',
    'Other Gases': 'Other',
    'All Other': 'Other',
}

# Words to strip when normalizing plant/project names
STRIP_WORDS = [
    r'\bsolar\s*(farm|project|plant|facility|energy|park|center|station|generation|power|array|pv|photovoltaic)?\b',
    r'\bwind\s*(farm|project|plant|facility|energy|park|center|power|generation)?\b',
    r'\bbattery\s*(storage|energy|facility|project)?\b',
    r'\benergy\s*(storage|center|facility|project|park)?\b',
    r'\bpower\s*(plant|station|facility|project|generation)?\b',
    r'\bgenerating\s*(station|facility|plant)?\b',
    r'\bllc\b', r'\binc\b', r'\bcorp\b', r'\bco\b',
    r'\bph(ase)?\s*[0-9ivx]+\b',
    r'\b[ivx]+\b',  # Roman numerals
    r'\(.*?\)',  # Parenthetical content
    r'\bfka\b.*',  # "formerly known as" and everything after
    r'\baka\b.*',  # "also known as"
    r'[,\-\.]+',  # Punctuation
]


def normalize_name(name: str) -> str:
    """Normalize a plant/project name for fuzzy matching."""
    if not name:
        return ''
    s = name.lower().strip()
    for pattern in STRIP_WORDS:
        s = re.sub(pattern, ' ', s, flags=re.IGNORECASE)
    # Collapse whitespace
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def build_eia_plant_index(eia_conn: sqlite3.Connection) -> dict:
    """
    Build a lookup index of EIA plants from generators_pudl.

    Returns dict keyed by (normalized_name, state) → list of plant records.
    Each plant record is aggregated from latest-year generators.
    """
    print("Building EIA plant index from generators_pudl...", flush=True)

    # Use plants_pudl if available (much smaller — ~234K rows with one row per plant-year)
    # Fall back to generators_pudl if not
    tables = [r[0] for r in eia_conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]

    # Flat query — no CTE, no self-join. Aggregate in Python for speed.
    print("  Fetching generators...", flush=True)
    rows_raw = eia_conn.execute('''
        SELECT plant_id_eia, plant_name_eia, state,
               CAST(capacity_mw AS REAL) as cap, technology_description,
               operational_status, county, latitude, longitude, report_date
        FROM generators_pudl
        WHERE plant_id_eia IS NOT NULL AND plant_id_eia != ''
    ''').fetchall()
    print(f"  Fetched {len(rows_raw)} generator rows, aggregating...", flush=True)

    # Aggregate: keep latest report_date per plant, sum capacity for that date
    plant_data = {}   # plant_id → best row
    plant_caps = defaultdict(float)
    plant_techs = defaultdict(set)
    plant_dates = {}  # plant_id → max report_date

    for r in rows_raw:
        pid = str(r[0])
        rdate = r[9] or ''
        if pid not in plant_dates or rdate > plant_dates[pid]:
            plant_dates[pid] = rdate
            plant_data[pid] = r
            plant_caps[pid] = 0.0
            plant_techs[pid] = set()
        if rdate == plant_dates.get(pid, ''):
            plant_caps[pid] += (r[3] or 0.0)
            if r[4]:
                plant_techs[pid].add(r[4])

    rows = []
    for pid, r in plant_data.items():
        rows.append((pid, r[1], r[2], plant_caps[pid],
                     ','.join(plant_techs[pid]), r[5], r[6], r[7], r[8]))
    print(f"  Aggregated to {len(rows)} unique plants", flush=True)

    # Build indexes
    by_name_state = defaultdict(list)  # (norm_name, state) → [plants]
    by_state = defaultdict(list)       # state → [plants]
    all_plants = {}                    # plant_id → record

    for row in rows:
        plant_id = str(row[0])
        name = row[1] or ''
        state = (row[2] or '').upper().strip()
        capacity = row[3] or 0.0
        techs = row[4] or ''
        op_status = row[5] or ''
        county = row[6] or ''
        lat = row[7]
        lon = row[8]

        # Determine primary type
        primary_type = 'Other'
        for tech in techs.split(','):
            tech = tech.strip()
            if tech in EIA_TECH_TO_TYPE:
                primary_type = EIA_TECH_TO_TYPE[tech]
                break

        norm = normalize_name(name)
        record = {
            'plant_id_eia': plant_id,
            'name': name,
            'norm_name': norm,
            'state': state,
            'capacity_mw': capacity,
            'techs': techs,
            'primary_type': primary_type,
            'op_status': op_status,
            'county': county,
            'lat': lat,
            'lon': lon,
        }

        if norm and state:
            by_name_state[(norm, state)].append(record)
        if state:
            by_state[state].append(record)
        all_plants[plant_id] = record

    print(f"  Indexed {len(all_plants)} EIA plants ({len(by_name_state)} unique name+state combos)")

    return {
        'by_name_state': dict(by_name_state),
        'by_state': dict(by_state),
        'all': all_plants,
    }


def match_projects(master_conn: sqlite3.Connection, eia_index: dict, dry_run: bool = False) -> dict:
    """
    Match master.db projects to EIA plants.

    Returns statistics about matching.
    """
    master_conn.row_factory = sqlite3.Row

    projects = master_conn.execute('''
        SELECT queue_id, region, name, state, capacity_mw, type_std, poi,
               developer, status, status_std
        FROM projects
        WHERE state IS NOT NULL AND state != ''
    ''').fetchall()

    print(f"Matching {len(projects)} projects against EIA plant index...")

    stats = {
        'total': len(projects),
        'matched_high': 0,
        'matched_medium': 0,
        'matched_low': 0,
        'unmatched': 0,
        'by_status': defaultdict(lambda: {'matched': 0, 'total': 0}),
        'by_type': defaultdict(lambda: {'matched': 0, 'total': 0}),
    }

    matches = []  # (queue_id, region, plant_id_eia, confidence, match_method)

    by_name_state = eia_index['by_name_state']
    by_state = eia_index['by_state']

    import sys
    for i, proj in enumerate(projects):
        if i % 5000 == 0 and i > 0:
            print(f"  Progress: {i}/{len(projects)} ({i/len(projects)*100:.0f}%)", flush=True)
        p_name = proj['name'] or ''
        p_state = (proj['state'] or '').upper().strip()
        p_cap = proj['capacity_mw'] or 0.0
        p_type = proj['type_std'] or ''
        p_poi = proj['poi'] or ''
        p_status = (proj['status_std'] or proj['status'] or '').lower()

        # Normalize status for stats
        if 'operational' in p_status or 'in service' in p_status or 'completed' in p_status:
            status_bucket = 'Operational'
        elif 'withdrawn' in p_status:
            status_bucket = 'Withdrawn'
        elif 'active' in p_status or 'study' in p_status:
            status_bucket = 'Active'
        else:
            status_bucket = 'Other'

        stats['by_status'][status_bucket]['total'] += 1
        stats['by_type'][p_type]['total'] += 1

        match = _find_match(p_name, p_state, p_cap, p_type, p_poi,
                           by_name_state, by_state)

        if match:
            plant_id, confidence, method = match
            matches.append((proj['queue_id'], proj['region'], plant_id, confidence, method))
            stats[f'matched_{confidence}'] += 1
            stats['by_status'][status_bucket]['matched'] += 1
            stats['by_type'][p_type]['matched'] += 1
        else:
            stats['unmatched'] += 1

    total_matched = stats['matched_high'] + stats['matched_medium'] + stats['matched_low']
    print(f"\n  Matched: {total_matched}/{stats['total']} ({total_matched/stats['total']*100:.1f}%)")
    print(f"    High:   {stats['matched_high']}")
    print(f"    Medium: {stats['matched_medium']}")
    print(f"    Low:    {stats['matched_low']}")

    if not dry_run and matches:
        _write_matches(master_conn, matches)

    stats['matches'] = matches
    return stats


def _find_match(name, state, capacity, type_std, poi, by_name_state, by_state):
    """
    Try to match a project to an EIA plant using tiered strategy.

    Returns (plant_id_eia, confidence, method) or None.
    """
    if not state:
        return None

    norm_name = normalize_name(name)
    norm_poi = normalize_name(poi)

    # === Tier 1: Exact normalized name + state + capacity within 20% ===
    candidates = by_name_state.get((norm_name, state), [])
    if norm_name and candidates:
        for plant in candidates:
            if _capacity_match(capacity, plant['capacity_mw'], tolerance=0.20):
                return (plant['plant_id_eia'], 'high', 'name+state+capacity')
        # Name+state match without capacity check = medium
        if len(candidates) == 1:
            return (candidates[0]['plant_id_eia'], 'medium', 'name+state')

    # === Tier 2: POI name as plant name + state ===
    if norm_poi:
        poi_candidates = by_name_state.get((norm_poi, state), [])
        if poi_candidates:
            for plant in poi_candidates:
                if _type_match(type_std, plant['primary_type']):
                    return (plant['plant_id_eia'], 'medium', 'poi+state+type')

    # === Tier 3: Fuzzy name match against all plants in same state + type ===
    if norm_name and len(norm_name) > 3 and state in by_state:
        state_plants = by_state[state]
        best_ratio = 0.0
        best_plant = None
        name_len = len(norm_name)

        for plant in state_plants:
            if not _type_match(type_std, plant['primary_type']):
                continue

            plant_norm = plant.get('norm_name', '')
            if not plant_norm:
                continue

            # Quick-reject: if lengths differ by >50%, fuzzy ratio can't be >0.75
            plen = len(plant_norm)
            if plen > 0 and (name_len / plen > 2.0 or plen / name_len > 2.0):
                continue

            ratio = SequenceMatcher(None, norm_name, plant_norm).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_plant = plant

        if best_ratio >= 0.85 and best_plant:
            if _capacity_match(capacity, best_plant['capacity_mw'], tolerance=0.30):
                return (best_plant['plant_id_eia'], 'medium', f'fuzzy({best_ratio:.2f})+type+capacity')
            return (best_plant['plant_id_eia'], 'low', f'fuzzy({best_ratio:.2f})+type')
        elif best_ratio >= 0.75 and best_plant:
            if _capacity_match(capacity, best_plant['capacity_mw'], tolerance=0.20):
                return (best_plant['plant_id_eia'], 'low', f'fuzzy({best_ratio:.2f})+capacity')

    return None


def _capacity_match(cap1, cap2, tolerance=0.20):
    """Check if two capacities are within tolerance of each other."""
    if not cap1 or not cap2:
        return False
    try:
        c1, c2 = float(cap1), float(cap2)
        if c1 == 0 and c2 == 0:
            return True
        if c1 == 0 or c2 == 0:
            return False
        ratio = min(c1, c2) / max(c1, c2)
        return ratio >= (1.0 - tolerance)
    except (ValueError, TypeError):
        return False


def _type_match(project_type, eia_type):
    """Check if project type matches EIA type."""
    if not project_type or not eia_type:
        return True  # Don't filter out if type unknown
    pt = project_type.lower()
    et = eia_type.lower()

    # Direct match
    if pt == et:
        return True

    # Solar + Storage matches both Solar and Storage
    if 'solar' in pt and 'storage' in pt:
        return et in ('solar', 'storage')
    if 'hybrid' in pt:
        return True  # Hybrid can match anything

    return False


def _write_matches(conn, matches):
    """Write match results to master.db."""
    cursor = conn.cursor()

    # Add columns if they don't exist
    cols = cursor.execute('PRAGMA table_info(projects)').fetchall()
    col_names = [c[1] for c in cols]

    if 'plant_id_eia' not in col_names:
        cursor.execute('ALTER TABLE projects ADD COLUMN plant_id_eia INTEGER')
        print("  Added plant_id_eia column")

    if 'eia_match_confidence' not in col_names:
        cursor.execute('ALTER TABLE projects ADD COLUMN eia_match_confidence TEXT')
        print("  Added eia_match_confidence column")

    if 'eia_match_method' not in col_names:
        cursor.execute('ALTER TABLE projects ADD COLUMN eia_match_method TEXT')
        print("  Added eia_match_method column")

    # Write matches
    updated = 0
    for queue_id, region, plant_id, confidence, method in matches:
        cursor.execute('''
            UPDATE projects
            SET plant_id_eia = ?, eia_match_confidence = ?, eia_match_method = ?
            WHERE queue_id = ? AND region = ?
        ''', (int(plant_id), confidence, method, queue_id, region))
        updated += cursor.rowcount

    conn.commit()
    print(f"  Wrote {updated} matches to master.db")


def verify_matches(master_conn, eia_conn, limit=20):
    """Spot-check matches by showing project + EIA plant side by side."""
    master_conn.row_factory = sqlite3.Row

    print("\n=== Match Verification (random sample) ===\n")

    matches = master_conn.execute('''
        SELECT queue_id, region, name, state, capacity_mw, type_std,
               plant_id_eia, eia_match_confidence, eia_match_method
        FROM projects
        WHERE plant_id_eia IS NOT NULL
        ORDER BY RANDOM()
        LIMIT ?
    ''', (limit,)).fetchall()

    for m in matches:
        plant = eia_conn.execute('''
            SELECT plant_name_eia, state, capacity_mw, technology_description
            FROM generators_pudl
            WHERE plant_id_eia = ?
            ORDER BY report_date DESC
            LIMIT 1
        ''', (str(m['plant_id_eia']),)).fetchone()

        if plant:
            print(f"  [{m['eia_match_confidence'].upper()}] {m['eia_match_method']}")
            print(f"    Project: {m['name']} ({m['state']}) {m['capacity_mw']} MW {m['type_std']}")
            print(f"    EIA:     {plant[0]} ({plant[1]}) {float(plant[2]):.0f} MW {plant[3]}")
            print(f"    IDs:     queue={m['queue_id']} region={m['region']} plant_id_eia={m['plant_id_eia']}")
            print()


def print_stats(master_conn):
    """Print match statistics from master.db."""
    master_conn.row_factory = sqlite3.Row

    total = master_conn.execute('SELECT COUNT(*) FROM projects').fetchone()[0]
    matched = master_conn.execute('SELECT COUNT(*) FROM projects WHERE plant_id_eia IS NOT NULL').fetchone()[0]

    print(f"\n=== EIA Match Statistics ===")
    print(f"Total projects: {total}")
    print(f"Matched: {matched} ({matched/total*100:.1f}%)")

    # By confidence
    print("\nBy confidence:")
    for conf in ['high', 'medium', 'low']:
        count = master_conn.execute(
            'SELECT COUNT(*) FROM projects WHERE eia_match_confidence = ?', (conf,)
        ).fetchone()[0]
        print(f"  {conf}: {count} ({count/total*100:.1f}%)")

    # By status (using status_std or status)
    print("\nBy project status:")
    rows = master_conn.execute('''
        SELECT
            CASE
                WHEN LOWER(COALESCE(status_std, status)) LIKE '%operational%'
                     OR LOWER(COALESCE(status_std, status)) LIKE '%in service%'
                     OR LOWER(COALESCE(status_std, status)) LIKE '%completed%' THEN 'Operational'
                WHEN LOWER(COALESCE(status_std, status)) LIKE '%withdrawn%' THEN 'Withdrawn'
                WHEN LOWER(COALESCE(status_std, status)) LIKE '%active%'
                     OR LOWER(COALESCE(status_std, status)) LIKE '%study%' THEN 'Active'
                ELSE 'Other'
            END as status_bucket,
            COUNT(*) as total,
            SUM(CASE WHEN plant_id_eia IS NOT NULL THEN 1 ELSE 0 END) as matched
        FROM projects
        GROUP BY status_bucket
    ''').fetchall()
    for r in rows:
        pct = r[2]/r[1]*100 if r[1] > 0 else 0
        print(f"  {r[0]}: {r[2]}/{r[1]} ({pct:.1f}%)")

    # By type_std
    print("\nBy technology type:")
    rows = master_conn.execute('''
        SELECT type_std, COUNT(*) as total,
               SUM(CASE WHEN plant_id_eia IS NOT NULL THEN 1 ELSE 0 END) as matched
        FROM projects
        WHERE type_std IS NOT NULL
        GROUP BY type_std
        ORDER BY total DESC
        LIMIT 10
    ''').fetchall()
    for r in rows:
        pct = r[2]/r[1]*100 if r[1] > 0 else 0
        print(f"  {r[0]}: {r[2]}/{r[1]} ({pct:.1f}%)")

    # Unique plant IDs matched
    unique = master_conn.execute(
        'SELECT COUNT(DISTINCT plant_id_eia) FROM projects WHERE plant_id_eia IS NOT NULL'
    ).fetchone()[0]
    print(f"\nUnique EIA plants matched: {unique}")


def main():
    parser = argparse.ArgumentParser(description='Match master.db projects to EIA-860 generators')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing to DB')
    parser.add_argument('--stats', action='store_true', help='Show current match statistics')
    parser.add_argument('--verify', action='store_true', help='Spot-check random matches')
    parser.add_argument('--verify-count', type=int, default=20, help='Number of matches to verify')
    parser.add_argument('--master-db', type=str, default=str(MASTER_DB), help='Path to master.db')
    parser.add_argument('--eia-db', type=str, default=str(EIA_DB), help='Path to eia.db')

    args = parser.parse_args()

    master_conn = sqlite3.connect(args.master_db)
    eia_conn = sqlite3.connect(args.eia_db)

    if args.stats:
        print_stats(master_conn)
        master_conn.close()
        eia_conn.close()
        return

    if args.verify:
        verify_matches(master_conn, eia_conn, args.verify_count)
        master_conn.close()
        eia_conn.close()
        return

    # Build EIA index
    eia_index = build_eia_plant_index(eia_conn)

    # Run matching
    stats = match_projects(master_conn, eia_index, dry_run=args.dry_run)

    # Print detailed stats
    print("\nBy project status:")
    for status, data in sorted(stats['by_status'].items()):
        pct = data['matched']/data['total']*100 if data['total'] > 0 else 0
        print(f"  {status}: {data['matched']}/{data['total']} ({pct:.1f}%)")

    print("\nBy technology type (top 10):")
    sorted_types = sorted(stats['by_type'].items(), key=lambda x: x[1]['total'], reverse=True)
    for typ, data in sorted_types[:10]:
        pct = data['matched']/data['total']*100 if data['total'] > 0 else 0
        print(f"  {typ or 'None'}: {data['matched']}/{data['total']} ({pct:.1f}%)")

    if not args.dry_run:
        print("\nVerifying sample matches...")
        verify_matches(master_conn, eia_conn, limit=10)

    master_conn.close()
    eia_conn.close()


if __name__ == '__main__':
    main()
