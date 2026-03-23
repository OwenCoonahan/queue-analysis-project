#!/usr/bin/env python3
"""
EIA Match Enhancement — Improves plant_id_eia coverage beyond the initial 35.1%.

Run AFTER eia_plant_matcher.py. Applies additional strategies:
  1. Adopt uswtdb_eia_id for wind projects missing plant_id_eia
  2. County + type + capacity matching for unmatched projects
  3. POI-to-substation name matching via EIA plant names
  4. Cross-state name matching (CAISO CA projects → AZ/NV EIA plants)
  5. Operational-focused fuzzy match (lower threshold, cross-state, capacity match)

Usage:
    python3 eia_match_enhance.py                # Run all enhancements
    python3 eia_match_enhance.py --dry-run      # Preview without writing
    python3 eia_match_enhance.py --stats        # Show current stats
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
GRID_DB = TOOLS_DIR / '.data' / 'grid.db'

# Import normalize_name and EIA_TECH_TO_TYPE from eia_plant_matcher
from eia_plant_matcher import normalize_name, EIA_TECH_TO_TYPE


def adopt_uswtdb_matches(master_conn, dry_run=False):
    """
    Strategy 1: Use uswtdb_eia_id (from Dev5's USWTDB cross-reference)
    for wind projects that don't have plant_id_eia yet.
    """
    print("\n--- Strategy 1: Adopt USWTDB EIA IDs ---", flush=True)

    # Check if uswtdb_eia_id column exists
    cols = [c[1] for c in master_conn.execute('PRAGMA table_info(projects)').fetchall()]
    if 'uswtdb_eia_id' not in cols:
        print("  uswtdb_eia_id column not found — skipping")
        return 0

    candidates = master_conn.execute('''
        SELECT queue_id, region, uswtdb_eia_id, name, state
        FROM projects
        WHERE uswtdb_eia_id IS NOT NULL
          AND plant_id_eia IS NULL
    ''').fetchall()

    print(f"  Found {len(candidates)} wind projects with uswtdb_eia_id but no plant_id_eia")

    if dry_run or not candidates:
        return len(candidates)

    updated = 0
    for row in candidates:
        master_conn.execute('''
            UPDATE projects
            SET plant_id_eia = ?, eia_match_confidence = 'high', eia_match_method = 'uswtdb_eia_id'
            WHERE queue_id = ? AND region = ?
        ''', (int(row[2]), row[0], row[1]))
        updated += 1

    master_conn.commit()
    print(f"  Wrote {updated} matches from USWTDB")
    return updated


def county_type_capacity_match(master_conn, eia_conn, dry_run=False):
    """
    Strategy 2: Match unmatched projects via county + type + capacity.
    When name matching fails, projects in the same county with the same type
    and similar capacity are likely the same plant.
    Only applies when there's exactly 1 candidate (to avoid false positives).
    """
    print("\n--- Strategy 2: County + Type + Capacity Match ---", flush=True)

    # Build EIA county index: (county, state, type) → [plants]
    print("  Building EIA county index...", flush=True)
    rows = eia_conn.execute('''
        SELECT plant_id_eia, plant_name_eia, state, county,
               CAST(capacity_mw AS REAL) as cap, technology_description
        FROM generators_pudl
        WHERE plant_id_eia IS NOT NULL AND plant_id_eia != ''
          AND county IS NOT NULL AND county != ''
        ORDER BY report_date DESC
    ''').fetchall()

    # Aggregate by plant_id
    plant_data = {}
    for r in rows:
        pid = str(r[0])
        if pid not in plant_data:
            # Map tech to type
            tech = r[5] or ''
            ptype = EIA_TECH_TO_TYPE.get(tech, 'Other')
            plant_data[pid] = {
                'plant_id': pid,
                'name': r[1] or '',
                'state': (r[2] or '').upper(),
                'county': (r[3] or '').upper().strip(),
                'capacity': r[4] or 0.0,
                'type': ptype,
            }

    # Index by (county, state, type)
    county_index = defaultdict(list)
    for p in plant_data.values():
        if p['county'] and p['state'] and p['type']:
            county_index[(p['county'], p['state'], p['type'])].append(p)

    print(f"  Indexed {len(plant_data)} plants across {len(county_index)} county+type combos")

    # Get unmatched projects with county
    unmatched = master_conn.execute('''
        SELECT queue_id, region, name, state, county, capacity_mw, type_std
        FROM projects
        WHERE plant_id_eia IS NULL
          AND state IS NOT NULL AND state != ''
          AND county IS NOT NULL AND county != ''
          AND type_std IS NOT NULL AND type_std != ''
    ''').fetchall()

    print(f"  Checking {len(unmatched)} unmatched projects with county data")

    matches = []
    for row in unmatched:
        county = (row[4] or '').upper().strip()
        state = (row[3] or '').upper().strip()
        ptype = row[6] or ''
        capacity = row[5] or 0.0

        candidates = county_index.get((county, state, ptype), [])
        if len(candidates) == 1:
            plant = candidates[0]
            # Check capacity within 30%
            if capacity > 0 and plant['capacity'] > 0:
                ratio = min(capacity, plant['capacity']) / max(capacity, plant['capacity'])
                if ratio >= 0.70:
                    matches.append((row[0], row[1], plant['plant_id'], 'low', 'county+type+capacity'))

    print(f"  Found {len(matches)} county+type+capacity matches (single-candidate only)")

    if dry_run or not matches:
        return len(matches)

    updated = 0
    for queue_id, region, plant_id, confidence, method in matches:
        master_conn.execute('''
            UPDATE projects
            SET plant_id_eia = ?, eia_match_confidence = ?, eia_match_method = ?
            WHERE queue_id = ? AND region = ? AND plant_id_eia IS NULL
        ''', (int(plant_id), confidence, method, queue_id, region))
        updated += master_conn.execute('SELECT changes()').fetchone()[0]

    master_conn.commit()
    print(f"  Wrote {updated} county matches")
    return updated


def poi_substation_match(master_conn, eia_conn, dry_run=False):
    """
    Strategy 3: Match POI names to EIA plant names more aggressively.
    Many POIs contain the plant name (e.g. "Smithburg 345kV" → "Smithburg Solar").
    Extract the core name from POI, strip voltage/substation suffixes, match to EIA.
    """
    print("\n--- Strategy 3: Enhanced POI Name Match ---", flush=True)

    # Build EIA name index by state
    print("  Building EIA name index...", flush=True)
    rows = eia_conn.execute('''
        SELECT plant_id_eia, plant_name_eia, state,
               CAST(capacity_mw AS REAL) as cap, technology_description
        FROM generators_pudl
        WHERE plant_id_eia IS NOT NULL AND plant_id_eia != ''
          AND plant_name_eia IS NOT NULL AND plant_name_eia != ''
        ORDER BY report_date DESC
    ''').fetchall()

    plant_by_id = {}
    for r in rows:
        pid = str(r[0])
        if pid not in plant_by_id:
            tech = r[4] or ''
            plant_by_id[pid] = {
                'plant_id': pid,
                'name': r[1],
                'norm_name': normalize_name(r[1]),
                'state': (r[2] or '').upper(),
                'capacity': r[3] or 0.0,
                'type': EIA_TECH_TO_TYPE.get(tech, 'Other'),
            }

    by_state = defaultdict(list)
    for p in plant_by_id.values():
        if p['state'] and p['norm_name']:
            by_state[p['state']].append(p)

    # Voltage/substation suffixes to strip from POI
    poi_strip = re.compile(
        r'\b\d+\s*kv\b|\bsubstation\b|\bsub\b|\bswitchyard\b|\btap\b|'
        r'\bbus\b|\bjunction\b|\bjct\b|\b\d{3,}\b',
        re.IGNORECASE
    )

    # Get unmatched projects with POI
    unmatched = master_conn.execute('''
        SELECT queue_id, region, name, state, poi, capacity_mw, type_std
        FROM projects
        WHERE plant_id_eia IS NULL
          AND state IS NOT NULL AND state != ''
          AND poi IS NOT NULL AND poi != ''
    ''').fetchall()

    print(f"  Checking {len(unmatched)} unmatched projects with POI data")

    matches = []
    for row in unmatched:
        state = (row[3] or '').upper()
        poi = row[4] or ''
        ptype = row[6] or ''
        capacity = row[5] or 0.0

        # Clean POI: strip voltage, "substation", numbers
        poi_clean = poi_strip.sub(' ', poi)
        poi_clean = re.sub(r'[,\-\.]+', ' ', poi_clean)
        poi_clean = re.sub(r'\s+', ' ', poi_clean).strip().lower()

        if len(poi_clean) < 4:
            continue

        state_plants = by_state.get(state, [])
        best_ratio = 0.0
        best_plant = None

        for plant in state_plants:
            # Type filter
            if ptype and plant['type'] != 'Other' and ptype.lower() != plant['type'].lower():
                if not ('solar' in ptype.lower() and 'storage' in ptype.lower()):
                    continue

            pn = plant['norm_name']
            if not pn or len(pn) < 4:
                continue

            # Quick length reject
            if len(poi_clean) / len(pn) > 3.0 or len(pn) / len(poi_clean) > 3.0:
                continue

            # Check if POI contains the plant name or vice versa
            if pn in poi_clean or poi_clean in pn:
                ratio = 0.90
            else:
                ratio = SequenceMatcher(None, poi_clean, pn).ratio()

            if ratio > best_ratio:
                best_ratio = ratio
                best_plant = plant

        if best_ratio >= 0.80 and best_plant:
            # Verify capacity is in the right ballpark (within 50% — looser for POI)
            if capacity > 0 and best_plant['capacity'] > 0:
                cap_ratio = min(capacity, best_plant['capacity']) / max(capacity, best_plant['capacity'])
                if cap_ratio >= 0.50:
                    matches.append((row[0], row[1], best_plant['plant_id'], 'low',
                                    f'poi_enhanced({best_ratio:.2f})'))

    print(f"  Found {len(matches)} enhanced POI matches")

    if dry_run or not matches:
        return len(matches)

    updated = 0
    for queue_id, region, plant_id, confidence, method in matches:
        master_conn.execute('''
            UPDATE projects
            SET plant_id_eia = ?, eia_match_confidence = ?, eia_match_method = ?
            WHERE queue_id = ? AND region = ? AND plant_id_eia IS NULL
        ''', (int(plant_id), confidence, method, queue_id, region))
        updated += master_conn.execute('SELECT changes()').fetchone()[0]

    master_conn.commit()
    print(f"  Wrote {updated} POI matches")
    return updated


# ISOs that span multiple states — projects listed under one state may
# actually be in a neighboring state in EIA's records
CROSS_STATE_MAP = {
    'CAISO': ['CA', 'AZ', 'NV', 'OR', 'WA', 'UT', 'NM'],
    'West': ['CA', 'AZ', 'NV', 'OR', 'WA', 'UT', 'NM', 'CO', 'WY', 'MT', 'ID'],
    'PJM': ['PA', 'NJ', 'MD', 'DE', 'VA', 'WV', 'OH', 'IN', 'IL', 'MI', 'KY', 'NC', 'TN', 'DC'],
    'MISO': ['MN', 'WI', 'IA', 'MO', 'IL', 'IN', 'MI', 'AR', 'MS', 'LA', 'TX', 'MT', 'ND', 'SD'],
    'SPP': ['KS', 'OK', 'TX', 'NE', 'SD', 'ND', 'AR', 'MO', 'NM', 'LA', 'MN', 'IA', 'MT', 'WY'],
    'ISO-NE': ['CT', 'MA', 'ME', 'NH', 'RI', 'VT'],
    'NYISO': ['NY'],
    'ERCOT': ['TX'],
    'Southeast': ['NC', 'SC', 'GA', 'FL', 'AL', 'TN', 'KY', 'VA', 'MS'],
}


def cross_state_name_match(master_conn, eia_conn, dry_run=False):
    """
    Strategy 4: Cross-state matching for multi-state ISOs.

    CAISO projects are listed as state=CA but the actual plant may be in AZ, NV, etc.
    Match by normalized name across all states in the ISO's footprint.
    """
    print("\n--- Strategy 4: Cross-State Name Match ---", flush=True)

    # Build EIA plant index by name (no state restriction)
    print("  Building EIA name-only index...", flush=True)
    rows = eia_conn.execute('''
        SELECT plant_id_eia, plant_name_eia, state,
               CAST(capacity_mw AS REAL) as cap, technology_description
        FROM generators_pudl
        WHERE plant_id_eia IS NOT NULL AND plant_id_eia != ''
          AND plant_name_eia IS NOT NULL AND plant_name_eia != ''
        ORDER BY report_date DESC
    ''').fetchall()

    plant_by_id = {}
    for r in rows:
        pid = str(r[0])
        if pid not in plant_by_id:
            tech = r[4] or ''
            plant_by_id[pid] = {
                'plant_id': pid,
                'name': r[1],
                'norm_name': normalize_name(r[1]),
                'state': (r[2] or '').upper(),
                'capacity': r[3] or 0.0,
                'type': EIA_TECH_TO_TYPE.get(tech, 'Other'),
            }

    # Index by (norm_name, state)
    by_name_state = defaultdict(list)
    by_state = defaultdict(list)
    for p in plant_by_id.values():
        if p['norm_name'] and p['state']:
            by_name_state[(p['norm_name'], p['state'])].append(p)
            by_state[p['state']].append(p)

    # Get unmatched projects where the region spans multiple states
    unmatched = master_conn.execute('''
        SELECT queue_id, region, name, state, capacity_mw, type_std
        FROM projects
        WHERE plant_id_eia IS NULL
          AND state IS NOT NULL AND state != ''
          AND name IS NOT NULL AND name != ''
    ''').fetchall()

    print(f"  Checking {len(unmatched)} unmatched projects for cross-state matches")

    matches = []
    for row in unmatched:
        region = row[1] or ''
        proj_state = (row[3] or '').upper()
        proj_name = row[2] or ''
        proj_cap = row[4] or 0.0
        proj_type = row[5] or ''
        norm_name = normalize_name(proj_name)

        if not norm_name or len(norm_name) < 4:
            continue

        # Get the states to search across for this ISO
        search_states = CROSS_STATE_MAP.get(region, [])
        # Only search OTHER states (same-state was already tried)
        other_states = [s for s in search_states if s != proj_state]

        if not other_states:
            continue

        # Tier A: Exact normalized name match in other states
        for alt_state in other_states:
            candidates = by_name_state.get((norm_name, alt_state), [])
            for plant in candidates:
                if proj_cap > 0 and plant['capacity'] > 0:
                    ratio = min(proj_cap, plant['capacity']) / max(proj_cap, plant['capacity'])
                    if ratio >= 0.50:
                        matches.append((row[0], row[1], plant['plant_id'], 'medium',
                                        f'cross_state_name({proj_state}->{alt_state})'))
                        break
                elif len(candidates) == 1:
                    matches.append((row[0], row[1], plant['plant_id'], 'low',
                                    f'cross_state_name({proj_state}->{alt_state})'))
                    break
            else:
                continue
            break  # Found a match, stop searching states

    print(f"  Found {len(matches)} cross-state name matches")

    if dry_run or not matches:
        return len(matches)

    updated = 0
    for queue_id, region, plant_id, confidence, method in matches:
        master_conn.execute('''
            UPDATE projects
            SET plant_id_eia = ?, eia_match_confidence = ?, eia_match_method = ?
            WHERE queue_id = ? AND region = ? AND plant_id_eia IS NULL
        ''', (int(plant_id), confidence, method, queue_id, region))
        updated += master_conn.execute('SELECT changes()').fetchone()[0]

    master_conn.commit()
    print(f"  Wrote {updated} cross-state matches")
    return updated


def operational_fuzzy_match(master_conn, eia_conn, dry_run=False):
    """
    Strategy 5: Aggressive fuzzy matching for OPERATIONAL projects only.

    Operational projects should have a corresponding EIA plant, so we can use
    lower thresholds with more confidence. Search across ISO footprint states.
    """
    print("\n--- Strategy 5: Operational-Focused Fuzzy Match ---", flush=True)

    # Build EIA index
    print("  Building EIA index...", flush=True)
    rows = eia_conn.execute('''
        SELECT plant_id_eia, plant_name_eia, state,
               CAST(capacity_mw AS REAL) as cap, technology_description
        FROM generators_pudl
        WHERE plant_id_eia IS NOT NULL AND plant_id_eia != ''
          AND plant_name_eia IS NOT NULL AND plant_name_eia != ''
        ORDER BY report_date DESC
    ''').fetchall()

    plant_by_id = {}
    for r in rows:
        pid = str(r[0])
        if pid not in plant_by_id:
            tech = r[4] or ''
            plant_by_id[pid] = {
                'plant_id': pid,
                'name': r[1],
                'norm_name': normalize_name(r[1]),
                'state': (r[2] or '').upper(),
                'capacity': r[3] or 0.0,
                'type': EIA_TECH_TO_TYPE.get(tech, 'Other'),
            }

    by_state = defaultdict(list)
    for p in plant_by_id.values():
        if p['norm_name'] and p['state']:
            by_state[p['state']].append(p)

    # Get ONLY unmatched operational projects
    unmatched = master_conn.execute('''
        SELECT queue_id, region, name, state, capacity_mw, type_std
        FROM projects
        WHERE plant_id_eia IS NULL
          AND state IS NOT NULL AND state != ''
          AND name IS NOT NULL AND name != ''
          AND (LOWER(COALESCE(status_std, status)) LIKE '%operational%'
               OR LOWER(COALESCE(status_std, status)) LIKE '%in service%'
               OR LOWER(COALESCE(status_std, status)) LIKE '%completed%')
    ''').fetchall()

    print(f"  Checking {len(unmatched)} unmatched operational projects")

    matches = []
    for i, row in enumerate(unmatched):
        if i % 1000 == 0 and i > 0:
            print(f"    Progress: {i}/{len(unmatched)}", flush=True)

        region = row[1] or ''
        proj_state = (row[3] or '').upper()
        proj_name = row[2] or ''
        proj_cap = row[4] or 0.0
        proj_type = row[5] or ''
        norm_name = normalize_name(proj_name)

        if not norm_name or len(norm_name) < 3:
            continue

        # Search project's state + ISO footprint states
        search_states = set([proj_state] + CROSS_STATE_MAP.get(region, []))

        best_ratio = 0.0
        best_plant = None
        name_len = len(norm_name)

        for search_state in search_states:
            for plant in by_state.get(search_state, []):
                # Type check (relaxed for operational — just exclude obvious mismatches)
                if proj_type and plant['type'] != 'Other' and proj_type != 'Other':
                    pt = proj_type.lower()
                    et = plant['type'].lower()
                    if pt != et and not ('solar' in pt and 'storage' in pt):
                        if not ('hybrid' in pt):
                            continue

                pn = plant['norm_name']
                if not pn:
                    continue

                plen = len(pn)
                if plen > 0 and (name_len / plen > 2.5 or plen / name_len > 2.5):
                    continue

                ratio = SequenceMatcher(None, norm_name, pn).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_plant = plant

        # Lower threshold for operational (0.70 vs 0.75), but require capacity match
        if best_ratio >= 0.70 and best_plant:
            if proj_cap > 0 and best_plant['capacity'] > 0:
                cap_ratio = min(proj_cap, best_plant['capacity']) / max(proj_cap, best_plant['capacity'])
                if cap_ratio >= 0.40:
                    conf = 'medium' if best_ratio >= 0.85 else 'low'
                    state_note = f"->{best_plant['state']}" if best_plant['state'] != proj_state else ""
                    matches.append((row[0], row[1], best_plant['plant_id'], conf,
                                    f'operational_fuzzy({best_ratio:.2f}{state_note})'))

    print(f"  Found {len(matches)} operational fuzzy matches")

    if dry_run or not matches:
        return len(matches)

    updated = 0
    for queue_id, region, plant_id, confidence, method in matches:
        master_conn.execute('''
            UPDATE projects
            SET plant_id_eia = ?, eia_match_confidence = ?, eia_match_method = ?
            WHERE queue_id = ? AND region = ? AND plant_id_eia IS NULL
        ''', (int(plant_id), confidence, method, queue_id, region))
        updated += master_conn.execute('SELECT changes()').fetchone()[0]

    master_conn.commit()
    print(f"  Wrote {updated} operational fuzzy matches")
    return updated


def print_stats(master_conn):
    """Print current match statistics."""
    total = master_conn.execute('SELECT COUNT(*) FROM projects').fetchone()[0]
    matched = master_conn.execute('SELECT COUNT(*) FROM projects WHERE plant_id_eia IS NOT NULL').fetchone()[0]

    print(f"\n=== EIA Match Statistics (Enhanced) ===")
    print(f"Total projects: {total}")
    print(f"Matched: {matched} ({matched/total*100:.1f}%)")

    print("\nBy confidence:")
    for conf in ['high', 'medium', 'low']:
        count = master_conn.execute(
            'SELECT COUNT(*) FROM projects WHERE eia_match_confidence = ?', (conf,)
        ).fetchone()[0]
        print(f"  {conf}: {count} ({count/total*100:.1f}%)")

    print("\nBy match method:")
    rows = master_conn.execute('''
        SELECT eia_match_method, COUNT(*) as cnt
        FROM projects
        WHERE plant_id_eia IS NOT NULL
        GROUP BY eia_match_method
        ORDER BY cnt DESC
        LIMIT 15
    ''').fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]}")

    unique = master_conn.execute(
        'SELECT COUNT(DISTINCT plant_id_eia) FROM projects WHERE plant_id_eia IS NOT NULL'
    ).fetchone()[0]
    print(f"\nUnique EIA plants matched: {unique}")


def main():
    parser = argparse.ArgumentParser(description='Enhance EIA plant matching coverage')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing')
    parser.add_argument('--stats', action='store_true', help='Show current stats')
    args = parser.parse_args()

    master_conn = sqlite3.connect(str(MASTER_DB))
    master_conn.row_factory = sqlite3.Row
    eia_conn = sqlite3.connect(str(EIA_DB))

    if args.stats:
        print_stats(master_conn)
        master_conn.close()
        eia_conn.close()
        return

    print("=== EIA Match Enhancement ===", flush=True)

    # Before stats
    before = master_conn.execute('SELECT COUNT(*) FROM projects WHERE plant_id_eia IS NOT NULL').fetchone()[0]
    total = master_conn.execute('SELECT COUNT(*) FROM projects').fetchone()[0]
    print(f"\nBefore: {before}/{total} ({before/total*100:.1f}%) matched")

    # Run strategies
    s1 = adopt_uswtdb_matches(master_conn, dry_run=args.dry_run)
    s2 = county_type_capacity_match(master_conn, eia_conn, dry_run=args.dry_run)
    s3 = poi_substation_match(master_conn, eia_conn, dry_run=args.dry_run)
    s4 = cross_state_name_match(master_conn, eia_conn, dry_run=args.dry_run)
    s5 = operational_fuzzy_match(master_conn, eia_conn, dry_run=args.dry_run)

    # After stats
    after = master_conn.execute('SELECT COUNT(*) FROM projects WHERE plant_id_eia IS NOT NULL').fetchone()[0]
    gained = after - before
    print(f"\n=== Summary ===")
    print(f"  Strategy 1 (USWTDB adoption):    +{s1}")
    print(f"  Strategy 2 (county+type+cap):    +{s2}")
    print(f"  Strategy 3 (POI enhanced):       +{s3}")
    print(f"  Strategy 4 (cross-state name):   +{s4}")
    print(f"  Strategy 5 (operational fuzzy):  +{s5}")
    print(f"  Total new matches: +{gained}")
    print(f"  After: {after}/{total} ({after/total*100:.1f}%) matched")

    if not args.dry_run:
        print_stats(master_conn)

    master_conn.close()
    eia_conn.close()


if __name__ == '__main__':
    main()
