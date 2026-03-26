#!/usr/bin/env python3
"""
Fix data quality issues in master.db.
Dev4, 2026-03-25.

Problems addressed:
1. CentraState Medical Center — 31 PJM projects share a wrong name (POI leaked into name)
2. Capacity outliers — 20 GW solar project and other implausible entries
3. COD outliers — 223 placeholder dates (2050-12-31) + 13 pre-1990 dates
4. Empty type_std — 3,088 projects with no type classification
5. Missing names — 12,833 projects with no name
"""

import json
import re
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent / ".data" / "master.db"


def fix_centrastate(conn, dry_run=False):
    """Problem 1: Fix 31 PJM projects named 'CentraState Medical Center PV Facility'.
    These are all different projects — the POI name leaked into the project name.
    No raw_data available, so construct name from queue_id + type + POI."""
    rows = conn.execute(
        "SELECT id, queue_id, poi, type, capacity_mw, developer "
        "FROM projects WHERE name LIKE '%CentraState%'"
    ).fetchall()
    print(f"\n=== Problem 1: CentraState name fix ({len(rows)} projects) ===")

    fixed = 0
    for r in rows:
        poi = r[2] or "Unknown POI"
        proj_type = r[3] or "Unknown"
        cap = r[4] or 0
        # Build a descriptive name: "Offshore Wind 369.6MW at Larrabee 230 kV (AI1-001)"
        new_name = f"{proj_type} {cap}MW at {poi}"
        if not dry_run:
            conn.execute("UPDATE projects SET name = ? WHERE id = ?", (new_name, r[0]))
        print(f"  {r[1]}: '{new_name}'")
        fixed += 1

    print(f"  Fixed: {fixed} projects")
    return fixed


def fix_capacity_outliers(conn, dry_run=False):
    """Problem 2: Flag capacity outliers. Add data_quality_flag column.
    Projects >5000 MW are almost certainly data errors (largest real US project is ~3.5 GW).
    Don't delete — flag them so they're excluded from aggregates."""

    # Add data_quality_flag column if missing
    cols = {c[1] for c in conn.execute("PRAGMA table_info(projects)").fetchall()}
    if "data_quality_flag" not in cols:
        conn.execute("ALTER TABLE projects ADD COLUMN data_quality_flag TEXT")
        print("\n  Added data_quality_flag column")

    # Flag projects > 5000 MW
    rows = conn.execute(
        "SELECT id, queue_id, capacity_mw, type_std, region, source "
        "FROM projects WHERE capacity_mw > 5000"
    ).fetchall()
    print(f"\n=== Problem 2: Capacity outliers ({len(rows)} projects > 5 GW) ===")

    flagged = 0
    for r in rows:
        flag = f"capacity_outlier: {r[2]}MW exceeds 5GW threshold"
        if not dry_run:
            conn.execute(
                "UPDATE projects SET data_quality_flag = ? WHERE id = ?",
                (flag, r[0]),
            )
        print(f"  {r[1]}: {r[2]}MW {r[3]} {r[4]} (src={r[5]}) → FLAGGED")
        flagged += 1

    # Also flag negative capacity
    neg = conn.execute("SELECT COUNT(*) FROM projects WHERE capacity_mw < 0").fetchone()[0]
    if neg > 0:
        if not dry_run:
            conn.execute(
                "UPDATE projects SET data_quality_flag = 'negative_capacity' WHERE capacity_mw < 0"
            )
        print(f"  Also flagged {neg} negative capacity projects")
        flagged += neg

    print(f"  Flagged: {flagged} projects")
    return flagged


def fix_cod_outliers(conn, dry_run=False):
    """Problem 3: Null out COD placeholder/impossible dates.
    - 2050-12-31 is a common placeholder (223 projects)
    - Pre-1990 dates are data errors for interconnection queue projects
    - Also catch anything > 2045 (unreasonable for current queue)"""

    count_2050 = conn.execute("SELECT COUNT(*) FROM projects WHERE cod_std = '2050-12-31'").fetchone()[0]
    count_pre90 = conn.execute("SELECT COUNT(*) FROM projects WHERE cod_std < '1990-01-01'").fetchone()[0]
    count_far = conn.execute(
        "SELECT COUNT(*) FROM projects WHERE cod_std >= '2045-01-01' AND cod_std != '2050-12-31'"
    ).fetchone()[0]

    total = count_2050 + count_pre90 + count_far
    print(f"\n=== Problem 3: COD outliers ({total} projects) ===")
    print(f"  2050-12-31 placeholders: {count_2050}")
    print(f"  Pre-1990 dates: {count_pre90}")
    print(f"  Post-2045 (excl 2050): {count_far}")

    if not dry_run:
        conn.execute("UPDATE projects SET cod_std = NULL WHERE cod_std = '2050-12-31'")
        conn.execute("UPDATE projects SET cod_std = NULL WHERE cod_std < '1990-01-01'")
        conn.execute("UPDATE projects SET cod_std = NULL WHERE cod_std >= '2045-01-01'")

    print(f"  Nulled: {total} cod_std values")
    return total


def fix_type_std(conn, dry_run=False):
    """Problem 4: Fix empty type_std values.
    - Infer from project name where possible
    - Mark ISO-NE ETU/transmission entries as 'Transmission'
    - Leave rest as NULL (unknown is better than wrong)"""

    # Type inference patterns (case-insensitive, applied to name)
    TYPE_PATTERNS = [
        (r'\bsolar\b', 'Solar'),
        (r'\bphotovoltaic\b', 'Solar'),
        (r'\bpv\b', 'Solar'),
        (r'\bwind\b', 'Wind'),
        (r'\boffshore wind\b', 'Offshore Wind'),
        (r'\bbattery\b', 'Storage'),
        (r'\bstorage\b', 'Storage'),
        (r'\bbess\b', 'Storage'),
        (r'\bnuclear\b', 'Nuclear'),
        (r'\bhydro\b', 'Hydro'),
        (r'\bgeothermal\b', 'Geothermal'),
        (r'\bgas\b', 'Gas'),
        (r'\bnatural gas\b', 'Gas'),
        (r'\bETU\b', 'Transmission'),
        (r'\btransmission\b', 'Transmission'),
    ]

    rows = conn.execute(
        "SELECT id, name, type, source FROM projects "
        "WHERE (type_std IS NULL OR type_std = '') AND name IS NOT NULL AND name != ''"
    ).fetchall()

    total_empty = conn.execute("SELECT COUNT(*) FROM projects WHERE type_std IS NULL OR type_std = ''").fetchone()[0]
    print(f"\n=== Problem 4: Empty type_std ({total_empty} total) ===")
    print(f"  Projects with names to check: {len(rows)}")

    inferred = 0
    by_type = {}
    for r in rows:
        name = r[1] or ""
        matched_type = None
        for pattern, type_val in TYPE_PATTERNS:
            if re.search(pattern, name, re.IGNORECASE):
                matched_type = type_val
                break

        if matched_type:
            if not dry_run:
                conn.execute("UPDATE projects SET type_std = ? WHERE id = ?", (matched_type, r[0]))
            by_type[matched_type] = by_type.get(matched_type, 0) + 1
            inferred += 1

    # Also handle raw type='N/A' from ISO-NE — most are ETU (transmission upgrades)
    isone_na = conn.execute(
        "SELECT COUNT(*) FROM projects WHERE type = 'N/A' AND source = 'isone' "
        "AND (type_std IS NULL OR type_std = '')"
    ).fetchone()[0]
    if isone_na > 0 and not dry_run:
        conn.execute(
            "UPDATE projects SET type_std = 'Transmission' "
            "WHERE type = 'N/A' AND source = 'isone' AND (type_std IS NULL OR type_std = '')"
        )
        by_type['Transmission'] = by_type.get('Transmission', 0) + isone_na
        inferred += isone_na

    # Map raw types that are valid but weren't standardized
    raw_type_map = {
        'NG RFO': 'Gas',
        'DFO NG': 'Gas',
        'Load': 'Load',
    }
    for raw, std in raw_type_map.items():
        count = conn.execute(
            "SELECT COUNT(*) FROM projects WHERE type = ? AND (type_std IS NULL OR type_std = '')",
            (raw,),
        ).fetchone()[0]
        if count > 0 and not dry_run:
            conn.execute(
                "UPDATE projects SET type_std = ? WHERE type = ? AND (type_std IS NULL OR type_std = '')",
                (std, raw),
            )
            by_type[std] = by_type.get(std, 0) + count
            inferred += count

    print(f"  Inferred: {inferred}")
    for t, c in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"    {t}: {c}")

    remaining = conn.execute(
        "SELECT COUNT(*) FROM projects WHERE type_std IS NULL OR type_std = ''"
    ).fetchone()[0]
    print(f"  Remaining untyped: {remaining}")
    return inferred


def fix_names(conn, dry_run=False):
    """Problem 5: Recover missing project names.
    - Check raw_data JSON for project_name or name fields
    - For MISO, construct from queue_id + type + state
    - For LBL, check raw_data for name field
    - For the rest, construct a descriptive name from available fields"""

    # First pass: check raw_data
    rows = conn.execute(
        "SELECT id, queue_id, raw_data, source, type, type_std, capacity_mw, state, region, poi "
        "FROM projects WHERE (name IS NULL OR name = '') AND raw_data IS NOT NULL"
    ).fetchall()

    total_nameless = conn.execute("SELECT COUNT(*) FROM projects WHERE name IS NULL OR name = ''").fetchone()[0]
    print(f"\n=== Problem 5: Missing names ({total_nameless} total) ===")
    print(f"  With raw_data: {len(rows)}")

    recovered = 0
    for r in rows:
        try:
            data = json.loads(r[2])
        except (json.JSONDecodeError, TypeError):
            continue

        # Try common name fields
        name = None
        for field in ['project_name', 'name', 'facility_name', 'ProjectName', 'Name']:
            val = data.get(field)
            if val and str(val).strip() and str(val).strip().lower() not in ('nan', 'none', 'null', ''):
                name = str(val).strip()
                break

        if name:
            if not dry_run:
                conn.execute("UPDATE projects SET name = ? WHERE id = ?", (name, r[0]))
            recovered += 1

    print(f"  Recovered from raw_data: {recovered}")

    # Second pass: construct descriptive names for remaining nameless projects
    remaining = conn.execute(
        "SELECT id, queue_id, source, type, type_std, capacity_mw, state, region, poi "
        "FROM projects WHERE (name IS NULL OR name = '')"
    ).fetchall()

    constructed = 0
    for r in remaining:
        qid = r[1] or ""
        proj_type = r[4] or r[3] or "Unknown"
        cap = r[5]
        state = r[6] or ""
        region = r[7] or ""
        poi = r[8] or ""

        # Build: "Solar 150MW - TX (ERCOT-123)"
        parts = [proj_type]
        if cap:
            parts.append(f"{cap}MW")
        if state:
            parts.append(f"- {state}")
        elif region:
            parts.append(f"- {region}")
        if poi:
            parts.append(f"at {poi[:40]}")

        name = " ".join(parts)
        if name and name != "Unknown":
            if not dry_run:
                conn.execute("UPDATE projects SET name = ? WHERE id = ?", (name, r[0]))
            constructed += 1

    print(f"  Constructed from metadata: {constructed}")

    final_remaining = conn.execute(
        "SELECT COUNT(*) FROM projects WHERE name IS NULL OR name = ''"
    ).fetchone()[0]
    print(f"  Still nameless: {final_remaining}")
    return recovered + constructed


def main():
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("=== DRY RUN — no changes will be made ===")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    total_fixes = 0
    total_fixes += fix_centrastate(conn, dry_run)
    total_fixes += fix_capacity_outliers(conn, dry_run)
    total_fixes += fix_cod_outliers(conn, dry_run)
    total_fixes += fix_type_std(conn, dry_run)
    total_fixes += fix_names(conn, dry_run)

    if not dry_run:
        conn.commit()
        print(f"\n=== TOTAL: {total_fixes} fixes applied ===")
    else:
        print(f"\n=== DRY RUN: {total_fixes} fixes would be applied ===")

    conn.close()


if __name__ == "__main__":
    main()
