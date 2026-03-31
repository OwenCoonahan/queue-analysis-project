#!/usr/bin/env python3
"""
Developer Backfill — fills developer names for projects that have none.

Three strategies:
  1. EIA-860 ownership: For projects with plant_id_eia, pull owner name
  2. Raw data JSON scan: Check raw_data blob for developer-like fields
  3. Name-based heuristic: Extract developer from project name patterns

After backfill, re-run developer_registry.py canonicalization.

Usage:
    python3 backfill_developer.py --dry-run    # Preview without writing
    python3 backfill_developer.py --enrich     # Apply backfill
    python3 backfill_developer.py --stats      # Show current gaps
"""

import argparse
import json
import logging
import re
import sqlite3
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

TOOLS_DIR = Path(__file__).parent
MASTER_DB = TOOLS_DIR / '.data' / 'master.db'
EIA_DB = TOOLS_DIR.parent.parent.parent / 'prospector-platform' / 'pipelines' / 'databases' / 'eia.db'

# Fields in raw_data JSON that might contain developer/owner names
DEVELOPER_JSON_FIELDS = [
    'developer', 'Developer', 'developer_name', 'Developer Name',
    'Interconnection Customer', 'interconnection_customer',
    'Project Owner', 'project_owner', 'Owner', 'owner',
    'Applicant', 'applicant', 'applicant_name', 'Applicant Name',
    'entity', 'Entity', 'entity_name',
    'Interconnecting Entity', 'interconnecting_entity',
    'Generation Developer', 'generation_developer',
    'Company Name', 'company_name', 'company',
    'Contact Name', 'contact_name',
    'Requesting Customer', 'requesting_customer',
]

# Patterns to skip (not real developer names)
SKIP_PATTERNS = [
    r'^(unknown|n/a|na|none|tbd|confidential|redacted|not available)$',
    r'^[\d\.\-]+$',  # Pure numbers
    r'^\s*$',         # Blank/whitespace
    r'^PD$',          # LBL placeholder
    r'^NA$',
]
SKIP_RE = re.compile('|'.join(SKIP_PATTERNS), re.IGNORECASE)


def is_valid_developer(name: str) -> bool:
    """Check if a string looks like a real developer name."""
    if not name or not name.strip():
        return False
    name = name.strip()
    if len(name) < 3:
        return False
    if SKIP_RE.match(name):
        return False
    return True


def strategy_eia_ownership(master_conn, dry_run=False):
    """Strategy 1: Backfill developer from EIA-860 ownership table.

    For projects that have plant_id_eia but no developer, look up the
    majority owner from the EIA ownership table.
    """
    logger.info("--- Strategy 1: EIA-860 Ownership Backfill ---")

    if not EIA_DB.exists():
        logger.warning(f"eia.db not found at {EIA_DB}")
        return 0

    # Find projects with EIA match but no developer
    candidates = master_conn.execute("""
        SELECT queue_id, region, plant_id_eia, name
        FROM projects
        WHERE plant_id_eia IS NOT NULL AND plant_id_eia != ''
          AND (developer IS NULL OR developer = '')
    """).fetchall()

    logger.info(f"  Found {len(candidates)} projects with plant_id_eia but no developer")

    if not candidates:
        return 0

    # Connect to EIA and build ownership lookup
    eia_conn = sqlite3.connect(EIA_DB)
    eia_conn.row_factory = sqlite3.Row

    # Get most recent majority owner for each plant
    ownership = {}
    rows = eia_conn.execute("""
        SELECT plant_id_eia, owner_utility_name_eia, fraction_owned, report_date
        FROM ownership
        WHERE fraction_owned >= 0.5
        ORDER BY report_date DESC
    """).fetchall()

    for row in rows:
        pid = str(row['plant_id_eia'])
        if pid not in ownership:  # Keep most recent (sorted DESC)
            ownership[pid] = row['owner_utility_name_eia']

    eia_conn.close()
    logger.info(f"  Loaded {len(ownership)} majority-owner mappings from EIA")

    updated = 0
    for cand in candidates:
        pid = str(cand['plant_id_eia']).split('.')[0]  # Remove decimal
        owner = ownership.get(pid)

        if owner and is_valid_developer(owner):
            if not dry_run:
                master_conn.execute("""
                    UPDATE projects SET developer = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE queue_id = ? AND region = ?
                """, (owner, cand['queue_id'], cand['region']))
            updated += 1
            if updated <= 10:
                logger.info(f"    {cand['queue_id']} ({cand['region']}): → {owner}")

    if not dry_run and updated > 0:
        master_conn.commit()

    logger.info(f"  EIA ownership backfill: {updated} projects updated")
    return updated


def strategy_raw_data_scan(master_conn, dry_run=False):
    """Strategy 2: Scan raw_data JSON blob for developer-like fields.

    Some sources store developer info in fields we don't extract during
    normalization. Check the raw JSON for any developer-like field names.
    """
    logger.info("--- Strategy 2: Raw Data JSON Scan ---")

    candidates = master_conn.execute("""
        SELECT queue_id, region, raw_data, name
        FROM projects
        WHERE (developer IS NULL OR developer = '')
          AND raw_data IS NOT NULL AND raw_data != ''
    """).fetchall()

    logger.info(f"  Scanning {len(candidates)} projects with raw_data but no developer")

    updated = 0
    field_hits = {}  # Track which fields yielded results

    for cand in candidates:
        try:
            raw = json.loads(cand['raw_data'])
        except (json.JSONDecodeError, TypeError):
            continue

        if not isinstance(raw, dict):
            continue

        # Check all known developer-like fields
        for field in DEVELOPER_JSON_FIELDS:
            val = raw.get(field)
            if val and isinstance(val, str) and is_valid_developer(val):
                # Don't use values that are just the project name repeated
                if val.strip().lower() == (cand['name'] or '').strip().lower():
                    continue

                if not dry_run:
                    master_conn.execute("""
                        UPDATE projects SET developer = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE queue_id = ? AND region = ?
                    """, (val.strip(), cand['queue_id'], cand['region']))

                field_hits[field] = field_hits.get(field, 0) + 1
                updated += 1
                if updated <= 10:
                    logger.info(f"    {cand['queue_id']} ({cand['region']}): {field} → {val.strip()}")
                break  # Use first valid field found

    if not dry_run and updated > 0:
        master_conn.commit()

    if field_hits:
        logger.info(f"  Fields that yielded results:")
        for field, count in sorted(field_hits.items(), key=lambda x: -x[1]):
            logger.info(f"    {field}: {count}")

    logger.info(f"  Raw data scan: {updated} projects updated")
    return updated


def strategy_name_heuristic(master_conn, dry_run=False):
    """Strategy 3: Extract developer from project name patterns.

    Many projects are named after their developer:
    - "NextEra Solar Project" → NextEra
    - "Invenergy Wind LLC" → Invenergy Wind
    - "Duke Energy Solar Farm" → Duke Energy

    Only applies when the project name starts with a known developer prefix.
    """
    logger.info("--- Strategy 3: Name-Based Heuristic ---")

    # Load known developers with 10+ projects AND a known tier (real companies)
    known_developers = set()
    rows = master_conn.execute("""
        SELECT developer_canonical, COUNT(*) as cnt
        FROM projects
        WHERE developer_canonical IS NOT NULL AND developer_canonical != ''
          AND developer_tier IN ('major_utility', 'major_ipp', 'mid_platform', 'pe_fund')
        GROUP BY developer_canonical
        HAVING cnt >= 10
    """).fetchall()

    for row in rows:
        known_developers.add(row['developer_canonical'].lower())

    logger.info(f"  Loaded {len(known_developers)} known developers (10+ projects, known tier)")

    # Get projects missing developer
    candidates = master_conn.execute("""
        SELECT queue_id, region, name
        FROM projects
        WHERE (developer IS NULL OR developer = '')
          AND name IS NOT NULL AND name != ''
    """).fetchall()

    logger.info(f"  Checking {len(candidates)} projects for name-based matches")

    updated = 0
    for cand in candidates:
        name = cand['name']
        name_lower = name.lower().strip()

        # Check if project name starts with any known developer
        best_match = None
        best_len = 0

        for dev in known_developers:
            if name_lower.startswith(dev) and len(dev) > best_len:
                # Make sure it's a word boundary (not partial match)
                rest = name_lower[len(dev):]
                if not rest or rest[0] in ' -_/':
                    best_match = dev
                    best_len = len(dev)

        if best_match and best_len >= 8:  # Min 8 chars to avoid false positives
            # Get the canonical form
            canonical_row = master_conn.execute("""
                SELECT developer_canonical
                FROM projects
                WHERE LOWER(developer_canonical) = ?
                LIMIT 1
            """, (best_match,)).fetchone()

            if canonical_row:
                dev_name = canonical_row['developer_canonical']
                if not dry_run:
                    master_conn.execute("""
                        UPDATE projects SET developer = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE queue_id = ? AND region = ?
                    """, (dev_name, cand['queue_id'], cand['region']))
                updated += 1
                if updated <= 10:
                    logger.info(f"    {cand['queue_id']}: '{name}' → {dev_name}")

    if not dry_run and updated > 0:
        master_conn.commit()

    logger.info(f"  Name heuristic: {updated} projects updated")
    return updated


def show_stats(master_conn):
    """Show current developer coverage gaps."""
    print("\n=== Developer Coverage Stats ===\n")

    total = master_conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    no_dev = master_conn.execute(
        "SELECT COUNT(*) FROM projects WHERE developer_canonical IS NULL OR developer_canonical = ''"
    ).fetchone()[0]

    print(f"Total projects: {total:,}")
    print(f"Missing developer_canonical: {no_dev:,} ({100*no_dev/total:.1f}%)")
    print(f"Has developer_canonical: {total - no_dev:,} ({100*(total-no_dev)/total:.1f}%)")

    # By stage
    print("\n--- Missing Developer by Construction Stage ---")
    rows = master_conn.execute("""
        SELECT construction_stage, COUNT(*) as cnt
        FROM projects
        WHERE (developer_canonical IS NULL OR developer_canonical = '')
        GROUP BY construction_stage
        ORDER BY cnt DESC
    """).fetchall()
    for row in rows:
        print(f"  {row['construction_stage'] or 'NULL':20s} {row['cnt']:6,}")

    # By ISO
    print("\n--- Missing Developer by ISO (mid/late/construction only) ---")
    rows = master_conn.execute("""
        SELECT region, COUNT(*) as cnt
        FROM projects
        WHERE (developer_canonical IS NULL OR developer_canonical = '')
          AND construction_stage IN ('mid', 'late', 'construction')
        GROUP BY region
        ORDER BY cnt DESC
    """).fetchall()
    for row in rows:
        print(f"  {row['region']:15s} {row['cnt']:6,}")

    # Backfill potential
    print("\n--- Backfill Potential ---")
    eia_potential = master_conn.execute("""
        SELECT COUNT(*) FROM projects
        WHERE plant_id_eia IS NOT NULL AND plant_id_eia != ''
          AND (developer IS NULL OR developer = '')
    """).fetchone()[0]
    print(f"  EIA ownership backfill candidates: {eia_potential:,}")

    raw_data_potential = master_conn.execute("""
        SELECT COUNT(*) FROM projects
        WHERE (developer IS NULL OR developer = '')
          AND raw_data IS NOT NULL AND raw_data != ''
    """).fetchone()[0]
    print(f"  Raw data scan candidates: {raw_data_potential:,}")

    # Investability impact
    print("\n--- Investability Impact ---")
    investable = master_conn.execute(
        "SELECT COUNT(*) FROM projects WHERE investable = 1"
    ).fetchone()[0]
    unknown_investable = master_conn.execute("""
        SELECT COUNT(*) FROM projects
        WHERE investable = 1 AND (developer_tier = 'unknown' OR developer_tier IS NULL)
    """).fetchone()[0]
    print(f"  Current investable: {investable}")
    print(f"  Investable with unknown developer tier: {unknown_investable}")


def run_canonicalization():
    """Re-run developer_registry canonicalization after backfill."""
    logger.info("\n--- Re-running Developer Canonicalization ---")
    try:
        from developer_registry import DeveloperRegistry
        registry = DeveloperRegistry()
        registry.load_from_database()
        updated, unique = registry.apply_canonicalization()
        logger.info(f"  Canonicalized {updated:,} projects → {unique:,} unique developers")
        return updated, unique
    except Exception as e:
        logger.error(f"  Failed to run canonicalization: {e}")
        return 0, 0


def main():
    parser = argparse.ArgumentParser(description='Backfill developer names for projects missing them')
    parser.add_argument('--enrich', action='store_true', help='Apply backfill to database')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing')
    parser.add_argument('--stats', action='store_true', help='Show current developer coverage gaps')
    args = parser.parse_args()

    if not MASTER_DB.exists():
        logger.error(f"master.db not found at {MASTER_DB}")
        return

    conn = sqlite3.connect(MASTER_DB)
    conn.row_factory = sqlite3.Row

    if args.stats:
        show_stats(conn)
        conn.close()
        return

    if not args.enrich and not args.dry_run:
        parser.print_help()
        conn.close()
        return

    dry_run = args.dry_run
    mode = "DRY RUN" if dry_run else "ENRICH"
    logger.info(f"=== Developer Backfill ({mode}) ===\n")

    # Show before stats
    before_no_dev = conn.execute(
        "SELECT COUNT(*) FROM projects WHERE developer IS NULL OR developer = ''"
    ).fetchone()[0]
    before_no_canonical = conn.execute(
        "SELECT COUNT(*) FROM projects WHERE developer_canonical IS NULL OR developer_canonical = ''"
    ).fetchone()[0]

    # Run strategies
    s1 = strategy_eia_ownership(conn, dry_run)
    s2 = strategy_raw_data_scan(conn, dry_run)
    s3 = strategy_name_heuristic(conn, dry_run)

    total = s1 + s2 + s3
    logger.info(f"\n=== Summary ===")
    logger.info(f"  Strategy 1 (EIA ownership): {s1}")
    logger.info(f"  Strategy 2 (raw data scan): {s2}")
    logger.info(f"  Strategy 3 (name heuristic): {s3}")
    logger.info(f"  Total backfilled: {total}")

    if not dry_run and total > 0:
        # Re-run canonicalization
        run_canonicalization()

        # Show after stats
        after_no_dev = conn.execute(
            "SELECT COUNT(*) FROM projects WHERE developer IS NULL OR developer = ''"
        ).fetchone()[0]
        after_no_canonical = conn.execute(
            "SELECT COUNT(*) FROM projects WHERE developer_canonical IS NULL OR developer_canonical = ''"
        ).fetchone()[0]

        logger.info(f"\n  Before: {before_no_dev:,} missing developer, {before_no_canonical:,} missing canonical")
        logger.info(f"  After:  {after_no_dev:,} missing developer, {after_no_canonical:,} missing canonical")
        logger.info(f"  Improvement: {before_no_dev - after_no_dev:,} developers filled, "
                    f"{before_no_canonical - after_no_canonical:,} canonicals filled")

    conn.close()


if __name__ == '__main__':
    main()
