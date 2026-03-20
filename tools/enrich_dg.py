#!/usr/bin/env python3
"""
DG Database Enrichment Script.

Enriches dg.db (799K distributed generation records) with:
- type_std / status_std standardization
- Energy community eligibility
- Tax credit eligibility (ITC/PTC)
- Low-income community eligibility

Imports checker classes from the existing enrichment tools.
Does NOT modify master.db or the enrichment tools themselves.

Usage:
    python3 enrich_dg.py              # Full enrichment
    python3 enrich_dg.py --dry-run    # Preview without saving
    python3 enrich_dg.py --stats      # Show current enrichment stats
"""

import sqlite3
import sys
import time
from pathlib import Path
from datetime import datetime

# Paths
TOOLS_DIR = Path(__file__).parent
DG_DB_PATH = TOOLS_DIR / '.data' / 'dg.db'

# Import checker classes from existing tools
sys.path.insert(0, str(TOOLS_DIR))
from energy_community import EnergyCommunityChecker
from tax_credits import TaxCreditEngine
from low_income_community import LowIncomeChecker


# =============================================================================
# Type/Status Standardization Maps
# =============================================================================

# DG types map directly — they're already clean
TYPE_STD_MAP = {
    'Solar': 'Solar',
    'Solar+Storage': 'Solar+Storage',
    'Storage': 'Storage',
    'Gas': 'Gas',
    'Wind': 'Wind',
    'Hydro': 'Hydro',
}

# DG statuses map directly
STATUS_STD_MAP = {
    'Operational': 'Operational',
    'Active': 'Active',
    'Withdrawn': 'Withdrawn',
}


# =============================================================================
# Enrichment Columns
# =============================================================================

ENRICHMENT_COLUMNS = [
    ("type_std", "TEXT"),
    ("status_std", "TEXT"),
    ("energy_community_eligible", "INTEGER"),
    ("energy_community_type", "TEXT"),
    ("tax_credit_type", "TEXT"),
    ("recommended_credit", "TEXT"),
    ("base_credit_rate", "REAL"),
    ("effective_credit_rate", "REAL"),
    ("estimated_credit_value", "REAL"),
    ("low_income_eligible", "INTEGER"),
    ("low_income_type", "TEXT"),
]


def add_columns(conn):
    """Add enrichment columns if they don't exist."""
    for col_name, col_type in ENRICHMENT_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE projects ADD COLUMN {col_name} {col_type}")
            print(f"  Added column: {col_name}")
        except sqlite3.OperationalError:
            pass  # Already exists


def enrich_type_status(conn, save=True):
    """Standardize type and status fields."""
    print("\n" + "=" * 60)
    print("Step 1: Type & Status Standardization")
    print("=" * 60)

    # type_std
    type_updated = 0
    for raw, std in TYPE_STD_MAP.items():
        cursor = conn.execute(
            "UPDATE projects SET type_std = ? WHERE type = ? AND (type_std IS NULL OR type_std != ?)",
            (std, raw, std)
        )
        type_updated += cursor.rowcount

    # status_std
    status_updated = 0
    for raw, std in STATUS_STD_MAP.items():
        cursor = conn.execute(
            "UPDATE projects SET status_std = ? WHERE status = ? AND (status_std IS NULL OR status_std != ?)",
            (std, raw, std)
        )
        status_updated += cursor.rowcount

    if save:
        conn.commit()

    print(f"  type_std updated: {type_updated:,}")
    print(f"  status_std updated: {status_updated:,}")
    return type_updated, status_updated


def enrich_energy_community(conn, save=True):
    """Run energy community checker against all DG projects with county data."""
    print("\n" + "=" * 60)
    print("Step 2: Energy Community Eligibility")
    print("=" * 60)

    checker = EnergyCommunityChecker()
    checker.load_data()

    # Get projects with state+county (needed for EC lookup)
    cursor = conn.execute("""
        SELECT id, state, county FROM projects
        WHERE state IS NOT NULL AND state != ''
          AND county IS NOT NULL AND county != ''
    """)
    projects = cursor.fetchall()
    print(f"  Projects with state+county: {len(projects):,}")

    # Cache lookups by (state, county) — most DG projects share the same county
    location_cache = {}
    eligible_count = 0
    coal_count = 0
    ffe_count = 0
    batch = []

    for i, (proj_id, state, county) in enumerate(projects):
        if (i + 1) % 10000 == 0:
            print(f"  Progress: {i+1:,}/{len(projects):,} ({eligible_count:,} eligible)")

        cache_key = (state.upper(), county)
        if cache_key not in location_cache:
            location_cache[cache_key] = checker.check_location(state, county)
        result = location_cache[cache_key]

        ec_eligible = 1 if result.is_energy_community else 0
        ec_type = None
        if result.is_energy_community:
            eligible_count += 1
            ec_types = []
            if result.coal_closure:
                ec_types.append('coal_closure')
                coal_count += 1
            if result.ffe_qualified:
                ec_types.append('ffe')
                ffe_count += 1
            ec_type = ','.join(ec_types) if ec_types else None

        batch.append((ec_eligible, ec_type, proj_id))

        # Batch commit every 5000
        if len(batch) >= 5000:
            if save:
                conn.executemany(
                    "UPDATE projects SET energy_community_eligible = ?, energy_community_type = ? WHERE id = ?",
                    batch
                )
                conn.commit()
            batch = []

    # Flush remaining
    if batch and save:
        conn.executemany(
            "UPDATE projects SET energy_community_eligible = ?, energy_community_type = ? WHERE id = ?",
            batch
        )
        conn.commit()

    # Set NULL for projects without county
    if save:
        conn.execute("""
            UPDATE projects SET energy_community_eligible = 0
            WHERE energy_community_eligible IS NULL
              AND (county IS NULL OR county = '')
        """)
        conn.commit()

    print(f"\n  Unique locations checked: {len(location_cache):,}")
    print(f"  Eligible: {eligible_count:,} ({100*eligible_count/max(len(projects),1):.1f}%)")
    print(f"    Coal closure: {coal_count:,}")
    print(f"    FFE: {ffe_count:,}")
    return eligible_count


def enrich_tax_credits(conn, save=True):
    """Run tax credit engine against all DG projects."""
    print("\n" + "=" * 60)
    print("Step 3: Tax Credit Eligibility")
    print("=" * 60)

    engine = TaxCreditEngine()

    cursor = conn.execute("""
        SELECT id, type, capacity_mw, state, county, cod
        FROM projects
    """)
    projects = cursor.fetchall()
    print(f"  Processing {len(projects):,} projects...")

    stats = {'eligible': 0, 'itc': 0, 'ptc': 0, 'no_tech': 0, 'not_eligible': 0}
    batch = []

    for i, (proj_id, tech, cap_mw, state, county, cod) in enumerate(projects):
        if (i + 1) % 10000 == 0:
            print(f"  Progress: {i+1:,}/{len(projects):,} ({stats['eligible']:,} eligible)")

        technology = tech or ''
        capacity_mw = cap_mw or 0
        state = state or ''
        county = county or ''

        # Parse COD year
        cod_year = None
        if cod:
            try:
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%Y']:
                    try:
                        cod_year = datetime.strptime(str(cod)[:10], fmt).year
                        break
                    except ValueError:
                        continue
            except (ValueError, TypeError):
                pass

        result = engine.calculate(
            technology=technology,
            capacity_mw=capacity_mw,
            state=state,
            county=county,
            cod_year=cod_year,
            prevailing_wage=True,  # DG <1MW exempt from PW&A
        )

        # Determine tax_credit_type
        if result.can_elect_itc and result.can_elect_ptc:
            tax_credit_type = 'both'
        elif result.can_elect_itc:
            tax_credit_type = 'itc'
        elif result.can_elect_ptc:
            tax_credit_type = 'ptc'
        else:
            tax_credit_type = None

        if result.can_elect_itc or result.can_elect_ptc:
            stats['eligible'] += 1
            if result.recommended_credit == 'itc':
                stats['itc'] += 1
            elif result.recommended_credit == 'ptc':
                stats['ptc'] += 1
        elif not technology:
            stats['no_tech'] += 1
        else:
            stats['not_eligible'] += 1

        rec_credit = result.recommended_credit if result.recommended_credit != 'none' else None
        base_rate = result.base_itc_rate if result.recommended_credit == 'itc' else result.base_ptc_rate_per_kwh
        eff_rate = result.effective_itc_rate if result.recommended_credit == 'itc' else result.effective_ptc_rate

        batch.append((
            tax_credit_type,
            rec_credit,
            base_rate,
            eff_rate,
            result.recommended_value,
            proj_id,
        ))

        if len(batch) >= 5000:
            if save:
                conn.executemany("""
                    UPDATE projects SET
                        tax_credit_type = ?,
                        recommended_credit = ?,
                        base_credit_rate = ?,
                        effective_credit_rate = ?,
                        estimated_credit_value = ?
                    WHERE id = ?
                """, batch)
                conn.commit()
            batch = []

    if batch and save:
        conn.executemany("""
            UPDATE projects SET
                tax_credit_type = ?,
                recommended_credit = ?,
                base_credit_rate = ?,
                effective_credit_rate = ?,
                estimated_credit_value = ?
            WHERE id = ?
        """, batch)
        conn.commit()

    print(f"\n  Eligible: {stats['eligible']:,} ({100*stats['eligible']/max(len(projects),1):.1f}%)")
    print(f"    ITC recommended: {stats['itc']:,}")
    print(f"    PTC recommended: {stats['ptc']:,}")
    print(f"  Not eligible (fossil): {stats['not_eligible']:,}")
    print(f"  No technology: {stats['no_tech']:,}")
    return stats


def enrich_low_income(conn, save=True):
    """Run low-income community checker against DG projects."""
    print("\n" + "=" * 60)
    print("Step 4: Low-Income Community Eligibility")
    print("=" * 60)

    checker = LowIncomeChecker()
    loaded = checker.load_data()
    if not loaded:
        print("  ERROR: Could not load low-income data. Skipping.")
        return 0

    cursor = conn.execute("""
        SELECT id, state, county FROM projects
        WHERE state IS NOT NULL AND state != ''
          AND county IS NOT NULL AND county != ''
    """)
    projects = cursor.fetchall()
    print(f"  Projects with state+county: {len(projects):,}")

    location_cache = {}
    eligible_count = 0
    batch = []

    for i, (proj_id, state, county) in enumerate(projects):
        if (i + 1) % 10000 == 0:
            print(f"  Progress: {i+1:,}/{len(projects):,} ({eligible_count:,} eligible)")

        cache_key = (state.upper(), county)
        if cache_key not in location_cache:
            location_cache[cache_key] = checker.check_location(state, county)
        result = location_cache[cache_key]

        li_eligible = 1 if result.is_low_income else 0
        li_type = None
        if result.is_low_income:
            eligible_count += 1
            li_types = []
            if result.nmtc_qualified:
                li_types.append('nmtc')
            if result.persistent_poverty:
                li_types.append('ppc')
            if result.cejst_energy:
                li_types.append('cejst_energy')
            li_type = ','.join(li_types) if li_types else None

        batch.append((li_eligible, li_type, proj_id))

        if len(batch) >= 5000:
            if save:
                conn.executemany(
                    "UPDATE projects SET low_income_eligible = ?, low_income_type = ? WHERE id = ?",
                    batch
                )
                conn.commit()
            batch = []

    if batch and save:
        conn.executemany(
            "UPDATE projects SET low_income_eligible = ?, low_income_type = ? WHERE id = ?",
            batch
        )
        conn.commit()

    # Set 0 for projects without county
    if save:
        conn.execute("""
            UPDATE projects SET low_income_eligible = 0
            WHERE low_income_eligible IS NULL
              AND (county IS NULL OR county = '')
        """)
        conn.commit()

    print(f"\n  Unique locations checked: {len(location_cache):,}")
    print(f"  Eligible: {eligible_count:,} ({100*eligible_count/max(len(projects),1):.1f}%)")
    return eligible_count


def show_stats():
    """Show current enrichment stats for dg.db."""
    if not DG_DB_PATH.exists():
        print(f"Database not found: {DG_DB_PATH}")
        return

    conn = sqlite3.connect(DG_DB_PATH)
    total = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]

    print(f"\ndg.db Enrichment Status ({total:,} records)")
    print("=" * 60)

    fields = [
        'type_std', 'status_std', 'energy_community_eligible',
        'energy_community_type', 'tax_credit_type', 'recommended_credit',
        'base_credit_rate', 'effective_credit_rate', 'estimated_credit_value',
        'low_income_eligible', 'low_income_type',
    ]

    # Check which columns exist
    cursor = conn.execute("PRAGMA table_info(projects)")
    existing = [row[1] for row in cursor.fetchall()]

    for f in fields:
        if f not in existing:
            print(f"  {f:<30} NOT ADDED YET")
            continue
        non_null = conn.execute(f"SELECT COUNT(*) FROM projects WHERE {f} IS NOT NULL").fetchone()[0]
        pct = 100 * non_null / max(total, 1)
        print(f"  {f:<30} {non_null:>10,} ({pct:.1f}%)")

    # EC breakdown
    if 'energy_community_eligible' in existing:
        ec_yes = conn.execute("SELECT COUNT(*) FROM projects WHERE energy_community_eligible = 1").fetchone()[0]
        print(f"\n  EC eligible: {ec_yes:,} ({100*ec_yes/max(total,1):.1f}%)")

    if 'tax_credit_type' in existing:
        for row in conn.execute("SELECT tax_credit_type, COUNT(*) FROM projects WHERE tax_credit_type IS NOT NULL GROUP BY tax_credit_type"):
            print(f"  Tax credit type '{row[0]}': {row[1]:,}")

    if 'low_income_eligible' in existing:
        li_yes = conn.execute("SELECT COUNT(*) FROM projects WHERE low_income_eligible = 1").fetchone()[0]
        print(f"  LI eligible: {li_yes:,} ({100*li_yes/max(total,1):.1f}%)")

    conn.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Enrich dg.db with standardization + eligibility data')
    parser.add_argument('--dry-run', action='store_true', help='Preview without saving')
    parser.add_argument('--stats', action='store_true', help='Show current enrichment stats')
    args = parser.parse_args()

    if args.stats:
        show_stats()
        return

    if not DG_DB_PATH.exists():
        print(f"ERROR: dg.db not found at {DG_DB_PATH}")
        sys.exit(1)

    save = not args.dry_run
    if args.dry_run:
        print("*** DRY RUN — no changes will be saved ***")

    print("=" * 60)
    print("DG Database Enrichment")
    print(f"Database: {DG_DB_PATH}")
    print(f"Save: {save}")
    print("=" * 60)

    start = time.time()

    conn = sqlite3.connect(DG_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Add columns
    print("\nAdding enrichment columns...")
    add_columns(conn)

    # Step 1: Type/Status
    enrich_type_status(conn, save=save)

    # Step 2: Energy Community
    enrich_energy_community(conn, save=save)

    # Step 3: Tax Credits
    enrich_tax_credits(conn, save=save)

    # Step 4: Low Income
    enrich_low_income(conn, save=save)

    conn.close()

    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"DONE in {elapsed:.0f}s ({elapsed/60:.1f}m)")
    print(f"{'=' * 60}")

    # Show final stats
    show_stats()


if __name__ == '__main__':
    main()
