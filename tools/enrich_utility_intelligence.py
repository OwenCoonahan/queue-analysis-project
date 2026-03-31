#!/usr/bin/env python3
"""
Utility Intelligence Enrichment for master.db
Dev2, 2026-03-23

Enriches projects that already have utility_id_eia (86.3%, set by Dev5) with:
- Net metering context (from eia.db net_metering): capacity, customers
- Distributed generation context (from eia.db distributed_generation): capacity
- FERC financials (from ferc.db via utility_id_pudl bridge): rate base, revenue, net income
- Utility name (from eia.db utility_sales)

New columns added to master.db projects:
  utility_name               — Utility name from EIA
  utility_net_metering_mw    — Total net metering capacity (MW)
  utility_net_metering_customers — Total net metering customers
  utility_dg_capacity_mw     — Total distributed generation capacity (MW)
  utility_rate_base          — FERC Form 1 total rate base ($)
  utility_revenue            — FERC Form 1 electric operating revenue ($)
  utility_net_income         — FERC Form 1 net income ($)

Existing columns (Dev5): utility_id_eia, utility_total_customers, utility_total_sales_mwh, utility_total_revenue

Usage:
    python3 enrich_utility_intelligence.py           # Run enrichment
    python3 enrich_utility_intelligence.py --stats   # Show current coverage
    python3 enrich_utility_intelligence.py --dry-run # Preview without saving
"""

import argparse
import shutil
import sqlite3
import sys
import time
from pathlib import Path

# Paths
TOOLS_DIR = Path(__file__).parent
MASTER_DB = TOOLS_DIR / '.data' / 'master.db'
EIA_DB = TOOLS_DIR.parent.parent.parent / 'prospector-platform' / 'pipelines' / 'databases' / 'eia.db'
FERC_DB = TOOLS_DIR.parent.parent.parent / 'prospector-platform' / 'pipelines' / 'databases' / 'ferc.db'

NEW_COLUMNS = [
    ("utility_name", "TEXT"),
    ("utility_net_metering_mw", "REAL"),
    ("utility_net_metering_customers", "INTEGER"),
    ("utility_dg_capacity_mw", "REAL"),
    ("utility_rate_base", "REAL"),
    ("utility_revenue", "REAL"),
    ("utility_net_income", "REAL"),
]


def add_columns(conn):
    """Add new enrichment columns if they don't exist."""
    for col_name, col_type in NEW_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE projects ADD COLUMN {col_name} {col_type}")
            print(f"  Added column: {col_name}")
        except sqlite3.OperationalError:
            pass  # Already exists


def build_eia_lookup(eia_conn):
    """Build lookup dicts from eia.db, keyed by utility_id_eia."""
    print("\n  Loading EIA reference data...")

    # Utility name — from utility_sales, most recent year, pick the name
    utility_names = {}
    rows = eia_conn.execute("""
        SELECT utility_id_eia, MAX(report_date) as latest
        FROM utility_sales
        WHERE utility_id_eia IS NOT NULL
        GROUP BY utility_id_eia
    """).fetchall()
    # We need the actual name — get it from service_territories or generators
    # utility_sales doesn't have name. Try plants_pudl.
    name_rows = eia_conn.execute("""
        SELECT DISTINCT utility_id_eia, utility_name_eia
        FROM plants_pudl
        WHERE utility_id_eia IS NOT NULL AND utility_name_eia IS NOT NULL
    """).fetchall()
    for uid, name in name_rows:
        utility_names[int(uid)] = name
    print(f"    Utility names: {len(utility_names)}")

    # Net metering — aggregate most recent year per utility
    # Use latest report_date per utility for totals
    nm_data = {}
    nm_rows = eia_conn.execute("""
        SELECT utility_id_eia,
               SUM(COALESCE(capacity_mw, 0)) as total_capacity,
               SUM(COALESCE(customers, 0)) as total_customers
        FROM net_metering
        WHERE utility_id_eia IS NOT NULL
          AND report_date = (SELECT MAX(report_date) FROM net_metering)
        GROUP BY utility_id_eia
        HAVING total_capacity > 0 OR total_customers > 0
    """).fetchall()
    for uid, cap, cust in nm_rows:
        nm_data[int(uid)] = (round(cap, 2), int(cust))
    print(f"    Net metering utilities: {len(nm_data)}")

    # Distributed generation — aggregate latest year
    dg_data = {}
    dg_rows = eia_conn.execute("""
        SELECT utility_id_eia, SUM(COALESCE(capacity_mw, 0)) as total_dg
        FROM distributed_generation
        WHERE utility_id_eia IS NOT NULL
          AND report_date = (SELECT MAX(report_date) FROM distributed_generation)
        GROUP BY utility_id_eia
        HAVING total_dg > 0
    """).fetchall()
    for uid, dg in dg_rows:
        dg_data[int(uid)] = round(dg, 2)
    print(f"    DG utilities: {len(dg_data)}")

    # EIA → PUDL bridge (for FERC lookup)
    eia_to_pudl = {}
    bridge_rows = eia_conn.execute("""
        SELECT DISTINCT CAST(utility_id_eia AS INTEGER), CAST(utility_id_pudl AS INTEGER)
        FROM plants_pudl
        WHERE utility_id_eia IS NOT NULL AND utility_id_pudl IS NOT NULL
    """).fetchall()
    for eia_id, pudl_id in bridge_rows:
        eia_to_pudl[eia_id] = pudl_id
    print(f"    EIA→PUDL bridge: {len(eia_to_pudl)} utilities")

    return utility_names, nm_data, dg_data, eia_to_pudl


def build_ferc_lookup(ferc_conn):
    """Build FERC financial lookup, keyed by utility_id_pudl."""
    print("\n  Loading FERC financial data...")

    # Use 2022 as latest reliable year (2023/2024 may be partial)
    # Try 2024 first, fall back to 2022
    latest_year = ferc_conn.execute(
        "SELECT MAX(report_year) FROM rate_base WHERE utility_type = 'electric'"
    ).fetchone()[0]
    print(f"    Using FERC year: {latest_year}")

    # Rate base — sum all ending_balance for electric utilities
    rate_base = {}
    rb_rows = ferc_conn.execute("""
        SELECT utility_id_pudl, SUM(ending_balance) as total
        FROM rate_base
        WHERE utility_type = 'electric' AND report_year = ?
        GROUP BY utility_id_pudl
        HAVING total IS NOT NULL
    """, (latest_year,)).fetchall()
    for pudl_id, total in rb_rows:
        rate_base[pudl_id] = round(total, 2)
    print(f"    Rate base utilities: {len(rate_base)}")

    # Operating revenue — sum electric operating revenues
    revenue = {}
    rev_rows = ferc_conn.execute("""
        SELECT utility_id_pudl, SUM(dollar_value) as total
        FROM operating_revenues
        WHERE utility_type = 'electric' AND report_year = ?
        GROUP BY utility_id_pudl
        HAVING total IS NOT NULL
    """, (latest_year,)).fetchall()
    for pudl_id, total in rev_rows:
        revenue[pudl_id] = round(total, 2)
    print(f"    Revenue utilities: {len(revenue)}")

    # Net income — sum entries with income_type = 'net_income'
    net_income = {}
    inc_rows = ferc_conn.execute("""
        SELECT utility_id_pudl, SUM(dollar_value) as total
        FROM income_statements
        WHERE utility_type = 'electric' AND report_year = ?
          AND income_type IN ('net_income', 'net_income_loss', 'net_utility_operating_income')
        GROUP BY utility_id_pudl
        HAVING total IS NOT NULL
    """, (latest_year,)).fetchall()
    for pudl_id, total in inc_rows:
        net_income[pudl_id] = round(total, 2)
    print(f"    Net income utilities: {len(net_income)}")

    return rate_base, revenue, net_income, latest_year


def enrich(save=True):
    """Run the full utility intelligence enrichment."""
    if not MASTER_DB.exists():
        print(f"ERROR: master.db not found at {MASTER_DB}")
        sys.exit(1)
    if not EIA_DB.exists():
        print(f"ERROR: eia.db not found at {EIA_DB}")
        sys.exit(1)
    if not FERC_DB.exists():
        print(f"ERROR: ferc.db not found at {FERC_DB}")
        sys.exit(1)

    # Backup
    if save:
        backup = MASTER_DB.parent / 'master_pre_utility_intel.db'
        if not backup.exists():
            print(f"  Backing up to {backup.name}...")
            shutil.copy2(MASTER_DB, backup)

    # Open connections
    master = sqlite3.connect(str(MASTER_DB))
    master.execute("PRAGMA journal_mode=WAL")
    master.execute("PRAGMA synchronous=NORMAL")

    eia = sqlite3.connect(str(EIA_DB))
    ferc = sqlite3.connect(str(FERC_DB))

    # Add columns
    print("\nAdding enrichment columns...")
    add_columns(master)

    # Build lookups
    utility_names, nm_data, dg_data, eia_to_pudl = build_eia_lookup(eia)
    ferc_rate_base, ferc_revenue, ferc_net_income, ferc_year = build_ferc_lookup(ferc)

    eia.close()
    ferc.close()

    # Get projects with utility_id_eia
    projects = master.execute("""
        SELECT id, utility_id_eia FROM projects
        WHERE utility_id_eia IS NOT NULL
    """).fetchall()
    print(f"\n  Projects with utility_id_eia: {len(projects)}")

    # Enrich
    stats = {
        'name': 0, 'nm': 0, 'dg': 0,
        'rate_base': 0, 'revenue': 0, 'net_income': 0,
    }
    batch = []

    for i, (proj_id, uid_raw) in enumerate(projects):
        if (i + 1) % 10000 == 0:
            print(f"  Progress: {i+1}/{len(projects)}")
            if batch and save:
                master.executemany("""
                    UPDATE projects SET
                        utility_name = ?,
                        utility_net_metering_mw = ?,
                        utility_net_metering_customers = ?,
                        utility_dg_capacity_mw = ?,
                        utility_rate_base = ?,
                        utility_revenue = ?,
                        utility_net_income = ?
                    WHERE id = ?
                """, batch)
                master.commit()
                batch = []

        uid = int(uid_raw) if uid_raw else None
        if uid is None:
            continue

        # Utility name
        name = utility_names.get(uid)
        if name:
            stats['name'] += 1

        # Net metering
        nm = nm_data.get(uid)
        nm_cap = nm[0] if nm else None
        nm_cust = nm[1] if nm else None
        if nm:
            stats['nm'] += 1

        # DG
        dg_cap = dg_data.get(uid)
        if dg_cap:
            stats['dg'] += 1

        # FERC via PUDL bridge
        pudl_id = eia_to_pudl.get(uid)
        rb = ferc_rate_base.get(pudl_id) if pudl_id else None
        rev = ferc_revenue.get(pudl_id) if pudl_id else None
        ni = ferc_net_income.get(pudl_id) if pudl_id else None
        if rb:
            stats['rate_base'] += 1
        if rev:
            stats['revenue'] += 1
        if ni:
            stats['net_income'] += 1

        batch.append((name, nm_cap, nm_cust, dg_cap, rb, rev, ni, proj_id))

    # Flush remaining
    if batch and save:
        master.executemany("""
            UPDATE projects SET
                utility_name = ?,
                utility_net_metering_mw = ?,
                utility_net_metering_customers = ?,
                utility_dg_capacity_mw = ?,
                utility_rate_base = ?,
                utility_revenue = ?,
                utility_net_income = ?
            WHERE id = ?
        """, batch)
        master.commit()

    master.close()

    print(f"\n{'='*60}")
    print(f"Enrichment Results ({len(projects)} projects with utility_id_eia)")
    print(f"{'='*60}")
    for key, count in stats.items():
        pct = 100 * count / max(len(projects), 1)
        print(f"  {key:30s} {count:>8,} ({pct:.1f}%)")
    print(f"\n  FERC data year: {ferc_year}")

    return stats


def show_stats():
    """Show current utility intelligence coverage."""
    if not MASTER_DB.exists():
        print(f"Database not found: {MASTER_DB}")
        return

    conn = sqlite3.connect(str(MASTER_DB))
    total = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    existing_cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]

    print(f"\nUtility Intelligence Coverage ({total} projects)")
    print("=" * 60)

    all_cols = [
        'utility_id_eia', 'utility_total_customers', 'utility_total_sales_mwh',
        'utility_total_revenue', 'utility_name',
        'utility_net_metering_mw', 'utility_net_metering_customers',
        'utility_dg_capacity_mw', 'utility_rate_base', 'utility_revenue', 'utility_net_income',
    ]

    for col in all_cols:
        if col not in existing_cols:
            print(f"  {col:40s} NOT ADDED")
            continue
        n = conn.execute(f"SELECT COUNT(*) FROM projects WHERE {col} IS NOT NULL").fetchone()[0]
        pct = 100 * n / max(total, 1)
        print(f"  {col:40s} {n:>8,} ({pct:.1f}%)")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description='Utility Intelligence enrichment for master.db')
    parser.add_argument('--stats', action='store_true', help='Show current coverage')
    parser.add_argument('--dry-run', action='store_true', help='Preview without saving')
    args = parser.parse_args()

    if args.stats:
        show_stats()
        return

    save = not args.dry_run
    if args.dry_run:
        print("*** DRY RUN — no changes will be saved ***")

    print("=" * 60)
    print("Utility Intelligence Enrichment")
    print(f"master.db: {MASTER_DB}")
    print(f"eia.db: {EIA_DB}")
    print(f"ferc.db: {FERC_DB}")
    print(f"Save: {save}")
    print("=" * 60)

    start = time.time()
    enrich(save=save)
    elapsed = time.time() - start

    print(f"\nDONE in {elapsed:.1f}s")
    show_stats()


if __name__ == '__main__':
    main()
