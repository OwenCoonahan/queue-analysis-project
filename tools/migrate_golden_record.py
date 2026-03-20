#!/usr/bin/env python3
"""
Migrate queue.db from duplicated source rows to golden records.

Before: 54K rows (one row per source per project, UNIQUE on queue_id+region+source)
After:  ~36K rows (one row per project, best value from each source, provenance tracked)

Usage:
    python3 migrate_golden_record.py --dry-run     # Preview without changing anything
    python3 migrate_golden_record.py               # Run the migration
    python3 migrate_golden_record.py --rollback     # Restore from backup

The migration:
1. Backs up queue.db → queue_pre_golden.db
2. Groups all rows by (queue_id, region)
3. For each group, merges into one golden record using source priority
4. Creates project_sources table for provenance
5. Rewrites the projects table with deduplicated data
6. Preserves all other tables (snapshots, changes, refresh_log, etc.)
"""

import sqlite3
import json
import hashlib
import shutil
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict

DB_PATH = Path(__file__).parent / ".data" / "queue.db"
BACKUP_PATH = Path(__file__).parent / ".data" / "queue_pre_golden.db"

# ──────────────────────────────────────────────
# Source Priority (lower number = higher priority)
# ──────────────────────────────────────────────

SOURCE_PRIORITY = {
    # Tier 1: Live ISO (freshest queue data)
    "pjm_direct": 10,
    "miso_api": 10,
    "nyiso_direct": 10,
    "ercot": 10,
    "caiso": 10,
    "spp": 10,
    "isone": 10,
    # Tier 2: Federal verified
    "eia_860": 20,
    "eia_860m": 20,
    # Tier 3: Historical reference
    "lbl": 30,
    # Tier 4: Legacy/other
    "nyiso": 40,
}

# Field-level priority overrides
# Some fields are better from specific source types
FIELD_SOURCE_PREFERENCE = {
    # Developer: LBL/EIA have the best coverage
    "developer": lambda sources: _prefer_non_null(sources, prefer_tiers=[30, 20, 10]),
    # Queue date: prefer ISO sources (LBL has Excel serial numbers)
    "queue_date": lambda sources: _prefer_valid_date(sources, prefer_tiers=[10, 20, 30]),
    # Status: prefer live ISO (most current)
    "status": lambda sources: _prefer_non_null(sources, prefer_tiers=[10, 20, 30]),
    # Capacity: prefer live ISO (most current)
    "capacity_mw": lambda sources: _prefer_non_null(sources, prefer_tiers=[10, 20, 30]),
    # Name: prefer the longer/more descriptive name
    "name": lambda sources: _prefer_longest(sources),
}


def _get_tier(source: str) -> int:
    return SOURCE_PRIORITY.get(source, 50)


def _prefer_non_null(sources: list, prefer_tiers: list) -> tuple:
    """Pick the best non-null value, preferring sources in tier order."""
    for tier in prefer_tiers:
        for source, value in sources:
            if value and str(value).strip() and _get_tier(source) == tier:
                return source, value
    # Fallback: any non-null value
    for source, value in sources:
        if value and str(value).strip():
            return source, value
    return sources[0] if sources else (None, None)


def _prefer_valid_date(sources: list, prefer_tiers: list) -> tuple:
    """Pick a valid date string, preferring ISO sources."""
    for tier in prefer_tiers:
        for source, value in sources:
            if value and _get_tier(source) == tier:
                v = str(value).strip()
                # Skip Excel serial numbers (pure floats like 35521.0)
                if v and not _is_excel_serial(v):
                    return source, v
    # Fallback: any value that's not an Excel serial
    for source, value in sources:
        if value:
            v = str(value).strip()
            if v and not _is_excel_serial(v):
                return source, v
    return sources[0] if sources else (None, None)


def _is_excel_serial(v: str) -> bool:
    """Check if a value looks like an Excel serial date number."""
    try:
        f = float(v)
        return 10000 < f < 100000  # Excel dates are typically 30000-50000
    except (ValueError, TypeError):
        return False


def _prefer_longest(sources: list) -> tuple:
    """Pick the longest non-null string value."""
    best_source, best_value = None, ""
    for source, value in sources:
        if value and len(str(value)) > len(str(best_value)):
            best_source, best_value = source, value
    if best_source:
        return best_source, best_value
    return sources[0] if sources else (None, None)


def _default_merge(sources: list) -> tuple:
    """Default: pick from highest-priority source with non-null value."""
    sorted_sources = sorted(sources, key=lambda x: _get_tier(x[0]))
    for source, value in sorted_sources:
        if value and str(value).strip():
            return source, value
    return sorted_sources[0] if sorted_sources else (None, None)


CORE_FIELDS = [
    "name", "developer", "capacity_mw", "type", "status",
    "state", "county", "poi", "queue_date", "cod",
]

ENRICHMENT_FIELDS = [
    "tax_credit_type", "recommended_credit", "effective_credit_rate",
    "estimated_credit_value", "energy_community_eligible", "energy_community_type",
    "low_income_eligible", "low_income_type",
]


def merge_group(rows: list) -> dict:
    """
    Merge multiple source rows for the same project into one golden record.

    Args:
        rows: list of dicts, each from a different source for the same (queue_id, region)

    Returns:
        dict with merged golden record + provenance metadata
    """
    if len(rows) == 1:
        record = dict(rows[0])
        record["sources"] = json.dumps([record["source"]])
        record["primary_source"] = record["source"]
        return record

    # Sort by priority (best source first)
    rows_sorted = sorted(rows, key=lambda r: _get_tier(r.get("source", "")))

    # Start with the highest-priority row as base
    golden = {
        "queue_id": rows_sorted[0]["queue_id"],
        "region": rows_sorted[0]["region"],
    }

    # Merge each core field using field-specific or default logic
    source_details = {}
    for field in CORE_FIELDS:
        sources_for_field = [
            (r["source"], r.get(field))
            for r in rows_sorted
            if r.get(field) is not None
        ]
        if not sources_for_field:
            golden[field] = None
            continue

        merge_fn = FIELD_SOURCE_PREFERENCE.get(field, _default_merge)
        winning_source, winning_value = merge_fn(sources_for_field)
        golden[field] = winning_value
        if winning_source:
            source_details[field] = winning_source

    # Enrichment fields: take any non-null value (these are computed, not source-specific)
    for field in ENRICHMENT_FIELDS:
        for r in rows_sorted:
            val = r.get(field)
            if val is not None and str(val).strip():
                golden[field] = val
                break
        else:
            golden[field] = None

    # Provenance
    all_sources = sorted(set(r["source"] for r in rows_sorted))
    golden["sources"] = json.dumps(all_sources)
    golden["primary_source"] = rows_sorted[0]["source"]  # Highest priority source
    golden["source"] = golden["primary_source"]  # Keep for backward compat

    # Timestamps: earliest created, latest updated
    created_dates = [r.get("created_at") for r in rows_sorted if r.get("created_at")]
    updated_dates = [r.get("updated_at") for r in rows_sorted if r.get("updated_at")]
    golden["created_at"] = min(created_dates) if created_dates else datetime.now().isoformat()
    golden["updated_at"] = max(updated_dates) if updated_dates else datetime.now().isoformat()

    # Row hash of merged record
    hash_fields = [str(golden.get(f, "")) for f in CORE_FIELDS]
    golden["row_hash"] = hashlib.md5("|".join(hash_fields).encode()).hexdigest()

    return golden


def run_migration(db_path: Path, dry_run: bool = False):
    """Execute the golden record migration."""
    print("=" * 60)
    print("GOLDEN RECORD MIGRATION")
    print(f"Database: {db_path}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print("=" * 60)
    print()

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Get all columns from projects table
    col_info = conn.execute("PRAGMA table_info(projects)").fetchall()
    all_columns = [c[1] for c in col_info]
    print(f"Projects table columns: {len(all_columns)}")

    # Load all projects
    rows = conn.execute("SELECT * FROM projects").fetchall()
    total_rows = len(rows)
    print(f"Total rows: {total_rows:,}")

    # Group by (queue_id, region)
    groups = defaultdict(list)
    for row in rows:
        key = (row["queue_id"], row["region"])
        groups[key].append(dict(row))

    unique_projects = len(groups)
    duplicated = total_rows - unique_projects
    multi_source = sum(1 for g in groups.values() if len(g) > 1)

    print(f"Unique projects: {unique_projects:,}")
    print(f"Duplicated rows: {duplicated:,} ({duplicated/total_rows*100:.1f}%)")
    print(f"Multi-source projects: {multi_source:,}")
    print()

    # Merge each group
    golden_records = []
    source_provenance = []  # For project_sources table

    for (queue_id, region), group_rows in groups.items():
        golden = merge_group(group_rows)
        golden_records.append(golden)

        # Track which sources contributed
        for r in group_rows:
            source_provenance.append({
                "queue_id": queue_id,
                "region": region,
                "source": r["source"],
                "updated_at": r.get("updated_at"),
            })

    print(f"Golden records: {len(golden_records):,}")
    print(f"Source provenance entries: {len(source_provenance):,}")

    # Stats on merge results
    sources_used = defaultdict(int)
    for g in golden_records:
        sources_used[g.get("primary_source", "unknown")] += 1

    print("\nPrimary source distribution:")
    for source, count in sorted(sources_used.items(), key=lambda x: -x[1]):
        print(f"  {source:<20} {count:>6,} projects")

    # Check enrichment preservation
    enriched = sum(1 for g in golden_records if g.get("tax_credit_type"))
    print(f"\nEnriched with tax credits: {enriched:,}")

    if dry_run:
        print("\n[DRY RUN] No changes made.")
        conn.close()
        return

    # ── BACKUP ──
    print(f"\nBacking up to {BACKUP_PATH}...")
    conn.close()
    shutil.copy2(str(db_path), str(BACKUP_PATH))
    print(f"  Backup: {BACKUP_PATH} ({BACKUP_PATH.stat().st_size / 1024 / 1024:.1f} MB)")

    # ── MIGRATE ──
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Add new columns if they don't exist
    existing_cols = {c[1] for c in cursor.execute("PRAGMA table_info(projects)").fetchall()}

    new_cols = {
        "sources": "TEXT",           # JSON array of all contributing sources
        "primary_source": "TEXT",    # Highest-priority source
    }
    for col_name, col_type in new_cols.items():
        if col_name not in existing_cols:
            cursor.execute(f"ALTER TABLE projects ADD COLUMN {col_name} {col_type}")
            print(f"  Added column: {col_name}")

    # Create project_sources table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS project_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue_id TEXT NOT NULL,
            region TEXT NOT NULL,
            source TEXT NOT NULL,
            updated_at TEXT,
            UNIQUE(queue_id, region, source)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ps_qid ON project_sources(queue_id, region)")

    # Drop the old unique constraint and rebuild
    # SQLite doesn't support DROP CONSTRAINT, so we rebuild the table
    print("\nRebuilding projects table with new unique constraint...")

    # Get current columns (including new ones)
    col_info = cursor.execute("PRAGMA table_info(projects)").fetchall()
    current_cols = [c[1] for c in col_info]

    # Create new table with UNIQUE on (queue_id, region) instead of (queue_id, region, source)
    cursor.execute("ALTER TABLE projects RENAME TO projects_old")

    # Build CREATE TABLE with same columns but new constraint
    col_defs = []
    for c in col_info:
        name, ctype, notnull, default, pk = c[1], c[2], c[3], c[4], c[5]
        if pk:
            col_defs.append(f"{name} INTEGER PRIMARY KEY AUTOINCREMENT")
        elif notnull and default:
            col_defs.append(f"{name} {ctype} NOT NULL DEFAULT {default}")
        elif notnull:
            col_defs.append(f"{name} {ctype} NOT NULL")
        elif default:
            col_defs.append(f"{name} {ctype} DEFAULT {default}")
        else:
            col_defs.append(f"{name} {ctype}")

    # Add the new unique constraint
    col_defs.append("UNIQUE(queue_id, region)")

    create_sql = f"CREATE TABLE projects ({', '.join(col_defs)})"
    cursor.execute(create_sql)

    # Insert golden records
    insert_cols = [c for c in current_cols if c != "id"]
    placeholders = ", ".join(["?" for _ in insert_cols])
    insert_sql = f"INSERT INTO projects ({', '.join(insert_cols)}) VALUES ({placeholders})"

    inserted = 0
    for g in golden_records:
        values = [g.get(c) for c in insert_cols]
        try:
            cursor.execute(insert_sql, values)
            inserted += 1
        except sqlite3.IntegrityError as e:
            # Shouldn't happen, but handle gracefully
            print(f"  WARN: Duplicate skipped: {g.get('queue_id')} / {g.get('region')}: {e}")

    print(f"  Inserted {inserted:,} golden records")

    # Insert source provenance
    for sp in source_provenance:
        cursor.execute("""
            INSERT OR IGNORE INTO project_sources (queue_id, region, source, updated_at)
            VALUES (?, ?, ?, ?)
        """, (sp["queue_id"], sp["region"], sp["source"], sp["updated_at"]))
    print(f"  Inserted {len(source_provenance):,} source provenance entries")

    # Drop old table
    cursor.execute("DROP TABLE projects_old")

    # Recreate indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_projects_region ON projects(region)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_projects_developer ON projects(developer)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_projects_queue_id ON projects(queue_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_projects_source ON projects(source)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_projects_sources ON projects(sources)")

    conn.commit()

    # Verify
    new_total = cursor.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    source_total = cursor.execute("SELECT COUNT(*) FROM project_sources").fetchone()[0]
    print(f"\n{'=' * 60}")
    print(f"MIGRATION COMPLETE")
    print(f"  Before: {total_rows:,} rows")
    print(f"  After:  {new_total:,} rows")
    print(f"  Reduced by: {total_rows - new_total:,} ({(total_rows - new_total)/total_rows*100:.1f}%)")
    print(f"  Source provenance: {source_total:,} entries")
    print(f"  Backup: {BACKUP_PATH}")
    print(f"{'=' * 60}")

    conn.close()


def rollback(db_path: Path):
    """Restore from backup."""
    if not BACKUP_PATH.exists():
        print(f"No backup found at {BACKUP_PATH}")
        return
    print(f"Restoring {db_path} from {BACKUP_PATH}...")
    shutil.copy2(str(BACKUP_PATH), str(db_path))
    print("Restored.")


def main():
    parser = argparse.ArgumentParser(description="Migrate queue.db to golden record format")
    parser.add_argument("--dry-run", action="store_true", help="Preview without making changes")
    parser.add_argument("--rollback", action="store_true", help="Restore from backup")
    parser.add_argument("--db", type=str, help="Path to database (default: .data/queue.db)")
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else DB_PATH

    if args.rollback:
        rollback(db_path)
    else:
        run_migration(db_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
