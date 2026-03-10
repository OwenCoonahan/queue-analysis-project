#!/usr/bin/env python3
"""
Recovery Script for False Withdrawals

Identifies and recovers projects that were incorrectly marked as 'Withdrawn'
by the auto-sync process when they were actually still active in the ISO data.

This script:
1. Identifies projects where current status = 'Withdrawn' but original (raw_data) status was NOT withdrawn
2. Creates a backup before making changes
3. Restores the original status with full audit trail
4. Adds a status_source field to track data provenance

Usage:
    python recover_false_withdrawals.py --analyze     # Show what would be recovered
    python recover_false_withdrawals.py --recover     # Actually recover (with backup)
    python recover_false_withdrawals.py --export      # Export to CSV for review
"""

import sqlite3
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple
import csv

DATA_DIR = Path(__file__).parent / '.data'
QUEUE_DB = DATA_DIR / 'queue.db'
BACKUP_DIR = DATA_DIR / 'backups'


def get_false_withdrawals(conn: sqlite3.Connection) -> List[Dict]:
    """
    Identify projects that appear to be falsely withdrawn.

    Criteria:
    - Current status is 'Withdrawn' or 'withdrawn'
    - Original status (in raw_data JSON) was NOT withdrawn/cancelled
    - Has valid raw_data
    """
    cursor = conn.execute("""
        SELECT
            id,
            queue_id,
            region,
            name,
            developer,
            capacity_mw,
            status as current_status,
            json_extract(raw_data, '$.status') as original_status,
            source,
            created_at,
            updated_at
        FROM projects
        WHERE status IN ('Withdrawn', 'withdrawn')
        AND json_extract(raw_data, '$.status') IS NOT NULL
        AND json_extract(raw_data, '$.status') NOT LIKE '%ithdr%'
        AND json_extract(raw_data, '$.status') NOT LIKE '%ancel%'
        AND json_extract(raw_data, '$.status') NOT LIKE '%ITHDR%'
        AND json_extract(raw_data, '$.status') NOT LIKE '%ANCEL%'
        ORDER BY capacity_mw DESC
    """)

    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def analyze_false_withdrawals(conn: sqlite3.Connection) -> Dict:
    """Analyze the scope of false withdrawals."""

    projects = get_false_withdrawals(conn)

    # Group by region
    by_region = {}
    for p in projects:
        region = p['region']
        if region not in by_region:
            by_region[region] = {'count': 0, 'mw': 0, 'statuses': {}}
        by_region[region]['count'] += 1
        by_region[region]['mw'] += p['capacity_mw'] or 0

        orig = p['original_status'] or 'unknown'
        by_region[region]['statuses'][orig] = by_region[region]['statuses'].get(orig, 0) + 1

    # Group by original status
    by_status = {}
    for p in projects:
        orig = p['original_status'] or 'unknown'
        if orig not in by_status:
            by_status[orig] = {'count': 0, 'mw': 0}
        by_status[orig]['count'] += 1
        by_status[orig]['mw'] += p['capacity_mw'] or 0

    return {
        'total_count': len(projects),
        'total_mw': sum(p['capacity_mw'] or 0 for p in projects),
        'by_region': by_region,
        'by_status': by_status,
        'projects': projects
    }


def create_backup(db_path: Path) -> Path:
    """Create a timestamped backup of the database."""
    BACKUP_DIR.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = BACKUP_DIR / f'queue_backup_{timestamp}.db'

    shutil.copy2(db_path, backup_path)
    print(f"Created backup: {backup_path}")

    return backup_path


def ensure_audit_columns(conn: sqlite3.Connection):
    """Add status tracking columns if they don't exist."""

    # Check existing columns
    cursor = conn.execute("PRAGMA table_info(projects)")
    existing = {row[1] for row in cursor.fetchall()}

    new_columns = [
        ('status_source', 'TEXT'),  # 'iso_raw', 'sync_inferred', 'manual_verified', 'recovered'
        ('status_recovered_at', 'TEXT'),
        ('status_recovered_from', 'TEXT'),
    ]

    for col_name, col_type in new_columns:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE projects ADD COLUMN {col_name} {col_type}")
            print(f"Added column: {col_name}")

    conn.commit()


def recover_projects(conn: sqlite3.Connection, dry_run: bool = True) -> int:
    """
    Recover falsely withdrawn projects by restoring their original status.

    Args:
        conn: Database connection
        dry_run: If True, don't actually make changes

    Returns:
        Number of projects recovered
    """
    projects = get_false_withdrawals(conn)

    if not projects:
        print("No false withdrawals found to recover.")
        return 0

    if not dry_run:
        ensure_audit_columns(conn)

    recovered = 0
    timestamp = datetime.now().isoformat()

    for p in projects:
        original_status = p['original_status']

        if dry_run:
            print(f"  Would recover: {p['queue_id']} ({p['region']}) - "
                  f"{p['capacity_mw'] or 0:.0f} MW - '{original_status}'")
        else:
            # Restore original status with audit trail
            conn.execute("""
                UPDATE projects
                SET status = ?,
                    status_source = 'recovered',
                    status_recovered_at = ?,
                    status_recovered_from = 'Withdrawn',
                    updated_at = ?
                WHERE id = ?
            """, (original_status, timestamp, timestamp, p['id']))

            # Log the recovery in changes table
            conn.execute("""
                INSERT INTO changes (
                    queue_id, region, detected_at, change_type,
                    field_name, old_value, new_value
                ) VALUES (?, ?, ?, 'recovery', 'status', 'Withdrawn', ?)
            """, (p['queue_id'], p['region'], timestamp, original_status))

        recovered += 1

    if not dry_run:
        conn.commit()

    return recovered


def export_to_csv(conn: sqlite3.Connection, output_path: Path) -> int:
    """Export false withdrawals to CSV for manual review."""

    projects = get_false_withdrawals(conn)

    if not projects:
        print("No false withdrawals to export.")
        return 0

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'queue_id', 'region', 'name', 'developer', 'capacity_mw',
            'current_status', 'original_status', 'source', 'created_at'
        ])
        writer.writeheader()

        for p in projects:
            writer.writerow({
                'queue_id': p['queue_id'],
                'region': p['region'],
                'name': p['name'],
                'developer': p['developer'],
                'capacity_mw': p['capacity_mw'],
                'current_status': p['current_status'],
                'original_status': p['original_status'],
                'source': p['source'],
                'created_at': p['created_at']
            })

    print(f"Exported {len(projects)} projects to {output_path}")
    return len(projects)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Recovery Script for False Withdrawals")
    parser.add_argument('--analyze', action='store_true', help='Analyze false withdrawals')
    parser.add_argument('--recover', action='store_true', help='Recover false withdrawals (creates backup first)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be recovered without making changes')
    parser.add_argument('--export', type=str, help='Export to CSV file for review')
    parser.add_argument('--region', type=str, help='Filter by region')

    args = parser.parse_args()

    conn = sqlite3.connect(QUEUE_DB)

    if args.analyze or (not args.recover and not args.export):
        print("\n" + "=" * 70)
        print("FALSE WITHDRAWAL ANALYSIS")
        print("=" * 70)

        analysis = analyze_false_withdrawals(conn)

        print(f"\nTotal potentially false withdrawals: {analysis['total_count']:,}")
        print(f"Total MW affected: {analysis['total_mw']:,.0f} MW")

        print("\n--- By Region ---")
        for region, data in sorted(analysis['by_region'].items(), key=lambda x: -x[1]['count']):
            print(f"\n{region}: {data['count']:,} projects ({data['mw']:,.0f} MW)")
            for status, count in sorted(data['statuses'].items(), key=lambda x: -x[1])[:5]:
                print(f"    '{status}': {count}")

        print("\n--- By Original Status ---")
        for status, data in sorted(analysis['by_status'].items(), key=lambda x: -x[1]['count'])[:15]:
            print(f"  '{status}': {data['count']:,} projects ({data['mw']:,.0f} MW)")

        print("\n--- Top 20 by Capacity ---")
        print(f"{'Queue ID':<20} {'Region':<10} {'MW':<10} {'Original Status':<30} {'Name'}")
        print("-" * 100)
        for p in analysis['projects'][:20]:
            name = (p['name'] or '')[:35]
            mw = f"{p['capacity_mw']:.0f}" if p['capacity_mw'] else '-'
            print(f"{p['queue_id']:<20} {p['region']:<10} {mw:<10} {(p['original_status'] or '-'):<30} {name}")

    if args.export:
        output_path = Path(args.export)
        export_to_csv(conn, output_path)

    if args.dry_run:
        print("\n" + "=" * 70)
        print("DRY RUN - Would recover these projects:")
        print("=" * 70)
        count = recover_projects(conn, dry_run=True)
        print(f"\nWould recover {count:,} projects")

    if args.recover:
        print("\n" + "=" * 70)
        print("RECOVERING FALSE WITHDRAWALS")
        print("=" * 70)

        # Create backup first
        backup_path = create_backup(QUEUE_DB)

        # Confirm
        analysis = analyze_false_withdrawals(conn)
        print(f"\nAbout to recover {analysis['total_count']:,} projects ({analysis['total_mw']:,.0f} MW)")
        response = input("Proceed? (yes/no): ")

        if response.lower() == 'yes':
            count = recover_projects(conn, dry_run=False)
            print(f"\n✓ Recovered {count:,} projects")
            print(f"  Backup saved to: {backup_path}")
        else:
            print("Aborted.")

    conn.close()


if __name__ == '__main__':
    main()
