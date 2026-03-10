#!/usr/bin/env python3
"""
Status Field Normalization

Standardizes the status field across all projects to consistent values.
This addresses the data quality issue of having multiple variations:
- withdrawn/Withdrawn → Withdrawn
- active/Active/ACTIVE → Active
- etc.

Usage:
    python normalize_status.py --analyze    # Show what would change
    python normalize_status.py --apply      # Apply normalization
    python normalize_status.py --export     # Export mapping to CSV
"""

import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple

DATA_DIR = Path(__file__).parent / '.data'
QUEUE_DB = DATA_DIR / 'queue.db'

# Status normalization mapping
# Key: raw value (case-insensitive match), Value: normalized value
STATUS_MAP = {
    # Withdrawn variations
    'withdrawn': 'Withdrawn',
    'wd': 'Withdrawn',
    'cancelled': 'Withdrawn',
    'canceled': 'Withdrawn',
    'terminated': 'Withdrawn',

    # Active variations
    'active': 'Active',
    'in progress': 'Active',
    'in-progress': 'Active',
    'pending': 'Active',
    'queued': 'Active',
    'ia pending': 'Active',
    'ia in progress': 'Active',
    'under study': 'Active',
    'under construction': 'Active',

    # Planned (ERCOT specific)
    'planned': 'Planned',

    # Operational/Completed
    'operational': 'Operational',
    'online': 'Operational',
    'in service': 'Operational',
    'in-service': 'Operational',
    'commercial operation': 'Operational',
    'done': 'Completed',
    'completed': 'Completed',
    'complete': 'Completed',
    'ia executed': 'Completed',
    'ia fully executed': 'Completed',

    # Suspended
    'suspended': 'Suspended',
    'on hold': 'Suspended',

    # Legacy (from LBL historical)
    'legacy: done': 'Completed',
    'legacy: archived': 'Withdrawn',

    # Unknown/null handling
    'unknown': None,
    '': None,
}

# MISO study phase numeric codes
# These are actually study phases, not final statuses
MISO_PHASE_MAP = {
    '1.0': 'Phase 1 - Feasibility',
    '2.0': 'Phase 2 - System Impact',
    '3.0': 'Phase 3 - Facilities',
    '4.0': 'Phase 4 - Final',
    '5.0': 'Phase 5 - IA',
    '6.0': 'Phase 6 - Construction',
    '7.0': 'Phase 7',
    '8.0': 'Phase 8',
    '9.0': 'Phase 9',
    '10.0': 'Phase 10',
    '11.0': 'Phase 11',
    '12.0': 'Phase 12',
}


def normalize_status(raw_status: str) -> Tuple[str, str]:
    """
    Normalize a status value.

    Returns:
        Tuple of (normalized_status, normalization_method)
    """
    if raw_status is None:
        return None, 'null_input'

    # Check for MISO numeric phases first
    if raw_status in MISO_PHASE_MAP:
        # Keep as Active since they're in-progress study phases
        return 'Active', f'miso_phase:{raw_status}'

    # Try lowercase match
    lower = raw_status.lower().strip()
    if lower in STATUS_MAP:
        return STATUS_MAP[lower], 'exact_match'

    # Try partial matches for common patterns
    if 'withdraw' in lower or 'cancel' in lower:
        return 'Withdrawn', 'partial_match:withdrawn'
    if 'active' in lower or 'progress' in lower:
        return 'Active', 'partial_match:active'
    if 'operat' in lower or 'online' in lower or 'service' in lower:
        return 'Operational', 'partial_match:operational'
    if 'complete' in lower or 'done' in lower:
        return 'Completed', 'partial_match:completed'
    if 'suspend' in lower or 'hold' in lower:
        return 'Suspended', 'partial_match:suspended'

    # If we can't normalize, keep original with capitalized first letter
    return raw_status.capitalize(), 'preserved'


def analyze_normalization(conn: sqlite3.Connection) -> Dict:
    """Analyze what would change with normalization."""

    cursor = conn.execute("""
        SELECT status, COUNT(*) as cnt, ROUND(SUM(capacity_mw), 0) as mw
        FROM projects
        GROUP BY status
        ORDER BY cnt DESC
    """)

    changes = {}
    preserved = {}

    for raw_status, cnt, mw in cursor.fetchall():
        normalized, method = normalize_status(raw_status)

        if normalized != raw_status:
            if raw_status not in changes:
                changes[raw_status] = {
                    'normalized': normalized,
                    'method': method,
                    'count': cnt,
                    'mw': mw or 0
                }
        else:
            preserved[raw_status] = {'count': cnt, 'mw': mw or 0}

    return {
        'changes': changes,
        'preserved': preserved,
        'total_to_change': sum(c['count'] for c in changes.values()),
        'total_mw_affected': sum(c['mw'] for c in changes.values()),
    }


def apply_normalization(conn: sqlite3.Connection, dry_run: bool = True) -> int:
    """Apply status normalization to all projects."""

    # Get distinct statuses
    cursor = conn.execute("SELECT DISTINCT status FROM projects")
    statuses = [row[0] for row in cursor.fetchall()]

    changes_applied = 0
    timestamp = datetime.now().isoformat()

    for raw_status in statuses:
        normalized, method = normalize_status(raw_status)

        if normalized != raw_status:
            if dry_run:
                print(f"  Would change: {repr(raw_status)} → {repr(normalized)} ({method})")
            else:
                # Update projects
                cursor = conn.execute("""
                    UPDATE projects
                    SET status = ?,
                        updated_at = ?
                    WHERE status = ?
                """, (normalized, timestamp, raw_status))

                count = cursor.rowcount
                changes_applied += count

                # Log the change
                conn.execute("""
                    INSERT INTO changes (
                        queue_id, region, detected_at, change_type,
                        field_name, old_value, new_value
                    ) VALUES ('BULK_NORMALIZE', 'ALL', ?, 'normalization', 'status', ?, ?)
                """, (timestamp, raw_status, normalized))

                print(f"  Changed: {repr(raw_status)} → {repr(normalized)} ({count} rows)")

    if not dry_run:
        conn.commit()

    return changes_applied


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Status Field Normalization")
    parser.add_argument('--analyze', action='store_true', help='Analyze what would change')
    parser.add_argument('--apply', action='store_true', help='Apply normalization')
    parser.add_argument('--dry-run', action='store_true', help='Show what would change without applying')
    parser.add_argument('--export', type=str, help='Export mapping to CSV')

    args = parser.parse_args()

    conn = sqlite3.connect(QUEUE_DB)

    if args.analyze or (not args.apply and not args.export):
        print("\n" + "=" * 70)
        print("STATUS NORMALIZATION ANALYSIS")
        print("=" * 70)

        analysis = analyze_normalization(conn)

        print(f"\nTotal values to normalize: {len(analysis['changes'])}")
        print(f"Total rows affected: {analysis['total_to_change']:,}")
        print(f"Total MW affected: {analysis['total_mw_affected']:,.0f}")

        print("\n--- Changes ---")
        print(f"{'Current Value':<35} {'Normalized':<20} {'Method':<25} {'Count':>10} {'MW':>12}")
        print("-" * 110)

        for raw, data in sorted(analysis['changes'].items(), key=lambda x: -x[1]['count']):
            raw_display = repr(raw)[:33] if raw else 'NULL'
            print(f"{raw_display:<35} {data['normalized'] or 'NULL':<20} {data['method']:<25} {data['count']:>10,} {data['mw']:>12,.0f}")

        print("\n--- Preserved (no change) ---")
        for status, data in sorted(analysis['preserved'].items(), key=lambda x: -x[1]['count']):
            print(f"  {status}: {data['count']:,} rows")

    if args.dry_run:
        print("\n" + "=" * 70)
        print("DRY RUN - Would apply these changes:")
        print("=" * 70)
        apply_normalization(conn, dry_run=True)

    if args.apply:
        print("\n" + "=" * 70)
        print("APPLYING NORMALIZATION")
        print("=" * 70)

        response = input("This will modify the database. Continue? (yes/no): ")
        if response.lower() == 'yes':
            count = apply_normalization(conn, dry_run=False)
            print(f"\nNormalized {count:,} rows")
        else:
            print("Aborted.")

    if args.export:
        import csv

        analysis = analyze_normalization(conn)

        with open(args.export, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['raw_status', 'normalized_status', 'method', 'count', 'mw'])

            for raw, data in analysis['changes'].items():
                writer.writerow([raw, data['normalized'], data['method'], data['count'], data['mw']])

        print(f"Exported to {args.export}")

    conn.close()


if __name__ == '__main__':
    main()
