#!/usr/bin/env python3
"""
Validation Gates - Phase 0 Emergency Fix

Prevents data corruption from automatic status changes by requiring
human approval for high-risk modifications (especially status→Withdrawn).

This is an emergency fix to stop false withdrawals from sync failures.

Usage:
    # Queue a change for review (used by sync_stale_records)
    from validation_gates import ValidationGates
    gates = ValidationGates()
    gates.queue_status_change(queue_id, region, 'Active', 'Withdrawn', 'not_in_fetch')

    # Review pending changes
    python validation_gates.py --pending
    python validation_gates.py --approve <change_id>
    python validation_gates.py --reject <change_id> --reason "False positive"
    python validation_gates.py --approve-all --region MISO
"""

import sqlite3
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / '.data'
QUEUE_DB = DATA_DIR / 'queue.db'


# =============================================================================
# CONFIGURATION - What requires human approval
# =============================================================================

# Status changes that ALWAYS require approval
HIGH_RISK_STATUS_CHANGES = {
    ('Active', 'Withdrawn'),
    ('Pending', 'Withdrawn'),
    ('In Progress', 'Withdrawn'),
    ('Under Construction', 'Withdrawn'),
}

# Capacity changes > this percentage require approval
CAPACITY_CHANGE_THRESHOLD = 0.20  # 20%

# Projects > this size always require approval for any status change
LARGE_PROJECT_MW = 500


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class PendingChange:
    """A change awaiting human approval."""
    change_id: int
    queue_id: str
    region: str
    project_name: Optional[str]
    capacity_mw: Optional[float]
    change_type: str  # 'status', 'capacity', 'developer'
    field_name: str
    old_value: str
    new_value: str
    change_reason: str
    change_source: str  # 'sync', 'enrichment', 'manual'
    risk_level: str  # 'high', 'medium', 'low'
    status: str  # 'pending', 'approved', 'rejected'
    created_at: str
    reviewed_at: Optional[str]
    reviewed_by: Optional[str]
    rejection_reason: Optional[str]


# =============================================================================
# VALIDATION GATES
# =============================================================================

class ValidationGates:
    """
    Manages the validation queue for high-risk data changes.

    Principle: Never auto-apply destructive changes. Queue them for review.
    """

    def __init__(self, db_path: str = None):
        """Initialize validation gates."""
        if db_path is None:
            db_path = QUEUE_DB
        self.db_path = Path(db_path)
        self._conn = None
        self._ensure_tables()

    def _get_conn(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _ensure_tables(self):
        """Create validation tables if they don't exist."""
        conn = self._get_conn()

        conn.execute("""
            CREATE TABLE IF NOT EXISTS validation_queue (
                change_id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id TEXT NOT NULL,
                region TEXT NOT NULL,
                project_name TEXT,
                capacity_mw REAL,

                -- What's changing
                change_type TEXT NOT NULL,      -- 'status', 'capacity', 'developer'
                field_name TEXT NOT NULL,       -- 'status', 'capacity_mw', etc.
                old_value TEXT,
                new_value TEXT,

                -- Why it's changing
                change_reason TEXT,             -- 'not_in_fetch', 'eia_match', etc.
                change_source TEXT,             -- 'sync', 'enrichment', 'manual'

                -- Risk assessment
                risk_level TEXT DEFAULT 'medium',

                -- Review workflow
                status TEXT DEFAULT 'pending',  -- 'pending', 'approved', 'rejected'
                created_at TEXT NOT NULL,
                reviewed_at TEXT,
                reviewed_by TEXT,
                rejection_reason TEXT,

                -- Prevent duplicate pending changes
                UNIQUE(queue_id, region, field_name, new_value, status)
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_validation_status
            ON validation_queue(status)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_validation_region
            ON validation_queue(region, status)
        """)

        # Audit log of all applied changes
        conn.execute("""
            CREATE TABLE IF NOT EXISTS validation_audit (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                change_id INTEGER,
                queue_id TEXT NOT NULL,
                region TEXT NOT NULL,
                change_type TEXT NOT NULL,
                field_name TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                change_source TEXT,
                action TEXT NOT NULL,           -- 'approved', 'rejected', 'auto_applied'
                action_by TEXT,
                action_reason TEXT,
                created_at TEXT NOT NULL
            )
        """)

        conn.commit()

    def assess_risk(
        self,
        change_type: str,
        field_name: str,
        old_value: str,
        new_value: str,
        capacity_mw: float = None
    ) -> str:
        """
        Assess the risk level of a change.

        Returns:
            'high', 'medium', or 'low'
        """
        # Status → Withdrawn is ALWAYS high risk
        if change_type == 'status' and new_value in ('Withdrawn', 'withdrawn'):
            return 'high'

        # Large projects are always high risk
        if capacity_mw and capacity_mw >= LARGE_PROJECT_MW:
            return 'high'

        # Capacity changes > threshold are high risk
        if change_type == 'capacity' and old_value and new_value:
            try:
                old_cap = float(old_value)
                new_cap = float(new_value)
                if old_cap > 0:
                    change_pct = abs(new_cap - old_cap) / old_cap
                    if change_pct > CAPACITY_CHANGE_THRESHOLD:
                        return 'high'
            except (ValueError, TypeError):
                pass

        return 'medium'

    def requires_approval(
        self,
        change_type: str,
        field_name: str,
        old_value: str,
        new_value: str,
        capacity_mw: float = None
    ) -> bool:
        """
        Check if a change requires human approval.

        Returns:
            True if change should be queued for review
        """
        risk = self.assess_risk(change_type, field_name, old_value, new_value, capacity_mw)
        return risk == 'high'

    def queue_change(
        self,
        queue_id: str,
        region: str,
        change_type: str,
        field_name: str,
        old_value: str,
        new_value: str,
        change_reason: str,
        change_source: str = 'sync',
        project_name: str = None,
        capacity_mw: float = None
    ) -> int:
        """
        Queue a change for human review.

        Returns:
            change_id of the queued change
        """
        conn = self._get_conn()

        risk_level = self.assess_risk(
            change_type, field_name, old_value, new_value, capacity_mw
        )

        try:
            cursor = conn.execute("""
                INSERT INTO validation_queue (
                    queue_id, region, project_name, capacity_mw,
                    change_type, field_name, old_value, new_value,
                    change_reason, change_source, risk_level,
                    status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            """, (
                queue_id, region, project_name, capacity_mw,
                change_type, field_name, old_value, new_value,
                change_reason, change_source, risk_level,
                datetime.now().isoformat()
            ))
            conn.commit()
            return cursor.lastrowid

        except sqlite3.IntegrityError:
            # Already queued
            row = conn.execute("""
                SELECT change_id FROM validation_queue
                WHERE queue_id = ? AND region = ? AND field_name = ?
                AND new_value = ? AND status = 'pending'
            """, (queue_id, region, field_name, new_value)).fetchone()
            return row['change_id'] if row else 0

    def queue_status_change(
        self,
        queue_id: str,
        region: str,
        old_status: str,
        new_status: str,
        reason: str,
        source: str = 'sync'
    ) -> int:
        """
        Queue a status change for review.

        Convenience wrapper for status changes.
        """
        # Get project info for context
        conn = self._get_conn()
        row = conn.execute("""
            SELECT name, capacity_mw FROM projects
            WHERE queue_id = ? AND region = ?
        """, (queue_id, region)).fetchone()

        project_name = row['name'] if row else None
        capacity_mw = row['capacity_mw'] if row else None

        return self.queue_change(
            queue_id=queue_id,
            region=region,
            change_type='status',
            field_name='status',
            old_value=old_status,
            new_value=new_status,
            change_reason=reason,
            change_source=source,
            project_name=project_name,
            capacity_mw=capacity_mw
        )

    def get_pending(
        self,
        region: str = None,
        change_type: str = None,
        limit: int = 100
    ) -> List[PendingChange]:
        """Get pending changes awaiting review."""
        conn = self._get_conn()

        query = """
            SELECT * FROM validation_queue
            WHERE status = 'pending'
        """
        params = []

        if region:
            query += " AND UPPER(region) = UPPER(?)"
            params.append(region)

        if change_type:
            query += " AND change_type = ?"
            params.append(change_type)

        query += " ORDER BY risk_level DESC, capacity_mw DESC NULLS LAST, created_at ASC"
        query += " LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()

        return [PendingChange(
            change_id=r['change_id'],
            queue_id=r['queue_id'],
            region=r['region'],
            project_name=r['project_name'],
            capacity_mw=r['capacity_mw'],
            change_type=r['change_type'],
            field_name=r['field_name'],
            old_value=r['old_value'],
            new_value=r['new_value'],
            change_reason=r['change_reason'],
            change_source=r['change_source'],
            risk_level=r['risk_level'],
            status=r['status'],
            created_at=r['created_at'],
            reviewed_at=r['reviewed_at'],
            reviewed_by=r['reviewed_by'],
            rejection_reason=r['rejection_reason']
        ) for r in rows]

    def approve(
        self,
        change_id: int,
        reviewed_by: str = 'manual',
        apply_change: bool = True
    ) -> bool:
        """
        Approve a pending change and optionally apply it.

        Args:
            change_id: ID of the change to approve
            reviewed_by: Who approved it
            apply_change: If True, apply the change to the database

        Returns:
            True if successful
        """
        conn = self._get_conn()

        # Get the change
        row = conn.execute("""
            SELECT * FROM validation_queue WHERE change_id = ?
        """, (change_id,)).fetchone()

        if not row:
            logger.error(f"Change {change_id} not found")
            return False

        if row['status'] != 'pending':
            logger.warning(f"Change {change_id} already {row['status']}")
            return False

        # Mark as approved
        conn.execute("""
            UPDATE validation_queue
            SET status = 'approved',
                reviewed_at = ?,
                reviewed_by = ?
            WHERE change_id = ?
        """, (datetime.now().isoformat(), reviewed_by, change_id))

        # Log to audit
        conn.execute("""
            INSERT INTO validation_audit (
                change_id, queue_id, region, change_type, field_name,
                old_value, new_value, change_source, action, action_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'approved', ?, ?)
        """, (
            change_id, row['queue_id'], row['region'], row['change_type'],
            row['field_name'], row['old_value'], row['new_value'],
            row['change_source'], reviewed_by, datetime.now().isoformat()
        ))

        # Apply the change
        if apply_change:
            if row['change_type'] == 'status':
                conn.execute("""
                    UPDATE projects
                    SET status = ?
                    WHERE queue_id = ? AND region = ?
                """, (row['new_value'], row['queue_id'], row['region']))
                logger.info(f"Applied status change: {row['queue_id']} → {row['new_value']}")

        conn.commit()
        return True

    def reject(
        self,
        change_id: int,
        reviewed_by: str = 'manual',
        reason: str = None
    ) -> bool:
        """
        Reject a pending change.

        Args:
            change_id: ID of the change to reject
            reviewed_by: Who rejected it
            reason: Why it was rejected

        Returns:
            True if successful
        """
        conn = self._get_conn()

        row = conn.execute("""
            SELECT * FROM validation_queue WHERE change_id = ?
        """, (change_id,)).fetchone()

        if not row:
            logger.error(f"Change {change_id} not found")
            return False

        if row['status'] != 'pending':
            logger.warning(f"Change {change_id} already {row['status']}")
            return False

        conn.execute("""
            UPDATE validation_queue
            SET status = 'rejected',
                reviewed_at = ?,
                reviewed_by = ?,
                rejection_reason = ?
            WHERE change_id = ?
        """, (datetime.now().isoformat(), reviewed_by, reason, change_id))

        conn.execute("""
            INSERT INTO validation_audit (
                change_id, queue_id, region, change_type, field_name,
                old_value, new_value, change_source, action, action_by,
                action_reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'rejected', ?, ?, ?)
        """, (
            change_id, row['queue_id'], row['region'], row['change_type'],
            row['field_name'], row['old_value'], row['new_value'],
            row['change_source'], reviewed_by, reason, datetime.now().isoformat()
        ))

        conn.commit()
        return True

    def approve_batch(
        self,
        region: str = None,
        change_type: str = None,
        max_count: int = 100,
        reviewed_by: str = 'batch'
    ) -> int:
        """
        Approve multiple pending changes at once.

        Use with caution - only for bulk approval after review.

        Returns:
            Number of changes approved
        """
        pending = self.get_pending(region=region, change_type=change_type, limit=max_count)

        approved = 0
        for change in pending:
            if self.approve(change.change_id, reviewed_by=reviewed_by):
                approved += 1

        return approved

    def reject_batch(
        self,
        region: str = None,
        change_type: str = None,
        reason: str = 'Batch rejection',
        max_count: int = 100,
        reviewed_by: str = 'batch'
    ) -> int:
        """Reject multiple pending changes at once."""
        pending = self.get_pending(region=region, change_type=change_type, limit=max_count)

        rejected = 0
        for change in pending:
            if self.reject(change.change_id, reviewed_by=reviewed_by, reason=reason):
                rejected += 1

        return rejected

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the validation queue."""
        conn = self._get_conn()

        # Status counts
        status_counts = dict(conn.execute("""
            SELECT status, COUNT(*) FROM validation_queue
            GROUP BY status
        """).fetchall())

        # Pending by region
        pending_by_region = dict(conn.execute("""
            SELECT region, COUNT(*) FROM validation_queue
            WHERE status = 'pending'
            GROUP BY region
        """).fetchall())

        # Pending by change type
        pending_by_type = dict(conn.execute("""
            SELECT change_type, COUNT(*) FROM validation_queue
            WHERE status = 'pending'
            GROUP BY change_type
        """).fetchall())

        # High risk count
        high_risk = conn.execute("""
            SELECT COUNT(*) FROM validation_queue
            WHERE status = 'pending' AND risk_level = 'high'
        """).fetchone()[0]

        # Total MW pending
        total_mw = conn.execute("""
            SELECT SUM(capacity_mw) FROM validation_queue
            WHERE status = 'pending'
        """).fetchone()[0] or 0

        return {
            'pending': status_counts.get('pending', 0),
            'approved': status_counts.get('approved', 0),
            'rejected': status_counts.get('rejected', 0),
            'pending_by_region': pending_by_region,
            'pending_by_type': pending_by_type,
            'high_risk_pending': high_risk,
            'total_mw_pending': total_mw,
        }

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI for validation gates."""
    import argparse

    parser = argparse.ArgumentParser(description="Validation Gates - Review pending changes")
    parser.add_argument('--stats', action='store_true', help='Show queue statistics')
    parser.add_argument('--pending', action='store_true', help='List pending changes')
    parser.add_argument('--region', type=str, help='Filter by region')
    parser.add_argument('--limit', type=int, default=50, help='Max items to show')
    parser.add_argument('--approve', type=int, help='Approve change by ID')
    parser.add_argument('--reject', type=int, help='Reject change by ID')
    parser.add_argument('--reason', type=str, help='Rejection reason')
    parser.add_argument('--approve-all', action='store_true', help='Approve all pending (use with --region)')
    parser.add_argument('--reject-all', action='store_true', help='Reject all pending (use with --region)')

    args = parser.parse_args()

    gates = ValidationGates()

    if args.stats:
        stats = gates.get_stats()
        print("\n=== Validation Queue Statistics ===")
        print(f"Pending: {stats['pending']}")
        print(f"Approved: {stats['approved']}")
        print(f"Rejected: {stats['rejected']}")
        print(f"High-risk pending: {stats['high_risk_pending']}")
        print(f"Total MW pending: {stats['total_mw_pending']:,.0f} MW")

        if stats['pending_by_region']:
            print("\nPending by region:")
            for region, count in sorted(stats['pending_by_region'].items()):
                print(f"  {region}: {count}")

        if stats['pending_by_type']:
            print("\nPending by type:")
            for ctype, count in sorted(stats['pending_by_type'].items()):
                print(f"  {ctype}: {count}")

    if args.pending:
        pending = gates.get_pending(region=args.region, limit=args.limit)

        if not pending:
            print("\nNo pending changes.")
        else:
            print(f"\n=== {len(pending)} Pending Changes ===")
            print(f"{'ID':<6} {'Risk':<6} {'Region':<8} {'MW':<8} {'Type':<10} {'Old':<15} {'New':<15} {'Project'}")
            print("-" * 100)

            for p in pending:
                project = (p.project_name or 'Unknown')[:25]
                mw = f"{p.capacity_mw:.0f}" if p.capacity_mw else "-"
                old = (p.old_value or '-')[:13]
                new = (p.new_value or '-')[:13]

                print(f"{p.change_id:<6} {p.risk_level:<6} {p.region:<8} {mw:<8} "
                      f"{p.change_type:<10} {old:<15} {new:<15} {project}")

    if args.approve:
        if gates.approve(args.approve):
            print(f"Approved change {args.approve}")
        else:
            print(f"Failed to approve change {args.approve}")

    if args.reject:
        if gates.reject(args.reject, reason=args.reason):
            print(f"Rejected change {args.reject}")
        else:
            print(f"Failed to reject change {args.reject}")

    if args.approve_all:
        if not args.region:
            print("ERROR: --approve-all requires --region to prevent accidents")
        else:
            count = gates.approve_batch(region=args.region)
            print(f"Approved {count} changes for {args.region}")

    if args.reject_all:
        if not args.region:
            print("ERROR: --reject-all requires --region to prevent accidents")
        else:
            reason = args.reason or 'Batch rejection'
            count = gates.reject_batch(region=args.region, reason=reason)
            print(f"Rejected {count} changes for {args.region}")

    gates.close()


if __name__ == '__main__':
    main()
