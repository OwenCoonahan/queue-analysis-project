#!/usr/bin/env python3
"""
Match Review System

Manual review queue for developer matches that need human verification.
Tracks matches with confidence between 0.75-0.85 and cross-source conflicts.

Usage:
    from match_review import MatchReviewQueue

    queue = MatchReviewQueue()
    queue.add_for_review(queue_id, region, candidate_matches)
    pending = queue.get_pending_reviews(limit=50)
    queue.approve_match(review_id, selected_match_idx)
"""

import sqlite3
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / '.data'
ENRICHMENT_DB = DATA_DIR / 'enrichment.db'


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class CandidateMatch:
    """A potential developer match candidate."""
    entity_name: str
    parent_company: Optional[str]
    confidence: float
    source: str  # 'eia', 'ferc', 'registry', 'fuzzy'
    match_method: str  # 'exact', 'fuzzy', 'keyword', 'location'
    supporting_data: Dict[str, Any] = None

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class ReviewItem:
    """An item in the review queue."""
    review_id: int
    queue_id: str
    region: str
    project_name: str
    raw_developer_name: Optional[str]
    capacity_mw: float
    fuel_type: str
    state: str
    candidates: List[CandidateMatch]
    priority_score: float
    status: str  # 'pending', 'approved', 'rejected', 'skipped'
    created_at: str
    reviewed_at: Optional[str] = None
    reviewed_by: Optional[str] = None
    selected_candidate_idx: Optional[int] = None
    notes: Optional[str] = None


@dataclass
class ConflictRecord:
    """A detected conflict between data sources."""
    conflict_id: int
    queue_id: str
    region: str
    source_a: str
    match_a: str
    confidence_a: float
    source_b: str
    match_b: str
    confidence_b: float
    resolved: bool
    resolution: Optional[str] = None


# =============================================================================
# MATCH REVIEW QUEUE
# =============================================================================

class MatchReviewQueue:
    """
    Manages the manual review queue for developer matches.

    Projects enter the queue when:
    - Fuzzy match confidence is 0.75-0.85
    - Multiple equally-likely matches exist
    - Cross-source conflicts detected
    - High-value projects (>100 MW) with any uncertainty
    """

    def __init__(self, db_path: str = None):
        """Initialize the review queue."""
        if db_path is None:
            db_path = ENRICHMENT_DB
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
        """Ensure review queue tables exist."""
        conn = self._get_conn()

        conn.execute("""
            CREATE TABLE IF NOT EXISTS developer_review_queue (
                review_id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id TEXT NOT NULL,
                region TEXT NOT NULL,
                project_name TEXT,
                raw_developer_name TEXT,
                capacity_mw REAL,
                fuel_type TEXT,
                state TEXT,
                candidates TEXT NOT NULL,  -- JSON array
                priority_score REAL DEFAULT 0,
                status TEXT DEFAULT 'pending',
                created_at TEXT NOT NULL,
                reviewed_at TEXT,
                reviewed_by TEXT,
                selected_candidate_idx INTEGER,
                notes TEXT,
                UNIQUE(queue_id, region)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS developer_conflicts (
                conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id TEXT NOT NULL,
                region TEXT NOT NULL,
                source_a TEXT NOT NULL,
                match_a TEXT NOT NULL,
                confidence_a REAL,
                source_b TEXT NOT NULL,
                match_b TEXT NOT NULL,
                confidence_b REAL,
                resolved BOOLEAN DEFAULT FALSE,
                resolution TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(queue_id, region, source_a, source_b)
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_review_status
            ON developer_review_queue(status)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_review_priority
            ON developer_review_queue(priority_score DESC)
        """)

        conn.commit()

    def calculate_priority(
        self,
        capacity_mw: float,
        region: str,
        fuel_type: str,
        confidence: float
    ) -> float:
        """
        Calculate review priority score.

        Higher priority for:
        - Larger projects (more impact on coverage)
        - Poor-coverage ISOs (CAISO, MISO)
        - Lower confidence (needs attention)
        - Solar/wind (primary focus)
        """
        score = 0.0

        # Capacity factor (0-40 points)
        if capacity_mw >= 500:
            score += 40
        elif capacity_mw >= 200:
            score += 30
        elif capacity_mw >= 100:
            score += 20
        elif capacity_mw >= 50:
            score += 10

        # ISO factor (0-30 points) - prioritize poor-coverage ISOs
        iso_priority = {
            'caiso': 30,
            'miso': 25,
            'spp': 20,
            'pjm': 15,
            'ercot': 15,
            'nyiso': 10,
            'isone': 10,
        }
        score += iso_priority.get(region.lower(), 10)

        # Fuel type factor (0-15 points)
        fuel_priority = {
            'solar': 15,
            'wind': 15,
            'battery': 12,
            'storage': 12,
            'hybrid': 10,
        }
        if fuel_type:
            score += fuel_priority.get(fuel_type.lower(), 5)

        # Confidence factor (0-15 points) - lower confidence = higher priority
        if confidence < 0.80:
            score += 15
        elif confidence < 0.85:
            score += 10
        elif confidence < 0.90:
            score += 5

        return score

    def add_for_review(
        self,
        queue_id: str,
        region: str,
        project_name: str,
        raw_developer_name: Optional[str],
        capacity_mw: float,
        fuel_type: str,
        state: str,
        candidates: List[CandidateMatch]
    ) -> int:
        """
        Add a project to the review queue.

        Args:
            queue_id: Unique project identifier
            region: ISO/RTO region
            project_name: Project name from queue
            raw_developer_name: Original developer name
            capacity_mw: Project capacity
            fuel_type: Fuel type
            state: State location
            candidates: List of potential matches

        Returns:
            review_id of created record
        """
        conn = self._get_conn()

        # Calculate priority
        top_confidence = max((c.confidence for c in candidates), default=0.0)
        priority = self.calculate_priority(
            capacity_mw, region, fuel_type, top_confidence
        )

        # Serialize candidates
        candidates_json = json.dumps([c.to_dict() for c in candidates])

        try:
            cursor = conn.execute("""
                INSERT INTO developer_review_queue (
                    queue_id, region, project_name, raw_developer_name,
                    capacity_mw, fuel_type, state, candidates,
                    priority_score, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            """, (
                queue_id, region, project_name, raw_developer_name,
                capacity_mw, fuel_type, state, candidates_json,
                priority, datetime.now().isoformat()
            ))
            conn.commit()
            return cursor.lastrowid

        except sqlite3.IntegrityError:
            # Already exists, update
            conn.execute("""
                UPDATE developer_review_queue
                SET project_name = ?, raw_developer_name = ?,
                    capacity_mw = ?, fuel_type = ?, state = ?,
                    candidates = ?, priority_score = ?, status = 'pending'
                WHERE queue_id = ? AND region = ?
            """, (
                project_name, raw_developer_name, capacity_mw, fuel_type,
                state, candidates_json, priority, queue_id, region
            ))
            conn.commit()

            row = conn.execute("""
                SELECT review_id FROM developer_review_queue
                WHERE queue_id = ? AND region = ?
            """, (queue_id, region)).fetchone()
            return row['review_id']

    def add_conflict(
        self,
        queue_id: str,
        region: str,
        source_a: str,
        match_a: str,
        confidence_a: float,
        source_b: str,
        match_b: str,
        confidence_b: float
    ) -> int:
        """Record a conflict between data sources."""
        conn = self._get_conn()

        try:
            cursor = conn.execute("""
                INSERT INTO developer_conflicts (
                    queue_id, region, source_a, match_a, confidence_a,
                    source_b, match_b, confidence_b, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                queue_id, region, source_a, match_a, confidence_a,
                source_b, match_b, confidence_b, datetime.now().isoformat()
            ))
            conn.commit()
            return cursor.lastrowid

        except sqlite3.IntegrityError:
            # Already exists
            return 0

    def get_pending_reviews(
        self,
        limit: int = 50,
        region: Optional[str] = None,
        min_capacity: Optional[float] = None
    ) -> List[ReviewItem]:
        """
        Get pending reviews ordered by priority.

        Args:
            limit: Maximum items to return
            region: Filter by ISO/RTO
            min_capacity: Filter by minimum capacity

        Returns:
            List of ReviewItem objects
        """
        conn = self._get_conn()

        query = """
            SELECT * FROM developer_review_queue
            WHERE status = 'pending'
        """
        params = []

        if region:
            query += " AND LOWER(region) = LOWER(?)"
            params.append(region)

        if min_capacity:
            query += " AND capacity_mw >= ?"
            params.append(min_capacity)

        query += " ORDER BY priority_score DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()

        items = []
        for row in rows:
            candidates_data = json.loads(row['candidates'])
            candidates = [
                CandidateMatch(**c) for c in candidates_data
            ]

            items.append(ReviewItem(
                review_id=row['review_id'],
                queue_id=row['queue_id'],
                region=row['region'],
                project_name=row['project_name'],
                raw_developer_name=row['raw_developer_name'],
                capacity_mw=row['capacity_mw'],
                fuel_type=row['fuel_type'],
                state=row['state'],
                candidates=candidates,
                priority_score=row['priority_score'],
                status=row['status'],
                created_at=row['created_at'],
                reviewed_at=row['reviewed_at'],
                reviewed_by=row['reviewed_by'],
                selected_candidate_idx=row['selected_candidate_idx'],
                notes=row['notes']
            ))

        return items

    def approve_match(
        self,
        review_id: int,
        selected_idx: int,
        reviewed_by: str = 'manual',
        notes: Optional[str] = None
    ) -> bool:
        """
        Approve a match from the review queue.

        Args:
            review_id: Review queue ID
            selected_idx: Index of selected candidate (0-based)
            reviewed_by: Who approved the match
            notes: Optional notes

        Returns:
            True if successful
        """
        conn = self._get_conn()

        conn.execute("""
            UPDATE developer_review_queue
            SET status = 'approved',
                selected_candidate_idx = ?,
                reviewed_by = ?,
                reviewed_at = ?,
                notes = ?
            WHERE review_id = ?
        """, (
            selected_idx, reviewed_by,
            datetime.now().isoformat(), notes, review_id
        ))
        conn.commit()

        return conn.total_changes > 0

    def reject_match(
        self,
        review_id: int,
        reviewed_by: str = 'manual',
        notes: Optional[str] = None
    ) -> bool:
        """Reject all candidates for a project."""
        conn = self._get_conn()

        conn.execute("""
            UPDATE developer_review_queue
            SET status = 'rejected',
                reviewed_by = ?,
                reviewed_at = ?,
                notes = ?
            WHERE review_id = ?
        """, (
            reviewed_by, datetime.now().isoformat(), notes, review_id
        ))
        conn.commit()

        return conn.total_changes > 0

    def skip_review(self, review_id: int) -> bool:
        """Skip a review item for now."""
        conn = self._get_conn()

        conn.execute("""
            UPDATE developer_review_queue
            SET status = 'skipped'
            WHERE review_id = ?
        """, (review_id,))
        conn.commit()

        return conn.total_changes > 0

    def get_review_stats(self) -> Dict[str, Any]:
        """Get statistics about the review queue."""
        conn = self._get_conn()

        # Status counts
        status_counts = dict(conn.execute("""
            SELECT status, COUNT(*) as count
            FROM developer_review_queue
            GROUP BY status
        """).fetchall())

        # By region
        region_counts = dict(conn.execute("""
            SELECT region, COUNT(*) as count
            FROM developer_review_queue
            WHERE status = 'pending'
            GROUP BY region
        """).fetchall())

        # Conflicts
        conflict_count = conn.execute("""
            SELECT COUNT(*) FROM developer_conflicts
            WHERE NOT resolved
        """).fetchone()[0]

        # Average priority of pending
        avg_priority = conn.execute("""
            SELECT AVG(priority_score) FROM developer_review_queue
            WHERE status = 'pending'
        """).fetchone()[0] or 0

        # Total MW pending
        total_mw_pending = conn.execute("""
            SELECT SUM(capacity_mw) FROM developer_review_queue
            WHERE status = 'pending'
        """).fetchone()[0] or 0

        return {
            'status_counts': status_counts,
            'region_counts': region_counts,
            'pending_count': status_counts.get('pending', 0),
            'approved_count': status_counts.get('approved', 0),
            'rejected_count': status_counts.get('rejected', 0),
            'unresolved_conflicts': conflict_count,
            'avg_priority': avg_priority,
            'total_mw_pending': total_mw_pending,
        }

    def get_approved_matches(self) -> List[Tuple[str, str, CandidateMatch]]:
        """
        Get all approved matches for applying to main database.

        Returns:
            List of (queue_id, region, selected_candidate) tuples
        """
        conn = self._get_conn()

        rows = conn.execute("""
            SELECT queue_id, region, candidates, selected_candidate_idx
            FROM developer_review_queue
            WHERE status = 'approved' AND selected_candidate_idx IS NOT NULL
        """).fetchall()

        results = []
        for row in rows:
            candidates = json.loads(row['candidates'])
            idx = row['selected_candidate_idx']
            if 0 <= idx < len(candidates):
                selected = CandidateMatch(**candidates[idx])
                results.append((row['queue_id'], row['region'], selected))

        return results

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI for match review queue."""
    import argparse

    parser = argparse.ArgumentParser(description="Developer Match Review Queue")
    parser.add_argument('--stats', action='store_true', help='Show queue statistics')
    parser.add_argument('--pending', action='store_true', help='List pending reviews')
    parser.add_argument('--limit', type=int, default=20, help='Number of items to show')
    parser.add_argument('--region', type=str, help='Filter by region')
    parser.add_argument('--approve', type=int, help='Approve review by ID')
    parser.add_argument('--reject', type=int, help='Reject review by ID')
    parser.add_argument('--candidate', type=int, default=0, help='Candidate index for approval')

    args = parser.parse_args()

    queue = MatchReviewQueue()

    if args.stats:
        stats = queue.get_review_stats()
        print("\n=== Match Review Queue Statistics ===")
        print(f"Pending: {stats['pending_count']}")
        print(f"Approved: {stats['approved_count']}")
        print(f"Rejected: {stats['rejected_count']}")
        print(f"Unresolved conflicts: {stats['unresolved_conflicts']}")
        print(f"Avg priority (pending): {stats['avg_priority']:.1f}")
        print(f"Total MW pending: {stats['total_mw_pending']:,.0f} MW")

        if stats['region_counts']:
            print("\nPending by region:")
            for region, count in sorted(stats['region_counts'].items()):
                print(f"  {region}: {count}")

    if args.pending:
        items = queue.get_pending_reviews(
            limit=args.limit,
            region=args.region
        )

        if not items:
            print("\nNo pending reviews.")
        else:
            print(f"\n=== Top {len(items)} Pending Reviews ===")
            print(f"{'ID':<6} {'Priority':<8} {'Region':<8} {'MW':<8} {'Fuel':<8} {'Project':<30} {'Candidates'}")
            print("-" * 100)

            for item in items:
                project = (item.project_name or 'Unknown')[:28]
                top_candidate = item.candidates[0] if item.candidates else None
                candidate_str = f"{top_candidate.entity_name} ({top_candidate.confidence:.0%})" if top_candidate else "None"

                print(f"{item.review_id:<6} {item.priority_score:<8.1f} {item.region:<8} "
                      f"{item.capacity_mw:<8.0f} {(item.fuel_type or '-'):<8} "
                      f"{project:<30} {candidate_str}")

    if args.approve:
        success = queue.approve_match(args.approve, args.candidate)
        if success:
            print(f"Approved review {args.approve} with candidate {args.candidate}")
        else:
            print(f"Failed to approve review {args.approve}")

    if args.reject:
        success = queue.reject_match(args.reject)
        if success:
            print(f"Rejected review {args.reject}")
        else:
            print(f"Failed to reject review {args.reject}")

    queue.close()


if __name__ == '__main__':
    main()
