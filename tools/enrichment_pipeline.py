#!/usr/bin/env python3
"""
Developer Enrichment Pipeline

Orchestrates multiple data sources to enrich queue projects with accurate
developer and parent company information. Sources are tried in order,
and the first match with confidence >= 0.85 is applied.

Pipeline Order:
1. Manual overrides (verified=true)
2. Exact match to parent company registry
3. EIA Form 860 (state+county+capacity+fuel)
4. FERC Form 1 PPAs (seller name match)
5. Fuzzy match to registry (>0.95 only)
6. Queue for manual review (<0.85)

Usage:
    from enrichment_pipeline import DeveloperEnrichmentPipeline

    pipeline = DeveloperEnrichmentPipeline()
    pipeline.run(region='pjm')  # Process one ISO
    pipeline.run_all()          # Process all ISOs

CLI:
    python enrichment_pipeline.py --run --region pjm
    python enrichment_pipeline.py --stats
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

# Local imports
from developer_matcher import DeveloperMatcher, MatchResult
from eia_pudl_loader import EIAPudlLoader
from ferc_seller_extractor import FERCSellerExtractor, FERCMatch
from match_review import MatchReviewQueue, CandidateMatch

DATA_DIR = Path(__file__).parent / '.data'
QUEUE_DB = DATA_DIR / 'queue.db'
ENRICHMENT_DB = DATA_DIR / 'enrichment.db'

# Confidence thresholds
AUTO_APPLY_THRESHOLD = 0.85    # Auto-apply matches at or above this
HIGH_CONFIDENCE = 0.95         # Very confident - apply immediately
REVIEW_THRESHOLD = 0.75        # Send to review queue if between 0.75-0.85


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class EnrichmentResult:
    """Result of enriching a single project."""
    queue_id: str
    region: str
    matched: bool = False
    filing_entity: Optional[str] = None
    parent_company: Optional[str] = None
    confidence: float = 0.0
    source: str = 'none'
    match_method: str = 'none'
    queued_for_review: bool = False
    conflict_detected: bool = False


@dataclass
class PipelineStats:
    """Statistics from a pipeline run."""
    total_projects: int = 0
    already_matched: int = 0
    new_matches: int = 0
    high_confidence: int = 0
    medium_confidence: int = 0
    queued_for_review: int = 0
    no_match: int = 0
    conflicts: int = 0
    by_source: Dict[str, int] = None
    by_region: Dict[str, int] = None


# =============================================================================
# ENRICHMENT PIPELINE
# =============================================================================

class DeveloperEnrichmentPipeline:
    """
    Orchestrates developer enrichment from multiple sources.

    Follows accuracy-first philosophy:
    - Only auto-apply matches with confidence >= 0.85
    - Track both filing entity (SPV) and parent company
    - Queue uncertain matches for human review
    - Full audit trail for every match
    """

    def __init__(
        self,
        queue_db: str = None,
        enrichment_db: str = None
    ):
        """Initialize the enrichment pipeline."""
        self.queue_db = Path(queue_db) if queue_db else QUEUE_DB
        self.enrichment_db = Path(enrichment_db) if enrichment_db else ENRICHMENT_DB

        # Lazy-loaded components
        self._matcher: Optional[DeveloperMatcher] = None
        self._eia_loader: Optional[EIAPudlLoader] = None
        self._ferc_extractor: Optional[FERCSellerExtractor] = None
        self._review_queue: Optional[MatchReviewQueue] = None

        self._ensure_tables()

    def _ensure_tables(self):
        """Ensure audit tables exist."""
        conn = sqlite3.connect(self.enrichment_db)

        # Audit log for all match attempts
        conn.execute("""
            CREATE TABLE IF NOT EXISTS developer_match_audit (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id TEXT NOT NULL,
                region TEXT NOT NULL,
                project_name TEXT,
                raw_developer_name TEXT,
                matched_entity TEXT,
                matched_parent TEXT,
                confidence REAL,
                source TEXT,
                match_method TEXT,
                alternative_matches TEXT,  -- JSON
                applied BOOLEAN DEFAULT FALSE,
                created_at TEXT NOT NULL,
                UNIQUE(queue_id, region, source)
            )
        """)

        # Applied developer assignments
        conn.execute("""
            CREATE TABLE IF NOT EXISTS developer_assignments (
                assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id TEXT NOT NULL,
                region TEXT NOT NULL,
                filing_entity TEXT,
                parent_company TEXT,
                parent_company_id INTEGER,
                confidence REAL,
                source TEXT,
                verified BOOLEAN DEFAULT FALSE,
                created_at TEXT NOT NULL,
                updated_at TEXT,
                UNIQUE(queue_id, region)
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_audit_queue
            ON developer_match_audit(queue_id, region)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_assignments_parent
            ON developer_assignments(parent_company)
        """)

        conn.commit()
        conn.close()

    @property
    def matcher(self) -> DeveloperMatcher:
        """Lazy-load the developer matcher."""
        if self._matcher is None:
            # Use default path (queue_v2.db) which has parent company data
            self._matcher = DeveloperMatcher()
        return self._matcher

    @property
    def eia_loader(self) -> EIAPudlLoader:
        """Lazy-load the EIA loader."""
        if self._eia_loader is None:
            self._eia_loader = EIAPudlLoader()
        return self._eia_loader

    @property
    def ferc_extractor(self) -> FERCSellerExtractor:
        """Lazy-load the FERC extractor."""
        if self._ferc_extractor is None:
            self._ferc_extractor = FERCSellerExtractor()
        return self._ferc_extractor

    @property
    def review_queue(self) -> MatchReviewQueue:
        """Lazy-load the review queue."""
        if self._review_queue is None:
            self._review_queue = MatchReviewQueue(str(self.enrichment_db))
        return self._review_queue

    def get_projects(
        self,
        region: Optional[str] = None,
        status: str = 'Active'
    ) -> List[Dict]:
        """
        Get queue projects to enrich.

        Args:
            region: Filter by ISO/RTO
            status: Filter by queue status

        Returns:
            List of project dictionaries
        """
        conn = sqlite3.connect(self.queue_db)
        conn.row_factory = sqlite3.Row

        query = """
            SELECT
                queue_id,
                region,
                name as project_name,
                developer as developer_name,
                capacity_mw,
                type as fuel_type,
                state,
                county,
                status as queue_status
            FROM projects
            WHERE 1=1
        """
        params = []

        if region:
            query += " AND LOWER(region) = LOWER(?)"
            params.append(region)

        if status:
            query += " AND status = ?"
            params.append(status)

        query += " ORDER BY capacity_mw DESC"

        rows = conn.execute(query, params).fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_existing_assignment(
        self,
        queue_id: str,
        region: str
    ) -> Optional[Dict]:
        """Check if project already has a developer assignment."""
        conn = sqlite3.connect(self.enrichment_db)
        conn.row_factory = sqlite3.Row

        row = conn.execute("""
            SELECT * FROM developer_assignments
            WHERE queue_id = ? AND region = ?
        """, (queue_id, region)).fetchone()

        conn.close()
        return dict(row) if row else None

    def log_match_attempt(
        self,
        queue_id: str,
        region: str,
        project_name: str,
        raw_developer: str,
        entity: str,
        parent: str,
        confidence: float,
        source: str,
        method: str,
        alternatives: List[Dict] = None,
        applied: bool = False
    ):
        """Log a match attempt to the audit table."""
        conn = sqlite3.connect(self.enrichment_db)

        try:
            conn.execute("""
                INSERT INTO developer_match_audit (
                    queue_id, region, project_name, raw_developer_name,
                    matched_entity, matched_parent, confidence,
                    source, match_method, alternative_matches, applied, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                queue_id, region, project_name, raw_developer,
                entity, parent, confidence, source, method,
                json.dumps(alternatives) if alternatives else None,
                applied, datetime.now().isoformat()
            ))
            conn.commit()
        except sqlite3.IntegrityError:
            # Already logged from this source
            pass
        finally:
            conn.close()

    def apply_assignment(
        self,
        queue_id: str,
        region: str,
        filing_entity: str,
        parent_company: str,
        parent_company_id: Optional[int],
        confidence: float,
        source: str
    ):
        """Apply a developer assignment."""
        conn = sqlite3.connect(self.enrichment_db)

        try:
            conn.execute("""
                INSERT INTO developer_assignments (
                    queue_id, region, filing_entity, parent_company,
                    parent_company_id, confidence, source, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                queue_id, region, filing_entity, parent_company,
                parent_company_id, confidence, source, datetime.now().isoformat()
            ))
        except sqlite3.IntegrityError:
            conn.execute("""
                UPDATE developer_assignments
                SET filing_entity = ?, parent_company = ?,
                    parent_company_id = ?, confidence = ?,
                    source = ?, updated_at = ?
                WHERE queue_id = ? AND region = ?
            """, (
                filing_entity, parent_company, parent_company_id,
                confidence, source, datetime.now().isoformat(),
                queue_id, region
            ))

        conn.commit()
        conn.close()

    def try_registry_match(
        self,
        developer_name: str,
        project_name: str
    ) -> Optional[Tuple[str, str, float, int]]:
        """
        Try matching against parent company registry.

        Returns:
            (entity_name, parent_name, confidence, parent_id) or None
        """
        if not developer_name:
            return None

        # Try exact match first
        result = self.matcher.match(developer_name)

        if result.matched and result.confidence >= AUTO_APPLY_THRESHOLD:
            return (
                result.entity_name,
                result.parent_company or result.entity_name,
                result.confidence,
                result.parent_id
            )

        # Try project name as fallback
        if project_name:
            result = self.matcher.match(project_name)
            if result.matched and result.confidence >= HIGH_CONFIDENCE:
                return (
                    result.entity_name,
                    result.parent_company or result.entity_name,
                    result.confidence,
                    result.parent_id
                )

        return None

    def try_eia_match(
        self,
        state: str,
        county: str,
        capacity_mw: float,
        fuel_type: str
    ) -> Optional[Tuple[str, float]]:
        """
        Try matching against EIA Form 860 data.

        Returns:
            (owner_name, confidence) or None
        """
        if not state or not capacity_mw:
            return None

        result = self.eia_loader.match_project(
            state=state,
            county=county,
            capacity_mw=capacity_mw,
            fuel_type=fuel_type
        )

        if result.matched and result.confidence >= AUTO_APPLY_THRESHOLD:
            return (result.owner_name, result.confidence)

        return None

    def try_ferc_match(
        self,
        developer_name: str,
        project_name: str
    ) -> Optional[Tuple[str, float, List[str]]]:
        """
        Try matching against FERC Form 1 PPA sellers.

        Returns:
            (seller_name, confidence, buyer_utilities) or None
        """
        # Try developer name
        if developer_name:
            result = self.ferc_extractor.match_name(developer_name)
            if result.matched and result.confidence >= AUTO_APPLY_THRESHOLD:
                return (result.seller_name, result.confidence, result.buyer_utilities)

            # Try as developer keyword match
            result = self.ferc_extractor.match_developer(developer_name)
            if result.matched and result.confidence >= AUTO_APPLY_THRESHOLD:
                return (result.seller_name, result.confidence, result.buyer_utilities)

        # Try project name
        if project_name:
            result = self.ferc_extractor.match_name(project_name)
            if result.matched and result.confidence >= AUTO_APPLY_THRESHOLD:
                return (result.seller_name, result.confidence, result.buyer_utilities)

        return None

    def enrich_project(self, project: Dict) -> EnrichmentResult:
        """
        Enrich a single project with developer information.

        Tries sources in order until a high-confidence match is found.

        Args:
            project: Project dictionary with queue_id, region, etc.

        Returns:
            EnrichmentResult with match details
        """
        queue_id = project['queue_id']
        region = project['region']
        project_name = project.get('project_name')
        developer_name = project.get('developer_name')
        capacity_mw = project.get('capacity_mw') or 0
        fuel_type = project.get('fuel_type')
        state = project.get('state')
        county = project.get('county')

        result = EnrichmentResult(
            queue_id=queue_id,
            region=region
        )

        # Check for existing assignment
        existing = self.get_existing_assignment(queue_id, region)
        if existing and existing.get('verified'):
            # Already verified, don't change
            result.matched = True
            result.filing_entity = existing['filing_entity']
            result.parent_company = existing['parent_company']
            result.confidence = existing['confidence']
            result.source = 'verified'
            return result

        # Collect all potential matches for review queue
        candidates = []

        # Source 1: Registry exact/fuzzy match
        registry_match = self.try_registry_match(developer_name, project_name)
        if registry_match:
            entity, parent, conf, parent_id = registry_match

            self.log_match_attempt(
                queue_id, region, project_name, developer_name,
                entity, parent, conf, 'registry', 'fuzzy', applied=(conf >= AUTO_APPLY_THRESHOLD)
            )

            if conf >= AUTO_APPLY_THRESHOLD:
                self.apply_assignment(
                    queue_id, region, entity, parent, parent_id, conf, 'registry'
                )
                result.matched = True
                result.filing_entity = entity
                result.parent_company = parent
                result.confidence = conf
                result.source = 'registry'
                result.match_method = 'fuzzy'
                return result

            candidates.append(CandidateMatch(
                entity_name=entity,
                parent_company=parent,
                confidence=conf,
                source='registry',
                match_method='fuzzy'
            ))

        # Source 2: EIA Form 860
        eia_match = self.try_eia_match(state, county, capacity_mw, fuel_type)
        if eia_match:
            owner, conf = eia_match

            # Try to resolve owner to parent company
            registry_result = self.matcher.match(owner)
            parent = registry_result.parent_company if registry_result.matched else owner
            parent_id = registry_result.parent_id if registry_result.matched else None

            self.log_match_attempt(
                queue_id, region, project_name, developer_name,
                owner, parent, conf, 'eia', 'location', applied=(conf >= AUTO_APPLY_THRESHOLD)
            )

            if conf >= AUTO_APPLY_THRESHOLD:
                self.apply_assignment(
                    queue_id, region, owner, parent, parent_id, conf, 'eia'
                )
                result.matched = True
                result.filing_entity = owner
                result.parent_company = parent
                result.confidence = conf
                result.source = 'eia'
                result.match_method = 'location'
                return result

            candidates.append(CandidateMatch(
                entity_name=owner,
                parent_company=parent,
                confidence=conf,
                source='eia',
                match_method='location'
            ))

        # Source 3: FERC Form 1 PPAs
        ferc_match = self.try_ferc_match(developer_name, project_name)
        if ferc_match:
            seller, conf, buyers = ferc_match

            # Try to resolve seller to parent company
            registry_result = self.matcher.match(seller)
            parent = registry_result.parent_company if registry_result.matched else seller
            parent_id = registry_result.parent_id if registry_result.matched else None

            self.log_match_attempt(
                queue_id, region, project_name, developer_name,
                seller, parent, conf, 'ferc', 'ppa_seller',
                alternatives=[{'buyers': buyers}],
                applied=(conf >= AUTO_APPLY_THRESHOLD)
            )

            if conf >= AUTO_APPLY_THRESHOLD:
                self.apply_assignment(
                    queue_id, region, seller, parent, parent_id, conf, 'ferc'
                )
                result.matched = True
                result.filing_entity = seller
                result.parent_company = parent
                result.confidence = conf
                result.source = 'ferc'
                result.match_method = 'ppa_seller'
                return result

            candidates.append(CandidateMatch(
                entity_name=seller,
                parent_company=parent,
                confidence=conf,
                source='ferc',
                match_method='ppa_seller',
                supporting_data={'buyers': buyers}
            ))

        # Check for conflicts between candidates
        if len(candidates) >= 2:
            # Different sources gave different answers
            for i, c1 in enumerate(candidates):
                for c2 in candidates[i+1:]:
                    if c1.parent_company != c2.parent_company:
                        self.review_queue.add_conflict(
                            queue_id, region,
                            c1.source, c1.entity_name, c1.confidence,
                            c2.source, c2.entity_name, c2.confidence
                        )
                        result.conflict_detected = True

        # Queue for review if we have candidates but none met threshold
        if candidates:
            top_conf = max(c.confidence for c in candidates)
            if top_conf >= REVIEW_THRESHOLD:
                self.review_queue.add_for_review(
                    queue_id=queue_id,
                    region=region,
                    project_name=project_name,
                    raw_developer_name=developer_name,
                    capacity_mw=capacity_mw,
                    fuel_type=fuel_type,
                    state=state,
                    candidates=candidates
                )
                result.queued_for_review = True

        return result

    def run(
        self,
        region: Optional[str] = None,
        limit: Optional[int] = None,
        status: str = 'Active'
    ) -> PipelineStats:
        """
        Run the enrichment pipeline.

        Args:
            region: Filter by ISO/RTO (or all if None)
            limit: Maximum projects to process
            status: Queue status filter

        Returns:
            PipelineStats with results
        """
        logger.info(f"Starting enrichment pipeline (region={region or 'all'})")

        # Initialize data sources
        logger.info("Loading data sources...")
        self.matcher.load_cache()
        self.eia_loader.build_index()
        self.ferc_extractor.build_index()

        # Get projects
        projects = self.get_projects(region=region, status=status)
        if limit:
            projects = projects[:limit]

        logger.info(f"Processing {len(projects)} projects...")

        stats = PipelineStats(
            total_projects=len(projects),
            by_source={},
            by_region={}
        )

        for i, project in enumerate(projects):
            if (i + 1) % 100 == 0:
                logger.info(f"  Progress: {i+1}/{len(projects)}")

            result = self.enrich_project(project)

            # Track statistics
            proj_region = project['region']
            stats.by_region[proj_region] = stats.by_region.get(proj_region, 0)

            if result.source == 'verified':
                stats.already_matched += 1
            elif result.matched:
                stats.new_matches += 1
                stats.by_source[result.source] = stats.by_source.get(result.source, 0) + 1
                stats.by_region[proj_region] += 1

                if result.confidence >= HIGH_CONFIDENCE:
                    stats.high_confidence += 1
                else:
                    stats.medium_confidence += 1
            elif result.queued_for_review:
                stats.queued_for_review += 1
            else:
                stats.no_match += 1

            if result.conflict_detected:
                stats.conflicts += 1

        logger.info(f"Pipeline complete: {stats.new_matches} new matches, "
                    f"{stats.queued_for_review} queued for review")

        return stats

    def run_all(self) -> PipelineStats:
        """Run pipeline for all ISOs."""
        return self.run(region=None)

    def get_coverage_stats(self) -> Dict[str, Any]:
        """Get developer coverage statistics."""
        conn = sqlite3.connect(self.enrichment_db)

        # Total assignments
        total = conn.execute("""
            SELECT COUNT(*) FROM developer_assignments
        """).fetchone()[0]

        # By source
        by_source = dict(conn.execute("""
            SELECT source, COUNT(*) FROM developer_assignments
            GROUP BY source
        """).fetchall())

        # By region
        by_region = dict(conn.execute("""
            SELECT region, COUNT(*) FROM developer_assignments
            GROUP BY region
        """).fetchall())

        # By parent company (top 20)
        top_parents = conn.execute("""
            SELECT parent_company, COUNT(*) as count
            FROM developer_assignments
            WHERE parent_company IS NOT NULL
            GROUP BY parent_company
            ORDER BY count DESC
            LIMIT 20
        """).fetchall()

        # Average confidence
        avg_conf = conn.execute("""
            SELECT AVG(confidence) FROM developer_assignments
        """).fetchone()[0] or 0

        # Verified count
        verified = conn.execute("""
            SELECT COUNT(*) FROM developer_assignments WHERE verified = 1
        """).fetchone()[0]

        conn.close()

        return {
            'total_assignments': total,
            'by_source': by_source,
            'by_region': by_region,
            'top_parent_companies': [(r[0], r[1]) for r in top_parents],
            'average_confidence': avg_conf,
            'verified_count': verified,
        }

    def close(self):
        """Close all connections."""
        if self._matcher:
            self._matcher.close()
        if self._eia_loader:
            self._eia_loader.close()
        if self._ferc_extractor:
            self._ferc_extractor.close()
        if self._review_queue:
            self._review_queue.close()


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI for enrichment pipeline."""
    import argparse

    parser = argparse.ArgumentParser(description="Developer Enrichment Pipeline")
    parser.add_argument('--run', action='store_true', help='Run the pipeline')
    parser.add_argument('--region', type=str, help='Filter by ISO/RTO')
    parser.add_argument('--limit', type=int, help='Maximum projects to process')
    parser.add_argument('--stats', action='store_true', help='Show coverage statistics')

    args = parser.parse_args()

    pipeline = DeveloperEnrichmentPipeline()

    if args.run:
        stats = pipeline.run(region=args.region, limit=args.limit)

        print("\n=== Pipeline Results ===")
        print(f"Total projects: {stats.total_projects}")
        print(f"Already matched: {stats.already_matched}")
        print(f"New matches: {stats.new_matches}")
        print(f"  High confidence (>0.95): {stats.high_confidence}")
        print(f"  Medium confidence (0.85-0.95): {stats.medium_confidence}")
        print(f"Queued for review: {stats.queued_for_review}")
        print(f"No match: {stats.no_match}")
        print(f"Conflicts detected: {stats.conflicts}")

        if stats.by_source:
            print("\nMatches by source:")
            for source, count in sorted(stats.by_source.items(), key=lambda x: -x[1]):
                print(f"  {source}: {count}")

    if args.stats:
        coverage = pipeline.get_coverage_stats()

        print("\n=== Developer Coverage Statistics ===")
        print(f"Total assignments: {coverage['total_assignments']}")
        print(f"Verified: {coverage['verified_count']}")
        print(f"Average confidence: {coverage['average_confidence']:.1%}")

        if coverage['by_source']:
            print("\nAssignments by source:")
            for source, count in sorted(coverage['by_source'].items(), key=lambda x: -x[1]):
                print(f"  {source}: {count}")

        if coverage['by_region']:
            print("\nAssignments by region:")
            for region, count in sorted(coverage['by_region'].items(), key=lambda x: -x[1]):
                print(f"  {region}: {count}")

        if coverage['top_parent_companies']:
            print("\nTop 20 parent companies:")
            for parent, count in coverage['top_parent_companies']:
                print(f"  {parent}: {count} projects")

    pipeline.close()


if __name__ == '__main__':
    main()
