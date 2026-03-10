#!/usr/bin/env python3
"""
Developer Matcher - Accuracy-First Fuzzy Matching Engine

Provides high-confidence developer name matching with:
- Two-tier entity model (filing entity + parent company)
- Multi-algorithm fuzzy matching (RapidFuzz)
- Confidence scoring with strict thresholds
- Audit trail for all matches

Thresholds (accuracy first):
- >= 0.95: Auto-apply match
- 0.85-0.95: Apply with secondary confirmation
- 0.75-0.85: Queue for human review
- < 0.75: No match

Usage:
    from developer_matcher import DeveloperMatcher

    matcher = DeveloperMatcher()
    match = matcher.match("NextEra Energy Resources LLC")
    # Returns: MatchResult(entity='NextEra Energy Resources', parent='NextEra Energy', confidence=0.98)
"""

import sqlite3
import re
import json
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import logging

try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    logging.warning("rapidfuzz not installed. Run: pip install rapidfuzz")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / '.data'


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class MatchResult:
    """Result of a developer name match."""
    matched: bool
    entity_name: Optional[str] = None
    entity_id: Optional[int] = None
    parent_company: Optional[str] = None
    parent_id: Optional[int] = None
    confidence: float = 0.0
    match_method: str = 'none'
    subscores: Optional[Dict[str, float]] = None
    alternatives: Optional[List[Dict]] = None
    auto_apply: bool = False
    needs_review: bool = False


@dataclass
class EntityRecord:
    """A filing entity record."""
    entity_id: int
    entity_name: str
    normalized_name: str
    entity_type: Optional[str]
    parent_company_id: Optional[int]
    parent_company_name: Optional[str]


# =============================================================================
# SCHEMA DEFINITION
# =============================================================================

DEVELOPER_MATCHING_SCHEMA = """
-- Ultimate parent companies (the actual business entities)
CREATE TABLE IF NOT EXISTS dim_parent_companies (
    company_id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL UNIQUE,
    short_name TEXT,
    company_type TEXT,  -- 'IPP', 'Utility', 'PE_Fund', 'Yieldco', 'Infrastructure'
    ticker_symbol TEXT,
    eia_utility_id INTEGER,
    headquarters_state TEXT,
    headquarters_country TEXT DEFAULT 'US',
    website TEXT,
    is_public BOOLEAN,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Filing entities (SPVs, project-specific LLCs)
CREATE TABLE IF NOT EXISTS dim_filing_entities (
    entity_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_name TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    entity_type TEXT,  -- 'SPV', 'LLC', 'Corporation', 'Partnership', 'Unknown'
    parent_company_id INTEGER REFERENCES dim_parent_companies(company_id),
    ownership_pct REAL,
    confidence REAL,
    confidence_source TEXT,  -- JSON: sources that contributed to confidence
    state_of_formation TEXT,
    verified_by TEXT,
    verified_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(normalized_name)
);

-- Alias names for fuzzy matching
CREATE TABLE IF NOT EXISTS dim_entity_aliases (
    alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER REFERENCES dim_filing_entities(entity_id),
    parent_company_id INTEGER REFERENCES dim_parent_companies(company_id),
    alias_name TEXT NOT NULL,
    normalized_alias TEXT NOT NULL,
    source TEXT,  -- 'eia', 'ferc', 'lbl', 'manual', 'queue', 'inferred'
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(normalized_alias)
);

-- Match audit log - tracks every match decision
CREATE TABLE IF NOT EXISTS developer_match_audit (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id TEXT NOT NULL,
    region TEXT NOT NULL,
    raw_developer_name TEXT,
    matched_entity_id INTEGER REFERENCES dim_filing_entities(entity_id),
    matched_parent_id INTEGER REFERENCES dim_parent_companies(company_id),
    match_method TEXT,  -- 'exact', 'fuzzy', 'eia_lookup', 'ferc_lookup', 'manual', 'alias'
    confidence REAL,
    confidence_factors TEXT,  -- JSON: breakdown of confidence contributors
    alternative_matches TEXT,  -- JSON: other possible matches considered
    matched_at TEXT DEFAULT CURRENT_TIMESTAMP,
    matched_by TEXT DEFAULT 'system',  -- 'system' or user identifier
    verified BOOLEAN DEFAULT FALSE,
    verified_by TEXT,
    verified_at TEXT,
    UNIQUE(queue_id, region)
);

-- Manual review queue for uncertain matches
CREATE TABLE IF NOT EXISTS developer_review_queue (
    review_id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id TEXT NOT NULL,
    region TEXT NOT NULL,
    raw_developer_name TEXT,
    capacity_mw REAL,
    candidate_matches TEXT,  -- JSON: list of potential matches with confidence
    priority INTEGER DEFAULT 50,  -- 1=highest, 100=lowest
    status TEXT DEFAULT 'pending',  -- 'pending', 'in_progress', 'approved', 'rejected', 'skipped'
    assigned_to TEXT,
    selected_entity_id INTEGER,
    selected_parent_id INTEGER,
    notes TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    resolved_at TEXT,
    resolved_by TEXT,
    UNIQUE(queue_id, region)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_filing_entities_normalized ON dim_filing_entities(normalized_name);
CREATE INDEX IF NOT EXISTS idx_filing_entities_parent ON dim_filing_entities(parent_company_id);
CREATE INDEX IF NOT EXISTS idx_aliases_normalized ON dim_entity_aliases(normalized_alias);
CREATE INDEX IF NOT EXISTS idx_audit_queue ON developer_match_audit(queue_id, region);
CREATE INDEX IF NOT EXISTS idx_review_status ON developer_review_queue(status);
CREATE INDEX IF NOT EXISTS idx_review_priority ON developer_review_queue(priority);
"""


# =============================================================================
# NORMALIZATION
# =============================================================================

class NameNormalizer:
    """Normalize developer/company names for matching."""

    # Suffixes to remove (order matters - longer patterns first)
    SUFFIX_PATTERNS = [
        # Legal entity suffixes with variations
        r',?\s*(Limited\s+Liability\s+Company|Limited\s+Partnership|Limited\s+Liability\s+Partnership)\.?\s*$',
        r',?\s*(L\.?L\.?C\.?|Inc\.?|Corp\.?|Corporation|Company|Co\.?|Ltd\.?|Limited|L\.?P\.?|P\.?L\.?C\.?)\.?\s*$',
        # Geographic/phase indicators
        r'\s+(Phase\s*[IVX\d]+|Unit\s*\d+|Project\s*\d+)\s*$',
        r'\s+(I{1,3}|IV|V|VI{1,3}|IX|X)\s*$',  # Roman numerals at end
        # Holdings/parent indicators
        r',?\s*(Holdings?|Holdco|Parent|Intermediate|Borrower)\s*$',
        # Parenthetical notes
        r'\s*\([^)]*\)\s*$',
        # Trailing punctuation
        r'[,;\.]+\s*$',
    ]

    # Words that indicate parent company vs SPV
    PARENT_INDICATORS = {'energy', 'power', 'renewables', 'resources', 'group', 'holdings', 'corporation', 'utilities'}
    SPV_INDICATORS = {'project', 'farm', 'facility', 'plant', 'station', 'phase', 'unit', 'solar', 'wind', 'storage'}

    # Acronyms to preserve in title case
    ACRONYMS = {'LLC', 'LP', 'PLC', 'USA', 'US', 'PV', 'PPA', 'BESS', 'ESS',
                'PGE', 'SCE', 'SDGE', 'APS', 'TVA', 'AES', 'EDF', 'BP', 'NRG',
                'RWE', 'EDP', 'IPP', 'ITC', 'AEP', 'DTE', 'PPL', 'WEC'}

    @classmethod
    def normalize(cls, name: str) -> str:
        """
        Normalize a developer/company name for matching.

        Steps:
        1. Lowercase and strip
        2. Remove legal suffixes
        3. Collapse whitespace
        4. Remove special characters (keep alphanumeric and spaces)

        Args:
            name: Raw name string

        Returns:
            Normalized name for matching
        """
        if not name or not isinstance(name, str):
            return ''

        # Start with lowercase and strip
        normalized = name.lower().strip()

        # Apply suffix patterns
        for pattern in cls.SUFFIX_PATTERNS:
            normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE)

        # Remove special characters (keep letters, numbers, spaces)
        normalized = re.sub(r'[^a-z0-9\s]', ' ', normalized)

        # Collapse multiple spaces
        normalized = ' '.join(normalized.split())

        return normalized.strip()

    @classmethod
    def to_display_name(cls, normalized: str) -> str:
        """
        Convert normalized name to display format (title case with preserved acronyms).

        Args:
            normalized: Normalized name string

        Returns:
            Display-friendly name
        """
        if not normalized:
            return ''

        words = normalized.split()
        result = []

        for word in words:
            upper = word.upper()
            if upper in cls.ACRONYMS:
                result.append(upper)
            elif word in {'of', 'the', 'and', 'for', 'in', 'on', 'at', 'to', 'a', 'an'}:
                result.append(word)
            else:
                result.append(word.capitalize())

        return ' '.join(result)

    @classmethod
    def classify_entity_type(cls, name: str) -> str:
        """
        Classify whether a name appears to be a parent company or SPV.

        Args:
            name: Raw or normalized name

        Returns:
            'parent', 'spv', or 'unknown'
        """
        name_lower = name.lower()
        words = set(name_lower.split())

        spv_score = len(words & cls.SPV_INDICATORS)
        parent_score = len(words & cls.PARENT_INDICATORS)

        # Check for project-like patterns
        if re.search(r'\d{2,}', name) or re.search(r'phase|unit|project', name_lower):
            spv_score += 2

        # Check for entity suffixes
        if re.search(r'llc|l\.l\.c|lp|l\.p', name_lower):
            spv_score += 1

        if spv_score > parent_score:
            return 'SPV'
        elif parent_score > spv_score:
            return 'Corporation'
        else:
            return 'Unknown'


# =============================================================================
# FUZZY MATCHER
# =============================================================================

class DeveloperMatcher:
    """
    High-accuracy developer name matching engine.

    Uses multiple fuzzy matching algorithms with strict thresholds
    to ensure accuracy over coverage.
    """

    # Confidence thresholds (accuracy first)
    EXACT_THRESHOLD = 100
    HIGH_CONFIDENCE = 95      # Auto-apply
    MEDIUM_CONFIDENCE = 85    # Apply with confirmation
    REVIEW_THRESHOLD = 75     # Queue for human review

    def __init__(self, db_path: str = None):
        """Initialize matcher with database connection."""
        if db_path is None:
            db_path = DATA_DIR / 'queue_v2.db'
        self.db_path = str(db_path)
        self._conn = None
        self._entity_cache = {}
        self._parent_cache = {}
        self._alias_cache = {}
        self._normalized_lookup = {}

        # Ensure schema exists
        self._init_schema()

    def _get_conn(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _init_schema(self):
        """Initialize database schema for developer matching."""
        conn = self._get_conn()
        conn.executescript(DEVELOPER_MATCHING_SCHEMA)
        conn.commit()
        logger.info("Developer matching schema initialized")

    def load_cache(self):
        """Load entities, parents, and aliases into memory for fast matching."""
        conn = self._get_conn()

        # Load parent companies
        cursor = conn.execute("SELECT company_id, company_name, short_name FROM dim_parent_companies")
        for row in cursor:
            self._parent_cache[row['company_id']] = dict(row)
            normalized = NameNormalizer.normalize(row['company_name'])
            self._normalized_lookup[normalized] = ('parent', row['company_id'])
            if row['short_name']:
                short_norm = NameNormalizer.normalize(row['short_name'])
                self._normalized_lookup[short_norm] = ('parent', row['company_id'])

        # Load filing entities
        cursor = conn.execute("""
            SELECT e.entity_id, e.entity_name, e.normalized_name, e.entity_type,
                   e.parent_company_id, p.company_name as parent_company_name
            FROM dim_filing_entities e
            LEFT JOIN dim_parent_companies p ON e.parent_company_id = p.company_id
        """)
        for row in cursor:
            self._entity_cache[row['entity_id']] = dict(row)
            self._normalized_lookup[row['normalized_name']] = ('entity', row['entity_id'])

        # Load aliases
        cursor = conn.execute("SELECT alias_id, entity_id, parent_company_id, normalized_alias FROM dim_entity_aliases")
        for row in cursor:
            self._alias_cache[row['normalized_alias']] = dict(row)
            if row['entity_id']:
                self._normalized_lookup[row['normalized_alias']] = ('entity', row['entity_id'])
            elif row['parent_company_id']:
                self._normalized_lookup[row['normalized_alias']] = ('parent', row['parent_company_id'])

        logger.info(f"Loaded cache: {len(self._parent_cache)} parents, {len(self._entity_cache)} entities, {len(self._alias_cache)} aliases")

    def match(self, raw_name: str) -> MatchResult:
        """
        Match a developer name to the registry.

        Tries in order:
        1. Exact normalized match
        2. Alias match
        3. Fuzzy match (if rapidfuzz available)

        Args:
            raw_name: Raw developer name from queue data

        Returns:
            MatchResult with match details and confidence
        """
        if not raw_name or not isinstance(raw_name, str):
            return MatchResult(matched=False, match_method='invalid_input')

        # Normalize input
        normalized = NameNormalizer.normalize(raw_name)
        if not normalized:
            return MatchResult(matched=False, match_method='empty_after_normalization')

        # Ensure cache is loaded
        if not self._normalized_lookup:
            self.load_cache()

        # Try exact match
        if normalized in self._normalized_lookup:
            return self._exact_match(normalized, raw_name)

        # Try fuzzy match
        if RAPIDFUZZ_AVAILABLE and self._normalized_lookup:
            return self._fuzzy_match(normalized, raw_name)

        return MatchResult(matched=False, match_method='no_match')

    def _exact_match(self, normalized: str, raw_name: str) -> MatchResult:
        """Handle exact normalized match."""
        match_type, match_id = self._normalized_lookup[normalized]

        if match_type == 'entity':
            entity = self._entity_cache.get(match_id, {})
            parent_id = entity.get('parent_company_id')
            parent = self._parent_cache.get(parent_id, {}) if parent_id else {}

            return MatchResult(
                matched=True,
                entity_name=entity.get('entity_name'),
                entity_id=match_id,
                parent_company=parent.get('company_name'),
                parent_id=parent_id,
                confidence=1.0,
                match_method='exact',
                auto_apply=True
            )
        else:  # parent
            parent = self._parent_cache.get(match_id, {})
            return MatchResult(
                matched=True,
                entity_name=parent.get('company_name'),
                parent_company=parent.get('company_name'),
                parent_id=match_id,
                confidence=1.0,
                match_method='exact_parent',
                auto_apply=True
            )

    def _fuzzy_match(self, normalized: str, raw_name: str) -> MatchResult:
        """
        Perform fuzzy matching using multiple algorithms.

        Weighted scoring:
        - fuzz.ratio: 40% (overall similarity)
        - fuzz.token_set_ratio: 30% (word overlap regardless of order)
        - fuzz.token_sort_ratio: 20% (sorted word comparison)
        - fuzz.partial_ratio: 10% (substring matching)
        """
        candidates = list(self._normalized_lookup.keys())
        if not candidates:
            return MatchResult(matched=False, match_method='no_candidates')

        # Calculate scores for all candidates
        scored_matches = []
        for candidate in candidates:
            scores = {
                'ratio': fuzz.ratio(normalized, candidate),
                'token_set': fuzz.token_set_ratio(normalized, candidate),
                'token_sort': fuzz.token_sort_ratio(normalized, candidate),
                'partial': fuzz.partial_ratio(normalized, candidate),
            }

            weighted_score = (
                scores['ratio'] * 0.4 +
                scores['token_set'] * 0.3 +
                scores['token_sort'] * 0.2 +
                scores['partial'] * 0.1
            )

            if weighted_score >= self.REVIEW_THRESHOLD:
                match_type, match_id = self._normalized_lookup[candidate]
                scored_matches.append({
                    'candidate': candidate,
                    'match_type': match_type,
                    'match_id': match_id,
                    'score': weighted_score,
                    'subscores': scores
                })

        if not scored_matches:
            return MatchResult(matched=False, match_method='below_threshold')

        # Sort by score descending
        scored_matches.sort(key=lambda x: -x['score'])
        best = scored_matches[0]

        # Determine if auto-apply or needs review
        auto_apply = best['score'] >= self.HIGH_CONFIDENCE
        needs_review = self.REVIEW_THRESHOLD <= best['score'] < self.MEDIUM_CONFIDENCE

        # Build result
        if best['match_type'] == 'entity':
            entity = self._entity_cache.get(best['match_id'], {})
            parent_id = entity.get('parent_company_id')
            parent = self._parent_cache.get(parent_id, {}) if parent_id else {}

            return MatchResult(
                matched=True,
                entity_name=entity.get('entity_name'),
                entity_id=best['match_id'],
                parent_company=parent.get('company_name'),
                parent_id=parent_id,
                confidence=best['score'] / 100,
                match_method='fuzzy',
                subscores=best['subscores'],
                alternatives=[m for m in scored_matches[1:4]],  # Top 3 alternatives
                auto_apply=auto_apply,
                needs_review=needs_review
            )
        else:  # parent
            parent = self._parent_cache.get(best['match_id'], {})
            return MatchResult(
                matched=True,
                entity_name=parent.get('company_name'),
                parent_company=parent.get('company_name'),
                parent_id=best['match_id'],
                confidence=best['score'] / 100,
                match_method='fuzzy_parent',
                subscores=best['subscores'],
                alternatives=[m for m in scored_matches[1:4]],
                auto_apply=auto_apply,
                needs_review=needs_review
            )

    # =========================================================================
    # REGISTRY MANAGEMENT
    # =========================================================================

    def add_parent_company(
        self,
        company_name: str,
        short_name: str = None,
        company_type: str = None,
        ticker: str = None,
        eia_utility_id: int = None,
        hq_state: str = None
    ) -> int:
        """
        Add a parent company to the registry.

        Returns:
            company_id of the new or existing record
        """
        conn = self._get_conn()
        normalized = NameNormalizer.normalize(company_name)

        # Check if exists
        cursor = conn.execute(
            "SELECT company_id FROM dim_parent_companies WHERE company_name = ?",
            (company_name,)
        )
        existing = cursor.fetchone()
        if existing:
            return existing['company_id']

        # Insert new
        cursor = conn.execute("""
            INSERT INTO dim_parent_companies
            (company_name, short_name, company_type, ticker_symbol, eia_utility_id, headquarters_state)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (company_name, short_name, company_type, ticker, eia_utility_id, hq_state))
        conn.commit()

        company_id = cursor.lastrowid

        # Update cache
        self._parent_cache[company_id] = {
            'company_id': company_id,
            'company_name': company_name,
            'short_name': short_name
        }
        self._normalized_lookup[normalized] = ('parent', company_id)
        if short_name:
            self._normalized_lookup[NameNormalizer.normalize(short_name)] = ('parent', company_id)

        return company_id

    def add_filing_entity(
        self,
        entity_name: str,
        parent_company_id: int = None,
        entity_type: str = None,
        confidence: float = 1.0,
        source: str = 'manual'
    ) -> int:
        """
        Add a filing entity (SPV/LLC) to the registry.

        Returns:
            entity_id of the new or existing record
        """
        conn = self._get_conn()
        normalized = NameNormalizer.normalize(entity_name)

        if not entity_type:
            entity_type = NameNormalizer.classify_entity_type(entity_name)

        # Check if exists
        cursor = conn.execute(
            "SELECT entity_id FROM dim_filing_entities WHERE normalized_name = ?",
            (normalized,)
        )
        existing = cursor.fetchone()
        if existing:
            return existing['entity_id']

        # Insert new
        cursor = conn.execute("""
            INSERT INTO dim_filing_entities
            (entity_name, normalized_name, entity_type, parent_company_id, confidence, confidence_source)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (entity_name, normalized, entity_type, parent_company_id, confidence, json.dumps({'source': source})))
        conn.commit()

        entity_id = cursor.lastrowid

        # Update cache
        parent = self._parent_cache.get(parent_company_id, {})
        self._entity_cache[entity_id] = {
            'entity_id': entity_id,
            'entity_name': entity_name,
            'normalized_name': normalized,
            'entity_type': entity_type,
            'parent_company_id': parent_company_id,
            'parent_company_name': parent.get('company_name')
        }
        self._normalized_lookup[normalized] = ('entity', entity_id)

        return entity_id

    def add_alias(
        self,
        alias_name: str,
        entity_id: int = None,
        parent_company_id: int = None,
        source: str = 'manual'
    ) -> int:
        """
        Add an alias for an entity or parent company.

        Returns:
            alias_id of the new record
        """
        conn = self._get_conn()
        normalized = NameNormalizer.normalize(alias_name)

        # Check if exists
        cursor = conn.execute(
            "SELECT alias_id FROM dim_entity_aliases WHERE normalized_alias = ?",
            (normalized,)
        )
        existing = cursor.fetchone()
        if existing:
            return existing['alias_id']

        # Insert
        cursor = conn.execute("""
            INSERT INTO dim_entity_aliases
            (alias_name, normalized_alias, entity_id, parent_company_id, source)
            VALUES (?, ?, ?, ?, ?)
        """, (alias_name, normalized, entity_id, parent_company_id, source))
        conn.commit()

        alias_id = cursor.lastrowid

        # Update cache
        self._alias_cache[normalized] = {
            'alias_id': alias_id,
            'entity_id': entity_id,
            'parent_company_id': parent_company_id,
            'normalized_alias': normalized
        }
        if entity_id:
            self._normalized_lookup[normalized] = ('entity', entity_id)
        elif parent_company_id:
            self._normalized_lookup[normalized] = ('parent', parent_company_id)

        return alias_id

    # =========================================================================
    # AUDIT & REVIEW
    # =========================================================================

    def log_match(
        self,
        queue_id: str,
        region: str,
        raw_name: str,
        result: MatchResult,
        matched_by: str = 'system'
    ):
        """Log a match decision to the audit trail."""
        conn = self._get_conn()

        conn.execute("""
            INSERT OR REPLACE INTO developer_match_audit
            (queue_id, region, raw_developer_name, matched_entity_id, matched_parent_id,
             match_method, confidence, confidence_factors, alternative_matches, matched_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            queue_id,
            region,
            raw_name,
            result.entity_id,
            result.parent_id,
            result.match_method,
            result.confidence,
            json.dumps(result.subscores) if result.subscores else None,
            json.dumps(result.alternatives) if result.alternatives else None,
            matched_by
        ))
        conn.commit()

    def add_to_review_queue(
        self,
        queue_id: str,
        region: str,
        raw_name: str,
        capacity_mw: float,
        candidates: List[Dict],
        priority: int = 50
    ):
        """Add a project to the manual review queue."""
        conn = self._get_conn()

        conn.execute("""
            INSERT OR REPLACE INTO developer_review_queue
            (queue_id, region, raw_developer_name, capacity_mw, candidate_matches, priority, status)
            VALUES (?, ?, ?, ?, ?, ?, 'pending')
        """, (
            queue_id,
            region,
            raw_name,
            capacity_mw,
            json.dumps(candidates),
            priority
        ))
        conn.commit()

    def get_review_queue(self, status: str = 'pending', limit: int = 100) -> List[Dict]:
        """Get items from the review queue."""
        conn = self._get_conn()
        cursor = conn.execute("""
            SELECT * FROM developer_review_queue
            WHERE status = ?
            ORDER BY priority ASC, created_at ASC
            LIMIT ?
        """, (status, limit))

        return [dict(row) for row in cursor.fetchall()]

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the developer registry."""
        conn = self._get_conn()

        stats = {}

        cursor = conn.execute("SELECT COUNT(*) FROM dim_parent_companies")
        stats['parent_companies'] = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(*) FROM dim_filing_entities")
        stats['filing_entities'] = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(*) FROM dim_filing_entities WHERE parent_company_id IS NOT NULL")
        stats['entities_with_parent'] = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(*) FROM dim_entity_aliases")
        stats['aliases'] = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(*) FROM developer_match_audit")
        stats['matches_logged'] = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(*) FROM developer_review_queue WHERE status = 'pending'")
        stats['pending_reviews'] = cursor.fetchone()[0]

        return stats

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI for developer matcher."""
    import argparse

    parser = argparse.ArgumentParser(description="Developer Name Matcher")
    parser.add_argument('--init', action='store_true', help='Initialize database schema')
    parser.add_argument('--match', type=str, help='Match a developer name')
    parser.add_argument('--stats', action='store_true', help='Show registry statistics')
    parser.add_argument('--load-seed', type=str, help='Load seed data from CSV file')

    args = parser.parse_args()

    matcher = DeveloperMatcher()

    if args.init:
        print("Schema initialized.")
        print(f"Database: {matcher.db_path}")

    if args.stats:
        stats = matcher.get_stats()
        print("\n=== Developer Registry Statistics ===")
        for key, value in stats.items():
            print(f"  {key}: {value:,}")

    if args.match:
        result = matcher.match(args.match)
        print(f"\n=== Match Result for '{args.match}' ===")
        print(f"  Matched: {result.matched}")
        if result.matched:
            print(f"  Entity: {result.entity_name}")
            print(f"  Parent Company: {result.parent_company}")
            print(f"  Confidence: {result.confidence:.1%}")
            print(f"  Method: {result.match_method}")
            print(f"  Auto-apply: {result.auto_apply}")
            print(f"  Needs review: {result.needs_review}")

    if args.load_seed:
        import pandas as pd
        df = pd.read_csv(args.load_seed)
        loaded = 0
        for _, row in df.iterrows():
            company_id = matcher.add_parent_company(
                company_name=row['company_name'],
                short_name=row.get('short_name'),
                company_type=row.get('company_type'),
                ticker=row.get('ticker'),
                hq_state=row.get('hq_state')
            )
            loaded += 1

            # Add aliases if present
            if 'aliases' in row and pd.notna(row['aliases']):
                for alias in str(row['aliases']).split(';'):
                    alias = alias.strip()
                    if alias:
                        matcher.add_alias(alias, parent_company_id=company_id, source='seed')

        print(f"Loaded {loaded} parent companies from {args.load_seed}")

    matcher.close()


if __name__ == '__main__':
    main()
