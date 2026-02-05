"""
PPA Discovery Module - Match queue projects with FERC Form 1 purchased power data.

Uses PUDL database to find potential PPA relationships between:
- Queue project developers/project names
- FERC Form 1 Schedule 326 sellers (purchased power agreements)

This helps identify:
1. Projects that may already have offtake agreements
2. Developer track records (past PPAs signed)
3. Utility buyer preferences and patterns
"""

import sqlite3
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
import re
from collections import defaultdict
from difflib import SequenceMatcher


# Paths
PUDL_PATH = Path(__file__).parent / '.cache' / 'pudl' / 'pudl.sqlite'
V2_PATH = Path(__file__).parent / '.data' / 'queue_v2.db'


@dataclass
class PPAMatch:
    """Represents a potential PPA match between queue project and FERC seller."""
    queue_id: str
    project_name: str
    developer: str
    region: str
    capacity_mw: float

    # FERC data
    seller_name: str
    utility_buyer: str
    report_year: int
    purchased_mwh: Optional[float]
    purchase_type: Optional[str]

    # Match metadata
    match_type: str  # 'exact_developer', 'fuzzy_developer', 'project_name', 'fuzzy_project'
    confidence: float  # 0.0 to 1.0


def normalize_name(name: str) -> str:
    """Normalize company/project name for matching."""
    if not name:
        return ''

    # Lowercase
    s = name.lower().strip()

    # Remove common suffixes
    suffixes = [
        r'\s+llc\.?$', r'\s+inc\.?$', r'\s+corp\.?$', r'\s+corporation$',
        r'\s+company$', r'\s+co\.?$', r'\s+lp\.?$', r'\s+limited$',
        r'\s+ltd\.?$', r'\s+holdings?$', r'\s+group$', r'\s+partners?$',
        r'\s+energy$', r'\s+power$', r'\s+renewables?$', r'\s+generation$',
        r'\s+solar$', r'\s+wind$', r'\s+project$', r'\s+projects?$',
        r'\s+i+$', r'\s+[ivx]+$',  # Roman numerals
        r'\s+\d+$',  # Trailing numbers
    ]

    for suffix in suffixes:
        s = re.sub(suffix, '', s)

    # Remove punctuation and extra spaces
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()

    return s


def similarity_score(s1: str, s2: str) -> float:
    """Calculate similarity between two strings."""
    if not s1 or not s2:
        return 0.0
    return SequenceMatcher(None, s1, s2).ratio()


class PPADiscovery:
    """Discover PPA relationships from FERC Form 1 data."""

    def __init__(self):
        self.pudl_conn = None
        self.v2_conn = None
        self.ferc_sellers = {}  # normalized_name -> list of records
        self.seller_word_index = defaultdict(set)  # word -> set of normalized seller names
        self.queue_projects = []
        self.matches = []

    def connect(self):
        """Connect to databases."""
        if not PUDL_PATH.exists():
            raise FileNotFoundError(f"PUDL database not found at {PUDL_PATH}")
        if not V2_PATH.exists():
            raise FileNotFoundError(f"V2 database not found at {V2_PATH}")

        self.pudl_conn = sqlite3.connect(PUDL_PATH)
        self.pudl_conn.row_factory = sqlite3.Row

        self.v2_conn = sqlite3.connect(V2_PATH)
        self.v2_conn.row_factory = sqlite3.Row

        print(f"Connected to PUDL: {PUDL_PATH}")
        print(f"Connected to V2: {V2_PATH}")

    def load_ferc_sellers(self, min_year: int = 2015):
        """Load FERC Form 1 purchased power sellers."""
        print(f"\nLoading FERC sellers (year >= {min_year})...")

        query = """
            SELECT
                seller_name,
                utility_name_ferc1 as utility_buyer,
                report_year,
                purchased_mwh,
                purchase_type_code
            FROM out_ferc1__yearly_purchased_power_and_exchanges_sched326
            WHERE report_year >= ?
              AND seller_name IS NOT NULL
              AND seller_name != ''
            ORDER BY report_year DESC
        """

        cursor = self.pudl_conn.execute(query, (min_year,))
        rows = cursor.fetchall()

        # Index by normalized seller name
        self.ferc_sellers = defaultdict(list)
        self.seller_word_index = defaultdict(set)

        for row in rows:
            norm_name = normalize_name(row['seller_name'])
            if norm_name:
                self.ferc_sellers[norm_name].append(dict(row))
                # Build word index for fast fuzzy matching
                for word in norm_name.split():
                    if len(word) >= 3:  # Skip short words
                        self.seller_word_index[word].add(norm_name)

        print(f"  Loaded {len(rows):,} FERC records")
        print(f"  {len(self.ferc_sellers):,} unique normalized sellers")
        print(f"  {len(self.seller_word_index):,} indexed words")

        return len(self.ferc_sellers)

    def load_queue_projects(self, status_filter: list = None):
        """Load queue projects from V2 database."""
        print("\nLoading queue projects...")

        query = """
            SELECT
                p.queue_id,
                p.project_name,
                d.canonical_name as developer,
                r.region_code as region,
                p.capacity_mw,
                s.status_name as status
            FROM fact_projects p
            LEFT JOIN dim_developers d ON p.developer_id = d.developer_id
            LEFT JOIN dim_regions r ON p.region_id = r.region_id
            LEFT JOIN dim_statuses s ON p.status_id = s.status_id
        """

        if status_filter:
            placeholders = ','.join('?' * len(status_filter))
            query += f" WHERE s.status_name IN ({placeholders})"
            cursor = self.v2_conn.execute(query, status_filter)
        else:
            cursor = self.v2_conn.execute(query)

        self.queue_projects = [dict(row) for row in cursor.fetchall()]
        print(f"  Loaded {len(self.queue_projects):,} projects")

        # Count with developer names
        with_dev = sum(1 for p in self.queue_projects if p['developer'])
        print(f"  {with_dev:,} have developer names ({100*with_dev/len(self.queue_projects):.1f}%)")

        return len(self.queue_projects)

    def _get_fuzzy_candidates(self, norm_name: str) -> set:
        """Get candidate sellers that share words with the name (for faster matching)."""
        candidates = set()
        words = norm_name.split()
        for word in words:
            if len(word) >= 3 and word in self.seller_word_index:
                candidates.update(self.seller_word_index[word])
        return candidates

    def find_matches(self, min_confidence: float = 0.7):
        """Find matches between queue projects and FERC sellers."""
        print(f"\nFinding PPA matches (min confidence: {min_confidence})...")

        self.matches = []
        matched_projects = set()
        total = len(self.queue_projects)

        for i, project in enumerate(self.queue_projects):
            if (i + 1) % 5000 == 0:
                print(f"  Progress: {i+1:,}/{total:,} ({100*(i+1)/total:.1f}%)")

            queue_id = project['queue_id']
            developer = project['developer'] or ''
            project_name = project['project_name'] or ''

            norm_dev = normalize_name(developer)
            norm_proj = normalize_name(project_name)

            best_match = None
            best_confidence = 0.0
            best_type = None

            # 1. Exact developer match
            if norm_dev and norm_dev in self.ferc_sellers:
                ferc_records = self.ferc_sellers[norm_dev]
                best_match = ferc_records[0]  # Most recent year
                best_confidence = 1.0
                best_type = 'exact_developer'

            # 2. Fuzzy developer match (word-indexed candidates only)
            elif norm_dev:
                candidates = self._get_fuzzy_candidates(norm_dev)
                for seller_norm in candidates:
                    score = similarity_score(norm_dev, seller_norm)
                    if score > best_confidence and score >= min_confidence:
                        best_match = self.ferc_sellers[seller_norm][0]
                        best_confidence = score
                        best_type = 'fuzzy_developer'

            # 3. Project name match (if no developer match found)
            if not best_match and norm_proj:
                # Exact project name
                if norm_proj in self.ferc_sellers:
                    ferc_records = self.ferc_sellers[norm_proj]
                    best_match = ferc_records[0]
                    best_confidence = 0.9  # Slightly lower than exact developer
                    best_type = 'project_name'
                else:
                    # Fuzzy project name (word-indexed candidates only)
                    candidates = self._get_fuzzy_candidates(norm_proj)
                    for seller_norm in candidates:
                        score = similarity_score(norm_proj, seller_norm)
                        if score > best_confidence and score >= min_confidence:
                            best_match = self.ferc_sellers[seller_norm][0]
                            best_confidence = score
                            best_type = 'fuzzy_project'

            if best_match and best_confidence >= min_confidence:
                match = PPAMatch(
                    queue_id=queue_id,
                    project_name=project_name,
                    developer=developer,
                    region=project['region'],
                    capacity_mw=project['capacity_mw'] or 0,
                    seller_name=best_match['seller_name'],
                    utility_buyer=best_match['utility_buyer'],
                    report_year=best_match['report_year'],
                    purchased_mwh=best_match['purchased_mwh'],
                    purchase_type=best_match['purchase_type_code'],
                    match_type=best_type,
                    confidence=best_confidence
                )
                self.matches.append(match)
                matched_projects.add(queue_id)

        print(f"  Found {len(self.matches):,} potential PPA matches")
        print(f"  {len(matched_projects):,} unique projects matched")

        # Breakdown by match type
        by_type = defaultdict(int)
        for m in self.matches:
            by_type[m.match_type] += 1

        print("\n  Match breakdown:")
        for match_type, count in sorted(by_type.items(), key=lambda x: -x[1]):
            print(f"    {match_type}: {count:,}")

        return self.matches

    def save_matches(self, output_table: str = 'ppa_matches'):
        """Save matches to V2 database."""
        print(f"\nSaving matches to {output_table}...")

        # Create table if not exists
        self.v2_conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {output_table} (
                queue_id TEXT,
                project_name TEXT,
                developer TEXT,
                region TEXT,
                capacity_mw REAL,
                seller_name TEXT,
                utility_buyer TEXT,
                report_year INTEGER,
                purchased_mwh REAL,
                purchase_type TEXT,
                match_type TEXT,
                confidence REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (queue_id, seller_name, report_year)
            )
        """)

        # Clear existing and insert new
        self.v2_conn.execute(f"DELETE FROM {output_table}")

        for m in self.matches:
            self.v2_conn.execute(f"""
                INSERT OR REPLACE INTO {output_table}
                (queue_id, project_name, developer, region, capacity_mw,
                 seller_name, utility_buyer, report_year, purchased_mwh,
                 purchase_type, match_type, confidence)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                m.queue_id, m.project_name, m.developer, m.region, m.capacity_mw,
                m.seller_name, m.utility_buyer, m.report_year, m.purchased_mwh,
                m.purchase_type, m.match_type, m.confidence
            ))

        self.v2_conn.commit()
        print(f"  Saved {len(self.matches):,} matches")

    def get_developer_ppa_history(self, developer_name: str) -> list:
        """Get PPA history for a specific developer."""
        norm_dev = normalize_name(developer_name)

        history = []
        for seller_norm, records in self.ferc_sellers.items():
            if similarity_score(norm_dev, seller_norm) >= 0.8:
                history.extend(records)

        # Sort by year descending
        history.sort(key=lambda x: -x['report_year'])
        return history

    def get_utility_buying_patterns(self, utility_name: str) -> dict:
        """Analyze buying patterns for a specific utility."""
        query = """
            SELECT
                report_year,
                purchase_type_code,
                COUNT(*) as num_contracts,
                SUM(purchased_mwh) as total_mwh
            FROM out_ferc1__yearly_purchased_power_and_exchanges_sched326
            WHERE utility_name_ferc1 LIKE ?
              AND report_year >= 2015
            GROUP BY report_year, purchase_type_code
            ORDER BY report_year DESC
        """

        cursor = self.pudl_conn.execute(query, (f'%{utility_name}%',))

        patterns = defaultdict(lambda: defaultdict(dict))
        for row in cursor:
            year = row['report_year']
            ptype = row['purchase_type_code'] or 'unknown'
            patterns[year][ptype] = {
                'contracts': row['num_contracts'],
                'total_mwh': row['total_mwh']
            }

        return dict(patterns)

    def close(self):
        """Close database connections."""
        if self.pudl_conn:
            self.pudl_conn.close()
        if self.v2_conn:
            self.v2_conn.close()


def run_discovery(min_confidence: float = 0.75, min_year: int = 2015,
                  status_filter: list = None, save: bool = True):
    """Run full PPA discovery pipeline."""
    print("=" * 60)
    print("PPA Discovery - FERC Form 1 to Queue Project Matching")
    print("=" * 60)

    discovery = PPADiscovery()

    try:
        discovery.connect()
        discovery.load_ferc_sellers(min_year=min_year)
        discovery.load_queue_projects(status_filter=status_filter)
        matches = discovery.find_matches(min_confidence=min_confidence)

        if save and matches:
            discovery.save_matches()

        # Summary stats
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)

        if matches:
            # High confidence matches
            high_conf = [m for m in matches if m.confidence >= 0.9]
            print(f"\nHigh confidence matches (>=0.9): {len(high_conf):,}")

            # By region
            by_region = defaultdict(int)
            for m in matches:
                by_region[m.region] += 1

            print("\nMatches by region:")
            for region, count in sorted(by_region.items(), key=lambda x: -x[1]):
                print(f"  {region}: {count:,}")

            # Sample matches
            print("\nSample high-confidence matches:")
            for m in high_conf[:5]:
                print(f"\n  Queue: {m.queue_id} ({m.region})")
                print(f"    Developer: {m.developer}")
                print(f"    FERC Seller: {m.seller_name}")
                print(f"    Buyer: {m.utility_buyer}")
                print(f"    Year: {m.report_year}, Type: {m.match_type}")
                print(f"    Confidence: {m.confidence:.2f}")

        return matches

    finally:
        discovery.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='PPA Discovery from FERC Form 1 data')
    parser.add_argument('--min-confidence', type=float, default=0.75,
                        help='Minimum match confidence (0-1, default: 0.75)')
    parser.add_argument('--min-year', type=int, default=2015,
                        help='Minimum FERC report year (default: 2015)')
    parser.add_argument('--active-only', action='store_true',
                        help='Only match active queue projects')
    parser.add_argument('--no-save', action='store_true',
                        help='Do not save matches to database')
    parser.add_argument('--developer', type=str,
                        help='Look up PPA history for specific developer')
    parser.add_argument('--utility', type=str,
                        help='Analyze buying patterns for specific utility')

    args = parser.parse_args()

    if args.developer:
        # Developer lookup mode
        discovery = PPADiscovery()
        discovery.connect()
        discovery.load_ferc_sellers(min_year=args.min_year)

        history = discovery.get_developer_ppa_history(args.developer)
        print(f"\nPPA History for '{args.developer}':")
        print(f"Found {len(history)} records\n")

        for record in history[:20]:
            print(f"  {record['report_year']}: {record['utility_buyer']}")
            print(f"    {record['purchased_mwh']:,.0f} MWh" if record['purchased_mwh'] else "    MWh: N/A")
            print(f"    Type: {record['purchase_type_code']}")
            print()

        discovery.close()

    elif args.utility:
        # Utility analysis mode
        discovery = PPADiscovery()
        discovery.connect()

        patterns = discovery.get_utility_buying_patterns(args.utility)
        print(f"\nBuying Patterns for '{args.utility}':")

        for year in sorted(patterns.keys(), reverse=True):
            print(f"\n  {year}:")
            for ptype, data in patterns[year].items():
                mwh = data['total_mwh']
                mwh_str = f"{mwh:,.0f} MWh" if mwh else "N/A"
                print(f"    {ptype}: {data['contracts']} contracts, {mwh_str}")

        discovery.close()

    else:
        # Full discovery mode
        status_filter = ['Active', 'Under Construction'] if args.active_only else None
        run_discovery(
            min_confidence=args.min_confidence,
            min_year=args.min_year,
            status_filter=status_filter,
            save=not args.no_save
        )
