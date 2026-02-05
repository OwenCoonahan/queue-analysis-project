#!/usr/bin/env python3
"""
FERC PPA / Purchased Power Data Integration

Matches interconnection queue projects to FERC Form 1 purchased power contracts
to identify projects that have active PPAs (Power Purchase Agreements).

Data Source: FERC Form 1 Schedule 326 (Purchased Power)
- Contains utility reports of all power purchases
- Includes seller name, MWh purchased, contract details

Usage:
    from ferc_ppa import FERCPPAMatcher

    matcher = FERCPPAMatcher()
    matcher.load_data()

    # Check if a project has a PPA
    has_ppa = matcher.check_project_ppa("Bronco Plains Wind", "CO")

    # Enrich queue database
    matcher.enrich_database()
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent / '.cache' / 'pudl'
DATA_DIR = Path(__file__).parent / '.data'


@dataclass
class PPAMatch:
    """Represents a PPA match for a queue project."""
    seller_name: str
    total_mwh: float
    total_contracts: int
    buyers: List[str]
    match_confidence: float
    match_method: str


class FERCPPAMatcher:
    """Match queue projects to FERC purchased power (PPA) data."""

    def __init__(self):
        self.ppa_df: Optional[pd.DataFrame] = None
        self.seller_index: Dict[str, Dict] = {}

    def load_data(self) -> bool:
        """Load FERC Form 1 purchased power data."""
        ferc_db = CACHE_DIR / 'ferc1_xbrl.sqlite'

        if not ferc_db.exists():
            logger.error(f"FERC Form 1 database not found: {ferc_db}")
            logger.info("Download from: https://zenodo.org/records/17606427")
            return False

        logger.info("Loading FERC purchased power data...")

        conn = sqlite3.connect(ferc_db)

        # Load purchased power data with aggregation by seller
        self.ppa_df = pd.read_sql('''
            SELECT
                name_of_company_or_public_authority_providing_purchased_power as seller,
                entity_id as buyer_entity,
                filing_name as buyer_filing,
                start_date,
                end_date,
                megawatt_hours_purchased_other_than_storage as mwh,
                megawatt_hours_purchased_for_energy_storage as mwh_storage,
                energy_charges_of_purchased_power as energy_charges,
                demand_charges_of_purchased_power as demand_charges,
                rate_schedule_tariff_number as tariff,
                statistical_classification_code as classification
            FROM purchased_power_326_duration
            WHERE name_of_company_or_public_authority_providing_purchased_power IS NOT NULL
        ''', conn)

        conn.close()

        logger.info(f"  Loaded {len(self.ppa_df):,} PPA records")

        # Build seller index
        self._build_seller_index()

        return True

    def _build_seller_index(self):
        """Build searchable index of power sellers."""
        logger.info("Building seller index...")

        # Aggregate by seller
        agg = self.ppa_df.groupby('seller').agg({
            'mwh': 'sum',
            'mwh_storage': 'sum',
            'energy_charges': 'sum',
            'buyer_entity': 'nunique',
            'buyer_filing': lambda x: list(x.unique())[:5]  # Sample buyers
        }).reset_index()

        # Build index with normalized keys
        self.seller_index = {}

        for _, row in agg.iterrows():
            seller = row['seller']
            if not seller or len(str(seller)) < 3:
                continue

            # Create normalized key (lowercase, remove common suffixes)
            key = self._normalize_name(seller)

            self.seller_index[key] = {
                'original_name': seller,
                'total_mwh': row['mwh'] or 0,
                'storage_mwh': row['mwh_storage'] or 0,
                'total_energy_charges': row['energy_charges'] or 0,
                'buyer_count': row['buyer_entity'],
                'sample_buyers': row['buyer_filing'],
            }

            # Also index by words for fuzzy matching
            words = self._extract_key_words(seller)
            for word in words:
                if len(word) >= 4:  # Only meaningful words
                    word_key = f"_word_{word}"
                    if word_key not in self.seller_index:
                        self.seller_index[word_key] = []
                    self.seller_index[word_key].append(key)

        logger.info(f"  Indexed {len([k for k in self.seller_index if not k.startswith('_word_')]):,} unique sellers")

    def _normalize_name(self, name: str) -> str:
        """Normalize a company/project name for matching."""
        if not name:
            return ""

        name = str(name).lower().strip()

        # Remove common suffixes
        suffixes = [
            r'\s*,?\s*llc\.?$', r'\s*,?\s*l\.l\.c\.?$',
            r'\s*,?\s*inc\.?$', r'\s*,?\s*corp\.?$',
            r'\s*,?\s*lp\.?$', r'\s*,?\s*l\.p\.?$',
            r'\s*,?\s*ltd\.?$', r'\s*,?\s*co\.?$',
            r'\s*,?\s*company$', r'\s*,?\s*corporation$',
            r'\s*\(.*\)$',  # Remove parenthetical notes
        ]

        for suffix in suffixes:
            name = re.sub(suffix, '', name, flags=re.IGNORECASE)

        # Remove extra whitespace
        name = ' '.join(name.split())

        return name

    def _extract_key_words(self, name: str) -> List[str]:
        """Extract key words from a name for fuzzy matching."""
        if not name:
            return []

        name = str(name).lower()

        # Remove common words
        stop_words = {
            'the', 'and', 'of', 'for', 'llc', 'inc', 'corp', 'company', 'co',
            'energy', 'power', 'electric', 'generation', 'project', 'farm',
            'wind', 'solar', 'renewable', 'renewables', 'holdings', 'group'
        }

        # Extract words
        words = re.findall(r'[a-z]{3,}', name)

        return [w for w in words if w not in stop_words]

    def check_project_ppa(self, project_name: str, developer: str = None,
                          state: str = None) -> Optional[PPAMatch]:
        """
        Check if a project or developer has PPAs in FERC data.

        Args:
            project_name: Name of the queue project
            developer: Developer/company name (optional)
            state: State abbreviation (optional, for filtering)

        Returns:
            PPAMatch if found, None otherwise
        """
        if not self.seller_index:
            self.load_data()

        # Try matching project name
        matches = []

        for name in [project_name, developer]:
            if not name:
                continue

            # Exact match (normalized)
            key = self._normalize_name(name)
            if key in self.seller_index and not key.startswith('_word_'):
                seller_data = self.seller_index[key]
                matches.append({
                    'method': 'exact',
                    'confidence': 0.95,
                    'data': seller_data
                })
                break

            # Word-based fuzzy match
            words = self._extract_key_words(name)
            if len(words) >= 2:
                # Find sellers with matching words
                candidate_keys = set()
                for word in words:
                    word_key = f"_word_{word}"
                    if word_key in self.seller_index:
                        candidate_keys.update(self.seller_index[word_key])

                # Score candidates by word overlap
                for cand_key in candidate_keys:
                    if cand_key in self.seller_index and not cand_key.startswith('_word_'):
                        cand_words = set(self._extract_key_words(
                            self.seller_index[cand_key]['original_name']
                        ))
                        overlap = len(set(words) & cand_words)
                        if overlap >= 2:
                            matches.append({
                                'method': 'fuzzy',
                                'confidence': 0.5 + (overlap * 0.15),
                                'data': self.seller_index[cand_key]
                            })

        if not matches:
            return None

        # Return best match
        best = max(matches, key=lambda x: x['confidence'])
        data = best['data']

        return PPAMatch(
            seller_name=data['original_name'],
            total_mwh=data['total_mwh'],
            total_contracts=data['buyer_count'],
            buyers=data['sample_buyers'][:3],
            match_confidence=min(best['confidence'], 0.99),
            match_method=best['method']
        )

    def enrich_database(self, min_confidence: float = 0.7) -> int:
        """
        Enrich queue database with PPA information.

        Adds 'has_ppa' flag to projects that have matching FERC purchased power records.

        Returns number of projects updated.
        """
        if not self.seller_index:
            self.load_data()

        db_path = DATA_DIR / 'queue.db'
        if not db_path.exists():
            logger.error(f"Queue database not found: {db_path}")
            return 0

        conn = sqlite3.connect(db_path)

        # Check if has_ppa column exists, add if not
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(projects)")
        columns = [col[1] for col in cursor.fetchall()]

        if 'has_ppa' not in columns:
            cursor.execute("ALTER TABLE projects ADD COLUMN has_ppa BOOLEAN DEFAULT NULL")
            conn.commit()
            logger.info("Added has_ppa column to projects table")

        if 'ppa_seller' not in columns:
            cursor.execute("ALTER TABLE projects ADD COLUMN ppa_seller TEXT DEFAULT NULL")
            conn.commit()

        # Get projects to check
        projects_df = pd.read_sql('''
            SELECT queue_id, region, name, developer
            FROM projects
            WHERE has_ppa IS NULL OR has_ppa = 0
        ''', conn)

        logger.info(f"Checking {len(projects_df):,} projects for PPA matches...")

        updated = 0
        ppa_found = 0

        for _, row in projects_df.iterrows():
            match = self.check_project_ppa(
                project_name=row['name'],
                developer=row['developer']
            )

            if match and match.match_confidence >= min_confidence:
                cursor.execute("""
                    UPDATE projects
                    SET has_ppa = 1, ppa_seller = ?
                    WHERE queue_id = ? AND region = ?
                """, (match.seller_name, row['queue_id'], row['region']))
                ppa_found += 1
            else:
                cursor.execute("""
                    UPDATE projects
                    SET has_ppa = 0
                    WHERE queue_id = ? AND region = ?
                """, (row['queue_id'], row['region']))

            updated += 1

            if updated % 5000 == 0:
                logger.info(f"  Processed {updated:,} projects, {ppa_found:,} with PPAs")

        conn.commit()
        conn.close()

        logger.info(f"Updated {updated:,} projects, {ppa_found:,} have PPAs ({ppa_found/updated*100:.1f}%)")
        return ppa_found

    def get_renewable_sellers(self, min_mwh: float = 1000000) -> pd.DataFrame:
        """Get list of renewable energy sellers with significant volume."""
        if not self.seller_index:
            self.load_data()

        renewable_keywords = ['solar', 'wind', 'renewable', 'green', 'clean']

        sellers = []
        for key, data in self.seller_index.items():
            if key.startswith('_word_'):
                continue

            name_lower = data['original_name'].lower()
            if any(kw in name_lower for kw in renewable_keywords):
                if data['total_mwh'] >= min_mwh:
                    sellers.append({
                        'seller': data['original_name'],
                        'total_mwh': data['total_mwh'],
                        'buyer_count': data['buyer_count'],
                    })

        return pd.DataFrame(sellers).sort_values('total_mwh', ascending=False)

    def get_ppa_stats(self) -> Dict:
        """Get statistics about PPA data coverage."""
        if not self.seller_index:
            self.load_data()

        total_sellers = len([k for k in self.seller_index if not k.startswith('_word_')])

        # Count renewable sellers
        renewable_count = 0
        renewable_keywords = ['solar', 'wind', 'renewable', 'green', 'clean']
        for key, data in self.seller_index.items():
            if key.startswith('_word_'):
                continue
            if any(kw in data['original_name'].lower() for kw in renewable_keywords):
                renewable_count += 1

        return {
            'total_ppa_records': len(self.ppa_df) if self.ppa_df is not None else 0,
            'unique_sellers': total_sellers,
            'renewable_sellers': renewable_count,
        }


def main():
    """CLI for FERC PPA matching."""
    import argparse

    parser = argparse.ArgumentParser(description="FERC PPA Data Matcher")
    parser.add_argument('--load', action='store_true', help='Load FERC purchased power data')
    parser.add_argument('--stats', action='store_true', help='Show PPA statistics')
    parser.add_argument('--renewables', action='store_true', help='List renewable sellers')
    parser.add_argument('--enrich', action='store_true', help='Enrich queue database with PPA data')
    parser.add_argument('--check', type=str, help='Check if a project has PPA')
    parser.add_argument('--min-confidence', type=float, default=0.7,
                       help='Minimum confidence for PPA matches')

    args = parser.parse_args()

    matcher = FERCPPAMatcher()

    if args.load or args.stats or args.renewables or args.enrich or args.check:
        matcher.load_data()

    if args.stats:
        stats = matcher.get_ppa_stats()
        print("\n=== FERC PPA Statistics ===")
        print(f"Total PPA records: {stats['total_ppa_records']:,}")
        print(f"Unique sellers: {stats['unique_sellers']:,}")
        print(f"Renewable sellers: {stats['renewable_sellers']:,}")

    if args.renewables:
        df = matcher.get_renewable_sellers()
        print("\n=== Top Renewable Power Sellers ===")
        print(df.head(30).to_string())

    if args.check:
        match = matcher.check_project_ppa(args.check)
        if match:
            print(f"\nPPA Found for '{args.check}':")
            print(f"  Seller: {match.seller_name}")
            print(f"  Total MWh: {match.total_mwh:,.0f}")
            print(f"  Contracts: {match.total_contracts}")
            print(f"  Confidence: {match.match_confidence:.0%}")
        else:
            print(f"\nNo PPA found for '{args.check}'")

    if args.enrich:
        count = matcher.enrich_database(min_confidence=args.min_confidence)
        print(f"\nEnriched {count:,} projects with PPA data")


if __name__ == '__main__':
    main()
