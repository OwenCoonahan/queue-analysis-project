#!/usr/bin/env python3
"""
FERC Form 1 Seller Extractor

Extracts renewable energy seller names from FERC Form 1 purchased power data
to identify developers with active PPAs (Power Purchase Agreements).

The FERC Form 1 Schedule 326 contains utility reports of all power purchases,
including seller names, MWh purchased, and contract details.

Usage:
    from ferc_seller_extractor import FERCSellerExtractor

    extractor = FERCSellerExtractor()
    sellers = extractor.get_renewable_sellers()
    match = extractor.match_project_to_seller("Bronco Plains Wind", "NextEra")
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass
from collections import defaultdict
import re
import logging

try:
    from rapidfuzz import fuzz
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent / '.cache'
FERC_DB = CACHE_DIR / 'pudl' / 'ferc1_xbrl.sqlite'


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class FERCSeller:
    """A FERC Form 1 power seller record."""
    seller_name: str
    normalized_name: str
    total_mwh: float
    total_charges: float
    contract_count: int
    buyer_names: List[str]
    is_renewable: bool
    fuel_type: Optional[str]  # Inferred from name


@dataclass
class FERCMatch:
    """Result of matching a project to FERC seller data."""
    matched: bool
    seller_name: Optional[str] = None
    total_mwh: Optional[float] = None
    confidence: float = 0.0
    match_method: str = 'none'
    buyer_utilities: List[str] = None


# =============================================================================
# RENEWABLE KEYWORDS
# =============================================================================

RENEWABLE_KEYWORDS = {
    'solar': 'Solar',
    'wind': 'Wind',
    'renewable': None,  # Could be either
    'clean energy': None,
    'green': None,
    'battery': 'Storage',
    'storage': 'Storage',
    'bess': 'Storage',
    'hydro': 'Hydro',
    'geothermal': 'Geothermal',
}

# Common utility names to exclude from seller matching
UTILITY_PATTERNS = [
    r'^(aep|american electric power)',
    r'^(dominion|duke|entergy|exelon|southern|xcel)',
    r'^(pacificorp|pacific gas|pge|pg&e)',
    r'^(con ?ed|consolidated edison)',
    r'^(first ?energy|dte|consumers energy)',
    r'^(pnm|public service)',
    r'^(iso|rto|miso|pjm|ercot|caiso|nyiso|spp|isone)',
    r'(transmission|distribution|municipal|cooperative|city of)',
    r'(electric.*company$|power.*company$|utility$)',
]


# =============================================================================
# FERC SELLER EXTRACTOR
# =============================================================================

class FERCSellerExtractor:
    """
    Extract and analyze power sellers from FERC Form 1 purchased power data.

    Identifies renewable energy sellers and provides matching to queue projects.
    """

    def __init__(self, ferc_db_path: str = None):
        """Initialize extractor with FERC database path."""
        if ferc_db_path is None:
            ferc_db_path = FERC_DB
        self.db_path = Path(ferc_db_path)
        self._conn = None

        # Caches
        self._sellers: Dict[str, FERCSeller] = {}
        self._renewable_sellers: Dict[str, FERCSeller] = {}
        self._normalized_lookup: Dict[str, str] = {}  # normalized -> original

    def _get_conn(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._conn is None:
            if not self.db_path.exists():
                raise FileNotFoundError(f"FERC database not found: {self.db_path}")
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    @staticmethod
    def normalize_name(name: str) -> str:
        """Normalize a seller name for matching."""
        if not name:
            return ''

        name = name.lower().strip()

        # Remove common suffixes
        suffixes = [
            r',?\s*(llc|l\.l\.c\.?|inc\.?|corp\.?|co\.?|ltd\.?|lp|l\.p\.)\.?\s*$',
            r',?\s*(company|corporation|limited|partnership)\.?\s*$',
            r'\s*\([^)]*\)\s*$',
        ]
        for pattern in suffixes:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)

        # Collapse whitespace
        name = ' '.join(name.split())

        return name

    def _classify_seller(self, name: str) -> Tuple[bool, Optional[str]]:
        """
        Classify if a seller is renewable and determine fuel type.

        Returns:
            (is_renewable, fuel_type)
        """
        name_lower = name.lower()

        # Check for utility patterns (exclude these)
        for pattern in UTILITY_PATTERNS:
            if re.search(pattern, name_lower):
                return False, None

        # Check for renewable keywords
        for keyword, fuel in RENEWABLE_KEYWORDS.items():
            if keyword in name_lower:
                return True, fuel

        return False, None

    def build_index(self, min_mwh: float = 1000) -> int:
        """
        Build index of power sellers from FERC data.

        Args:
            min_mwh: Minimum MWh to include a seller

        Returns:
            Number of sellers indexed
        """
        logger.info("Building FERC seller index...")

        conn = self._get_conn()

        # Aggregate by seller
        query = """
        SELECT
            name_of_company_or_public_authority_providing_purchased_power as seller,
            filing_name as buyer,
            SUM(megawatt_hours_purchased_other_than_storage) as total_mwh,
            SUM(energy_charges_of_purchased_power) as total_charges,
            COUNT(*) as contract_count
        FROM purchased_power_326_duration
        WHERE seller IS NOT NULL
        GROUP BY seller, buyer
        HAVING total_mwh >= ?
        ORDER BY total_mwh DESC
        """

        df = pd.read_sql(query, conn, params=(min_mwh,))
        logger.info(f"  Found {len(df):,} seller-buyer combinations")

        # Aggregate by seller across all buyers
        seller_agg = df.groupby('seller').agg({
            'total_mwh': 'sum',
            'total_charges': 'sum',
            'contract_count': 'sum',
            'buyer': lambda x: list(x.unique())[:10]  # Top 10 buyers
        }).reset_index()

        count = 0
        renewable_count = 0

        for _, row in seller_agg.iterrows():
            seller_name = row['seller']
            normalized = self.normalize_name(seller_name)

            if not normalized:
                continue

            is_renewable, fuel_type = self._classify_seller(seller_name)

            # Extract buyer utility names (clean up filing_name format)
            buyers = []
            for b in row['buyer']:
                # filing_name format: "Utility_Name_form1_Q4_..."
                clean = b.split('_form1')[0].replace('_', ' ')
                buyers.append(clean)

            seller = FERCSeller(
                seller_name=seller_name,
                normalized_name=normalized,
                total_mwh=row['total_mwh'] or 0,
                total_charges=row['total_charges'] or 0,
                contract_count=row['contract_count'],
                buyer_names=buyers,
                is_renewable=is_renewable,
                fuel_type=fuel_type
            )

            self._sellers[normalized] = seller
            self._normalized_lookup[normalized] = seller_name

            if is_renewable:
                self._renewable_sellers[normalized] = seller
                renewable_count += 1

            count += 1

        logger.info(f"  Indexed {count:,} unique sellers")
        logger.info(f"  {renewable_count:,} classified as renewable")

        return count

    def get_renewable_sellers(self, min_mwh: float = 10000) -> pd.DataFrame:
        """
        Get DataFrame of renewable energy sellers.

        Args:
            min_mwh: Minimum MWh threshold

        Returns:
            DataFrame with seller details
        """
        if not self._sellers:
            self.build_index()

        records = []
        for seller in self._renewable_sellers.values():
            if seller.total_mwh >= min_mwh:
                records.append({
                    'seller_name': seller.seller_name,
                    'fuel_type': seller.fuel_type,
                    'total_mwh': seller.total_mwh,
                    'total_charges_m': seller.total_charges / 1_000_000,
                    'contract_count': seller.contract_count,
                    'buyers': '; '.join(seller.buyer_names[:5]),
                })

        df = pd.DataFrame(records)
        return df.sort_values('total_mwh', ascending=False)

    def match_name(self, name: str, threshold: float = 0.80) -> FERCMatch:
        """
        Match a name (project or developer) to FERC sellers.

        Uses fuzzy matching if rapidfuzz is available.

        Args:
            name: Project name or developer name to match
            threshold: Minimum fuzzy match score (0-1)

        Returns:
            FERCMatch with seller details
        """
        if not self._sellers:
            self.build_index()

        if not name:
            return FERCMatch(matched=False, match_method='empty_input')

        normalized = self.normalize_name(name)

        # Try exact match first
        if normalized in self._sellers:
            seller = self._sellers[normalized]
            return FERCMatch(
                matched=True,
                seller_name=seller.seller_name,
                total_mwh=seller.total_mwh,
                confidence=1.0,
                match_method='exact',
                buyer_utilities=seller.buyer_names
            )

        # Try fuzzy match (only for renewable sellers to avoid utility matches)
        if RAPIDFUZZ_AVAILABLE:
            best_match = None
            best_score = 0

            for norm_name, seller in self._renewable_sellers.items():
                # Use token_set_ratio for better partial matching
                score = fuzz.token_set_ratio(normalized, norm_name) / 100

                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = seller

            if best_match:
                return FERCMatch(
                    matched=True,
                    seller_name=best_match.seller_name,
                    total_mwh=best_match.total_mwh,
                    confidence=best_score,
                    match_method='fuzzy',
                    buyer_utilities=best_match.buyer_names
                )

        return FERCMatch(matched=False, match_method='no_match')

    def match_developer(self, developer_name: str) -> FERCMatch:
        """
        Match a developer name to FERC sellers.

        Looks for the developer as a seller or in seller names.

        Args:
            developer_name: Developer company name

        Returns:
            FERCMatch if found
        """
        if not developer_name:
            return FERCMatch(matched=False, match_method='empty_input')

        # First try direct match
        result = self.match_name(developer_name)
        if result.matched:
            return result

        # Try matching developer name as substring of seller names
        if not self._sellers:
            self.build_index()

        dev_lower = developer_name.lower()
        dev_words = set(self.normalize_name(developer_name).split())

        # Remove common words
        stop_words = {'energy', 'power', 'solar', 'wind', 'llc', 'inc', 'corp', 'the', 'of'}
        dev_words = dev_words - stop_words

        if len(dev_words) < 1:
            return FERCMatch(matched=False, match_method='insufficient_keywords')

        # Find sellers containing developer keywords
        matches = []
        for norm_name, seller in self._renewable_sellers.items():
            seller_words = set(norm_name.split()) - stop_words

            overlap = len(dev_words & seller_words)
            if overlap >= 1:
                # Calculate Jaccard-like score
                score = overlap / max(len(dev_words), 1)
                if score >= 0.5:
                    matches.append((seller, score))

        if matches:
            # Return best match
            matches.sort(key=lambda x: (-x[1], -x[0].total_mwh))
            best = matches[0]
            return FERCMatch(
                matched=True,
                seller_name=best[0].seller_name,
                total_mwh=best[0].total_mwh,
                confidence=min(0.85, best[1]),  # Cap at 0.85 for keyword matches
                match_method='keyword_match',
                buyer_utilities=best[0].buyer_names
            )

        return FERCMatch(matched=False, match_method='no_match')

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about FERC seller data."""
        if not self._sellers:
            self.build_index()

        total_mwh = sum(s.total_mwh for s in self._sellers.values())
        renewable_mwh = sum(s.total_mwh for s in self._renewable_sellers.values())

        # Count by fuel type
        fuel_counts = defaultdict(int)
        fuel_mwh = defaultdict(float)
        for seller in self._renewable_sellers.values():
            fuel = seller.fuel_type or 'Unknown'
            fuel_counts[fuel] += 1
            fuel_mwh[fuel] += seller.total_mwh

        return {
            'total_sellers': len(self._sellers),
            'renewable_sellers': len(self._renewable_sellers),
            'total_mwh': total_mwh,
            'renewable_mwh': renewable_mwh,
            'renewable_pct': renewable_mwh / total_mwh * 100 if total_mwh else 0,
            'by_fuel_type': dict(fuel_counts),
            'mwh_by_fuel': {k: v / 1e6 for k, v in fuel_mwh.items()},  # TWh
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
    """CLI for FERC seller extractor."""
    import argparse

    parser = argparse.ArgumentParser(description="FERC Form 1 Seller Extractor")
    parser.add_argument('--build', action='store_true', help='Build seller index')
    parser.add_argument('--stats', action='store_true', help='Show seller statistics')
    parser.add_argument('--renewable', action='store_true', help='List renewable sellers')
    parser.add_argument('--match', type=str, help='Match a name to sellers')
    parser.add_argument('--top', type=int, default=30, help='Number of top sellers to show')

    args = parser.parse_args()

    if not FERC_DB.exists():
        print(f"FERC database not found: {FERC_DB}")
        print("Download from: https://zenodo.org/records/...")
        return

    extractor = FERCSellerExtractor()

    if args.build or args.stats or args.renewable or args.match:
        extractor.build_index()

    if args.stats:
        stats = extractor.get_stats()
        print("\n=== FERC Seller Statistics ===")
        print(f"Total sellers: {stats['total_sellers']:,}")
        print(f"Renewable sellers: {stats['renewable_sellers']:,}")
        print(f"Total MWh: {stats['total_mwh']/1e6:,.1f} TWh")
        print(f"Renewable MWh: {stats['renewable_mwh']/1e6:,.1f} TWh ({stats['renewable_pct']:.1f}%)")
        print("\nRenewable by fuel type:")
        for fuel, count in sorted(stats['by_fuel_type'].items(), key=lambda x: -x[1]):
            mwh = stats['mwh_by_fuel'].get(fuel, 0)
            print(f"  {fuel}: {count} sellers ({mwh:.1f} TWh)")

    if args.renewable:
        df = extractor.get_renewable_sellers(min_mwh=10000)
        print(f"\n=== Top {args.top} Renewable Sellers ===")
        print(df.head(args.top).to_string(index=False))

    if args.match:
        match = extractor.match_name(args.match)
        print(f"\n=== Match Result for '{args.match}' ===")
        print(f"  Matched: {match.matched}")
        if match.matched:
            print(f"  Seller: {match.seller_name}")
            print(f"  Total MWh: {match.total_mwh:,.0f}")
            print(f"  Confidence: {match.confidence:.1%}")
            print(f"  Method: {match.match_method}")
            if match.buyer_utilities:
                print(f"  Buyers: {', '.join(match.buyer_utilities[:5])}")

    extractor.close()


if __name__ == '__main__':
    main()
