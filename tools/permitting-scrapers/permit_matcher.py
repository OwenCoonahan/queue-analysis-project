#!/usr/bin/env python3
"""
Permit Matcher - Match permits to queue projects.

Uses tiered matching to link permit records to interconnection queue projects:
    Tier 1 (0.95+): EIA Plant Code matches existing project
    Tier 2 (0.85-0.94): Developer + State + County + Capacity (±10%) + Technology
    Tier 3 (0.75-0.84): State + County + Capacity (±15%) + Technology
    Tier 4 (0.65-0.74): State + Capacity (±10%) + Technology + Name similarity

Usage:
    from permitting_scrapers import PermitMatcher

    matcher = PermitMatcher(queue_df)
    matches = matcher.match_batch(permits_df)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class PermitMatch:
    """Result of matching a permit to a queue project."""
    permit_id: str
    queue_id: str
    region: str
    confidence: float
    match_method: str
    match_details: str


class PermitMatcher:
    """Match permits to existing queue projects using tiered matching."""

    # Technology normalization for matching
    TECH_NORMALIZE = {
        'solar': 'solar',
        'photovoltaic': 'solar',
        'pv': 'solar',
        'wind': 'wind',
        'onshore wind': 'wind',
        'offshore wind': 'wind',
        'storage': 'storage',
        'battery': 'storage',
        'bess': 'storage',
        'batteries': 'storage',
        'gas': 'gas',
        'natural gas': 'gas',
        'ng': 'gas',
        'hydro': 'hydro',
        'hydroelectric': 'hydro',
    }

    # State to region mapping
    STATE_TO_REGION = {
        'TX': 'ERCOT',
        'CA': 'CAISO',
        'NY': 'NYISO',
        # PJM states
        'PA': 'PJM', 'NJ': 'PJM', 'MD': 'PJM', 'DE': 'PJM', 'VA': 'PJM',
        'WV': 'PJM', 'OH': 'PJM', 'KY': 'PJM', 'IN': 'PJM', 'IL': 'PJM',
        'MI': 'PJM', 'NC': 'PJM', 'DC': 'PJM',
        # MISO states
        'MN': 'MISO', 'WI': 'MISO', 'IA': 'MISO', 'MO': 'MISO', 'AR': 'MISO',
        'LA': 'MISO', 'MS': 'MISO', 'ND': 'MISO', 'SD': 'MISO', 'MT': 'MISO',
        # SPP states
        'OK': 'SPP', 'KS': 'SPP', 'NE': 'SPP', 'NM': 'SPP',
        # ISO-NE states
        'MA': 'ISO-NE', 'CT': 'ISO-NE', 'RI': 'ISO-NE', 'NH': 'ISO-NE',
        'VT': 'ISO-NE', 'ME': 'ISO-NE',
        # Southeast (non-ISO)
        'GA': 'Southeast', 'FL': 'Southeast', 'AL': 'Southeast', 'SC': 'Southeast', 'TN': 'Southeast',
        # West (non-ISO)
        'AZ': 'West', 'NV': 'West', 'UT': 'West', 'CO': 'West', 'WY': 'West',
        'OR': 'West', 'WA': 'West', 'ID': 'West',
    }

    def __init__(self, queue_df: pd.DataFrame = None):
        """
        Initialize matcher with queue data.

        Args:
            queue_df: DataFrame with queue projects (from DataStore.get_projects())
        """
        self.queue_df = queue_df
        self._indexes = {}

        if queue_df is not None:
            self._build_indexes()

    def load_queue_data(self):
        """Load queue data from database."""
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from data_store import DataStore

        db = DataStore()
        self.queue_df = db.get_projects()
        self._build_indexes()
        logger.info(f"Loaded {len(self.queue_df):,} queue projects for matching")

    def _build_indexes(self):
        """Build lookup indexes for efficient matching."""
        if self.queue_df is None or self.queue_df.empty:
            return

        df = self.queue_df.copy()

        # Normalize for matching
        df['_state'] = df['state'].fillna('').str.upper().str.strip()
        df['_county'] = df['county'].fillna('').str.upper().str.strip()
        df['_developer'] = df['developer'].fillna('').apply(self._normalize_developer)
        df['_tech'] = df['type'].fillna('').apply(self._normalize_tech)
        df['_capacity'] = pd.to_numeric(df['capacity_mw'], errors='coerce').fillna(0)
        df['_name'] = df['name'].fillna('').str.lower()

        # Index by state + county
        self._indexes['state_county'] = df.groupby(['_state', '_county']).apply(
            lambda x: x.to_dict('records')
        ).to_dict()

        # Index by state only
        self._indexes['state'] = df.groupby('_state').apply(
            lambda x: x.to_dict('records')
        ).to_dict()

        # Index by developer (normalized)
        self._indexes['developer'] = {}
        for _, row in df.iterrows():
            dev = row['_developer']
            if dev:
                if dev not in self._indexes['developer']:
                    self._indexes['developer'][dev] = []
                self._indexes['developer'][dev].append(row.to_dict())

        logger.info(f"Built indexes: {len(self._indexes['state_county'])} state+county, "
                   f"{len(self._indexes['state'])} state, {len(self._indexes['developer'])} developers")

    def _normalize_developer(self, name: str) -> str:
        """Normalize developer name for matching."""
        if not name:
            return ''
        name = str(name).lower()
        # Remove common suffixes
        for suffix in ['llc', 'inc', 'corp', 'co', 'company', 'limited', 'lp', 'lc']:
            name = re.sub(rf'\b{suffix}\b\.?', '', name)
        # Remove punctuation
        name = re.sub(r'[^\w\s]', '', name)
        # Collapse whitespace
        name = ' '.join(name.split())
        return name.strip()

    def _normalize_tech(self, tech: str) -> str:
        """Normalize technology type for matching."""
        if not tech:
            return ''
        tech_lower = str(tech).lower()
        for key, normalized in self.TECH_NORMALIZE.items():
            if key in tech_lower:
                return normalized
        return tech_lower

    def _name_similarity(self, name1: str, name2: str) -> float:
        """Calculate simple name similarity (0-1)."""
        if not name1 or not name2:
            return 0.0

        # Normalize
        n1 = set(name1.lower().split())
        n2 = set(name2.lower().split())

        if not n1 or not n2:
            return 0.0

        # Jaccard similarity
        intersection = len(n1 & n2)
        union = len(n1 | n2)
        return intersection / union if union > 0 else 0.0

    def match(self, permit_row: Dict) -> Optional[PermitMatch]:
        """
        Find best matching queue project for a permit.

        Args:
            permit_row: Dict with permit data (permit_id, state, county, capacity_mw, technology, developer, project_name)

        Returns:
            PermitMatch if found, None otherwise
        """
        if not self._indexes:
            if self.queue_df is None:
                self.load_queue_data()
            else:
                self._build_indexes()

        permit_id = str(permit_row.get('permit_id', ''))
        state = str(permit_row.get('state', '')).upper().strip()
        county = str(permit_row.get('county', '')).upper().strip()
        capacity = float(permit_row.get('capacity_mw') or 0)
        tech = self._normalize_tech(permit_row.get('technology', ''))
        developer = self._normalize_developer(permit_row.get('developer', ''))
        name = str(permit_row.get('project_name', '')).lower()

        best_match = None
        best_score = 0

        # Tier 2: Developer + State + County + Capacity + Technology (0.85-0.94)
        if developer and developer in self._indexes.get('developer', {}):
            for cand in self._indexes['developer'][developer]:
                if cand['_state'] != state:
                    continue

                # Check capacity match (within 10%)
                cand_cap = cand['_capacity']
                if capacity > 0 and cand_cap > 0:
                    cap_diff = abs(cand_cap - capacity) / max(capacity, cand_cap)
                    if cap_diff > 0.10:
                        continue

                # Check technology match
                if tech and cand['_tech'] and tech != cand['_tech']:
                    continue

                score = 0.90
                if cand['_county'] == county:
                    score = 0.94

                if score > best_score:
                    best_score = score
                    best_match = {
                        'queue_id': cand['queue_id'],
                        'region': cand['region'],
                        'method': 'developer_state_capacity',
                        'details': f"Developer '{developer}' + state {state}"
                    }

        # Tier 3: State + County + Capacity + Technology (0.75-0.84)
        if best_score < 0.85 and state and county:
            candidates = self._indexes.get('state_county', {}).get((state, county), [])
            for cand in candidates:
                # Check capacity match (within 15%)
                cand_cap = cand['_capacity']
                if capacity > 0 and cand_cap > 0:
                    cap_diff = abs(cand_cap - capacity) / max(capacity, cand_cap)
                    if cap_diff > 0.15:
                        continue

                    # Check technology match
                    tech_match = (not tech or not cand['_tech'] or tech == cand['_tech'])
                    if not tech_match:
                        continue

                    score = 0.75 + (0.09 * (1 - cap_diff))  # 0.75-0.84 based on capacity match

                    if score > best_score:
                        best_score = score
                        best_match = {
                            'queue_id': cand['queue_id'],
                            'region': cand['region'],
                            'method': 'state_county_capacity',
                            'details': f"State {state}, County {county}, Capacity {capacity:.0f}MW"
                        }

        # Tier 4: State + Capacity + Technology + Name Similarity (0.65-0.74)
        if best_score < 0.75 and state:
            candidates = self._indexes.get('state', {}).get(state, [])
            for cand in candidates:
                # Check capacity match (within 10%)
                cand_cap = cand['_capacity']
                if capacity > 0 and cand_cap > 0:
                    cap_diff = abs(cand_cap - capacity) / max(capacity, cand_cap)
                    if cap_diff > 0.10:
                        continue

                    # Check technology match
                    tech_match = (not tech or not cand['_tech'] or tech == cand['_tech'])
                    if not tech_match:
                        continue

                    # Check name similarity
                    name_sim = self._name_similarity(name, cand['_name'])
                    if name_sim < 0.3:
                        continue

                    score = 0.65 + (0.09 * name_sim)  # 0.65-0.74 based on name match

                    if score > best_score:
                        best_score = score
                        best_match = {
                            'queue_id': cand['queue_id'],
                            'region': cand['region'],
                            'method': 'state_capacity_name',
                            'details': f"State {state}, Capacity {capacity:.0f}MW, Name similarity {name_sim:.2f}"
                        }

        if best_match and best_score >= 0.65:
            return PermitMatch(
                permit_id=permit_id,
                queue_id=best_match['queue_id'],
                region=best_match['region'],
                confidence=best_score,
                match_method=best_match['method'],
                match_details=best_match['details']
            )

        return None

    def match_batch(self, permits_df: pd.DataFrame) -> pd.DataFrame:
        """
        Match multiple permits to queue projects.

        Args:
            permits_df: DataFrame with permit data

        Returns:
            permits_df with added columns: queue_id, region, match_confidence, match_method
        """
        if not self._indexes:
            if self.queue_df is None:
                self.load_queue_data()
            else:
                self._build_indexes()

        results = []
        matched_count = 0

        for _, row in permits_df.iterrows():
            match = self.match(row.to_dict())

            if match:
                matched_count += 1
                results.append({
                    'permit_id': match.permit_id,
                    'queue_id': match.queue_id,
                    'region': match.region,
                    'match_confidence': match.confidence,
                    'match_method': match.match_method,
                })
            else:
                # Infer region from state if no match
                state = str(row.get('state', '')).upper()
                region = self.STATE_TO_REGION.get(state, 'Unknown')
                results.append({
                    'permit_id': row.get('permit_id'),
                    'queue_id': None,
                    'region': region,
                    'match_confidence': None,
                    'match_method': None,
                })

        logger.info(f"Matched {matched_count:,} of {len(permits_df):,} permits ({100*matched_count/len(permits_df):.1f}%)")

        results_df = pd.DataFrame(results)
        return permits_df.merge(results_df, on='permit_id', how='left', suffixes=('', '_match'))

    def get_match_stats(self, matched_df: pd.DataFrame) -> Dict:
        """Get statistics about matching results."""
        total = len(matched_df)
        matched = matched_df['queue_id'].notna().sum()

        by_method = matched_df['match_method'].value_counts().to_dict()
        by_confidence = {
            'high (0.85+)': len(matched_df[matched_df['match_confidence'] >= 0.85]),
            'medium (0.75-0.85)': len(matched_df[(matched_df['match_confidence'] >= 0.75) & (matched_df['match_confidence'] < 0.85)]),
            'low (0.65-0.75)': len(matched_df[(matched_df['match_confidence'] >= 0.65) & (matched_df['match_confidence'] < 0.75)]),
            'unmatched': len(matched_df[matched_df['match_confidence'].isna()]),
        }

        return {
            'total': total,
            'matched': matched,
            'match_rate': matched / total if total > 0 else 0,
            'by_method': by_method,
            'by_confidence': by_confidence,
        }


def main():
    """CLI for permit matcher."""
    import argparse

    parser = argparse.ArgumentParser(description="Permit Matcher - Match permits to queue projects")
    parser.add_argument('--test', action='store_true', help='Run test matching with EIA data')
    parser.add_argument('--state', type=str, help='Filter by state for testing')
    parser.add_argument('--limit', type=int, default=100, help='Limit records for testing')

    args = parser.parse_args()

    if args.test:
        print("Loading EIA planned generators...")
        from eia_planned_loader import EIAPlannedLoader
        loader = EIAPlannedLoader()
        permits_df = loader.load()

        if args.state:
            permits_df = permits_df[permits_df['state'] == args.state.upper()]
            print(f"Filtered to {len(permits_df)} permits in {args.state.upper()}")

        if args.limit:
            permits_df = permits_df.head(args.limit)
            print(f"Limited to {len(permits_df)} permits")

        print("\nLoading queue data and matching...")
        matcher = PermitMatcher()
        matched_df = matcher.match_batch(permits_df)

        stats = matcher.get_match_stats(matched_df)
        print(f"\n=== Matching Results ===")
        print(f"Total permits: {stats['total']}")
        print(f"Matched: {stats['matched']} ({100*stats['match_rate']:.1f}%)")
        print(f"\nBy Confidence:")
        for level, count in stats['by_confidence'].items():
            print(f"  {level}: {count}")
        print(f"\nBy Method:")
        for method, count in stats['by_method'].items():
            print(f"  {method}: {count}")

        # Show sample matches
        matches = matched_df[matched_df['queue_id'].notna()].head(5)
        if not matches.empty:
            print(f"\nSample Matches:")
            for _, row in matches.iterrows():
                print(f"  {row['permit_id']} -> {row['queue_id']} ({row['region']}) "
                      f"[{row['match_method']}, conf={row['match_confidence']:.2f}]")


if __name__ == '__main__':
    main()
