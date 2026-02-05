#!/usr/bin/env python3
"""
EIA Form 860 Data Loader and Matcher

Loads EIA Form 860 data (plants, generators, owners) and matches
interconnection queue projects to find developer/owner names.

Usage:
    from eia_loader import EIAMatcher

    matcher = EIAMatcher()
    matcher.load_eia_data()
    matches = matcher.match_queue_projects(queue_df)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent / '.cache' / 'eia'


@dataclass
class EIAMatch:
    """Represents a match between a queue project and EIA record."""
    queue_id: str
    iso: str
    eia_plant_code: str
    eia_plant_name: str
    owner_name: str
    utility_name: str
    match_method: str  # 'state_county_capacity', 'state_capacity', 'name_similarity'
    confidence: float  # 0-1


class EIAMatcher:
    """Match interconnection queue projects to EIA Form 860 data."""

    # Fuel type mapping from EIA codes to queue types
    FUEL_MAP = {
        'SUN': 'Solar',
        'WND': 'Wind',
        'MWH': 'Storage',
        'WAT': 'Hydro',
        'NG': 'Gas',
        'NUC': 'Nuclear',
        'BIT': 'Coal',
        'SUB': 'Coal',
        'LIG': 'Coal',
        'DFO': 'Oil',
        'RFO': 'Oil',
        'WH': 'Waste Heat',
        'OBG': 'Biogas',
        'LFG': 'Landfill Gas',
        'WDS': 'Biomass',
        'BLQ': 'Biomass',
    }

    def __init__(self):
        self.plants_df: Optional[pd.DataFrame] = None
        self.generators_df: Optional[pd.DataFrame] = None
        self.owners_df: Optional[pd.DataFrame] = None
        self.lookup: Dict[str, List[Dict]] = {}

    def load_eia_data(self, year: int = 2024) -> bool:
        """
        Load EIA Form 860 data files.

        Returns True if data loaded successfully.
        """
        plant_file = CACHE_DIR / f'2___Plant_Y{year}.xlsx'
        gen_file = CACHE_DIR / f'3_1_Generator_Y{year}.xlsx'
        owner_file = CACHE_DIR / f'4___Owner_Y{year}.xlsx'

        if not all(f.exists() for f in [plant_file, gen_file, owner_file]):
            logger.error(f"EIA {year} data files not found in {CACHE_DIR}")
            logger.info("Download from: https://www.eia.gov/electricity/data/eia860/")
            return False

        logger.info(f"Loading EIA Form 860 {year} data...")

        # Load plant data (location info)
        self.plants_df = pd.read_excel(plant_file, header=1)
        logger.info(f"  Loaded {len(self.plants_df):,} plants")

        # Load generator data (both operable and proposed)
        gen_xlsx = pd.ExcelFile(gen_file)
        operable = pd.read_excel(gen_xlsx, sheet_name='Operable', header=1)
        proposed = pd.read_excel(gen_xlsx, sheet_name='Proposed', header=1)
        self.generators_df = pd.concat([operable, proposed], ignore_index=True)
        logger.info(f"  Loaded {len(self.generators_df):,} generators ({len(operable):,} operable, {len(proposed):,} proposed)")

        # Load owner data
        self.owners_df = pd.read_excel(owner_file, header=1)
        logger.info(f"  Loaded {len(self.owners_df):,} owner records")

        # Build lookup index
        self._build_lookup()

        return True

    def _build_lookup(self):
        """Build lookup dictionary for efficient matching."""
        logger.info("Building EIA lookup index...")

        # Merge plant location with generator capacity
        merged = self.generators_df.merge(
            self.plants_df[['Plant Code', 'State', 'County', 'Latitude', 'Longitude']],
            on='Plant Code',
            how='left',
            suffixes=('', '_plant')
        )

        # Use State from generators if available, else from plants
        merged['state'] = merged['State'].fillna(merged.get('State_plant', ''))

        # Get owner/utility info - prefer owner name, fall back to utility
        owner_lookup = {}
        for _, row in self.owners_df.iterrows():
            key = (row['Plant Code'], row.get('Generator ID', ''))
            if key not in owner_lookup:
                owner_lookup[key] = {
                    'owner_name': row['Owner Name'],
                    'utility_name': row['Utility Name'],
                }

        # Also get utility name from generators
        utility_lookup = {}
        for _, row in merged.iterrows():
            plant_code = row['Plant Code']
            if plant_code not in utility_lookup:
                utility_lookup[plant_code] = row.get('Utility Name', '')

        # Build state+county+capacity lookup
        self.lookup = {}

        for _, row in merged.iterrows():
            state = str(row.get('state', '')).strip().upper()
            county = str(row.get('County', '')).strip().upper()
            capacity = row.get('Nameplate Capacity (MW)', 0)
            plant_code = row['Plant Code']
            plant_name = row.get('Plant Name', '')
            gen_id = row.get('Generator ID', '')
            fuel = row.get('Energy Source 1', '')

            if not state or pd.isna(capacity):
                continue

            # Get owner info
            owner_info = owner_lookup.get((plant_code, gen_id), {})
            owner_name = owner_info.get('owner_name', '')
            utility_name = owner_info.get('utility_name', '') or utility_lookup.get(plant_code, '')

            # Use owner name if available, else utility name
            entity_name = owner_name if owner_name else utility_name

            if not entity_name:
                continue

            record = {
                'plant_code': plant_code,
                'plant_name': plant_name,
                'owner_name': owner_name,
                'utility_name': utility_name,
                'entity_name': entity_name,
                'capacity': float(capacity) if not pd.isna(capacity) else 0,
                'fuel': fuel,
                'fuel_type': self.FUEL_MAP.get(fuel, fuel),
                'state': state,
                'county': county,
                'lat': row.get('Latitude'),
                'lon': row.get('Longitude'),
            }

            # Index by state + county
            key = f"{state}_{county}"
            if key not in self.lookup:
                self.lookup[key] = []
            self.lookup[key].append(record)

            # Also index by state only (for fallback matching)
            if state not in self.lookup:
                self.lookup[state] = []
            self.lookup[state].append(record)

        logger.info(f"  Built lookup with {len(self.lookup):,} location keys")

        # Count unique entities
        entities = set()
        for records in self.lookup.values():
            for r in records:
                if r['entity_name']:
                    entities.add(r['entity_name'])
        logger.info(f"  Found {len(entities):,} unique owner/utility entities")

    def match_project(self, state: str, county: str, capacity_mw: float,
                      fuel_type: str = None) -> Optional[EIAMatch]:
        """
        Match a single queue project to EIA data.

        Args:
            state: State abbreviation (e.g., 'TX', 'CA')
            county: County name
            capacity_mw: Project capacity in MW
            fuel_type: Optional fuel type to improve matching

        Returns:
            EIAMatch if found, None otherwise
        """
        if not self.lookup:
            logger.warning("EIA lookup not built. Call load_eia_data() first.")
            return None

        state = str(state).strip().upper()
        county = str(county).strip().upper()

        # Try state + county first
        key = f"{state}_{county}"
        candidates = self.lookup.get(key, [])

        if candidates:
            match = self._find_best_match(candidates, capacity_mw, fuel_type)
            if match:
                return EIAMatch(
                    queue_id='',  # Filled by caller
                    iso='',
                    eia_plant_code=str(match['plant_code']),
                    eia_plant_name=match['plant_name'],
                    owner_name=match['owner_name'],
                    utility_name=match['utility_name'],
                    match_method='state_county_capacity',
                    confidence=0.85 if match['capacity_diff'] < 0.1 else 0.75
                )

        # Fall back to state-only matching
        candidates = self.lookup.get(state, [])
        if candidates:
            match = self._find_best_match(candidates, capacity_mw, fuel_type)
            if match:
                return EIAMatch(
                    queue_id='',
                    iso='',
                    eia_plant_code=str(match['plant_code']),
                    eia_plant_name=match['plant_name'],
                    owner_name=match['owner_name'],
                    utility_name=match['utility_name'],
                    match_method='state_capacity',
                    confidence=0.65 if match['capacity_diff'] < 0.1 else 0.55
                )

        return None

    def _find_best_match(self, candidates: List[Dict], capacity_mw: float,
                         fuel_type: str = None) -> Optional[Dict]:
        """Find best matching EIA record from candidates."""
        if not candidates or not capacity_mw:
            return None

        best_match = None
        best_score = float('inf')

        for cand in candidates:
            cand_cap = cand.get('capacity', 0)
            if not cand_cap:
                continue

            # Calculate capacity difference (relative)
            cap_diff = abs(cand_cap - capacity_mw) / max(capacity_mw, 1)

            # Only consider matches within 50% capacity difference
            if cap_diff > 0.5:
                continue

            # Bonus for fuel type match
            score = cap_diff
            if fuel_type and cand.get('fuel_type'):
                cand_fuel = cand['fuel_type'].lower()
                queue_fuel = fuel_type.lower()
                if cand_fuel in queue_fuel or queue_fuel in cand_fuel:
                    score -= 0.1  # Boost score for fuel match

            if score < best_score:
                best_score = score
                best_match = {**cand, 'capacity_diff': cap_diff}

        return best_match

    def match_queue_dataframe(self, queue_df: pd.DataFrame) -> pd.DataFrame:
        """
        Match all projects in a queue DataFrame.

        Args:
            queue_df: DataFrame with columns: queue_id, iso, state, county, capacity_mw, type, developer

        Returns:
            DataFrame with additional columns: eia_owner, eia_match_method, eia_confidence
        """
        if not self.lookup:
            self.load_eia_data()

        results = []
        matched = 0

        for _, row in queue_df.iterrows():
            queue_id = row.get('queue_id', '')
            iso = row.get('iso', row.get('region', ''))
            state = row.get('state', '')
            county = row.get('county', '')
            capacity = row.get('capacity_mw', 0)
            fuel_type = row.get('type', '')
            current_dev = row.get('developer', '')

            # Skip if already has developer
            if current_dev and str(current_dev).lower() not in ['', 'nan', 'none', 'unknown']:
                results.append({
                    'queue_id': queue_id,
                    'eia_owner': None,
                    'eia_match_method': None,
                    'eia_confidence': None,
                })
                continue

            match = self.match_project(state, county, capacity, fuel_type)

            if match:
                matched += 1
                results.append({
                    'queue_id': queue_id,
                    'eia_owner': match.owner_name or match.utility_name,
                    'eia_match_method': match.match_method,
                    'eia_confidence': match.confidence,
                })
            else:
                results.append({
                    'queue_id': queue_id,
                    'eia_owner': None,
                    'eia_match_method': None,
                    'eia_confidence': None,
                })

        logger.info(f"Matched {matched:,} of {len(queue_df):,} projects to EIA data")

        results_df = pd.DataFrame(results)
        return queue_df.merge(results_df, on='queue_id', how='left')

    def enrich_database(self, min_confidence: float = 0.5) -> int:
        """
        Enrich queue database with EIA owner data.

        Returns number of projects updated.
        """
        import sqlite3

        if not self.lookup:
            self.load_eia_data()

        db_path = Path(__file__).parent / '.data' / 'queue.db'
        if not db_path.exists():
            logger.error(f"Database not found: {db_path}")
            return 0

        conn = sqlite3.connect(db_path)

        # Get projects missing developer
        query = """
            SELECT queue_id, region, state, county, capacity_mw, type
            FROM projects
            WHERE (developer IS NULL OR developer = '' OR developer = 'nan')
            AND state IS NOT NULL AND state != ''
        """
        projects_df = pd.read_sql_query(query, conn)
        logger.info(f"Found {len(projects_df):,} projects missing developer data")

        if projects_df.empty:
            conn.close()
            return 0

        # Match projects
        updated = 0
        for _, row in projects_df.iterrows():
            match = self.match_project(
                state=row['state'],
                county=row['county'],
                capacity_mw=row['capacity_mw'],
                fuel_type=row['type']
            )

            if match and match.confidence >= min_confidence:
                owner = match.owner_name or match.utility_name
                if owner:
                    conn.execute("""
                        UPDATE projects
                        SET developer = ?
                        WHERE queue_id = ? AND region = ?
                    """, (owner, row['queue_id'], row['region']))
                    updated += 1

        conn.commit()
        conn.close()

        logger.info(f"Updated {updated:,} projects with EIA owner data")
        return updated

    def get_coverage_stats(self) -> Dict:
        """Get statistics about EIA data coverage."""
        if not self.lookup:
            self.load_eia_data()

        # Count records by state
        by_state = {}
        for key, records in self.lookup.items():
            if '_' not in key:  # State-only keys
                by_state[key] = len(records)

        return {
            'total_lookup_keys': len(self.lookup),
            'records_by_state': dict(sorted(by_state.items(), key=lambda x: -x[1])[:20]),
            'total_plants': len(self.plants_df) if self.plants_df is not None else 0,
            'total_generators': len(self.generators_df) if self.generators_df is not None else 0,
            'total_owners': len(self.owners_df) if self.owners_df is not None else 0,
        }


def main():
    """CLI for EIA matching."""
    import argparse

    parser = argparse.ArgumentParser(description="EIA Form 860 Data Matcher")
    parser.add_argument('--load', action='store_true', help='Load and index EIA data')
    parser.add_argument('--stats', action='store_true', help='Show EIA data statistics')
    parser.add_argument('--enrich', action='store_true', help='Enrich queue database with EIA data')
    parser.add_argument('--min-confidence', type=float, default=0.5,
                       help='Minimum confidence for matches (default: 0.5)')
    parser.add_argument('--year', type=int, default=2024, help='EIA data year')

    args = parser.parse_args()

    matcher = EIAMatcher()

    if args.load or args.stats:
        matcher.load_eia_data(year=args.year)

    if args.stats:
        stats = matcher.get_coverage_stats()
        print("\n=== EIA Form 860 Statistics ===")
        print(f"Total plants: {stats['total_plants']:,}")
        print(f"Total generators: {stats['total_generators']:,}")
        print(f"Total owner records: {stats['total_owners']:,}")
        print(f"Lookup index keys: {stats['total_lookup_keys']:,}")
        print("\nTop states by records:")
        for state, count in list(stats['records_by_state'].items())[:10]:
            print(f"  {state}: {count:,}")

    if args.enrich:
        if not matcher.lookup:
            matcher.load_eia_data(year=args.year)
        updated = matcher.enrich_database(min_confidence=args.min_confidence)
        print(f"\nEnriched {updated:,} projects with EIA data")


if __name__ == '__main__':
    main()
