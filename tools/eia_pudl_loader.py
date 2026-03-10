#!/usr/bin/env python3
"""
EIA PUDL Data Loader

Extracts EIA Form 860 ownership data from the PUDL database to match
interconnection queue projects with plant owners/developers.

The PUDL database (Public Utility Data Liberation) contains processed
EIA 860 data with ownership records linked to generators and plants.

Matching Strategy:
1. State + County + Fuel Type + Capacity (within 10%) -> 0.88-0.90 confidence
2. State + County + Fuel Type + Capacity (within 20%) -> 0.83-0.85 confidence
3. State + Fuel Type + Capacity (within 10%) -> 0.73-0.75 confidence
4. State + Fuel Type + Capacity (within 20%) -> 0.68-0.70 confidence (review)

Usage:
    from eia_pudl_loader import EIAPudlLoader

    loader = EIAPudlLoader()
    loader.build_index()
    match = loader.match_project(state='TX', county='ECTOR', fuel='Solar', capacity_mw=150)
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent / '.cache'
PUDL_DB = CACHE_DIR / 'pudl' / 'pudl.sqlite'


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class EIAOwnerRecord:
    """An EIA ownership record with location and generator data."""
    owner_name: str
    owner_id: int
    plant_id: int
    generator_id: str
    plant_name: str
    capacity_mw: float
    fuel_code: str
    fuel_type: str  # Standardized
    state: str
    county: str
    city: str
    latitude: float
    longitude: float
    fraction_owned: float
    operational_status: str


@dataclass
class EIAMatch:
    """Result of matching a queue project to EIA data."""
    matched: bool
    owner_name: Optional[str] = None
    owner_id: Optional[int] = None
    plant_id: Optional[int] = None
    plant_name: Optional[str] = None
    capacity_mw: Optional[float] = None
    confidence: float = 0.0
    match_method: str = 'none'
    capacity_diff_pct: float = 0.0
    location_match: str = 'none'  # 'state_county', 'state_only'


# =============================================================================
# FUEL TYPE MAPPING
# =============================================================================

# EIA energy source codes to standardized fuel types
FUEL_TYPE_MAP = {
    # Solar
    'SUN': 'Solar',
    # Wind
    'WND': 'Wind',
    # Storage
    'MWH': 'Storage',
    'WAT': 'Hydro',  # Some pumped storage uses WAT
    # Hydro
    'HYC': 'Hydro',  # Conventional hydro
    # Gas
    'NG': 'Gas',
    'LFG': 'Gas',  # Landfill gas
    'OBG': 'Gas',  # Other biomass gas
    'BFG': 'Gas',  # Blast furnace gas
    # Nuclear
    'NUC': 'Nuclear',
    # Coal
    'BIT': 'Coal',
    'SUB': 'Coal',
    'LIG': 'Coal',
    'WC': 'Coal',
    'RC': 'Coal',
    'PC': 'Coal',
    # Oil
    'DFO': 'Oil',
    'RFO': 'Oil',
    'JF': 'Oil',
    'KER': 'Oil',
    'WO': 'Oil',
    # Biomass
    'WDS': 'Biomass',
    'BLQ': 'Biomass',
    'AB': 'Biomass',
    'MSW': 'Biomass',
    'OBS': 'Biomass',
    'SLW': 'Biomass',
    # Geothermal
    'GEO': 'Geothermal',
    # Other
    'OTH': 'Other',
    'PUR': 'Other',
    'WH': 'Other',
}

# Queue fuel types to standardized (for reverse mapping)
QUEUE_FUEL_MAP = {
    'solar': 'Solar',
    'wind': 'Wind',
    'storage': 'Storage',
    'battery': 'Storage',
    'bess': 'Storage',
    'gas': 'Gas',
    'natural gas': 'Gas',
    'ng': 'Gas',
    'hydro': 'Hydro',
    'hydroelectric': 'Hydro',
    'nuclear': 'Nuclear',
    'coal': 'Coal',
    'oil': 'Oil',
    'petroleum': 'Oil',
    'biomass': 'Biomass',
    'geothermal': 'Geothermal',
    'hybrid': 'Hybrid',
    'solar + storage': 'Hybrid',
    'wind + storage': 'Hybrid',
}


# =============================================================================
# EIA PUDL LOADER
# =============================================================================

class EIAPudlLoader:
    """
    Load and index EIA Form 860 ownership data from PUDL database.

    Provides high-confidence matching of queue projects to EIA owner/operators
    based on location (state/county) and capacity/fuel type.
    """

    def __init__(self, pudl_db_path: str = None):
        """Initialize loader with PUDL database path."""
        if pudl_db_path is None:
            pudl_db_path = PUDL_DB
        self.db_path = Path(pudl_db_path)
        self._conn = None

        # Indexes for efficient lookup
        self._state_county_fuel_index: Dict[str, List[EIAOwnerRecord]] = defaultdict(list)
        self._state_fuel_index: Dict[str, List[EIAOwnerRecord]] = defaultdict(list)
        self._owner_names: set = set()

    def _get_conn(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._conn is None:
            if not self.db_path.exists():
                raise FileNotFoundError(f"PUDL database not found: {self.db_path}")
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def build_index(self, min_capacity_mw: float = 1.0) -> int:
        """
        Build lookup indexes from PUDL EIA ownership data.

        Extracts the latest ownership records and indexes by:
        - state + county + fuel type
        - state + fuel type

        Args:
            min_capacity_mw: Minimum generator capacity to include

        Returns:
            Number of records indexed
        """
        logger.info("Building EIA ownership index from PUDL database...")

        conn = self._get_conn()

        # Get the most recent report date
        cursor = conn.execute("SELECT MAX(report_date) FROM core_eia860__scd_ownership")
        latest_date = cursor.fetchone()[0]
        logger.info(f"  Using report date: {latest_date}")

        # Query ownership + generators + plants
        query = """
        SELECT
            o.owner_utility_name_eia as owner_name,
            o.owner_utility_id_eia as owner_id,
            o.plant_id_eia as plant_id,
            o.generator_id,
            o.fraction_owned,
            p.plant_name_eia as plant_name,
            p.state,
            p.county,
            p.city,
            p.latitude,
            p.longitude,
            g.capacity_mw,
            g.energy_source_code_1 as fuel_code,
            g.operational_status
        FROM core_eia860__scd_ownership o
        JOIN core_eia__entity_plants p ON o.plant_id_eia = p.plant_id_eia
        JOIN core_eia860__scd_generators g
            ON o.plant_id_eia = g.plant_id_eia
            AND o.generator_id = g.generator_id
            AND o.report_date = g.report_date
        WHERE o.report_date = ?
        AND o.owner_utility_name_eia IS NOT NULL
        AND g.capacity_mw >= ?
        AND g.operational_status IN ('existing', 'OP', 'proposed', 'P', 'SB', 'TS', 'V')
        """

        cursor = conn.execute(query, (latest_date, min_capacity_mw))

        count = 0
        for row in cursor:
            # Skip if missing critical data
            if not row['state'] or not row['capacity_mw']:
                continue

            fuel_code = row['fuel_code'] or 'OTH'
            fuel_type = FUEL_TYPE_MAP.get(fuel_code, 'Other')

            record = EIAOwnerRecord(
                owner_name=row['owner_name'],
                owner_id=row['owner_id'],
                plant_id=row['plant_id'],
                generator_id=row['generator_id'],
                plant_name=row['plant_name'],
                capacity_mw=row['capacity_mw'],
                fuel_code=fuel_code,
                fuel_type=fuel_type,
                state=row['state'].upper(),
                county=(row['county'] or '').upper(),
                city=row['city'] or '',
                latitude=row['latitude'] or 0,
                longitude=row['longitude'] or 0,
                fraction_owned=row['fraction_owned'] or 1.0,
                operational_status=row['operational_status'] or ''
            )

            # Index by state + county + fuel
            state_county_key = f"{record.state}_{record.county}_{record.fuel_type}"
            self._state_county_fuel_index[state_county_key].append(record)

            # Index by state + fuel
            state_key = f"{record.state}_{record.fuel_type}"
            self._state_fuel_index[state_key].append(record)

            self._owner_names.add(record.owner_name)
            count += 1

        logger.info(f"  Indexed {count:,} ownership records")
        logger.info(f"  {len(self._owner_names):,} unique owners")
        logger.info(f"  {len(self._state_county_fuel_index):,} state+county+fuel combinations")
        logger.info(f"  {len(self._state_fuel_index):,} state+fuel combinations")

        return count

    def match_project(
        self,
        state: str,
        county: str = None,
        fuel_type: str = None,
        capacity_mw: float = None,
        capacity_tolerance: float = 0.20
    ) -> EIAMatch:
        """
        Match a queue project to EIA ownership data.

        Args:
            state: State abbreviation (e.g., 'TX', 'CA')
            county: County name (optional, improves confidence)
            fuel_type: Fuel/technology type (e.g., 'Solar', 'Wind')
            capacity_mw: Project capacity in MW
            capacity_tolerance: Maximum capacity difference ratio (default 20%)

        Returns:
            EIAMatch with owner details and confidence score
        """
        if not state:
            return EIAMatch(matched=False, match_method='missing_state')

        # Ensure index is built
        if not self._state_fuel_index:
            self.build_index()

        state = state.upper().strip()
        county = (county or '').upper().strip()

        # Standardize fuel type
        std_fuel = None
        if fuel_type:
            fuel_lower = fuel_type.lower().strip()
            std_fuel = QUEUE_FUEL_MAP.get(fuel_lower)
            if not std_fuel:
                # Try direct match
                for key, val in QUEUE_FUEL_MAP.items():
                    if key in fuel_lower:
                        std_fuel = val
                        break
            if not std_fuel:
                std_fuel = fuel_type.title()

        # Try state + county + fuel first
        if county and std_fuel:
            key = f"{state}_{county}_{std_fuel}"
            candidates = self._state_county_fuel_index.get(key, [])
            match = self._find_best_match(candidates, capacity_mw, capacity_tolerance)
            if match:
                match.location_match = 'state_county_fuel'
                match.match_method = 'eia_state_county_fuel_capacity'
                # High confidence for full location match
                if match.capacity_diff_pct <= 0.10:
                    match.confidence = 0.90 - (match.capacity_diff_pct * 0.2)
                else:
                    match.confidence = 0.85 - (match.capacity_diff_pct * 0.2)
                return match

        # Try state + fuel (lower confidence)
        if std_fuel:
            key = f"{state}_{std_fuel}"
            candidates = self._state_fuel_index.get(key, [])
            match = self._find_best_match(candidates, capacity_mw, capacity_tolerance)
            if match:
                match.location_match = 'state_fuel'
                match.match_method = 'eia_state_fuel_capacity'
                if match.capacity_diff_pct <= 0.10:
                    match.confidence = 0.75 - (match.capacity_diff_pct * 0.2)
                else:
                    match.confidence = 0.70 - (match.capacity_diff_pct * 0.2)
                return match

        return EIAMatch(matched=False, match_method='no_match')

    def _find_best_match(
        self,
        candidates: List[EIAOwnerRecord],
        capacity_mw: float,
        tolerance: float
    ) -> Optional[EIAMatch]:
        """Find best matching record from candidates based on capacity."""
        if not candidates or not capacity_mw:
            return None

        best_match = None
        best_diff = float('inf')

        for cand in candidates:
            if not cand.capacity_mw:
                continue

            diff = abs(cand.capacity_mw - capacity_mw) / max(capacity_mw, 1)

            if diff <= tolerance and diff < best_diff:
                best_diff = diff
                best_match = EIAMatch(
                    matched=True,
                    owner_name=cand.owner_name,
                    owner_id=cand.owner_id,
                    plant_id=cand.plant_id,
                    plant_name=cand.plant_name,
                    capacity_mw=cand.capacity_mw,
                    capacity_diff_pct=diff
                )

        return best_match

    def get_owner_list(self) -> List[str]:
        """Get list of all unique owner names."""
        if not self._owner_names:
            self.build_index()
        return sorted(self._owner_names)

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the EIA data."""
        if not self._state_fuel_index:
            self.build_index()

        # Count by fuel type
        fuel_counts = defaultdict(int)
        for key in self._state_fuel_index:
            _, fuel = key.rsplit('_', 1)
            fuel_counts[fuel] += len(self._state_fuel_index[key])

        # Count by state
        state_counts = defaultdict(int)
        for key in self._state_fuel_index:
            state, _ = key.split('_', 1)
            state_counts[state] += len(self._state_fuel_index[key])

        return {
            'unique_owners': len(self._owner_names),
            'state_county_fuel_keys': len(self._state_county_fuel_index),
            'state_fuel_keys': len(self._state_fuel_index),
            'by_fuel_type': dict(fuel_counts),
            'top_states': dict(sorted(state_counts.items(), key=lambda x: -x[1])[:10]),
        }

    def enrich_queue_projects(
        self,
        projects_df: pd.DataFrame,
        min_confidence: float = 0.85
    ) -> pd.DataFrame:
        """
        Enrich queue projects with EIA owner data.

        Args:
            projects_df: DataFrame with columns: queue_id, region, state, county, type, capacity_mw, developer
            min_confidence: Minimum confidence to apply match

        Returns:
            DataFrame with additional columns: eia_owner, eia_confidence, eia_match_method
        """
        if not self._state_fuel_index:
            self.build_index()

        results = []
        matched = 0
        high_confidence = 0

        for _, row in projects_df.iterrows():
            # Skip if already has developer
            current_dev = row.get('developer', '')
            if current_dev and str(current_dev).lower() not in ['', 'nan', 'none', 'unknown']:
                results.append({
                    'queue_id': row.get('queue_id'),
                    'eia_owner': None,
                    'eia_confidence': None,
                    'eia_match_method': 'skipped_has_developer'
                })
                continue

            match = self.match_project(
                state=row.get('state'),
                county=row.get('county'),
                fuel_type=row.get('type'),
                capacity_mw=row.get('capacity_mw')
            )

            if match.matched and match.confidence >= min_confidence:
                matched += 1
                if match.confidence >= 0.85:
                    high_confidence += 1
                results.append({
                    'queue_id': row.get('queue_id'),
                    'eia_owner': match.owner_name,
                    'eia_confidence': match.confidence,
                    'eia_match_method': match.match_method
                })
            else:
                results.append({
                    'queue_id': row.get('queue_id'),
                    'eia_owner': None,
                    'eia_confidence': match.confidence if match.matched else None,
                    'eia_match_method': match.match_method
                })

        logger.info(f"Matched {matched:,} of {len(projects_df):,} projects ({matched/len(projects_df)*100:.1f}%)")
        logger.info(f"  High confidence (>=0.85): {high_confidence:,}")

        return pd.DataFrame(results)

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI for EIA PUDL loader."""
    import argparse

    parser = argparse.ArgumentParser(description="EIA PUDL Data Loader")
    parser.add_argument('--build', action='store_true', help='Build index from PUDL database')
    parser.add_argument('--stats', action='store_true', help='Show EIA data statistics')
    parser.add_argument('--owners', action='store_true', help='List unique owners')
    parser.add_argument('--match', nargs=4, metavar=('STATE', 'COUNTY', 'FUEL', 'CAPACITY'),
                       help='Match a project: STATE COUNTY FUEL CAPACITY_MW')

    args = parser.parse_args()

    if not PUDL_DB.exists():
        print(f"PUDL database not found: {PUDL_DB}")
        print("Download from: https://zenodo.org/records/...")
        return

    loader = EIAPudlLoader()

    if args.build or args.stats or args.owners or args.match:
        loader.build_index()

    if args.stats:
        stats = loader.get_stats()
        print("\n=== EIA PUDL Statistics ===")
        print(f"Unique owners: {stats['unique_owners']:,}")
        print(f"State+County+Fuel keys: {stats['state_county_fuel_keys']:,}")
        print(f"State+Fuel keys: {stats['state_fuel_keys']:,}")
        print("\nBy fuel type:")
        for fuel, count in sorted(stats['by_fuel_type'].items(), key=lambda x: -x[1]):
            print(f"  {fuel}: {count:,}")
        print("\nTop states:")
        for state, count in stats['top_states'].items():
            print(f"  {state}: {count:,}")

    if args.owners:
        owners = loader.get_owner_list()
        print(f"\n=== {len(owners):,} Unique Owners ===")
        for owner in owners[:50]:
            print(f"  {owner}")
        if len(owners) > 50:
            print(f"  ... and {len(owners) - 50} more")

    if args.match:
        state, county, fuel, capacity = args.match
        match = loader.match_project(
            state=state,
            county=county if county != '-' else None,
            fuel_type=fuel if fuel != '-' else None,
            capacity_mw=float(capacity) if capacity != '-' else None
        )
        print(f"\n=== Match Result ===")
        print(f"  Matched: {match.matched}")
        if match.matched:
            print(f"  Owner: {match.owner_name}")
            print(f"  Plant: {match.plant_name}")
            print(f"  Confidence: {match.confidence:.1%}")
            print(f"  Method: {match.match_method}")
            print(f"  Capacity diff: {match.capacity_diff_pct:.1%}")

    loader.close()


if __name__ == '__main__':
    main()
