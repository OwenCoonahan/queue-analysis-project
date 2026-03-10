#!/usr/bin/env python3
"""
California CEC Power Plants Loader

Loads California Energy Commission Power Plants dataset from California Open Data portal.
Contains all CEC-jurisdictional power plants (typically 50+ MW).

Data Source:
    https://data.ca.gov/dataset/california-power-plants

Usage:
    from permitting_scrapers import CaliforniaCECLoader

    loader = CaliforniaCECLoader()
    df = loader.load()
"""

import pandas as pd
import requests
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent.parent / '.cache' / 'permits' / 'california'


class CaliforniaCECLoader:
    """Load California CEC Power Plants dataset."""

    # CSV download URL from California Open Data
    DATA_URL = 'https://data.ca.gov/dataset/359c0035-4ed7-454a-b475-f68e93cee70a/resource/4a7e8c47-1a40-4c3d-ad2e-6b5a71e8ec76/download/power_plants.csv'

    # Technology mapping
    TECH_MAP = {
        'Solar Photovoltaic': 'Solar',
        'Solar Thermal': 'Solar',
        'Wind': 'Wind',
        'Natural Gas': 'Gas',
        'Geothermal': 'Geothermal',
        'Biomass': 'Biomass',
        'Hydroelectric': 'Hydro',
        'Nuclear': 'Nuclear',
        'Battery Storage': 'Storage',
        'Petroleum': 'Oil',
        'Coal': 'Coal',
    }

    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.df: Optional[pd.DataFrame] = None

    def fetch(self, use_cache: bool = True) -> pd.DataFrame:
        """
        Fetch CEC power plants data.

        Args:
            use_cache: Use cached file if available and recent

        Returns:
            Raw DataFrame from CEC
        """
        cache_file = self.cache_dir / 'cec_power_plants_raw.csv'

        # Check cache
        if use_cache and cache_file.exists():
            cache_age_days = (datetime.now().timestamp() - cache_file.stat().st_mtime) / 86400
            if cache_age_days < 7:  # Cache for 7 days
                logger.info(f"Loading cached CEC data ({cache_age_days:.1f} days old)")
                return pd.read_csv(cache_file)

        # Fetch from source
        logger.info(f"Fetching CEC power plants data from data.ca.gov...")
        try:
            response = requests.get(self.DATA_URL, timeout=60)
            response.raise_for_status()

            # Save to cache
            cache_file.write_bytes(response.content)
            logger.info(f"  Saved to cache: {cache_file.name}")

            # Parse CSV
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            logger.info(f"  Loaded {len(df):,} records")
            return df

        except requests.RequestException as e:
            logger.error(f"Failed to fetch CEC data: {e}")
            if cache_file.exists():
                logger.info("  Falling back to cached data")
                return pd.read_csv(cache_file)
            return pd.DataFrame()

    def load(self, use_cache: bool = True) -> pd.DataFrame:
        """
        Load and normalize CEC power plants data.

        Returns:
            DataFrame with normalized permit columns
        """
        raw_df = self.fetch(use_cache=use_cache)

        if raw_df.empty:
            return raw_df

        self.df = self._normalize(raw_df)
        return self.df

    def _normalize(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Normalize raw CEC data to permit schema."""
        records = []

        for _, row in raw_df.iterrows():
            # Build permit_id from CEC plant ID
            plant_id = row.get('OBJECTID') or row.get('Plant_ID') or row.get('Plant ID')
            if pd.isna(plant_id):
                continue

            permit_id = f"CEC_{int(plant_id)}"

            # Get technology and normalize
            raw_tech = str(row.get('Fuel_Type') or row.get('Primary_Fuel') or '').strip()
            technology = self.TECH_MAP.get(raw_tech, raw_tech)

            # Get capacity
            capacity_mw = row.get('Nameplate_Capacity') or row.get('Capacity_MW') or row.get('Capacity')

            # Get status
            status = str(row.get('Status') or row.get('Plant_Status') or '').strip()
            if not status:
                status = 'Unknown'

            # Get location
            county = str(row.get('County') or '').strip()
            latitude = row.get('Latitude') or row.get('Lat')
            longitude = row.get('Longitude') or row.get('Long')

            records.append({
                'permit_id': permit_id,
                'project_name': row.get('Plant_Name') or row.get('Plant Name'),
                'developer': row.get('Owner') or row.get('Operator'),
                'capacity_mw': float(capacity_mw) if not pd.isna(capacity_mw) else None,
                'technology': technology,
                'state': 'CA',
                'county': county if county else None,
                'latitude': float(latitude) if not pd.isna(latitude) else None,
                'longitude': float(longitude) if not pd.isna(longitude) else None,
                'status': status,
                'status_code': None,  # CEC doesn't use EIA-style status codes
                'expected_cod': None,
                # CEC-specific fields
                'cec_plant_id': int(plant_id),
                'cec_docket': row.get('Docket') or row.get('Docket_Number'),
            })

        df = pd.DataFrame(records)
        logger.info(f"Normalized {len(df):,} CEC permits")
        return df

    def get_stats(self) -> Dict:
        """Get statistics about loaded data."""
        if self.df is None:
            self.load()

        df = self.df

        return {
            'total_permits': len(df),
            'total_capacity_gw': df['capacity_mw'].sum() / 1000 if 'capacity_mw' in df.columns else 0,
            'by_status': df['status'].value_counts().to_dict() if 'status' in df.columns else {},
            'by_technology': df['technology'].value_counts().to_dict() if 'technology' in df.columns else {},
            'by_county': df['county'].value_counts().head(10).to_dict() if 'county' in df.columns else {},
        }


def main():
    """CLI for CEC loader."""
    import argparse

    parser = argparse.ArgumentParser(description="California CEC Power Plants Loader")
    parser.add_argument('--stats', action='store_true', help='Show statistics')
    parser.add_argument('--export', type=str, metavar='FILE', help='Export to CSV')
    parser.add_argument('--no-cache', action='store_true', help='Skip cache')

    args = parser.parse_args()

    loader = CaliforniaCECLoader()
    df = loader.load(use_cache=not args.no_cache)

    if args.stats:
        stats = loader.get_stats()
        print("\n=== CEC Power Plants Statistics ===")
        print(f"Total permits: {stats['total_permits']:,}")
        print(f"Total capacity: {stats['total_capacity_gw']:.1f} GW")
        print(f"\nBy Status:")
        for status, count in list(stats.get('by_status', {}).items())[:10]:
            print(f"  {status}: {count:,}")
        print(f"\nBy Technology:")
        for tech, count in list(stats.get('by_technology', {}).items())[:10]:
            print(f"  {tech}: {count:,}")

    if args.export:
        df.to_csv(args.export, index=False)
        print(f"\nExported {len(df):,} records to {args.export}")


if __name__ == '__main__':
    main()
