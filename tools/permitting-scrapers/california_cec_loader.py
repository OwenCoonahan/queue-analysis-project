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

    # CSV download URL from CEC GIS
    DATA_URL = 'https://cecgis-caenergy.opendata.arcgis.com/api/download/v1/items/4a702cd67be24ae7ab8173423a768e1b/csv?layers=0'

    # ArcGIS REST API endpoint (alternative)
    API_URL = 'https://services3.arcgis.com/bWPjFyq029ChCGur/arcgis/rest/services/Power_Plant/FeatureServer/0/query'

    # Technology mapping - CEC uses fuel source codes
    TECH_MAP = {
        # CEC PriEnergySource codes
        'SUN': 'Solar',
        'WND': 'Wind',
        'GEO': 'Geothermal',
        'WAT': 'Hydro',
        'NUC': 'Nuclear',
        'NG': 'Gas',
        'Natural Gas': 'Gas',
        'BIO': 'Biomass',
        'BIOG': 'Biogas',
        'MSW': 'Waste',
        'PET': 'Oil',
        'DFO': 'Oil',
        'COL': 'Coal',
        'OTH': 'Other',
        'BAT': 'Storage',
        'LFG': 'Biogas',        # Landfill Gas
        'WDS': 'Biomass',       # Wood/Wood Waste Solids
        'OBG': 'Biogas',        # Other Biogas
        'PC': 'Coal',           # Petroleum Coke
        'AB': 'Biomass',        # Agricultural Byproducts
        'BLQ': 'Biomass',       # Black Liquor
        'SGC': 'Gas',           # Synthetic Gas from Coal
        'PUR': 'Other',         # Purchased Steam
        # Full names (if API returns these)
        'Solar Photovoltaic': 'Solar',
        'Solar Thermal': 'Solar',
        'Wind': 'Wind',
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
            logger.warning(f"CSV download failed: {e}")

            # Try ArcGIS API as fallback
            logger.info("  Trying ArcGIS API fallback...")
            try:
                df = self._fetch_arcgis()
                if not df.empty:
                    df.to_csv(cache_file, index=False)
                    return df
            except Exception as api_err:
                logger.warning(f"  ArcGIS API also failed: {api_err}")

            if cache_file.exists():
                logger.info("  Falling back to cached data")
                return pd.read_csv(cache_file)
            return pd.DataFrame()

    def _fetch_arcgis(self) -> pd.DataFrame:
        """Fetch data via ArcGIS REST API."""
        all_features = []
        offset = 0
        batch_size = 2000

        while True:
            params = {
                'where': '1=1',
                'outFields': '*',
                'f': 'json',
                'resultOffset': offset,
                'resultRecordCount': batch_size
            }

            response = requests.get(self.API_URL, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            features = data.get('features', [])
            if not features:
                break

            for f in features:
                all_features.append(f.get('attributes', {}))

            logger.info(f"    Fetched {len(all_features)} records via API...")

            if len(features) < batch_size:
                break
            offset += batch_size

        if all_features:
            return pd.DataFrame(all_features)
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
        """Normalize raw CEC data to permit schema.

        Actual CEC columns (as of 2024):
        - OBJECTID, CECPlantID, PlantName, Retired Plant, Operator Company
        - County, Capacity_Latest, Units, PriEnergySource, StartDate
        - CEC_Jurisdictional, x (longitude), y (latitude)
        """
        records = []

        for _, row in raw_df.iterrows():
            # Build permit_id from CEC plant ID
            plant_id = row.get('CECPlantID') or row.get('OBJECTID')
            if pd.isna(plant_id):
                continue

            # CECPlantID is like 'S0335', OBJECTID is numeric
            if isinstance(plant_id, str):
                permit_id = f"CEC_{plant_id}"
            else:
                permit_id = f"CEC_{int(plant_id)}"

            # Get technology and normalize (PriEnergySource uses codes like SUN, WND, NG)
            raw_tech = str(row.get('PriEnergySource') or '').strip()
            technology = self.TECH_MAP.get(raw_tech, raw_tech if raw_tech else 'Unknown')

            # Get capacity (Capacity_Latest is in MW)
            capacity_mw = row.get('Capacity_Latest')

            # Determine status from Retired Plant flag and StartDate
            retired = row.get('Retired Plant')
            start_date = row.get('StartDate')

            if retired == 1 or str(retired).lower() == 'yes':
                status = 'Retired'
            elif start_date and not pd.isna(start_date):
                status = 'Operating'
            else:
                status = 'Proposed'

            # Get location
            county = str(row.get('County') or '').strip()
            # CEC uses x for longitude, y for latitude
            latitude = row.get('y')
            longitude = row.get('x')

            # Parse start date for COD
            expected_cod = None
            if start_date and not pd.isna(start_date):
                try:
                    # Format is typically 'M/D/YYYY'
                    expected_cod = pd.to_datetime(start_date).strftime('%Y-%m-%d')
                except Exception:
                    expected_cod = str(start_date)

            records.append({
                'permit_id': permit_id,
                'project_name': row.get('PlantName'),
                'developer': row.get('Operator Company'),
                'capacity_mw': float(capacity_mw) if capacity_mw and not pd.isna(capacity_mw) else None,
                'technology': technology,
                'state': 'CA',
                'county': county if county else None,
                'latitude': float(latitude) if latitude and not pd.isna(latitude) else None,
                'longitude': float(longitude) if longitude and not pd.isna(longitude) else None,
                'status': status,
                'status_code': None,  # CEC doesn't use EIA-style status codes
                'expected_cod': expected_cod,
                # CEC-specific fields
                'cec_plant_id': str(plant_id),
                'cec_jurisdictional': row.get('CEC_Jurisdictional'),
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
