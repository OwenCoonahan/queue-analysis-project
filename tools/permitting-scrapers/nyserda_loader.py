#!/usr/bin/env python3
"""
New York NYSERDA Large-Scale Renewables Loader

Loads large-scale renewable projects (25+ MW) from NYSERDA's public database.
Includes interconnection queue numbers, permit status, and developer info.

Data Source:
    https://data.ny.gov/Energy-Environment/Large-scale-Renewable-Projects-Reported-by-NYSERDA/dprp-55ye

API: Socrata Open Data API (SODA)

Usage:
    from permitting_scrapers import NYSERDALoader

    loader = NYSERDALoader()
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

CACHE_DIR = Path(__file__).parent.parent / '.cache' / 'permits' / 'nyserda'


class NYSERDALoader:
    """Load NYSERDA large-scale renewable projects."""

    # Socrata API endpoint
    API_URL = 'https://data.ny.gov/resource/dprp-55ye.json'

    # CSV download (backup)
    CSV_URL = 'https://data.ny.gov/api/views/dprp-55ye/rows.csv?accessType=DOWNLOAD'

    # Technology mapping
    TECH_MAP = {
        'Solar': 'Solar',
        'Land-Based Wind': 'Wind',
        'Offshore Wind': 'Wind',
        'Hydroelectric': 'Hydro',
        'Fuel Cell': 'Fuel Cell',
        'Renewable Natural Gas': 'Biogas',
        'Biomass': 'Biomass',
    }

    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.df: Optional[pd.DataFrame] = None

    def fetch(self, use_cache: bool = True) -> pd.DataFrame:
        """
        Fetch NYSERDA data via Socrata API.

        Args:
            use_cache: Use cached file if available and recent

        Returns:
            Raw DataFrame from NYSERDA
        """
        cache_file = self.cache_dir / 'nyserda_raw.json'

        # Check cache
        if use_cache and cache_file.exists():
            cache_age_hours = (datetime.now().timestamp() - cache_file.stat().st_mtime) / 3600
            if cache_age_hours < 24:
                logger.info(f"Loading cached NYSERDA data ({cache_age_hours:.1f}h old)")
                return pd.read_json(cache_file)

        # Fetch from API (paginated - Socrata limits to 50k per request)
        logger.info("Fetching NYSERDA data from data.ny.gov...")
        all_records = []
        offset = 0
        limit = 10000  # Fetch in chunks

        try:
            while True:
                params = {
                    '$limit': limit,
                    '$offset': offset,
                    '$order': 'project_name'
                }

                response = requests.get(
                    self.API_URL,
                    params=params,
                    timeout=60,
                    headers={'Accept': 'application/json'}
                )
                response.raise_for_status()

                data = response.json()
                if not data:
                    break

                all_records.extend(data)
                logger.info(f"  Fetched {len(all_records)} records...")

                if len(data) < limit:
                    break
                offset += limit

            df = pd.DataFrame(all_records)

            # Cache the data
            df.to_json(cache_file, orient='records', indent=2)
            logger.info(f"  Cached {len(df)} records to {cache_file.name}")

            return df

        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")

            # Try CSV fallback
            logger.info("  Trying CSV download fallback...")
            try:
                response = requests.get(self.CSV_URL, timeout=120)
                response.raise_for_status()

                from io import StringIO
                df = pd.read_csv(StringIO(response.text))
                df.to_json(cache_file, orient='records', indent=2)
                return df

            except Exception as csv_err:
                logger.error(f"CSV fallback failed: {csv_err}")

                if cache_file.exists():
                    logger.info("  Using stale cache")
                    return pd.read_json(cache_file)

                return pd.DataFrame()

    def load(self, use_cache: bool = True) -> pd.DataFrame:
        """
        Load and normalize NYSERDA data.

        Returns:
            DataFrame with normalized permit columns
        """
        raw_df = self.fetch(use_cache=use_cache)

        if raw_df.empty:
            return raw_df

        self.df = self._normalize(raw_df)
        return self.df

    def _normalize(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Normalize raw NYSERDA data to permit schema."""
        records = []

        for _, row in raw_df.iterrows():
            project_name = row.get('project_name', '')
            if not project_name:
                continue

            # Build permit_id
            permit_id = f"NYSERDA_{project_name.replace(' ', '_')[:50]}"

            # Get technology
            raw_tech = str(row.get('renewable_technology', '')).strip()
            technology = self.TECH_MAP.get(raw_tech, raw_tech)

            # Get capacity (try multiple fields)
            capacity_mw = None
            for cap_field in ['new_renewable_capacity_mw', 'bid_capacity_mw']:
                val = row.get(cap_field)
                if val and not pd.isna(val):
                    try:
                        capacity_mw = float(val)
                        break
                    except (ValueError, TypeError):
                        pass

            # Get storage capacity if available
            storage_mw = row.get('energy_storage_power_capacity_mwac')
            storage_mwh = row.get('energy_storage_energy_capacity_mwh')

            # Get COD
            cod_year = row.get('year_of_commercial_operation')
            expected_cod = None
            if cod_year and not pd.isna(cod_year):
                try:
                    expected_cod = f"{int(float(cod_year))}-01-01"
                except (ValueError, TypeError):
                    expected_cod = str(cod_year)

            # Get permit/regulatory status
            permit_process = row.get('permit_process', '')
            regulatory = row.get('regulatory_permitting', '')
            article_vii = row.get('article_vii', '')

            # Determine status based on available info
            status = row.get('project_status', 'Unknown')

            # Get interconnection queue number (links to our queue data!)
            queue_number = row.get('interconnection_queue_number', '')

            records.append({
                'permit_id': permit_id,
                'project_name': project_name,
                'developer': row.get('developer_name', ''),
                'capacity_mw': capacity_mw,
                'technology': technology,
                'state': 'NY',
                'county': row.get('county_province', ''),
                'latitude': None,
                'longitude': None,
                'status': status,
                'status_code': None,
                'expected_cod': expected_cod,
                # NYSERDA-specific fields
                'nyiso_zone': row.get('nyiso_zone', ''),
                'interconnection_queue_number': queue_number,
                'permit_process': permit_process,
                'regulatory_permitting': regulatory,
                'article_vii': article_vii,
                'storage_mw': float(storage_mw) if storage_mw and not pd.isna(storage_mw) else None,
                'storage_mwh': float(storage_mwh) if storage_mwh and not pd.isna(storage_mwh) else None,
                'counterparty': row.get('counterparty', ''),
                'contract_duration': row.get('contract_duration', ''),
            })

        df = pd.DataFrame(records)
        logger.info(f"Normalized {len(df):,} NYSERDA permits")
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
            'by_zone': df['nyiso_zone'].value_counts().to_dict() if 'nyiso_zone' in df.columns else {},
            'with_queue_number': df['interconnection_queue_number'].notna().sum(),
        }

    def get_with_queue_links(self) -> pd.DataFrame:
        """Get projects that have interconnection queue numbers (for matching)."""
        if self.df is None:
            self.load()
        return self.df[self.df['interconnection_queue_number'].notna() &
                       (self.df['interconnection_queue_number'] != '')].copy()


def main():
    """CLI for NYSERDA loader."""
    import argparse

    parser = argparse.ArgumentParser(description="NYSERDA Large-Scale Renewables Loader")
    parser.add_argument('--stats', action='store_true', help='Show statistics')
    parser.add_argument('--export', type=str, metavar='FILE', help='Export to CSV')
    parser.add_argument('--no-cache', action='store_true', help='Skip cache')
    parser.add_argument('--with-queue', action='store_true', help='Only show projects with queue numbers')

    args = parser.parse_args()

    loader = NYSERDALoader()
    df = loader.load(use_cache=not args.no_cache)

    if args.with_queue:
        df = loader.get_with_queue_links()
        print(f"\nProjects with interconnection queue numbers: {len(df)}")

    if args.stats:
        stats = loader.get_stats()
        print("\n=== NYSERDA Large-Scale Renewables Statistics ===")
        print(f"Total projects: {stats['total_permits']:,}")
        print(f"Total capacity: {stats['total_capacity_gw']:.1f} GW")
        print(f"With queue numbers: {stats['with_queue_number']:,}")
        print(f"\nBy Status:")
        for status, count in list(stats.get('by_status', {}).items())[:10]:
            print(f"  {status}: {count:,}")
        print(f"\nBy Technology:")
        for tech, count in list(stats.get('by_technology', {}).items())[:10]:
            print(f"  {tech}: {count:,}")
        print(f"\nBy NYISO Zone:")
        for zone, count in list(stats.get('by_zone', {}).items())[:10]:
            print(f"  {zone}: {count:,}")

    if args.export:
        df.to_csv(args.export, index=False)
        print(f"\nExported {len(df):,} records to {args.export}")


if __name__ == '__main__':
    main()
