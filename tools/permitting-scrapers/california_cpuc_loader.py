#!/usr/bin/env python3
"""
California CPUC RPS Database Loader

Loads CPUC Renewable Portfolio Standard (RPS) executed contracts data.
Contains utility-scale renewable contracts from PG&E, SCE, and SDG&E.

Data Source:
    https://www.cpuc.ca.gov/rps_reports_data/

Usage:
    from permitting_scrapers import CaliforniaCPUCLoader

    loader = CaliforniaCPUCLoader()
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


class CaliforniaCPUCLoader:
    """Load CPUC RPS executed contracts database."""

    # CPUC RPS data URLs (these may change - check CPUC website for current links)
    # The RPS report is typically an Excel file
    RPS_URL = 'https://www.cpuc.ca.gov/-/media/cpuc-website/divisions/energy-division/documents/rps/rps-procurement-status/2024-rps-procurement-summary.xlsx'

    # Alternative: DG Stats for smaller projects
    DG_STATS_URL = 'https://www.californiadgstats.ca.gov/downloads/'

    # Technology mapping
    TECH_MAP = {
        'Solar PV': 'Solar',
        'Solar': 'Solar',
        'Wind': 'Wind',
        'Geothermal': 'Geothermal',
        'Small Hydro': 'Hydro',
        'Biomass': 'Biomass',
        'Biogas': 'Biogas',
        'Battery': 'Storage',
        'Storage': 'Storage',
    }

    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.df: Optional[pd.DataFrame] = None

    def fetch(self, use_cache: bool = True) -> pd.DataFrame:
        """
        Fetch CPUC RPS data.

        Note: CPUC data is typically published as Excel files that may require
        manual download. This loader attempts to fetch automatically but may
        fall back to cached data.

        Args:
            use_cache: Use cached file if available

        Returns:
            Raw DataFrame from CPUC RPS
        """
        cache_file = self.cache_dir / 'cpuc_rps_raw.xlsx'

        # Check cache
        if use_cache and cache_file.exists():
            cache_age_days = (datetime.now().timestamp() - cache_file.stat().st_mtime) / 86400
            if cache_age_days < 30:  # Cache for 30 days (monthly updates)
                logger.info(f"Loading cached CPUC RPS data ({cache_age_days:.1f} days old)")
                try:
                    return pd.read_excel(cache_file)
                except Exception as e:
                    logger.warning(f"Failed to read cache: {e}")

        # Try to fetch from source
        logger.info(f"Attempting to fetch CPUC RPS data...")
        try:
            response = requests.get(
                self.RPS_URL,
                timeout=60,
                headers={'User-Agent': 'Mozilla/5.0 Queue-Analysis/1.0'}
            )
            response.raise_for_status()

            # Save to cache
            cache_file.write_bytes(response.content)
            logger.info(f"  Saved to cache: {cache_file.name}")

            # Parse Excel
            df = pd.read_excel(cache_file)
            logger.info(f"  Loaded {len(df):,} records")
            return df

        except requests.RequestException as e:
            logger.warning(f"Could not fetch CPUC data automatically: {e}")
            logger.info("  CPUC RPS data requires manual download from:")
            logger.info("  https://www.cpuc.ca.gov/rps_reports_data/")

            if cache_file.exists():
                logger.info("  Using cached data")
                try:
                    return pd.read_excel(cache_file)
                except Exception:
                    pass

            return pd.DataFrame()

    def load(self, use_cache: bool = True) -> pd.DataFrame:
        """
        Load and normalize CPUC RPS data.

        Returns:
            DataFrame with normalized permit columns
        """
        raw_df = self.fetch(use_cache=use_cache)

        if raw_df.empty:
            logger.warning("No CPUC data available. Download manually from:")
            logger.warning("https://www.cpuc.ca.gov/rps_reports_data/")
            return raw_df

        self.df = self._normalize(raw_df)
        return self.df

    def _normalize(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Normalize raw CPUC RPS data to permit schema."""
        records = []

        # Column names vary by report version - try common patterns
        id_cols = ['Contract ID', 'Project ID', 'Resource ID', 'Project Name']
        name_cols = ['Project Name', 'Facility Name', 'Resource Name']
        capacity_cols = ['Capacity (MW)', 'Contracted Capacity', 'MW', 'Nameplate']
        tech_cols = ['Technology', 'Resource Type', 'Fuel Type']
        developer_cols = ['Developer', 'Owner', 'Seller']
        cod_cols = ['COD', 'Online Date', 'Expected Online', 'Commercial Operation Date']
        county_cols = ['County', 'Location']

        def get_col(row, col_options):
            for col in col_options:
                if col in row.index and not pd.isna(row[col]):
                    return row[col]
            return None

        for idx, row in raw_df.iterrows():
            # Build permit_id
            permit_id_val = get_col(row, id_cols)
            if permit_id_val:
                permit_id = f"CPUC_{permit_id_val}"
            else:
                permit_id = f"CPUC_ROW_{idx}"

            # Get technology and normalize
            raw_tech = str(get_col(row, tech_cols) or '').strip()
            technology = self.TECH_MAP.get(raw_tech, raw_tech)

            # Get capacity
            capacity_mw = get_col(row, capacity_cols)

            # Get COD
            cod = get_col(row, cod_cols)
            expected_cod = None
            if cod:
                try:
                    if isinstance(cod, datetime):
                        expected_cod = cod.strftime('%Y-%m-%d')
                    else:
                        expected_cod = pd.to_datetime(cod).strftime('%Y-%m-%d')
                except Exception:
                    expected_cod = str(cod)

            records.append({
                'permit_id': permit_id,
                'project_name': get_col(row, name_cols),
                'developer': get_col(row, developer_cols),
                'capacity_mw': float(capacity_mw) if capacity_mw and not pd.isna(capacity_mw) else None,
                'technology': technology,
                'state': 'CA',
                'county': get_col(row, county_cols),
                'latitude': None,
                'longitude': None,
                'status': 'Contracted',  # RPS contracts are executed
                'status_code': 'RPS',
                'expected_cod': expected_cod,
            })

        df = pd.DataFrame(records)

        # Remove rows with no meaningful data
        df = df.dropna(subset=['project_name', 'capacity_mw'], how='all')

        logger.info(f"Normalized {len(df):,} CPUC RPS permits")
        return df

    def get_stats(self) -> Dict:
        """Get statistics about loaded data."""
        if self.df is None:
            self.load()

        if self.df is None or self.df.empty:
            return {'total_permits': 0, 'note': 'No data loaded. Manual download may be required.'}

        df = self.df

        return {
            'total_permits': len(df),
            'total_capacity_gw': df['capacity_mw'].sum() / 1000 if 'capacity_mw' in df.columns else 0,
            'by_technology': df['technology'].value_counts().to_dict() if 'technology' in df.columns else {},
            'by_county': df['county'].value_counts().head(10).to_dict() if 'county' in df.columns else {},
        }


def main():
    """CLI for CPUC loader."""
    import argparse

    parser = argparse.ArgumentParser(description="California CPUC RPS Database Loader")
    parser.add_argument('--stats', action='store_true', help='Show statistics')
    parser.add_argument('--export', type=str, metavar='FILE', help='Export to CSV')
    parser.add_argument('--no-cache', action='store_true', help='Skip cache')

    args = parser.parse_args()

    loader = CaliforniaCPUCLoader()
    df = loader.load(use_cache=not args.no_cache)

    if args.stats:
        stats = loader.get_stats()
        print("\n=== CPUC RPS Database Statistics ===")
        print(f"Total contracts: {stats.get('total_permits', 0):,}")
        if stats.get('total_capacity_gw'):
            print(f"Total capacity: {stats['total_capacity_gw']:.1f} GW")
        if stats.get('note'):
            print(f"Note: {stats['note']}")
        if stats.get('by_technology'):
            print(f"\nBy Technology:")
            for tech, count in list(stats['by_technology'].items())[:10]:
                print(f"  {tech}: {count:,}")

    if args.export and not df.empty:
        df.to_csv(args.export, index=False)
        print(f"\nExported {len(df):,} records to {args.export}")


if __name__ == '__main__':
    main()
