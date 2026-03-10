#!/usr/bin/env python3
"""
EIA Planned Generators Loader

Loads EIA Form 860 Proposed sheet to track permit status of planned generators.

EIA Status Codes:
    P  - Planned (regulatory approvals not initiated)
    L  - Regulatory Approved (not under construction)
    T  - Regulatory Approved (under construction)
    U  - Under Construction (<50% complete)
    V  - Under Construction (>50% complete)
    TS - Testing/Commissioning

Usage:
    from permitting_scrapers import EIAPlannedLoader

    loader = EIAPlannedLoader()
    df = loader.load()
    print(f"Loaded {len(df)} planned generators")
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent.parent / '.cache' / 'eia'


class EIAPlannedLoader:
    """Load EIA Form 860 Proposed generators for permit tracking."""

    # Status code mapping to normalized status descriptions
    STATUS_MAP = {
        'P': 'Proposed - Permits Pending',
        'L': 'Permitted - Not Started',
        'T': 'Permitted - Under Construction',
        'U': 'Under Construction (<50%)',
        'V': 'Under Construction (>50%)',
        'TS': 'Testing/Commissioning',
        'OT': 'Other',
    }

    # Technology mapping from EIA codes to standardized names
    TECH_MAP = {
        'Solar Photovoltaic': 'Solar',
        'Onshore Wind Turbine': 'Wind',
        'Batteries': 'Storage',
        'Natural Gas Fired Combined Cycle': 'Gas',
        'Natural Gas Fired Combustion Turbine': 'Gas',
        'Natural Gas Internal Combustion Engine': 'Gas',
        'Natural Gas Steam Turbine': 'Gas',
        'Natural Gas with Compressed Air Storage': 'Gas',
        'Petroleum Liquids': 'Oil',
        'All Other': 'Other',
        'Hydroelectric Pumped Storage': 'Hydro',
        'Conventional Hydroelectric': 'Hydro',
        'Nuclear': 'Nuclear',
        'Landfill Gas': 'Biogas',
        'Wood/Wood Waste Biomass': 'Biomass',
        'Geothermal': 'Geothermal',
        'Offshore Wind Turbine': 'Wind',
        'Solar Thermal without Energy Storage': 'Solar',
        'Solar Thermal with Energy Storage': 'Solar',
        'Flywheels': 'Storage',
        'Other Natural Gas': 'Gas',
        'Other Gases': 'Gas',
    }

    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or CACHE_DIR
        self.df: Optional[pd.DataFrame] = None

    def load(self, year: int = 2024, use_cache: bool = True) -> pd.DataFrame:
        """
        Load EIA Form 860 Proposed generators.

        Args:
            year: EIA data year (default 2024)
            use_cache: Use cached parquet if available

        Returns:
            DataFrame with normalized permit columns
        """
        cache_file = self.cache_dir / f'eia_proposed_normalized_{year}.parquet'

        # Check for cached normalized data
        if use_cache and cache_file.exists():
            cache_age_hours = (datetime.now().timestamp() - cache_file.stat().st_mtime) / 3600
            if cache_age_hours < 24:
                logger.info(f"Loading cached EIA proposed data ({cache_age_hours:.1f}h old)")
                self.df = pd.read_parquet(cache_file)
                return self.df

        # Load from raw Excel
        raw_file = self.cache_dir / f'3_1_Generator_Y{year}.xlsx'
        if not raw_file.exists():
            raise FileNotFoundError(
                f"EIA Form 860 file not found: {raw_file}\n"
                f"Download from: https://www.eia.gov/electricity/data/eia860/"
            )

        logger.info(f"Loading EIA Form 860 {year} Proposed sheet...")
        xlsx = pd.ExcelFile(raw_file)
        raw_df = pd.read_excel(xlsx, sheet_name='Proposed', header=1)
        logger.info(f"  Loaded {len(raw_df):,} raw records")

        # Normalize to permit schema
        self.df = self._normalize(raw_df)
        logger.info(f"  Normalized to {len(self.df):,} permit records")

        # Cache normalized data
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.df.to_parquet(cache_file, index=False)
        logger.info(f"  Cached to {cache_file.name}")

        return self.df

    def _normalize(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Normalize raw EIA data to permit schema."""
        records = []

        for _, row in raw_df.iterrows():
            plant_code = row.get('Plant Code')
            gen_id = row.get('Generator ID', '')

            if pd.isna(plant_code):
                continue

            # Build permit_id from plant code + generator ID
            permit_id = f"EIA_{int(plant_code)}_{gen_id}"

            # Get status code and normalize
            status_code = str(row.get('Status', '')).strip()
            status = self.STATUS_MAP.get(status_code, f'Unknown ({status_code})')

            # Get technology and normalize
            raw_tech = str(row.get('Technology', '')).strip()
            technology = self.TECH_MAP.get(raw_tech, raw_tech)

            # Get capacity
            capacity_mw = row.get('Nameplate Capacity (MW)')
            if pd.isna(capacity_mw):
                capacity_mw = row.get('Summer Capacity (MW)')

            # Build expected COD from planned operation month/year
            op_month = row.get('Planned Operation Month')
            op_year = row.get('Planned Operation Year')
            expected_cod = None
            if not pd.isna(op_year):
                if not pd.isna(op_month):
                    expected_cod = f"{int(op_year)}-{int(op_month):02d}-01"
                else:
                    expected_cod = f"{int(op_year)}-01-01"

            # Get location
            state = str(row.get('State', '')).strip()
            county = str(row.get('County', '')).strip()
            latitude = row.get('Latitude')
            longitude = row.get('Longitude')

            records.append({
                'permit_id': permit_id,
                'project_name': row.get('Plant Name'),
                'developer': row.get('Utility Name'),
                'capacity_mw': float(capacity_mw) if not pd.isna(capacity_mw) else None,
                'technology': technology,
                'state': state if state else None,
                'county': county if county else None,
                'latitude': float(latitude) if not pd.isna(latitude) else None,
                'longitude': float(longitude) if not pd.isna(longitude) else None,
                'status': status,
                'status_code': status_code,
                'expected_cod': expected_cod,
                # Additional EIA fields for matching
                'eia_plant_code': int(plant_code),
                'eia_generator_id': gen_id,
                'eia_utility_id': row.get('Utility ID'),
                'prime_mover': row.get('Prime Mover'),
            })

        df = pd.DataFrame(records)

        # Add aggregated capacity for multi-generator plants
        plant_capacity = df.groupby('eia_plant_code')['capacity_mw'].sum().to_dict()
        df['plant_total_mw'] = df['eia_plant_code'].map(plant_capacity)

        return df

    def get_stats(self) -> Dict:
        """Get statistics about loaded data."""
        if self.df is None:
            self.load()

        df = self.df

        return {
            'total_permits': len(df),
            'total_capacity_gw': df['capacity_mw'].sum() / 1000,
            'by_status': df['status'].value_counts().to_dict(),
            'by_status_code': df['status_code'].value_counts().to_dict(),
            'by_technology': df['technology'].value_counts().to_dict(),
            'by_state': df['state'].value_counts().head(15).to_dict(),
            'permitted_count': len(df[df['status_code'].isin(['L', 'T'])]),
            'under_construction_count': len(df[df['status_code'].isin(['U', 'V'])]),
            'testing_count': len(df[df['status_code'] == 'TS']),
        }

    def get_permitted(self) -> pd.DataFrame:
        """Get only permitted projects (L, T status)."""
        if self.df is None:
            self.load()
        return self.df[self.df['status_code'].isin(['L', 'T'])].copy()

    def get_under_construction(self) -> pd.DataFrame:
        """Get projects under construction (U, V status)."""
        if self.df is None:
            self.load()
        return self.df[self.df['status_code'].isin(['U', 'V'])].copy()

    def get_by_state(self, state: str) -> pd.DataFrame:
        """Get projects in a specific state."""
        if self.df is None:
            self.load()
        return self.df[self.df['state'] == state.upper()].copy()

    def get_by_technology(self, technology: str) -> pd.DataFrame:
        """Get projects by technology type."""
        if self.df is None:
            self.load()
        return self.df[self.df['technology'].str.contains(technology, case=False, na=False)].copy()


def main():
    """CLI for EIA planned generators loader."""
    import argparse

    parser = argparse.ArgumentParser(description="EIA Form 860 Planned Generators Loader")
    parser.add_argument('--year', type=int, default=2024, help='EIA data year')
    parser.add_argument('--stats', action='store_true', help='Show statistics')
    parser.add_argument('--permitted', action='store_true', help='Show only permitted projects')
    parser.add_argument('--state', type=str, help='Filter by state')
    parser.add_argument('--export', type=str, metavar='FILE', help='Export to CSV')
    parser.add_argument('--no-cache', action='store_true', help='Skip cache')

    args = parser.parse_args()

    loader = EIAPlannedLoader()
    df = loader.load(year=args.year, use_cache=not args.no_cache)

    if args.state:
        df = loader.get_by_state(args.state)
        print(f"\nProjects in {args.state.upper()}: {len(df)}")

    if args.permitted:
        df = loader.get_permitted()
        print(f"\nPermitted projects: {len(df)}")

    if args.stats:
        stats = loader.get_stats()
        print("\n=== EIA Planned Generators Statistics ===")
        print(f"Total permits: {stats['total_permits']:,}")
        print(f"Total capacity: {stats['total_capacity_gw']:.1f} GW")
        print(f"\nBy Status Code:")
        for code, count in sorted(stats['by_status_code'].items()):
            status_name = EIAPlannedLoader.STATUS_MAP.get(code, code)
            print(f"  {code}: {count:,} ({status_name})")
        print(f"\nBy Technology:")
        for tech, count in list(stats['by_technology'].items())[:10]:
            print(f"  {tech}: {count:,}")
        print(f"\nTop States:")
        for state, count in list(stats['by_state'].items())[:10]:
            print(f"  {state}: {count:,}")

    if args.export:
        df.to_csv(args.export, index=False)
        print(f"\nExported {len(df):,} records to {args.export}")


if __name__ == '__main__':
    main()
