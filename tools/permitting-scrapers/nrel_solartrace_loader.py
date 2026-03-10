#!/usr/bin/env python3
"""
NREL SolarTRACE Permitting Cycle Time Loader

Loads NREL's Solar Time-based Residential Analytics and Cycle time Estimator (SolarTRACE)
dataset which contains permitting, inspection, and interconnection cycle times for
1,500+ jurisdictions across 26 states.

Note: This data is primarily for residential rooftop PV (1-20kW), but provides useful
signals about permitting climate in different jurisdictions for utility-scale projects.

Data Source:
    https://data.nrel.gov/submissions/160 (redirects to nlr.gov)

Interactive Viewer:
    https://maps.nrel.gov/solarTRACE/

Usage:
    from permitting_scrapers import NRELSolarTraceLoader

    loader = NRELSolarTraceLoader()
    df = loader.load()

    # Get cycle time for a specific jurisdiction
    times = loader.get_jurisdiction_times(state='CA', county='Los Angeles')
"""

import pandas as pd
import requests
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent.parent / '.cache' / 'permits' / 'nrel'


class NRELSolarTraceLoader:
    """Load NREL SolarTRACE permitting cycle time data."""

    # NREL Data catalog page (requires manual download)
    DATA_PAGE = 'https://data.nrel.gov/submissions/160'

    # Possible file names to look for in cache directory
    EXPECTED_FILES = [
        'SolarTRACE Dataset v9-9-2025.xlsx',
        'solartrace_raw.xlsx',
        'SolarTRACE_Dataset.xlsx',
        'pii_cycletimes_requirements.xlsx',
    ]

    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.df: Optional[pd.DataFrame] = None
        self._raw_data: Dict[str, pd.DataFrame] = {}

    def fetch(self, use_cache: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Fetch SolarTRACE data.

        Note: NREL's data portal requires manual download. This method will:
        1. Check for manually downloaded files in cache directory
        2. Provide download instructions if no file found

        To use:
        1. Visit https://data.nrel.gov/submissions/160
        2. Download the SolarTRACE Dataset xlsx file
        3. Save to: {cache_dir}/solartrace_raw.xlsx

        Returns:
            Dictionary of DataFrames by sheet name
        """
        # Check for any expected file in cache
        for filename in self.EXPECTED_FILES:
            cache_file = self.cache_dir / filename
            if cache_file.exists():
                cache_age_days = (datetime.now().timestamp() - cache_file.stat().st_mtime) / 86400
                logger.info(f"Loading SolarTRACE data from {filename} ({cache_age_days:.1f} days old)")
                try:
                    return pd.read_excel(cache_file, sheet_name=None)
                except Exception as e:
                    logger.warning(f"Failed to read {filename}: {e}")

        # Also check Downloads folder
        downloads_paths = [
            Path.home() / 'Downloads' / 'SolarTRACE Dataset v9-9-2025.xlsx',
            Path.home() / 'Downloads' / 'SolarTRACE_Dataset.xlsx',
        ]

        for dl_path in downloads_paths:
            if dl_path.exists():
                logger.info(f"Found SolarTRACE data in Downloads, copying to cache...")
                cache_file = self.cache_dir / 'solartrace_raw.xlsx'
                import shutil
                shutil.copy(dl_path, cache_file)
                return pd.read_excel(cache_file, sheet_name=None)

        # No data available
        logger.warning("SolarTRACE data not available.")
        logger.info("")
        logger.info("  To download manually:")
        logger.info("  1. Visit: https://data.nrel.gov/submissions/160")
        logger.info("  2. Download 'SolarTRACE Dataset v9-9-2025.xlsx'")
        logger.info(f"  3. Save to: {self.cache_dir / 'solartrace_raw.xlsx'}")
        logger.info("")
        return {}

    def load(self, use_cache: bool = True) -> pd.DataFrame:
        """
        Load and normalize SolarTRACE data.

        Returns:
            DataFrame with jurisdiction-level cycle times
        """
        self._raw_data = self.fetch(use_cache=use_cache)

        if not self._raw_data:
            return pd.DataFrame()

        self.df = self._normalize()
        return self.df

    def _normalize(self) -> pd.DataFrame:
        """Normalize raw SolarTRACE data to cycle time schema."""
        records = []

        # Log available sheets
        logger.info(f"  Available sheets: {list(self._raw_data.keys())}")

        # Process each sheet - structure varies by version
        for sheet_name, sheet_df in self._raw_data.items():
            logger.info(f"  Processing sheet '{sheet_name}': {len(sheet_df)} rows")

            # Skip sheets that are metadata or documentation
            if 'readme' in sheet_name.lower() or 'metadata' in sheet_name.lower():
                continue

            # Try to identify jurisdiction columns
            for _, row in sheet_df.iterrows():
                try:
                    # Common column patterns in SolarTRACE
                    ahj_name = None
                    state = None
                    county = None

                    # Try different column name patterns
                    for col in sheet_df.columns:
                        col_lower = str(col).lower()
                        if 'ahj' in col_lower or 'jurisdiction' in col_lower:
                            ahj_name = row.get(col)
                        elif col_lower == 'state' or 'state' in col_lower:
                            state = row.get(col)
                        elif 'county' in col_lower:
                            county = row.get(col)

                    if not (ahj_name or (state and county)):
                        continue

                    # Extract cycle times (median days)
                    permit_time = None
                    inspection_time = None
                    interconnection_time = None
                    total_time = None

                    for col in sheet_df.columns:
                        col_lower = str(col).lower()
                        val = row.get(col)

                        if pd.isna(val):
                            continue

                        try:
                            val = float(val)
                        except (ValueError, TypeError):
                            continue

                        if 'permit' in col_lower and 'time' in col_lower:
                            permit_time = val
                        elif 'inspect' in col_lower and 'time' in col_lower:
                            inspection_time = val
                        elif 'interconnect' in col_lower and 'time' in col_lower:
                            interconnection_time = val
                        elif 'total' in col_lower or 'overall' in col_lower:
                            total_time = val

                    # Only include if we have meaningful data
                    if any([permit_time, inspection_time, interconnection_time, total_time]):
                        records.append({
                            'ahj_name': str(ahj_name) if ahj_name else None,
                            'state': str(state)[:2].upper() if state else None,
                            'county': str(county) if county else None,
                            'permit_days_median': permit_time,
                            'inspection_days_median': inspection_time,
                            'interconnection_days_median': interconnection_time,
                            'total_days_median': total_time,
                            'data_source': 'nrel_solartrace',
                            'sheet': sheet_name,
                        })

                except Exception as e:
                    continue

        df = pd.DataFrame(records)

        if df.empty:
            # If normalization failed, return raw first sheet as fallback
            first_sheet = list(self._raw_data.keys())[0] if self._raw_data else None
            if first_sheet:
                logger.info(f"  Using raw data from '{first_sheet}' sheet")
                df = self._raw_data[first_sheet].copy()
                df['data_source'] = 'nrel_solartrace'

        logger.info(f"Loaded {len(df):,} SolarTRACE records")
        return df

    def get_jurisdiction_times(
        self,
        state: str = None,
        county: str = None,
        ahj_name: str = None
    ) -> Optional[Dict]:
        """
        Get cycle times for a specific jurisdiction.

        Args:
            state: Two-letter state code
            county: County name
            ahj_name: Authority Having Jurisdiction name

        Returns:
            Dictionary with cycle times or None if not found
        """
        if self.df is None:
            self.load()

        if self.df is None or self.df.empty:
            return None

        df = self.df

        # Filter by criteria
        if state:
            df = df[df['state'] == state.upper()]
        if county:
            df = df[df['county'].str.lower().str.contains(county.lower(), na=False)]
        if ahj_name:
            df = df[df['ahj_name'].str.lower().str.contains(ahj_name.lower(), na=False)]

        if df.empty:
            return None

        # Return first match
        row = df.iloc[0]
        return {
            'ahj_name': row.get('ahj_name'),
            'state': row.get('state'),
            'county': row.get('county'),
            'permit_days_median': row.get('permit_days_median'),
            'inspection_days_median': row.get('inspection_days_median'),
            'interconnection_days_median': row.get('interconnection_days_median'),
            'total_days_median': row.get('total_days_median'),
        }

    def get_state_summary(self, state: str) -> Dict:
        """Get summary statistics for a state."""
        if self.df is None:
            self.load()

        if self.df is None or self.df.empty:
            return {'error': 'No data loaded'}

        state_df = self.df[self.df['state'] == state.upper()]

        if state_df.empty:
            return {'error': f'No data for state {state}'}

        return {
            'state': state.upper(),
            'jurisdiction_count': len(state_df),
            'permit_days_median': state_df['permit_days_median'].median() if 'permit_days_median' in state_df.columns else None,
            'inspection_days_median': state_df['inspection_days_median'].median() if 'inspection_days_median' in state_df.columns else None,
            'interconnection_days_median': state_df['interconnection_days_median'].median() if 'interconnection_days_median' in state_df.columns else None,
            'total_days_median': state_df['total_days_median'].median() if 'total_days_median' in state_df.columns else None,
        }

    def get_stats(self) -> Dict:
        """Get overall statistics about loaded data."""
        if self.df is None:
            self.load()

        if self.df is None or self.df.empty:
            return {'total_records': 0, 'note': 'No data loaded. Download manually from data.nrel.gov'}

        df = self.df

        stats = {
            'total_records': len(df),
            'states_covered': df['state'].nunique() if 'state' in df.columns else 0,
            'sheets_processed': len(self._raw_data),
        }

        # Add median cycle times if available
        for col in ['permit_days_median', 'inspection_days_median', 'interconnection_days_median', 'total_days_median']:
            if col in df.columns:
                stats[f'overall_{col}'] = df[col].median()

        if 'state' in df.columns:
            stats['by_state'] = df['state'].value_counts().head(10).to_dict()

        return stats


def main():
    """CLI for SolarTRACE loader."""
    import argparse

    parser = argparse.ArgumentParser(description="NREL SolarTRACE Cycle Time Loader")
    parser.add_argument('--stats', action='store_true', help='Show statistics')
    parser.add_argument('--export', type=str, metavar='FILE', help='Export to CSV')
    parser.add_argument('--no-cache', action='store_true', help='Skip cache')
    parser.add_argument('--state', type=str, help='Get summary for a state')
    parser.add_argument('--county', type=str, help='Look up a specific county')

    args = parser.parse_args()

    loader = NRELSolarTraceLoader()
    df = loader.load(use_cache=not args.no_cache)

    if args.state and args.county:
        times = loader.get_jurisdiction_times(state=args.state, county=args.county)
        if times:
            print(f"\n=== Cycle Times for {args.county}, {args.state} ===")
            for k, v in times.items():
                if v is not None:
                    print(f"  {k}: {v}")
        else:
            print(f"No data found for {args.county}, {args.state}")
    elif args.state:
        summary = loader.get_state_summary(args.state)
        print(f"\n=== {args.state} Summary ===")
        for k, v in summary.items():
            if v is not None:
                print(f"  {k}: {v}")

    if args.stats:
        stats = loader.get_stats()
        print("\n=== SolarTRACE Statistics ===")
        print(f"Total records: {stats.get('total_records', 0):,}")
        print(f"States covered: {stats.get('states_covered', 0)}")
        if stats.get('by_state'):
            print("\nBy State (top 10):")
            for state, count in list(stats['by_state'].items())[:10]:
                print(f"  {state}: {count:,}")

    if args.export and not df.empty:
        df.to_csv(args.export, index=False)
        print(f"\nExported {len(df):,} records to {args.export}")


if __name__ == '__main__':
    main()
