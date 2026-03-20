#!/usr/bin/env python3
"""
EIA Form 860M Monthly Loader

Downloads and processes EIA Form 860M (Monthly Update to the Annual Electric
Generator Report). This is the backbone of the sub-5 MW pre-operational
project pipeline — updated monthly by EIA with all generators >1 MW.

Data source: https://www.eia.gov/electricity/data/eia860m/
File: EIA860M.xlsx (multiple sheets)

Key sheets:
  - Operating: Currently operational generators
  - Planned: Proposed/planned generators (status P, L, T)
  - Under Construction: Under construction generators (status U, V, TS)
  - Retired: Recently retired generators

Status codes:
  P  = Planned (regulatory approvals not started)
  L  = Regulatory Hold (approvals pending/delayed)
  T  = Regulatory Approved (approved but not yet under construction)
  U  = Under Construction (<50% complete)
  V  = Under Construction (>50% complete)
  TS = Testing/Commissioning (construction complete, not yet commercial)
  OP = Operating
  RE = Retired

Usage:
    python3 eia860m_loader.py                     # Download + show stats
    python3 eia860m_loader.py --refresh           # Download + update database
    python3 eia860m_loader.py --stats             # Stats from cached data
    python3 eia860m_loader.py --export FILE.csv   # Export to CSV
    python3 eia860m_loader.py --filter-sardar     # Show Sardar-relevant projects
"""

import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json
import warnings
import logging
import re

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent / '.cache' / 'eia860m'
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# EIA 860M download URL — they publish a single xlsx file updated monthly
EIA_860M_URL = "https://www.eia.gov/electricity/data/eia860m/xls/june_generator2025.xlsx"

# Status code descriptions
STATUS_MAP = {
    'P': 'Planned',
    'L': 'Regulatory Hold',
    'T': 'Regulatory Approved',
    'U': 'Under Construction (<50%)',
    'V': 'Under Construction (>50%)',
    'TS': 'Testing & Commissioning',
    'OT': 'Other',
    'SB': 'Standby',
    'OP': 'Operating',
    'RE': 'Retired',
    'OS': 'Out of Service',
    'BU': 'Backup',
}

# Technology normalization
TECH_MAP = {
    'Solar Photovoltaic': 'Solar',
    'Solar Thermal with Energy Storage': 'Solar + Storage',
    'Solar Thermal without Energy Storage': 'Solar Thermal',
    'Onshore Wind Turbine': 'Wind',
    'Offshore Wind Turbine': 'Offshore Wind',
    'Batteries': 'Storage',
    'Natural Gas Fired Combined Cycle': 'Gas',
    'Natural Gas Fired Combustion Turbine': 'Gas',
    'Natural Gas Steam Turbine': 'Gas',
    'Natural Gas Internal Combustion Engine': 'Gas',
    'Conventional Hydroelectric': 'Hydro',
    'Pumped Storage': 'Pumped Storage',
    'Nuclear': 'Nuclear',
    'Petroleum Liquids': 'Oil',
    'Coal Integrated Gasification Combined Cycle': 'Coal',
    'Conventional Steam Coal': 'Coal',
    'Wood/Wood Waste Biomass': 'Biomass',
    'Landfill Gas': 'Biomass',
    'Municipal Solid Waste': 'Biomass',
    'Geothermal': 'Geothermal',
    'Hydroelectric Pumped Storage': 'Pumped Storage',
    'All Other': 'Other',
    'Flywheels': 'Storage',
    'Other Waste Biomass': 'Biomass',
    'Other Gases': 'Gas',
    'Other Natural Gas': 'Gas',
    'Petroleum Coke': 'Coal',
}

# Sector mapping for developer type classification
SECTOR_MAP = {
    'IPP Non-CHP': 'IPP',
    'IPP CHP': 'IPP',
    'Electric Utility': 'Utility',
    'Commercial Non-CHP': 'Commercial',
    'Commercial CHP': 'Commercial',
    'Industrial Non-CHP': 'Industrial',
    'Industrial CHP': 'Industrial',
}


class EIA860MLoader:
    """Load and process EIA Form 860M monthly generator data."""

    def __init__(self):
        self.cache_file = CACHE_DIR / 'eia860m_latest.xlsx'
        self.metadata_file = CACHE_DIR / 'eia860m_metadata.json'

    def _get_metadata(self) -> Dict:
        """Read cached metadata."""
        if self.metadata_file.exists():
            with open(self.metadata_file) as f:
                return json.load(f)
        return {}

    def _save_metadata(self, meta: Dict):
        """Save metadata about the cached file."""
        with open(self.metadata_file, 'w') as f:
            json.dump(meta, f, indent=2, default=str)

    def download(self, url: str = None, force: bool = False) -> Path:
        """
        Download EIA-860M Excel file.

        Args:
            url: Override download URL (for specific months)
            force: Force re-download even if cache exists

        Returns:
            Path to downloaded file
        """
        url = url or EIA_860M_URL

        # Check cache freshness (re-download if older than 7 days)
        meta = self._get_metadata()
        if not force and self.cache_file.exists():
            cache_age_days = (datetime.now().timestamp() - self.cache_file.stat().st_mtime) / 86400
            if cache_age_days < 7:
                logger.info(f"Using cached EIA-860M data ({cache_age_days:.1f} days old)")
                return self.cache_file

        logger.info(f"Downloading EIA-860M from {url}...")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) QueueAnalysis/1.0',
        }

        response = requests.get(url, headers=headers, timeout=120)
        response.raise_for_status()

        with open(self.cache_file, 'wb') as f:
            f.write(response.content)

        # Save metadata
        self._save_metadata({
            'download_url': url,
            'download_date': datetime.now().isoformat(),
            'file_size_mb': len(response.content) / (1024 * 1024),
        })

        logger.info(f"  Downloaded {len(response.content) / (1024*1024):.1f} MB")
        return self.cache_file

    def load_from_file(self, filepath: Path = None) -> Dict[str, pd.DataFrame]:
        """
        Load EIA-860M data from Excel file.

        Returns dict of DataFrames keyed by sheet category:
          - 'planned': Planned generators (P, L, T status)
          - 'under_construction': Under construction (U, V, TS)
          - 'operating': Operating generators
          - 'retired': Retired generators
        """
        filepath = filepath or self.cache_file

        if not filepath.exists():
            raise FileNotFoundError(f"EIA-860M file not found: {filepath}")

        logger.info(f"Loading EIA-860M from {filepath.name}...")

        xlsx = pd.ExcelFile(filepath)
        sheet_names = xlsx.sheet_names
        logger.info(f"  Sheets found: {sheet_names}")

        results = {}

        # Try to find the right sheets — EIA changes names slightly between months
        for sheet in sheet_names:
            sheet_lower = sheet.lower()
            try:
                # Read with header on row 1 (0-indexed) — EIA has a title row
                df = pd.read_excel(xlsx, sheet_name=sheet, header=1)

                if 'planned' in sheet_lower or 'proposed' in sheet_lower:
                    results['planned'] = df
                    logger.info(f"  Planned: {len(df):,} generators")
                elif 'construction' in sheet_lower or 'under con' in sheet_lower:
                    results['under_construction'] = df
                    logger.info(f"  Under Construction: {len(df):,} generators")
                elif 'operating' in sheet_lower or 'operable' in sheet_lower:
                    results['operating'] = df
                    logger.info(f"  Operating: {len(df):,} generators")
                elif 'retired' in sheet_lower or 'retire' in sheet_lower:
                    results['retired'] = df
                    logger.info(f"  Retired: {len(df):,} generators")
            except Exception as e:
                logger.warning(f"  Could not read sheet '{sheet}': {e}")

        return results

    def normalize(self, sheets: Dict[str, pd.DataFrame],
                  pre_operational_only: bool = True) -> pd.DataFrame:
        """
        Normalize EIA-860M data into a standard format.

        Args:
            sheets: Dict of DataFrames from load_from_file()
            pre_operational_only: If True, only return non-operating projects

        Returns:
            Normalized DataFrame with standard columns
        """
        frames = []

        target_sheets = ['planned', 'under_construction']
        if not pre_operational_only:
            target_sheets.extend(['operating', 'retired'])

        for sheet_key in target_sheets:
            if sheet_key not in sheets:
                continue

            df = sheets[sheet_key].copy()

            # Standardize column names — EIA uses varying names
            col_map = {}
            for col in df.columns:
                col_clean = str(col).strip()
                cl = col_clean.lower()

                if 'entity id' in cl or 'utility id' in cl:
                    col_map[col] = 'entity_id'
                elif 'entity name' in cl or 'utility name' in cl:
                    col_map[col] = 'entity_name'
                elif 'plant id' in cl or 'plant code' in cl:
                    col_map[col] = 'plant_id'
                elif 'plant name' in cl:
                    col_map[col] = 'plant_name'
                elif 'plant state' in cl or col_clean == 'State':
                    col_map[col] = 'state'
                elif cl == 'county':
                    col_map[col] = 'county'
                elif 'generator id' in cl:
                    col_map[col] = 'generator_id'
                elif 'nameplate' in cl and 'capacity' in cl and 'mw' in cl:
                    col_map[col] = 'nameplate_mw'
                elif 'net summer' in cl and 'capacity' in cl:
                    col_map[col] = 'net_summer_mw'
                elif 'technology' in cl:
                    col_map[col] = 'technology'
                elif 'energy source' in cl and '1' in cl:
                    col_map[col] = 'energy_source'
                elif 'status' in cl and 'proposed' not in cl:
                    col_map[col] = 'status_code'
                elif 'sector' in cl and 'name' in cl:
                    col_map[col] = 'sector'
                elif 'sector' in cl and 'number' not in cl and 'name' not in cl:
                    col_map[col] = 'sector'
                elif 'planned operation' in cl and 'month' in cl:
                    col_map[col] = 'planned_op_month'
                elif 'planned operation' in cl and 'year' in cl:
                    col_map[col] = 'planned_op_year'
                elif 'effective' in cl and 'month' in cl:
                    col_map[col] = 'effective_month'
                elif 'effective' in cl and 'year' in cl:
                    col_map[col] = 'effective_year'
                elif 'latitude' in cl:
                    col_map[col] = 'latitude'
                elif 'longitude' in cl:
                    col_map[col] = 'longitude'
                elif 'balancing authority' in cl:
                    col_map[col] = 'balancing_authority'

            df = df.rename(columns=col_map)

            # Add sheet source
            df['sheet_source'] = sheet_key

            frames.append(df)

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)

        # Normalize types
        if 'nameplate_mw' in combined.columns:
            combined['nameplate_mw'] = pd.to_numeric(combined['nameplate_mw'], errors='coerce')

        if 'latitude' in combined.columns:
            combined['latitude'] = pd.to_numeric(combined['latitude'], errors='coerce')

        if 'longitude' in combined.columns:
            combined['longitude'] = pd.to_numeric(combined['longitude'], errors='coerce')

        # Create unique project ID: plant_id + generator_id
        if 'plant_id' in combined.columns:
            combined['plant_id'] = combined['plant_id'].astype(str).str.replace('.0', '', regex=False)
        if 'generator_id' in combined.columns:
            combined['permit_id'] = combined['plant_id'].astype(str) + '_' + combined['generator_id'].astype(str)
        else:
            combined['permit_id'] = combined['plant_id'].astype(str)

        # Normalize technology
        if 'technology' in combined.columns:
            combined['tech_clean'] = combined['technology'].map(
                lambda x: TECH_MAP.get(x, x) if pd.notna(x) else 'Unknown'
            )

        # Normalize status
        if 'status_code' in combined.columns:
            combined['status_label'] = combined['status_code'].map(
                lambda x: STATUS_MAP.get(str(x).strip(), x) if pd.notna(x) else 'Unknown'
            )

        # Normalize sector/owner type
        if 'sector' in combined.columns:
            combined['owner_type'] = combined['sector'].map(
                lambda x: SECTOR_MAP.get(x, x) if pd.notna(x) else 'Unknown'
            )

        # Build expected COD from planned operation month/year
        if 'planned_op_month' in combined.columns and 'planned_op_year' in combined.columns:
            combined['expected_cod'] = pd.to_datetime(
                combined.apply(
                    lambda r: f"{int(r['planned_op_year'])}-{int(r['planned_op_month']):02d}-01"
                    if pd.notna(r.get('planned_op_year')) and pd.notna(r.get('planned_op_month'))
                    else None,
                    axis=1
                ),
                errors='coerce'
            )

        # Add metadata
        combined['source'] = 'eia_860m'
        combined['refresh_date'] = datetime.now().strftime('%Y-%m-%d')

        return combined

    def load(self, filepath: Path = None, pre_operational_only: bool = True,
             min_mw: float = None, max_mw: float = None,
             tech_filter: List[str] = None, state_filter: List[str] = None) -> pd.DataFrame:
        """
        Full load pipeline: read file → normalize → filter.

        Args:
            filepath: Path to EIA-860M xlsx (uses cache if None)
            pre_operational_only: Only return non-operating projects
            min_mw: Minimum capacity filter
            max_mw: Maximum capacity filter
            tech_filter: List of technologies to include (e.g., ['Solar', 'Storage'])
            state_filter: List of state abbreviations

        Returns:
            Filtered, normalized DataFrame
        """
        sheets = self.load_from_file(filepath)
        df = self.normalize(sheets, pre_operational_only=pre_operational_only)

        if df.empty:
            return df

        # Apply filters
        if min_mw is not None:
            df = df[df['nameplate_mw'] >= min_mw]

        if max_mw is not None:
            df = df[df['nameplate_mw'] <= max_mw]

        if tech_filter:
            tech_lower = [t.lower() for t in tech_filter]
            df = df[df['tech_clean'].str.lower().isin(tech_lower)]

        if state_filter:
            state_upper = [s.upper() for s in state_filter]
            df = df[df['state'].str.upper().isin(state_upper)]

        return df

    def get_sardar_targets(self, filepath: Path = None) -> pd.DataFrame:
        """
        Get projects matching Sardar's investment criteria:
        - 0.5 to 5 MW
        - Solar or Storage
        - Pre-operational (P, L, T, U, V, TS)
        - IPP or Commercial sector (not utility-owned)
        """
        df = self.load(
            filepath=filepath,
            pre_operational_only=True,
            min_mw=0.5,
            max_mw=5.0,
            tech_filter=['Solar', 'Storage', 'Solar + Storage'],
        )

        if df.empty:
            return df

        # Filter to IPP and Commercial (not utility-owned)
        if 'owner_type' in df.columns:
            df = df[df['owner_type'].isin(['IPP', 'Commercial', 'Industrial'])]

        return df

    def get_developer_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Count projects per developer for size classification."""
        if df.empty or 'entity_name' not in df.columns:
            return pd.DataFrame()

        stats = df.groupby('entity_name').agg(
            project_count=('permit_id', 'nunique'),
            total_mw=('nameplate_mw', 'sum'),
            states=('state', lambda x: ', '.join(sorted(x.dropna().unique()))),
            techs=('tech_clean', lambda x: ', '.join(sorted(x.dropna().unique()))),
            avg_mw=('nameplate_mw', 'mean'),
        ).reset_index()

        # Classify developer size
        stats['dev_size'] = stats['project_count'].apply(
            lambda x: 'Small' if x <= 5 else ('Medium' if x <= 20 else 'Large')
        )

        return stats.sort_values('project_count', ascending=False)

    def get_stats(self, filepath: Path = None) -> Dict:
        """Get summary statistics."""
        sheets = self.load_from_file(filepath)
        df = self.normalize(sheets, pre_operational_only=True)

        if df.empty:
            return {'error': 'No data loaded'}

        by_status = df.groupby('status_code').size().to_dict() if 'status_code' in df.columns else {}
        by_tech = df.groupby('tech_clean').size().to_dict() if 'tech_clean' in df.columns else {}
        by_state = df.groupby('state').size().sort_values(ascending=False).head(15).to_dict() if 'state' in df.columns else {}

        # Sardar targets
        sardar = self.get_sardar_targets(filepath)

        return {
            'total_pre_operational': len(df),
            'total_capacity_gw': df['nameplate_mw'].sum() / 1000 if 'nameplate_mw' in df.columns else 0,
            'by_status': by_status,
            'by_technology': by_tech,
            'top_states': by_state,
            'sardar_targets': len(sardar),
            'sardar_capacity_mw': sardar['nameplate_mw'].sum() if not sardar.empty else 0,
            'sheets_loaded': list(sheets.keys()),
        }


def refresh_eia860m(quiet: bool = False, filepath: Path = None) -> Dict:
    """
    Refresh EIA-860M data in the database (permits table).

    Returns:
        Dict with refresh statistics
    """
    try:
        from data_store import DataStore

        loader = EIA860MLoader()

        if filepath:
            df = loader.load(filepath=filepath, pre_operational_only=True)
        else:
            # Try to download latest
            try:
                loader.download()
            except Exception as e:
                logger.warning(f"Download failed ({e}), using cached data")

            df = loader.load(pre_operational_only=True)

        if df.empty:
            return {'success': False, 'error': 'No data loaded'}

        if not quiet:
            print(f"  Loaded {len(df):,} pre-operational generators from EIA-860M")

        # Map to permits table schema
        db_df = pd.DataFrame({
            'permit_id': df['permit_id'],
            'source': 'eia_860m',
            'project_name': df.get('plant_name', ''),
            'developer': df.get('entity_name', ''),
            'capacity_mw': df.get('nameplate_mw'),
            'technology': df.get('tech_clean', df.get('technology', '')),
            'state': df.get('state', ''),
            'county': df.get('county', ''),
            'latitude': df.get('latitude'),
            'longitude': df.get('longitude'),
            'status': df.get('status_label', ''),
            'status_code': df.get('status_code', ''),
            'expected_cod': df.get('expected_cod', '').astype(str).replace('NaT', ''),
        })

        # Upsert to database
        db = DataStore()
        stats = db.upsert_permits(db_df, source='eia_860m')

        if not quiet:
            print(f"  Database Update:")
            print(f"    Added: {stats['added']}")
            print(f"    Updated: {stats['updated']}")
            print(f"    Unchanged: {stats['unchanged']}")

        return {'success': True, 'rows_loaded': len(df), **stats}

    except Exception as e:
        logger.error(f"EIA-860M refresh failed: {e}")
        return {'success': False, 'error': str(e)}


def main():
    import argparse

    parser = argparse.ArgumentParser(description='EIA Form 860M Monthly Loader')
    parser.add_argument('--refresh', action='store_true', help='Download + update database')
    parser.add_argument('--stats', action='store_true', help='Show statistics')
    parser.add_argument('--export', type=str, metavar='FILE', help='Export to CSV')
    parser.add_argument('--file', type=str, help='Use specific xlsx file instead of downloading')
    parser.add_argument('--filter-sardar', action='store_true', help='Show Sardar-relevant targets')
    parser.add_argument('--developer-stats', action='store_true', help='Show developer size breakdown')
    parser.add_argument('--force-download', action='store_true', help='Force re-download')
    parser.add_argument('--url', type=str, help='Override download URL')

    args = parser.parse_args()

    loader = EIA860MLoader()
    filepath = Path(args.file) if args.file else None

    if args.refresh:
        if args.force_download or args.url:
            loader.download(url=args.url, force=True)
        result = refresh_eia860m(filepath=filepath)
        if result['success']:
            print("\nEIA-860M refresh complete!")
        else:
            print(f"\nEIA-860M refresh failed: {result.get('error')}")
            return 1
        return 0

    if args.stats:
        stats = loader.get_stats(filepath)
        print("\n" + "=" * 60)
        print("EIA FORM 860M — PRE-OPERATIONAL GENERATORS")
        print("=" * 60)
        print(f"Total pre-operational: {stats['total_pre_operational']:,}")
        print(f"Total capacity: {stats['total_capacity_gw']:.1f} GW")
        print(f"Sheets loaded: {', '.join(stats['sheets_loaded'])}")

        print(f"\nSardar targets (0.5-5 MW, Solar/Storage, IPP/Commercial):")
        print(f"  Projects: {stats['sardar_targets']:,}")
        print(f"  Capacity: {stats['sardar_capacity_mw']:.0f} MW")

        print("\nBy status:")
        for status, count in sorted(stats['by_status'].items(), key=lambda x: -x[1]):
            label = STATUS_MAP.get(status, status)
            print(f"  {status} ({label}): {count:,}")

        print("\nBy technology:")
        for tech, count in sorted(stats['by_technology'].items(), key=lambda x: -x[1]):
            print(f"  {tech}: {count:,}")

        print("\nTop states:")
        for state, count in list(stats['top_states'].items())[:10]:
            print(f"  {state}: {count:,}")

        return 0

    if args.filter_sardar:
        sardar = loader.get_sardar_targets(filepath)
        print(f"\n{'='*60}")
        print(f"SARDAR TARGETS — {len(sardar):,} projects")
        print(f"{'='*60}")

        if not sardar.empty:
            print(f"Total capacity: {sardar['nameplate_mw'].sum():.0f} MW")
            print(f"\nBy status:")
            for status, count in sardar.groupby('status_code').size().sort_values(ascending=False).items():
                print(f"  {STATUS_MAP.get(status, status)}: {count}")

            print(f"\nBy state (top 10):")
            for state, count in sardar.groupby('state').size().sort_values(ascending=False).head(10).items():
                print(f"  {state}: {count}")

            # Show developer stats
            dev_stats = loader.get_developer_stats(sardar)
            small = dev_stats[dev_stats['dev_size'] == 'Small']
            print(f"\nSmall developers (≤5 projects): {len(small):,}")
            print(f"Large developers (>20 projects): {len(dev_stats[dev_stats['dev_size'] == 'Large']):,}")

        return 0

    if args.developer_stats:
        df = loader.load(filepath=filepath, pre_operational_only=True)
        dev_stats = loader.get_developer_stats(df)
        print(f"\n{'='*60}")
        print(f"DEVELOPER SIZE BREAKDOWN — {len(dev_stats):,} developers")
        print(f"{'='*60}")

        for size in ['Small', 'Medium', 'Large']:
            subset = dev_stats[dev_stats['dev_size'] == size]
            print(f"\n{size} ({len(subset):,} developers):")
            for _, row in subset.head(10).iterrows():
                print(f"  {row['entity_name']}: {row['project_count']} projects, {row['total_mw']:.0f} MW")

        return 0

    if args.export:
        df = loader.load(filepath=filepath, pre_operational_only=True)
        df.to_csv(args.export, index=False)
        print(f"Exported {len(df):,} projects to {args.export}")
        return 0

    # Default: load and show summary
    try:
        stats = loader.get_stats(filepath)
        print(f"\nEIA-860M: {stats['total_pre_operational']:,} pre-operational generators")
        print(f"  Capacity: {stats['total_capacity_gw']:.1f} GW")
        print(f"  Sardar targets: {stats['sardar_targets']:,}")
    except Exception as e:
        print(f"Error: {e}")
        print("Try: python3 eia860m_loader.py --file /path/to/EIA860M.xlsx")
        return 1

    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
