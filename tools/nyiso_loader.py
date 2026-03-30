#!/usr/bin/env python3
"""
NYISO Comprehensive Data Loader

Loads ALL sheets from NYISO interconnection queue files:
- Interconnection Queue (active)
- Cluster Projects (active)
- Affected System Studies (active)
- Withdrawn (historical)
- Cluster Projects-Withdrawn (historical)
- Affected System-Withdrawn (historical)
- In Service (completed)

This provides a complete picture of NYISO queue history, not just current active projects.

Usage:
    python3 nyiso_loader.py                    # Load and show stats
    python3 nyiso_loader.py --refresh          # Load and update database
    python3 nyiso_loader.py --export FILE.csv  # Export combined data
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import warnings
warnings.filterwarnings('ignore')

CACHE_DIR = Path(__file__).parent / '.cache'


class NYISOLoader:
    """Load and normalize all NYISO interconnection queue data."""

    # Sheet configurations: (sheet_name, status_override, project_type)
    SHEETS = {
        'Interconnection Queue': {'status': None, 'type': 'standard'},
        ' Cluster Projects': {'status': None, 'type': 'cluster'},  # Note leading space
        'Affected System Studies': {'status': None, 'type': 'affected_system'},
        'Withdrawn': {'status': 'Withdrawn', 'type': 'standard'},
        'Cluster Projects-Withdrawn': {'status': 'Withdrawn', 'type': 'cluster'},
        'Affected System- Withdrawn': {'status': 'Withdrawn', 'type': 'affected_system'},
        'In Service': {'status': 'Operational', 'type': 'completed'},
    }

    # Column mappings (NYISO column -> standard column)
    COLUMN_MAP = {
        'Queue Pos.': 'queue_id',
        'Queue': 'queue_id',
        'Developer/Interconnection Customer': 'developer',
        'Interconnection Customer Name': 'developer',
        'Owner/Developer': 'developer',
        'Project Name': 'name',
        'Date of IR': 'queue_date',
        'Date': 'queue_date',
        'SP (MW)': 'capacity_mw',
        'SP': 'capacity_mw',
        'WP (MW)': 'winter_capacity_mw',
        'WP': 'winter_capacity_mw',
        'Type/ Fuel': 'fuel_type',
        'Type/': 'fuel_type',
        'County': 'county',
        'Location': 'county',
        'State': 'state',
        'Z': 'zone',
        'Zone': 'zone',
        'Points of Interconnection': 'poi',
        'Interconnection Point': 'poi',
        'Interconnection': 'poi',
        'Utility': 'utility',
        'Utility ': 'utility',
        'S': 'status_code',
        'Last Updated Date': 'last_updated',
        'Last Update': 'last_updated',
        'Proposed COD': 'cod',
        'Proposed In-Service/Initial Backfeed Date': 'backfeed_date',
        'IA Tender Date': 'ia_date',
        'Energy Storage Capability': 'storage_mwh',
        'Minimum_Duration Full Discharge': 'storage_duration_hrs',
    }

    # Fuel type normalization
    FUEL_MAP = {
        'S': 'Solar',
        'W': 'Wind',
        'ES': 'Storage',
        'NG': 'Gas',
        'AC': 'AC Transmission',
        'OSW': 'Offshore Wind',
        'H': 'Hydro',
        'NUC': 'Nuclear',
        'L': 'Load',
        'DC': 'DC Transmission',
    }

    # Status code mapping (S column values)
    STATUS_CODE_MAP = {
        0: 'Withdrawn',
        1: 'SRIS',
        2: 'SRIS Complete',
        3: 'FS',
        4: 'FS Complete',
        5: 'SIS',
        6: 'SIS Complete',
        7: 'IA Pending',
        8: 'IA Executed',
        9: 'Under Construction',
        10: 'In Service',
        11: 'IA Executed',
        12: 'FS',
        13: 'FS Complete',
        14: 'In Service',
    }

    def __init__(self, file_path: Optional[Path] = None):
        """
        Initialize loader.

        Args:
            file_path: Path to NYISO queue Excel file. If None, uses latest in cache.
        """
        if file_path:
            self.file_path = Path(file_path)
        else:
            # Find latest NYISO file in cache
            candidates = list(CACHE_DIR.glob('nyiso_queue*.xlsx'))
            if candidates:
                self.file_path = max(candidates, key=lambda p: p.stat().st_mtime)
            else:
                self.file_path = None

    def load_sheet(self, xlsx: pd.ExcelFile, sheet_name: str, config: Dict) -> pd.DataFrame:
        """Load and normalize a single sheet."""
        try:
            df = pd.read_excel(xlsx, sheet_name=sheet_name)
        except Exception as e:
            print(f"  Warning: Could not load sheet '{sheet_name}': {e}")
            return pd.DataFrame()

        if df.empty:
            return df

        # Handle "In Service" sheet which has weird header
        if sheet_name == 'In Service':
            # First row is the real header
            if 'Queue' in df.columns and df.iloc[0]['Queue'] == 'Pos.':
                df.columns = df.iloc[0].tolist()
                df = df.iloc[1:].reset_index(drop=True)

        # Rename columns to standard names
        rename_map = {}
        for old_col in df.columns:
            if old_col in self.COLUMN_MAP:
                rename_map[old_col] = self.COLUMN_MAP[old_col]
        df = df.rename(columns=rename_map)

        # Add metadata
        df['sheet_source'] = sheet_name
        df['project_type'] = config['type']

        # Override status if specified
        if config['status']:
            df['status'] = config['status']
        elif 'status_code' in df.columns:
            df['status'] = df['status_code'].map(
                lambda x: self.STATUS_CODE_MAP.get(int(x), f'Code_{x}') if pd.notna(x) else 'Unknown'
            )
        else:
            df['status'] = 'Active'

        # Normalize fuel type
        if 'fuel_type' in df.columns:
            df['type'] = df['fuel_type'].map(
                lambda x: self.FUEL_MAP.get(str(x).strip(), str(x)) if pd.notna(x) else None
            )

        # Parse dates
        for date_col in ['queue_date', 'last_updated', 'cod', 'backfeed_date', 'ia_date']:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

        # Clean queue_id
        if 'queue_id' in df.columns:
            df['queue_id'] = df['queue_id'].astype(str).str.strip()
            df = df[df['queue_id'].notna() & (df['queue_id'] != '') & (df['queue_id'] != 'nan')]

        # Use summer capacity, fall back to winter
        if 'capacity_mw' in df.columns:
            df['capacity_mw'] = pd.to_numeric(df['capacity_mw'], errors='coerce')
        if 'winter_capacity_mw' in df.columns and 'capacity_mw' in df.columns:
            df['capacity_mw'] = df['capacity_mw'].fillna(df['winter_capacity_mw'])

        return df

    def load(self) -> pd.DataFrame:
        """Load all sheets and combine into single DataFrame."""
        if not self.file_path or not self.file_path.exists():
            raise FileNotFoundError(
                f"NYISO queue file not found. Download from https://www.nyiso.com/interconnections"
            )

        print(f"Loading NYISO data from {self.file_path.name}...")

        xlsx = pd.ExcelFile(self.file_path)
        all_dfs = []

        for sheet_name, config in self.SHEETS.items():
            if sheet_name in xlsx.sheet_names:
                df = self.load_sheet(xlsx, sheet_name, config)
                if not df.empty:
                    print(f"  {sheet_name}: {len(df)} records")
                    all_dfs.append(df)

        if not all_dfs:
            return pd.DataFrame()

        # Ensure all DataFrames have unique columns before concat
        for i, df in enumerate(all_dfs):
            # Remove any duplicate columns
            all_dfs[i] = df.loc[:, ~df.columns.duplicated()]

        # Combine all sheets
        combined = pd.concat(all_dfs, ignore_index=True, sort=False)

        # Add region
        combined['region'] = 'NYISO'
        combined['source'] = 'nyiso_direct'

        # Select standard columns
        std_cols = [
            'queue_id', 'name', 'developer', 'capacity_mw', 'type', 'status',
            'county', 'state', 'zone', 'poi', 'queue_date', 'cod',
            'region', 'source', 'sheet_source', 'project_type',
            'storage_mwh', 'storage_duration_hrs', 'last_updated',
            'ia_date', 'backfeed_date',
        ]
        result = combined[[c for c in std_cols if c in combined.columns]]

        # Summary
        print(f"\n  NYISO Data Summary:")
        print(f"    Total records: {len(result):,}")
        active_mask = (result['status'] != 'Withdrawn') & (result['status'] != 'Operational')
        print(f"    Active: {active_mask.sum():,}")
        print(f"    Withdrawn: {(result['status'] == 'Withdrawn').sum():,}")
        print(f"    Operational: {(result['status'] == 'Operational').sum():,}")
        print(f"    With developers: {result['developer'].notna().sum():,}")
        result['capacity_mw'] = pd.to_numeric(result['capacity_mw'], errors='coerce')
        print(f"    Total capacity: {result['capacity_mw'].sum()/1000:.1f} GW")

        return result

    def get_stats(self) -> Dict:
        """Get statistics about NYISO data."""
        df = self.load()

        active_mask = (df['status'] != 'Withdrawn') & (df['status'] != 'Operational')

        return {
            'total_records': len(df),
            'active': int(active_mask.sum()),
            'withdrawn': int((df['status'] == 'Withdrawn').sum()),
            'operational': int((df['status'] == 'Operational').sum()),
            'developer_coverage': float(df['developer'].notna().mean()),
            'total_capacity_gw': float(df['capacity_mw'].sum() / 1000),
            'by_sheet': df.groupby('sheet_source').size().to_dict(),
            'by_type': df.groupby('type').size().to_dict() if 'type' in df.columns else {},
        }


def refresh_nyiso(file_path: Optional[str] = None, quiet: bool = False) -> Dict:
    """
    Refresh NYISO data in the database using all sheets.

    Args:
        file_path: Path to NYISO file. If None, uses latest in cache.
        quiet: Suppress output

    Returns:
        Dict with refresh statistics
    """
    try:
        from data_store import DataStore

        loader = NYISOLoader(file_path)
        df = loader.load()

        if df.empty:
            return {'success': False, 'error': 'No data loaded'}

        # Prepare for database
        db_df = df[[c for c in [
            'queue_id', 'name', 'developer', 'capacity_mw', 'type',
            'status', 'state', 'county', 'queue_date', 'cod', 'region', 'source',
            'backfeed_date', 'ia_date',
        ] if c in df.columns]].copy()

        # Convert dates to strings
        for col in ['queue_date', 'cod', 'backfeed_date', 'ia_date']:
            if col in db_df.columns:
                db_df[col] = pd.to_datetime(db_df[col], errors='coerce').dt.strftime('%Y-%m-%d')

        # Upsert to database
        db = DataStore()
        stats = db.upsert_projects(db_df, source='nyiso_direct', region='NYISO')

        if not quiet:
            print(f"\n  Database Update:")
            print(f"    Added: {stats['added']}")
            print(f"    Updated: {stats['updated']}")
            print(f"    Unchanged: {stats['unchanged']}")

        return {'success': True, **stats}

    except Exception as e:
        return {'success': False, 'error': str(e)}


def main():
    import argparse

    parser = argparse.ArgumentParser(description='NYISO Comprehensive Data Loader')
    parser.add_argument('--file', type=str, help='Path to NYISO queue Excel file')
    parser.add_argument('--refresh', action='store_true', help='Refresh database')
    parser.add_argument('--export', type=str, metavar='FILE', help='Export to CSV')
    parser.add_argument('--stats', action='store_true', help='Show detailed statistics')

    args = parser.parse_args()

    loader = NYISOLoader(args.file)

    if not loader.file_path or not loader.file_path.exists():
        print("ERROR: NYISO queue file not found")
        print("Download from: https://www.nyiso.com/interconnections")
        print(f"Save to: {CACHE_DIR / 'nyiso_queue_current.xlsx'}")
        return 1

    print(f"Using file: {loader.file_path}")
    print()

    if args.stats:
        stats = loader.get_stats()
        print("\nNYISO Data Statistics:")
        print(f"  Total records: {stats['total_records']:,}")
        print(f"  Active: {stats['active']:,}")
        print(f"  Withdrawn: {stats['withdrawn']:,}")
        print(f"  Operational: {stats['operational']:,}")
        print(f"  Developer coverage: {stats['developer_coverage']*100:.1f}%")
        print(f"  Total capacity: {stats['total_capacity_gw']:.1f} GW")
        print(f"\n  By Sheet:")
        for sheet, count in stats['by_sheet'].items():
            print(f"    {sheet}: {count:,}")
        return 0

    if args.export:
        df = loader.load()
        df.to_csv(args.export, index=False)
        print(f"\nExported {len(df):,} records to {args.export}")
        return 0

    if args.refresh:
        result = refresh_nyiso(args.file)
        if result['success']:
            print("\nNYISO refresh complete!")
        else:
            print(f"\nNYISO refresh failed: {result.get('error')}")
            return 1
        return 0

    # Default: load and show stats
    df = loader.load()
    print(f"\nLoaded {len(df):,} NYISO records")
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
