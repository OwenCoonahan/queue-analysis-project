#!/usr/bin/env python3
"""
PJM Data Loader

Loads PJM interconnection queue data from downloaded Excel files:
- PlanningQueues.xlsx: Full queue history (8,200+ projects)
- CycleProjects-All.xlsx: Transition Cycle projects with developer names

Data Sources:
- PJM Queue: https://www.pjm.com/planning/services-requests/interconnection-queues
- Download "Queue (Excel)" for PlanningQueues.xlsx
- For Cycle data, check PJM's Transition Cycle reports

Usage:
    python3 pjm_loader.py              # Load and show stats
    python3 pjm_loader.py --refresh    # Load and update database
    python3 pjm_loader.py --export     # Export combined data to CSV
"""

import pandas as pd
import requests
import io
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Tuple
import warnings
import logging
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent / '.cache'
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# URLs
PJM_QUEUE_URL = "https://www.pjm.com/planning/services-requests/interconnection-queues"
PJM_DATAMINER_API = "https://api.pjm.com/api/v1/gen_queues"  # Requires API key
PJM_QUEUE_EXPORT_URL = "https://services.pjm.com/PJMPlanningApi/api/Queue/ExportToXls"
PJM_QUEUE_EXPORT_KEY = "E29477D0-70E0-4825-89B0-43F460BF9AB4"  # Public key used by pjm.com


class PJMLoader:
    """Load and normalize PJM interconnection queue data."""

    # Standard file names in cache
    QUEUE_FILE = 'pjm_planning_queues.xlsx'
    CYCLE_FILE = 'pjm_cycle_projects.xlsx'

    # Column mapping from PJM to standard schema
    COLUMN_MAP = {
        'Project ID': 'queue_id',
        'Name': 'name',
        'Commercial Name': 'commercial_name',
        'State': 'state',
        'County': 'county',
        'Status': 'status',
        'Transmission Owner': 'transmission_owner',
        'MW Energy': 'mw_energy',
        'MW Capacity': 'capacity_mw',
        'MW In Service': 'mw_in_service',
        'Fuel': 'type',
        'Submitted Date': 'queue_date',
        'Projected In Service Date': 'cod',
        'Actual In Service Date': 'actual_cod',
        'Withdrawal Date': 'withdrawal_date',
        'Developer': 'developer',
    }

    # Status normalization
    STATUS_MAP = {
        'Active': 'Active',
        'Withdrawn': 'Withdrawn',
        'In Service': 'Operational',
        'Engineering and Procurement': 'Active',
        'EP': 'Active',
        'Suspended': 'Suspended',
        'Under Construction': 'Active',
        'Deactivated': 'Withdrawn',
        'Partially in Service - Under Construction': 'Active',
        'Canceled': 'Withdrawn',
        'Confirmed': 'Active',
        'Retracted': 'Withdrawn',
        'Annulled': 'Withdrawn',
    }

    # Fuel type normalization
    FUEL_MAP = {
        'Solar': 'Solar',
        'Natural Gas': 'Gas',
        'Storage': 'Storage',
        'Wind': 'Wind',
        'Solar; Storage': 'Solar + Storage',
        'Methane': 'Gas',
        'Coal': 'Coal',
        'Hydro': 'Hydro',
        'Offshore Wind': 'Offshore Wind',
        'Nuclear': 'Nuclear',
        'Wind; Storage': 'Wind + Storage',
        'Other': 'Other',
    }

    def __init__(self):
        self.queue_path = CACHE_DIR / self.QUEUE_FILE
        self.cycle_path = CACHE_DIR / self.CYCLE_FILE

    def download_queue(self, force: bool = False) -> Path:
        """Auto-download PJM queue Excel from the PJM Planning API.

        Uses the same endpoint as the PJM website's "Export to Excel" button.
        The file is cached for 7 days unless force=True.

        Returns:
            Path to downloaded file
        """
        # Check cache freshness
        if not force and self.queue_path.exists():
            age_days = (datetime.now().timestamp() - self.queue_path.stat().st_mtime) / 86400
            if age_days < 7:
                logger.info(f"Using cached PJM queue data ({age_days:.1f} days old)")
                print(f"  Using cached PJM queue data ({age_days:.1f} days old)")
                return self.queue_path

        print("  Downloading PJM queue from PJM Planning API...")
        logger.info("Downloading PJM queue from PJM Planning API")

        headers = {
            'api-subscription-key': PJM_QUEUE_EXPORT_KEY,
            'Origin': 'https://www.pjm.com',
            'Referer': 'https://www.pjm.com/',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ProspectorLabs/1.0',
        }

        resp = requests.post(PJM_QUEUE_EXPORT_URL, headers=headers, timeout=120)
        resp.raise_for_status()

        # Verify we got an Excel file (not an error page)
        content_type = resp.headers.get('content-type', '')
        if 'html' in content_type.lower():
            raise ValueError(f"PJM returned HTML instead of Excel (may be rate-limited): {resp.text[:200]}")

        if len(resp.content) < 10000:
            raise ValueError(f"PJM response too small ({len(resp.content)} bytes) — likely an error")

        with open(self.queue_path, 'wb') as f:
            f.write(resp.content)

        size_mb = len(resp.content) / (1024 * 1024)
        print(f"  Downloaded {size_mb:.1f} MB PJM queue data")
        logger.info(f"Downloaded PJM queue: {size_mb:.1f} MB")
        return self.queue_path

    def check_files(self) -> Dict[str, bool]:
        """Check if required files exist."""
        return {
            'planning_queues': self.queue_path.exists(),
            'cycle_projects': self.cycle_path.exists(),
        }

    def load_planning_queues(self) -> pd.DataFrame:
        """Load the main PJM planning queues file.

        Automatically downloads if not cached.
        """
        if not self.queue_path.exists():
            try:
                self.download_queue()
            except Exception as e:
                raise FileNotFoundError(
                    f"PJM queue auto-download failed ({e}). "
                    f"Manual download: {PJM_QUEUE_URL}"
                )

        df = pd.read_excel(self.queue_path, sheet_name='Data', engine='openpyxl')
        print(f"  Loaded {len(df):,} projects from PlanningQueues.xlsx")
        return df

    def load_cycle_projects(self) -> pd.DataFrame:
        """Load the PJM Cycle Projects file (has developer names)."""
        if not self.cycle_path.exists():
            print(f"  Warning: CycleProjects file not found, skipping developer enrichment")
            return pd.DataFrame()

        df = pd.read_excel(self.cycle_path, sheet_name='Data')
        print(f"  Loaded {len(df):,} projects from CycleProjects.xlsx (with developers)")
        return df

    def merge_developer_data(self, queues: pd.DataFrame, cycles: pd.DataFrame) -> pd.DataFrame:
        """
        Merge developer data from Cycle Projects into main queue.

        Matching strategy:
        1. Try exact name match (normalized)
        2. For Cycle projects not in main queue, add them directly
        """
        if cycles.empty:
            return queues

        # Normalize names for matching
        queues['_name_norm'] = queues['Name'].str.lower().str.strip()
        cycles['_name_norm'] = cycles['Name'].str.lower().str.strip()

        # Create developer lookup from cycles
        dev_lookup = cycles[['_name_norm', 'Developer']].drop_duplicates()
        dev_lookup = dev_lookup.set_index('_name_norm')['Developer'].to_dict()

        # Match by normalized name
        queues['Developer'] = queues['_name_norm'].map(dev_lookup)

        matched = queues['Developer'].notna().sum()
        print(f"  Matched {matched:,} developers by name")

        # Find Cycle projects not in main queue (by name)
        queue_names = set(queues['_name_norm'].dropna())
        new_from_cycles = cycles[~cycles['_name_norm'].isin(queue_names)].copy()

        if not new_from_cycles.empty:
            print(f"  Adding {len(new_from_cycles):,} projects from CycleProjects not in main queue")
            # Align columns
            for col in queues.columns:
                if col not in new_from_cycles.columns:
                    new_from_cycles[col] = None
            new_from_cycles = new_from_cycles[queues.columns]
            queues = pd.concat([queues, new_from_cycles], ignore_index=True)

        # Clean up temp columns
        queues = queues.drop(columns=['_name_norm'], errors='ignore')

        return queues

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize PJM data to standard schema."""
        # Select and rename columns
        result = pd.DataFrame()

        for pjm_col, std_col in self.COLUMN_MAP.items():
            if pjm_col in df.columns:
                result[std_col] = df[pjm_col]

        # Add region
        result['region'] = 'PJM'
        result['source'] = 'pjm_direct'

        # Normalize status
        if 'status' in result.columns:
            result['status'] = result['status'].map(
                lambda x: self.STATUS_MAP.get(x, x) if pd.notna(x) else None
            )

        # Normalize fuel type
        if 'type' in result.columns:
            result['type'] = result['type'].map(
                lambda x: self.FUEL_MAP.get(x, x) if pd.notna(x) else None
            )

        # Parse dates
        for date_col in ['queue_date', 'cod', 'actual_cod', 'withdrawal_date']:
            if date_col in result.columns:
                result[date_col] = pd.to_datetime(result[date_col], errors='coerce')

        # Use MW Capacity as primary, fall back to MW Energy
        if 'mw_energy' in result.columns and 'capacity_mw' in result.columns:
            result['capacity_mw'] = result['capacity_mw'].fillna(result['mw_energy'])

        # Clean up queue_id (remove " - moved to TCx" suffixes)
        if 'queue_id' in result.columns:
            result['queue_id'] = result['queue_id'].str.replace(r' - moved to TC\d+', '', regex=True)

        return result

    def load(self) -> pd.DataFrame:
        """Load, merge, and normalize all PJM data."""
        print("Loading PJM data...")

        # Load main queue
        queues = self.load_planning_queues()

        # Load cycle projects (for developer names)
        cycles = self.load_cycle_projects()

        # Merge developer data
        if not cycles.empty:
            queues = self.merge_developer_data(queues, cycles)

        # Normalize to standard schema
        normalized = self.normalize(queues)

        # Summary stats
        print(f"\n  PJM Data Summary:")
        print(f"    Total projects: {len(normalized):,}")
        print(f"    With developers: {normalized['developer'].notna().sum():,} ({100*normalized['developer'].notna().mean():.1f}%)")
        print(f"    With queue dates: {normalized['queue_date'].notna().sum():,} ({100*normalized['queue_date'].notna().mean():.1f}%)")
        print(f"    Total capacity: {normalized['capacity_mw'].sum()/1000:.1f} GW")

        # Status breakdown
        print(f"\n  Status Distribution:")
        status_counts = normalized['status'].value_counts()
        for status, count in status_counts.items():
            print(f"    {status}: {count:,}")

        return normalized

    def get_stats(self) -> Dict:
        """Get statistics about PJM data."""
        df = self.load()

        return {
            'total_projects': len(df),
            'developer_coverage': df['developer'].notna().mean(),
            'queue_date_coverage': df['queue_date'].notna().mean(),
            'total_capacity_gw': df['capacity_mw'].sum() / 1000,
            'active_projects': (df['status'] == 'Active').sum(),
            'active_capacity_gw': df[df['status'] == 'Active']['capacity_mw'].sum() / 1000,
            'latest_queue_date': df['queue_date'].max(),
            'status_breakdown': df['status'].value_counts().to_dict(),
            'fuel_breakdown': df['type'].value_counts().to_dict(),
        }


def refresh_pjm(quiet: bool = False) -> Dict:
    """
    Refresh PJM data in the database.

    Returns:
        Dict with refresh statistics
    """
    from data_store import DataStore

    loader = PJMLoader()

    # Check files
    files = loader.check_files()
    if not files['planning_queues']:
        return {
            'success': False,
            'error': f"PJM PlanningQueues.xlsx not found. Download from {PJM_QUEUE_URL}"
        }

    try:
        # Load and normalize
        df = loader.load()

        if df.empty:
            return {'success': False, 'error': 'No data loaded'}

        # Prepare for database
        db_df = df.rename(columns={
            'queue_id': 'queue_id',
            'name': 'name',
            'developer': 'developer',
            'capacity_mw': 'capacity_mw',
            'type': 'type',
            'status': 'status',
            'state': 'state',
            'county': 'county',
            'queue_date': 'queue_date',
            'cod': 'cod',
        })

        # Keep only standard columns
        std_cols = ['queue_id', 'name', 'developer', 'capacity_mw', 'type',
                    'status', 'state', 'county', 'queue_date', 'cod', 'region', 'source']
        db_df = db_df[[c for c in std_cols if c in db_df.columns]]

        # Convert dates to strings for database
        for col in ['queue_date', 'cod']:
            if col in db_df.columns:
                db_df[col] = db_df[col].dt.strftime('%Y-%m-%d')

        # Upsert to database
        db = DataStore()
        stats = db.upsert_projects(db_df, source='pjm_direct', region='PJM')

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

    parser = argparse.ArgumentParser(description='PJM Data Loader')
    parser.add_argument('--refresh', action='store_true', help='Refresh database with PJM data')
    parser.add_argument('--export', type=str, metavar='FILE', help='Export combined data to CSV')
    parser.add_argument('--stats', action='store_true', help='Show detailed statistics')

    args = parser.parse_args()

    loader = PJMLoader()

    # Check files
    files = loader.check_files()
    print("File Status:")
    for name, exists in files.items():
        status = "FOUND" if exists else "MISSING"
        print(f"  {name}: {status}")
    print()

    if not files['planning_queues']:
        print(f"ERROR: PlanningQueues.xlsx not found")
        print(f"Download from: {PJM_QUEUE_URL}")
        print(f"Save to: {CACHE_DIR / 'pjm_planning_queues.xlsx'}")
        return 1

    if args.stats:
        stats = loader.get_stats()
        print("\nPJM Data Statistics:")
        print(f"  Total projects: {stats['total_projects']:,}")
        print(f"  Developer coverage: {stats['developer_coverage']*100:.1f}%")
        print(f"  Queue date coverage: {stats['queue_date_coverage']*100:.1f}%")
        print(f"  Total capacity: {stats['total_capacity_gw']:.1f} GW")
        print(f"  Active projects: {stats['active_projects']:,}")
        print(f"  Active capacity: {stats['active_capacity_gw']:.1f} GW")
        print(f"  Latest queue date: {stats['latest_queue_date']}")
        return 0

    if args.export:
        df = loader.load()
        df.to_csv(args.export, index=False)
        print(f"\nExported {len(df):,} projects to {args.export}")
        return 0

    if args.refresh:
        result = refresh_pjm()
        if result['success']:
            print("\nPJM refresh complete!")
        else:
            print(f"\nPJM refresh failed: {result.get('error')}")
            return 1
        return 0

    # Default: just load and show stats
    df = loader.load()
    print(f"\nLoaded {len(df):,} PJM projects")
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
