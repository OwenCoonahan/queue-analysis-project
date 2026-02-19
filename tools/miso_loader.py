#!/usr/bin/env python3
"""
MISO API Data Loader

Fetches interconnection queue data directly from MISO's public API.
No authentication required - returns complete queue data in JSON format.

API Endpoint: https://www.misoenergy.org/api/giqueue/getprojects

Usage:
    python3 miso_loader.py                    # Load and show stats
    python3 miso_loader.py --refresh          # Load and update database
    python3 miso_loader.py --export FILE.csv  # Export to CSV
    python3 miso_loader.py --active-only      # Show only active projects
"""

import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import json
import warnings
warnings.filterwarnings('ignore')

CACHE_DIR = Path(__file__).parent / '.cache'
API_URL = "https://www.misoenergy.org/api/giqueue/getprojects"


class MISOLoader:
    """Load MISO interconnection queue data from their public API."""

    # Fuel type normalization
    FUEL_MAP = {
        'Solar': 'Solar',
        'Wind': 'Wind',
        'Battery Storage': 'Storage',
        'Hybrid': 'Hybrid',
        'Gas': 'Gas',
        'Combined Cycle': 'Gas',
        'Nuclear': 'Nuclear',
        'Hydro': 'Hydro',
        'Waste Heat Recovery': 'Other',
        'Coal': 'Coal',
        'Diesel': 'Oil',
        'Biomass': 'Biomass',
        'Landfill Gas': 'Biomass',
        'Synchronous Condenser': 'Other',
        '': 'Unknown',
    }

    # Status normalization
    STATUS_MAP = {
        'Active': 'Active',
        'Withdrawn': 'Withdrawn',
        'Done': 'Operational',
        'Pending Transfer': 'Active',
        'LEGACY: Done': 'Operational',
        'LEGACY: Archived': 'Withdrawn',
    }

    def __init__(self):
        self.api_url = API_URL
        self.cache_file = CACHE_DIR / 'miso_api_cache.json'

    def fetch_from_api(self, use_cache: bool = False) -> List[Dict]:
        """
        Fetch queue data from MISO API.

        Args:
            use_cache: If True, use cached data if available and fresh (< 1 hour)

        Returns:
            List of project dictionaries
        """
        # Check cache
        if use_cache and self.cache_file.exists():
            cache_age = datetime.now().timestamp() - self.cache_file.stat().st_mtime
            if cache_age < 3600:  # 1 hour
                print("Using cached MISO data...")
                with open(self.cache_file) as f:
                    return json.load(f)

        print("Fetching data from MISO API...")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Queue-Analysis/1.0',
            'Accept': 'application/json',
        }

        response = requests.get(self.api_url, headers=headers, timeout=60)
        response.raise_for_status()

        data = response.json()
        print(f"  Received {len(data):,} projects")

        # Cache the data
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(self.cache_file, 'w') as f:
            json.dump(data, f)

        return data

    def load(self, use_cache: bool = False, active_only: bool = False) -> pd.DataFrame:
        """
        Load and normalize MISO queue data.

        Args:
            use_cache: Use cached data if fresh
            active_only: Filter to only active projects

        Returns:
            Normalized DataFrame
        """
        data = self.fetch_from_api(use_cache=use_cache)

        df = pd.DataFrame(data)

        # Normalize columns
        df = df.rename(columns={
            'projectNumber': 'queue_id',
            'transmissionOwner': 'utility',
            'county': 'county',
            'state': 'state',
            'studyCycle': 'study_cycle',
            'studyGroup': 'study_group',
            'studyPhase': 'study_phase',
            'svcType': 'service_type',
            'poiName': 'poi',
            'summerNetMW': 'capacity_mw',
            'winterNetMW': 'winter_capacity_mw',
            'fuelType': 'fuel_type',
            'facilityType': 'facility_type',
            'applicationStatus': 'status_raw',
            'inService': 'cod',
            'withdrawnDate': 'withdrawn_date',
            'postGIAStatus': 'post_gia_status',
        })

        # Normalize fuel type
        df['type'] = df['fuel_type'].map(
            lambda x: self.FUEL_MAP.get(x, x) if pd.notna(x) and x else 'Unknown'
        )

        # Normalize status
        df['status'] = df['status_raw'].map(
            lambda x: self.STATUS_MAP.get(x, x) if pd.notna(x) else 'Unknown'
        )

        # Parse dates
        for date_col in ['cod', 'withdrawn_date']:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

        # Add metadata
        df['region'] = 'MISO'
        df['source'] = 'miso_api'
        df['refresh_date'] = datetime.now().strftime('%Y-%m-%d')

        # Filter if requested
        if active_only:
            df = df[df['status'] == 'Active']

        # Select standard columns
        std_cols = [
            'queue_id', 'capacity_mw', 'winter_capacity_mw', 'type', 'fuel_type',
            'status', 'status_raw', 'state', 'county', 'utility', 'poi',
            'cod', 'withdrawn_date', 'study_cycle', 'study_group', 'study_phase',
            'service_type', 'facility_type', 'post_gia_status',
            'region', 'source', 'refresh_date'
        ]
        result = df[[c for c in std_cols if c in df.columns]]

        return result

    def get_stats(self, use_cache: bool = True) -> Dict:
        """Get statistics about MISO data."""
        df = self.load(use_cache=use_cache)

        # Calculate stats
        active = df[df['status'] == 'Active']
        withdrawn = df[df['status'] == 'Withdrawn']
        operational = df[df['status'] == 'Operational']

        return {
            'total_projects': len(df),
            'active': len(active),
            'withdrawn': len(withdrawn),
            'operational': len(operational),
            'active_capacity_gw': active['capacity_mw'].sum() / 1000,
            'by_status': df.groupby('status').size().to_dict(),
            'by_type': df.groupby('type').size().to_dict(),
            'by_state': df.groupby('state').size().to_dict(),
            'by_study_cycle': active.groupby('study_cycle').size().to_dict() if 'study_cycle' in active.columns else {},
        }


def refresh_miso(quiet: bool = False) -> Dict:
    """
    Refresh MISO data in the database.

    Returns:
        Dict with refresh statistics
    """
    try:
        from data_store import DataStore

        loader = MISOLoader()
        df = loader.load(use_cache=False)

        if df.empty:
            return {'success': False, 'error': 'No data loaded'}

        # Prepare for database
        db_df = df[[c for c in [
            'queue_id', 'capacity_mw', 'type', 'status', 'state', 'county',
            'cod', 'region', 'source'
        ] if c in df.columns]].copy()

        # Convert dates to strings
        if 'cod' in db_df.columns:
            db_df['cod'] = pd.to_datetime(db_df['cod'], errors='coerce').dt.strftime('%Y-%m-%d')

        # Upsert to database
        db = DataStore()
        stats = db.upsert_projects(db_df, source='miso_api', region='MISO')

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

    parser = argparse.ArgumentParser(description='MISO API Data Loader')
    parser.add_argument('--refresh', action='store_true', help='Refresh database')
    parser.add_argument('--export', type=str, metavar='FILE', help='Export to CSV')
    parser.add_argument('--active-only', action='store_true', help='Only show active projects')
    parser.add_argument('--stats', action='store_true', help='Show detailed statistics')
    parser.add_argument('--no-cache', action='store_true', help='Force fresh API fetch')

    args = parser.parse_args()

    loader = MISOLoader()
    use_cache = not args.no_cache

    if args.stats:
        stats = loader.get_stats(use_cache=use_cache)
        print("\n" + "="*60)
        print("MISO QUEUE STATISTICS")
        print("="*60)
        print(f"Total projects: {stats['total_projects']:,}")
        print(f"Active: {stats['active']:,}")
        print(f"Withdrawn: {stats['withdrawn']:,}")
        print(f"Operational: {stats['operational']:,}")
        print(f"Active capacity: {stats['active_capacity_gw']:.1f} GW")

        print("\nBy status:")
        for status, count in sorted(stats['by_status'].items(), key=lambda x: -x[1]):
            print(f"  {status}: {count:,}")

        print("\nBy fuel type:")
        for fuel, count in sorted(stats['by_type'].items(), key=lambda x: -x[1]):
            print(f"  {fuel}: {count:,}")

        print("\nBy state (top 10):")
        for state, count in sorted(stats['by_state'].items(), key=lambda x: -x[1])[:10]:
            print(f"  {state}: {count:,}")

        return 0

    if args.export:
        df = loader.load(use_cache=use_cache, active_only=args.active_only)
        df.to_csv(args.export, index=False)
        print(f"\nExported {len(df):,} projects to {args.export}")
        return 0

    if args.refresh:
        result = refresh_miso()
        if result['success']:
            print("\nMISO refresh complete!")
        else:
            print(f"\nMISO refresh failed: {result.get('error')}")
            return 1
        return 0

    # Default: load and show summary
    df = loader.load(use_cache=use_cache, active_only=args.active_only)

    print("\n" + "="*60)
    print("MISO INTERCONNECTION QUEUE")
    print("="*60)
    print(f"Total projects: {len(df):,}")

    active = df[df['status'] == 'Active']
    print(f"Active projects: {len(active):,}")
    print(f"Active capacity: {active['capacity_mw'].sum()/1000:.1f} GW")

    print("\nBy fuel type (active):")
    for fuel, count in active.groupby('type').size().sort_values(ascending=False).head(10).items():
        capacity = active[active['type'] == fuel]['capacity_mw'].sum() / 1000
        print(f"  {fuel}: {count:,} projects ({capacity:.1f} GW)")

    print("\nBy state (active, top 10):")
    for state, count in active.groupby('state').size().sort_values(ascending=False).head(10).items():
        if state:
            print(f"  {state}: {count:,}")

    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
