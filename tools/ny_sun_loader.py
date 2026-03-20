#!/usr/bin/env python3
"""
NY-SUN DG Loader — NYSERDA Solar Projects from data.ny.gov

Loads ~189K distributed generation solar projects from NY Open Data (Socrata API).
Targets dg.db projects table with source='ny_sun'.

Data Source: https://data.ny.gov/resource/3x8r-34rs.json
Auth: None required
Refresh: Daily recommended

Usage:
    python3 ny_sun_loader.py              # Full load
    python3 ny_sun_loader.py --stats      # Show stats only
    python3 ny_sun_loader.py --no-cache   # Skip cache
    python3 ny_sun_loader.py --dry-run    # Fetch + normalize, don't write DB
"""

import sqlite3
import hashlib
import json
import logging
import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / '.data'
DG_DB_PATH = DATA_DIR / 'dg.db'
CACHE_DIR = Path(__file__).parent / '.cache' / 'dg' / 'ny_sun'

SOURCE = 'ny_sun'
REGION = 'NYISO'

# Socrata API
API_URL = 'https://data.ny.gov/resource/3x8r-34rs.json'
CSV_URL = 'https://data.ny.gov/api/views/3x8r-34rs/rows.csv?accessType=DOWNLOAD'
PAGE_SIZE = 50000

# Status mapping: NY-SUN statuses → normalized
STATUS_MAP = {
    'Complete': 'Operational',
    'Completed': 'Operational',
    'Pipeline': 'Active',
    'Installed': 'Operational',
    'Cancelled': 'Withdrawn',
    'Canceled': 'Withdrawn',
    'Suspended': 'Suspended',
    'Inactive': 'Withdrawn',
}

# Sector mapping
SECTOR_MAP = {
    'Residential': 'Residential',
    'Small Commercial': 'Commercial',
    'Large Commercial': 'Commercial',
    'Commercial': 'Commercial',
    'Industrial': 'Industrial',
    'Not-for-Profit': 'Non-Profit',
    'Government': 'Government',
    'Municipal': 'Government',
    'School': 'Government',
    'Agricultural': 'Agricultural',
    'Non-Residential': 'Commercial',
}


def compute_hash(row_dict: dict) -> str:
    """Compute MD5 hash of key fields for change detection."""
    key_fields = ['queue_id', 'capacity_kw', 'status', 'utility', 'county',
                  'customer_sector', 'installer', 'total_system_cost']
    parts = [str(row_dict.get(f, '')) for f in key_fields]
    return hashlib.md5('|'.join(parts).encode()).hexdigest()


class NYSunLoader:
    """Load NY-SUN distributed generation projects from Socrata API."""

    def __init__(self, db_path: Path = None, cache_dir: Path = None):
        self.db_path = db_path or DG_DB_PATH
        self.cache_dir = cache_dir or CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.df: Optional[pd.DataFrame] = None

    def fetch(self, use_cache: bool = True) -> pd.DataFrame:
        """Fetch NY-SUN data via Socrata API with pagination."""
        cache_file = self.cache_dir / 'ny_sun_raw.csv'

        if use_cache and cache_file.exists():
            cache_age_hours = (datetime.now().timestamp() - cache_file.stat().st_mtime) / 3600
            if cache_age_hours < 24:
                logger.info(f"Using cached data ({cache_age_hours:.1f}h old, {cache_file.stat().st_size / 1e6:.1f} MB)")
                return pd.read_csv(cache_file, low_memory=False)

        logger.info("Fetching NY-SUN data from data.ny.gov...")
        all_records = []
        offset = 0

        try:
            while True:
                params = {
                    '$limit': PAGE_SIZE,
                    '$offset': offset,
                    '$order': 'project_number',
                }
                response = requests.get(
                    API_URL,
                    params=params,
                    timeout=120,
                    headers={'Accept': 'application/json'}
                )
                response.raise_for_status()

                data = response.json()
                if not data:
                    break

                all_records.extend(data)
                logger.info(f"  Fetched {len(all_records):,} records (offset {offset})")

                if len(data) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE

            df = pd.DataFrame(all_records)
            df.to_csv(cache_file, index=False)
            logger.info(f"  Cached {len(df):,} records to {cache_file.name}")
            return df

        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")

            # CSV fallback
            logger.info("  Trying CSV download fallback...")
            try:
                response = requests.get(CSV_URL, timeout=300)
                response.raise_for_status()
                from io import StringIO
                df = pd.read_csv(StringIO(response.text), low_memory=False)
                df.to_csv(cache_file, index=False)
                logger.info(f"  CSV fallback: {len(df):,} records")
                return df
            except Exception as csv_err:
                logger.error(f"CSV fallback failed: {csv_err}")

            # Stale cache fallback
            if cache_file.exists():
                logger.info("  Using stale cache")
                return pd.read_csv(cache_file, low_memory=False)

            return pd.DataFrame()

    def normalize(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Normalize raw NY-SUN data to dg.db projects schema."""
        logger.info(f"Normalizing {len(raw_df):,} raw records...")
        records = []

        for _, row in raw_df.iterrows():
            project_number = str(row.get('project_number', '')).strip()
            if not project_number:
                continue

            # Capacity: field is totalnameplatekwdc (kW DC)
            capacity_kw = None
            raw_cap = row.get('totalnameplatekwdc')
            if raw_cap and not pd.isna(raw_cap):
                try:
                    capacity_kw = float(raw_cap)
                except (ValueError, TypeError):
                    pass

            capacity_mw = capacity_kw / 1000.0 if capacity_kw else None

            # Status
            raw_status = str(row.get('project_status', '')).strip()
            status = STATUS_MAP.get(raw_status, raw_status if raw_status else 'Unknown')

            # Sector
            raw_sector = str(row.get('sector', '')).strip()
            customer_sector = SECTOR_MAP.get(raw_sector, raw_sector if raw_sector else None)

            # Dates
            queue_date = self._parse_date(row.get('date_application_received'))
            cod = self._parse_date(row.get('date_install'))

            # Cost
            project_cost = None
            raw_cost = row.get('project_cost')
            if raw_cost and not pd.isna(raw_cost):
                try:
                    project_cost = float(raw_cost)
                except (ValueError, TypeError):
                    pass

            # Location
            latitude = None
            longitude = None
            for lat_field in ['latitude']:
                val = row.get(lat_field)
                if val and not pd.isna(val):
                    try:
                        latitude = float(val)
                    except (ValueError, TypeError):
                        pass
            for lon_field in ['longitude']:
                val = row.get(lon_field)
                if val and not pd.isna(val):
                    try:
                        longitude = float(val)
                    except (ValueError, TypeError):
                        pass

            record = {
                'queue_id': project_number,
                'region': REGION,
                'name': None,  # NY-SUN doesn't have project names
                'developer': '',
                'capacity_mw': capacity_mw,
                'capacity_kw': capacity_kw,
                'type': 'Solar',
                'status': status,
                'state': 'NY',
                'county': str(row.get('county', '')).strip() or None,
                'city': str(row.get('city', '')).strip() or None,
                'utility': str(row.get('electric_utility', '')).strip() or None,
                'queue_date': queue_date,
                'cod': cod,
                'customer_sector': customer_sector,
                'system_size_dc_kw': capacity_kw,
                'installer': None,  # Not in Socrata dataset
                'total_system_cost': project_cost,
                'interconnection_program': str(row.get('program_type', '')).strip() or None,
                'latitude': latitude,
                'longitude': longitude,
                # Extra fields for raw_data
                '_purchase_type': str(row.get('purchase_type', '')).strip(),
                '_solicitation': str(row.get('solicitation', '')).strip(),
                '_expected_kwh': row.get('expected_kwh_annual_production'),
                '_community_dg': str(row.get('community_distributed_generation', '')).strip(),
                '_incentive': row.get('total_nyserda_incentive'),
                '_zip_code': str(row.get('zip_code', '')).strip(),
            }
            record['row_hash'] = compute_hash(record)
            records.append(record)

        df = pd.DataFrame(records)
        logger.info(f"  Normalized {len(df):,} records")

        # Stats
        if not df.empty:
            logger.info(f"  Statuses: {df['status'].value_counts().to_dict()}")
            logger.info(f"  Capacity range: {df['capacity_kw'].min():.1f} - {df['capacity_kw'].max():.1f} kW")
            logger.info(f"  Utilities: {df['utility'].nunique()} unique")
            logger.info(f"  Counties: {df['county'].nunique()} unique")

        return df

    def store(self, df: pd.DataFrame) -> Dict[str, int]:
        """Upsert normalized records into dg.db projects table."""
        logger.info(f"Upserting {len(df):,} records into dg.db...")

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        stats = {'added': 0, 'updated': 0, 'unchanged': 0, 'errors': 0}

        for _, row in df.iterrows():
            try:
                row_dict = row.to_dict()
                queue_id = row_dict['queue_id']
                row_hash = row_dict['row_hash']

                # Check existing
                cursor.execute(
                    'SELECT id, row_hash FROM projects WHERE queue_id = ? AND region = ?',
                    (queue_id, REGION)
                )
                existing = cursor.fetchone()

                if existing:
                    if existing['row_hash'] != row_hash:
                        cursor.execute('''
                            UPDATE projects SET
                                capacity_mw = ?, capacity_kw = ?, type = ?, status = ?,
                                state = ?, county = ?, city = ?, utility = ?,
                                queue_date = ?, cod = ?, customer_sector = ?,
                                system_size_dc_kw = ?, total_system_cost = ?,
                                interconnection_program = ?, row_hash = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', (
                            row_dict.get('capacity_mw'), row_dict.get('capacity_kw'),
                            'Solar', row_dict.get('status'),
                            'NY', row_dict.get('county'), row_dict.get('city'),
                            row_dict.get('utility'),
                            row_dict.get('queue_date'), row_dict.get('cod'),
                            row_dict.get('customer_sector'),
                            row_dict.get('system_size_dc_kw'), row_dict.get('total_system_cost'),
                            row_dict.get('interconnection_program'), row_hash,
                            existing['id']
                        ))
                        stats['updated'] += 1
                    else:
                        stats['unchanged'] += 1
                else:
                    sources_json = json.dumps([SOURCE])
                    cursor.execute('''
                        INSERT INTO projects (
                            queue_id, region, name, developer, capacity_mw, capacity_kw,
                            type, status, state, county, city, utility,
                            queue_date, cod, source, primary_source, sources,
                            customer_sector, system_size_dc_kw, total_system_cost,
                            interconnection_program, row_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        queue_id, REGION, row_dict.get('name'), row_dict.get('developer', ''),
                        row_dict.get('capacity_mw'), row_dict.get('capacity_kw'),
                        'Solar', row_dict.get('status'),
                        'NY', row_dict.get('county'), row_dict.get('city'),
                        row_dict.get('utility'),
                        row_dict.get('queue_date'), row_dict.get('cod'),
                        SOURCE, SOURCE, sources_json,
                        row_dict.get('customer_sector'), row_dict.get('system_size_dc_kw'),
                        row_dict.get('total_system_cost'),
                        row_dict.get('interconnection_program'), row_hash
                    ))
                    stats['added'] += 1

                # Track source provenance
                cursor.execute('''
                    INSERT OR REPLACE INTO project_sources (queue_id, region, source, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ''', (queue_id, REGION, SOURCE))

            except Exception as e:
                stats['errors'] += 1
                if stats['errors'] <= 5:
                    logger.warning(f"Error on {row_dict.get('queue_id', '?')}: {e}")

        conn.commit()

        # Update dg_programs registry
        cursor.execute('''
            INSERT OR REPLACE INTO dg_programs (
                program_key, program_name, state, utility, source_url,
                refresh_method, last_refreshed, record_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            SOURCE, 'NY-SUN Solar Incentive Program', 'NY', 'Multiple',
            'https://data.ny.gov/resource/3x8r-34rs',
            'socrata_api', datetime.now().isoformat(),
            stats['added'] + stats['updated'] + stats['unchanged']
        ))
        conn.commit()

        # Log refresh
        cursor.execute('''
            INSERT INTO refresh_log (source, started_at, completed_at, status,
                                     rows_processed, rows_added, rows_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            SOURCE, datetime.now().isoformat(), datetime.now().isoformat(),
            'success', len(df), stats['added'], stats['updated']
        ))
        conn.commit()
        conn.close()

        logger.info(f"  Added: {stats['added']:,}, Updated: {stats['updated']:,}, "
                     f"Unchanged: {stats['unchanged']:,}, Errors: {stats['errors']}")
        return stats

    def load(self, use_cache: bool = True, dry_run: bool = False) -> Dict[str, int]:
        """Full pipeline: fetch → normalize → store."""
        raw_df = self.fetch(use_cache=use_cache)
        if raw_df.empty:
            logger.error("No data fetched")
            return {'added': 0, 'updated': 0, 'unchanged': 0, 'errors': 0}

        self.df = self.normalize(raw_df)

        if dry_run:
            logger.info("DRY RUN — skipping database write")
            return {'added': 0, 'updated': 0, 'unchanged': 0, 'would_process': len(self.df)}

        return self.store(self.df)

    def get_stats(self) -> Dict:
        """Get statistics from loaded data."""
        if self.df is None:
            return {}
        df = self.df
        return {
            'total_records': len(df),
            'total_capacity_mw': df['capacity_mw'].sum() if 'capacity_mw' in df.columns else 0,
            'by_status': df['status'].value_counts().to_dict(),
            'by_utility': df['utility'].value_counts().head(10).to_dict(),
            'by_county': df['county'].value_counts().head(10).to_dict(),
            'by_sector': df['customer_sector'].value_counts().to_dict() if 'customer_sector' in df.columns else {},
            'capacity_stats': {
                'min_kw': df['capacity_kw'].min(),
                'median_kw': df['capacity_kw'].median(),
                'mean_kw': df['capacity_kw'].mean(),
                'max_kw': df['capacity_kw'].max(),
            } if 'capacity_kw' in df.columns else {},
        }

    @staticmethod
    def _parse_date(val) -> Optional[str]:
        """Parse various date formats to YYYY-MM-DD or MM/DD/YYYY."""
        if not val or pd.isna(val):
            return None
        val_str = str(val).strip()
        if not val_str:
            return None
        # Socrata ISO format: 2024-01-15T00:00:00.000
        for fmt in ['%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d',
                     '%m/%d/%Y', '%m/%d/%y']:
            try:
                return datetime.strptime(val_str[:19], fmt).strftime('%m/%d/%Y')
            except ValueError:
                continue
        return val_str[:10]


def main():
    import argparse
    parser = argparse.ArgumentParser(description="NY-SUN DG Loader")
    parser.add_argument('--stats', action='store_true', help='Show statistics after load')
    parser.add_argument('--no-cache', action='store_true', help='Skip cache, fetch fresh')
    parser.add_argument('--dry-run', action='store_true', help='Fetch + normalize only, no DB write')
    parser.add_argument('--db', type=str, help='Override DB path')
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else None
    loader = NYSunLoader(db_path=db_path)

    print(f"\n{'='*60}")
    print(f"NY-SUN DG Loader — source: {SOURCE}")
    print(f"Target DB: {loader.db_path}")
    print(f"{'='*60}\n")

    results = loader.load(use_cache=not args.no_cache, dry_run=args.dry_run)

    print(f"\n{'='*60}")
    print(f"Results: +{results.get('added', 0):,} added, "
          f"~{results.get('updated', 0):,} updated, "
          f"={results.get('unchanged', 0):,} unchanged")
    print(f"{'='*60}")

    if args.stats:
        stats = loader.get_stats()
        print(f"\nTotal records: {stats.get('total_records', 0):,}")
        print(f"Total capacity: {stats.get('total_capacity_mw', 0):,.1f} MW")
        print(f"\nBy Status:")
        for k, v in stats.get('by_status', {}).items():
            print(f"  {k}: {v:,}")
        print(f"\nBy Utility (top 10):")
        for k, v in stats.get('by_utility', {}).items():
            print(f"  {k}: {v:,}")
        print(f"\nBy Sector:")
        for k, v in stats.get('by_sector', {}).items():
            print(f"  {k}: {v:,}")
        cap = stats.get('capacity_stats', {})
        if cap:
            print(f"\nCapacity: min={cap.get('min_kw', 0):.1f} kW, "
                  f"median={cap.get('median_kw', 0):.1f} kW, "
                  f"max={cap.get('max_kw', 0):.1f} kW")


if __name__ == '__main__':
    main()
