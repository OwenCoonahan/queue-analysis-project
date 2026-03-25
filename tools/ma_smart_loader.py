#!/usr/bin/env python3
"""
MA SMART Loader — Solar Massachusetts Renewable Target qualified units.

Loads ~52K distributed generation solar projects from the MA SMART program.
Rich data: status, capacity, installer, owner, installation cost, adders, COD.

Data Source: https://mass.gov/doc/smart-qualified-units-list/download
Auth: None required (public Excel download)
Format: Excel (.xlsx), sheet "Qualified Units"
Refresh: Monthly recommended

Usage:
    python3 ma_smart_loader.py              # Full load
    python3 ma_smart_loader.py --stats      # Show stats after load
    python3 ma_smart_loader.py --no-cache   # Force fresh download
    python3 ma_smart_loader.py --dry-run    # Fetch + normalize, don't write DB
"""

import hashlib
import json
import logging
import sqlite3
import requests
import pandas as pd
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
CACHE_DIR = Path(__file__).parent / '.cache' / 'dg' / 'ma_smart'

SOURCE = 'ma_smart'
REGION = 'ISO-NE'

DOWNLOAD_URL = 'https://mass.gov/doc/smart-qualified-units-list/download'

# Status mapping: MA SMART → normalized status
STATUS_MAP = {
    'Approved': 'Operational',  # Most Approved have COD — effectively operational
    'Qualified': 'Active',      # Qualified but not yet COD
    'Under Review': 'Active',
    'Waitlist': 'Active',
}

# DG stage mapping: MA SMART status → (stage, confidence)
STAGE_MAP = {
    'Approved': ('operational', 0.95),   # Has passed all program gates
    'Qualified': ('approved', 0.85),     # Qualified into program, pre-COD
    'Under Review': ('applied', 0.80),   # Application under review
    'Waitlist': ('applied', 0.70),       # On waitlist
}


def compute_hash(row_dict: dict) -> str:
    key_fields = ['queue_id', 'capacity_kw', 'status', 'utility',
                  'installer', 'total_system_cost']
    parts = [str(row_dict.get(f, '')) for f in key_fields]
    return hashlib.md5('|'.join(parts).encode()).hexdigest()


class MASmartLoader:
    """Load MA SMART qualified units from mass.gov Excel download."""

    def __init__(self, db_path: Path = None, cache_dir: Path = None):
        self.db_path = db_path or DG_DB_PATH
        self.cache_dir = cache_dir or CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.df: Optional[pd.DataFrame] = None

    def download(self, force: bool = False) -> Path:
        filepath = self.cache_dir / 'smart_qualified_units.xlsx'

        if not force and filepath.exists():
            age_days = (datetime.now().timestamp() - filepath.stat().st_mtime) / 86400
            if age_days < 30:
                logger.info(f"  Using cached SMART data ({age_days:.0f} days old)")
                return filepath

        logger.info("  Downloading MA SMART qualified units list...")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ProspectorLabs/1.0',
        }
        resp = requests.get(DOWNLOAD_URL, headers=headers, timeout=120, allow_redirects=True)
        resp.raise_for_status()

        with open(filepath, 'wb') as f:
            f.write(resp.content)
        logger.info(f"    Downloaded {filepath.stat().st_size / 1e6:.1f} MB")
        return filepath

    def fetch(self, use_cache: bool = True) -> pd.DataFrame:
        filepath = self.download(force=not use_cache)
        df = pd.read_excel(filepath, sheet_name='Qualified Units', engine='openpyxl')
        logger.info(f"  Loaded {len(df):,} rows from Qualified Units sheet")
        return df

    def normalize(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Normalizing {len(raw_df):,} raw records...")
        records = []

        for _, row in raw_df.iterrows():
            project_number = str(row.get('Project Number', '')).strip()
            if not project_number or project_number == 'nan':
                continue

            queue_id = f"SMART-{project_number}"

            # Capacity
            capacity_kw_ac = self._parse_float(row.get('Capacity (kW AC)'))
            capacity_kw_dc = self._parse_float(row.get('Capacity (kW DC)'))
            capacity_kw = capacity_kw_ac or capacity_kw_dc or 0
            if capacity_kw <= 0:
                continue

            capacity_mw = capacity_kw / 1000.0

            # Status
            raw_status = str(row.get('Status', '')).strip()
            status = STATUS_MAP.get(raw_status, 'Unknown')

            # Stage
            stage_info = STAGE_MAP.get(raw_status, ('applied', 0.50))
            # If Approved but has no COD, downgrade to approved
            cod_val = row.get('Commercial Operation Date')
            if raw_status == 'Approved' and (pd.isna(cod_val) or str(cod_val).strip() in ('', 'nan', 'NaT')):
                stage_info = ('approved', 0.80)
                status = 'Active'

            dg_stage, dg_stage_confidence = stage_info

            # Dates
            cod = self._parse_date(row.get('Commercial Operation Date'))
            incentive_date = self._parse_date(row.get('Incentive Payment Effective Date'))
            reservation_expiry = self._parse_date(row.get('Reservation Period Expiration Date'))

            # Use reservation expiry as proxy for application date if no other date
            queue_date = incentive_date or reservation_expiry

            # Technology
            facility_type = str(row.get('Facility Type', '')).strip()
            has_storage = pd.notna(row.get('Storage Adder')) and str(row.get('Storage Adder')).strip() not in ('', 'nan', 'No')
            if has_storage or (self._parse_float(row.get('Storage Power Capacity (kVa)')) or 0) > 0:
                tech_type = 'Solar + Storage'
            else:
                tech_type = 'Solar'

            # Cost
            total_cost = self._parse_float(row.get('Total Installation Cost'))

            # Developer/installer/owner
            installer = str(row.get('Installer Company', '')).strip() or None
            owner = str(row.get('Owner Company', '')).strip() or None
            applicant = str(row.get('Applicant Company', '')).strip() or None
            developer = applicant or owner or ''

            record = {
                'queue_id': queue_id,
                'region': REGION,
                'name': None,
                'developer': developer,
                'capacity_mw': round(capacity_mw, 4),
                'capacity_kw': round(capacity_kw, 2),
                'type': tech_type,
                'status': status,
                'raw_status': raw_status,
                'state': 'MA',
                'county': None,  # Not in SMART data
                'city': str(row.get('City/Town', '')).strip() or None,
                'utility': str(row.get('Distribution Company', '')).strip() or None,
                'queue_date': queue_date,
                'cod': cod,
                'customer_sector': None,
                'system_size_dc_kw': capacity_kw_dc,
                'system_size_ac_kw': capacity_kw_ac,
                'installer': installer,
                'total_system_cost': total_cost,
                'interconnection_program': 'MA SMART',
                'dg_stage': dg_stage,
                'dg_stage_confidence': dg_stage_confidence,
                'dg_stage_method': 'raw_status',
            }
            record['row_hash'] = compute_hash(record)
            records.append(record)

        df = pd.DataFrame(records)
        logger.info(f"  Normalized {len(df):,} records")

        if not df.empty:
            logger.info(f"  Statuses: {df['status'].value_counts().to_dict()}")
            logger.info(f"  DG stages: {df['dg_stage'].value_counts().to_dict()}")
            logger.info(f"  Utilities: {df['utility'].nunique()} unique")
            active = df[df['status'] == 'Active']
            if not active.empty:
                logger.info(f"  Active by stage: {active['dg_stage'].value_counts().to_dict()}")

        return df

    def store(self, df: pd.DataFrame) -> Dict[str, int]:
        logger.info(f"Upserting {len(df):,} records into dg.db...")

        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Ensure columns exist
        for col, col_type in [
            ('raw_status', 'TEXT'),
            ('dg_stage', 'TEXT'),
            ('dg_stage_confidence', 'REAL'),
            ('dg_stage_method', 'TEXT'),
            ('system_size_ac_kw', 'REAL'),
            ('installer', 'TEXT'),
        ]:
            try:
                cursor.execute(f"ALTER TABLE projects ADD COLUMN {col} {col_type}")
                conn.commit()
            except sqlite3.OperationalError:
                pass

        stats = {'added': 0, 'updated': 0, 'unchanged': 0, 'errors': 0}

        for _, row in df.iterrows():
            try:
                row_dict = row.to_dict()
                queue_id = row_dict['queue_id']
                row_hash = row_dict['row_hash']

                cursor.execute(
                    'SELECT id, row_hash FROM projects WHERE queue_id = ? AND region = ?',
                    (queue_id, REGION)
                )
                existing = cursor.fetchone()

                if existing:
                    if existing['row_hash'] != row_hash:
                        cursor.execute('''
                            UPDATE projects SET
                                developer = COALESCE(NULLIF(?, ''), developer),
                                capacity_mw = ?, capacity_kw = ?,
                                type = ?, status = ?, raw_status = ?,
                                state = ?, city = ?, utility = ?,
                                queue_date = COALESCE(?, queue_date),
                                cod = COALESCE(?, cod),
                                system_size_dc_kw = ?, system_size_ac_kw = ?,
                                installer = COALESCE(?, installer),
                                total_system_cost = ?,
                                interconnection_program = ?,
                                dg_stage = ?, dg_stage_confidence = ?, dg_stage_method = ?,
                                row_hash = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', (
                            row_dict.get('developer', ''),
                            row_dict.get('capacity_mw'), row_dict.get('capacity_kw'),
                            row_dict.get('type'), row_dict.get('status'),
                            row_dict.get('raw_status'),
                            'MA', row_dict.get('city'), row_dict.get('utility'),
                            row_dict.get('queue_date'), row_dict.get('cod'),
                            row_dict.get('system_size_dc_kw'), row_dict.get('system_size_ac_kw'),
                            row_dict.get('installer'), row_dict.get('total_system_cost'),
                            'MA SMART',
                            row_dict.get('dg_stage'), row_dict.get('dg_stage_confidence'),
                            row_dict.get('dg_stage_method'),
                            row_hash, existing['id']
                        ))
                        stats['updated'] += 1
                    else:
                        stats['unchanged'] += 1
                else:
                    sources_json = json.dumps([SOURCE])
                    cursor.execute('''
                        INSERT INTO projects (
                            queue_id, region, name, developer, capacity_mw, capacity_kw,
                            type, status, raw_status, state, city, utility,
                            queue_date, cod, source, primary_source, sources,
                            system_size_dc_kw, system_size_ac_kw,
                            installer, total_system_cost, interconnection_program,
                            dg_stage, dg_stage_confidence, dg_stage_method, row_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        queue_id, REGION, None, row_dict.get('developer', ''),
                        row_dict.get('capacity_mw'), row_dict.get('capacity_kw'),
                        row_dict.get('type'), row_dict.get('status'),
                        row_dict.get('raw_status'),
                        'MA', row_dict.get('city'), row_dict.get('utility'),
                        row_dict.get('queue_date'), row_dict.get('cod'),
                        SOURCE, SOURCE, sources_json,
                        row_dict.get('system_size_dc_kw'), row_dict.get('system_size_ac_kw'),
                        row_dict.get('installer'), row_dict.get('total_system_cost'),
                        'MA SMART',
                        row_dict.get('dg_stage'), row_dict.get('dg_stage_confidence'),
                        row_dict.get('dg_stage_method'), row_hash,
                    ))
                    stats['added'] += 1

                cursor.execute('''
                    INSERT OR REPLACE INTO project_sources (queue_id, region, source, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ''', (queue_id, REGION, SOURCE))

            except Exception as e:
                stats['errors'] += 1
                if stats['errors'] <= 5:
                    logger.warning(f"Error on {row_dict.get('queue_id', '?')}: {e}")

        conn.commit()

        cursor.execute('''
            INSERT OR REPLACE INTO dg_programs (
                program_key, program_name, state, utility, source_url,
                refresh_method, last_refreshed, record_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            SOURCE, 'MA SMART Program', 'MA', 'Multiple',
            DOWNLOAD_URL, 'excel_download', datetime.now().isoformat(),
            stats['added'] + stats['updated'] + stats['unchanged']
        ))

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
        raw_df = self.fetch(use_cache=use_cache)
        if raw_df.empty:
            logger.error("No data fetched")
            return {'added': 0, 'updated': 0, 'unchanged': 0, 'errors': 0}

        self.df = self.normalize(raw_df)

        if dry_run:
            logger.info("DRY RUN — skipping database write")
            return {'added': 0, 'updated': 0, 'unchanged': 0, 'would_process': len(self.df)}

        return self.store(self.df)

    @staticmethod
    def _parse_float(val) -> Optional[float]:
        if val is None or pd.isna(val):
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_date(val) -> Optional[str]:
        if val is None or pd.isna(val):
            return None
        if isinstance(val, pd.Timestamp):
            return val.strftime('%m/%d/%Y')
        if isinstance(val, datetime):
            return val.strftime('%m/%d/%Y')
        val_str = str(val).strip()
        if not val_str or val_str.lower() in ('nan', 'nat'):
            return None
        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y']:
            try:
                return datetime.strptime(val_str[:19], fmt).strftime('%m/%d/%Y')
            except ValueError:
                continue
        return None


def main():
    import argparse
    parser = argparse.ArgumentParser(description="MA SMART Loader")
    parser.add_argument('--stats', action='store_true')
    parser.add_argument('--no-cache', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--db', type=str)
    args = parser.parse_args()

    loader = MASmartLoader(db_path=Path(args.db) if args.db else None)

    print(f"\n{'='*60}")
    print(f"MA SMART Loader — {SOURCE}")
    print(f"Target DB: {loader.db_path}")
    print(f"{'='*60}\n")

    results = loader.load(use_cache=not args.no_cache, dry_run=args.dry_run)

    print(f"\n{'='*60}")
    print(f"Results: +{results.get('added', 0):,} added, "
          f"~{results.get('updated', 0):,} updated, "
          f"={results.get('unchanged', 0):,} unchanged")
    print(f"{'='*60}")

    if args.stats and loader.df is not None:
        df = loader.df
        print(f"\nTotal: {len(df):,} records, {df['capacity_mw'].sum():,.1f} MW")
        print(f"By Status: {df['status'].value_counts().to_dict()}")
        print(f"By Stage: {df['dg_stage'].value_counts().to_dict()}")
        active = df[df['status'] == 'Active']
        if not active.empty:
            print(f"Active by stage: {active['dg_stage'].value_counts().to_dict()}")


if __name__ == '__main__':
    main()
