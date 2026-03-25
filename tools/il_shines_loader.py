#!/usr/bin/env python3
"""
IL Shines Loader — Illinois Adjustable Block Program project applications.

Loads DG projects from 3 IL Shines reports + IPA Dashboard, tracking 7 lifecycle phases:
  1. Application Submitted (Report 1: Need_Info/Submitted)
  2. Part I Verified (Report 1: Verified)
  3. ICC Approved / Contracted (Report 2 / Dashboard: ICC_Approved)
  4. Part II Submitted (Dashboard: Part II Status = Submitted/InProgress)
  5. Part II Verified (Report 3 / Dashboard: Part II Status = Verified)
  6. Energized (Report 3: has Online Date)

Data Source: https://illinoisshines.com/project-application-reports/
Auth: None required (public Excel downloads)
Format: 3 Excel files + IPA Dashboard, weekly updates
Refresh: Weekly recommended

Usage:
    python3 il_shines_loader.py              # Full load
    python3 il_shines_loader.py --stats      # Show stats after load
    python3 il_shines_loader.py --no-cache   # Force fresh download
    python3 il_shines_loader.py --dry-run    # Fetch + normalize, don't write DB
"""

import hashlib
import json
import logging
import sqlite3
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / '.data'
DG_DB_PATH = DATA_DIR / 'dg.db'
CACHE_DIR = Path(__file__).parent / '.cache' / 'dg' / 'il_shines'

SOURCE = 'il_shines'
REGION = 'MISO'  # Most IL projects are in MISO (some ComEd in PJM)

# Download URLs
REPORT_URLS = {
    'report1': {
        'url': 'https://illinoisshines.com/wp-content/uploads/Report-1-Applications-Received.xlsx',
        'filename': 'Report-1-Applications-Received.xlsx',
        'sheet': 'Project Applications Received',
    },
    'report2': {
        'url': 'https://illinoisshines.com/wp-content/uploads/Report-2-ICC-Approved.xlsx',
        'filename': 'Report-2-ICC-Approved.xlsx',
        'sheet': 'ICC Approved Part I Application',
    },
    'report3': {
        'url': 'https://illinoisshines.com/wp-content/uploads/Report-3-Part-II-Complete.xlsx',
        'filename': 'Report-3-Part-II-Complete.xlsx',
        'sheet': 'Project Application Report 3',
    },
}

IPA_DASHBOARD_URL = 'https://cleanenergy.illinois.gov/content/dam/soi/en/web/cleanenergy/documents/IPA-Dashboard-Illinois-Shines.xlsx'
IPA_DASHBOARD_FILENAME = 'IPA-Dashboard-Illinois-Shines.xlsx'


def compute_hash(row_dict: dict) -> str:
    key_fields = ['queue_id', 'capacity_kw', 'status', 'utility',
                  'dg_stage', 'raw_status']
    parts = [str(row_dict.get(f, '')) for f in key_fields]
    return hashlib.md5('|'.join(parts).encode()).hexdigest()


def derive_stage(report1_status: Optional[str],
                 part2_status: Optional[str],
                 has_online_date: bool,
                 in_report3: bool,
                 in_report2: bool) -> Tuple[str, float, str]:
    """Derive DG stage from IL Shines lifecycle position.

    Priority (highest to lowest):
    1. Has Online Date → operational (0.95)
    2. In Report 3 (Part II complete) → operational (0.90)
    3. Part II Verified → operational (0.90)
    4. Part II InProgress/Submitted → construction (0.80)
    5. In Report 2 (ICC approved) → approved (0.85)
    6. Report 1 Verified → approved (0.75)
    7. Report 1 Submitted/Need_Info → applied (0.80)
    """
    if has_online_date:
        return 'operational', 0.95, 'il_lifecycle'
    if in_report3:
        return 'operational', 0.90, 'il_lifecycle'
    if part2_status == 'Verified':
        return 'operational', 0.90, 'il_lifecycle'
    if part2_status in ('InProgress', 'Submitted', 'Need_Info'):
        return 'construction', 0.80, 'il_lifecycle'
    if in_report2:
        return 'approved', 0.85, 'il_lifecycle'
    if report1_status == 'Verified':
        return 'approved', 0.75, 'il_lifecycle'
    if report1_status in ('Submitted', 'Need_Info', 'NI_Unresponsive_AV'):
        return 'applied', 0.80, 'il_lifecycle'
    return 'applied', 0.50, 'il_lifecycle'


class ILShinesLoader:
    """Load IL Shines (ABP) distributed generation projects."""

    def __init__(self, db_path: Path = None, cache_dir: Path = None):
        self.db_path = db_path or DG_DB_PATH
        self.cache_dir = cache_dir or CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.df: Optional[pd.DataFrame] = None

    def download(self, force: bool = False) -> Dict[str, Path]:
        downloaded = {}
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ProspectorLabs/1.0',
        }

        # Download 3 reports + dashboard
        all_files = {
            **{k: v for k, v in REPORT_URLS.items()},
            'dashboard': {
                'url': IPA_DASHBOARD_URL,
                'filename': IPA_DASHBOARD_FILENAME,
            },
        }

        for key, config in all_files.items():
            filepath = self.cache_dir / config['filename']

            if not force and filepath.exists():
                age_days = (datetime.now().timestamp() - filepath.stat().st_mtime) / 86400
                if age_days < 7:  # Weekly refresh
                    logger.info(f"  {key}: cached ({age_days:.0f} days old)")
                    downloaded[key] = filepath
                    continue

            logger.info(f"  Downloading {key}...")
            try:
                resp = requests.get(config['url'], headers=headers, timeout=180)
                resp.raise_for_status()
                with open(filepath, 'wb') as f:
                    f.write(resp.content)
                logger.info(f"    Downloaded {filepath.stat().st_size / 1e6:.1f} MB")
                downloaded[key] = filepath
            except Exception as e:
                logger.warning(f"    Failed: {e}")
                if filepath.exists():
                    logger.info(f"    Using stale cache")
                    downloaded[key] = filepath

        return downloaded

    def fetch(self, use_cache: bool = True) -> pd.DataFrame:
        """Download and parse IL Shines data. Merge across reports by Application ID."""
        files = self.download(force=not use_cache)

        if not files:
            return pd.DataFrame()

        # Strategy: Use Report 1 as base (all applications), then merge info
        # from Report 2 (ICC approved) and Report 3 (Part II complete).
        # The IPA Dashboard is also useful — has both Part I and Part II status.

        # Load Report 1 — all applications
        r1 = pd.DataFrame()
        if 'report1' in files:
            try:
                r1 = pd.read_excel(files['report1'],
                                   sheet_name=REPORT_URLS['report1']['sheet'],
                                   engine='openpyxl')
                logger.info(f"  Report 1: {len(r1):,} rows")
            except Exception as e:
                logger.warning(f"  Report 1 failed: {e}")

        # Load Report 2 — ICC approved
        r2_ids = set()
        r2_data = {}
        if 'report2' in files:
            try:
                r2 = pd.read_excel(files['report2'],
                                   sheet_name=REPORT_URLS['report2']['sheet'],
                                   engine='openpyxl')
                logger.info(f"  Report 2: {len(r2):,} rows")
                for _, row in r2.iterrows():
                    app_id = str(row.get('Application ID', '')).strip()
                    if app_id and app_id != 'nan':
                        r2_ids.add(app_id)
                        r2_data[app_id] = {
                            'address': str(row.get('Address', '')).strip() or None,
                            'city': str(row.get('City', '')).strip() or None,
                            'county': str(row.get('County', '')).strip() or None,
                            'zip': str(row.get('Zip', '')).strip() or None,
                            'icc_approval_date': row.get('ICC Approval Date'),
                            'contract_date': row.get('Contract Effective Date'),
                            'utility': str(row.get('Counterparty Utility', '')).strip() or None,
                            'rec_price': row.get('REC Price'),
                            'scheduled_energization': row.get('Scheduled Energization Date'),
                            'capacity_factor': row.get('Capacity Factor'),
                            'financing': str(row.get('Financing Structure', '')).strip() or None,
                        }
            except Exception as e:
                logger.warning(f"  Report 2 failed: {e}")

        # Load Report 3 — Part II complete (energized)
        r3_ids = set()
        r3_data = {}
        if 'report3' in files:
            try:
                r3 = pd.read_excel(files['report3'],
                                   sheet_name=REPORT_URLS['report3']['sheet'],
                                   engine='openpyxl')
                logger.info(f"  Report 3: {len(r3):,} rows")
                for _, row in r3.iterrows():
                    app_id = str(row.get('Application ID', '')).strip()
                    if app_id and app_id != 'nan':
                        r3_ids.add(app_id)
                        r3_data[app_id] = {
                            'online_date': row.get('Online Date'),
                            'part2_verification_date': row.get('Part II Application Verification/Energization Date'),
                        }
            except Exception as e:
                logger.warning(f"  Report 3 failed: {e}")

        # Load IPA Dashboard for Part II status
        dash_data = {}
        if 'dashboard' in files:
            try:
                dash = pd.read_excel(files['dashboard'],
                                     sheet_name='Illinois Shines Data',
                                     engine='openpyxl')
                logger.info(f"  Dashboard: {len(dash):,} rows")
                for _, row in dash.iterrows():
                    app_id = str(row.get('App ID', '')).strip()
                    if app_id and app_id != 'nan':
                        dash_data[app_id] = {
                            'part2_status': str(row.get('Part II Status', '')).strip() or None,
                            'part2_online_date': row.get('Part II Online Date'),
                            'part1_verification_date': row.get('Part I Verification Date'),
                            'scheduled_energization': row.get('Scheduled Energized Date'),
                        }
            except Exception as e:
                logger.warning(f"  Dashboard failed: {e}")

        logger.info(f"  Merge sets: R1={len(r1):,}, R2={len(r2_ids):,}, "
                     f"R3={len(r3_ids):,}, Dashboard={len(dash_data):,}")

        # Build combined records from Report 1 as base
        combined = r1.copy() if not r1.empty else pd.DataFrame()
        combined['_in_report2'] = combined.get('Application ID', pd.Series()).apply(
            lambda x: str(x).strip() in r2_ids if pd.notna(x) else False
        ) if not combined.empty else pd.Series(dtype=bool)
        combined['_in_report3'] = combined.get('Application ID', pd.Series()).apply(
            lambda x: str(x).strip() in r3_ids if pd.notna(x) else False
        ) if not combined.empty else pd.Series(dtype=bool)

        # Merge Report 2 and 3 data
        def get_r2(app_id, field):
            return r2_data.get(str(app_id).strip(), {}).get(field) if pd.notna(app_id) else None

        def get_r3(app_id, field):
            return r3_data.get(str(app_id).strip(), {}).get(field) if pd.notna(app_id) else None

        def get_dash(app_id, field):
            return dash_data.get(str(app_id).strip(), {}).get(field) if pd.notna(app_id) else None

        if not combined.empty:
            combined['_r2_city'] = combined['Application ID'].apply(lambda x: get_r2(x, 'city'))
            combined['_r2_county'] = combined['Application ID'].apply(lambda x: get_r2(x, 'county'))
            combined['_r2_utility'] = combined['Application ID'].apply(lambda x: get_r2(x, 'utility'))
            combined['_r2_icc_date'] = combined['Application ID'].apply(lambda x: get_r2(x, 'icc_approval_date'))
            combined['_r2_sched_energize'] = combined['Application ID'].apply(lambda x: get_r2(x, 'scheduled_energization'))
            combined['_r3_online_date'] = combined['Application ID'].apply(lambda x: get_r3(x, 'online_date'))
            combined['_dash_part2_status'] = combined['Application ID'].apply(lambda x: get_dash(x, 'part2_status'))

        return combined

    def normalize(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Normalizing {len(raw_df):,} raw records...")
        records = []

        for _, row in raw_df.iterrows():
            app_id = str(row.get('Application ID', '')).strip()
            if not app_id or app_id == 'nan':
                continue

            queue_id = f"ILS-{app_id}"

            # Capacity
            capacity_kw_ac = self._parse_float(row.get('Project Size AC kW'))
            capacity_kw_dc = self._parse_float(row.get('Project Size DC kW'))
            capacity_kw = capacity_kw_ac or capacity_kw_dc or 0
            if capacity_kw <= 0:
                continue

            capacity_mw = capacity_kw / 1000.0

            # Category
            category = str(row.get('Category', '')).strip()
            ceja_cat = str(row.get('CEJA Project Category', '')).strip()

            # Technology type
            if 'CS' in category or 'CS' in ceja_cat:
                tech_type = 'Community Solar'
            else:
                tech_type = 'Solar'

            # Report 1 status
            r1_status = str(row.get('Part I Application Status', '')).strip()
            in_r2 = bool(row.get('_in_report2', False))
            in_r3 = bool(row.get('_in_report3', False))
            part2_status = str(row.get('_dash_part2_status', '')).strip() if pd.notna(row.get('_dash_part2_status')) else None
            online_date = row.get('_r3_online_date')
            has_online = pd.notna(online_date) and str(online_date).strip() not in ('', 'nan', 'NaT')

            # Derive stage
            dg_stage, dg_confidence, dg_method = derive_stage(
                r1_status, part2_status, has_online, in_r3, in_r2
            )

            # Status
            if dg_stage == 'operational':
                status = 'Operational'
            elif dg_stage == 'withdrawn':
                status = 'Withdrawn'
            else:
                status = 'Active'

            # Build raw_status string
            parts = []
            if r1_status:
                parts.append(f"Part I: {r1_status}")
            if in_r2:
                parts.append("ICC Approved")
            if part2_status:
                parts.append(f"Part II: {part2_status}")
            if has_online:
                parts.append("Energized")
            raw_status = ' | '.join(parts) if parts else None

            # Dates
            submit_date = self._parse_date(row.get('Part I Application Submission Date'))
            icc_date = self._parse_date(row.get('_r2_icc_date'))
            sched_energize = self._parse_date(row.get('_r2_sched_energize'))
            cod = self._parse_date(online_date) if has_online else sched_energize

            # Location (from Report 2 if available)
            city = str(row.get('_r2_city', '')).strip() if pd.notna(row.get('_r2_city')) else None
            county = str(row.get('_r2_county', '')).strip() if pd.notna(row.get('_r2_county')) else None
            utility = str(row.get('_r2_utility', '')).strip() if pd.notna(row.get('_r2_utility')) else None
            zip_code = str(row.get('Zip Code', '')).strip() if pd.notna(row.get('Zip Code')) else None

            # Developer
            vendor = str(row.get('Vendor Company Name', '')).strip() or None
            installer = str(row.get('Installer Company Name', '')).strip() or None

            # Determine region — ComEd is PJM, Ameren/MidAmerican is MISO
            region = REGION
            if utility and 'ComEd' in utility:
                region = 'PJM'

            record = {
                'queue_id': queue_id,
                'region': region,
                'name': None,
                'developer': vendor or '',
                'capacity_mw': round(capacity_mw, 4),
                'capacity_kw': round(capacity_kw, 2),
                'type': tech_type,
                'status': status,
                'raw_status': raw_status,
                'state': 'IL',
                'county': county,
                'city': city,
                'utility': utility,
                'queue_date': submit_date,
                'cod': cod,
                'customer_sector': None,
                'system_size_dc_kw': capacity_kw_dc,
                'system_size_ac_kw': capacity_kw_ac,
                'installer': installer,
                'interconnection_program': f"IL Shines {category}" if category else 'IL Shines',
                'dg_stage': dg_stage,
                'dg_stage_confidence': dg_confidence,
                'dg_stage_method': dg_method,
            }
            record['row_hash'] = compute_hash(record)
            records.append(record)

        df = pd.DataFrame(records)
        logger.info(f"  Normalized {len(df):,} records")

        if not df.empty:
            logger.info(f"  Statuses: {df['status'].value_counts().to_dict()}")
            logger.info(f"  DG stages: {df['dg_stage'].value_counts().to_dict()}")
            logger.info(f"  Regions: {df['region'].value_counts().to_dict()}")
            active = df[df['status'] == 'Active']
            if not active.empty:
                logger.info(f"  Active by stage: {active['dg_stage'].value_counts().to_dict()}")

        return df

    def store(self, df: pd.DataFrame) -> Dict[str, int]:
        logger.info(f"Upserting {len(df):,} records into dg.db...")

        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

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
                region = row_dict['region']
                row_hash = row_dict['row_hash']

                cursor.execute(
                    'SELECT id, row_hash FROM projects WHERE queue_id = ? AND region = ?',
                    (queue_id, region)
                )
                existing = cursor.fetchone()

                if existing:
                    if existing['row_hash'] != row_hash:
                        cursor.execute('''
                            UPDATE projects SET
                                developer = COALESCE(NULLIF(?, ''), developer),
                                capacity_mw = ?, capacity_kw = ?,
                                type = ?, status = ?, raw_status = ?,
                                state = ?, county = COALESCE(?, county),
                                city = COALESCE(?, city), utility = COALESCE(?, utility),
                                queue_date = COALESCE(?, queue_date),
                                cod = COALESCE(?, cod),
                                system_size_dc_kw = ?, system_size_ac_kw = ?,
                                installer = COALESCE(?, installer),
                                interconnection_program = ?,
                                dg_stage = ?, dg_stage_confidence = ?, dg_stage_method = ?,
                                row_hash = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', (
                            row_dict.get('developer', ''),
                            row_dict.get('capacity_mw'), row_dict.get('capacity_kw'),
                            row_dict.get('type'), row_dict.get('status'),
                            row_dict.get('raw_status'),
                            'IL', row_dict.get('county'), row_dict.get('city'),
                            row_dict.get('utility'),
                            row_dict.get('queue_date'), row_dict.get('cod'),
                            row_dict.get('system_size_dc_kw'), row_dict.get('system_size_ac_kw'),
                            row_dict.get('installer'),
                            row_dict.get('interconnection_program'),
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
                            type, status, raw_status, state, county, city, utility,
                            queue_date, cod, source, primary_source, sources,
                            system_size_dc_kw, system_size_ac_kw,
                            installer, interconnection_program,
                            dg_stage, dg_stage_confidence, dg_stage_method, row_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        queue_id, region, None, row_dict.get('developer', ''),
                        row_dict.get('capacity_mw'), row_dict.get('capacity_kw'),
                        row_dict.get('type'), row_dict.get('status'),
                        row_dict.get('raw_status'),
                        'IL', row_dict.get('county'), row_dict.get('city'),
                        row_dict.get('utility'),
                        row_dict.get('queue_date'), row_dict.get('cod'),
                        SOURCE, SOURCE, sources_json,
                        row_dict.get('system_size_dc_kw'), row_dict.get('system_size_ac_kw'),
                        row_dict.get('installer'),
                        row_dict.get('interconnection_program'),
                        row_dict.get('dg_stage'), row_dict.get('dg_stage_confidence'),
                        row_dict.get('dg_stage_method'), row_hash,
                    ))
                    stats['added'] += 1

                cursor.execute('''
                    INSERT OR REPLACE INTO project_sources (queue_id, region, source, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ''', (queue_id, region, SOURCE))

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
            SOURCE, 'Illinois Shines (Adjustable Block Program)', 'IL', 'Multiple',
            'https://illinoisshines.com/project-application-reports/',
            'excel_download', datetime.now().isoformat(),
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
    parser = argparse.ArgumentParser(description="IL Shines Loader")
    parser.add_argument('--stats', action='store_true')
    parser.add_argument('--no-cache', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--db', type=str)
    args = parser.parse_args()

    loader = ILShinesLoader(db_path=Path(args.db) if args.db else None)

    print(f"\n{'='*60}")
    print(f"IL Shines Loader — {SOURCE}")
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
        print(f"By Region: {df['region'].value_counts().to_dict()}")
        active = df[df['status'] == 'Active']
        if not active.empty:
            print(f"Active by stage: {active['dg_stage'].value_counts().to_dict()}")


if __name__ == '__main__':
    main()
