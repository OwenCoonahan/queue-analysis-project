#!/usr/bin/env python3
"""
NC NCUC Renewable Energy Facility Registration Loader

Loads North Carolina solar facilities from the NCUC registration spreadsheet
into dg.db. Includes both active and revoked/canceled registrations.

Data Source: https://www.ncuc.gov/Reps/RegistrationSpreadsheetPresent.xlsx
Auth: None required (public download)
Refresh: Periodic (updated by NCUC ~quarterly)

Usage:
    python3 nc_ncuc_loader.py              # Full load
    python3 nc_ncuc_loader.py --stats      # Show stats after load
    python3 nc_ncuc_loader.py --dry-run    # Parse only, no DB write
"""

import sqlite3
import hashlib
import json
import logging
import openpyxl
from datetime import datetime
from pathlib import Path
from typing import Dict, List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / '.data'
DG_DB_PATH = DATA_DIR / 'dg.db'
CACHE_DIR = Path(__file__).parent / '.cache' / 'nc_ncuc'

SOURCE = 'nc_ncuc'
REGION = 'Southeast'
EXCEL_FILE = CACHE_DIR / 'RegistrationSpreadsheetPresent.xlsx'
DOWNLOAD_URL = 'https://www.ncuc.gov/Reps/RegistrationSpreadsheetPresent.xlsx'

# Status mapping based on sheet
SHEET_STATUS = {
    'New REF - All': 'Operational',
    'Rev|Can': 'Withdrawn',
}

# Fuel type filter — only load solar
SOLAR_FUELS = {'Solar Photovoltaic', 'Solar Thermal'}


def compute_hash(row_dict: dict) -> str:
    key_fields = ['queue_id', 'capacity_kw', 'status', 'name', 'developer']
    parts = [str(row_dict.get(f, '')) for f in key_fields]
    return hashlib.md5('|'.join(parts).encode()).hexdigest()


class NCNCUCLoader:
    """Load NC NCUC renewable energy facility registrations."""

    def __init__(self, db_path: Path = None, excel_path: Path = None):
        self.db_path = db_path or DG_DB_PATH
        self.excel_path = excel_path or EXCEL_FILE
        self.records: List[dict] = []

    def fetch(self) -> List[dict]:
        """Read the NCUC Excel file."""
        if not self.excel_path.exists():
            logger.error(f"Excel file not found: {self.excel_path}")
            logger.info(f"Download from: {DOWNLOAD_URL}")
            return []

        logger.info(f"Reading {self.excel_path.name}...")
        wb = openpyxl.load_workbook(self.excel_path, data_only=True)
        all_raw = []

        for sheet_name, status in SHEET_STATUS.items():
            ws = wb[sheet_name]
            rows = list(ws.iter_rows(values_only=True))

            # Find header row (contains 'Docket')
            header_idx = None
            for i, row in enumerate(rows):
                if row and any(str(v).startswith('Docket') for v in row if v):
                    header_idx = i
                    break

            if header_idx is None:
                logger.warning(f"Could not find header row in '{sheet_name}'")
                continue

            headers = rows[header_idx]
            count = 0
            for row in rows[header_idx + 1:]:
                if row[0] is None:
                    continue
                record = {}
                for j, h in enumerate(headers):
                    if h and j < len(row):
                        record[str(h).strip()] = row[j]
                record['_status'] = status
                record['_sheet'] = sheet_name
                all_raw.append(record)
                count += 1

            logger.info(f"  {sheet_name}: {count:,} records")

        wb.close()
        logger.info(f"  Total raw records: {len(all_raw):,}")
        return all_raw

    def normalize(self, raw_data: List[dict]) -> List[dict]:
        """Normalize NCUC data to dg.db schema."""
        logger.info(f"Normalizing {len(raw_data):,} records...")
        records = []

        for row in raw_data:
            fuel = str(row.get('Primary Fuel Type', '')).strip()
            if fuel not in SOLAR_FUELS:
                continue

            docket = str(row.get('Docket #', '')).strip()
            sub = row.get('Sub', '')
            if sub is not None:
                sub = str(int(sub)) if isinstance(sub, (int, float)) else str(sub).strip()
            else:
                sub = '0'

            queue_id = f"NCUC-{docket}-{sub}"

            facility = row.get('Facility')
            name = str(facility).strip() if facility and not isinstance(facility, (int, float)) else None
            if isinstance(facility, (int, float)):
                name = None  # Some entries have numeric facility values

            company = str(row.get('Company', '')).strip() or None
            capacity_kw = row.get('Capacity (kW)')

            if capacity_kw is None:
                continue
            try:
                capacity_kw = float(capacity_kw)
            except (ValueError, TypeError):
                continue
            if capacity_kw <= 0:
                continue

            # Cap obvious data entry errors (11 GW single facility)
            if capacity_kw > 2_000_000:  # > 2 GW is unreasonable for DG
                continue

            capacity_mw = capacity_kw / 1000.0
            status = row['_status']

            record = {
                'queue_id': queue_id,
                'region': REGION,
                'name': name,
                'developer': company,
                'capacity_mw': capacity_mw,
                'capacity_kw': capacity_kw,
                'system_size_dc_kw': None,
                'system_size_ac_kw': capacity_kw,
                'type': 'Solar',
                'status': status,
                'raw_status': f"{row['_sheet']}: {fuel}",
                'state': 'NC',
                'county': None,
                'city': None,
                'utility': company if company and ('Duke' in company or 'Dominion' in company) else None,
                'queue_date': None,
                'cod': None,
                'customer_sector': None,
                'interconnection_program': 'NCUC REF Registration',
            }
            record['row_hash'] = compute_hash(record)
            records.append(record)

        self.records = records
        logger.info(f"  Normalized {len(records):,} solar records")

        if records:
            from collections import Counter
            statuses = Counter(r['status'] for r in records)
            logger.info(f"  By status: {dict(statuses)}")
            caps = [r['capacity_kw'] for r in records]
            logger.info(f"  Capacity range: {min(caps):.1f} - {max(caps):.1f} kW")
            logger.info(f"  Total: {sum(r['capacity_mw'] for r in records):,.1f} MW")
            developers = Counter(r['developer'] for r in records if r['developer'])
            logger.info(f"  Top developers: {developers.most_common(5)}")

        return records

    def store(self, records: List[dict]) -> Dict[str, int]:
        """Upsert records into dg.db."""
        logger.info(f"Upserting {len(records):,} records into dg.db...")

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        stats = {'added': 0, 'updated': 0, 'unchanged': 0, 'errors': 0}

        for rec in records:
            try:
                queue_id = rec['queue_id']
                row_hash = rec['row_hash']

                cursor.execute(
                    'SELECT id, row_hash FROM projects WHERE queue_id = ? AND source = ?',
                    (queue_id, SOURCE)
                )
                existing = cursor.fetchone()

                if existing:
                    if existing['row_hash'] != row_hash:
                        cursor.execute('''
                            UPDATE projects SET
                                name = ?, developer = ?, capacity_mw = ?, capacity_kw = ?,
                                type = ?, status = ?, raw_status = ?,
                                state = ?, utility = ?,
                                system_size_ac_kw = ?,
                                interconnection_program = ?, row_hash = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', (
                            rec['name'], rec['developer'],
                            rec['capacity_mw'], rec['capacity_kw'],
                            'Solar', rec['status'], rec['raw_status'],
                            'NC', rec['utility'],
                            rec['system_size_ac_kw'],
                            rec['interconnection_program'], row_hash,
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
                            type, status, raw_status, state, utility,
                            source, primary_source, sources,
                            system_size_ac_kw,
                            interconnection_program, row_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        queue_id, rec['region'], rec['name'], rec['developer'],
                        rec['capacity_mw'], rec['capacity_kw'],
                        'Solar', rec['status'], rec['raw_status'],
                        'NC', rec['utility'],
                        SOURCE, SOURCE, sources_json,
                        rec['system_size_ac_kw'],
                        rec['interconnection_program'], row_hash
                    ))
                    stats['added'] += 1

                cursor.execute('''
                    INSERT OR REPLACE INTO project_sources (queue_id, region, source, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ''', (queue_id, rec['region'], SOURCE))

            except Exception as e:
                stats['errors'] += 1
                if stats['errors'] <= 5:
                    logger.warning(f"Error on {rec.get('queue_id', '?')}: {e}")

        conn.commit()

        cursor.execute('''
            INSERT OR REPLACE INTO dg_programs (
                program_key, program_name, state, utility, source_url,
                refresh_method, last_refreshed, record_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            SOURCE, 'NC Utilities Commission REF Registration', 'NC', 'Multiple',
            DOWNLOAD_URL, 'excel_download', datetime.now().isoformat(),
            stats['added'] + stats['updated'] + stats['unchanged']
        ))
        conn.commit()

        cursor.execute('''
            INSERT INTO refresh_log (source, started_at, completed_at, status,
                                     rows_processed, rows_added, rows_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            SOURCE, datetime.now().isoformat(), datetime.now().isoformat(),
            'success', len(records), stats['added'], stats['updated']
        ))
        conn.commit()
        conn.close()

        logger.info(f"  Added: {stats['added']:,}, Updated: {stats['updated']:,}, "
                     f"Unchanged: {stats['unchanged']:,}, Errors: {stats['errors']}")
        return stats

    def load(self, dry_run: bool = False) -> Dict[str, int]:
        raw_data = self.fetch()
        if not raw_data:
            return {'added': 0, 'updated': 0, 'unchanged': 0, 'errors': 0}
        records = self.normalize(raw_data)
        if dry_run:
            logger.info("DRY RUN — skipping database write")
            return {'would_process': len(records)}
        return self.store(records)

    def get_stats(self) -> Dict:
        if not self.records:
            return {}
        from collections import Counter
        r = self.records
        return {
            'total': len(r),
            'total_mw': sum(x['capacity_mw'] for x in r),
            'by_status': dict(Counter(x['status'] for x in r)),
            'by_developer': dict(Counter(x['developer'] for x in r if x['developer']).most_common(15)),
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="NC NCUC DG Loader")
    parser.add_argument('--stats', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--db', type=str)
    args = parser.parse_args()

    loader = NCNCUCLoader(db_path=Path(args.db) if args.db else None)
    print(f"\n{'='*60}")
    print(f"NC NCUC Loader — source: {SOURCE}")
    print(f"Target DB: {loader.db_path}")
    print(f"{'='*60}\n")

    results = loader.load(dry_run=args.dry_run)
    print(f"\nResults: +{results.get('added', 0):,} added, "
          f"~{results.get('updated', 0):,} updated, "
          f"={results.get('unchanged', 0):,} unchanged")

    if args.stats:
        stats = loader.get_stats()
        print(f"\nTotal: {stats.get('total', 0):,} records, {stats.get('total_mw', 0):,.1f} MW")
        print(f"By status: {stats.get('by_status', {})}")
        print(f"Top developers: {stats.get('by_developer', {})}")


if __name__ == '__main__':
    main()
