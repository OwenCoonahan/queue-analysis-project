#!/usr/bin/env python3
"""
NREL Sharing the Sun (STS) Loader — Community Solar Projects

Loads ~4,000 community solar projects from NREL's Sharing the Sun database
into dg.db. Covers 43 states with project-level data including capacity,
utility, developer, LMI info, and year of interconnection.

Data Source: https://www.nrel.gov/solar/market-research-analysis/sharing-the-sun.html
Auth: None (public Excel download)
Refresh: Annual (dataset updated ~yearly)

Usage:
    python3 nrel_sts_loader.py              # Full load
    python3 nrel_sts_loader.py --stats      # Show stats only
    python3 nrel_sts_loader.py --dry-run    # Parse only, don't write DB
"""

import sqlite3
import hashlib
import json
import logging
import openpyxl
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
CACHE_DIR = Path(__file__).parent / '.cache' / 'nrel_sts'

SOURCE = 'nrel_sts'
EXCEL_FILE = CACHE_DIR / 'sharing_the_sun_2025.xlsx'
SHEET_NAME = 'Project List'

# Map utility types to simplified form
UTILITY_TYPE_MAP = {
    'Investor Owned': 'IOU',
    'Cooperative': 'Coop',
    'Municipal': 'Muni',
    'Political Subdivision': 'Muni',
    'Federal': 'Federal',
    'State': 'State',
}

# State → ISO/RTO region mapping
STATE_REGION = {
    'CT': 'ISO-NE', 'MA': 'ISO-NE', 'ME': 'ISO-NE', 'NH': 'ISO-NE',
    'RI': 'ISO-NE', 'VT': 'ISO-NE',
    'NY': 'NYISO',
    'NJ': 'PJM', 'PA': 'PJM', 'DE': 'PJM', 'MD': 'PJM', 'DC': 'PJM',
    'VA': 'PJM', 'WV': 'PJM', 'OH': 'PJM', 'IN': 'PJM', 'IL': 'PJM',
    'MI': 'PJM', 'KY': 'PJM', 'NC': 'PJM', 'TN': 'PJM',
    'MN': 'MISO', 'WI': 'MISO', 'IA': 'MISO', 'MO': 'MISO',
    'AR': 'MISO', 'LA': 'MISO', 'MS': 'MISO', 'ND': 'MISO',
    'TX': 'ERCOT',
    'CA': 'CAISO',
    'OK': 'SPP', 'KS': 'SPP', 'NE': 'SPP',
    'FL': 'Southeast', 'GA': 'Southeast', 'SC': 'Southeast', 'AL': 'Southeast',
    'CO': 'West', 'AZ': 'West', 'NM': 'West', 'UT': 'West',
    'NV': 'West', 'OR': 'West', 'WA': 'West', 'MT': 'West',
    'ID': 'West', 'WY': 'West', 'HI': 'West', 'AK': 'West',
}


def compute_hash(row_dict: dict) -> str:
    """Compute MD5 hash of key fields for change detection."""
    key_fields = ['queue_id', 'capacity_kw', 'status', 'utility',
                  'state', 'city', 'developer']
    parts = [str(row_dict.get(f, '')) for f in key_fields]
    return hashlib.md5('|'.join(parts).encode()).hexdigest()


class NRELSTSLoader:
    """Load NREL Sharing the Sun community solar projects."""

    def __init__(self, db_path: Path = None, excel_path: Path = None):
        self.db_path = db_path or DG_DB_PATH
        self.excel_path = excel_path or EXCEL_FILE
        self.records = []

    def fetch(self) -> list:
        """Read the Excel file and return raw rows."""
        if not self.excel_path.exists():
            logger.error(f"Excel file not found: {self.excel_path}")
            logger.error("Download from: https://www.nrel.gov/solar/market-research-analysis/sharing-the-sun.html")
            return []

        logger.info(f"Reading {self.excel_path.name}...")
        wb = openpyxl.load_workbook(self.excel_path, data_only=True)
        ws = wb[SHEET_NAME]

        rows = list(ws.iter_rows(values_only=True))
        headers = rows[0]
        data = []
        for row in rows[1:]:
            record = {}
            for i, h in enumerate(headers):
                if h is not None:
                    record[h] = row[i] if i < len(row) else None
            data.append(record)

        wb.close()
        logger.info(f"  Read {len(data):,} rows from '{SHEET_NAME}'")
        return data

    def normalize(self, raw_data: list) -> list:
        """Normalize raw STS data to dg.db projects schema."""
        logger.info(f"Normalizing {len(raw_data):,} raw records...")
        records = []

        for i, row in enumerate(raw_data):
            state = str(row.get('State', '')).strip()
            if not state or len(state) != 2:
                continue

            # Build a unique ID: STS-{state}-{utility_id}-{row_index}
            utility_id = row.get('Utility ID', '')
            project_name = str(row.get('Project Name', '')).strip()
            queue_id = f"STS-{state}-{utility_id}-{i+1:04d}"

            # Capacity: use kW-AC (has actual values), fall back to kW-DC
            capacity_kw = None
            for field in ['System Size (kW-AC)', 'System Size (kW-DC)']:
                val = row.get(field)
                if val is not None:
                    try:
                        capacity_kw = float(val)
                        if capacity_kw > 0:
                            break
                    except (ValueError, TypeError):
                        continue

            if capacity_kw is None or capacity_kw <= 0:
                continue  # Skip records with no capacity

            capacity_mw = capacity_kw / 1000.0

            # DC capacity
            system_size_dc_kw = None
            dc_val = row.get('System Size (kW-DC)')
            if dc_val is not None:
                try:
                    system_size_dc_kw = float(dc_val)
                except (ValueError, TypeError):
                    pass

            # Year of interconnection → approximate COD
            year = row.get('Year of Interconnection')
            cod = None
            if year is not None:
                try:
                    yr = int(float(year))
                    if 2000 <= yr <= 2030:
                        cod = f"01/01/{yr}"
                except (ValueError, TypeError):
                    pass

            # Developer
            developer = str(row.get('Developer, Subscription Management, or Contractor Name', '')).strip()
            if developer in ('.', '', 'None', 'Unknown', 'N/A'):
                developer = None

            # Utility
            utility = str(row.get('Utility', '')).strip() or None

            # City
            city = str(row.get('City', '')).strip() or None

            # Program
            program = str(row.get('Program Name', '')).strip() or None

            # LMI info
            has_lmi = row.get('Does this Project have LMI Portion Requirement?')
            lmi_portion = row.get('LI/LMI Portion')

            # Region
            region = STATE_REGION.get(state, 'Other')

            record = {
                'queue_id': queue_id,
                'region': region,
                'name': project_name if project_name else None,
                'developer': developer,
                'capacity_mw': capacity_mw,
                'capacity_kw': capacity_kw,
                'system_size_dc_kw': system_size_dc_kw,
                'system_size_ac_kw': capacity_kw,
                'type': 'Solar',
                'status': 'Operational',  # STS tracks completed community solar
                'raw_status': 'Community Solar',
                'state': state,
                'county': None,  # Not in STS data
                'city': city,
                'utility': utility,
                'queue_date': None,  # Not in STS data
                'cod': cod,
                'customer_sector': 'Community Solar',
                'interconnection_program': program,
                'installer': None,
            }
            record['row_hash'] = compute_hash(record)
            records.append(record)

        self.records = records
        logger.info(f"  Normalized {len(records):,} records")

        # Stats
        if records:
            from collections import Counter
            states = Counter(r['state'] for r in records)
            logger.info(f"  States: {len(states)} unique")
            logger.info(f"  Top states: {states.most_common(10)}")
            total_mw = sum(r['capacity_mw'] for r in records if r['capacity_mw'])
            logger.info(f"  Total capacity: {total_mw:,.1f} MW")

        return records

    def store(self, records: list) -> Dict[str, int]:
        """Upsert normalized records into dg.db projects table."""
        logger.info(f"Upserting {len(records):,} records into dg.db...")

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        stats = {'added': 0, 'updated': 0, 'unchanged': 0, 'errors': 0}

        for rec in records:
            try:
                queue_id = rec['queue_id']
                row_hash = rec['row_hash']

                # Check existing
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
                                state = ?, city = ?, utility = ?,
                                cod = ?, customer_sector = ?,
                                system_size_dc_kw = ?, system_size_ac_kw = ?,
                                interconnection_program = ?, row_hash = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', (
                            rec['name'], rec['developer'],
                            rec['capacity_mw'], rec['capacity_kw'],
                            'Solar', rec['status'], rec['raw_status'],
                            rec['state'], rec['city'], rec['utility'],
                            rec['cod'], rec['customer_sector'],
                            rec['system_size_dc_kw'], rec['system_size_ac_kw'],
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
                            type, status, raw_status, state, city, utility,
                            cod, source, primary_source, sources,
                            customer_sector, system_size_dc_kw, system_size_ac_kw,
                            interconnection_program, row_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        queue_id, rec['region'], rec['name'], rec['developer'],
                        rec['capacity_mw'], rec['capacity_kw'],
                        'Solar', rec['status'], rec['raw_status'],
                        rec['state'], rec['city'], rec['utility'],
                        rec['cod'], SOURCE, SOURCE, sources_json,
                        rec['customer_sector'], rec['system_size_dc_kw'],
                        rec['system_size_ac_kw'], rec['interconnection_program'],
                        row_hash
                    ))
                    stats['added'] += 1

                # Track source provenance
                cursor.execute('''
                    INSERT OR REPLACE INTO project_sources (queue_id, region, source, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ''', (queue_id, rec['region'], SOURCE))

            except Exception as e:
                stats['errors'] += 1
                if stats['errors'] <= 5:
                    logger.warning(f"Error on {rec.get('queue_id', '?')}: {e}")

        conn.commit()

        # Update dg_programs registry
        cursor.execute('''
            INSERT OR REPLACE INTO dg_programs (
                program_key, program_name, state, utility, source_url,
                refresh_method, last_refreshed, record_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            SOURCE, 'NREL Sharing the Sun — Community Solar Database',
            'Multi-State', 'Multiple',
            'https://www.nrel.gov/solar/market-research-analysis/sharing-the-sun.html',
            'annual_excel', datetime.now().isoformat(),
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
            'success', len(records), stats['added'], stats['updated']
        ))
        conn.commit()
        conn.close()

        logger.info(f"  Added: {stats['added']:,}, Updated: {stats['updated']:,}, "
                     f"Unchanged: {stats['unchanged']:,}, Errors: {stats['errors']}")
        return stats

    def load(self, dry_run: bool = False) -> Dict[str, int]:
        """Full pipeline: fetch → normalize → store."""
        raw_data = self.fetch()
        if not raw_data:
            logger.error("No data loaded")
            return {'added': 0, 'updated': 0, 'unchanged': 0, 'errors': 0}

        records = self.normalize(raw_data)

        if dry_run:
            logger.info("DRY RUN — skipping database write")
            return {'added': 0, 'updated': 0, 'unchanged': 0, 'would_process': len(records)}

        return self.store(records)

    def get_stats(self) -> Dict:
        """Get statistics from loaded data."""
        if not self.records:
            return {}
        from collections import Counter
        records = self.records
        states = Counter(r['state'] for r in records)
        regions = Counter(r['region'] for r in records)
        utilities = Counter(r['utility'] for r in records if r['utility'])
        developers = Counter(r['developer'] for r in records if r['developer'])
        return {
            'total_records': len(records),
            'total_capacity_mw': sum(r['capacity_mw'] for r in records if r['capacity_mw']),
            'by_state': dict(states.most_common()),
            'by_region': dict(regions.most_common()),
            'by_utility': dict(utilities.most_common(15)),
            'by_developer': dict(developers.most_common(15)),
            'states_count': len(states),
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="NREL Sharing the Sun DG Loader")
    parser.add_argument('--stats', action='store_true', help='Show statistics after load')
    parser.add_argument('--dry-run', action='store_true', help='Parse only, no DB write')
    parser.add_argument('--db', type=str, help='Override DB path')
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else None
    loader = NRELSTSLoader(db_path=db_path)

    print(f"\n{'='*60}")
    print(f"NREL Sharing the Sun Loader — source: {SOURCE}")
    print(f"Target DB: {loader.db_path}")
    print(f"Excel: {loader.excel_path}")
    print(f"{'='*60}\n")

    results = loader.load(dry_run=args.dry_run)

    print(f"\n{'='*60}")
    print(f"Results: +{results.get('added', 0):,} added, "
          f"~{results.get('updated', 0):,} updated, "
          f"={results.get('unchanged', 0):,} unchanged")
    print(f"{'='*60}")

    if args.stats:
        stats = loader.get_stats()
        print(f"\nTotal records: {stats.get('total_records', 0):,}")
        print(f"Total capacity: {stats.get('total_capacity_mw', 0):,.1f} MW")
        print(f"States: {stats.get('states_count', 0)}")
        print(f"\nBy State:")
        for k, v in stats.get('by_state', {}).items():
            print(f"  {k}: {v:,}")
        print(f"\nBy Region:")
        for k, v in stats.get('by_region', {}).items():
            print(f"  {k}: {v:,}")
        print(f"\nBy Developer (top 15):")
        for k, v in stats.get('by_developer', {}).items():
            print(f"  {k}: {v:,}")


if __name__ == '__main__':
    main()
