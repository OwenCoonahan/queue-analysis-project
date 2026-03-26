#!/usr/bin/env python3
"""
Ameren Illinois DG Interconnection Queue Loader

Loads ~3,190 DG interconnection queue projects from Ameren Illinois public CSV
into dg.db. Good granular status data (Pending Review, Construction, Review
Complete, Post Construction, etc.) useful for DG stage classification.

Data Source: https://www.ameren.com/-/media/interconnect/queuereport/amerenpublicqueue.ashx
Auth: None required (public CSV)
Refresh: Daily

Usage:
    python3 ameren_il_loader.py              # Full load
    python3 ameren_il_loader.py --stats      # Show stats after load
    python3 ameren_il_loader.py --dry-run    # Parse only, no DB write
    python3 ameren_il_loader.py --refresh    # Re-download CSV first
"""

import sqlite3
import hashlib
import json
import logging
import pandas as pd
import requests
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
CACHE_DIR = Path(__file__).parent / '.cache' / 'ameren_il'

SOURCE = 'ameren_il'
REGION = 'MISO'  # Ameren IL is in MISO
CSV_URL = 'https://www.ameren.com/-/media/interconnect/queuereport/amerenpublicqueue.ashx'

# Status mapping: Ameren statuses → normalized + DG stage
STATUS_MAP = {
    'Pending Review': ('Active', 'applied'),
    'Review': ('Active', 'applied'),
    'UNKNOWN': ('Active', 'applied'),
    'In Dispute': ('Active', 'applied'),
    'Review Complete Pending Construction': ('Active', 'approved'),
    'Construction': ('Active', 'construction'),
    'Post Construction': ('Active', 'inspection'),
    'Withdrawn': ('Withdrawn', 'withdrawn'),
}

# DG stage confidence by status
STAGE_CONFIDENCE = {
    'applied': 0.80,
    'approved': 0.85,
    'construction': 0.90,
    'inspection': 0.85,
    'withdrawn': 1.0,
}


def compute_hash(row_dict: dict) -> str:
    key_fields = ['queue_id', 'capacity_kw', 'status', 'raw_status']
    parts = [str(row_dict.get(f, '')) for f in key_fields]
    return hashlib.md5('|'.join(parts).encode()).hexdigest()


class AmerenILLoader:
    """Load Ameren Illinois DG interconnection queue."""

    def __init__(self, db_path: Path = None):
        self.db_path = db_path or DG_DB_PATH
        self.cache_file = CACHE_DIR / 'AmerenPublicQueue.csv'
        self.records: List[dict] = []

    def fetch(self, refresh: bool = False) -> pd.DataFrame:
        """Download or read cached Ameren IL queue CSV."""
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

        if refresh or not self.cache_file.exists():
            logger.info(f"Downloading Ameren IL queue from {CSV_URL}...")
            resp = requests.get(CSV_URL, timeout=60)
            resp.raise_for_status()
            self.cache_file.write_bytes(resp.content)
            logger.info(f"  Downloaded {len(resp.content):,} bytes")

        cache_age = (datetime.now().timestamp() - self.cache_file.stat().st_mtime) / 3600
        logger.info(f"Reading {self.cache_file.name} ({cache_age:.1f}h old)...")
        df = pd.read_csv(self.cache_file, skiprows=1)
        logger.info(f"  {len(df):,} records")
        return df

    def normalize(self, df: pd.DataFrame) -> List[dict]:
        """Normalize Ameren IL data to dg.db schema."""
        logger.info(f"Normalizing {len(df):,} records...")
        records = []

        for _, row in df.iterrows():
            project_id = str(row.get('Project ID', '')).strip()
            if not project_id:
                continue

            queue_id = f"AMEREN-{project_id}"

            # Capacity: column says MW but values are in kW
            raw_size = row.get('Proposed Size (MW)', 0)
            try:
                capacity_kw = float(raw_size)
            except (ValueError, TypeError):
                capacity_kw = 0
            if capacity_kw <= 0:
                continue

            capacity_mw = capacity_kw / 1000.0

            # Status
            raw_status = str(row.get('Current Status', '')).strip()
            status_info = STATUS_MAP.get(raw_status, ('Active', 'applied'))
            normalized_status, dg_stage = status_info
            stage_confidence = STAGE_CONFIDENCE.get(dg_stage, 0.5)

            # Application type
            app_type = str(row.get('Application Type', '')).strip()

            # Queue position
            queue_pos = row.get('Queue Position')
            try:
                queue_pos = int(queue_pos) if pd.notna(queue_pos) else None
            except (ValueError, TypeError):
                queue_pos = None

            record = {
                'queue_id': queue_id,
                'region': REGION,
                'name': None,
                'developer': None,
                'capacity_mw': capacity_mw,
                'capacity_kw': capacity_kw,
                'type': 'Solar',  # Ameren IL queue is predominantly solar DG
                'status': normalized_status,
                'raw_status': raw_status,
                'state': 'IL',
                'county': None,
                'city': None,
                'utility': 'Ameren Illinois',
                'queue_date': None,
                'cod': None,
                'customer_sector': None,
                'interconnection_program': f'Ameren IL {app_type}',
                'dg_stage': dg_stage,
                'dg_stage_confidence': stage_confidence,
                'dg_stage_method': 'raw_status',
            }
            record['row_hash'] = compute_hash(record)
            records.append(record)

        self.records = records
        logger.info(f"  Normalized {len(records):,} records")

        if records:
            from collections import Counter
            statuses = Counter(r['raw_status'] for r in records)
            stages = Counter(r['dg_stage'] for r in records)
            logger.info(f"  Raw statuses: {dict(statuses)}")
            logger.info(f"  DG stages: {dict(stages)}")
            total_mw = sum(r['capacity_mw'] for r in records)
            logger.info(f"  Total capacity: {total_mw:,.1f} MW")

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
                                capacity_mw = ?, capacity_kw = ?,
                                type = ?, status = ?, raw_status = ?,
                                state = ?, utility = ?,
                                interconnection_program = ?,
                                dg_stage = ?, dg_stage_confidence = ?, dg_stage_method = ?,
                                row_hash = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', (
                            rec['capacity_mw'], rec['capacity_kw'],
                            rec['type'], rec['status'], rec['raw_status'],
                            'IL', rec['utility'],
                            rec['interconnection_program'],
                            rec['dg_stage'], rec['dg_stage_confidence'], rec['dg_stage_method'],
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
                            type, status, raw_status, state, utility,
                            source, primary_source, sources,
                            interconnection_program,
                            dg_stage, dg_stage_confidence, dg_stage_method,
                            row_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        queue_id, rec['region'], None, None,
                        rec['capacity_mw'], rec['capacity_kw'],
                        rec['type'], rec['status'], rec['raw_status'],
                        'IL', rec['utility'],
                        SOURCE, SOURCE, sources_json,
                        rec['interconnection_program'],
                        rec['dg_stage'], rec['dg_stage_confidence'], rec['dg_stage_method'],
                        row_hash
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
            SOURCE, 'Ameren Illinois DG Interconnection Queue', 'IL', 'Ameren Illinois',
            CSV_URL, 'csv_download', datetime.now().isoformat(),
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

    def load(self, refresh: bool = False, dry_run: bool = False) -> Dict[str, int]:
        df = self.fetch(refresh=refresh)
        if df.empty:
            return {'added': 0, 'updated': 0, 'unchanged': 0, 'errors': 0}
        records = self.normalize(df)
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
            'by_status': dict(Counter(x['raw_status'] for x in r)),
            'by_stage': dict(Counter(x['dg_stage'] for x in r)),
            'by_program': dict(Counter(x['interconnection_program'] for x in r)),
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Ameren IL DG Queue Loader")
    parser.add_argument('--stats', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--refresh', action='store_true', help='Re-download CSV')
    parser.add_argument('--db', type=str)
    args = parser.parse_args()

    loader = AmerenILLoader(db_path=Path(args.db) if args.db else None)
    print(f"\n{'='*60}")
    print(f"Ameren IL DG Queue Loader — source: {SOURCE}")
    print(f"Target DB: {loader.db_path}")
    print(f"{'='*60}\n")

    results = loader.load(refresh=args.refresh, dry_run=args.dry_run)
    print(f"\nResults: +{results.get('added', 0):,} added, "
          f"~{results.get('updated', 0):,} updated, "
          f"={results.get('unchanged', 0):,} unchanged")

    if args.stats:
        stats = loader.get_stats()
        print(f"\nTotal: {stats.get('total', 0):,} records, {stats.get('total_mw', 0):,.1f} MW")
        print(f"By status: {stats.get('by_status', {})}")
        print(f"By DG stage: {stats.get('by_stage', {})}")
        print(f"By program: {stats.get('by_program', {})}")


if __name__ == '__main__':
    main()
