#!/usr/bin/env python3
"""
Load Public Datasets — Dev5, 2026-03-20

Three high-value public datasets:
1. LBNL Tracking the Sun (Category A → dg.db) — 3.6M DG solar installs
2. USWTDB (Category C → grid.db) — 75K wind turbines
3. HIFLD Transmission Lines (Category C → grid.db) — 95K segments

Usage:
    python3 load_public_datasets.py --all
    python3 load_public_datasets.py --tts           # Tracking the Sun only
    python3 load_public_datasets.py --uswtdb        # Wind turbines only
    python3 load_public_datasets.py --hifld         # Transmission lines only
    python3 load_public_datasets.py --dry-run       # No DB writes
"""

import csv
import hashlib
import json
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / '.data'
CACHE_DIR = Path(__file__).parent / '.cache'
DG_DB = DATA_DIR / 'dg.db'
GRID_DB = DATA_DIR / 'grid.db'

# State → ISO region mapping for DG projects
STATE_TO_REGION = {
    'CA': 'CAISO', 'NV': 'CAISO',
    'TX': 'ERCOT',
    'NY': 'NYISO',
    'CT': 'ISO-NE', 'MA': 'ISO-NE', 'ME': 'ISO-NE', 'NH': 'ISO-NE',
    'RI': 'ISO-NE', 'VT': 'ISO-NE',
    'IL': 'PJM', 'IN': 'PJM', 'OH': 'PJM', 'PA': 'PJM', 'NJ': 'PJM',
    'DE': 'PJM', 'MD': 'PJM', 'VA': 'PJM', 'WV': 'PJM', 'DC': 'PJM',
    'MI': 'MISO', 'MN': 'MISO', 'IA': 'MISO', 'WI': 'MISO',
    'AR': 'MISO', 'MS': 'MISO', 'LA': 'MISO', 'MO': 'MISO',
    'ND': 'MISO', 'SD': 'MISO', 'MT': 'MISO',
    'KS': 'SPP', 'OK': 'SPP', 'NE': 'SPP', 'NM': 'SPP',
    'AL': 'Southeast', 'FL': 'Southeast', 'GA': 'Southeast',
    'SC': 'Southeast', 'NC': 'Southeast', 'TN': 'Southeast', 'KY': 'Southeast',
    'AZ': 'West', 'CO': 'West', 'ID': 'West', 'OR': 'West',
    'UT': 'West', 'WA': 'West', 'WY': 'West',
    'HI': 'Hawaii', 'AK': 'Alaska', 'PR': 'Puerto Rico', 'GU': 'Guam',
}

SEGMENT_MAP = {
    'RES_SF': 'Residential',
    'RES_MF': 'Residential',
    'RES': 'Residential',
    'COM': 'Commercial',
    'NON-RES': 'Commercial',
    'AGRICULTURAL': 'Agricultural',
    'SCHOOL': 'Government',
    'GOV': 'Government',
    'OTHER TAX-EXEMPT': 'Non-Profit',
    'NON-PROFIT': 'Non-Profit',
}

TECH_TYPE_MAP = {
    'pv-only': 'Solar',
    'pv+storage': 'Solar+Storage',
    'storage-only': 'Storage',
}


# ─────────────────────────────────────────────────────────────
# 1. LBNL TRACKING THE SUN → dg.db (Category A)
# ─────────────────────────────────────────────────────────────

def load_tracking_the_sun(dry_run: bool = False) -> Dict:
    """Load LBNL Tracking the Sun into dg.db as Category A (new project records)."""
    csv_path = CACHE_DIR / 'tracking_the_sun' / 'TTS_LBNL_public_file_29-Sep-2025_all.csv'
    if not csv_path.exists():
        logger.error(f"Missing: {csv_path}")
        return {'error': 'file not found'}

    logger.info("Loading LBNL Tracking the Sun...")
    logger.info(f"  Source: {csv_path}")

    # First pass: count and get existing queue_ids for dedup
    existing_ids = set()
    if not dry_run:
        conn = sqlite3.connect(DG_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT queue_id FROM projects")
        existing_ids = {r[0] for r in cursor.fetchall()}
        logger.info(f"  Existing dg.db records: {len(existing_ids):,}")
        conn.close()

    # Process in streaming fashion (1.9GB file)
    stats = {
        'total': 0, 'added': 0, 'skipped_existing': 0,
        'skipped_no_state': 0, 'skipped_no_size': 0, 'errors': 0,
        'by_state': {},
    }

    def safe_float(val):
        if val is None or val == '' or val == '-1' or val == '-1.0':
            return None
        try:
            v = float(val)
            return v if v > 0 else None
        except (ValueError, TypeError):
            return None

    def safe_date(val):
        if not val or val == '-1':
            return None
        # TTS dates are typically MM/DD/YYYY or YYYY-MM-DD
        val = str(val).strip()
        for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%m/%d/%y'):
            try:
                from datetime import datetime as dt
                return dt.strptime(val, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        return None

    batch = []
    BATCH_SIZE = 10000

    if not dry_run:
        conn = sqlite3.connect(DG_DB)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        cursor = conn.cursor()

    with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            stats['total'] += 1

            state = (row.get('state') or '').strip()
            if not state or state == '-1' or len(state) != 2:
                stats['skipped_no_state'] += 1
                continue

            size_kw = safe_float(row.get('PV_system_size_DC'))
            if size_kw is None or size_kw <= 0:
                stats['skipped_no_size'] += 1
                continue

            # Build queue_id: TTS_{data_provider}_{system_id}
            provider = (row.get('data_provider_1') or 'UNK').strip().replace(' ', '_')[:20]
            sys_id = (row.get('system_ID_1') or '').strip()
            if not sys_id or sys_id == '-1':
                sys_id = str(stats['total'])
            queue_id = f"TTS_{provider}_{sys_id}"

            # Skip if already exists
            if queue_id in existing_ids:
                stats['skipped_existing'] += 1
                continue

            region = STATE_TO_REGION.get(state, 'Unknown')
            tech = TECH_TYPE_MAP.get(
                (row.get('technology_type') or 'pv-only').strip().lower(), 'Solar'
            )
            segment = SEGMENT_MAP.get(
                (row.get('customer_segment') or '').strip(), None
            )

            capacity_mw = size_kw / 1000.0
            installer = (row.get('installer_name') or '').strip()
            if installer == '-1':
                installer = None
            utility = (row.get('utility_service_territory') or '').strip()
            if utility == '-1':
                utility = None
            city = (row.get('city') or '').strip()
            if city == '-1':
                city = None
            cost = safe_float(row.get('total_installed_price'))

            install_date = safe_date(row.get('installation_date'))

            # Battery storage
            batt_kwh = safe_float(row.get('battery_rated_capacity_kWh'))

            record = (
                queue_id,           # queue_id
                region,             # region
                None,               # name
                installer,          # developer (installer is the closest)
                capacity_mw,        # capacity_mw
                size_kw,            # capacity_kw
                tech,               # type
                'Operational',      # status (TTS = installed systems)
                state,              # state
                None,               # county (not in TTS)
                city,               # city
                utility,            # utility
                install_date,       # queue_date (use install date)
                install_date,       # cod (same as install for operational)
                'lbnl_tts',         # source
                'lbnl_tts',         # primary_source
                '["lbnl_tts"]',     # sources
                segment,            # customer_sector
                size_kw,            # system_size_dc_kw
                None,               # system_size_ac_kw
                batt_kwh,           # storage_capacity_kwh
                installer,          # installer
                cost,               # total_system_cost
                None,               # interconnection_program
            )
            batch.append(record)
            stats['by_state'][state] = stats['by_state'].get(state, 0) + 1

            if not dry_run and len(batch) >= BATCH_SIZE:
                _insert_batch(cursor, batch)
                stats['added'] += len(batch)
                batch = []
                if stats['added'] % 100000 == 0:
                    conn.commit()
                    logger.info(f"  Loaded {stats['added']:,} / {stats['total']:,}...")

    # Final batch
    if not dry_run and batch:
        _insert_batch(cursor, batch)
        stats['added'] += len(batch)
        conn.commit()

    if not dry_run:
        # Update refresh_log
        cursor.execute('''
            INSERT INTO refresh_log (source, started_at, completed_at, status, rows_processed, rows_added, rows_updated)
            VALUES (?, ?, ?, ?, ?, ?, 0)
        ''', ('lbnl_tts', datetime.now().isoformat(), datetime.now().isoformat(),
              'success', stats['total'], stats['added']))
        conn.commit()
        conn.close()

    logger.info(f"  Total processed: {stats['total']:,}")
    logger.info(f"  Added: {stats['added']:,}")
    logger.info(f"  Skipped (existing): {stats['skipped_existing']:,}")
    logger.info(f"  Skipped (no state): {stats['skipped_no_state']:,}")
    logger.info(f"  Skipped (no size): {stats['skipped_no_size']:,}")
    logger.info(f"  States: {len(stats['by_state'])}")
    top_states = sorted(stats['by_state'].items(), key=lambda x: -x[1])[:10]
    logger.info(f"  Top states: {dict(top_states)}")

    return stats


def _insert_batch(cursor, batch):
    """Insert a batch of records into dg.db projects table."""
    cursor.executemany('''
        INSERT OR IGNORE INTO projects (
            queue_id, region, name, developer, capacity_mw, capacity_kw,
            type, status, state, county, city, utility,
            queue_date, cod, source, primary_source, sources,
            customer_sector, system_size_dc_kw, system_size_ac_kw,
            storage_capacity_kwh, installer, total_system_cost,
            interconnection_program
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', batch)


# ─────────────────────────────────────────────────────────────
# 2. USWTDB → grid.db (Category C)
# ─────────────────────────────────────────────────────────────

def load_uswtdb(dry_run: bool = False) -> Dict:
    """Load US Wind Turbine Database into grid.db as Category C (reference table)."""
    csv_path = CACHE_DIR / 'uswtdb' / 'uswtdb_V8_2_20251210.csv'
    if not csv_path.exists():
        # Try to find any uswtdb CSV
        csvs = list((CACHE_DIR / 'uswtdb').glob('uswtdb*.csv'))
        if csvs:
            csv_path = csvs[0]
        else:
            logger.error(f"Missing USWTDB CSV in {CACHE_DIR / 'uswtdb'}")
            return {'error': 'file not found'}

    logger.info(f"Loading USWTDB from {csv_path.name}...")

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    logger.info(f"  Total turbines: {len(rows):,}")

    if dry_run:
        from collections import Counter
        states = Counter(r['t_state'] for r in rows)
        manus = Counter(r['t_manu'] for r in rows)
        logger.info(f"  States: {len(states)}")
        logger.info(f"  Top states: {states.most_common(5)}")
        logger.info(f"  Top manufacturers: {manus.most_common(5)}")
        return {'would_load': len(rows)}

    conn = sqlite3.connect(GRID_DB)
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS wind_turbines (
            case_id INTEGER PRIMARY KEY,
            eia_id INTEGER,
            state TEXT,
            county TEXT,
            fips TEXT,
            project_name TEXT,
            project_year INTEGER,
            project_turbine_count INTEGER,
            project_capacity_mw REAL,
            manufacturer TEXT,
            model TEXT,
            capacity_kw INTEGER,
            hub_height_m REAL,
            rotor_diameter_m REAL,
            rotor_swept_area_m2 REAL,
            total_height_m REAL,
            offshore INTEGER,
            retrofit INTEGER,
            retrofit_year INTEGER,
            confidence_attr INTEGER,
            confidence_loc INTEGER,
            longitude REAL,
            latitude REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_wt_state ON wind_turbines(state)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_wt_project ON wind_turbines(project_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_wt_eia ON wind_turbines(eia_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_wt_manu ON wind_turbines(manufacturer)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_wt_year ON wind_turbines(project_year)')

    def safe_int(val):
        if not val or val == '':
            return None
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return None

    def safe_float(val):
        if not val or val == '':
            return None
        try:
            v = float(val)
            return v if v > -999999 else None
        except (ValueError, TypeError):
            return None

    stats = {'added': 0, 'errors': 0}
    batch = []
    for row in rows:
        batch.append((
            safe_int(row.get('case_id')),
            safe_int(row.get('eia_id')),
            row.get('t_state', '').strip() or None,
            row.get('t_county', '').strip() or None,
            row.get('t_fips', '').strip() or None,
            row.get('p_name', '').strip() or None,
            safe_int(row.get('p_year')),
            safe_int(row.get('p_tnum')),
            safe_float(row.get('p_cap')),
            row.get('t_manu', '').strip() or None,
            row.get('t_model', '').strip() or None,
            safe_int(row.get('t_cap')),
            safe_float(row.get('t_hh')),
            safe_float(row.get('t_rd')),
            safe_float(row.get('t_rsa')),
            safe_float(row.get('t_ttlh')),
            safe_int(row.get('t_offshore')),
            safe_int(row.get('t_retrofit')),
            safe_int(row.get('t_retro_yr')),
            safe_int(row.get('t_conf_atr')),
            safe_int(row.get('t_conf_loc')),
            safe_float(row.get('xlong')),
            safe_float(row.get('ylat')),
        ))

    cursor.executemany('''
        INSERT OR REPLACE INTO wind_turbines (
            case_id, eia_id, state, county, fips, project_name,
            project_year, project_turbine_count, project_capacity_mw,
            manufacturer, model, capacity_kw, hub_height_m, rotor_diameter_m,
            rotor_swept_area_m2, total_height_m, offshore, retrofit, retrofit_year,
            confidence_attr, confidence_loc, longitude, latitude
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', batch)
    stats['added'] = len(batch)

    conn.commit()
    conn.close()

    logger.info(f"  Loaded {stats['added']:,} turbines into grid.db:wind_turbines")
    return stats


# ─────────────────────────────────────────────────────────────
# 3. HIFLD TRANSMISSION LINES → grid.db (Category C)
# ─────────────────────────────────────────────────────────────

def load_hifld_transmission(dry_run: bool = False) -> Dict:
    """Load HIFLD transmission lines into grid.db as Category C (reference table)."""
    json_path = CACHE_DIR / 'hifld' / 'transmission_lines.json'
    if not json_path.exists():
        logger.error(f"Missing: {json_path}")
        return {'error': 'file not found'}

    logger.info(f"Loading HIFLD transmission lines...")

    with open(json_path) as f:
        data = json.load(f)

    logger.info(f"  Total segments: {len(data):,}")

    if dry_run:
        from collections import Counter
        statuses = Counter(r.get('STATUS', '') for r in data)
        volt_classes = Counter(r.get('VOLT_CLASS', '') for r in data)
        logger.info(f"  Statuses: {dict(statuses)}")
        logger.info(f"  Voltage classes: {dict(volt_classes.most_common(10))}")
        return {'would_load': len(data)}

    conn = sqlite3.connect(GRID_DB)
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transmission_lines (
            id TEXT PRIMARY KEY,
            type TEXT,
            status TEXT,
            owner TEXT,
            voltage_kv REAL,
            voltage_class TEXT,
            substation_1 TEXT,
            substation_2 TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tl_owner ON transmission_lines(owner)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tl_voltage ON transmission_lines(voltage_kv)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tl_status ON transmission_lines(status)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tl_sub1 ON transmission_lines(substation_1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tl_sub2 ON transmission_lines(substation_2)')

    batch = []
    for rec in data:
        voltage = rec.get('VOLTAGE')
        if voltage is not None and voltage <= -999999:
            voltage = None

        batch.append((
            rec.get('ID'),
            rec.get('TYPE'),
            rec.get('STATUS'),
            rec.get('OWNER'),
            voltage,
            rec.get('VOLT_CLASS'),
            rec.get('SUB_1'),
            rec.get('SUB_2'),
        ))

    cursor.executemany('''
        INSERT OR REPLACE INTO transmission_lines (
            id, type, status, owner, voltage_kv, voltage_class,
            substation_1, substation_2
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', batch)

    conn.commit()

    # Also extract unique substations from the endpoints
    logger.info("  Extracting substations from line endpoints...")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS substations (
            name TEXT PRIMARY KEY,
            connected_lines INTEGER DEFAULT 0,
            max_voltage_kv REAL,
            owners TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sub_voltage ON substations(max_voltage_kv)')

    cursor.execute('''
        INSERT OR REPLACE INTO substations (name, connected_lines, max_voltage_kv, owners)
        SELECT
            sub_name,
            COUNT(*) as connected_lines,
            MAX(CASE WHEN voltage_kv > 0 THEN voltage_kv ELSE NULL END) as max_voltage_kv,
            GROUP_CONCAT(DISTINCT owner) as owners
        FROM (
            SELECT substation_1 as sub_name, voltage_kv, owner FROM transmission_lines
            WHERE substation_1 IS NOT NULL AND substation_1 != ''
            UNION ALL
            SELECT substation_2 as sub_name, voltage_kv, owner FROM transmission_lines
            WHERE substation_2 IS NOT NULL AND substation_2 != ''
        )
        GROUP BY sub_name
    ''')

    sub_count = cursor.execute('SELECT COUNT(*) FROM substations').fetchone()[0]
    conn.commit()
    conn.close()

    stats = {'lines_added': len(batch), 'substations_extracted': sub_count}
    logger.info(f"  Loaded {stats['lines_added']:,} transmission lines")
    logger.info(f"  Extracted {stats['substations_extracted']:,} unique substations")
    return stats


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Load public datasets")
    parser.add_argument('--all', action='store_true', help='Load everything')
    parser.add_argument('--tts', action='store_true', help='Tracking the Sun only')
    parser.add_argument('--uswtdb', action='store_true', help='USWTDB only')
    parser.add_argument('--hifld', action='store_true', help='HIFLD transmission only')
    parser.add_argument('--dry-run', action='store_true', help='No DB writes')
    args = parser.parse_args()

    if not any([args.all, args.tts, args.uswtdb, args.hifld]):
        args.all = True

    print(f"\n{'='*60}")
    print(f"Load Public Datasets (Dev5)")
    print(f"{'='*60}\n")

    results = {}

    if args.all or args.uswtdb:
        results['uswtdb'] = load_uswtdb(dry_run=args.dry_run)

    if args.all or args.hifld:
        results['hifld_transmission'] = load_hifld_transmission(dry_run=args.dry_run)

    if args.all or args.tts:
        results['tracking_the_sun'] = load_tracking_the_sun(dry_run=args.dry_run)

    print(f"\n{'='*60}")
    print("Results:")
    for name, stats in results.items():
        print(f"  {name}: {stats}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
