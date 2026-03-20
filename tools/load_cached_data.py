#!/usr/bin/env python3
"""
Load Cached Data Sources into master.db

Integrates data files sitting on disk that haven't been loaded yet:
1. LBNL Interconnection Cost Studies (5 ISOs) → interconnection_costs table
2. CEC Power Plants (CA) → permits table
3. NYSERDA Large-Scale Renewables → match to NYISO projects
4. FERC Interconnection Filings → regulatory_filings table
5. Texas PUC Filings → regulatory_filings table

Usage:
    python3 load_cached_data.py --all           # Load everything
    python3 load_cached_data.py --costs         # Just interconnection costs
    python3 load_cached_data.py --cec           # Just CEC plants
    python3 load_cached_data.py --nyserda       # Just NYSERDA
    python3 load_cached_data.py --ferc          # Just FERC filings
    python3 load_cached_data.py --puc           # Just Texas PUC
    python3 load_cached_data.py --dry-run       # No DB writes
"""

import sqlite3
import hashlib
import json
import logging
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
MASTER_DB = DATA_DIR / 'master.db'
CACHE_DIR = Path(__file__).parent / '.cache'

# ─────────────────────────────────────────────────────────────
# 1. INTERCONNECTION COST STUDIES
# ─────────────────────────────────────────────────────────────

COST_FILES = {
    'ISO-NE': {
        'file': 'isone_interconnection_cost_data.xlsx',
        'queue_id_col': 'Queue ID 1',
        'poi_col': '$2022 POI Cost/kW',
        'network_col': '$2022 Network Cost/kW',
        'total_col': '$2022 Total Cost/kW',
        'resource_col': 'Resource Type',
        'study_type_col': 'Study Type',
        'study_date_col': 'Study Date',
        'state_col': 'State',
        'mw_col': 'Nameplate MW',
        'queue_date_col': 'Queue Date',
        'status_col': 'Request Status',
    },
    'NYISO': {
        'file': 'nyiso_interconnection_cost_data.xlsx',
        'queue_id_col': 'Queue ID',
        'poi_col': '$2022 POI Cost/kW',
        'network_col': '$2022 Network Cost/kW',
        'total_col': '$2022 Total Cost/kW',
        'resource_col': 'Resource Type',
        'study_type_col': 'Study Type',
        'study_date_col': 'Study Date',
        'state_col': 'State',
        'mw_col': 'Nameplate MW',
        'queue_date_col': 'Queue Date',
        'status_col': 'Request Status',
    },
    'MISO': {
        'file': 'miso_costs_2021_clean_data.xlsx',
        'queue_id_col': 'Project #',
        'poi_col': 'Real POI/kW',
        'network_col': 'Real Network/kW',
        'total_col': 'Real Total/kW',
        'resource_col': 'Fuel',
        'study_type_col': 'Study Type',
        'study_date_col': 'Study Date',
        'state_col': 'State',
        'mw_col': 'Nameplate MW',
        'queue_date_col': 'Queue Date',
        'status_col': 'Request Status',
    },
    'PJM': {
        'file': 'pjm_costs_2022_clean_data.xlsx',
        'queue_id_col': 'Project #',
        'poi_col': '$2022 POI Cost/kW',
        'network_col': '$2022 Network Cost/kW',
        'total_col': '$2022 Total Cost/kW',
        'resource_col': 'Fuel',
        'study_type_col': 'Study Type',
        'study_date_col': 'Study Date',
        'state_col': 'State',
        'mw_col': 'Nameplate MW',
        'queue_date_col': 'Queue Date',
        'status_col': 'Request Status',
    },
    'SPP': {
        'file': 'spp_costs_2023_clean_data.xlsx',
        'queue_id_col': 'Project #',
        'poi_col': '$2022 POI Cost/kW',
        'network_col': '$2022 Network Cost/kW',
        'total_col': '$2022 Total Cost/kW',
        'resource_col': 'Fuel',
        'study_type_col': 'Study Type',
        'study_date_col': 'Study Date',
        'state_col': 'State',
        'mw_col': 'Nameplate MW',
        'queue_date_col': 'Queue Date',
        'status_col': 'Request Status',
    },
}


def load_interconnection_costs(dry_run: bool = False) -> Dict[str, int]:
    """Load LBNL interconnection cost studies into master.db."""
    logger.info("Loading interconnection cost studies...")

    all_records = []

    for region, cfg in COST_FILES.items():
        filepath = CACHE_DIR / cfg['file']
        if not filepath.exists():
            logger.warning(f"  Missing: {filepath}")
            continue

        df = pd.read_excel(filepath, sheet_name='data')
        logger.info(f"  {region}: {len(df):,} rows from {cfg['file']}")

        for _, row in df.iterrows():
            queue_id = str(row.get(cfg['queue_id_col'], '')).strip()
            if not queue_id or queue_id == 'nan':
                continue

            def safe_float(val):
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return None
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return None

            def safe_date(val):
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return None
                if isinstance(val, pd.Timestamp):
                    return val.strftime('%Y-%m-%d')
                s = str(val).strip()
                if s == 'nan':
                    return None
                # Handle year-only values
                try:
                    yr = int(float(s))
                    if 1990 < yr < 2030:
                        return f"{yr}-01-01"
                except (ValueError, TypeError):
                    pass
                return s[:10]

            record = {
                'queue_id': queue_id,
                'region': region,
                'state': str(row.get(cfg['state_col'], '')).strip() or None,
                'resource_type': str(row.get(cfg['resource_col'], '')).strip() or None,
                'nameplate_mw': safe_float(row.get(cfg['mw_col'])),
                'study_type': str(row.get(cfg['study_type_col'], '')).strip() or None,
                'study_date': safe_date(row.get(cfg['study_date_col'])),
                'queue_date': safe_date(row.get(cfg['queue_date_col'])),
                'request_status': str(row.get(cfg['status_col'], '')).strip() or None,
                'poi_cost_per_kw': safe_float(row.get(cfg['poi_col'])),
                'network_cost_per_kw': safe_float(row.get(cfg['network_col'])),
                'total_cost_per_kw': safe_float(row.get(cfg['total_col'])),
                'source_file': cfg['file'],
            }
            all_records.append(record)

    logger.info(f"  Total: {len(all_records):,} cost records across {len(COST_FILES)} ISOs")

    if dry_run:
        # Print summary stats
        df = pd.DataFrame(all_records)
        logger.info(f"  By region: {df['region'].value_counts().to_dict()}")
        logger.info(f"  By resource: {df['resource_type'].value_counts().head(10).to_dict()}")
        costs = df['total_cost_per_kw'].dropna()
        logger.info(f"  Cost stats: median=${costs.median():.0f}/kW, mean=${costs.mean():.0f}/kW, max=${costs.max():.0f}/kW")
        return {'would_load': len(all_records)}

    # Create table and insert
    conn = sqlite3.connect(MASTER_DB)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS interconnection_costs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue_id TEXT NOT NULL,
            region TEXT NOT NULL,
            state TEXT,
            resource_type TEXT,
            nameplate_mw REAL,
            study_type TEXT,
            study_date TEXT,
            queue_date TEXT,
            request_status TEXT,
            poi_cost_per_kw REAL,
            network_cost_per_kw REAL,
            total_cost_per_kw REAL,
            source_file TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(queue_id, region, study_type)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ic_queue_id ON interconnection_costs(queue_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ic_region ON interconnection_costs(region)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ic_resource ON interconnection_costs(resource_type)')

    stats = {'added': 0, 'updated': 0, 'skipped': 0}

    for rec in all_records:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO interconnection_costs (
                    queue_id, region, state, resource_type, nameplate_mw,
                    study_type, study_date, queue_date, request_status,
                    poi_cost_per_kw, network_cost_per_kw, total_cost_per_kw,
                    source_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                rec['queue_id'], rec['region'], rec['state'], rec['resource_type'],
                rec['nameplate_mw'], rec['study_type'], rec['study_date'],
                rec['queue_date'], rec['request_status'],
                rec['poi_cost_per_kw'], rec['network_cost_per_kw'],
                rec['total_cost_per_kw'], rec['source_file']
            ))
            stats['added'] += 1
        except Exception as e:
            stats['skipped'] += 1
            if stats['skipped'] <= 3:
                logger.warning(f"  Error: {e}")

    conn.commit()

    # Match costs to existing projects
    matched = _match_costs_to_projects(cursor)
    stats['matched'] = matched
    conn.commit()
    conn.close()

    logger.info(f"  Loaded: {stats['added']:,}, Matched to projects: {stats['matched']:,}")
    return stats


def _match_costs_to_projects(cursor) -> int:
    """Add cost data columns to matching projects in master.db."""
    # Add columns if they don't exist
    for col in ['ic_poi_cost_per_kw', 'ic_network_cost_per_kw', 'ic_total_cost_per_kw',
                'ic_study_type', 'ic_study_date']:
        try:
            cursor.execute(f'ALTER TABLE projects ADD COLUMN {col} REAL')
        except sqlite3.OperationalError:
            pass  # Column already exists

    # For text columns
    for col in ['ic_study_type', 'ic_study_date']:
        try:
            cursor.execute(f'ALTER TABLE projects ADD COLUMN {col} TEXT')
        except sqlite3.OperationalError:
            pass

    # Match by queue_id + region — use the most expensive study (usually final/facilities)
    cursor.execute('''
        UPDATE projects SET
            ic_poi_cost_per_kw = (
                SELECT poi_cost_per_kw FROM interconnection_costs ic
                WHERE ic.queue_id = projects.queue_id AND ic.region = projects.region
                ORDER BY ic.total_cost_per_kw DESC LIMIT 1
            ),
            ic_network_cost_per_kw = (
                SELECT network_cost_per_kw FROM interconnection_costs ic
                WHERE ic.queue_id = projects.queue_id AND ic.region = projects.region
                ORDER BY ic.total_cost_per_kw DESC LIMIT 1
            ),
            ic_total_cost_per_kw = (
                SELECT total_cost_per_kw FROM interconnection_costs ic
                WHERE ic.queue_id = projects.queue_id AND ic.region = projects.region
                ORDER BY ic.total_cost_per_kw DESC LIMIT 1
            )
        WHERE EXISTS (
            SELECT 1 FROM interconnection_costs ic
            WHERE ic.queue_id = projects.queue_id AND ic.region = projects.region
        )
    ''')

    matched = cursor.execute(
        'SELECT COUNT(*) FROM projects WHERE ic_total_cost_per_kw IS NOT NULL'
    ).fetchone()[0]
    return matched


# ─────────────────────────────────────────────────────────────
# 2. CEC POWER PLANTS
# ─────────────────────────────────────────────────────────────

def load_cec_plants(dry_run: bool = False) -> Dict[str, int]:
    """Load California Energy Commission power plant data into permits table."""
    filepath = CACHE_DIR / 'permits' / 'california' / 'cec_power_plants_raw.csv'
    if not filepath.exists():
        logger.warning(f"Missing: {filepath}")
        return {'error': 'file not found'}

    df = pd.read_csv(filepath)
    logger.info(f"CEC Power Plants: {len(df):,} rows")

    TECH_MAP = {
        'SUN': 'Solar', 'WND': 'Wind', 'WAT': 'Hydro', 'GEO': 'Geothermal',
        'NUC': 'Nuclear', 'NG': 'Gas', 'BIO': 'Biomass', 'OTH': 'Other',
        'PET': 'Oil', 'COL': 'Coal', 'STM': 'Steam', 'BAT': 'Storage',
        'MWS': 'Waste', 'LFG': 'Biogas', 'WH': 'Waste Heat', 'DIG': 'Biogas',
    }

    records = []
    for _, row in df.iterrows():
        cec_id = str(row.get('CECPlantID', '')).strip()
        if not cec_id or cec_id == 'nan':
            continue

        raw_tech = str(row.get('PriEnergySource', '')).strip()
        technology = TECH_MAP.get(raw_tech, raw_tech)

        status = 'Operational'
        if row.get('Retired Plant') == 1:
            status = 'Retired'

        records.append({
            'permit_id': f"CEC_{cec_id}",
            'source': 'cec_plants',
            'project_name': str(row.get('PlantName', '')).strip() or None,
            'developer': str(row.get('Operator Company', '')).strip() or None,
            'capacity_mw': float(row['Capacity_Latest']) if pd.notna(row.get('Capacity_Latest')) else None,
            'technology': technology,
            'state': 'CA',
            'county': str(row.get('County', '')).strip() or None,
            'latitude': float(row['y']) if pd.notna(row.get('y')) else None,
            'longitude': float(row['x']) if pd.notna(row.get('x')) else None,
            'status': status,
            'expected_cod': str(row.get('StartDate', ''))[:10] if pd.notna(row.get('StartDate')) else None,
        })

    logger.info(f"  Normalized {len(records):,} plants")

    if dry_run:
        rdf = pd.DataFrame(records)
        logger.info(f"  By tech: {rdf['technology'].value_counts().head(10).to_dict()}")
        logger.info(f"  By status: {rdf['status'].value_counts().to_dict()}")
        return {'would_load': len(records)}

    conn = sqlite3.connect(MASTER_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    stats = {'added': 0, 'updated': 0, 'errors': 0}
    for rec in records:
        try:
            row_hash = hashlib.md5(json.dumps(rec, sort_keys=True, default=str).encode()).hexdigest()
            cursor.execute(
                'SELECT id FROM permits WHERE permit_id = ? AND source = ?',
                (rec['permit_id'], rec['source'])
            )
            existing = cursor.fetchone()
            if existing:
                stats['updated'] += 1
            else:
                cursor.execute('''
                    INSERT INTO permits (
                        permit_id, source, project_name, developer, capacity_mw,
                        technology, state, county, latitude, longitude,
                        status, expected_cod, row_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    rec['permit_id'], rec['source'], rec['project_name'],
                    rec['developer'], rec['capacity_mw'], rec['technology'],
                    rec['state'], rec['county'], rec['latitude'], rec['longitude'],
                    rec['status'], rec['expected_cod'], row_hash
                ))
                stats['added'] += 1
        except Exception as e:
            stats['errors'] += 1
            if stats['errors'] <= 3:
                logger.warning(f"  Error: {e}")

    conn.commit()
    conn.close()
    logger.info(f"  Added: {stats['added']:,}, Updated: {stats['updated']:,}")
    return stats


# ─────────────────────────────────────────────────────────────
# 3. NYSERDA LARGE-SCALE RENEWABLES
# ─────────────────────────────────────────────────────────────

def load_nyserda(dry_run: bool = False) -> Dict[str, int]:
    """Load NYSERDA large-scale renewable projects, match to NYISO queue."""
    filepath = CACHE_DIR / 'permits' / 'nyserda' / 'nyserda_raw.json'
    if not filepath.exists():
        logger.warning(f"Missing: {filepath}")
        return {'error': 'file not found'}

    with open(filepath) as f:
        data = json.load(f)
    logger.info(f"NYSERDA: {len(data):,} projects")

    TECH_MAP = {
        'Solar': 'Solar', 'Land-Based Wind': 'Wind', 'Offshore Wind': 'Wind',
        'Hydroelectric': 'Hydro', 'Fuel Cell': 'Fuel Cell',
        'Renewable Natural Gas': 'Biogas', 'Biomass': 'Biomass',
    }

    records = []
    for proj in data:
        name = proj.get('project_name', '')
        if not name:
            continue

        raw_tech = str(proj.get('renewable_technology', '')).strip()
        technology = TECH_MAP.get(raw_tech, raw_tech)

        capacity_mw = None
        for cap_field in ['new_renewable_capacity_mw', 'bid_capacity_mw']:
            val = proj.get(cap_field)
            if val:
                try:
                    capacity_mw = float(val)
                    break
                except (ValueError, TypeError):
                    pass

        queue_number = proj.get('interconnection_queue_number')

        records.append({
            'permit_id': f"NYSERDA_{name.replace(' ', '_')[:50]}",
            'source': 'nyserda',
            'queue_id': str(queue_number).strip() if queue_number else None,
            'project_name': name,
            'developer': proj.get('developer_name') or proj.get('counterparty') or '',
            'capacity_mw': capacity_mw,
            'technology': technology,
            'state': 'NY',
            'county': proj.get('county_province'),
            'status': proj.get('project_status', 'Unknown'),
            'expected_cod': f"{int(float(proj['year_of_commercial_operation']))}-01-01"
                if proj.get('year_of_commercial_operation') else None,
            'nyiso_zone': proj.get('nyiso_zone'),
            'rec_price': proj.get('fixed_rec_price'),
            'orec_price': proj.get('index_orec_strike_price'),
        })

    logger.info(f"  Normalized {len(records):,} projects")
    with_queue = sum(1 for r in records if r.get('queue_id') and r['queue_id'] != 'None')
    logger.info(f"  With queue numbers: {with_queue}")

    if dry_run:
        rdf = pd.DataFrame(records)
        logger.info(f"  By tech: {rdf['technology'].value_counts().to_dict()}")
        logger.info(f"  By status: {rdf['status'].value_counts().to_dict()}")
        return {'would_load': len(records), 'with_queue_ids': with_queue}

    conn = sqlite3.connect(MASTER_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    stats = {'added': 0, 'matched': 0, 'errors': 0}
    for rec in records:
        try:
            row_hash = hashlib.md5(json.dumps(rec, sort_keys=True, default=str).encode()).hexdigest()
            cursor.execute('''
                INSERT OR REPLACE INTO permits (
                    permit_id, source, queue_id, project_name, developer,
                    capacity_mw, technology, state, county, status,
                    expected_cod, row_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                rec['permit_id'], rec['source'], rec.get('queue_id'),
                rec['project_name'], rec['developer'], rec['capacity_mw'],
                rec['technology'], rec['state'], rec['county'],
                rec['status'], rec['expected_cod'], row_hash
            ))
            stats['added'] += 1

            # Try to match to existing NYISO project
            if rec.get('queue_id') and rec['queue_id'] != 'None':
                cursor.execute(
                    'SELECT id FROM projects WHERE queue_id = ? AND region = ?',
                    (rec['queue_id'], 'NYISO')
                )
                match = cursor.fetchone()
                if match:
                    cursor.execute('''
                        UPDATE permits SET match_confidence = 1.0, match_method = 'queue_id',
                            region = 'NYISO'
                        WHERE permit_id = ? AND source = ?
                    ''', (rec['permit_id'], rec['source']))
                    stats['matched'] += 1

        except Exception as e:
            stats['errors'] += 1
            if stats['errors'] <= 3:
                logger.warning(f"  Error: {e}")

    conn.commit()
    conn.close()
    logger.info(f"  Added: {stats['added']:,}, Matched to NYISO: {stats['matched']:,}")
    return stats


# ─────────────────────────────────────────────────────────────
# 4. FERC INTERCONNECTION FILINGS
# ─────────────────────────────────────────────────────────────

def load_ferc_filings(dry_run: bool = False) -> Dict[str, int]:
    """Load FERC interconnection filings into regulatory_filings table."""
    filepath = CACHE_DIR / 'ferc' / 'ferc_interconnection_filings.parquet'
    if not filepath.exists():
        logger.warning(f"Missing: {filepath}")
        return {'error': 'file not found'}

    df = pd.read_parquet(filepath)
    logger.info(f"FERC Filings: {len(df):,} rows")

    if dry_run:
        logger.info(f"  Categories: {df['category'].value_counts().to_dict()}")
        logger.info(f"  Date range: {df['filed_date'].min()} to {df['filed_date'].max()}")
        return {'would_load': len(df)}

    conn = sqlite3.connect(MASTER_DB)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS regulatory_filings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            filing_id TEXT,
            docket TEXT,
            filed_date TEXT,
            category TEXT,
            description TEXT,
            developer TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, filing_id)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rf_source ON regulatory_filings(source)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rf_docket ON regulatory_filings(docket)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rf_developer ON regulatory_filings(developer)')

    stats = {'added': 0, 'errors': 0}
    for _, row in df.iterrows():
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO regulatory_filings (
                    source, filing_id, docket, filed_date, category, description, developer
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                'ferc',
                str(row.get('accession', '')).strip(),
                str(row.get('docket', '')).strip(),
                str(row.get('filed_date', '')).strip(),
                str(row.get('category', '')).strip(),
                str(row.get('description', '')).strip()[:500],
                str(row.get('developer', '')).strip() or None,
            ))
            stats['added'] += 1
        except Exception as e:
            stats['errors'] += 1

    conn.commit()
    conn.close()
    logger.info(f"  Added: {stats['added']:,}")
    return stats


# ─────────────────────────────────────────────────────────────
# 5. TEXAS PUC FILINGS
# ─────────────────────────────────────────────────────────────

def load_texas_puc(dry_run: bool = False) -> Dict[str, int]:
    """Load Texas PUC filings into regulatory_filings table."""
    filepath = CACHE_DIR / 'puc' / 'texas_puc_filings.parquet'
    if not filepath.exists():
        logger.warning(f"Missing: {filepath}")
        return {'error': 'file not found'}

    df = pd.read_parquet(filepath)
    logger.info(f"Texas PUC Filings: {len(df):,} rows")

    if dry_run:
        logger.info(f"  Types: {df['type'].value_counts().to_dict()}")
        logger.info(f"  Date range: {df['date'].min()} to {df['date'].max()}")
        return {'would_load': len(df)}

    conn = sqlite3.connect(MASTER_DB)
    cursor = conn.cursor()

    # Ensure table exists (may already from FERC load)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS regulatory_filings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            filing_id TEXT,
            docket TEXT,
            filed_date TEXT,
            category TEXT,
            description TEXT,
            developer TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, filing_id)
        )
    ''')

    stats = {'added': 0, 'errors': 0}
    for _, row in df.iterrows():
        try:
            filing_id = f"TX_{row.get('item', '')}"
            cursor.execute('''
                INSERT OR REPLACE INTO regulatory_filings (
                    source, filing_id, docket, filed_date, category, description, developer
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                'texas_puc',
                filing_id,
                None,  # Texas PUC doesn't use docket numbers in this data
                str(row.get('date', '')).strip(),
                str(row.get('type', '')).strip(),
                str(row.get('description', '')).strip()[:500],
                str(row.get('developer', '')).strip() if pd.notna(row.get('developer')) else None,
            ))
            stats['added'] += 1
        except Exception as e:
            stats['errors'] += 1

    conn.commit()
    conn.close()
    logger.info(f"  Added: {stats['added']:,}")
    return stats


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Load cached data into master.db")
    parser.add_argument('--all', action='store_true', help='Load everything')
    parser.add_argument('--costs', action='store_true', help='Interconnection costs only')
    parser.add_argument('--cec', action='store_true', help='CEC plants only')
    parser.add_argument('--nyserda', action='store_true', help='NYSERDA only')
    parser.add_argument('--ferc', action='store_true', help='FERC filings only')
    parser.add_argument('--puc', action='store_true', help='Texas PUC only')
    parser.add_argument('--dry-run', action='store_true', help='No DB writes')
    args = parser.parse_args()

    if not any([args.all, args.costs, args.cec, args.nyserda, args.ferc, args.puc]):
        args.all = True

    print(f"\n{'='*60}")
    print(f"Load Cached Data into master.db")
    print(f"Target: {MASTER_DB}")
    print(f"Dry run: {args.dry_run}")
    print(f"{'='*60}\n")

    results = {}

    if args.all or args.costs:
        results['interconnection_costs'] = load_interconnection_costs(dry_run=args.dry_run)

    if args.all or args.cec:
        results['cec_plants'] = load_cec_plants(dry_run=args.dry_run)

    if args.all or args.nyserda:
        results['nyserda'] = load_nyserda(dry_run=args.dry_run)

    if args.all or args.ferc:
        results['ferc_filings'] = load_ferc_filings(dry_run=args.dry_run)

    if args.all or args.puc:
        results['texas_puc'] = load_texas_puc(dry_run=args.dry_run)

    print(f"\n{'='*60}")
    print("Results:")
    for name, stats in results.items():
        print(f"  {name}: {stats}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
