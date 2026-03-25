#!/usr/bin/env python3
"""
NY DPS SIR Loader — Utility Interconnection Queue Data from NY DPS

Loads DG interconnection queue data with full milestone tracking from all 6 NY utilities.
Each utility files monthly SIR Inventory Reports to the NY DPS in Excel format.

This is the HIGHEST VALUE DG stage data source — every project has timestamped milestones:
  Application → Approval → Preliminary Review → CESIR Study → Construction → Acceptance

Data Source: https://dps.ny.gov/distributed-generation-information
Auth: None required (public downloads)
Format: Excel (xlsx/xls per utility, monthly filings)
Refresh: Monthly recommended

Usage:
    python3 ny_dps_sir_loader.py                  # Full load (download + parse + store)
    python3 ny_dps_sir_loader.py --stats          # Show stats after load
    python3 ny_dps_sir_loader.py --no-cache       # Force fresh download
    python3 ny_dps_sir_loader.py --dry-run        # Fetch + normalize, don't write DB
    python3 ny_dps_sir_loader.py --download-only  # Just download files
"""

import hashlib
import json
import logging
import sqlite3
import requests
import zipfile
import io
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / '.data'
DG_DB_PATH = DATA_DIR / 'dg.db'
CACHE_DIR = Path(__file__).parent / '.cache' / 'dg' / 'ny_dps_sir'

SOURCE = 'ny_dps_sir'
REGION = 'NYISO'

# ── Utility download configs ─────────────────────────────────────────────

UTILITIES = {
    'national_grid': {
        'name': 'National Grid',
        'url': 'https://dps.ny.gov/national-grid-interconnection-queue-data-0',
        'filename': 'national_grid_interconnection_queue.xlsx',
        'engine': 'openpyxl',
        'has_developer': False,
    },
    'central_hudson': {
        'name': 'Central Hudson',
        'url': 'https://dps.ny.gov/central-hudson-interconnection-queue-data-0',
        'filename': 'central_hudson_interconnection_queue.xlsx',
        'engine': 'openpyxl',
        'has_developer': True,
    },
    'orange_rockland': {
        'name': 'Orange & Rockland',
        'url': 'https://dps.ny.gov/orange-and-rockland-interconnection-queue-data-0',
        'filename': 'orange_rockland_interconnection_queue.xlsx',
        'engine': 'openpyxl',
        'has_developer': True,
    },
    'nyseg_rge': {
        'name': 'NYSEG & RG&E',
        'url': 'https://dps.ny.gov/nyseg-and-rge-interconnection-queue-data-0',
        'filename': 'nyseg_rge_interconnection_queue.xls',
        'engine': None,  # xlrd auto-detected
        'has_developer': True,
    },
    'pseg_li': {
        'name': 'PSEG Long Island',
        'url': 'https://dps.ny.gov/pseg-li-interconnection-queue-data-0',
        'filename': 'pseg_li_interconnection_queue.xlsx',
        'engine': 'openpyxl',
        'has_developer': True,
    },
    # Con Edison uses .xlsb (binary Excel) inside a zip — requires pyxlsb
    'con_edison': {
        'name': 'Con Edison',
        'url': 'https://dps.ny.gov/con-edison-interconnection-queue-data',
        'filename': 'con_edison_interconnection_queue.xlsb',
        'engine': 'pyxlsb',
        'has_developer': True,
        'is_zip': True,
    },
}

# ── Column normalization ─────────────────────────────────────────────────
# Column names contain newlines and slight variations across utilities.
# We normalize by stripping whitespace/newlines and matching patterns.

def _normalize_col(col: str) -> str:
    """Normalize column name: strip newlines, extra spaces, footnote numbers."""
    import re
    col = re.sub(r'\n', ' ', str(col))
    col = re.sub(r'\s+', ' ', col).strip()
    # Remove trailing footnote numbers like "4" in "cost paid by applicant4"
    col = re.sub(r'(\d+)$', '', col).strip()
    return col


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns using pattern matching on normalized names.

    Handles the tricky part: 'Start Date', 'End Date' appear 3 times each
    (App Review, Prelim Review, CESIR). Pandas adds .1, .2 suffixes.
    We use those suffixes to disambiguate.
    """
    rename = {}
    for col in df.columns:
        norm = _normalize_col(col)
        # Exact pandas-suffixed columns (disambiguation for repeated headers)
        if col == 'Start Date':
            rename[col] = 'app_review_start'
        elif col == 'End Date':
            rename[col] = 'app_review_end'
        elif col == 'End Date.1':
            rename[col] = 'prelim_end'
        elif col == 'Start Date.1':
            rename[col] = 'cesir_start'
        elif col == 'End Date.2':
            rename[col] = 'cesir_end'
        elif col == 'Calculated Duration':
            rename[col] = '_calc_dur_app'
        elif col == 'Calculated Duration.1':
            rename[col] = '_calc_dur_prelim'
        elif col == 'Calculated Duration.2':
            rename[col] = '_calc_dur_cesir'
        # Pattern-based matching for the rest
        elif 'Application' in norm and 'Job' in norm and 'Related' not in norm:
            rename[col] = 'application_id'
        elif norm.startswith('Application Approved'):
            rename[col] = 'approval_date'
        elif 'Prelim Start' in norm or ('Start Date' in norm and 'Match' in norm):
            rename[col] = 'prelim_start'
        elif 'CESIR Payment' in norm:
            rename[col] = 'cesir_payment_date'
        elif 'CESIR Study cost' in norm or 'CESIR Study Cost' in norm:
            rename[col] = 'cesir_cost'
        elif 'Estimated Upgrade' in norm:
            rename[col] = 'upgrade_cost_estimate'
        elif 'Actual Project Costs' in norm:
            rename[col] = 'actual_costs'
        elif 'Down Payment' in norm:
            rename[col] = 'down_payment_date'
        elif 'Full Payment' in norm:
            rename[col] = 'full_payment_date'
        elif 'Construction Start' in norm:
            rename[col] = 'construction_start'
        elif 'Construction' in norm and 'Complete' in norm:
            rename[col] = 'construction_complete'
        elif 'Verification' in norm or 'Final Acceptance' in norm:
            if 'Letter' not in norm:
                rename[col] = 'verification_date'
        elif 'Final Letter' in norm:
            rename[col] = 'acceptance_date'
        elif 'Project Complete' in norm or ('Project' in norm and 'Y/N/W' in norm):
            rename[col] = 'project_complete'
        elif 'Reconciliation' in norm:
            rename[col] = 'reconciliation_date'
        elif 'Retention' in norm and 'REC' in norm:
            rename[col] = 'utility_rec_retention'
        elif norm == 'Company':
            rename[col] = 'company'
        elif norm == 'Developer':
            rename[col] = 'developer'
        elif norm == 'Division':
            rename[col] = 'division'
        elif 'City' in norm:
            rename[col] = 'city'
        elif norm == 'County':
            rename[col] = 'county'
        elif 'Zip' in norm:
            rename[col] = 'zip_code'
        elif 'Load Zone' in norm:
            rename[col] = 'load_zone'
        elif 'Circuit' in norm:
            rename[col] = 'circuit_id'
        elif norm == 'Substation':
            rename[col] = 'substation'
        elif 'Hybrid' in norm:
            rename[col] = 'is_hybrid'
        elif 'Related' in norm:
            rename[col] = 'related_application'
        elif 'PV' in norm and 'kW' in norm:
            rename[col] = 'pv_kwac'
        elif 'ESS' in norm and 'kW' in norm:
            rename[col] = 'ess_kwac'
        elif 'WIND' in norm and 'kW' in norm:
            rename[col] = 'wind_kwac'
        elif 'CHP' in norm and 'kW' in norm:
            rename[col] = 'chp_kwac'
        elif 'HYDRO' in norm and 'kW' in norm:
            rename[col] = 'hydro_kwac'
        elif 'OTHER' in norm and 'kW' in norm:
            rename[col] = 'other_kwac'
        elif 'Metering' in norm:
            rename[col] = 'metering'
        elif 'Value Stack' in norm:
            rename[col] = 'value_stack'
        elif 'Protective' in norm:
            rename[col] = 'protective_equipment'

    return df.rename(columns=rename)


def compute_hash(row_dict: dict) -> str:
    """Compute MD5 hash of key fields for change detection."""
    key_fields = ['queue_id', 'capacity_kw', 'status', 'utility',
                  'project_complete', 'approval_date', 'construction_start',
                  'acceptance_date']
    parts = [str(row_dict.get(f, '')) for f in key_fields]
    return hashlib.md5('|'.join(parts).encode()).hexdigest()


def derive_stage(row: dict) -> Tuple[str, float, str]:
    """Derive DG development stage from IC milestone data.

    Priority (highest to lowest):
    1. acceptance_date or verification_date → operational (0.95)
    2. construction_complete → operational (0.90)
    3. construction_start → construction (0.90)
    4. down_payment/full_payment → construction (0.85)
    5. cesir_end → approved (0.85) — study complete, awaiting construction
    6. cesir_start → approved (0.80) — in study phase
    7. prelim_end or prelim_start → approved (0.75)
    8. approval_date → approved (0.70)
    9. app_review_end → applied (0.80) — review complete
    10. app_review_start → applied (0.75) — in review
    11. project_complete == 'W' → withdrawn (1.0)
    12. project_complete == 'Y' → operational (1.0)
    """
    complete = str(row.get('project_complete', '')).strip().upper()

    if complete == 'W':
        return 'withdrawn', 1.0, 'sir_milestone'
    if complete == 'Y':
        return 'operational', 1.0, 'sir_milestone'

    # For pending (N) projects, use milestones to determine stage
    if row.get('acceptance_date') or row.get('verification_date'):
        return 'operational', 0.95, 'sir_milestone'
    if row.get('construction_complete'):
        return 'operational', 0.90, 'sir_milestone'
    if row.get('construction_start'):
        return 'construction', 0.90, 'sir_milestone'
    if row.get('down_payment_date') or row.get('full_payment_date'):
        return 'construction', 0.85, 'sir_milestone'
    if row.get('cesir_end'):
        return 'approved', 0.85, 'sir_milestone'
    if row.get('cesir_start') or row.get('cesir_payment_date'):
        return 'approved', 0.80, 'sir_milestone'
    if row.get('prelim_end') or row.get('prelim_start'):
        return 'approved', 0.75, 'sir_milestone'
    if row.get('approval_date'):
        return 'approved', 0.70, 'sir_milestone'
    if row.get('app_review_end'):
        return 'applied', 0.80, 'sir_milestone'
    if row.get('app_review_start'):
        return 'applied', 0.75, 'sir_milestone'

    return 'applied', 0.50, 'sir_milestone'


class NYDPSSIRLoader:
    """Load NY utility SIR interconnection queue data from DPS filings."""

    def __init__(self, db_path: Path = None, cache_dir: Path = None):
        self.db_path = db_path or DG_DB_PATH
        self.cache_dir = cache_dir or CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.df: Optional[pd.DataFrame] = None

    def download(self, force: bool = False) -> Dict[str, Path]:
        """Download SIR files from all utilities. Returns dict of utility→path."""
        downloaded = {}
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ProspectorLabs/1.0',
        }

        for key, config in UTILITIES.items():
            filepath = self.cache_dir / config['filename']

            # Check cache
            if not force and filepath.exists():
                age_days = (datetime.now().timestamp() - filepath.stat().st_mtime) / 86400
                if age_days < 30:  # Monthly refresh
                    logger.info(f"  {config['name']}: cached ({age_days:.0f} days old)")
                    downloaded[key] = filepath
                    continue

            logger.info(f"  Downloading {config['name']}...")
            try:
                resp = requests.get(config['url'], headers=headers, timeout=120)
                resp.raise_for_status()

                if config.get('is_zip'):
                    # Con Edison serves a zip file
                    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                        # Find the xlsb file inside
                        xlsb_files = [f for f in zf.namelist() if f.endswith('.xlsb')]
                        if xlsb_files:
                            with open(filepath, 'wb') as f:
                                f.write(zf.read(xlsb_files[0]))
                            logger.info(f"    Extracted {xlsb_files[0]} ({filepath.stat().st_size / 1e6:.1f} MB)")
                        else:
                            logger.warning(f"    No .xlsb found in zip: {zf.namelist()}")
                            continue
                else:
                    with open(filepath, 'wb') as f:
                        f.write(resp.content)
                    logger.info(f"    Downloaded {filepath.stat().st_size / 1e6:.1f} MB")

                downloaded[key] = filepath

            except Exception as e:
                logger.warning(f"    Failed to download {config['name']}: {e}")
                # Fall back to cached file if it exists
                if filepath.exists():
                    logger.info(f"    Using stale cache")
                    downloaded[key] = filepath

        return downloaded

    def parse_utility(self, key: str, filepath: Path) -> pd.DataFrame:
        """Parse a single utility's SIR file into normalized DataFrame."""
        config = UTILITIES[key]
        engine = config.get('engine')

        try:
            read_kwargs = {'header': 2}
            if engine:
                read_kwargs['engine'] = engine
            df = pd.read_excel(filepath, **read_kwargs)
        except Exception as e:
            logger.error(f"Failed to parse {config['name']}: {e}")
            return pd.DataFrame()

        logger.info(f"  {config['name']}: {len(df):,} rows, {len(df.columns)} cols")

        # Normalize column names using pattern-based renaming
        df = _rename_columns(df)

        # Add utility identifier
        df['_utility_key'] = key
        df['_utility_name'] = config['name']

        return df

    def fetch(self, use_cache: bool = True) -> pd.DataFrame:
        """Download and parse all utility SIR files."""
        files = self.download(force=not use_cache)

        if not files:
            logger.error("No SIR files available")
            return pd.DataFrame()

        all_dfs = []
        for key, filepath in files.items():
            df = self.parse_utility(key, filepath)
            if not df.empty:
                all_dfs.append(df)

        if not all_dfs:
            return pd.DataFrame()

        # Combine — columns will be superset of all utilities
        combined = pd.concat(all_dfs, ignore_index=True, sort=False)
        logger.info(f"  Combined: {len(combined):,} total rows from {len(all_dfs)} utilities")
        return combined

    def normalize(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Normalize combined SIR data to dg.db projects schema."""
        logger.info(f"Normalizing {len(raw_df):,} raw records...")
        records = []

        for _, row in raw_df.iterrows():
            # Application ID is the unique identifier
            app_id = str(row.get('application_id', '')).strip()
            if not app_id or app_id == 'nan':
                continue

            utility_key = row.get('_utility_key', '')
            utility_name = row.get('_utility_name', '')

            # Build queue_id: utility prefix + application number
            queue_id = f"SIR-{utility_key[:4].upper()}-{app_id}"

            # Capacity: sum all technology types (kWAC)
            capacity_kw = 0
            for tech_col in ['pv_kwac', 'ess_kwac', 'wind_kwac', 'chp_kwac',
                             'hydro_kwac', 'other_kwac']:
                val = row.get(tech_col)
                if pd.notna(val):
                    try:
                        capacity_kw += float(val)
                    except (ValueError, TypeError):
                        pass

            if capacity_kw <= 0:
                continue  # Skip records with no capacity

            capacity_mw = capacity_kw / 1000.0

            # Determine technology type
            pv = float(row.get('pv_kwac', 0) or 0)
            ess = float(row.get('ess_kwac', 0) or 0)
            wind = float(row.get('wind_kwac', 0) or 0)
            chp = float(row.get('chp_kwac', 0) or 0)
            hydro = float(row.get('hydro_kwac', 0) or 0)

            if pv > 0 and ess > 0:
                tech_type = 'Solar + Storage'
            elif pv > 0:
                tech_type = 'Solar'
            elif wind > 0:
                tech_type = 'Wind'
            elif ess > 0:
                tech_type = 'Storage'
            elif chp > 0:
                tech_type = 'CHP'
            elif hydro > 0:
                tech_type = 'Hydro'
            else:
                tech_type = 'Other'

            # Status from Project Complete flag
            project_complete = str(row.get('project_complete', '')).strip().upper()
            if project_complete == 'Y':
                status = 'Operational'
            elif project_complete == 'W':
                status = 'Withdrawn'
            else:
                status = 'Active'

            # Parse milestone dates
            milestones = {}
            date_fields = [
                'app_review_start', 'app_review_end', 'approval_date',
                'prelim_start', 'prelim_end',
                'cesir_payment_date', 'cesir_start', 'cesir_end',
                'down_payment_date', 'full_payment_date',
                'construction_start', 'construction_complete',
                'verification_date', 'acceptance_date',
                'reconciliation_date',
            ]
            for field in date_fields:
                val = row.get(field)
                parsed = self._parse_date(val)
                milestones[field] = parsed

            # Derive DG stage from milestones
            stage, confidence, method = derive_stage({
                **milestones,
                'project_complete': project_complete,
            })

            # Queue date = application review start
            queue_date = milestones.get('app_review_start')

            # COD = acceptance date or verification date
            cod = (milestones.get('acceptance_date') or
                   milestones.get('verification_date') or
                   milestones.get('construction_complete'))

            # Build raw_status string from milestones for dg_stage.py compatibility
            if project_complete == 'Y':
                raw_status = 'SIR Complete'
            elif project_complete == 'W':
                raw_status = 'SIR Withdrawn'
            elif milestones.get('construction_start'):
                raw_status = 'SIR Under Construction'
            elif milestones.get('cesir_end'):
                raw_status = 'SIR CESIR Complete'
            elif milestones.get('cesir_start'):
                raw_status = 'SIR CESIR In Progress'
            elif milestones.get('approval_date'):
                raw_status = 'SIR Approved'
            elif milestones.get('app_review_end'):
                raw_status = 'SIR Review Complete'
            else:
                raw_status = 'SIR Application Received'

            # Developer (some utilities have it)
            developer = None
            dev_val = row.get('developer')
            if pd.notna(dev_val) and str(dev_val).strip():
                developer = str(dev_val).strip()

            record = {
                'queue_id': queue_id,
                'region': REGION,
                'name': None,
                'developer': developer or '',
                'capacity_mw': round(capacity_mw, 4),
                'capacity_kw': round(capacity_kw, 2),
                'type': tech_type,
                'status': status,
                'raw_status': raw_status,
                'state': 'NY',
                'county': str(row.get('county', '')).strip() or None,
                'city': str(row.get('city', '')).strip() or None,
                'utility': utility_name,
                'queue_date': queue_date,
                'cod': cod,
                'customer_sector': None,  # Not in SIR data
                'system_size_dc_kw': None,  # SIR reports kWAC only
                'system_size_ac_kw': capacity_kw,
                'substation': str(row.get('substation', '')).strip() or None,
                # DG stage fields (derived from milestones)
                'dg_stage': stage,
                'dg_stage_confidence': confidence,
                'dg_stage_method': method,
                # Milestone data stored as JSON for rich querying
                '_milestones': {k: v for k, v in milestones.items() if v},
                '_project_complete': project_complete,
                '_utility_key': utility_key,
                '_application_id': app_id,
                '_load_zone': str(row.get('load_zone', '')).strip() or None,
                '_circuit_id': str(row.get('circuit_id', '')).strip() or None,
                '_metering': str(row.get('metering', '')).strip() or None,
                '_is_hybrid': str(row.get('is_hybrid', '')).strip() or None,
                '_cesir_cost': row.get('cesir_cost') if pd.notna(row.get('cesir_cost')) else None,
                '_upgrade_cost': row.get('upgrade_cost_estimate') if pd.notna(row.get('upgrade_cost_estimate')) else None,
            }
            record['row_hash'] = compute_hash(record)
            records.append(record)

        df = pd.DataFrame(records)
        logger.info(f"  Normalized {len(df):,} records")

        if not df.empty:
            logger.info(f"  Statuses: {df['status'].value_counts().to_dict()}")
            logger.info(f"  DG stages: {df['dg_stage'].value_counts().to_dict()}")
            logger.info(f"  Types: {df['type'].value_counts().to_dict()}")
            logger.info(f"  Utilities: {df['utility'].value_counts().to_dict()}")
            logger.info(f"  Capacity: {df['capacity_kw'].min():.1f} - {df['capacity_kw'].max():.1f} kW")
            dev_pct = (df['developer'].str.len() > 0).mean() * 100
            logger.info(f"  Developer coverage: {dev_pct:.1f}%")

            # Stage distribution for active (pending) projects
            active = df[df['status'] == 'Active']
            if not active.empty:
                logger.info(f"\n  Active project stages ({len(active):,} projects):")
                for stage_val, count in active['dg_stage'].value_counts().items():
                    avg_conf = active[active['dg_stage'] == stage_val]['dg_stage_confidence'].mean()
                    logger.info(f"    {stage_val}: {count:,} (avg conf {avg_conf:.2f})")

        return df

    def store(self, df: pd.DataFrame) -> Dict[str, int]:
        """Upsert normalized SIR records into dg.db projects table."""
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
            ('substation', 'TEXT'),
            ('raw_data', 'TEXT'),
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

                # Build raw_data JSON with milestone details
                raw_data = {
                    'milestones': row_dict.get('_milestones', {}),
                    'project_complete': row_dict.get('_project_complete'),
                    'utility_key': row_dict.get('_utility_key'),
                    'application_id': row_dict.get('_application_id'),
                    'load_zone': row_dict.get('_load_zone'),
                    'circuit_id': row_dict.get('_circuit_id'),
                    'metering': row_dict.get('_metering'),
                    'is_hybrid': row_dict.get('_is_hybrid'),
                    'cesir_cost': row_dict.get('_cesir_cost'),
                    'upgrade_cost': row_dict.get('_upgrade_cost'),
                }

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
                                developer = COALESCE(NULLIF(?, ''), developer),
                                capacity_mw = ?, capacity_kw = ?,
                                type = ?, status = ?, raw_status = ?,
                                state = ?, county = ?, city = ?, utility = ?,
                                queue_date = COALESCE(?, queue_date),
                                cod = COALESCE(?, cod),
                                system_size_ac_kw = ?,
                                substation = COALESCE(?, substation),
                                dg_stage = ?, dg_stage_confidence = ?, dg_stage_method = ?,
                                raw_data = ?, row_hash = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', (
                            row_dict.get('developer', ''),
                            row_dict.get('capacity_mw'), row_dict.get('capacity_kw'),
                            row_dict.get('type'), row_dict.get('status'),
                            row_dict.get('raw_status'),
                            'NY', row_dict.get('county'), row_dict.get('city'),
                            row_dict.get('utility'),
                            row_dict.get('queue_date'), row_dict.get('cod'),
                            row_dict.get('system_size_ac_kw'),
                            row_dict.get('substation'),
                            row_dict.get('dg_stage'), row_dict.get('dg_stage_confidence'),
                            row_dict.get('dg_stage_method'),
                            json.dumps(raw_data, default=str),
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
                            system_size_ac_kw, substation,
                            dg_stage, dg_stage_confidence, dg_stage_method,
                            raw_data, row_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        queue_id, REGION, None, row_dict.get('developer', ''),
                        row_dict.get('capacity_mw'), row_dict.get('capacity_kw'),
                        row_dict.get('type'), row_dict.get('status'),
                        row_dict.get('raw_status'),
                        'NY', row_dict.get('county'), row_dict.get('city'),
                        row_dict.get('utility'),
                        row_dict.get('queue_date'), row_dict.get('cod'),
                        SOURCE, SOURCE, sources_json,
                        row_dict.get('system_size_ac_kw'),
                        row_dict.get('substation'),
                        row_dict.get('dg_stage'), row_dict.get('dg_stage_confidence'),
                        row_dict.get('dg_stage_method'),
                        json.dumps(raw_data, default=str),
                        row_hash,
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
            SOURCE, 'NY DPS SIR Interconnection Queue', 'NY', 'All NY Utilities',
            'https://dps.ny.gov/distributed-generation-information',
            'excel_download', datetime.now().isoformat(),
            stats['added'] + stats['updated'] + stats['unchanged']
        ))

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
        """Full pipeline: download → parse → normalize → store."""
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
        active = df[df['status'] == 'Active']
        return {
            'total_records': len(df),
            'total_capacity_mw': df['capacity_mw'].sum(),
            'by_status': df['status'].value_counts().to_dict(),
            'by_utility': df['utility'].value_counts().to_dict(),
            'by_type': df['type'].value_counts().to_dict(),
            'by_stage': df['dg_stage'].value_counts().to_dict(),
            'active_count': len(active),
            'active_by_stage': active['dg_stage'].value_counts().to_dict() if not active.empty else {},
            'developer_coverage': (df['developer'].str.len() > 0).mean(),
        }

    @staticmethod
    def _parse_date(val) -> Optional[str]:
        """Parse various date formats to MM/DD/YYYY."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        # Handle pandas NaT
        if pd.isna(val):
            return None
        # Handle pandas Timestamp
        if isinstance(val, pd.Timestamp):
            return val.strftime('%m/%d/%Y')
        # Handle datetime
        if isinstance(val, datetime):
            return val.strftime('%m/%d/%Y')
        val_str = str(val).strip()
        if not val_str or val_str.lower() == 'nan' or val_str.lower() == 'nat':
            return None
        # Try standard formats
        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y',
                     '%Y-%m-%dT%H:%M:%S', '%m/%d/%Y %H:%M:%S']:
            try:
                return datetime.strptime(val_str[:19], fmt).strftime('%m/%d/%Y')
            except ValueError:
                continue
        # Handle Excel serial dates (float like 46053.0)
        try:
            serial = float(val_str)
            if 30000 < serial < 60000:  # Reasonable Excel date range
                from datetime import timedelta
                base = datetime(1899, 12, 30)
                dt = base + timedelta(days=serial)
                return dt.strftime('%m/%d/%Y')
        except (ValueError, TypeError):
            pass
        return val_str[:10] if len(val_str) >= 10 else None


def main():
    import argparse
    parser = argparse.ArgumentParser(description="NY DPS SIR Loader")
    parser.add_argument('--stats', action='store_true', help='Show statistics after load')
    parser.add_argument('--no-cache', action='store_true', help='Force fresh download')
    parser.add_argument('--dry-run', action='store_true', help='Fetch + normalize, no DB write')
    parser.add_argument('--download-only', action='store_true', help='Just download files')
    parser.add_argument('--db', type=str, help='Override DB path')
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else None
    loader = NYDPSSIRLoader(db_path=db_path)

    print(f"\n{'='*60}")
    print(f"NY DPS SIR Loader — 6 utility interconnection queues")
    print(f"Target DB: {loader.db_path}")
    print(f"{'='*60}\n")

    if args.download_only:
        files = loader.download(force=args.no_cache)
        print(f"\nDownloaded {len(files)} files:")
        for key, path in files.items():
            print(f"  {key}: {path.name} ({path.stat().st_size / 1e6:.1f} MB)")
        return

    results = loader.load(use_cache=not args.no_cache, dry_run=args.dry_run)

    print(f"\n{'='*60}")
    print(f"Results: +{results.get('added', 0):,} added, "
          f"~{results.get('updated', 0):,} updated, "
          f"={results.get('unchanged', 0):,} unchanged")
    if results.get('errors', 0):
        print(f"  Errors: {results['errors']:,}")
    print(f"{'='*60}")

    if args.stats and loader.df is not None:
        stats = loader.get_stats()
        print(f"\nTotal records: {stats['total_records']:,}")
        print(f"Total capacity: {stats['total_capacity_mw']:,.1f} MW")
        print(f"Developer coverage: {stats['developer_coverage']*100:.1f}%")
        print(f"\nBy Status:")
        for k, v in stats.get('by_status', {}).items():
            print(f"  {k}: {v:,}")
        print(f"\nBy Utility:")
        for k, v in stats.get('by_utility', {}).items():
            print(f"  {k}: {v:,}")
        print(f"\nDG Stage Distribution:")
        for k, v in stats.get('by_stage', {}).items():
            print(f"  {k}: {v:,}")
        if stats.get('active_by_stage'):
            print(f"\nActive Projects by Stage ({stats['active_count']:,} total):")
            for k, v in stats['active_by_stage'].items():
                print(f"  {k}: {v:,}")
        print(f"\nBy Technology:")
        for k, v in stats.get('by_type', {}).items():
            print(f"  {k}: {v:,}")


if __name__ == '__main__':
    main()
