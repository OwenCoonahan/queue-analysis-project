#!/usr/bin/env python3
"""
NJ Clean Energy DG Scraper — Playwright browser automation

Downloads installation + pipeline Excel files from NJ Clean Energy solar activity
reports page, then loads into dg.db. ~247K solar projects across 6 NJ programs:
SRP, TI, ADI, CSEP, CSI, RNM.

Data Source: https://cleanenergy.nj.gov/resources/solar-activity-reports
Auth: None (Playwright needed — site blocks direct HTTP requests)
Refresh: Monthly (data posted 4th Wednesday of each month)

Usage:
    python3 nj_dg_scraper.py                    # Full scrape + load
    python3 nj_dg_scraper.py --download-only    # Just download files
    python3 nj_dg_scraper.py --load-only        # Load from cached files
    python3 nj_dg_scraper.py --stats            # Show stats after load
    python3 nj_dg_scraper.py --dry-run          # Download + normalize, no DB write
"""

import sqlite3
import hashlib
import json
import logging
import pandas as pd
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / '.data'
DG_DB_PATH = DATA_DIR / 'dg.db'
CACHE_DIR = Path(__file__).parent / '.cache' / 'dg' / 'nj'

SOURCE = 'nj_dg'
REGION = 'PJM'

# Solar activity reports page (correct URL as of March 2026)
REPORTS_URL = 'https://cleanenergy.nj.gov/resources/solar-activity-reports'

# NJ county codes → county names (NJ FIPS-style 1-21)
NJ_COUNTY_CODES = {
    1: 'Atlantic', 2: 'Bergen', 3: 'Burlington', 4: 'Camden',
    5: 'Cape May', 6: 'Cumberland', 7: 'Essex', 8: 'Gloucester',
    9: 'Hudson', 10: 'Hunterdon', 11: 'Mercer', 12: 'Middlesex',
    13: 'Monmouth', 14: 'Morris', 15: 'Ocean', 16: 'Passaic',
    17: 'Salem', 18: 'Somerset', 19: 'Sussex', 20: 'Union', 21: 'Warren',
}

# Status mapping: NJ program statuses → normalized
STATUS_MAP = {
    'Registration Complete': 'Operational',
    'SRP Registration Complete': 'Operational',
    'TI Registration Complete': 'Operational',
    'ADI Registration Complete': 'Operational',
    'CSEP Registration Complete': 'Operational',
    'CSI Registration Complete': 'Operational',
    'RNM Registration Complete': 'Operational',
    'As Built Complete': 'Operational',
    'As Built Incomplete-Review': 'Active',
    'Decertified': 'Withdrawn',
    'Verification Inspection': 'Active',
    'Verification Inspection Failed': 'Suspended',
    'Registration Accepted': 'Active',
    'Registration Received': 'Active',
    'Registration Pending': 'Active',
    'Conditional Approval': 'Active',
    'Expired': 'Withdrawn',
    'Cancelled': 'Withdrawn',
    'Withdrawn': 'Withdrawn',
    # TI program statuses
    'TI Application Complete': 'Active',
    'TI Extension Request Incomplete - Review': 'Active',
    # ADI program statuses
    'Accepted': 'Active',
    'Accepted Pending Verification': 'Active',
    'Final As-Built Received': 'Operational',
    'As-Built Incomplete': 'Active',
    'As-Built Complete': 'Operational',
    'As-Built Incomplete - Review': 'Active',
    'Onsite Inspection': 'Active',
    'Onsite Inspection Fail': 'Suspended',
    'Onsite Complete - Grid Supply': 'Operational',
    'Registration On Hold-PTO Prior to Acceptance': 'Active',
    'Registration On Hold-System Exceeds 20%': 'Active',
    'Registration On Hold –PTO Prior August 28, 2021': 'Active',
    'Public Entity': 'Active',
}

# Customer type mapping
SECTOR_MAP = {
    'Residential': 'Residential',
    'Commercial': 'Commercial',
    'Non Profit': 'Non-Profit',
    'School Public K-12': 'Government',
    'School Other': 'Government',
    'School Charter': 'Government',
    'Municipality': 'Government',
    'Government Facility': 'Government',
    'Public University': 'Government',
    'Private University': 'Commercial',
    'Farm': 'Agricultural',
    'SUNLIT': 'Residential',
}


def compute_hash(row_dict: dict) -> str:
    key_fields = ['queue_id', 'capacity_kw', 'status', 'utility', 'county', 'installer']
    parts = [str(row_dict.get(f, '')) for f in key_fields]
    return hashlib.md5('|'.join(parts).encode()).hexdigest()


class NJDGScraper:
    """Scrape NJ Clean Energy solar project data using Playwright."""

    def __init__(self, db_path: Path = None, cache_dir: Path = None):
        self.db_path = db_path or DG_DB_PATH
        self.cache_dir = cache_dir or CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.df: Optional[pd.DataFrame] = None

    def download(self, use_cache: bool = True) -> List[Path]:
        """Download Excel files from NJ Clean Energy using Playwright."""
        cached_files = sorted(self.cache_dir.glob('nj_*.xlsx'))
        if use_cache and cached_files:
            newest = max(f.stat().st_mtime for f in cached_files)
            cache_age_hours = (datetime.now().timestamp() - newest) / 3600
            if cache_age_hours < 168:  # 1 week
                logger.info(f"Using {len(cached_files)} cached files ({cache_age_hours:.0f}h old)")
                return cached_files

        logger.info("Launching Playwright to find NJ Clean Energy download links...")
        from playwright.sync_api import sync_playwright

        downloaded_files = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            try:
                page.goto(REPORTS_URL, timeout=60000, wait_until='networkidle')
                time.sleep(3)

                # Find xlsx download links on the page
                links = page.query_selector_all('a[href]')
                download_links = {}

                for link in links:
                    href = link.get_attribute('href') or ''
                    text = ''.join(c for c in (link.inner_text() or '') if ord(c) < 128).strip().lower()

                    if '.xlsx' in href or '/files/' in href:
                        full_url = href if href.startswith('http') else f"https://cleanenergy.nj.gov{href}"
                        # Identify which file this is
                        if 'installation data' in text:
                            download_links['installation'] = full_url
                        elif 'pipeline data' in text:
                            download_links['pipeline'] = full_url
                        elif 'installation report' in text:
                            download_links['installation_report'] = full_url
                        elif 'pipeline report' in text:
                            download_links['pipeline_report'] = full_url
                        elif 'equipment installation' in text:
                            download_links['equipment_installation'] = full_url
                        elif 'equipment pipeline' in text:
                            download_links['equipment_pipeline'] = full_url
                        logger.info(f"  Found: [{text}] → {full_url[:80]}")

                if not download_links:
                    logger.warning("No download links found on page")
                    screenshot = self.cache_dir / 'debug_screenshot.png'
                    page.screenshot(path=str(screenshot), full_page=True)
                    logger.info(f"Screenshot saved to {screenshot}")
                    browser.close()
                    # Fall back to cached files
                    return sorted(self.cache_dir.glob('nj_*.xlsx'))

                # Download the key files: Installation Data and Pipeline Data
                for key in ['installation', 'pipeline']:
                    if key not in download_links:
                        logger.warning(f"  Missing {key} link")
                        continue

                    url = download_links[key]
                    filename = f"nj_{key}_data.xlsx"
                    dest = self.cache_dir / filename

                    logger.info(f"  Downloading {filename}...")
                    try:
                        response = page.request.get(url, timeout=180000)
                        if response.ok:
                            dest.write_bytes(response.body())
                            downloaded_files.append(dest)
                            logger.info(f"    Saved: {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
                        else:
                            logger.error(f"    HTTP {response.status}")
                    except Exception as e:
                        logger.error(f"    Download failed: {e}")

            except Exception as e:
                logger.error(f"Browser scraping failed: {e}")
            finally:
                browser.close()

        if not downloaded_files:
            cached = sorted(self.cache_dir.glob('nj_*.xlsx'))
            if cached:
                logger.info(f"Using {len(cached)} stale cached files")
                return cached

        return downloaded_files

    def load_files(self, files: List[Path]) -> pd.DataFrame:
        """Load and combine all sheets from downloaded Excel files."""
        all_dfs = []

        for f in files:
            try:
                xl = pd.ExcelFile(f)
                for sheet in xl.sheet_names:
                    df = pd.read_excel(xl, sheet_name=sheet)
                    if len(df) < 2:
                        logger.info(f"  Skipping {f.name}/{sheet}: only {len(df)} rows")
                        continue

                    # Fix CSEP sheets where Project Number and Program Name are swapped
                    if 'CSEP' in sheet:
                        if 'Program Name' in df.columns and 'Project Number' in df.columns:
                            first_pn = str(df['Program Name'].iloc[0])
                            if first_pn.startswith('NJ'):  # Project numbers start with NJ
                                df = df.rename(columns={
                                    'Program Name': 'Project Number',
                                    'Project Number': 'Program Name'
                                })

                    # Determine if installed or pipeline
                    is_pipeline = 'PIPELINE' in sheet.upper()
                    df['_sheet'] = sheet
                    df['_is_pipeline'] = is_pipeline
                    df['_source_file'] = f.name

                    all_dfs.append(df)
                    logger.info(f"  Loaded {f.name}/{sheet}: {len(df):,} rows")

            except Exception as e:
                logger.warning(f"  Failed to read {f.name}: {e}")

        if not all_dfs:
            return pd.DataFrame()

        combined = pd.concat(all_dfs, ignore_index=True, sort=False)
        logger.info(f"Combined: {len(combined):,} rows from {len(all_dfs)} sheets")
        return combined

    def normalize(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Normalize raw NJ data to dg.db projects schema."""
        logger.info(f"Normalizing {len(raw_df):,} raw records...")
        records = []

        for _, row in raw_df.iterrows():
            # Project number is the unique ID
            project_number = str(row.get('Project Number', '')).strip()
            if not project_number or project_number == 'nan':
                continue

            queue_id = project_number  # Already prefixed with NJ program code

            # Capacity (kW)
            capacity_kw = None
            raw_cap = row.get('Calculated Total System Size')
            if raw_cap and not pd.isna(raw_cap):
                try:
                    capacity_kw = float(raw_cap)
                except (ValueError, TypeError):
                    pass
            capacity_mw = capacity_kw / 1000.0 if capacity_kw else None

            # Status
            raw_status = str(row.get('Status', '')).strip()
            if raw_status == 'nan':
                raw_status = ''
            status = STATUS_MAP.get(raw_status, raw_status if raw_status else 'Unknown')
            # Pipeline projects default to Active
            if row.get('_is_pipeline') and status == 'Unknown':
                status = 'Active'

            # County (numeric code → name)
            county = None
            county_code = row.get('County Code')
            if county_code and not pd.isna(county_code):
                try:
                    county = NJ_COUNTY_CODES.get(int(county_code))
                except (ValueError, TypeError):
                    county = str(county_code)

            # City
            city = str(row.get('Premise City', '')).strip()
            if city == 'nan':
                city = None

            # Utility
            utility = str(row.get('Electric Utility Name', '')).strip()
            if utility == 'nan':
                utility = None

            # Customer sector
            raw_sector = str(row.get('Customer Type', '')).strip()
            customer_sector = SECTOR_MAP.get(raw_sector, raw_sector if raw_sector != 'nan' else None)

            # Dates
            queue_date = self._parse_date(row.get('Registration Received Date'))
            # COD = PTO Date (Permission to Operate) for installed, else Registration Completed Date
            cod = self._parse_date(row.get('PTO Date')) or self._parse_date(row.get('Registration Completed Date'))

            # Installer
            installer = str(row.get('Contractor Company', '')).strip()
            if installer == 'nan':
                installer = None

            # Developer/Company
            developer = str(row.get('Premise Company', '')).strip()
            if developer == 'nan':
                developer = ''

            # Program
            program = str(row.get('Program Name', '')).strip()
            if program == 'nan':
                program = None

            # Interconnection type
            interconnection = str(row.get('Interconnection Type', '')).strip()
            if interconnection == 'nan':
                interconnection = program

            record = {
                'queue_id': queue_id,
                'region': REGION,
                'name': None,
                'developer': developer,
                'capacity_mw': capacity_mw,
                'capacity_kw': capacity_kw,
                'type': 'Solar',
                'status': status,
                'state': 'NJ',
                'county': county,
                'city': city,
                'utility': utility,
                'queue_date': queue_date,
                'cod': cod,
                'customer_sector': customer_sector,
                'system_size_dc_kw': capacity_kw,
                'installer': installer,
                'total_system_cost': None,
                'interconnection_program': interconnection,
            }
            record['row_hash'] = compute_hash(record)
            records.append(record)

        df = pd.DataFrame(records)

        # Deduplicate (a project could appear in both installation and pipeline files
        # if it was recently completed — keep the most recent/complete version)
        before = len(df)
        df = df.drop_duplicates(subset='queue_id', keep='first')
        if len(df) < before:
            logger.info(f"  Deduped: {before:,} → {len(df):,} ({before - len(df):,} duplicates)")

        logger.info(f"  Normalized {len(df):,} records")
        if not df.empty:
            logger.info(f"  Statuses: {df['status'].value_counts().to_dict()}")
            if df['capacity_kw'].notna().any():
                logger.info(f"  Capacity range: {df['capacity_kw'].min():.1f} - {df['capacity_kw'].max():.1f} kW")
            logger.info(f"  Utilities: {df['utility'].nunique()} unique")
            logger.info(f"  Counties: {df['county'].nunique()} unique")
            logger.info(f"  Installers: {df['installer'].nunique()} unique")

        return df

    def store(self, df: pd.DataFrame) -> Dict[str, int]:
        """Upsert normalized records into dg.db."""
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
                                system_size_dc_kw = ?, installer = ?,
                                total_system_cost = ?,
                                interconnection_program = ?, row_hash = ?,
                                source = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', (
                            row_dict.get('capacity_mw'), row_dict.get('capacity_kw'),
                            'Solar', row_dict.get('status'),
                            'NJ', row_dict.get('county'), row_dict.get('city'),
                            row_dict.get('utility'),
                            row_dict.get('queue_date'), row_dict.get('cod'),
                            row_dict.get('customer_sector'),
                            row_dict.get('system_size_dc_kw'), row_dict.get('installer'),
                            row_dict.get('total_system_cost'),
                            row_dict.get('interconnection_program'), row_hash,
                            SOURCE,
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
                            customer_sector, system_size_dc_kw, installer,
                            total_system_cost, interconnection_program, row_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        queue_id, REGION, row_dict.get('name'), row_dict.get('developer', ''),
                        row_dict.get('capacity_mw'), row_dict.get('capacity_kw'),
                        'Solar', row_dict.get('status'),
                        'NJ', row_dict.get('county'), row_dict.get('city'),
                        row_dict.get('utility'),
                        row_dict.get('queue_date'), row_dict.get('cod'),
                        SOURCE, SOURCE, sources_json,
                        row_dict.get('customer_sector'), row_dict.get('system_size_dc_kw'),
                        row_dict.get('installer'),
                        row_dict.get('total_system_cost'),
                        row_dict.get('interconnection_program'), row_hash
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

        # Update dg_programs registry
        cursor.execute('''
            INSERT OR REPLACE INTO dg_programs (
                program_key, program_name, state, utility, source_url,
                refresh_method, last_refreshed, record_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            SOURCE, 'NJ Clean Energy Solar Programs (SRP/TI/ADI/CSEP/CSI/RNM)', 'NJ', 'Multiple',
            REPORTS_URL,
            'playwright_scraper', datetime.now().isoformat(),
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

    def load(self, use_cache: bool = True, dry_run: bool = False,
             download_only: bool = False, load_only: bool = False) -> Dict[str, int]:
        """Full pipeline: download → load files → normalize → store."""
        if load_only:
            files = sorted(self.cache_dir.glob('nj_*.xlsx'))
            if not files:
                logger.error("No cached files to load")
                return {'added': 0, 'updated': 0, 'unchanged': 0, 'errors': 0}
        else:
            files = self.download(use_cache=use_cache)

        if download_only:
            return {'files_downloaded': len(files)}

        if not files:
            logger.error("No files available")
            return {'added': 0, 'updated': 0, 'unchanged': 0, 'errors': 0}

        raw_df = self.load_files(files)
        if raw_df.empty:
            return {'added': 0, 'updated': 0, 'unchanged': 0, 'errors': 0}

        self.df = self.normalize(raw_df)

        if dry_run:
            logger.info("DRY RUN — skipping database write")
            return {'would_process': len(self.df)}

        return self.store(self.df)

    @staticmethod
    def _parse_date(val) -> Optional[str]:
        if not val or (isinstance(val, float) and pd.isna(val)):
            return None
        if isinstance(val, pd.Timestamp):
            return val.strftime('%m/%d/%Y')
        val_str = str(val).strip()
        if not val_str or val_str.lower() in ('nan', 'none', 'nat'):
            return None
        for fmt in ['%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d',
                     '%m/%d/%Y', '%m/%d/%y']:
            try:
                return datetime.strptime(val_str[:19], fmt).strftime('%m/%d/%Y')
            except ValueError:
                continue
        return val_str[:10]


def main():
    import argparse
    parser = argparse.ArgumentParser(description="NJ Clean Energy DG Scraper")
    parser.add_argument('--stats', action='store_true', help='Show statistics')
    parser.add_argument('--no-cache', action='store_true', help='Skip cache')
    parser.add_argument('--dry-run', action='store_true', help='No DB write')
    parser.add_argument('--download-only', action='store_true', help='Just download files')
    parser.add_argument('--load-only', action='store_true', help='Load from cached files only')
    parser.add_argument('--db', type=str, help='Override DB path')
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else None
    scraper = NJDGScraper(db_path=db_path)

    print(f"\n{'='*60}")
    print(f"NJ Clean Energy DG Scraper — source: {SOURCE}")
    print(f"Target DB: {scraper.db_path}")
    print(f"{'='*60}\n")

    results = scraper.load(
        use_cache=not args.no_cache,
        dry_run=args.dry_run,
        download_only=args.download_only,
        load_only=args.load_only
    )

    print(f"\n{'='*60}")
    if 'added' in results:
        print(f"Results: +{results.get('added', 0):,} added, "
              f"~{results.get('updated', 0):,} updated, "
              f"={results.get('unchanged', 0):,} unchanged, "
              f"!{results.get('errors', 0)} errors")
    else:
        print(f"Results: {results}")
    print(f"{'='*60}")

    if args.stats and scraper.df is not None and not scraper.df.empty:
        df = scraper.df
        print(f"\nTotal records: {len(df):,}")
        cap = df['capacity_mw'].sum()
        print(f"Total capacity: {cap:,.1f} MW ({cap/1000:.2f} GW)")
        print(f"\nBy Status:")
        for k, v in df['status'].value_counts().items():
            print(f"  {k}: {v:,}")
        print(f"\nBy Utility (top 10):")
        for k, v in df['utility'].value_counts().head(10).items():
            print(f"  {k}: {v:,}")
        print(f"\nBy County (top 10):")
        for k, v in df['county'].value_counts().head(10).items():
            print(f"  {k}: {v:,}")
        print(f"\nBy Sector:")
        for k, v in df['customer_sector'].value_counts().items():
            print(f"  {k}: {v:,}")
        if df['capacity_kw'].notna().any():
            print(f"\nCapacity: min={df['capacity_kw'].min():.1f} kW, "
                  f"median={df['capacity_kw'].median():.1f} kW, "
                  f"max={df['capacity_kw'].max():.1f} kW")


if __name__ == '__main__':
    main()
