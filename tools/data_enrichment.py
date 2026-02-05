#!/usr/bin/env python3
"""
Data Enrichment Module

Provides multi-source data enrichment for interconnection queue projects,
focusing on developer/company name enrichment where ISO APIs don't provide it.

Data Sources:
1. EIA Form 860 - Operational generator ownership data
2. FERC eLibrary - Interconnection agreements with developer names
3. Interconnection.fyi - Commercial data provider (manual import)
4. MISO Interactive Queue - Web scraping for additional details
5. Manual Enrichment - CSV-based manual data entry

Architecture:
- EnrichmentStore: SQLite-based storage for enriched data
- Matchers: Logic to match enriched data to queue projects
- Scrapers: Web scraping modules for each source
- Importers: Bulk import from external data files

Usage:
    from data_enrichment import DataEnrichment

    enricher = DataEnrichment()
    enricher.enrich_from_eia()
    enricher.enrich_from_ferc(docket_prefix="ER")

    # Apply enrichments to queue data
    df = enricher.apply_enrichments(queue_df)
"""

import pandas as pd
import numpy as np
import sqlite3
import requests
import json
import re
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths
DATA_DIR = Path(__file__).parent / '.data'
ENRICHMENT_DB = DATA_DIR / 'enrichment.db'
CACHE_DIR = Path(__file__).parent / '.cache'


@dataclass
class EnrichmentMatch:
    """Represents a potential enrichment match."""
    queue_id: str
    iso: str
    source: str
    developer: str
    confidence: float  # 0-1 confidence score
    match_method: str  # How we matched (queue_id, name, location, etc.)
    source_data: Dict[str, Any]


class EnrichmentStore:
    """SQLite-based storage for enrichment data and matches."""

    def __init__(self, db_path: Path = None):
        self.db_path = db_path or ENRICHMENT_DB
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Initialize enrichment database schema."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Developer registry - canonical developer names
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS developers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                aliases TEXT,  -- JSON array of alternate names
                parent_company TEXT,
                website TEXT,
                hq_state TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Enrichment data from various sources
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS enrichments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id TEXT NOT NULL,
                iso TEXT NOT NULL,
                source TEXT NOT NULL,  -- eia, ferc, manual, interconnection_fyi, etc.
                developer TEXT,
                developer_id INTEGER,
                confidence REAL DEFAULT 1.0,
                match_method TEXT,
                source_data TEXT,  -- JSON blob of original source data
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                verified BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (developer_id) REFERENCES developers(id),
                UNIQUE(queue_id, iso, source)
            )
        ''')

        # EIA 860 data cache
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS eia_plants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plant_id TEXT NOT NULL,
                plant_name TEXT,
                utility_id TEXT,
                utility_name TEXT,
                operator_name TEXT,
                owner_name TEXT,
                state TEXT,
                county TEXT,
                capacity_mw REAL,
                fuel_type TEXT,
                status TEXT,
                operating_year INTEGER,
                data_year INTEGER,
                UNIQUE(plant_id, data_year)
            )
        ''')

        # FERC filings cache
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ferc_filings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                docket TEXT NOT NULL,
                filing_date TEXT,
                company TEXT,
                description TEXT,
                iso TEXT,
                project_details TEXT,  -- JSON
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(docket)
            )
        ''')

        # Manual enrichment queue - projects needing manual lookup
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS manual_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id TEXT NOT NULL,
                iso TEXT NOT NULL,
                project_name TEXT,
                capacity_mw REAL,
                state TEXT,
                county TEXT,
                poi TEXT,
                priority INTEGER DEFAULT 50,  -- 1=highest, 100=lowest
                status TEXT DEFAULT 'pending',  -- pending, in_progress, completed, skipped
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(queue_id, iso)
            )
        ''')

        # Indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_enrichments_queue ON enrichments(queue_id, iso)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_eia_state ON eia_plants(state)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_manual_status ON manual_queue(status)')

        conn.commit()
        conn.close()

    def add_developer(self, name: str, aliases: List[str] = None,
                      parent_company: str = None, website: str = None,
                      hq_state: str = None) -> int:
        """Add or update a developer in the registry."""
        conn = self._get_conn()
        cursor = conn.cursor()

        aliases_json = json.dumps(aliases) if aliases else None

        cursor.execute('''
            INSERT INTO developers (name, aliases, parent_company, website, hq_state)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                aliases = COALESCE(excluded.aliases, developers.aliases),
                parent_company = COALESCE(excluded.parent_company, developers.parent_company),
                website = COALESCE(excluded.website, developers.website),
                hq_state = COALESCE(excluded.hq_state, developers.hq_state),
                updated_at = CURRENT_TIMESTAMP
        ''', (name, aliases_json, parent_company, website, hq_state))

        dev_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return dev_id

    def add_enrichment(self, queue_id: str, iso: str, source: str,
                       developer: str, confidence: float = 1.0,
                       match_method: str = None, source_data: Dict = None) -> int:
        """Add an enrichment record."""
        conn = self._get_conn()
        cursor = conn.cursor()

        source_json = json.dumps(source_data, default=str) if source_data else None

        cursor.execute('''
            INSERT INTO enrichments (queue_id, iso, source, developer, confidence, match_method, source_data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(queue_id, iso, source) DO UPDATE SET
                developer = excluded.developer,
                confidence = excluded.confidence,
                match_method = excluded.match_method,
                source_data = excluded.source_data
        ''', (queue_id, iso, source, developer, confidence, match_method, source_json))

        enrich_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return enrich_id

    def get_enrichment(self, queue_id: str, iso: str) -> Optional[Dict]:
        """Get the best enrichment for a project (highest confidence)."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM enrichments
            WHERE queue_id = ? AND iso = ?
            ORDER BY confidence DESC, verified DESC
            LIMIT 1
        ''', (queue_id, iso))

        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return None

    def get_all_enrichments(self) -> pd.DataFrame:
        """Get all enrichments as a DataFrame."""
        conn = self._get_conn()
        df = pd.read_sql_query('SELECT * FROM enrichments', conn)
        conn.close()
        return df

    def add_to_manual_queue(self, queue_id: str, iso: str, project_name: str = None,
                            capacity_mw: float = None, state: str = None,
                            county: str = None, poi: str = None, priority: int = 50):
        """Add a project to the manual enrichment queue."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO manual_queue (queue_id, iso, project_name, capacity_mw, state, county, poi, priority)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(queue_id, iso) DO UPDATE SET
                project_name = COALESCE(excluded.project_name, manual_queue.project_name),
                capacity_mw = COALESCE(excluded.capacity_mw, manual_queue.capacity_mw),
                state = COALESCE(excluded.state, manual_queue.state),
                county = COALESCE(excluded.county, manual_queue.county),
                poi = COALESCE(excluded.poi, manual_queue.poi),
                priority = excluded.priority
        ''', (queue_id, iso, project_name, capacity_mw, state, county, poi, priority))

        conn.commit()
        conn.close()

    def get_manual_queue(self, status: str = 'pending', limit: int = 100) -> pd.DataFrame:
        """Get projects in the manual enrichment queue."""
        conn = self._get_conn()
        df = pd.read_sql_query('''
            SELECT * FROM manual_queue
            WHERE status = ?
            ORDER BY priority ASC, capacity_mw DESC
            LIMIT ?
        ''', conn, params=[status, limit])
        conn.close()
        return df

    def get_stats(self) -> Dict[str, Any]:
        """Get enrichment statistics."""
        conn = self._get_conn()
        cursor = conn.cursor()

        stats = {}

        cursor.execute('SELECT COUNT(*) FROM enrichments')
        stats['total_enrichments'] = cursor.fetchone()[0]

        cursor.execute('SELECT source, COUNT(*) as cnt FROM enrichments GROUP BY source')
        stats['by_source'] = {row['source']: row['cnt'] for row in cursor.fetchall()}

        cursor.execute('SELECT iso, COUNT(*) as cnt FROM enrichments GROUP BY iso')
        stats['by_iso'] = {row['iso']: row['cnt'] for row in cursor.fetchall()}

        cursor.execute('SELECT COUNT(*) FROM manual_queue WHERE status = "pending"')
        stats['pending_manual'] = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM developers')
        stats['developers_in_registry'] = cursor.fetchone()[0]

        conn.close()
        return stats


class EIAEnricher:
    """Enrichment from EIA Form 860 data via GridStatus API."""

    def __init__(self, store: EnrichmentStore, api_key: str = None):
        self.store = store
        self.api_key = api_key
        self.cache_dir = CACHE_DIR / 'eia'
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._client = None

    @property
    def client(self):
        """Lazy load GridStatus client."""
        if self._client is None and self.api_key:
            try:
                from gridstatusio import GridStatusClient
                self._client = GridStatusClient(api_key=self.api_key)
            except ImportError:
                logger.warning("gridstatusio not installed. Install with: pip install gridstatusio")
        return self._client

    def load_eia_operating_generators(self, limit: int = 50000) -> pd.DataFrame:
        """Load EIA operating generator inventory via GridStatus API."""
        if not self.client:
            logger.warning("GridStatus API key not configured")
            return pd.DataFrame()

        cache_file = self.cache_dir / 'eia_operating_generators.parquet'

        # Use cache if less than 7 days old
        if cache_file.exists():
            cache_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            if cache_age < timedelta(days=7):
                logger.info(f"Using cached EIA operating generators ({cache_age.days} days old)")
                return pd.read_parquet(cache_file)

        try:
            logger.info(f"Fetching EIA operating generators from GridStatus API (limit={limit})...")
            df = self.client.get_dataset('eia_monthly_generator_inventory_operating', limit=limit)
            logger.info(f"Loaded {len(df):,} operating generator records")

            # Cache the data
            df.to_parquet(cache_file)
            return df
        except Exception as e:
            logger.error(f"Failed to load EIA operating generators: {e}")
            return pd.DataFrame()

    def load_eia_planned_generators(self, limit: int = 50000) -> pd.DataFrame:
        """Load EIA planned generator inventory via GridStatus API."""
        if not self.client:
            logger.warning("GridStatus API key not configured")
            return pd.DataFrame()

        cache_file = self.cache_dir / 'eia_planned_generators.parquet'

        # Use cache if less than 7 days old
        if cache_file.exists():
            cache_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            if cache_age < timedelta(days=7):
                logger.info(f"Using cached EIA planned generators ({cache_age.days} days old)")
                return pd.read_parquet(cache_file)

        try:
            logger.info(f"Fetching EIA planned generators from GridStatus API (limit={limit})...")
            df = self.client.get_dataset('eia_monthly_generator_inventory_planned', limit=limit)
            logger.info(f"Loaded {len(df):,} planned generator records")

            # Cache the data
            df.to_parquet(cache_file)
            return df
        except Exception as e:
            logger.error(f"Failed to load EIA planned generators: {e}")
            return pd.DataFrame()

    def build_entity_lookup(self) -> Dict[str, List[Dict]]:
        """Build a lookup of entities by state and county for matching."""
        operating = self.load_eia_operating_generators()
        planned = self.load_eia_planned_generators()

        eia_df = pd.concat([operating, planned], ignore_index=True)

        if eia_df.empty:
            return {}

        # Build lookup by state + county
        lookup = defaultdict(list)

        for _, row in eia_df.iterrows():
            state = str(row.get('plant_state', '')).strip().upper()
            county = str(row.get('county', '')).strip().upper()
            entity = row.get('entity_name', '')

            if state and entity:
                key = f"{state}_{county}" if county else state
                lookup[key].append({
                    'entity_name': entity,
                    'plant_name': row.get('plant_name', ''),
                    'capacity': row.get('nameplate_capacity'),
                    'technology': row.get('technology', ''),
                    'plant_id': row.get('plant_id', ''),
                })

        logger.info(f"Built EIA entity lookup with {len(lookup)} location keys")
        return dict(lookup)

    def enrich_from_eia(self, queue_df: pd.DataFrame) -> int:
        """
        Enrich queue data from EIA generator inventory.

        Matches by:
        1. State + County + Capacity (high confidence)
        2. State + Capacity (medium confidence)
        """
        lookup = self.build_entity_lookup()

        if not lookup:
            return 0

        enriched_count = 0

        for _, row in queue_df.iterrows():
            queue_id = row.get('queue_id') or row.get('Queue ID')
            iso = row.get('iso') or row.get('region')
            state = str(row.get('state') or row.get('State', '')).strip().upper()
            county = str(row.get('county') or row.get('County', '')).strip().upper()
            capacity = row.get('capacity_mw') or row.get('Capacity (MW)', 0)

            if not queue_id or not state:
                continue

            # Try state + county match first
            key = f"{state}_{county}" if county else state
            candidates = lookup.get(key, [])

            # Fall back to state-only if no county match
            if not candidates and county:
                candidates = lookup.get(state, [])

            if not candidates:
                continue

            # Find best capacity match
            best_match = None
            best_diff = float('inf')

            for cand in candidates:
                cand_cap = cand.get('capacity') or 0
                if cand_cap and capacity:
                    diff = abs(cand_cap - capacity) / max(capacity, 1)
                    if diff < 0.2 and diff < best_diff:  # Within 20%
                        best_diff = diff
                        best_match = cand

            if best_match:
                confidence = 0.9 if county else 0.7
                confidence -= best_diff * 0.3  # Reduce confidence based on capacity match

                self.store.add_enrichment(
                    queue_id=str(queue_id),
                    iso=str(iso),
                    source='eia_gridstatus',
                    developer=best_match['entity_name'],
                    confidence=confidence,
                    match_method='eia_location_capacity_match',
                    source_data={
                        'plant_name': best_match.get('plant_name'),
                        'plant_id': best_match.get('plant_id'),
                        'eia_capacity': best_match.get('capacity'),
                        'technology': best_match.get('technology'),
                    }
                )
                enriched_count += 1

        logger.info(f"Enriched {enriched_count} projects from EIA data")
        return enriched_count


class FERCEnricher:
    """Enrichment from FERC eLibrary filings using Playwright browser automation."""

    FERC_SEARCH_URL = "https://elibrary.ferc.gov/eLibrary/search"

    def __init__(self, store: EnrichmentStore):
        self.store = store
        self.cache_dir = CACHE_DIR / 'ferc'
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def scrape_interconnection_filings(self, date_from: str = None, max_pages: int = 50) -> pd.DataFrame:
        """
        Scrape FERC eLibrary for interconnection agreement filings.

        Args:
            date_from: Start date in MM/DD/YYYY format (default: 2 years ago)
            max_pages: Maximum number of result pages to scrape

        Returns:
            DataFrame with filing data including dockets, filers, and descriptions
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error("Playwright not installed. Install with: pip install playwright && playwright install chromium")
            return pd.DataFrame()

        # Check cache first
        cache_file = self.cache_dir / 'ferc_interconnection_filings.parquet'
        if cache_file.exists():
            cache_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            if cache_age < timedelta(days=7):
                logger.info(f"Using cached FERC data ({cache_age.days} days old)")
                return pd.read_parquet(cache_file)

        if not date_from:
            date_from = (datetime.now() - timedelta(days=730)).strftime('%m/%d/%Y')

        logger.info(f"Scraping FERC eLibrary for interconnection filings since {date_from}...")

        all_filings = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            try:
                # Load search page
                page.goto(self.FERC_SEARCH_URL, timeout=30000)
                page.wait_for_load_state('networkidle', timeout=30000)
                import time
                time.sleep(2)

                # Fill search form
                text_input = page.query_selector('input[name="textsearch"]')
                if text_input:
                    text_input.fill('generator interconnection agreement')

                date_input = page.query_selector('input[name="dFROM"]')
                if date_input:
                    date_input.fill(date_from)

                # Submit search
                search_btn = page.query_selector('button[type="submit"], input[type="submit"]')
                if not search_btn:
                    # Try finding by text
                    search_btn = page.query_selector('button:has-text("Search")')

                if search_btn:
                    search_btn.click()
                    time.sleep(5)

                    # Scrape results pages
                    for page_num in range(max_pages):
                        filings = self._parse_results_page(page)
                        if not filings:
                            break

                        all_filings.extend(filings)
                        logger.info(f"Page {page_num + 1}: scraped {len(filings)} filings (total: {len(all_filings)})")

                        # Look for next page button
                        next_btn = page.query_selector('button:has-text("Next"), a:has-text("Next"), .pagination-next')
                        if next_btn and next_btn.is_visible():
                            next_btn.click()
                            time.sleep(3)
                        else:
                            break

            except Exception as e:
                logger.error(f"Error scraping FERC: {e}")
            finally:
                browser.close()

        if all_filings:
            df = pd.DataFrame(all_filings)
            logger.info(f"Scraped {len(df)} total FERC filings")

            # Extract developer names from descriptions
            df['developer'] = df['description'].apply(self._extract_developer_from_description)

            # Cache results
            df.to_parquet(cache_file)
            return df

        return pd.DataFrame()

    def _parse_results_page(self, page) -> List[Dict]:
        """Parse a single results page."""
        filings = []

        rows = page.query_selector_all('table tr')
        for row in rows[1:]:  # Skip header
            cells = row.query_selector_all('td')
            if len(cells) >= 5:
                try:
                    filing = {
                        'category': cells[0].inner_text().strip() if cells[0] else '',
                        'accession': cells[1].inner_text().strip() if cells[1] else '',
                        'filed_date': cells[2].inner_text().strip() if cells[2] else '',
                        'docket': cells[4].inner_text().strip() if cells[4] else '',
                        'description': cells[5].inner_text().strip() if len(cells) > 5 and cells[5] else '',
                    }

                    # Only keep relevant filings
                    if filing['docket'] and filing['docket'].startswith('ER'):
                        filings.append(filing)
                except Exception:
                    continue

        return filings

    def _extract_developer_from_description(self, description: str) -> Optional[str]:
        """Extract developer/filer name from FERC filing description."""
        if not description:
            return None

        # Common patterns in FERC interconnection filings:
        # "Company Name submits tariff filing..."
        # "Company Name, LLC submits..."

        patterns = [
            # "Company submits tariff filing"
            r'^([A-Z][A-Za-z0-9\s,\.&\'-]+(?:LLC|L\.L\.C\.|LP|L\.P\.|Inc\.?|Corp\.?|Company|Co\.?|Ltd\.?))\s+submits',
            # "Company, LLC submits"
            r'^([A-Z][A-Za-z0-9\s]+,?\s*(?:LLC|LP|Inc|Corp)\.?)\s+submits',
            # Just the first entity before "submits"
            r'^([A-Z][A-Za-z0-9\s,\.&\'-]{5,50})\s+submits',
        ]

        for pattern in patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                developer = match.group(1).strip().rstrip(',.')
                # Only filter out ISOs/RTOs (not utilities, as many are also developers)
                skip_terms = [
                    # ISOs/RTOs only
                    'Southwest Power Pool', 'Midcontinent Independent System Operator',
                    'PJM Interconnection', 'California Independent System Operator',
                    'New York Independent System Operator', 'ISO New England',
                    'Electric Reliability Council of Texas',
                ]
                if not any(skip.lower() in developer.lower() for skip in skip_terms):
                    return developer

        return None

    def enrich_from_ferc(self, queue_df: pd.DataFrame) -> int:
        """
        Enrich queue data from FERC eLibrary filings.

        Matches filings to queue projects by company name similarity.
        """
        df = self.scrape_interconnection_filings()

        if df.empty:
            return 0

        # Get filings with extracted developers
        dev_df = df[df['developer'].notna()].copy()
        if dev_df.empty:
            logger.warning("No developers extracted from FERC filings")
            return 0

        logger.info(f"Found {len(dev_df)} FERC filings with developer names")

        # Build developer lookup
        dev_lookup = {}
        for _, row in dev_df.iterrows():
            dev = row['developer']
            key = dev.upper().strip()
            dev_lookup[key] = {
                'developer': dev,
                'docket': row['docket'],
                'description': row['description'][:200],
            }

        enriched = 0

        # Match queue projects to FERC developers
        for _, q_row in queue_df.iterrows():
            queue_id = q_row.get('queue_id') or q_row.get('Queue ID')
            iso = q_row.get('iso', 'UNKNOWN')
            project_name = str(q_row.get('name') or q_row.get('Project Name', '')).upper()

            if not queue_id or not project_name:
                continue

            # Try to match project name to FERC developer
            for ferc_key, ferc_data in dev_lookup.items():
                # Word-based matching
                ferc_words = set(re.findall(r'[A-Z][A-Z]+', ferc_key))
                proj_words = set(re.findall(r'[A-Z][A-Z]+', project_name))

                # Remove common words
                common_skip = {'WIND', 'SOLAR', 'ENERGY', 'STORAGE', 'BESS', 'LLC', 'LP', 'INC', 'CORP',
                              'PROJECT', 'FARM', 'POWER', 'GENERATION', 'GENERATING'}
                ferc_words = ferc_words - common_skip
                proj_words = proj_words - common_skip

                overlap = ferc_words & proj_words
                if len(overlap) >= 2 or (len(overlap) == 1 and any(len(w) > 4 for w in overlap)):
                    self.store.add_enrichment(
                        queue_id=str(queue_id),
                        iso=iso,
                        source='ferc_elibrary',
                        developer=ferc_data['developer'],
                        confidence=0.75,
                        match_method='ferc_name_match',
                        source_data={
                            'ferc_docket': ferc_data['docket'],
                            'ferc_description': ferc_data['description'],
                        }
                    )
                    enriched += 1
                    break

        logger.info(f"Enriched {enriched} projects from FERC eLibrary")
        return enriched

    def import_ferc_csv(self, csv_path: Path) -> int:
        """
        Import FERC search results from manually exported CSV.

        Steps to export:
        1. Go to https://elibrary.ferc.gov/eLibrary/search
        2. Search for interconnection agreements (e.g., description contains "interconnection")
        3. Export results to CSV
        4. Import here
        """
        if not csv_path.exists():
            logger.error(f"FERC CSV not found: {csv_path}")
            return 0

        df = pd.read_csv(csv_path)
        imported = 0

        for _, row in df.iterrows():
            docket = row.get('Docket')
            company = row.get('Company') or row.get('Filer')
            description = row.get('Description', '')

            if docket and company:
                conn = self.store._get_conn()
                cursor = conn.cursor()

                cursor.execute('''
                    INSERT OR REPLACE INTO ferc_filings (docket, company, description, filing_date)
                    VALUES (?, ?, ?, ?)
                ''', (docket, company, description, row.get('Filing Date')))

                conn.commit()
                conn.close()
                imported += 1

        logger.info(f"Imported {imported} FERC filings")
        return imported


class ManualEnricher:
    """Manual enrichment workflow."""

    def __init__(self, store: EnrichmentStore):
        self.store = store

    def populate_manual_queue(self, queue_df: pd.DataFrame) -> int:
        """
        Add projects missing developer data to the manual enrichment queue.

        Prioritizes by:
        1. Capacity (larger projects first)
        2. ISO (MISO, SPP first as they have most missing data)
        """
        added = 0

        # Check which projects already have enrichments
        existing = self.store.get_all_enrichments()
        existing_keys = set(zip(existing['queue_id'].astype(str), existing['iso'].astype(str)))

        for _, row in queue_df.iterrows():
            queue_id = str(row.get('queue_id') or row.get('Queue ID', ''))
            iso = str(row.get('iso') or row.get('region', ''))
            developer = row.get('developer') or row.get('Developer')

            # Skip if we already have a developer
            if developer and str(developer).lower() not in ['none', 'nan', '', 'unknown']:
                continue

            # Skip if already enriched
            if (queue_id, iso) in existing_keys:
                continue

            # Calculate priority (1-100, lower is higher priority)
            capacity = row.get('capacity_mw') or row.get('Capacity (MW)', 0)

            # Base priority on capacity
            if capacity >= 500:
                priority = 10
            elif capacity >= 200:
                priority = 20
            elif capacity >= 100:
                priority = 30
            elif capacity >= 50:
                priority = 50
            else:
                priority = 70

            # Boost priority for ISOs with most missing data
            if iso.upper() in ['MISO', 'SPP']:
                priority = max(1, priority - 10)

            self.store.add_to_manual_queue(
                queue_id=queue_id,
                iso=iso,
                project_name=row.get('name') or row.get('Project Name'),
                capacity_mw=capacity,
                state=row.get('state') or row.get('State'),
                county=row.get('county') or row.get('County'),
                poi=row.get('poi') or row.get('Interconnection Location'),
                priority=priority
            )
            added += 1

        logger.info(f"Added {added} projects to manual enrichment queue")
        return added

    def export_for_research(self, output_path: Path = None, limit: int = 500) -> Path:
        """
        Export projects needing research to CSV for manual lookup.
        """
        output_path = output_path or (DATA_DIR / 'manual_research_queue.csv')

        df = self.store.get_manual_queue(status='pending', limit=limit)

        # Add helpful columns for research
        df['search_query'] = df.apply(
            lambda r: f"{r['project_name']} {r['state']} interconnection"
            if r['project_name'] else f"{r['queue_id']} {r['iso']} interconnection",
            axis=1
        )
        df['ferc_search_url'] = 'https://elibrary.ferc.gov/eLibrary/search'
        df['developer_found'] = ''
        df['source'] = ''
        df['notes'] = ''

        df.to_csv(output_path, index=False)
        logger.info(f"Exported {len(df)} projects to {output_path}")

        return output_path

    def import_research_results(self, csv_path: Path) -> int:
        """
        Import manually researched developer data from CSV.

        Expected columns: queue_id, iso, developer_found, source, confidence
        """
        if not csv_path.exists():
            logger.error(f"Research results CSV not found: {csv_path}")
            return 0

        df = pd.read_csv(csv_path)
        imported = 0

        for _, row in df.iterrows():
            queue_id = str(row.get('queue_id', ''))
            iso = str(row.get('iso', ''))
            developer = row.get('developer_found', '')
            source = row.get('source', 'manual')
            confidence = row.get('confidence', 0.9)

            if not queue_id or not iso or not developer:
                continue

            if str(developer).lower() in ['', 'nan', 'none']:
                continue

            self.store.add_enrichment(
                queue_id=queue_id,
                iso=iso,
                source=f'manual_{source}',
                developer=developer,
                confidence=float(confidence),
                match_method='manual_research',
                source_data={'notes': row.get('notes', '')}
            )

            # Update manual queue status
            conn = self.store._get_conn()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE manual_queue
                SET status = 'completed', notes = ?
                WHERE queue_id = ? AND iso = ?
            ''', (row.get('notes', ''), queue_id, iso))
            conn.commit()
            conn.close()

            imported += 1

        logger.info(f"Imported {imported} manual enrichments")
        return imported


class InterconnectionFyiImporter:
    """Import data from Interconnection.fyi exports."""

    def __init__(self, store: EnrichmentStore):
        self.store = store

    def import_csv(self, csv_path: Path) -> int:
        """
        Import developer data from Interconnection.fyi CSV export.

        Contact: interconnection.fyi for data access
        """
        if not csv_path.exists():
            logger.error(f"Interconnection.fyi CSV not found: {csv_path}")
            return 0

        df = pd.read_csv(csv_path)
        imported = 0

        # Column names may vary - try common variations
        queue_id_cols = ['Queue ID', 'queue_id', 'Project ID', 'Request ID']
        developer_cols = ['Developer', 'Company', 'Applicant', 'Owner']
        iso_cols = ['ISO', 'RTO', 'Market', 'Region']

        queue_id_col = next((c for c in queue_id_cols if c in df.columns), None)
        developer_col = next((c for c in developer_cols if c in df.columns), None)
        iso_col = next((c for c in iso_cols if c in df.columns), None)

        if not queue_id_col or not developer_col:
            logger.error("Required columns not found in Interconnection.fyi CSV")
            logger.info(f"Available columns: {list(df.columns)}")
            return 0

        for _, row in df.iterrows():
            queue_id = str(row[queue_id_col])
            developer = row[developer_col]
            iso = row[iso_col] if iso_col else 'UNKNOWN'

            if not queue_id or not developer or str(developer).lower() in ['nan', 'none', '']:
                continue

            self.store.add_enrichment(
                queue_id=queue_id,
                iso=str(iso),
                source='interconnection_fyi',
                developer=str(developer),
                confidence=0.95,  # Commercial data source, high confidence
                match_method='direct_import',
                source_data={'source_file': str(csv_path)}
            )
            imported += 1

        logger.info(f"Imported {imported} enrichments from Interconnection.fyi")
        return imported


class StatePUCScraper:
    """Scrape interconnection agreements from State PUC filings."""

    # State PUC URLs
    TEXAS_PUC_URL = "https://interchange.puc.texas.gov/Search/Filings"
    TEXAS_ERCOT_DOCKET = "35077"  # ERCOT Interconnection Agreements docket

    def __init__(self, store: EnrichmentStore):
        self.store = store
        self.cache_dir = CACHE_DIR / 'puc'
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def scrape_texas_puc(self) -> pd.DataFrame:
        """
        Scrape Texas PUC ERCOT interconnection agreements.

        Returns DataFrame with developer names and project info.
        """
        import re
        from bs4 import BeautifulSoup

        cache_file = self.cache_dir / 'texas_puc_filings.parquet'

        # Use cache if less than 7 days old
        if cache_file.exists():
            cache_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            if cache_age < timedelta(days=7):
                logger.info(f"Using cached Texas PUC data ({cache_age.days} days old)")
                return pd.read_parquet(cache_file)

        logger.info("Scraping Texas PUC ERCOT interconnection filings...")

        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
        params = {'ControlNumber': self.TEXAS_ERCOT_DOCKET}

        try:
            response = requests.get(self.TEXAS_PUC_URL, params=params, headers=headers, timeout=60)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table')

            if not table:
                logger.error("Could not find filings table on Texas PUC page")
                return pd.DataFrame()

            rows = table.find_all('tr')
            logger.info(f"Found {len(rows)} filings")

            filings = []
            for row in rows[1:]:  # Skip header
                cells = row.find_all('td')
                if len(cells) >= 5:
                    filing = {
                        'item': cells[0].get_text(strip=True),
                        'date': cells[1].get_text(strip=True),
                        'party': cells[2].get_text(strip=True),
                        'type': cells[3].get_text(strip=True),
                        'description': cells[4].get_text(strip=True),
                    }

                    # Extract developer name from description
                    desc = filing['description']

                    # Pattern to extract developer names - focus on project/company names
                    # Look for patterns like "with X LLC" or "and X, LLC"
                    patterns = [
                        # "Agreement with/between/and COMPANY NAME LLC/LP/Inc"
                        r'(?:with|between)\s+([A-Z][A-Za-z0-9\s,\.&\'-]+?(?:LLC|L\.L\.C\.|LP|L\.P\.|Inc\.?|Corp\.?|Company|Ltd\.?))',
                        # "COMPANY Solar/Wind/Energy/Storage/BESS Project/LLC/LP"
                        r'\b([A-Z][A-Za-z0-9\s]+?(?:Solar|Wind|Energy|Storage|BESS)\s*(?:LLC|LP|Inc|Project|I{1,3})?)\b',
                        # "for the PROJECT NAME Project"
                        r'for\s+(?:the\s+)?([A-Z][A-Za-z0-9\s]+?(?:Solar|Wind|Storage|BESS|Energy)\s*(?:Project|Farm)?)',
                    ]

                    developers = []
                    for pattern in patterns:
                        matches = re.findall(pattern, desc, re.IGNORECASE)
                        for match in matches:
                            dev = match.strip().rstrip(',.;')
                            # Clean up common prefixes and suffixes
                            dev = re.sub(r'^(?:LLC|LP|Inc\.?|and|the|for)\s+', '', dev, flags=re.IGNORECASE)
                            dev = re.sub(r'^(?:LLC|LP|Inc\.?|and|the|for)\s+', '', dev, flags=re.IGNORECASE)  # Run twice
                            dev = re.sub(r'\s+(?:AND|FOR|THE)$', '', dev, flags=re.IGNORECASE)
                            dev = dev.strip()

                            # Filter out transmission companies and generic terms
                            skip_terms = ['ONCOR', 'AEP TEXAS', 'AEP TX', 'CENTERPOINT', 'LCRA', 'ERCOT',
                                         'BRAZOS ELECTRIC', 'SHARYLAND', 'LONE STAR TRANSMISSION',
                                         'ELECTRIC TRANSMISSION', 'AGREEMENT', 'AMENDMENT', 'WETT',
                                         'WIND ENERGY TRANSMISSION', 'TEXAS-NEW MEXICO POWER',
                                         'CROSS TEXAS TRANSMISSION', 'CTT', 'TNMP', 'AUSTIN ENERGY',
                                         'CPS ENERGY', 'GEUS', 'REC SILICON']
                            if len(dev) > 5 and not any(skip in dev.upper() for skip in skip_terms):
                                # Also skip if it starts with common transmission prefixes
                                if not dev.upper().startswith(('BETWEEN', 'TEXAS-', 'FOR THE')):
                                    developers.append(dev)

                    if developers:
                        filing['developer'] = developers[0]  # Take first match
                        filing['all_developers'] = '; '.join(developers)
                    else:
                        filing['developer'] = None
                        filing['all_developers'] = None

                    filings.append(filing)

            df = pd.DataFrame(filings)
            logger.info(f"Extracted {len(df)} filings, {df['developer'].notna().sum()} with developers")

            # Cache the results
            df.to_parquet(cache_file)
            return df

        except Exception as e:
            logger.error(f"Failed to scrape Texas PUC: {e}")
            return pd.DataFrame()

    def extract_developers_from_texas(self) -> Dict[str, str]:
        """
        Build a lookup of project/company names to developer names.
        """
        df = self.scrape_texas_puc()

        if df.empty:
            return {}

        # Build lookup
        lookup = {}
        for _, row in df.iterrows():
            dev = row.get('developer')
            if dev:
                # Use developer name as key (normalized)
                key = dev.strip().upper()
                lookup[key] = dev

                # Also add variations
                # Remove LLC, LP, Inc suffixes for matching
                clean_key = re.sub(r'\s*(LLC|LP|Inc|Corp|Company|Co\.?|Ltd)\.?$', '', key, flags=re.IGNORECASE).strip()
                if clean_key and clean_key != key:
                    lookup[clean_key] = dev

        logger.info(f"Built Texas PUC developer lookup with {len(lookup)} entries")
        return lookup

    def enrich_from_texas_puc(self, queue_df: pd.DataFrame) -> int:
        """
        Enrich ERCOT queue data from Texas PUC filings.
        """
        df = self.scrape_texas_puc()

        if df.empty:
            return 0

        # Get developers with valid names
        dev_df = df[df['developer'].notna()].copy()

        if dev_df.empty:
            return 0

        enriched = 0

        # For ERCOT projects, try to match by project name or developer patterns
        ercot_df = queue_df[queue_df['iso'].str.upper() == 'ERCOT'] if 'iso' in queue_df.columns else queue_df

        for _, puc_row in dev_df.iterrows():
            developer = puc_row['developer']
            desc = puc_row['description']

            # Try to find matching queue project
            # This is a simplified match - could be enhanced with fuzzy matching
            for _, q_row in ercot_df.iterrows():
                queue_id = q_row.get('queue_id') or q_row.get('Queue ID')
                project_name = q_row.get('name') or q_row.get('Project Name', '')

                if not queue_id:
                    continue

                # Check if project name appears in PUC description
                if project_name and len(project_name) > 3:
                    if project_name.upper() in desc.upper():
                        self.store.add_enrichment(
                            queue_id=str(queue_id),
                            iso='ERCOT',
                            source='texas_puc',
                            developer=developer,
                            confidence=0.9,
                            match_method='project_name_match',
                            source_data={
                                'puc_description': desc[:200],
                                'puc_date': puc_row.get('date'),
                            }
                        )
                        enriched += 1
                        break

        logger.info(f"Enriched {enriched} projects from Texas PUC data")
        return enriched


class AirtableScraper:
    """Scrape interconnection.fyi Airtable data using browser automation."""

    # Airtable IDs from intercepted requests
    APP_ID = 'appyLVDZTQzJIIq2T'
    SHARE_ID = 'shrOkRhZ2XQShuaHB'
    VIEW_ID = 'viwGxwvXxAkQcrXU9'

    def __init__(self, store: EnrichmentStore):
        self.store = store
        self.cache_dir = CACHE_DIR / 'airtable'
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def scrape_with_playwright(self) -> pd.DataFrame:
        """
        Scrape Airtable data using Playwright browser automation.

        Requires: pip install playwright && playwright install chromium
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error("Playwright not installed. Install with: pip install playwright && playwright install chromium")
            return pd.DataFrame()

        embed_url = f'https://airtable.com/embed/{self.APP_ID}/{self.SHARE_ID}'

        logger.info(f"Scraping Airtable data from {embed_url}")

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()

                # Intercept API responses
                api_data = []

                def handle_response(response):
                    if 'readSharedViewData' in response.url:
                        try:
                            data = response.json()
                            api_data.append(data)
                        except:
                            pass

                page.on('response', handle_response)

                # Load the page
                page.goto(embed_url, wait_until='networkidle')
                page.wait_for_timeout(5000)  # Wait for data to load

                browser.close()

                if api_data:
                    logger.info(f"Captured {len(api_data)} API responses")
                    return self._parse_airtable_response(api_data[0])
                else:
                    logger.warning("No API data captured")
                    return pd.DataFrame()

        except Exception as e:
            logger.error(f"Playwright scraping failed: {e}")
            return pd.DataFrame()

    def _parse_airtable_response(self, data: Dict) -> pd.DataFrame:
        """Parse Airtable API response into DataFrame."""
        try:
            table = data.get('data', {}).get('table', {})
            columns = table.get('columns', [])
            rows = table.get('rows', [])

            if not columns or not rows:
                return pd.DataFrame()

            # Build column name mapping
            col_map = {col['id']: col['name'] for col in columns}

            # Extract row data
            records = []
            for row in rows:
                record = {}
                cell_values = row.get('cellValuesByColumnId', {})
                for col_id, value in cell_values.items():
                    col_name = col_map.get(col_id, col_id)
                    record[col_name] = value
                records.append(record)

            df = pd.DataFrame(records)
            logger.info(f"Parsed {len(df)} records from Airtable")
            return df

        except Exception as e:
            logger.error(f"Failed to parse Airtable response: {e}")
            return pd.DataFrame()

    def import_scraped_data(self, df: pd.DataFrame) -> int:
        """Import scraped Airtable data into enrichment store."""
        if df.empty:
            return 0

        imported = 0

        # Try to find relevant columns
        queue_id_cols = ['Queue ID', 'Request ID', 'Project ID', 'queue_id']
        developer_cols = ['Developer', 'Company', 'Applicant', 'Owner', 'Entity']
        iso_cols = ['ISO', 'RTO', 'Market', 'Power Market', 'Region']

        queue_id_col = next((c for c in queue_id_cols if c in df.columns), None)
        developer_col = next((c for c in developer_cols if c in df.columns), None)
        iso_col = next((c for c in iso_cols if c in df.columns), None)

        if not queue_id_col or not developer_col:
            logger.warning(f"Required columns not found. Available: {list(df.columns)}")
            return 0

        for _, row in df.iterrows():
            queue_id = str(row.get(queue_id_col, ''))
            developer = row.get(developer_col, '')
            iso = str(row.get(iso_col, 'UNKNOWN')) if iso_col else 'UNKNOWN'

            if not queue_id or not developer:
                continue

            if str(developer).lower() in ['', 'nan', 'none', 'null']:
                continue

            self.store.add_enrichment(
                queue_id=queue_id,
                iso=iso,
                source='interconnection_fyi_scrape',
                developer=str(developer),
                confidence=0.95,
                match_method='airtable_scrape',
                source_data={'source': 'interconnection.fyi'}
            )
            imported += 1

        logger.info(f"Imported {imported} enrichments from Airtable scrape")
        return imported


class DataEnrichment:
    """Main orchestrator for data enrichment."""

    def __init__(self, gridstatus_api_key: str = None):
        self.store = EnrichmentStore()
        self.eia = EIAEnricher(self.store, api_key=gridstatus_api_key)
        self.ferc = FERCEnricher(self.store)
        self.manual = ManualEnricher(self.store)
        self.ifyi = InterconnectionFyiImporter(self.store)
        self.airtable = AirtableScraper(self.store)
        self.state_puc = StatePUCScraper(self.store)
        self.gridstatus_api_key = gridstatus_api_key

    def apply_enrichments(self, queue_df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all available enrichments to queue data.

        Returns DataFrame with enriched developer data.
        """
        df = queue_df.copy()

        # Ensure we have standardized columns - coalesce from multiple sources
        dev_cols = ['Developer', 'Developer/Interconnection Customer', 'Interconnecting Entity', 'developer']
        df['developer'] = None
        for col in dev_cols:
            if col in df.columns:
                # Fill in developer where currently None
                mask = df['developer'].isna() | (df['developer'].astype(str).str.lower().isin(['none', 'nan', '']))
                df.loc[mask, 'developer'] = df.loc[mask, col]

        if 'queue_id' not in df.columns:
            qid_cols = ['Queue ID', 'queue_id', 'q_id']
            for col in qid_cols:
                if col in df.columns:
                    df['queue_id'] = df[col]
                    break

        if 'iso' not in df.columns:
            iso_cols = ['iso', 'ISO', 'region', 'Region']
            for col in iso_cols:
                if col in df.columns:
                    df['iso'] = df[col]
                    break

        # Get all enrichments
        enrichments = self.store.get_all_enrichments()

        if enrichments.empty:
            logger.info("No enrichments available")
            return df

        # Create lookup dict
        enrich_lookup = {}
        for _, row in enrichments.iterrows():
            key = (str(row['queue_id']), str(row['iso']))
            if key not in enrich_lookup or row['confidence'] > enrich_lookup[key]['confidence']:
                enrich_lookup[key] = row

        # Apply enrichments
        enriched_count = 0
        for idx, row in df.iterrows():
            queue_id = str(row.get('queue_id', ''))
            iso = str(row.get('iso', ''))
            current_dev = row.get('developer')

            # Skip if already has valid developer
            if current_dev and str(current_dev).lower() not in ['none', 'nan', '', 'unknown']:
                continue

            key = (queue_id, iso)
            if key in enrich_lookup:
                enrichment = enrich_lookup[key]
                df.at[idx, 'developer'] = enrichment['developer']
                df.at[idx, '_enrichment_source'] = enrichment['source']
                df.at[idx, '_enrichment_confidence'] = enrichment['confidence']
                enriched_count += 1

        logger.info(f"Applied {enriched_count} enrichments to queue data")
        return df

    def run_enrichment_pipeline(self, queue_df: pd.DataFrame) -> pd.DataFrame:
        """
        Run the full enrichment pipeline:
        1. Apply existing enrichments
        2. Try EIA enrichment for operational matches
        3. Populate manual queue for remaining gaps
        """
        logger.info("Starting enrichment pipeline...")

        # Step 1: Apply existing enrichments
        df = self.apply_enrichments(queue_df)

        # Step 2: Try EIA enrichment
        self.eia.enrich_from_eia(df)

        # Step 3: Re-apply enrichments (including new EIA ones)
        df = self.apply_enrichments(df)

        # Step 4: Add remaining gaps to manual queue
        self.manual.populate_manual_queue(df)

        # Report stats
        stats = self.store.get_stats()
        logger.info(f"Enrichment pipeline complete:")
        logger.info(f"  Total enrichments: {stats['total_enrichments']}")
        logger.info(f"  By source: {stats['by_source']}")
        logger.info(f"  Pending manual: {stats['pending_manual']}")

        return df

    def get_coverage_report(self, queue_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate enrichment coverage report."""
        df = self.apply_enrichments(queue_df)

        total = len(df)

        # Count valid developers (including enriched)
        valid_dev = df['developer'].notna() & \
                    (df['developer'].astype(str).str.lower().str.strip() != '') & \
                    (~df['developer'].astype(str).str.lower().isin(['none', 'nan', 'unknown']))

        coverage = {
            'total_projects': total,
            'with_developer': valid_dev.sum(),
            'without_developer': total - valid_dev.sum(),
            'coverage_pct': valid_dev.sum() / total if total > 0 else 0,
        }

        # By source
        if '_enrichment_source' in df.columns:
            coverage['enriched_by_source'] = df[df['_enrichment_source'].notna()]['_enrichment_source'].value_counts().to_dict()

        # By ISO
        if 'iso' in df.columns:
            coverage['by_iso'] = {}
            for iso in df['iso'].unique():
                iso_df = df[df['iso'] == iso]
                iso_valid = iso_df['developer'].notna() & \
                            (iso_df['developer'].astype(str).str.lower().str.strip() != '') & \
                            (~iso_df['developer'].astype(str).str.lower().isin(['none', 'nan', 'unknown']))
                coverage['by_iso'][iso] = {
                    'total': len(iso_df),
                    'with_developer': iso_valid.sum(),
                    'coverage_pct': iso_valid.sum() / len(iso_df) if len(iso_df) > 0 else 0
                }

        return coverage


def main():
    """Demo and CLI for data enrichment."""
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Data Enrichment for Interconnection Queue")
    parser.add_argument('--stats', action='store_true', help='Show enrichment statistics')
    parser.add_argument('--export-manual', type=str, help='Export manual research queue to CSV')
    parser.add_argument('--import-manual', type=str, help='Import manual research results from CSV')
    parser.add_argument('--import-ifyi', type=str, help='Import Interconnection.fyi data from CSV')
    parser.add_argument('--run-pipeline', action='store_true', help='Run full enrichment pipeline')
    parser.add_argument('--enrich-eia', action='store_true', help='Run EIA enrichment via GridStatus API')
    parser.add_argument('--scrape-airtable', action='store_true', help='Scrape interconnection.fyi Airtable')
    parser.add_argument('--scrape-texas-puc', action='store_true', help='Scrape Texas PUC ERCOT filings')
    parser.add_argument('--api-key', type=str, help='GridStatus API key')

    args = parser.parse_args()

    # Get API key from args or environment
    api_key = args.api_key or os.environ.get('GRIDSTATUS_API_KEY')

    enricher = DataEnrichment(gridstatus_api_key=api_key)

    if args.stats:
        stats = enricher.store.get_stats()
        print("\n=== Enrichment Statistics ===")
        print(f"Total enrichments: {stats['total_enrichments']}")
        print(f"By source: {stats['by_source']}")
        print(f"By ISO: {stats['by_iso']}")
        print(f"Pending manual: {stats['pending_manual']}")
        print(f"Developers in registry: {stats['developers_in_registry']}")

    elif args.export_manual:
        path = enricher.manual.export_for_research(Path(args.export_manual))
        print(f"Exported to: {path}")

    elif args.import_manual:
        count = enricher.manual.import_research_results(Path(args.import_manual))
        print(f"Imported {count} manual enrichments")

    elif args.import_ifyi:
        count = enricher.ifyi.import_csv(Path(args.import_ifyi))
        print(f"Imported {count} enrichments from Interconnection.fyi")

    elif args.enrich_eia:
        if not api_key:
            print("Error: GridStatus API key required. Use --api-key or set GRIDSTATUS_API_KEY env var")
            return

        try:
            from market_intel import MarketData
            market = MarketData()
            df = market.get_latest_data()

            if not df.empty:
                print(f"Running EIA enrichment on {len(df):,} projects...")
                count = enricher.eia.enrich_from_eia(df)
                print(f"Enriched {count} projects from EIA data")

                # Show updated coverage
                coverage = enricher.get_coverage_report(df)
                print(f"\nUpdated coverage: {coverage['coverage_pct']:.1%}")
            else:
                print("No queue data available.")
        except ImportError as e:
            print(f"Error: {e}")

    elif args.scrape_airtable:
        print("Scraping interconnection.fyi Airtable...")
        df = enricher.airtable.scrape_with_playwright()
        if not df.empty:
            print(f"Scraped {len(df)} records")
            print(f"Columns: {list(df.columns)}")
            count = enricher.airtable.import_scraped_data(df)
            print(f"Imported {count} enrichments")
        else:
            print("No data scraped. Make sure Playwright is installed.")

    elif args.scrape_texas_puc:
        print("Scraping Texas PUC ERCOT interconnection filings...")
        df = enricher.state_puc.scrape_texas_puc()
        if not df.empty:
            print(f"Scraped {len(df)} filings")
            print(f"Filings with developers: {df['developer'].notna().sum()}")
            print(f"\nSample developers:")
            for dev in df['developer'].dropna().unique()[:20]:
                print(f"  - {dev}")

            # Save to CSV for review
            output_file = DATA_DIR / 'texas_puc_developers.csv'
            df[df['developer'].notna()].to_csv(output_file, index=False)
            print(f"\nSaved to: {output_file}")
        else:
            print("No data scraped.")

    elif args.run_pipeline:
        try:
            from market_intel import MarketData
            market = MarketData()
            df = market.get_latest_data()

            if not df.empty:
                enriched_df = enricher.run_enrichment_pipeline(df)
                coverage = enricher.get_coverage_report(enriched_df)

                print("\n=== Coverage Report ===")
                print(f"Total projects: {coverage['total_projects']}")
                print(f"With developer: {coverage['with_developer']} ({coverage['coverage_pct']:.1%})")
                print(f"Without developer: {coverage['without_developer']}")

                if 'by_iso' in coverage:
                    print("\nBy ISO:")
                    for iso, data in coverage['by_iso'].items():
                        print(f"  {iso}: {data['coverage_pct']:.1%} ({data['with_developer']}/{data['total']})")
            else:
                print("No queue data available. Run market_intel.py first.")
        except ImportError:
            print("market_intel module not available. Please load queue data manually.")

    else:
        parser.print_help()


if __name__ == '__main__':
    main()
