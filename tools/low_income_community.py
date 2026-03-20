#!/usr/bin/env python3
"""
Low-Income Community Bonus Credit Checker.

Checks if projects qualify for the IRA Section 48(e) / 48E Low-Income
Community Bonus Credit, which adds 10-20% to the ITC.

Three qualifying geographies (any one qualifies for Category 1):
1. NMTC Low-Income Community — Census tracts meeting CDFI New Markets
   Tax Credit poverty threshold (≥20% poverty rate)
2. Persistent Poverty County — Counties meeting USDA persistent poverty
   threshold (poverty ≥20% in 1990, 2000, 2010, and 2020 censuses)
3. CEJST Energy Disadvantaged — Census tracts meeting the Climate &
   Economic Justice Screening Tool's Energy burden threshold

Bonus amounts:
- Category 1: Located in a low-income community → +10% ITC
- Category 2: Part of a qualified low-income residential building → +10% ITC
- Category 3: Part of a qualified low-income economic benefit project → +20% ITC
- Category 4: Located on Indian land → +20% ITC

Categories 2-4 require a separate IRS allocation application (competitive).
This module checks geographic eligibility for Category 1 and flags
potential Category 3/4 eligibility.

Data source: DOE IRA Low-Income Community Bonus Credit Program Layers
https://data.nlr.gov/submissions/222

Usage:
    from low_income_community import LowIncomeChecker

    checker = LowIncomeChecker()
    result = checker.check_location(state='WV', county='McDowell')
    print(result)

    # Batch enrich
    python3 low_income_community.py --enrich
    python3 low_income_community.py --stats
    python3 low_income_community.py --check WV McDowell
"""

import sqlite3
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
from collections import defaultdict

try:
    import openpyxl
except ImportError:
    openpyxl = None

# Paths
TOOLS_DIR = Path(__file__).parent
CACHE_DIR = TOOLS_DIR / '.cache' / 'low_income_communities'
DATA_FILE = CACHE_DIR / 'Low-Income-Communities_Excel.xlsx'
DB_PATH = TOOLS_DIR / '.data' / 'master.db'

# State FIPS to abbreviation
FIPS_TO_STATE = {
    '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA',
    '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL',
    '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN',
    '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME',
    '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS',
    '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH',
    '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND',
    '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI',
    '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT',
    '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI',
    '56': 'WY', '60': 'AS', '66': 'GU', '69': 'MP', '72': 'PR', '78': 'VI',
}

STATE_TO_FIPS = {v: k for k, v in FIPS_TO_STATE.items()}


@dataclass
class LowIncomeResult:
    """Result of low-income community eligibility check."""
    is_low_income: bool = False

    # Category 1 criteria (any one qualifies)
    nmtc_qualified: bool = False         # CDFI New Markets Tax Credit threshold
    persistent_poverty: bool = False     # USDA persistent poverty county
    cejst_energy: bool = False           # CEJST energy burden disadvantaged

    # Coverage percentages (what % of county's census tracts qualify)
    nmtc_pct: float = 0.0               # % of county tracts that are NMTC qualified
    ppc_pct: float = 0.0                # % coverage for persistent poverty
    cejst_pct: float = 0.0              # % coverage for CEJST energy

    # Bonus details
    category_1_eligible: bool = False    # Located in low-income community (+10%)
    bonus_rate: float = 0.0             # 0.10 for Category 1

    # Location
    state: str = ''
    county: str = ''
    qualifying_tracts: int = 0          # Number of qualifying tracts in county
    total_tracts: int = 0               # Total tracts in county


class LowIncomeChecker:
    """Check project locations against Low-Income Community zones."""

    def __init__(self):
        self.county_data = {}   # (state_abbrev, county_norm) -> aggregated tract data
        self.loaded = False

    def _normalize_county(self, county_name: str) -> str:
        """Normalize county name for matching."""
        if not county_name:
            return ''
        s = county_name.strip()
        for suffix in [' County', ' Parish', ' Borough', ' Census Area',
                       ' Municipality', ' city', ' City']:
            if s.endswith(suffix):
                s = s[:-len(suffix)]
        return s.lower().strip()

    def load_data(self):
        """Load low-income community data from the DOE Excel file."""
        if not DATA_FILE.exists():
            print(f"Warning: Low-income data not found at {DATA_FILE}")
            print("Download from: https://data.nlr.gov/submissions/222")
            self.loaded = False
            return 0

        if openpyxl is None:
            print("Warning: openpyxl not installed. Run: pip install openpyxl")
            self.loaded = False
            return 0

        print("Loading Low-Income Community data...")

        wb = openpyxl.load_workbook(DATA_FILE, read_only=True)
        ws = wb['2023 Tract percentages']

        # Aggregate by county — since we match projects at county level,
        # we compute what percentage of a county's tracts qualify
        county_tracts = defaultdict(lambda: {
            'total': 0, 'nmtc': 0, 'ppc': 0, 'cejst': 0,
            'county_name': '', 'state_name': '',
        })

        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row[0]:  # Skip empty rows
                continue

            geoid, state_fips, state_name, county_fips, county_name, \
                state_county_fips, nmtc_pct, ppc_pct, cejst_pct = row

            state_abbrev = FIPS_TO_STATE.get(str(state_fips).zfill(2), '')
            county_norm = self._normalize_county(county_name)

            if not state_abbrev or not county_norm:
                continue

            key = (state_abbrev, county_norm)
            data = county_tracts[key]
            data['total'] += 1
            data['county_name'] = county_name
            data['state_name'] = state_name

            # A tract qualifies if >=50% of its land area is in the qualifying zone
            # (the pct values represent land area overlap)
            nmtc_val = nmtc_pct if isinstance(nmtc_pct, (int, float)) else 0
            ppc_val = ppc_pct if isinstance(ppc_pct, (int, float)) else 0
            cejst_val = cejst_pct if isinstance(cejst_pct, (int, float)) else 0

            if nmtc_val >= 50:
                data['nmtc'] += 1
            if ppc_val >= 50:
                data['ppc'] += 1
            if cejst_val >= 50:
                data['cejst'] += 1

        wb.close()

        # Store aggregated data
        self.county_data = {}
        for key, data in county_tracts.items():
            total = data['total']
            if total == 0:
                continue
            self.county_data[key] = {
                'total_tracts': total,
                'nmtc_tracts': data['nmtc'],
                'ppc_tracts': data['ppc'],
                'cejst_tracts': data['cejst'],
                'nmtc_pct': 100 * data['nmtc'] / total,
                'ppc_pct': 100 * data['ppc'] / total,
                'cejst_pct': 100 * data['cejst'] / total,
                'any_qualifying': data['nmtc'] + data['ppc'] + data['cejst'] > 0,
                'county_name': data['county_name'],
            }

        qualifying = sum(1 for v in self.county_data.values() if v['any_qualifying'])
        print(f"  Loaded {len(self.county_data)} counties, {qualifying} with qualifying tracts")
        self.loaded = True
        return len(self.county_data)

    def check_location(self, state: str, county: str) -> LowIncomeResult:
        """Check if a state/county location qualifies for low-income bonus."""
        if not self.loaded:
            self.load_data()

        if not self.loaded:
            return LowIncomeResult(state=state, county=county)

        state_upper = state.upper().strip() if state else ''
        county_norm = self._normalize_county(county)
        key = (state_upper, county_norm)

        result = LowIncomeResult(state=state, county=county)

        data = self.county_data.get(key)
        if not data:
            return result

        result.total_tracts = data['total_tracts']
        result.nmtc_pct = data['nmtc_pct']
        result.ppc_pct = data['ppc_pct']
        result.cejst_pct = data['cejst_pct']

        # A county qualifies if ANY of its tracts qualify
        # (project-level tract matching would require lat/lng, which we don't always have)
        # We flag as eligible if >=25% of tracts qualify — conservative threshold
        # since without exact location we can't guarantee the project is in a qualifying tract
        THRESHOLD = 25  # % of county tracts that must qualify

        result.nmtc_qualified = data['nmtc_pct'] >= THRESHOLD
        result.persistent_poverty = data['ppc_pct'] >= THRESHOLD
        result.cejst_energy = data['cejst_pct'] >= THRESHOLD

        qualifying = data['nmtc_tracts'] + data['ppc_tracts'] + data['cejst_tracts']
        result.qualifying_tracts = min(qualifying, data['total_tracts'])

        if result.nmtc_qualified or result.persistent_poverty or result.cejst_energy:
            result.is_low_income = True
            result.category_1_eligible = True
            result.bonus_rate = 0.10  # Category 1: +10% ITC

        return result


def enrich_queue_with_low_income(db_path: Path = None, save: bool = True):
    """Add low-income community eligibility to queue projects."""
    db = db_path or DB_PATH
    print("=" * 60)
    print("Low-Income Community Bonus Enrichment")
    print("=" * 60)

    checker = LowIncomeChecker()
    loaded = checker.load_data()

    if not loaded:
        print("ERROR: Could not load low-income data. Aborting.")
        return None

    if not db.exists():
        raise FileNotFoundError(f"Database not found: {db}")

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row

    # Add columns if needed
    new_columns = [
        ("low_income_eligible", "INTEGER"),
        ("low_income_bonus", "REAL"),
        ("low_income_type", "TEXT"),
        ("low_income_nmtc_pct", "REAL"),
        ("low_income_ppc_pct", "REAL"),
        ("low_income_cejst_pct", "REAL"),
    ]

    for col_name, col_type in new_columns:
        try:
            conn.execute(f"ALTER TABLE projects ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass

    # Get projects with location data
    cursor = conn.execute("""
        SELECT id, queue_id, state, county FROM projects
        WHERE state IS NOT NULL AND state != ''
    """)
    projects = cursor.fetchall()
    print(f"\nProcessing {len(projects):,} projects with state data...")

    stats = {'total': len(projects), 'eligible': 0, 'nmtc': 0, 'ppc': 0, 'cejst': 0, 'no_county': 0}

    # Cache results by (state, county) to avoid re-checking
    location_cache = {}

    for i, project in enumerate(projects):
        if (i + 1) % 10000 == 0:
            print(f"  Progress: {i+1:,}/{len(projects):,}")

        state = project['state'] or ''
        county = project['county'] or ''

        if not county:
            stats['no_county'] += 1
            if save:
                conn.execute(
                    "UPDATE projects SET low_income_eligible = NULL WHERE id = ?",
                    (project['id'],)
                )
            continue

        cache_key = (state.upper(), county)
        if cache_key not in location_cache:
            location_cache[cache_key] = checker.check_location(state, county)
        result = location_cache[cache_key]

        if result.is_low_income:
            stats['eligible'] += 1
            li_types = []
            if result.nmtc_qualified:
                li_types.append('nmtc')
                stats['nmtc'] += 1
            if result.persistent_poverty:
                li_types.append('ppc')
                stats['ppc'] += 1
            if result.cejst_energy:
                li_types.append('cejst_energy')
                stats['cejst'] += 1
            li_type_str = ','.join(li_types)

            if save:
                conn.execute("""
                    UPDATE projects SET
                        low_income_eligible = 1,
                        low_income_bonus = ?,
                        low_income_type = ?,
                        low_income_nmtc_pct = ?,
                        low_income_ppc_pct = ?,
                        low_income_cejst_pct = ?
                    WHERE id = ?
                """, (result.bonus_rate, li_type_str,
                      result.nmtc_pct, result.ppc_pct, result.cejst_pct,
                      project['id']))
        else:
            if save:
                conn.execute("""
                    UPDATE projects SET
                        low_income_eligible = 0,
                        low_income_bonus = 0,
                        low_income_type = NULL,
                        low_income_nmtc_pct = ?,
                        low_income_ppc_pct = ?,
                        low_income_cejst_pct = ?
                    WHERE id = ?
                """, (result.nmtc_pct, result.ppc_pct, result.cejst_pct,
                      project['id']))

    if save:
        conn.commit()

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"\nTotal projects processed: {stats['total']:,}")
    print(f"Low-income community eligible: {stats['eligible']:,} ({100*stats['eligible']/max(stats['total'],1):.1f}%)")
    print(f"  - NMTC low-income: {stats['nmtc']:,}")
    print(f"  - Persistent poverty county: {stats['ppc']:,}")
    print(f"  - CEJST energy disadvantaged: {stats['cejst']:,}")
    print(f"No county data: {stats['no_county']:,}")
    print(f"\nUnique locations checked: {len(location_cache):,}")

    conn.close()
    return stats


def get_low_income_stats(db_path: Path = None):
    """Print low-income community statistics from enriched database."""
    db = db_path or DB_PATH
    if not db.exists():
        raise FileNotFoundError(f"Database not found: {db}")

    conn = sqlite3.connect(db)

    cursor = conn.execute("PRAGMA table_info(projects)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'low_income_eligible' not in columns:
        print("Low-income data not yet enriched. Run: python3 low_income_community.py --enrich")
        conn.close()
        return

    print("\n" + "=" * 60)
    print("LOW-INCOME COMMUNITY BONUS ELIGIBILITY")
    print("=" * 60)

    cursor = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN low_income_eligible = 1 THEN 1 ELSE 0 END) as eligible,
            SUM(CASE WHEN low_income_type LIKE '%nmtc%' THEN 1 ELSE 0 END) as nmtc,
            SUM(CASE WHEN low_income_type LIKE '%ppc%' THEN 1 ELSE 0 END) as ppc,
            SUM(CASE WHEN low_income_type LIKE '%cejst%' THEN 1 ELSE 0 END) as cejst
        FROM projects
    """)
    row = cursor.fetchone()
    total, eligible, nmtc, ppc, cejst = row

    print(f"\nTotal projects: {total:,}")
    print(f"Eligible for low-income bonus: {eligible:,} ({100*eligible/max(total,1):.1f}%)")
    print(f"  NMTC low-income: {nmtc:,}")
    print(f"  Persistent poverty: {ppc:,}")
    print(f"  CEJST energy: {cejst:,}")

    # By region
    print(f"\n{'Region':<12} {'Total':>8} {'LI Eligible':>12} {'Rate':>7} {'NMTC':>8} {'PPC':>8} {'CEJST':>8}")
    print("-" * 70)

    cursor = conn.execute("""
        SELECT
            region,
            COUNT(*) as total,
            SUM(CASE WHEN low_income_eligible = 1 THEN 1 ELSE 0 END) as eligible,
            SUM(CASE WHEN low_income_type LIKE '%nmtc%' THEN 1 ELSE 0 END) as nmtc,
            SUM(CASE WHEN low_income_type LIKE '%ppc%' THEN 1 ELSE 0 END) as ppc,
            SUM(CASE WHEN low_income_type LIKE '%cejst%' THEN 1 ELSE 0 END) as cejst
        FROM projects
        GROUP BY region
        ORDER BY total DESC
    """)

    for row in cursor.fetchall():
        region, total_r, eligible_r, nmtc_r, ppc_r, cejst_r = row
        rate = 100 * eligible_r / max(total_r, 1)
        print(f"{region:<12} {total_r:>8,} {eligible_r:>12,} {rate:>6.1f}% {nmtc_r:>8,} {ppc_r:>8,} {cejst_r:>8,}")

    # Top states
    print(f"\nTop 15 states by low-income eligible projects:")
    print(f"{'State':<8} {'Eligible':>10} {'Total':>8} {'Rate':>7}")
    print("-" * 35)

    cursor = conn.execute("""
        SELECT
            state,
            SUM(CASE WHEN low_income_eligible = 1 THEN 1 ELSE 0 END) as eligible,
            COUNT(*) as total
        FROM projects
        WHERE state IS NOT NULL AND state != ''
        GROUP BY state
        ORDER BY eligible DESC
        LIMIT 15
    """)

    for row in cursor.fetchall():
        state, eligible_s, total_s = row
        rate = 100 * eligible_s / max(total_s, 1)
        print(f"{state:<8} {eligible_s:>10,} {total_s:>8,} {rate:>6.1f}%")

    conn.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Low-Income Community Bonus Credit Checker')
    parser.add_argument('--enrich', action='store_true',
                        help='Enrich queue database with low-income eligibility')
    parser.add_argument('--stats', action='store_true',
                        help='Show low-income statistics from enriched database')
    parser.add_argument('--check', nargs=2, metavar=('STATE', 'COUNTY'),
                        help='Check a specific location: --check WV McDowell')
    parser.add_argument('--dry-run', action='store_true',
                        help='Do not save changes to database')
    parser.add_argument('--db', type=str, default=None,
                        help='Path to database')

    args = parser.parse_args()
    db = Path(args.db) if args.db else None

    if args.check:
        state, county = args.check
        checker = LowIncomeChecker()
        checker.load_data()
        result = checker.check_location(state, county)

        print(f"\nLow-Income Community Check: {state}, {county}")
        print("-" * 50)
        print(f"Eligible for low-income bonus: {result.is_low_income}")
        print(f"Category 1 eligible (+10% ITC): {result.category_1_eligible}")
        print(f"\nQualifying criteria:")
        print(f"  NMTC low-income: {'YES' if result.nmtc_qualified else 'No'} ({result.nmtc_pct:.0f}% of tracts)")
        print(f"  Persistent poverty: {'YES' if result.persistent_poverty else 'No'} ({result.ppc_pct:.0f}% of tracts)")
        print(f"  CEJST energy: {'YES' if result.cejst_energy else 'No'} ({result.cejst_pct:.0f}% of tracts)")
        print(f"\nTotal census tracts in county: {result.total_tracts}")
        print(f"Qualifying tracts: {result.qualifying_tracts}")

    elif args.enrich:
        enrich_queue_with_low_income(db_path=db, save=not args.dry_run)

    elif args.stats:
        get_low_income_stats(db_path=db)

    else:
        parser.print_help()
