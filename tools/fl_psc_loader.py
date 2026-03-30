#!/usr/bin/env python3
"""
FL PSC Net Metering Report Loader — Duke Energy FL + Tampa Electric

Parses project-level interconnection data from Florida PSC Rule 25-6.065
utility filings (PDF format) and loads into dg.db.

Data Sources:
  - Duke Energy Florida: 89,710 projects (1,695 pages)
  - Tampa Electric: 29,026 projects (878 pages)
  Total: ~118,736 FL solar projects

Auth: None (public PDFs from floridapsc.com)
Refresh: Annual (utilities file by April each year)

Usage:
    python3 fl_psc_loader.py              # Full load (both utilities)
    python3 fl_psc_loader.py --stats      # Show stats after load
    python3 fl_psc_loader.py --dry-run    # Parse only, no DB write
    python3 fl_psc_loader.py --duke-only  # Duke Energy FL only
    python3 fl_psc_loader.py --tampa-only # Tampa Electric only
"""

import sqlite3
import hashlib
import json
import logging
import re
import pdfplumber
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
CACHE_DIR = Path(__file__).parent / '.cache' / 'fl_psc'

SOURCE = 'fl_psc'
REGION = 'Southeast'

DUKE_PDF = CACHE_DIR / 'duke_fl_2024_report.pdf'
TAMPA_PDF = CACHE_DIR / 'tampa_2024_report.pdf'

# Florida county list for validation
FL_COUNTIES = {
    'ALACHUA', 'BAKER', 'BAY', 'BRADFORD', 'BREVARD', 'BROWARD', 'CALHOUN',
    'CHARLOTTE', 'CITRUS', 'CLAY', 'COLLIER', 'COLUMBIA', 'DESOTO', 'DIXIE',
    'DUVAL', 'ESCAMBIA', 'FLAGLER', 'FRANKLIN', 'GADSDEN', 'GILCHRIST',
    'GLADES', 'GULF', 'HAMILTON', 'HARDEE', 'HENDRY', 'HERNANDO', 'HIGHLANDS',
    'HILLSBOROUGH', 'HOLMES', 'INDIAN RIVER', 'JACKSON', 'JEFFERSON',
    'LAFAYETTE', 'LAKE', 'LEE', 'LEON', 'LEVY', 'LIBERTY', 'MADISON',
    'MANATEE', 'MARION', 'MARTIN', 'MIAMI-DADE', 'MONROE', 'NASSAU',
    'OKALOOSA', 'OKEECHOBEE', 'ORANGE', 'OSCEOLA', 'PALM BEACH', 'PASCO',
    'PINELLAS', 'POLK', 'PUTNAM', 'SANTA ROSA', 'SARASOTA', 'SEMINOLE',
    'ST. JOHNS', 'ST. LUCIE', 'SUMTER', 'SUWANNEE', 'TAYLOR', 'UNION',
    'VOLUSIA', 'WAKULLA', 'WALTON', 'WASHINGTON',
}


def compute_hash(row_dict: dict) -> str:
    key_fields = ['queue_id', 'capacity_kw', 'status', 'county', 'utility', 'cod']
    parts = [str(row_dict.get(f, '')) for f in key_fields]
    return hashlib.md5('|'.join(parts).encode()).hexdigest()


def parse_duke_pdf(pdf_path: Path) -> List[dict]:
    """Parse Duke Energy FL interconnection report PDF."""
    logger.info(f"Parsing Duke Energy FL PDF ({pdf_path.name})...")
    pdf = pdfplumber.open(pdf_path)
    records = []

    # Duke format per line:
    # Count PREMISE CITY COUNTY Technology kW-AC Date
    # e.g.: 1 5204710266 SUMMERFIELD MARION Solar 5.8 12/31/2024
    # Some cities have spaces: HAINES CITY, PALM BAY, etc.
    # Strategy: find county by matching against FL_COUNTIES from right side

    for page_num in range(len(pdf.pages)):
        text = pdf.pages[page_num].extract_text()
        if not text:
            continue

        lines = text.split('\n')
        for line in lines:
            line = line.strip()
            if not line or line.startswith('Duke Energy') or line.startswith('Annual') or \
               line.startswith('In Accordance') or line.startswith('Reporting') or \
               line.startswith('(f)') or line.startswith('Count '):
                continue

            # Match: number premise_id rest_of_line
            m = re.match(r'^(\d+)\s+(\d{10})\s+(.+)$', line)
            if not m:
                continue

            count_num = m.group(1)
            premise_id = m.group(2)
            rest = m.group(3).strip()

            # Parse from the right: date, kw, technology, county, city
            # Date is MM/DD/YYYY at end
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})\s*$', rest)
            if not date_match:
                continue
            date_str = date_match.group(1)
            rest = rest[:date_match.start()].strip()

            # kW-AC is a number before the date
            kw_match = re.search(r'([\d,.]+)\s*$', rest)
            if not kw_match:
                continue
            try:
                kw_ac = float(kw_match.group(1).replace(',', ''))
            except ValueError:
                continue
            rest = rest[:kw_match.start()].strip()

            # Technology (Solar, Wind, etc.) before kW
            tech_match = re.search(r'(Solar|Wind|Biomass|Other|Fuel Cell|Micro[\s-]?Turbine)\s*$', rest, re.IGNORECASE)
            if not tech_match:
                continue
            technology = tech_match.group(1)
            rest = rest[:tech_match.start()].strip()

            # Now rest is "CITY COUNTY" — find county from right
            county = None
            city = None
            rest_upper = rest.upper()
            for c in sorted(FL_COUNTIES, key=len, reverse=True):
                if rest_upper.endswith(c):
                    county = c
                    city = rest[:len(rest) - len(c)].strip()
                    break

            if not county:
                # Fallback: last word is county
                parts = rest.rsplit(None, 1)
                if len(parts) == 2:
                    city, county = parts
                    county = county.upper()
                else:
                    county = rest.upper()
                    city = None

            records.append({
                'premise_id': premise_id,
                'city': city.title() if city else None,
                'county': county.title() if county else None,
                'technology': technology,
                'capacity_kw': kw_ac,
                'date_interconnected': date_str,
                'utility': 'Duke Energy Florida',
            })

        if (page_num + 1) % 500 == 0:
            logger.info(f"  Parsed {page_num + 1}/{len(pdf.pages)} pages, {len(records):,} records")

    pdf.close()
    logger.info(f"  Duke FL: {len(records):,} records parsed")
    return records


def parse_tampa_pdf(pdf_path: Path) -> List[dict]:
    """Parse Tampa Electric interconnection report PDF."""
    logger.info(f"Parsing Tampa Electric PDF ({pdf_path.name})...")
    pdf = pdfplumber.open(pdf_path)
    records = []

    # Tampa format per line:
    # Number Type GPR_DC GPR_AC County Date ID monthly_kwh...
    # e.g.: 1 SOLAR PV 659.18 560.30 HILLSBOROUGH Oct-20 12546 0 0 ...

    month_abbrs = {'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug',
                   'Sep', 'Oct', 'Nov', 'Dec', 'Mav'}  # 'Mav' is a typo in the PDF for May

    for page_num in range(len(pdf.pages)):
        text = pdf.pages[page_num].extract_text()
        if not text:
            continue

        lines = text.split('\n')
        for line in lines:
            line = line.strip()
            if not line or line.startswith('10(') or line.startswith('C N') or \
               line.startswith('TAMPA') or line.startswith('NET METERING'):
                continue

            # Match: number SOLAR PV|WIND ... rest
            m = re.match(r'^(\d+)\s+(SOLAR PV|WIND|FUEL CELL|OTHER)\s+([\d,.]+)\s+([\d,.]+)\s+(\S+(?:\s+\S+)?)\s+([A-Za-z]{3})-(\d{2})\s+(\d+)', line)
            if not m:
                continue

            count_num = m.group(1)
            tech_type = m.group(2)
            kw_dc = float(m.group(3).replace(',', ''))
            kw_ac = float(m.group(4).replace(',', ''))
            county = m.group(5).strip()
            month_str = m.group(6)
            year_str = m.group(7)
            customer_id = m.group(8)

            # Parse date
            month_map = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5,
                         'Jun': 6, 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10,
                         'Nov': 11, 'Dec': 12, 'Mav': 5}
            month_num = month_map.get(month_str, 1)
            year_full = 2000 + int(year_str)
            date_str = f"{month_num:02d}/01/{year_full}"

            records.append({
                'premise_id': customer_id,
                'city': None,  # Tampa doesn't include city
                'county': county.title(),
                'technology': 'Solar' if 'SOLAR' in tech_type else tech_type.title(),
                'capacity_kw': kw_ac,
                'capacity_dc_kw': kw_dc,
                'date_interconnected': date_str,
                'utility': 'Tampa Electric',
            })

        if (page_num + 1) % 200 == 0:
            logger.info(f"  Parsed {page_num + 1}/{len(pdf.pages)} pages, {len(records):,} records")

    pdf.close()
    logger.info(f"  Tampa Electric: {len(records):,} records parsed")
    return records


class FLPSCLoader:
    """Load FL PSC interconnection data from utility PDFs."""

    def __init__(self, db_path: Path = None):
        self.db_path = db_path or DG_DB_PATH
        self.records: List[dict] = []

    def fetch(self, duke: bool = True, tampa: bool = True) -> List[dict]:
        """Parse PDF files and return raw records."""
        all_records = []
        if duke and DUKE_PDF.exists():
            all_records.extend(parse_duke_pdf(DUKE_PDF))
        elif duke:
            logger.warning(f"Duke PDF not found: {DUKE_PDF}")

        if tampa and TAMPA_PDF.exists():
            all_records.extend(parse_tampa_pdf(TAMPA_PDF))
        elif tampa:
            logger.warning(f"Tampa PDF not found: {TAMPA_PDF}")

        logger.info(f"Total raw records: {len(all_records):,}")
        return all_records

    def normalize(self, raw_data: List[dict]) -> List[dict]:
        """Normalize to dg.db schema."""
        logger.info(f"Normalizing {len(raw_data):,} records...")
        records = []

        for rec in raw_data:
            premise_id = rec['premise_id']
            utility_short = 'DEF' if 'Duke' in rec['utility'] else 'TEC'
            queue_id = f"FLPSC-{utility_short}-{premise_id}"

            capacity_kw = rec['capacity_kw']
            if capacity_kw <= 0:
                continue

            # Parse COD date
            cod = rec.get('date_interconnected')

            record = {
                'queue_id': queue_id,
                'region': REGION,
                'name': None,
                'developer': None,
                'capacity_mw': capacity_kw / 1000.0,
                'capacity_kw': capacity_kw,
                'system_size_dc_kw': rec.get('capacity_dc_kw'),
                'system_size_ac_kw': capacity_kw,
                'type': 'Solar' if 'Solar' in rec.get('technology', 'Solar') else rec['technology'],
                'status': 'Operational',
                'raw_status': f"FL PSC 25-6.065 Net Metering",
                'state': 'FL',
                'county': rec.get('county'),
                'city': rec.get('city'),
                'utility': rec['utility'],
                'queue_date': None,
                'cod': cod,
                'customer_sector': 'Net Metering',
                'interconnection_program': 'FL PSC Rule 25-6.065',
            }
            record['row_hash'] = compute_hash(record)
            records.append(record)

        self.records = records
        logger.info(f"  Normalized {len(records):,} records")

        if records:
            from collections import Counter
            utilities = Counter(r['utility'] for r in records)
            counties = Counter(r['county'] for r in records if r['county'])
            logger.info(f"  By utility: {dict(utilities)}")
            logger.info(f"  Top counties: {counties.most_common(10)}")
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
        batch = []

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
                    # Always bump last_checked_at to prove we verified this row
                    cursor.execute(
                        'UPDATE projects SET last_checked_at = CURRENT_TIMESTAMP WHERE id = ?',
                        (existing['id'],)
                    )
                    if existing['row_hash'] != row_hash:
                        cursor.execute('''
                            UPDATE projects SET
                                capacity_mw = ?, capacity_kw = ?,
                                type = ?, status = ?, raw_status = ?,
                                state = ?, county = ?, city = ?, utility = ?,
                                cod = ?, customer_sector = ?,
                                system_size_dc_kw = ?, system_size_ac_kw = ?,
                                interconnection_program = ?, row_hash = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', (
                            rec['capacity_mw'], rec['capacity_kw'],
                            rec['type'], rec['status'], rec['raw_status'],
                            'FL', rec['county'], rec['city'], rec['utility'],
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
                            type, status, raw_status, state, county, city, utility,
                            cod, source, primary_source, sources,
                            customer_sector, system_size_dc_kw, system_size_ac_kw,
                            interconnection_program, row_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        queue_id, rec['region'], None, None,
                        rec['capacity_mw'], rec['capacity_kw'],
                        rec['type'], rec['status'], rec['raw_status'],
                        'FL', rec['county'], rec['city'], rec['utility'],
                        rec['cod'], SOURCE, SOURCE, sources_json,
                        rec['customer_sector'], rec['system_size_dc_kw'],
                        rec['system_size_ac_kw'], rec['interconnection_program'],
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

            if (stats['added'] + stats['updated'] + stats['unchanged']) % 10000 == 0:
                conn.commit()
                logger.info(f"  Progress: {stats['added'] + stats['updated'] + stats['unchanged']:,} processed")

        conn.commit()

        # Register programs
        for prog_key, prog_name, utility_name, count in [
            (f'{SOURCE}_duke', 'Duke Energy FL Net Metering (Rule 25-6.065)', 'Duke Energy Florida',
             sum(1 for r in records if r['utility'] == 'Duke Energy Florida')),
            (f'{SOURCE}_tampa', 'Tampa Electric Net Metering (Rule 25-6.065)', 'Tampa Electric',
             sum(1 for r in records if r['utility'] == 'Tampa Electric')),
        ]:
            if count > 0:
                cursor.execute('''
                    INSERT OR REPLACE INTO dg_programs (
                        program_key, program_name, state, utility, source_url,
                        refresh_method, last_refreshed, record_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    prog_key, prog_name, 'FL', utility_name,
                    'https://www.floridapsc.com/ElectricNaturalGas/CustomerOwnedRenewableEnergy',
                    'annual_pdf_parse', datetime.now().isoformat(), count
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

    def load(self, duke: bool = True, tampa: bool = True, dry_run: bool = False) -> Dict[str, int]:
        raw_data = self.fetch(duke=duke, tampa=tampa)
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
            'by_utility': dict(Counter(x['utility'] for x in r)),
            'by_county': dict(Counter(x['county'] for x in r if x['county']).most_common(15)),
            'by_type': dict(Counter(x['type'] for x in r)),
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="FL PSC Net Metering Loader")
    parser.add_argument('--stats', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--duke-only', action='store_true')
    parser.add_argument('--tampa-only', action='store_true')
    parser.add_argument('--db', type=str)
    args = parser.parse_args()

    loader = FLPSCLoader(db_path=Path(args.db) if args.db else None)
    duke = not args.tampa_only
    tampa = not args.duke_only

    print(f"\n{'='*60}")
    print(f"FL PSC Net Metering Loader — source: {SOURCE}")
    print(f"Duke: {'yes' if duke else 'skip'}, Tampa: {'yes' if tampa else 'skip'}")
    print(f"Target DB: {loader.db_path}")
    print(f"{'='*60}\n")

    results = loader.load(duke=duke, tampa=tampa, dry_run=args.dry_run)
    print(f"\nResults: +{results.get('added', 0):,} added, "
          f"~{results.get('updated', 0):,} updated, "
          f"={results.get('unchanged', 0):,} unchanged")

    if args.stats:
        stats = loader.get_stats()
        print(f"\nTotal: {stats.get('total', 0):,} records, {stats.get('total_mw', 0):,.1f} MW")
        print(f"By utility: {stats.get('by_utility', {})}")
        print(f"By type: {stats.get('by_type', {})}")
        print(f"Top counties: {stats.get('by_county', {})}")


if __name__ == '__main__':
    main()
