#!/usr/bin/env python3
"""
Permit Data Pilot — DG Phase 3 Layer 4 evaluation.

Loads solar permit data from free Socrata portals and matches against dg.db
to evaluate permit data as a high-confidence construction stage signal.

Target datasets:
  1. Chicago Building Permits (data.cityofchicago.org, ydr8-5enu) — 12K+ solar
  2. CT RSIP Solar (data.ct.gov, fvw8-89kt) — 48K residential solar enrollments
  3. NYSERDA Solar Projects (data.ny.gov, 3x8r-34rs) — 189K solar projects
  4. Cambridge MA Solar Permits (data.cambridgema.gov, whpw-w55x) — 713 permits

NJ Construction Permits (data.nj.gov) were evaluated but don't categorize solar —
permits fall under generic "Residential ALTER" with no way to filter.

Usage:
    python3 permit_loader.py                    # Fetch all + match
    python3 permit_loader.py --fetch            # Fetch only (no matching)
    python3 permit_loader.py --match            # Match only (use cached data)
    python3 permit_loader.py --stats            # Show stats
    python3 permit_loader.py --source chicago
    python3 permit_loader.py --source ct
    python3 permit_loader.py --source nyserda
    python3 permit_loader.py --source cambridge
"""

import argparse
import json
import logging
import re
import sqlite3
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
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
CACHE_DIR = Path(__file__).parent / '.cache' / 'permits'

# Socrata API endpoints
SOURCES = {
    'chicago': {
        'name': 'Chicago Building Permits',
        'domain': 'data.cityofchicago.org',
        'dataset_id': 'ydr8-5enu',
        'base_url': 'https://data.cityofchicago.org/resource/ydr8-5enu.json',
        'solar_filter': "$where=upper(work_description) like '%SOLAR%'",
        'limit': 50000,
    },
    'ct': {
        'name': 'CT RSIP Solar Enrollments',
        'domain': 'data.ct.gov',
        'dataset_id': 'fvw8-89kt',
        'base_url': 'https://data.ct.gov/resource/fvw8-89kt.json',
        'solar_filter': '',  # Already all solar
        'limit': 50000,
    },
    'nyserda': {
        'name': 'NYSERDA Solar Projects',
        'domain': 'data.ny.gov',
        'dataset_id': '3x8r-34rs',
        'base_url': 'https://data.ny.gov/resource/3x8r-34rs.json',
        'solar_filter': '',  # Already all solar
        'limit': 50000,
    },
    'cambridge': {
        'name': 'Cambridge MA Solar Permits',
        'domain': 'data.cambridgema.gov',
        'dataset_id': 'whpw-w55x',
        'base_url': 'https://data.cambridgema.gov/resource/whpw-w55x.json',
        'solar_filter': '',  # Already all solar
        'limit': 5000,
    },
}


def ensure_permits_table(db: sqlite3.Connection):
    """Create the permits table if it doesn't exist."""
    db.execute("""
        CREATE TABLE IF NOT EXISTS permits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            permit_id TEXT,
            source TEXT NOT NULL,
            address TEXT,
            city TEXT,
            county TEXT,
            state TEXT,
            zip_code TEXT,
            permit_type TEXT,
            work_description TEXT,
            status TEXT,
            issue_date TEXT,
            completion_date TEXT,
            inspection_date TEXT,
            capacity_kw REAL,
            contractor TEXT,
            total_cost REAL,
            latitude REAL,
            longitude REAL,
            raw_data TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(permit_id, source)
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS permit_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            permit_id TEXT NOT NULL,
            permit_source TEXT NOT NULL,
            dg_queue_id TEXT NOT NULL,
            dg_region TEXT NOT NULL,
            match_method TEXT NOT NULL,
            match_confidence REAL NOT NULL,
            city_match INTEGER DEFAULT 0,
            capacity_match INTEGER DEFAULT 0,
            date_match INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(permit_id, permit_source, dg_queue_id)
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_permits_city_state ON permits(city, state)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_permits_source ON permits(source)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_permit_matches_dg ON permit_matches(dg_queue_id)")
    db.commit()


def _http_get_json(url: str, timeout: int = 120) -> list:
    """Fetch JSON from a URL using stdlib urllib."""
    req = urllib.request.Request(url, headers={'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))


def fetch_socrata(source_key: str) -> List[dict]:
    """Fetch solar permit data from a Socrata dataset."""
    src = SOURCES[source_key]
    logger.info(f"Fetching {src['name']} from {src['domain']}...")

    all_records = []
    offset = 0
    limit = min(src['limit'], 10000)  # Socrata page size

    while True:
        params = {
            '$limit': str(limit),
            '$offset': str(offset),
            '$order': ':id',
        }

        # Add solar filter if present
        if src['solar_filter']:
            filter_str = src['solar_filter']
            if filter_str.startswith('$where='):
                params['$where'] = filter_str[7:]

        url = src['base_url'] + '?' + urllib.parse.urlencode(params)

        try:
            records = _http_get_json(url)
        except urllib.error.HTTPError as e:
            if e.code == 400:
                logger.warning(f"Socrata query error for {source_key}: {e}")
                logger.info("Trying alternative query approach...")
                records = _fetch_socrata_fallback(source_key, offset, limit)
                if records is None:
                    break
            else:
                raise

        if not records:
            break

        all_records.extend(records)
        logger.info(f"  Fetched {len(all_records)} records so far...")

        if len(records) < limit:
            break
        offset += limit

        if len(all_records) >= src['limit']:
            logger.info(f"  Reached limit of {src['limit']} records")
            break

    logger.info(f"  Total: {len(all_records)} solar permit records from {src['name']}")

    # Cache to disk
    cache_file = CACHE_DIR / f"{source_key}_permits.json"
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(cache_file, 'w') as f:
        json.dump(all_records, f)
    logger.info(f"  Cached to {cache_file}")

    return all_records


def _fetch_socrata_fallback(source_key: str, offset: int, limit: int) -> Optional[List[dict]]:
    """Fallback: fetch without filter, then filter client-side."""
    src = SOURCES[source_key]
    # Try full-text search as fallback
    params = {
        '$limit': str(limit),
        '$offset': str(offset),
        '$order': ':id',
        '$q': 'SOLAR',
    }
    url = src['base_url'] + '?' + urllib.parse.urlencode(params)
    try:
        return _http_get_json(url)
    except Exception as e:
        logger.error(f"Fallback also failed for {source_key}: {e}")
        return None


def normalize_chicago(records: List[dict]) -> List[dict]:
    """Normalize Chicago building permit records."""
    normalized = []
    for r in records:
        permit = {
            'permit_id': r.get('id', r.get('permit_', '')),
            'source': 'chicago_permits',
            'address': r.get('street_address', ''),
            'city': 'Chicago',
            'county': 'Cook',
            'state': 'IL',
            'zip_code': r.get('zip_code', ''),
            'permit_type': r.get('permit_type', ''),
            'work_description': r.get('work_description', ''),
            'status': r.get('permit_status', ''),
            'issue_date': _parse_date(r.get('issue_date', '')),
            'completion_date': None,
            'inspection_date': None,
            'capacity_kw': _extract_capacity_from_description(r.get('work_description', '')),
            'contractor': r.get('contractor_1_name', ''),
            'total_cost': _parse_float(r.get('reported_cost', 0)),
            'latitude': _parse_float(r.get('latitude')),
            'longitude': _parse_float(r.get('longitude')),
            'raw_data': json.dumps(r),
        }
        if permit['permit_id']:
            normalized.append(permit)
    return normalized


def normalize_ct(records: List[dict]) -> List[dict]:
    """Normalize CT RSIP solar enrollment records."""
    normalized = []
    for r in records:
        permit = {
            'permit_id': r.get('project_number', r.get('application_id', f"CT-{len(normalized)}")),
            'source': 'ct_rsip',
            'address': None,  # CT RSIP doesn't publish addresses
            'city': r.get('municipality', r.get('town', '')),
            'county': r.get('county', ''),
            'state': 'CT',
            'zip_code': r.get('zip_code', r.get('zip', '')),
            'permit_type': 'Residential Solar',
            'work_description': f"RSIP Solar Installation - {r.get('kw_stc', '')} kW",
            'status': r.get('project_status', r.get('status', '')),
            'issue_date': _parse_date(r.get('approved_date', r.get('approval_date', ''))),
            'completion_date': _parse_date(r.get('completed_date', r.get('completion_date', ''))),
            'inspection_date': None,
            'capacity_kw': _parse_float(r.get('kw_stc', r.get('system_size_kw', 0))),
            'contractor': r.get('contractor', r.get('installer', '')),
            'total_cost': _parse_float(r.get('total_system_cost', 0)),
            'latitude': None,
            'longitude': None,
            'raw_data': json.dumps(r),
        }
        normalized.append(permit)
    return normalized


def normalize_nyserda(records: List[dict]) -> List[dict]:
    """Normalize NYSERDA solar project records."""
    normalized = []
    for r in records:
        permit = {
            'permit_id': r.get('project_number', r.get('solicitation_', f"NYSERDA-{len(normalized)}")),
            'source': 'nyserda_solar',
            'address': None,
            'city': (r.get('city', '') or '').strip().title(),
            'county': (r.get('county', '') or '').strip().title(),
            'state': 'NY',
            'zip_code': r.get('zip_code', r.get('zip', '')),
            'permit_type': r.get('sector', 'Solar'),
            'work_description': f"NYSERDA Solar - {r.get('project_status', '')}",
            'status': r.get('project_status', ''),
            'issue_date': _parse_date(r.get('date_application_received', '')),
            'completion_date': _parse_date(r.get('date_install', r.get('date_completed', ''))),
            'inspection_date': None,
            'capacity_kw': _parse_float(r.get('totalnameplatekwdc', r.get('total_nameplate_kw_dc', 0))),
            'contractor': r.get('contractor', r.get('installer', '')),
            'total_cost': _parse_float(r.get('project_cost', 0)),
            'latitude': _parse_float(r.get('latitude')),
            'longitude': _parse_float(r.get('longitude')),
            'raw_data': json.dumps(r),
        }
        normalized.append(permit)
    return normalized


def normalize_cambridge(records: List[dict]) -> List[dict]:
    """Normalize Cambridge MA solar permit records."""
    normalized = []
    for r in records:
        permit = {
            'permit_id': r.get('permitnumber', r.get('permit_number', f"CAMB-{len(normalized)}")),
            'source': 'cambridge_solar',
            'address': r.get('full_address', r.get('address', '')),
            'city': 'Cambridge',
            'county': 'Middlesex',
            'state': 'MA',
            'zip_code': '',
            'permit_type': 'Solar Installation',
            'work_description': f"Solar {r.get('solar_energy_type', 'PV')} - {r.get('mount_type', '')}",
            'status': r.get('status', ''),
            'issue_date': _parse_date(r.get('issue_date', r.get('issued_date', ''))),
            'completion_date': None,
            'inspection_date': None,
            'capacity_kw': _parse_float(r.get('watt_capacity', r.get('pv_capacity_kw', 0))),
            'contractor': r.get('contractor', ''),
            'total_cost': _parse_float(r.get('total_cost', 0)),
            'latitude': None,
            'longitude': None,
            'raw_data': json.dumps(r),
        }
        # Cambridge uses watts, convert if > 100 (likely watts not kW)
        if permit['capacity_kw'] and permit['capacity_kw'] > 100:
            permit['capacity_kw'] = permit['capacity_kw'] / 1000
        normalized.append(permit)
    return normalized


NORMALIZERS = {
    'chicago': normalize_chicago,
    'ct': normalize_ct,
    'nyserda': normalize_nyserda,
    'cambridge': normalize_cambridge,
}


def store_permits(db: sqlite3.Connection, permits: List[dict]) -> Dict[str, int]:
    """Store normalized permits in the database."""
    added = 0
    updated = 0
    skipped = 0

    for p in permits:
        try:
            db.execute("""
                INSERT INTO permits (
                    permit_id, source, address, city, county, state, zip_code,
                    permit_type, work_description, status, issue_date, completion_date,
                    inspection_date, capacity_kw, contractor, total_cost,
                    latitude, longitude, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(permit_id, source) DO UPDATE SET
                    status = excluded.status,
                    completion_date = excluded.completion_date,
                    inspection_date = excluded.inspection_date
            """, (
                p['permit_id'], p['source'], p['address'], p['city'], p['county'],
                p['state'], p['zip_code'], p['permit_type'], p['work_description'],
                p['status'], p['issue_date'], p['completion_date'], p['inspection_date'],
                p['capacity_kw'], p['contractor'], p['total_cost'],
                p['latitude'], p['longitude'], p['raw_data'],
            ))
            if db.total_changes:
                added += 1
        except sqlite3.IntegrityError:
            skipped += 1

    db.commit()
    return {'added': added, 'updated': updated, 'skipped': skipped}


def match_permits_to_dg(dg_db: sqlite3.Connection) -> Dict[str, int]:
    """Match permits against DG projects in dg.db using batch approach.

    Loads all pre-operational DG projects into memory indexed by (city, state),
    then matches each permit against candidates in O(1) city lookup.

    Matching strategies:
    1. City + capacity (±20%) + date window (±6 months) → confidence 0.8
    2. City + capacity (±20%) → confidence 0.55
    3. City + contractor/installer match → confidence 0.45
    """
    logger.info("Matching permits to DG projects...")

    # Load permits from permits table
    permits = dg_db.execute("SELECT * FROM permits").fetchall()
    cols = [d[0] for d in dg_db.execute("SELECT * FROM permits LIMIT 0").description]
    permits = [dict(zip(cols, p)) for p in permits]

    if not permits:
        logger.warning("No permits to match")
        return {'total_permits': 0, 'matched': 0, 'unmatched': 0}

    # Get the states we need from permits
    permit_states = set()
    for p in permits:
        s = (p.get('state') or '').strip().upper()
        if s:
            permit_states.add(s)
            permit_states.add(_state_full_name(s))

    if not permit_states:
        logger.warning("No permit states found")
        return {'total_permits': len(permits), 'matched': 0, 'unmatched': len(permits)}

    # Batch-load pre-operational DG projects for relevant states
    placeholders = ','.join('?' * len(permit_states))
    logger.info(f"Loading pre-operational DG projects for states: {permit_states}...")
    dg_rows = dg_db.execute(f"""
        SELECT queue_id, region, city, capacity_kw, queue_date, installer, dg_stage, state
        FROM projects
        WHERE UPPER(state) IN ({placeholders})
          AND dg_stage IN ('applied', 'approved', 'construction', 'inspection')
    """, list(permit_states)).fetchall()

    logger.info(f"Loaded {len(dg_rows):,} pre-operational DG projects")

    # Index by (city_upper, state_abbrev)
    from collections import defaultdict
    dg_by_city: Dict[tuple, list] = defaultdict(list)
    for row in dg_rows:
        qid, region, city, cap, qdate, installer, stage, state = row
        city_key = (city or '').strip().upper()
        state_key = (state or '').strip().upper()
        # Normalize state to abbreviation for consistent lookup
        state_abbrev = _state_abbrev(state_key)
        if city_key:
            dg_by_city[(city_key, state_abbrev)].append({
                'queue_id': qid, 'region': region, 'capacity_kw': cap,
                'queue_date': qdate, 'installer': installer, 'dg_stage': stage,
            })

    logger.info(f"Indexed {len(dg_by_city):,} unique city/state combinations")

    # Clear old matches
    dg_db.execute("DELETE FROM permit_matches")

    matched = 0
    unmatched = 0
    batch_inserts = []

    for i, permit in enumerate(permits):
        if i > 0 and i % 10000 == 0:
            logger.info(f"  Processed {i:,}/{len(permits):,} permits, {matched:,} matched...")

        p_city = (permit.get('city') or '').strip().upper()
        p_state = (permit.get('state') or '').strip().upper()
        p_cap = permit.get('capacity_kw')
        p_date = permit.get('issue_date')
        p_contractor = (permit.get('contractor') or '').strip().upper()

        if not p_city or not p_state:
            unmatched += 1
            continue

        candidates = dg_by_city.get((p_city, p_state), [])
        if not candidates:
            unmatched += 1
            continue

        best_match = None
        best_confidence = 0
        best_method = ''

        for c in candidates:
            confidence = 0.3  # Base: same city + state + pre-operational
            method_parts = ['city']

            # Capacity match (±20%)
            c_cap = c['capacity_kw']
            if p_cap and c_cap and c_cap > 0:
                ratio = p_cap / c_cap
                if 0.8 <= ratio <= 1.2:
                    confidence += 0.25
                    method_parts.append('capacity')

            # Date match (±6 months)
            c_date = c['queue_date']
            if p_date and c_date:
                try:
                    pd = datetime.strptime(p_date[:10], '%Y-%m-%d')
                    cd = datetime.strptime(c_date[:10], '%Y-%m-%d')
                    if abs((pd - cd).days) <= 180:
                        confidence += 0.2
                        method_parts.append('date')
                except (ValueError, TypeError):
                    pass

            # Contractor/installer match
            c_inst = (c['installer'] or '').strip().upper()
            if p_contractor and c_inst and (
                p_contractor in c_inst or c_inst in p_contractor
            ):
                confidence += 0.15
                method_parts.append('installer')

            if confidence > best_confidence:
                best_confidence = confidence
                best_match = c
                best_method = '+'.join(method_parts)

        if best_match and best_confidence >= 0.4:
            batch_inserts.append((
                permit['permit_id'], permit['source'],
                best_match['queue_id'], best_match['region'],
                best_method, best_confidence,
                1 if 'city' in best_method else 0,
                1 if 'capacity' in best_method else 0,
                1 if 'date' in best_method else 0,
            ))
            matched += 1
        else:
            unmatched += 1

    # Batch insert all matches
    if batch_inserts:
        dg_db.executemany("""
            INSERT OR REPLACE INTO permit_matches
            (permit_id, permit_source, dg_queue_id, dg_region,
             match_method, match_confidence,
             city_match, capacity_match, date_match)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch_inserts)

    dg_db.commit()

    result = {
        'total_permits': len(permits),
        'matched': matched,
        'unmatched': unmatched,
        'match_rate': round(matched / len(permits) * 100, 1) if permits else 0,
    }
    logger.info(f"Match results: {matched}/{len(permits)} ({result['match_rate']}%)")
    return result


def show_stats(db: sqlite3.Connection):
    """Show permit data and match statistics."""
    print("\n" + "=" * 70)
    print("PERMIT DATA PILOT — STATISTICS")
    print("=" * 70)

    # Permit counts by source
    rows = db.execute("""
        SELECT source, COUNT(*) as cnt,
               COUNT(capacity_kw) as with_capacity,
               COUNT(issue_date) as with_date,
               COUNT(city) as with_city
        FROM permits GROUP BY source
    """).fetchall()

    if not rows:
        print("\nNo permit data loaded yet. Run: python3 permit_loader.py --fetch")
        return

    print("\n--- Permits by Source ---")
    print(f"{'Source':<25} {'Count':>8} {'w/Capacity':>12} {'w/Date':>10} {'w/City':>10}")
    print("-" * 70)
    total = 0
    for source, cnt, cap, dt, city in rows:
        print(f"{source:<25} {cnt:>8,} {cap:>12,} {dt:>10,} {city:>10,}")
        total += cnt
    print(f"{'TOTAL':<25} {total:>8,}")

    # Match stats
    matches = db.execute("""
        SELECT permit_source, COUNT(*) as cnt,
               ROUND(AVG(match_confidence), 3) as avg_conf,
               SUM(CASE WHEN match_confidence >= 0.7 THEN 1 ELSE 0 END) as high_conf,
               SUM(CASE WHEN match_confidence >= 0.5 AND match_confidence < 0.7 THEN 1 ELSE 0 END) as med_conf,
               SUM(CASE WHEN match_confidence < 0.5 THEN 1 ELSE 0 END) as low_conf
        FROM permit_matches GROUP BY permit_source
    """).fetchall()

    if matches:
        print("\n--- Match Results ---")
        print(f"{'Source':<25} {'Matched':>8} {'Avg Conf':>10} {'High':>8} {'Med':>8} {'Low':>8}")
        print("-" * 70)
        for src, cnt, avg, high, med, low in matches:
            total_src = db.execute("SELECT COUNT(*) FROM permits WHERE source = ?", (src,)).fetchone()[0]
            rate = round(cnt / total_src * 100, 1) if total_src > 0 else 0
            print(f"{src:<25} {cnt:>8,} {avg:>10.3f} {high:>8,} {med:>8,} {low:>8,}  ({rate}%)")

        # Match method breakdown
        methods = db.execute("""
            SELECT match_method, COUNT(*) as cnt
            FROM permit_matches GROUP BY match_method ORDER BY cnt DESC
        """).fetchall()
        print("\n--- Match Methods ---")
        for method, cnt in methods:
            print(f"  {method}: {cnt:,}")

        # Sample matches
        samples = db.execute("""
            SELECT pm.permit_id, pm.dg_queue_id, pm.match_method, pm.match_confidence,
                   p.city, p.capacity_kw, p.issue_date
            FROM permit_matches pm
            JOIN permits p ON pm.permit_id = p.permit_id AND pm.permit_source = p.source
            ORDER BY pm.match_confidence DESC
            LIMIT 10
        """).fetchall()
        if samples:
            print("\n--- Top 10 Matches (by confidence) ---")
            for pid, qid, method, conf, city, cap, dt in samples:
                cap_str = f"{cap:.1f}kW" if cap else "N/A"
                print(f"  Permit {pid} → DG {qid}  [{method}] conf={conf:.2f}  city={city} cap={cap_str} date={dt}")

    # DG stage distribution of matched projects
    stage_dist = db.execute("""
        SELECT p.dg_stage, COUNT(*) as cnt
        FROM permit_matches pm
        JOIN projects p ON pm.dg_queue_id = p.queue_id
        GROUP BY p.dg_stage ORDER BY cnt DESC
    """).fetchall()
    if stage_dist:
        print("\n--- DG Stage of Matched Projects ---")
        for stage, cnt in stage_dist:
            print(f"  {stage}: {cnt:,}")

    print()


# ─── Helpers ───────────────────────────────────────────────────────────

def _clean_city(city: str) -> str:
    """Normalize city name."""
    if not city:
        return ''
    return city.strip().title()

def _parse_date(val) -> Optional[str]:
    """Parse various date formats to YYYY-MM-DD."""
    if not val:
        return None
    val = str(val).strip()
    if not val:
        return None

    # Socrata ISO format: 2024-01-15T00:00:00.000
    if 'T' in val:
        return val[:10]

    for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y', '%Y%m%d'):
        try:
            return datetime.strptime(val, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None

def _parse_float(val) -> Optional[float]:
    """Parse a value to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def _extract_capacity_from_description(desc: str) -> Optional[float]:
    """Try to extract kW capacity from a work description string."""
    if not desc:
        return None
    desc = desc.upper()
    # Look for patterns like "10.5 KW", "10.5KW", "10500 WATT"
    kw_match = re.search(r'(\d+\.?\d*)\s*KW', desc)
    if kw_match:
        return float(kw_match.group(1))
    watt_match = re.search(r'(\d+)\s*WATT', desc)
    if watt_match:
        return float(watt_match.group(1)) / 1000
    return None

def _state_full_name(abbrev: str) -> str:
    """Return full state name for matching."""
    names = {
        'NJ': 'NEW JERSEY', 'CT': 'CONNECTICUT', 'IL': 'ILLINOIS',
        'NY': 'NEW YORK', 'CA': 'CALIFORNIA', 'MA': 'MASSACHUSETTS',
    }
    return names.get(abbrev.upper(), abbrev)

def _state_abbrev(state: str) -> str:
    """Return 2-letter abbreviation for a state."""
    abbrevs = {
        'NEW JERSEY': 'NJ', 'CONNECTICUT': 'CT', 'ILLINOIS': 'IL',
        'NEW YORK': 'NY', 'CALIFORNIA': 'CA', 'MASSACHUSETTS': 'MA',
        'NJ': 'NJ', 'CT': 'CT', 'IL': 'IL', 'NY': 'NY', 'CA': 'CA', 'MA': 'MA',
    }
    return abbrevs.get(state.upper(), state.upper()[:2])


def main():
    parser = argparse.ArgumentParser(description='Permit Data Pilot — DG Phase 3')
    parser.add_argument('--fetch', action='store_true', help='Fetch permits only')
    parser.add_argument('--match', action='store_true', help='Match permits to DG only')
    parser.add_argument('--stats', action='store_true', help='Show statistics')
    parser.add_argument('--source', choices=list(SOURCES.keys()), help='Single source')
    args = parser.parse_args()

    # Default: do everything
    do_fetch = args.fetch or (not args.match and not args.stats)
    do_match = args.match or (not args.fetch and not args.stats)
    do_stats = args.stats or (not args.fetch and not args.match)

    if not DG_DB_PATH.exists():
        logger.error(f"dg.db not found at {DG_DB_PATH}")
        sys.exit(1)

    db = sqlite3.connect(str(DG_DB_PATH))
    ensure_permits_table(db)

    sources_to_process = [args.source] if args.source else list(SOURCES.keys())

    if do_fetch:
        for source_key in sources_to_process:
            try:
                records = fetch_socrata(source_key)
                if records:
                    normalizer = NORMALIZERS[source_key]
                    normalized = normalizer(records)
                    result = store_permits(db, normalized)
                    logger.info(f"{source_key}: stored {result['added']} permits")
                else:
                    logger.warning(f"{source_key}: no records fetched")
            except Exception as e:
                logger.error(f"Error fetching {source_key}: {e}")

    if do_match:
        match_result = match_permits_to_dg(db)
        logger.info(f"Matching complete: {match_result}")

    if do_stats:
        show_stats(db)

    db.close()


if __name__ == '__main__':
    main()
