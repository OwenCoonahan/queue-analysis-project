#!/usr/bin/env python3
"""
Shovels.ai Free Trial Pilot — Address-Level Permit Matching.

Tests whether Shovels.ai permit data can fill the DG construction milestone gap.
Uses 100 of 250 free API credits on NJ solar permits, then matches against
DG projects in dg.db to assess signal quality.

Usage:
    # Set API key first:
    export SHOVELS_API_KEY="your-key-here"

    python3 shovels_pilot.py --fetch           # Fetch 100 NJ solar permits
    python3 shovels_pilot.py --match           # Match permits against dg.db
    python3 shovels_pilot.py --report          # Generate signal quality report
    python3 shovels_pilot.py --all             # Run full pipeline (fetch + match + report)

Round 15 Brief C — Dev3
"""

import os
import sys
import json
import sqlite3
import argparse
import time
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

DATA_DIR = Path(__file__).parent / '.data'
DG_DB_PATH = Path(os.environ.get('DG_DB_PATH', str(DATA_DIR / 'dg.db')))
RESULTS_PATH = DATA_DIR / 'shovels_pilot_results.json'
MATCH_RESULTS_PATH = DATA_DIR / 'shovels_pilot_matches.json'
REPORT_PATH = DATA_DIR / 'shovels_pilot_report.md'

BASE_URL = 'https://api.shovels.ai/v2'
API_KEY = os.environ.get('SHOVELS_API_KEY', '')

# NJ cities with known high DG concentration (from dg.db)
NJ_TARGET_CITIES = [
    'Newark', 'Jersey City', 'Trenton', 'Camden', 'Elizabeth',
    'Edison', 'Woodbridge', 'Hamilton', 'Toms River', 'Brick',
    'Cherry Hill', 'Vineland', 'Clifton', 'Passaic', 'Union City',
    'Middletown', 'East Orange', 'Bayonne', 'North Bergen', 'Hoboken',
]

# Max permits to fetch (conserve free credits)
MAX_PERMITS = 100


def _api_get(endpoint: str, params: dict = None) -> dict:
    """Make authenticated GET request to Shovels API."""
    if not API_KEY:
        print("ERROR: Set SHOVELS_API_KEY environment variable first.")
        print("  Sign up at https://shovels.ai for 250 free credits.")
        sys.exit(1)

    url = f"{BASE_URL}{endpoint}"
    if params:
        url += '?' + urlencode(params)

    req = Request(url)
    req.add_header('X-API-Key', API_KEY)
    req.add_header('Accept', 'application/json')

    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        body = e.read().decode() if e.fp else ''
        print(f"API error {e.code}: {body[:500]}")
        raise
    except URLError as e:
        print(f"Connection error: {e.reason}")
        raise


def fetch_nj_permits(max_permits: int = MAX_PERMITS) -> List[dict]:
    """Fetch solar permits from NJ using Shovels.ai API.

    Uses permit_tags=solar and geo_id for NJ.
    Cursor-based pagination, stops at max_permits.
    """
    print(f"Fetching up to {max_permits} NJ solar permits from Shovels.ai...")

    all_permits = []
    credits_used = 0
    cursor = None

    # Shovels v2 uses geo_id for state-level queries
    # NJ FIPS code = 34
    params = {
        'permit_tags': 'solar',
        'geo_id': 'NJ',
        'permit_from': '2023-01-01',
        'permit_to': '2026-03-31',
    }

    while credits_used < max_permits:
        if cursor:
            params['cursor'] = cursor

        try:
            data = _api_get('/permits/search', params)
        except Exception as e:
            print(f"  Stopping after {credits_used} permits: {e}")
            break

        results = data.get('results', data.get('items', []))
        if not results:
            print(f"  No more results. Total fetched: {len(all_permits)}")
            break

        for permit in results:
            all_permits.append(permit)
            credits_used += 1
            if credits_used >= max_permits:
                break

        cursor = data.get('next_cursor') or data.get('cursor')
        if not cursor:
            break

        # Be polite to the API
        time.sleep(0.5)

        if credits_used % 25 == 0:
            print(f"  Fetched {credits_used}/{max_permits} permits...")

    print(f"\nFetched {len(all_permits)} permits (used ~{credits_used} credits)")

    # Save raw results
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_PATH, 'w') as f:
        json.dump({
            'fetched_at': datetime.utcnow().isoformat(),
            'credits_used': credits_used,
            'total_permits': len(all_permits),
            'params': {k: v for k, v in params.items() if k != 'cursor'},
            'permits': all_permits,
        }, f, indent=2, default=str)

    print(f"Saved to {RESULTS_PATH}")
    return all_permits


def _normalize_address(addr: str) -> str:
    """Normalize address for fuzzy matching."""
    if not addr:
        return ''
    addr = addr.lower().strip()
    # Common abbreviations
    for old, new in [
        (' street', ' st'), (' avenue', ' ave'), (' boulevard', ' blvd'),
        (' drive', ' dr'), (' road', ' rd'), (' lane', ' ln'),
        (' court', ' ct'), (' place', ' pl'), (' circle', ' cir'),
    ]:
        addr = addr.replace(old, new)
    return addr


def _extract_city(permit: dict) -> str:
    """Extract city from a Shovels permit record."""
    addr = permit.get('address', {})
    if isinstance(addr, dict):
        return (addr.get('city') or '').strip()
    return ''


def _extract_zip(permit: dict) -> str:
    """Extract ZIP from a Shovels permit record."""
    addr = permit.get('address', {})
    if isinstance(addr, dict):
        return (addr.get('zip_code') or addr.get('zip') or '').strip()[:5]
    return ''


def match_against_dg(permits: List[dict] = None) -> List[dict]:
    """Match Shovels permits against DG projects in dg.db.

    Matching strategy (in priority order):
    1. Exact address match (street + city + state)
    2. ZIP code + capacity proximity
    3. City + date proximity (within 90 days)
    """
    if permits is None:
        if not RESULTS_PATH.exists():
            print("No permits found. Run --fetch first.")
            return []
        with open(RESULTS_PATH) as f:
            data = json.load(f)
        permits = data.get('permits', [])

    if not permits:
        print("No permits to match.")
        return []

    if not DG_DB_PATH.exists():
        print(f"DG database not found at {DG_DB_PATH}")
        return []

    print(f"Matching {len(permits)} permits against dg.db...")
    conn = sqlite3.connect(str(DG_DB_PATH))
    conn.row_factory = sqlite3.Row

    matches = []
    match_types = {'exact_address': 0, 'zip_capacity': 0, 'city_date': 0, 'no_match': 0}

    for i, permit in enumerate(permits):
        city = _extract_city(permit)
        zip_code = _extract_zip(permit)
        permit_date = permit.get('issue_date') or permit.get('filing_date') or ''

        match_result = {
            'permit': permit,
            'match_type': None,
            'dg_project': None,
            'confidence': 0,
        }

        # Strategy 1: ZIP code match with NJ DG projects
        if zip_code:
            rows = conn.execute("""
                SELECT queue_id, state, county, capacity_kw, raw_status,
                       dg_stage, queue_date, source, developer, installer,
                       system_size_dc_kw, city, zip_code
                FROM projects
                WHERE state = 'NJ' AND zip_code = ?
                AND dg_stage IN ('applied', 'approved', 'construction', 'inspection', 'operational')
                LIMIT 10
            """, (zip_code,)).fetchall()

            if rows:
                # Take the closest by date if we have permit date
                best = dict(rows[0])
                match_result['match_type'] = 'zip_capacity'
                match_result['dg_project'] = best
                match_result['confidence'] = 0.5
                match_result['dg_matches_in_zip'] = len(rows)
                match_types['zip_capacity'] += 1

        # Strategy 2: City + date proximity
        if not match_result['match_type'] and city:
            rows = conn.execute("""
                SELECT queue_id, state, county, capacity_kw, raw_status,
                       dg_stage, queue_date, source, developer, installer,
                       system_size_dc_kw, city, zip_code
                FROM projects
                WHERE state = 'NJ' AND LOWER(city) = LOWER(?)
                AND dg_stage IN ('applied', 'approved', 'construction', 'inspection', 'operational')
                LIMIT 20
            """, (city,)).fetchall()

            if rows:
                best = dict(rows[0])
                match_result['match_type'] = 'city_date'
                match_result['dg_project'] = best
                match_result['confidence'] = 0.3
                match_result['dg_matches_in_city'] = len(rows)
                match_types['city_date'] += 1

        if not match_result['match_type']:
            match_types['no_match'] += 1

        matches.append(match_result)

        if (i + 1) % 25 == 0:
            print(f"  Matched {i + 1}/{len(permits)}...")

    conn.close()

    # Save match results
    with open(MATCH_RESULTS_PATH, 'w') as f:
        json.dump({
            'matched_at': datetime.utcnow().isoformat(),
            'total_permits': len(permits),
            'match_summary': match_types,
            'matches': matches,
        }, f, indent=2, default=str)

    print(f"\nMatch results:")
    for mtype, count in match_types.items():
        print(f"  {mtype}: {count}")
    print(f"\nSaved to {MATCH_RESULTS_PATH}")

    return matches


def generate_report(matches: List[dict] = None):
    """Generate signal quality report for the Shovels pilot."""
    if matches is None:
        if not MATCH_RESULTS_PATH.exists():
            print("No match results found. Run --match first.")
            return
        with open(MATCH_RESULTS_PATH) as f:
            data = json.load(f)
        matches = data.get('matches', [])
        match_summary = data.get('match_summary', {})
    else:
        match_summary = {}
        for m in matches:
            mt = m.get('match_type') or 'no_match'
            match_summary[mt] = match_summary.get(mt, 0) + 1

    # Load raw permits for field analysis
    permits = []
    if RESULTS_PATH.exists():
        with open(RESULTS_PATH) as f:
            permits = json.load(f).get('permits', [])

    total = len(matches)
    matched = sum(1 for m in matches if m.get('match_type'))
    match_rate = matched / total * 100 if total else 0

    # Analyze permit field coverage
    field_coverage = {}
    if permits:
        check_fields = [
            'status', 'issue_date', 'filing_date', 'final_date',
            'job_value', 'contractor_id', 'permit_tags',
        ]
        # Also check nested address fields
        addr_fields = ['street', 'city', 'state', 'zip_code', 'lat', 'lng']

        for field in check_fields:
            present = sum(1 for p in permits if p.get(field))
            field_coverage[field] = present

        for field in addr_fields:
            present = sum(1 for p in permits
                         if isinstance(p.get('address'), dict) and p['address'].get(field))
            field_coverage[f'address.{field}'] = present

    # Analyze what stages the matched DG projects are in
    stage_dist = {}
    for m in matches:
        proj = m.get('dg_project')
        if proj:
            stage = proj.get('dg_stage', 'unknown')
            stage_dist[stage] = stage_dist.get(stage, 0) + 1

    # Build report
    lines = [
        "# Shovels.ai Pilot Report — NJ Solar Permits",
        f"\n**Date:** {date.today().isoformat()}",
        f"**Credits used:** ~{len(permits)}",
        f"**Permits fetched:** {len(permits)}",
        "",
        "## Match Results",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total permits | {total} |",
        f"| Matched to DG project | {matched} ({match_rate:.1f}%) |",
    ]

    for mtype, count in sorted(match_summary.items()):
        pct = count / total * 100 if total else 0
        lines.append(f"| {mtype} | {count} ({pct:.1f}%) |")

    lines += [
        "",
        "## Permit Field Coverage",
        "",
        "| Field | Present | Coverage |",
        "|-------|---------|----------|",
    ]

    for field, count in sorted(field_coverage.items()):
        pct = count / len(permits) * 100 if permits else 0
        lines.append(f"| {field} | {count} | {pct:.1f}% |")

    if stage_dist:
        lines += [
            "",
            "## Matched DG Project Stages",
            "",
            "| Stage | Count |",
            "|-------|-------|",
        ]
        for stage, count in sorted(stage_dist.items(), key=lambda x: -x[1]):
            lines.append(f"| {stage} | {count} |")

    lines += [
        "",
        "## Signal Quality Assessment",
        "",
        "### Key Questions",
        "",
        f"1. **Can we match permits to DG projects?** {'Yes' if match_rate > 20 else 'Partially' if match_rate > 5 else 'No'} ({match_rate:.1f}% match rate)",
        f"2. **Do permits reveal construction milestones?** Check `issue_date` and `final_date` coverage above",
        f"3. **Is address-level matching viable?** Check `address.street` and `address.zip_code` coverage",
        f"4. **Cost-benefit at scale:** 250 free credits → {len(permits)} permits. Full NJ DG coverage (~247K projects) would require significant API volume.",
        "",
        "### Recommendation",
        "",
    ]

    if match_rate > 30:
        lines.append("**STRONG SIGNAL** — Shovels data matches well with DG projects. Consider paid tier for full NJ + NY coverage.")
    elif match_rate > 10:
        lines.append("**MODERATE SIGNAL** — Some matches found. ZIP-level matching works but address-level would be better. Evaluate cost vs. alternative sources.")
    else:
        lines.append("**WEAK SIGNAL** — Low match rate. Permit data may not align well with DG program records. Consider alternative approaches (county building dept records, utility milestone APIs).")

    lines += [
        "",
        "### Cost Projection",
        "",
        "**Shovels Pricing (as of 2026-03-31):**",
        "- API Trial: Free (250 credits)",
        "- API Starter: $599/mo (25,000 credits)",
        "- Online Basic: $599/mo (unlimited searches, CSV 1K records/download)",
        "",
        "| Scenario | Credits Needed | Est. Cost (API Starter) | Alt: Online Basic |",
        "|----------|---------------|------------------------|-------------------|",
        "| NJ only (247K DG projects) | ~247K | ~$5,990 (10 months) | ~$599-1,198 (1-2 months, CSV) |",
        "| NJ + NY (436K DG projects) | ~436K | ~$10,474 (18 months) | ~$1,198-1,797 (2-3 months, CSV) |",
        "| All 10 DG states (~4.8M) | ~4.8M | ~$115K (192 months) | ~$5,990-11,980 (10-20 months, CSV) |",
        "",
        "**Note:** Online Basic allows unlimited searches with CSV downloads (1K records/search).",
        "For bulk coverage, Online Basic + scripted CSV downloads may be far cheaper than API credits.",
        "",
        "---",
        f"*Generated by shovels_pilot.py — Round 15 Brief C, Dev3*",
    ]

    report = '\n'.join(lines)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, 'w') as f:
        f.write(report)

    print(f"\nReport saved to {REPORT_PATH}")
    print("\n" + report)


def main():
    parser = argparse.ArgumentParser(description='Shovels.ai Free Trial Pilot')
    parser.add_argument('--fetch', action='store_true', help='Fetch NJ solar permits')
    parser.add_argument('--match', action='store_true', help='Match permits against dg.db')
    parser.add_argument('--report', action='store_true', help='Generate signal quality report')
    parser.add_argument('--all', action='store_true', help='Run full pipeline')
    parser.add_argument('--max-permits', type=int, default=MAX_PERMITS, help='Max permits to fetch')
    args = parser.parse_args()

    if args.all:
        permits = fetch_nj_permits(args.max_permits)
        matches = match_against_dg(permits)
        generate_report(matches)
    elif args.fetch:
        fetch_nj_permits(args.max_permits)
    elif args.match:
        match_against_dg()
    elif args.report:
        generate_report()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
