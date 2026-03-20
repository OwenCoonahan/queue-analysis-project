#!/usr/bin/env python3
"""
Data Health Monitor

Shows the freshness, completeness, and quality of all data sources
at a glance. Run this to know exactly what's current and what's stale.

Usage:
    python3 data_health.py              # Full health report
    python3 data_health.py --brief      # One-line per source
    python3 data_health.py --json       # JSON output (for dashboards)
    python3 data_health.py --stale      # Only show stale sources
    python3 data_health.py --html       # Generate HTML health dashboard
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import sys

DATA_DIR = Path(__file__).parent / '.data'
DB_PATH = DATA_DIR / 'queue.db'
CACHE_DIR = Path(__file__).parent / '.cache'


# Expected refresh frequencies per source (in days)
SOURCE_CONFIG = {
    # ISO queue data
    'miso_api': {
        'name': 'MISO Queue',
        'type': 'queue',
        'table': 'projects',
        'expected_refresh_days': 1,
        'method': 'API (automated)',
        'url': 'misoenergy.org/api/giqueue/getprojects',
    },
    'nyiso': {
        'name': 'NYISO Queue',
        'type': 'queue',
        'table': 'projects',
        'expected_refresh_days': 1,
        'method': 'Excel download (automated)',
        'url': 'nyiso.com',
    },
    'pjm_direct': {
        'name': 'PJM Queue',
        'type': 'queue',
        'table': 'projects',
        'expected_refresh_days': 7,
        'method': 'Excel (MANUAL download)',
        'url': 'pjm.com/planning/services-requests/interconnection-queues',
    },
    'ercot': {
        'name': 'ERCOT Queue',
        'type': 'queue',
        'table': 'projects',
        'expected_refresh_days': 7,
        'method': 'Excel download (automated)',
        'url': 'ercot.com',
    },
    'caiso': {
        'name': 'CAISO Queue',
        'type': 'queue',
        'table': 'projects',
        'expected_refresh_days': 1,
        'method': 'Excel download (automated)',
        'url': 'caiso.com',
    },
    'spp': {
        'name': 'SPP Queue',
        'type': 'queue',
        'table': 'projects',
        'expected_refresh_days': 1,
        'method': 'gridstatus (automated)',
        'url': 'spp.org',
    },
    'isone': {
        'name': 'ISO-NE Queue',
        'type': 'queue',
        'table': 'projects',
        'expected_refresh_days': 1,
        'method': 'gridstatus (automated)',
        'url': 'iso-ne.com',
    },
    'lbl': {
        'name': 'LBL Historical',
        'type': 'queue',
        'table': 'projects',
        'expected_refresh_days': 365,
        'method': 'Annual download (manual)',
        'url': 'emp.lbl.gov/queues',
    },
    # Permit/generator data
    'eia_860m': {
        'name': 'EIA-860M Monthly',
        'type': 'permits',
        'table': 'permits',
        'expected_refresh_days': 30,
        'method': 'Excel download (automated)',
        'url': 'eia.gov/electricity/data/eia860m/',
    },
    'eia_permits': {
        'name': 'EIA Planned Generators',
        'type': 'permits',
        'table': 'permits',
        'expected_refresh_days': 365,
        'method': 'Excel download (manual)',
        'url': 'eia.gov/electricity/data/eia860/',
    },
    'cec_permits': {
        'name': 'California CEC',
        'type': 'permits',
        'table': 'permits',
        'expected_refresh_days': 30,
        'method': 'Web scrape (automated)',
        'url': 'energy.ca.gov',
    },
    'cpuc_permits': {
        'name': 'California CPUC',
        'type': 'permits',
        'table': 'permits',
        'expected_refresh_days': 90,
        'method': 'Excel (MANUAL download)',
        'url': 'cpuc.ca.gov',
    },
    'nyserda_permits': {
        'name': 'NYSERDA NY-Sun',
        'type': 'permits',
        'table': 'permits',
        'expected_refresh_days': 30,
        'method': 'CSV download (automated)',
        'url': 'nyserda.ny.gov',
    },
    # Market data
    'lmp': {
        'name': 'LMP Prices',
        'type': 'market',
        'table': 'market',
        'expected_refresh_days': 30,
        'method': 'Benchmark data',
    },
    'capacity': {
        'name': 'Capacity Prices',
        'type': 'market',
        'table': 'market',
        'expected_refresh_days': 90,
        'method': 'Benchmark data',
    },
    'permits': {
        'name': 'Permitting Rules',
        'type': 'market',
        'table': 'market',
        'expected_refresh_days': 90,
        'method': 'Benchmark data',
    },
}

# Staleness thresholds
FRESHNESS_THRESHOLDS = {
    'fresh': 1.0,      # Within expected refresh window = fresh
    'aging': 2.0,      # Up to 2x expected = aging
    'stale': float('inf'),  # Beyond 2x = stale
}


def get_db_connection():
    """Get database connection."""
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_refresh_log() -> Dict[str, Dict]:
    """Get last refresh time and status for each source."""
    conn = get_db_connection()
    if not conn:
        return {}

    cursor = conn.cursor()

    # Get latest successful refresh per source
    cursor.execute('''
        SELECT source,
               MAX(completed_at) as last_refresh,
               (SELECT rows_processed FROM refresh_log r2
                WHERE r2.source = r1.source AND r2.status = 'success'
                ORDER BY completed_at DESC LIMIT 1) as rows_processed,
               (SELECT rows_added FROM refresh_log r2
                WHERE r2.source = r1.source AND r2.status = 'success'
                ORDER BY completed_at DESC LIMIT 1) as rows_added,
               (SELECT rows_updated FROM refresh_log r2
                WHERE r2.source = r1.source AND r2.status = 'success'
                ORDER BY completed_at DESC LIMIT 1) as rows_updated
        FROM refresh_log r1
        WHERE status = 'success'
        GROUP BY source
    ''')

    results = {}
    for row in cursor.fetchall():
        results[row['source']] = {
            'last_refresh': row['last_refresh'],
            'rows_processed': row['rows_processed'],
            'rows_added': row['rows_added'],
            'rows_updated': row['rows_updated'],
        }

    # Get latest failed refresh per source
    cursor.execute('''
        SELECT source,
               MAX(completed_at) as last_error,
               error_message
        FROM refresh_log
        WHERE status = 'error'
        GROUP BY source
    ''')

    for row in cursor.fetchall():
        if row['source'] in results:
            results[row['source']]['last_error'] = row['last_error']
            results[row['source']]['error_message'] = row['error_message']

    conn.close()
    return results


def get_source_counts() -> Dict[str, Dict]:
    """Get record counts per source from actual tables."""
    conn = get_db_connection()
    if not conn:
        return {}

    cursor = conn.cursor()
    results = {}

    # Map from table source names to config source names
    # (table stores data with one name, refresh_log may use another)
    TABLE_TO_CONFIG = {
        'eia_860': 'eia_permits',
        'cec': 'cec_permits',
        'nyserda': 'nyserda_permits',
        'cpuc_rps': 'cpuc_permits',
    }

    # Projects table
    cursor.execute('''
        SELECT source, COUNT(*) as count, SUM(capacity_mw) as total_mw,
               COUNT(DISTINCT developer) as developers,
               MIN(updated_at) as oldest_record,
               MAX(updated_at) as newest_record
        FROM projects
        GROUP BY source
    ''')
    for row in cursor.fetchall():
        config_key = TABLE_TO_CONFIG.get(row['source'], row['source'])
        if config_key in results:
            # Merge counts for sources that map to the same config key
            results[config_key]['count'] += row['count']
            results[config_key]['total_mw'] += (row['total_mw'] or 0)
        else:
            results[config_key] = {
                'table': 'projects',
                'count': row['count'],
                'total_mw': row['total_mw'] or 0,
                'developers': row['developers'],
                'oldest_record': row['oldest_record'],
                'newest_record': row['newest_record'],
            }

    # Permits table
    cursor.execute('''
        SELECT source, COUNT(*) as count, SUM(capacity_mw) as total_mw,
               COUNT(DISTINCT developer) as developers,
               MIN(updated_at) as oldest_record,
               MAX(updated_at) as newest_record
        FROM permits
        GROUP BY source
    ''')
    for row in cursor.fetchall():
        config_key = TABLE_TO_CONFIG.get(row['source'], row['source'])
        results[config_key] = {
            'table': 'permits',
            'count': row['count'],
            'total_mw': row['total_mw'] or 0,
            'developers': row['developers'],
            'oldest_record': row['oldest_record'],
            'newest_record': row['newest_record'],
        }

    conn.close()
    return results


def get_cache_status() -> Dict[str, Dict]:
    """Check freshness of cached files."""
    results = {}

    cache_files = {
        'miso_api': CACHE_DIR / 'miso_api_cache.json',
        'nyiso': CACHE_DIR / 'nyiso_queue_direct.xlsx',
        'pjm': CACHE_DIR / 'pjm_planning_queues.xlsx',
        'ercot': CACHE_DIR / 'ercot_gis_report.xlsx',
        'caiso': CACHE_DIR / 'caiso_queue_direct.xlsx',
        'eia860m': CACHE_DIR / 'eia860m' / 'eia860m_latest.xlsx',
        'eia_annual': CACHE_DIR / 'eia' / '3_1_Generator_Y2024.xlsx',
        'lbl': CACHE_DIR / 'lbl_queued_up.xlsx',
    }

    for name, path in cache_files.items():
        if path.exists():
            mtime = datetime.fromtimestamp(path.stat().st_mtime)
            age_days = (datetime.now() - mtime).total_seconds() / 86400
            size_mb = path.stat().st_size / (1024 * 1024)
            results[name] = {
                'path': str(path),
                'modified': mtime.strftime('%Y-%m-%d %H:%M'),
                'age_days': round(age_days, 1),
                'size_mb': round(size_mb, 1),
            }
        else:
            results[name] = {
                'path': str(path),
                'modified': None,
                'age_days': None,
                'size_mb': None,
                'missing': True,
            }

    return results


def assess_freshness(source: str, last_refresh: str) -> Tuple[str, float]:
    """
    Assess freshness of a source.

    Returns:
        Tuple of (status, age_days)
        status is one of: 'fresh', 'aging', 'stale', 'never'
    """
    if not last_refresh:
        return 'never', float('inf')

    try:
        refresh_dt = datetime.fromisoformat(last_refresh.replace('Z', '+00:00').replace('+00:00', ''))
    except (ValueError, AttributeError):
        try:
            refresh_dt = datetime.strptime(last_refresh, '%Y-%m-%d %H:%M:%S')
        except (ValueError, AttributeError):
            return 'unknown', float('inf')

    age_days = (datetime.now() - refresh_dt).total_seconds() / 86400
    config = SOURCE_CONFIG.get(source, {})
    expected = config.get('expected_refresh_days', 7)

    ratio = age_days / expected

    if ratio <= FRESHNESS_THRESHOLDS['fresh']:
        return 'fresh', age_days
    elif ratio <= FRESHNESS_THRESHOLDS['aging']:
        return 'aging', age_days
    else:
        return 'stale', age_days


def generate_health_report(stale_only: bool = False) -> Dict:
    """Generate comprehensive health report."""
    refresh_log = get_refresh_log()
    source_counts = get_source_counts()
    cache_status = get_cache_status()

    sources = []
    overall_fresh = 0
    overall_aging = 0
    overall_stale = 0
    overall_never = 0

    for source_id, config in SOURCE_CONFIG.items():
        refresh_info = refresh_log.get(source_id, {})
        count_info = source_counts.get(source_id, {})
        last_refresh = refresh_info.get('last_refresh')
        freshness, age_days = assess_freshness(source_id, last_refresh)

        if stale_only and freshness in ('fresh', 'aging'):
            continue

        if freshness == 'fresh':
            overall_fresh += 1
        elif freshness == 'aging':
            overall_aging += 1
        elif freshness == 'stale':
            overall_stale += 1
        else:
            overall_never += 1

        sources.append({
            'id': source_id,
            'name': config['name'],
            'type': config['type'],
            'freshness': freshness,
            'age_days': round(age_days, 1) if age_days != float('inf') else None,
            'expected_refresh_days': config['expected_refresh_days'],
            'method': config['method'],
            'last_refresh': last_refresh,
            'records': count_info.get('count', 0),
            'total_mw': count_info.get('total_mw', 0),
            'rows_added': refresh_info.get('rows_added', 0),
            'rows_updated': refresh_info.get('rows_updated', 0),
            'last_error': refresh_info.get('last_error'),
            'error_message': refresh_info.get('error_message'),
            'url': config.get('url', ''),
        })

    return {
        'timestamp': datetime.now().isoformat(),
        'database_path': str(DB_PATH),
        'database_exists': DB_PATH.exists(),
        'database_size_mb': round(DB_PATH.stat().st_size / (1024 * 1024), 1) if DB_PATH.exists() else 0,
        'summary': {
            'fresh': overall_fresh,
            'aging': overall_aging,
            'stale': overall_stale,
            'never_refreshed': overall_never,
            'total': len(SOURCE_CONFIG),
        },
        'sources': sources,
        'cache': cache_status,
    }


def print_health_report(report: Dict, brief: bool = False):
    """Print formatted health report."""
    summary = report['summary']

    if brief:
        # One-line summary
        print(f"Data Health: {summary['fresh']} fresh, {summary['aging']} aging, "
              f"{summary['stale']} stale, {summary['never_refreshed']} never | "
              f"DB: {report['database_size_mb']} MB")
        for src in report['sources']:
            icon = {'fresh': 'OK', 'aging': '!!', 'stale': 'XX', 'never': '--', 'unknown': '??'}
            status = icon.get(src['freshness'], '??')
            age = f"{src['age_days']}d" if src['age_days'] is not None else 'never'
            print(f"  [{status}] {src['name']:.<25} {src['records']:>7,} records  {age:>8}")
        return

    print()
    print("=" * 70)
    print("DATA HEALTH REPORT")
    print(f"Generated: {report['timestamp'][:19]}")
    print(f"Database: {report['database_path']} ({report['database_size_mb']} MB)")
    print("=" * 70)

    # Overall status
    total = summary['total']
    healthy = summary['fresh'] + summary['aging']
    health_pct = (healthy / total * 100) if total > 0 else 0

    print(f"\nOverall Health: {health_pct:.0f}% ({healthy}/{total} sources current)")
    print(f"  Fresh:  {summary['fresh']}")
    print(f"  Aging:  {summary['aging']}")
    print(f"  Stale:  {summary['stale']}")
    print(f"  Never:  {summary['never_refreshed']}")

    # Group by type
    for data_type in ['queue', 'permits', 'market']:
        type_sources = [s for s in report['sources'] if s['type'] == data_type]
        if not type_sources:
            continue

        type_label = {'queue': 'ISO Queue Data', 'permits': 'Generator/Permit Data', 'market': 'Market Data'}
        print(f"\n--- {type_label.get(data_type, data_type)} ---")

        for src in type_sources:
            icon = {'fresh': '[OK]', 'aging': '[!!]', 'stale': '[XX]', 'never': '[--]', 'unknown': '[??]'}
            status_icon = icon.get(src['freshness'], '[??]')

            if src['age_days'] is not None:
                if src['age_days'] < 1:
                    age_str = f"{src['age_days']*24:.0f}h ago"
                else:
                    age_str = f"{src['age_days']:.0f}d ago"
            else:
                age_str = "never"

            expected = f"(every {src['expected_refresh_days']}d)"

            print(f"  {status_icon} {src['name']:<25} {src['records']:>7,} records  "
                  f"Last: {age_str:<10} {expected}")

            if src.get('error_message') and src['freshness'] in ('stale', 'never'):
                print(f"         Error: {src['error_message'][:60]}")

    # Cache file status
    print(f"\n--- Cache Files ---")
    for name, info in report['cache'].items():
        if info.get('missing'):
            print(f"  [--] {name:<15} MISSING")
        else:
            age = f"{info['age_days']:.0f}d" if info['age_days'] else "?"
            print(f"  {name:<15} {info['size_mb']:>6.1f} MB  Modified: {info['modified']}  ({age} old)")

    # Recommended actions
    stale_sources = [s for s in report['sources'] if s['freshness'] in ('stale', 'never')]
    if stale_sources:
        print(f"\n--- Recommended Actions ---")
        for src in stale_sources:
            if 'MANUAL' in src.get('method', ''):
                print(f"  * {src['name']}: Manual download needed from {src.get('url', 'source')}")
            else:
                print(f"  * {src['name']}: Run `python3 refresh_data.py --source {src['id']}`")


def generate_html_dashboard(report: Dict) -> str:
    """Generate an HTML health dashboard."""
    sources_json = json.dumps(report['sources'], default=str)
    cache_json = json.dumps(report['cache'], default=str)
    summary = report['summary']

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Data Health Dashboard</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
  background: #0a0a0a; color: #e0e0e0; padding: 24px; }}
h1 {{ font-size: 1.5rem; margin-bottom: 8px; }}
.subtitle {{ color: #888; margin-bottom: 24px; }}
.grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin-bottom: 24px; }}
.card {{ background: #1a1a1a; border-radius: 8px; padding: 16px; border: 1px solid #333; }}
.card h3 {{ font-size: 0.75rem; color: #888; text-transform: uppercase; margin-bottom: 4px; }}
.card .value {{ font-size: 1.8rem; font-weight: 700; }}
.fresh {{ color: #4ade80; }}
.aging {{ color: #fbbf24; }}
.stale {{ color: #f87171; }}
.never {{ color: #666; }}
table {{ width: 100%; border-collapse: collapse; margin-top: 16px; }}
th {{ text-align: left; padding: 8px 12px; border-bottom: 2px solid #333; font-size: 0.8rem;
  color: #888; text-transform: uppercase; }}
td {{ padding: 8px 12px; border-bottom: 1px solid #222; font-size: 0.85rem; }}
tr:hover {{ background: #1a1a1a; }}
.badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }}
.badge-fresh {{ background: #052e16; color: #4ade80; }}
.badge-aging {{ background: #422006; color: #fbbf24; }}
.badge-stale {{ background: #450a0a; color: #f87171; }}
.badge-never {{ background: #222; color: #666; }}
</style>
</head>
<body>
<h1>Data Health Dashboard</h1>
<p class="subtitle">Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} | DB: {report['database_size_mb']} MB</p>

<div class="grid">
  <div class="card"><h3>Fresh</h3><div class="value fresh">{summary['fresh']}</div></div>
  <div class="card"><h3>Aging</h3><div class="value aging">{summary['aging']}</div></div>
  <div class="card"><h3>Stale</h3><div class="value stale">{summary['stale']}</div></div>
  <div class="card"><h3>Never</h3><div class="value never">{summary['never_refreshed']}</div></div>
</div>

<table>
<thead>
<tr><th>Source</th><th>Status</th><th>Records</th><th>Last Refresh</th><th>Expected</th><th>Method</th></tr>
</thead>
<tbody id="sources"></tbody>
</table>

<script>
const sources = {sources_json};
const tbody = document.getElementById('sources');
sources.forEach(s => {{
  const badge = `<span class="badge badge-${{s.freshness}}">${{s.freshness.toUpperCase()}}</span>`;
  const age = s.age_days != null ? (s.age_days < 1 ? `${{(s.age_days*24).toFixed(0)}}h ago` : `${{s.age_days.toFixed(0)}}d ago`) : 'never';
  const expected = `${{s.expected_refresh_days}}d`;
  tbody.innerHTML += `<tr>
    <td>${{s.name}}</td>
    <td>${{badge}}</td>
    <td>${{s.records.toLocaleString()}}</td>
    <td>${{age}}</td>
    <td>${{expected}}</td>
    <td>${{s.method}}</td>
  </tr>`;
}});
</script>
</body>
</html>"""


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Data Health Monitor')
    parser.add_argument('--brief', action='store_true', help='One-line per source')
    parser.add_argument('--json', action='store_true', help='JSON output')
    parser.add_argument('--stale', action='store_true', help='Only show stale sources')
    parser.add_argument('--html', type=str, nargs='?', const='data_health.html',
                       metavar='FILE', help='Generate HTML dashboard')

    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        print("Run `python3 refresh_data.py --all` to populate.")
        return 1

    report = generate_health_report(stale_only=args.stale)

    if args.json:
        print(json.dumps(report, indent=2, default=str))
        return 0

    if args.html:
        html = generate_html_dashboard(report)
        output_path = Path(args.html)
        output_path.write_text(html)
        print(f"Health dashboard written to {output_path}")
        return 0

    print_health_report(report, brief=args.brief)
    return 0


if __name__ == '__main__':
    sys.exit(main())
