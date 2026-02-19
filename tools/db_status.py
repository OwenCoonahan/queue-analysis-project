#!/usr/bin/env python3
"""
Database Status Report

Generates comprehensive statistics on the queue database including:
- Unique project counts by region (deduplicated)
- Data quality metrics (developer coverage, queue date coverage)
- Source breakdown and overlap analysis
- Status distribution

Usage:
    python3 db_status.py              # Full report
    python3 db_status.py --summary    # Quick summary
    python3 db_status.py --markdown   # Output as markdown (for docs)
    python3 db_status.py --json       # Output as JSON
    python3 db_status.py --brief      # One-line summary
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import json
import argparse

DATA_DIR = Path(__file__).parent / '.data'
DB_PATH = DATA_DIR / 'queue.db'


def get_unique_project_stats() -> Dict[str, Any]:
    """Generate comprehensive database statistics with deduplication."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query('''
        SELECT queue_id, region, source, capacity_mw, developer, queue_date, status
        FROM projects
    ''', conn)
    conn.close()

    stats = {
        'generated_at': datetime.now().isoformat(),
        'total_records': len(df),
        'unique_projects': 0,
        'overlap_records': 0,
        'by_region': [],
        'by_status': {},
        'by_source': [],
        'data_quality': {},
    }

    # Per-region stats (unique = queue_id + region combination)
    regions_order = ['PJM', 'West', 'MISO', 'ERCOT', 'Southeast', 'NYISO', 'SPP', 'CAISO', 'ISO-NE']

    for region in regions_order:
        region_df = df[df['region'] == region]
        if region_df.empty:
            continue

        unique = region_df['queue_id'].nunique()
        stats['unique_projects'] += unique

        # Deduplicate to get best data for each project
        deduped = region_df.groupby('queue_id').agg({
            'capacity_mw': 'max',
            'developer': lambda x: next((v for v in x if pd.notna(v) and v != ''), None),
            'queue_date': lambda x: next((v for v in x if pd.notna(v) and v != ''), None),
        }).reset_index()

        capacity_gw = deduped['capacity_mw'].sum() / 1000
        has_dev = deduped['developer'].notna() & (deduped['developer'] != '')
        has_date = deduped['queue_date'].notna() & (deduped['queue_date'] != '')

        stats['by_region'].append({
            'region': region,
            'unique_projects': unique,
            'capacity_gw': round(capacity_gw, 0),
            'developer_coverage': round(has_dev.mean() * 100, 0),
            'queue_date_coverage': round(has_date.mean() * 100, 0),
        })

    stats['overlap_records'] = stats['total_records'] - stats['unique_projects']

    # Status distribution (use most recent status per project)
    status_df = df.sort_values('source').groupby(['queue_id', 'region'])['status'].last()
    status_counts = status_df.value_counts()

    # Normalize status names
    status_map = {
        'Withdrawn': 'Withdrawn',
        'withdrawn': 'Withdrawn',
        'Active': 'Active',
        'active': 'Active',
        'Operational': 'Operational',
        'In Service': 'Operational',
        'Done': 'Operational',
        'Suspended': 'Suspended',
        'Planned': 'Planned',
    }

    normalized_status = {}
    for status, count in status_counts.items():
        norm = status_map.get(status, 'Other')
        normalized_status[norm] = normalized_status.get(norm, 0) + count

    stats['by_status'] = dict(sorted(normalized_status.items(), key=lambda x: -x[1]))

    # Source breakdown
    source_stats = df.groupby('source').agg({
        'queue_id': 'count',
        'capacity_mw': lambda x: x.sum() / 1000
    }).reset_index()
    source_stats.columns = ['source', 'records', 'capacity_gw']
    stats['by_source'] = source_stats.sort_values('records', ascending=False).to_dict('records')

    # Overall data quality (deduplicated)
    all_deduped = df.groupby(['queue_id', 'region']).agg({
        'capacity_mw': 'max',
        'developer': lambda x: next((v for v in x if pd.notna(v) and v != ''), None),
        'queue_date': lambda x: next((v for v in x if pd.notna(v) and v != ''), None),
    }).reset_index()

    has_dev = all_deduped['developer'].notna() & (all_deduped['developer'] != '')
    has_date = all_deduped['queue_date'].notna() & (all_deduped['queue_date'] != '')
    has_cap = all_deduped['capacity_mw'].notna() & (all_deduped['capacity_mw'] > 0)

    stats['data_quality'] = {
        'developer_coverage': round(has_dev.mean() * 100, 1),
        'queue_date_coverage': round(has_date.mean() * 100, 1),
        'capacity_coverage': round(has_cap.mean() * 100, 1),
    }

    # Total capacity
    stats['total_capacity_gw'] = round(sum(r['capacity_gw'] for r in stats['by_region']), 0)

    return stats


def get_refresh_status() -> List[Dict]:
    """Get last refresh time for each source."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    refresh_times = []
    try:
        cursor.execute("""
            SELECT source, MAX(completed_at) as last_refresh, status
            FROM refresh_log
            WHERE status = 'success'
            GROUP BY source
            ORDER BY last_refresh DESC
        """)
        for source, last_refresh, status in cursor.fetchall():
            hours_ago = None
            if last_refresh:
                try:
                    last_dt = datetime.fromisoformat(last_refresh.replace('Z', '+00:00'))
                    if last_dt.tzinfo:
                        last_dt = last_dt.replace(tzinfo=None)
                    hours_ago = (datetime.now() - last_dt).total_seconds() / 3600
                except:
                    pass

            refresh_times.append({
                'source': source,
                'last_refresh': last_refresh,
                'hours_ago': round(hours_ago, 1) if hours_ago else None,
                'is_stale': hours_ago > 48 if hours_ago else True,
            })
    except:
        pass

    conn.close()
    return refresh_times


def print_brief(stats: Dict[str, Any]):
    """Print one-line summary."""
    print(f"Queue DB: {stats['unique_projects']:,} unique projects / {stats['total_capacity_gw']:,.0f} GW | "
          f"Developer: {stats['data_quality']['developer_coverage']}% | "
          f"Queue Date: {stats['data_quality']['queue_date_coverage']}%")


def print_summary(stats: Dict[str, Any]):
    """Print quick summary."""
    print(f"Queue Database: {stats['unique_projects']:,} unique projects / {stats['total_capacity_gw']:,.0f} GW")
    print(f"Data Quality: {stats['data_quality']['developer_coverage']}% developer, "
          f"{stats['data_quality']['queue_date_coverage']}% queue date, "
          f"{stats['data_quality']['capacity_coverage']}% capacity")
    print()
    print("By Region:")
    for r in stats['by_region']:
        print(f"  {r['region']:<12}: {r['unique_projects']:>6,} projects ({r['capacity_gw']:>5,.0f} GW)")


def print_full_report(stats: Dict[str, Any]):
    """Print full report."""
    print('=' * 70)
    print('QUEUE DATABASE STATUS REPORT')
    print(f"Generated: {stats['generated_at'][:19]}")
    print('=' * 70)
    print()

    print(f"Total records in database: {stats['total_records']:,}")
    print(f"Unique projects:           {stats['unique_projects']:,}")
    print(f"Overlap (duplicates):      {stats['overlap_records']:,}")
    print(f"Total capacity:            {stats['total_capacity_gw']:,.0f} GW")
    print()

    print('UNIQUE PROJECTS BY REGION:')
    print('-' * 70)
    print(f"{'Region':<12} {'Unique':>10} {'Capacity':>12} {'Developer':>12} {'Queue Date':>12}")
    print('-' * 70)

    for r in stats['by_region']:
        print(f"{r['region']:<12} {r['unique_projects']:>10,} {r['capacity_gw']:>9,.0f} GW "
              f"{r['developer_coverage']:>10.0f}% {r['queue_date_coverage']:>10.0f}%")

    print('-' * 70)
    print(f"{'TOTAL':<12} {stats['unique_projects']:>10,} {stats['total_capacity_gw']:>9,.0f} GW")
    print()

    print('UNIQUE PROJECTS BY STATUS:')
    print('-' * 40)
    total = sum(stats['by_status'].values())
    for status, count in list(stats['by_status'].items())[:6]:
        pct = count / total * 100
        print(f"  {status:<20} {count:>8,} ({pct:>4.0f}%)")
    print()

    print('DATA QUALITY (Deduplicated):')
    print('-' * 40)
    print(f"  Developer coverage:  {stats['data_quality']['developer_coverage']}%")
    print(f"  Queue date coverage: {stats['data_quality']['queue_date_coverage']}%")
    print(f"  Capacity coverage:   {stats['data_quality']['capacity_coverage']}%")
    print()

    print('RECORDS BY SOURCE:')
    print('-' * 50)
    for s in stats['by_source']:
        print(f"  {s['source']:<18} {s['records']:>8,} records ({s['capacity_gw']:>6,.0f} GW)")

    # Refresh status
    refresh = get_refresh_status()
    if refresh:
        print()
        print('LAST REFRESH BY SOURCE:')
        print('-' * 50)
        for r in refresh[:10]:
            stale = " (STALE)" if r['is_stale'] else ""
            hours = f"{r['hours_ago']:.0f}h ago" if r['hours_ago'] else "never"
            print(f"  {r['source']:<18} {r['last_refresh'][:16] if r['last_refresh'] else 'never':<20} {hours}{stale}")


def print_markdown(stats: Dict[str, Any]):
    """Print as markdown for documentation."""
    print("## Queue Database Statistics")
    print()
    print(f"*Last updated: {stats['generated_at'][:10]}*")
    print()
    print("### Summary")
    print()
    print("| Metric | Value |")
    print("|--------|-------|")
    print(f"| Total Records | {stats['total_records']:,} |")
    print(f"| **Unique Projects** | **{stats['unique_projects']:,}** |")
    print(f"| Total Capacity | {stats['total_capacity_gw']:,.0f} GW |")
    print(f"| Overlap (multi-source) | {stats['overlap_records']:,} |")
    print()

    print("### Unique Projects by Region")
    print()
    print("| Region | Unique Projects | Capacity | Developer Coverage | Queue Date Coverage |")
    print("|--------|-----------------|----------|-------------------|-------------------|")
    for r in stats['by_region']:
        print(f"| {r['region']} | {r['unique_projects']:,} | {r['capacity_gw']:,.0f} GW | "
              f"{r['developer_coverage']:.0f}% | {r['queue_date_coverage']:.0f}% |")
    print(f"| **TOTAL** | **{stats['unique_projects']:,}** | **{stats['total_capacity_gw']:,.0f} GW** | | |")
    print()

    print("### Unique Projects by Status")
    print()
    print("| Status | Count |")
    print("|--------|-------|")
    total = sum(stats['by_status'].values())
    for status, count in list(stats['by_status'].items())[:6]:
        pct = count / total * 100
        print(f"| {status} | {count:,} ({pct:.0f}%) |")
    print()

    print("### Data Quality (Deduplicated)")
    print()
    print("| Field | Coverage |")
    print("|-------|----------|")
    print(f"| Developer | {stats['data_quality']['developer_coverage']}% |")
    print(f"| Queue Date | {stats['data_quality']['queue_date_coverage']}% |")
    print(f"| Capacity | {stats['data_quality']['capacity_coverage']}% |")
    print()

    print("### Records by Source")
    print()
    print("| Source | Records | Capacity |")
    print("|--------|---------|----------|")
    for s in stats['by_source']:
        print(f"| {s['source']} | {s['records']:,} | {s['capacity_gw']:,.0f} GW |")


def main():
    parser = argparse.ArgumentParser(description='Queue Database Status Report')
    parser.add_argument('--summary', action='store_true', help='Quick summary')
    parser.add_argument('--markdown', action='store_true', help='Output as markdown')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--brief', action='store_true', help='One-line summary')

    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        return 1

    stats = get_unique_project_stats()

    if args.json:
        print(json.dumps(stats, indent=2))
    elif args.markdown:
        print_markdown(stats)
    elif args.brief:
        print_brief(stats)
    elif args.summary:
        print_summary(stats)
    else:
        print_full_report(stats)

    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
