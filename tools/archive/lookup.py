#!/usr/bin/env python3
"""
Quick project lookup CLI tool.

Usage:
    python lookup.py PJM AB1-123          # Look up by queue ID
    python lookup.py ERCOT --name "Solar" # Search by name
    python lookup.py PJM --poi "Loudoun"  # Find all projects at POI
    python lookup.py --stats PJM          # Get queue statistics
    python lookup.py --all-stats          # Get stats for all ISOs
"""

import argparse
import sys
from queue_analyzer import QueueAnalyzer, get_all_queues


def format_number(n):
    """Format number with commas."""
    if n is None:
        return "N/A"
    if isinstance(n, float):
        return f"{n:,.1f}"
    return f"{n:,}"


def print_project(row, columns=None):
    """Print a single project's details."""
    print("-" * 70)

    # Priority columns to display
    priority_patterns = [
        'queue', 'id', 'request',  # ID
        'name',  # Name
        'capacity', 'mw',  # Size
        'status',  # Status
        'fuel', 'type', 'generation',  # Type
        'state', 'county',  # Location
        'poi', 'substation', 'interconnection',  # POI
        'date',  # Dates
        'developer', 'owner', 'entity',  # Developer
    ]

    displayed = set()
    for pattern in priority_patterns:
        for col in row.index:
            if pattern in col.lower() and col not in displayed:
                val = row[col]
                if val is not None and str(val) != 'nan' and str(val) != '':
                    print(f"  {col}: {val}")
                    displayed.add(col)


def lookup_project(iso: str, queue_id: str):
    """Look up a specific project by queue ID."""
    analyzer = QueueAnalyzer(iso)
    results = analyzer.find_project(queue_id=queue_id)

    if len(results) == 0:
        print(f"\nNo projects found matching ID: {queue_id}")
        return

    print(f"\n{'=' * 70}")
    print(f"Found {len(results)} project(s) in {iso}")
    print(f"{'=' * 70}")

    for _, row in results.head(10).iterrows():
        print_project(row)

    if len(results) > 10:
        print(f"\n... and {len(results) - 10} more projects")


def search_by_name(iso: str, name: str):
    """Search projects by name."""
    analyzer = QueueAnalyzer(iso)
    results = analyzer.find_project(project_name=name)

    if len(results) == 0:
        print(f"\nNo projects found matching name: {name}")
        return

    print(f"\n{'=' * 70}")
    print(f"Found {len(results)} project(s) matching '{name}' in {iso}")
    print(f"{'=' * 70}")

    for _, row in results.head(10).iterrows():
        print_project(row)

    if len(results) > 10:
        print(f"\n... and {len(results) - 10} more projects")


def analyze_poi(iso: str, poi_name: str):
    """Analyze all projects at a POI."""
    analyzer = QueueAnalyzer(iso)
    analysis = analyzer.get_poi_analysis(poi_name)

    if "error" in analysis:
        print(f"\nError: {analysis['error']}")
        return

    print(f"\n{'=' * 70}")
    print(f"POI ANALYSIS: {poi_name}")
    print(f"{'=' * 70}")

    print(f"\n  Total projects: {analysis['total_projects']}")
    if 'total_capacity_mw' in analysis:
        print(f"  Total capacity: {format_number(analysis['total_capacity_mw'])} MW")
    if 'active_projects' in analysis:
        print(f"  Active projects: {analysis['active_projects']}")
    if 'active_capacity_mw' in analysis:
        print(f"  Active capacity: {format_number(analysis['active_capacity_mw'])} MW")

    if 'status_breakdown' in analysis:
        print(f"\n  Status breakdown:")
        for status, count in analysis['status_breakdown'].items():
            print(f"    {status}: {count}")

    # Show projects
    if 'projects' in analysis:
        print(f"\n  Projects at this POI:")
        for _, row in analysis['projects'].head(10).iterrows():
            print_project(row)

        if len(analysis['projects']) > 10:
            print(f"\n  ... and {len(analysis['projects']) - 10} more projects")


def show_stats(iso: str):
    """Show queue statistics for an ISO."""
    analyzer = QueueAnalyzer(iso)
    stats = analyzer.get_queue_statistics()
    rates = analyzer.calculate_completion_rate()

    print(f"\n{'=' * 70}")
    print(f"{iso} QUEUE STATISTICS")
    print(f"{'=' * 70}")

    print(f"\n  Total projects: {format_number(stats['total_projects'])}")
    if 'total_capacity_mw' in stats:
        print(f"  Total capacity: {format_number(stats['total_capacity_mw'])} MW")
    if 'avg_capacity_mw' in stats:
        print(f"  Avg capacity: {format_number(stats['avg_capacity_mw'])} MW")

    print(f"\n  Completion rate: {format_number(rates.get('completion_rate'))}%")
    print(f"  Withdrawal rate: {format_number(rates.get('withdrawal_rate'))}%")
    print(f"  Active: {format_number(rates['active'])}")
    print(f"  Completed: {format_number(rates['completed'])}")
    print(f"  Withdrawn: {format_number(rates['withdrawn'])}")

    if 'fuel_type_breakdown' in stats:
        print(f"\n  Top fuel types:")
        for fuel, count in list(stats['fuel_type_breakdown'].items())[:5]:
            print(f"    {fuel}: {count}")

    if 'status_breakdown' in stats:
        print(f"\n  Status breakdown:")
        for status, count in list(stats['status_breakdown'].items())[:5]:
            print(f"    {status}: {count}")


def show_all_stats():
    """Show summary stats for all ISOs."""
    print(f"\n{'=' * 70}")
    print("ALL ISO QUEUE STATISTICS")
    print(f"{'=' * 70}")

    isos = ['PJM', 'ERCOT', 'MISO', 'CAISO', 'NYISO', 'ISONE', 'SPP']

    print(f"\n{'ISO':<10} {'Projects':>12} {'Capacity (GW)':>15} {'Active':>10}")
    print("-" * 50)

    for iso in isos:
        try:
            analyzer = QueueAnalyzer(iso)
            stats = analyzer.get_queue_statistics()
            rates = analyzer.calculate_completion_rate()

            cap_gw = stats.get('total_capacity_mw', 0) / 1000 if stats.get('total_capacity_mw') else 0

            print(f"{iso:<10} {stats['total_projects']:>12,} {cap_gw:>15,.1f} {rates['active']:>10,}")
        except Exception as e:
            print(f"{iso:<10} {'Error':>12}")


def main():
    parser = argparse.ArgumentParser(
        description="Interconnection Queue Lookup Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python lookup.py PJM AB1-123          # Look up by queue ID
    python lookup.py ERCOT --name "Solar" # Search by name
    python lookup.py PJM --poi "Loudoun"  # Find all at POI
    python lookup.py --stats PJM          # Queue statistics
    python lookup.py --all-stats          # All ISO stats
        """
    )

    parser.add_argument('iso', nargs='?', help='ISO name (PJM, ERCOT, MISO, etc.)')
    parser.add_argument('queue_id', nargs='?', help='Queue ID to look up')
    parser.add_argument('--name', help='Search by project name')
    parser.add_argument('--poi', help='Analyze projects at POI')
    parser.add_argument('--stats', action='store_true', help='Show queue statistics')
    parser.add_argument('--all-stats', action='store_true', help='Show stats for all ISOs')

    args = parser.parse_args()

    if args.all_stats:
        show_all_stats()
        return

    if not args.iso:
        parser.print_help()
        return

    if args.stats:
        show_stats(args.iso)
    elif args.poi:
        analyze_poi(args.iso, args.poi)
    elif args.name:
        search_by_name(args.iso, args.name)
    elif args.queue_id:
        lookup_project(args.iso, args.queue_id)
    else:
        show_stats(args.iso)


if __name__ == "__main__":
    main()
