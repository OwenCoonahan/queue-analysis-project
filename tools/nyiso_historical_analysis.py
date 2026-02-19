#!/usr/bin/env python3
"""
NYISO Historical Analysis Module

Analyzes 134 monthly queue snapshots (2014-2025) to track:
- Individual project progression through study phases
- Withdrawal patterns and timing
- Completion rates by year, fuel type, developer
- Queue dynamics over time

Usage:
    python3 nyiso_historical_analysis.py --summary          # Overall statistics
    python3 nyiso_historical_analysis.py --track Q0495      # Track specific project
    python3 nyiso_historical_analysis.py --withdrawals      # Analyze withdrawal patterns
    python3 nyiso_historical_analysis.py --completions      # Analyze completion rates
    python3 nyiso_historical_analysis.py --build-db         # Build historical database
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import sqlite3
import re
import warnings
warnings.filterwarnings('ignore')

CACHE_DIR = Path(__file__).parent / '.cache'
HISTORICAL_DIR = CACHE_DIR / 'nyiso_historical'
DATA_DIR = Path(__file__).parent / '.data'
HISTORICAL_DB = DATA_DIR / 'nyiso_historical.db'


class NYISOHistoricalAnalyzer:
    """Analyze NYISO queue history across monthly snapshots."""

    # Status codes from NYISO files
    STATUS_MAP = {
        0: 'Withdrawn',
        1: 'SRIS',
        2: 'SRIS Complete',
        3: 'FS',
        4: 'FS Complete',
        5: 'SIS',
        6: 'SIS Complete',
        7: 'IA Pending',
        8: 'IA Executed',
        9: 'Under Construction',
        10: 'In Service',
        11: 'IA Executed',
        12: 'FS',
        13: 'FS Complete',
        14: 'In Service',
    }

    # Study phase progression (ordered)
    PHASE_ORDER = [
        'SRIS', 'SRIS Complete',
        'FS', 'FS Complete',
        'SIS', 'SIS Complete',
        'IA Pending', 'IA Executed',
        'Under Construction', 'In Service'
    ]

    def __init__(self):
        self.historical_dir = HISTORICAL_DIR
        self.snapshots = self._find_snapshots()

    def _find_snapshots(self) -> List[Tuple[datetime, Path]]:
        """Find all historical snapshot files, sorted by date."""
        snapshots = []

        for f in self.historical_dir.glob('nyiso_queue_*.xls*'):
            # Parse date from filename
            match = re.search(r'(\d{4})-(\d{2})-(\d{2})', f.name)
            if match:
                try:
                    date = datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
                    snapshots.append((date, f))
                except:
                    pass

        snapshots.sort(key=lambda x: x[0])
        return snapshots

    def _load_snapshot(self, filepath: Path) -> pd.DataFrame:
        """Load a single snapshot file."""
        try:
            # Try different sheet names
            df = pd.DataFrame()
            for sheet_name in ['Interconnection Queue', 'Sheet1', 0]:
                try:
                    df = pd.read_excel(filepath, sheet_name=sheet_name)
                    if not df.empty and len(df.columns) > 5:
                        break
                except:
                    continue

            if df.empty:
                return pd.DataFrame()

            # Standardize column names
            col_map = {
                'Queue Pos.': 'queue_id',
                'Queue': 'queue_id',
                'Developer/Interconnection Customer': 'developer',
                'Owner/Developer': 'developer',
                'Project Name': 'name',
                'Date of IR': 'queue_date',
                'SP (MW)': 'capacity_mw',
                'Type/ Fuel': 'fuel_type',
                'S': 'status_code',
                'County': 'county',
                'State': 'state',
            }

            rename_map = {k: v for k, v in col_map.items() if k in df.columns}
            df = df.rename(columns=rename_map)

            # Clean queue_id
            if 'queue_id' in df.columns:
                df['queue_id'] = df['queue_id'].astype(str).str.strip()
                df = df[df['queue_id'].notna() & (df['queue_id'] != '') & (df['queue_id'] != 'nan')]

            # Convert capacity to numeric
            if 'capacity_mw' in df.columns:
                df['capacity_mw'] = pd.to_numeric(df['capacity_mw'], errors='coerce')

            # Parse status (handle various formats like "9", "9, 10", "5P", etc.)
            if 'status_code' in df.columns:
                def parse_status(x):
                    if pd.isna(x):
                        return 'Active'
                    try:
                        # Try direct int conversion
                        code = int(x)
                        return self.STATUS_MAP.get(code, f'Code_{code}')
                    except (ValueError, TypeError):
                        # Handle string formats like "9, 10" - take first number
                        s = str(x).strip()
                        match = re.match(r'(\d+)', s)
                        if match:
                            code = int(match.group(1))
                            return self.STATUS_MAP.get(code, f'Code_{code}')
                        return f'Code_{s}'
                df['status'] = df['status_code'].apply(parse_status)
            else:
                # No status_code column - mark as Active
                df['status'] = 'Active'

            return df

        except Exception as e:
            print(f"  Warning: Could not load {filepath.name}: {e}")
            return pd.DataFrame()

    def build_project_history(self, progress_callback=None) -> pd.DataFrame:
        """
        Build complete history of all projects across all snapshots.

        Returns DataFrame with one row per project per snapshot.
        """
        all_records = []

        for i, (snapshot_date, filepath) in enumerate(self.snapshots):
            if progress_callback:
                progress_callback(i, len(self.snapshots), filepath.name)

            df = self._load_snapshot(filepath)
            if df.empty:
                continue

            df['snapshot_date'] = snapshot_date

            # Select key columns
            cols = ['queue_id', 'name', 'developer', 'capacity_mw', 'fuel_type',
                    'status', 'status_code', 'county', 'state', 'queue_date', 'snapshot_date']
            df = df[[c for c in cols if c in df.columns]]

            all_records.append(df)

        if not all_records:
            return pd.DataFrame()

        history = pd.concat(all_records, ignore_index=True)
        return history

    def track_project(self, queue_id: str) -> pd.DataFrame:
        """
        Track a specific project's history across all snapshots.

        Returns timeline of project's status changes.
        """
        history = []

        for snapshot_date, filepath in self.snapshots:
            df = self._load_snapshot(filepath)
            if df.empty:
                continue

            # Find project
            project = df[df['queue_id'].astype(str) == str(queue_id)]

            if not project.empty:
                row = project.iloc[0]
                history.append({
                    'snapshot_date': snapshot_date,
                    'name': row.get('name'),
                    'status': row.get('status'),
                    'status_code': row.get('status_code'),
                    'capacity_mw': row.get('capacity_mw'),
                    'developer': row.get('developer'),
                })
            else:
                # Project not in this snapshot - may have withdrawn or completed
                history.append({
                    'snapshot_date': snapshot_date,
                    'name': None,
                    'status': 'Not in Queue',
                    'status_code': None,
                    'capacity_mw': None,
                    'developer': None,
                })

        return pd.DataFrame(history)

    def analyze_withdrawals(self) -> Dict:
        """
        Analyze withdrawal patterns across all snapshots.

        Returns statistics on when/why projects withdraw.
        """
        print("Building project history for withdrawal analysis...")

        def progress(i, total, name):
            if i % 20 == 0:
                print(f"  Processing snapshot {i+1}/{total}: {name}")

        history = self.build_project_history(progress_callback=progress)

        if history.empty:
            return {}

        # Find projects that disappeared (withdrew)
        projects = history.groupby('queue_id').agg({
            'snapshot_date': ['min', 'max', 'count'],
            'status': 'last',
            'capacity_mw': 'first',
            'fuel_type': 'first',
            'name': 'first',
        }).reset_index()

        projects.columns = ['queue_id', 'first_seen', 'last_seen', 'appearances',
                           'last_status', 'capacity_mw', 'fuel_type', 'name']

        # Calculate time in queue
        projects['time_in_queue_days'] = (projects['last_seen'] - projects['first_seen']).dt.days

        # Identify withdrawn projects (last status = Withdrawn or disappeared)
        total_snapshots = len(self.snapshots)
        latest_date = self.snapshots[-1][0] if self.snapshots else datetime.now()

        # Projects that didn't make it to the latest snapshot and weren't completed
        withdrawn = projects[
            (projects['last_seen'] < latest_date) &
            (~projects['last_status'].isin(['In Service', 'Under Construction']))
        ]

        # Statistics
        stats = {
            'total_projects': len(projects),
            'withdrawn_projects': len(withdrawn),
            'withdrawal_rate': len(withdrawn) / len(projects) if len(projects) > 0 else 0,
            'avg_time_to_withdrawal_days': withdrawn['time_in_queue_days'].mean(),
            'median_time_to_withdrawal_days': withdrawn['time_in_queue_days'].median(),
            'withdrawals_by_fuel': withdrawn.groupby('fuel_type').size().to_dict(),
            'withdrawals_by_year': withdrawn.groupby(withdrawn['last_seen'].dt.year).size().to_dict(),
            'withdrawal_capacity_gw': pd.to_numeric(withdrawn['capacity_mw'], errors='coerce').fillna(0).sum() / 1000,
        }

        return stats

    def analyze_completions(self) -> Dict:
        """
        Analyze completion patterns - which projects made it to In Service.
        """
        print("Building project history for completion analysis...")

        def progress(i, total, name):
            if i % 20 == 0:
                print(f"  Processing snapshot {i+1}/{total}: {name}")

        history = self.build_project_history(progress_callback=progress)

        if history.empty:
            return {}

        # Find projects that reached "In Service"
        completed = history[history['status'] == 'In Service']['queue_id'].unique()

        # Get first and last snapshot for each completed project
        completed_details = []
        for qid in completed:
            proj_history = history[history['queue_id'] == qid].sort_values('snapshot_date')
            if len(proj_history) > 0:
                first = proj_history.iloc[0]
                last = proj_history[proj_history['status'] == 'In Service'].iloc[0]
                completed_details.append({
                    'queue_id': qid,
                    'name': first['name'],
                    'fuel_type': first.get('fuel_type'),
                    'capacity_mw': first.get('capacity_mw'),
                    'first_seen': first['snapshot_date'],
                    'completed_date': last['snapshot_date'],
                    'time_to_completion_days': (last['snapshot_date'] - first['snapshot_date']).days,
                })

        completed_df = pd.DataFrame(completed_details)

        # All projects
        all_projects = history.groupby('queue_id').first().reset_index()

        stats = {
            'total_projects': len(all_projects),
            'completed_projects': len(completed_df),
            'completion_rate': len(completed_df) / len(all_projects) if len(all_projects) > 0 else 0,
            'avg_time_to_completion_days': completed_df['time_to_completion_days'].mean() if len(completed_df) > 0 else 0,
            'median_time_to_completion_days': completed_df['time_to_completion_days'].median() if len(completed_df) > 0 else 0,
            'completions_by_fuel': completed_df.groupby('fuel_type').size().to_dict() if len(completed_df) > 0 else {},
            'completions_by_year': completed_df.groupby(completed_df['completed_date'].dt.year).size().to_dict() if len(completed_df) > 0 else {},
            'completed_capacity_gw': pd.to_numeric(completed_df['capacity_mw'], errors='coerce').fillna(0).sum() / 1000 if len(completed_df) > 0 else 0,
        }

        return stats

    def get_summary(self) -> Dict:
        """Get summary statistics across all snapshots."""
        print(f"Found {len(self.snapshots)} historical snapshots")
        print(f"Date range: {self.snapshots[0][0].strftime('%Y-%m-%d')} to {self.snapshots[-1][0].strftime('%Y-%m-%d')}")

        # Sample a few snapshots for quick stats
        sample_dates = [
            self.snapshots[0],   # First
            self.snapshots[len(self.snapshots)//2],  # Middle
            self.snapshots[-1],  # Latest
        ]

        samples = []
        for date, filepath in sample_dates:
            df = self._load_snapshot(filepath)
            if not df.empty:
                samples.append({
                    'date': date,
                    'projects': len(df),
                    'capacity_gw': df['capacity_mw'].sum() / 1000 if 'capacity_mw' in df.columns else 0,
                })

        return {
            'total_snapshots': len(self.snapshots),
            'date_range': {
                'start': self.snapshots[0][0].strftime('%Y-%m-%d'),
                'end': self.snapshots[-1][0].strftime('%Y-%m-%d'),
            },
            'samples': samples,
        }

    def build_historical_db(self):
        """Build SQLite database with all historical data for fast querying."""
        print("Building historical database...")

        DATA_DIR.mkdir(exist_ok=True)

        conn = sqlite3.connect(HISTORICAL_DB)

        # Create tables
        conn.execute('''
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY,
                snapshot_date TEXT UNIQUE,
                file_path TEXT,
                project_count INTEGER,
                total_capacity_mw REAL
            )
        ''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS project_history (
                id INTEGER PRIMARY KEY,
                queue_id TEXT,
                snapshot_date TEXT,
                name TEXT,
                developer TEXT,
                capacity_mw REAL,
                fuel_type TEXT,
                status TEXT,
                status_code INTEGER,
                county TEXT,
                state TEXT,
                UNIQUE(queue_id, snapshot_date)
            )
        ''')

        conn.execute('CREATE INDEX IF NOT EXISTS idx_queue_id ON project_history(queue_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_snapshot_date ON project_history(snapshot_date)')

        # Load each snapshot
        for i, (snapshot_date, filepath) in enumerate(self.snapshots):
            print(f"  [{i+1}/{len(self.snapshots)}] Processing {filepath.name}")

            df = self._load_snapshot(filepath)
            if df.empty:
                continue

            # Record snapshot
            conn.execute('''
                INSERT OR REPLACE INTO snapshots (snapshot_date, file_path, project_count, total_capacity_mw)
                VALUES (?, ?, ?, ?)
            ''', (
                snapshot_date.strftime('%Y-%m-%d'),
                str(filepath),
                len(df),
                df['capacity_mw'].sum() if 'capacity_mw' in df.columns else 0
            ))

            # Record projects
            for _, row in df.iterrows():
                try:
                    conn.execute('''
                        INSERT OR REPLACE INTO project_history
                        (queue_id, snapshot_date, name, developer, capacity_mw, fuel_type, status, status_code, county, state)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        str(row.get('queue_id', '')),
                        snapshot_date.strftime('%Y-%m-%d'),
                        row.get('name'),
                        row.get('developer'),
                        row.get('capacity_mw'),
                        row.get('fuel_type'),
                        row.get('status'),
                        row.get('status_code'),
                        row.get('county'),
                        row.get('state'),
                    ))
                except Exception as e:
                    continue

            conn.commit()

        conn.close()
        print(f"\nHistorical database built: {HISTORICAL_DB}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='NYISO Historical Analysis')
    parser.add_argument('--summary', action='store_true', help='Show summary statistics')
    parser.add_argument('--track', type=str, metavar='QUEUE_ID', help='Track specific project')
    parser.add_argument('--withdrawals', action='store_true', help='Analyze withdrawal patterns')
    parser.add_argument('--completions', action='store_true', help='Analyze completion rates')
    parser.add_argument('--build-db', action='store_true', help='Build historical SQLite database')

    args = parser.parse_args()

    analyzer = NYISOHistoricalAnalyzer()

    if len(analyzer.snapshots) == 0:
        print("No historical snapshots found!")
        print(f"Expected directory: {HISTORICAL_DIR}")
        print("Run nyiso_historical_downloader.py --download first")
        return 1

    if args.summary:
        stats = analyzer.get_summary()
        print("\n" + "="*60)
        print("NYISO HISTORICAL DATA SUMMARY")
        print("="*60)
        print(f"Total snapshots: {stats['total_snapshots']}")
        print(f"Date range: {stats['date_range']['start']} to {stats['date_range']['end']}")
        print("\nSample snapshots:")
        for s in stats['samples']:
            print(f"  {s['date'].strftime('%Y-%m-%d')}: {s['projects']} projects, {s['capacity_gw']:.1f} GW")
        return 0

    if args.track:
        print(f"Tracking project {args.track}...")
        history = analyzer.track_project(args.track)
        if history.empty:
            print("Project not found in any snapshot")
            return 1

        print("\n" + "="*60)
        print(f"PROJECT HISTORY: {args.track}")
        print("="*60)

        # Show status changes
        prev_status = None
        for _, row in history.iterrows():
            status = row['status'] if pd.notna(row['status']) else 'Unknown'
            if status != prev_status:
                print(f"  {row['snapshot_date'].strftime('%Y-%m-%d')}: {status}")
                if row['name'] and pd.notna(row['name']) and prev_status is None:
                    print(f"    Name: {row['name']}")
                    print(f"    Developer: {row['developer'] if pd.notna(row['developer']) else 'N/A'}")
                    print(f"    Capacity: {row['capacity_mw'] if pd.notna(row['capacity_mw']) else 'N/A'} MW")
                prev_status = status
        return 0

    if args.withdrawals:
        stats = analyzer.analyze_withdrawals()
        print("\n" + "="*60)
        print("WITHDRAWAL ANALYSIS")
        print("="*60)
        print(f"Total projects tracked: {stats.get('total_projects', 0):,}")
        print(f"Withdrawn projects: {stats.get('withdrawn_projects', 0):,}")
        print(f"Withdrawal rate: {stats.get('withdrawal_rate', 0)*100:.1f}%")
        print(f"Avg time to withdrawal: {stats.get('avg_time_to_withdrawal_days', 0):.0f} days")
        print(f"Median time to withdrawal: {stats.get('median_time_to_withdrawal_days', 0):.0f} days")
        print(f"Withdrawn capacity: {stats.get('withdrawal_capacity_gw', 0):.1f} GW")
        print("\nWithdrawals by fuel type:")
        for fuel, count in stats.get('withdrawals_by_fuel', {}).items():
            print(f"  {fuel}: {count}")
        return 0

    if args.completions:
        stats = analyzer.analyze_completions()
        print("\n" + "="*60)
        print("COMPLETION ANALYSIS")
        print("="*60)
        print(f"Total projects tracked: {stats.get('total_projects', 0):,}")
        print(f"Completed projects: {stats.get('completed_projects', 0):,}")
        print(f"Completion rate: {stats.get('completion_rate', 0)*100:.1f}%")
        print(f"Avg time to completion: {stats.get('avg_time_to_completion_days', 0):.0f} days")
        print(f"Median time to completion: {stats.get('median_time_to_completion_days', 0):.0f} days")
        print(f"Completed capacity: {stats.get('completed_capacity_gw', 0):.1f} GW")
        print("\nCompletions by fuel type:")
        for fuel, count in stats.get('completions_by_fuel', {}).items():
            print(f"  {fuel}: {count}")
        return 0

    if args.build_db:
        analyzer.build_historical_db()
        return 0

    # Default: show summary
    stats = analyzer.get_summary()
    print(f"\nFound {stats['total_snapshots']} snapshots from {stats['date_range']['start']} to {stats['date_range']['end']}")
    print("\nUse --summary, --track, --withdrawals, --completions, or --build-db")
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
