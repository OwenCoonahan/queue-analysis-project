#!/usr/bin/env python3
"""
Queue Analysis Tool

Analyzes interconnection queue data from various sources.
Works with downloaded Excel/CSV files or fetches from available APIs.

Usage:
    python3 analyze.py --stats                    # Overall statistics (NYISO)
    python3 analyze.py --file data.xlsx --stats   # Analyze local file
    python3 analyze.py --search "Solar" --state VA
    python3 analyze.py --poi "Loudoun"
    python3 analyze.py --project "ABC-123"
"""

import argparse
import sys
import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

# Suppress warnings
import warnings
warnings.filterwarnings('ignore')

# Cache directory
CACHE_DIR = Path(__file__).parent / '.cache'
CACHE_DIR.mkdir(exist_ok=True)


class QueueData:
    """Load and manage queue data from various sources."""

    # Known working data sources
    SOURCES = {
        'NYISO': 'https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx',
    }

    def __init__(self):
        self.df = None
        self.source = None

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove non-project rows (Excel footer notes, blank rows, etc.)."""
        if df.empty:
            return df

        original_count = len(df)

        # Find the queue position column
        id_col = None
        for col in df.columns:
            if any(kw in col.lower() for kw in ['queue', 'pos', 'number']):
                id_col = col
                break

        # Find the project name column
        name_col = None
        for col in df.columns:
            if 'name' in col.lower() and 'project' in col.lower():
                name_col = col
                break
        if name_col is None:
            for col in df.columns:
                if 'name' in col.lower():
                    name_col = col
                    break

        # Filter out non-project rows
        # Logic: Keep rows where queue position is numeric OR (queue position is null AND name exists)
        if id_col:
            # Queue positions should be numeric (possibly with leading zeros)
            id_is_numeric = df[id_col].astype(str).str.match(r'^\d+$', na=False)
            id_is_null = df[id_col].isna()

            if name_col:
                # Keep if: (numeric ID) OR (null ID but has name)
                name_exists = df[name_col].notna() & (df[name_col].astype(str).str.strip() != '')
                mask = id_is_numeric | (id_is_null & name_exists)
            else:
                # No name column - just filter on numeric ID
                mask = id_is_numeric

            df = df[mask]

        cleaned_count = len(df)
        if original_count != cleaned_count:
            print(f"  Cleaned: removed {original_count - cleaned_count} non-project rows")

        return df

    def load_file(self, filepath: str) -> pd.DataFrame:
        """Load data from a local Excel or CSV file."""
        filepath = Path(filepath)

        if not filepath.exists():
            print(f"Error: File not found: {filepath}")
            return pd.DataFrame()

        print(f"Loading: {filepath}")

        if filepath.suffix.lower() in ['.xlsx', '.xls']:
            # Try to read the main data sheet
            xl = pd.ExcelFile(filepath)
            print(f"  Sheets: {xl.sheet_names[:5]}...")

            # Find the best sheet (look for 'data', 'queue', or use first)
            sheet = None
            for s in xl.sheet_names:
                if any(kw in s.lower() for kw in ['data', 'queue', 'project', 'request']):
                    sheet = s
                    break
            if sheet is None:
                sheet = xl.sheet_names[0]

            print(f"  Using sheet: {sheet}")
            self.df = pd.read_excel(filepath, sheet_name=sheet)

        elif filepath.suffix.lower() == '.csv':
            self.df = pd.read_csv(filepath)
        else:
            print(f"Error: Unsupported file type: {filepath.suffix}")
            return pd.DataFrame()

        # Clean data to remove footer rows
        self.df = self._clean_data(self.df)

        self.source = str(filepath)
        print(f"  Loaded {len(self.df)} rows, {len(self.df.columns)} columns")
        return self.df

    def load_nyiso(self, force_refresh: bool = False) -> pd.DataFrame:
        """Fetch NYISO interconnection queue."""
        cache_file = CACHE_DIR / 'nyiso_queue.xlsx'

        # Use cache if available and fresh
        if not force_refresh and cache_file.exists():
            age_hours = (datetime.now().timestamp() - cache_file.stat().st_mtime) / 3600
            if age_hours < 24:
                print(f"Loading NYISO from cache (age: {age_hours:.1f} hours)")
                self.df = pd.read_excel(cache_file)
                self.df = self._clean_data(self.df)
                self.source = 'NYISO (cached)'
                return self.df

        print("Fetching NYISO interconnection queue...")
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'}

        try:
            response = requests.get(self.SOURCES['NYISO'], headers=headers, timeout=60)
            response.raise_for_status()

            with open(cache_file, 'wb') as f:
                f.write(response.content)

            self.df = pd.read_excel(cache_file)
            self.df = self._clean_data(self.df)
            self.source = 'NYISO'
            print(f"  Loaded {len(self.df)} projects")
            return self.df

        except Exception as e:
            print(f"Error fetching NYISO: {e}")
            return pd.DataFrame()

    def get_columns(self) -> List[str]:
        """Get list of column names."""
        return list(self.df.columns) if self.df is not None else []


class QueueAnalyzer:
    """Analyze queue data."""

    # Status keywords for categorization
    ACTIVE_KW = ['active', 'pending', 'study', 'queue', 'in progress']
    WITHDRAWN_KW = ['withdrawn', 'cancelled', 'suspended', 'terminated', 'inactive']
    COMPLETED_KW = ['complete', 'operational', 'service', 'commercial', 'done', 'energized']

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self._map_columns()

    def _map_columns(self):
        """Map standard column names to actual column names in the data."""
        self.col_map = {}

        mappings = {
            'id': ['queue', 'id', 'request', 'pos', 'number'],
            'name': ['name', 'project'],
            'developer': ['developer', 'owner', 'customer', 'entity', 'applicant'],
            'capacity': ['capacity', 'mw', ' mw', 'sp (mw)', 'wp (mw)'],
            'type': ['type', 'fuel', 'resource', 'technology', 'generation'],
            'status': ['status', 'phase', 'stage'],
            'state': ['state'],
            'county': ['county'],
            'poi': ['poi', 'interconnection', 'substation', 'point'],
            'date': ['date', 'ir', 'queue date', 'request date'],
            'cod': ['cod', 'in-service', 'commercial', 'operation date'],
        }

        for key, patterns in mappings.items():
            for col in self.df.columns:
                col_lower = col.lower()
                if any(p.lower() in col_lower for p in patterns):
                    self.col_map[key] = col
                    break

    def _get_col(self, key: str) -> Optional[str]:
        """Get mapped column name."""
        return self.col_map.get(key)

    def _categorize_status(self, val) -> str:
        """Categorize a status value."""
        if pd.isna(val):
            return 'Unknown'
        val_lower = str(val).lower()

        for kw in self.WITHDRAWN_KW:
            if kw in val_lower:
                return 'Withdrawn'
        for kw in self.COMPLETED_KW:
            if kw in val_lower:
                return 'Completed'
        for kw in self.ACTIVE_KW:
            if kw in val_lower:
                return 'Active'
        return 'Other'

    def get_stats(self) -> Dict[str, Any]:
        """Get overall statistics."""
        stats = {
            'total_projects': len(self.df),
            'columns': list(self.df.columns),
        }

        # Capacity
        cap_col = self._get_col('capacity')
        if cap_col:
            numeric_cap = pd.to_numeric(self.df[cap_col], errors='coerce')
            stats['total_capacity_mw'] = numeric_cap.sum()
            stats['avg_capacity_mw'] = numeric_cap.mean()
            stats['median_capacity_mw'] = numeric_cap.median()

        # Type breakdown
        type_col = self._get_col('type')
        if type_col:
            stats['type_breakdown'] = self.df[type_col].value_counts().head(10).to_dict()

        # State breakdown
        state_col = self._get_col('state')
        if state_col:
            stats['state_breakdown'] = self.df[state_col].value_counts().head(10).to_dict()

        # Status breakdown
        status_col = self._get_col('status')
        if status_col:
            stats['status_breakdown'] = self.df[status_col].value_counts().head(10).to_dict()

        return stats

    def search(self,
               name: Optional[str] = None,
               developer: Optional[str] = None,
               state: Optional[str] = None,
               fuel_type: Optional[str] = None,
               poi: Optional[str] = None,
               queue_id: Optional[str] = None,
               min_mw: Optional[float] = None,
               max_mw: Optional[float] = None) -> pd.DataFrame:
        """Search for projects matching criteria."""
        mask = pd.Series([True] * len(self.df))

        if name:
            col = self._get_col('name')
            if col:
                mask &= self.df[col].astype(str).str.contains(name, case=False, na=False)

        if developer:
            col = self._get_col('developer')
            if col:
                mask &= self.df[col].astype(str).str.contains(developer, case=False, na=False)

        if state:
            col = self._get_col('state')
            if col:
                mask &= self.df[col].astype(str).str.contains(state, case=False, na=False)

        if fuel_type:
            col = self._get_col('type')
            if col:
                mask &= self.df[col].astype(str).str.contains(fuel_type, case=False, na=False)

        if poi:
            col = self._get_col('poi')
            if col:
                mask &= self.df[col].astype(str).str.contains(poi, case=False, na=False)

        if queue_id:
            col = self._get_col('id')
            if col:
                mask &= self.df[col].astype(str).str.contains(queue_id, case=False, na=False)

        # Capacity range
        cap_col = self._get_col('capacity')
        if cap_col:
            numeric_cap = pd.to_numeric(self.df[cap_col], errors='coerce')
            if min_mw is not None:
                mask &= numeric_cap >= min_mw
            if max_mw is not None:
                mask &= numeric_cap <= max_mw

        return self.df[mask]

    def analyze_poi(self, poi_name: str) -> Dict[str, Any]:
        """Analyze all projects at a POI."""
        projects = self.search(poi=poi_name)

        if len(projects) == 0:
            return {'error': f'No projects found at POI: {poi_name}'}

        cap_col = self._get_col('capacity')

        result = {
            'poi': poi_name,
            'project_count': len(projects),
        }

        if cap_col:
            numeric_cap = pd.to_numeric(projects[cap_col], errors='coerce')
            result['total_capacity_mw'] = numeric_cap.sum()
            result['avg_capacity_mw'] = numeric_cap.mean()

        # Type breakdown at this POI
        type_col = self._get_col('type')
        if type_col:
            result['type_breakdown'] = projects[type_col].value_counts().to_dict()

        return result

    def rank_projects(self, criteria: str = 'capacity') -> pd.DataFrame:
        """Rank projects by various criteria."""
        df = self.df.copy()

        if criteria == 'capacity':
            cap_col = self._get_col('capacity')
            if cap_col:
                df['_capacity_numeric'] = pd.to_numeric(df[cap_col], errors='coerce')
                return df.sort_values('_capacity_numeric', ascending=False)

        elif criteria == 'date':
            date_col = self._get_col('date')
            if date_col:
                df['_date_parsed'] = pd.to_datetime(df[date_col], errors='coerce')
                return df.sort_values('_date_parsed', ascending=True)

        return df


def print_separator(title: str = ""):
    """Print a visual separator."""
    print("\n" + "=" * 60)
    if title:
        print(title)
        print("=" * 60)


def print_stats(stats: Dict[str, Any]):
    """Pretty print statistics."""
    print_separator("QUEUE STATISTICS")

    print(f"\nTotal Projects: {stats['total_projects']:,}")

    if 'total_capacity_mw' in stats:
        cap_gw = stats['total_capacity_mw'] / 1000
        print(f"Total Capacity: {stats['total_capacity_mw']:,.0f} MW ({cap_gw:,.1f} GW)")
        print(f"Avg Capacity: {stats['avg_capacity_mw']:,.1f} MW")
        print(f"Median Capacity: {stats['median_capacity_mw']:,.1f} MW")

    if 'type_breakdown' in stats:
        print("\nBy Type/Fuel:")
        for t, count in list(stats['type_breakdown'].items())[:7]:
            print(f"  {t}: {count}")

    if 'state_breakdown' in stats:
        print("\nBy State:")
        for s, count in list(stats['state_breakdown'].items())[:7]:
            print(f"  {s}: {count}")


def print_projects(df: pd.DataFrame, max_rows: int = 10):
    """Print project summary."""
    if df.empty:
        print("No projects found.")
        return

    print(f"\nFound {len(df)} project(s):")
    print("-" * 60)

    # Key columns to display
    display_cols = []
    for col in df.columns:
        col_lower = col.lower()
        if any(kw in col_lower for kw in ['queue', 'pos', 'id', 'name', 'developer', 'customer',
                                           'mw', 'capacity', 'type', 'fuel', 'state', 'county',
                                           'poi', 'interconnection', 'status', 'phase']):
            display_cols.append(col)

    # Limit to reasonable number
    display_cols = display_cols[:8]

    for idx, (_, row) in enumerate(df.head(max_rows).iterrows()):
        print(f"\n[{idx + 1}]")
        for col in display_cols:
            val = row[col]
            if pd.notna(val) and str(val).strip():
                print(f"  {col}: {val}")

    if len(df) > max_rows:
        print(f"\n... and {len(df) - max_rows} more projects")


def main():
    parser = argparse.ArgumentParser(
        description="Interconnection Queue Analysis Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 analyze.py --stats                    # NYISO statistics
    python3 analyze.py --file queue.xlsx --stats  # Analyze local file
    python3 analyze.py --search "Solar"           # Search by fuel type
    python3 analyze.py --poi "Indian Point"       # POI analysis
    python3 analyze.py --state NY --type Solar    # Filter search
    python3 analyze.py --rank capacity            # Rank by size

Data Sources:
    Download LBL Queued Up data from: https://emp.lbl.gov/queues
    (Look for "Excel data file" in Attachments section)
        """
    )

    # Data source
    parser.add_argument('--file', '-f', help='Local Excel/CSV file to analyze')
    parser.add_argument('--refresh', action='store_true', help='Force refresh cached data')

    # Actions
    parser.add_argument('--stats', action='store_true', help='Show queue statistics')
    parser.add_argument('--columns', action='store_true', help='List available columns')
    parser.add_argument('--rank', choices=['capacity', 'date'], help='Rank projects')

    # Search filters
    parser.add_argument('--search', help='Search by project name')
    parser.add_argument('--developer', help='Search by developer name')
    parser.add_argument('--poi', help='Analyze projects at POI')
    parser.add_argument('--state', help='Filter by state')
    parser.add_argument('--type', help='Filter by fuel/project type')
    parser.add_argument('--project', help='Search by queue ID')
    parser.add_argument('--min-mw', type=float, help='Minimum capacity (MW)')
    parser.add_argument('--max-mw', type=float, help='Maximum capacity (MW)')

    # Output
    parser.add_argument('--limit', type=int, default=10, help='Max results to show')
    parser.add_argument('--export', help='Export results to CSV file')

    args = parser.parse_args()

    # Load data
    loader = QueueData()

    if args.file:
        df = loader.load_file(args.file)
    else:
        df = loader.load_nyiso(force_refresh=args.refresh)

    if df.empty:
        print("No data loaded. Exiting.")
        return 1

    analyzer = QueueAnalyzer(df)

    # List columns
    if args.columns:
        print_separator("AVAILABLE COLUMNS")
        for i, col in enumerate(df.columns):
            print(f"  {i+1}. {col}")
        return 0

    # Statistics
    if args.stats:
        stats = analyzer.get_stats()
        print_stats(stats)
        return 0

    # POI analysis
    if args.poi:
        result = analyzer.analyze_poi(args.poi)
        print_separator(f"POI ANALYSIS: {args.poi}")
        if 'error' in result:
            print(f"Error: {result['error']}")
        else:
            print(f"\nProjects at POI: {result['project_count']}")
            if 'total_capacity_mw' in result:
                print(f"Total Capacity: {result['total_capacity_mw']:,.0f} MW")
            if 'type_breakdown' in result:
                print("\nBy Type:")
                for t, count in result['type_breakdown'].items():
                    print(f"  {t}: {count}")

        # Also show the projects
        projects = analyzer.search(poi=args.poi)
        print_projects(projects, args.limit)
        return 0

    # Rank projects
    if args.rank:
        print_separator(f"PROJECTS RANKED BY {args.rank.upper()}")
        ranked = analyzer.rank_projects(args.rank)
        print_projects(ranked, args.limit)

        if args.export:
            ranked.to_csv(args.export, index=False)
            print(f"\nExported to: {args.export}")
        return 0

    # Search
    if args.search or args.developer or args.state or args.type or args.project or args.min_mw or args.max_mw:
        print_separator("SEARCH RESULTS")
        results = analyzer.search(
            name=args.search,
            developer=args.developer,
            state=args.state,
            fuel_type=args.type,
            queue_id=args.project,
            min_mw=args.min_mw,
            max_mw=args.max_mw,
        )
        print_projects(results, args.limit)

        if args.export and not results.empty:
            results.to_csv(args.export, index=False)
            print(f"\nExported to: {args.export}")
        return 0

    # Default: show stats
    stats = analyzer.get_stats()
    print_stats(stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
