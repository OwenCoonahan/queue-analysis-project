#!/usr/bin/env python3
"""
Interconnection Queue Analysis Toolkit v2

Compatible with Python 3.9+
Uses direct data fetching from RTO sources and LBL Queued Up data.

Requirements:
    pip3 install pandas requests openpyxl xlrd
"""

import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from pathlib import Path
import warnings
import os
import json

warnings.filterwarnings('ignore')

# Data URLs
DATA_URLS = {
    'LBL_QUEUED_UP': 'https://emp.lbl.gov/sites/default/files/queued_up_2025_data_through_2024.xlsx',
    'NYISO_QUEUE': 'https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx',
    'MISO_API': 'https://www.misoenergy.org/api/giqueue/getprojects',
}

# Cache directory
CACHE_DIR = Path(__file__).parent / '.cache'


class QueueDataLoader:
    """Load and cache queue data from various sources."""

    def __init__(self, cache_hours: int = 24):
        self.cache_hours = cache_hours
        CACHE_DIR.mkdir(exist_ok=True)

    def _get_cache_path(self, name: str) -> Path:
        return CACHE_DIR / f"{name}.pkl"

    def _is_cache_valid(self, name: str) -> bool:
        cache_path = self._get_cache_path(name)
        if not cache_path.exists():
            return False
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        return datetime.now() - mtime < timedelta(hours=self.cache_hours)

    def _load_from_cache(self, name: str) -> Optional[pd.DataFrame]:
        if self._is_cache_valid(name):
            try:
                return pd.read_pickle(self._get_cache_path(name))
            except Exception:
                return None
        return None

    def _save_to_cache(self, name: str, df: pd.DataFrame):
        df.to_pickle(self._get_cache_path(name))

    def load_lbl_data(self, force_refresh: bool = False) -> pd.DataFrame:
        """
        Load Lawrence Berkeley Lab Queued Up data.
        This is the most comprehensive free dataset available.
        """
        cache_name = 'lbl_queued_up'

        if not force_refresh:
            cached = self._load_from_cache(cache_name)
            if cached is not None:
                print(f"Loaded LBL data from cache ({len(cached)} projects)")
                return cached

        print("Downloading LBL Queued Up data (this may take a minute)...")

        try:
            # Download the Excel file
            response = requests.get(DATA_URLS['LBL_QUEUED_UP'], timeout=120)
            response.raise_for_status()

            # Save temporarily
            temp_path = CACHE_DIR / 'lbl_temp.xlsx'
            with open(temp_path, 'wb') as f:
                f.write(response.content)

            # Read the main data sheet (usually 'Data' or first sheet)
            xl = pd.ExcelFile(temp_path)
            print(f"Available sheets: {xl.sheet_names[:10]}...")

            # Try to find the main data sheet
            data_sheet = None
            for sheet in xl.sheet_names:
                if 'data' in sheet.lower() or 'queue' in sheet.lower():
                    data_sheet = sheet
                    break

            if data_sheet is None:
                data_sheet = xl.sheet_names[0]

            print(f"Reading sheet: {data_sheet}")
            df = pd.read_excel(temp_path, sheet_name=data_sheet)

            # Clean up
            temp_path.unlink()

            self._save_to_cache(cache_name, df)
            print(f"Loaded {len(df)} projects from LBL data")
            return df

        except Exception as e:
            print(f"Error loading LBL data: {e}")
            # Return empty dataframe with expected columns
            return pd.DataFrame()

    def load_miso_queue(self, force_refresh: bool = False) -> pd.DataFrame:
        """Load MISO queue directly from their API."""
        cache_name = 'miso_queue'

        if not force_refresh:
            cached = self._load_from_cache(cache_name)
            if cached is not None:
                print(f"Loaded MISO data from cache ({len(cached)} projects)")
                return cached

        print("Fetching MISO interconnection queue...")

        try:
            response = requests.get(DATA_URLS['MISO_API'], timeout=60)
            response.raise_for_status()
            data = response.json()

            df = pd.DataFrame(data)
            self._save_to_cache(cache_name, df)
            print(f"Loaded {len(df)} projects from MISO")
            return df

        except Exception as e:
            print(f"Error loading MISO data: {e}")
            return pd.DataFrame()

    def load_nyiso_queue(self, force_refresh: bool = False) -> pd.DataFrame:
        """Load NYISO queue from their Excel file."""
        cache_name = 'nyiso_queue'

        if not force_refresh:
            cached = self._load_from_cache(cache_name)
            if cached is not None:
                print(f"Loaded NYISO data from cache ({len(cached)} projects)")
                return cached

        print("Downloading NYISO interconnection queue...")

        try:
            response = requests.get(DATA_URLS['NYISO_QUEUE'], timeout=60)
            response.raise_for_status()

            temp_path = CACHE_DIR / 'nyiso_temp.xlsx'
            with open(temp_path, 'wb') as f:
                f.write(response.content)

            df = pd.read_excel(temp_path)
            temp_path.unlink()

            self._save_to_cache(cache_name, df)
            print(f"Loaded {len(df)} projects from NYISO")
            return df

        except Exception as e:
            print(f"Error loading NYISO data: {e}")
            return pd.DataFrame()


class QueueAnalyzer:
    """Analyze interconnection queue data."""

    # Status categories
    ACTIVE_KEYWORDS = ['active', 'pending', 'study', 'queue', 'application']
    WITHDRAWN_KEYWORDS = ['withdrawn', 'cancelled', 'suspended', 'terminated']
    COMPLETED_KEYWORDS = ['complete', 'operational', 'service', 'commercial', 'done']

    def __init__(self, data_source: str = 'LBL'):
        """
        Initialize analyzer.

        Args:
            data_source: 'LBL' for Berkeley Lab data, 'MISO', 'NYISO' for direct RTO data
        """
        self.loader = QueueDataLoader()
        self.data_source = data_source.upper()
        self._data = None

    def load_data(self, force_refresh: bool = False) -> pd.DataFrame:
        """Load queue data based on configured source."""
        if self._data is not None and not force_refresh:
            return self._data

        if self.data_source == 'LBL':
            self._data = self.loader.load_lbl_data(force_refresh)
        elif self.data_source == 'MISO':
            self._data = self.loader.load_miso_queue(force_refresh)
        elif self.data_source == 'NYISO':
            self._data = self.loader.load_nyiso_queue(force_refresh)
        else:
            print(f"Unknown data source: {self.data_source}, using LBL")
            self._data = self.loader.load_lbl_data(force_refresh)

        return self._data

    def _find_column(self, patterns: List[str], df: pd.DataFrame = None) -> Optional[str]:
        """Find a column matching any of the given patterns."""
        if df is None:
            df = self.load_data()

        for col in df.columns:
            col_lower = col.lower()
            for pattern in patterns:
                if pattern.lower() in col_lower:
                    return col
        return None

    def _categorize_status(self, status_val: str) -> str:
        """Categorize a status value."""
        if pd.isna(status_val):
            return 'Unknown'

        status_lower = str(status_val).lower()

        for kw in self.WITHDRAWN_KEYWORDS:
            if kw in status_lower:
                return 'Withdrawn'

        for kw in self.COMPLETED_KEYWORDS:
            if kw in status_lower:
                return 'Completed'

        for kw in self.ACTIVE_KEYWORDS:
            if kw in status_lower:
                return 'Active'

        return 'Other'

    def get_statistics(self,
                       iso: Optional[str] = None,
                       fuel_type: Optional[str] = None,
                       state: Optional[str] = None) -> Dict[str, Any]:
        """
        Get queue statistics with optional filters.
        """
        df = self.load_data()

        if df.empty:
            return {"error": "No data loaded"}

        # Apply filters
        mask = pd.Series([True] * len(df))

        # ISO filter
        iso_col = self._find_column(['iso', 'region', 'rto', 'balancing'])
        if iso and iso_col:
            mask &= df[iso_col].astype(str).str.contains(iso, case=False, na=False)

        # Fuel type filter
        fuel_col = self._find_column(['fuel', 'type', 'resource', 'generation', 'technology'])
        if fuel_type and fuel_col:
            mask &= df[fuel_col].astype(str).str.contains(fuel_type, case=False, na=False)

        # State filter
        state_col = self._find_column(['state'])
        if state and state_col:
            mask &= df[state_col].astype(str).str.contains(state, case=False, na=False)

        filtered = df[mask]

        # Calculate statistics
        stats = {
            "total_projects": len(filtered),
            "filters": {"iso": iso, "fuel_type": fuel_type, "state": state},
            "data_source": self.data_source
        }

        # Capacity
        cap_col = self._find_column(['capacity', 'mw', 'size'])
        if cap_col:
            stats["total_capacity_mw"] = filtered[cap_col].sum()
            stats["avg_capacity_mw"] = filtered[cap_col].mean()
            stats["median_capacity_mw"] = filtered[cap_col].median()

        # Status breakdown
        status_col = self._find_column(['status'])
        if status_col:
            stats["status_breakdown"] = filtered[status_col].value_counts().head(10).to_dict()

            # Categorized status
            categorized = filtered[status_col].apply(self._categorize_status)
            stats["status_categories"] = categorized.value_counts().to_dict()

        # Fuel type breakdown
        if fuel_col:
            stats["fuel_breakdown"] = filtered[fuel_col].value_counts().head(10).to_dict()

        # ISO breakdown (if not filtered)
        if not iso and iso_col:
            stats["iso_breakdown"] = filtered[iso_col].value_counts().head(10).to_dict()

        return stats

    def find_projects(self,
                      queue_id: Optional[str] = None,
                      name: Optional[str] = None,
                      developer: Optional[str] = None,
                      poi: Optional[str] = None,
                      state: Optional[str] = None,
                      iso: Optional[str] = None,
                      min_capacity: Optional[float] = None,
                      max_capacity: Optional[float] = None) -> pd.DataFrame:
        """
        Find projects matching criteria.
        """
        df = self.load_data()

        if df.empty:
            return df

        mask = pd.Series([True] * len(df))

        # Queue ID
        id_col = self._find_column(['queue', 'id', 'request', 'project_id'])
        if queue_id and id_col:
            mask &= df[id_col].astype(str).str.contains(queue_id, case=False, na=False)

        # Name
        name_col = self._find_column(['name', 'project'])
        if name and name_col:
            mask &= df[name_col].astype(str).str.contains(name, case=False, na=False)

        # Developer
        dev_col = self._find_column(['developer', 'owner', 'entity', 'applicant', 'interconnection_customer'])
        if developer and dev_col:
            mask &= df[dev_col].astype(str).str.contains(developer, case=False, na=False)

        # POI
        poi_col = self._find_column(['poi', 'substation', 'interconnection_point', 'point_of_interconnection'])
        if poi and poi_col:
            mask &= df[poi_col].astype(str).str.contains(poi, case=False, na=False)

        # State
        state_col = self._find_column(['state'])
        if state and state_col:
            mask &= df[state_col].astype(str).str.contains(state, case=False, na=False)

        # ISO
        iso_col = self._find_column(['iso', 'region', 'rto'])
        if iso and iso_col:
            mask &= df[iso_col].astype(str).str.contains(iso, case=False, na=False)

        # Capacity range
        cap_col = self._find_column(['capacity', 'mw'])
        if cap_col:
            if min_capacity is not None:
                mask &= df[cap_col] >= min_capacity
            if max_capacity is not None:
                mask &= df[cap_col] <= max_capacity

        return df[mask]

    def analyze_poi(self, poi_name: str, iso: Optional[str] = None) -> Dict[str, Any]:
        """Analyze all projects at a specific POI."""
        projects = self.find_projects(poi=poi_name, iso=iso)

        if len(projects) == 0:
            return {"error": f"No projects found at POI: {poi_name}"}

        cap_col = self._find_column(['capacity', 'mw'])
        status_col = self._find_column(['status'])

        analysis = {
            "poi": poi_name,
            "total_projects": len(projects),
        }

        if cap_col:
            analysis["total_capacity_mw"] = projects[cap_col].sum()
            analysis["avg_capacity_mw"] = projects[cap_col].mean()

        if status_col:
            analysis["status_breakdown"] = projects[status_col].value_counts().to_dict()

            # Active projects
            active_mask = projects[status_col].apply(
                lambda x: self._categorize_status(x) == 'Active'
            )
            active = projects[active_mask]
            analysis["active_projects"] = len(active)
            if cap_col:
                analysis["active_capacity_mw"] = active[cap_col].sum()

        return analysis

    def calculate_completion_rates(self,
                                   iso: Optional[str] = None,
                                   fuel_type: Optional[str] = None) -> Dict[str, Any]:
        """Calculate completion and withdrawal rates."""
        df = self.load_data()

        if df.empty:
            return {"error": "No data loaded"}

        # Apply filters
        mask = pd.Series([True] * len(df))

        iso_col = self._find_column(['iso', 'region', 'rto'])
        if iso and iso_col:
            mask &= df[iso_col].astype(str).str.contains(iso, case=False, na=False)

        fuel_col = self._find_column(['fuel', 'type', 'resource'])
        if fuel_type and fuel_col:
            mask &= df[fuel_col].astype(str).str.contains(fuel_type, case=False, na=False)

        filtered = df[mask]

        # Categorize status
        status_col = self._find_column(['status'])
        if not status_col:
            return {"error": "No status column found"}

        categories = filtered[status_col].apply(self._categorize_status)

        total = len(filtered)
        active = (categories == 'Active').sum()
        withdrawn = (categories == 'Withdrawn').sum()
        completed = (categories == 'Completed').sum()
        other = total - active - withdrawn - completed

        # Rates based on resolved projects
        resolved = completed + withdrawn

        return {
            "total_projects": total,
            "active": active,
            "withdrawn": withdrawn,
            "completed": completed,
            "other": other,
            "completion_rate_pct": round(completed / resolved * 100, 1) if resolved > 0 else None,
            "withdrawal_rate_pct": round(withdrawn / resolved * 100, 1) if resolved > 0 else None,
            "filters": {"iso": iso, "fuel_type": fuel_type}
        }

    def get_column_info(self) -> Dict[str, List[str]]:
        """Get information about available columns."""
        df = self.load_data()
        return {
            "columns": list(df.columns),
            "dtypes": {col: str(df[col].dtype) for col in df.columns},
            "sample_values": {col: df[col].dropna().head(3).tolist() for col in df.columns[:20]}
        }


def print_stats(stats: Dict[str, Any]):
    """Pretty print statistics."""
    print("\n" + "=" * 60)
    print("QUEUE STATISTICS")
    print("=" * 60)

    if "error" in stats:
        print(f"Error: {stats['error']}")
        return

    print(f"\nData Source: {stats.get('data_source', 'Unknown')}")
    print(f"Total Projects: {stats['total_projects']:,}")

    if 'total_capacity_mw' in stats:
        print(f"Total Capacity: {stats['total_capacity_mw']:,.0f} MW ({stats['total_capacity_mw']/1000:,.1f} GW)")
    if 'avg_capacity_mw' in stats:
        print(f"Avg Capacity: {stats['avg_capacity_mw']:,.1f} MW")

    if 'status_categories' in stats:
        print("\nStatus Categories:")
        for status, count in stats['status_categories'].items():
            print(f"  {status}: {count:,}")

    if 'fuel_breakdown' in stats:
        print("\nTop Fuel Types:")
        for fuel, count in list(stats['fuel_breakdown'].items())[:5]:
            print(f"  {fuel}: {count:,}")

    if 'iso_breakdown' in stats:
        print("\nBy ISO/Region:")
        for iso, count in list(stats['iso_breakdown'].items())[:7]:
            print(f"  {iso}: {count:,}")


def main():
    """Main demo."""
    print("=" * 60)
    print("INTERCONNECTION QUEUE ANALYZER v2")
    print("=" * 60)

    # Initialize with LBL data (most comprehensive)
    analyzer = QueueAnalyzer('LBL')

    # Load data
    print("\nLoading data...")
    df = analyzer.load_data()

    if df.empty:
        print("Failed to load data. Check your internet connection.")
        return

    # Show available columns
    print(f"\nLoaded {len(df)} projects")
    print(f"Columns: {list(df.columns)[:10]}...")

    # Get overall statistics
    stats = analyzer.get_statistics()
    print_stats(stats)

    # PJM-specific stats
    print("\n" + "-" * 60)
    print("PJM STATISTICS")
    print("-" * 60)
    pjm_stats = analyzer.get_statistics(iso='PJM')
    if pjm_stats.get('total_projects', 0) > 0:
        print(f"PJM Projects: {pjm_stats['total_projects']:,}")
        if 'total_capacity_mw' in pjm_stats:
            print(f"PJM Capacity: {pjm_stats['total_capacity_mw']:,.0f} MW")

    # Completion rates
    print("\n" + "-" * 60)
    print("COMPLETION RATES")
    print("-" * 60)
    rates = analyzer.calculate_completion_rates()
    print(f"Completed: {rates['completed']:,} ({rates.get('completion_rate_pct', 'N/A')}%)")
    print(f"Withdrawn: {rates['withdrawn']:,} ({rates.get('withdrawal_rate_pct', 'N/A')}%)")
    print(f"Active: {rates['active']:,}")

    # Solar stats
    print("\n" + "-" * 60)
    print("SOLAR PROJECT STATISTICS")
    print("-" * 60)
    solar_stats = analyzer.get_statistics(fuel_type='Solar')
    if solar_stats.get('total_projects', 0) > 0:
        print(f"Solar Projects: {solar_stats['total_projects']:,}")
        if 'total_capacity_mw' in solar_stats:
            print(f"Solar Capacity: {solar_stats['total_capacity_mw']:,.0f} MW")


if __name__ == "__main__":
    main()
