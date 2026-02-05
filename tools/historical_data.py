#!/usr/bin/env python3
"""
Historical Data Module for Interconnection Queue Analysis

Loads and parses LBL Queued Up and Interconnection Costs datasets
to provide historical comparables for feasibility reports.

Data Sources:
- LBL Queued Up: https://emp.lbl.gov/queues (main queue tracker)
- LBL IC Costs: Interconnection cost studies by region (PJM, SPP, etc.)

Usage:
    # As a module
    from historical_data import HistoricalData, get_comparables

    hd = HistoricalData()
    comparables = hd.get_comparable_projects('PJM', 'Solar', 200)
    funnel = hd.get_completion_funnel('PJM', 'Solar')

    # Quick access
    comps = get_comparables('MISO', 'Wind', 150)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime
from dataclasses import dataclass, field
import warnings

warnings.filterwarnings('ignore')

# Cache directory for downloaded data
CACHE_DIR = Path(__file__).parent / '.cache'
CACHE_DIR.mkdir(exist_ok=True)


# =============================================================================
# DATA STRUCTURE DEFINITIONS
# =============================================================================

@dataclass
class ProjectComparable:
    """A historical project used as a comparable."""
    project_id: str
    project_name: Optional[str] = None
    region: Optional[str] = None
    state: Optional[str] = None
    project_type: Optional[str] = None
    capacity_mw: Optional[float] = None
    queue_date: Optional[datetime] = None
    cod_date: Optional[datetime] = None
    status: Optional[str] = None
    developer: Optional[str] = None
    interconnection_cost_total: Optional[float] = None  # $ millions
    interconnection_cost_per_kw: Optional[float] = None  # $/kW
    time_to_cod_months: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'project_id': self.project_id,
            'project_name': self.project_name,
            'region': self.region,
            'state': self.state,
            'project_type': self.project_type,
            'capacity_mw': self.capacity_mw,
            'queue_date': str(self.queue_date) if self.queue_date else None,
            'cod_date': str(self.cod_date) if self.cod_date else None,
            'status': self.status,
            'developer': self.developer,
            'interconnection_cost_total': self.interconnection_cost_total,
            'interconnection_cost_per_kw': self.interconnection_cost_per_kw,
            'time_to_cod_months': self.time_to_cod_months,
        }


@dataclass
class CompletionFunnel:
    """Funnel data showing project attrition through stages."""
    region: str
    project_type: str
    total_entered: int = 0
    active_in_queue: int = 0
    withdrawn: int = 0
    completed: int = 0
    operational: int = 0

    @property
    def completion_rate(self) -> float:
        """Percentage that reached operational status."""
        if self.total_entered == 0:
            return 0.0
        return (self.operational / self.total_entered) * 100

    @property
    def withdrawal_rate(self) -> float:
        """Percentage that withdrew."""
        if self.total_entered == 0:
            return 0.0
        return (self.withdrawn / self.total_entered) * 100

    def to_dict(self) -> Dict[str, Any]:
        return {
            'region': self.region,
            'project_type': self.project_type,
            'total_entered': self.total_entered,
            'active_in_queue': self.active_in_queue,
            'withdrawn': self.withdrawn,
            'completed': self.completed,
            'operational': self.operational,
            'completion_rate_pct': round(self.completion_rate, 1),
            'withdrawal_rate_pct': round(self.withdrawal_rate, 1),
        }


# =============================================================================
# COLUMN MAPPING PATTERNS
# =============================================================================

# Patterns for identifying columns in LBL data
COLUMN_PATTERNS = {
    'project_id': ['queue', 'id', 'request', 'pos', 'number', 'q_id'],
    'project_name': ['name', 'project'],
    'region': ['region', 'iso', 'rto', 'entity'],
    'state': ['state'],
    'project_type': ['type', 'fuel', 'resource', 'technology', 'generation'],
    'capacity': ['capacity', 'mw', 'nameplate', 'size'],
    'status': ['status', 'phase', 'stage', 'withdrawn'],
    'developer': ['developer', 'owner', 'customer', 'applicant', 'entity'],
    'queue_date': ['queue date', 'request date', 'ir date', 'entry'],
    'cod_date': ['cod', 'in-service', 'commercial', 'operation date', 'online'],
    'cost_total': ['cost', 'total cost', 'upgrade cost', 'ic cost'],
    'cost_per_kw': ['$/kw', 'cost per kw', 'per kw'],
}


# =============================================================================
# MAIN CLASS
# =============================================================================

class HistoricalData:
    """
    Load and query historical interconnection data.

    Provides access to LBL Queued Up data and interconnection cost studies
    for generating historical comparables in feasibility reports.
    """

    # Status keywords for categorization
    ACTIVE_KEYWORDS = ['active', 'pending', 'study', 'queue', 'in progress']
    WITHDRAWN_KEYWORDS = ['withdrawn', 'cancelled', 'suspended', 'terminated', 'inactive']
    COMPLETED_KEYWORDS = ['complete', 'operational', 'service', 'commercial', 'done', 'energized', 'online']

    # Known regions/ISOs
    KNOWN_REGIONS = [
        'PJM', 'MISO', 'ERCOT', 'CAISO', 'SPP', 'NYISO', 'ISONE',
        'WECC', 'SERC', 'Southeast', 'Southwest', 'Northwest',
    ]

    # Project type normalization
    TYPE_MAPPINGS = {
        'solar': ['solar', 'pv', 'photovoltaic', 's'],
        'wind': ['wind', 'w', 'onshore wind'],
        'offshore_wind': ['offshore', 'osw', 'offshore wind'],
        'battery': ['battery', 'storage', 'bess', 'es', 'energy storage'],
        'gas': ['gas', 'ng', 'natural gas', 'cc', 'ct', 'combined cycle', 'combustion'],
        'hybrid': ['hybrid', 'solar+storage', 'wind+storage', 'co-located'],
        'load': ['load', 'l', 'datacenter', 'data center', 'industrial'],
    }

    def __init__(self, auto_load: bool = True):
        """
        Initialize the historical data handler.

        Args:
            auto_load: Automatically load cached data if available
        """
        self.queued_up_df: Optional[pd.DataFrame] = None
        self.ic_costs_df: Optional[pd.DataFrame] = None
        self.ic_costs_by_region: Dict[str, pd.DataFrame] = {}

        # Column mappings (populated during load)
        self._queued_up_cols: Dict[str, str] = {}
        self._ic_costs_cols: Dict[str, str] = {}

        if auto_load:
            self._load_data()

    def _load_data(self) -> None:
        """Load cached LBL datasets from disk."""
        self._load_queued_up_data()
        self._load_ic_costs_data()

    def _load_queued_up_data(self) -> None:
        """
        Load LBL Queued Up data from cache.

        Expected file: .cache/lbl_queued_up.xlsx
        This is the main interconnection queue tracker dataset.
        """
        queued_up_path = CACHE_DIR / 'lbl_queued_up.xlsx'

        if not queued_up_path.exists():
            # Try alternative filenames
            alternatives = [
                'queued_up.xlsx',
                'lbl_queued_up_data.xlsx',
                'queues.xlsx',
                'LBNL_Ix_Queue_Data_File_thru2024_v2.xlsx',
            ]
            for alt in alternatives:
                alt_path = CACHE_DIR / alt
                if alt_path.exists():
                    queued_up_path = alt_path
                    break

        if not queued_up_path.exists():
            print(f"Note: LBL Queued Up data not found at {queued_up_path}")
            print("  Download from: https://emp.lbl.gov/queues")
            return

        try:
            # Load Excel file
            xl = pd.ExcelFile(queued_up_path)

            # Look for the Complete Queue Data sheet (LBL format)
            sheet_name = None
            for s in xl.sheet_names:
                s_lower = s.lower()
                if 'complete queue' in s_lower or '03.' in s:
                    sheet_name = s
                    break
                elif any(kw in s_lower for kw in ['data', 'project', 'queue', 'all']):
                    sheet_name = s

            if sheet_name is None:
                sheet_name = xl.sheet_names[0]

            # LBL files have header on row 1 (0-indexed), skip row 0
            self.queued_up_df = pd.read_excel(queued_up_path, sheet_name=sheet_name, header=1)

            # Convert Excel serial dates to datetime
            self._convert_lbl_dates()

            # Use direct column mapping for LBL format
            self._map_lbl_columns()

            print(f"Loaded Queued Up data: {len(self.queued_up_df):,} rows from {queued_up_path.name}")

        except Exception as e:
            print(f"Error loading Queued Up data: {e}")

    def _convert_lbl_dates(self) -> None:
        """Convert Excel serial date numbers to datetime objects."""
        from datetime import timedelta

        date_cols = ['q_date', 'on_date', 'wd_date', 'ia_date', 'prop_date']

        for col in date_cols:
            if col in self.queued_up_df.columns:
                def excel_to_date(val):
                    if pd.isna(val) or val == 'NA':
                        return pd.NaT
                    try:
                        return datetime(1899, 12, 30) + timedelta(days=float(val))
                    except:
                        return pd.NaT

                self.queued_up_df[f'{col}_parsed'] = self.queued_up_df[col].apply(excel_to_date)

    def _map_lbl_columns(self) -> None:
        """Map LBL-specific column names to standard names."""
        # LBL column name -> standard key
        lbl_mappings = {
            'q_id': 'project_id',
            'q_status': 'status',
            'q_date_parsed': 'queue_date',
            'on_date_parsed': 'cod_date',
            'wd_date_parsed': 'withdrawal_date',
            'region': 'region',
            'state': 'state',
            'county': 'county',
            'type_clean': 'project_type',
            'mw1': 'capacity',
            'developer': 'developer',
            'project_name': 'project_name',
            'poi_name': 'poi',
        }

        for lbl_col, std_key in lbl_mappings.items():
            if lbl_col in self.queued_up_df.columns:
                self._queued_up_cols[std_key] = lbl_col

    def _load_ic_costs_data(self) -> None:
        """
        Load interconnection cost data from cache.

        Expected files (LBL format with 'introduction', 'data', 'codebook' sheets):
        - .cache/pjm_costs_2022_clean_data.xlsx
        - .cache/spp_costs_2023_clean_data.xlsx
        - .cache/miso_costs_2021_clean_data.xlsx
        - .cache/nyiso_interconnection_cost_data.xlsx
        - .cache/isone_interconnection_cost_data.xlsx
        """
        # Regional file patterns (filename contains -> region)
        regional_files = {
            'PJM': ['pjm_costs', 'pjm_ic'],
            'SPP': ['spp_costs', 'spp_ic'],
            'MISO': ['miso_costs', 'miso_ic'],
            'NYISO': ['nyiso_interconnection', 'nyiso_costs', 'nyiso_ic'],
            'ISO-NE': ['isone_interconnection', 'isone_costs', 'isone_ic'],
            'CAISO': ['caiso_costs', 'caiso_ic'],
            'ERCOT': ['ercot_costs', 'ercot_ic'],
        }

        # Scan cache directory for matching files
        for filepath in CACHE_DIR.glob('*.xlsx'):
            filename_lower = filepath.name.lower()

            for region, patterns in regional_files.items():
                if any(p in filename_lower for p in patterns):
                    try:
                        # LBL cost files have 'data' sheet with actual data
                        xl = pd.ExcelFile(filepath)
                        if 'data' in xl.sheet_names:
                            df = pd.read_excel(filepath, sheet_name='data')
                        else:
                            df = pd.read_excel(filepath)

                        # Standardize column names for cost files
                        df = self._standardize_cost_columns(df, region)

                        self.ic_costs_by_region[region] = df
                        print(f"Loaded {region} IC Costs: {len(df):,} rows from {filepath.name}")
                    except Exception as e:
                        print(f"Error loading {region} costs from {filepath.name}: {e}")
                    break

        # Combine all regional data into single dataframe
        if self.ic_costs_by_region:
            all_dfs = []
            for region, df in self.ic_costs_by_region.items():
                df = df.copy()
                df['_region'] = region
                all_dfs.append(df)
            self.ic_costs_df = pd.concat(all_dfs, ignore_index=True)
            print(f"Combined IC Costs: {len(self.ic_costs_df):,} total rows")

    def _standardize_cost_columns(self, df: pd.DataFrame, region: str) -> pd.DataFrame:
        """Standardize column names across different LBL cost files."""
        # Common column mappings
        col_renames = {}

        for col in df.columns:
            col_lower = col.lower()
            if 'total cost/kw' in col_lower or 'total/kw' in col_lower:
                col_renames[col] = 'cost_per_kw'
            elif 'poi cost/kw' in col_lower or 'poi/kw' in col_lower:
                col_renames[col] = 'poi_cost_per_kw'
            elif 'network cost/kw' in col_lower or 'network/kw' in col_lower:
                col_renames[col] = 'network_cost_per_kw'
            elif col_lower in ['nameplate mw', 'nameplate', 'mw']:
                col_renames[col] = 'capacity_mw'
            elif col_lower in ['fuel', 'resource type', 'type']:
                col_renames[col] = 'project_type'
            elif 'request status' in col_lower or col_lower == 'status':
                col_renames[col] = 'status'
            elif 'queue date' in col_lower:
                col_renames[col] = 'queue_date'
            elif 'in service' in col_lower or 'service date' in col_lower:
                col_renames[col] = 'cod_date'
            elif 'withdrawn date' in col_lower:
                col_renames[col] = 'withdrawal_date'

        if col_renames:
            df = df.rename(columns=col_renames)

        return df

    def _map_columns(self, df: pd.DataFrame, col_map: Dict[str, str]) -> None:
        """Map standard column names to actual column names in the dataframe."""
        col_map.clear()

        for key, patterns in COLUMN_PATTERNS.items():
            for col in df.columns:
                col_lower = str(col).lower()
                if any(p.lower() in col_lower for p in patterns):
                    col_map[key] = col
                    break

    def _get_col(self, df: pd.DataFrame, key: str, col_map: Dict[str, str]) -> Optional[str]:
        """Get mapped column name for a key."""
        return col_map.get(key)

    def _normalize_project_type(self, type_value: Any) -> Optional[str]:
        """Normalize project type to standard categories."""
        if pd.isna(type_value):
            return None

        type_lower = str(type_value).lower().strip()

        for normalized, keywords in self.TYPE_MAPPINGS.items():
            for kw in keywords:
                if kw in type_lower:
                    return normalized

        return str(type_value)  # Return original if no match

    def _categorize_status(self, status_value: Any) -> str:
        """Categorize a status value into Active/Withdrawn/Completed/Unknown."""
        if pd.isna(status_value):
            return 'Unknown'

        status_lower = str(status_value).lower()

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

    # =========================================================================
    # PUBLIC QUERY METHODS
    # =========================================================================

    def get_comparable_projects(
        self,
        region: str,
        project_type: str,
        capacity_mw: float,
        capacity_tolerance: float = 0.5,
        status_filter: Optional[str] = None,
        limit: int = 20,
    ) -> pd.DataFrame:
        """
        Get historical projects similar to the target project.

        Args:
            region: ISO/RTO region (e.g., 'PJM', 'MISO')
            project_type: Project type (e.g., 'Solar', 'Wind', 'Battery')
            capacity_mw: Target capacity in MW
            capacity_tolerance: Tolerance range as fraction (0.5 = +/- 50%)
            status_filter: Optional status filter ('Active', 'Completed', 'Withdrawn')
            limit: Maximum number of results

        Returns:
            DataFrame with comparable projects
        """
        if self.queued_up_df is None:
            print("Error: Queued Up data not loaded")
            return pd.DataFrame()

        df = self.queued_up_df.copy()

        # Get column mappings
        region_col = self._get_col(df, 'region', self._queued_up_cols)
        type_col = self._get_col(df, 'project_type', self._queued_up_cols)
        cap_col = self._get_col(df, 'capacity', self._queued_up_cols)
        status_col = self._get_col(df, 'status', self._queued_up_cols)

        # Build filter mask
        mask = pd.Series([True] * len(df))

        # Filter by region
        if region_col:
            region_upper = region.upper()
            mask &= df[region_col].astype(str).str.upper().str.contains(region_upper, na=False)

        # Filter by project type
        if type_col:
            normalized_type = self._normalize_project_type(project_type)
            # Match against normalized types
            def type_matches(val):
                return self._normalize_project_type(val) == normalized_type
            mask &= df[type_col].apply(type_matches)

        # Filter by capacity range
        if cap_col:
            min_cap = capacity_mw * (1 - capacity_tolerance)
            max_cap = capacity_mw * (1 + capacity_tolerance)
            numeric_cap = pd.to_numeric(df[cap_col], errors='coerce')
            mask &= (numeric_cap >= min_cap) & (numeric_cap <= max_cap)

        # Filter by status
        if status_filter and status_col:
            status_cat = df[status_col].apply(self._categorize_status)
            mask &= (status_cat == status_filter)

        # Apply filter and limit
        result = df[mask].head(limit)

        return result

    def get_completion_funnel(
        self,
        region: str,
        project_type: str,
        min_capacity_mw: float = 0,
        year_range: Optional[Tuple[int, int]] = None,
    ) -> Dict[str, Any]:
        """
        Get counts at each stage for funnel visualization.

        Args:
            region: ISO/RTO region
            project_type: Project type to filter
            min_capacity_mw: Minimum capacity filter
            year_range: Optional (start_year, end_year) to filter queue entry

        Returns:
            Dictionary with funnel stage counts
        """
        if self.queued_up_df is None:
            return {'error': 'Queued Up data not loaded'}

        df = self.queued_up_df.copy()

        # Get column mappings
        region_col = self._get_col(df, 'region', self._queued_up_cols)
        type_col = self._get_col(df, 'project_type', self._queued_up_cols)
        cap_col = self._get_col(df, 'capacity', self._queued_up_cols)
        status_col = self._get_col(df, 'status', self._queued_up_cols)
        date_col = self._get_col(df, 'queue_date', self._queued_up_cols)

        # Build filter mask
        mask = pd.Series([True] * len(df))

        # Filter by region
        if region_col:
            mask &= df[region_col].astype(str).str.upper().str.contains(region.upper(), na=False)

        # Filter by project type
        if type_col:
            normalized_type = self._normalize_project_type(project_type)
            mask &= df[type_col].apply(lambda x: self._normalize_project_type(x) == normalized_type)

        # Filter by capacity
        if cap_col:
            numeric_cap = pd.to_numeric(df[cap_col], errors='coerce')
            mask &= (numeric_cap >= min_capacity_mw)

        # Filter by year range
        if year_range and date_col:
            dates = pd.to_datetime(df[date_col], errors='coerce')
            mask &= (dates.dt.year >= year_range[0]) & (dates.dt.year <= year_range[1])

        filtered = df[mask]

        # Count by status category
        funnel = CompletionFunnel(region=region, project_type=project_type)
        funnel.total_entered = len(filtered)

        if status_col:
            status_cats = filtered[status_col].apply(self._categorize_status)
            funnel.active_in_queue = (status_cats == 'Active').sum()
            funnel.withdrawn = (status_cats == 'Withdrawn').sum()
            funnel.completed = (status_cats == 'Completed').sum()
            funnel.operational = funnel.completed  # Assume completed = operational for now

        return funnel.to_dict()

    def get_cost_distribution(
        self,
        region: Optional[str] = None,
        project_type: Optional[str] = None,
        min_capacity_mw: float = 0,
    ) -> pd.DataFrame:
        """
        Get cost $/kW distribution for projects (preferably completed).

        Args:
            region: Optional region filter
            project_type: Optional project type filter
            min_capacity_mw: Minimum capacity filter

        Returns:
            DataFrame with cost data including percentiles
        """
        # Prefer regional cost data if available
        if region and region.upper() in self.ic_costs_by_region:
            df = self.ic_costs_by_region[region.upper()].copy()
        elif self.ic_costs_df is not None:
            df = self.ic_costs_df.copy()
        else:
            return pd.DataFrame({'error': ['IC Costs data not loaded']})

        # Get column mappings
        col_map: Dict[str, str] = {}
        self._map_columns(df, col_map)

        type_col = self._get_col(df, 'project_type', col_map)
        cap_col = self._get_col(df, 'capacity', col_map)
        cost_col = self._get_col(df, 'cost_per_kw', col_map)
        region_col = self._get_col(df, 'region', col_map)

        # Build filter
        mask = pd.Series([True] * len(df))

        if region and region_col:
            mask &= df[region_col].astype(str).str.upper().str.contains(region.upper(), na=False)

        if project_type and type_col:
            normalized = self._normalize_project_type(project_type)
            mask &= df[type_col].apply(lambda x: self._normalize_project_type(x) == normalized)

        if cap_col:
            numeric_cap = pd.to_numeric(df[cap_col], errors='coerce')
            mask &= (numeric_cap >= min_capacity_mw)

        result = df[mask]

        # Add summary statistics if cost column exists
        if cost_col and not result.empty:
            costs = pd.to_numeric(result[cost_col], errors='coerce').dropna()
            if len(costs) > 0:
                result = result.assign(
                    _p25=costs.quantile(0.25),
                    _p50=costs.quantile(0.50),
                    _p75=costs.quantile(0.75),
                    _mean=costs.mean(),
                )

        return result

    def get_timeline_distribution(
        self,
        region: Optional[str] = None,
        project_type: Optional[str] = None,
        completed_only: bool = True,
    ) -> pd.DataFrame:
        """
        Get time-to-COD distribution for projects.

        Args:
            region: Optional region filter
            project_type: Optional project type filter
            completed_only: Only include completed projects

        Returns:
            DataFrame with timeline data
        """
        if self.queued_up_df is None:
            return pd.DataFrame({'error': ['Queued Up data not loaded']})

        df = self.queued_up_df.copy()

        # Get column mappings
        region_col = self._get_col(df, 'region', self._queued_up_cols)
        type_col = self._get_col(df, 'project_type', self._queued_up_cols)
        status_col = self._get_col(df, 'status', self._queued_up_cols)
        queue_date_col = self._get_col(df, 'queue_date', self._queued_up_cols)
        cod_date_col = self._get_col(df, 'cod_date', self._queued_up_cols)

        # Build filter
        mask = pd.Series([True] * len(df))

        if region and region_col:
            mask &= df[region_col].astype(str).str.upper().str.contains(region.upper(), na=False)

        if project_type and type_col:
            normalized = self._normalize_project_type(project_type)
            mask &= df[type_col].apply(lambda x: self._normalize_project_type(x) == normalized)

        if completed_only and status_col:
            mask &= df[status_col].apply(self._categorize_status) == 'Completed'

        result = df[mask]

        # Calculate time to COD if both date columns exist
        if queue_date_col and cod_date_col and not result.empty:
            queue_dates = pd.to_datetime(result[queue_date_col], errors='coerce')
            cod_dates = pd.to_datetime(result[cod_date_col], errors='coerce')

            # Calculate months difference
            time_to_cod = ((cod_dates - queue_dates).dt.days / 30).round(0)
            result = result.assign(time_to_cod_months=time_to_cod)

        return result

    def get_developer_outcomes(
        self,
        developer_category: Optional[str] = None,
        region: Optional[str] = None,
    ) -> Dict[str, float]:
        """
        Get completion rates by developer type.

        Args:
            developer_category: Filter by category ('Experienced (5+)', 'Mid-tier (2-4)', etc.)
            region: Optional region filter

        Returns:
            Dictionary with completion statistics by developer category
        """
        if self.queued_up_df is None:
            return {'error': 'Queued Up data not loaded'}

        df = self.queued_up_df.copy()

        # Get column mappings
        dev_col = self._get_col(df, 'developer', self._queued_up_cols)
        status_col = self._get_col(df, 'status', self._queued_up_cols)
        region_col = self._get_col(df, 'region', self._queued_up_cols)

        if not dev_col or not status_col:
            return {'error': 'Required columns not found'}

        # Apply region filter
        if region and region_col:
            df = df[df[region_col].astype(str).str.upper().str.contains(region.upper(), na=False)]

        # Count projects per developer
        dev_counts = df[dev_col].value_counts()

        # Categorize developers
        def categorize(dev_name):
            count = dev_counts.get(dev_name, 0)
            return self.categorize_developer(dev_name, count)

        df = df.assign(_dev_category=df[dev_col].apply(categorize))

        # Calculate completion rates by category
        results = {}

        for category in df['_dev_category'].unique():
            cat_df = df[df['_dev_category'] == category]
            total = len(cat_df)
            completed = (cat_df[status_col].apply(self._categorize_status) == 'Completed').sum()

            results[category] = {
                'total_projects': total,
                'completed_projects': completed,
                'completion_rate': round((completed / total * 100) if total > 0 else 0, 1),
            }

        # Filter by category if specified
        if developer_category and developer_category in results:
            return results[developer_category]

        return results

    def categorize_developer(
        self,
        developer_name: str,
        projects_count: int,
    ) -> str:
        """
        Categorize developer by experience level.

        Args:
            developer_name: Name of the developer
            projects_count: Number of projects in queue

        Returns:
            Category string
        """
        if projects_count >= 5:
            return "Experienced (5+)"
        elif projects_count >= 2:
            return "Mid-tier (2-4)"
        elif projects_count == 1:
            return "Single-project"
        else:
            return "Unknown/SPV"

    def get_summary_statistics(
        self,
        region: Optional[str] = None,
        project_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get summary statistics for the dataset.

        Args:
            region: Optional region filter
            project_type: Optional project type filter

        Returns:
            Dictionary with summary statistics
        """
        if self.queued_up_df is None:
            return {'error': 'Queued Up data not loaded'}

        df = self.queued_up_df.copy()

        # Apply filters
        region_col = self._get_col(df, 'region', self._queued_up_cols)
        type_col = self._get_col(df, 'project_type', self._queued_up_cols)
        cap_col = self._get_col(df, 'capacity', self._queued_up_cols)
        status_col = self._get_col(df, 'status', self._queued_up_cols)

        mask = pd.Series([True] * len(df))

        if region and region_col:
            mask &= df[region_col].astype(str).str.upper().str.contains(region.upper(), na=False)

        if project_type and type_col:
            normalized = self._normalize_project_type(project_type)
            mask &= df[type_col].apply(lambda x: self._normalize_project_type(x) == normalized)

        filtered = df[mask]

        stats = {
            'total_projects': len(filtered),
            'filters_applied': {
                'region': region,
                'project_type': project_type,
            },
        }

        # Capacity statistics
        if cap_col:
            numeric_cap = pd.to_numeric(filtered[cap_col], errors='coerce')
            stats['capacity'] = {
                'total_mw': round(numeric_cap.sum(), 0),
                'mean_mw': round(numeric_cap.mean(), 1),
                'median_mw': round(numeric_cap.median(), 1),
                'min_mw': round(numeric_cap.min(), 1),
                'max_mw': round(numeric_cap.max(), 1),
            }

        # Status breakdown
        if status_col:
            status_cats = filtered[status_col].apply(self._categorize_status)
            stats['status_breakdown'] = status_cats.value_counts().to_dict()

        # Type breakdown (if not filtering by type)
        if type_col and not project_type:
            stats['type_breakdown'] = filtered[type_col].value_counts().head(10).to_dict()

        return stats

    def is_data_loaded(self) -> bool:
        """Check if any data has been loaded."""
        return self.queued_up_df is not None or self.ic_costs_df is not None


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def get_comparables(
    region: str,
    project_type: str,
    capacity_mw: float,
    capacity_tolerance: float = 0.5,
) -> pd.DataFrame:
    """
    Quick access to comparable projects.

    Args:
        region: ISO/RTO region (e.g., 'PJM', 'MISO')
        project_type: Project type (e.g., 'Solar', 'Wind')
        capacity_mw: Target capacity in MW
        capacity_tolerance: Tolerance range as fraction

    Returns:
        DataFrame with comparable projects
    """
    hd = HistoricalData()
    return hd.get_comparable_projects(region, project_type, capacity_mw, capacity_tolerance)


def get_funnel(region: str, project_type: str) -> Dict[str, Any]:
    """
    Quick access to completion funnel data.

    Args:
        region: ISO/RTO region
        project_type: Project type

    Returns:
        Dictionary with funnel stage counts
    """
    hd = HistoricalData()
    return hd.get_completion_funnel(region, project_type)


def get_cost_stats(
    region: Optional[str] = None,
    project_type: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Quick access to cost statistics.

    Args:
        region: Optional region filter
        project_type: Optional project type filter

    Returns:
        Dictionary with cost statistics
    """
    hd = HistoricalData()
    cost_df = hd.get_cost_distribution(region, project_type)

    if cost_df.empty or 'error' in cost_df.columns:
        return {'error': 'No cost data available'}

    # Return statistics if available
    if '_mean' in cost_df.columns:
        return {
            'p25': cost_df['_p25'].iloc[0],
            'median': cost_df['_p50'].iloc[0],
            'p75': cost_df['_p75'].iloc[0],
            'mean': cost_df['_mean'].iloc[0],
            'sample_size': len(cost_df),
        }

    return {'sample_size': len(cost_df)}


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for historical data queries."""
    import argparse
    import json

    parser = argparse.ArgumentParser(
        description="Query LBL Historical Interconnection Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 historical_data.py --stats                    # Overall statistics
    python3 historical_data.py --region PJM --type Solar  # Filter statistics
    python3 historical_data.py --comparables PJM Solar 200  # Get comparables
    python3 historical_data.py --funnel PJM Solar         # Completion funnel

Data Setup:
    Download LBL Queued Up data from: https://emp.lbl.gov/queues
    Save as: tools/.cache/lbl_queued_up.xlsx
        """
    )

    # Data options
    parser.add_argument('--region', help='Filter by region (e.g., PJM, MISO)')
    parser.add_argument('--type', help='Filter by project type')

    # Actions
    parser.add_argument('--stats', action='store_true', help='Show summary statistics')
    parser.add_argument('--comparables', nargs=3, metavar=('REGION', 'TYPE', 'MW'),
                        help='Get comparable projects')
    parser.add_argument('--funnel', nargs=2, metavar=('REGION', 'TYPE'),
                        help='Get completion funnel')
    parser.add_argument('--costs', action='store_true', help='Show cost distribution')

    # Output
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--limit', type=int, default=20, help='Limit results')

    args = parser.parse_args()

    # Initialize
    hd = HistoricalData()

    if not hd.is_data_loaded():
        print("\nNo historical data loaded.")
        print("Please download LBL Queued Up data and place in .cache/ directory.")
        print("Download from: https://emp.lbl.gov/queues")
        return 1

    # Execute action
    if args.comparables:
        region, proj_type, capacity = args.comparables
        result = hd.get_comparable_projects(region, proj_type, float(capacity), limit=args.limit)

        if args.json:
            print(result.to_json(orient='records', indent=2))
        else:
            print(f"\nComparable projects to {capacity} MW {proj_type} in {region}:")
            print(result.to_string())

    elif args.funnel:
        region, proj_type = args.funnel
        result = hd.get_completion_funnel(region, proj_type)

        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print(f"\nCompletion Funnel: {proj_type} in {region}")
            for key, val in result.items():
                print(f"  {key}: {val}")

    elif args.costs:
        result = hd.get_cost_distribution(args.region, args.type)

        if args.json:
            print(result.to_json(orient='records', indent=2))
        else:
            print(f"\nCost Distribution:")
            print(result.head(args.limit).to_string())

    else:
        # Default: show statistics
        result = hd.get_summary_statistics(args.region, args.type)

        if args.json:
            print(json.dumps(result, indent=2, default=str))
        else:
            print("\n" + "=" * 60)
            print("HISTORICAL DATA SUMMARY")
            print("=" * 60)

            print(f"\nTotal Projects: {result.get('total_projects', 0):,}")

            if 'capacity' in result:
                cap = result['capacity']
                print(f"\nCapacity:")
                print(f"  Total: {cap.get('total_mw', 0):,.0f} MW")
                print(f"  Mean: {cap.get('mean_mw', 0):,.1f} MW")
                print(f"  Median: {cap.get('median_mw', 0):,.1f} MW")

            if 'status_breakdown' in result:
                print(f"\nBy Status:")
                for status, count in result['status_breakdown'].items():
                    print(f"  {status}: {count:,}")

            if 'type_breakdown' in result:
                print(f"\nBy Type (top 10):")
                for typ, count in list(result['type_breakdown'].items())[:10]:
                    print(f"  {typ}: {count:,}")

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
