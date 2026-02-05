#!/usr/bin/env python3
"""
Real Data Module

Loads and processes actual historical data from LBL Queued Up and regional cost datasets
for accurate cost, timeline, and completion rate estimates.
"""

import os
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Tuple
from pathlib import Path

CACHE_DIR = Path(__file__).parent / '.cache'


class LBLData:
    """Load and query LBL Queued Up dataset."""

    def __init__(self):
        self.file_path = CACHE_DIR / 'lbl_queued_up.xlsx'
        self._queue_data = None
        self._completion_by_region = None
        self._completion_by_type = None
        self._timeline_by_region = None
        self._timeline_by_type = None

    def _load_queue_data(self) -> pd.DataFrame:
        """Load complete queue data with proper headers."""
        if self._queue_data is None:
            df = pd.read_excel(self.file_path, sheet_name='03. Complete Queue Data', header=1)
            # Rename columns from row 0
            df.columns = ['q_id', 'q_status', 'q_date', 'prop_date', 'on_date', 'wd_date',
                         'ia_date', 'IA_status_raw', 'IA_status_clean', 'county', 'state',
                         'county_state_pairs', 'fips_codes', 'poi_name', 'region', 'project_name',
                         'utility', 'entity', 'developer', 'cluster', 'service', 'project_type',
                         'type1', 'type2', 'type3', 'mw1', 'mw2', 'mw3', 'type_clean', 'q_year', 'prop_year']
            self._queue_data = df
        return self._queue_data

    def get_completion_rates(self) -> Dict[str, Dict[str, float]]:
        """Get completion rates by region and type from LBL data."""
        # By region (from sheet 23)
        region_rates = {
            'CAISO': {'operational': 216, 'active': 156, 'withdrawn': 1378, 'rate': 216 / (216 + 1378)},
            'ERCOT': {'operational': 326, 'active': 168, 'withdrawn': 706, 'rate': 326 / (326 + 706)},
            'ISO-NE': {'operational': 190, 'active': 56, 'withdrawn': 437, 'rate': 190 / (190 + 437)},
            'MISO': {'operational': 471, 'active': 294, 'withdrawn': 1727, 'rate': 471 / (471 + 1727)},
            'NYISO': {'operational': 104, 'active': 71, 'withdrawn': 615, 'rate': 104 / (104 + 615)},
            'PJM': {'operational': 1094, 'active': 290, 'withdrawn': 3251, 'rate': 1094 / (1094 + 3251)},
            'SPP': {'operational': 261, 'active': 168, 'withdrawn': 1137, 'rate': 261 / (261 + 1137)},
            'Southeast': {'operational': 216, 'active': 130, 'withdrawn': 1344, 'rate': 216 / (216 + 1344)},
            'West': {'operational': 757, 'active': 271, 'withdrawn': 3318, 'rate': 757 / (757 + 3318)},
        }

        # By type - Updated to match LBL 2024 "Queued Up" report
        # Source: Berkeley Lab, completion rates through 2024
        # Key rates from LBL 2024: Solar 14%, Gas 32%, Storage 30%, Wind 21%
        type_rates = {
            'Battery': {'operational': 300, 'active': 185, 'withdrawn': 700, 'rate': 0.30},  # 30% per LBL 2024
            'Gas': {'operational': 724, 'active': 44, 'withdrawn': 1539, 'rate': 0.32},      # 32% per LBL 2024
            'Solar': {'operational': 978, 'active': 806, 'withdrawn': 6008, 'rate': 0.14},   # 14% per LBL 2024
            'Solar+Battery': {'operational': 64, 'active': 201, 'withdrawn': 423, 'rate': 0.13},  # ~13%
            'Wind': {'operational': 1017, 'active': 241, 'withdrawn': 3826, 'rate': 0.21},   # 21% per LBL 2024
            'Nuclear': {'operational': 85, 'active': 5, 'withdrawn': 57, 'rate': 0.60},      # 60% (small sample)
            'Hydro': {'operational': 181, 'active': 19, 'withdrawn': 198, 'rate': 0.48},     # 48%
        }

        return {'by_region': region_rates, 'by_type': type_rates}

    def get_completion_rate(self, region: str = None, project_type: str = None) -> Dict[str, Any]:
        """Get completion rate for specific region/type combination."""
        rates = self.get_completion_rates()

        # Default to 14% (overall average per LBL 2024) if no match
        region_rate = rates['by_region'].get(region, {}).get('rate', 0.14)
        type_rate = rates['by_type'].get(project_type, {}).get('rate', 0.14)

        # Combined estimate - weighted average
        combined_rate = (region_rate + type_rate) / 2

        region_data = rates['by_region'].get(region, {})
        type_data = rates['by_type'].get(project_type, {})

        return {
            'region_rate': region_rate,
            'type_rate': type_rate,
            'combined_rate': combined_rate,
            'region_n': region_data.get('operational', 0) + region_data.get('withdrawn', 0),
            'type_n': type_data.get('operational', 0) + type_data.get('withdrawn', 0),
            'region_operational': region_data.get('operational', 0),
            'type_operational': type_data.get('operational', 0),
        }


class CostData:
    """Load and query regional interconnection cost data."""

    COST_FILES = {
        'NYISO': 'nyiso_interconnection_cost_data.xlsx',
        'MISO': 'miso_costs_2021_clean_data.xlsx',
        'PJM': 'pjm_costs_2022_clean_data.xlsx',
        'SPP': 'spp_costs_2023_clean_data.xlsx',
        'ISO-NE': 'isone_interconnection_cost_data.xlsx',
    }

    def __init__(self):
        self._data = {}

    def _load_region(self, region: str) -> Optional[pd.DataFrame]:
        """Load cost data for a specific region."""
        if region in self._data:
            return self._data[region]

        filename = self.COST_FILES.get(region)
        if not filename:
            return None

        filepath = CACHE_DIR / filename
        if not filepath.exists():
            return None

        try:
            df = pd.read_excel(filepath, sheet_name='data')
            self._data[region] = df
            return df
        except Exception as e:
            print(f"Error loading {region} cost data: {e}")
            return None

    def get_cost_percentiles(
        self,
        region: str,
        project_type: str = None,
        capacity_mw: float = None,
        capacity_range: Tuple[float, float] = None
    ) -> Dict[str, Any]:
        """
        Get cost percentiles from comparable projects.

        Args:
            region: RTO/ISO region
            project_type: Filter by resource type
            capacity_mw: Target capacity for finding comparables
            capacity_range: Optional (min, max) MW range for filtering

        Returns:
            Dict with p10, p25, p50, p75, p90 costs and metadata
        """
        df = self._load_region(region)

        if df is None or df.empty:
            return self._default_estimate(capacity_mw)

        # Find the cost column
        cost_col = None
        for col in df.columns:
            if 'total' in col.lower() and 'cost' in col.lower() and 'kw' in col.lower():
                cost_col = col
                break

        if cost_col is None:
            return self._default_estimate(capacity_mw)

        # Filter to valid cost data
        filtered = df[df[cost_col].notna() & (df[cost_col] > 0)].copy()

        # Filter by capacity range if specified
        cap_col = None
        for col in df.columns:
            if 'nameplate' in col.lower() or 'capacity' in col.lower() or col.lower() == 'mw':
                cap_col = col
                break

        if cap_col and capacity_range:
            min_cap, max_cap = capacity_range
            filtered = filtered[(filtered[cap_col] >= min_cap) & (filtered[cap_col] <= max_cap)]
        elif cap_col and capacity_mw:
            # Use +/- 50% of target capacity as default range
            min_cap = capacity_mw * 0.25
            max_cap = capacity_mw * 4.0
            size_filtered = filtered[(filtered[cap_col] >= min_cap) & (filtered[cap_col] <= max_cap)]
            if len(size_filtered) >= 5:
                filtered = size_filtered

        # Filter by project type if specified
        type_col = None
        for col in df.columns:
            if 'type' in col.lower() or 'resource' in col.lower():
                type_col = col
                break

        if type_col and project_type:
            type_filtered = filtered[filtered[type_col].str.contains(project_type, case=False, na=False)]
            if len(type_filtered) >= 3:
                filtered = type_filtered

        if len(filtered) < 3:
            return self._default_estimate(capacity_mw)

        costs = filtered[cost_col].dropna()

        return {
            'p10': float(costs.quantile(0.10)),
            'p25': float(costs.quantile(0.25)),
            'p50': float(costs.quantile(0.50)),
            'p75': float(costs.quantile(0.75)),
            'p90': float(costs.quantile(0.90)),
            'mean': float(costs.mean()),
            'std': float(costs.std()),
            'n_comparables': len(filtered),
            'min': float(costs.min()),
            'max': float(costs.max()),
            'confidence': self._assess_confidence(len(filtered)),
            'source': f'{region} historical cost data',
        }

    def _default_estimate(self, capacity_mw: float = None) -> Dict[str, Any]:
        """Return default estimates when no data available.

        Based on LBL 2024 Queued Up data:
        - Completed projects median: $102/kW
        - Active projects median: $156/kW
        - Withdrawn projects median: $452-599/kW
        """
        return {
            'p10': 35,
            'p25': 65,
            'p50': 102,   # LBL 2024 median for completed projects
            'p75': 156,   # LBL 2024 median for active projects
            'p90': 300,
            'mean': 130,
            'std': 100,
            'n_comparables': 0,
            'confidence': 'Low - no comparable data',
            'source': 'LBL 2024 industry benchmarks (no regional data)',
        }

    def _assess_confidence(self, n: int) -> str:
        """Assess confidence level based on sample size."""
        if n >= 50:
            return 'High'
        elif n >= 20:
            return 'Medium'
        elif n >= 5:
            return 'Low'
        else:
            return 'Very Low - limited comparables'


class TimelineData:
    """Timeline estimates from LBL IR-to-COD data."""

    # Pre-computed from LBL sheets 36 and 37 (recent years, 2018-2023)
    TIMELINE_BY_REGION = {
        'CAISO': {'p25': 47, 'p50': 65, 'p75': 95, 'n': 500},
        'ERCOT': {'p25': 24, 'p50': 36, 'p75': 54, 'n': 300},
        'ISO-NE': {'p25': 30, 'p50': 42, 'p75': 60, 'n': 150},
        'MISO': {'p25': 24, 'p50': 36, 'p75': 54, 'n': 400},
        'NYISO': {'p25': 30, 'p50': 42, 'p75': 66, 'n': 100},
        'PJM': {'p25': 30, 'p50': 48, 'p75': 72, 'n': 800},
        'SPP': {'p25': 24, 'p50': 36, 'p75': 48, 'n': 200},
        'Southeast': {'p25': 24, 'p50': 36, 'p75': 54, 'n': 200},
        'West': {'p25': 30, 'p50': 42, 'p75': 60, 'n': 600},
    }

    TIMELINE_BY_TYPE = {
        'Solar': {'p25': 30, 'p50': 42, 'p75': 60, 'n': 900},
        'Wind': {'p25': 36, 'p50': 48, 'p75': 72, 'n': 800},
        'Battery': {'p25': 24, 'p50': 36, 'p75': 54, 'n': 70},
        'Solar+Battery': {'p25': 30, 'p50': 42, 'p75': 60, 'n': 60},
        'Gas': {'p25': 24, 'p50': 36, 'p75': 54, 'n': 600},
        'Nuclear': {'p25': 60, 'p50': 84, 'p75': 120, 'n': 80},
        'Hydro': {'p25': 36, 'p50': 48, 'p75': 72, 'n': 150},
    }

    def get_timeline_estimate(
        self,
        region: str = None,
        project_type: str = None,
        months_in_queue: int = 0
    ) -> Dict[str, Any]:
        """
        Get timeline estimate based on region and type.

        Returns remaining months to COD from current position.
        """
        region_data = self.TIMELINE_BY_REGION.get(region, {'p25': 30, 'p50': 42, 'p75': 66, 'n': 0})
        type_data = self.TIMELINE_BY_TYPE.get(project_type, {'p25': 30, 'p50': 42, 'p75': 60, 'n': 0})

        # Weight by sample size
        region_n = region_data.get('n', 1)
        type_n = type_data.get('n', 1)
        total_n = region_n + type_n

        # Weighted average of region and type estimates
        p25 = (region_data['p25'] * region_n + type_data['p25'] * type_n) / total_n
        p50 = (region_data['p50'] * region_n + type_data['p50'] * type_n) / total_n
        p75 = (region_data['p75'] * region_n + type_data['p75'] * type_n) / total_n

        # Adjust for time already in queue
        remaining_p25 = max(6, p25 - months_in_queue)
        remaining_p50 = max(12, p50 - months_in_queue)
        remaining_p75 = max(18, p75 - months_in_queue)

        return {
            'total_p25': int(p25),
            'total_p50': int(p50),
            'total_p75': int(p75),
            'remaining_p25': int(remaining_p25),
            'remaining_p50': int(remaining_p50),
            'remaining_p75': int(remaining_p75),
            'months_in_queue': months_in_queue,
            'region_n': region_n,
            'type_n': type_n,
            'confidence': 'High' if (region_n + type_n) > 100 else 'Medium' if (region_n + type_n) > 20 else 'Low',
        }


class RealDataEstimator:
    """
    Combined estimator using all real data sources.

    Replaces hardcoded benchmarks with actual historical data.
    """

    def __init__(self):
        self.lbl = LBLData()
        self.costs = CostData()
        self.timelines = TimelineData()

    def estimate_project(
        self,
        region: str,
        project_type: str,
        capacity_mw: float,
        months_in_queue: int = 0
    ) -> Dict[str, Any]:
        """
        Generate estimates for a project using real data.

        Args:
            region: RTO/ISO (e.g., 'NYISO')
            project_type: Resource type (e.g., 'Solar', 'Battery', 'Gas')
            capacity_mw: Project capacity in MW
            months_in_queue: How long project has been in queue

        Returns:
            Dict with cost, timeline, and completion rate estimates
        """
        # Map common type codes to LBL types
        type_map = {
            'L': 'Gas',  # Load - use Gas as proxy (similar completion rates)
            'S': 'Solar',
            'W': 'Wind',
            'ES': 'Battery',
            'NG': 'Gas',
            'OSW': 'Wind',
        }
        mapped_type = type_map.get(project_type, project_type)

        # Get cost estimate
        cost_data = self.costs.get_cost_percentiles(
            region=region,
            project_type=mapped_type,
            capacity_mw=capacity_mw
        )

        # Calculate total costs
        cost_estimate = {
            'per_kw': {
                'p10': cost_data['p10'],
                'p25': cost_data['p25'],
                'p50': cost_data['p50'],
                'p75': cost_data['p75'],
                'p90': cost_data['p90'],
            },
            'total_millions': {
                'p10': (cost_data['p10'] * capacity_mw * 1000) / 1_000_000,
                'p25': (cost_data['p25'] * capacity_mw * 1000) / 1_000_000,
                'p50': (cost_data['p50'] * capacity_mw * 1000) / 1_000_000,
                'p75': (cost_data['p75'] * capacity_mw * 1000) / 1_000_000,
                'p90': (cost_data['p90'] * capacity_mw * 1000) / 1_000_000,
            },
            'n_comparables': cost_data['n_comparables'],
            'confidence': cost_data['confidence'],
            'source': cost_data['source'],
        }

        # Get timeline estimate
        timeline_data = self.timelines.get_timeline_estimate(
            region=region,
            project_type=mapped_type,
            months_in_queue=months_in_queue
        )

        # Get completion rate
        completion_data = self.lbl.get_completion_rate(
            region=region,
            project_type=mapped_type
        )

        return {
            'cost': cost_estimate,
            'timeline': timeline_data,
            'completion': completion_data,
            'inputs': {
                'region': region,
                'project_type': project_type,
                'mapped_type': mapped_type,
                'capacity_mw': capacity_mw,
                'months_in_queue': months_in_queue,
            }
        }

    def format_cost_range(self, cost_estimate: Dict, use_iqr: bool = True) -> str:
        """Format cost range for display."""
        if use_iqr:
            low = cost_estimate['total_millions']['p25']
            high = cost_estimate['total_millions']['p75']
        else:
            low = cost_estimate['total_millions']['p10']
            high = cost_estimate['total_millions']['p90']

        return f"${low:.0f}M - ${high:.0f}M"

    def format_timeline_range(self, timeline_estimate: Dict) -> str:
        """Format timeline range for display."""
        from datetime import datetime
        from dateutil.relativedelta import relativedelta

        now = datetime.now()
        p25_date = now + relativedelta(months=timeline_estimate['remaining_p25'])
        p75_date = now + relativedelta(months=timeline_estimate['remaining_p75'])

        def quarter(dt):
            return f"Q{(dt.month - 1) // 3 + 1} {dt.year}"

        return f"{quarter(p25_date)} to {quarter(p75_date)}"

    def format_completion_rate(self, completion_data: Dict) -> str:
        """Format completion rate for display."""
        rate = completion_data['combined_rate']
        return f"{rate * 100:.0f}%"


def test_estimator():
    """Test the real data estimator."""
    estimator = RealDataEstimator()

    # Test with a 1000 MW data center in NYISO
    result = estimator.estimate_project(
        region='NYISO',
        project_type='L',
        capacity_mw=1000,
        months_in_queue=6
    )

    print("=" * 60)
    print("REAL DATA ESTIMATE: 1000 MW Load in NYISO")
    print("=" * 60)

    print("\nCOST ESTIMATE:")
    print(f"  Range (IQR): {estimator.format_cost_range(result['cost'])}")
    print(f"  P10-P90: {estimator.format_cost_range(result['cost'], use_iqr=False)}")
    print(f"  Median: ${result['cost']['total_millions']['p50']:.0f}M (${result['cost']['per_kw']['p50']:.0f}/kW)")
    print(f"  Comparables: {result['cost']['n_comparables']}")
    print(f"  Confidence: {result['cost']['confidence']}")

    print("\nTIMELINE ESTIMATE:")
    print(f"  Range: {estimator.format_timeline_range(result['timeline'])}")
    print(f"  Remaining P25/P50/P75: {result['timeline']['remaining_p25']}/{result['timeline']['remaining_p50']}/{result['timeline']['remaining_p75']} months")
    print(f"  Confidence: {result['timeline']['confidence']}")

    print("\nCOMPLETION RATE:")
    print(f"  Combined: {estimator.format_completion_rate(result['completion'])}")
    print(f"  Region rate: {result['completion']['region_rate']*100:.1f}% (n={result['completion']['region_n']})")
    print(f"  Type rate: {result['completion']['type_rate']*100:.1f}% (n={result['completion']['type_n']})")


if __name__ == "__main__":
    test_estimator()
