#!/usr/bin/env python3
"""
PE Analytics Module

Provides PE firm-focused analytics for interconnection queue analysis:
- Completion probability by phase
- Expected MW (risk-adjusted pipeline)
- Developer concentration metrics
- Regional saturation analysis
- Technology trends over time

Based on LBL 2024 "Queued Up" research and industry benchmarks.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict

# Try to import unified_data benchmarks
try:
    from unified_data import RegionalBenchmarks
    _benchmarks = RegionalBenchmarks()
    REGIONAL_COMPLETION_RATES = _benchmarks.COMPLETION_RATES
    COMPLETION_RATES_BY_TYPE = _benchmarks.TYPE_COMPLETION_RATES
    MEDIAN_TIMELINE_MONTHS = _benchmarks.MEDIAN_TIMELINE_MONTHS
    MEDIAN_COSTS_PER_KW = _benchmarks.MEDIAN_COSTS_PER_KW
    USE_UNIFIED_BENCHMARKS = True
except ImportError:
    USE_UNIFIED_BENCHMARKS = False
    # Fallback to hardcoded values if unified_data not available
    REGIONAL_COMPLETION_RATES = {
        'ERCOT': 0.339,
        'ISO-NE': 0.248,
        'PJM': 0.197,
        'MISO': 0.178,
        'West': 0.165,
        'SPP': 0.158,
        'Southeast': 0.152,
        'CAISO': 0.104,
        'NYISO': 0.079,
    }
    COMPLETION_RATES_BY_TYPE = {
        'Gas': 0.32,
        'Natural Gas': 0.32,
        'Coal': 0.28,
        'Nuclear': 0.25,
        'Wind': 0.21,
        'Hydro': 0.20,
        'Battery': 0.18,
        'Storage': 0.18,
        'Solar+Battery': 0.15,
        'Solar': 0.14,
        'Offshore Wind': 0.10,
        'Other': 0.14,
    }
    MEDIAN_TIMELINE_MONTHS = {
        'ERCOT': 36,
        'SPP': 42,
        'MISO': 45,
        'PJM': 48,
        'CAISO': 52,
        'ISO-NE': 54,
        'NYISO': 56,
    }
    MEDIAN_COSTS_PER_KW = {
        'PJM': 6,
        'SPP': 25,
        'NYISO': 52,
        'MISO': 83,
        'ISO-NE': 224,
        'CAISO': 100,
        'ERCOT': 50,
    }

# Completion Rates by Study Phase (industry estimates)
COMPLETION_RATES_BY_PHASE = {
    'Feasibility Study': 0.15,
    'System Impact Study': 0.25,
    'Facilities Study': 0.45,
    'IA Executed': 0.65,
    'Under Construction': 0.85,
    'Active': 0.20,  # Generic active status
    'Pending': 0.10,
    'Unknown': 0.15,
}

# IC Cost ranges $/kW by region (with percentiles for reporting)
IC_COST_BENCHMARKS = {
    'CAISO': {'p25': 60, 'p50': 100, 'p75': 180},
    'ERCOT': {'p25': 30, 'p50': 50, 'p75': 90},
    'MISO': {'p25': 50, 'p50': 83, 'p75': 150},
    'NYISO': {'p25': 30, 'p50': 52, 'p75': 100},
    'PJM': {'p25': 3, 'p50': 6, 'p75': 15},
    'SPP': {'p25': 15, 'p50': 25, 'p75': 50},
    'ISO-NE': {'p25': 140, 'p50': 224, 'p75': 350},
}


class PEAnalytics:
    """PE-focused analytics for interconnection queue data."""

    def __init__(self, df: pd.DataFrame = None):
        """
        Initialize with queue data.

        Args:
            df: DataFrame with queue data (optional, can be set later)
        """
        self.df = df
        self._standardize_columns()

    def set_data(self, df: pd.DataFrame):
        """Set or update the queue data."""
        self.df = df
        self._standardize_columns()

    def _standardize_columns(self):
        """Standardize column names for analysis."""
        if self.df is None or self.df.empty:
            return

        # Find and standardize capacity column - try multiple sources per row
        cap_cols = ['Capacity (MW)', 'SP (MW)', 'Summer Capacity (MW)',
                   'MW-1', 'Net MWs to Grid', 'mw1', 'capacity_mw']

        # Create capacity_mw by coalescing multiple columns
        self.df['capacity_mw'] = np.nan
        for col in cap_cols:
            if col in self.df.columns:
                # Fill missing capacity values from this column
                col_vals = pd.to_numeric(self.df[col], errors='coerce')
                self.df['capacity_mw'] = self.df['capacity_mw'].fillna(col_vals)

        # Ensure capacity is numeric and handle missing
        self.df['capacity_mw'] = pd.to_numeric(self.df['capacity_mw'], errors='coerce')
        self.df['capacity_mw'] = self.df['capacity_mw'].fillna(0)

        # Find and standardize technology column - coalesce multiple sources
        tech_cols = ['Generation Type', 'Type/ Fuel', 'Fuel', 'Fuel-1', 'fuel_type', 'type_clean']
        self.df['technology'] = None
        for col in tech_cols:
            if col in self.df.columns:
                self.df['technology'] = self.df['technology'].fillna(self.df[col])

        # Find and standardize other columns
        col_mappings = {
            'status': ['Status', 'Queue Status', 'Application Status', 'q_status', 'S'],
            'state': ['State', 'state'],
            'county': ['County', 'county'],
            'iso': ['iso', 'ISO', 'region'],
            'queue_date': ['Queue Date', 'Date of IR', 'queue_date', 'q_date'],
            'developer': ['Developer', 'Developer/Interconnection Customer', 'Interconnecting Entity', 'developer'],
        }

        for std_col, source_cols in col_mappings.items():
            if std_col not in self.df.columns:
                self.df[std_col] = None
            for col in source_cols:
                if col in self.df.columns:
                    self.df[std_col] = self.df[std_col].fillna(self.df[col])

        # Standardize technology names
        if 'technology' in self.df.columns:
            self.df['tech_category'] = self.df['technology'].apply(self._categorize_technology)

    def _categorize_technology(self, tech: str) -> str:
        """Categorize technology into standard buckets."""
        if pd.isna(tech):
            return 'Other'

        tech = str(tech).lower()

        if 'solar' in tech or 'pv' in tech or 'photovoltaic' in tech:
            if 'storage' in tech or 'battery' in tech or 'bess' in tech:
                return 'Hybrid'
            return 'Solar'
        elif 'wind' in tech:
            if 'storage' in tech or 'battery' in tech:
                return 'Hybrid'
            return 'Wind'
        elif 'storage' in tech or 'battery' in tech or 'bess' in tech:
            return 'Storage'
        elif 'gas' in tech or 'natural gas' in tech or 'ng' in tech or 'ccgt' in tech or 'ct' in tech:
            return 'Gas'
        elif 'nuclear' in tech:
            return 'Nuclear'
        elif 'hydro' in tech:
            return 'Hydro'
        else:
            return 'Other'

    # =========================================================================
    # COMPLETION PROBABILITY ANALYTICS
    # =========================================================================

    def completion_probability_by_phase(self) -> Dict[str, Dict[str, Any]]:
        """
        Calculate completion probability by study phase.

        Returns:
            Dict with phase name -> {probability, project_count, total_mw, expected_mw}
        """
        if self.df is None or self.df.empty:
            return {}

        results = {}

        # Try to identify study phase from status column
        if 'status' not in self.df.columns:
            return results

        for phase, base_rate in COMPLETION_RATES_BY_PHASE.items():
            # Filter projects in this phase
            mask = self.df['status'].str.contains(phase, case=False, na=False)
            phase_df = self.df[mask]

            if len(phase_df) == 0:
                continue

            total_mw = phase_df['capacity_mw'].sum() if 'capacity_mw' in phase_df.columns else 0
            expected_mw = total_mw * base_rate

            results[phase] = {
                'probability': base_rate,
                'project_count': len(phase_df),
                'total_mw': total_mw,
                'expected_mw': expected_mw,
            }

        return results

    def completion_probability_by_technology(self) -> Dict[str, Dict[str, Any]]:
        """
        Calculate completion probability by technology type.

        Returns:
            Dict with technology -> {probability, project_count, total_mw, expected_mw}
        """
        if self.df is None or self.df.empty:
            return {}

        results = {}

        if 'tech_category' not in self.df.columns:
            return results

        for tech in self.df['tech_category'].unique():
            if pd.isna(tech):
                continue

            tech_df = self.df[self.df['tech_category'] == tech]
            base_rate = COMPLETION_RATES_BY_TYPE.get(tech, 0.20)

            total_mw = tech_df['capacity_mw'].sum() if 'capacity_mw' in tech_df.columns else 0
            expected_mw = total_mw * base_rate

            results[tech] = {
                'probability': base_rate,
                'project_count': len(tech_df),
                'total_mw': total_mw,
                'expected_mw': expected_mw,
            }

        return results

    def get_expected_mw(self) -> Dict[str, float]:
        """
        Calculate risk-adjusted expected MW.

        Applies completion probabilities to nominal MW by technology.

        Returns:
            Dict with nominal_mw, expected_mw, discount_rate
        """
        if self.df is None or self.df.empty:
            return {'nominal_mw': 0, 'expected_mw': 0, 'discount_rate': 0}

        nominal_mw = 0
        expected_mw = 0

        if 'tech_category' in self.df.columns and 'capacity_mw' in self.df.columns:
            for tech in self.df['tech_category'].unique():
                if pd.isna(tech):
                    continue

                tech_df = self.df[self.df['tech_category'] == tech]
                tech_mw = tech_df['capacity_mw'].sum()
                rate = COMPLETION_RATES_BY_TYPE.get(tech, 0.20)

                nominal_mw += tech_mw
                expected_mw += tech_mw * rate
        else:
            nominal_mw = self.df['capacity_mw'].sum() if 'capacity_mw' in self.df.columns else 0
            expected_mw = nominal_mw * 0.20  # Default 20% rate

        discount_rate = 1 - (expected_mw / nominal_mw) if nominal_mw > 0 else 0

        return {
            'nominal_mw': nominal_mw,
            'expected_mw': expected_mw,
            'discount_rate': discount_rate,
        }

    # =========================================================================
    # DEVELOPER CONCENTRATION ANALYTICS
    # =========================================================================

    def _filter_valid_developers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter out invalid/unknown developer names."""
        if 'developer' not in df.columns:
            return df

        # List of invalid developer values to exclude
        invalid_devs = ['none', 'nan', 'null', 'unknown', 'n/a', 'na', '', ' ', 'tbd', 'pending']

        mask = df['developer'].notna()
        mask &= ~df['developer'].astype(str).str.lower().str.strip().isin(invalid_devs)
        mask &= df['developer'].astype(str).str.len() > 2  # Exclude very short names

        return df[mask]

    def developer_market_share(self, top_n: int = 15) -> Dict[str, Dict[str, Any]]:
        """
        Calculate developer market share by MW.

        Returns:
            Dict with developer -> {mw, project_count, market_share}
        """
        if self.df is None or self.df.empty:
            return {}

        if 'developer' not in self.df.columns or 'capacity_mw' not in self.df.columns:
            return {}

        # Filter to only valid developers
        valid_df = self._filter_valid_developers(self.df)

        if valid_df.empty:
            return {}

        # Group by developer
        dev_stats = valid_df.groupby('developer').agg({
            'capacity_mw': 'sum',
            'developer': 'count'
        }).rename(columns={'developer': 'project_count'})

        # Sort by MW
        dev_stats = dev_stats.sort_values('capacity_mw', ascending=False)

        # Calculate market share (of projects WITH developer info)
        total_mw = dev_stats['capacity_mw'].sum()
        dev_stats['market_share'] = dev_stats['capacity_mw'] / total_mw

        # Take top N
        top_devs = dev_stats.head(top_n)

        # Add "Other" category
        other_mw = dev_stats.iloc[top_n:]['capacity_mw'].sum() if len(dev_stats) > top_n else 0
        other_count = dev_stats.iloc[top_n:]['project_count'].sum() if len(dev_stats) > top_n else 0

        results = {}
        for dev, row in top_devs.iterrows():
            results[dev] = {
                'mw': row['capacity_mw'],
                'project_count': int(row['project_count']),
                'market_share': row['market_share'],
            }

        if other_mw > 0:
            results['Other'] = {
                'mw': other_mw,
                'project_count': int(other_count),
                'market_share': other_mw / total_mw,
            }

        return results

    def developer_hhi(self) -> Dict[str, Any]:
        """
        Calculate Herfindahl-Hirschman Index for developer concentration.

        HHI = sum of squared market shares (in percentage points)
        - < 1500: Competitive market
        - 1500-2500: Moderately concentrated
        - > 2500: Highly concentrated

        Returns:
            Dict with hhi, interpretation, top_5_share, data_coverage
        """
        if self.df is None or self.df.empty:
            return {'hhi': 0, 'interpretation': 'No data', 'top_5_share': 0, 'data_coverage': 0}

        if 'developer' not in self.df.columns or 'capacity_mw' not in self.df.columns:
            return {'hhi': 0, 'interpretation': 'Missing data', 'top_5_share': 0, 'data_coverage': 0}

        # Filter to only valid developers
        valid_df = self._filter_valid_developers(self.df)
        total_pipeline_mw = self.df['capacity_mw'].sum()
        valid_mw = valid_df['capacity_mw'].sum() if not valid_df.empty else 0
        data_coverage = valid_mw / total_pipeline_mw if total_pipeline_mw > 0 else 0

        if valid_df.empty:
            return {'hhi': 0, 'interpretation': 'No developer data', 'top_5_share': 0, 'data_coverage': data_coverage}

        # Calculate market shares
        dev_mw = valid_df.groupby('developer')['capacity_mw'].sum()
        total_mw = dev_mw.sum()

        if total_mw == 0:
            return {'hhi': 0, 'interpretation': 'No capacity', 'top_5_share': 0, 'data_coverage': data_coverage}

        market_shares = (dev_mw / total_mw) * 100  # Convert to percentage points

        # Calculate HHI
        hhi = (market_shares ** 2).sum()

        # Interpretation
        if hhi < 1500:
            interpretation = 'Competitive'
        elif hhi < 2500:
            interpretation = 'Moderately Concentrated'
        else:
            interpretation = 'Highly Concentrated'

        # Top 5 share
        top_5_share = market_shares.nlargest(5).sum() / 100

        return {
            'hhi': hhi,
            'interpretation': interpretation,
            'top_5_share': top_5_share,
            'data_coverage': data_coverage,
            'developers_analyzed': len(dev_mw),
        }

    # =========================================================================
    # REGIONAL ANALYSIS
    # =========================================================================

    def regional_breakdown(self) -> Dict[str, Dict[str, Any]]:
        """
        Calculate pipeline metrics by ISO/region.

        Returns:
            Dict with region -> {mw, project_count, avg_project_size, completion_rate}
        """
        if self.df is None or self.df.empty:
            return {}

        if 'iso' not in self.df.columns:
            return {}

        results = {}

        for iso in self.df['iso'].unique():
            if pd.isna(iso):
                continue

            iso_df = self.df[self.df['iso'] == iso]
            mw = iso_df['capacity_mw'].sum() if 'capacity_mw' in iso_df.columns else 0
            count = len(iso_df)
            avg_size = mw / count if count > 0 else 0

            # Get regional completion rate from LBL benchmarks
            iso_upper = str(iso).upper()
            # Map common variations
            region_map = {
                'NYISO': 'NYISO', 'CAISO': 'CAISO', 'ERCOT': 'ERCOT',
                'PJM': 'PJM', 'MISO': 'MISO', 'SPP': 'SPP',
                'ISONE': 'ISO-NE', 'ISO-NE': 'ISO-NE',
            }
            region_key = region_map.get(iso_upper, iso_upper)
            completion_rate = REGIONAL_COMPLETION_RATES.get(region_key, 0.14)

            results[iso] = {
                'mw': mw,
                'project_count': count,
                'avg_project_size': avg_size,
                'completion_rate': completion_rate,
            }

        return results

    def state_breakdown(self) -> Dict[str, Dict[str, Any]]:
        """
        Calculate pipeline metrics by state.

        Returns:
            Dict with state -> {mw, project_count, technologies}
        """
        if self.df is None or self.df.empty:
            return {}

        if 'state' not in self.df.columns:
            return {}

        results = {}

        for state in self.df['state'].unique():
            if pd.isna(state) or str(state).strip() == '':
                continue

            state_df = self.df[self.df['state'] == state]
            mw = state_df['capacity_mw'].sum() if 'capacity_mw' in state_df.columns else 0
            count = len(state_df)

            # Technology mix
            tech_mix = {}
            if 'tech_category' in state_df.columns:
                tech_mw = state_df.groupby('tech_category')['capacity_mw'].sum()
                tech_mix = tech_mw.to_dict()

            results[state] = {
                'mw': mw,
                'project_count': count,
                'technologies': tech_mix,
            }

        return results

    # =========================================================================
    # TECHNOLOGY TRENDS
    # =========================================================================

    def technology_breakdown(self) -> Dict[str, Dict[str, Any]]:
        """
        Calculate pipeline metrics by technology.

        Returns:
            Dict with technology -> {mw, project_count, share, completion_rate, expected_mw}
        """
        if self.df is None or self.df.empty:
            return {}

        if 'tech_category' not in self.df.columns:
            return {}

        results = {}
        total_mw = self.df['capacity_mw'].sum() if 'capacity_mw' in self.df.columns else 0

        for tech in self.df['tech_category'].unique():
            if pd.isna(tech):
                continue

            tech_df = self.df[self.df['tech_category'] == tech]
            mw = tech_df['capacity_mw'].sum() if 'capacity_mw' in tech_df.columns else 0
            count = len(tech_df)
            share = mw / total_mw if total_mw > 0 else 0
            completion_rate = COMPLETION_RATES_BY_TYPE.get(tech, 0.20)
            expected_mw = mw * completion_rate

            results[tech] = {
                'mw': mw,
                'project_count': count,
                'share': share,
                'completion_rate': completion_rate,
                'expected_mw': expected_mw,
            }

        return results

    def queue_vintage_analysis(self) -> Dict[str, Dict[str, Any]]:
        """
        Analyze queue by entry year (vintage).

        Returns:
            Dict with year -> {mw, project_count, avg_age_months}
        """
        if self.df is None or self.df.empty:
            return {}

        if 'queue_date' not in self.df.columns:
            return {}

        # Convert to datetime
        df = self.df.copy()
        df['queue_date'] = pd.to_datetime(df['queue_date'], errors='coerce')
        df = df.dropna(subset=['queue_date'])

        if df.empty:
            return {}

        # Handle timezone-aware datetimes
        if df['queue_date'].dt.tz is not None:
            df['queue_date'] = df['queue_date'].dt.tz_localize(None)

        df['queue_year'] = df['queue_date'].dt.year
        now = datetime.now()

        results = {}

        for year in sorted(df['queue_year'].unique()):
            if pd.isna(year) or year < 2010 or year > now.year:
                continue

            year_df = df[df['queue_year'] == year]
            mw = year_df['capacity_mw'].sum() if 'capacity_mw' in year_df.columns else 0
            count = len(year_df)

            # Calculate average age in months
            avg_date = year_df['queue_date'].mean()
            age_months = (now - avg_date).days / 30.44 if pd.notna(avg_date) else 0

            results[int(year)] = {
                'mw': mw,
                'project_count': count,
                'avg_age_months': age_months,
            }

        return results

    # =========================================================================
    # TIMELINE ANALYTICS
    # =========================================================================

    def time_in_queue_distribution(self) -> Dict[str, Any]:
        """
        Calculate time-in-queue distribution statistics.

        Returns:
            Dict with percentiles and histogram buckets
        """
        if self.df is None or self.df.empty:
            return {}

        if 'queue_date' not in self.df.columns:
            return {}

        df = self.df.copy()
        df['queue_date'] = pd.to_datetime(df['queue_date'], errors='coerce')
        df = df.dropna(subset=['queue_date'])

        if df.empty:
            return {}

        # Handle timezone-aware datetimes by removing timezone info
        if df['queue_date'].dt.tz is not None:
            df['queue_date'] = df['queue_date'].dt.tz_localize(None)

        now = datetime.now()
        df['months_in_queue'] = (now - df['queue_date']).dt.days / 30.44

        months = df['months_in_queue']

        # Percentiles
        percentiles = {
            'p10': months.quantile(0.10),
            'p25': months.quantile(0.25),
            'p50': months.quantile(0.50),
            'p75': months.quantile(0.75),
            'p90': months.quantile(0.90),
            'mean': months.mean(),
        }

        # Histogram buckets (0-12, 12-24, 24-36, 36-48, 48-60, 60+)
        buckets = {
            '0-12 months': len(df[(months >= 0) & (months < 12)]),
            '12-24 months': len(df[(months >= 12) & (months < 24)]),
            '24-36 months': len(df[(months >= 24) & (months < 36)]),
            '36-48 months': len(df[(months >= 36) & (months < 48)]),
            '48-60 months': len(df[(months >= 48) & (months < 60)]),
            '60+ months': len(df[months >= 60]),
        }

        return {
            'percentiles': percentiles,
            'buckets': buckets,
            'total_projects': len(df),
        }

    # =========================================================================
    # REGIONAL ATTRACTIVENESS SCORING
    # =========================================================================

    def regional_attractiveness_scores(self) -> Dict[str, Dict[str, Any]]:
        """
        Calculate composite attractiveness scores for each ISO/region.

        Scores based on:
        - Historical completion rate (25% weight)
        - Median timeline (25% weight)
        - IC cost burden (25% weight)
        - Queue velocity/health (25% weight)

        Returns:
            Dict with region -> {score, rank, grade, components}
        """
        if self.df is None or self.df.empty:
            return {}

        regional = self.regional_breakdown()
        if not regional:
            return {}

        scores = {}

        for iso, data in regional.items():
            iso_upper = str(iso).upper()

            # Map to standard names
            region_map = {
                'NYISO': 'NYISO', 'CAISO': 'CAISO', 'ERCOT': 'ERCOT',
                'PJM': 'PJM', 'MISO': 'MISO', 'SPP': 'SPP',
                'ISONE': 'ISO-NE', 'ISO-NE': 'ISO-NE',
            }
            region_key = region_map.get(iso_upper, iso_upper)

            # Component 1: Completion Rate Score (higher = better)
            completion_rate = REGIONAL_COMPLETION_RATES.get(region_key, 0.14)
            # Normalize: 0.08 (worst) to 0.35 (best) -> 0 to 100
            completion_score = min(100, max(0, (completion_rate - 0.08) / 0.27 * 100))

            # Component 2: Timeline Score (lower = better)
            timeline_months = MEDIAN_TIMELINE_MONTHS.get(region_key, 48)
            # Normalize: 36 (best) to 60 (worst) -> 100 to 0
            timeline_score = min(100, max(0, (60 - timeline_months) / 24 * 100))

            # Component 3: IC Cost Score (lower = better)
            ic_cost = MEDIAN_COSTS_PER_KW.get(region_key, 50)
            # Normalize: $6/kW (best) to $224/kW (worst) -> 100 to 0
            cost_score = min(100, max(0, (224 - ic_cost) / 218 * 100))

            # Component 4: Queue Health Score (based on project count and avg size)
            project_count = data.get('project_count', 0)
            total_mw = data.get('mw', 0)
            avg_size = total_mw / project_count if project_count > 0 else 0
            # Larger average project size indicates more serious developers
            health_score = min(100, avg_size / 3)  # 300 MW avg = 100 score

            # Composite score (weighted average)
            composite = (
                completion_score * 0.30 +  # Completion rate most important
                timeline_score * 0.25 +
                cost_score * 0.25 +
                health_score * 0.20
            )

            # Grade
            if composite >= 70:
                grade = 'A'
            elif composite >= 55:
                grade = 'B'
            elif composite >= 40:
                grade = 'C'
            else:
                grade = 'D'

            scores[iso] = {
                'composite_score': composite,
                'grade': grade,
                'completion_rate': completion_rate,
                'timeline_months': timeline_months,
                'ic_cost_per_kw': ic_cost,
                'project_count': project_count,
                'total_mw': total_mw,
                'components': {
                    'completion_score': completion_score,
                    'timeline_score': timeline_score,
                    'cost_score': cost_score,
                    'health_score': health_score,
                }
            }

        # Add rankings
        sorted_regions = sorted(scores.items(), key=lambda x: x[1]['composite_score'], reverse=True)
        for rank, (iso, _) in enumerate(sorted_regions, 1):
            scores[iso]['rank'] = rank

        return scores

    # =========================================================================
    # DEVELOPER QUALITY ANALYSIS
    # =========================================================================

    def developer_quality_tiers(self, top_n: int = 20) -> Dict[str, Dict[str, Any]]:
        """
        Classify developers into quality tiers based on portfolio characteristics.

        Tier A: Large, diversified portfolios (>1.5 GW, multiple projects)
        Tier B: Medium portfolios (500 MW - 1.5 GW)
        Tier C: Small/single-project developers

        Returns:
            Dict with developer -> {tier, mw, project_count, avg_size, tech_diversity, regional_diversity}
        """
        if self.df is None or self.df.empty:
            return {}

        if 'developer' not in self.df.columns:
            return {}

        # Filter to valid developers
        valid_df = self._filter_valid_developers(self.df)

        if valid_df.empty:
            return {}

        results = {}

        # Group by developer
        for dev in valid_df['developer'].unique():
            dev_df = valid_df[valid_df['developer'] == dev]

            mw = dev_df['capacity_mw'].sum() if 'capacity_mw' in dev_df.columns else 0
            project_count = len(dev_df)
            avg_size = mw / project_count if project_count > 0 else 0

            # Technology diversity (number of unique tech categories)
            tech_diversity = 0
            if 'tech_category' in dev_df.columns:
                tech_diversity = dev_df['tech_category'].nunique()

            # Regional diversity (number of unique ISOs)
            regional_diversity = 0
            if 'iso' in dev_df.columns:
                regional_diversity = dev_df['iso'].nunique()

            # Calculate tier score - adjusted for realistic thresholds
            # Points for: MW (up to 35), project count (up to 25), avg size (up to 20),
            #            tech diversity (up to 10), regional diversity (up to 10)
            mw_score = min(35, mw / 100 * 1.75)  # 2 GW = 35 points
            count_score = min(25, project_count * 5)  # 5 projects = 25 points
            size_score = min(20, avg_size / 15)  # 300 MW avg = 20 points
            tech_score = min(10, tech_diversity * 5)  # 2 techs = 10 points
            region_score = min(10, regional_diversity * 5)  # 2 regions = 10 points

            total_score = mw_score + count_score + size_score + tech_score + region_score

            # Assign tier - adjusted thresholds
            if total_score >= 45:
                tier = 'A'
            elif total_score >= 25:
                tier = 'B'
            else:
                tier = 'C'

            results[dev] = {
                'tier': tier,
                'tier_score': total_score,
                'mw': mw,
                'project_count': int(project_count),
                'avg_project_size': avg_size,
                'tech_diversity': int(tech_diversity),
                'regional_diversity': int(regional_diversity),
            }

        # Sort by tier score and return top N
        sorted_devs = sorted(results.items(), key=lambda x: x[1]['tier_score'], reverse=True)
        return dict(sorted_devs[:top_n])

    def developer_tier_summary(self) -> Dict[str, Dict[str, Any]]:
        """
        Get summary statistics by developer tier.

        Returns:
            Dict with tier -> {count, total_mw, avg_mw, pct_of_pipeline}
        """
        tiers = self.developer_quality_tiers(top_n=1000)  # Get all

        if not tiers:
            return {}

        # Calculate total MW for developers with data
        total_dev_mw = sum(d['mw'] for d in tiers.values())

        # Calculate data coverage
        total_pipeline_mw = self.df['capacity_mw'].sum() if 'capacity_mw' in self.df.columns else 0
        data_coverage = total_dev_mw / total_pipeline_mw if total_pipeline_mw > 0 else 0

        summary = {'A': {'count': 0, 'total_mw': 0},
                   'B': {'count': 0, 'total_mw': 0},
                   'C': {'count': 0, 'total_mw': 0}}

        for dev, data in tiers.items():
            tier = data['tier']
            summary[tier]['count'] += 1
            summary[tier]['total_mw'] += data['mw']

        for tier in summary:
            count = summary[tier]['count']
            mw = summary[tier]['total_mw']
            summary[tier]['avg_mw'] = mw / count if count > 0 else 0
            # Calculate as % of developers WITH data (not total pipeline)
            summary[tier]['pct_of_pipeline'] = mw / total_dev_mw if total_dev_mw > 0 else 0

        # Add metadata
        summary['_metadata'] = {
            'data_coverage': data_coverage,
            'total_developers': len(tiers),
            'mw_with_developer_data': total_dev_mw,
        }

        return summary

    # =========================================================================
    # QUEUE HEALTH METRICS
    # =========================================================================

    def queue_health_metrics(self) -> Dict[str, Any]:
        """
        Calculate queue health indicators.

        Returns:
            Dict with health metrics by region and overall
        """
        if self.df is None or self.df.empty:
            return {}

        results = {
            'overall': {},
            'by_region': {},
        }

        total_projects = len(self.df)
        total_mw = self.df['capacity_mw'].sum() if 'capacity_mw' in self.df.columns else 0

        # Overall metrics
        results['overall'] = {
            'total_projects': total_projects,
            'total_mw': total_mw,
            'avg_project_size_mw': total_mw / total_projects if total_projects > 0 else 0,
        }

        # Add vintage analysis for queue age profile
        vintage = self.queue_vintage_analysis()
        if vintage:
            recent_years = [y for y in vintage.keys() if y >= 2022]
            recent_mw = sum(vintage[y]['mw'] for y in recent_years)
            recent_count = sum(vintage[y]['project_count'] for y in recent_years)

            results['overall']['recent_entry_mw'] = recent_mw
            results['overall']['recent_entry_count'] = recent_count
            results['overall']['recent_entry_pct'] = recent_mw / total_mw if total_mw > 0 else 0

        # By region metrics
        if 'iso' in self.df.columns:
            for iso in self.df['iso'].unique():
                if pd.isna(iso):
                    continue

                iso_df = self.df[self.df['iso'] == iso]
                iso_mw = iso_df['capacity_mw'].sum() if 'capacity_mw' in iso_df.columns else 0
                iso_count = len(iso_df)

                # Technology concentration (HHI for tech mix)
                tech_hhi = 0
                if 'tech_category' in iso_df.columns:
                    tech_shares = iso_df.groupby('tech_category')['capacity_mw'].sum() / iso_mw * 100 if iso_mw > 0 else pd.Series()
                    tech_hhi = (tech_shares ** 2).sum()

                results['by_region'][iso] = {
                    'project_count': iso_count,
                    'total_mw': iso_mw,
                    'avg_project_size': iso_mw / iso_count if iso_count > 0 else 0,
                    'pct_of_total_mw': iso_mw / total_mw if total_mw > 0 else 0,
                    'tech_concentration_hhi': tech_hhi,
                }

        return results

    # =========================================================================
    # INVESTMENT RECOMMENDATIONS
    # =========================================================================

    def investment_recommendations(self) -> Dict[str, Any]:
        """
        Generate actionable investment recommendations based on analysis.

        Returns:
            Dict with recommendations by category
        """
        results = {
            'top_regions': [],
            'avoid_regions': [],
            'target_technologies': [],
            'developer_strategy': {},
            'market_timing': {},
            'key_risks': [],
        }

        # Regional recommendations
        regional_scores = self.regional_attractiveness_scores()
        if regional_scores:
            sorted_regions = sorted(regional_scores.items(),
                                   key=lambda x: x[1]['composite_score'], reverse=True)

            # Top 3 regions
            for iso, data in sorted_regions[:3]:
                results['top_regions'].append({
                    'region': iso,
                    'score': data['composite_score'],
                    'grade': data['grade'],
                    'rationale': f"{data['completion_rate']:.0%} completion rate, {data['timeline_months']:.0f} month timeline, ${data['ic_cost_per_kw']}/kW IC cost",
                })

            # Regions to avoid (bottom 2 or grade D)
            for iso, data in sorted_regions[-2:]:
                if data['grade'] in ['C', 'D']:
                    results['avoid_regions'].append({
                        'region': iso,
                        'score': data['composite_score'],
                        'grade': data['grade'],
                        'rationale': f"Only {data['completion_rate']:.0%} completion rate, {data['timeline_months']:.0f} month timeline",
                    })

        # Technology recommendations
        tech = self.technology_breakdown()
        if tech:
            # Sort by expected MW / total MW ratio (effective completion rate accounting for volume)
            tech_ranked = sorted(tech.items(),
                                key=lambda x: x[1].get('completion_rate', 0), reverse=True)

            for t, data in tech_ranked[:3]:
                results['target_technologies'].append({
                    'technology': t,
                    'completion_rate': data.get('completion_rate', 0),
                    'pipeline_mw': data.get('mw', 0),
                    'expected_mw': data.get('expected_mw', 0),
                })

        # Developer strategy
        tier_summary = self.developer_tier_summary()
        if tier_summary:
            results['developer_strategy'] = {
                'recommendation': 'Partner with Tier A developers or acquire from Tier B/C',
                'tier_a_pipeline_pct': tier_summary.get('A', {}).get('pct_of_pipeline', 0),
                'tier_b_pipeline_pct': tier_summary.get('B', {}).get('pct_of_pipeline', 0),
                'tier_c_pipeline_pct': tier_summary.get('C', {}).get('pct_of_pipeline', 0),
            }

        # Market timing
        expected = self.get_expected_mw()
        time_dist = self.time_in_queue_distribution()

        results['market_timing'] = {
            'queue_discount': expected.get('discount_rate', 0),
            'median_queue_months': time_dist.get('percentiles', {}).get('p50', 0),
            'signal': 'FERC Order 2023 implementation may accelerate timelines - consider entry before queue clears',
        }

        # Key risks
        results['key_risks'] = [
            {
                'risk': 'Completion Risk',
                'severity': 'High',
                'mitigation': f"Only {1 - expected.get('discount_rate', 0):.0%} of pipeline expected to reach COD. Focus on post-IA projects.",
            },
            {
                'risk': 'Timeline Risk',
                'severity': 'Medium',
                'mitigation': f"Median {time_dist.get('percentiles', {}).get('p50', 0):.0f} months in queue. Model 4-5 year development timelines.",
            },
            {
                'risk': 'IC Cost Risk',
                'severity': 'Medium',
                'mitigation': 'Wide regional variation. Require detailed cost studies before acquisition.',
            },
        ]

        return results

    # =========================================================================
    # VALUATION METRICS
    # =========================================================================

    def ic_cost_analysis(self) -> Dict[str, Dict[str, Any]]:
        """
        Get interconnection cost benchmarks by region.

        Returns:
            Dict with region -> {p25, p50, p75} $/kW costs
        """
        results = {}

        if self.df is None or self.df.empty:
            return IC_COST_BENCHMARKS

        # If we have regional breakdown, add project counts
        regional = self.regional_breakdown()

        for region, costs in IC_COST_BENCHMARKS.items():
            results[region] = costs.copy()
            if region in regional:
                results[region]['project_count'] = regional[region]['project_count']
                results[region]['total_mw'] = regional[region]['mw']
            else:
                results[region]['project_count'] = 0
                results[region]['total_mw'] = 0

        return results

    # =========================================================================
    # DATA QUALITY METRICS
    # =========================================================================

    def data_quality_summary(self) -> Dict[str, Any]:
        """
        Calculate data quality metrics by ISO.

        Returns:
            Dict with coverage percentages for key fields by ISO
        """
        if self.df is None or self.df.empty:
            return {'error': 'No data available'}

        results = {
            'overall': {},
            'by_iso': {},
        }

        total_projects = len(self.df)
        total_mw = self.df['capacity_mw'].sum() if 'capacity_mw' in self.df.columns else 0

        # Overall developer coverage
        valid_dev_df = self._filter_valid_developers(self.df)
        dev_projects = len(valid_dev_df)
        dev_mw = valid_dev_df['capacity_mw'].sum() if not valid_dev_df.empty else 0

        results['overall'] = {
            'total_projects': total_projects,
            'total_mw': total_mw,
            'developer_coverage_projects': dev_projects / total_projects if total_projects > 0 else 0,
            'developer_coverage_mw': dev_mw / total_mw if total_mw > 0 else 0,
            'projects_with_developer': dev_projects,
            'projects_without_developer': total_projects - dev_projects,
        }

        # By ISO breakdown
        if 'iso' in self.df.columns:
            for iso in self.df['iso'].unique():
                if pd.isna(iso):
                    continue

                iso_df = self.df[self.df['iso'] == iso]
                iso_count = len(iso_df)
                iso_mw = iso_df['capacity_mw'].sum() if 'capacity_mw' in iso_df.columns else 0

                # Developer coverage for this ISO
                iso_valid_dev = self._filter_valid_developers(iso_df)
                iso_dev_count = len(iso_valid_dev)
                iso_dev_mw = iso_valid_dev['capacity_mw'].sum() if not iso_valid_dev.empty else 0

                # Other field coverage
                def field_coverage(df, field):
                    if field not in df.columns:
                        return 0
                    valid = df[field].notna() & (df[field].astype(str).str.strip() != '') & (df[field].astype(str).str.lower() != 'none')
                    return valid.sum() / len(df) if len(df) > 0 else 0

                results['by_iso'][iso] = {
                    'projects': iso_count,
                    'mw': iso_mw,
                    'developer_coverage': iso_dev_count / iso_count if iso_count > 0 else 0,
                    'developer_projects': iso_dev_count,
                    'technology_coverage': field_coverage(iso_df, 'tech_category'),
                    'state_coverage': field_coverage(iso_df, 'state'),
                    'county_coverage': field_coverage(iso_df, 'county'),
                    'queue_date_coverage': field_coverage(iso_df, 'queue_date'),
                }

        return results

    # =========================================================================
    # SUMMARY METRICS
    # =========================================================================

    def get_summary_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive summary metrics for PE analysis.

        Returns:
            Dict with all key metrics
        """
        if self.df is None or self.df.empty:
            return {'error': 'No data available'}

        # Basic counts
        total_projects = len(self.df)
        total_mw = self.df['capacity_mw'].sum() if 'capacity_mw' in self.df.columns else 0

        # Expected MW
        expected = self.get_expected_mw()

        # Developer concentration
        hhi = self.developer_hhi()

        # Technology breakdown
        tech = self.technology_breakdown()

        # Regional breakdown
        regional = self.regional_breakdown()

        # Time in queue
        time_dist = self.time_in_queue_distribution()

        return {
            'total_projects': total_projects,
            'total_mw': total_mw,
            'expected_mw': expected['expected_mw'],
            'discount_rate': expected['discount_rate'],
            'developer_hhi': hhi['hhi'],
            'market_concentration': hhi['interpretation'],
            'top_5_developer_share': hhi['top_5_share'],
            'technology_breakdown': tech,
            'regional_breakdown': regional,
            'median_time_in_queue': time_dist.get('percentiles', {}).get('p50', 0),
            'iso_count': len(regional),
        }


# Convenience function for quick analysis
def analyze_portfolio(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Quick portfolio analysis for PE firms.

    Args:
        df: DataFrame with queue data

    Returns:
        Dict with comprehensive analytics
    """
    analytics = PEAnalytics(df)

    return {
        'summary': analytics.get_summary_metrics(),
        'completion_by_technology': analytics.completion_probability_by_technology(),
        'completion_by_phase': analytics.completion_probability_by_phase(),
        'developer_market_share': analytics.developer_market_share(),
        'developer_concentration': analytics.developer_hhi(),
        'regional_breakdown': analytics.regional_breakdown(),
        'technology_breakdown': analytics.technology_breakdown(),
        'queue_vintage': analytics.queue_vintage_analysis(),
        'time_in_queue': analytics.time_in_queue_distribution(),
        'ic_cost_benchmarks': analytics.ic_cost_analysis(),
        # New PE-focused analytics
        'regional_attractiveness': analytics.regional_attractiveness_scores(),
        'developer_quality_tiers': analytics.developer_quality_tiers(),
        'developer_tier_summary': analytics.developer_tier_summary(),
        'queue_health': analytics.queue_health_metrics(),
        'investment_recommendations': analytics.investment_recommendations(),
        # Data quality metrics
        'data_quality': analytics.data_quality_summary(),
    }


class DealAnalyzer:
    """
    Deal-specific analyzer for PE firms evaluating interconnection queue projects.

    Provides comprehensive analysis without requiring developer information,
    with optional developer insights when that data is provided by the client.
    """

    def __init__(self, queue_df: pd.DataFrame):
        """
        Initialize with full queue data for comparative analysis.

        Args:
            queue_df: Full queue DataFrame for market context
        """
        self.queue_df = queue_df
        self.analytics = PEAnalytics(queue_df)

    def analyze_deal(
        self,
        queue_id: str = None,
        iso: str = None,
        state: str = None,
        capacity_mw: float = None,
        technology: str = None,
        status: str = None,
        queue_date: str = None,
        developer: str = None,  # Optional - provided by PE client
    ) -> Dict[str, Any]:
        """
        Analyze a specific deal against the broader market.

        Can identify project by queue_id OR by characteristics.
        Developer is optional - analysis works without it.

        Args:
            queue_id: Project queue ID (if known)
            iso: ISO/RTO region
            state: State location
            capacity_mw: Project capacity in MW
            technology: Generation type (Solar, Wind, Storage, etc.)
            status: Current queue status
            queue_date: Date entered queue
            developer: Developer name (OPTIONAL - enhances analysis if provided)

        Returns:
            Comprehensive deal analysis dict
        """
        project = {}

        # If queue_id provided, look up project details
        if queue_id and self.queue_df is not None:
            # Try to find project in queue
            mask = self.queue_df['Queue ID'].astype(str) == str(queue_id)
            if not mask.any() and 'queue_id' in self.queue_df.columns:
                mask = self.queue_df['queue_id'].astype(str) == str(queue_id)

            if mask.any():
                row = self.queue_df[mask].iloc[0]
                project = {
                    'queue_id': queue_id,
                    'iso': row.get('iso') or iso,
                    'state': row.get('state') or row.get('State') or state,
                    'capacity_mw': row.get('capacity_mw') or row.get('Capacity (MW)') or capacity_mw,
                    'technology': row.get('technology') or row.get('Generation Type') or technology,
                    'status': row.get('status') or row.get('Status') or status,
                    'queue_date': row.get('queue_date') or row.get('Queue Date') or queue_date,
                    'project_name': row.get('Project Name') or row.get('name'),
                    'developer': developer or row.get('developer') or row.get('Developer'),
                }

        # Otherwise use provided characteristics
        if not project:
            project = {
                'queue_id': queue_id,
                'iso': iso,
                'state': state,
                'capacity_mw': capacity_mw,
                'technology': technology,
                'status': status,
                'queue_date': queue_date,
                'developer': developer,
            }

        # Build analysis
        analysis = {
            'project': project,
            'market_context': self._get_market_context(project),
            'completion_probability': self._estimate_completion_probability(project),
            'timeline_estimate': self._estimate_timeline(project),
            'cost_benchmarks': self._get_cost_benchmarks(project),
            'competitive_landscape': self._get_competitive_landscape(project),
            'risk_factors': self._identify_risk_factors(project),
            'recommendations': self._generate_recommendations(project),
        }

        # Add developer insights if developer provided
        if project.get('developer'):
            analysis['developer_insights'] = self._get_developer_insights(project['developer'])
        else:
            analysis['developer_insights'] = {
                'status': 'Developer name not provided',
                'recommendation': 'Request developer name from seller for track record analysis',
            }

        return analysis

    def _get_market_context(self, project: Dict) -> Dict[str, Any]:
        """Get market context for the project's region and technology."""
        iso = project.get('iso', '').upper()
        tech = project.get('technology', '')
        state = project.get('state', '')

        context = {
            'iso': iso,
            'state': state,
            'technology': tech,
        }

        if self.queue_df is None or self.queue_df.empty:
            return context

        # Regional stats
        if iso:
            iso_mask = self.queue_df['iso'].str.upper() == iso
            iso_df = self.queue_df[iso_mask]
            context['regional_pipeline_mw'] = iso_df['capacity_mw'].sum() if 'capacity_mw' in iso_df.columns else 0
            context['regional_project_count'] = len(iso_df)
            context['regional_completion_rate'] = REGIONAL_COMPLETION_RATES.get(iso, 0.15)

        # State stats
        if state and not pd.isna(state):
            state_mask = self.queue_df['state'].fillna('').str.upper() == str(state).upper() if 'state' in self.queue_df.columns else pd.Series([False] * len(self.queue_df))
            state_df = self.queue_df[state_mask]
            context['state_pipeline_mw'] = state_df['capacity_mw'].sum() if 'capacity_mw' in state_df.columns else 0
            context['state_project_count'] = len(state_df)

        # Technology stats
        if tech:
            tech_category = self.analytics._categorize_technology(tech)
            tech_mask = self.queue_df['tech_category'] == tech_category if 'tech_category' in self.queue_df.columns else pd.Series([False] * len(self.queue_df))
            tech_df = self.queue_df[tech_mask]
            context['technology_pipeline_mw'] = tech_df['capacity_mw'].sum() if 'capacity_mw' in tech_df.columns else 0
            context['technology_project_count'] = len(tech_df)
            context['technology_completion_rate'] = COMPLETION_RATES_BY_TYPE.get(tech_category, 0.15)

        return context

    def _estimate_completion_probability(self, project: Dict) -> Dict[str, Any]:
        """Estimate completion probability based on project characteristics."""
        iso = project.get('iso', '').upper()
        tech = project.get('technology', '')
        status = project.get('status', '')
        capacity = project.get('capacity_mw', 0)

        # Base rates
        regional_rate = REGIONAL_COMPLETION_RATES.get(iso, 0.15)
        tech_category = self.analytics._categorize_technology(tech)
        tech_rate = COMPLETION_RATES_BY_TYPE.get(tech_category, 0.15)

        # Phase adjustment
        phase_rate = 0.15  # Default
        for phase, rate in COMPLETION_RATES_BY_PHASE.items():
            if status and phase.lower() in status.lower():
                phase_rate = rate
                break

        # Composite probability (weighted average)
        composite = (regional_rate * 0.3 + tech_rate * 0.3 + phase_rate * 0.4)

        # Size adjustment (larger projects slightly more likely to complete)
        if capacity and capacity > 200:
            composite *= 1.1
        elif capacity and capacity < 50:
            composite *= 0.9

        composite = min(composite, 0.95)  # Cap at 95%

        return {
            'composite_probability': round(composite, 3),
            'regional_factor': regional_rate,
            'technology_factor': tech_rate,
            'phase_factor': phase_rate,
            'interpretation': self._interpret_probability(composite),
            'confidence': 'Medium' if status else 'Low',  # Higher confidence with known status
        }

    def _interpret_probability(self, prob: float) -> str:
        """Interpret completion probability."""
        if prob >= 0.6:
            return 'High likelihood of completion - project is advanced'
        elif prob >= 0.4:
            return 'Moderate likelihood - typical for mid-stage projects'
        elif prob >= 0.2:
            return 'Below average - expect significant attrition risk'
        else:
            return 'Low likelihood - early stage with high dropout risk'

    def _estimate_timeline(self, project: Dict) -> Dict[str, Any]:
        """Estimate timeline to commercial operation."""
        iso = project.get('iso', '').upper()
        status = project.get('status', '')
        queue_date = project.get('queue_date')

        # Base timeline by ISO
        base_months = MEDIAN_TIMELINE_MONTHS.get(iso, 48)

        # Adjust for current phase
        phase_remaining = {
            'Feasibility': 0.9,
            'System Impact': 0.7,
            'Facilities': 0.5,
            'IA Executed': 0.3,
            'Under Construction': 0.1,
        }

        multiplier = 1.0
        for phase, pct in phase_remaining.items():
            if status and phase.lower() in status.lower():
                multiplier = pct
                break

        remaining_months = base_months * multiplier

        # Calculate time already in queue
        months_in_queue = 0
        if queue_date:
            try:
                if isinstance(queue_date, str):
                    qd = pd.to_datetime(queue_date)
                else:
                    qd = queue_date
                months_in_queue = (datetime.now() - qd).days / 30
            except:
                pass

        return {
            'estimated_months_remaining': round(remaining_months, 0),
            'total_timeline_months': base_months,
            'months_in_queue': round(months_in_queue, 0),
            'estimated_cod_year': datetime.now().year + int(remaining_months / 12) + 1,
            'timeline_risk': 'High' if remaining_months > 36 else 'Medium' if remaining_months > 18 else 'Low',
        }

    def _get_cost_benchmarks(self, project: Dict) -> Dict[str, Any]:
        """Get interconnection cost benchmarks for the region."""
        iso = project.get('iso', '').upper()
        capacity = project.get('capacity_mw', 100)

        benchmarks = IC_COST_BENCHMARKS.get(iso, {'p25': 30, 'p50': 50, 'p75': 100})

        return {
            'region': iso,
            'ic_cost_per_kw': {
                'p25': benchmarks['p25'],
                'p50': benchmarks['p50'],
                'p75': benchmarks['p75'],
            },
            'estimated_ic_cost_range': {
                'low': benchmarks['p25'] * capacity * 1000,
                'mid': benchmarks['p50'] * capacity * 1000,
                'high': benchmarks['p75'] * capacity * 1000,
            },
            'note': 'Actual IC costs vary widely. Request transmission study for accurate estimate.',
        }

    def _get_competitive_landscape(self, project: Dict) -> Dict[str, Any]:
        """Analyze competitive landscape in project's area."""
        iso = project.get('iso', '').upper()
        state = project.get('state', '')
        tech = project.get('technology', '')
        capacity = project.get('capacity_mw', 0)

        landscape = {
            'region': iso,
            'state': state,
        }

        if self.queue_df is None or self.queue_df.empty:
            return landscape

        # Same-state, same-technology projects
        tech_category = self.analytics._categorize_technology(tech)

        mask = pd.Series([True] * len(self.queue_df))
        if state and not pd.isna(state) and 'state' in self.queue_df.columns:
            mask &= self.queue_df['state'].fillna('').str.upper() == str(state).upper()
        if 'tech_category' in self.queue_df.columns:
            mask &= self.queue_df['tech_category'] == tech_category

        competitors = self.queue_df[mask]

        landscape['competing_projects'] = len(competitors)
        landscape['competing_mw'] = competitors['capacity_mw'].sum() if 'capacity_mw' in competitors.columns else 0

        # Project size percentile
        if capacity and 'capacity_mw' in self.queue_df.columns:
            all_capacities = self.queue_df['capacity_mw'].dropna()
            if len(all_capacities) > 0:
                percentile = (all_capacities < capacity).mean() * 100
                landscape['size_percentile'] = round(percentile, 1)
                landscape['size_category'] = 'Large' if percentile > 75 else 'Medium' if percentile > 25 else 'Small'

        return landscape

    def _identify_risk_factors(self, project: Dict) -> List[Dict[str, str]]:
        """Identify key risk factors for the project."""
        risks = []

        iso = project.get('iso', '').upper()
        tech = project.get('technology', '')
        status = project.get('status', '')
        capacity = project.get('capacity_mw', 0)
        developer = project.get('developer')

        # Completion risk
        tech_category = self.analytics._categorize_technology(tech)
        tech_rate = COMPLETION_RATES_BY_TYPE.get(tech_category, 0.15)
        if tech_rate < 0.20:
            risks.append({
                'risk': 'High Attrition Risk',
                'severity': 'High',
                'detail': f'{tech_category} projects have only {tech_rate:.0%} completion rate',
                'mitigation': 'Focus on post-IA projects, require development milestones',
            })

        # Timeline risk
        timeline = MEDIAN_TIMELINE_MONTHS.get(iso, 48)
        if timeline > 48:
            risks.append({
                'risk': 'Extended Timeline',
                'severity': 'Medium',
                'detail': f'{iso} has {timeline} month median queue time',
                'mitigation': 'Model conservative COD dates, build in schedule contingency',
            })

        # IC cost risk
        ic_benchmarks = IC_COST_BENCHMARKS.get(iso, {})
        if ic_benchmarks.get('p75', 0) > 100:
            risks.append({
                'risk': 'High IC Cost Variability',
                'severity': 'Medium',
                'detail': f'{iso} IC costs range ${ic_benchmarks.get("p25", 0)}-${ic_benchmarks.get("p75", 0)}/kW',
                'mitigation': 'Require completed transmission study before close',
            })

        # Developer unknown
        if not developer:
            risks.append({
                'risk': 'Unknown Developer',
                'severity': 'Medium',
                'detail': 'Developer track record cannot be assessed',
                'mitigation': 'Request developer identity and development history from seller',
            })

        # Early stage
        if status and any(term in status.lower() for term in ['feasibility', 'pending', 'active']):
            risks.append({
                'risk': 'Early Stage Project',
                'severity': 'High',
                'detail': 'Project has not completed key study milestones',
                'mitigation': 'Price in high attrition risk, consider earnout structure',
            })

        return risks

    def _generate_recommendations(self, project: Dict) -> Dict[str, Any]:
        """Generate deal-specific recommendations."""
        iso = project.get('iso', '').upper()
        status = project.get('status', '')
        developer = project.get('developer')

        completion = self._estimate_completion_probability(project)

        recs = {
            'overall_assessment': '',
            'pricing_guidance': '',
            'due_diligence_focus': [],
            'structure_suggestions': [],
        }

        # Overall assessment
        prob = completion['composite_probability']
        if prob >= 0.5:
            recs['overall_assessment'] = 'Attractive - Advanced project with reasonable completion likelihood'
        elif prob >= 0.3:
            recs['overall_assessment'] = 'Moderate - Standard development risk, price accordingly'
        else:
            recs['overall_assessment'] = 'Challenging - High attrition risk, requires significant discount'

        # Pricing guidance
        if prob >= 0.5:
            recs['pricing_guidance'] = 'Market pricing appropriate given advancement'
        elif prob >= 0.3:
            recs['pricing_guidance'] = f'Apply {(1 - prob) * 100:.0f}% risk discount to market rates'
        else:
            recs['pricing_guidance'] = 'Option or earnout structure preferred over upfront payment'

        # Due diligence focus
        recs['due_diligence_focus'] = [
            'Transmission study results and IC cost estimate',
            'Permitting status (land, environmental, local)',
            'Offtake strategy and PPA prospects',
        ]

        if not developer:
            recs['due_diligence_focus'].insert(0, 'Developer identity and track record')

        # Structure suggestions
        if prob < 0.4:
            recs['structure_suggestions'] = [
                'Consider option to acquire at future milestone',
                'Milestone-based earnouts tied to IA, permits, PPA',
                'Cap upfront payment, back-end on COD',
            ]
        else:
            recs['structure_suggestions'] = [
                'Standard acquisition with holdback for IC cost true-up',
                'Representations on study results and timeline',
            ]

        return recs

    def _get_developer_insights(self, developer: str) -> Dict[str, Any]:
        """
        Get insights on developer if name is provided.

        This is where we add value when PE client provides developer info.
        """
        if not developer or self.queue_df is None:
            return {'status': 'No developer data available'}

        # Find this developer's other projects
        dev_mask = pd.Series([False] * len(self.queue_df))
        for col in ['developer', 'Developer', 'Developer/Interconnection Customer', 'Interconnecting Entity']:
            if col in self.queue_df.columns:
                col_mask = self.queue_df[col].fillna('').str.upper().str.contains(developer.upper(), regex=False)
                dev_mask |= col_mask

        dev_projects = self.queue_df[dev_mask]

        if len(dev_projects) == 0:
            return {
                'status': 'Developer not found in queue data',
                'note': 'May be a new entrant or using different entity names',
                'recommendation': 'Request development history directly from developer',
            }

        # Analyze developer's portfolio
        total_mw = dev_projects['capacity_mw'].sum() if 'capacity_mw' in dev_projects.columns else 0
        project_count = len(dev_projects)

        # Status distribution
        status_dist = {}
        if 'status' in dev_projects.columns:
            status_dist = dev_projects['status'].value_counts().to_dict()

        # Technology mix
        tech_mix = {}
        if 'tech_category' in dev_projects.columns:
            tech_mix = dev_projects['tech_category'].value_counts().to_dict()

        # Geographic spread
        regions = []
        if 'iso' in dev_projects.columns:
            regions = dev_projects['iso'].unique().tolist()

        return {
            'developer_name': developer,
            'total_pipeline_mw': total_mw,
            'project_count': project_count,
            'status_distribution': status_dist,
            'technology_mix': tech_mix,
            'regions_active': regions,
            'assessment': self._assess_developer(project_count, total_mw),
        }

    def _assess_developer(self, project_count: int, total_mw: float) -> str:
        """Assess developer based on portfolio size."""
        if project_count >= 10 and total_mw >= 1000:
            return 'Major Developer - Large established player with significant pipeline'
        elif project_count >= 5 or total_mw >= 500:
            return 'Established Developer - Meaningful track record in queue'
        elif project_count >= 2 or total_mw >= 100:
            return 'Active Developer - Building portfolio, moderate experience'
        else:
            return 'Emerging Developer - Limited queue presence, verify experience'


def analyze_deal(
    queue_df: pd.DataFrame,
    queue_id: str = None,
    iso: str = None,
    state: str = None,
    capacity_mw: float = None,
    technology: str = None,
    status: str = None,
    developer: str = None,
) -> Dict[str, Any]:
    """
    Convenience function to analyze a specific deal.

    Args:
        queue_df: Full queue DataFrame for market context
        queue_id: Project queue ID (if known)
        iso: ISO/RTO region
        state: State location
        capacity_mw: Project capacity in MW
        technology: Generation type
        status: Current queue status
        developer: Developer name (OPTIONAL)

    Returns:
        Comprehensive deal analysis
    """
    analyzer = DealAnalyzer(queue_df)
    return analyzer.analyze_deal(
        queue_id=queue_id,
        iso=iso,
        state=state,
        capacity_mw=capacity_mw,
        technology=technology,
        status=status,
        developer=developer,
    )


if __name__ == '__main__':
    # Demo with sample data
    print("PE Analytics Module")
    print("=" * 50)

    # Try to load real data
    try:
        from market_intel import MarketData
        market = MarketData()
        df = market.get_latest_data()

        if not df.empty:
            print(f"\nAnalyzing {len(df)} projects...")

            results = analyze_portfolio(df)

            print(f"\nSummary:")
            summary = results['summary']
            print(f"  Total Projects: {summary['total_projects']:,}")
            print(f"  Total MW: {summary['total_mw']:,.0f}")
            print(f"  Expected MW (risk-adjusted): {summary['expected_mw']:,.0f}")
            print(f"  Discount Rate: {summary['discount_rate']:.1%}")
            print(f"  Market Concentration: {summary['market_concentration']} (HHI: {summary['developer_hhi']:.0f})")
            print(f"  Top 5 Developer Share: {summary['top_5_developer_share']:.1%}")

            print(f"\nRegional Attractiveness Scores:")
            for iso, data in sorted(results['regional_attractiveness'].items(),
                                   key=lambda x: x[1]['composite_score'], reverse=True):
                print(f"  {iso}: {data['composite_score']:.0f} (Grade {data['grade']}) - Rank #{data['rank']}")

            print(f"\nTop Developers by Tier:")
            for dev, data in list(results['developer_quality_tiers'].items())[:5]:
                print(f"  Tier {data['tier']}: {dev[:35]} - {data['mw']/1000:,.1f} GW, {data['project_count']} projects")

            print(f"\nInvestment Recommendations:")
            recs = results['investment_recommendations']
            print(f"  Top Regions: {', '.join([r['region'] for r in recs['top_regions']])}")
            print(f"  Target Technologies: {', '.join([t['technology'] for t in recs['target_technologies']])}")
            print(f"  Market Timing: {recs['market_timing']['signal']}")

        else:
            print("No data available. Run market_intel.py first to fetch queue data.")

    except ImportError as e:
        print(f"Could not load market data: {e}")
        print("\nRun with sample data:")
        print("  from pe_analytics import PEAnalytics")
        print("  analytics = PEAnalytics(your_dataframe)")
        print("  results = analytics.get_summary_metrics()")
