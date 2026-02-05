#!/usr/bin/env python3
"""
Unified Data Loader

Provides a single interface to load and query interconnection queue data
from multiple RTOs and data sources.

Architecture (Option C - Hybrid):
- Direct ISO feeds for ACTIVE queue data (fresh): NYISO, CAISO, ERCOT
- LBL Queued Up for HISTORICAL benchmarks: completion rates, timelines, costs

Data Sources:
- NYISO: Direct queue feed (160 active projects)
- CAISO: Direct queue feed (326 active projects)
- ERCOT: Direct GIS report (~1,800 projects)
- LBL Queued Up: 36K+ historical records across all ISOs (for benchmarking)

Usage:
    from unified_data import UnifiedQueue, RegionalBenchmarks

    # Get active queue data
    uq = UnifiedQueue()
    results = uq.search(region="NYISO", fuel_type="Solar")

    # Get regional benchmarks for scoring
    benchmarks = RegionalBenchmarks()
    completion_rate = benchmarks.get_completion_rate("NYISO", "Solar")

Storage:
    Primary: SQLite database (.data/queue.db)
    Fallback: Excel files in .cache/ folder
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import re

CACHE_DIR = Path(__file__).parent / '.cache'
DATA_DIR = Path(__file__).parent / '.data'
DB_PATH = DATA_DIR / 'queue.db'


# =============================================================================
# REGIONAL BENCHMARKS (from LBL data)
# =============================================================================

class RegionalBenchmarks:
    """
    Regional benchmark data derived from LBL Queued Up dataset.
    Used for scoring and risk assessment.
    """

    # Completion rates by region (operational / (operational + withdrawn))
    # Source: LBL Queued Up 2024 data
    COMPLETION_RATES = {
        'ERCOT': 0.339,      # 33.9% - highest
        'ISO-NE': 0.248,     # 24.8%
        'PJM': 0.197,        # 19.7%
        'MISO': 0.178,       # 17.8%
        'West': 0.165,       # 16.5%
        'SPP': 0.158,        # 15.8%
        'Southeast': 0.152,  # 15.2%
        'CAISO': 0.104,      # 10.4%
        'NYISO': 0.079,      # 7.9% - lowest
    }

    # Completion rates by project type (from LBL data)
    TYPE_COMPLETION_RATES = {
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

    # Median timeline to COD by region (months from IR to COD)
    MEDIAN_TIMELINE_MONTHS = {
        'ERCOT': 36,
        'SPP': 42,
        'MISO': 45,
        'PJM': 48,
        'CAISO': 52,
        'ISO-NE': 54,
        'NYISO': 56,
        'West': 48,
        'Southeast': 44,
    }

    # Median interconnection costs ($/kW) by region
    # Source: LBL Interconnection Cost data
    MEDIAN_COSTS_PER_KW = {
        'PJM': 6,        # Very low - mostly POI costs
        'SPP': 25,
        'NYISO': 52,
        'MISO': 83,      # Estimated from total costs
        'ISO-NE': 224,   # Highest
        'CAISO': 100,    # Estimated
        'ERCOT': 50,     # Estimated
    }

    def __init__(self):
        """Initialize benchmarks, optionally refresh from LBL data."""
        self._lbl_data = None

    def _load_lbl(self) -> pd.DataFrame:
        """Load LBL data lazily."""
        if self._lbl_data is None:
            path = CACHE_DIR / 'lbl_queued_up.xlsx'
            if path.exists():
                self._lbl_data = pd.read_excel(path, sheet_name='03. Complete Queue Data', header=1)
            else:
                self._lbl_data = pd.DataFrame()
        return self._lbl_data

    def get_completion_rate(self, region: str, project_type: str = None) -> float:
        """
        Get completion rate for a region and optionally project type.

        Args:
            region: ISO/RTO region
            project_type: Optional project type for more specific rate

        Returns:
            Completion rate as decimal (0-1)
        """
        # Normalize region name
        region_map = {
            'NYISO': 'NYISO', 'NY-ISO': 'NYISO', 'NEW YORK': 'NYISO',
            'CAISO': 'CAISO', 'CA-ISO': 'CAISO', 'CALIFORNIA': 'CAISO',
            'ERCOT': 'ERCOT', 'TEXAS': 'ERCOT',
            'PJM': 'PJM',
            'MISO': 'MISO',
            'SPP': 'SPP',
            'ISO-NE': 'ISO-NE', 'ISONE': 'ISO-NE', 'NEW ENGLAND': 'ISO-NE',
            'WEST': 'West', 'WECC': 'West',
            'SOUTHEAST': 'Southeast', 'SERC': 'Southeast',
        }
        region_key = region_map.get(region.upper(), region)

        # Get base regional rate
        regional_rate = self.COMPLETION_RATES.get(region_key, 0.14)  # Default to average

        # Adjust by project type if provided
        if project_type:
            type_rate = self._get_type_rate(project_type)
            # Blend regional and type rates (weighted toward regional)
            return regional_rate * 0.6 + type_rate * 0.4

        return regional_rate

    def _get_type_rate(self, project_type: str) -> float:
        """Get completion rate for a project type."""
        type_upper = project_type.upper()

        for key, rate in self.TYPE_COMPLETION_RATES.items():
            if key.upper() in type_upper or type_upper in key.upper():
                return rate

        return 0.14  # Default

    def get_median_timeline(self, region: str) -> int:
        """Get median timeline in months for a region."""
        region_map = {
            'NYISO': 'NYISO', 'CAISO': 'CAISO', 'ERCOT': 'ERCOT',
            'PJM': 'PJM', 'MISO': 'MISO', 'SPP': 'SPP', 'ISO-NE': 'ISO-NE',
        }
        region_key = region_map.get(region.upper(), region)
        return self.MEDIAN_TIMELINE_MONTHS.get(region_key, 48)

    def get_cost_benchmark(self, region: str) -> int:
        """Get median interconnection cost ($/kW) for a region."""
        region_key = region.upper()
        for key, cost in self.MEDIAN_COSTS_PER_KW.items():
            if key.upper() == region_key:
                return cost
        return 75  # Default

    def calculate_dynamic_rates(self) -> Dict[str, float]:
        """
        Calculate completion rates dynamically from LBL data.
        Useful for refreshing benchmarks.
        """
        df = self._load_lbl()
        if df.empty:
            return self.COMPLETION_RATES

        rates = {}
        for region in df['region'].unique():
            region_df = df[df['region'] == region]
            operational = (region_df['q_status'] == 'operational').sum()
            withdrawn = (region_df['q_status'] == 'withdrawn').sum()

            if operational + withdrawn > 0:
                rates[region] = operational / (operational + withdrawn)

        return rates

    # =========================================================================
    # ENHANCED ANALYSIS METHODS (calculated from LBL historical data)
    # =========================================================================

    def get_actual_completion_rate(self, region: str, fuel_type: str = None) -> Dict[str, Any]:
        """
        Get ACTUAL completion rate from LBL historical data for region × fuel type.

        Returns detailed stats, not just a blended estimate.

        Args:
            region: ISO/RTO region (NYISO, CAISO, ERCOT, PJM, MISO, SPP, ISO-NE)
            fuel_type: Optional fuel type (Solar, Wind, Battery, Gas, etc.)

        Returns:
            Dictionary with completion rate, sample size, and confidence level
        """
        df = self._load_lbl()
        if df.empty:
            return {
                'rate': self.get_completion_rate(region, fuel_type),
                'completed': 0,
                'withdrawn': 0,
                'sample_size': 0,
                'confidence': 'low',
                'source': 'static_estimate'
            }

        # Normalize region name
        region_map = {
            'NYISO': 'NYISO', 'NY-ISO': 'NYISO',
            'CAISO': 'CAISO', 'CA-ISO': 'CAISO',
            'ERCOT': 'ERCOT',
            'PJM': 'PJM',
            'MISO': 'MISO',
            'SPP': 'SPP',
            'ISO-NE': 'ISO-NE', 'ISONE': 'ISO-NE',
            'WEST': 'West', 'WECC': 'West',
            'SOUTHEAST': 'Southeast',
        }
        region_key = region_map.get(region.upper(), region)

        # Filter to resolved projects (operational or withdrawn)
        resolved = df[df['q_status'].isin(['operational', 'withdrawn'])].copy()

        # Filter by region
        region_df = resolved[resolved['region'] == region_key]

        # Filter by fuel type if specified
        if fuel_type and 'type_clean' in region_df.columns:
            fuel_upper = fuel_type.upper()
            # Match fuel type flexibly
            mask = region_df['type_clean'].str.upper().str.contains(fuel_upper, na=False)
            # Also try exact match
            if mask.sum() == 0:
                fuel_map = {
                    'SOLAR': 'Solar', 'PV': 'Solar',
                    'WIND': 'Wind',
                    'BATTERY': 'Battery', 'STORAGE': 'Battery', 'BESS': 'Battery', 'ES': 'Battery',
                    'GAS': 'Gas', 'NG': 'Gas', 'NATURAL GAS': 'Gas',
                    'HYDRO': 'Hydro',
                    'NUCLEAR': 'Nuclear',
                }
                mapped_type = fuel_map.get(fuel_upper)
                if mapped_type:
                    mask = region_df['type_clean'] == mapped_type
            region_df = region_df[mask]

        # Calculate stats
        total = len(region_df)
        completed = (region_df['q_status'] == 'operational').sum()
        withdrawn = (region_df['q_status'] == 'withdrawn').sum()

        if total == 0:
            return {
                'rate': self.get_completion_rate(region, fuel_type),
                'completed': 0,
                'withdrawn': 0,
                'sample_size': 0,
                'confidence': 'none',
                'source': 'fallback_estimate',
                'note': f'No historical data for {region} + {fuel_type}'
            }

        rate = completed / total if total > 0 else 0

        # Determine confidence based on sample size
        if total >= 100:
            confidence = 'high'
        elif total >= 30:
            confidence = 'medium'
        elif total >= 10:
            confidence = 'low'
        else:
            confidence = 'very_low'

        return {
            'rate': round(rate, 4),
            'rate_pct': f"{rate * 100:.1f}%",
            'completed': int(completed),
            'withdrawn': int(withdrawn),
            'sample_size': int(total),
            'confidence': confidence,
            'source': 'lbl_historical',
            'region': region_key,
            'fuel_type': fuel_type
        }

    def get_phase_completion_probability(self, phase: str, region: str = None) -> Dict[str, Any]:
        """
        Get probability of completion given current study phase.

        This answers: "If a project is currently at phase X, what % eventually complete?"

        Note: LBL data shows FINAL phase before resolution, not current phase.
        We can use this to estimate conditional probabilities.

        Args:
            phase: Study phase (Feasibility, System Impact, Facility Study, IA Executed, etc.)
            region: Optional region filter

        Returns:
            Dictionary with completion probability and sample stats
        """
        df = self._load_lbl()
        if df.empty:
            return {'probability': 0.15, 'confidence': 'none', 'source': 'default'}

        # Map common phase names to LBL terminology
        phase_map = {
            'FEASIBILITY': 'Feasibility Study',
            'FES': 'Feasibility Study',
            'FEASIBILITY STUDY': 'Feasibility Study',
            'SYSTEM IMPACT': 'System Impact Study',
            'SIS': 'System Impact Study',
            'SRIS': 'System Impact Study',
            'SYSTEM IMPACT STUDY': 'System Impact Study',
            'FACILITY': 'Facility Study',
            'FACILITIES': 'Facility Study',
            'FACILITY STUDY': 'Facility Study',
            'FS': 'Facility Study',
            'IA': 'IA Executed',
            'IA EXECUTED': 'IA Executed',
            'IA SIGNED': 'IA Executed',
            'INTERCONNECTION AGREEMENT': 'IA Executed',
            'CLUSTER': 'Cluster Study',
            'CLUSTER STUDY': 'Cluster Study',
        }
        phase_key = phase_map.get(phase.upper(), phase)

        # Filter to resolved projects
        resolved = df[df['q_status'].isin(['operational', 'withdrawn'])].copy()

        # Filter by region if specified
        if region:
            region_map = {'NYISO': 'NYISO', 'CAISO': 'CAISO', 'ERCOT': 'ERCOT',
                          'PJM': 'PJM', 'MISO': 'MISO', 'SPP': 'SPP', 'ISO-NE': 'ISO-NE'}
            region_key = region_map.get(region.upper(), region)
            resolved = resolved[resolved['region'] == region_key]

        # Filter by phase
        if 'IA_status_clean' in resolved.columns:
            phase_df = resolved[resolved['IA_status_clean'] == phase_key]
        else:
            return {'probability': 0.15, 'confidence': 'none', 'source': 'no_phase_data'}

        total = len(phase_df)
        completed = (phase_df['q_status'] == 'operational').sum()

        if total == 0:
            return {
                'probability': 0.15,
                'completed': 0,
                'total': 0,
                'confidence': 'none',
                'source': 'no_data',
                'note': f'No data for phase: {phase_key}'
            }

        prob = completed / total

        # Confidence based on sample size
        if total >= 100:
            confidence = 'high'
        elif total >= 30:
            confidence = 'medium'
        else:
            confidence = 'low'

        return {
            'probability': round(prob, 4),
            'probability_pct': f"{prob * 100:.1f}%",
            'completed': int(completed),
            'total': int(total),
            'confidence': confidence,
            'source': 'lbl_historical',
            'phase': phase_key,
            'region': region,
            'interpretation': self._interpret_phase_probability(prob, phase_key)
        }

    def _interpret_phase_probability(self, prob: float, phase: str) -> str:
        """Generate human-readable interpretation of phase probability."""
        if 'IA' in phase.upper() or 'EXECUTED' in phase.upper():
            if prob >= 0.9:
                return "Very high - IA signed projects rarely fail"
            elif prob >= 0.6:
                return "High - most IA signed projects complete"
            else:
                return "Moderate - some IA signed projects still withdraw"
        elif 'FACILITY' in phase.upper():
            if prob >= 0.3:
                return "Above average - Facility Study is a positive signal"
            elif prob >= 0.1:
                return "Moderate - many projects still drop at this stage"
            else:
                return "Below average - high dropout even at Facility Study"
        elif 'SYSTEM IMPACT' in phase.upper():
            if prob >= 0.15:
                return "Above average for this stage"
            else:
                return "Typical - most projects at SIS stage don't complete"
        else:
            if prob >= 0.1:
                return "Early stage - high uncertainty"
            else:
                return "Very early - majority of projects at this stage withdraw"

    def get_all_region_type_rates(self) -> pd.DataFrame:
        """
        Calculate completion rates for ALL region × fuel type combinations.

        Returns a DataFrame with rates for every combination that has data.
        Useful for building lookup tables.
        """
        df = self._load_lbl()
        if df.empty:
            return pd.DataFrame()

        resolved = df[df['q_status'].isin(['operational', 'withdrawn'])].copy()
        resolved['completed'] = (resolved['q_status'] == 'operational').astype(int)

        # Calculate by region × type
        stats = resolved.groupby(['region', 'type_clean']).agg(
            total=('completed', 'count'),
            completed=('completed', 'sum')
        ).reset_index()

        stats['rate'] = stats['completed'] / stats['total']
        stats['rate_pct'] = (stats['rate'] * 100).round(1).astype(str) + '%'

        # Add confidence level
        def get_confidence(n):
            if n >= 100: return 'high'
            elif n >= 30: return 'medium'
            elif n >= 10: return 'low'
            else: return 'very_low'

        stats['confidence'] = stats['total'].apply(get_confidence)

        return stats.sort_values(['region', 'rate'], ascending=[True, False])

    def get_queue_year_effect(self, region: str = None) -> pd.DataFrame:
        """
        Analyze completion rates by queue entry year.

        Shows how vintage affects completion probability.
        """
        df = self._load_lbl()
        if df.empty:
            return pd.DataFrame()

        resolved = df[df['q_status'].isin(['operational', 'withdrawn'])].copy()
        resolved['completed'] = (resolved['q_status'] == 'operational').astype(int)

        if region:
            region_map = {'NYISO': 'NYISO', 'CAISO': 'CAISO', 'ERCOT': 'ERCOT',
                          'PJM': 'PJM', 'MISO': 'MISO', 'SPP': 'SPP', 'ISO-NE': 'ISO-NE'}
            region_key = region_map.get(region.upper(), region)
            resolved = resolved[resolved['region'] == region_key]

        if 'q_year' not in resolved.columns:
            return pd.DataFrame()

        # Filter to reasonable years
        resolved = resolved[resolved['q_year'] >= 2000]

        stats = resolved.groupby('q_year').agg(
            total=('completed', 'count'),
            completed=('completed', 'sum')
        ).reset_index()

        stats['rate'] = stats['completed'] / stats['total']
        stats['rate_pct'] = (stats['rate'] * 100).round(1).astype(str) + '%'

        return stats

    def get_comprehensive_stats(self, region: str, fuel_type: str = None) -> Dict[str, Any]:
        """
        Get comprehensive statistics for a region and optional fuel type.

        This is the main method to call for detailed analysis.

        Returns:
            Dictionary with all relevant benchmarks and statistics
        """
        # Get actual completion rate
        actual_rate = self.get_actual_completion_rate(region, fuel_type)

        # Get static benchmarks for comparison
        static_rate = self.get_completion_rate(region, fuel_type)
        median_timeline = self.get_median_timeline(region)
        cost_benchmark = self.get_cost_benchmark(region)

        # Get phase probabilities for context
        phase_probs = {}
        for phase in ['Feasibility Study', 'System Impact Study', 'Facility Study', 'IA Executed']:
            prob = self.get_phase_completion_probability(phase, region)
            phase_probs[phase] = prob['probability'] if 'probability' in prob else 0

        return {
            'region': region,
            'fuel_type': fuel_type,
            'completion_rate': {
                'actual': actual_rate,
                'static_estimate': static_rate,
            },
            'benchmarks': {
                'median_timeline_months': median_timeline,
                'cost_per_kw': cost_benchmark,
            },
            'phase_probabilities': phase_probs,
            'interpretation': self._generate_interpretation(actual_rate, region, fuel_type)
        }

    def _generate_interpretation(self, rate_data: Dict, region: str, fuel_type: str) -> str:
        """Generate human-readable interpretation of the data."""
        rate = rate_data.get('rate', 0)
        sample = rate_data.get('sample_size', 0)

        if sample == 0:
            return f"No historical data available for {fuel_type or 'all types'} in {region}."

        # Compare to overall average (17.5%)
        overall_avg = 0.175

        if rate >= 0.30:
            comparison = "well above average"
        elif rate >= 0.20:
            comparison = "above average"
        elif rate >= 0.15:
            comparison = "near average"
        elif rate >= 0.10:
            comparison = "below average"
        else:
            comparison = "significantly below average"

        type_str = f"{fuel_type} projects" if fuel_type else "projects"

        interpretation = (
            f"{type_str.title()} in {region} have a {rate*100:.1f}% historical completion rate "
            f"(based on {sample:,} resolved projects). This is {comparison} compared to the "
            f"national average of {overall_avg*100:.1f}%."
        )

        if rate < 0.10:
            interpretation += f" This combination faces significant headwinds - proceed with caution."
        elif rate > 0.25:
            interpretation += f" This is a relatively favorable combination for completion."

        return interpretation

    # =========================================================================
    # POI-LEVEL ANALYSIS
    # =========================================================================

    # POI names to exclude (not useful for analysis)
    EXCLUDED_POI_PATTERNS = [
        'unknown', 'undefined', 'tbd', 'n/a', 'na', 'none', 'other_',
        'distribution feeder', 'dist feeder', '138kv', '230kv', '345kv',
        '500kv', '69kv', '34.5kv', '34.5', '12.5kv', '12kv',
    ]

    def _is_valid_poi(self, poi_name: str) -> bool:
        """Check if POI name is valid (not generic/placeholder)."""
        if pd.isna(poi_name):
            return False
        poi_lower = str(poi_name).lower().strip()
        if len(poi_lower) < 3:
            return False
        for pattern in self.EXCLUDED_POI_PATTERNS:
            if poi_lower == pattern or poi_lower.startswith(pattern):
                return False
        return True

    def search_poi(self, poi_query: str, region: str = None, limit: int = 20) -> pd.DataFrame:
        """
        Search for POIs matching a query string.

        Args:
            poi_query: Search string (partial match)
            region: Optional region filter
            limit: Max results to return

        Returns:
            DataFrame with matching POIs and their project counts
        """
        df = self._load_lbl()
        if df.empty or 'poi_name' not in df.columns:
            return pd.DataFrame()

        # Filter by region if specified
        if region:
            region_map = {'NYISO': 'NYISO', 'CAISO': 'CAISO', 'ERCOT': 'ERCOT',
                          'PJM': 'PJM', 'MISO': 'MISO', 'SPP': 'SPP', 'ISO-NE': 'ISO-NE'}
            region_key = region_map.get(region.upper(), region)
            df = df[df['region'] == region_key]

        # Search for matching POIs
        query_lower = poi_query.lower()
        mask = df['poi_name'].astype(str).str.lower().str.contains(query_lower, na=False)
        matches = df[mask]

        if len(matches) == 0:
            return pd.DataFrame()

        # Group by POI and count
        poi_stats = matches.groupby('poi_name').agg(
            project_count=('q_id', 'count'),
            region=('region', 'first'),
            operational=('q_status', lambda x: (x == 'operational').sum()),
            withdrawn=('q_status', lambda x: (x == 'withdrawn').sum()),
            active=('q_status', lambda x: (x == 'active').sum()),
        ).reset_index()

        poi_stats['resolved'] = poi_stats['operational'] + poi_stats['withdrawn']
        poi_stats['completion_rate'] = (poi_stats['operational'] / poi_stats['resolved']).fillna(0)

        return poi_stats.sort_values('project_count', ascending=False).head(limit)

    def get_poi_history(self, poi_name: str, exact_match: bool = False) -> Dict[str, Any]:
        """
        Get complete historical analysis for a specific POI.

        This answers: "What happened to every project that was ever at this POI?"

        Args:
            poi_name: POI name to analyze
            exact_match: If True, require exact match. If False, use contains match.

        Returns:
            Dictionary with comprehensive POI statistics
        """
        # Guard against empty/None POI names - these would match entire database
        if not poi_name or not str(poi_name).strip() or poi_name.lower() in ('unknown', 'nan', 'none', 'n/a'):
            return {
                'error': 'POI name is required',
                'poi_name': poi_name,
                'suggestion': 'Provide a valid POI name to search historical data'
            }

        df = self._load_lbl()
        if df.empty or 'poi_name' not in df.columns:
            return {'error': 'No POI data available'}

        # Find matching projects
        if exact_match:
            poi_df = df[df['poi_name'] == poi_name]
        else:
            poi_lower = poi_name.lower()
            mask = df['poi_name'].astype(str).str.lower().str.contains(poi_lower, na=False)
            poi_df = df[mask]

        if len(poi_df) == 0:
            return {
                'poi_name': poi_name,
                'error': 'No projects found at this POI',
                'suggestion': 'Try a broader search term'
            }

        # Calculate statistics
        total = len(poi_df)
        operational = (poi_df['q_status'] == 'operational').sum()
        withdrawn = (poi_df['q_status'] == 'withdrawn').sum()
        active = (poi_df['q_status'] == 'active').sum()
        suspended = (poi_df['q_status'] == 'suspended').sum()

        resolved = operational + withdrawn
        completion_rate = operational / resolved if resolved > 0 else 0
        withdrawal_rate = withdrawn / resolved if resolved > 0 else 0

        # Get unique POI names if multiple matched
        unique_pois = poi_df['poi_name'].unique().tolist()

        # Get region distribution
        regions = poi_df['region'].value_counts().to_dict()

        # Get fuel type distribution
        fuel_types = poi_df['type_clean'].value_counts().to_dict()

        # Get year distribution
        years = poi_df['q_year'].value_counts().sort_index().to_dict() if 'q_year' in poi_df.columns else {}

        # Calculate timeline stats for completed projects
        timeline_stats = {}
        if 'q_date' in poi_df.columns and 'on_date' in poi_df.columns:
            completed_df = poi_df[poi_df['q_status'] == 'operational'].copy()
            if len(completed_df) > 0:
                # Try to calculate time to completion
                try:
                    completed_df['q_date'] = pd.to_datetime(completed_df['q_date'], errors='coerce')
                    completed_df['on_date'] = pd.to_datetime(completed_df['on_date'], errors='coerce')
                    completed_df['days_to_complete'] = (completed_df['on_date'] - completed_df['q_date']).dt.days
                    valid_times = completed_df['days_to_complete'].dropna()
                    if len(valid_times) > 0:
                        timeline_stats = {
                            'median_days': int(valid_times.median()),
                            'median_months': round(valid_times.median() / 30, 1),
                            'min_days': int(valid_times.min()),
                            'max_days': int(valid_times.max()),
                            'sample_size': len(valid_times)
                        }
                except:
                    pass

        # Get list of projects
        project_list = []
        for _, row in poi_df.iterrows():
            project_list.append({
                'q_id': row.get('q_id'),
                'name': row.get('project_name'),
                'status': row.get('q_status'),
                'type': row.get('type_clean'),
                'mw': row.get('mw1'),
                'region': row.get('region'),
                'year': row.get('q_year'),
            })

        # Determine risk level
        if completion_rate >= 0.25:
            risk_level = 'low'
            risk_interpretation = 'Above-average completion rate at this POI'
        elif completion_rate >= 0.15:
            risk_level = 'medium'
            risk_interpretation = 'Near-average completion rate at this POI'
        elif completion_rate >= 0.05:
            risk_level = 'high'
            risk_interpretation = 'Below-average completion rate - elevated risk'
        else:
            risk_level = 'very_high'
            risk_interpretation = 'Very low completion rate - significant risk'

        return {
            'poi_name': poi_name,
            'matched_pois': unique_pois[:5],  # Limit to first 5 matches
            'summary': {
                'total_projects': int(total),
                'operational': int(operational),
                'withdrawn': int(withdrawn),
                'active': int(active),
                'suspended': int(suspended),
                'resolved': int(resolved),
            },
            'rates': {
                'completion_rate': round(completion_rate, 4),
                'completion_rate_pct': f"{completion_rate * 100:.1f}%",
                'withdrawal_rate': round(withdrawal_rate, 4),
                'withdrawal_rate_pct': f"{withdrawal_rate * 100:.1f}%",
            },
            'risk_assessment': {
                'level': risk_level,
                'interpretation': risk_interpretation,
            },
            'timeline_stats': timeline_stats,
            'distributions': {
                'by_region': regions,
                'by_fuel_type': fuel_types,
                'by_year': years,
            },
            'projects': project_list[:50],  # Limit to first 50 projects
            'confidence': 'high' if resolved >= 20 else 'medium' if resolved >= 5 else 'low'
        }

    def get_poi_comparison(self, poi_name: str, region: str = None) -> Dict[str, Any]:
        """
        Compare a POI's performance to regional and national averages.

        Args:
            poi_name: POI to analyze
            region: Region for comparison (auto-detected if not provided)

        Returns:
            Dictionary with comparison metrics
        """
        poi_history = self.get_poi_history(poi_name)

        if 'error' in poi_history:
            return poi_history

        poi_rate = poi_history['rates']['completion_rate']
        poi_sample = poi_history['summary']['resolved']

        # Get regional rate
        if not region and poi_history['distributions']['by_region']:
            region = max(poi_history['distributions']['by_region'],
                        key=poi_history['distributions']['by_region'].get)

        regional_rate = self.COMPLETION_RATES.get(region, 0.175) if region else 0.175
        national_rate = 0.175  # Overall average

        # Calculate relative performance
        vs_regional = poi_rate - regional_rate
        vs_national = poi_rate - national_rate

        return {
            'poi_name': poi_name,
            'poi_completion_rate': poi_rate,
            'poi_sample_size': poi_sample,
            'region': region,
            'regional_completion_rate': regional_rate,
            'national_completion_rate': national_rate,
            'comparison': {
                'vs_regional': {
                    'difference': round(vs_regional, 4),
                    'difference_pct': f"{vs_regional * 100:+.1f}%",
                    'assessment': 'better' if vs_regional > 0.02 else 'worse' if vs_regional < -0.02 else 'similar'
                },
                'vs_national': {
                    'difference': round(vs_national, 4),
                    'difference_pct': f"{vs_national * 100:+.1f}%",
                    'assessment': 'better' if vs_national > 0.02 else 'worse' if vs_national < -0.02 else 'similar'
                }
            },
            'interpretation': self._interpret_poi_comparison(poi_rate, regional_rate, poi_sample, region)
        }

    def _interpret_poi_comparison(self, poi_rate: float, regional_rate: float,
                                   sample_size: int, region: str) -> str:
        """Generate interpretation of POI comparison."""
        if sample_size < 5:
            return f"Limited historical data ({sample_size} resolved projects). Results may not be statistically significant."

        diff = poi_rate - regional_rate
        region_str = region or "the national average"

        if poi_rate == 0:
            return f"No projects have completed at this POI. This is a significant red flag."
        elif diff > 0.10:
            return f"This POI significantly outperforms {region_str} ({poi_rate*100:.1f}% vs {regional_rate*100:.1f}%). Favorable location."
        elif diff > 0.02:
            return f"This POI performs above {region_str} average ({poi_rate*100:.1f}% vs {regional_rate*100:.1f}%)."
        elif diff > -0.02:
            return f"This POI performs near {region_str} average ({poi_rate*100:.1f}% vs {regional_rate*100:.1f}%)."
        elif diff > -0.10:
            return f"This POI underperforms {region_str} ({poi_rate*100:.1f}% vs {regional_rate*100:.1f}%). Elevated risk."
        else:
            return f"This POI significantly underperforms {region_str} ({poi_rate*100:.1f}% vs {regional_rate*100:.1f}%). Major red flag."

    # =========================================================================
    # TIMELINE PREDICTION
    # =========================================================================

    def _safe_date_convert(self, series: pd.Series) -> pd.Series:
        """Convert dates, handling both Excel serial and datetime formats."""
        def convert_val(val):
            if pd.isna(val):
                return pd.NaT
            try:
                num_val = float(val)
                if 1 < num_val < 100000:
                    return pd.Timestamp('1899-12-30') + pd.Timedelta(days=num_val)
                return pd.NaT
            except (ValueError, TypeError):
                try:
                    return pd.to_datetime(val, errors='coerce')
                except:
                    return pd.NaT
        return series.apply(convert_val)

    def _get_timeline_data(self) -> pd.DataFrame:
        """Load and prepare timeline data from completed projects."""
        df = self._load_lbl()
        if df.empty:
            return pd.DataFrame()

        # Focus on completed projects
        completed = df[df['q_status'] == 'operational'].copy()

        # Convert dates
        if 'q_date' in completed.columns and 'on_date' in completed.columns:
            completed['q_date_dt'] = self._safe_date_convert(completed['q_date'])
            completed['on_date_dt'] = self._safe_date_convert(completed['on_date'])

            # Calculate timeline
            completed['days_to_complete'] = (completed['on_date_dt'] - completed['q_date_dt']).dt.days
            completed['months_to_complete'] = completed['days_to_complete'] / 30.44

            # Filter to valid timelines (positive, reasonable)
            valid = completed[(completed['days_to_complete'] > 0) & (completed['days_to_complete'] < 10000)]
            return valid

        return pd.DataFrame()

    def get_timeline_prediction(self, region: str, fuel_type: str = None,
                                 current_phase: str = None,
                                 queue_entry_date: str = None) -> Dict[str, Any]:
        """
        Predict realistic timeline (P50/P75/P90) based on historical data.

        Args:
            region: ISO/RTO region
            fuel_type: Project fuel type (Solar, Wind, Battery, Gas, etc.)
            current_phase: Current study phase (optional - for phase-adjusted prediction)
            queue_entry_date: Queue entry date (optional - for calculating remaining time)

        Returns:
            Dictionary with timeline predictions and comparison to proposed COD
        """
        timeline_data = self._get_timeline_data()
        if timeline_data.empty:
            return {
                'error': 'No timeline data available',
                'fallback': self.MEDIAN_TIMELINE_MONTHS.get(region, 48)
            }

        # Normalize region
        region_map = {'NYISO': 'NYISO', 'CAISO': 'CAISO', 'ERCOT': 'ERCOT',
                      'PJM': 'PJM', 'MISO': 'MISO', 'SPP': 'SPP', 'ISO-NE': 'ISO-NE',
                      'WEST': 'West', 'SOUTHEAST': 'Southeast'}
        region_key = region_map.get(region.upper(), region)

        # Filter by region
        filtered = timeline_data[timeline_data['region'] == region_key]

        # Filter by fuel type if specified
        if fuel_type and 'type_clean' in filtered.columns:
            fuel_upper = fuel_type.upper()
            fuel_map = {
                'SOLAR': 'Solar', 'PV': 'Solar',
                'WIND': 'Wind',
                'BATTERY': 'Battery', 'STORAGE': 'Battery', 'BESS': 'Battery',
                'GAS': 'Gas', 'NG': 'Gas', 'NATURAL GAS': 'Gas',
                'HYDRO': 'Hydro',
            }
            mapped_type = fuel_map.get(fuel_upper, fuel_type)
            type_mask = filtered['type_clean'].str.contains(mapped_type, case=False, na=False)
            if type_mask.sum() >= 10:  # Only filter if enough data
                filtered = filtered[type_mask]

        sample_size = len(filtered)

        if sample_size < 5:
            # Fall back to region-only data
            filtered = timeline_data[timeline_data['region'] == region_key]
            sample_size = len(filtered)
            fuel_type_used = None
        else:
            fuel_type_used = fuel_type

        if sample_size < 3:
            # Fall back to national data
            filtered = timeline_data
            region_used = 'National'
        else:
            region_used = region_key

        # Calculate percentiles
        months = filtered['months_to_complete']
        p25 = months.quantile(0.25)
        p50 = months.quantile(0.50)  # Median
        p75 = months.quantile(0.75)
        p90 = months.quantile(0.90)
        mean = months.mean()

        # Determine confidence
        if sample_size >= 100:
            confidence = 'high'
        elif sample_size >= 30:
            confidence = 'medium'
        elif sample_size >= 10:
            confidence = 'low'
        else:
            confidence = 'very_low'

        # Calculate estimated COD if queue entry date provided
        cod_estimates = None
        if queue_entry_date:
            try:
                entry_date = pd.to_datetime(queue_entry_date)
                cod_estimates = {
                    'p50_cod': (entry_date + pd.DateOffset(months=int(p50))).strftime('%Y-%m-%d'),
                    'p75_cod': (entry_date + pd.DateOffset(months=int(p75))).strftime('%Y-%m-%d'),
                    'p90_cod': (entry_date + pd.DateOffset(months=int(p90))).strftime('%Y-%m-%d'),
                }
            except:
                pass

        return {
            'region': region_key,
            'region_used': region_used,
            'fuel_type': fuel_type_used,
            'sample_size': int(sample_size),
            'confidence': confidence,
            'timeline_months': {
                'p25': round(p25, 1),
                'p50': round(p50, 1),  # Median - use this as "expected"
                'p75': round(p75, 1),
                'p90': round(p90, 1),  # Conservative estimate
                'mean': round(mean, 1),
            },
            'cod_estimates': cod_estimates,
            'interpretation': self._interpret_timeline(p50, p90, region_key, fuel_type_used)
        }

    def _interpret_timeline(self, p50: float, p90: float, region: str, fuel_type: str) -> str:
        """Generate interpretation of timeline prediction."""
        type_str = f"{fuel_type} projects" if fuel_type else "projects"

        return (
            f"Based on historical data, {type_str} in {region} typically reach COD in "
            f"{p50:.0f} months (median). For conservative planning, use {p90:.0f} months (P90). "
            f"These estimates are from queue entry to commercial operation."
        )

    def compare_to_proposed_cod(self, region: str, fuel_type: str,
                                 queue_entry_date: str, proposed_cod: str) -> Dict[str, Any]:
        """
        Compare developer's proposed COD to realistic historical timelines.

        This is the key deliverable for PE due diligence.

        Args:
            region: ISO/RTO region
            fuel_type: Project fuel type
            queue_entry_date: When project entered queue
            proposed_cod: Developer's proposed COD

        Returns:
            Dictionary with comparison and risk assessment
        """
        # Get timeline prediction
        prediction = self.get_timeline_prediction(region, fuel_type, queue_entry_date=queue_entry_date)

        if 'error' in prediction:
            return prediction

        try:
            entry_date = pd.to_datetime(queue_entry_date)
            proposed = pd.to_datetime(proposed_cod)
        except:
            return {'error': 'Invalid date format'}

        # Calculate proposed timeline
        proposed_months = (proposed - entry_date).days / 30.44

        # Get predicted timelines
        p50 = prediction['timeline_months']['p50']
        p75 = prediction['timeline_months']['p75']
        p90 = prediction['timeline_months']['p90']

        # Calculate variance from prediction
        variance_from_p50 = proposed_months - p50
        variance_from_p90 = proposed_months - p90

        # Determine risk level
        if proposed_months >= p90:
            risk = 'low'
            assessment = 'Conservative - proposed COD is at or beyond P90'
        elif proposed_months >= p75:
            risk = 'medium-low'
            assessment = 'Moderately conservative - proposed COD between P75 and P90'
        elif proposed_months >= p50:
            risk = 'medium'
            assessment = 'Median estimate - 50% of similar projects took longer'
        elif proposed_months >= p50 * 0.75:
            risk = 'medium-high'
            assessment = 'Optimistic - proposed COD faster than median'
        else:
            risk = 'high'
            assessment = 'Aggressive - proposed COD significantly faster than historical median'

        # Calculate realistic COD estimates
        realistic_p50 = entry_date + pd.DateOffset(months=int(p50))
        realistic_p75 = entry_date + pd.DateOffset(months=int(p75))
        realistic_p90 = entry_date + pd.DateOffset(months=int(p90))

        return {
            'proposed_cod': proposed_cod,
            'proposed_timeline_months': round(proposed_months, 1),
            'historical_timeline': prediction['timeline_months'],
            'realistic_cod_estimates': {
                'p50': realistic_p50.strftime('%Y-%m-%d'),
                'p75': realistic_p75.strftime('%Y-%m-%d'),
                'p90': realistic_p90.strftime('%Y-%m-%d'),
            },
            'variance': {
                'vs_p50_months': round(variance_from_p50, 1),
                'vs_p90_months': round(variance_from_p90, 1),
            },
            'risk_assessment': {
                'level': risk,
                'assessment': assessment,
            },
            'sample_size': prediction['sample_size'],
            'confidence': prediction['confidence'],
            'recommendation': self._generate_cod_recommendation(risk, proposed_months, p50, p90, proposed_cod, realistic_p50, realistic_p90)
        }

    def _generate_cod_recommendation(self, risk: str, proposed_months: float,
                                      p50: float, p90: float, proposed_cod: str,
                                      realistic_p50, realistic_p90) -> str:
        """Generate recommendation text for COD assessment."""
        if risk == 'high':
            return (
                f"RED FLAG: Developer proposes {proposed_months:.0f} month timeline, but historical median is "
                f"{p50:.0f} months. Recommend adjusting financial model to P90 ({realistic_p90.strftime('%Y-%m-%d')}) "
                f"or requiring timeline de-risk provisions in SPA."
            )
        elif risk == 'medium-high':
            return (
                f"CAUTION: Proposed timeline is optimistic. Historical data suggests median COD of "
                f"{realistic_p50.strftime('%Y-%m-%d')}. Consider P75 for base case modeling."
            )
        elif risk == 'medium':
            return (
                f"MODERATE: Proposed COD is near historical median. 50% of similar projects took longer. "
                f"Consider using P75 ({realistic_p90.strftime('%Y-%m-%d')}) for downside scenario."
            )
        else:
            return (
                f"CONSERVATIVE: Proposed COD ({proposed_cod}) allows adequate buffer vs historical timelines. "
                f"Timeline risk appears manageable."
            )

    # =========================================================================
    # DEVELOPER ANALYSIS
    # =========================================================================

    def get_developer_track_record(self, developer_name: str, region: str = None) -> Dict[str, Any]:
        """
        Get comprehensive track record for a developer.

        This answers: "What % of this developer's historical projects reached COD?"

        Args:
            developer_name: Developer/entity name (partial match supported)
            region: Optional region filter

        Returns:
            Dictionary with developer statistics and assessment
        """
        df = self._load_lbl()
        if df.empty or 'developer' not in df.columns:
            return {'error': 'No developer data available'}

        # Filter to resolved projects with developer data
        resolved = df[df['q_status'].isin(['operational', 'withdrawn'])].copy()
        resolved = resolved[resolved['developer'].notna()]

        if region:
            region_map = {'NYISO': 'NYISO', 'CAISO': 'CAISO', 'ERCOT': 'ERCOT',
                          'PJM': 'PJM', 'MISO': 'MISO', 'SPP': 'SPP', 'ISO-NE': 'ISO-NE'}
            region_key = region_map.get(region.upper(), region)
            resolved = resolved[resolved['region'] == region_key]

        # Search for developer (case-insensitive partial match)
        dev_lower = developer_name.lower()
        mask = resolved['developer'].str.lower().str.contains(dev_lower, na=False)
        dev_projects = resolved[mask]

        if len(dev_projects) == 0:
            return {
                'developer': developer_name,
                'error': 'No historical data found for this developer',
                'suggestion': 'Developer may be new or name may differ in historical records'
            }

        # Calculate statistics
        total = len(dev_projects)
        completed = (dev_projects['q_status'] == 'operational').sum()
        withdrawn = (dev_projects['q_status'] == 'withdrawn').sum()
        completion_rate = completed / total if total > 0 else 0

        # Get matched developer names
        matched_names = dev_projects['developer'].unique().tolist()

        # Get regional breakdown
        regional_breakdown = {}
        for reg in dev_projects['region'].unique():
            reg_df = dev_projects[dev_projects['region'] == reg]
            reg_completed = (reg_df['q_status'] == 'operational').sum()
            reg_total = len(reg_df)
            regional_breakdown[reg] = {
                'completed': int(reg_completed),
                'total': int(reg_total),
                'rate': round(reg_completed / reg_total, 4) if reg_total > 0 else 0
            }

        # Get fuel type breakdown
        type_breakdown = {}
        for fuel in dev_projects['type_clean'].unique():
            type_df = dev_projects[dev_projects['type_clean'] == fuel]
            type_completed = (type_df['q_status'] == 'operational').sum()
            type_total = len(type_df)
            type_breakdown[fuel] = {
                'completed': int(type_completed),
                'total': int(type_total),
                'rate': round(type_completed / type_total, 4) if type_total > 0 else 0
            }

        # Compare to overall average
        overall_avg = 0.175  # 17.5% national average
        relative_performance = completion_rate / overall_avg if overall_avg > 0 else 1

        # Determine assessment
        if completion_rate >= 0.40:
            assessment = 'excellent'
            assessment_text = 'Top-tier developer with exceptional track record'
        elif completion_rate >= 0.25:
            assessment = 'good'
            assessment_text = 'Above-average completion rate - experienced developer'
        elif completion_rate >= 0.15:
            assessment = 'average'
            assessment_text = 'Near-average completion rate'
        elif completion_rate >= 0.05:
            assessment = 'below_average'
            assessment_text = 'Below-average completion rate - elevated risk'
        elif completion_rate > 0:
            assessment = 'poor'
            assessment_text = 'Very low completion rate - significant concern'
        else:
            assessment = 'no_completions'
            assessment_text = 'No completed projects in historical data - high risk'

        # Confidence based on sample size
        if total >= 20:
            confidence = 'high'
        elif total >= 10:
            confidence = 'medium'
        elif total >= 5:
            confidence = 'low'
        else:
            confidence = 'very_low'

        return {
            'developer': developer_name,
            'matched_names': matched_names[:5],
            'region_filter': region,
            'summary': {
                'total_projects': int(total),
                'completed': int(completed),
                'withdrawn': int(withdrawn),
                'completion_rate': round(completion_rate, 4),
                'completion_rate_pct': f"{completion_rate * 100:.1f}%",
            },
            'comparison': {
                'national_average': overall_avg,
                'relative_performance': round(relative_performance, 2),
                'vs_average': 'above' if relative_performance > 1.1 else 'below' if relative_performance < 0.9 else 'near'
            },
            'assessment': {
                'level': assessment,
                'text': assessment_text,
            },
            'breakdowns': {
                'by_region': regional_breakdown,
                'by_fuel_type': type_breakdown,
            },
            'confidence': confidence,
            'interpretation': self._interpret_developer_track_record(
                developer_name, completion_rate, total, completed, regional_breakdown
            )
        }

    def _interpret_developer_track_record(self, name: str, rate: float, total: int,
                                           completed: int, regional: Dict) -> str:
        """Generate interpretation of developer track record."""
        if total < 5:
            return f"Limited historical data for {name} ({total} projects). Track record inconclusive."

        if rate == 0:
            return (
                f"MAJOR RED FLAG: {name} has {total} historical projects but ZERO completions. "
                f"This developer has never successfully brought a project to COD in our dataset."
            )
        elif rate < 0.10:
            return (
                f"SIGNIFICANT CONCERN: {name} has only {completed} completions out of {total} projects "
                f"({rate*100:.1f}% rate). This is well below the 17.5% national average."
            )
        elif rate < 0.20:
            return (
                f"{name} has {completed}/{total} completions ({rate*100:.1f}%), near the national average. "
                f"Moderate track record."
            )
        else:
            best_region = max(regional.items(), key=lambda x: x[1]['rate']) if regional else None
            best_text = f" Strongest in {best_region[0]} ({best_region[1]['rate']*100:.0f}%)." if best_region else ""
            return (
                f"POSITIVE: {name} has {completed}/{total} completions ({rate*100:.1f}%), "
                f"above the 17.5% national average.{best_text}"
            )

    def search_developers(self, query: str = None, region: str = None,
                          min_projects: int = 5, limit: int = 50) -> pd.DataFrame:
        """
        Search and rank developers by completion rate.

        Args:
            query: Optional search string for developer name
            region: Optional region filter
            min_projects: Minimum resolved projects to include
            limit: Max results to return

        Returns:
            DataFrame with developer statistics
        """
        df = self._load_lbl()
        if df.empty or 'developer' not in df.columns:
            return pd.DataFrame()

        resolved = df[df['q_status'].isin(['operational', 'withdrawn'])].copy()
        resolved = resolved[resolved['developer'].notna()]

        if region:
            region_map = {'NYISO': 'NYISO', 'CAISO': 'CAISO', 'ERCOT': 'ERCOT',
                          'PJM': 'PJM', 'MISO': 'MISO', 'SPP': 'SPP', 'ISO-NE': 'ISO-NE'}
            region_key = region_map.get(region.upper(), region)
            resolved = resolved[resolved['region'] == region_key]

        if query:
            query_lower = query.lower()
            mask = resolved['developer'].str.lower().str.contains(query_lower, na=False)
            resolved = resolved[mask]

        # Calculate stats by developer
        dev_stats = resolved.groupby('developer').agg(
            total=('q_status', 'count'),
            completed=('q_status', lambda x: (x == 'operational').sum()),
            regions=('region', lambda x: list(x.unique()))
        ).reset_index()

        dev_stats['rate'] = dev_stats['completed'] / dev_stats['total']
        dev_stats['rate_pct'] = (dev_stats['rate'] * 100).round(1).astype(str) + '%'

        # Filter by min projects
        dev_stats = dev_stats[dev_stats['total'] >= min_projects]

        # Sort by rate descending
        dev_stats = dev_stats.sort_values('rate', ascending=False).head(limit)

        return dev_stats

    def get_developer_comparison(self, developer_name: str, region: str = None) -> Dict[str, Any]:
        """
        Compare a developer's performance to regional and fuel-type benchmarks.

        Args:
            developer_name: Developer name
            region: Optional region filter

        Returns:
            Dictionary with comparison metrics
        """
        track_record = self.get_developer_track_record(developer_name, region)

        if 'error' in track_record:
            return track_record

        dev_rate = track_record['summary']['completion_rate']
        dev_total = track_record['summary']['total_projects']

        # Get regional benchmark
        regional_benchmark = None
        if track_record['breakdowns']['by_region']:
            primary_region = max(track_record['breakdowns']['by_region'].items(),
                               key=lambda x: x[1]['total'])[0]
            regional_benchmark = self.COMPLETION_RATES.get(primary_region, 0.175)
        else:
            primary_region = region
            regional_benchmark = self.COMPLETION_RATES.get(region, 0.175) if region else 0.175

        # Calculate comparisons
        vs_regional = dev_rate - regional_benchmark if regional_benchmark else None
        vs_national = dev_rate - 0.175

        return {
            'developer': developer_name,
            'developer_rate': dev_rate,
            'developer_projects': dev_total,
            'primary_region': primary_region,
            'regional_benchmark': regional_benchmark,
            'national_benchmark': 0.175,
            'comparison': {
                'vs_regional': {
                    'difference': round(vs_regional, 4) if vs_regional else None,
                    'assessment': 'better' if vs_regional and vs_regional > 0.02 else
                                'worse' if vs_regional and vs_regional < -0.02 else 'similar'
                },
                'vs_national': {
                    'difference': round(vs_national, 4),
                    'assessment': 'better' if vs_national > 0.02 else
                                'worse' if vs_national < -0.02 else 'similar'
                }
            },
            'track_record': track_record
        }


# =============================================================================
# UNIFIED QUEUE DATA
# =============================================================================


class UnifiedQueue:
    """
    Unified interface to query interconnection queues across all RTOs.
    """

    # Standard column mapping - order matters (first match wins)
    # Sources: NYISO, CAISO, ERCOT, LBL
    COLUMN_MAP = {
        # Queue ID
        'queue_id': [
            'Queue Pos.',           # NYISO
            'Queue Position',       # CAISO
            'INR',                  # ERCOT
            'q_id',                 # LBL
        ],
        # Project name
        'name': [
            'Project Name',         # NYISO, CAISO
            'project_name',         # LBL
        ],
        # Developer/Entity
        'developer': [
            'Developer/Interconnection Customer',  # NYISO
            'developer',            # LBL
            'entity',               # LBL fallback
            'Interconnecting Entity',  # ERCOT
        ],
        # Capacity
        'capacity_mw': [
            'SP (MW)',              # NYISO (summer peak)
            'capacity_mw',          # Normalized CAISO
            'Net MWs to Grid',      # CAISO
            'MW-1',                 # CAISO alternate
            'mw1',                  # LBL
            'Capacity (MW)',        # ERCOT
        ],
        # Type/Fuel
        'type': [
            'type_clean',           # LBL (cleanest)
            'Type/ Fuel',           # NYISO
            'Fuel-1',               # CAISO
            'Fuel',                 # ERCOT
        ],
        # Status
        'status': [
            'q_status',             # LBL
            'Application Status',   # CAISO
            'S',                    # NYISO (numeric code)
            'GIM Study Phase',      # ERCOT
        ],
        # Study Phase (for scoring)
        'study_phase': [
            'Availability of Studies',  # NYISO
            'IA_status_clean',          # LBL
            'Study\nProcess',           # CAISO
        ],
        # State
        'state': ['state', 'State'],
        # County
        'county': ['county', 'County'],
        # POI
        'poi': [
            'Points of Interconnection',  # NYISO
            'Station or Transmission Line',  # CAISO
            'poi_name',                   # LBL
            'POI Location',               # ERCOT
        ],
        # Queue Date
        'queue_date': [
            'Date of IR',           # NYISO
            'Queue Date',           # CAISO
            'q_date',               # LBL
        ],
        # Proposed COD
        'cod': [
            'Proposed COD',             # NYISO
            'Current\nOn-line Date',    # CAISO
            'prop_date',                # LBL
        ],
        # Region
        'region': ['region', 'Region', 'RTO'],
        # Utility
        'utility': ['Utility', 'utility'],
    }

    def __init__(self, auto_load: bool = True, use_sqlite: bool = True):
        """Initialize the unified queue.

        Args:
            auto_load: Whether to load data automatically
            use_sqlite: Use SQLite database if available (faster), else load from files
        """
        self.data = {}  # Dict of DataFrames by source
        self.combined = None  # Combined normalized DataFrame
        self.use_sqlite = use_sqlite and DB_PATH.exists()

        if auto_load:
            self.load_all()

    def load_all(self):
        """Load all available data sources."""
        if self.use_sqlite:
            self._load_from_sqlite()
        else:
            self._load_from_files()

    def _load_from_sqlite(self):
        """Load data from SQLite database (fast)."""
        import sqlite3

        print("Loading from SQLite database...")

        conn = sqlite3.connect(DB_PATH)
        self.combined = pd.read_sql_query("""
            SELECT queue_id, name, developer, capacity_mw, type, status,
                   state, county, poi, queue_date, cod, region, source as _source
            FROM projects
        """, conn)
        conn.close()

        # Clean up
        self.combined['capacity_mw'] = self.combined['capacity_mw'].fillna(0)
        self.combined['developer'] = self.combined['developer'].fillna('Unknown')
        self.combined['name'] = self.combined['name'].fillna('Unknown')

        print(f"Loaded {len(self.combined):,} projects across {self.combined['region'].nunique()} regions")

    def _load_from_files(self):
        """Load data from Excel files (fallback)."""
        print("Loading unified queue data from files...")

        # Load direct ISO feeds (fresh active data)
        self._load_nyiso()
        self._load_caiso()
        self._load_ercot()

        # Load LBL for ISOs without direct feeds (MISO, SPP, ISO-NE)
        self._load_lbl_regions(['MISO', 'SPP', 'ISO-NE'])

        # Load full LBL historical (for benchmarking - includes all ISOs)
        self._load_lbl()

        # Combine all data
        self._combine_data()

        if self.combined is not None and not self.combined.empty:
            print(f"Loaded {len(self.combined):,} total projects across {self.combined['region'].nunique()} regions")

    def _load_nyiso(self):
        """Load NYISO live queue data."""
        path = CACHE_DIR / 'nyiso_queue.xlsx'
        if path.exists():
            df = pd.read_excel(path, sheet_name='Interconnection Queue')
            # Clean footer rows
            df = df[df['Queue Pos.'].astype(str).str.match(r'^\d+$', na=False)]
            df['_source'] = 'nyiso_live'
            df['region'] = 'NYISO'
            self.data['nyiso_live'] = df
            print(f"  NYISO Live: {len(df)} active projects")

    def _load_caiso(self):
        """Load CAISO live queue data."""
        path = CACHE_DIR / 'caiso_queue_direct.xlsx'
        if path.exists():
            df = pd.read_excel(path, sheet_name='Grid GenerationQueue', header=3)
            df = df.dropna(subset=['Project Name'])
            df['_source'] = 'caiso_live'
            df['region'] = 'CAISO'
            # Normalize capacity column
            if 'Net MWs to Grid' in df.columns:
                df['capacity_mw'] = pd.to_numeric(df['Net MWs to Grid'], errors='coerce')
            elif 'MW-1' in df.columns:
                df['capacity_mw'] = pd.to_numeric(df['MW-1'], errors='coerce')
            self.data['caiso_live'] = df
            print(f"  CAISO Live: {len(df)} active projects")

    def _load_ercot(self):
        """Load ERCOT GIS report."""
        path = CACHE_DIR / 'ercot_gis_report.xlsx'
        if path.exists():
            # ERCOT has complex header - find the row with column names
            df_raw = pd.read_excel(path, sheet_name='Project Details - Large Gen', header=None, nrows=50)

            # Find header row (contains 'INR' or 'County')
            header_row = None
            for i, row in df_raw.iterrows():
                row_str = ' '.join([str(v) for v in row.values if pd.notna(v)])
                if 'INR' in row_str and 'County' in row_str:
                    header_row = i
                    break

            if header_row is not None:
                df = pd.read_excel(path, sheet_name='Project Details - Large Gen', header=header_row)
                df = df.dropna(how='all')
                # Remove rows that look like section headers
                if 'INR' in df.columns:
                    df = df[df['INR'].notna()]
                df['_source'] = 'ercot_live'
                df['region'] = 'ERCOT'
                self.data['ercot_live'] = df
                print(f"  ERCOT Live: {len(df)} large gen projects")
            else:
                print(f"  ERCOT: Could not find header row")

    def _load_lbl_regions(self, regions: list):
        """Load specific regions from LBL as active queue data."""
        path = CACHE_DIR / 'lbl_queued_up.xlsx'
        if not path.exists():
            return

        df = pd.read_excel(path, sheet_name='03. Complete Queue Data', header=1)

        for region in regions:
            # Filter to this region's active projects
            region_df = df[(df['region'] == region) & (df['q_status'] == 'active')].copy()

            if len(region_df) > 0:
                region_df['_source'] = f'{region.lower()}_lbl'
                self.data[f'{region.lower()}_lbl'] = region_df
                print(f"  {region} (via LBL): {len(region_df):,} active projects")

    def _load_lbl(self):
        """Load LBL Queued Up historical data."""
        path = CACHE_DIR / 'lbl_queued_up.xlsx'
        if path.exists():
            df = pd.read_excel(path, sheet_name='03. Complete Queue Data', header=1)
            df['_source'] = 'lbl_historical'
            self.data['lbl'] = df
            print(f"  LBL Historical: {len(df):,} projects across all ISOs")

    def _normalize_column(self, df: pd.DataFrame, target_col: str) -> pd.Series:
        """Find and return the normalized column."""
        possible_names = self.COLUMN_MAP.get(target_col, [target_col])

        for name in possible_names:
            if name in df.columns:
                return df[name]

        return pd.Series([None] * len(df), index=df.index)

    def _combine_data(self):
        """Combine all data sources into normalized DataFrame."""
        frames = []

        for source, df in self.data.items():
            normalized = pd.DataFrame({
                'queue_id': self._normalize_column(df, 'queue_id'),
                'name': self._normalize_column(df, 'name'),
                'developer': self._normalize_column(df, 'developer'),
                'capacity_mw': pd.to_numeric(self._normalize_column(df, 'capacity_mw'), errors='coerce'),
                'type': self._normalize_column(df, 'type'),
                'status': self._normalize_column(df, 'status'),
                'state': self._normalize_column(df, 'state'),
                'county': self._normalize_column(df, 'county'),
                'poi': self._normalize_column(df, 'poi'),
                'queue_date': self._normalize_column(df, 'queue_date'),
                'cod': self._normalize_column(df, 'cod'),
                'region': self._normalize_column(df, 'region'),
                '_source': df['_source'],
            })
            frames.append(normalized)

        if frames:
            # Filter out all-NA columns before concat to avoid FutureWarning
            cleaned_frames = []
            for frame in frames:
                frame = frame.dropna(axis=1, how='all')
                cleaned_frames.append(frame)
            self.combined = pd.concat(cleaned_frames, ignore_index=True)
        else:
            self.combined = pd.DataFrame()
            return

        # Clean up
        self.combined['capacity_mw'] = self.combined['capacity_mw'].fillna(0)
        self.combined['developer'] = self.combined['developer'].fillna('Unknown')
        self.combined['name'] = self.combined['name'].fillna('Unknown')

    def search(
        self,
        developer: str = None,
        name: str = None,
        region: str = None,
        state: str = None,
        fuel_type: str = None,
        min_mw: float = None,
        max_mw: float = None,
        status: str = None,
        source: str = None,
    ) -> pd.DataFrame:
        """
        Search across all queue data.

        Args:
            developer: Developer name (partial match)
            name: Project name (partial match)
            region: RTO/ISO region
            state: State abbreviation
            fuel_type: Fuel/technology type
            min_mw: Minimum capacity
            max_mw: Maximum capacity
            status: Project status
            source: Data source ('nyiso_live', 'ercot_live', 'lbl_historical')

        Returns:
            DataFrame of matching projects
        """
        df = self.combined.copy()

        if developer:
            df = df[df['developer'].str.contains(developer, case=False, na=False)]

        if name:
            df = df[df['name'].str.contains(name, case=False, na=False)]

        if region:
            df = df[df['region'].str.upper() == region.upper()]

        if state:
            df = df[df['state'].str.upper() == state.upper()]

        if fuel_type:
            df = df[df['type'].str.contains(fuel_type, case=False, na=False)]

        if min_mw is not None:
            df = df[df['capacity_mw'] >= min_mw]

        if max_mw is not None:
            df = df[df['capacity_mw'] <= max_mw]

        if status:
            df = df[df['status'].str.contains(status, case=False, na=False)]

        if source:
            df = df[df['_source'] == source]

        return df.sort_values('capacity_mw', ascending=False)

    def get_rto(self, region: str) -> pd.DataFrame:
        """Get all projects for a specific RTO."""
        return self.search(region=region)

    def developer_profile(self, developer_name: str) -> Dict[str, Any]:
        """
        Get comprehensive developer profile across all RTOs.

        Args:
            developer_name: Developer name to search for

        Returns:
            Dict with developer statistics
        """
        matches = self.search(developer=developer_name)

        if len(matches) == 0:
            return {'found': False, 'developer': developer_name}

        # Aggregate stats
        by_region = matches.groupby('region').agg({
            'capacity_mw': ['count', 'sum'],
            'name': 'first'
        }).reset_index()
        by_region.columns = ['region', 'project_count', 'total_mw', 'sample_project']

        by_type = matches.groupby('type').agg({
            'capacity_mw': ['count', 'sum']
        }).reset_index()
        by_type.columns = ['type', 'project_count', 'total_mw']

        by_status = matches['status'].value_counts().to_dict()

        # Calculate success metrics if we have status data
        operational = len(matches[matches['status'].str.contains('operational|complete', case=False, na=False)])
        withdrawn = len(matches[matches['status'].str.contains('withdraw', case=False, na=False)])
        active = len(matches[matches['status'].str.contains('active|pending|study', case=False, na=False)])

        return {
            'found': True,
            'developer': developer_name,
            'total_projects': len(matches),
            'total_capacity_mw': matches['capacity_mw'].sum(),
            'regions': by_region.to_dict('records'),
            'by_type': by_type.to_dict('records'),
            'status_breakdown': by_status,
            'operational': operational,
            'withdrawn': withdrawn,
            'active': active,
            'success_rate': operational / (operational + withdrawn) if (operational + withdrawn) > 0 else None,
            'projects': matches[['queue_id', 'name', 'region', 'capacity_mw', 'type', 'status']].head(20).to_dict('records'),
        }

    def compare_developers(self, developers: List[str]) -> pd.DataFrame:
        """Compare multiple developers side by side."""
        profiles = []
        for dev in developers:
            profile = self.developer_profile(dev)
            if profile['found']:
                profiles.append({
                    'developer': dev,
                    'total_projects': profile['total_projects'],
                    'total_mw': profile['total_capacity_mw'],
                    'regions': len(profile['regions']),
                    'operational': profile['operational'],
                    'withdrawn': profile['withdrawn'],
                    'success_rate': profile['success_rate'],
                })
        return pd.DataFrame(profiles)

    def queue_stats(self, region: str = None) -> Dict[str, Any]:
        """Get queue statistics."""
        df = self.combined if region is None else self.search(region=region)

        return {
            'total_projects': len(df),
            'total_capacity_gw': df['capacity_mw'].sum() / 1000,
            'by_region': df.groupby('region')['capacity_mw'].agg(['count', 'sum']).to_dict(),
            'by_type': df.groupby('type')['capacity_mw'].agg(['count', 'sum']).head(10).to_dict(),
            'avg_project_size_mw': df['capacity_mw'].mean(),
            'median_project_size_mw': df['capacity_mw'].median(),
        }

    def find_similar_projects(
        self,
        region: str,
        fuel_type: str,
        capacity_mw: float,
        tolerance: float = 0.5
    ) -> pd.DataFrame:
        """
        Find similar projects for comparables analysis.

        Args:
            region: Target region
            fuel_type: Project type
            capacity_mw: Target capacity
            tolerance: Size tolerance (0.5 = +/- 50%)

        Returns:
            DataFrame of similar completed projects
        """
        min_mw = capacity_mw * (1 - tolerance)
        max_mw = capacity_mw * (1 + tolerance)

        similar = self.search(
            region=region,
            fuel_type=fuel_type,
            min_mw=min_mw,
            max_mw=max_mw,
        )

        # Filter to completed projects
        completed = similar[similar['status'].str.contains('operational|complete', case=False, na=False)]

        return completed.sort_values('capacity_mw', ascending=False)


def main():
    """CLI interface for unified queue queries."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Query interconnection queues across all RTOs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 unified_data.py --developer "NextEra"
  python3 unified_data.py --region ERCOT --type Solar --min-mw 100
  python3 unified_data.py --profile "Invenergy"
  python3 unified_data.py --compare "NextEra,Invenergy,EDF"
  python3 unified_data.py --stats
  python3 unified_data.py --stats --region PJM
        """
    )

    # Search filters
    parser.add_argument('--developer', '-d', help='Search by developer name')
    parser.add_argument('--name', '-n', help='Search by project name')
    parser.add_argument('--region', '-r', help='Filter by RTO/ISO region')
    parser.add_argument('--state', '-s', help='Filter by state')
    parser.add_argument('--type', '-t', help='Filter by fuel/technology type')
    parser.add_argument('--min-mw', type=float, help='Minimum capacity (MW)')
    parser.add_argument('--max-mw', type=float, help='Maximum capacity (MW)')
    parser.add_argument('--status', help='Filter by status')

    # Special modes
    parser.add_argument('--profile', help='Get developer profile')
    parser.add_argument('--compare', help='Compare developers (comma-separated)')
    parser.add_argument('--stats', action='store_true', help='Show queue statistics')
    parser.add_argument('--regions', action='store_true', help='List available regions')

    # Output options
    parser.add_argument('--limit', type=int, default=20, help='Limit results (default: 20)')
    parser.add_argument('--output', '-o', help='Export results to CSV')

    args = parser.parse_args()

    # Load data
    print("Loading unified queue data...")
    uq = UnifiedQueue()
    print()

    # Handle special modes
    if args.regions:
        print("Available Regions:")
        print("-" * 40)
        regions = uq.combined['region'].value_counts()
        for region, count in regions.items():
            mw = uq.combined[uq.combined['region'] == region]['capacity_mw'].sum()
            print(f"  {region}: {count:,} projects ({mw/1000:,.1f} GW)")
        return 0

    if args.stats:
        print("="*60)
        print("QUEUE STATISTICS")
        print("="*60)
        stats = uq.queue_stats(args.region)
        print(f"\nTotal Projects: {stats['total_projects']:,}")
        print(f"Total Capacity: {stats['total_capacity_gw']:,.1f} GW")
        print(f"Average Size: {stats['avg_project_size_mw']:,.1f} MW")
        print(f"Median Size: {stats['median_project_size_mw']:,.1f} MW")

        if not args.region:
            print("\nBy Region:")
            for region in sorted(stats['by_region']['count'].keys()):
                count = stats['by_region']['count'][region]
                mw = stats['by_region']['sum'][region]
                print(f"  {region}: {count:,} projects ({mw/1000:,.1f} GW)")
        return 0

    if args.profile:
        print("="*60)
        print(f"DEVELOPER PROFILE: {args.profile}")
        print("="*60)
        profile = uq.developer_profile(args.profile)
        if not profile['found']:
            print(f"No projects found for developer: {args.profile}")
            return 1

        print(f"\nTotal Projects: {profile['total_projects']}")
        print(f"Total Capacity: {profile['total_capacity_mw']:,.0f} MW")
        print(f"Success Rate: {profile['success_rate']*100:.1f}%" if profile['success_rate'] else "Success Rate: N/A")

        print("\nBy Region:")
        for r in profile['regions']:
            print(f"  {r['region']}: {r['project_count']} projects ({r['total_mw']:,.0f} MW)")

        print("\nBy Technology:")
        for t in profile['by_type'][:5]:
            print(f"  {t['type']}: {t['project_count']} projects ({t['total_mw']:,.0f} MW)")

        print("\nStatus Breakdown:")
        for status, count in list(profile['status_breakdown'].items())[:5]:
            print(f"  {status}: {count}")

        print(f"\nRecent Projects:")
        for p in profile['projects'][:10]:
            print(f"  [{p['queue_id']}] {p['name'][:40]} - {p['capacity_mw']:,.0f} MW ({p['region']})")
        return 0

    if args.compare:
        devs = [d.strip() for d in args.compare.split(',')]
        print("="*60)
        print("DEVELOPER COMPARISON")
        print("="*60)
        comparison = uq.compare_developers(devs)
        if comparison.empty:
            print("No matches found for any developer")
            return 1
        print()
        print(comparison.to_string(index=False))
        return 0

    # Regular search
    results = uq.search(
        developer=args.developer,
        name=args.name,
        region=args.region,
        state=args.state,
        fuel_type=args.type,
        min_mw=args.min_mw,
        max_mw=args.max_mw,
        status=args.status,
    )

    print("="*60)
    print(f"SEARCH RESULTS: {len(results):,} projects found")
    print("="*60)

    if len(results) == 0:
        print("No projects match your criteria")
        return 0

    # Summary stats
    print(f"\nTotal Capacity: {results['capacity_mw'].sum()/1000:,.1f} GW")
    print(f"Average Size: {results['capacity_mw'].mean():,.1f} MW")

    # Show results
    print(f"\nTop {min(args.limit, len(results))} Projects:")
    print("-" * 100)
    display_cols = ['queue_id', 'name', 'developer', 'capacity_mw', 'type', 'region', 'status']
    available_cols = [c for c in display_cols if c in results.columns]
    display = results[available_cols].head(args.limit).copy()
    display['name'] = display['name'].apply(lambda x: str(x)[:35] if pd.notna(x) else 'Unknown')
    display['developer'] = display['developer'].apply(lambda x: str(x)[:25] if pd.notna(x) else 'Unknown')
    print(display.to_string(index=False))

    # Export if requested
    if args.output:
        results.to_csv(args.output, index=False)
        print(f"\nExported {len(results)} results to: {args.output}")

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
