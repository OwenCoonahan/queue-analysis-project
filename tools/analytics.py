#!/usr/bin/env python3
"""
Queue Analytics - Consolidated Analytics Infrastructure

SINGLE SOURCE OF TRUTH for all interconnection queue analytics.
See ANALYTICS.md for architecture documentation.

Usage:
    from analytics import QueueAnalytics

    qa = QueueAnalytics()
    completion = qa.get_completion_probability('PJM', 'Solar', 200)
    developer = qa.get_developer_track_record('Invenergy')
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import warnings

warnings.filterwarnings('ignore')

# =============================================================================
# PATHS
# =============================================================================

TOOLS_DIR = Path(__file__).parent
DATA_DIR = TOOLS_DIR / '.data'
CACHE_DIR = TOOLS_DIR / '.cache'

# Database paths
QUEUE_V2_DB = DATA_DIR / 'queue_v2.db'
QUEUE_DB = DATA_DIR / 'queue.db'
ENRICHMENT_DB = DATA_DIR / 'enrichment.db'

# Cache file paths
LBL_QUEUED_UP = CACHE_DIR / 'lbl_queued_up.xlsx'
EIA_GENERATORS = CACHE_DIR / 'eia_operating_generators.parquet'
ENERGY_COMMUNITIES = CACHE_DIR / 'energy_communities_msa.csv'


# =============================================================================
# CONSTANTS
# =============================================================================

# Status categorization
ACTIVE_STATUSES = ['active', 'pending', 'study', 'in progress', 'engineering']
WITHDRAWN_STATUSES = ['withdrawn', 'cancelled', 'suspended', 'terminated', 'inactive']
COMPLETED_STATUSES = ['operational', 'in service', 'commercial', 'completed', 'online']

# Technology normalization
TECH_MAPPINGS = {
    'solar': ['solar', 'pv', 'photovoltaic'],
    'wind': ['wind', 'onshore wind'],
    'offshore_wind': ['offshore', 'osw', 'offshore wind'],
    'battery': ['battery', 'storage', 'bess', 'energy storage'],
    'gas': ['gas', 'natural gas', 'cc', 'ct', 'combined cycle', 'combustion turbine'],
    'hybrid': ['hybrid', 'solar+storage', 'wind+storage', 'co-located'],
}

# Capacity bands for analysis
CAPACITY_BANDS = [
    ('small', 0, 50),
    ('medium', 50, 200),
    ('large', 200, 500),
    ('utility', 500, float('inf')),
]


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class CompletionProbability:
    """Result of completion probability calculation."""
    region_rate: float
    technology_rate: float
    capacity_band_rate: float
    combined_rate: float
    confidence: str
    sample_size: int
    methodology: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            'region_rate': self.region_rate,
            'technology_rate': self.technology_rate,
            'capacity_band_rate': self.capacity_band_rate,
            'combined_rate': self.combined_rate,
            'confidence': self.confidence,
            'sample_size': self.sample_size,
            'methodology': self.methodology,
        }


@dataclass
class DeveloperRecord:
    """Developer track record result."""
    developer_name: str
    total_projects: int
    completed: int
    withdrawn: int
    active: int
    completion_rate: float
    eia_verified_plants: int
    total_operational_mw: float
    assessment: str
    confidence: str
    completed_projects: List[Dict]

    def to_dict(self) -> Dict[str, Any]:
        return {
            'developer_name': self.developer_name,
            'total_projects': self.total_projects,
            'completed': self.completed,
            'withdrawn': self.withdrawn,
            'active': self.active,
            'completion_rate': self.completion_rate,
            'eia_verified_plants': self.eia_verified_plants,
            'total_operational_mw': self.total_operational_mw,
            'assessment': self.assessment,
            'confidence': self.confidence,
            'completed_projects': self.completed_projects,
        }


# =============================================================================
# MAIN ANALYTICS CLASS
# =============================================================================

class QueueAnalytics:
    """
    Consolidated analytics for interconnection queue analysis.

    This is the SINGLE SOURCE OF TRUTH for all calculations.
    Use this class for all analytics needs - do not create separate modules.
    """

    def __init__(self, use_cache: bool = True):
        """
        Initialize analytics with database connections.

        Args:
            use_cache: Whether to cache loaded data in memory
        """
        self.use_cache = use_cache
        self._cache = {}

        # Lazy-loaded data
        self._lbl_df: Optional[pd.DataFrame] = None
        self._eia_df: Optional[pd.DataFrame] = None
        self._energy_communities_df: Optional[pd.DataFrame] = None

    # =========================================================================
    # DATABASE CONNECTIONS
    # =========================================================================

    def _get_db_connection(self, db_path: Path) -> sqlite3.Connection:
        """Get SQLite connection."""
        if not db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")
        return sqlite3.connect(str(db_path))

    def _query_db(self, query: str, db_path: Path = QUEUE_V2_DB, params: tuple = ()) -> pd.DataFrame:
        """Execute query and return DataFrame."""
        conn = self._get_db_connection(db_path)
        try:
            return pd.read_sql(query, conn, params=params)
        finally:
            conn.close()

    # =========================================================================
    # DATA LOADING
    # =========================================================================

    def _load_lbl_data(self) -> pd.DataFrame:
        """Load LBL Queued Up data."""
        if self._lbl_df is not None:
            return self._lbl_df

        if not LBL_QUEUED_UP.exists():
            raise FileNotFoundError(f"LBL data not found: {LBL_QUEUED_UP}")

        # Load the Complete Queue Data sheet
        xl = pd.ExcelFile(LBL_QUEUED_UP)
        sheet_name = None
        for s in xl.sheet_names:
            if 'complete queue' in s.lower() or '03.' in s:
                sheet_name = s
                break

        if sheet_name is None:
            sheet_name = xl.sheet_names[0]

        df = pd.read_excel(LBL_QUEUED_UP, sheet_name=sheet_name, header=1)

        # Convert Excel dates (handle mixed formats)
        date_cols = ['q_date', 'on_date', 'wd_date', 'ia_date']
        for col in date_cols:
            if col in df.columns:
                def convert_date(val):
                    if pd.isna(val) or val == 'NA' or val == '':
                        return pd.NaT
                    try:
                        # If already datetime
                        if isinstance(val, (datetime, pd.Timestamp)):
                            return val
                        # If numeric (Excel serial date)
                        if isinstance(val, (int, float)) and 10000 < val < 100000:
                            return datetime(1899, 12, 30) + timedelta(days=int(val))
                        # Try string parsing
                        return pd.to_datetime(val, errors='coerce')
                    except:
                        return pd.NaT
                df[col] = df[col].apply(convert_date)

        if self.use_cache:
            self._lbl_df = df

        return df

    def _load_eia_data(self) -> pd.DataFrame:
        """Load EIA 860 operating generators."""
        if self._eia_df is not None:
            return self._eia_df

        if not EIA_GENERATORS.exists():
            # Try loading from Excel
            excel_path = CACHE_DIR / '3_1_Generator_Y2024.xlsx'
            if excel_path.exists():
                df = pd.read_excel(excel_path, skiprows=1)
            else:
                return pd.DataFrame()
        else:
            df = pd.read_parquet(EIA_GENERATORS)

        if self.use_cache:
            self._eia_df = df

        return df

    def _load_energy_communities(self) -> pd.DataFrame:
        """Load energy communities data for IRA eligibility."""
        if self._energy_communities_df is not None:
            return self._energy_communities_df

        if not ENERGY_COMMUNITIES.exists():
            # Try alternative paths
            alt_paths = [
                CACHE_DIR / 'MSA_NMSA_EC_Status_2023.csv',
                CACHE_DIR / 'energy_communities.csv',
            ]
            for path in alt_paths:
                if path.exists():
                    df = pd.read_csv(path)
                    if self.use_cache:
                        self._energy_communities_df = df
                    return df
            return pd.DataFrame()

        df = pd.read_csv(ENERGY_COMMUNITIES)
        if self.use_cache:
            self._energy_communities_df = df
        return df

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _normalize_technology(self, tech: str) -> str:
        """Normalize technology name to standard category."""
        if pd.isna(tech):
            return 'unknown'

        tech_lower = str(tech).lower().strip()

        for normalized, keywords in TECH_MAPPINGS.items():
            for kw in keywords:
                if kw in tech_lower:
                    return normalized

        return tech_lower

    def _categorize_status(self, status: str) -> str:
        """Categorize status into Active/Withdrawn/Completed/Unknown."""
        if pd.isna(status):
            return 'Unknown'

        status_lower = str(status).lower()

        for kw in WITHDRAWN_STATUSES:
            if kw in status_lower:
                return 'Withdrawn'

        for kw in COMPLETED_STATUSES:
            if kw in status_lower:
                return 'Completed'

        for kw in ACTIVE_STATUSES:
            if kw in status_lower:
                return 'Active'

        return 'Unknown'

    def _get_capacity_band(self, capacity_mw: float) -> str:
        """Get capacity band for a given MW value."""
        for name, low, high in CAPACITY_BANDS:
            if low <= capacity_mw < high:
                return name
        return 'utility'

    def _calculate_confidence(self, sample_size: int) -> str:
        """Determine confidence level based on sample size."""
        if sample_size >= 100:
            return 'high'
        elif sample_size >= 30:
            return 'medium'
        elif sample_size >= 10:
            return 'low'
        else:
            return 'very_low'

    def _to_native(self, val):
        """Convert numpy types to native Python types for JSON serialization."""
        if isinstance(val, (np.integer, np.int64, np.int32)):
            return int(val)
        elif isinstance(val, (np.floating, np.float64, np.float32)):
            return float(val)
        elif isinstance(val, np.ndarray):
            return val.tolist()
        elif isinstance(val, pd.Timestamp):
            return val.isoformat()
        elif pd.isna(val):
            return None
        return val

    # =========================================================================
    # TIER 1 ANALYTICS
    # =========================================================================

    def get_completion_probability(
        self,
        region: str,
        technology: str,
        capacity_mw: float,
    ) -> Dict[str, Any]:
        """
        Calculate completion probability based on historical data.

        Uses LBL Queued Up data to determine likelihood of reaching COD
        based on region, technology, and capacity band.

        Args:
            region: ISO/RTO region (e.g., 'PJM', 'MISO')
            technology: Project type (e.g., 'Solar', 'Battery')
            capacity_mw: Project capacity in MW

        Returns:
            Dictionary with completion rates and confidence
        """
        try:
            df = self._load_lbl_data()
        except FileNotFoundError:
            return {'error': 'LBL data not available'}

        # Normalize inputs
        region_upper = region.upper()
        tech_normalized = self._normalize_technology(technology)
        capacity_band = self._get_capacity_band(capacity_mw)

        # Get status column
        status_col = 'q_status' if 'q_status' in df.columns else 'status'
        region_col = 'region' if 'region' in df.columns else 'Region'
        type_col = 'type_clean' if 'type_clean' in df.columns else 'type'
        cap_col = 'mw1' if 'mw1' in df.columns else 'capacity_mw'

        # Categorize all statuses
        df['_status_cat'] = df[status_col].apply(self._categorize_status)

        # Calculate REGION completion rate
        region_mask = df[region_col].astype(str).str.upper().str.contains(region_upper, na=False)
        region_df = df[region_mask]
        region_completed = (region_df['_status_cat'] == 'Completed').sum()
        region_resolved = ((region_df['_status_cat'] == 'Completed') | (region_df['_status_cat'] == 'Withdrawn')).sum()
        region_rate = region_completed / region_resolved if region_resolved > 0 else 0.20

        # Calculate TECHNOLOGY completion rate
        df['_tech_norm'] = df[type_col].apply(self._normalize_technology)
        tech_df = df[df['_tech_norm'] == tech_normalized]
        tech_completed = (tech_df['_status_cat'] == 'Completed').sum()
        tech_resolved = ((tech_df['_status_cat'] == 'Completed') | (tech_df['_status_cat'] == 'Withdrawn')).sum()
        tech_rate = tech_completed / tech_resolved if tech_resolved > 0 else 0.20

        # Calculate CAPACITY BAND completion rate
        df['_cap_numeric'] = pd.to_numeric(df[cap_col], errors='coerce')
        band_low, band_high = 0, float('inf')
        for name, low, high in CAPACITY_BANDS:
            if name == capacity_band:
                band_low, band_high = low, high
                break

        cap_mask = (df['_cap_numeric'] >= band_low) & (df['_cap_numeric'] < band_high)
        cap_df = df[cap_mask]
        cap_completed = (cap_df['_status_cat'] == 'Completed').sum()
        cap_resolved = ((cap_df['_status_cat'] == 'Completed') | (cap_df['_status_cat'] == 'Withdrawn')).sum()
        cap_rate = cap_completed / cap_resolved if cap_resolved > 0 else 0.20

        # Combined rate (weighted average - region most important)
        combined_rate = (region_rate * 0.4 + tech_rate * 0.35 + cap_rate * 0.25)

        # Sample size for confidence
        combined_mask = region_mask & (df['_tech_norm'] == tech_normalized)
        sample_size = len(df[combined_mask])

        return {
            'region_rate': round(float(region_rate), 3),
            'technology_rate': round(float(tech_rate), 3),
            'capacity_band_rate': round(float(cap_rate), 3),
            'combined_rate': round(float(combined_rate), 3),
            'confidence': self._calculate_confidence(sample_size),
            'sample_size': int(sample_size),
            'region_sample': int(len(region_df)),
            'tech_sample': int(len(tech_df)),
            'capacity_band': capacity_band,
            'methodology': f'LBL historical data, {region} {tech_normalized} projects',
        }

    def get_developer_track_record(
        self,
        developer_name: str,
        region: Optional[str] = None,
        verify_eia: bool = True,
    ) -> Dict[str, Any]:
        """
        Analyze developer's historical track record.

        Cross-references queue data with EIA 860 to verify actual completions.

        Args:
            developer_name: Developer/company name
            region: Optional region filter
            verify_eia: Whether to cross-reference with EIA data

        Returns:
            Dictionary with developer statistics and project list
        """
        try:
            df = self._load_lbl_data()
        except FileNotFoundError:
            return {'error': 'LBL data not available'}

        if not developer_name or developer_name.lower() in ['unknown', 'n/a', 'nan']:
            return {
                'developer_name': developer_name,
                'total_projects': 0,
                'completed': 0,
                'withdrawn': 0,
                'active': 0,
                'completion_rate': 0,
                'assessment': 'Developer name not provided',
                'confidence': 'none',
                'completed_projects': [],
            }

        # Find developer column
        dev_col = 'developer' if 'developer' in df.columns else 'Developer'
        status_col = 'q_status' if 'q_status' in df.columns else 'status'
        region_col = 'region' if 'region' in df.columns else 'Region'

        # Match developer (case-insensitive, partial match)
        dev_lower = developer_name.lower().strip()
        mask = df[dev_col].fillna('').astype(str).str.lower().str.contains(dev_lower, regex=False)

        # Optional region filter
        if region:
            region_mask = df[region_col].astype(str).str.upper().str.contains(region.upper(), na=False)
            mask = mask & region_mask

        dev_projects = df[mask].copy()

        if dev_projects.empty:
            return {
                'developer_name': developer_name,
                'total_projects': 0,
                'completed': 0,
                'withdrawn': 0,
                'active': 0,
                'completion_rate': 0,
                'assessment': 'No historical projects found',
                'confidence': 'none',
                'completed_projects': [],
            }

        # Categorize statuses
        dev_projects['_status_cat'] = dev_projects[status_col].apply(self._categorize_status)

        total = len(dev_projects)
        completed = (dev_projects['_status_cat'] == 'Completed').sum()
        withdrawn = (dev_projects['_status_cat'] == 'Withdrawn').sum()
        active = (dev_projects['_status_cat'] == 'Active').sum()

        # Completion rate (of resolved projects)
        resolved = completed + withdrawn
        completion_rate = completed / resolved if resolved > 0 else 0

        # Get completed project details
        completed_df = dev_projects[dev_projects['_status_cat'] == 'Completed']
        name_col = 'project_name' if 'project_name' in df.columns else 'name'
        cap_col = 'mw1' if 'mw1' in df.columns else 'capacity_mw'
        state_col = 'state' if 'state' in df.columns else 'State'
        cod_col = 'on_date' if 'on_date' in df.columns else 'cod_date'
        type_col = 'type_clean' if 'type_clean' in df.columns else 'type'

        completed_projects = []
        for _, row in completed_df.head(10).iterrows():
            proj = {
                'name': str(row.get(name_col, 'Unknown'))[:50],
                'capacity_mw': float(row[cap_col]) if pd.notna(row.get(cap_col)) else None,
                'state': str(row.get(state_col, '')),
                'type': str(row.get(type_col, '')),
                'region': str(row.get(region_col, '')),
            }
            if cod_col in row and pd.notna(row[cod_col]):
                try:
                    proj['cod_date'] = pd.to_datetime(row[cod_col]).strftime('%Y-%m')
                except:
                    proj['cod_date'] = None
            completed_projects.append(proj)

        # Calculate total completed MW
        completed_mw = completed_df[cap_col].fillna(0).sum() if cap_col in completed_df.columns else 0

        # EIA verification
        eia_verified = 0
        eia_mw = 0
        if verify_eia:
            try:
                eia_df = self._load_eia_data()
                if not eia_df.empty:
                    # Find EIA operator column
                    eia_op_col = None
                    for col in ['Operator Name', 'operator_name', 'Entity Name']:
                        if col in eia_df.columns:
                            eia_op_col = col
                            break

                    if eia_op_col:
                        eia_mask = eia_df[eia_op_col].fillna('').astype(str).str.lower().str.contains(dev_lower, regex=False)
                        eia_matched = eia_df[eia_mask]
                        eia_verified = len(eia_matched)

                        # Sum capacity
                        cap_cols = ['Nameplate Capacity (MW)', 'nameplate_capacity_mw', 'Capacity']
                        for col in cap_cols:
                            if col in eia_matched.columns:
                                eia_mw = eia_matched[col].fillna(0).sum()
                                break
            except:
                pass

        # Assessment
        if completion_rate >= 0.40 and completed >= 5:
            assessment = f'Excellent track record: {completed} projects completed ({completion_rate*100:.0f}% rate)'
        elif completion_rate >= 0.25 and completed >= 3:
            assessment = f'Good track record: {completed} completed, {withdrawn} withdrawn'
        elif completion_rate >= 0.15 or completed >= 2:
            assessment = f'Mixed track record: {completion_rate*100:.0f}% completion rate'
        elif completed >= 1:
            assessment = f'Limited track record: {completed} completion, {withdrawn} withdrawals'
        else:
            assessment = f'Poor track record: {withdrawn} withdrawals, no completions'

        if eia_verified > 0:
            assessment += f' | EIA verified: {eia_verified} plants ({eia_mw:,.0f} MW)'

        return {
            'developer_name': developer_name,
            'total_projects': total,
            'completed': completed,
            'withdrawn': withdrawn,
            'active': active,
            'completion_rate': round(completion_rate, 3),
            'completed_mw': round(completed_mw, 0),
            'eia_verified_plants': eia_verified,
            'total_operational_mw': round(eia_mw, 0),
            'assessment': assessment,
            'confidence': self._calculate_confidence(total),
            'completed_projects': completed_projects,
        }

    def get_poi_congestion_score(
        self,
        poi_name: str,
        region: str,
        project_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Analyze queue depth and competition at Point of Interconnection.

        Args:
            poi_name: Substation/POI name
            region: ISO/RTO region
            project_id: Optional - this project's ID to calculate position

        Returns:
            Dictionary with POI congestion metrics
        """
        if not poi_name or poi_name.lower() in ['unknown', 'n/a', 'nan', 'tbd']:
            return {
                'poi_name': poi_name,
                'error': 'POI name not specified',
                'risk_level': 'UNKNOWN',
            }

        try:
            df = self._load_lbl_data()
        except FileNotFoundError:
            # Fallback to queue database
            try:
                df = self._query_db('SELECT * FROM projects', QUEUE_DB)
            except:
                return {'error': 'No data available'}

        # Find POI column
        poi_col = None
        for col in ['poi_name', 'poi', 'POI', 'substation', 'Substation']:
            if col in df.columns:
                poi_col = col
                break

        if poi_col is None:
            return {'error': 'POI column not found in data'}

        # Find matching projects at this POI
        poi_lower = poi_name.lower().strip()
        poi_mask = df[poi_col].fillna('').astype(str).str.lower().str.contains(poi_lower, regex=False)

        # Optional region filter
        region_col = 'region' if 'region' in df.columns else 'Region'
        if region:
            region_mask = df[region_col].astype(str).str.upper().str.contains(region.upper(), na=False)
            poi_mask = poi_mask & region_mask

        poi_projects = df[poi_mask].copy()

        if poi_projects.empty:
            return {
                'poi_name': poi_name,
                'total_projects': 0,
                'risk_level': 'LOW',
                'risk_reason': 'No other projects at this POI',
            }

        # Get status and capacity
        status_col = 'q_status' if 'q_status' in df.columns else 'status'
        cap_col = 'mw1' if 'mw1' in df.columns else 'capacity_mw'
        date_col = 'q_date' if 'q_date' in df.columns else 'queue_date'
        id_col = 'q_id' if 'q_id' in df.columns else 'queue_id'

        poi_projects['_status_cat'] = poi_projects[status_col].apply(self._categorize_status)

        total_projects = len(poi_projects)
        active_projects = (poi_projects['_status_cat'] == 'Active').sum()
        withdrawn_projects = (poi_projects['_status_cat'] == 'Withdrawn').sum()
        completed_projects = (poi_projects['_status_cat'] == 'Completed').sum()

        total_capacity = poi_projects[cap_col].fillna(0).sum() if cap_col in poi_projects.columns else 0

        # Calculate withdrawal rate
        resolved = withdrawn_projects + completed_projects
        withdrawal_rate = withdrawn_projects / resolved if resolved > 0 else 0

        # Calculate queue position if project_id provided
        queue_position = None
        projects_ahead = 0
        capacity_ahead = 0

        if project_id and date_col in poi_projects.columns and id_col in poi_projects.columns:
            try:
                this_project = poi_projects[poi_projects[id_col].astype(str) == str(project_id)]
                if not this_project.empty:
                    this_date = pd.to_datetime(this_project.iloc[0][date_col], errors='coerce')
                    if pd.notna(this_date):
                        # Count active projects that entered before this one
                        active_poi = poi_projects[poi_projects['_status_cat'] == 'Active']
                        for _, row in active_poi.iterrows():
                            other_date = pd.to_datetime(row[date_col], errors='coerce')
                            if pd.notna(other_date) and other_date < this_date:
                                projects_ahead += 1
                                if cap_col in row:
                                    capacity_ahead += row[cap_col] if pd.notna(row[cap_col]) else 0

                        queue_position = projects_ahead + 1
            except:
                pass

        # Risk assessment
        if withdrawal_rate > 0.70 or projects_ahead > 5:
            risk_level = 'HIGH'
            risk_reason = f'{withdrawal_rate*100:.0f}% withdrawal rate, {projects_ahead} projects ahead'
        elif withdrawal_rate > 0.50 or projects_ahead > 3:
            risk_level = 'ELEVATED'
            risk_reason = f'{projects_ahead} projects ahead, {withdrawal_rate*100:.0f}% historical withdrawal'
        elif withdrawal_rate > 0.30 or projects_ahead > 1:
            risk_level = 'MODERATE'
            risk_reason = f'{projects_ahead} projects ahead at POI'
        else:
            risk_level = 'LOW'
            risk_reason = 'Favorable queue position'

        return {
            'poi_name': poi_name,
            'region': region,
            'total_projects': total_projects,
            'active_projects': active_projects,
            'withdrawn_projects': withdrawn_projects,
            'completed_projects': completed_projects,
            'total_capacity_mw': round(total_capacity, 0),
            'queue_position': queue_position,
            'projects_ahead': projects_ahead,
            'capacity_ahead_mw': round(capacity_ahead, 0),
            'withdrawal_rate': round(withdrawal_rate, 3),
            'risk_level': risk_level,
            'risk_reason': risk_reason,
        }

    def get_cost_percentile(
        self,
        region: str,
        technology: str,
        capacity_mw: float,
        estimated_cost_per_kw: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Rank project's estimated IC cost against historical actuals.

        Args:
            region: ISO/RTO region
            technology: Project type
            capacity_mw: Project capacity
            estimated_cost_per_kw: Optional - estimated cost to rank

        Returns:
            Dictionary with cost percentiles and histogram
        """
        # Try to load regional IC cost data
        cost_files = {
            'PJM': CACHE_DIR / 'pjm_costs_2022_clean_data.xlsx',
            'MISO': CACHE_DIR / 'miso_costs_2021_clean_data.xlsx',
            'SPP': CACHE_DIR / 'spp_costs_2023_clean_data.xlsx',
            'NYISO': CACHE_DIR / 'nyiso_interconnection_cost_data.xlsx',
            'ISO-NE': CACHE_DIR / 'isone_interconnection_cost_data.xlsx',
            'ISONE': CACHE_DIR / 'isone_interconnection_cost_data.xlsx',
        }

        region_upper = region.upper()
        cost_file = cost_files.get(region_upper)

        if cost_file is None or not cost_file.exists():
            return {
                'error': f'No IC cost data available for {region}',
                'available_regions': list(cost_files.keys()),
            }

        try:
            # Load cost data - LBL format has 'data' sheet
            xl = pd.ExcelFile(cost_file)
            if 'data' in xl.sheet_names:
                df = pd.read_excel(cost_file, sheet_name='data')
            else:
                df = pd.read_excel(cost_file)
        except Exception as e:
            return {'error': f'Failed to load cost data: {e}'}

        # Find cost per kW column
        cost_col = None
        for col in df.columns:
            col_lower = col.lower()
            if 'total cost/kw' in col_lower or 'total/kw' in col_lower or 'cost_per_kw' in col_lower:
                cost_col = col
                break

        if cost_col is None:
            return {'error': 'Cost column not found in data'}

        # Filter by technology if possible
        tech_normalized = self._normalize_technology(technology)
        type_col = None
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['fuel', 'resource type', 'type', 'project_type']:
                type_col = col
                break

        if type_col:
            df['_tech_norm'] = df[type_col].apply(self._normalize_technology)
            tech_match = df[df['_tech_norm'] == tech_normalized]
            if len(tech_match) >= 10:
                df = tech_match

        # Get numeric costs
        costs = pd.to_numeric(df[cost_col], errors='coerce').dropna()
        costs = costs[costs > 0]  # Remove zeros/negatives

        if len(costs) < 5:
            return {'error': 'Insufficient cost data'}

        # Calculate percentiles
        p10, p25, p50, p75, p90 = np.percentile(costs, [10, 25, 50, 75, 90])

        # Build histogram
        hist, bin_edges = np.histogram(costs, bins=10)
        histogram = []
        for i in range(len(hist)):
            bucket = {
                'range_low': int(bin_edges[i]),
                'range_high': int(bin_edges[i+1]),
                'count': int(hist[i]),
                'contains_project': False,
            }
            if estimated_cost_per_kw is not None:
                if bin_edges[i] <= estimated_cost_per_kw < bin_edges[i+1]:
                    bucket['contains_project'] = True
            histogram.append(bucket)

        # Calculate project's percentile if cost provided
        project_percentile = None
        interpretation = None
        if estimated_cost_per_kw is not None:
            project_percentile = (costs < estimated_cost_per_kw).mean() * 100

            if project_percentile <= 25:
                interpretation = 'Lowest quartile - favorable cost position'
            elif project_percentile <= 50:
                interpretation = 'Below median - competitive cost'
            elif project_percentile <= 75:
                interpretation = 'Above median - elevated cost'
            else:
                interpretation = 'Highest quartile - significant cost risk'

        return {
            'region': region,
            'technology': technology,
            'sample_size': len(costs),
            'p10': round(p10, 0),
            'p25': round(p25, 0),
            'p50': round(p50, 0),
            'p75': round(p75, 0),
            'p90': round(p90, 0),
            'mean': round(costs.mean(), 0),
            'min': round(costs.min(), 0),
            'max': round(costs.max(), 0),
            'estimated_cost_per_kw': estimated_cost_per_kw,
            'project_percentile': round(project_percentile, 0) if project_percentile is not None else None,
            'interpretation': interpretation,
            'histogram': histogram,
        }

    def get_timeline_benchmarks(
        self,
        region: str,
        technology: str,
    ) -> Dict[str, Any]:
        """
        Get historical time-to-COD benchmarks for similar projects.

        Args:
            region: ISO/RTO region
            technology: Project type

        Returns:
            Dictionary with timeline percentiles
        """
        try:
            df = self._load_lbl_data()
        except FileNotFoundError:
            return {'error': 'LBL data not available'}

        # Get date columns
        queue_col = 'q_date' if 'q_date' in df.columns else 'queue_date'
        cod_col = 'on_date' if 'on_date' in df.columns else 'cod_date'
        status_col = 'q_status' if 'q_status' in df.columns else 'status'
        region_col = 'region' if 'region' in df.columns else 'Region'
        type_col = 'type_clean' if 'type_clean' in df.columns else 'type'

        # Filter to completed projects
        df['_status_cat'] = df[status_col].apply(self._categorize_status)
        completed = df[df['_status_cat'] == 'Completed'].copy()

        if completed.empty:
            return {'error': 'No completed projects in data'}

        # Filter by region
        region_upper = region.upper()
        region_mask = completed[region_col].astype(str).str.upper().str.contains(region_upper, na=False)

        # Filter by technology
        tech_normalized = self._normalize_technology(technology)
        completed['_tech_norm'] = completed[type_col].apply(self._normalize_technology)
        tech_mask = completed['_tech_norm'] == tech_normalized

        # Apply filters (prefer both, fall back to just region)
        combined_mask = region_mask & tech_mask
        if combined_mask.sum() >= 10:
            filtered = completed[combined_mask]
        elif region_mask.sum() >= 10:
            filtered = completed[region_mask]
        else:
            filtered = completed

        # Calculate time to COD
        filtered[queue_col] = pd.to_datetime(filtered[queue_col], errors='coerce')
        filtered[cod_col] = pd.to_datetime(filtered[cod_col], errors='coerce')

        valid = filtered[filtered[queue_col].notna() & filtered[cod_col].notna()].copy()
        valid['_months_to_cod'] = (valid[cod_col] - valid[queue_col]).dt.days / 30

        # Filter outliers
        valid = valid[(valid['_months_to_cod'] > 0) & (valid['_months_to_cod'] < 180)]

        if len(valid) < 5:
            return {'error': 'Insufficient timeline data'}

        times = valid['_months_to_cod']

        p10, p25, p50, p75, p90 = np.percentile(times, [10, 25, 50, 75, 90])

        return {
            'region': region,
            'technology': technology,
            'sample_size': int(len(valid)),
            'p10_months': int(round(p10, 0)),
            'p25_months': int(round(p25, 0)),
            'p50_months': int(round(p50, 0)),
            'p75_months': int(round(p75, 0)),
            'p90_months': int(round(p90, 0)),
            'mean_months': int(round(float(times.mean()), 0)),
            'min_months': int(round(float(times.min()), 0)),
            'max_months': int(round(float(times.max()), 0)),
            'methodology': f'Completed {tech_normalized} projects in {region}',
        }

    def get_ira_eligibility(
        self,
        state: str,
        county: Optional[str] = None,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Check if project location qualifies for IRA energy community bonus.

        Args:
            state: State abbreviation (e.g., 'TX', 'PA')
            county: County name (optional but recommended)
            lat: Latitude (optional, for precise lookup)
            lon: Longitude (optional)

        Returns:
            Dictionary with eligibility status and bonus details
        """
        try:
            ec_df = self._load_energy_communities()
        except Exception as e:
            return {'error': f'Energy communities data not available: {e}'}

        if ec_df.empty:
            return {
                'eligible': None,
                'error': 'Energy communities data not loaded',
            }

        # Normalize inputs
        state_upper = state.upper().strip() if state else None

        # Find state column
        state_col = None
        for col in ec_df.columns:
            col_lower = col.lower()
            if 'state' in col_lower or col_lower == 'st':
                state_col = col
                break

        if state_col is None:
            return {'error': 'State column not found in energy communities data'}

        # Filter by state
        state_mask = ec_df[state_col].astype(str).str.upper().str.strip() == state_upper
        state_matches = ec_df[state_mask]

        if state_matches.empty:
            return {
                'eligible': False,
                'state': state,
                'reason': 'State not in energy communities list',
                'bonus_adder': 0,
            }

        # If county provided, try to match
        county_match = None
        if county:
            county_col = None
            for col in ec_df.columns:
                col_lower = col.lower()
                if 'county' in col_lower or 'area' in col_lower:
                    county_col = col
                    break

            if county_col:
                county_lower = county.lower().strip()
                county_mask = state_matches[county_col].fillna('').astype(str).str.lower().str.contains(county_lower, regex=False)
                county_matches = state_matches[county_mask]
                if not county_matches.empty:
                    county_match = county_matches.iloc[0]

        # Determine eligibility
        if county_match is not None:
            # Check for eligibility column
            eligible_col = None
            for col in ec_df.columns:
                col_lower = col.lower()
                if 'eligible' in col_lower or 'status' in col_lower or 'ec' in col_lower:
                    eligible_col = col
                    break

            if eligible_col:
                eligible_val = str(county_match[eligible_col]).lower()
                eligible = eligible_val in ['yes', 'true', '1', 'eligible', 'y']
            else:
                # If in list, assume eligible
                eligible = True

            # Determine category
            category = 'unknown'
            for col in ec_df.columns:
                col_lower = col.lower()
                if 'type' in col_lower or 'category' in col_lower or 'criteria' in col_lower:
                    category = str(county_match[col])
                    break

            return {
                'eligible': eligible,
                'state': state,
                'county': county,
                'category': category,
                'bonus_adder': 0.10 if eligible else 0,  # 10% ITC/PTC bonus
                'bonus_description': '10% adder for ITC or 10% increase to PTC' if eligible else None,
            }

        # State match but no county match
        return {
            'eligible': True,  # Conservative - state has some eligible areas
            'state': state,
            'county': county,
            'category': 'state_level_match',
            'bonus_adder': 0.10,
            'note': f'{state} has energy communities - verify specific location',
        }

    # =========================================================================
    # TIER 2 ANALYTICS - Revenue, Capacity, Transmission
    # =========================================================================

    def get_revenue_estimate(
        self,
        region: str,
        technology: str,
        capacity_mw: float,
        zone: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Estimate annual energy revenue based on LMP prices.

        Uses benchmark LMP data and technology-specific capacity factors
        to project annual energy revenue.

        Args:
            region: ISO/RTO region
            technology: Project type (Solar, Wind, Battery, etc.)
            capacity_mw: Project nameplate capacity
            zone: Optional specific pricing zone

        Returns:
            Dictionary with revenue estimates and assumptions
        """
        try:
            from lmp_data import RevenueEstimator
            estimator = RevenueEstimator()
            result = estimator.estimate_annual_revenue(region, technology, capacity_mw, zone)

            if 'error' in result:
                return self._fallback_revenue_estimate(region, technology, capacity_mw)

            return {
                'region': region,
                'technology': technology,
                'capacity_mw': capacity_mw,
                'zone': result.get('zone', zone),
                # Revenue estimates
                'annual_revenue_millions': round(result.get('annual_revenue', 0) / 1_000_000, 2),
                'revenue_low_millions': round(result.get('revenue_low', 0) / 1_000_000, 2),
                'revenue_high_millions': round(result.get('revenue_high', 0) / 1_000_000, 2),
                'revenue_per_kw': round(result.get('revenue_per_kw', 0), 0),
                # Price assumptions
                'avg_lmp': result.get('price_stats', {}).get('avg_lmp', 0),
                'peak_lmp': result.get('price_stats', {}).get('peak_lmp', 0),
                'offpeak_lmp': result.get('price_stats', {}).get('offpeak_lmp', 0),
                # Capacity factor
                'capacity_factor': result.get('capacity_factor', 0),
                'annual_generation_mwh': round(capacity_mw * result.get('capacity_factor', 0.25) * 8760, 0),
                # Methodology
                'data_source': 'benchmark',
                'methodology': f"Capacity factor × LMP × hours. {technology} CF={result.get('capacity_factor', 0.25):.0%}",
            }
        except ImportError:
            return self._fallback_revenue_estimate(region, technology, capacity_mw)
        except Exception:
            return self._fallback_revenue_estimate(region, technology, capacity_mw)

    def _fallback_revenue_estimate(
        self,
        region: str,
        technology: str,
        capacity_mw: float,
    ) -> Dict[str, Any]:
        """Fallback revenue estimate when lmp_data module unavailable."""
        # Benchmark capacity factors
        capacity_factors = {
            'solar': 0.25,
            'wind': 0.35,
            'battery': 0.15,
            'gas': 0.40,
            'hybrid': 0.28,
        }

        # Benchmark LMPs by region ($/MWh)
        lmps = {
            'PJM': 42,
            'MISO': 32,
            'ERCOT': 28,
            'CAISO': 45,
            'NYISO': 52,
            'SPP': 26,
            'ISO-NE': 58,
        }

        tech_lower = technology.lower()
        cf = 0.25
        for key, val in capacity_factors.items():
            if key in tech_lower:
                cf = val
                break

        lmp = lmps.get(region.upper(), 35)
        annual_mwh = capacity_mw * cf * 8760
        annual_revenue = annual_mwh * lmp

        return {
            'region': region,
            'technology': technology,
            'capacity_mw': capacity_mw,
            'annual_revenue_millions': round(annual_revenue / 1_000_000, 2),
            'revenue_low_millions': round(annual_revenue * 0.7 / 1_000_000, 2),
            'revenue_high_millions': round(annual_revenue * 1.3 / 1_000_000, 2),
            'revenue_per_kw': round(annual_revenue / (capacity_mw * 1000), 0),
            'avg_lmp': lmp,
            'capacity_factor': cf,
            'annual_generation_mwh': round(annual_mwh, 0),
            'data_source': 'fallback_benchmark',
            'methodology': 'Fallback calculation using regional benchmark LMPs',
        }

    def get_capacity_value(
        self,
        region: str,
        technology: str,
        capacity_mw: float,
        delivery_year: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Calculate capacity value based on ELCC and market prices.

        Uses regional capacity market prices (PJM RPM, NYISO ICAP, etc.)
        and technology-specific ELCC values.

        Args:
            region: ISO/RTO region
            technology: Project type
            capacity_mw: Nameplate capacity
            delivery_year: Target delivery year (default: next year)

        Returns:
            Dictionary with capacity value estimates
        """
        if delivery_year is None:
            delivery_year = datetime.now().year + 1

        try:
            from capacity_data import CapacityValue
            cv = CapacityValue()

            result = cv.calculate_capacity_value(region, technology, capacity_mw, delivery_year)

            if 'error' in result:
                return self._fallback_capacity_value(region, technology, capacity_mw, delivery_year)

            return {
                'region': region,
                'technology': technology,
                'capacity_mw': capacity_mw,
                'delivery_year': delivery_year,
                # ELCC
                'elcc_percent': result.get('elcc_percent', 0),
                'accredited_mw': round(result.get('accredited_mw', 0), 1),
                # Pricing
                'price_mw_day': result.get('price_mw_day'),
                'price_kw_month': result.get('price_kw_month'),
                'price_source': result.get('price_source', 'benchmark'),
                # Value
                'annual_value': round(result.get('annual_value', 0), 0),
                'annual_value_millions': round(result.get('annual_value', 0) / 1_000_000, 2),
                'value_per_kw': round(result.get('value_per_kw', 0), 2),
                # Context
                'market_type': self._get_capacity_market_type(region),
                'methodology': result.get('methodology', ''),
            }
        except ImportError:
            return self._fallback_capacity_value(region, technology, capacity_mw, delivery_year)
        except Exception:
            return self._fallback_capacity_value(region, technology, capacity_mw, delivery_year)

    def _get_capacity_market_type(self, region: str) -> str:
        """Get capacity market type for region."""
        markets = {
            'PJM': 'RPM (Reliability Pricing Model)',
            'NYISO': 'ICAP (Installed Capacity)',
            'ISO-NE': 'FCM (Forward Capacity Market)',
            'MISO': 'PRA (Planning Resource Auction)',
            'CAISO': 'RA (Resource Adequacy)',
            'ERCOT': 'Energy-Only (No capacity market)',
            'SPP': 'Energy-Only (No capacity market)',
        }
        return markets.get(region.upper(), 'Unknown')

    def _fallback_capacity_value(
        self,
        region: str,
        technology: str,
        capacity_mw: float,
        delivery_year: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Fallback capacity value when capacity_data module unavailable."""
        # Benchmark ELCC values
        elcc = {
            'solar': 0.35,
            'wind': 0.15,
            'battery': 0.90,
            'gas': 0.95,
            'hybrid': 0.50,
        }

        # Benchmark capacity prices ($/MW-day for PJM/MISO, $/kW-month for others)
        prices = {
            'PJM': {'type': 'mw_day', 'price': 270},
            'MISO': {'type': 'mw_day', 'price': 45},
            'NYISO': {'type': 'kw_month', 'price': 5.0},
            'ISO-NE': {'type': 'kw_month', 'price': 3.5},
            'CAISO': {'type': 'kw_month', 'price': 7.0},
            'ERCOT': {'type': 'none', 'price': 0},
            'SPP': {'type': 'none', 'price': 0},
        }

        tech_lower = technology.lower()
        elcc_pct = 0.35
        for key, val in elcc.items():
            if key in tech_lower:
                elcc_pct = val
                break

        region_upper = region.upper()
        price_info = prices.get(region_upper, {'type': 'none', 'price': 0})
        accredited_mw = capacity_mw * elcc_pct

        if price_info['type'] == 'mw_day':
            annual_value = accredited_mw * price_info['price'] * 365
        elif price_info['type'] == 'kw_month':
            annual_value = accredited_mw * 1000 * price_info['price'] * 12
        else:
            annual_value = 0

        return {
            'region': region,
            'technology': technology,
            'capacity_mw': capacity_mw,
            'delivery_year': delivery_year or datetime.now().year + 1,
            'elcc_percent': elcc_pct,
            'accredited_mw': round(accredited_mw, 1),
            'price_mw_day': price_info['price'] if price_info['type'] == 'mw_day' else None,
            'price_kw_month': price_info['price'] if price_info['type'] == 'kw_month' else None,
            'annual_value': round(annual_value, 0),
            'annual_value_millions': round(annual_value / 1_000_000, 2),
            'value_per_kw': round(annual_value / (capacity_mw * 1000), 2) if capacity_mw > 0 else 0,
            'market_type': self._get_capacity_market_type(region),
            'data_source': 'fallback_benchmark',
        }

    def get_transmission_risk(
        self,
        region: str,
        zone: Optional[str] = None,
        poi: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Assess transmission constraint risk for a location.

        Analyzes congestion patterns, binding constraints, and
        planned upgrades to score transmission risk.

        Args:
            region: ISO/RTO region
            zone: Pricing/transmission zone
            poi: Point of Interconnection name

        Returns:
            Dictionary with transmission risk assessment
        """
        try:
            from transmission_data import ConstraintAnalysis
            ca = ConstraintAnalysis()
            result = ca.assess_poi_risk(region, zone, poi)

            if 'error' in result:
                return self._fallback_transmission_risk(region, zone)

            return {
                'region': region,
                'zone': zone or result.get('zone'),
                'poi': poi,
                # Risk scores
                'risk_score': result.get('risk_score', 0),
                'risk_rating': result.get('risk_rating', 'UNKNOWN'),
                # Congestion metrics
                'congestion_level': result.get('congestion_level', 'unknown'),
                'avg_congestion_cost': result.get('avg_congestion_cost', 0),
                'pct_hours_congested': result.get('pct_hours_congested', 0),
                'annual_congestion_impact': result.get('annual_congestion_impact', 0),
                # Constraints
                'relevant_constraints': result.get('relevant_constraints', []),
                'constraint_count': len(result.get('relevant_constraints', [])),
                # Upgrades
                'planned_upgrades': result.get('planned_upgrades', []),
                'upgrade_count': len(result.get('planned_upgrades', [])),
                # Notes
                'risk_notes': result.get('risk_notes', []),
                'methodology': 'Benchmark congestion data and constraint analysis',
            }
        except ImportError:
            return self._fallback_transmission_risk(region, zone)
        except Exception:
            return self._fallback_transmission_risk(region, zone)

    def _fallback_transmission_risk(
        self,
        region: str,
        zone: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fallback transmission risk when transmission_data module unavailable."""
        # Benchmark congestion levels by region
        congestion = {
            'ERCOT': {'level': 'high', 'cost': 12.0, 'pct': 0.20},
            'NYISO': {'level': 'high', 'cost': 10.0, 'pct': 0.18},
            'CAISO': {'level': 'medium', 'cost': 7.0, 'pct': 0.15},
            'SPP': {'level': 'medium', 'cost': 6.0, 'pct': 0.12},
            'PJM': {'level': 'low', 'cost': 4.0, 'pct': 0.08},
            'MISO': {'level': 'low', 'cost': 4.0, 'pct': 0.10},
            'ISO-NE': {'level': 'medium', 'cost': 5.0, 'pct': 0.12},
        }

        region_upper = region.upper()
        data = congestion.get(region_upper, {'level': 'unknown', 'cost': 5.0, 'pct': 0.10})

        risk_ratings = {'low': 'LOW', 'medium': 'MODERATE', 'high': 'ELEVATED', 'very_high': 'HIGH'}
        risk_scores = {'low': 0.25, 'medium': 0.50, 'high': 0.70, 'very_high': 0.85}

        return {
            'region': region,
            'zone': zone,
            'risk_score': risk_scores.get(data['level'], 0.5),
            'risk_rating': risk_ratings.get(data['level'], 'MODERATE'),
            'congestion_level': data['level'],
            'avg_congestion_cost': data['cost'],
            'pct_hours_congested': data['pct'],
            'annual_congestion_impact': data['cost'] * data['pct'] * 8760,
            'relevant_constraints': [],
            'planned_upgrades': [],
            'data_source': 'fallback_benchmark',
            'methodology': 'Regional benchmark congestion data',
        }

    def get_ppa_benchmarks(
        self,
        region: str,
        technology: str,
        year: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Get PPA price benchmarks for region and technology.

        Returns market PPA pricing ranges based on recent transactions.

        Args:
            region: ISO/RTO region
            technology: Project type
            year: Benchmark year (default: current)

        Returns:
            Dictionary with PPA price ranges
        """
        if year is None:
            year = datetime.now().year

        try:
            from ppa_data import PPABenchmarks
            ppa = PPABenchmarks()

            result = ppa.get_benchmark(region, technology, year)

            if 'error' in result:
                return self._fallback_ppa_benchmarks(region, technology, year)

            return {
                'region': region,
                'technology': technology,
                'year': year,
                'price_low': result.get('price_low', 0),
                'price_mid': result.get('price_mid', 0),
                'price_high': result.get('price_high', 0),
                'trend': result.get('trend', 'stable'),
                'sample_deals': result.get('sample_deals', []),
                'methodology': 'Public PPA announcements and market intelligence',
            }
        except ImportError:
            return self._fallback_ppa_benchmarks(region, technology, year)
        except Exception:
            return self._fallback_ppa_benchmarks(region, technology, year)

    def _fallback_ppa_benchmarks(
        self,
        region: str,
        technology: str,
        year: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Fallback PPA benchmarks when ppa_data module unavailable."""
        # Benchmark PPA prices ($/MWh)
        benchmarks = {
            'ERCOT': {'solar': 24, 'wind': 22, 'battery': 9, 'hybrid': 30},
            'PJM': {'solar': 38, 'wind': 35, 'battery': 14, 'hybrid': 45},
            'NYISO': {'solar': 48, 'wind': 45, 'battery': 20, 'hybrid': 60},
            'MISO': {'solar': 30, 'wind': 24, 'battery': 11, 'hybrid': 38},
            'CAISO': {'solar': 35, 'wind': 48, 'battery': 16, 'hybrid': 50},
            'SPP': {'solar': 24, 'wind': 20, 'battery': 9, 'hybrid': 32},
            'ISO-NE': {'solar': 58, 'wind': 55, 'battery': 22, 'hybrid': 68},
        }

        region_upper = region.upper()
        tech_lower = technology.lower()

        # Find matching technology
        tech_key = 'solar'
        for key in ['solar', 'wind', 'battery', 'hybrid']:
            if key in tech_lower:
                tech_key = key
                break

        region_data = benchmarks.get(region_upper, benchmarks['PJM'])
        mid_price = region_data.get(tech_key, 35)

        return {
            'region': region,
            'technology': technology,
            'year': year or datetime.now().year,
            'price_low': round(mid_price * 0.85, 0),
            'price_mid': mid_price,
            'price_high': round(mid_price * 1.15, 0),
            'trend': 'stable',
            'sample_deals': [],
            'data_source': 'fallback_benchmark',
        }

    def get_full_revenue_stack(
        self,
        region: str,
        technology: str,
        capacity_mw: float,
        zone: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Calculate full revenue stack: energy + capacity + ancillary.

        Combines LMP revenue, capacity value, and estimates for
        ancillary services to provide total revenue projection.

        Args:
            region: ISO/RTO region
            technology: Project type
            capacity_mw: Nameplate capacity
            zone: Optional pricing zone

        Returns:
            Dictionary with full revenue stack breakdown
        """
        # Get components
        energy = self.get_revenue_estimate(region, technology, capacity_mw, zone)
        capacity = self.get_capacity_value(region, technology, capacity_mw)
        ppa = self.get_ppa_benchmarks(region, technology)

        # Energy revenue
        energy_revenue = energy.get('annual_revenue_millions', 0)

        # Capacity revenue
        capacity_revenue = capacity.get('annual_value_millions', 0)

        # Ancillary services estimate (typically 5-15% of energy for batteries, 2-5% for others)
        tech_lower = technology.lower()
        if 'battery' in tech_lower or 'storage' in tech_lower:
            ancillary_pct = 0.20  # Batteries can get significant ancillary revenue
        elif 'gas' in tech_lower:
            ancillary_pct = 0.05
        else:
            ancillary_pct = 0.03

        ancillary_revenue = energy_revenue * ancillary_pct

        # Total revenue
        total_revenue = energy_revenue + capacity_revenue + ancillary_revenue

        # Revenue per kW
        revenue_per_kw = (total_revenue * 1_000_000) / (capacity_mw * 1000) if capacity_mw > 0 else 0

        return {
            'region': region,
            'technology': technology,
            'capacity_mw': capacity_mw,
            # Revenue components
            'energy_revenue_millions': round(energy_revenue, 2),
            'capacity_revenue_millions': round(capacity_revenue, 2),
            'ancillary_revenue_millions': round(ancillary_revenue, 2),
            'total_revenue_millions': round(total_revenue, 2),
            # Per-unit metrics
            'revenue_per_kw': round(revenue_per_kw, 0),
            'revenue_per_mw_year': round(total_revenue * 1_000_000 / capacity_mw, 0) if capacity_mw > 0 else 0,
            # Revenue mix
            'energy_pct': round(energy_revenue / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'capacity_pct': round(capacity_revenue / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'ancillary_pct': round(ancillary_revenue / total_revenue * 100, 1) if total_revenue > 0 else 0,
            # Assumptions
            'capacity_factor': energy.get('capacity_factor', 0),
            'elcc_percent': capacity.get('elcc_percent', 0),
            'avg_lmp': energy.get('avg_lmp', 0),
            'ppa_benchmark': ppa.get('price_mid', 0),
            # Data quality
            'data_source': 'benchmark',
            'methodology': 'Energy (LMP × CF × hours) + Capacity (ELCC × price) + Ancillary estimate',
        }

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_project_analysis(
        self,
        project_id: str,
        region: str,
        technology: str,
        capacity_mw: float,
        developer: str,
        poi: str,
        state: str,
        county: Optional[str] = None,
        estimated_cost_per_kw: Optional[float] = None,
        include_tier2: bool = True,
    ) -> Dict[str, Any]:
        """
        Run all analytics for a project.

        Convenience method that calls all Tier 1 and optionally Tier 2
        analysis functions and bundles results.

        Args:
            include_tier2: Whether to include revenue/capacity/transmission analysis

        Returns:
            Dictionary with all analysis results
        """
        result = {
            'project_id': project_id,
            # Tier 1: Feasibility
            'completion_probability': self.get_completion_probability(region, technology, capacity_mw),
            'developer_track_record': self.get_developer_track_record(developer, region),
            'poi_congestion': self.get_poi_congestion_score(poi, region, project_id),
            'cost_percentile': self.get_cost_percentile(region, technology, capacity_mw, estimated_cost_per_kw),
            'timeline_benchmarks': self.get_timeline_benchmarks(region, technology),
            'ira_eligibility': self.get_ira_eligibility(state, county),
        }

        # Tier 2: Revenue & Market
        if include_tier2:
            result['revenue_estimate'] = self.get_revenue_estimate(region, technology, capacity_mw)
            result['capacity_value'] = self.get_capacity_value(region, technology, capacity_mw)
            result['transmission_risk'] = self.get_transmission_risk(region, poi=poi)
            result['ppa_benchmarks'] = self.get_ppa_benchmarks(region, technology)
            result['full_revenue_stack'] = self.get_full_revenue_stack(region, technology, capacity_mw)

        result['generated_at'] = datetime.now().isoformat()
        return result

    def get_summary_stats(self, region: Optional[str] = None) -> Dict[str, Any]:
        """Get summary statistics for the database."""
        try:
            df = self._load_lbl_data()
        except FileNotFoundError:
            return {'error': 'Data not available'}

        status_col = 'q_status' if 'q_status' in df.columns else 'status'
        region_col = 'region' if 'region' in df.columns else 'Region'
        cap_col = 'mw1' if 'mw1' in df.columns else 'capacity_mw'

        if region:
            region_mask = df[region_col].astype(str).str.upper().str.contains(region.upper(), na=False)
            df = df[region_mask]

        df['_status_cat'] = df[status_col].apply(self._categorize_status)

        return {
            'total_projects': int(len(df)),
            'completed': int((df['_status_cat'] == 'Completed').sum()),
            'withdrawn': int((df['_status_cat'] == 'Withdrawn').sum()),
            'active': int((df['_status_cat'] == 'Active').sum()),
            'total_capacity_gw': round(float(df[cap_col].fillna(0).sum()) / 1000, 1) if cap_col in df.columns else None,
            'region_filter': region,
        }


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for analytics."""
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Queue Analytics CLI")
    parser.add_argument('--completion', nargs=3, metavar=('REGION', 'TECH', 'MW'),
                        help='Get completion probability')
    parser.add_argument('--developer', type=str, help='Get developer track record')
    parser.add_argument('--poi', nargs=2, metavar=('POI', 'REGION'),
                        help='Get POI congestion score')
    parser.add_argument('--cost', nargs=3, metavar=('REGION', 'TECH', 'MW'),
                        help='Get cost percentile')
    parser.add_argument('--timeline', nargs=2, metavar=('REGION', 'TECH'),
                        help='Get timeline benchmarks')
    parser.add_argument('--ira', nargs=2, metavar=('STATE', 'COUNTY'),
                        help='Check IRA eligibility')
    parser.add_argument('--stats', action='store_true', help='Show summary stats')
    parser.add_argument('--region', type=str, help='Filter by region')

    args = parser.parse_args()

    qa = QueueAnalytics()

    if args.completion:
        result = qa.get_completion_probability(args.completion[0], args.completion[1], float(args.completion[2]))
    elif args.developer:
        result = qa.get_developer_track_record(args.developer, args.region)
    elif args.poi:
        result = qa.get_poi_congestion_score(args.poi[0], args.poi[1])
    elif args.cost:
        result = qa.get_cost_percentile(args.cost[0], args.cost[1], float(args.cost[2]))
    elif args.timeline:
        result = qa.get_timeline_benchmarks(args.timeline[0], args.timeline[1])
    elif args.ira:
        result = qa.get_ira_eligibility(args.ira[0], args.ira[1])
    elif args.stats:
        result = qa.get_summary_stats(args.region)
    else:
        parser.print_help()
        return

    print(json.dumps(result, indent=2, default=str))


if __name__ == '__main__':
    main()
