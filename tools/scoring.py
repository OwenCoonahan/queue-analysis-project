#!/usr/bin/env python3
"""
Project Feasibility Scoring Model

Scores interconnection queue projects on likelihood of reaching COD.
Designed for deal diligence and portfolio screening.

Scoring Framework (0-100 total):
- Queue Position (25 pts): Position relative to other projects at same POI
- Study Progress (25 pts): How far through the interconnection process
- Developer Track Record (20 pts): Historical completion rate
- POI Congestion (15 pts): Competition for capacity at interconnection point
- Project Characteristics (15 pts): Type, size, and other factors

Usage:
    python3 scoring.py --file queue.xlsx --score PROJECT-123
    python3 scoring.py --file queue.xlsx --rank --limit 20
    python3 scoring.py --file queue.xlsx --rank --type Solar --export top_solar.csv
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path
from dataclasses import dataclass, field
import json
import argparse
import sys

# Import our base analyzer and regional benchmarks
from analyze import QueueData, QueueAnalyzer
from unified_data import RegionalBenchmarks


@dataclass
class ScoreBreakdown:
    """Detailed score breakdown for a project."""
    queue_position: float = 0
    study_progress: float = 0
    developer_track_record: float = 0
    poi_congestion: float = 0
    project_characteristics: float = 0

    # Bonus/penalty adjustments
    adjustments: float = 0
    adjustment_reasons: List[str] = field(default_factory=list)

    # Flags
    red_flags: List[str] = field(default_factory=list)
    green_flags: List[str] = field(default_factory=list)

    @property
    def total(self) -> float:
        base = (self.queue_position + self.study_progress +
                self.developer_track_record + self.poi_congestion +
                self.project_characteristics)
        return max(0, min(100, base + self.adjustments))

    @property
    def grade(self) -> str:
        """Letter grade based on score."""
        if self.total >= 80:
            return 'A'
        elif self.total >= 65:
            return 'B'
        elif self.total >= 50:
            return 'C'
        elif self.total >= 35:
            return 'D'
        else:
            return 'F'

    @property
    def recommendation(self) -> str:
        """High-level recommendation based on score."""
        if self.total >= 70 and not self.red_flags:
            return 'GO'
        elif self.total >= 50 or (self.total >= 40 and self.green_flags):
            return 'CONDITIONAL'
        else:
            return 'NO-GO'

    @property
    def confidence(self) -> str:
        """Confidence level based on data quality and certainty factors."""
        # Calculate confidence based on multiple factors
        confidence_score = 0

        # Study progress contributes most (more advanced = more certain)
        study_pct = self.study_progress / 25  # 0-1
        if study_pct >= 0.8:
            confidence_score += 40  # IA signed - very certain
        elif study_pct >= 0.6:
            confidence_score += 30  # Facilities study
        elif study_pct >= 0.4:
            confidence_score += 20  # SIS
        else:
            confidence_score += 10  # Early stage

        # Developer track record adds certainty
        dev_pct = self.developer_track_record / 20
        if dev_pct >= 0.7:
            confidence_score += 25  # Experienced developer
        elif dev_pct >= 0.5:
            confidence_score += 15
        else:
            confidence_score += 5

        # Red flags reduce confidence
        confidence_score -= len(self.red_flags) * 5

        # Green flags increase confidence
        confidence_score += len(self.green_flags) * 3

        # Clamp to 0-100
        confidence_score = max(0, min(100, confidence_score))

        # Map to levels
        if confidence_score >= 70:
            return 'High'
        elif confidence_score >= 50:
            return 'Medium-High'
        elif confidence_score >= 30:
            return 'Medium'
        elif confidence_score >= 15:
            return 'Medium-Low'
        else:
            return 'Low'

    def to_dict(self) -> Dict[str, Any]:
        return {
            'total_score': round(self.total, 1),
            'grade': self.grade,
            'recommendation': self.recommendation,
            'confidence': self.confidence,
            'breakdown': {
                'queue_position': round(self.queue_position, 1),
                'study_progress': round(self.study_progress, 1),
                'developer_track_record': round(self.developer_track_record, 1),
                'poi_congestion': round(self.poi_congestion, 1),
                'project_characteristics': round(self.project_characteristics, 1),
                'adjustments': round(self.adjustments, 1),
            },
            'adjustment_reasons': self.adjustment_reasons,
            'red_flags': self.red_flags,
            'green_flags': self.green_flags,
        }


class FeasibilityScorer:
    """
    Score projects on feasibility/likelihood of reaching COD.

    The scoring model is designed to be:
    - Transparent: Every point can be traced to a reason
    - Configurable: Weights can be adjusted
    - Extensible: Easy to add new scoring factors
    """

    # Scoring weights (sum to 100)
    WEIGHTS = {
        'queue_position': 25,
        'study_progress': 25,
        'developer_track_record': 20,
        'poi_congestion': 15,
        'project_characteristics': 15,
    }

    # Study phase scoring (normalized 0-1)
    # ISO-specific status codes mapped to standardized progress scores
    STUDY_PHASES = {
        # === UNIVERSAL PHASES (all ISOs) ===
        'ia executed': 1.0,
        'ia signed': 1.0,
        'interconnection agreement': 0.95,
        'under construction': 0.98,
        'in service': 1.0,
        'operational': 1.0,

        # Facilities Study phase
        'facilities study complete': 0.85,
        'facilities study': 0.75,
        'facility study': 0.75,

        # System Impact Study phase
        'system impact complete': 0.65,
        'system impact study': 0.55,
        'system impact': 0.55,

        # Feasibility Study phase
        'feasibility complete': 0.45,
        'feasibility study': 0.35,

        # Early phases
        'scoping': 0.25,
        'application': 0.15,
        'queue': 0.10,

        # === NYISO-SPECIFIC ===
        # NYISO uses: FES (Feasibility), SRIS (System Reliability Impact), FS (Facilities), IA
        'fs complete': 0.85,
        'fs': 0.75,
        'fs pending': 0.70,
        'sris complete': 0.65,
        'sris': 0.55,
        'sris pending': 0.50,
        'fes complete': 0.45,
        'fes': 0.35,
        'fes pending': 0.30,

        # === ERCOT-SPECIFIC ===
        # ERCOT uses: FIS (Full Interconnection Study), SS (Screening Study)
        'ia pending': 0.90,
        'fis complete': 0.80,
        'fis': 0.60,
        'fis pending': 0.55,
        'ss complete': 0.40,
        'ss': 0.30,
        'screening study': 0.30,

        # === MISO-SPECIFIC ===
        # MISO uses: DPP (Definitive Planning Phase), Phase 1/2/3
        'dpp phase 3': 0.85,
        'phase 3': 0.85,
        'dpp phase 2': 0.65,
        'phase 2': 0.65,
        'dpp phase 1': 0.45,
        'phase 1': 0.45,
        'dpp': 0.40,
        'definitional': 0.30,

        # === PJM-SPECIFIC ===
        # PJM uses: Feasibility, System Impact, Facilities, IA
        'facilities study agreement': 0.80,
        'system impact study agreement': 0.60,
        'feasibility study agreement': 0.40,
        'new services queue': 0.15,

        # === CAISO-SPECIFIC ===
        # CAISO uses: Cluster Study phases, Phase I/II
        'cluster phase ii': 0.70,
        'phase ii': 0.70,
        'cluster phase i': 0.45,
        'phase i': 0.45,
        'cluster study': 0.50,

        # === SPP-SPECIFIC ===
        # SPP uses: DISIS (Definitive Interconnection System Impact Study)
        'disis complete': 0.75,
        'disis': 0.55,
        'preliminary study': 0.35,

        # === ISO-NE-SPECIFIC ===
        # ISO-NE uses: System Impact Study, Feasibility Study, IA
        'sis agreement': 0.55,
        'fes agreement': 0.35,
    }

    # Project type scoring - based on LBL 2024 historical completion rates
    # Actual completion rates (LBL Queued Up 2024):
    #   Nuclear: 60%, Hydro: 48%, Gas: 32%, Storage: 30%, Wind: 21%, Solar: 14%
    # Scores are normalized for the 15-point project characteristics component
    TYPE_SCORES = {
        # Gas/thermal: 32% completion rate
        'natural gas': 0.65,
        'gas': 0.65,
        'ng': 0.65,
        'combined cycle': 0.65,
        'cc': 0.65,
        'peaker': 0.60,
        'ct': 0.60,
        # Storage: 30% completion rate
        'battery': 0.60,
        'storage': 0.60,
        'bess': 0.60,
        'es': 0.60,
        # Wind: 21% completion rate
        'wind': 0.50,
        'w': 0.50,
        'onshore wind': 0.50,
        'offshore wind': 0.35,  # Lower due to complexity
        'osw': 0.35,
        # Solar: 14% completion rate (lowest among major types)
        'solar': 0.45,
        's': 0.45,
        'pv': 0.45,
        # Hydro: 48% completion rate
        'hydro': 0.75,
        # Nuclear: 60% completion rate (highest, but rare)
        'nuclear': 0.80,
        # Other/unknown
        'other': 0.45,
    }

    def __init__(self, df: pd.DataFrame, region: str = 'NYISO'):
        """
        Initialize scorer with queue data.

        Args:
            df: DataFrame with queue data
            region: ISO/RTO region for regional benchmarks (NYISO, CAISO, ERCOT, PJM, MISO, etc.)
        """
        self.df = df
        self.region = region
        self.analyzer = QueueAnalyzer(df)
        self.benchmarks = RegionalBenchmarks()
        self._build_indices()

        # Get regional completion rate for context
        self.regional_completion_rate = self.benchmarks.get_completion_rate(region)
        self.regional_median_timeline = self.benchmarks.get_median_timeline(region)
        self.regional_cost_benchmark = self.benchmarks.get_cost_benchmark(region)

    def _build_indices(self):
        """Build indices for fast lookups."""
        # POI index: map POI -> list of projects
        poi_col = self.analyzer._get_col('poi')
        if poi_col:
            self.poi_index = self.df.groupby(poi_col).indices
        else:
            self.poi_index = {}

        # Developer index
        dev_col = self.analyzer._get_col('developer')
        if dev_col:
            self.dev_index = self.df.groupby(dev_col).indices
        else:
            self.dev_index = {}

    def _get_value(self, row: pd.Series, key: str) -> Any:
        """Get value from row using column mapping."""
        col = self.analyzer._get_col(key)
        if col and col in row.index:
            return row[col]
        return None

    def _score_queue_position(self, row: pd.Series, poi_projects: pd.DataFrame) -> Tuple[float, List[str]]:
        """
        Score based on queue position relative to other projects at same POI.

        Earlier position = higher score
        Fewer competitors = higher score
        """
        max_score = self.WEIGHTS['queue_position']
        notes = []

        if len(poi_projects) <= 1:
            notes.append("Only project at this POI")
            return max_score, notes

        # Get queue date or position
        date_col = self.analyzer._get_col('date')
        id_col = self.analyzer._get_col('id')

        if date_col:
            try:
                dates = pd.to_datetime(poi_projects[date_col], errors='coerce')
                project_date = pd.to_datetime(self._get_value(row, 'date'), errors='coerce')

                if pd.notna(project_date):
                    # Count projects that entered queue before this one
                    earlier = (dates < project_date).sum()
                    total = len(poi_projects)

                    # Score: earlier is better
                    position_pct = earlier / total
                    score = max_score * (1 - position_pct * 0.7)  # Even last position gets 30% of points

                    notes.append(f"Position {earlier + 1} of {total} at POI")
                    return score, notes
            except:
                pass

        # Fallback: assume middle position
        notes.append("Queue position unclear, assuming average")
        return max_score * 0.5, notes

    def _score_study_progress(self, row: pd.Series) -> Tuple[float, List[str]]:
        """
        Score based on how far through the interconnection study process.

        Later stage = higher score (more invested, more certainty)
        """
        max_score = self.WEIGHTS['study_progress']
        notes = []

        # Check status column
        status = self._get_value(row, 'status')
        if pd.isna(status):
            status = ""
        status_lower = str(status).lower()

        # Also check for dedicated phase/study columns
        for col in self.df.columns:
            if any(kw in col.lower() for kw in ['study', 'phase', 'stage', 'availability']):
                val = row.get(col)
                if pd.notna(val):
                    status_lower += " " + str(val).lower()

        # Find best matching phase
        best_score = 0.1  # Default: just in queue
        best_phase = "Unknown phase"

        for phase, score in self.STUDY_PHASES.items():
            if phase in status_lower:
                if score > best_score:
                    best_score = score
                    best_phase = phase.title()

        final_score = max_score * best_score
        notes.append(f"Study phase: {best_phase}")

        return final_score, notes

    def _score_developer_track_record(self, row: pd.Series) -> Tuple[float, List[str]]:
        """
        Score based on developer's historical track record using LBL data.

        Uses actual historical completion rates from the LBL Queued Up dataset
        to assess developer capability, rather than just counting current projects.
        """
        max_score = self.WEIGHTS['developer_track_record']
        notes = []

        developer = self._get_value(row, 'developer')

        if pd.isna(developer) or str(developer).strip() == '':
            notes.append("Developer unknown - cannot assess track record")
            return max_score * 0.4, notes

        developer_str = str(developer).strip()

        # Try to get actual historical track record from LBL data
        try:
            track_record = self.benchmarks.get_developer_track_record(developer_str, self.region)

            if 'error' not in track_record:
                summary = track_record.get('summary', {})
                assessment = track_record.get('assessment', {})
                confidence = track_record.get('confidence', 'unknown')

                total_projects = summary.get('total_projects', 0)
                completed = summary.get('completed', 0)
                completion_rate = summary.get('completion_rate', 0)
                assessment_level = assessment.get('level', 'unknown')

                # Score based on actual completion rate
                # Excellent (40%+): 90-100% of max score
                # Good (25-40%): 75-90% of max score
                # Average (15-25%): 55-75% of max score
                # Below average (5-15%): 35-55% of max score
                # Poor (<5%): 20-35% of max score
                # No completions: 15-20% of max score

                if assessment_level == 'excellent':
                    score = max_score * (0.90 + (completion_rate - 0.40) * 0.25)  # 90-100%
                    notes.append(f"Excellent track record: {completed}/{total_projects} completed ({completion_rate*100:.0f}%)")
                elif assessment_level == 'good':
                    score = max_score * (0.75 + (completion_rate - 0.25) / 0.15 * 0.15)  # 75-90%
                    notes.append(f"Good track record: {completed}/{total_projects} completed ({completion_rate*100:.0f}%)")
                elif assessment_level == 'average':
                    score = max_score * (0.55 + (completion_rate - 0.15) / 0.10 * 0.20)  # 55-75%
                    notes.append(f"Average track record: {completed}/{total_projects} completed ({completion_rate*100:.0f}%)")
                elif assessment_level == 'below_average':
                    score = max_score * (0.35 + (completion_rate - 0.05) / 0.10 * 0.20)  # 35-55%
                    notes.append(f"Below-average track record: {completed}/{total_projects} completed ({completion_rate*100:.0f}%)")
                elif assessment_level == 'poor':
                    score = max_score * (0.20 + completion_rate / 0.05 * 0.15)  # 20-35%
                    notes.append(f"Poor track record: {completed}/{total_projects} completed ({completion_rate*100:.0f}%)")
                elif assessment_level == 'no_completions':
                    score = max_score * 0.15  # 15%
                    notes.append(f"WARNING: {total_projects} projects, ZERO completions")
                else:
                    score = max_score * 0.5
                    notes.append(f"Track record: {completed}/{total_projects} completed")

                # Adjust for confidence level
                if confidence == 'very_low':
                    notes.append(f"Low confidence (only {total_projects} historical projects)")
                    # Regress toward average for low sample sizes
                    score = score * 0.7 + max_score * 0.5 * 0.3
                elif confidence == 'low':
                    notes.append(f"Limited sample ({total_projects} projects)")
                    score = score * 0.85 + max_score * 0.5 * 0.15

                return min(max_score, max(0, score)), notes

        except Exception as e:
            # Fall back to queue-based counting if LBL lookup fails
            pass

        # Fallback: Count developer's projects in this queue (less reliable)
        dev_col = self.analyzer._get_col('developer')
        if dev_col:
            dev_projects = self.df[self.df[dev_col].astype(str).str.strip() == developer_str]
            project_count = len(dev_projects)

            if project_count > 10:
                notes.append(f"No historical data; {project_count} active projects (appears experienced)")
                score = max_score * 0.65
            elif project_count > 5:
                notes.append(f"No historical data; {project_count} active projects")
                score = max_score * 0.55
            elif project_count > 1:
                notes.append(f"No historical data; {project_count} active projects")
                score = max_score * 0.50
            else:
                notes.append("No historical data; single project developer")
                score = max_score * 0.45

            return score, notes

        # Final fallback
        notes.append("Developer track record unknown")
        return max_score * 0.45, notes

    def _score_poi_congestion(self, row: pd.Series, poi_projects: pd.DataFrame) -> Tuple[float, List[str]]:
        """
        Score based on congestion at the POI.

        Less congestion = higher score (less competition, faster processing)
        """
        max_score = self.WEIGHTS['poi_congestion']
        notes = []

        num_projects = len(poi_projects)

        # Get total capacity at POI
        cap_col = self.analyzer._get_col('capacity')
        if cap_col:
            total_cap = pd.to_numeric(poi_projects[cap_col], errors='coerce').sum()
        else:
            total_cap = None

        if num_projects == 1:
            notes.append("Only project at this POI - no congestion")
            return max_score, notes
        elif num_projects <= 3:
            notes.append(f"Low congestion: {num_projects} projects at POI")
            score = max_score * 0.85
        elif num_projects <= 7:
            notes.append(f"Moderate congestion: {num_projects} projects at POI")
            score = max_score * 0.6
        elif num_projects <= 15:
            notes.append(f"High congestion: {num_projects} projects at POI")
            score = max_score * 0.35
        else:
            notes.append(f"Very high congestion: {num_projects} projects at POI")
            score = max_score * 0.15

        if total_cap:
            notes.append(f"Total capacity at POI: {total_cap:,.0f} MW")

        return score, notes

    def _score_project_characteristics(self, row: pd.Series) -> Tuple[float, List[str]]:
        """
        Score based on project type, size, and regional characteristics.

        Uses regional benchmarks for more accurate completion rate estimates.
        """
        max_score = self.WEIGHTS['project_characteristics']
        notes = []

        # Project type
        proj_type = self._get_value(row, 'type')
        type_score = 0.5  # default

        if pd.notna(proj_type):
            type_str = str(proj_type)
            type_lower = type_str.lower().strip()

            # Get regional completion rate adjusted for project type
            completion_rate = self.benchmarks.get_completion_rate(self.region, type_str)

            # Convert completion rate to score (higher rate = higher score)
            # Normalize: 0% -> 0.3, 35% -> 0.8 (top performers)
            type_score = 0.3 + (completion_rate / 0.35) * 0.5
            type_score = min(0.85, max(0.3, type_score))  # Clamp to 0.3-0.85

            notes.append(f"Project type: {proj_type} in {self.region}")
            notes.append(f"Regional completion rate: {completion_rate*100:.1f}%")

            # Fallback to static scores for unknown types
            if completion_rate == 0.14:  # Default rate, type not recognized
                for type_key, score in self.TYPE_SCORES.items():
                    if type_key in type_lower or type_lower == type_key:
                        type_score = score
                        break
        else:
            notes.append("Project type unknown")
            notes.append(f"{self.region} regional completion rate: {self.regional_completion_rate*100:.1f}%")

        # Capacity - moderate size is better (not too speculative, not too large)
        capacity = self._get_value(row, 'capacity')
        cap_factor = 0.5

        if pd.notna(capacity):
            try:
                cap_mw = float(capacity)
                if 10 <= cap_mw <= 500:
                    cap_factor = 0.8
                    notes.append(f"Capacity {cap_mw:.0f} MW (reasonable size)")
                elif cap_mw < 10:
                    cap_factor = 0.6
                    notes.append(f"Small project: {cap_mw:.0f} MW")
                elif cap_mw <= 1000:
                    cap_factor = 0.7
                    notes.append(f"Large project: {cap_mw:.0f} MW")
                else:
                    cap_factor = 0.5
                    notes.append(f"Very large project: {cap_mw:.0f} MW (execution risk)")
            except:
                pass

        # Combine factors
        final_score = max_score * (type_score * 0.7 + cap_factor * 0.3)

        return final_score, notes

    def _identify_flags(self, row: pd.Series, score: ScoreBreakdown,
                        poi_projects: pd.DataFrame) -> None:
        """Identify red and green flags for the project."""

        # RED FLAGS

        # Very late queue position
        if score.queue_position < self.WEIGHTS['queue_position'] * 0.3:
            score.red_flags.append("Late queue position at congested POI")

        # Very early study phase
        if score.study_progress < self.WEIGHTS['study_progress'] * 0.3:
            score.red_flags.append("Early study phase - high uncertainty")

        # Heavily congested POI
        if len(poi_projects) > 10:
            score.red_flags.append(f"Highly congested POI ({len(poi_projects)} projects)")

        # Check for withdrawn pattern at POI
        status_col = self.analyzer._get_col('status')
        if status_col:
            withdrawn_kw = ['withdrawn', 'cancelled', 'suspended']
            withdrawn_at_poi = poi_projects[
                poi_projects[status_col].astype(str).str.lower().str.contains('|'.join(withdrawn_kw), na=False)
            ]
            if len(withdrawn_at_poi) > len(poi_projects) * 0.5:
                score.red_flags.append(f"High withdrawal rate at POI ({len(withdrawn_at_poi)} of {len(poi_projects)})")

        # Check queue age - extended time in queue is a major risk indicator
        queue_date = self._get_value(row, 'date')
        if pd.notna(queue_date):
            try:
                queue_dt = pd.to_datetime(queue_date)
                days_in_queue = (datetime.now() - queue_dt).days
                years_in_queue = days_in_queue / 365.25

                if years_in_queue >= 7:
                    score.red_flags.append(f"CRITICAL: Project in queue {years_in_queue:.1f} years - severe execution risk")
                    # Also apply score penalty for extremely old projects
                    score.adjustments -= 10
                    score.adjustment_reasons.append(f"Extended queue time penalty ({years_in_queue:.1f} years)")
                elif years_in_queue >= 5:
                    score.red_flags.append(f"Project in queue {years_in_queue:.1f} years - significant timeline risk")
                    score.adjustments -= 5
                    score.adjustment_reasons.append(f"Queue age penalty ({years_in_queue:.1f} years)")
                elif years_in_queue >= 3:
                    score.red_flags.append(f"Project in queue {years_in_queue:.1f} years - may face execution challenges")
            except:
                pass  # Skip if date parsing fails

        # Poor developer track record (score-based flag)
        if score.developer_track_record < self.WEIGHTS['developer_track_record'] * 0.35:
            score.red_flags.append("Developer has poor or no historical track record")
        elif score.developer_track_record < self.WEIGHTS['developer_track_record'] * 0.5:
            score.red_flags.append("Developer track record below average")

        # GREEN FLAGS

        # Strong queue position
        if score.queue_position > self.WEIGHTS['queue_position'] * 0.8:
            score.green_flags.append("Strong queue position")

        # Advanced study phase
        if score.study_progress > self.WEIGHTS['study_progress'] * 0.8:
            score.green_flags.append("Advanced in study process")

        # Low POI congestion
        if len(poi_projects) <= 3:
            score.green_flags.append("Low POI congestion")

        # Experienced developer with good track record
        if score.developer_track_record > self.WEIGHTS['developer_track_record'] * 0.8:
            score.green_flags.append("Developer has excellent historical track record")
        elif score.developer_track_record > self.WEIGHTS['developer_track_record'] * 0.7:
            score.green_flags.append("Developer has above-average track record")

        # Check for PPA/offtake (major positive indicator)
        has_ppa = row.get('has_ppa') if 'has_ppa' in row.index else None
        if has_ppa == True or has_ppa == 1:
            score.green_flags.append("Project has confirmed PPA/offtake agreement")
            # Bonus for having committed offtake
            score.adjustments += 5
            score.adjustment_reasons.append("PPA/offtake bonus (+5 pts)")

    def score_project(self, project_id: Optional[str] = None,
                      row: Optional[pd.Series] = None) -> Dict[str, Any]:
        """
        Score a single project.

        Args:
            project_id: Queue ID to look up
            row: Or pass a row directly

        Returns:
            Dictionary with score breakdown and recommendation
        """
        # Get the project row
        if row is None:
            if project_id is None:
                raise ValueError("Must provide project_id or row")

            results = self.analyzer.search(queue_id=project_id)
            if len(results) == 0:
                return {"error": f"Project not found: {project_id}"}
            row = results.iloc[0]

        # Get projects at same POI
        poi = self._get_value(row, 'poi')
        if pd.notna(poi):
            poi_projects = self.analyzer.search(poi=str(poi))
        else:
            poi_projects = pd.DataFrame([row])

        # Calculate sub-scores
        score = ScoreBreakdown()
        all_notes = []

        # Queue position
        pts, notes = self._score_queue_position(row, poi_projects)
        score.queue_position = pts
        all_notes.extend(notes)

        # Study progress
        pts, notes = self._score_study_progress(row)
        score.study_progress = pts
        all_notes.extend(notes)

        # Developer track record
        pts, notes = self._score_developer_track_record(row)
        score.developer_track_record = pts
        all_notes.extend(notes)

        # POI congestion
        pts, notes = self._score_poi_congestion(row, poi_projects)
        score.poi_congestion = pts
        all_notes.extend(notes)

        # Project characteristics
        pts, notes = self._score_project_characteristics(row)
        score.project_characteristics = pts
        all_notes.extend(notes)

        # Identify flags
        self._identify_flags(row, score, poi_projects)

        # Build result
        result = score.to_dict()
        result['scoring_notes'] = all_notes

        # Add project info
        result['project'] = {
            'id': self._get_value(row, 'id'),
            'name': self._get_value(row, 'name'),
            'developer': self._get_value(row, 'developer'),
            'capacity_mw': self._get_value(row, 'capacity'),
            'type': self._get_value(row, 'type'),
            'state': self._get_value(row, 'state'),
            'poi': poi,
        }

        # Add regional benchmarks for context
        proj_type = self._get_value(row, 'type')
        result['regional_context'] = {
            'region': self.region,
            'regional_completion_rate': self.regional_completion_rate,
            'type_adjusted_completion_rate': self.benchmarks.get_completion_rate(
                self.region, str(proj_type) if pd.notna(proj_type) else None
            ),
            'median_timeline_months': self.regional_median_timeline,
            'cost_benchmark_per_kw': self.regional_cost_benchmark,
        }

        # =====================================================================
        # ENHANCED ANALYSIS (from LBL historical data)
        # =====================================================================
        result['enhanced_analysis'] = {}

        # 1. Actual completion rate from LBL historical data
        try:
            actual_rate = self.benchmarks.get_actual_completion_rate(
                self.region,
                str(proj_type) if pd.notna(proj_type) else None
            )
            result['enhanced_analysis']['completion_rate'] = {
                'rate': actual_rate.get('rate', 0),
                'rate_pct': actual_rate.get('rate_pct', 'N/A'),
                'sample_size': actual_rate.get('sample_size', 0),
                'confidence': actual_rate.get('confidence', 'unknown'),
            }
        except Exception as e:
            result['enhanced_analysis']['completion_rate'] = {'error': str(e)}

        # 2. Developer historical track record
        developer = self._get_value(row, 'developer')
        if pd.notna(developer) and str(developer).strip():
            try:
                dev_record = self.benchmarks.get_developer_track_record(str(developer), self.region)
                if 'error' not in dev_record:
                    result['enhanced_analysis']['developer_track_record'] = {
                        'total_projects': dev_record['summary']['total_projects'],
                        'completed': dev_record['summary']['completed'],
                        'completion_rate': dev_record['summary']['completion_rate'],
                        'completion_rate_pct': dev_record['summary']['completion_rate_pct'],
                        'assessment': dev_record['assessment']['level'],
                        'assessment_text': dev_record['assessment']['text'],
                        'confidence': dev_record['confidence'],
                        'interpretation': dev_record['interpretation'],
                    }
                else:
                    result['enhanced_analysis']['developer_track_record'] = {
                        'error': dev_record.get('error', 'Unknown developer')
                    }
            except Exception as e:
                result['enhanced_analysis']['developer_track_record'] = {'error': str(e)}
        else:
            result['enhanced_analysis']['developer_track_record'] = {'error': 'Developer unknown'}

        # 3. POI historical analysis
        if pd.notna(poi) and str(poi).strip():
            try:
                poi_history = self.benchmarks.get_poi_history(str(poi))
                if 'error' not in poi_history:
                    result['enhanced_analysis']['poi_history'] = {
                        'total_projects': poi_history['summary']['total_projects'],
                        'completed': poi_history['summary']['operational'],
                        'withdrawn': poi_history['summary']['withdrawn'],
                        'completion_rate': poi_history['rates']['completion_rate'],
                        'completion_rate_pct': poi_history['rates']['completion_rate_pct'],
                        'risk_level': poi_history['risk_assessment']['level'],
                        'risk_interpretation': poi_history['risk_assessment']['interpretation'],
                        'confidence': poi_history['confidence'],
                    }
                else:
                    result['enhanced_analysis']['poi_history'] = {
                        'note': 'No historical data for this POI'
                    }
            except Exception as e:
                result['enhanced_analysis']['poi_history'] = {'error': str(e)}
        else:
            result['enhanced_analysis']['poi_history'] = {'note': 'POI not specified'}

        # 4. Timeline prediction
        queue_date = self._get_value(row, 'date')
        if pd.notna(queue_date):
            try:
                queue_date_str = pd.to_datetime(queue_date).strftime('%Y-%m-%d')
                timeline = self.benchmarks.get_timeline_prediction(
                    self.region,
                    str(proj_type) if pd.notna(proj_type) else None,
                    queue_entry_date=queue_date_str
                )
                if 'error' not in timeline:
                    result['enhanced_analysis']['timeline_prediction'] = {
                        'p50_months': timeline['timeline_months']['p50'],
                        'p75_months': timeline['timeline_months']['p75'],
                        'p90_months': timeline['timeline_months']['p90'],
                        'sample_size': timeline['sample_size'],
                        'confidence': timeline['confidence'],
                        'cod_estimates': timeline.get('cod_estimates', {}),
                    }
                else:
                    result['enhanced_analysis']['timeline_prediction'] = {
                        'fallback_months': timeline.get('fallback', 48)
                    }
            except Exception as e:
                result['enhanced_analysis']['timeline_prediction'] = {'error': str(e)}
        else:
            result['enhanced_analysis']['timeline_prediction'] = {'note': 'Queue date not available'}

        return result

    def rank_projects(self,
                      fuel_type: Optional[str] = None,
                      state: Optional[str] = None,
                      min_capacity: Optional[float] = None,
                      max_capacity: Optional[float] = None,
                      min_score: Optional[float] = None,
                      limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Score and rank all projects matching criteria.

        Returns list of scored projects, sorted by score descending.
        """
        # Filter projects
        filtered = self.analyzer.search(
            fuel_type=fuel_type,
            state=state,
            min_mw=min_capacity,
            max_mw=max_capacity,
        )

        if len(filtered) == 0:
            return []

        print(f"Scoring {len(filtered)} projects...")

        # Score each project
        results = []
        for idx, (_, row) in enumerate(filtered.iterrows()):
            if idx % 50 == 0 and idx > 0:
                print(f"  Scored {idx}/{len(filtered)}...")

            try:
                score_result = self.score_project(row=row)
                if 'error' not in score_result:
                    results.append(score_result)
            except Exception as e:
                # Skip projects that error
                continue

        # Sort by score
        results.sort(key=lambda x: x['total_score'], reverse=True)

        # Apply min score filter
        if min_score is not None:
            results = [r for r in results if r['total_score'] >= min_score]

        # Apply limit
        if limit is not None:
            results = results[:limit]

        print(f"Ranked {len(results)} projects")

        return results


def print_score(result: Dict[str, Any]):
    """Pretty print a score result."""
    if 'error' in result:
        print(f"Error: {result['error']}")
        return

    proj = result['project']

    print("\n" + "=" * 60)
    print("PROJECT FEASIBILITY SCORE")
    print("=" * 60)

    # Project info
    print(f"\nProject: {proj.get('name', 'Unknown')}")
    print(f"ID: {proj.get('id', 'Unknown')}")
    print(f"Developer: {proj.get('developer', 'Unknown')}")
    print(f"Type: {proj.get('type', 'Unknown')} | Capacity: {proj.get('capacity_mw', '?')} MW")
    print(f"POI: {proj.get('poi', 'Unknown')}")

    # Score
    print("\n" + "-" * 60)
    print(f"TOTAL SCORE: {result['total_score']:.0f}/100 (Grade: {result['grade']})")
    print(f"RECOMMENDATION: {result['recommendation']}")
    print("-" * 60)

    # Breakdown
    breakdown = result['breakdown']
    print("\nScore Breakdown:")
    print(f"  Queue Position:      {breakdown['queue_position']:5.1f}/25")
    print(f"  Study Progress:      {breakdown['study_progress']:5.1f}/25")
    print(f"  Developer Record:    {breakdown['developer_track_record']:5.1f}/20")
    print(f"  POI Congestion:      {breakdown['poi_congestion']:5.1f}/15")
    print(f"  Project Type/Size:   {breakdown['project_characteristics']:5.1f}/15")
    if breakdown['adjustments'] != 0:
        print(f"  Adjustments:         {breakdown['adjustments']:+5.1f}")

    # Flags
    if result['red_flags']:
        print("\nRED FLAGS:")
        for flag in result['red_flags']:
            print(f"  ! {flag}")

    if result['green_flags']:
        print("\nGREEN FLAGS:")
        for flag in result['green_flags']:
            print(f"  + {flag}")

    # Notes
    if result.get('scoring_notes'):
        print("\nScoring Notes:")
        for note in result['scoring_notes']:
            print(f"  - {note}")


def print_rankings(results: List[Dict[str, Any]], limit: int = 20):
    """Pretty print ranking results."""
    print("\n" + "=" * 80)
    print("PROJECT RANKINGS")
    print("=" * 80)

    print(f"\n{'Rank':<5} {'Score':<7} {'Grade':<6} {'Rec':<12} {'ID':<12} {'Type':<8} {'MW':<8} {'Project Name':<30}")
    print("-" * 100)

    for i, r in enumerate(results[:limit], 1):
        proj = r['project']

        # Format values
        score = f"{r['total_score']:.0f}"
        grade = r['grade']
        rec = r['recommendation']
        proj_id = str(proj.get('id', '?'))[:10]
        proj_type = str(proj.get('type', '?'))[:6]
        capacity = proj.get('capacity_mw', 0)
        try:
            cap_str = f"{float(capacity):.0f}"
        except:
            cap_str = "?"
        name = str(proj.get('name', 'Unknown'))[:28]

        # Color coding for recommendation
        if rec == 'GO':
            rec_display = f"GO"
        elif rec == 'CONDITIONAL':
            rec_display = f"COND"
        else:
            rec_display = f"NO-GO"

        print(f"{i:<5} {score:<7} {grade:<6} {rec_display:<12} {proj_id:<12} {proj_type:<8} {cap_str:<8} {name:<30}")

        # Show flags for top projects
        if i <= 5:
            if r['red_flags']:
                print(f"      Red flags: {', '.join(r['red_flags'][:2])}")
            if r['green_flags']:
                print(f"      Green flags: {', '.join(r['green_flags'][:2])}")

    if len(results) > limit:
        print(f"\n... and {len(results) - limit} more projects")

    # Summary stats
    print("\n" + "-" * 80)
    scores = [r['total_score'] for r in results]
    go_count = len([r for r in results if r['recommendation'] == 'GO'])
    cond_count = len([r for r in results if r['recommendation'] == 'CONDITIONAL'])
    nogo_count = len([r for r in results if r['recommendation'] == 'NO-GO'])

    print(f"Summary: {len(results)} projects scored")
    print(f"  Avg Score: {np.mean(scores):.1f} | Median: {np.median(scores):.1f} | Range: {min(scores):.0f}-{max(scores):.0f}")
    print(f"  GO: {go_count} | CONDITIONAL: {cond_count} | NO-GO: {nogo_count}")


def main():
    parser = argparse.ArgumentParser(
        description="Project Feasibility Scoring Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 scoring.py --score 0276               # Score single project
    python3 scoring.py --rank                     # Rank all projects
    python3 scoring.py --rank --type Solar        # Rank solar projects
    python3 scoring.py --rank --min-score 60      # Only show score >= 60
    python3 scoring.py --rank --export top.csv    # Export rankings

    # With local file
    python3 scoring.py --file queue.xlsx --rank
        """
    )

    # Data source
    parser.add_argument('--file', '-f', help='Local Excel/CSV file')
    parser.add_argument('--refresh', action='store_true', help='Force refresh data')

    # Actions
    parser.add_argument('--score', metavar='ID', help='Score single project by ID')
    parser.add_argument('--rank', action='store_true', help='Rank all projects')

    # Filters
    parser.add_argument('--type', help='Filter by fuel/project type')
    parser.add_argument('--state', help='Filter by state')
    parser.add_argument('--min-mw', type=float, help='Minimum capacity')
    parser.add_argument('--max-mw', type=float, help='Maximum capacity')
    parser.add_argument('--min-score', type=float, help='Minimum score to include')

    # Output
    parser.add_argument('--limit', type=int, default=20, help='Max results')
    parser.add_argument('--export', help='Export to CSV')
    parser.add_argument('--json', action='store_true', help='Output as JSON')

    args = parser.parse_args()

    # Load data
    loader = QueueData()

    if args.file:
        df = loader.load_file(args.file)
    else:
        df = loader.load_nyiso(force_refresh=args.refresh)

    if df.empty:
        print("No data loaded.")
        return 1

    # Initialize scorer
    scorer = FeasibilityScorer(df)

    # Score single project
    if args.score:
        result = scorer.score_project(project_id=args.score)

        if args.json:
            print(json.dumps(result, indent=2, default=str))
        else:
            print_score(result)

        return 0

    # Rank projects
    if args.rank:
        results = scorer.rank_projects(
            fuel_type=args.type,
            state=args.state,
            min_capacity=args.min_mw,
            max_capacity=args.max_mw,
            min_score=args.min_score,
            limit=args.limit if not args.export else None,
        )

        if not results:
            print("No projects matched criteria.")
            return 1

        if args.json:
            print(json.dumps(results[:args.limit], indent=2, default=str))
        else:
            print_rankings(results, args.limit)

        if args.export:
            # Export to CSV
            export_data = []
            for r in results:
                proj = r['project']
                export_data.append({
                    'score': r['total_score'],
                    'grade': r['grade'],
                    'recommendation': r['recommendation'],
                    'id': proj.get('id'),
                    'name': proj.get('name'),
                    'developer': proj.get('developer'),
                    'type': proj.get('type'),
                    'capacity_mw': proj.get('capacity_mw'),
                    'state': proj.get('state'),
                    'poi': proj.get('poi'),
                    'queue_position_score': r['breakdown']['queue_position'],
                    'study_progress_score': r['breakdown']['study_progress'],
                    'developer_score': r['breakdown']['developer_track_record'],
                    'poi_congestion_score': r['breakdown']['poi_congestion'],
                    'characteristics_score': r['breakdown']['project_characteristics'],
                    'red_flags': '; '.join(r['red_flags']),
                    'green_flags': '; '.join(r['green_flags']),
                })

            export_df = pd.DataFrame(export_data)
            export_df.to_csv(args.export, index=False)
            print(f"\nExported {len(export_data)} projects to: {args.export}")

        return 0

    # Default: show help
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
