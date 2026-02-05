"""
Interconnection Queue Analysis Toolkit

Tools for analyzing RTO interconnection queue data for project feasibility assessments.
Uses GridStatus library for data access.

Requirements:
    pip install gridstatus pandas numpy
"""

import gridstatus
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import warnings
warnings.filterwarnings('ignore')


class QueueAnalyzer:
    """Main class for interconnection queue analysis."""

    # Supported ISOs
    SUPPORTED_ISOS = ['PJM', 'ERCOT', 'MISO', 'CAISO', 'NYISO', 'ISONE', 'SPP']

    # Status categories
    ACTIVE_STATUSES = ['Active', 'active', 'In Progress', 'Pending', 'Under Study']
    WITHDRAWN_STATUSES = ['Withdrawn', 'withdrawn', 'Cancelled', 'Suspended']
    COMPLETED_STATUSES = ['Completed', 'completed', 'Operational', 'In Service', 'Commercial Operation']

    def __init__(self, iso: str = 'PJM'):
        """
        Initialize analyzer for a specific ISO.

        Args:
            iso: ISO name (PJM, ERCOT, MISO, CAISO, NYISO, ISONE, SPP)
        """
        self.iso_name = iso.upper()
        if self.iso_name not in self.SUPPORTED_ISOS:
            raise ValueError(f"ISO must be one of: {self.SUPPORTED_ISOS}")

        self.iso = getattr(gridstatus, self.iso_name)()
        self._queue_cache = None
        self._cache_time = None
        self._cache_duration = timedelta(hours=1)

    def get_queue(self, force_refresh: bool = False) -> pd.DataFrame:
        """
        Get the interconnection queue data.

        Args:
            force_refresh: Force refresh from API even if cached

        Returns:
            DataFrame with queue data
        """
        now = datetime.now()

        if (self._queue_cache is None or
            force_refresh or
            (self._cache_time and now - self._cache_time > self._cache_duration)):

            print(f"Fetching {self.iso_name} interconnection queue...")
            self._queue_cache = self.iso.get_interconnection_queue()
            self._cache_time = now
            print(f"Loaded {len(self._queue_cache)} projects")

        return self._queue_cache.copy()

    def find_project(self,
                     queue_id: Optional[str] = None,
                     project_name: Optional[str] = None,
                     developer: Optional[str] = None) -> pd.DataFrame:
        """
        Find projects matching search criteria.

        Args:
            queue_id: Queue ID to search for (exact or partial match)
            project_name: Project name to search for (partial match)
            developer: Developer name to search for (partial match)

        Returns:
            DataFrame with matching projects
        """
        queue = self.get_queue()
        mask = pd.Series([True] * len(queue))

        # Find queue ID column
        id_cols = [c for c in queue.columns if 'queue' in c.lower() or 'id' in c.lower() or 'request' in c.lower()]

        if queue_id and id_cols:
            id_col = id_cols[0]
            mask &= queue[id_col].astype(str).str.contains(str(queue_id), case=False, na=False)

        # Find name column
        name_cols = [c for c in queue.columns if 'name' in c.lower() and 'owner' not in c.lower()]

        if project_name and name_cols:
            name_col = name_cols[0]
            mask &= queue[name_col].astype(str).str.contains(project_name, case=False, na=False)

        # Find developer column
        dev_cols = [c for c in queue.columns if any(x in c.lower() for x in ['developer', 'owner', 'entity', 'applicant'])]

        if developer and dev_cols:
            dev_col = dev_cols[0]
            mask &= queue[dev_col].astype(str).str.contains(developer, case=False, na=False)

        return queue[mask]

    def get_poi_analysis(self, poi_name: str) -> Dict[str, Any]:
        """
        Analyze all projects at a specific Point of Interconnection.

        Args:
            poi_name: Name of the POI/substation (partial match)

        Returns:
            Dictionary with POI analysis
        """
        queue = self.get_queue()

        # Find POI column
        poi_cols = [c for c in queue.columns if any(x in c.lower() for x in ['poi', 'substation', 'interconnection', 'point'])]

        if not poi_cols:
            return {"error": "Could not find POI column in queue data"}

        poi_col = poi_cols[0]

        # Find matching projects
        mask = queue[poi_col].astype(str).str.contains(poi_name, case=False, na=False)
        poi_projects = queue[mask]

        if len(poi_projects) == 0:
            return {"error": f"No projects found at POI matching '{poi_name}'"}

        # Find capacity column
        cap_cols = [c for c in queue.columns if any(x in c.lower() for x in ['capacity', 'mw', 'size'])]
        cap_col = cap_cols[0] if cap_cols else None

        # Find status column
        status_cols = [c for c in queue.columns if 'status' in c.lower()]
        status_col = status_cols[0] if status_cols else None

        # Calculate metrics
        analysis = {
            "poi_name": poi_name,
            "total_projects": len(poi_projects),
            "projects": poi_projects,
        }

        if cap_col:
            analysis["total_capacity_mw"] = poi_projects[cap_col].sum()
            analysis["avg_capacity_mw"] = poi_projects[cap_col].mean()

        if status_col:
            analysis["status_breakdown"] = poi_projects[status_col].value_counts().to_dict()

            # Count by status category
            active = poi_projects[poi_projects[status_col].isin(self.ACTIVE_STATUSES)]
            analysis["active_projects"] = len(active)
            if cap_col:
                analysis["active_capacity_mw"] = active[cap_col].sum()

        return analysis

    def get_queue_statistics(self,
                             fuel_type: Optional[str] = None,
                             state: Optional[str] = None) -> Dict[str, Any]:
        """
        Get summary statistics for the queue.

        Args:
            fuel_type: Filter by fuel type (Solar, Wind, Battery, etc.)
            state: Filter by state

        Returns:
            Dictionary with queue statistics
        """
        queue = self.get_queue()

        # Apply filters
        if fuel_type:
            fuel_cols = [c for c in queue.columns if any(x in c.lower() for x in ['fuel', 'type', 'generation', 'resource'])]
            if fuel_cols:
                mask = queue[fuel_cols[0]].astype(str).str.contains(fuel_type, case=False, na=False)
                queue = queue[mask]

        if state:
            state_cols = [c for c in queue.columns if 'state' in c.lower()]
            if state_cols:
                mask = queue[state_cols[0]].astype(str).str.contains(state, case=False, na=False)
                queue = queue[mask]

        # Find key columns
        cap_cols = [c for c in queue.columns if any(x in c.lower() for x in ['capacity', 'mw'])]
        status_cols = [c for c in queue.columns if 'status' in c.lower()]
        date_cols = [c for c in queue.columns if any(x in c.lower() for x in ['date', 'queue'])]

        stats = {
            "iso": self.iso_name,
            "total_projects": len(queue),
            "filters_applied": {"fuel_type": fuel_type, "state": state}
        }

        if cap_cols:
            cap_col = cap_cols[0]
            stats["total_capacity_mw"] = queue[cap_col].sum()
            stats["avg_capacity_mw"] = queue[cap_col].mean()
            stats["median_capacity_mw"] = queue[cap_col].median()
            stats["max_capacity_mw"] = queue[cap_col].max()

        if status_cols:
            stats["status_breakdown"] = queue[status_cols[0]].value_counts().to_dict()

        # Fuel type breakdown
        fuel_cols = [c for c in queue.columns if any(x in c.lower() for x in ['fuel', 'type', 'generation'])]
        if fuel_cols:
            stats["fuel_type_breakdown"] = queue[fuel_cols[0]].value_counts().head(10).to_dict()

        return stats

    def calculate_completion_rate(self,
                                  years_back: int = 5,
                                  fuel_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Calculate historical completion/withdrawal rates.

        Args:
            years_back: Number of years of history to analyze
            fuel_type: Filter by fuel type

        Returns:
            Dictionary with completion rate analysis
        """
        queue = self.get_queue()

        # Find status column
        status_cols = [c for c in queue.columns if 'status' in c.lower()]
        if not status_cols:
            return {"error": "Could not find status column"}

        status_col = status_cols[0]

        # Apply fuel filter
        if fuel_type:
            fuel_cols = [c for c in queue.columns if any(x in c.lower() for x in ['fuel', 'type', 'generation'])]
            if fuel_cols:
                mask = queue[fuel_cols[0]].astype(str).str.contains(fuel_type, case=False, na=False)
                queue = queue[mask]

        # Count by status category
        total = len(queue)

        active = len(queue[queue[status_col].astype(str).str.lower().isin([s.lower() for s in self.ACTIVE_STATUSES])])
        withdrawn = len(queue[queue[status_col].astype(str).str.lower().isin([s.lower() for s in self.WITHDRAWN_STATUSES])])
        completed = len(queue[queue[status_col].astype(str).str.lower().isin([s.lower() for s in self.COMPLETED_STATUSES])])
        other = total - active - withdrawn - completed

        # Calculate rates (for non-active projects)
        resolved = completed + withdrawn

        return {
            "total_projects": total,
            "active": active,
            "withdrawn": withdrawn,
            "completed": completed,
            "other": other,
            "completion_rate": completed / resolved * 100 if resolved > 0 else None,
            "withdrawal_rate": withdrawn / resolved * 100 if resolved > 0 else None,
            "fuel_type_filter": fuel_type
        }

    def find_similar_projects(self,
                              capacity_mw: float,
                              fuel_type: str,
                              state: Optional[str] = None,
                              tolerance_pct: float = 0.25) -> pd.DataFrame:
        """
        Find similar projects for benchmarking.

        Args:
            capacity_mw: Target capacity in MW
            fuel_type: Fuel/project type
            state: State to filter by
            tolerance_pct: Capacity tolerance (e.g., 0.25 = +/- 25%)

        Returns:
            DataFrame with similar projects
        """
        queue = self.get_queue()

        # Find relevant columns
        cap_cols = [c for c in queue.columns if any(x in c.lower() for x in ['capacity', 'mw'])]
        fuel_cols = [c for c in queue.columns if any(x in c.lower() for x in ['fuel', 'type', 'generation'])]
        state_cols = [c for c in queue.columns if 'state' in c.lower()]

        mask = pd.Series([True] * len(queue))

        # Capacity filter
        if cap_cols:
            cap_col = cap_cols[0]
            min_cap = capacity_mw * (1 - tolerance_pct)
            max_cap = capacity_mw * (1 + tolerance_pct)
            mask &= (queue[cap_col] >= min_cap) & (queue[cap_col] <= max_cap)

        # Fuel type filter
        if fuel_cols:
            mask &= queue[fuel_cols[0]].astype(str).str.contains(fuel_type, case=False, na=False)

        # State filter
        if state and state_cols:
            mask &= queue[state_cols[0]].astype(str).str.contains(state, case=False, na=False)

        return queue[mask]

    def export_analysis(self,
                        queue_id: str,
                        output_path: str) -> str:
        """
        Export a full project analysis to a file.

        Args:
            queue_id: Queue ID to analyze
            output_path: Path to save the analysis

        Returns:
            Path to the saved file
        """
        project = self.find_project(queue_id=queue_id)

        if len(project) == 0:
            raise ValueError(f"No project found with queue ID: {queue_id}")

        if len(project) > 1:
            print(f"Warning: Found {len(project)} projects matching ID. Using first match.")

        project = project.iloc[0]

        # Find POI for related analysis
        poi_cols = [c for c in self.get_queue().columns if any(x in c.lower() for x in ['poi', 'substation', 'interconnection'])]

        analysis = {
            "project_details": project.to_dict(),
            "iso": self.iso_name,
            "analysis_date": datetime.now().isoformat()
        }

        if poi_cols:
            poi_name = project[poi_cols[0]]
            if pd.notna(poi_name):
                poi_analysis = self.get_poi_analysis(str(poi_name))
                analysis["poi_analysis"] = {
                    k: v for k, v in poi_analysis.items()
                    if k != 'projects'  # Exclude full dataframe
                }
                analysis["poi_project_count"] = poi_analysis.get("total_projects", 0)

        # Save to JSON
        import json
        with open(output_path, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)

        print(f"Analysis saved to: {output_path}")
        return output_path


def get_all_queues() -> pd.DataFrame:
    """
    Get interconnection queues from all supported ISOs.

    Returns:
        Combined DataFrame with all queues
    """
    print("Fetching queues from all ISOs...")
    return gridstatus.get_interconnection_queues()


def quick_project_lookup(iso: str, queue_id: str) -> None:
    """
    Quick lookup of a project by queue ID.

    Args:
        iso: ISO name
        queue_id: Queue ID to look up
    """
    analyzer = QueueAnalyzer(iso)
    project = analyzer.find_project(queue_id=queue_id)

    if len(project) == 0:
        print(f"No project found with ID: {queue_id}")
        return

    print(f"\nFound {len(project)} matching project(s):\n")

    # Display key fields
    display_cols = ['Queue ID', 'Project Name', 'Capacity (MW)', 'Status',
                    'Fuel', 'Type', 'State', 'County', 'POI', 'Substation']

    for _, row in project.iterrows():
        print("-" * 60)
        for col in row.index:
            # Check if column matches any display column pattern
            for dc in display_cols:
                if dc.lower() in col.lower():
                    print(f"{col}: {row[col]}")
                    break


# Example usage
if __name__ == "__main__":
    # Example: Analyze PJM queue
    print("=" * 60)
    print("INTERCONNECTION QUEUE ANALYZER")
    print("=" * 60)

    # Initialize analyzer
    analyzer = QueueAnalyzer('PJM')

    # Get basic statistics
    print("\n--- PJM Queue Statistics ---")
    stats = analyzer.get_queue_statistics()
    print(f"Total projects: {stats['total_projects']}")
    print(f"Total capacity: {stats.get('total_capacity_mw', 'N/A'):,.0f} MW")

    # Get completion rates
    print("\n--- Completion Rates ---")
    rates = analyzer.calculate_completion_rate()
    print(f"Completed: {rates['completed']} ({rates.get('completion_rate', 0):.1f}%)")
    print(f"Withdrawn: {rates['withdrawn']} ({rates.get('withdrawal_rate', 0):.1f}%)")
    print(f"Active: {rates['active']}")

    # Solar-specific stats
    print("\n--- Solar Project Statistics ---")
    solar_stats = analyzer.get_queue_statistics(fuel_type='Solar')
    print(f"Solar projects: {solar_stats['total_projects']}")
    print(f"Solar capacity: {solar_stats.get('total_capacity_mw', 'N/A'):,.0f} MW")
