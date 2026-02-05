#!/usr/bin/env python3
"""
Queue Database Access Layer (V2 Schema)

Provides a clean Python interface for querying the interconnection queue database.

Usage:
    from queue_db import QueueDB

    db = QueueDB()

    # Get all active solar projects
    projects = db.get_projects(technology='Solar', status_category='Active')

    # Get developer portfolio
    portfolio = db.get_developer_portfolio('NextEra Energy')

    # Get regional summary
    summary = db.get_regional_summary('MISO')
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date


class QueueDB:
    """Data access layer for the V2 queue database."""

    def __init__(self, db_path: str = None):
        """Initialize database connection.

        Args:
            db_path: Path to SQLite database. Defaults to .data/queue_v2.db
        """
        if db_path is None:
            db_path = Path(__file__).parent / '.data' / 'queue_v2.db'
        self.db_path = str(db_path)
        self._conn = None

    def _get_conn(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # =========================================================================
    # PROJECT QUERIES
    # =========================================================================

    def get_projects(
        self,
        region: Optional[str] = None,
        technology: Optional[str] = None,
        status: Optional[str] = None,
        status_category: Optional[str] = None,
        state: Optional[str] = None,
        developer: Optional[str] = None,
        parent_company: Optional[str] = None,
        min_capacity_mw: Optional[float] = None,
        max_capacity_mw: Optional[float] = None,
        queue_date_after: Optional[str] = None,
        queue_date_before: Optional[str] = None,
        has_ppa: Optional[bool] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Query projects with optional filters.

        Args:
            region: Filter by region code (e.g., 'MISO', 'PJM')
            technology: Filter by technology (e.g., 'Solar', 'Wind')
            status: Filter by exact status code
            status_category: Filter by status category ('Active', 'Withdrawn', etc.)
            state: Filter by state (e.g., 'TX', 'CA')
            developer: Filter by developer name (partial match)
            parent_company: Filter by parent company
            min_capacity_mw: Minimum capacity in MW
            max_capacity_mw: Maximum capacity in MW
            queue_date_after: Queue date on or after (YYYY-MM-DD)
            queue_date_before: Queue date on or before (YYYY-MM-DD)
            has_ppa: Filter by PPA status
            limit: Maximum number of results

        Returns:
            DataFrame with project data
        """
        query = """
            SELECT
                p.project_id,
                p.queue_id,
                r.region_code as region,
                p.project_name as name,
                d.canonical_name as developer,
                d.parent_company,
                t.technology_code as technology,
                t.technology_category,
                s.status_code as status,
                s.status_category,
                p.capacity_mw,
                l.state,
                l.county,
                p.queue_date,
                p.cod_proposed as cod,
                p.has_ppa,
                p.ppa_seller,
                p.data_source
            FROM fact_projects p
            JOIN dim_regions r ON p.region_id = r.region_id
            LEFT JOIN dim_developers d ON p.developer_id = d.developer_id
            LEFT JOIN dim_technologies t ON p.technology_id = t.technology_id
            LEFT JOIN dim_statuses s ON p.status_id = s.status_id
            LEFT JOIN dim_locations l ON p.location_id = l.location_id
            WHERE 1=1
        """
        params = []

        if region:
            query += " AND r.region_code = ?"
            params.append(region)

        if technology:
            query += " AND t.technology_code = ?"
            params.append(technology)

        if status:
            query += " AND s.status_code = ?"
            params.append(status)

        if status_category:
            query += " AND s.status_category = ?"
            params.append(status_category)

        if state:
            query += " AND l.state = ?"
            params.append(state.upper())

        if developer:
            query += " AND d.canonical_name LIKE ?"
            params.append(f"%{developer}%")

        if parent_company:
            query += " AND d.parent_company = ?"
            params.append(parent_company)

        if min_capacity_mw is not None:
            query += " AND p.capacity_mw >= ?"
            params.append(min_capacity_mw)

        if max_capacity_mw is not None:
            query += " AND p.capacity_mw <= ?"
            params.append(max_capacity_mw)

        if queue_date_after:
            query += " AND p.queue_date >= ?"
            params.append(queue_date_after)

        if queue_date_before:
            query += " AND p.queue_date <= ?"
            params.append(queue_date_before)

        if has_ppa is not None:
            query += " AND p.has_ppa = ?"
            params.append(1 if has_ppa else 0)

        query += " ORDER BY p.capacity_mw DESC"

        if limit:
            query += f" LIMIT {int(limit)}"

        return pd.read_sql(query, self._get_conn(), params=params)

    def get_project_by_id(self, queue_id: str, region: str) -> Optional[Dict]:
        """Get a single project by queue_id and region.

        Args:
            queue_id: The project's queue ID
            region: The region code

        Returns:
            Dictionary with project data or None if not found
        """
        df = self.get_projects(region=region, limit=None)
        match = df[df['queue_id'] == queue_id]
        if len(match) > 0:
            return match.iloc[0].to_dict()
        return None

    # =========================================================================
    # DEVELOPER QUERIES
    # =========================================================================

    def get_developers(
        self,
        parent_company: Optional[str] = None,
        min_projects: int = 1,
        min_capacity_mw: float = 0
    ) -> pd.DataFrame:
        """Get developer summary data.

        Args:
            parent_company: Filter by parent company
            min_projects: Minimum number of projects
            min_capacity_mw: Minimum total capacity

        Returns:
            DataFrame with developer metrics
        """
        query = """
            SELECT
                d.developer_id,
                d.canonical_name as developer,
                d.parent_company,
                COUNT(p.project_id) as total_projects,
                SUM(CASE WHEN s.status_category = 'Active' THEN 1 ELSE 0 END) as active_projects,
                SUM(CASE WHEN s.status_category = 'Completed' THEN 1 ELSE 0 END) as operational_projects,
                SUM(CASE WHEN s.status_category = 'Withdrawn' THEN 1 ELSE 0 END) as withdrawn_projects,
                ROUND(SUM(p.capacity_mw), 1) as total_mw,
                ROUND(SUM(CASE WHEN s.status_category = 'Active' THEN p.capacity_mw ELSE 0 END), 1) as active_mw,
                COUNT(DISTINCT r.region_code) as regions,
                COUNT(DISTINCT l.state) as states
            FROM dim_developers d
            LEFT JOIN fact_projects p ON d.developer_id = p.developer_id
            LEFT JOIN dim_statuses s ON p.status_id = s.status_id
            LEFT JOIN dim_regions r ON p.region_id = r.region_id
            LEFT JOIN dim_locations l ON p.location_id = l.location_id
            WHERE 1=1
        """
        params = []

        if parent_company:
            query += " AND d.parent_company = ?"
            params.append(parent_company)

        query += """
            GROUP BY d.developer_id, d.canonical_name, d.parent_company
            HAVING total_projects >= ? AND total_mw >= ?
            ORDER BY active_mw DESC
        """
        params.extend([min_projects, min_capacity_mw])

        return pd.read_sql(query, self._get_conn(), params=params)

    def get_developer_portfolio(self, developer_name: str) -> Dict[str, Any]:
        """Get detailed portfolio for a specific developer.

        Args:
            developer_name: Developer name (exact or partial match)

        Returns:
            Dictionary with portfolio details
        """
        conn = self._get_conn()

        # Find developer
        cursor = conn.execute("""
            SELECT developer_id, canonical_name, parent_company
            FROM dim_developers
            WHERE canonical_name LIKE ?
            LIMIT 1
        """, (f"%{developer_name}%",))
        dev = cursor.fetchone()

        if not dev:
            return {'error': f'Developer not found: {developer_name}'}

        dev_id = dev['developer_id']

        # Get projects
        projects = self.get_projects(developer=dev['canonical_name'])

        # Calculate metrics
        result = {
            'developer_id': dev_id,
            'developer': dev['canonical_name'],
            'parent_company': dev['parent_company'],
            'total_projects': len(projects),
            'total_capacity_mw': projects['capacity_mw'].sum(),
            'by_status': projects.groupby('status_category')['capacity_mw'].agg(['count', 'sum']).to_dict(),
            'by_technology': projects.groupby('technology')['capacity_mw'].agg(['count', 'sum']).to_dict(),
            'by_region': projects.groupby('region')['capacity_mw'].agg(['count', 'sum']).to_dict(),
            'by_state': projects.groupby('state')['capacity_mw'].agg(['count', 'sum']).to_dict(),
            'projects': projects
        }

        return result

    def get_parent_companies(self) -> pd.DataFrame:
        """Get summary by parent company."""
        query = """
            SELECT
                COALESCE(d.parent_company, d.canonical_name) as parent_company,
                COUNT(DISTINCT d.developer_id) as subsidiaries,
                COUNT(p.project_id) as total_projects,
                ROUND(SUM(p.capacity_mw)/1000, 1) as total_gw,
                ROUND(SUM(CASE WHEN s.status_category = 'Active' THEN p.capacity_mw ELSE 0 END)/1000, 1) as active_gw,
                COUNT(DISTINCT r.region_code) as regions
            FROM dim_developers d
            LEFT JOIN fact_projects p ON d.developer_id = p.developer_id
            LEFT JOIN dim_statuses s ON p.status_id = s.status_id
            LEFT JOIN dim_regions r ON p.region_id = r.region_id
            GROUP BY COALESCE(d.parent_company, d.canonical_name)
            HAVING total_projects > 0
            ORDER BY active_gw DESC
        """
        return pd.read_sql(query, self._get_conn())

    # =========================================================================
    # REGIONAL QUERIES
    # =========================================================================

    def get_regional_summary(self, region: Optional[str] = None) -> pd.DataFrame:
        """Get summary statistics by region.

        Args:
            region: Optional region code to filter

        Returns:
            DataFrame with regional metrics
        """
        query = """
            SELECT
                r.region_code as region,
                r.region_name,
                COUNT(p.project_id) as total_projects,
                ROUND(SUM(p.capacity_mw)/1000, 1) as total_gw,
                SUM(CASE WHEN s.status_category = 'Active' THEN 1 ELSE 0 END) as active_projects,
                ROUND(SUM(CASE WHEN s.status_category = 'Active' THEN p.capacity_mw ELSE 0 END)/1000, 1) as active_gw,
                SUM(CASE WHEN s.status_category = 'Completed' THEN 1 ELSE 0 END) as operational_projects,
                SUM(CASE WHEN s.status_category = 'Withdrawn' THEN 1 ELSE 0 END) as withdrawn_projects,
                ROUND(100.0 * SUM(CASE WHEN s.status_category = 'Completed' THEN 1 ELSE 0 END) /
                    NULLIF(SUM(CASE WHEN s.status_category IN ('Completed', 'Withdrawn') THEN 1 ELSE 0 END), 0), 1) as success_rate_pct
            FROM dim_regions r
            LEFT JOIN fact_projects p ON r.region_id = p.region_id
            LEFT JOIN dim_statuses s ON p.status_id = s.status_id
            WHERE 1=1
        """
        params = []

        if region:
            query += " AND r.region_code = ?"
            params.append(region)

        query += " GROUP BY r.region_code, r.region_name ORDER BY active_gw DESC"

        return pd.read_sql(query, self._get_conn(), params=params)

    def get_technology_mix(
        self,
        region: Optional[str] = None,
        status_category: str = 'Active'
    ) -> pd.DataFrame:
        """Get technology mix breakdown.

        Args:
            region: Optional region filter
            status_category: Status category filter

        Returns:
            DataFrame with technology breakdown
        """
        query = """
            SELECT
                r.region_code as region,
                t.technology_code as technology,
                t.technology_category,
                COUNT(p.project_id) as projects,
                ROUND(SUM(p.capacity_mw), 0) as total_mw,
                ROUND(AVG(p.capacity_mw), 1) as avg_project_mw
            FROM fact_projects p
            JOIN dim_regions r ON p.region_id = r.region_id
            JOIN dim_technologies t ON p.technology_id = t.technology_id
            JOIN dim_statuses s ON p.status_id = s.status_id
            WHERE s.status_category = ?
        """
        params = [status_category]

        if region:
            query += " AND r.region_code = ?"
            params.append(region)

        query += " GROUP BY r.region_code, t.technology_code, t.technology_category ORDER BY total_mw DESC"

        return pd.read_sql(query, self._get_conn(), params=params)

    # =========================================================================
    # LMP / PRICING QUERIES
    # =========================================================================

    def get_lmp_prices(
        self,
        region: Optional[str] = None,
        node: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """Get LMP price data.

        Args:
            region: Region code filter
            node: Specific node name
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            DataFrame with LMP data
        """
        query = """
            SELECT
                r.region_code as region,
                l.node_name,
                l.price_date,
                l.lmp_energy,
                l.lmp_congestion,
                l.lmp_losses,
                l.lmp_total
            FROM fact_lmp_prices l
            JOIN dim_regions r ON l.region_id = r.region_id
            WHERE 1=1
        """
        params = []

        if region:
            query += " AND r.region_code = ?"
            params.append(region)

        if node:
            query += " AND l.node_name LIKE ?"
            params.append(f"%{node}%")

        if start_date:
            query += " AND l.price_date >= ?"
            params.append(start_date)

        if end_date:
            query += " AND l.price_date <= ?"
            params.append(end_date)

        query += " ORDER BY l.price_date DESC"

        return pd.read_sql(query, self._get_conn(), params=params)

    # =========================================================================
    # STATISTICS & ANALYTICS
    # =========================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Get overall database statistics."""
        conn = self._get_conn()

        stats = {}

        # Total counts
        cursor = conn.execute("SELECT COUNT(*) FROM fact_projects")
        stats['total_projects'] = cursor.fetchone()[0]

        cursor = conn.execute("SELECT ROUND(SUM(capacity_mw)/1000, 1) FROM fact_projects")
        stats['total_capacity_gw'] = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(*) FROM dim_developers")
        stats['total_developers'] = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(DISTINCT parent_company) FROM dim_developers WHERE parent_company IS NOT NULL")
        stats['parent_companies'] = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(*) FROM fact_lmp_prices")
        stats['lmp_records'] = cursor.fetchone()[0]

        # By status category
        cursor = conn.execute("""
            SELECT s.status_category, COUNT(*), ROUND(SUM(p.capacity_mw)/1000, 1)
            FROM fact_projects p
            JOIN dim_statuses s ON p.status_id = s.status_id
            GROUP BY s.status_category
        """)
        stats['by_status'] = {row[0]: {'projects': row[1], 'gw': row[2]} for row in cursor.fetchall()}

        return stats

    def get_queue_trends(self, years: int = 5) -> pd.DataFrame:
        """Get queue addition trends by year.

        Args:
            years: Number of years to include

        Returns:
            DataFrame with yearly trends
        """
        query = f"""
            SELECT
                SUBSTR(p.queue_date, 1, 4) as year,
                t.technology_code as technology,
                COUNT(p.project_id) as projects,
                ROUND(SUM(p.capacity_mw)/1000, 1) as gw
            FROM fact_projects p
            JOIN dim_technologies t ON p.technology_id = t.technology_id
            WHERE p.queue_date IS NOT NULL
            AND CAST(SUBSTR(p.queue_date, 1, 4) AS INTEGER) >= CAST(strftime('%Y', 'now') AS INTEGER) - {years}
            GROUP BY year, technology
            ORDER BY year, gw DESC
        """
        return pd.read_sql(query, self._get_conn())


# Convenience function for quick access
def get_db(db_path: str = None) -> QueueDB:
    """Get a QueueDB instance."""
    return QueueDB(db_path)


if __name__ == '__main__':
    # Demo usage
    print("Queue Database Access Layer Demo")
    print("=" * 50)

    with QueueDB() as db:
        # Get stats
        stats = db.get_stats()
        print(f"\nDatabase Stats:")
        print(f"  Total Projects: {stats['total_projects']:,}")
        print(f"  Total Capacity: {stats['total_capacity_gw']:,} GW")
        print(f"  Developers: {stats['total_developers']:,}")
        print(f"  Parent Companies: {stats['parent_companies']}")

        # Get regional summary
        print("\nRegional Summary:")
        regions = db.get_regional_summary()
        print(regions[['region', 'total_projects', 'active_gw', 'success_rate_pct']].to_string(index=False))

        # Get top developers
        print("\nTop Developers by Active Pipeline:")
        devs = db.get_developers(min_projects=10, min_capacity_mw=1000)
        print(devs[['developer', 'parent_company', 'active_projects', 'active_mw']].head(10).to_string(index=False))

        # Get CAISO LMP data
        print("\nCAISO LMP Prices (last 7 days):")
        lmp = db.get_lmp_prices(region='CAISO')
        if not lmp.empty:
            print(lmp.head(10).to_string(index=False))
        else:
            print("  No LMP data available")
