#!/usr/bin/env python3
"""
Data Store - SQLite-based storage for interconnection queue data.

Provides:
- Structured storage for all RTO queue data
- Historical snapshots for change tracking
- Refresh logging and status tracking

Usage:
    from data_store import DataStore

    db = DataStore()

    # Load current data
    df = db.get_projects(region='ERCOT')

    # Get recent changes
    changes = db.get_changes(since_days=7)

    # Check last refresh
    status = db.get_refresh_status()
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import json
import hashlib


# Database location - can be overridden for cloud sync
DATA_DIR = Path(__file__).parent / '.data'
DB_PATH = DATA_DIR / 'master.db'
DG_DB_PATH = DATA_DIR / 'dg.db'


class DataStore:
    """SQLite-based storage for queue data with change tracking."""

    def __init__(self, db_path: Path = None):
        """Initialize the data store."""
        self.db_path = db_path or DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """Get database connection with row factory."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Initialize database schema."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Main projects table - current state of all projects
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id TEXT NOT NULL,
                region TEXT NOT NULL,
                name TEXT,
                developer TEXT,
                capacity_mw REAL,
                type TEXT,
                status TEXT,
                state TEXT,
                county TEXT,
                poi TEXT,
                queue_date TEXT,
                cod TEXT,
                source TEXT,
                raw_data TEXT,
                row_hash TEXT,
                sources TEXT,
                primary_source TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(queue_id, region)
            )
        ''')

        # Snapshots table - historical state for change tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL,
                queue_id TEXT NOT NULL,
                region TEXT NOT NULL,
                name TEXT,
                developer TEXT,
                capacity_mw REAL,
                type TEXT,
                status TEXT,
                source TEXT,
                row_hash TEXT
            )
        ''')

        # Changes table - detected changes between refreshes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                detected_at TEXT DEFAULT CURRENT_TIMESTAMP,
                queue_id TEXT NOT NULL,
                region TEXT NOT NULL,
                change_type TEXT NOT NULL,
                field_name TEXT,
                old_value TEXT,
                new_value TEXT,
                project_name TEXT
            )
        ''')

        # Refresh log - track data updates
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS refresh_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                started_at TEXT DEFAULT CURRENT_TIMESTAMP,
                completed_at TEXT,
                status TEXT DEFAULT 'running',
                rows_processed INTEGER DEFAULT 0,
                rows_added INTEGER DEFAULT 0,
                rows_updated INTEGER DEFAULT 0,
                error_message TEXT
            )
        ''')

        # Qualified developers table - pre-qualified interconnection developers
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS qualified_developers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                region TEXT NOT NULL,
                qualification_date TEXT,
                source TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Planning documents metadata
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS planning_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT NOT NULL,
                section TEXT NOT NULL,
                filename TEXT NOT NULL,
                file_path TEXT,
                download_date TEXT,
                size_kb REAL,
                document_date TEXT,
                UNIQUE(region, section, filename)
            )
        ''')

        # Permits table - tracks permitting status from EIA/state sources
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS permits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                permit_id TEXT NOT NULL,
                source TEXT NOT NULL,
                queue_id TEXT,
                region TEXT,
                match_confidence REAL,
                match_method TEXT,
                project_name TEXT,
                developer TEXT,
                capacity_mw REAL,
                technology TEXT,
                state TEXT,
                county TEXT,
                latitude REAL,
                longitude REAL,
                status TEXT,
                status_code TEXT,
                status_date TEXT,
                expected_cod TEXT,
                raw_data TEXT,
                row_hash TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(permit_id, source)
            )
        ''')

        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_projects_region ON projects(region)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_projects_developer ON projects(developer)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_projects_queue_id ON projects(queue_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_snapshots_date ON snapshots(snapshot_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_changes_date ON changes(detected_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_qualified_devs_region ON qualified_developers(region)')

        # Permit indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_permits_queue_id ON permits(queue_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_permits_source ON permits(source)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_permits_state ON permits(state)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_permits_status ON permits(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_permits_developer ON permits(developer)')

        conn.commit()
        conn.close()

    def _compute_hash(self, row: Dict) -> str:
        """Compute hash of row for change detection."""
        # Include key fields that we want to track changes for
        key_fields = ['name', 'developer', 'capacity_mw', 'type', 'status', 'cod']
        values = [str(row.get(f, '')) for f in key_fields]
        return hashlib.md5('|'.join(values).encode()).hexdigest()

    def upsert_projects(self, df: pd.DataFrame, source: str, region: str = None) -> Dict[str, int]:
        """
        Insert or update projects from a DataFrame.

        Golden record pattern: one row per (queue_id, region).
        Each source updates the existing record rather than creating duplicates.
        Source provenance is tracked in the project_sources table.

        Returns dict with counts: added, updated, unchanged
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        # Ensure project_sources table exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS project_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id TEXT NOT NULL,
                region TEXT NOT NULL,
                source TEXT NOT NULL,
                updated_at TEXT,
                UNIQUE(queue_id, region, source)
            )
        ''')

        stats = {'added': 0, 'updated': 0, 'unchanged': 0}

        for _, row in df.iterrows():
            row_dict = row.to_dict()
            row_region = region or row_dict.get('region', 'Unknown')
            queue_id = str(row_dict.get('queue_id', ''))

            if not queue_id:
                continue

            row_hash = self._compute_hash(row_dict)

            # Check if project exists (by queue_id + region, regardless of source)
            cursor.execute('''
                SELECT id, row_hash, status, source, sources FROM projects
                WHERE queue_id = ? AND region = ?
            ''', (queue_id, row_region))
            existing = cursor.fetchone()

            if existing:
                if existing['row_hash'] != row_hash:
                    # Record change if status changed
                    if existing['status'] != row_dict.get('status') and row_dict.get('status'):
                        cursor.execute('''
                            INSERT INTO changes (queue_id, region, change_type, field_name,
                                               old_value, new_value, project_name)
                            VALUES (?, ?, 'status_change', 'status', ?, ?, ?)
                        ''', (queue_id, row_region, existing['status'],
                              row_dict.get('status'), row_dict.get('name')))

                    # Update fields — only overwrite with non-null values
                    update_fields = {}
                    for field in ['name', 'developer', 'capacity_mw', 'type', 'status',
                                  'state', 'county', 'poi', 'queue_date', 'cod']:
                        new_val = row_dict.get(field)
                        if new_val and str(new_val).strip():
                            update_fields[field] = new_val

                    if update_fields:
                        set_clause = ', '.join(f"{f} = ?" for f in update_fields)
                        values = list(update_fields.values())

                        cursor.execute(f'''
                            UPDATE projects SET {set_clause},
                                raw_data = ?, row_hash = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', values + [json.dumps(row_dict, default=str), row_hash, existing['id']])

                    # Update sources list
                    try:
                        current_sources = json.loads(existing['sources'] or '[]')
                    except (json.JSONDecodeError, TypeError):
                        current_sources = [existing['source']] if existing['source'] else []
                    if source not in current_sources:
                        current_sources.append(source)
                        cursor.execute(
                            'UPDATE projects SET sources = ? WHERE id = ?',
                            (json.dumps(sorted(current_sources)), existing['id'])
                        )

                    stats['updated'] += 1
                else:
                    stats['unchanged'] += 1
            else:
                # Insert new golden record
                sources_json = json.dumps([source])
                cursor.execute('''
                    INSERT INTO projects (queue_id, region, name, developer, capacity_mw,
                                        type, status, state, county, poi, queue_date, cod,
                                        source, primary_source, sources, raw_data, row_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    queue_id, row_region, row_dict.get('name'), row_dict.get('developer'),
                    row_dict.get('capacity_mw'), row_dict.get('type'),
                    row_dict.get('status'), row_dict.get('state'),
                    row_dict.get('county'), row_dict.get('poi'),
                    str(row_dict.get('queue_date', '')), str(row_dict.get('cod', '')),
                    source, source, sources_json,
                    json.dumps(row_dict, default=str), row_hash
                ))

                # Record as new project
                cursor.execute('''
                    INSERT INTO changes (queue_id, region, change_type, project_name)
                    VALUES (?, ?, 'new_project', ?)
                ''', (queue_id, row_region, row_dict.get('name')))

                stats['added'] += 1

            # Track source provenance
            cursor.execute('''
                INSERT OR REPLACE INTO project_sources (queue_id, region, source, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''', (queue_id, row_region, source))

        conn.commit()
        conn.close()
        return stats

    def create_snapshot(self):
        """Create a snapshot of current state for historical tracking."""
        conn = self._get_conn()
        cursor = conn.cursor()

        snapshot_date = datetime.now().strftime('%Y-%m-%d')

        # Check if snapshot already exists for today
        cursor.execute('SELECT COUNT(*) FROM snapshots WHERE snapshot_date = ?', (snapshot_date,))
        if cursor.fetchone()[0] > 0:
            conn.close()
            return False

        # Copy current state to snapshots
        cursor.execute('''
            INSERT INTO snapshots (snapshot_date, queue_id, region, name, developer,
                                  capacity_mw, type, status, source, row_hash)
            SELECT ?, queue_id, region, name, developer, capacity_mw, type, status,
                   source, row_hash
            FROM projects
        ''', (snapshot_date,))

        rows = cursor.rowcount
        conn.commit()
        conn.close()

        return rows

    def get_projects(
        self,
        region: str = None,
        developer: str = None,
        status: str = None,
        fuel_type: str = None,
        min_mw: float = None,
        max_mw: float = None,
        source: str = None,
    ) -> pd.DataFrame:
        """Query projects with filters."""
        conn = self._get_conn()

        query = "SELECT * FROM projects WHERE 1=1"
        params = []

        if region:
            query += " AND region = ?"
            params.append(region)

        if developer:
            query += " AND developer LIKE ?"
            params.append(f'%{developer}%')

        if status:
            query += " AND status LIKE ?"
            params.append(f'%{status}%')

        if fuel_type:
            query += " AND type LIKE ?"
            params.append(f'%{fuel_type}%')

        if min_mw is not None:
            query += " AND capacity_mw >= ?"
            params.append(min_mw)

        if max_mw is not None:
            query += " AND capacity_mw <= ?"
            params.append(max_mw)

        if source:
            query += " AND source = ?"
            params.append(source)

        query += " ORDER BY capacity_mw DESC"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df

    def get_changes(self, since_days: int = 7, region: str = None) -> pd.DataFrame:
        """Get recent changes."""
        conn = self._get_conn()

        since_date = (datetime.now() - timedelta(days=since_days)).strftime('%Y-%m-%d')

        query = """
            SELECT * FROM changes
            WHERE detected_at >= ?
        """
        params = [since_date]

        if region:
            query += " AND region = ?"
            params.append(region)

        query += " ORDER BY detected_at DESC"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df

    def get_stats(self, region: str = None) -> Dict[str, Any]:
        """Get database statistics."""
        conn = self._get_conn()
        cursor = conn.cursor()

        where = "WHERE region = ?" if region else ""
        params = [region] if region else []

        cursor.execute(f"SELECT COUNT(*) FROM projects {where}", params)
        total = cursor.fetchone()[0]

        cursor.execute(f"SELECT SUM(capacity_mw) FROM projects {where}", params)
        capacity = cursor.fetchone()[0] or 0

        cursor.execute(f"""
            SELECT region, COUNT(*) as count, SUM(capacity_mw) as mw
            FROM projects
            {where}
            GROUP BY region
            ORDER BY count DESC
        """, params)
        by_region = [dict(row) for row in cursor.fetchall()]

        cursor.execute("""
            SELECT source, MAX(completed_at) as last_refresh, status
            FROM refresh_log
            WHERE status = 'success'
            GROUP BY source
        """)
        refresh_status = {row['source']: row['last_refresh'] for row in cursor.fetchall()}

        conn.close()

        return {
            'total_projects': total,
            'total_capacity_gw': capacity / 1000,
            'by_region': by_region,
            'last_refresh': refresh_status,
        }

    def log_refresh_start(self, source: str) -> int:
        """Start a refresh log entry, return the log ID."""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO refresh_log (source) VALUES (?)", (source,))
        log_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return log_id

    def log_refresh_complete(self, log_id: int, stats: Dict[str, int], error: str = None):
        """Complete a refresh log entry."""
        conn = self._get_conn()
        cursor = conn.cursor()

        status = 'error' if error else 'success'
        cursor.execute('''
            UPDATE refresh_log SET
                completed_at = CURRENT_TIMESTAMP,
                status = ?,
                rows_processed = ?,
                rows_added = ?,
                rows_updated = ?,
                error_message = ?
            WHERE id = ?
        ''', (
            status,
            stats.get('added', 0) + stats.get('updated', 0) + stats.get('unchanged', 0),
            stats.get('added', 0),
            stats.get('updated', 0),
            error,
            log_id
        ))

        conn.commit()
        conn.close()

    def upsert_qualified_developers(self, developers: List[Dict], region: str) -> Dict[str, int]:
        """Insert or update qualified developers."""
        conn = self._get_conn()
        cursor = conn.cursor()

        stats = {'added': 0, 'updated': 0}

        for dev in developers:
            name = dev.get('name', '').strip()
            if not name:
                continue

            cursor.execute('''
                SELECT id FROM qualified_developers WHERE name = ?
            ''', (name,))
            existing = cursor.fetchone()

            if existing:
                cursor.execute('''
                    UPDATE qualified_developers SET
                        region = ?, qualification_date = ?, source = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (region, dev.get('qualification_date'), dev.get('source'), existing['id']))
                stats['updated'] += 1
            else:
                cursor.execute('''
                    INSERT INTO qualified_developers (name, region, qualification_date, source)
                    VALUES (?, ?, ?, ?)
                ''', (name, region, dev.get('qualification_date'), dev.get('source')))
                stats['added'] += 1

        conn.commit()
        conn.close()
        return stats

    def get_qualified_developers(self, region: str = None) -> List[Dict]:
        """Get qualified developers, optionally filtered by region."""
        conn = self._get_conn()
        cursor = conn.cursor()

        if region:
            cursor.execute('SELECT * FROM qualified_developers WHERE region = ?', (region,))
        else:
            cursor.execute('SELECT * FROM qualified_developers')

        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results

    def get_refresh_status(self) -> List[Dict]:
        """Get refresh status for all sources."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT source,
                   MAX(completed_at) as last_success,
                   (SELECT status FROM refresh_log r2
                    WHERE r2.source = r1.source
                    ORDER BY started_at DESC LIMIT 1) as last_status
            FROM refresh_log r1
            WHERE status = 'success'
            GROUP BY source
        ''')

        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results

    def export_to_csv(self, output_path: Path, region: str = None):
        """Export projects to CSV for backup/sharing."""
        df = self.get_projects(region=region)
        df.to_csv(output_path, index=False)
        return len(df)

    # =========================================================================
    # Permit Methods
    # =========================================================================

    def _compute_permit_hash(self, row: Dict) -> str:
        """Compute hash of permit row for change detection."""
        key_fields = ['project_name', 'developer', 'capacity_mw', 'technology', 'status', 'status_code', 'expected_cod']
        values = [str(row.get(f, '')) for f in key_fields]
        return hashlib.md5('|'.join(values).encode()).hexdigest()

    def upsert_permits(self, df: pd.DataFrame, source: str) -> Dict[str, int]:
        """
        Insert or update permits from a DataFrame.

        Required columns: permit_id
        Optional columns: project_name, developer, capacity_mw, technology,
                         state, county, latitude, longitude, status, status_code,
                         expected_cod, queue_id, region, match_confidence

        Returns dict with counts: added, updated, unchanged
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        stats = {'added': 0, 'updated': 0, 'unchanged': 0}

        for _, row in df.iterrows():
            row_dict = row.to_dict()
            permit_id = str(row_dict.get('permit_id', ''))

            if not permit_id:
                continue

            row_hash = self._compute_permit_hash(row_dict)

            # Check if exists
            cursor.execute('''
                SELECT id, row_hash FROM permits
                WHERE permit_id = ? AND source = ?
            ''', (permit_id, source))
            existing = cursor.fetchone()

            if existing:
                if existing['row_hash'] != row_hash:
                    # Update existing row
                    cursor.execute('''
                        UPDATE permits SET
                            queue_id = ?, region = ?, match_confidence = ?, match_method = ?,
                            project_name = ?, developer = ?, capacity_mw = ?, technology = ?,
                            state = ?, county = ?, latitude = ?, longitude = ?,
                            status = ?, status_code = ?, status_date = ?, expected_cod = ?,
                            raw_data = ?, row_hash = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (
                        row_dict.get('queue_id'), row_dict.get('region'),
                        row_dict.get('match_confidence'), row_dict.get('match_method'),
                        row_dict.get('project_name'), row_dict.get('developer'),
                        row_dict.get('capacity_mw'), row_dict.get('technology'),
                        row_dict.get('state'), row_dict.get('county'),
                        row_dict.get('latitude'), row_dict.get('longitude'),
                        row_dict.get('status'), row_dict.get('status_code'),
                        row_dict.get('status_date'), row_dict.get('expected_cod'),
                        json.dumps(row_dict, default=str), row_hash,
                        existing['id']
                    ))
                    stats['updated'] += 1
                else:
                    stats['unchanged'] += 1
            else:
                # Insert new row
                cursor.execute('''
                    INSERT INTO permits (
                        permit_id, source, queue_id, region, match_confidence, match_method,
                        project_name, developer, capacity_mw, technology,
                        state, county, latitude, longitude,
                        status, status_code, status_date, expected_cod,
                        raw_data, row_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    permit_id, source,
                    row_dict.get('queue_id'), row_dict.get('region'),
                    row_dict.get('match_confidence'), row_dict.get('match_method'),
                    row_dict.get('project_name'), row_dict.get('developer'),
                    row_dict.get('capacity_mw'), row_dict.get('technology'),
                    row_dict.get('state'), row_dict.get('county'),
                    row_dict.get('latitude'), row_dict.get('longitude'),
                    row_dict.get('status'), row_dict.get('status_code'),
                    row_dict.get('status_date'), row_dict.get('expected_cod'),
                    json.dumps(row_dict, default=str), row_hash
                ))
                stats['added'] += 1

        conn.commit()
        conn.close()
        return stats

    def get_permits(
        self,
        queue_id: str = None,
        state: str = None,
        status: str = None,
        source: str = None,
        technology: str = None,
        developer: str = None,
    ) -> pd.DataFrame:
        """Query permits with filters."""
        conn = self._get_conn()

        query = "SELECT * FROM permits WHERE 1=1"
        params = []

        if queue_id:
            query += " AND queue_id = ?"
            params.append(queue_id)

        if state:
            query += " AND state = ?"
            params.append(state)

        if status:
            query += " AND status LIKE ?"
            params.append(f'%{status}%')

        if source:
            query += " AND source = ?"
            params.append(source)

        if technology:
            query += " AND technology LIKE ?"
            params.append(f'%{technology}%')

        if developer:
            query += " AND developer LIKE ?"
            params.append(f'%{developer}%')

        query += " ORDER BY capacity_mw DESC"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df

    def link_permit_to_project(self, permit_id: str, source: str, queue_id: str,
                                region: str, confidence: float, method: str) -> bool:
        """Link a permit to a queue project."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE permits SET
                queue_id = ?, region = ?, match_confidence = ?, match_method = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE permit_id = ? AND source = ?
        ''', (queue_id, region, confidence, method, permit_id, source))

        updated = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return updated

    def get_permit_for_project(self, queue_id: str, region: str = None) -> Optional[Dict]:
        """Get permit data for a specific queue project."""
        conn = self._get_conn()
        cursor = conn.cursor()

        if region:
            cursor.execute('''
                SELECT * FROM permits
                WHERE queue_id = ? AND region = ?
                ORDER BY match_confidence DESC
                LIMIT 1
            ''', (queue_id, region))
        else:
            cursor.execute('''
                SELECT * FROM permits
                WHERE queue_id = ?
                ORDER BY match_confidence DESC
                LIMIT 1
            ''', (queue_id,))

        result = cursor.fetchone()
        conn.close()

        return dict(result) if result else None

    def get_permit_stats(self) -> Dict[str, Any]:
        """Get permit statistics."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM permits")
        total = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM permits WHERE queue_id IS NOT NULL")
        matched = cursor.fetchone()[0]

        cursor.execute("""
            SELECT source, COUNT(*) as count, SUM(capacity_mw) as mw
            FROM permits GROUP BY source
        """)
        by_source = [dict(row) for row in cursor.fetchall()]

        cursor.execute("""
            SELECT status, COUNT(*) as count
            FROM permits GROUP BY status ORDER BY count DESC
        """)
        by_status = [dict(row) for row in cursor.fetchall()]

        cursor.execute("""
            SELECT state, COUNT(*) as count
            FROM permits WHERE state IS NOT NULL
            GROUP BY state ORDER BY count DESC LIMIT 10
        """)
        by_state = [dict(row) for row in cursor.fetchall()]

        conn.close()

        return {
            'total_permits': total,
            'matched_to_queue': matched,
            'match_rate': matched / total if total > 0 else 0,
            'by_source': by_source,
            'by_status': by_status,
            'top_states': by_state,
        }


def main():
    """Test the data store."""
    print("Initializing DataStore...")
    db = DataStore()

    print(f"\nDatabase location: {db.db_path}")

    stats = db.get_stats()
    print(f"\nDatabase Stats:")
    print(f"  Total projects: {stats['total_projects']:,}")
    print(f"  Total capacity: {stats['total_capacity_gw']:.1f} GW")

    if stats['by_region']:
        print(f"\n  By Region:")
        for r in stats['by_region']:
            print(f"    {r['region']}: {r['count']:,} projects ({r['mw']/1000:.1f} GW)")

    if stats['last_refresh']:
        print(f"\n  Last Refresh:")
        for source, time in stats['last_refresh'].items():
            print(f"    {source}: {time}")
    else:
        print("\n  No data loaded yet. Run refresh_data.py to populate.")


if __name__ == "__main__":
    main()
