#!/usr/bin/env python3
"""
V2 Database Refresh Script

Refreshes the V1 database from live sources, syncs stale records,
and rebuilds the V2 normalized database.

Usage:
    python3 refresh_v2.py              # Full refresh and rebuild
    python3 refresh_v2.py --quick      # Skip source refresh, just rebuild V2
    python3 refresh_v2.py --sync-only  # Only sync stale records
    python3 refresh_v2.py --cron       # Cron-friendly (quiet mode)
    python3 refresh_v2.py --dry-run    # Preview changes without committing
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import argparse
import sys
from collections import defaultdict

# Suppress warnings
import warnings
warnings.filterwarnings('ignore')

TOOLS_DIR = Path(__file__).parent
V1_PATH = TOOLS_DIR / '.data' / 'queue.db'
V2_PATH = TOOLS_DIR / '.data' / 'queue_v2.db'
SCHEMA_PATH = TOOLS_DIR / 'schema_v2.sql'

# Track unknown statuses encountered
UNKNOWN_STATUSES = defaultdict(int)


def capture_project_snapshot_by_key():
    """
    Capture current state of projects before rebuild.
    Uses (queue_id, region_code) as key since project_id changes on rebuild.

    Returns:
        Dict mapping (queue_id, region_code) to current state dict
    """
    if not V2_PATH.exists():
        return {}

    conn = sqlite3.connect(V2_PATH)
    cursor = conn.execute("""
        SELECT
            p.queue_id, r.region_code,
            s.status_code, p.capacity_mw, p.cod_proposed,
            d.canonical_name as developer_name
        FROM fact_projects p
        JOIN dim_regions r ON p.region_id = r.region_id
        LEFT JOIN dim_statuses s ON p.status_id = s.status_id
        LEFT JOIN dim_developers d ON p.developer_id = d.developer_id
    """)

    snapshot = {}
    for row in cursor.fetchall():
        key = (row[0], row[1])  # (queue_id, region_code)
        snapshot[key] = {
            'queue_id': row[0],
            'region_code': row[1],
            'status_code': row[2],
            'capacity_mw': row[3],
            'cod_proposed': row[4],
            'developer_name': row[5]
        }
    conn.close()
    return snapshot


def record_project_changes(old_snapshot, quiet=False):
    """
    Compare current V2 state to old snapshot and record changes.
    Creates a change log file since fact_project_history requires project_id.

    Args:
        old_snapshot: Dict from capture_project_snapshot_by_key before rebuild
        quiet: Suppress output

    Returns:
        Dict with change statistics
    """
    import json
    from datetime import datetime

    if not old_snapshot:
        return {'new_projects': 0, 'changes': 0, 'removed': 0}

    today = datetime.now().strftime('%Y-%m-%d')

    conn = sqlite3.connect(V2_PATH)
    cursor = conn.execute("""
        SELECT
            p.project_id, p.queue_id, r.region_code,
            s.status_code, p.capacity_mw, p.cod_proposed,
            d.canonical_name as developer_name
        FROM fact_projects p
        JOIN dim_regions r ON p.region_id = r.region_id
        LEFT JOIN dim_statuses s ON p.status_id = s.status_id
        LEFT JOIN dim_developers d ON p.developer_id = d.developer_id
    """)

    new_snapshot = {}
    project_id_lookup = {}
    for row in cursor.fetchall():
        key = (row[1], row[2])  # (queue_id, region_code)
        new_snapshot[key] = {
            'project_id': row[0],
            'queue_id': row[1],
            'region_code': row[2],
            'status_code': row[3],
            'capacity_mw': row[4],
            'cod_proposed': row[5],
            'developer_name': row[6]
        }
        project_id_lookup[key] = row[0]

    changes = []
    stats = {'new_projects': 0, 'status_changes': 0, 'capacity_changes': 0, 'removed': 0}

    # Find changes and new projects
    for key, new_state in new_snapshot.items():
        if key not in old_snapshot:
            stats['new_projects'] += 1
            continue

        old_state = old_snapshot[key]
        changed_fields = []

        if old_state['status_code'] != new_state['status_code']:
            changed_fields.append('status')
            stats['status_changes'] += 1

        # Compare capacity with tolerance for floating point
        old_cap = old_state['capacity_mw'] or 0
        new_cap = new_state['capacity_mw'] or 0
        if abs(old_cap - new_cap) > 0.1:
            changed_fields.append('capacity')
            stats['capacity_changes'] += 1

        if old_state['cod_proposed'] != new_state['cod_proposed']:
            changed_fields.append('cod')

        if old_state['developer_name'] != new_state['developer_name']:
            changed_fields.append('developer')

        if changed_fields:
            changes.append({
                'queue_id': key[0],
                'region': key[1],
                'changed_fields': changed_fields,
                'old_status': old_state['status_code'],
                'new_status': new_state['status_code'],
                'old_capacity': old_cap,
                'new_capacity': new_cap
            })

            # Record in fact_project_history
            project_id = project_id_lookup.get(key)
            if project_id:
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO fact_project_history
                        (project_id, snapshot_date, status_id, capacity_mw, cod_proposed, developer_id, changed_fields)
                        SELECT ?, ?, status_id, capacity_mw, cod_proposed, developer_id, ?
                        FROM fact_projects WHERE project_id = ?
                    """, (project_id, today, json.dumps(changed_fields), project_id))
                except:
                    pass  # Table might not exist yet

    # Find removed projects
    for key in old_snapshot:
        if key not in new_snapshot:
            stats['removed'] += 1

    conn.commit()
    conn.close()

    # Save change log to file
    if changes:
        log_path = TOOLS_DIR / '.data' / 'change_log.json'
        existing = []
        if log_path.exists():
            try:
                with open(log_path) as f:
                    existing = json.load(f)
            except:
                existing = []

        existing.append({
            'date': today,
            'stats': stats,
            'changes': changes[:100]  # Limit to first 100 detailed changes
        })

        # Keep last 30 days of logs
        existing = existing[-30:]

        with open(log_path, 'w') as f:
            json.dump(existing, f, indent=2, default=str)

    if not quiet:
        if stats['new_projects'] > 0:
            print(f"  New projects: {stats['new_projects']}")
        if stats['status_changes'] > 0:
            print(f"  Status changes: {stats['status_changes']}")
        if stats['capacity_changes'] > 0:
            print(f"  Capacity changes: {stats['capacity_changes']}")
        if stats['removed'] > 0:
            print(f"  Removed projects: {stats['removed']}")

    return stats


def sync_stale_records(quiet=False, dry_run=False, force_apply=False):
    """
    Queue potential withdrawals for human review instead of auto-applying.

    SAFETY FIX: This function now queues status changes to 'Withdrawn' for
    human approval instead of auto-applying them. This prevents data corruption
    from API failures, network issues, or format changes.

    Args:
        quiet: Suppress output
        dry_run: Preview changes without committing
        force_apply: DANGEROUS - bypass validation and auto-apply (legacy behavior)
    """
    from validation_gates import ValidationGates

    if not quiet:
        if dry_run:
            print("\n--- Syncing Stale Records (DRY RUN) ---")
        elif force_apply:
            print("\n--- Syncing Stale Records (FORCE MODE - NO VALIDATION) ---")
        else:
            print("\n--- Syncing Stale Records (Queueing for Review) ---")

    from direct_fetcher import DirectFetcher
    fetcher = DirectFetcher()

    conn = sqlite3.connect(V1_PATH)
    cursor = conn.cursor()

    gates = ValidationGates()
    total_queued = 0
    total_applied = 0

    def process_stale_records(region: str, live_ids: set, sources: list, exclude_statuses: list):
        """Process stale records for a region - queue or apply based on mode."""
        nonlocal total_queued, total_applied

        placeholders_exclude = ','.join(['?'] * len(exclude_statuses))
        cursor.execute(f"""
            SELECT DISTINCT queue_id, status FROM projects
            WHERE region = ? AND source IN ({','.join(['?']*len(sources))})
            AND status NOT IN ({placeholders_exclude})
        """, [region] + sources + exclude_statuses)

        db_records = {r[0]: r[1] for r in cursor.fetchall()}
        db_ids = set(db_records.keys())

        stale = db_ids - live_ids
        if not stale:
            return 0

        count = 0
        for queue_id in stale:
            old_status = db_records.get(queue_id, 'Active')

            if force_apply and not dry_run:
                # Legacy dangerous behavior - only with explicit flag
                cursor.execute("""
                    UPDATE projects SET status = 'Withdrawn'
                    WHERE queue_id = ? AND region = ?
                """, (queue_id, region))
                total_applied += 1
                count += 1
            else:
                # Safe behavior - queue for review
                gates.queue_status_change(
                    queue_id=queue_id,
                    region=region,
                    old_status=old_status,
                    new_status='Withdrawn',
                    reason='not_in_latest_fetch',
                    source='sync'
                )
                total_queued += 1
                count += 1

        return count

    # ERCOT sync
    try:
        live_ercot = fetcher.fetch_ercot(use_cache=True)
        if not live_ercot.empty:
            live_ids = set(live_ercot['Queue ID'].tolist())
            count = process_stale_records(
                'ERCOT', live_ids,
                ['ercot', 'lbl'],
                ['withdrawn', 'Withdrawn', 'operational', 'Operational']
            )
            if not quiet and count:
                action = "applied" if force_apply else "queued"
                print(f"  ERCOT: {count} stale records {action}")
    except Exception as e:
        if not quiet:
            print(f"  ERCOT sync error: {e}")

    # MISO sync
    try:
        live_miso = fetcher.fetch_miso(use_cache=True)
        if not live_miso.empty:
            live_ids = set(live_miso['Queue ID'].tolist())
            count = process_stale_records(
                'MISO', live_ids,
                ['miso_api', 'lbl'],
                ['withdrawn', 'Withdrawn', 'operational', 'Operational', 'Done']
            )
            if not quiet and count:
                action = "applied" if force_apply else "queued"
                print(f"  MISO: {count} stale records {action}")
    except Exception as e:
        if not quiet:
            print(f"  MISO sync error: {e}")

    # NYISO sync
    try:
        live_nyiso = fetcher.fetch_nyiso(use_cache=True)
        if not live_nyiso.empty:
            queue_col = 'Queue Pos.' if 'Queue Pos.' in live_nyiso.columns else 'Queue ID'
            live_ids = set(str(x) for x in live_nyiso[queue_col].dropna().tolist())
            count = process_stale_records(
                'NYISO', live_ids,
                ['nyiso', 'lbl'],
                ['withdrawn', 'Withdrawn', 'operational', 'Operational']
            )
            if not quiet and count:
                action = "applied" if force_apply else "queued"
                print(f"  NYISO: {count} stale records {action}")
    except Exception as e:
        if not quiet:
            print(f"  NYISO sync error: {e}")

    # CAISO sync
    try:
        live_caiso = fetcher.fetch_caiso(use_cache=True)
        if not live_caiso.empty:
            queue_col = 'Queue Position' if 'Queue Position' in live_caiso.columns else 'Queue ID'
            live_ids = set(str(x) for x in live_caiso[queue_col].dropna().tolist())
            count = process_stale_records(
                'CAISO', live_ids,
                ['caiso', 'lbl'],
                ['withdrawn', 'Withdrawn', 'operational', 'Operational']
            )
            if not quiet and count:
                action = "applied" if force_apply else "queued"
                print(f"  CAISO: {count} stale records {action}")
    except Exception as e:
        if not quiet:
            print(f"  CAISO sync error: {e}")

    # ISO-NE sync
    try:
        live_isone = fetcher.fetch_isone(use_cache=True)
        if not live_isone.empty:
            if 'Status' in live_isone.columns:
                live_isone_active = live_isone[live_isone['Status'] == 'Active']
            else:
                live_isone_active = live_isone
            queue_col = 'Queue ID' if 'Queue ID' in live_isone_active.columns else live_isone_active.columns[0]
            live_ids = set(str(x) for x in live_isone_active[queue_col].dropna().tolist())
            count = process_stale_records(
                'ISO-NE', live_ids,
                ['isone', 'lbl'],
                ['withdrawn', 'Withdrawn', 'operational', 'Operational', 'Completed']
            )
            if not quiet and count:
                action = "applied" if force_apply else "queued"
                print(f"  ISO-NE: {count} stale records {action}")
    except Exception as e:
        if not quiet:
            print(f"  ISO-NE sync error: {e}")

    # SPP sync
    try:
        live_spp = fetcher.fetch_spp(use_cache=True)
        if not live_spp.empty:
            if 'Status' in live_spp.columns:
                live_spp_active = live_spp[live_spp['Status'] == 'Active']
            else:
                live_spp_active = live_spp
            queue_col = 'Queue ID' if 'Queue ID' in live_spp_active.columns else live_spp_active.columns[0]
            live_ids = set(str(x) for x in live_spp_active[queue_col].dropna().tolist())
            count = process_stale_records(
                'SPP', live_ids,
                ['spp', 'lbl'],
                ['withdrawn', 'Withdrawn', 'operational', 'Operational']
            )
            if not quiet and count:
                action = "applied" if force_apply else "queued"
                print(f"  SPP: {count} stale records {action}")
    except Exception as e:
        if not quiet:
            print(f"  SPP sync error: {e}")

    if dry_run:
        conn.rollback()
        if not quiet:
            print(f"  DRY RUN: Would queue {total_queued} records for review")
    elif force_apply:
        conn.commit()
        if not quiet:
            print(f"  FORCE APPLIED: {total_applied} stale records (DANGEROUS)")
    else:
        conn.commit()
        if not quiet:
            print(f"  Queued {total_queued} records for review")
            print(f"  Run 'python validation_gates.py --pending' to review")

    conn.close()
    gates.close()
    return total_queued if not force_apply else total_applied


def rebuild_v2(quiet=False, dry_run=False):
    """Rebuild V2 database from V1.

    Args:
        quiet: Suppress output
        dry_run: Preview changes without committing
    """
    if not quiet:
        if dry_run:
            print("\n--- Rebuilding V2 Database (DRY RUN - analyzing only) ---")
        else:
            print("\n--- Rebuilding V2 Database ---")

    # Capture current state BEFORE rebuild for change tracking
    old_snapshot = {}
    if not dry_run:
        old_snapshot = capture_project_snapshot_by_key()
        if old_snapshot and not quiet:
            print(f"  Captured snapshot of {len(old_snapshot)} existing projects")

    # For dry_run, use temporary in-memory database
    if dry_run:
        conn_v2 = sqlite3.connect(':memory:')
    else:
        # Remove existing V2
        if V2_PATH.exists():
            V2_PATH.unlink()
        conn_v2 = sqlite3.connect(V2_PATH)
    cursor_v2 = conn_v2.cursor()

    # Create schema
    with open(SCHEMA_PATH) as f:
        cursor_v2.executescript(f.read())

    conn_v1 = sqlite3.connect(V1_PATH)

    # =========================================================================
    # DIMENSIONS
    # =========================================================================

    # Regions
    regions = [
        ('MISO', 'Midcontinent ISO', 'ISO', 'America/Chicago'),
        ('PJM', 'PJM Interconnection', 'RTO', 'America/New_York'),
        ('ERCOT', 'Electric Reliability Council of Texas', 'ISO', 'America/Chicago'),
        ('CAISO', 'California ISO', 'ISO', 'America/Los_Angeles'),
        ('NYISO', 'New York ISO', 'ISO', 'America/New_York'),
        ('ISO-NE', 'ISO New England', 'ISO', 'America/New_York'),
        ('SPP', 'Southwest Power Pool', 'RTO', 'America/Chicago'),
        ('Southeast', 'Southeast (non-ISO)', 'Other', 'America/New_York'),
        ('West', 'Western Interconnection (non-ISO)', 'Other', 'America/Los_Angeles'),
    ]
    cursor_v2.executemany(
        "INSERT INTO dim_regions (region_code, region_name, region_type, timezone) VALUES (?, ?, ?, ?)",
        regions
    )

    # Technologies
    technologies = [
        ('Solar', 'Solar', 'Renewable', 1, 0, 0.25, 1000),
        ('Wind', 'Wind', 'Renewable', 1, 0, 0.35, 1300),
        ('Offshore Wind', 'Offshore Wind', 'Renewable', 1, 0, 0.45, 3500),
        ('Storage', 'Storage', 'Storage', 0, 1, None, 800),
        ('Solar + Storage', 'Solar + Storage', 'Hybrid', 1, 1, 0.30, 1200),
        ('Wind + Storage', 'Wind + Storage', 'Hybrid', 1, 1, 0.38, 1500),
        ('Gas', 'Gas', 'Thermal', 0, 1, 0.50, 800),
        ('Coal', 'Coal', 'Thermal', 0, 1, 0.60, 2500),
        ('Nuclear', 'Nuclear', 'Thermal', 0, 1, 0.90, 6000),
        ('Hydro', 'Hydro', 'Renewable', 1, 1, 0.40, 2000),
        ('Geothermal', 'Geothermal', 'Renewable', 1, 1, 0.85, 2500),
        ('Biomass', 'Biomass', 'Renewable', 1, 1, 0.70, 3000),
        ('Other', 'Other', 'Other', 0, 0, None, None),
        ('Hybrid', 'Hybrid', 'Hybrid', 1, 1, None, None),
        ('Solar + Wind + Storage', 'Solar + Wind + Storage', 'Hybrid', 1, 1, 0.50, 1600),
        ('Storage + Gas', 'Storage + Gas', 'Hybrid', 0, 1, None, 900),
        ('Oil', 'Oil', 'Thermal', 0, 1, 0.30, 1500),
    ]
    cursor_v2.executemany("""
        INSERT INTO dim_technologies (technology_code, technology_name, technology_category,
                                      is_renewable, is_dispatchable, typical_capacity_factor, typical_capex_per_kw)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, technologies)

    # Statuses
    # NOTE: Unknown statuses are now mapped to 'Unknown' category, NOT 'Active'
    # This prevents unknown statuses from inflating active project counts
    statuses = [
        # Generic statuses
        ('Active', 'Active', 'Active', 1),
        ('active', 'Active', 'Active', 1),
        ('ACTIVE', 'Active', 'Active', 1),
        ('Planned', 'Planned', 'Active', 1),
        ('Facilities Study', 'Facilities Study', 'Active', 4),
        ('IA Executed', 'IA Executed', 'Active', 7),
        ('Pending Transfer', 'Pending Transfer', 'Active', 9),
        # MISO study phases
        ('SS Completed, FIS Started, No IA', 'Study Phase', 'Active', 4),
        ('SS Completed, FIS Completed, IA', 'Study Phase', 'Active', 7),
        ('SS Completed, FIS Started, IA', 'Study Phase', 'Active', 6),
        ('SS Completed, FIS Completed, No IA', 'Study Phase', 'Active', 5),
        ('SS Started, FIS Started, No IA', 'Study Phase', 'Active', 3),
        ('SS Started, FIS Started, IA', 'Study Phase', 'Active', 3),
        ('SS Completed, FIS Not Started, IA', 'Study Phase', 'Active', 5),
        # NYISO numeric status codes (from NYISO Interconnection Queue)
        ('1.0', 'NYISO Scoping', 'Active', 1),           # Scoping Meeting Pending
        ('2.0', 'NYISO SRIS', 'Active', 2),              # SRIS in Progress
        ('3.0', 'NYISO SRIS Complete', 'Active', 3),     # SRIS Complete
        ('4.0', 'NYISO FS', 'Active', 4),                # Facilities Study
        ('5.0', 'NYISO FS Complete', 'Active', 5),       # Facilities Study Complete
        ('6.0', 'NYISO SIS', 'Active', 6),               # System Impact Study
        ('7.0', 'NYISO SIS Complete', 'Active', 7),      # System Impact Study Complete
        ('8.0', 'NYISO IA Negotiation', 'Active', 8),    # IA Negotiation
        ('9.0', 'NYISO IA Pending', 'Active', 9),        # IA Pending
        ('10.0', 'NYISO IA Executed', 'Active', 10),     # IA Executed
        ('11.0', 'NYISO In Service', 'Completed', 11),   # In Service
        ('12.0', 'NYISO Withdrawn', 'Withdrawn', 20),    # Withdrawn
        # Completion statuses
        ('Operational', 'Operational', 'Completed', 10),
        ('operational', 'Operational', 'Completed', 10),
        ('Completed', 'Completed', 'Completed', 10),
        ('In Service', 'In Service', 'Completed', 10),
        ('Done', 'Done', 'Active', 8),  # MISO: studies complete, awaiting COD - still active
        ('LEGACY: Done', 'Done', 'Active', 8),
        # Suspended/Withdrawn
        ('Suspended', 'Suspended', 'Suspended', 15),
        ('suspended', 'Suspended', 'Suspended', 15),
        ('Withdrawn', 'Withdrawn', 'Withdrawn', 20),
        ('withdrawn', 'Withdrawn', 'Withdrawn', 20),
        ('LEGACY: Archived', 'Archived', 'Withdrawn', 20),
        # Unknown/Unmapped (NOT Active)
        ('None', 'None', 'Unknown', 25),
        ('unknown', 'Unknown', 'Unknown', 25),
        ('_UNMAPPED_', 'Unmapped Status', 'Unknown', 25),
    ]
    cursor_v2.executemany("""
        INSERT OR IGNORE INTO dim_statuses (status_code, status_name, status_category, sort_order)
        VALUES (?, ?, ?, ?)
    """, statuses)

    conn_v2.commit()

    # Build lookups
    cursor_v2.execute("SELECT region_id, region_code FROM dim_regions")
    region_lookup = {row[1]: row[0] for row in cursor_v2.fetchall()}

    cursor_v2.execute("SELECT technology_id, technology_code FROM dim_technologies")
    tech_lookup = {row[1].lower(): row[0] for row in cursor_v2.fetchall()}

    cursor_v2.execute("SELECT status_id, status_code FROM dim_statuses")
    status_lookup = {row[1]: row[0] for row in cursor_v2.fetchall()}

    # =========================================================================
    # DEVELOPERS
    # =========================================================================

    developers = pd.read_sql("""
        SELECT DISTINCT TRIM(developer) as dev
        FROM projects
        WHERE developer IS NOT NULL AND TRIM(developer) != ''
        AND developer NOT IN ('None', 'nan', 'N/A')
    """, conn_v1)

    seen = {}
    for d in developers['dev']:
        key = d.lower().strip()
        if key not in seen:
            seen[key] = d

    for canonical in seen.values():
        cursor_v2.execute(
            "INSERT OR IGNORE INTO dim_developers (canonical_name, display_name) VALUES (?, ?)",
            (canonical, canonical)
        )
    conn_v2.commit()

    cursor_v2.execute("SELECT developer_id, canonical_name FROM dim_developers")
    dev_lookup = {row[1].lower(): row[0] for row in cursor_v2.fetchall()}

    # =========================================================================
    # LOCATIONS
    # =========================================================================

    locations = pd.read_sql("""
        SELECT DISTINCT state, county
        FROM projects
        WHERE state IS NOT NULL AND TRIM(state) != ''
    """, conn_v1)

    for _, row in locations.iterrows():
        state = row['state']
        county = row['county'] if pd.notna(row['county']) else None
        if state and len(str(state)) <= 5:
            cursor_v2.execute(
                "INSERT OR IGNORE INTO dim_locations (state, county) VALUES (?, ?)",
                (str(state).upper(), county)
            )
    conn_v2.commit()

    cursor_v2.execute("SELECT location_id, state, county FROM dim_locations")
    loc_lookup = {}
    for row in cursor_v2.fetchall():
        key = (row[1], row[2] if row[2] else '')
        loc_lookup[key] = row[0]

    # =========================================================================
    # PROJECTS
    # =========================================================================

    projects = pd.read_sql("""
        SELECT queue_id, region, name, developer, capacity_mw, type, status,
               state, county, poi, queue_date, cod, source
        FROM projects
        WHERE queue_id IS NOT NULL AND queue_id != ''
    """, conn_v1)

    # Deduplication
    def completeness_score(row):
        score = 0
        if pd.notna(row['name']) and row['name']: score += 1
        if pd.notna(row['developer']) and row['developer']: score += 2
        if pd.notna(row['capacity_mw']) and row['capacity_mw'] > 0: score += 1
        if pd.notna(row['queue_date']) and row['queue_date']: score += 1
        if row['source'] in ['miso_api', 'ercot', 'nyiso', 'caiso', 'spp', 'isone']: score += 3
        return score

    projects['score'] = projects.apply(completeness_score, axis=1)
    projects = projects.sort_values('score', ascending=False)
    projects = projects.drop_duplicates(subset=['queue_id', 'region'], keep='first')

    # Mapping functions
    def map_technology(type_val):
        if pd.isna(type_val) or not type_val:
            return tech_lookup.get('other')
        t = str(type_val).lower().strip()
        if t in tech_lookup:
            return tech_lookup[t]
        if 'solar' in t and 'storage' in t:
            return tech_lookup.get('solar + storage')
        if 'wind' in t and 'storage' in t:
            return tech_lookup.get('wind + storage')
        if 'offshore' in t:
            return tech_lookup.get('offshore wind')
        if 'solar' in t or 'pv' in t:
            return tech_lookup.get('solar')
        if 'wind' in t:
            return tech_lookup.get('wind')
        if 'battery' in t or 'storage' in t or 'bess' in t:
            return tech_lookup.get('storage')
        if 'gas' in t or 'ng' in t or 'ct' in t or 'cc' in t:
            return tech_lookup.get('gas')
        if 'coal' in t:
            return tech_lookup.get('coal')
        if 'nuclear' in t:
            return tech_lookup.get('nuclear')
        if 'hydro' in t:
            return tech_lookup.get('hydro')
        return tech_lookup.get('other')

    def map_status(status_val):
        """
        Map status values to standardized status IDs.

        IMPORTANT: Unknown statuses are now mapped to 'Unknown' category,
        NOT 'Active'. This prevents unknown statuses from inflating active counts.
        """
        global UNKNOWN_STATUSES
        if pd.isna(status_val) or not status_val:
            UNKNOWN_STATUSES['NULL/Empty'] += 1
            return status_lookup.get('_UNMAPPED_')

        s = str(status_val).strip()

        # Direct lookup
        if s in status_lookup:
            return status_lookup[s]

        # Case-insensitive lookup
        s_lower = s.lower()
        for key in status_lookup:
            if key.lower() == s_lower:
                return status_lookup[key]

        # Handle numeric status codes (NYISO uses floats like 11.0)
        try:
            num = float(s)
            # Format as "X.0" to match NYISO codes in status_lookup
            numeric_key = f"{num:.1f}"
            if numeric_key in status_lookup:
                return status_lookup[numeric_key]
        except ValueError:
            pass

        # Common status variations
        if s_lower in ['withdrawn', 'cancelled', 'canceled', 'cancelled by applicant']:
            return status_lookup.get('Withdrawn')
        if s_lower in ['operational', 'completed', 'done', 'in-service', 'in service', 'commercial operation']:
            return status_lookup.get('Operational')
        if s_lower in ['suspended', 'on hold', 'on-hold']:
            return status_lookup.get('Suspended')
        # Active-like statuses that should be counted as active
        if s_lower in ['in progress', 'under review', 'pending', 'queued', 'study', 'under study']:
            return status_lookup.get('Active')

        # Log unknown status and map to Unknown category (NOT Active)
        UNKNOWN_STATUSES[s] += 1
        return status_lookup.get('_UNMAPPED_')

    # Insert
    inserted = 0
    for _, row in projects.iterrows():
        region_id = region_lookup.get(row['region'])
        if not region_id:
            continue

        dev_id = None
        if pd.notna(row['developer']) and row['developer']:
            dev_key = str(row['developer']).lower().strip()
            dev_id = dev_lookup.get(dev_key)

        loc_id = None
        if pd.notna(row['state']) and row['state']:
            state = str(row['state']).upper()
            county = str(row['county']) if pd.notna(row['county']) else ''
            loc_id = loc_lookup.get((state, county)) or loc_lookup.get((state, ''))

        tech_id = map_technology(row['type'])
        status_id = map_status(row['status'])

        queue_date = None
        if pd.notna(row['queue_date']) and row['queue_date']:
            try:
                queue_date = pd.to_datetime(row['queue_date']).strftime('%Y-%m-%d')
            except:
                pass

        cod = None
        if pd.notna(row['cod']) and row['cod']:
            try:
                cod = pd.to_datetime(row['cod']).strftime('%Y-%m-%d')
            except:
                pass

        capacity = row['capacity_mw'] if pd.notna(row['capacity_mw']) else None

        try:
            cursor_v2.execute("""
                INSERT INTO fact_projects
                (queue_id, region_id, project_name, developer_id, location_id,
                 technology_id, status_id, capacity_mw, queue_date, cod_proposed,
                 data_source, last_updated_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['queue_id'], region_id, row['name'], dev_id, loc_id,
                tech_id, status_id, capacity, queue_date, cod, row['source'],
                datetime.now().strftime('%Y-%m-%d')
            ))
            inserted += 1
        except:
            pass

    conn_v2.commit()

    if not quiet:
        print(f"  Projects migrated: {inserted:,}")

    # Summary
    query = """
    SELECT
        r.region_code as region,
        COUNT(*) as total,
        SUM(CASE WHEN s.status_category = 'Active' THEN 1 ELSE 0 END) as active,
        ROUND(SUM(CASE WHEN s.status_category = 'Active' THEN p.capacity_mw ELSE 0 END)/1000, 1) as active_gw
    FROM fact_projects p
    JOIN dim_regions r ON p.region_id = r.region_id
    LEFT JOIN dim_statuses s ON p.status_id = s.status_id
    GROUP BY r.region_code
    ORDER BY active_gw DESC
    """
    result = pd.read_sql(query, conn_v2)

    if not quiet:
        print("\n  V2 Regional Summary:")
        print(result.to_string(index=False))
        print(f"\n  Total Active: {result['active_gw'].sum():.1f} GW")

    conn_v1.close()
    conn_v2.close()

    # Record changes compared to previous snapshot
    if not dry_run and old_snapshot:
        if not quiet:
            print("\n  Change tracking:")
        record_project_changes(old_snapshot, quiet=quiet)

    return inserted


def show_change_log(days=7):
    """Show recent changes from the change log."""
    import json
    from datetime import datetime, timedelta

    log_path = TOOLS_DIR / '.data' / 'change_log.json'
    if not log_path.exists():
        print("No change log found. Run a refresh first.")
        return

    with open(log_path) as f:
        logs = json.load(f)

    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    print("=" * 60)
    print(f"CHANGE LOG (Last {days} days)")
    print("=" * 60)

    recent = [l for l in logs if l['date'] >= cutoff]
    if not recent:
        print(f"\nNo changes recorded in the last {days} days.")
        return

    for log in recent:
        print(f"\n{log['date']}:")
        stats = log['stats']
        print(f"  New projects: {stats.get('new_projects', 0)}")
        print(f"  Status changes: {stats.get('status_changes', 0)}")
        print(f"  Capacity changes: {stats.get('capacity_changes', 0)}")
        print(f"  Removed: {stats.get('removed', 0)}")

        if log.get('changes'):
            print(f"\n  Sample changes:")
            for change in log['changes'][:5]:
                print(f"    [{change['region']}] {change['queue_id']}: {', '.join(change['changed_fields'])}")
                if 'status' in change['changed_fields']:
                    print(f"      Status: {change['old_status']} -> {change['new_status']}")


def main():
    parser = argparse.ArgumentParser(description='Refresh V2 Database')
    parser.add_argument('--quick', action='store_true', help='Skip source refresh, just rebuild V2')
    parser.add_argument('--sync-only', action='store_true', help='Only sync stale records')
    parser.add_argument('--cron', action='store_true', help='Cron-friendly quiet mode')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without committing')
    parser.add_argument('--changes', type=int, metavar='DAYS', help='Show change log for last N days')

    args = parser.parse_args()

    # Handle --changes flag
    if args.changes:
        show_change_log(args.changes)
        return 0
    quiet = args.cron
    dry_run = args.dry_run

    if not quiet:
        print("=" * 70)
        if dry_run:
            print("V2 DATABASE REFRESH (DRY RUN)")
        else:
            print("V2 DATABASE REFRESH")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

    try:
        # Step 1: Refresh source data (unless --quick or --dry-run)
        if not args.quick and not args.sync_only and not dry_run:
            if not quiet:
                print("\n--- Refreshing Source Data ---")
            from refresh_data import DataRefresher
            refresher = DataRefresher()
            refresher.refresh_all(quiet=quiet)

        # Step 2: Sync stale records
        if not args.sync_only or args.sync_only:
            sync_stale_records(quiet=quiet, dry_run=dry_run)

        # Step 3: Rebuild V2 (unless --sync-only)
        if not args.sync_only:
            rebuild_v2(quiet=quiet, dry_run=dry_run)

        # Report unknown statuses
        if UNKNOWN_STATUSES and not quiet:
            print("\n--- Unknown Statuses Encountered ---")
            for status, count in sorted(UNKNOWN_STATUSES.items(), key=lambda x: -x[1]):
                print(f"  {status}: {count} occurrences (mapped to Active)")

        if not quiet:
            print("\n" + "=" * 70)
            if dry_run:
                print("DRY RUN COMPLETE - No changes committed")
            else:
                print("REFRESH COMPLETE")
            print("=" * 70)

        return 0

    except Exception as e:
        if quiet:
            print(f"ERROR: {e}", file=sys.stderr)
        else:
            print(f"\nERROR: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
