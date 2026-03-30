#!/usr/bin/env python3
"""
Build and maintain developer.db — standalone developer entity database.

Aggregates developer data from master.db and dg.db into a dedicated database
for tracking developer metrics, relationships, tier classification, and
capital needs assessment.

Usage:
    python3 build_developer_db.py --build          # Full build from scratch
    python3 build_developer_db.py --update         # Incremental update metrics
    python3 build_developer_db.py --stats          # Show developer stats
    python3 build_developer_db.py --preview        # Preview top developers
    python3 build_developer_db.py --classify       # Run tier classification
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get('DATA_DIR', str(Path(__file__).parent / '.data')))
MASTER_DB = Path(os.environ.get('QUEUE_DB_PATH', os.environ.get('MASTER_DB_PATH', str(DATA_DIR / 'master.db'))))
DG_DB = Path(os.environ.get('DG_DB_PATH', str(DATA_DIR / 'dg.db')))
DEV_DB = Path(os.environ.get('DEVELOPER_DB_PATH', str(DATA_DIR / 'developer.db')))
CORPORATE_DB_DEFAULT = Path(__file__).parent.parent.parent.parent / 'prospector-platform' / 'pipelines' / 'databases' / 'corporate.db'
CORPORATE_DB = Path(os.environ.get('CORPORATE_DB_PATH', str(CORPORATE_DB_DEFAULT)))
# On Railway, DATA_DIR=/data/ and individual paths are set via env vars

# ============================================================================
# Schema
# ============================================================================

SCHEMA_SQL = """
-- Core developer entity
CREATE TABLE IF NOT EXISTS developers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name TEXT UNIQUE NOT NULL,
    display_name TEXT,
    parent_company TEXT,
    tier TEXT,                              -- major_utility, major_ipp, mid_platform, pe_fund, independent, unknown
    tier_confidence REAL,
    tier_method TEXT,

    -- Company info
    website TEXT,
    headquarters_state TEXT,
    founded_year INTEGER,

    -- Portfolio metrics (utility-scale from master.db)
    total_projects INTEGER DEFAULT 0,
    active_projects INTEGER DEFAULT 0,
    operational_projects INTEGER DEFAULT 0,
    withdrawn_projects INTEGER DEFAULT 0,
    suspended_projects INTEGER DEFAULT 0,
    total_capacity_mw REAL DEFAULT 0,
    active_capacity_mw REAL DEFAULT 0,
    operational_capacity_mw REAL DEFAULT 0,

    -- DG metrics (from dg.db)
    dg_total_projects INTEGER DEFAULT 0,
    dg_total_capacity_kw REAL DEFAULT 0,

    -- Geographic spread
    regions TEXT,                           -- JSON array
    states TEXT,                            -- JSON array
    region_count INTEGER DEFAULT 0,
    state_count INTEGER DEFAULT 0,

    -- Technology mix
    primary_technology TEXT,
    technology_mix TEXT,                    -- JSON: {"Solar": 150, "Wind": 80, ...}

    -- Performance metrics
    completion_rate REAL,                  -- operational / (operational + withdrawn)
    withdrawal_rate REAL,                  -- withdrawn / total
    avg_project_size_mw REAL,
    median_project_size_mw REAL,
    avg_time_to_cod_days INTEGER,

    -- Capital assessment
    needs_capital INTEGER,                 -- 0/1/NULL
    capital_evidence TEXT,                 -- JSON

    -- Timestamps
    first_queue_date TEXT,                 -- earliest queue_date across projects
    last_updated TEXT,                     -- most recent project updated_at
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

-- Name aliases (many raw names → one canonical)
CREATE TABLE IF NOT EXISTS developer_aliases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alias TEXT NOT NULL,
    developer_id INTEGER REFERENCES developers(id),
    source TEXT,                            -- master_db, dg_db, manual
    project_count INTEGER DEFAULT 0,
    UNIQUE(alias, developer_id)
);

-- Parent/subsidiary relationships
CREATE TABLE IF NOT EXISTS developer_relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id INTEGER REFERENCES developers(id),
    child_id INTEGER REFERENCES developers(id),
    relationship_type TEXT DEFAULT 'subsidiary',  -- subsidiary, acquired_by, joint_venture
    source TEXT,                            -- corporate_db, registry, manual
    UNIQUE(parent_id, child_id)
);

-- Per-region breakdown
CREATE TABLE IF NOT EXISTS developer_regions (
    developer_id INTEGER REFERENCES developers(id),
    region TEXT NOT NULL,
    total_projects INTEGER DEFAULT 0,
    active_projects INTEGER DEFAULT 0,
    operational_projects INTEGER DEFAULT 0,
    total_capacity_mw REAL DEFAULT 0,
    PRIMARY KEY (developer_id, region)
);

-- Per-technology breakdown
CREATE TABLE IF NOT EXISTS developer_technologies (
    developer_id INTEGER REFERENCES developers(id),
    technology TEXT NOT NULL,
    total_projects INTEGER DEFAULT 0,
    total_capacity_mw REAL DEFAULT 0,
    PRIMARY KEY (developer_id, technology)
);

-- Historical snapshots (for tracking portfolio changes over time)
CREATE TABLE IF NOT EXISTS developer_snapshots (
    developer_id INTEGER REFERENCES developers(id),
    snapshot_date TEXT NOT NULL,
    total_projects INTEGER,
    active_projects INTEGER,
    operational_projects INTEGER,
    total_capacity_mw REAL,
    completion_rate REAL,
    PRIMARY KEY (developer_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_dev_tier ON developers(tier);
CREATE INDEX IF NOT EXISTS idx_dev_parent ON developers(parent_company);
CREATE INDEX IF NOT EXISTS idx_dev_needs_capital ON developers(needs_capital);
CREATE INDEX IF NOT EXISTS idx_dev_total_projects ON developers(total_projects);
CREATE INDEX IF NOT EXISTS idx_alias_name ON developer_aliases(alias);
"""

# ============================================================================
# Tier classification
# ============================================================================

MAJOR_UTILITIES = {
    'entergy', 'dominion energy', 'berkshire hathaway energy', 'duke energy',
    'southern company', 'ameren', 'xcel energy', 'aep', 'american electric power',
    'firstenergy', 'evergy', 'alliant energy', 'oge energy', 'consumers energy',
    'dte energy', 'eversource', 'national grid', 'ppl', 'pseg', 'sempra energy',
    'edison international', 'pacific gas and electric', 'pg&e', 'exelon',
    'commonwealth edison', 'peco energy', 'baltimore gas and electric',
    'consolidated edison', 'con edison', 'we energies', 'wisconsin energy',
    'centerpoint energy', 'entergy arkansas', 'entergy louisiana',
    'entergy mississippi', 'entergy texas', 'alabama power', 'georgia power',
    'gulf power', 'mississippi power', 'virginia electric and power',
    'duke energy indiana', 'duke energy progress', 'duke energy carolinas',
    'pacificorp', 'rocky mountain power', 'midamerican energy',
    'idaho power', 'portland general electric', 'puget sound energy',
    'tucson electric power', 'arizona public service', 'aps',
    'nextera energy', 'fpl', 'tampa electric', 'teco energy',
}

MAJOR_IPPS = {
    'invenergy', 'avangrid', 'avangrid power', 'enel', 'enel green power',
    'edf renewables', 'orsted', 'ørsted', 'aes', 'aes clean energy',
    'clearway energy', 'pattern energy', 'longroad energy', 'savion',
    'lightsource bp', 'bp solar', 'shell new energies', 'shell',
    'totalenergies', 'total energies', 'equinor', 'rwe', 'rwe renewables',
    'engie', 'engie north america', 'iberdrola', 'edp renewables',
    'canadian solar', 'recurrent energy', 'innergex', 'boralex',
    'northland power', 'capital power', 'transalta', 'algonquin power',
}

MID_PLATFORMS = {
    'cypress creek', 'cypress creek renewables', 'pine gate renewables',
    'silicon ranch', 'summit ridge energy', 'arevon', 'leeward renewable energy',
    'leeward renewables', 'geronimo energy', 'apex clean energy',
    'ranger power', 'origis energy', 'sol systems', 'us solar',
    'community energy', 'core solar', 'lightsource', 'open road renewables',
    'tri global energy', '174 power global', 'hanwha', 'hanwha energy',
    'canadian solar development', 'swift current energy',
    'hecate energy', 'terra-gen', 'terra gen', 'calpine',
    'vistra energy', 'vistra', 'nrg energy', 'nrg',
    'tenaska', 'intersect power', 'onward energy',
}

PE_FUNDS = {
    'brookfield', 'brookfield renewable', 'brookfield asset management',
    'kkr', 'arclight capital', 'stonepeak', 'ls power',
    'blackrock', 'global infrastructure partners', 'gip',
    'ares management', 'apollo global', 'carlyle',
    'energy capital partners', 'i squared capital',
    'actis', 'macquarie', 'macquarie group',
}


def classify_tier(canonical_name: str, parent_company: str = None,
                  project_count: int = 0) -> tuple:
    """Classify developer into tier.

    Returns (tier, confidence, method).
    """
    name_lower = canonical_name.lower().strip()
    parent_lower = (parent_company or '').lower().strip()

    # Direct match against known sets
    for name_set, tier in [
        (MAJOR_UTILITIES, 'major_utility'),
        (MAJOR_IPPS, 'major_ipp'),
        (MID_PLATFORMS, 'mid_platform'),
        (PE_FUNDS, 'pe_fund'),
    ]:
        if name_lower in name_set:
            return (tier, 0.95, 'direct_match')
        if parent_lower and parent_lower in name_set:
            return (tier, 0.90, 'parent_match')

    # Partial match (developer name contains a known entity)
    for name_set, tier in [
        (MAJOR_UTILITIES, 'major_utility'),
        (MAJOR_IPPS, 'major_ipp'),
        (MID_PLATFORMS, 'mid_platform'),
        (PE_FUNDS, 'pe_fund'),
    ]:
        for known in name_set:
            if known in name_lower or name_lower in known:
                return (tier, 0.80, 'partial_match')
            if parent_lower and (known in parent_lower or parent_lower in known):
                return (tier, 0.75, 'parent_partial')

    # Heuristic based on project count
    if project_count >= 50:
        return ('mid_platform', 0.60, 'size_heuristic')
    if project_count >= 20:
        return ('independent', 0.50, 'size_heuristic')

    if not canonical_name or canonical_name.strip() == '':
        return ('unknown', 0.0, 'no_data')

    return ('independent', 0.40, 'default')


# ============================================================================
# Build logic
# ============================================================================

def init_db():
    """Create developer.db with schema."""
    conn = sqlite3.connect(DEV_DB)
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    conn.close()
    logger.info(f"Initialized developer.db at {DEV_DB}")


def build_from_master():
    """Build developer entities from master.db project data."""
    logger.info("\n[1/4] Loading project data from master.db...")

    master = sqlite3.connect(MASTER_DB)
    master.row_factory = sqlite3.Row

    rows = master.execute("""
        SELECT developer_canonical, developer, parent_company,
               COALESCE(status_std, status) as status,
               COALESCE(type_std, type) as tech,
               capacity_mw, region, state, source,
               queue_date_std, updated_at
        FROM projects
        WHERE developer_canonical IS NOT NULL
    """).fetchall()

    logger.info(f"  Loaded {len(rows):,} projects with developer data")

    # Aggregate by developer
    devs = defaultdict(lambda: {
        'raw_names': set(),
        'parent': None,
        'projects': [],
        'regions': set(),
        'states': set(),
        'techs': defaultdict(lambda: {'count': 0, 'mw': 0}),
        'region_stats': defaultdict(lambda: {'total': 0, 'active': 0, 'operational': 0, 'mw': 0}),
        'statuses': defaultdict(int),
        'capacities': [],
        'queue_dates': [],
        'updated_ats': [],
    })

    for row in rows:
        d = devs[row['developer_canonical']]
        if row['developer']:
            d['raw_names'].add(row['developer'])
        if row['parent_company'] and not d['parent']:
            d['parent'] = row['parent_company']

        status = row['status'] or 'Unknown'
        tech = row['tech'] or 'Unknown'
        mw = row['capacity_mw'] or 0
        region = row['region'] or 'Unknown'

        d['projects'].append(row)
        d['regions'].add(region)
        if row['state']:
            d['states'].add(row['state'])
        d['techs'][tech]['count'] += 1
        d['techs'][tech]['mw'] += mw
        d['statuses'][status] += 1
        d['capacities'].append(mw)
        d['region_stats'][region]['total'] += 1
        d['region_stats'][region]['mw'] += mw
        if status == 'Active':
            d['region_stats'][region]['active'] += 1
        elif status == 'Operational':
            d['region_stats'][region]['operational'] += 1
        if row['queue_date_std']:
            d['queue_dates'].append(row['queue_date_std'])
        if row['updated_at']:
            d['updated_ats'].append(row['updated_at'])

    master.close()
    logger.info(f"  Aggregated {len(devs):,} unique developers")
    return devs


def load_dg_developers():
    """Load developer/installer counts from dg.db."""
    if not DG_DB.exists():
        logger.info("  dg.db not found, skipping DG data")
        return {}

    conn = sqlite3.connect(DG_DB)
    # Get developer aggregates from DG
    rows = conn.execute("""
        SELECT developer, COUNT(*) as cnt, SUM(capacity_kw) as kw
        FROM projects
        WHERE developer IS NOT NULL AND developer != ''
        GROUP BY developer
    """).fetchall()
    conn.close()

    dg_data = {}
    for name, cnt, kw in rows:
        dg_data[name.strip().lower()] = {'count': cnt, 'kw': kw or 0}

    logger.info(f"  Loaded {len(dg_data):,} DG developers/installers")
    return dg_data


def load_corporate_relationships():
    """Load parent/subsidiary from corporate.db."""
    if not CORPORATE_DB.exists():
        logger.info("  corporate.db not found, skipping corporate data")
        return {}

    conn = sqlite3.connect(CORPORATE_DB)
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]

    relationships = {}
    if 'parents_and_subsidiaries' in tables:
        rows = conn.execute("""
            SELECT subsidiary_company_name, parent_company_name
            FROM parents_and_subsidiaries
            WHERE subsidiary_company_name IS NOT NULL AND parent_company_name IS NOT NULL
            LIMIT 50000
        """).fetchall()
        for sub, parent in rows:
            relationships[sub.strip().lower()] = parent.strip()
        logger.info(f"  Loaded {len(relationships):,} corporate relationships")
    conn.close()
    return relationships


def populate_db(devs: dict, dg_data: dict, corporate: dict):
    """Write developer entities to developer.db."""
    logger.info("\n[3/4] Writing to developer.db...")

    conn = sqlite3.connect(DEV_DB)
    conn.execute("DELETE FROM developer_technologies")
    conn.execute("DELETE FROM developer_regions")
    conn.execute("DELETE FROM developer_aliases")
    conn.execute("DELETE FROM developers")
    conn.commit()

    now = datetime.now().isoformat()
    dev_count = 0
    alias_count = 0

    for canonical, data in devs.items():
        total = len(data['projects'])
        operational = data['statuses'].get('Operational', 0)
        withdrawn = data['statuses'].get('Withdrawn', 0)
        active = data['statuses'].get('Active', 0)
        suspended = data['statuses'].get('Suspended', 0)
        total_mw = sum(data['capacities'])
        active_mw = sum(p['capacity_mw'] or 0 for p in data['projects']
                       if (p['status'] or '') == 'Active')
        op_mw = sum(p['capacity_mw'] or 0 for p in data['projects']
                   if (p['status'] or '') == 'Operational')

        # Completion rate
        decided = operational + withdrawn
        comp_rate = operational / decided if decided > 0 else None
        wd_rate = withdrawn / total if total > 0 else None

        # Avg project size
        avg_mw = total_mw / total if total > 0 else None
        median_mw = sorted(data['capacities'])[len(data['capacities']) // 2] if data['capacities'] else None

        # Technology mix
        tech_mix = {k: v['count'] for k, v in data['techs'].items()}
        primary_tech = max(tech_mix, key=tech_mix.get) if tech_mix else None

        # DG data
        dg_key = canonical.lower()
        dg_info = dg_data.get(dg_key, {'count': 0, 'kw': 0})

        # Tier classification
        tier, tier_conf, tier_method = classify_tier(
            canonical, data['parent'], total
        )

        # Check corporate.db for parent if we don't have one
        parent = data['parent']
        if not parent:
            corp_parent = corporate.get(canonical.lower())
            if corp_parent:
                parent = corp_parent

        # Headquarters state (most common state)
        state_counts = defaultdict(int)
        for p in data['projects']:
            if p['state']:
                state_counts[p['state']] += 1
        hq_state = max(state_counts, key=state_counts.get) if state_counts else None

        cursor = conn.execute("""
            INSERT INTO developers (
                canonical_name, display_name, parent_company,
                tier, tier_confidence, tier_method,
                headquarters_state,
                total_projects, active_projects, operational_projects,
                withdrawn_projects, suspended_projects,
                total_capacity_mw, active_capacity_mw, operational_capacity_mw,
                dg_total_projects, dg_total_capacity_kw,
                regions, states, region_count, state_count,
                primary_technology, technology_mix,
                completion_rate, withdrawal_rate,
                avg_project_size_mw, median_project_size_mw,
                needs_capital,
                first_queue_date, last_updated,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            canonical, canonical, parent,
            tier, tier_conf, tier_method,
            hq_state,
            total, active, operational, withdrawn, suspended,
            round(total_mw, 2), round(active_mw, 2), round(op_mw, 2),
            dg_info['count'], round(dg_info['kw'], 2),
            json.dumps(sorted(data['regions'])),
            json.dumps(sorted(data['states'])),
            len(data['regions']), len(data['states']),
            primary_tech, json.dumps(tech_mix),
            round(comp_rate, 4) if comp_rate is not None else None,
            round(wd_rate, 4) if wd_rate is not None else None,
            round(avg_mw, 2) if avg_mw is not None else None,
            round(median_mw, 2) if median_mw is not None else None,
            None,  # needs_capital — filled later by classification
            min(data['queue_dates']) if data['queue_dates'] else None,
            max(data['updated_ats']) if data['updated_ats'] else None,
            now, now,
        ))

        dev_id = cursor.lastrowid
        dev_count += 1

        # Aliases
        for raw_name in data['raw_names']:
            if raw_name != canonical:
                raw_count = sum(1 for p in data['projects'] if p['developer'] == raw_name)
                conn.execute("""
                    INSERT OR IGNORE INTO developer_aliases (alias, developer_id, source, project_count)
                    VALUES (?, ?, 'master_db', ?)
                """, (raw_name, dev_id, raw_count))
                alias_count += 1

        # Region breakdown
        for region, stats in data['region_stats'].items():
            conn.execute("""
                INSERT INTO developer_regions (developer_id, region, total_projects, active_projects, operational_projects, total_capacity_mw)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (dev_id, region, stats['total'], stats['active'], stats['operational'], round(stats['mw'], 2)))

        # Technology breakdown
        for tech, stats in data['techs'].items():
            conn.execute("""
                INSERT INTO developer_technologies (developer_id, technology, total_projects, total_capacity_mw)
                VALUES (?, ?, ?, ?)
            """, (dev_id, tech, stats['count'], round(stats['mw'], 2)))

    conn.commit()
    conn.close()

    logger.info(f"  Wrote {dev_count:,} developers, {alias_count:,} aliases")


def create_snapshot():
    """Create a snapshot of current developer metrics."""
    conn = sqlite3.connect(DEV_DB)
    today = datetime.now().strftime('%Y-%m-%d')

    conn.execute("""
        INSERT OR REPLACE INTO developer_snapshots
        (developer_id, snapshot_date, total_projects, active_projects,
         operational_projects, total_capacity_mw, completion_rate)
        SELECT id, ?, total_projects, active_projects, operational_projects,
               total_capacity_mw, completion_rate
        FROM developers
    """, (today,))
    conn.commit()
    count = conn.execute("SELECT changes()").fetchone()[0]
    conn.close()
    logger.info(f"  Created snapshot for {count:,} developers (date: {today})")


def build():
    """Full build of developer.db from scratch."""
    logger.info("=" * 60)
    logger.info("BUILDING developer.db")
    logger.info("=" * 60)

    init_db()

    devs = build_from_master()

    logger.info("\n[2/4] Loading supplementary data...")
    dg_data = load_dg_developers()
    corporate = load_corporate_relationships()

    populate_db(devs, dg_data, corporate)

    logger.info("\n[4/4] Creating initial snapshot...")
    create_snapshot()

    # Final stats
    show_stats()


def classify_all():
    """Re-run tier classification on all developers."""
    conn = sqlite3.connect(DEV_DB)
    rows = conn.execute("""
        SELECT id, canonical_name, parent_company, total_projects
        FROM developers
    """).fetchall()

    updates = 0
    for dev_id, name, parent, total in rows:
        tier, conf, method = classify_tier(name, parent, total)
        conn.execute("""
            UPDATE developers SET tier = ?, tier_confidence = ?, tier_method = ?,
                   updated_at = datetime('now')
            WHERE id = ?
        """, (tier, conf, method, dev_id))
        updates += 1

    conn.commit()
    conn.close()
    logger.info(f"Classified {updates:,} developers")
    show_tier_stats()


def show_stats():
    """Show developer database statistics."""
    conn = sqlite3.connect(DEV_DB)

    total = conn.execute("SELECT COUNT(*) FROM developers").fetchone()[0]
    aliases = conn.execute("SELECT COUNT(*) FROM developer_aliases").fetchone()[0]
    relationships = conn.execute("SELECT COUNT(*) FROM developer_relationships").fetchone()[0]
    snapshots = conn.execute("SELECT COUNT(DISTINCT snapshot_date) FROM developer_snapshots").fetchone()[0]

    print(f"\n{'=' * 60}")
    print(f"DEVELOPER DATABASE STATS")
    print(f"{'=' * 60}")
    print(f"\nTotal developers:    {total:,}")
    print(f"Name aliases:        {aliases:,}")
    print(f"Relationships:       {relationships:,}")
    print(f"Snapshot dates:      {snapshots}")

    size_mb = os.path.getsize(DEV_DB) / 1024 / 1024
    print(f"Database size:       {size_mb:.1f} MB")

    show_tier_stats()

    # Top by project count
    print(f"\nTop 15 developers by project count:")
    rows = conn.execute("""
        SELECT canonical_name, tier, total_projects, operational_projects,
               withdrawn_projects, active_projects,
               ROUND(total_capacity_mw, 0) as mw,
               ROUND(completion_rate * 100, 1) as comp_pct,
               region_count, state_count
        FROM developers ORDER BY total_projects DESC LIMIT 15
    """).fetchall()
    print(f"  {'Developer':<35s} {'Tier':<15s} {'Total':>5s} {'Op':>4s} {'Wd':>4s} {'Act':>4s} {'MW':>9s} {'Comp%':>6s} {'Rgn':>3s} {'St':>3s}")
    print(f"  {'-'*35} {'-'*15} {'-'*5} {'-'*4} {'-'*4} {'-'*4} {'-'*9} {'-'*6} {'-'*3} {'-'*3}")
    for r in rows:
        comp = f"{r[7]:.1f}" if r[7] is not None else "N/A"
        print(f"  {r[0][:35]:<35s} {r[1]:<15s} {r[2]:>5d} {r[3]:>4d} {r[4]:>4d} {r[5]:>4d} {r[6]:>9,.0f} {comp:>6s} {r[8]:>3d} {r[9]:>3d}")

    conn.close()


def show_tier_stats():
    """Show tier distribution."""
    conn = sqlite3.connect(DEV_DB)
    print(f"\nTier distribution:")
    rows = conn.execute("""
        SELECT tier, COUNT(*) as cnt,
               SUM(total_projects) as projects,
               ROUND(SUM(total_capacity_mw), 0) as mw,
               ROUND(AVG(completion_rate) * 100, 1) as avg_comp
        FROM developers GROUP BY tier ORDER BY cnt DESC
    """).fetchall()
    print(f"  {'Tier':<15s} {'Devs':>6s} {'Projects':>8s} {'MW':>12s} {'Avg Comp%':>10s}")
    print(f"  {'-'*15} {'-'*6} {'-'*8} {'-'*12} {'-'*10}")
    for r in rows:
        avg_comp = f"{r[4]:.1f}" if r[4] is not None else "N/A"
        print(f"  {r[0]:<15s} {r[1]:>6,d} {r[2]:>8,d} {r[3]:>12,.0f} {avg_comp:>10s}")
    conn.close()


def preview():
    """Preview investable developers (independent with active projects)."""
    conn = sqlite3.connect(DEV_DB)
    print(f"\n{'=' * 60}")
    print(f"INDEPENDENT DEVELOPERS WITH ACTIVE PROJECTS")
    print(f"{'=' * 60}")

    rows = conn.execute("""
        SELECT canonical_name, total_projects, active_projects,
               active_capacity_mw, state_count,
               ROUND(completion_rate * 100, 1) as comp_pct,
               primary_technology, headquarters_state
        FROM developers
        WHERE tier = 'independent' AND active_projects > 0
        ORDER BY active_capacity_mw DESC
        LIMIT 30
    """).fetchall()

    print(f"\n  {'Developer':<35s} {'Tot':>4s} {'Act':>4s} {'Act MW':>8s} {'States':>6s} {'Comp%':>6s} {'Tech':<12s} {'HQ':>3s}")
    print(f"  {'-'*35} {'-'*4} {'-'*4} {'-'*8} {'-'*6} {'-'*6} {'-'*12} {'-'*3}")
    for r in rows:
        comp = f"{r[5]:.1f}" if r[5] is not None else "N/A"
        tech = (r[6] or 'Unknown')[:12]
        hq = r[7] or '??'
        print(f"  {r[0][:35]:<35s} {r[1]:>4d} {r[2]:>4d} {r[3]:>8,.1f} {r[4]:>6d} {comp:>6s} {tech:<12s} {hq:>3s}")

    total_ind = conn.execute("SELECT COUNT(*) FROM developers WHERE tier = 'independent'").fetchone()[0]
    active_ind = conn.execute("SELECT COUNT(*) FROM developers WHERE tier = 'independent' AND active_projects > 0").fetchone()[0]
    print(f"\n  Total independent: {total_ind:,} | With active projects: {active_ind:,}")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description='Build and manage developer.db')
    parser.add_argument('--build', action='store_true', help='Full build from scratch')
    parser.add_argument('--update', action='store_true', help='Incremental update metrics')
    parser.add_argument('--stats', action='store_true', help='Show developer stats')
    parser.add_argument('--preview', action='store_true', help='Preview investable developers')
    parser.add_argument('--classify', action='store_true', help='Re-run tier classification')

    args = parser.parse_args()

    if not any([args.build, args.update, args.stats, args.preview, args.classify]):
        parser.print_help()
        return

    if args.build:
        build()
    if args.update:
        build()  # For now, update = rebuild. Incremental can be added later.
    if args.classify:
        classify_all()
    if args.stats:
        show_stats()
    if args.preview:
        preview()


if __name__ == '__main__':
    main()
