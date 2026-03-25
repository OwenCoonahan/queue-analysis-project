#!/usr/bin/env python3
"""DG Development Stage Classifier.

Maps raw_status values from state DG programs to standardized development stages.
Analogous to construction_stage.py for interconnection queue projects.

Stages (ordered by progression):
  - applied:       Application submitted, not yet approved
  - approved:      Approved/accepted into program, pre-construction
  - construction:  Under construction or as-built submitted
  - inspection:    Physical inspection phase (near completion)
  - operational:   Fully operational / registration complete
  - withdrawn:     Cancelled, expired, or decertified
  - suspended:     On hold or failed inspection

Usage:
    python3 dg_stage.py --enrich          # Classify all DG projects with raw_status
    python3 dg_stage.py --stats           # Show stage distribution
    python3 dg_stage.py --preview         # Preview first 20 classifications
"""

import argparse
import json
import logging
import sqlite3
from pathlib import Path
from typing import Dict, Optional, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DG_DB_PATH = Path(__file__).parent / '.data' / 'dg.db'

# ── NJ raw_status → (dg_stage, confidence) ──────────────────────────────
# These are the ~25 granular statuses from NJ Clean Energy programs
NJ_STAGE_MAP = {
    # Operational
    'Registration Complete': ('operational', 1.0),
    'SRP Registration Complete': ('operational', 1.0),
    'TI Registration Complete': ('operational', 1.0),
    'ADI Registration Complete': ('operational', 1.0),
    'CSEP Registration Complete': ('operational', 1.0),
    'CSI Registration Complete': ('operational', 1.0),
    'RNM Registration Complete': ('operational', 1.0),
    'As Built Complete': ('operational', 0.95),
    'As-Built Complete': ('operational', 0.95),
    'Final As-Built Received': ('operational', 0.95),
    'Onsite Complete - Grid Supply': ('operational', 0.95),

    # Inspection — very close to operational
    'Verification Inspection': ('inspection', 0.9),
    'Onsite Inspection': ('inspection', 0.9),

    # Construction — as-built submitted but not yet complete
    'As Built Incomplete-Review': ('construction', 0.85),
    'As-Built Incomplete': ('construction', 0.85),
    'As-Built Incomplete - Review': ('construction', 0.85),

    # Approved — accepted into program, pre-construction
    'Registration Accepted': ('approved', 0.8),
    'Accepted': ('approved', 0.8),
    'Accepted Pending Verification': ('approved', 0.8),
    'Conditional Approval': ('approved', 0.75),
    'TI Application Complete': ('approved', 0.75),
    'TI Extension Request Incomplete - Review': ('approved', 0.7),
    'Public Entity': ('approved', 0.6),

    # Applied — early stage
    'Registration Received': ('applied', 0.8),
    'Registration Pending': ('applied', 0.8),
    'Registration On Hold-PTO Prior to Acceptance': ('applied', 0.7),
    'Registration On Hold-System Exceeds 20%': ('applied', 0.65),
    'Registration On Hold –PTO Prior August 28, 2021': ('applied', 0.65),

    # Withdrawn
    'Decertified': ('withdrawn', 1.0),
    'Expired': ('withdrawn', 1.0),
    'Cancelled': ('withdrawn', 1.0),
    'Withdrawn': ('withdrawn', 1.0),

    # Suspended
    'Verification Inspection Failed': ('suspended', 0.9),
    'Onsite Inspection Fail': ('suspended', 0.9),
}

# ── NY raw_status → (dg_stage, confidence) ──────────────────────────────
NY_STAGE_MAP = {
    'Complete': ('operational', 1.0),
    'Completed': ('operational', 1.0),
    'Installed': ('operational', 1.0),
    'Pipeline': ('applied', 0.6),  # Could be anywhere from applied to construction
    'Cancelled': ('withdrawn', 1.0),
    'Suspended': ('suspended', 0.9),
}

# ── NY DPS SIR raw_status → (dg_stage, confidence) ───────────────────
# These are synthetic statuses derived from IC milestone progression
# in ny_dps_sir_loader.py. The actual stage is computed from milestone
# dates directly (sir_milestone method), but these mappings serve as
# fallback when re-classifying from raw_status alone.
NY_DPS_SIR_STAGE_MAP = {
    'SIR Complete': ('operational', 1.0),
    'SIR Withdrawn': ('withdrawn', 1.0),
    'SIR Under Construction': ('construction', 0.90),
    'SIR CESIR Complete': ('approved', 0.85),
    'SIR CESIR In Progress': ('approved', 0.80),
    'SIR Approved': ('approved', 0.70),
    'SIR Review Complete': ('applied', 0.80),
    'SIR Application Received': ('applied', 0.75),
}

# ── MA SMART raw_status → (dg_stage, confidence) ─────────────────────
MA_SMART_STAGE_MAP = {
    'Approved': ('operational', 0.95),
    'Qualified': ('approved', 0.85),
    'Under Review': ('applied', 0.80),
    'Waitlist': ('applied', 0.70),
}

# ── IL Shines raw_status → (dg_stage, confidence) ────────────────────
# IL Shines raw_status is a composite string like "Part I: Verified | ICC Approved | Part II: InProgress"
# These are matched as substrings, so we handle the most specific patterns first.
IL_SHINES_STAGE_MAP = {
    'Energized': ('operational', 0.95),
    'Part II: Verified': ('operational', 0.90),
    'Part II: InProgress': ('construction', 0.80),
    'Part II: Submitted': ('construction', 0.80),
    'Part II: Need_Info': ('construction', 0.75),
    'ICC Approved': ('approved', 0.85),
    'Part I: Verified': ('approved', 0.75),
    'Part I: Submitted': ('applied', 0.80),
    'Part I: Need_Info': ('applied', 0.75),
    'Part I: NI_Unresponsive_AV': ('applied', 0.70),
}

# ── Combined map keyed by source ────────────────────────────────────────
SOURCE_STAGE_MAPS = {
    'nj_dg': NJ_STAGE_MAP,
    'ny_sun': NY_STAGE_MAP,
    'ny_dps_sir': NY_DPS_SIR_STAGE_MAP,
    'ma_smart': MA_SMART_STAGE_MAP,
    # il_shines uses substring matching (handled in classify_dg_stage)
}

# Fallback: classify from normalized status when no raw_status available
NORMALIZED_STAGE_MAP = {
    'Operational': ('operational', 0.9),
    'Active': ('applied', 0.3),  # Low confidence — could be any pre-operational stage
    'Withdrawn': ('withdrawn', 0.9),
    'Suspended': ('suspended', 0.9),
}


def classify_dg_stage(
    raw_status: Optional[str],
    normalized_status: Optional[str],
    source: Optional[str],
) -> Tuple[str, float, str]:
    """Classify a DG project's development stage.

    Returns: (stage, confidence, method)
    """
    # Try raw_status with source-specific map first
    if raw_status and source and source in SOURCE_STAGE_MAPS:
        stage_map = SOURCE_STAGE_MAPS[source]
        if raw_status in stage_map:
            stage, conf = stage_map[raw_status]
            return stage, conf, 'raw_status'

    # IL Shines uses composite raw_status strings — match by substring
    if raw_status and (source == 'il_shines' or source is None):
        for pattern, (stage, conf) in IL_SHINES_STAGE_MAP.items():
            if pattern in raw_status:
                return stage, conf, 'raw_status'

    # Try raw_status across all maps (in case source is wrong/missing)
    if raw_status:
        for src, stage_map in SOURCE_STAGE_MAPS.items():
            if raw_status in stage_map:
                stage, conf = stage_map[raw_status]
                return stage, conf * 0.9, 'raw_status_cross'

    # Fall back to normalized status
    if normalized_status and normalized_status in NORMALIZED_STAGE_MAP:
        stage, conf = NORMALIZED_STAGE_MAP[normalized_status]
        return stage, conf, 'normalized_status'

    return 'unknown', 0.0, 'none'


def enrich_dg_stages(db_path: Path = DG_DB_PATH, save: bool = True) -> Dict:
    """Classify all DG projects and optionally save to database."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Ensure columns exist
    for col, col_type in [
        ('dg_stage', 'TEXT'),
        ('dg_stage_confidence', 'REAL'),
        ('dg_stage_method', 'TEXT'),
    ]:
        try:
            cursor.execute(f"ALTER TABLE projects ADD COLUMN {col} {col_type}")
            conn.commit()
            logger.info(f"Added {col} column")
        except sqlite3.OperationalError:
            pass

    # Get all projects
    cursor.execute("""
        SELECT id, raw_status, status, source
        FROM projects
    """)
    rows = cursor.fetchall()
    logger.info(f"Classifying {len(rows):,} DG projects...")

    stats = {'total': 0, 'classified': 0, 'by_method': {}, 'by_stage': {}}

    batch = []
    for row in rows:
        stage, conf, method = classify_dg_stage(
            row['raw_status'], row['status'], row['source']
        )
        stats['total'] += 1
        if stage != 'unknown':
            stats['classified'] += 1
        stats['by_method'][method] = stats['by_method'].get(method, 0) + 1
        stats['by_stage'][stage] = stats['by_stage'].get(stage, 0) + 1

        batch.append((stage, conf, method, row['id']))

    if save:
        logger.info("Saving classifications...")
        cursor.executemany("""
            UPDATE projects SET dg_stage = ?, dg_stage_confidence = ?, dg_stage_method = ?
            WHERE id = ?
        """, batch)
        conn.commit()
        logger.info(f"Updated {len(batch):,} records")

    conn.close()
    return stats


def show_stats(db_path: Path = DG_DB_PATH):
    """Show DG stage distribution."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if columns exist
    cursor.execute("PRAGMA table_info(projects)")
    cols = {r[1] for r in cursor.fetchall()}
    if 'dg_stage' not in cols:
        print("dg_stage column not found — run --enrich first")
        conn.close()
        return

    print("\n=== DG Stage Distribution ===\n")

    # Overall
    cursor.execute("""
        SELECT dg_stage, COUNT(*), ROUND(AVG(dg_stage_confidence), 2)
        FROM projects
        GROUP BY dg_stage
        ORDER BY COUNT(*) DESC
    """)
    print(f"{'Stage':<15} {'Count':>10} {'Avg Conf':>10}")
    print("-" * 37)
    for row in cursor.fetchall():
        print(f"{row[0] or 'NULL':<15} {row[1]:>10,} {row[2] or 0:>10.2f}")

    # By source
    print("\n=== By Source ===\n")
    cursor.execute("""
        SELECT source, dg_stage, COUNT(*)
        FROM projects
        WHERE dg_stage IS NOT NULL
        GROUP BY source, dg_stage
        ORDER BY source, COUNT(*) DESC
    """)
    current_source = None
    for row in cursor.fetchall():
        if row[0] != current_source:
            current_source = row[0]
            print(f"\n  {current_source}:")
        print(f"    {row[1]:<15} {row[2]:>10,}")

    # By method
    print("\n\n=== By Classification Method ===\n")
    cursor.execute("""
        SELECT dg_stage_method, COUNT(*)
        FROM projects
        GROUP BY dg_stage_method
        ORDER BY COUNT(*) DESC
    """)
    for row in cursor.fetchall():
        print(f"  {row[0] or 'NULL':<20} {row[1]:>10,}")

    # Active projects with high-confidence stages (the investable sweet spot)
    print("\n\n=== Active Projects with Stage Detail ===\n")
    cursor.execute("""
        SELECT dg_stage, COUNT(*), SUM(capacity_mw), dg_stage_method
        FROM projects
        WHERE status = 'Active' AND dg_stage NOT IN ('operational', 'withdrawn', 'unknown')
        GROUP BY dg_stage, dg_stage_method
        ORDER BY COUNT(*) DESC
    """)
    print(f"{'Stage':<15} {'Method':<20} {'Count':>8} {'MW':>10}")
    print("-" * 55)
    for row in cursor.fetchall():
        mw = row[2] or 0
        print(f"{row[0]:<15} {row[3]:<20} {row[1]:>8,} {mw:>10,.1f}")

    conn.close()


def preview(db_path: Path = DG_DB_PATH, limit: int = 20):
    """Preview classifications for active projects with raw_status."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT queue_id, source, status, raw_status, capacity_mw, state
        FROM projects
        WHERE status = 'Active' AND raw_status IS NOT NULL
        LIMIT ?
    """, (limit,))

    print(f"\n{'Queue ID':<20} {'Source':<8} {'Raw Status':<35} {'Stage':<12} {'Conf':>5} {'MW':>8}")
    print("-" * 95)

    for row in cursor.fetchall():
        stage, conf, method = classify_dg_stage(row[3], row[2], row[1])
        mw = row[4] or 0
        print(f"{row[0]:<20} {row[1]:<8} {row[3]:<35} {stage:<12} {conf:>5.2f} {mw:>8.2f}")

    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DG Development Stage Classifier')
    parser.add_argument('--enrich', action='store_true', help='Classify all DG projects')
    parser.add_argument('--stats', action='store_true', help='Show stage distribution')
    parser.add_argument('--preview', action='store_true', help='Preview classifications')
    parser.add_argument('--db', type=str, help='Path to dg.db')
    args = parser.parse_args()

    db = Path(args.db) if args.db else DG_DB_PATH

    if args.enrich:
        stats = enrich_dg_stages(db, save=True)
        print(f"\nClassified {stats['classified']:,}/{stats['total']:,} projects")
        print(f"By stage: {json.dumps(stats['by_stage'], indent=2)}")
        print(f"By method: {json.dumps(stats['by_method'], indent=2)}")
    elif args.stats:
        show_stats(db)
    elif args.preview:
        preview(db)
    else:
        parser.print_help()
