#!/usr/bin/env python3
"""
Construction Stage Classification (v2 — milestone-aware).

Classifies each project's development stage using milestone dates, study phases,
IA status, and COD heuristics. Gracefully degrades when milestone columns are
not yet populated (falls back to status_std + cod_std).

Stages:
- early: Just entered queue, feasibility study or no study data
- mid: System impact study phase
- late: IA executed, facilities study complete, COD within 1-2 years
- construction: Under construction, backfeed date set, COD imminent
- operational: Already operating
- withdrawn: Withdrawn from queue
- suspended: Suspended/on hold

Usage:
    python3 construction_stage.py --enrich     # Classify all projects
    python3 construction_stage.py --stats      # Show distribution
    python3 construction_stage.py --preview    # Show 20 sample upgrades (dry run)
"""

import os
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime, date
from typing import Tuple, Optional

DATA_DIR = Path(__file__).parent / '.data'
DB_PATH = Path(os.environ.get('QUEUE_DB_PATH', str(DATA_DIR / 'master.db')))

# Milestone columns added by Dev1 Brief A (R12). If missing, classifier
# falls back to status_std + cod_std only.
MILESTONE_COLS = [
    'ia_date', 'actual_cod', 'withdrawn_date', 'study_phase',
    'backfeed_date', 'feasibility_study_date', 'system_impact_study_date',
    'facilities_study_date', 'ia_status', 'study_cycle', 'study_group',
]


def _parse_date(val: str) -> Optional[date]:
    """Parse a date string into a date object."""
    if not val or not str(val).strip():
        return None
    val = str(val).strip()[:10]
    for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%Y']:
        try:
            return datetime.strptime(val, fmt).date()
        except ValueError:
            continue
    try:
        year = int(val[:4])
        if 2000 <= year <= 2050:
            return date(year, 6, 1)
    except (ValueError, TypeError):
        pass
    return None


def _has(row: dict, key: str) -> bool:
    """Check if a row has a non-empty value for key."""
    v = row.get(key)
    return v is not None and str(v).strip() != ''


def _contains(row: dict, key: str, *terms: str) -> bool:
    """Check if row[key] contains any of the given terms (case-insensitive)."""
    v = row.get(key)
    if not v:
        return False
    v_lower = str(v).lower()
    return any(t.lower() in v_lower for t in terms)


def classify_stage(row: dict, today: date = None) -> Tuple[str, float, str]:
    """
    Classify a project's construction stage from a row dict.

    Returns (stage, confidence, method) tuple.
    method is one of: 'status', 'milestone', 'study_phase', 'cod_heuristic'
    """
    today = today or date.today()
    status = (row.get('status_std') or '').strip()

    # ── Priority 1: Deterministic statuses ────────────────────────────
    if status == 'Operational':
        return ('operational', 1.0, 'status')
    if status == 'Withdrawn':
        return ('withdrawn', 1.0, 'status')
    if status == 'Suspended':
        return ('suspended', 1.0, 'status')

    # actual_cod in the past → operational (even if status hasn't caught up)
    actual_cod = _parse_date(row.get('actual_cod', ''))
    if actual_cod and actual_cod < today:
        return ('operational', 0.95, 'milestone')

    # ── Priority 2: Milestone-based (highest confidence) ──────────────

    # IA executed (from ia_status field or ia_date)
    ia_status = (row.get('ia_status') or '').strip()
    ia_status_lower = ia_status.lower()
    # ia_status can also carry terminal statuses
    if ia_status_lower == 'operational':
        return ('operational', 0.95, 'milestone')
    if ia_status_lower == 'withdrawn':
        return ('withdrawn', 0.95, 'milestone')
    if ia_status_lower == 'suspended':
        return ('suspended', 0.95, 'milestone')
    if ia_status_lower == 'construction':
        return ('construction', 0.90, 'milestone')
    if ia_status_lower in ('ia executed', 'executed', 'gia executed', 'ia pending'):
        return ('late', 0.95, 'milestone')
    if _has(row, 'ia_date'):
        return ('late', 0.95, 'milestone')

    # Backfeed date in the future → under construction
    backfeed = _parse_date(row.get('backfeed_date', ''))
    if backfeed and backfeed >= today:
        return ('construction', 0.90, 'milestone')

    # ia_status = "Facility Study" → late stage (past SIS, in facilities)
    if ia_status_lower in ('facility study', 'facilities study'):
        return ('late', 0.90, 'milestone')

    # Facilities study date exists → late stage
    if _has(row, 'facilities_study_date'):
        return ('late', 0.90, 'milestone')

    # ia_status = "System Impact Study" → mid stage
    if ia_status_lower in ('system impact study', 'cluster study'):
        return ('mid', 0.85, 'milestone')

    # System impact study date exists → mid stage
    if _has(row, 'system_impact_study_date'):
        return ('mid', 0.85, 'milestone')

    # ia_status = "Feasibility Study" or "Not Started" → early
    if ia_status_lower in ('feasibility study', 'not started'):
        return ('early', 0.75, 'milestone')

    # ia_status = generic "In Progress" → at least early (better than cod_heuristic)
    if 'in progress' in ia_status_lower:
        return ('early', 0.65, 'milestone')

    # Feasibility study date only (no later study) → early
    if _has(row, 'feasibility_study_date'):
        return ('early', 0.70, 'milestone')

    # ── Priority 3: Study phase text (from MISO, PJM, CAISO, etc.) ──

    study_phase = row.get('study_phase') or ''
    if study_phase:
        sp_lower = study_phase.lower().strip()
        # IA Executed in study_phase (PJM)
        if 'ia executed' in sp_lower or 'gia' in sp_lower:
            return ('late', 0.90, 'study_phase')
        if _contains(row, 'study_phase', 'Facilities', 'FACA', 'Phase 3'):
            return ('late', 0.85, 'study_phase')
        if _contains(row, 'study_phase', 'SIS', 'System Impact', 'DISIS', 'Phase 2'):
            return ('mid', 0.80, 'study_phase')
        if _contains(row, 'study_phase', 'DPP', 'Feasibility', 'ERIS', 'Phase 1',
                      'Cluster', 'Study Not Started'):
            return ('early', 0.75, 'study_phase')
        # PJM cycle IDs (C14, C13, etc.) — project is in a study cycle
        if sp_lower.startswith('c') and sp_lower[1:].isdigit():
            return ('early', 0.70, 'study_phase')
        # ISP (Interconnection Service Provider) — generic but real signal
        if sp_lower == 'isp':
            return ('early', 0.65, 'study_phase')

    # ── Priority 4: status_std text (lower confidence than milestones) ─

    if status == 'IA Executed':
        return ('late', 0.90, 'status')
    if status in ('Facilities Study Complete', 'Facilities Study'):
        return ('late', 0.85, 'status')
    if status in ('System Impact Study Complete', 'System Impact Study'):
        return ('mid', 0.75, 'status')
    if status == 'Unknown':
        return ('early', 0.3, 'status')

    # ── Priority 5: COD heuristic (lowest confidence) ─────────────────

    cod = _parse_date(row.get('cod_std', ''))
    if cod is None:
        return ('early', 0.50, 'cod_heuristic')

    months_to_cod = (cod.year - today.year) * 12 + (cod.month - today.month)

    if months_to_cod < 0:
        return ('construction', 0.50, 'cod_heuristic')
    elif months_to_cod <= 12:
        return ('construction', 0.65, 'cod_heuristic')
    elif months_to_cod <= 24:
        return ('late', 0.70, 'cod_heuristic')
    elif months_to_cod <= 48:
        return ('mid', 0.60, 'cod_heuristic')
    else:
        return ('early', 0.50, 'cod_heuristic')


def _detect_columns(conn: sqlite3.Connection) -> set:
    """Return set of column names in projects table."""
    return {row[1] for row in conn.execute("PRAGMA table_info(projects)").fetchall()}


def enrich_construction_stage(db_path: Path = None, save: bool = True):
    """Add construction_stage, construction_stage_confidence, and
    construction_stage_method to all projects."""
    db = db_path or DB_PATH
    print("=" * 60)
    print("Construction Stage Classification (v2 — milestone-aware)")
    print("=" * 60)

    if not db.exists():
        raise FileNotFoundError(f"Database not found: {db}")

    conn = sqlite3.connect(db, timeout=120)
    conn.row_factory = sqlite3.Row

    # Ensure output columns exist
    for col_name, col_type in [
        ("construction_stage", "TEXT"),
        ("construction_stage_confidence", "REAL"),
        ("construction_stage_method", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE projects ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass

    # Detect which milestone columns are available
    existing_cols = _detect_columns(conn)
    available_milestones = [c for c in MILESTONE_COLS if c in existing_cols]
    missing_milestones = [c for c in MILESTONE_COLS if c not in existing_cols]

    if available_milestones:
        print(f"\n  Milestone columns available: {', '.join(available_milestones)}")
    if missing_milestones:
        print(f"  Milestone columns missing (will use fallback): {', '.join(missing_milestones)}")

    # Build SELECT with available columns
    base_cols = ['id', 'status_std', 'cod_std']
    select_cols = base_cols + available_milestones
    cursor = conn.execute(f"SELECT {', '.join(select_cols)} FROM projects")
    projects = cursor.fetchall()
    print(f"\n  Processing {len(projects):,} projects...")

    today = date.today()
    stats = {}
    method_stats = {}

    for i, project in enumerate(projects):
        if (i + 1) % 10000 == 0:
            print(f"    Progress: {i+1:,}/{len(projects):,}")

        row = dict(project)
        stage, confidence, method = classify_stage(row, today=today)

        stats[stage] = stats.get(stage, 0) + 1
        method_stats[method] = method_stats.get(method, 0) + 1

        if save:
            conn.execute("""
                UPDATE projects SET
                    construction_stage = ?,
                    construction_stage_confidence = ?,
                    construction_stage_method = ?
                WHERE id = ?
            """, (stage, confidence, method, project['id']))

    if save:
        conn.commit()

    conn.close()

    # Print summary
    total = sum(stats.values())
    print(f"\n  {'Stage':<20} {'Count':>8} {'Pct':>7}")
    print("  " + "-" * 37)
    for stage in ['early', 'mid', 'late', 'construction', 'operational', 'withdrawn', 'suspended']:
        count = stats.get(stage, 0)
        pct = count / total * 100 if total else 0
        print(f"  {stage:<20} {count:>8,} {pct:>6.1f}%")
    print(f"\n  Total: {total:,}")

    print(f"\n  {'Method':<20} {'Count':>8} {'Pct':>7}")
    print("  " + "-" * 37)
    for method in ['status', 'milestone', 'study_phase', 'cod_heuristic']:
        count = method_stats.get(method, 0)
        pct = count / total * 100 if total else 0
        print(f"  {method:<20} {count:>8,} {pct:>6.1f}%")

    return stats


def show_stats(db_path: Path = None):
    """Show current construction stage distribution with confidence breakdown."""
    db = db_path or DB_PATH
    conn = sqlite3.connect(db)

    cols = _detect_columns(conn)
    if 'construction_stage' not in cols:
        print("construction_stage column not found. Run --enrich first.")
        conn.close()
        return

    has_method = 'construction_stage_method' in cols

    # Stage distribution
    rows = conn.execute("""
        SELECT construction_stage, COUNT(*) as cnt,
               ROUND(AVG(construction_stage_confidence), 2) as avg_conf
        FROM projects
        GROUP BY construction_stage
        ORDER BY cnt DESC
    """).fetchall()

    print(f"\n  {'Stage':<20} {'Count':>8} {'Avg Conf':>10}")
    print("  " + "-" * 40)
    for row in rows:
        print(f"  {row[0] or 'NULL':<20} {row[1]:>8,} {row[2]:>10.2f}")

    # Method distribution
    if has_method:
        method_rows = conn.execute("""
            SELECT construction_stage_method, COUNT(*) as cnt,
                   ROUND(AVG(construction_stage_confidence), 2) as avg_conf
            FROM projects
            GROUP BY construction_stage_method
            ORDER BY cnt DESC
        """).fetchall()
        print(f"\n  {'Method':<20} {'Count':>8} {'Avg Conf':>10}")
        print("  " + "-" * 40)
        for row in method_rows:
            print(f"  {row[0] or 'NULL':<20} {row[1]:>8,} {row[2]:>10.2f}")

    # Confidence distribution
    conf_rows = conn.execute("""
        SELECT
            CASE
                WHEN construction_stage_confidence >= 0.9 THEN '0.90-1.00 (high)'
                WHEN construction_stage_confidence >= 0.7 THEN '0.70-0.89 (medium)'
                WHEN construction_stage_confidence >= 0.5 THEN '0.50-0.69 (low)'
                ELSE '< 0.50 (very low)'
            END as band,
            COUNT(*) as cnt
        FROM projects
        WHERE construction_stage NOT IN ('operational', 'withdrawn', 'suspended')
        GROUP BY band
        ORDER BY band
    """).fetchall()
    print(f"\n  Confidence distribution (active projects only):")
    print(f"  {'Band':<25} {'Count':>8}")
    print("  " + "-" * 35)
    for row in conf_rows:
        print(f"  {row[0]:<25} {row[1]:>8,}")

    # Investable subset
    investable = conn.execute("""
        SELECT COUNT(*) FROM projects
        WHERE construction_stage IN ('late', 'construction')
          AND type_std IN ('Solar', 'Battery', 'Solar+Storage', 'Hybrid')
          AND capacity_mw BETWEEN 0.5 AND 10
    """).fetchone()[0]
    print(f"\n  Late/construction solar/battery 0.5-10 MW: {investable:,}")

    conn.close()


def show_preview(db_path: Path = None, limit: int = 20):
    """Show sample projects that would be upgraded from low to high confidence."""
    db = db_path or DB_PATH
    conn = sqlite3.connect(db, timeout=120)
    conn.row_factory = sqlite3.Row

    existing_cols = _detect_columns(conn)
    has_method = 'construction_stage_method' in existing_cols
    available_milestones = [c for c in MILESTONE_COLS if c in existing_cols]

    base_cols = ['id', 'name', 'region', 'status_std', 'cod_std',
                 'construction_stage', 'construction_stage_confidence']
    if has_method:
        base_cols.append('construction_stage_method')
    select_cols = base_cols + [c for c in available_milestones if c not in base_cols]

    # Get active projects with low confidence
    rows = conn.execute(f"""
        SELECT {', '.join(select_cols)} FROM projects
        WHERE construction_stage NOT IN ('operational', 'withdrawn', 'suspended')
          AND construction_stage_confidence < 0.75
        ORDER BY RANDOM()
        LIMIT {limit * 3}
    """).fetchall()

    today = date.today()
    upgrades = []

    for row in rows:
        row_dict = dict(row)
        new_stage, new_conf, new_method = classify_stage(row_dict, today=today)
        old_conf = row_dict.get('construction_stage_confidence', 0) or 0
        old_stage = row_dict.get('construction_stage', '')

        if new_conf > old_conf + 0.05:
            upgrades.append({
                'id': row_dict['id'],
                'name': (row_dict.get('name') or '')[:40],
                'region': row_dict.get('region', ''),
                'old_stage': old_stage,
                'old_conf': old_conf,
                'new_stage': new_stage,
                'new_conf': new_conf,
                'method': new_method,
            })

        if len(upgrades) >= limit:
            break

    conn.close()

    if not upgrades:
        print("\n  No upgrades found (milestone columns may not be populated yet).")
        print("  Run after Dev1 completes Brief A (milestone data capture).")
        return

    print(f"\n  Sample upgrades ({len(upgrades)} projects):")
    print(f"  {'ID':<15} {'Name':<40} {'Old':>12} {'New':>12} {'Method':<15}")
    print("  " + "-" * 96)
    for u in upgrades:
        old = f"{u['old_stage']}({u['old_conf']:.2f})"
        new = f"{u['new_stage']}({u['new_conf']:.2f})"
        print(f"  {u['id']:<15} {u['name']:<40} {old:>12} {new:>12} {u['method']:<15}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Construction stage classification (v2)')
    parser.add_argument('--enrich', action='store_true', help='Run enrichment')
    parser.add_argument('--stats', action='store_true', help='Show distribution')
    parser.add_argument('--preview', action='store_true', help='Show 20 sample upgrades (dry run)')
    parser.add_argument('--db', type=str, help='Database path')
    args = parser.parse_args()

    db = Path(args.db) if args.db else None

    if args.enrich:
        enrich_construction_stage(db_path=db)
    elif args.stats:
        show_stats(db_path=db)
    elif args.preview:
        show_preview(db_path=db)
    else:
        parser.print_help()
