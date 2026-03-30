#!/usr/bin/env python3
"""
DG Investability Scoring Engine.

Scores pre-operational DG projects for small ITC investor suitability.
Uses dg_stage classification, tax credit enrichment, project size,
state program quality, recency, and data completeness.

Usage:
    python3 dg_investability.py --enrich         # Score all pre-operational DG projects
    python3 dg_investability.py --stats          # Show score distribution
    python3 dg_investability.py --preview        # Show top 20 investable DG projects
"""

import os
import sys
import json
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Optional, Tuple

DATA_DIR = Path(__file__).parent / '.data'
DG_DB_PATH = Path(os.environ.get('DG_DB_PATH', str(DATA_DIR / 'dg.db')))

# States with rich DG program data (higher confidence in stage classification)
HIGH_QUALITY_STATES = {'NJ', 'NY', 'MA', 'IL'}

# Pre-operational stages eligible for scoring
SCORABLE_STAGES = {'applied', 'approved', 'construction', 'inspection'}

# =============================================================================
# Scoring Components (max 100 points)
# =============================================================================

WEIGHTS = {
    'itc_eligible': 20,       # Is it solar/ITC-eligible?
    'size_fit': 15,           # Sweet spot for small investors
    'stage': 25,              # How far along in development?
    'not_stalled': 10,        # Active vs stalled
    'state_quality': 10,      # Data quality of source program
    'recency': 10,            # How recently applied?
    'completeness': 10,       # Data fields populated
}
MAX_SCORE = sum(WEIGHTS.values())
MIN_INVESTABLE_SCORE = 50


def score_itc_eligible(project: Dict) -> Tuple[float, str]:
    """Score ITC eligibility. Nearly all DG is solar = ITC eligible."""
    type_std = (project.get('type_std') or project.get('type') or '').lower()
    tax_type = (project.get('tax_credit_type') or '').lower()
    rate = project.get('effective_credit_rate') or project.get('base_credit_rate') or 0

    if tax_type in ('itc', 'both') or rate > 0:
        return (WEIGHTS['itc_eligible'], f"ITC eligible, rate={rate}")
    if 'solar' in type_std or 'pv' in type_std:
        return (WEIGHTS['itc_eligible'], "Solar (ITC assumed)")
    if 'storage' in type_std or 'battery' in type_std:
        return (WEIGHTS['itc_eligible'] * 0.8, "Storage (ITC eligible)")
    if type_std == '' or type_std is None:
        # Most DG is solar — assume eligible with reduced confidence
        return (WEIGHTS['itc_eligible'] * 0.5, "Unknown type (likely solar)")
    return (0, f"Non-ITC type: {type_std}")


def score_size_fit(project: Dict) -> Tuple[float, str]:
    """Score capacity for small investor fit. Sweet spot: 10-500 kW."""
    kw = project.get('capacity_kw') or project.get('system_size_dc_kw') or 0
    if not kw and project.get('capacity_mw'):
        kw = project['capacity_mw'] * 1000

    if kw <= 0:
        return (WEIGHTS['size_fit'] * 0.3, "No capacity data")
    if 10 <= kw <= 500:
        return (WEIGHTS['size_fit'], f"{kw:.0f} kW (sweet spot)")
    if 500 < kw <= 1000:
        return (WEIGHTS['size_fit'] * 0.8, f"{kw:.0f} kW (acceptable)")
    if 5 <= kw < 10:
        return (WEIGHTS['size_fit'] * 0.6, f"{kw:.0f} kW (small)")
    if 1000 < kw <= 2000:
        return (WEIGHTS['size_fit'] * 0.5, f"{kw:.0f} kW (large for DG)")
    return (0, f"{kw:.0f} kW (outside range)")


def score_stage(project: Dict) -> Tuple[float, str]:
    """Score development stage. Construction/inspection = highest value."""
    stage = project.get('dg_stage', '')
    confidence = project.get('dg_stage_confidence') or 0.5

    stage_scores = {
        'construction': 1.0,
        'inspection': 1.0,
        'approved': 0.6,
        'applied': 0.2,
    }

    base = stage_scores.get(stage, 0)
    # Weight by classification confidence
    score = WEIGHTS['stage'] * base * min(confidence, 1.0)
    return (score, f"{stage} (conf={confidence:.2f})")


def score_not_stalled(project: Dict) -> Tuple[float, str]:
    """Penalize stalled projects."""
    stalled = project.get('stalled', 0)
    if stalled:
        return (0, "Project is stalled")
    return (WEIGHTS['not_stalled'], "Not stalled")


def score_state_quality(project: Dict) -> Tuple[float, str]:
    """Score based on state program data quality."""
    state = project.get('state', '')
    source = project.get('source', '')

    if state in HIGH_QUALITY_STATES:
        return (WEIGHTS['state_quality'], f"{state} (high-quality program)")
    # Some sources outside target states still have good data
    if source in ('ny_dps_sir', 'nj_dg', 'ma_doer', 'ma_smart', 'il_shines', 'ameren_il'):
        return (WEIGHTS['state_quality'], f"{source} (rich data)")
    return (WEIGHTS['state_quality'] * 0.5, f"{state} (standard)")


def score_recency(project: Dict) -> Tuple[float, str]:
    """Score based on how recently the project was filed."""
    queue_date = project.get('queue_date', '')
    if not queue_date:
        return (WEIGHTS['recency'] * 0.3, "No queue date")

    try:
        qd = datetime.strptime(str(queue_date)[:10], '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return (WEIGHTS['recency'] * 0.3, "Unparseable date")

    age_days = (date.today() - qd).days
    if age_days < 365:
        return (WEIGHTS['recency'], f"{age_days}d old (recent)")
    if age_days < 730:
        return (WEIGHTS['recency'] * 0.7, f"{age_days}d old (1-2 years)")
    if age_days < 1095:
        return (WEIGHTS['recency'] * 0.3, f"{age_days}d old (2-3 years)")
    return (0, f"{age_days}d old (>3 years)")


def score_completeness(project: Dict) -> Tuple[float, str]:
    """Score data completeness — more fields = higher confidence."""
    fields = ['capacity_kw', 'state', 'raw_status', 'source']
    bonus_fields = ['developer', 'installer', 'county', 'queue_date', 'type_std']

    present = sum(1 for f in fields if project.get(f))
    bonus = sum(1 for f in bonus_fields if project.get(f))

    base = (present / len(fields)) * WEIGHTS['completeness'] * 0.7
    extra = (bonus / len(bonus_fields)) * WEIGHTS['completeness'] * 0.3
    score = base + extra

    total = present + bonus
    return (score, f"{total}/{len(fields) + len(bonus_fields)} fields")


def score_project(project: Dict) -> Dict:
    """Score a single DG project for investability."""
    components = {}
    total = 0

    for name, func in [
        ('itc_eligible', score_itc_eligible),
        ('size_fit', score_size_fit),
        ('stage', score_stage),
        ('not_stalled', score_not_stalled),
        ('state_quality', score_state_quality),
        ('recency', score_recency),
        ('completeness', score_completeness),
    ]:
        score, detail = func(project)
        components[name] = {'score': round(score, 1), 'max': WEIGHTS[name], 'detail': detail}
        total += score

    total = round(total, 1)

    # Determine investability
    stage = project.get('dg_stage', '')
    stalled = project.get('stalled', 0)
    investable = (
        total >= MIN_INVESTABLE_SCORE
        and stage in ('approved', 'construction', 'inspection')
        and not stalled
    )

    # Grade
    pct = total / MAX_SCORE
    if pct >= 0.8:
        grade = 'A'
    elif pct >= 0.65:
        grade = 'B'
    elif pct >= 0.5:
        grade = 'C'
    else:
        grade = 'D'

    return {
        'score': total,
        'max_score': MAX_SCORE,
        'grade': grade,
        'investable': investable,
        'components': components,
        'scored_at': datetime.utcnow().isoformat(),
    }


# =============================================================================
# Database operations
# =============================================================================

def _ensure_columns(conn: sqlite3.Connection):
    """Add investability columns to dg.db if they don't exist."""
    existing = {row[1] for row in conn.execute('PRAGMA table_info(projects)').fetchall()}
    new_cols = [
        ('dg_investable', 'INTEGER DEFAULT 0'),
        ('dg_investability_score', 'REAL'),
        ('dg_investability_json', 'TEXT'),
    ]
    for col, typ in new_cols:
        if col not in existing:
            conn.execute(f'ALTER TABLE projects ADD COLUMN {col} {typ}')
            print(f"  Added column: {col}")
    conn.commit()


def enrich(db_path: Path = None, batch_size: int = 5000):
    """Score all pre-operational DG projects."""
    path = db_path or DG_DB_PATH
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row

    _ensure_columns(conn)

    # Count targets
    total = conn.execute(
        "SELECT COUNT(*) FROM projects WHERE dg_stage IN ('applied','approved','construction','inspection')"
    ).fetchone()[0]
    print(f"Scoring {total:,} pre-operational DG projects...")

    scored = 0
    investable_count = 0

    # Process in batches using ROWID ranges for efficiency
    min_id = conn.execute(
        "SELECT MIN(id) FROM projects WHERE dg_stage IN ('applied','approved','construction','inspection')"
    ).fetchone()[0] or 0
    max_id = conn.execute(
        "SELECT MAX(id) FROM projects WHERE dg_stage IN ('applied','approved','construction','inspection')"
    ).fetchone()[0] or 0

    cursor = conn.cursor()
    batch_start = min_id

    while batch_start <= max_id:
        batch_end = batch_start + batch_size * 10  # IDs aren't contiguous

        rows = conn.execute("""
            SELECT id, queue_id, capacity_kw, capacity_mw, system_size_dc_kw,
                   type, type_std, state, county, source, developer, installer,
                   queue_date, raw_status, dg_stage, dg_stage_confidence, stalled,
                   tax_credit_type, base_credit_rate, effective_credit_rate,
                   energy_community_eligible, low_income_eligible
            FROM projects
            WHERE dg_stage IN ('applied','approved','construction','inspection')
            AND id >= ? AND id < ?
        """, (batch_start, batch_end)).fetchall()

        if not rows:
            batch_start = batch_end
            continue

        for row in rows:
            project = dict(row)
            result = score_project(project)

            cursor.execute("""
                UPDATE projects SET
                    dg_investable = ?,
                    dg_investability_score = ?,
                    dg_investability_json = ?
                WHERE id = ?
            """, (
                1 if result['investable'] else 0,
                result['score'],
                json.dumps(result, default=str),
                project['id'],
            ))

            scored += 1
            if result['investable']:
                investable_count += 1

        conn.commit()
        batch_start = batch_end

        if scored % 20000 == 0 or scored >= total:
            print(f"  Scored {scored:,}/{total:,} ({investable_count:,} investable)")

    print(f"\nDone. {scored:,} scored, {investable_count:,} investable.")
    conn.close()
    return {'scored': scored, 'investable': investable_count}


def show_stats(db_path: Path = None):
    """Show DG investability score distribution."""
    path = db_path or DG_DB_PATH
    conn = sqlite3.connect(str(path))

    print("=== DG Investability Stats ===\n")

    total = conn.execute("SELECT COUNT(*) FROM projects WHERE dg_investability_score IS NOT NULL").fetchone()[0]
    investable = conn.execute("SELECT COUNT(*) FROM projects WHERE dg_investable = 1").fetchone()[0]
    print(f"Total scored: {total:,}")
    print(f"Investable: {investable:,}")
    print()

    # By grade
    print("Grade distribution:")
    for grade in ['A', 'B', 'C', 'D']:
        lo = {'A': 80, 'B': 65, 'C': 50, 'D': 0}[grade]
        hi = {'A': 999, 'B': 80, 'C': 65, 'D': 50}[grade]
        count = conn.execute(
            "SELECT COUNT(*) FROM projects WHERE dg_investability_score >= ? AND dg_investability_score < ?",
            (lo, hi)
        ).fetchone()[0]
        print(f"  {grade}: {count:,}")

    print()
    print("Investable by state:")
    rows = conn.execute("""
        SELECT state, COUNT(*) as c, ROUND(AVG(dg_investability_score), 1) as avg_score
        FROM projects WHERE dg_investable = 1
        GROUP BY state ORDER BY c DESC LIMIT 15
    """).fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]:,} (avg score: {r[2]})")

    print()
    print("Investable by stage:")
    rows = conn.execute("""
        SELECT dg_stage, COUNT(*) as c, ROUND(AVG(dg_investability_score), 1) as avg_score
        FROM projects WHERE dg_investable = 1
        GROUP BY dg_stage ORDER BY c DESC
    """).fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]:,} (avg score: {r[2]})")

    print()
    print("Investable by source:")
    rows = conn.execute("""
        SELECT source, COUNT(*) as c
        FROM projects WHERE dg_investable = 1
        GROUP BY source ORDER BY c DESC LIMIT 10
    """).fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]:,}")

    conn.close()


def show_preview(db_path: Path = None, limit: int = 20):
    """Show top investable DG projects."""
    path = db_path or DG_DB_PATH
    conn = sqlite3.connect(str(path))

    print(f"=== Top {limit} Investable DG Projects ===\n")
    rows = conn.execute("""
        SELECT queue_id, state, dg_stage, capacity_kw, dg_investability_score,
               source, developer, installer, effective_credit_rate
        FROM projects
        WHERE dg_investable = 1
        ORDER BY dg_investability_score DESC
        LIMIT ?
    """, (limit,)).fetchall()

    for r in rows:
        dev = r[6] or r[7] or 'unknown'
        rate = f"{r[8]*100:.0f}%" if r[8] else 'n/a'
        print(f"  {r[0]:20s} | {r[1]:2s} | {r[2]:12s} | {r[3] or 0:8.1f} kW | score={r[4]:5.1f} | {r[5]:12s} | {dev[:25]:25s} | ITC={rate}")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description='DG Investability Scoring Engine')
    parser.add_argument('--enrich', action='store_true', help='Score all pre-operational DG projects')
    parser.add_argument('--stats', action='store_true', help='Show score distribution')
    parser.add_argument('--preview', action='store_true', help='Show top investable projects')
    parser.add_argument('--db', type=str, help='Path to dg.db')
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else None

    if args.enrich:
        enrich(db_path)
    if args.stats:
        show_stats(db_path)
    if args.preview:
        show_preview(db_path)

    if not any([args.enrich, args.stats, args.preview]):
        parser.print_help()


if __name__ == '__main__':
    main()
