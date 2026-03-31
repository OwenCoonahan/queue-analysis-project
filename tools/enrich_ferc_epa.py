#!/usr/bin/env python3
"""
FERC + EPA Enrichment for master.db (Parts 1 & 2)

DEPENDS ON: plant_id_eia column existing in master.db (added by Dev3 EIA match).
If plant_id_eia doesn't exist, this script will exit with a clear message.

Part 1 — FERC Financial Enrichment:
  Uses plant_id_eia → plant_id_pudl bridge → ferc.db plants_all
  Adds: ferc_capex_total, ferc_opex_total, ferc_net_generation_mwh, ferc_capacity_factor

Part 2 — EPA Emissions Enrichment:
  Uses plant_id_eia → epa.db egrid_plants.plant_code
  Adds: epa_co2_rate_lb_per_mwh, epa_co2_tons, epa_capacity_factor
"""

import os
import sqlite3
import sys
from typing import Optional

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_DB = os.path.join(SCRIPT_DIR, ".data", "master.db")
FERC_DB = os.path.join(
    SCRIPT_DIR, "..", "..", "..", "prospector-platform", "pipelines", "databases", "ferc.db"
)
EPA_DB = os.path.join(
    SCRIPT_DIR, "..", "..", "..", "prospector-platform", "pipelines", "databases", "epa.db"
)
PUDL_DB = os.path.join(SCRIPT_DIR, ".cache", "pudl", "pudl.sqlite")

# Columns to add to master.db
FERC_COLUMNS = [
    ("ferc_capex_total", "REAL"),
    ("ferc_opex_total", "REAL"),
    ("ferc_net_generation_mwh", "REAL"),
    ("ferc_capacity_factor", "REAL"),
]

EPA_COLUMNS = [
    ("epa_co2_rate_lb_per_mwh", "REAL"),
    ("epa_co2_tons", "REAL"),
    ("epa_capacity_factor", "REAL"),
]


def ensure_columns(conn: sqlite3.Connection, columns: list):
    """Add columns to projects table if they don't exist."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(projects)").fetchall()}
    for col_name, col_type in columns:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE projects ADD COLUMN {col_name} {col_type}")
            print(f"  Added column: {col_name} ({col_type})")
    conn.commit()


def build_eia_to_pudl_map() -> dict:
    """Build plant_id_eia → plant_id_pudl mapping from PUDL."""
    if not os.path.exists(PUDL_DB):
        print(f"  WARNING: PUDL database not found at {PUDL_DB}")
        print(f"  FERC enrichment will be skipped (no EIA→PUDL bridge)")
        return {}

    pudl_conn = sqlite3.connect(PUDL_DB)
    rows = pudl_conn.execute(
        "SELECT plant_id_eia, plant_id_pudl FROM core_pudl__assn_eia_pudl_plants"
    ).fetchall()
    pudl_conn.close()

    mapping = {}
    for eia_id, pudl_id in rows:
        if eia_id is not None and pudl_id is not None:
            mapping[str(eia_id)] = pudl_id
    print(f"  Loaded {len(mapping):,} EIA→PUDL plant mappings")
    return mapping


def build_ferc_lookup(eia_to_pudl: dict) -> dict:
    """Build plant_id_eia → FERC financials (most recent year)."""
    if not eia_to_pudl:
        return {}
    if not os.path.exists(FERC_DB):
        print(f"  WARNING: ferc.db not found at {FERC_DB}")
        return {}

    ferc_conn = sqlite3.connect(FERC_DB)
    ferc_conn.row_factory = sqlite3.Row

    # Get most recent year data for each plant_id_pudl
    pudl_ids = set(eia_to_pudl.values())
    ferc_by_pudl = {}

    # Process in batches to avoid SQLite variable limits
    pudl_list = list(pudl_ids)
    for i in range(0, len(pudl_list), 500):
        batch = pudl_list[i:i + 500]
        placeholders = ",".join("?" * len(batch))
        rows = ferc_conn.execute(f"""
            SELECT plant_id_pudl, capex_total, opex_total, net_generation_mwh, capacity_factor,
                   report_year
            FROM plants_all
            WHERE plant_id_pudl IN ({placeholders})
            ORDER BY report_year DESC
        """, batch).fetchall()

        for row in rows:
            pid = row["plant_id_pudl"]
            if pid not in ferc_by_pudl:  # Keep most recent year only
                ferc_by_pudl[pid] = {
                    "ferc_capex_total": row["capex_total"],
                    "ferc_opex_total": row["opex_total"],
                    "ferc_net_generation_mwh": row["net_generation_mwh"],
                    "ferc_capacity_factor": row["capacity_factor"],
                }

    ferc_conn.close()

    # Map back to EIA IDs
    ferc_by_eia = {}
    for eia_id, pudl_id in eia_to_pudl.items():
        if pudl_id in ferc_by_pudl:
            ferc_by_eia[eia_id] = ferc_by_pudl[pudl_id]

    print(f"  FERC data available for {len(ferc_by_eia):,} EIA plant IDs")
    return ferc_by_eia


def build_epa_lookup() -> dict:
    """Build plant_code → EPA emissions data."""
    if not os.path.exists(EPA_DB):
        print(f"  WARNING: epa.db not found at {EPA_DB}")
        return {}

    epa_conn = sqlite3.connect(EPA_DB)
    epa_conn.row_factory = sqlite3.Row

    rows = epa_conn.execute("""
        SELECT plant_code, co2_rate_lb_per_mwh, co2_tons,
               CASE WHEN capacity_mw > 0 AND annual_generation_mwh > 0
                    THEN annual_generation_mwh / (capacity_mw * 8760)
                    ELSE NULL END as capacity_factor
        FROM egrid_plants
        WHERE plant_code IS NOT NULL
    """).fetchall()
    epa_conn.close()

    epa_by_code = {}
    for row in rows:
        code = str(row["plant_code"])
        epa_by_code[code] = {
            "epa_co2_rate_lb_per_mwh": row["co2_rate_lb_per_mwh"],
            "epa_co2_tons": row["co2_tons"],
            "epa_capacity_factor": row["capacity_factor"],
        }

    print(f"  EPA data available for {len(epa_by_code):,} plant codes")
    return epa_by_code


def run_enrichment(dry_run: bool = False, stats_only: bool = False):
    """Run FERC + EPA enrichment on master.db."""

    if not os.path.exists(MASTER_DB):
        print(f"ERROR: master.db not found at {MASTER_DB}")
        sys.exit(1)

    print("=== FERC + EPA Enrichment ===\n")

    # Check if plant_id_eia exists
    master_conn = sqlite3.connect(MASTER_DB)
    master_conn.row_factory = sqlite3.Row
    existing_cols = {row[1] for row in master_conn.execute("PRAGMA table_info(projects)").fetchall()}

    if "plant_id_eia" not in existing_cols:
        print("ERROR: plant_id_eia column does not exist in master.db")
        print("Dev3 needs to complete the EIA plant match first.")
        print("Run this script again after plant_id_eia is populated.")
        master_conn.close()
        sys.exit(1)

    # Count projects with plant_id_eia
    total_projects = master_conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    with_eia = master_conn.execute(
        "SELECT COUNT(*) FROM projects WHERE plant_id_eia IS NOT NULL"
    ).fetchone()[0]
    print(f"Projects with plant_id_eia: {with_eia:,} / {total_projects:,} ({100 * with_eia / total_projects:.1f}%)")

    if with_eia == 0:
        print("No projects have plant_id_eia yet. Nothing to enrich.")
        master_conn.close()
        return

    # Build lookups
    print("\nBuilding lookups...")
    eia_to_pudl = build_eia_to_pudl_map()
    ferc_lookup = build_ferc_lookup(eia_to_pudl)
    epa_lookup = build_epa_lookup()

    # Get projects with plant_id_eia
    projects = master_conn.execute(
        "SELECT id, plant_id_eia FROM projects WHERE plant_id_eia IS NOT NULL"
    ).fetchall()

    # Count potential matches
    ferc_matches = 0
    epa_matches = 0
    for row in projects:
        eia_id = str(row["plant_id_eia"])
        if eia_id in ferc_lookup:
            ferc_matches += 1
        if eia_id in epa_lookup:
            epa_matches += 1

    print(f"\n--- Match Results ---")
    print(f"  Projects with plant_id_eia: {with_eia:,}")
    print(f"  FERC matches:              {ferc_matches:,} ({100 * ferc_matches / with_eia:.1f}%)")
    print(f"  EPA matches:               {epa_matches:,} ({100 * epa_matches / with_eia:.1f}%)")

    if stats_only:
        master_conn.close()
        return

    if dry_run:
        print("\n[DRY RUN] No changes written.")
        master_conn.close()
        return

    # Add columns
    print("\nAdding enrichment columns...")
    ensure_columns(master_conn, FERC_COLUMNS + EPA_COLUMNS)

    # Write FERC data
    print(f"\nWriting FERC data for {ferc_matches:,} projects...")
    ferc_updated = 0
    for row in projects:
        eia_id = str(row["plant_id_eia"])
        if eia_id in ferc_lookup:
            data = ferc_lookup[eia_id]
            master_conn.execute("""
                UPDATE projects SET
                    ferc_capex_total = ?,
                    ferc_opex_total = ?,
                    ferc_net_generation_mwh = ?,
                    ferc_capacity_factor = ?
                WHERE id = ?
            """, (
                data["ferc_capex_total"],
                data["ferc_opex_total"],
                data["ferc_net_generation_mwh"],
                data["ferc_capacity_factor"],
                row["id"],
            ))
            ferc_updated += 1

    # Write EPA data
    print(f"Writing EPA data for {epa_matches:,} projects...")
    epa_updated = 0
    for row in projects:
        eia_id = str(row["plant_id_eia"])
        if eia_id in epa_lookup:
            data = epa_lookup[eia_id]
            master_conn.execute("""
                UPDATE projects SET
                    epa_co2_rate_lb_per_mwh = ?,
                    epa_co2_tons = ?,
                    epa_capacity_factor = ?
                WHERE id = ?
            """, (
                data["epa_co2_rate_lb_per_mwh"],
                data["epa_co2_tons"],
                data["epa_capacity_factor"],
                row["id"],
            ))
            epa_updated += 1

    master_conn.commit()

    # Verify
    ferc_count = master_conn.execute(
        "SELECT COUNT(*) FROM projects WHERE ferc_capex_total IS NOT NULL"
    ).fetchone()[0]
    epa_count = master_conn.execute(
        "SELECT COUNT(*) FROM projects WHERE epa_co2_rate_lb_per_mwh IS NOT NULL"
    ).fetchone()[0]

    print(f"\n--- Final Coverage ---")
    print(f"  FERC enriched: {ferc_count:,} / {total_projects:,} ({100 * ferc_count / total_projects:.1f}%)")
    print(f"  EPA enriched:  {epa_count:,} / {total_projects:,} ({100 * epa_count / total_projects:.1f}%)")

    master_conn.close()
    print("\nDone.")


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    stats_only = "--stats" in sys.argv
    run_enrichment(dry_run=dry_run, stats_only=stats_only)
