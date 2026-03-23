#!/usr/bin/env python3
"""
Domestic Content Eligibility Checker — IRA Section 45X / 48(a)(12) bonus.

The domestic content bonus adds 10 percentage points to the ITC (30% → 40%)
for projects using US-manufactured equipment meeting threshold requirements:
  - Steel/iron: 100% US-made
  - Manufactured products: 40% US-made (20% for projects starting construction 2025+)

This checker uses:
  1. USWTDB manufacturer data (high confidence for wind)
  2. Manufacturer country-of-origin mappings
  3. Heuristics for solar/storage based on project characteristics

Usage:
    python3 domestic_content.py              # Run enrichment
    python3 domestic_content.py --dry-run    # Preview without writing
    python3 domestic_content.py --stats      # Show current coverage
"""

import sqlite3
import argparse
from pathlib import Path
from collections import defaultdict

TOOLS_DIR = Path(__file__).parent
MASTER_DB = TOOLS_DIR / '.data' / 'master.db'
GRID_DB = TOOLS_DIR / '.data' / 'grid.db'

# ─── Manufacturer → Country Mapping ──────────────────────────────────────────
# "US" means primary manufacturing in the US (or significant US factory capacity)
# "foreign" means primarily manufactured outside the US

WIND_MANUFACTURERS = {
    # US-manufactured or significant US factory presence
    'GE Wind': 'US',
    'GE': 'US',
    'GE Vernova': 'US',
    'Clipper': 'US',
    'Northern Power Systems': 'US',
    'Zond': 'US',          # acquired by GE, was US
    'Enron': 'US',         # Enron Wind → GE Wind
    'DeWind': 'US',        # US-based (defunct)

    # Foreign-manufactured (some have US assembly but majority foreign components)
    'Vestas': 'foreign',           # Denmark HQ, US factories for nacelles/blades but many imports
    'Siemens': 'foreign',          # Germany
    'Siemens Gamesa Renewable Energy': 'foreign',  # Spain/Germany
    'Gamesa': 'foreign',           # Spain (now Siemens Gamesa)
    'Nordex': 'foreign',           # Germany
    'Suzlon': 'foreign',           # India
    'Acciona': 'foreign',          # Spain
    'Mitsubishi': 'foreign',       # Japan
    'REpower': 'foreign',          # Germany (now Senvion)
    'Goldwind': 'foreign',         # China
    'Goldwind Americas': 'foreign', # China (US subsidiary)
    'NEG Micon': 'foreign',        # Denmark (now Vestas)
    'Bonus': 'foreign',            # Denmark (now Siemens)
    'Nordtank': 'foreign',         # Denmark
    'Micon': 'foreign',            # Denmark
    'Sany': 'foreign',             # China
    'China Creative Wind Energy': 'foreign',  # China
    'Vensys': 'foreign',           # Germany/China (Goldwind subsidiary)
    'Envision': 'foreign',         # China
    'Enercon': 'foreign',          # Germany
    'Senvion': 'foreign',          # Germany (formerly REpower)
    'Mingyang': 'foreign',         # China
}

# Note: Vestas has US blade/nacelle factories (CO, IA) but turbine-level domestic
# content depends on specific model and supply chain. We mark as "foreign" to be
# conservative — the 40% manufactured product threshold is hard to confirm without
# project-specific BOM data. Projects with Vestas could qualify if they use
# US-factory models, but we can't verify from public data alone.

SOLAR_MANUFACTURERS = {
    # US manufacturing (panels)
    'First Solar': 'US',           # Ohio — only large-scale US panel maker
    'Qcells': 'US',                # Georgia factory (Hanwha subsidiary)
    'Mission Solar': 'US',         # Texas
    'Silfab Solar': 'US',          # Washington state
    'Heliene': 'US',               # Minnesota
    'CertainTeed': 'US',           # Connecticut (Saint-Gobain)
    'SunPower': 'US',              # US design, some US assembly (mostly Maxeon/foreign)

    # Foreign manufacturing (panels)
    'Canadian Solar': 'foreign',   # China
    'LONGi': 'foreign',            # China
    'JinkoSolar': 'foreign',       # China
    'Trina Solar': 'foreign',      # China
    'JA Solar': 'foreign',         # China
    'REC': 'foreign',              # Norway/Singapore
    'Risen': 'foreign',            # China
    'Astronergy': 'foreign',       # China
    'Yingli': 'foreign',           # China
    'Hanwha': 'foreign',           # Korea (except Qcells US factory line)
}

STORAGE_MANUFACTURERS = {
    # US manufacturing
    'Tesla': 'US',                 # Nevada Gigafactory
    'Fluence': 'US',               # Virginia (Siemens/AES JV)
    'Powin': 'US',                 # Oregon
    'ESS Inc': 'US',               # Oregon (iron flow)
    'Form Energy': 'US',           # West Virginia
    'Eos Energy': 'US',            # Pennsylvania (zinc battery)

    # Foreign manufacturing
    'BYD': 'foreign',              # China
    'Samsung SDI': 'foreign',      # Korea
    'LG Energy Solution': 'foreign', # Korea
    'LG': 'foreign',               # Korea
    'CATL': 'foreign',             # China
    'Wärtsilä': 'foreign',         # Finland
    'Sungrow': 'foreign',          # China
    'Huawei': 'foreign',           # China
    'BYD Energy': 'foreign',       # China
}

INVERTER_MANUFACTURERS = {
    'Enphase': 'US',               # US design, Mexico assembly
    'SolarEdge': 'foreign',        # Israel
    'SMA': 'foreign',              # Germany
    'Huawei': 'foreign',           # China
    'Sungrow': 'foreign',          # China
    'ABB': 'foreign',              # Switzerland
    'Siemens': 'foreign',          # Germany
    'GE': 'US',
    'Power Electronics': 'foreign', # Spain
}


def _normalize_manufacturer(name):
    """Normalize manufacturer name for lookup."""
    if not name:
        return ''
    s = name.strip()
    # Common normalizations
    s = s.replace('Siemens Gamesa Renewable Energy', 'Siemens Gamesa Renewable Energy')
    return s


def check_wind_domestic_content(project, grid_conn=None):
    """
    Check domestic content for a wind project.
    Uses primary_manufacturer from USWTDB cross-reference.
    """
    manufacturer = project.get('primary_manufacturer') or ''

    if manufacturer:
        mfg_norm = _normalize_manufacturer(manufacturer)
        country = WIND_MANUFACTURERS.get(mfg_norm)

        if country == 'US':
            return {
                'eligible': 1,
                'confidence': 'high',
                'basis': f'Wind turbine manufacturer {manufacturer} has US manufacturing (USWTDB data)',
            }
        elif country == 'foreign':
            return {
                'eligible': 0,
                'confidence': 'high',
                'basis': f'Wind turbine manufacturer {manufacturer} is foreign-manufactured (USWTDB data)',
            }
        else:
            # Unknown manufacturer — check if name contains known keywords
            mfg_lower = manufacturer.lower()
            if any(k in mfg_lower for k in ['ge ', 'ge wind', 'general electric', 'clipper']):
                return {
                    'eligible': 1,
                    'confidence': 'medium',
                    'basis': f'Wind manufacturer {manufacturer} appears US-based',
                }
            return {
                'eligible': None,
                'confidence': 'low',
                'basis': f'Unknown wind manufacturer: {manufacturer}',
            }

    # No manufacturer data — try to look up via plant_id_eia in grid.db
    plant_id = project.get('plant_id_eia')
    if plant_id and grid_conn:
        row = grid_conn.execute('''
            SELECT manufacturer, COUNT(*) as cnt
            FROM wind_turbines
            WHERE eia_id = ?
            GROUP BY manufacturer
            ORDER BY cnt DESC
            LIMIT 1
        ''', (plant_id,)).fetchone()

        if row and row[0]:
            mfg = row[0]
            country = WIND_MANUFACTURERS.get(mfg)
            if country == 'US':
                return {
                    'eligible': 1,
                    'confidence': 'medium',
                    'basis': f'Wind turbines at EIA plant {plant_id} made by {mfg} (US) via grid.db lookup',
                }
            elif country == 'foreign':
                return {
                    'eligible': 0,
                    'confidence': 'medium',
                    'basis': f'Wind turbines at EIA plant {plant_id} made by {mfg} (foreign) via grid.db lookup',
                }

    return None  # Can't determine


def check_solar_domestic_content(project):
    """
    Check domestic content for a solar project.
    Without project-level BOM data, use heuristics:
    - Large utility-scale projects (>100 MW) in 2024+ are more likely to pursue DC bonus
    - Projects in states with domestic content incentives
    - Default to 'unknown' with low confidence
    """
    capacity = project.get('capacity_mw') or 0
    cod = project.get('cod_std') or project.get('cod') or ''
    state = (project.get('state') or '').upper()

    # Most US solar panels are imported. Only First Solar and Qcells have
    # significant US manufacturing. Without knowing the specific panel supplier,
    # we estimate based on market share:
    # - First Solar: ~15% of US utility-scale market
    # - Qcells (US factory): ~10%
    # - All others (imported): ~75%

    # Large projects post-IRA (2023+) in states with strong DC policies
    # are more likely to source domestically
    dc_friendly_states = {'GA', 'OH', 'TX', 'CA', 'AZ', 'NV', 'FL', 'IN', 'TN', 'SC'}

    if capacity >= 100 and cod >= '2024':
        if state in dc_friendly_states:
            return {
                'eligible': None,  # Unknown — can't confirm without BOM
                'confidence': 'low',
                'basis': f'Large solar ({capacity:.0f} MW) post-IRA in {state} — domestic content possible but unverified. ~25% of US solar panels are US-made (First Solar, Qcells).',
            }

    # Default for solar: likely not eligible (75% of panels are imported)
    return {
        'eligible': 0,
        'confidence': 'low',
        'basis': 'Solar project — ~75% of US solar panels are imported. Cannot verify domestic content without project-specific BOM data.',
    }


def check_storage_domestic_content(project):
    """
    Check domestic content for a storage project.
    US has growing but still minority battery manufacturing.
    Tesla Megapack (Nevada) and Fluence are the main US options.
    """
    capacity = project.get('capacity_mw') or 0
    developer = (project.get('developer') or project.get('developer_canonical') or '').lower()

    # Tesla is the only large-scale US battery manufacturer with significant capacity
    # If developer is Tesla or project name suggests Tesla
    name = (project.get('name') or '').lower()
    if 'tesla' in developer or 'tesla' in name or 'megapack' in name:
        return {
            'eligible': 1,
            'confidence': 'medium',
            'basis': 'Storage project associated with Tesla — Megapack manufactured in Nevada (US)',
        }

    # Most grid-scale storage uses imported cells (LG, Samsung, CATL, BYD)
    return {
        'eligible': 0,
        'confidence': 'low',
        'basis': 'Storage project — majority of US grid battery cells are imported (LG, Samsung, CATL). Cannot verify without project-specific BOM.',
    }


def check_hybrid_domestic_content(project):
    """Solar + Storage hybrid — check both components."""
    solar_result = check_solar_domestic_content(project)
    storage_result = check_storage_domestic_content(project)

    # Both components need to meet threshold. If either is clearly not eligible, project isn't.
    if solar_result['eligible'] == 0 and storage_result['eligible'] == 0:
        return {
            'eligible': 0,
            'confidence': 'low',
            'basis': 'Solar+Storage hybrid — both solar panels and battery cells likely imported. ' + solar_result['basis'],
        }

    if solar_result['eligible'] == 1 and storage_result['eligible'] == 1:
        return {
            'eligible': 1,
            'confidence': min(solar_result['confidence'], storage_result['confidence'], key=['high', 'medium', 'low'].index),
            'basis': f"Both components likely US-sourced. Solar: {solar_result['basis']}; Storage: {storage_result['basis']}",
        }

    return {
        'eligible': 0,
        'confidence': 'low',
        'basis': 'Solar+Storage hybrid — mixed domestic content status, unlikely to meet threshold for both components.',
    }


def run_enrichment(master_conn, grid_conn=None, dry_run=False):
    """Run domestic content checks on all projects in master.db."""
    master_conn.row_factory = sqlite3.Row

    # Add columns if needed
    cols = [c[1] for c in master_conn.execute('PRAGMA table_info(projects)').fetchall()]
    if not dry_run:
        if 'domestic_content_confidence' not in cols:
            master_conn.execute('ALTER TABLE projects ADD COLUMN domestic_content_confidence TEXT')
            print("  Added domestic_content_confidence column")
        if 'domestic_content_basis' not in cols:
            master_conn.execute('ALTER TABLE projects ADD COLUMN domestic_content_basis TEXT')
            print("  Added domestic_content_basis column")

    projects = master_conn.execute('''
        SELECT id, queue_id, region, name, state, capacity_mw, type_std,
               primary_manufacturer, plant_id_eia, developer, developer_canonical,
               cod_std, cod
        FROM projects
        WHERE type_std IN ('Wind', 'Solar', 'Storage', 'Solar + Storage', 'Hybrid')
    ''').fetchall()

    print(f"Checking {len(projects)} eligible projects (Wind/Solar/Storage/Hybrid)...", flush=True)

    stats = defaultdict(int)
    results = []

    for proj in projects:
        p = dict(proj)
        ptype = p.get('type_std', '')
        result = None

        if ptype == 'Wind':
            result = check_wind_domestic_content(p, grid_conn)
        elif ptype == 'Solar':
            result = check_solar_domestic_content(p)
        elif ptype == 'Storage':
            result = check_storage_domestic_content(p)
        elif ptype in ('Solar + Storage', 'Hybrid'):
            result = check_hybrid_domestic_content(p)

        if result:
            results.append((p['id'], result['eligible'], result['confidence'], result['basis']))
            if result['eligible'] == 1:
                stats['eligible'] += 1
            elif result['eligible'] == 0:
                stats['not_eligible'] += 1
            else:
                stats['unknown'] += 1
            stats[f"conf_{result['confidence']}"] += 1
        else:
            stats['no_data'] += 1

    stats['total_checked'] = len(results)
    stats['total_projects'] = len(projects)

    print(f"\nResults:")
    print(f"  Checked: {len(results)}/{len(projects)}")
    print(f"  Eligible: {stats['eligible']}")
    print(f"  Not eligible: {stats['not_eligible']}")
    print(f"  Unknown: {stats['unknown']}")
    print(f"  No data: {stats['no_data']}")
    print(f"  Confidence — High: {stats.get('conf_high', 0)}, Medium: {stats.get('conf_medium', 0)}, Low: {stats.get('conf_low', 0)}")

    if not dry_run and results:
        print("\nWriting to master.db...", flush=True)
        updated = 0
        for proj_id, eligible, confidence, basis in results:
            master_conn.execute('''
                UPDATE projects
                SET domestic_content_eligible = ?,
                    domestic_content_confidence = ?,
                    domestic_content_basis = ?
                WHERE id = ?
            ''', (eligible, confidence, basis, proj_id))
            updated += 1
        master_conn.commit()
        print(f"  Updated {updated} projects")

    return stats


def print_stats(master_conn):
    """Print current domestic content stats."""
    master_conn.row_factory = sqlite3.Row
    total = master_conn.execute('SELECT COUNT(*) FROM projects').fetchone()[0]
    checked = master_conn.execute('SELECT COUNT(*) FROM projects WHERE domestic_content_eligible IS NOT NULL OR domestic_content_confidence IS NOT NULL').fetchone()[0]

    print(f"\n=== Domestic Content Statistics ===")
    print(f"Total projects: {total}")
    print(f"Checked: {checked} ({checked/total*100:.1f}%)")

    print("\nBy eligibility:")
    for val, label in [(1, 'Eligible'), (0, 'Not eligible')]:
        cnt = master_conn.execute(
            'SELECT COUNT(*) FROM projects WHERE domestic_content_eligible = ?', (val,)
        ).fetchone()[0]
        print(f"  {label}: {cnt}")

    null_conf = master_conn.execute(
        "SELECT COUNT(*) FROM projects WHERE domestic_content_confidence IS NOT NULL AND domestic_content_eligible IS NULL"
    ).fetchone()[0]
    if null_conf:
        print(f"  Unknown (checked but indeterminate): {null_conf}")

    print("\nBy confidence:")
    for conf in ['high', 'medium', 'low']:
        cnt = master_conn.execute(
            'SELECT COUNT(*) FROM projects WHERE domestic_content_confidence = ?', (conf,)
        ).fetchone()[0]
        if cnt:
            print(f"  {conf}: {cnt}")

    print("\nBy type:")
    rows = master_conn.execute('''
        SELECT type_std,
               COUNT(*) as total,
               SUM(CASE WHEN domestic_content_eligible = 1 THEN 1 ELSE 0 END) as eligible,
               SUM(CASE WHEN domestic_content_eligible = 0 THEN 1 ELSE 0 END) as not_eligible,
               SUM(CASE WHEN domestic_content_confidence IS NOT NULL THEN 1 ELSE 0 END) as checked
        FROM projects
        WHERE type_std IN ('Wind', 'Solar', 'Storage', 'Solar + Storage', 'Hybrid')
        GROUP BY type_std
        ORDER BY total DESC
    ''').fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[4]} checked / {r[1]} total — {r[2]} eligible, {r[3]} not")

    # Wind manufacturer breakdown
    print("\nWind by manufacturer (top 10):")
    rows = master_conn.execute('''
        SELECT primary_manufacturer,
               COUNT(*) as cnt,
               SUM(CASE WHEN domestic_content_eligible = 1 THEN 1 ELSE 0 END) as eligible
        FROM projects
        WHERE type_std = 'Wind' AND primary_manufacturer IS NOT NULL AND primary_manufacturer != ''
        GROUP BY primary_manufacturer
        ORDER BY cnt DESC
        LIMIT 10
    ''').fetchall()
    for r in rows:
        status = "US" if r[2] > 0 else "foreign"
        print(f"  {r[0]}: {r[1]} projects ({status})")


def main():
    parser = argparse.ArgumentParser(description='Domestic content eligibility checker')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing')
    parser.add_argument('--stats', action='store_true', help='Show current stats')
    args = parser.parse_args()

    master_conn = sqlite3.connect(str(MASTER_DB))
    grid_conn = None
    if GRID_DB.exists():
        grid_conn = sqlite3.connect(str(GRID_DB))

    if args.stats:
        print_stats(master_conn)
        master_conn.close()
        if grid_conn:
            grid_conn.close()
        return

    print("=== Domestic Content Eligibility Checker ===\n", flush=True)
    stats = run_enrichment(master_conn, grid_conn, dry_run=args.dry_run)

    if not args.dry_run:
        print_stats(master_conn)

    master_conn.close()
    if grid_conn:
        grid_conn.close()


if __name__ == '__main__':
    main()
