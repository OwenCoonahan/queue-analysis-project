#!/usr/bin/env python3
"""Enrich developer.db with contact info from EIA-860 ownership and corporate.db.

Also syncs developer_tier from developer.db back to master.db projects.

Sources:
  - EIA-860 ownership table: owner_state, owner_city for 3,200+ utilities
  - corporate.db company_info: phone_number, business_state, business_city for 46K+ companies
  - corporate.db parents_and_subsidiaries: parent-subsidiary relationships

Usage:
    python3 enrich_developer_contacts.py --enrich    # Enrich contacts + sync tiers
    python3 enrich_developer_contacts.py --stats     # Show enrichment stats
    python3 enrich_developer_contacts.py --top       # Show top investable developer contacts
"""

import argparse
import json
import logging
import os
import re
import sqlite3
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get('DATA_DIR', str(Path(__file__).parent / '.data')))
MASTER_DB = Path(os.environ.get('QUEUE_DB_PATH', os.environ.get('MASTER_DB_PATH', str(DATA_DIR / 'master.db'))))
DEV_DB = Path(os.environ.get('DEVELOPER_DB_PATH', str(DATA_DIR / 'developer.db')))
PIPELINE_DB_DIR = Path(os.environ.get('PIPELINE_DB_DIR',
    str(Path(__file__).parent.parent.parent.parent / 'prospector-platform' / 'pipelines' / 'databases')))
EIA_DB = PIPELINE_DB_DIR / 'eia.db'
CORPORATE_DB = PIPELINE_DB_DIR / 'corporate.db'


def _normalize(name: str) -> str:
    """Normalize a company name for fuzzy matching."""
    if not name:
        return ''
    name = name.lower().strip()
    # Remove common suffixes
    for suffix in [', llc', ' llc', ', inc', ' inc', ', corp', ' corp',
                   ', ltd', ' ltd', ', lp', ' lp', ', co', ' co',
                   ', l.l.c.', ', l.p.', ' company', ' corporation',
                   ' holdings', ' group', ' services', ' enterprises']:
        name = name.replace(suffix, '')
    # Remove punctuation
    name = re.sub(r'[^\w\s]', '', name)
    return name.strip()


def enrich_contacts():
    """Enrich developer.db with contact info from EIA and corporate.db."""

    if not DEV_DB.exists():
        logger.error(f"developer.db not found at {DEV_DB}")
        return

    dev_conn = sqlite3.connect(DEV_DB)
    dev_conn.row_factory = sqlite3.Row

    # Ensure contact columns exist
    for col, col_type in [
        ('contact_city', 'TEXT'),
        ('contact_state', 'TEXT'),
        ('contact_phone', 'TEXT'),
        ('contact_source', 'TEXT'),
    ]:
        try:
            dev_conn.execute(f"ALTER TABLE developers ADD COLUMN {col} {col_type}")
            dev_conn.commit()
            logger.info(f"  Added column {col}")
        except sqlite3.OperationalError:
            pass

    # Load all developers
    devs = dev_conn.execute(
        "SELECT id, canonical_name, display_name, parent_company, headquarters_state FROM developers"
    ).fetchall()
    logger.info(f"Loaded {len(devs):,} developers from developer.db")

    # Build name → developer_id lookup (normalized)
    dev_lookup = {}
    for d in devs:
        norm = _normalize(d['canonical_name'])
        if norm:
            dev_lookup[norm] = d['id']
        if d['display_name']:
            norm2 = _normalize(d['display_name'])
            if norm2:
                dev_lookup[norm2] = d['id']

    stats = {'eia_matched': 0, 'corp_matched': 0, 'phone_added': 0,
             'city_added': 0, 'state_added': 0, 'hq_state_added': 0}

    # ── Step 1: EIA-860 ownership → state, city ─────────────────────────
    if EIA_DB.exists():
        logger.info(f"\n[1/3] Loading EIA-860 ownership data from {EIA_DB}...")
        eia_conn = sqlite3.connect(EIA_DB)
        # Get latest record per utility (most recent report_date)
        eia_rows = eia_conn.execute("""
            SELECT owner_utility_name_eia, owner_state, owner_city,
                   MAX(report_date) as latest
            FROM ownership
            WHERE owner_state IS NOT NULL AND owner_state <> ''
            GROUP BY owner_utility_name_eia
        """).fetchall()
        eia_conn.close()
        logger.info(f"  {len(eia_rows):,} distinct EIA utilities with location data")

        for row in eia_rows:
            name = row[0]
            state = row[1]
            city = row[2]
            norm = _normalize(name)
            if norm in dev_lookup:
                dev_id = dev_lookup[norm]
                updates = []
                params = []
                if city:
                    updates.append("contact_city = COALESCE(contact_city, ?)")
                    params.append(city)
                    stats['city_added'] += 1
                if state:
                    updates.append("contact_state = COALESCE(contact_state, ?)")
                    params.append(state)
                    stats['state_added'] += 1
                    updates.append("headquarters_state = COALESCE(headquarters_state, ?)")
                    params.append(state)
                    stats['hq_state_added'] += 1
                if updates:
                    updates.append("contact_source = COALESCE(contact_source, 'eia_860')")
                    dev_conn.execute(
                        f"UPDATE developers SET {', '.join(updates)} WHERE id = ?",
                        params + [dev_id]
                    )
                    stats['eia_matched'] += 1

        dev_conn.commit()
        logger.info(f"  Matched {stats['eia_matched']:,} developers via EIA-860")
    else:
        logger.warning(f"  EIA database not found at {EIA_DB}")

    # ── Step 2: corporate.db → phone, state, city ────────────────────────
    if CORPORATE_DB.exists():
        logger.info(f"\n[2/3] Loading corporate.db contact data from {CORPORATE_DB}...")
        corp_conn = sqlite3.connect(CORPORATE_DB)

        # Get company_info with phone numbers (deduplicated by company name)
        corp_rows = corp_conn.execute("""
            SELECT company_name, business_state, business_city, phone_number,
                   MAX(filing_date) as latest
            FROM company_info
            WHERE phone_number IS NOT NULL AND phone_number <> ''
            GROUP BY company_name
        """).fetchall()
        logger.info(f"  {len(corp_rows):,} companies with phone data")

        for row in corp_rows:
            name = row[0]
            state = row[1]
            city = row[2]
            phone = row[3]
            norm = _normalize(name)
            if norm in dev_lookup:
                dev_id = dev_lookup[norm]
                updates = []
                params = []
                if phone:
                    # Format phone number
                    phone_str = str(phone).strip()
                    if len(phone_str) == 10:
                        phone_str = f"({phone_str[:3]}) {phone_str[3:6]}-{phone_str[6:]}"
                    updates.append("contact_phone = COALESCE(contact_phone, ?)")
                    params.append(phone_str)
                    stats['phone_added'] += 1
                if city:
                    updates.append("contact_city = COALESCE(contact_city, ?)")
                    params.append(city)
                if state:
                    updates.append("contact_state = COALESCE(contact_state, ?)")
                    params.append(state)
                    updates.append("headquarters_state = COALESCE(headquarters_state, ?)")
                    params.append(state)
                if updates:
                    updates.append("contact_source = CASE WHEN contact_source IS NULL THEN 'corporate_db' WHEN contact_source NOT LIKE '%corporate_db%' THEN contact_source || ',corporate_db' ELSE contact_source END")
                    dev_conn.execute(
                        f"UPDATE developers SET {', '.join(updates)} WHERE id = ?",
                        params + [dev_id]
                    )
                    stats['corp_matched'] += 1

        # Also load parent-subsidiary relationships
        logger.info("  Loading parent-subsidiary relationships...")
        rel_rows = corp_conn.execute("""
            SELECT DISTINCT parent_company_name, subsidiary_company_name
            FROM parents_and_subsidiaries
            WHERE parent_company_name IS NOT NULL AND subsidiary_company_name IS NOT NULL
            LIMIT 100000
        """).fetchall()
        corp_conn.close()

        # Build id lookup for relationships
        id_by_name = {}
        for d in devs:
            id_by_name[_normalize(d['canonical_name'])] = d['id']

        rel_count = 0
        for row in rel_rows:
            parent_norm = _normalize(row[0])
            child_norm = _normalize(row[1])
            if parent_norm in id_by_name and child_norm in id_by_name:
                parent_id = id_by_name[parent_norm]
                child_id = id_by_name[child_norm]
                if parent_id != child_id:
                    try:
                        dev_conn.execute(
                            "INSERT OR IGNORE INTO developer_relationships (parent_id, child_id, relationship_type, source) VALUES (?, ?, 'subsidiary', 'corporate_db')",
                            [parent_id, child_id]
                        )
                        rel_count += 1
                    except sqlite3.IntegrityError:
                        pass

        dev_conn.commit()
        logger.info(f"  Matched {stats['corp_matched']:,} developers via corporate.db")
        logger.info(f"  Added {rel_count:,} parent-subsidiary relationships")
    else:
        logger.warning(f"  Corporate database not found at {CORPORATE_DB}")

    # ── Step 3: Sync developer_tier to master.db ─────────────────────────
    logger.info(f"\n[3/3] Syncing developer_tier to master.db...")
    if MASTER_DB.exists():
        master_conn = sqlite3.connect(MASTER_DB)

        # Get tier data from developer.db
        tier_data = dev_conn.execute(
            "SELECT canonical_name, tier, tier_confidence, tier_method, needs_capital, capital_evidence FROM developers"
        ).fetchall()

        updated = 0
        for row in tier_data:
            result = master_conn.execute(
                """UPDATE projects SET
                    developer_tier = ?,
                    developer_tier_confidence = ?,
                    developer_tier_method = ?,
                    developer_needs_capital = ?,
                    developer_capital_evidence = ?
                WHERE developer_canonical = ?""",
                [row[1], row[2], row[3], row[4], row[5], row[0]]
            )
            updated += result.rowcount

        master_conn.commit()
        master_conn.close()
        logger.info(f"  Updated developer_tier on {updated:,} master.db projects")
    else:
        logger.warning(f"  master.db not found at {MASTER_DB}")

    dev_conn.close()

    # Print summary
    logger.info(f"\n{'='*50}")
    logger.info("Contact Enrichment Summary:")
    logger.info(f"  EIA-860 matches:    {stats['eia_matched']:,}")
    logger.info(f"  Corporate matches:  {stats['corp_matched']:,}")
    logger.info(f"  Phone numbers added:{stats['phone_added']:,}")
    logger.info(f"  Cities added:       {stats['city_added']:,}")
    logger.info(f"  States added:       {stats['state_added']:,}")
    logger.info(f"  HQ states added:    {stats['hq_state_added']:,}")


def show_stats():
    """Show contact enrichment coverage."""
    if not DEV_DB.exists():
        print("developer.db not found")
        return

    conn = sqlite3.connect(DEV_DB)

    total = conn.execute("SELECT COUNT(*) FROM developers").fetchone()[0]

    # Check if contact columns exist
    cols = {r[1] for r in conn.execute("PRAGMA table_info(developers)").fetchall()}

    print(f"\n=== Developer Contact Coverage ({total:,} developers) ===\n")

    for col in ['headquarters_state', 'contact_state', 'contact_city', 'contact_phone', 'contact_source', 'website']:
        if col in cols:
            count = conn.execute(f"SELECT COUNT(*) FROM developers WHERE {col} IS NOT NULL AND {col} <> ''").fetchone()[0]
            pct = count / total * 100 if total else 0
            print(f"  {col:<25} {count:>6,} ({pct:>5.1f}%)")
        else:
            print(f"  {col:<25} — (column not yet created)")

    print(f"\n=== Relationships ===")
    rel_count = conn.execute("SELECT COUNT(*) FROM developer_relationships").fetchone()[0]
    print(f"  Parent-subsidiary links: {rel_count:,}")

    # Tier distribution
    print(f"\n=== Tier Distribution ===\n")
    for row in conn.execute("SELECT tier, COUNT(*) FROM developers GROUP BY tier ORDER BY COUNT(*) DESC").fetchall():
        print(f"  {row[0] or 'NULL':<20} {row[1]:>6,}")

    conn.close()


def show_top_investable_contacts():
    """Show contact info for developers on investable projects."""
    if not DEV_DB.exists() or not MASTER_DB.exists():
        print("developer.db or master.db not found")
        return

    master = sqlite3.connect(MASTER_DB)
    dev_conn = sqlite3.connect(DEV_DB)
    dev_conn.row_factory = sqlite3.Row

    # Get unique developers from investable projects
    inv_devs = master.execute("""
        SELECT DISTINCT developer_canonical, COUNT(*) as cnt,
               ROUND(SUM(capacity_mw), 1) as mw,
               GROUP_CONCAT(DISTINCT state) as states
        FROM projects
        WHERE investable = 1 AND developer_canonical IS NOT NULL
        GROUP BY developer_canonical
        ORDER BY cnt DESC
    """).fetchall()
    master.close()

    print(f"\n=== Investable Project Developers — Contact Info ===\n")
    print(f"{'Developer':<40} {'Proj':>4} {'MW':>8} {'Tier':<15} {'State':<5} {'City':<20} {'Phone':<16}")
    print("-" * 115)

    found = 0
    for row in inv_devs:
        canonical = row[0]
        dev = dev_conn.execute(
            """SELECT tier, headquarters_state, contact_city, contact_phone, contact_source
            FROM developers WHERE canonical_name = ?""",
            [canonical]
        ).fetchone()
        if dev:
            found += 1
            tier = dev['tier'] or '—'
            state = dev['headquarters_state'] or '—'
            city = (dev['contact_city'] or '—')[:20]
            phone = dev['contact_phone'] or '—'
            print(f"{canonical[:40]:<40} {row[1]:>4} {row[2]:>8.1f} {tier:<15} {state:<5} {city:<20} {phone:<16}")
        else:
            print(f"{canonical[:40]:<40} {row[1]:>4} {row[2]:>8.1f} {'—':<15} {'—':<5} {'—':<20} {'—':<16}")

    dev_conn.close()
    print(f"\n{found}/{len(inv_devs)} investable developers found in developer.db")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Enrich developer contacts')
    parser.add_argument('--enrich', action='store_true', help='Run contact enrichment')
    parser.add_argument('--stats', action='store_true', help='Show coverage stats')
    parser.add_argument('--top', action='store_true', help='Show top investable developer contacts')
    args = parser.parse_args()

    if args.enrich:
        enrich_contacts()
    elif args.stats:
        show_stats()
    elif args.top:
        show_top_investable_contacts()
    else:
        parser.print_help()
