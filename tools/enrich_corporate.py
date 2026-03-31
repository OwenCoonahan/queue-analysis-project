#!/usr/bin/env python3
"""
Corporate Ownership Enrichment for master.db

Matches developer_canonical → corporate.db to find parent companies.
Uses SEC 10-K filings (company_info) and parent-subsidiary relationships.

Strategy:
1. Build a normalized lookup from corporate.db company names → parent company
2. For each unique developer_canonical in master.db, try to match
3. Write parent_company back to master.db for all matched projects

Matching approach (in order):
  a) Exact normalized match (strip LLC/Inc/Corp, lowercase)
  b) Contains match (developer name contains or is contained by company name)
  c) Known manual mappings for top developers
"""

import os
import re
import sqlite3
import sys
from collections import defaultdict
from typing import Optional

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_DB = os.path.join(SCRIPT_DIR, ".data", "master.db")
CORPORATE_DB = os.path.join(
    SCRIPT_DIR, "..", "..", "..", "prospector-platform", "pipelines", "databases", "corporate.db"
)

# Manual mappings for developers that won't fuzzy-match well
# These are the biggest developers that are private or use different names
MANUAL_PARENT_MAP = {
    "invenergy": "Invenergy LLC",
    "leeward asset management": "Leeward Renewable Energy",
    "mn8 energy": "MN8 Energy",
    "cypress creek renewables": "Cypress Creek Renewables",
    "metc": "METC (Michigan Electric Transmission Co.)",
    "itc midwest": "ITC Holdings Corp.",
    "itc great plains": "ITC Holdings Corp.",
    "itc transmission": "ITC Holdings Corp.",
    "clearway energy": "Clearway Energy Group",
    "lightsource bp": "Lightsource BP",
    "longroad energy": "Longroad Energy",
    "orsted": "Ørsted",
    "ørsted": "Ørsted",
    "savion": "Savion (Shell subsidiary)",
    "terra-gen": "Terra-Gen LLC",
    "recurrent energy": "Recurrent Energy (Canadian Solar)",
    "canadian solar": "Canadian Solar Inc.",
    "8minute solar energy": "8minute Solar Energy",
    "pine gate renewables": "Pine Gate Renewables",
    "silicon ranch": "Silicon Ranch (Shell subsidiary)",
    "sol systems": "Sol Systems",
    "open road renewables": "Open Road Renewables",
    "ranger power": "Ranger Power",
    "apex clean energy": "Apex Clean Energy",
    "origis energy": "Origis Energy",
    "engie": "ENGIE SA",
    "swift current energy": "Swift Current Energy",
    "calpine": "Calpine Corporation",
    "tenaska": "Tenaska Inc.",
    "vistra": "Vistra Corp.",
    "talen energy": "Talen Energy Corporation",
    "sempra": "Sempra Energy",
    "we energies": "WEC Energy Group",
    "consumers energy": "CMS Energy Corporation",
    "alliant energy": "Alliant Energy Corporation",
    "aep": "American Electric Power Co.",
    "american electric power": "American Electric Power Co.",
    "firstenergy": "FirstEnergy Corp.",
    "exelon": "Exelon Corporation",
    "constellation": "Constellation Energy Corp.",
    "evergy": "Evergy Inc.",
    "eversource": "Eversource Energy",
    "national grid": "National Grid plc",
    "pseg": "Public Service Enterprise Group",
    "edison international": "Edison International",
    "southern california edison": "Edison International",
    "american transmission": "American Transmission Co. (ITC Holdings)",
    "edf renewables": "EDF Renewables (EDF Group)",
    "edf renewable asset holdings": "EDF Renewables (EDF Group)",
    "aes clean energy": "AES Corporation",
    "origis energy usa": "Origis Energy",
    "origis energy": "Origis Energy",
    "arevon energy": "Arevon Energy (CDPQ)",
    "onward energy": "Onward Energy",
    "altus power america management": "Altus Power Inc.",
    "altus power": "Altus Power Inc.",
    "urban grid solar": "Urban Grid (AES subsidiary)",
    "basin electric power coop": "Basin Electric Power Cooperative",
    "boralex us operations": "Boralex Inc.",
    "boralex": "Boralex Inc.",
    "montana-dakota utilities": "MDU Resources Group",
    "northern states power co - minnesota": "Xcel Energy Inc.",
    "northern states power": "Xcel Energy Inc.",
    "public service co of nm": "PNM Resources Inc.",
    "ameren transmission company of illinois": "Ameren Corporation",
    "ameren illinois": "Ameren Corporation",
    "nextera energy resources": "NextEra Energy Inc.",
    "fpl group": "NextEra Energy Inc.",
    "florida power & light": "NextEra Energy Inc.",
    "northern states power co - minnesota": "Xcel Energy Inc.",
    "montana-dakota utilities": "MDU Resources Group Inc.",
    "edf renewable asset holdings": "EDF Renewables (EDF Group)",
    "national grid renewables": "National Grid plc",
    "calpine mid-atlantic generation": "Calpine Corporation",
    "acciona energy usa global": "Acciona SA",
    "bhe renewables": "Berkshire Hathaway Energy",
    "big rivers electric": "Big Rivers Electric Corporation",
    "great river energy": "Great River Energy",
    "omaha public power district": "Omaha Public Power District",
    "dg amp solar": "DG AMP Solar",
    "naturgy candela devco": "Naturgy Energy Group",
    "u s bureau of reclamation": "US Bureau of Reclamation (DOI)",
    "usace northwestern division": "US Army Corps of Engineers",
    "consolidated edison development": "Consolidated Edison Inc.",
    "pge": "PG&E Corporation",
    "pacific gas & electric": "PG&E Corporation",
    "avangrid power": "Avangrid Inc.",
    "avangrid renewables": "Avangrid Inc.",
    "avangrid": "Avangrid Inc.",
    "bp": "BP plc",
    "enel": "Enel SpA",
    "enel green power": "Enel SpA",
    "pattern energy group": "Pattern Energy Group",
    "tva": "Tennessee Valley Authority",
}


def normalize_name(name: str) -> str:
    """Normalize a company name for matching."""
    if not name:
        return ""
    s = name.lower().strip()
    # Remove common suffixes
    for suffix in [
        ", inc.", ", inc", " inc.", " inc", " incorporated",
        ", llc", " llc", ", l.l.c.", " l.l.c.",
        ", lp", " lp", ", l.p.", " l.p.",
        " corp.", " corp", " corporation",
        ", co.", " co.", " company",
        " ltd.", " ltd", " limited",
        " /de/", " /va/", " /md/", " /ma/", " /ny/", " /ct/", " /ga/",
        " plc", " sa", " s.a.", " ag", " gmbh", " n.v.",
        " group", " holdings", " holding",
    ]:
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()
    # Remove punctuation
    s = re.sub(r"[,.'\"()-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def build_parent_lookup(corp_conn: sqlite3.Connection) -> dict:
    """
    Build a mapping: normalized_company_name → ultimate_parent_company_name

    Uses the most recent parent-subsidiary filings to find who owns whom.
    """
    # Step 1: Build subsidiary → parent mapping from most recent filings
    sub_to_parent = {}
    rows = corp_conn.execute("""
        SELECT subsidiary_company_name, parent_company_name, report_date
        FROM parents_and_subsidiaries
        WHERE subsidiary_company_name IS NOT NULL
          AND parent_company_name IS NOT NULL
        ORDER BY report_date DESC
    """).fetchall()

    for sub_name, parent_name, _ in rows:
        norm_sub = normalize_name(sub_name)
        if norm_sub and norm_sub not in sub_to_parent:
            sub_to_parent[norm_sub] = parent_name

    # Step 2: Build company_name → most common/recent parent
    # For companies in company_info, check if they appear as subsidiaries
    company_names = corp_conn.execute("""
        SELECT DISTINCT company_name FROM company_info
        WHERE company_name IS NOT NULL
    """).fetchall()

    name_to_parent = {}
    for (cname,) in company_names:
        norm = normalize_name(cname)
        if norm in sub_to_parent:
            name_to_parent[norm] = sub_to_parent[norm]
        else:
            # Company is itself a parent (or standalone)
            name_to_parent[norm] = cname

    # Step 3: Also add parent companies directly
    parent_names = corp_conn.execute("""
        SELECT DISTINCT parent_company_name FROM parents_and_subsidiaries
        WHERE parent_company_name IS NOT NULL
    """).fetchall()

    for (pname,) in parent_names:
        norm = normalize_name(pname)
        if norm not in name_to_parent:
            name_to_parent[norm] = pname

    # Step 4: Resolve chains (A owned by B owned by C → A maps to C)
    def resolve_ultimate(name, depth=0):
        if depth > 5:
            return name
        norm = normalize_name(name)
        parent = sub_to_parent.get(norm)
        if parent and normalize_name(parent) != norm:
            return resolve_ultimate(parent, depth + 1)
        return name

    ultimate = {}
    for norm, parent in name_to_parent.items():
        ultimate[norm] = resolve_ultimate(parent)

    return ultimate


def title_case_company(name: str) -> str:
    """Proper title case for company names, preserving known acronyms."""
    if not name:
        return name
    # Already looks good (has mixed case)
    if name != name.lower() and name != name.upper():
        return name
    return name.title()


def match_developer(dev_canonical: str, parent_lookup: dict) -> Optional[str]:
    """Try to match a developer_canonical to a parent company."""
    if not dev_canonical:
        return None

    norm = normalize_name(dev_canonical)

    # 1. Check manual mappings first
    if norm in MANUAL_PARENT_MAP:
        return MANUAL_PARENT_MAP[norm]

    # 2. Exact normalized match
    if norm in parent_lookup:
        return title_case_company(parent_lookup[norm])

    # 3. Try with common energy suffixes stripped
    for suffix in [" energy", " power", " renewables", " renewable", " solar", " wind", " generation"]:
        base = norm.rstrip(suffix) if norm.endswith(suffix) else None
        if base and base in parent_lookup:
            return title_case_company(parent_lookup[base])

    # 4. Try adding common suffixes
    for suffix in [" energy", " corp", ""]:
        candidate = norm + suffix
        if candidate in parent_lookup:
            return title_case_company(parent_lookup[candidate])

    # 5. Containment match — developer name is prefix of a company name
    # Only for names >= 5 chars to avoid false positives
    if len(norm) >= 5:
        matches = []
        for lookup_name, parent in parent_lookup.items():
            if lookup_name.startswith(norm + " ") or lookup_name == norm:
                matches.append((lookup_name, parent))
        if len(matches) == 1:
            return title_case_company(matches[0][1])
        elif len(matches) > 1:
            # Pick the shortest/most-specific match
            matches.sort(key=lambda x: len(x[0]))
            return title_case_company(matches[0][1])

    return None


def run_enrichment(dry_run: bool = False, stats_only: bool = False):
    """Run the corporate ownership enrichment."""

    # Verify databases exist
    if not os.path.exists(MASTER_DB):
        print(f"ERROR: master.db not found at {MASTER_DB}")
        sys.exit(1)
    if not os.path.exists(CORPORATE_DB):
        print(f"ERROR: corporate.db not found at {CORPORATE_DB}")
        sys.exit(1)

    print("=== Corporate Ownership Enrichment ===\n")

    # Connect to databases
    corp_conn = sqlite3.connect(CORPORATE_DB)
    corp_conn.row_factory = sqlite3.Row

    master_conn = sqlite3.connect(MASTER_DB)
    master_conn.row_factory = sqlite3.Row

    # Build parent lookup
    print("Building parent company lookup from corporate.db...")
    parent_lookup = build_parent_lookup(corp_conn)
    print(f"  {len(parent_lookup):,} normalized company names indexed")

    # Get unique developers
    developers = master_conn.execute("""
        SELECT developer_canonical, COUNT(*) as cnt
        FROM projects
        WHERE developer_canonical IS NOT NULL
        GROUP BY developer_canonical
        ORDER BY cnt DESC
    """).fetchall()

    print(f"  {len(developers):,} unique developers in master.db")

    # Match each developer
    matched = {}
    unmatched = []
    already_has_parent = {}

    for row in developers:
        dev = row["developer_canonical"]
        cnt = row["cnt"]
        parent = match_developer(dev, parent_lookup)
        if parent:
            matched[dev] = (parent, cnt)
        else:
            unmatched.append((dev, cnt))

    # Check existing parent_company coverage
    existing = master_conn.execute("""
        SELECT COUNT(*) FROM projects WHERE parent_company IS NOT NULL
    """).fetchone()[0]

    # Count new matches (projects that don't already have parent_company)
    new_match_devs = []
    for dev, (parent, cnt) in matched.items():
        check = master_conn.execute(
            "SELECT COUNT(*) FROM projects WHERE developer_canonical = ? AND parent_company IS NULL",
            (dev,)
        ).fetchone()[0]
        if check > 0:
            new_match_devs.append((dev, parent, check))

    total_new_projects = sum(c for _, _, c in new_match_devs)
    total_matched_projects = sum(cnt for _, (_, cnt) in matched.items())

    print(f"\n--- Match Results ---")
    print(f"  Developers matched:   {len(matched):,} / {len(developers):,} ({100*len(matched)/len(developers):.1f}%)")
    print(f"  Projects covered:     {total_matched_projects:,} / 37,810 with developer ({100*total_matched_projects/37810:.1f}%)")
    print(f"  Existing parent_co:   {existing:,} projects")
    print(f"  NEW matches:          {total_new_projects:,} projects ({len(new_match_devs)} developers)")
    print(f"  After enrichment:     {existing + total_new_projects:,} projects with parent_company")

    # Show top new matches
    new_match_devs.sort(key=lambda x: x[2], reverse=True)
    print(f"\n--- Top 25 New Matches ---")
    for dev, parent, cnt in new_match_devs[:25]:
        print(f"  {dev:40s} → {parent:40s} ({cnt} projects)")

    # Show top unmatched
    print(f"\n--- Top 25 Unmatched Developers ---")
    for dev, cnt in unmatched[:25]:
        print(f"  {dev:40s} ({cnt} projects)")

    if stats_only:
        corp_conn.close()
        master_conn.close()
        return

    if dry_run:
        print("\n[DRY RUN] No changes written.")
        corp_conn.close()
        master_conn.close()
        return

    # Write matches to master.db
    print(f"\nWriting parent_company to master.db...")

    updated = 0
    for dev, (parent, _) in matched.items():
        cur = master_conn.execute(
            "UPDATE projects SET parent_company = ? WHERE developer_canonical = ? AND parent_company IS NULL",
            (parent, dev)
        )
        updated += cur.rowcount

    master_conn.commit()

    # Verify
    final_count = master_conn.execute(
        "SELECT COUNT(*) FROM projects WHERE parent_company IS NOT NULL"
    ).fetchone()[0]

    total = master_conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]

    print(f"  Updated {updated:,} projects")
    print(f"  Final parent_company coverage: {final_count:,} / {total:,} ({100*final_count/total:.1f}%)")

    # Show parent_company distribution
    print(f"\n--- Top 20 Parent Companies ---")
    top_parents = master_conn.execute("""
        SELECT parent_company, COUNT(*) as cnt
        FROM projects WHERE parent_company IS NOT NULL
        GROUP BY parent_company ORDER BY cnt DESC LIMIT 20
    """).fetchall()
    for row in top_parents:
        print(f"  {row['parent_company']:45s} {row['cnt']:,} projects")

    corp_conn.close()
    master_conn.close()
    print("\nDone.")


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    stats_only = "--stats" in sys.argv
    run_enrichment(dry_run=dry_run, stats_only=stats_only)
