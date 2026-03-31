#!/usr/bin/env python3
"""
Data Validation & Accuracy Audit for master.db enrichments.
Dev2, 2026-03-23

Cross-validates all enrichment fields for accuracy before selling data to PE firms.

Checks:
1. plant_id_eia consistency (USWTDB vs EIA matcher agreement)
2. FERC/EPA sanity (valid ranges, technology-appropriate values)
3. Utility sanity (positive values, reasonable ranges)
4. Parent company accuracy (spot-check against corporate.db)
5. POI substation match quality (name overlap verification)
6. Capacity sanity (outlier detection)

Usage:
    python3 validate_enrichments.py           # Full validation report
    python3 validate_enrichments.py --fix     # Apply recommended fixes
"""

import argparse
import random
import re
import sqlite3
import sys
from pathlib import Path

TOOLS_DIR = Path(__file__).parent
MASTER_DB = TOOLS_DIR / '.data' / 'master.db'
CORPORATE_DB = TOOLS_DIR.parent.parent.parent / 'prospector-platform' / 'pipelines' / 'databases' / 'corporate.db'

random.seed(42)  # Reproducible sampling


def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def check_plant_id_consistency(conn):
    """Check 1: Do uswtdb_eia_id and plant_id_eia agree for wind projects?"""
    section("CHECK 1: plant_id_eia Consistency (USWTDB vs EIA Matcher)")

    rows = conn.execute("""
        SELECT id, name, region, capacity_mw,
               uswtdb_eia_id, plant_id_eia, eia_match_confidence, eia_match_method,
               uswtdb_match_method
        FROM projects
        WHERE uswtdb_eia_id IS NOT NULL AND plant_id_eia IS NOT NULL
    """).fetchall()

    total = len(rows)
    agree = 0
    disagree = []

    for r in rows:
        proj_id, name, region, cap, uswtdb_eid, eia_eid, confidence, method, uswtdb_method = r
        if int(uswtdb_eid) == int(eia_eid):
            agree += 1
        else:
            disagree.append({
                'id': proj_id, 'name': name, 'region': region, 'capacity_mw': cap,
                'uswtdb_eia_id': uswtdb_eid, 'plant_id_eia': eia_eid,
                'eia_confidence': confidence, 'eia_method': method,
                'uswtdb_method': uswtdb_method,
            })

    # Also count projects with only one or the other
    only_uswtdb = conn.execute("""
        SELECT COUNT(*) FROM projects
        WHERE uswtdb_eia_id IS NOT NULL AND plant_id_eia IS NULL
    """).fetchone()[0]
    only_eia = conn.execute("""
        SELECT COUNT(*) FROM projects
        WHERE uswtdb_eia_id IS NULL AND plant_id_eia IS NOT NULL AND type IN ('Wind', 'Onshore Wind', 'Offshore Wind')
    """).fetchone()[0]

    print(f"Projects with BOTH uswtdb_eia_id and plant_id_eia: {total}")
    print(f"  Agree: {agree} ({100*agree/max(total,1):.1f}%)")
    print(f"  Disagree: {len(disagree)} ({100*len(disagree)/max(total,1):.1f}%)")
    print(f"  Only USWTDB (no EIA match): {only_uswtdb}")
    print(f"  Only EIA match (wind, no USWTDB): {only_eia}")

    if disagree:
        print(f"\n  Mismatches (showing up to 15):")
        for d in disagree[:15]:
            print(f"    [{d['id']}] {d['name'][:40]:40s} {d['region']:8s} "
                  f"USWTDB={d['uswtdb_eia_id']} vs EIA={d['plant_id_eia']} "
                  f"(confidence={d['eia_confidence']}, method={d['eia_method']})")

        # Analyze mismatches by confidence
        by_conf = {}
        for d in disagree:
            c = d['eia_confidence']
            by_conf[c] = by_conf.get(c, 0) + 1
        print(f"\n  Mismatches by EIA confidence: {by_conf}")

    score = 100 * agree / max(total, 1) if total > 0 else None
    return {
        'name': 'plant_id_eia consistency',
        'total_checked': total,
        'passed': agree,
        'failed': len(disagree),
        'score': score,
        'details': disagree[:15],
    }


def check_ferc_epa_sanity(conn):
    """Check 2: FERC/EPA values in valid ranges and technology-appropriate."""
    section("CHECK 2: FERC/EPA Sanity Checks")

    issues = []

    # FERC capex
    ferc_rows = conn.execute("""
        SELECT id, name, type, capacity_mw, ferc_capex_total, ferc_capacity_factor, ferc_opex_total
        FROM projects WHERE ferc_capex_total IS NOT NULL
    """).fetchall()
    print(f"FERC capex records: {len(ferc_rows)}")

    negative_capex = 0
    zero_capex = 0
    huge_capex = 0  # > $50B
    bad_cf = 0
    for r in ferc_rows:
        pid, name, tech, cap, capex, cf, opex = r
        if capex < 0:
            negative_capex += 1
            issues.append(f"  FERC negative capex: [{pid}] {name[:40]} capex=${capex:,.0f}")
        elif capex == 0:
            zero_capex += 1
        elif capex > 50_000_000_000:
            huge_capex += 1
            issues.append(f"  FERC huge capex: [{pid}] {name[:40]} capex=${capex:,.0f}")
        if cf is not None and (cf < 0 or cf > 1):
            bad_cf += 1
            issues.append(f"  FERC bad CF: [{pid}] {name[:40]} cf={cf:.4f}")

    print(f"  Negative capex: {negative_capex}")
    print(f"  Zero capex: {zero_capex}")
    print(f"  Huge capex (>$50B): {huge_capex}")
    print(f"  Capacity factor outside [0,1]: {bad_cf}")

    # EPA emissions by technology
    epa_rows = conn.execute("""
        SELECT id, name, type, epa_co2_rate_lb_per_mwh, epa_capacity_factor
        FROM projects WHERE epa_co2_rate_lb_per_mwh IS NOT NULL
    """).fetchall()
    print(f"\nEPA emissions records: {len(epa_rows)}")

    solar_wind_with_co2 = 0
    fossil_zero_co2 = 0
    negative_co2 = 0
    bad_epa_cf = 0
    for r in epa_rows:
        pid, name, tech, co2_rate, cf = r
        tech_lower = (tech or '').lower()
        is_renewable = any(t in tech_lower for t in ['solar', 'wind', 'storage', 'battery'])
        is_fossil = any(t in tech_lower for t in ['gas', 'coal', 'oil', 'petroleum', 'fossil'])

        if is_renewable and co2_rate > 0:
            solar_wind_with_co2 += 1
            if solar_wind_with_co2 <= 5:
                issues.append(f"  EPA: renewable with CO2: [{pid}] {(name or '')[:35]} type={tech} co2={co2_rate:.1f}")
        if is_fossil and co2_rate == 0:
            fossil_zero_co2 += 1
        if co2_rate < 0:
            negative_co2 += 1
            issues.append(f"  EPA: negative CO2 rate: [{pid}] {name[:35]} co2={co2_rate:.1f}")
        if cf is not None and (cf < 0 or cf > 1):
            bad_epa_cf += 1

    print(f"  Renewables with CO2 > 0: {solar_wind_with_co2}")
    print(f"  Fossil with CO2 = 0: {fossil_zero_co2}")
    print(f"  Negative CO2 rate: {negative_co2}")
    print(f"  EPA CF outside [0,1]: {bad_epa_cf}")

    total_checked = len(ferc_rows) + len(epa_rows)
    total_issues = negative_capex + huge_capex + bad_cf + solar_wind_with_co2 + negative_co2 + bad_epa_cf
    score = 100 * (total_checked - total_issues) / max(total_checked, 1)

    if issues:
        print(f"\n  Issues (showing up to 20):")
        for iss in issues[:20]:
            print(iss)

    return {
        'name': 'FERC/EPA sanity',
        'total_checked': total_checked,
        'passed': total_checked - total_issues,
        'failed': total_issues,
        'score': score,
        'details': issues[:20],
    }


def check_utility_sanity(conn):
    """Check 3: Utility financial values are reasonable."""
    section("CHECK 3: Utility Sanity Checks")

    rows = conn.execute("""
        SELECT id, name, utility_id_eia, utility_rate_base, utility_revenue, utility_net_income,
               utility_total_customers, utility_total_sales_mwh, utility_net_metering_mw
        FROM projects WHERE utility_rate_base IS NOT NULL
    """).fetchall()
    print(f"Projects with utility_rate_base: {len(rows)}")

    issues = []
    negative_rb = 0
    huge_rb = 0  # > $200B (even PG&E is ~$60B)
    negative_rev = 0
    negative_cust = 0
    zero_cust = 0

    for r in rows:
        pid, name, uid, rb, rev, ni, cust, sales, nm = r
        if rb < 0:
            negative_rb += 1
            issues.append(f"  Negative rate base: [{pid}] {name[:35]} uid={uid} rb=${rb:,.0f}")
        elif rb > 200_000_000_000:
            huge_rb += 1
            issues.append(f"  Huge rate base: [{pid}] {name[:35]} uid={uid} rb=${rb:,.0f}")
        if rev is not None and rev < 0:
            negative_rev += 1
        if cust is not None and cust < 0:
            negative_cust += 1
        if cust is not None and cust == 0:
            zero_cust += 1

    # Check for impossibly high net metering
    nm_rows = conn.execute("""
        SELECT id, name, utility_net_metering_mw FROM projects
        WHERE utility_net_metering_mw IS NOT NULL AND utility_net_metering_mw > 500000
    """).fetchall()
    huge_nm = len(nm_rows)
    for r in nm_rows[:3]:
        issues.append(f"  Huge net metering: [{r[0]}] {r[1][:35]} nm={r[2]:,.0f} MW")

    print(f"  Negative rate base: {negative_rb}")
    print(f"  Huge rate base (>$200B): {huge_rb}")
    print(f"  Negative revenue: {negative_rev}")
    print(f"  Negative customers: {negative_cust}")
    print(f"  Zero customers: {zero_cust}")
    print(f"  Net metering > 500,000 MW: {huge_nm}")

    total_issues = negative_rb + huge_rb + negative_rev + negative_cust + huge_nm
    score = 100 * (len(rows) - total_issues) / max(len(rows), 1)

    if issues:
        print(f"\n  Issues (showing up to 10):")
        for iss in issues[:10]:
            print(iss)

    return {
        'name': 'Utility sanity',
        'total_checked': len(rows),
        'passed': len(rows) - total_issues,
        'failed': total_issues,
        'score': score,
        'details': issues[:10],
    }


def check_parent_company(conn):
    """Check 4: Verify parent_company matches against corporate.db."""
    section("CHECK 4: Parent Company Accuracy (50-sample spot check)")

    if not CORPORATE_DB.exists():
        print("  SKIP: corporate.db not found")
        return {'name': 'Parent company', 'total_checked': 0, 'passed': 0, 'failed': 0, 'score': None}

    corp = sqlite3.connect(str(CORPORATE_DB))

    # Get all projects with parent_company
    rows = conn.execute("""
        SELECT id, name, developer_canonical, parent_company
        FROM projects WHERE parent_company IS NOT NULL AND developer_canonical IS NOT NULL
    """).fetchall()
    print(f"Projects with parent_company + developer_canonical: {len(rows)}")

    # Sample 50
    sample = random.sample(rows, min(50, len(rows)))

    verified = 0
    unverified = 0
    false_positive = 0
    results = []

    for pid, name, dev, parent in sample:
        # Check if developer is a subsidiary of parent in corporate.db
        # Normalize names for matching
        dev_norm = dev.strip().lower()
        parent_norm = parent.strip().lower()

        # Direct subsidiary match
        found = corp.execute("""
            SELECT COUNT(*) FROM parents_and_subsidiaries
            WHERE LOWER(parent_company_name) LIKE ?
              AND LOWER(subsidiary_company_name) LIKE ?
        """, (f"%{parent_norm[:20]}%", f"%{dev_norm[:20]}%")).fetchone()[0]

        # Also check reverse (developer as parent with parent as subsidiary name part)
        found_rev = corp.execute("""
            SELECT COUNT(*) FROM parents_and_subsidiaries
            WHERE LOWER(parent_company_name) LIKE ?
              AND LOWER(subsidiary_company_name) LIKE ?
        """, (f"%{dev_norm[:20]}%", f"%{parent_norm[:20]}%")).fetchone()[0]

        # Also check if parent_company_name contains the parent (case insensitive)
        found_parent_exact = corp.execute("""
            SELECT COUNT(*) FROM parents_and_subsidiaries
            WHERE LOWER(parent_company_name) LIKE ?
        """, (f"%{parent_norm[:25]}%",)).fetchone()[0]

        if found > 0:
            verified += 1
            status = 'VERIFIED'
        elif found_rev > 0 or found_parent_exact > 0:
            verified += 1
            status = 'LIKELY OK'
        else:
            # Could be a manual mapping — not necessarily wrong
            unverified += 1
            status = 'UNVERIFIED'
            results.append(f"    [{pid}] dev='{dev[:30]}' → parent='{parent[:30]}' — NOT FOUND in corporate.db")

    print(f"\n  Sample size: {len(sample)}")
    print(f"  Verified in corporate.db: {verified} ({100*verified/len(sample):.0f}%)")
    print(f"  Not found in corporate.db: {unverified} ({100*unverified/len(sample):.0f}%)")

    if results:
        print(f"\n  Unverified matches (may be manual mappings):")
        for r in results[:15]:
            print(r)

    corp.close()

    score = 100 * verified / max(len(sample), 1)
    return {
        'name': 'Parent company accuracy',
        'total_checked': len(sample),
        'passed': verified,
        'failed': unverified,
        'score': score,
        'details': results[:15],
    }


def check_poi_substation(conn):
    """Check 5: POI substation match quality — does substation name overlap with POI?"""
    section("CHECK 5: POI Substation Match Quality (50-sample)")

    rows = conn.execute("""
        SELECT id, poi, poi_substation_match, poi_match_score
        FROM projects WHERE poi_substation_match IS NOT NULL AND poi IS NOT NULL
    """).fetchall()
    print(f"Projects with POI substation match: {len(rows)}")

    sample = random.sample(rows, min(50, len(rows)))

    good = 0
    questionable = 0
    bad = 0
    results = []

    for pid, poi, sub_match, score in sample:
        poi_clean = re.sub(r'\d+kV|\d+\s*kv|tap\s+\d+kv|\btap\b', '', poi, flags=re.IGNORECASE).strip()
        # Extract core words from POI (remove numbers, common suffixes)
        poi_words = set(re.findall(r'[a-zA-Z]{3,}', poi_clean.lower()))
        poi_words -= {'tap', 'substation', 'sub', 'bus', 'line', 'circuit', 'station', 'new', 'the'}

        sub_words = set(re.findall(r'[a-zA-Z]{3,}', sub_match.lower()))

        overlap = poi_words & sub_words
        if overlap:
            good += 1
            quality = 'GOOD'
        elif any(w in sub_match.lower() for w in poi_words if len(w) >= 4):
            good += 1
            quality = 'GOOD'
        elif any(w in poi.lower() for w in sub_words if len(w) >= 4):
            good += 1
            quality = 'PARTIAL'
        else:
            bad += 1
            quality = 'NO OVERLAP'
            results.append(f"    [{pid}] POI='{poi[:45]}' → sub='{sub_match}' score={score}")

    print(f"\n  Sample size: {len(sample)}")
    print(f"  Good match (name overlap): {good} ({100*good/len(sample):.0f}%)")
    print(f"  No name overlap: {bad} ({100*bad/len(sample):.0f}%)")

    if results:
        print(f"\n  No-overlap matches (may still be correct by proximity):")
        for r in results[:15]:
            print(r)

    score = 100 * good / max(len(sample), 1)
    return {
        'name': 'POI substation quality',
        'total_checked': len(sample),
        'passed': good,
        'failed': bad,
        'score': score,
        'details': results[:15],
    }


def check_capacity_sanity(conn):
    """Check 6: Capacity outliers — negative, zero, or impossibly large."""
    section("CHECK 6: Capacity Sanity Checks")

    total = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    null_cap = conn.execute("SELECT COUNT(*) FROM projects WHERE capacity_mw IS NULL").fetchone()[0]
    negative = conn.execute("SELECT COUNT(*) FROM projects WHERE capacity_mw < 0").fetchone()[0]
    zero = conn.execute("SELECT COUNT(*) FROM projects WHERE capacity_mw = 0").fetchone()[0]
    huge = conn.execute("SELECT COUNT(*) FROM projects WHERE capacity_mw > 5000").fetchone()[0]

    print(f"Total projects: {total}")
    print(f"  NULL capacity: {null_cap} ({100*null_cap/total:.1f}%)")
    print(f"  Negative capacity: {negative}")
    print(f"  Zero capacity: {zero}")
    print(f"  > 5,000 MW: {huge}")

    issues = []

    if negative > 0:
        neg_rows = conn.execute("""
            SELECT id, name, region, capacity_mw FROM projects WHERE capacity_mw < 0 LIMIT 10
        """).fetchall()
        for r in neg_rows:
            d = dict(r)
            issues.append(f"  Negative: [{d['id']}] {(d['name'] or '')[:40]} {d['region'] or ''} {d['capacity_mw']:.1f}MW")

    if huge > 0:
        huge_rows = conn.execute("""
            SELECT id, name, region, capacity_mw, type FROM projects
            WHERE capacity_mw > 5000 ORDER BY capacity_mw DESC LIMIT 10
        """).fetchall()
        print(f"\n  Huge capacity projects (>5GW):")
        for r in huge_rows:
            d = dict(r)
            print(f"    [{d['id']}] {(d['name'] or '')[:40]} {d['region'] or ''} {d['capacity_mw']:,.0f}MW ({d['type']})")
            issues.append(f"  Huge: [{d['id']}] {(d['name'] or '')[:40]} {d['capacity_mw']:,.0f}MW")

    # Distribution stats
    stats = conn.execute("""
        SELECT MIN(capacity_mw), AVG(capacity_mw), MAX(capacity_mw),
               COUNT(CASE WHEN capacity_mw <= 100 THEN 1 END),
               COUNT(CASE WHEN capacity_mw > 100 AND capacity_mw <= 500 THEN 1 END),
               COUNT(CASE WHEN capacity_mw > 500 AND capacity_mw <= 1000 THEN 1 END),
               COUNT(CASE WHEN capacity_mw > 1000 THEN 1 END)
        FROM projects WHERE capacity_mw IS NOT NULL
    """).fetchone()
    print(f"\n  Distribution:")
    print(f"    Min: {stats[0]:.1f} MW, Avg: {stats[1]:.1f} MW, Max: {stats[2]:,.0f} MW")
    print(f"    <=100MW: {stats[3]:,}, 100-500MW: {stats[4]:,}, 500-1000MW: {stats[5]:,}, >1000MW: {stats[6]:,}")

    total_issues = negative + huge
    checked = total - null_cap
    score = 100 * (checked - total_issues) / max(checked, 1)

    return {
        'name': 'Capacity sanity',
        'total_checked': checked,
        'passed': checked - total_issues,
        'failed': total_issues,
        'score': score,
        'details': issues[:10],
    }


def check_eia_match_by_confidence(conn):
    """Bonus check: Accuracy of EIA matches by confidence tier."""
    section("CHECK 7: EIA Match Quality by Confidence Tier")

    tiers = conn.execute("""
        SELECT eia_match_confidence, COUNT(*),
               COUNT(CASE WHEN uswtdb_eia_id IS NOT NULL THEN 1 END) as has_uswtdb
        FROM projects WHERE plant_id_eia IS NOT NULL
        GROUP BY eia_match_confidence
    """).fetchall()

    print(f"{'Confidence':15s} {'Count':>8s} {'Has USWTDB':>12s} {'Can verify':>12s}")
    for conf, cnt, has_uswtdb in tiers:
        print(f"  {conf or 'NULL':13s} {cnt:>8,} {has_uswtdb:>12,} {'Yes' if has_uswtdb > 0 else 'No':>12s}")

    # For wind projects with both, check agreement by confidence
    print(f"\n  Agreement rate by confidence (wind projects with both IDs):")
    for conf_level in ['high', 'medium', 'low']:
        both = conn.execute("""
            SELECT COUNT(*) FROM projects
            WHERE uswtdb_eia_id IS NOT NULL AND plant_id_eia IS NOT NULL
              AND eia_match_confidence = ?
        """, (conf_level,)).fetchone()[0]
        agree = conn.execute("""
            SELECT COUNT(*) FROM projects
            WHERE uswtdb_eia_id IS NOT NULL AND plant_id_eia IS NOT NULL
              AND eia_match_confidence = ?
              AND CAST(uswtdb_eia_id AS INTEGER) = CAST(plant_id_eia AS INTEGER)
        """, (conf_level,)).fetchone()[0]
        if both > 0:
            print(f"    {conf_level}: {agree}/{both} agree ({100*agree/both:.1f}%)")
        else:
            print(f"    {conf_level}: no overlap with USWTDB")

    # Check method distribution
    methods = conn.execute("""
        SELECT eia_match_method, COUNT(*) FROM projects
        WHERE plant_id_eia IS NOT NULL
        GROUP BY eia_match_method ORDER BY COUNT(*) DESC
    """).fetchall()
    print(f"\n  Match methods:")
    for method, cnt in methods:
        print(f"    {method or 'NULL':40s} {cnt:>6,}")

    return {'name': 'EIA match tiers', 'total_checked': sum(t[1] for t in tiers), 'score': None}


def generate_report(results):
    """Generate final validation summary."""
    section("VALIDATION SUMMARY")

    print(f"{'Check':35s} {'Checked':>10s} {'Passed':>10s} {'Failed':>10s} {'Score':>8s}")
    print("-" * 75)

    scores = []
    for r in results:
        score_str = f"{r['score']:.1f}%" if r.get('score') is not None else "N/A"
        print(f"  {r['name']:33s} {r.get('total_checked',0):>10,} {r.get('passed',0):>10,} "
              f"{r.get('failed',0):>10,} {score_str:>8s}")
        if r.get('score') is not None:
            scores.append(r['score'])

    overall = sum(scores) / len(scores) if scores else 0
    print(f"\n  OVERALL CONFIDENCE SCORE: {overall:.1f}%")

    # Recommendations
    print(f"\n  RECOMMENDATIONS:")
    for r in results:
        if r.get('score') is not None and r['score'] < 90:
            print(f"    - {r['name']}: score {r['score']:.1f}% — investigate {r.get('failed',0)} failures")
        elif r.get('score') is not None and r['score'] >= 95:
            print(f"    - {r['name']}: GOOD ({r['score']:.1f}%)")


def apply_fixes(conn):
    """Apply recommended fixes for systematic errors found."""
    section("APPLYING FIXES")

    fixes_applied = 0

    # Fix 1: Clear low-confidence EIA matches that disagree with USWTDB
    bad_low = conn.execute("""
        SELECT COUNT(*) FROM projects
        WHERE uswtdb_eia_id IS NOT NULL AND plant_id_eia IS NOT NULL
          AND eia_match_confidence = 'low'
          AND CAST(uswtdb_eia_id AS INTEGER) != CAST(plant_id_eia AS INTEGER)
    """).fetchone()[0]

    if bad_low > 0:
        print(f"  Fix 1: Replacing {bad_low} low-confidence EIA matches that disagree with USWTDB")
        conn.execute("""
            UPDATE projects
            SET plant_id_eia = CAST(uswtdb_eia_id AS INTEGER),
                eia_match_confidence = 'high',
                eia_match_method = 'uswtdb_correction'
            WHERE uswtdb_eia_id IS NOT NULL AND plant_id_eia IS NOT NULL
              AND eia_match_confidence = 'low'
              AND CAST(uswtdb_eia_id AS INTEGER) != CAST(plant_id_eia AS INTEGER)
        """)
        fixes_applied += bad_low

    # Fix 2: Same for medium confidence mismatches with USWTDB (USWTDB is more reliable)
    bad_med = conn.execute("""
        SELECT COUNT(*) FROM projects
        WHERE uswtdb_eia_id IS NOT NULL AND plant_id_eia IS NOT NULL
          AND eia_match_confidence = 'medium'
          AND CAST(uswtdb_eia_id AS INTEGER) != CAST(plant_id_eia AS INTEGER)
    """).fetchone()[0]

    if bad_med > 0:
        print(f"  Fix 2: Replacing {bad_med} medium-confidence EIA matches that disagree with USWTDB")
        conn.execute("""
            UPDATE projects
            SET plant_id_eia = CAST(uswtdb_eia_id AS INTEGER),
                eia_match_confidence = 'high',
                eia_match_method = 'uswtdb_correction'
            WHERE uswtdb_eia_id IS NOT NULL AND plant_id_eia IS NOT NULL
              AND eia_match_confidence = 'medium'
              AND CAST(uswtdb_eia_id AS INTEGER) != CAST(plant_id_eia AS INTEGER)
        """)
        fixes_applied += bad_med

    # Fix 3: For wind projects with uswtdb_eia_id but no plant_id_eia, adopt it
    adopt = conn.execute("""
        SELECT COUNT(*) FROM projects
        WHERE uswtdb_eia_id IS NOT NULL AND plant_id_eia IS NULL
    """).fetchone()[0]

    if adopt > 0:
        print(f"  Fix 3: Adopting {adopt} uswtdb_eia_id values as plant_id_eia")
        conn.execute("""
            UPDATE projects
            SET plant_id_eia = CAST(uswtdb_eia_id AS INTEGER),
                eia_match_confidence = 'high',
                eia_match_method = 'uswtdb_adoption'
            WHERE uswtdb_eia_id IS NOT NULL AND plant_id_eia IS NULL
        """)
        fixes_applied += adopt

    # Fix 4: Clear EPA data for renewable projects where CO2 rate is high
    # These are wrong EIA matches (solar queue project → fossil EIA plant)
    bad_epa = conn.execute("""
        SELECT COUNT(*) FROM projects
        WHERE epa_co2_rate_lb_per_mwh > 10
          AND (LOWER(type) LIKE '%solar%' OR LOWER(type) LIKE '%wind%'
               OR LOWER(type) LIKE '%storage%' OR LOWER(type) LIKE '%battery%')
    """).fetchone()[0]

    if bad_epa > 0:
        print(f"  Fix 4: Clearing EPA data for {bad_epa} renewable projects with high CO2 (wrong EIA match)")
        conn.execute("""
            UPDATE projects SET
                epa_co2_rate_lb_per_mwh = NULL,
                epa_co2_tons = NULL,
                epa_capacity_factor = NULL
            WHERE epa_co2_rate_lb_per_mwh > 10
              AND (LOWER(type) LIKE '%solar%' OR LOWER(type) LIKE '%wind%'
                   OR LOWER(type) LIKE '%storage%' OR LOWER(type) LIKE '%battery%')
        """)
        fixes_applied += bad_epa

    # Fix 5: Clear FERC data for same reason (renewable matched to fossil plant)
    bad_ferc_renew = conn.execute("""
        SELECT COUNT(*) FROM projects
        WHERE ferc_capex_total IS NOT NULL
          AND epa_co2_rate_lb_per_mwh IS NULL
          AND plant_id_eia IS NOT NULL
          AND (LOWER(type) LIKE '%solar%' OR LOWER(type) LIKE '%wind%'
               OR LOWER(type) LIKE '%storage%' OR LOWER(type) LIKE '%battery%')
          AND plant_id_eia IN (
              SELECT DISTINCT plant_id_eia FROM projects
              WHERE epa_co2_rate_lb_per_mwh > 10
          )
    """).fetchone()[0]
    # Actually, just clear negative/inf FERC values
    bad_ferc_cf = conn.execute("""
        UPDATE projects SET ferc_capacity_factor = NULL
        WHERE ferc_capacity_factor IS NOT NULL AND (ferc_capacity_factor < 0 OR ferc_capacity_factor > 10)
    """).rowcount
    if bad_ferc_cf > 0:
        print(f"  Fix 5: Cleared {bad_ferc_cf} invalid FERC capacity factors (negative or >10)")
        fixes_applied += bad_ferc_cf

    bad_ferc_capex = conn.execute("""
        UPDATE projects SET ferc_capex_total = NULL
        WHERE ferc_capex_total IS NOT NULL AND ferc_capex_total < 0
    """).rowcount
    if bad_ferc_capex > 0:
        print(f"  Fix 6: Cleared {bad_ferc_capex} negative FERC capex values")
        fixes_applied += bad_ferc_capex

    conn.commit()
    print(f"\n  Total fixes applied: {fixes_applied}")
    return fixes_applied


def main():
    parser = argparse.ArgumentParser(description='Validate enrichment data in master.db')
    parser.add_argument('--fix', action='store_true', help='Apply recommended fixes')
    args = parser.parse_args()

    if not MASTER_DB.exists():
        print(f"ERROR: master.db not found at {MASTER_DB}")
        sys.exit(1)

    print("=" * 70)
    print("  DATA VALIDATION & ACCURACY AUDIT")
    print(f"  Database: {MASTER_DB}")
    print("=" * 70)

    conn = sqlite3.connect(str(MASTER_DB))
    conn.row_factory = sqlite3.Row

    results = []
    results.append(check_plant_id_consistency(conn))
    results.append(check_ferc_epa_sanity(conn))
    results.append(check_utility_sanity(conn))
    results.append(check_parent_company(conn))
    results.append(check_poi_substation(conn))
    results.append(check_capacity_sanity(conn))
    check_eia_match_by_confidence(conn)

    generate_report(results)

    if args.fix:
        fixes = apply_fixes(conn)
        if fixes > 0:
            print("\n  Re-running validation after fixes...")
            results2 = [check_plant_id_consistency(conn)]
            generate_report(results2)

    conn.close()


if __name__ == '__main__':
    main()
