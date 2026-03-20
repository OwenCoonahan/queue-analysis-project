#!/usr/bin/env python3
"""
ITC Deal Sourcing Engine.

Finds sub-5MW DG/small projects nearing completion that qualify for ITC
tax credits — for investors who want to buy ITC credits from developers.

Queries both master.db (47K queue projects) and dg.db (1.2M DG projects)
to build a unified deal pipeline.

Usage:
    # Search for deals
    python3 itc_deal_finder.py --search --state NY --max-mw 5
    python3 itc_deal_finder.py --search --ec-only --min-rate 0.40
    python3 itc_deal_finder.py --search --state NJ --type Solar+Storage

    # Summary statistics
    python3 itc_deal_finder.py --summary
    python3 itc_deal_finder.py --summary --state CA

    # Detail on a specific project
    python3 itc_deal_finder.py --detail <queue_id>
"""

import sqlite3
import json
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional
from datetime import datetime, date

TOOLS_DIR = Path(__file__).parent
MASTER_DB = TOOLS_DIR / '.data' / 'master.db'
DG_DB = TOOLS_DIR / '.data' / 'dg.db'

# ITC base rate (30% with prevailing wage, projects <1MW exempt from PW&A)
BASE_ITC_RATE = 0.30
EC_BONUS = 0.10
LI_BONUS = 0.10
DC_BONUS = 0.10  # domestic content — not yet tracked

# Technologies eligible for ITC
ITC_ELIGIBLE_TYPES = {
    'Solar', 'Solar+Storage', 'Storage', 'Solar / Battery Storage',
    'Solar / Storage', 'Battery Storage', 'Battery', 'BESS',
    'Hybrid', 'Fuel Cell', 'CHP', 'Geothermal', 'Hydro',
    'Hydroelectric', 'Wind', 'Onshore Wind', 'Offshore Wind',
    'Wind+Storage', 'Nuclear', 'Photovoltaic',
}

# Statuses that represent active/in-progress deals
ACTIVE_STATUSES = {'Active', 'Under Construction', 'IA Executed'}


@dataclass
class Deal:
    """A single ITC deal opportunity."""
    id: int
    queue_id: str
    source_db: str  # 'master' or 'dg'
    name: Optional[str] = None
    developer: Optional[str] = None
    installer: Optional[str] = None
    capacity_mw: float = 0.0
    capacity_kw: float = 0.0
    type: Optional[str] = None
    type_std: Optional[str] = None
    status: Optional[str] = None
    state: Optional[str] = None
    county: Optional[str] = None
    utility: Optional[str] = None
    region: Optional[str] = None
    cod: Optional[str] = None
    queue_date: Optional[str] = None
    source: Optional[str] = None

    # ITC-specific fields
    itc_rate: float = 0.0
    itc_value: float = 0.0
    energy_community: bool = False
    energy_community_type: Optional[str] = None
    low_income: bool = False
    low_income_type: Optional[str] = None
    bonus_count: int = 0

    # Scoring
    deal_score: int = 0
    score_breakdown: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


def _compute_itc_rate(ec: bool, li: bool) -> float:
    """Compute effective ITC rate from bonus flags."""
    rate = BASE_ITC_RATE
    if ec:
        rate += EC_BONUS
    if li:
        rate += LI_BONUS
    return rate


def _estimate_itc_value(capacity_mw: float, type_std: str, itc_rate: float) -> float:
    """Estimate ITC dollar value."""
    # Installed cost per kW by technology
    cost_map = {
        'Solar': 1100, 'Solar+Storage': 1500, 'Storage': 1300,
        'Wind': 1400, 'Hydro': 3500, 'Geothermal': 4500,
        'Nuclear': 8000, 'Hybrid': 1400,
    }
    cost_per_kw = cost_map.get(type_std, 1200)
    return capacity_mw * 1000 * cost_per_kw * itc_rate


def score_deal(deal: Deal) -> Deal:
    """Score a deal opportunity (0-100)."""
    score = 0
    breakdown = {}

    # Base: effective ITC rate (higher = better)
    # 30% base → 30 points, 40% → 40, 50% → 50
    rate_points = int(deal.itc_rate * 100)
    score += rate_points
    breakdown['itc_rate'] = rate_points

    # Bonus: energy community
    if deal.energy_community:
        score += 10
        breakdown['energy_community'] = 10

    # Bonus: low income
    if deal.low_income:
        score += 10
        breakdown['low_income'] = 10

    # Bonus: COD within 12 months
    if deal.cod:
        try:
            for fmt in ['%Y-%m-%d', '%m/%d/%Y']:
                try:
                    cod_date = datetime.strptime(str(deal.cod)[:10], fmt).date()
                    break
                except ValueError:
                    cod_date = None
            if cod_date:
                months_out = (cod_date - date.today()).days / 30
                if 0 <= months_out <= 12:
                    score += 10
                    breakdown['cod_near'] = 10
                elif months_out < 0:
                    # Already past COD — might be operational soon
                    score += 5
                    breakdown['cod_past'] = 5
        except (ValueError, TypeError):
            pass
    else:
        score -= 10
        breakdown['no_cod'] = -10

    # Developer track record (master.db only — we have developer_canonical)
    # For DG, use developer presence as a weak signal
    if deal.developer:
        score += 5
        breakdown['has_developer'] = 5
    else:
        score -= 5
        breakdown['no_developer'] = -5

    # Capacity bonus — larger sub-5MW projects are more efficient deals
    if deal.capacity_mw >= 1.0:
        score += 5
        breakdown['capacity_1mw_plus'] = 5
    elif deal.capacity_mw >= 0.1:
        score += 2
        breakdown['capacity_100kw_plus'] = 2

    # Clamp to 0-100
    score = max(0, min(100, score))
    deal.deal_score = score
    deal.score_breakdown = breakdown
    return deal


def _query_db(db_path: Path, source_db: str, filters: dict) -> list[Deal]:
    """Query a database for ITC-eligible active projects."""
    if not db_path.exists():
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Build WHERE clause
    conditions = []
    params = []

    # Status filter
    statuses = filters.get('statuses', ACTIVE_STATUSES)
    if source_db == 'dg':
        # dg.db uses 'status' not 'status_std'
        placeholders = ','.join(['?' for _ in statuses])
        conditions.append(f"status IN ({placeholders})")
        params.extend(statuses)
    else:
        placeholders = ','.join(['?' for _ in statuses])
        conditions.append(f"(status_std IN ({placeholders}) OR status IN ({placeholders}))")
        params.extend(statuses)
        params.extend(statuses)

    # ITC eligibility — must have 'itc' or 'both'
    conditions.append("tax_credit_type IN ('itc', 'both')")

    # Technology filter
    if filters.get('types'):
        type_placeholders = ','.join(['?' for _ in filters['types']])
        if source_db == 'dg':
            conditions.append(f"type IN ({type_placeholders})")
        else:
            conditions.append(f"(type_std IN ({type_placeholders}) OR type IN ({type_placeholders}))")
            params.extend(filters['types'])
        params.extend(filters['types'])

    # Capacity filter
    max_mw = filters.get('max_mw', 5.0)
    min_mw = filters.get('min_mw', 0.0)
    conditions.append("capacity_mw <= ?")
    params.append(max_mw)
    if min_mw > 0:
        conditions.append("capacity_mw >= ?")
        params.append(min_mw)

    # State filter
    if filters.get('states'):
        state_placeholders = ','.join(['?' for _ in filters['states']])
        conditions.append(f"state IN ({state_placeholders})")
        params.extend(filters['states'])

    # Energy community only
    if filters.get('ec_only'):
        conditions.append("energy_community_eligible = 1")

    # Low income only
    if filters.get('li_only'):
        conditions.append("low_income_eligible = 1")

    # Min ITC rate filter (computed from bonuses)
    # We'll filter post-query since ITC rate is computed from EC + LI flags

    where = ' AND '.join(conditions)

    # Select columns available in both DBs
    if source_db == 'dg':
        query = f"""
            SELECT id, queue_id, name, developer, capacity_mw, capacity_kw,
                   type, type_std, status, state, county, utility, region,
                   cod, queue_date, source, installer,
                   energy_community_eligible, energy_community_type,
                   low_income_eligible, low_income_type,
                   tax_credit_type, effective_credit_rate, estimated_credit_value
            FROM projects
            WHERE {where}
            ORDER BY capacity_mw DESC
        """
    else:
        query = f"""
            SELECT id, queue_id, name,
                   COALESCE(developer_canonical, developer) as developer,
                   capacity_mw,
                   capacity_mw * 1000 as capacity_kw,
                   type, type_std, status_std as status, state, county,
                   '' as utility, region,
                   COALESCE(cod_std, cod) as cod,
                   COALESCE(queue_date_std, queue_date) as queue_date,
                   source, '' as installer,
                   energy_community_eligible, energy_community_type,
                   low_income_eligible, low_income_type,
                   tax_credit_type, effective_credit_rate, estimated_credit_value
            FROM projects
            WHERE {where}
            ORDER BY capacity_mw DESC
        """

    cursor = conn.execute(query, params)
    rows = cursor.fetchall()

    deals = []
    min_rate = filters.get('min_rate', 0.0)

    for row in rows:
        ec = bool(row['energy_community_eligible'])
        li = bool(row['low_income_eligible'])
        itc_rate = _compute_itc_rate(ec, li)

        if itc_rate < min_rate:
            continue

        type_std = row['type_std'] or row['type'] or ''
        cap_mw = row['capacity_mw'] or 0

        deal = Deal(
            id=row['id'],
            queue_id=row['queue_id'],
            source_db=source_db,
            name=row['name'],
            developer=row['developer'],
            installer=row['installer'] if source_db == 'dg' else None,
            capacity_mw=cap_mw,
            capacity_kw=row['capacity_kw'] or cap_mw * 1000,
            type=row['type'],
            type_std=type_std,
            status=row['status'],
            state=row['state'],
            county=row['county'],
            utility=row['utility'],
            region=row['region'],
            cod=row['cod'],
            queue_date=row['queue_date'],
            source=row['source'],
            itc_rate=itc_rate,
            itc_value=_estimate_itc_value(cap_mw, type_std, itc_rate),
            energy_community=ec,
            energy_community_type=row['energy_community_type'],
            low_income=li,
            low_income_type=row['low_income_type'],
            bonus_count=(1 if ec else 0) + (1 if li else 0),
        )
        deals.append(score_deal(deal))

    conn.close()
    return deals


def find_itc_deals(
    min_mw: float = 0.0,
    max_mw: float = 5.0,
    states: list[str] = None,
    types: list[str] = None,
    min_rate: float = 0.0,
    ec_only: bool = False,
    li_only: bool = False,
    statuses: set[str] = None,
    limit: int = 100,
    include_master: bool = True,
    include_dg: bool = True,
) -> list[Deal]:
    """
    Find ITC-eligible deal opportunities across both databases.

    Args:
        min_mw: Minimum capacity in MW
        max_mw: Maximum capacity in MW (default 5)
        states: Filter by state abbreviations (e.g., ['NY', 'NJ'])
        types: Filter by technology type (e.g., ['Solar', 'Solar+Storage'])
        min_rate: Minimum effective ITC rate (e.g., 0.40 for 40%)
        ec_only: Only energy community projects
        li_only: Only low-income community projects
        statuses: Override default active statuses
        limit: Max results to return
        include_master: Include master.db results
        include_dg: Include dg.db results

    Returns:
        List of Deal objects sorted by deal_score descending
    """
    filters = {
        'min_mw': min_mw,
        'max_mw': max_mw,
        'states': [s.upper() for s in states] if states else None,
        'types': types,
        'min_rate': min_rate,
        'ec_only': ec_only,
        'li_only': li_only,
        'statuses': statuses or ACTIVE_STATUSES,
    }

    deals = []
    if include_master:
        deals.extend(_query_db(MASTER_DB, 'master', filters))
    if include_dg:
        deals.extend(_query_db(DG_DB, 'dg', filters))

    # Sort by score descending, then by ITC value descending
    deals.sort(key=lambda d: (d.deal_score, d.itc_value), reverse=True)

    return deals[:limit]


def get_deal_summary(
    states: list[str] = None,
    types: list[str] = None,
    ec_only: bool = False,
    li_only: bool = False,
    max_mw: float = 5.0,
) -> dict:
    """
    Get aggregate statistics for the ITC deal pipeline.

    Returns dict with total counts, MW, credit value, breakdowns by state/type/rate.
    """
    deals = find_itc_deals(
        states=states, types=types, ec_only=ec_only, li_only=li_only,
        max_mw=max_mw, limit=999999,
    )

    if not deals:
        return {'total_deals': 0, 'total_mw': 0, 'total_itc_value': 0}

    total_mw = sum(d.capacity_mw for d in deals)
    total_value = sum(d.itc_value for d in deals)

    # By state
    by_state = {}
    for d in deals:
        st = d.state or 'Unknown'
        if st not in by_state:
            by_state[st] = {'count': 0, 'mw': 0.0, 'itc_value': 0.0}
        by_state[st]['count'] += 1
        by_state[st]['mw'] += d.capacity_mw
        by_state[st]['itc_value'] += d.itc_value

    # By type
    by_type = {}
    for d in deals:
        t = d.type_std or d.type or 'Unknown'
        if t not in by_type:
            by_type[t] = {'count': 0, 'mw': 0.0, 'itc_value': 0.0}
        by_type[t]['count'] += 1
        by_type[t]['mw'] += d.capacity_mw
        by_type[t]['itc_value'] += d.itc_value

    # By ITC rate tier
    by_rate = {'30%': 0, '40%': 0, '50%': 0}
    for d in deals:
        if d.itc_rate >= 0.50:
            by_rate['50%'] += 1
        elif d.itc_rate >= 0.40:
            by_rate['40%'] += 1
        else:
            by_rate['30%'] += 1

    # Score distribution
    avg_score = sum(d.deal_score for d in deals) / len(deals)
    top_deals = [d for d in deals if d.deal_score >= 50]

    return {
        'total_deals': len(deals),
        'total_mw': round(total_mw, 1),
        'total_itc_value': round(total_value),
        'avg_deal_score': round(avg_score, 1),
        'top_deals_count': len(top_deals),
        'by_state': dict(sorted(by_state.items(), key=lambda x: x[1]['count'], reverse=True)),
        'by_type': dict(sorted(by_type.items(), key=lambda x: x[1]['count'], reverse=True)),
        'by_rate_tier': by_rate,
        'ec_eligible': sum(1 for d in deals if d.energy_community),
        'li_eligible': sum(1 for d in deals if d.low_income),
        'both_bonuses': sum(1 for d in deals if d.energy_community and d.low_income),
    }


def get_deal_detail(queue_id: str) -> Optional[Deal]:
    """Get detailed deal profile for a specific project by queue_id."""
    # Search both databases
    for db_path, source in [(MASTER_DB, 'master'), (DG_DB, 'dg')]:
        if not db_path.exists():
            continue
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        if source == 'dg':
            row = conn.execute("""
                SELECT id, queue_id, name, developer, capacity_mw, capacity_kw,
                       type, type_std, status, state, county, utility, region,
                       cod, queue_date, source, installer,
                       energy_community_eligible, energy_community_type,
                       low_income_eligible, low_income_type,
                       tax_credit_type, effective_credit_rate, estimated_credit_value
                FROM projects WHERE queue_id = ?
            """, (queue_id,)).fetchone()
        else:
            row = conn.execute("""
                SELECT id, queue_id, name,
                       COALESCE(developer_canonical, developer) as developer,
                       capacity_mw, capacity_mw * 1000 as capacity_kw,
                       type, type_std, status_std as status, state, county,
                       '' as utility, region,
                       COALESCE(cod_std, cod) as cod,
                       COALESCE(queue_date_std, queue_date) as queue_date,
                       source, '' as installer,
                       energy_community_eligible, energy_community_type,
                       low_income_eligible, low_income_type,
                       tax_credit_type, effective_credit_rate, estimated_credit_value
                FROM projects WHERE queue_id = ?
            """, (queue_id,)).fetchone()

        conn.close()
        if row:
            ec = bool(row['energy_community_eligible'])
            li = bool(row['low_income_eligible'])
            itc_rate = _compute_itc_rate(ec, li)
            type_std = row['type_std'] or row['type'] or ''
            cap_mw = row['capacity_mw'] or 0

            deal = Deal(
                id=row['id'],
                queue_id=row['queue_id'],
                source_db=source,
                name=row['name'],
                developer=row['developer'],
                installer=row['installer'] if source == 'dg' else None,
                capacity_mw=cap_mw,
                capacity_kw=row['capacity_kw'] or cap_mw * 1000,
                type=row['type'],
                type_std=type_std,
                status=row['status'],
                state=row['state'],
                county=row['county'],
                utility=row['utility'],
                region=row['region'],
                cod=row['cod'],
                queue_date=row['queue_date'],
                source=row['source'],
                itc_rate=itc_rate,
                itc_value=_estimate_itc_value(cap_mw, type_std, itc_rate),
                energy_community=ec,
                energy_community_type=row['energy_community_type'],
                low_income=li,
                low_income_type=row['low_income_type'],
                bonus_count=(1 if ec else 0) + (1 if li else 0),
            )
            return score_deal(deal)

    return None


# =============================================================================
# API-ready serialization
# =============================================================================

def deals_to_json(deals: list[Deal]) -> list[dict]:
    """Convert deals to JSON-serializable dicts for API responses."""
    return [d.to_dict() for d in deals]


# =============================================================================
# CLI
# =============================================================================

def _print_deals_table(deals: list[Deal], limit: int = 50):
    """Pretty-print deals as a table."""
    if not deals:
        print("No deals found matching criteria.")
        return

    print(f"\n{'Score':>5} {'ITC%':>5} {'MW':>6} {'Type':<14} {'State':<5} {'County':<18} {'Developer/Installer':<30} {'ITC Value':>12}")
    print("-" * 105)

    for d in deals[:limit]:
        dev = d.developer or d.installer or '-'
        if len(dev) > 29:
            dev = dev[:27] + '..'
        county = (d.county or '-')[:17]
        type_str = (d.type_std or d.type or '-')[:13]
        bonuses = ''
        if d.energy_community:
            bonuses += ' EC'
        if d.low_income:
            bonuses += ' LI'

        print(f"{d.deal_score:>5} {d.itc_rate:>4.0%}{bonuses:<4} {d.capacity_mw:>5.2f} {type_str:<14} {d.state or '-':<5} {county:<18} {dev:<30} ${d.itc_value:>10,.0f}")

    if len(deals) > limit:
        print(f"\n  ... showing {limit} of {len(deals)} deals")

    # Summary line
    total_mw = sum(d.capacity_mw for d in deals)
    total_val = sum(d.itc_value for d in deals)
    avg_score = sum(d.deal_score for d in deals) / len(deals)
    print(f"\n  {len(deals)} deals | {total_mw:,.1f} MW | ${total_val:,.0f} total ITC value | avg score {avg_score:.0f}")


def _print_summary(summary: dict):
    """Pretty-print deal summary."""
    print(f"\n{'=' * 60}")
    print("ITC DEAL PIPELINE SUMMARY")
    print(f"{'=' * 60}")
    print(f"\n  Total deals: {summary['total_deals']:,}")
    print(f"  Total capacity: {summary['total_mw']:,.1f} MW")
    print(f"  Total ITC value: ${summary['total_itc_value']:,.0f}")
    print(f"  Avg deal score: {summary['avg_deal_score']:.0f}/100")
    print(f"  Top deals (score>=50): {summary['top_deals_count']:,}")

    print(f"\n  Bonus eligibility:")
    print(f"    Energy community: {summary['ec_eligible']:,}")
    print(f"    Low income: {summary['li_eligible']:,}")
    print(f"    Both bonuses: {summary['both_bonuses']:,}")

    print(f"\n  By ITC rate tier:")
    for tier, cnt in summary['by_rate_tier'].items():
        print(f"    {tier}: {cnt:,}")

    print(f"\n  By State (top 10):")
    print(f"  {'State':<6} {'Deals':>8} {'MW':>10} {'ITC Value':>14}")
    print(f"  {'-'*42}")
    for st, data in list(summary['by_state'].items())[:10]:
        print(f"  {st:<6} {data['count']:>8,} {data['mw']:>9,.1f} ${data['itc_value']:>12,.0f}")

    print(f"\n  By Technology:")
    print(f"  {'Type':<16} {'Deals':>8} {'MW':>10} {'ITC Value':>14}")
    print(f"  {'-'*52}")
    for t, data in summary['by_type'].items():
        print(f"  {t:<16} {data['count']:>8,} {data['mw']:>9,.1f} ${data['itc_value']:>12,.0f}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='ITC Deal Sourcing Engine')
    parser.add_argument('--search', action='store_true', help='Search for ITC deals')
    parser.add_argument('--summary', action='store_true', help='Show deal pipeline summary')
    parser.add_argument('--detail', type=str, help='Get detail for a specific queue_id')

    # Filters
    parser.add_argument('--state', type=str, help='Filter by state (comma-separated: NY,NJ)')
    parser.add_argument('--type', type=str, help='Filter by technology (comma-separated: Solar,Storage)')
    parser.add_argument('--min-mw', type=float, default=0.0, help='Minimum MW')
    parser.add_argument('--max-mw', type=float, default=5.0, help='Maximum MW')
    parser.add_argument('--min-rate', type=float, default=0.0, help='Minimum ITC rate (e.g., 0.40)')
    parser.add_argument('--ec-only', action='store_true', help='Energy community only')
    parser.add_argument('--li-only', action='store_true', help='Low-income community only')
    parser.add_argument('--limit', type=int, default=50, help='Max results')
    parser.add_argument('--json', action='store_true', help='Output as JSON')

    args = parser.parse_args()

    states = [s.strip().upper() for s in args.state.split(',')] if args.state else None
    types = [t.strip() for t in args.type.split(',')] if args.type else None

    if args.detail:
        deal = get_deal_detail(args.detail)
        if deal:
            if args.json:
                print(json.dumps(deal.to_dict(), indent=2, default=str))
            else:
                print(f"\n{'=' * 60}")
                print(f"DEAL DETAIL: {deal.queue_id}")
                print(f"{'=' * 60}")
                print(f"  Score: {deal.deal_score}/100")
                print(f"  Source: {deal.source_db}")
                print(f"  Developer: {deal.developer or '-'}")
                print(f"  Installer: {deal.installer or '-'}")
                print(f"  Type: {deal.type_std or deal.type}")
                print(f"  Capacity: {deal.capacity_mw:.3f} MW ({deal.capacity_kw:.1f} kW)")
                print(f"  Status: {deal.status}")
                print(f"  Location: {deal.county}, {deal.state}")
                print(f"  Utility: {deal.utility or '-'}")
                print(f"  COD: {deal.cod or '-'}")
                print(f"  Queue Date: {deal.queue_date or '-'}")
                print(f"\n  ITC Rate: {deal.itc_rate:.0%}")
                print(f"  ITC Value: ${deal.itc_value:,.0f}")
                print(f"  Energy Community: {'YES' if deal.energy_community else 'No'} {deal.energy_community_type or ''}")
                print(f"  Low Income: {'YES' if deal.low_income else 'No'} {deal.low_income_type or ''}")
                print(f"\n  Score Breakdown:")
                for k, v in deal.score_breakdown.items():
                    print(f"    {k}: {'+' if v > 0 else ''}{v}")
        else:
            print(f"No project found with queue_id: {args.detail}")

    elif args.summary:
        summary = get_deal_summary(
            states=states, types=types,
            ec_only=args.ec_only, li_only=args.li_only,
            max_mw=args.max_mw,
        )
        if args.json:
            print(json.dumps(summary, indent=2, default=str))
        else:
            _print_summary(summary)

    elif args.search:
        deals = find_itc_deals(
            min_mw=args.min_mw, max_mw=args.max_mw,
            states=states, types=types,
            min_rate=args.min_rate,
            ec_only=args.ec_only, li_only=args.li_only,
            limit=args.limit,
        )
        if args.json:
            print(json.dumps(deals_to_json(deals), indent=2, default=str))
        else:
            _print_deals_table(deals, limit=args.limit)

    else:
        parser.print_help()


if __name__ == '__main__':
    main()
