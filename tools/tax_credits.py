#!/usr/bin/env python3
"""
Tax Credit Eligibility Engine.

Calculates ITC/PTC eligibility, bonus adders, and estimated credit values
for every project in the interconnection queue database.

Covers:
- Base ITC (Section 48/48E) and PTC (Section 45/45Y)
- Energy Community bonus (+10%)
- Domestic Content bonus (+10%)
- Low-Income Community bonus (+10-20%)
- Phase-down schedules based on technology and placed-in-service date

Data sources:
- IRS Notice 2023-29, 2023-38, 2023-47, 2024-30 (energy communities)
- IRS Notice 2023-38 (domestic content)
- IRS Notice 2023-17 (low-income)
- IRC Sections 45, 45Y, 48, 48E
- DOE NETL Energy Community data layers
- CEJST (Climate & Economic Justice Screening Tool) for low-income

Usage:
    from tax_credits import TaxCreditEngine

    engine = TaxCreditEngine()
    result = engine.calculate(
        technology='Solar',
        capacity_mw=200,
        state='TX',
        county='Pecos',
        cod_year=2027,
    )
    print(result)

    # Batch enrich the database
    python3 tax_credits.py --enrich
    python3 tax_credits.py --stats
    python3 tax_credits.py --check Solar 200 TX Pecos 2027
"""

import sqlite3
import json
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional
from datetime import datetime

# Paths — consistent with existing Queue Analysis conventions
TOOLS_DIR = Path(__file__).parent
DATA_DIR = TOOLS_DIR / '.data'
CACHE_DIR = TOOLS_DIR / '.cache'
DB_PATH = DATA_DIR / 'master.db'


# =============================================================================
# ITC/PTC Rate Tables
# =============================================================================

# Technology → eligible credit type(s)
# Based on IRC 45, 45Y, 48, 48E as amended by IRA
TECH_CREDIT_MAP = {
    # Technology: (can_elect_itc, can_elect_ptc, default_credit)
    'Solar': (True, True, 'itc'),
    'Solar / Battery Storage': (True, True, 'itc'),
    'Solar / Storage': (True, True, 'itc'),
    'Photovoltaic': (True, True, 'itc'),
    'Wind': (True, True, 'ptc'),
    'Onshore Wind': (True, True, 'ptc'),
    'Offshore Wind': (True, True, 'ptc'),
    'Wind / Battery Storage': (True, True, 'ptc'),
    'Storage': (True, False, 'itc'),
    'Battery Storage': (True, False, 'itc'),
    'Battery': (True, False, 'itc'),
    'BESS': (True, False, 'itc'),
    'Hybrid': (True, True, 'itc'),
    'Solar+Storage': (True, True, 'itc'),
    'Wind+Storage': (True, True, 'ptc'),
    'Geothermal': (True, True, 'ptc'),
    'Hydroelectric': (True, True, 'ptc'),
    'Hydro': (True, True, 'ptc'),
    'Biomass': (False, True, 'ptc'),
    'Landfill Gas': (False, True, 'ptc'),
    'Nuclear': (True, True, 'ptc'),
    'Fuel Cell': (True, False, 'itc'),
    'CHP': (True, False, 'itc'),
    'Microturbine': (True, False, 'itc'),
    'Combined Heat and Power': (True, False, 'itc'),
    'Waste Heat': (True, True, 'ptc'),
    'Marine': (True, True, 'ptc'),
    'Tidal': (True, True, 'ptc'),
}

# Base credit rates (post-IRA, assuming prevailing wage & apprenticeship met)
# Projects < 1 MW AC are exempt from PW&A requirements
BASE_ITC_RATE = 0.30          # 30% ITC (6% without PW&A)
BASE_ITC_RATE_NO_PWA = 0.06   # 6% without prevailing wage & apprenticeship
BASE_PTC_RATE_PER_KWH = 0.0275  # $/kWh (2024, inflation-adjusted annually)
BASE_PTC_RATE_NO_PWA = 0.0055   # $/kWh without PW&A

# Bonus adders (percentage points added to ITC, or multiplied for PTC)
ENERGY_COMMUNITY_BONUS = 0.10   # +10% ITC or +10% PTC increase
DOMESTIC_CONTENT_BONUS = 0.10   # +10% ITC or +10% PTC increase
LOW_INCOME_BONUS_TIER1 = 0.10   # +10% for located in low-income community
LOW_INCOME_BONUS_TIER2 = 0.20   # +20% for qualified low-income residential or economic benefit

# ITC phase-down schedule for Section 48 (pre-tech-neutral)
# Section 48E (tech-neutral) begins for facilities placed in service after 12/31/2024
# Phase-down triggers when US GHG emissions fall 75% below 2022 levels, or 2032, whichever is later
# For now, assume full rates through at least 2032
ITC_PHASEDOWN = {
    # year: rate_multiplier (1.0 = full rate)
    2024: 1.0, 2025: 1.0, 2026: 1.0, 2027: 1.0,
    2028: 1.0, 2029: 1.0, 2030: 1.0, 2031: 1.0, 2032: 1.0,
    2033: 1.0, 2034: 1.0,  # Assumed stable until GHG trigger
}

# Capacity factors by technology for PTC revenue estimation
CAPACITY_FACTORS = {
    'Solar': 0.25,
    'Photovoltaic': 0.25,
    'Wind': 0.35,
    'Onshore Wind': 0.35,
    'Offshore Wind': 0.45,
    'Geothermal': 0.90,
    'Hydroelectric': 0.40,
    'Hydro': 0.40,
    'Biomass': 0.80,
    'Landfill Gas': 0.85,
    'Nuclear': 0.92,
}

# Estimated installed cost per kW by technology (2024 dollars)
# Used for ITC value estimation
INSTALLED_COST_PER_KW = {
    'Solar': 1100,
    'Photovoltaic': 1100,
    'Solar / Battery Storage': 1500,
    'Solar / Storage': 1500,
    'Solar+Storage': 1500,
    'Wind': 1400,
    'Onshore Wind': 1400,
    'Offshore Wind': 4500,
    'Wind / Battery Storage': 1800,
    'Wind+Storage': 1800,
    'Storage': 1300,
    'Battery Storage': 1300,
    'Battery': 1300,
    'BESS': 1300,
    'Geothermal': 4500,
    'Hydroelectric': 3500,
    'Hydro': 3500,
    'Nuclear': 8000,
    'Fuel Cell': 5000,
}


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class TaxCreditResult:
    """Complete tax credit eligibility result for a project."""
    # Project identifiers
    technology: str
    capacity_mw: float
    state: str
    county: str
    cod_year: Optional[int]

    # Credit type
    can_elect_itc: bool = False
    can_elect_ptc: bool = False
    recommended_credit: str = 'none'  # 'itc', 'ptc', or 'none'

    # Base rates
    base_itc_rate: float = 0.0
    base_ptc_rate_per_kwh: float = 0.0
    pwa_assumed: bool = True  # Prevailing wage & apprenticeship

    # Bonus adders
    energy_community_eligible: bool = False
    energy_community_bonus: float = 0.0
    energy_community_type: Optional[str] = None

    domestic_content_eligible: Optional[bool] = None  # None = unknown
    domestic_content_bonus: float = 0.0

    low_income_eligible: bool = False
    low_income_bonus: float = 0.0
    low_income_tier: Optional[str] = None

    # Combined rates
    effective_itc_rate: float = 0.0      # Base + all bonuses
    effective_ptc_rate: float = 0.0      # Base + all bonuses ($/kWh)
    max_itc_rate: float = 0.0           # If all bonuses achieved

    # Estimated values
    estimated_itc_value: float = 0.0     # $ (one-time)
    estimated_annual_ptc_value: float = 0.0  # $/year
    estimated_10yr_ptc_value: float = 0.0    # $ (10-year total)
    recommended_value: float = 0.0       # $ value of recommended credit

    # Phase-down
    phasedown_applies: bool = False
    phasedown_rate: float = 1.0

    # Metadata
    notes: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    def summary(self) -> str:
        """One-line summary."""
        bonuses = []
        if self.energy_community_eligible:
            bonuses.append('EC')
        if self.domestic_content_eligible:
            bonuses.append('DC')
        if self.low_income_eligible:
            bonuses.append('LI')
        bonus_str = '+'.join(bonuses) if bonuses else 'no bonus'

        if self.recommended_credit == 'itc':
            return f"ITC {self.effective_itc_rate:.0%} ({bonus_str}) = ${self.estimated_itc_value:,.0f}"
        elif self.recommended_credit == 'ptc':
            return f"PTC ${self.effective_ptc_rate:.4f}/kWh ({bonus_str}) = ${self.estimated_annual_ptc_value:,.0f}/yr"
        else:
            return f"Not eligible for ITC or PTC"


# =============================================================================
# Engine
# =============================================================================

class TaxCreditEngine:
    """Calculate tax credit eligibility and values for energy projects."""

    def __init__(self):
        self._energy_community_checker = None

    def _get_energy_community_checker(self):
        """Lazy-load the energy community checker."""
        if self._energy_community_checker is None:
            try:
                from energy_community import EnergyCommunityChecker
                self._energy_community_checker = EnergyCommunityChecker()
                self._energy_community_checker.load_data()
            except Exception as e:
                print(f"Warning: Could not load energy community data: {e}")
                self._energy_community_checker = None
        return self._energy_community_checker

    def _get_low_income_checker(self):
        """Lazy-load the low-income community checker."""
        if not hasattr(self, '_low_income_checker') or self._low_income_checker is None:
            try:
                from low_income_community import LowIncomeChecker
                self._low_income_checker = LowIncomeChecker()
                self._low_income_checker.load_data()
            except Exception as e:
                print(f"Warning: Could not load low-income community data: {e}")
                self._low_income_checker = None
        return self._low_income_checker

    def _normalize_tech(self, technology: str) -> str:
        """Normalize technology string to match our lookup tables."""
        if not technology:
            return ''
        tech = technology.strip()

        # Try exact match first
        if tech in TECH_CREDIT_MAP:
            return tech

        # Case-insensitive match
        tech_lower = tech.lower()
        for key in TECH_CREDIT_MAP:
            if key.lower() == tech_lower:
                return key

        # Partial match
        if 'solar' in tech_lower and 'storage' in tech_lower:
            return 'Solar+Storage'
        if 'solar' in tech_lower or 'pv' in tech_lower or 'photovoltaic' in tech_lower:
            return 'Solar'
        if 'wind' in tech_lower and 'offshore' in tech_lower:
            return 'Offshore Wind'
        if 'wind' in tech_lower and 'storage' in tech_lower:
            return 'Wind+Storage'
        if 'wind' in tech_lower:
            return 'Wind'
        if 'batter' in tech_lower or 'storage' in tech_lower or 'bess' in tech_lower:
            return 'Storage'
        if 'hydro' in tech_lower:
            return 'Hydroelectric'
        if 'geotherm' in tech_lower:
            return 'Geothermal'
        if 'biomass' in tech_lower:
            return 'Biomass'
        if 'nuclear' in tech_lower:
            return 'Nuclear'
        if 'landfill' in tech_lower:
            return 'Landfill Gas'
        if 'fuel cell' in tech_lower:
            return 'Fuel Cell'
        if 'hybrid' in tech_lower:
            return 'Hybrid'
        if 'natural gas' in tech_lower or 'gas' in tech_lower:
            return ''  # Fossil not eligible
        if 'coal' in tech_lower:
            return ''  # Not eligible

        return tech  # Return as-is, will get 'none' credit

    def calculate(
        self,
        technology: str,
        capacity_mw: float = 0,
        state: str = '',
        county: str = '',
        cod_year: int = None,
        domestic_content: bool = None,
        prevailing_wage: bool = True,
    ) -> TaxCreditResult:
        """
        Calculate full tax credit eligibility for a project.

        Args:
            technology: Project technology type
            capacity_mw: Nameplate capacity in MW
            state: State abbreviation (e.g., 'TX')
            county: County name
            cod_year: Expected commercial operation date year
            domestic_content: Whether project meets domestic content requirements
                              (None = unknown, True = meets, False = doesn't meet)
            prevailing_wage: Whether project meets prevailing wage & apprenticeship
        """
        norm_tech = self._normalize_tech(technology)
        if cod_year is None:
            cod_year = datetime.now().year + 2  # Default assumption

        result = TaxCreditResult(
            technology=norm_tech or technology,
            capacity_mw=capacity_mw or 0,
            state=state,
            county=county,
            cod_year=cod_year,
        )

        # Check if technology is eligible
        credit_info = TECH_CREDIT_MAP.get(norm_tech)
        if not credit_info:
            result.notes.append(f"Technology '{technology}' not eligible for ITC or PTC")
            return result

        can_itc, can_ptc, default_credit = credit_info
        result.can_elect_itc = can_itc
        result.can_elect_ptc = can_ptc
        result.recommended_credit = default_credit

        # Prevailing wage & apprenticeship
        # Projects < 1 MW are exempt
        result.pwa_assumed = prevailing_wage or (capacity_mw and capacity_mw < 1)

        # Base rates
        if can_itc:
            result.base_itc_rate = BASE_ITC_RATE if result.pwa_assumed else BASE_ITC_RATE_NO_PWA
        if can_ptc:
            result.base_ptc_rate_per_kwh = BASE_PTC_RATE_PER_KWH if result.pwa_assumed else BASE_PTC_RATE_NO_PWA

        # Phase-down
        phasedown = ITC_PHASEDOWN.get(cod_year, 1.0)
        if phasedown < 1.0:
            result.phasedown_applies = True
            result.phasedown_rate = phasedown
            result.base_itc_rate *= phasedown
            result.base_ptc_rate_per_kwh *= phasedown
            result.notes.append(f"Phase-down: {phasedown:.0%} of base rate for COD year {cod_year}")

        # --- Bonus: Energy Community ---
        if state and county:
            checker = self._get_energy_community_checker()
            if checker:
                ec_result = checker.check_location(state, county)
                if ec_result.is_energy_community:
                    result.energy_community_eligible = True
                    result.energy_community_bonus = ENERGY_COMMUNITY_BONUS
                    ec_types = []
                    if ec_result.coal_closure:
                        ec_types.append('coal_closure')
                    if ec_result.ffe_qualified:
                        ec_types.append('ffe')
                    result.energy_community_type = ','.join(ec_types) if ec_types else 'qualified'

        # --- Bonus: Domestic Content ---
        if domestic_content is True:
            result.domestic_content_eligible = True
            result.domestic_content_bonus = DOMESTIC_CONTENT_BONUS
        elif domestic_content is False:
            result.domestic_content_eligible = False
        else:
            result.domestic_content_eligible = None
            result.notes.append("Domestic content status unknown — bonus not applied")

        # --- Bonus: Low-Income Community ---
        if state and county:
            li_checker = self._get_low_income_checker()
            if li_checker and li_checker.loaded:
                li_result = li_checker.check_location(state, county)
                if li_result.is_low_income:
                    result.low_income_eligible = True
                    result.low_income_bonus = li_result.bonus_rate  # 0.10 for Category 1
                    li_types = []
                    if li_result.nmtc_qualified:
                        li_types.append('nmtc')
                    if li_result.persistent_poverty:
                        li_types.append('ppc')
                    if li_result.cejst_energy:
                        li_types.append('cejst_energy')
                    result.low_income_tier = ','.join(li_types) if li_types else 'category_1'

        # --- Calculate effective rates ---
        total_itc_bonus = (result.energy_community_bonus +
                          result.domestic_content_bonus +
                          result.low_income_bonus)

        if can_itc:
            result.effective_itc_rate = result.base_itc_rate + total_itc_bonus
            result.max_itc_rate = result.base_itc_rate + ENERGY_COMMUNITY_BONUS + DOMESTIC_CONTENT_BONUS + LOW_INCOME_BONUS_TIER2

        if can_ptc:
            # PTC bonuses are multiplicative (10% increase to PTC rate)
            ptc_multiplier = 1.0
            if result.energy_community_eligible:
                ptc_multiplier += 0.10
            if result.domestic_content_eligible:
                ptc_multiplier += 0.10
            result.effective_ptc_rate = result.base_ptc_rate_per_kwh * ptc_multiplier

        # --- Estimate values ---
        capacity_kw = (capacity_mw or 0) * 1000

        if can_itc and capacity_kw > 0:
            cost_per_kw = INSTALLED_COST_PER_KW.get(norm_tech, 1200)
            total_cost = cost_per_kw * capacity_kw
            result.estimated_itc_value = total_cost * result.effective_itc_rate

        if can_ptc and capacity_kw > 0:
            cf = CAPACITY_FACTORS.get(norm_tech, 0.25)
            annual_kwh = capacity_kw * cf * 8760
            result.estimated_annual_ptc_value = annual_kwh * result.effective_ptc_rate
            result.estimated_10yr_ptc_value = result.estimated_annual_ptc_value * 10

        # --- Recommend ITC vs PTC ---
        if can_itc and can_ptc and capacity_kw > 0:
            # Compare 10-year PTC value vs ITC value
            if result.estimated_10yr_ptc_value > result.estimated_itc_value:
                result.recommended_credit = 'ptc'
                result.recommended_value = result.estimated_10yr_ptc_value
                result.notes.append(
                    f"PTC recommended: ${result.estimated_10yr_ptc_value:,.0f} (10yr) > "
                    f"ITC ${result.estimated_itc_value:,.0f}"
                )
            else:
                result.recommended_credit = 'itc'
                result.recommended_value = result.estimated_itc_value
                result.notes.append(
                    f"ITC recommended: ${result.estimated_itc_value:,.0f} > "
                    f"PTC ${result.estimated_10yr_ptc_value:,.0f} (10yr)"
                )
        elif can_itc:
            result.recommended_credit = 'itc'
            result.recommended_value = result.estimated_itc_value
        elif can_ptc:
            result.recommended_credit = 'ptc'
            result.recommended_value = result.estimated_10yr_ptc_value

        return result


# =============================================================================
# Database Enrichment
# =============================================================================

def enrich_queue_with_tax_credits(db_path: Path = None, save: bool = True):
    """
    Add tax credit eligibility data to all projects in queue.db.

    Adds/updates columns:
    - tax_credit_type: 'itc', 'ptc', 'both', or NULL
    - recommended_credit: 'itc' or 'ptc'
    - base_credit_rate: Base ITC rate or PTC $/kWh
    - energy_community_eligible: 0/1
    - energy_community_bonus: Bonus rate (0.10 or 0)
    - domestic_content_eligible: 0/1/NULL (unknown)
    - effective_credit_rate: Total rate with bonuses
    - estimated_credit_value: Estimated $ value
    - max_possible_rate: Rate if all bonuses achieved
    - tax_credit_json: Full result as JSON
    """
    db = db_path or DB_PATH
    print("=" * 60)
    print("Tax Credit Eligibility Enrichment")
    print("=" * 60)

    engine = TaxCreditEngine()

    if not db.exists():
        raise FileNotFoundError(f"Database not found: {db}")

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row

    # Add columns if they don't exist
    new_columns = [
        ("tax_credit_type", "TEXT"),
        ("recommended_credit", "TEXT"),
        ("base_credit_rate", "REAL"),
        ("energy_community_eligible", "INTEGER"),
        ("energy_community_bonus", "REAL"),
        ("energy_community_type", "TEXT"),
        ("domestic_content_eligible", "INTEGER"),
        ("effective_credit_rate", "REAL"),
        ("estimated_credit_value", "REAL"),
        ("max_possible_rate", "REAL"),
        ("tax_credit_json", "TEXT"),
    ]

    for col_name, col_type in new_columns:
        try:
            conn.execute(f"ALTER TABLE projects ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass  # Column already exists

    # Get all projects
    cursor = conn.execute("""
        SELECT id, queue_id, region, type, capacity_mw, state, county, cod
        FROM projects
    """)
    projects = cursor.fetchall()
    print(f"\nProcessing {len(projects):,} projects...")

    stats = {
        'total': len(projects),
        'eligible': 0,
        'itc': 0,
        'ptc': 0,
        'energy_community': 0,
        'not_eligible': 0,
        'no_tech': 0,
    }

    for i, project in enumerate(projects):
        if (i + 1) % 5000 == 0:
            print(f"  Progress: {i+1:,}/{len(projects):,}")

        technology = project['type'] or ''
        capacity_mw = project['capacity_mw'] or 0
        state = project['state'] or ''
        county = project['county'] or ''

        # Parse COD year from cod field
        cod_year = None
        cod_str = project['cod'] or ''
        if cod_str:
            try:
                # Handle various date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%Y']:
                    try:
                        cod_year = datetime.strptime(cod_str[:10], fmt).year
                        break
                    except ValueError:
                        continue
                if cod_year is None and len(cod_str) >= 4:
                    cod_year = int(cod_str[:4])
            except (ValueError, TypeError):
                pass

        result = engine.calculate(
            technology=technology,
            capacity_mw=capacity_mw,
            state=state,
            county=county,
            cod_year=cod_year,
        )

        # Update stats
        if result.can_elect_itc or result.can_elect_ptc:
            stats['eligible'] += 1
            if result.recommended_credit == 'itc':
                stats['itc'] += 1
            elif result.recommended_credit == 'ptc':
                stats['ptc'] += 1
        elif not technology:
            stats['no_tech'] += 1
        else:
            stats['not_eligible'] += 1

        if result.energy_community_eligible:
            stats['energy_community'] += 1

        # Determine tax_credit_type
        if result.can_elect_itc and result.can_elect_ptc:
            tax_credit_type = 'both'
        elif result.can_elect_itc:
            tax_credit_type = 'itc'
        elif result.can_elect_ptc:
            tax_credit_type = 'ptc'
        else:
            tax_credit_type = None

        if save:
            conn.execute("""
                UPDATE projects SET
                    tax_credit_type = ?,
                    recommended_credit = ?,
                    base_credit_rate = ?,
                    energy_community_eligible = ?,
                    energy_community_bonus = ?,
                    energy_community_type = ?,
                    domestic_content_eligible = ?,
                    effective_credit_rate = ?,
                    estimated_credit_value = ?,
                    max_possible_rate = ?,
                    tax_credit_json = ?
                WHERE id = ?
            """, (
                tax_credit_type,
                result.recommended_credit if result.recommended_credit != 'none' else None,
                result.base_itc_rate if result.recommended_credit == 'itc' else result.base_ptc_rate_per_kwh,
                1 if result.energy_community_eligible else 0,
                result.energy_community_bonus,
                result.energy_community_type,
                1 if result.domestic_content_eligible is True else (0 if result.domestic_content_eligible is False else None),
                result.effective_itc_rate if result.recommended_credit == 'itc' else result.effective_ptc_rate,
                result.recommended_value,
                result.max_itc_rate if result.can_elect_itc else 0,
                json.dumps(result.to_dict(), default=str),
                project['id'],
            ))

    if save:
        conn.commit()

    # Print summary
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"\nTotal projects processed: {stats['total']:,}")
    print(f"Eligible for tax credits: {stats['eligible']:,} ({100*stats['eligible']/max(stats['total'],1):.1f}%)")
    print(f"  - ITC recommended: {stats['itc']:,}")
    print(f"  - PTC recommended: {stats['ptc']:,}")
    print(f"Energy community bonus: {stats['energy_community']:,}")
    print(f"Not eligible (fossil/other): {stats['not_eligible']:,}")
    print(f"No technology data: {stats['no_tech']:,}")

    conn.close()
    return stats


def get_tax_credit_stats(db_path: Path = None):
    """Print tax credit statistics from enriched database."""
    db = db_path or DB_PATH
    if not db.exists():
        raise FileNotFoundError(f"Database not found: {db}")

    conn = sqlite3.connect(db)

    # Check if columns exist
    cursor = conn.execute("PRAGMA table_info(projects)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'tax_credit_type' not in columns:
        print("Tax credit data not yet enriched. Run: python3 tax_credits.py --enrich")
        conn.close()
        return

    # Overall stats
    print("\n" + "=" * 60)
    print("TAX CREDIT ELIGIBILITY SUMMARY")
    print("=" * 60)

    cursor = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN tax_credit_type IS NOT NULL THEN 1 ELSE 0 END) as eligible,
            SUM(CASE WHEN recommended_credit = 'itc' THEN 1 ELSE 0 END) as itc,
            SUM(CASE WHEN recommended_credit = 'ptc' THEN 1 ELSE 0 END) as ptc,
            SUM(CASE WHEN energy_community_eligible = 1 THEN 1 ELSE 0 END) as ec,
            SUM(estimated_credit_value) as total_value,
            SUM(CASE WHEN recommended_credit = 'itc' THEN estimated_credit_value ELSE 0 END) as itc_value,
            SUM(CASE WHEN recommended_credit = 'ptc' THEN estimated_credit_value ELSE 0 END) as ptc_value
        FROM projects
    """)
    row = cursor.fetchone()
    total, eligible, itc, ptc, ec, total_val, itc_val, ptc_val = row

    print(f"\nTotal projects: {total:,}")
    print(f"Eligible: {eligible:,} ({100*eligible/max(total,1):.1f}%)")
    print(f"  ITC recommended: {itc:,}")
    print(f"  PTC recommended: {ptc:,}")
    print(f"  Energy community bonus: {ec:,}")
    print(f"\nEstimated total credit value: ${total_val/1e9:,.1f}B")
    print(f"  ITC value: ${itc_val/1e9:,.1f}B")
    print(f"  PTC value (10yr): ${ptc_val/1e9:,.1f}B")

    # By region
    print(f"\n{'Region':<12} {'Total':>8} {'Eligible':>10} {'Rate':>7} {'EC Bonus':>10} {'Est Value':>14}")
    print("-" * 65)

    cursor = conn.execute("""
        SELECT
            region,
            COUNT(*) as total,
            SUM(CASE WHEN tax_credit_type IS NOT NULL THEN 1 ELSE 0 END) as eligible,
            SUM(CASE WHEN energy_community_eligible = 1 THEN 1 ELSE 0 END) as ec,
            SUM(estimated_credit_value) as value
        FROM projects
        GROUP BY region
        ORDER BY total DESC
    """)

    for row in cursor.fetchall():
        region, total_r, eligible_r, ec_r, val_r = row
        rate = 100 * eligible_r / max(total_r, 1)
        val_str = f"${val_r/1e6:,.0f}M" if val_r else "$0"
        print(f"{region:<12} {total_r:>8,} {eligible_r:>10,} {rate:>6.1f}% {ec_r:>10,} {val_str:>14}")

    # By technology
    print(f"\n{'Technology':<25} {'Projects':>10} {'Rec Credit':>12} {'Avg Rate':>10} {'Est Value':>14}")
    print("-" * 75)

    cursor = conn.execute("""
        SELECT
            type,
            COUNT(*) as cnt,
            recommended_credit,
            AVG(effective_credit_rate) as avg_rate,
            SUM(estimated_credit_value) as value
        FROM projects
        WHERE tax_credit_type IS NOT NULL AND type IS NOT NULL
        GROUP BY type, recommended_credit
        ORDER BY cnt DESC
        LIMIT 20
    """)

    for row in cursor.fetchall():
        tech, cnt, rec, avg_rate, val = row
        tech_short = (tech or 'Unknown')[:24]
        rec_str = (rec or 'N/A').upper()
        rate_str = f"{avg_rate:.1%}" if avg_rate else "N/A"
        val_str = f"${val/1e6:,.0f}M" if val else "$0"
        print(f"{tech_short:<25} {cnt:>10,} {rec_str:>12} {rate_str:>10} {val_str:>14}")

    conn.close()


# =============================================================================
# CLI
# =============================================================================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Tax Credit Eligibility Engine')
    parser.add_argument('--enrich', action='store_true',
                        help='Enrich queue database with tax credit eligibility')
    parser.add_argument('--stats', action='store_true',
                        help='Show tax credit statistics from enriched database')
    parser.add_argument('--check', nargs=5,
                        metavar=('TECH', 'MW', 'STATE', 'COUNTY', 'YEAR'),
                        help='Check a specific project: --check Solar 200 TX Pecos 2027')
    parser.add_argument('--dry-run', action='store_true',
                        help='Do not save changes to database')
    parser.add_argument('--db', type=str, default=None,
                        help='Path to database (default: .data/queue.db)')

    args = parser.parse_args()
    db = Path(args.db) if args.db else None

    if args.check:
        tech, mw, state, county, year = args.check
        engine = TaxCreditEngine()
        result = engine.calculate(
            technology=tech,
            capacity_mw=float(mw),
            state=state,
            county=county,
            cod_year=int(year),
        )

        print(f"\nTax Credit Analysis: {tech} {mw}MW in {county}, {state} (COD {year})")
        print("=" * 60)
        print(f"Summary: {result.summary()}")
        print(f"\nEligible for ITC: {result.can_elect_itc}")
        print(f"Eligible for PTC: {result.can_elect_ptc}")
        print(f"Recommended: {result.recommended_credit.upper()}")
        print(f"\nBase ITC rate: {result.base_itc_rate:.0%}")
        print(f"Base PTC rate: ${result.base_ptc_rate_per_kwh:.4f}/kWh")
        print(f"\nEnergy Community: {'YES' if result.energy_community_eligible else 'No'}")
        if result.energy_community_eligible:
            print(f"  Type: {result.energy_community_type}")
            print(f"  Bonus: +{result.energy_community_bonus:.0%}")
        print(f"Domestic Content: {'YES' if result.domestic_content_eligible else 'Unknown' if result.domestic_content_eligible is None else 'No'}")
        if result.domestic_content_eligible:
            print(f"  Bonus: +{result.domestic_content_bonus:.0%}")
        print(f"\nEffective ITC rate: {result.effective_itc_rate:.0%}")
        print(f"Max possible ITC rate: {result.max_itc_rate:.0%}")
        print(f"Effective PTC rate: ${result.effective_ptc_rate:.4f}/kWh")
        print(f"\nEstimated ITC value: ${result.estimated_itc_value:,.0f}")
        print(f"Estimated annual PTC value: ${result.estimated_annual_ptc_value:,.0f}/yr")
        print(f"Estimated 10yr PTC value: ${result.estimated_10yr_ptc_value:,.0f}")
        print(f"\nRecommended value: ${result.recommended_value:,.0f}")
        if result.notes:
            print(f"\nNotes:")
            for note in result.notes:
                print(f"  - {note}")

    elif args.enrich:
        enrich_queue_with_tax_credits(db_path=db, save=not args.dry_run)

    elif args.stats:
        get_tax_credit_stats(db_path=db)

    else:
        parser.print_help()
