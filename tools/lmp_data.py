#!/usr/bin/env python3
"""
LMP (Locational Marginal Pricing) Data Module

Provides energy price data by location for revenue estimation.

Data Sources:
- ERCOT: Settlement Point Prices (SPP)
- PJM: Day-ahead LMPs via Data Miner
- NYISO: Day-ahead LBMPs
- MISO: Day-ahead LMPs
- CAISO: Day-ahead LMPs
- ISO-NE: Day-ahead LMPs
- SPP: Day-ahead LMPs

Usage:
    from lmp_data import LMPData, RevenueEstimator

    lmp = LMPData()
    lmp.refresh_all()

    # Get price stats for a zone
    stats = lmp.get_zone_stats('ERCOT', 'WEST')

    # Estimate revenue for a project
    estimator = RevenueEstimator()
    revenue = estimator.estimate_annual_revenue(
        region='ERCOT',
        zone='WEST',
        capacity_mw=200,
        technology='Solar'
    )
"""

import pandas as pd
import numpy as np
import requests
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import json
import warnings

warnings.filterwarnings('ignore')

DATA_DIR = Path(__file__).parent / '.data'
DB_PATH = DATA_DIR / 'queue.db'


# Typical capacity factors by technology
CAPACITY_FACTORS = {
    'Solar': 0.25,
    'Wind': 0.35,
    'Battery': 0.15,  # Assumes 4-hour duration, 1 cycle/day
    'Gas': 0.40,
    'Nuclear': 0.90,
    'Hydro': 0.40,
    'Other': 0.30,
}

# Technology aliases for matching
TECH_ALIASES = {
    'SOL': 'Solar',
    'Solar': 'Solar',
    'PV': 'Solar',
    'WIN': 'Wind',
    'Wind': 'Wind',
    'WND': 'Wind',
    'BAT': 'Battery',
    'Battery': 'Battery',
    'Storage': 'Battery',
    'BESS': 'Battery',
    'GAS': 'Gas',
    'Gas': 'Gas',
    'CCGT': 'Gas',
    'CT': 'Gas',
    'NG': 'Gas',
    'Natural Gas': 'Gas',
}


class LMPData:
    """Load and manage LMP pricing data."""

    def __init__(self):
        self.db_path = DB_PATH
        self._init_tables()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_tables(self):
        """Create LMP tables if they don't exist."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Pricing zones/nodes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS lmp_zones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT NOT NULL,
                zone_id TEXT NOT NULL,
                zone_name TEXT,
                zone_type TEXT,  -- hub, zone, node
                latitude REAL,
                longitude REAL,
                UNIQUE(region, zone_id)
            )
        ''')

        # Price summaries (monthly aggregates)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS lmp_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT NOT NULL,
                zone_id TEXT NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                avg_lmp REAL,
                peak_lmp REAL,
                offpeak_lmp REAL,
                min_lmp REAL,
                max_lmp REAL,
                volatility REAL,
                hours_negative INTEGER,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(region, zone_id, year, month)
            )
        ''')

        # Annual summaries for quick access
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS lmp_annual (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT NOT NULL,
                zone_id TEXT NOT NULL,
                year INTEGER NOT NULL,
                avg_lmp REAL,
                peak_lmp REAL,
                offpeak_lmp REAL,
                solar_weighted_avg REAL,
                wind_weighted_avg REAL,
                volatility REAL,
                pct_negative_hours REAL,
                UNIQUE(region, zone_id, year)
            )
        ''')

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_lmp_prices_region ON lmp_prices(region)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_lmp_prices_zone ON lmp_prices(zone_id)')

        conn.commit()
        conn.close()

    def refresh_all(self):
        """Refresh LMP data from all sources."""
        print("Refreshing LMP data...")

        # For now, load benchmark data
        # In production, these would pull from RTO APIs
        self._load_benchmark_data()

        print("LMP data refresh complete.")

    def _load_benchmark_data(self):
        """Load benchmark LMP data (static estimates based on historical averages)."""
        # Regional average LMPs based on 2023-2024 data
        # These are simplified zone-level averages
        benchmark_data = [
            # ERCOT zones
            ('ERCOT', 'WEST', 'West Zone', 'zone', 28.50, 45.20, 18.30, 0.08),
            ('ERCOT', 'NORTH', 'North Zone', 'zone', 32.10, 52.40, 22.50, 0.06),
            ('ERCOT', 'SOUTH', 'South Zone', 'zone', 30.80, 48.90, 21.20, 0.07),
            ('ERCOT', 'HOUSTON', 'Houston Zone', 'zone', 35.20, 58.30, 24.10, 0.05),
            ('ERCOT', 'HUB_AVG', 'ERCOT Hub Average', 'hub', 31.50, 51.20, 21.50, 0.065),

            # PJM zones
            ('PJM', 'WEST', 'PJM Western Hub', 'hub', 38.20, 55.80, 28.40, 0.02),
            ('PJM', 'AEP', 'AEP Zone', 'zone', 36.50, 52.30, 27.10, 0.02),
            ('PJM', 'COMED', 'ComEd Zone', 'zone', 34.80, 49.60, 25.90, 0.03),
            ('PJM', 'DOM', 'Dominion Zone', 'zone', 40.20, 58.90, 29.80, 0.02),
            ('PJM', 'PECO', 'PECO Zone', 'zone', 42.10, 62.40, 31.20, 0.01),

            # NYISO zones
            ('NYISO', 'WEST', 'Zone A (West)', 'zone', 35.40, 52.10, 26.30, 0.04),
            ('NYISO', 'CENTRAL', 'Zone C (Central)', 'zone', 38.20, 56.80, 28.40, 0.03),
            ('NYISO', 'HUDSON', 'Zone G (Hudson Valley)', 'zone', 45.60, 68.90, 33.80, 0.02),
            ('NYISO', 'NYC', 'Zone J (NYC)', 'zone', 52.30, 82.40, 38.60, 0.01),
            ('NYISO', 'LI', 'Zone K (Long Island)', 'zone', 58.90, 95.20, 43.50, 0.01),

            # MISO zones
            ('MISO', 'INDIANA', 'Indiana Hub', 'hub', 32.40, 48.60, 24.10, 0.04),
            ('MISO', 'MICHIGAN', 'Michigan Hub', 'hub', 34.80, 52.30, 25.90, 0.03),
            ('MISO', 'MINNESOTA', 'Minnesota Hub', 'hub', 28.60, 42.10, 21.30, 0.05),
            ('MISO', 'LOUISIANA', 'Louisiana Hub', 'hub', 36.20, 54.80, 26.80, 0.03),
            ('MISO', 'ARKANSAS', 'Arkansas Hub', 'hub', 30.40, 45.60, 22.60, 0.04),

            # CAISO zones
            ('CAISO', 'SP15', 'SP15 (Southern)', 'zone', 42.80, 68.40, 28.50, 0.12),
            ('CAISO', 'NP15', 'NP15 (Northern)', 'zone', 45.20, 72.30, 30.10, 0.10),
            ('CAISO', 'ZP26', 'ZP26 (Central)', 'zone', 40.60, 64.80, 27.20, 0.11),

            # SPP zones
            ('SPP', 'NORTH', 'SPP North', 'zone', 24.30, 38.60, 18.10, 0.08),
            ('SPP', 'SOUTH', 'SPP South', 'zone', 26.80, 42.40, 19.90, 0.07),
            ('SPP', 'HUB', 'SPP Hub', 'hub', 25.50, 40.20, 19.00, 0.075),

            # ISO-NE zones
            ('ISO-NE', 'MAINE', 'Maine Zone', 'zone', 48.20, 72.60, 35.80, 0.02),
            ('ISO-NE', 'NH', 'New Hampshire Zone', 'zone', 50.40, 76.20, 37.40, 0.02),
            ('ISO-NE', 'SEMA', 'SE Mass Zone', 'zone', 52.80, 82.40, 39.20, 0.01),
            ('ISO-NE', 'BOSTON', 'Boston Zone', 'zone', 54.60, 86.80, 40.50, 0.01),
            ('ISO-NE', 'HUB', 'ISO-NE Hub', 'hub', 51.50, 78.60, 38.20, 0.015),

            # West (non-ISO)
            ('West', 'PALO_VERDE', 'Palo Verde Hub', 'hub', 38.40, 62.80, 26.20, 0.09),
            ('West', 'MID_C', 'Mid-Columbia', 'hub', 32.60, 48.90, 24.10, 0.06),
            ('West', 'COB', 'California-Oregon Border', 'hub', 36.20, 56.40, 25.80, 0.08),

            # Southeast
            ('Southeast', 'SOUTHEAST', 'Southeast Average', 'hub', 42.50, 64.20, 31.40, 0.02),
        ]

        conn = self._get_conn()
        cursor = conn.cursor()

        for region, zone_id, zone_name, zone_type, avg, peak, offpeak, neg_pct in benchmark_data:
            # Insert zone
            cursor.execute('''
                INSERT OR REPLACE INTO lmp_zones (region, zone_id, zone_name, zone_type)
                VALUES (?, ?, ?, ?)
            ''', (region, zone_id, zone_name, zone_type))

            # Insert annual summary for recent years
            for year in [2023, 2024, 2025]:
                # Add some year-over-year variation
                year_factor = 1.0 + (year - 2024) * 0.03
                cursor.execute('''
                    INSERT OR REPLACE INTO lmp_annual
                    (region, zone_id, year, avg_lmp, peak_lmp, offpeak_lmp,
                     solar_weighted_avg, wind_weighted_avg, volatility, pct_negative_hours)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    region, zone_id, year,
                    avg * year_factor,
                    peak * year_factor,
                    offpeak * year_factor,
                    avg * year_factor * 0.85,  # Solar sees lower prices (midday)
                    avg * year_factor * 1.05,  # Wind sees slightly higher (evening)
                    avg * 0.4,  # Volatility ~40% of average
                    neg_pct
                ))

        conn.commit()
        conn.close()
        print(f"  Loaded benchmark LMP data for {len(benchmark_data)} zones")

    def get_zones(self, region: str = None) -> pd.DataFrame:
        """Get available pricing zones."""
        conn = self._get_conn()
        query = "SELECT * FROM lmp_zones"
        if region:
            query += f" WHERE region = '{region}'"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def get_zone_stats(self, region: str, zone_id: str = None, year: int = 2024) -> Dict:
        """Get price statistics for a zone."""
        conn = self._get_conn()
        cursor = conn.cursor()

        if zone_id:
            cursor.execute('''
                SELECT * FROM lmp_annual
                WHERE region = ? AND zone_id = ? AND year = ?
            ''', (region, zone_id, year))
        else:
            # Get hub/average for region - join with lmp_zones for zone_type
            cursor.execute('''
                SELECT a.* FROM lmp_annual a
                LEFT JOIN lmp_zones z ON a.region = z.region AND a.zone_id = z.zone_id
                WHERE a.region = ? AND a.year = ?
                AND (z.zone_type = 'hub' OR a.zone_id LIKE '%HUB%' OR a.zone_id LIKE '%AVG%')
                LIMIT 1
            ''', (region, year))

        row = cursor.fetchone()
        conn.close()

        if not row:
            return self._get_default_stats(region)

        return {
            'region': row['region'],
            'zone_id': row['zone_id'],
            'year': row['year'],
            'avg_lmp': row['avg_lmp'],
            'peak_lmp': row['peak_lmp'],
            'offpeak_lmp': row['offpeak_lmp'],
            'solar_weighted_avg': row['solar_weighted_avg'],
            'wind_weighted_avg': row['wind_weighted_avg'],
            'volatility': row['volatility'],
            'pct_negative_hours': row['pct_negative_hours'],
        }

    def _get_default_stats(self, region: str) -> Dict:
        """Return default stats when no data available."""
        # Regional defaults
        defaults = {
            'ERCOT': {'avg': 31.5, 'peak': 51.2, 'offpeak': 21.5, 'neg': 0.065},
            'PJM': {'avg': 38.2, 'peak': 55.8, 'offpeak': 28.4, 'neg': 0.02},
            'NYISO': {'avg': 42.5, 'peak': 65.0, 'offpeak': 31.5, 'neg': 0.02},
            'MISO': {'avg': 32.5, 'peak': 48.5, 'offpeak': 24.0, 'neg': 0.04},
            'CAISO': {'avg': 43.0, 'peak': 68.5, 'offpeak': 28.5, 'neg': 0.11},
            'SPP': {'avg': 25.5, 'peak': 40.2, 'offpeak': 19.0, 'neg': 0.075},
            'ISO-NE': {'avg': 51.5, 'peak': 78.6, 'offpeak': 38.2, 'neg': 0.015},
            'West': {'avg': 35.5, 'peak': 55.0, 'offpeak': 25.0, 'neg': 0.07},
            'Southeast': {'avg': 42.5, 'peak': 64.2, 'offpeak': 31.4, 'neg': 0.02},
        }

        d = defaults.get(region, {'avg': 35.0, 'peak': 52.0, 'offpeak': 26.0, 'neg': 0.05})

        return {
            'region': region,
            'zone_id': 'DEFAULT',
            'year': 2024,
            'avg_lmp': d['avg'],
            'peak_lmp': d['peak'],
            'offpeak_lmp': d['offpeak'],
            'solar_weighted_avg': d['avg'] * 0.85,
            'wind_weighted_avg': d['avg'] * 1.05,
            'volatility': d['avg'] * 0.4,
            'pct_negative_hours': d['neg'],
        }

    def map_poi_to_zone(self, region: str, poi: str, state: str = None) -> str:
        """Map a POI to the best matching pricing zone."""
        # Simple mapping logic - in production would use geographic lookup
        poi_lower = poi.lower() if poi else ''
        state_upper = state.upper() if state else ''

        # ERCOT zone mapping
        if region == 'ERCOT':
            if 'houston' in poi_lower or state_upper == 'TX' and 'harris' in poi_lower:
                return 'HOUSTON'
            elif 'dallas' in poi_lower or 'north' in poi_lower:
                return 'NORTH'
            elif 'corpus' in poi_lower or 'south' in poi_lower:
                return 'SOUTH'
            else:
                return 'WEST'

        # NYISO zone mapping
        elif region == 'NYISO':
            if 'nyc' in poi_lower or 'new york city' in poi_lower:
                return 'NYC'
            elif 'long island' in poi_lower:
                return 'LI'
            elif 'hudson' in poi_lower:
                return 'HUDSON'
            elif 'buffalo' in poi_lower or 'rochester' in poi_lower:
                return 'WEST'
            else:
                return 'CENTRAL'

        # PJM zone mapping
        elif region == 'PJM':
            if state_upper in ['VA', 'NC']:
                return 'DOM'
            elif state_upper in ['PA']:
                return 'PECO'
            elif state_upper in ['IL']:
                return 'COMED'
            elif state_upper in ['OH', 'WV', 'KY']:
                return 'AEP'
            else:
                return 'WEST'

        # Default to hub for other regions
        return 'HUB'


class RevenueEstimator:
    """Estimate project revenue based on LMP data."""

    def __init__(self):
        self.lmp = LMPData()

    def estimate_annual_revenue(
        self,
        region: str,
        capacity_mw: float,
        technology: str,
        zone_id: str = None,
        poi: str = None,
        state: str = None,
    ) -> Dict[str, Any]:
        """
        Estimate annual energy revenue for a project.

        Args:
            region: RTO/ISO region
            capacity_mw: Project capacity in MW
            technology: Technology type (Solar, Wind, Battery, Gas, etc.)
            zone_id: Optional specific zone
            poi: POI name for zone mapping
            state: State for zone mapping

        Returns:
            Dict with revenue estimates and breakdown
        """
        # Normalize technology
        tech = TECH_ALIASES.get(technology, technology)
        if tech not in CAPACITY_FACTORS:
            tech = 'Other'

        # Get capacity factor
        cf = CAPACITY_FACTORS[tech]

        # Map to zone if not specified
        if not zone_id and poi:
            zone_id = self.lmp.map_poi_to_zone(region, poi, state)

        # Get price stats
        stats = self.lmp.get_zone_stats(region, zone_id)

        # Use technology-appropriate price
        if tech == 'Solar':
            effective_price = stats['solar_weighted_avg']
        elif tech == 'Wind':
            effective_price = stats['wind_weighted_avg']
        else:
            effective_price = stats['avg_lmp']

        # Calculate annual generation (MWh)
        hours_per_year = 8760
        annual_generation = capacity_mw * cf * hours_per_year

        # Calculate revenue
        annual_revenue = annual_generation * effective_price

        # Calculate range (using volatility)
        volatility_factor = stats['volatility'] / stats['avg_lmp'] if stats['avg_lmp'] > 0 else 0.3
        revenue_low = annual_revenue * (1 - volatility_factor)
        revenue_high = annual_revenue * (1 + volatility_factor)

        # Negative price adjustment
        if stats['pct_negative_hours'] > 0.05:
            # Significant negative pricing - reduce revenue estimate
            curtailment_factor = 1 - (stats['pct_negative_hours'] * 0.5)
            annual_revenue *= curtailment_factor
            revenue_low *= curtailment_factor
            revenue_high *= curtailment_factor

        return {
            'region': region,
            'zone_id': stats['zone_id'],
            'technology': tech,
            'capacity_mw': capacity_mw,
            'capacity_factor': cf,
            'annual_generation_mwh': round(annual_generation, 0),
            'effective_price_mwh': round(effective_price, 2),
            'annual_revenue': round(annual_revenue, 0),
            'revenue_low': round(revenue_low, 0),
            'revenue_high': round(revenue_high, 0),
            'revenue_per_kw': round(annual_revenue / (capacity_mw * 1000), 2) if capacity_mw > 0 else 0,
            'price_stats': {
                'avg_lmp': round(stats['avg_lmp'], 2),
                'peak_lmp': round(stats['peak_lmp'], 2),
                'offpeak_lmp': round(stats['offpeak_lmp'], 2),
                'pct_negative_hours': round(stats['pct_negative_hours'] * 100, 1),
            },
            'notes': self._generate_notes(stats, tech),
        }

    def _generate_notes(self, stats: Dict, tech: str) -> List[str]:
        """Generate notes about the revenue estimate."""
        notes = []

        if stats['pct_negative_hours'] > 0.10:
            notes.append(f"High negative pricing risk ({stats['pct_negative_hours']*100:.0f}% of hours) - common for {tech} in this zone")

        if stats['avg_lmp'] < 30:
            notes.append("Below-average energy prices in this zone")
        elif stats['avg_lmp'] > 50:
            notes.append("Above-average energy prices in this zone")

        if tech == 'Solar' and stats.get('solar_weighted_avg', 0) < stats['avg_lmp'] * 0.8:
            notes.append("Solar generation coincides with low-price hours (duck curve effect)")

        return notes

    def format_revenue(self, estimate: Dict) -> str:
        """Format revenue estimate for display."""
        low = estimate['revenue_low'] / 1_000_000
        mid = estimate['annual_revenue'] / 1_000_000
        high = estimate['revenue_high'] / 1_000_000

        return f"${low:.1f}M - ${mid:.1f}M - ${high:.1f}M"


def main():
    """Demo the LMP data module."""
    print("=" * 60)
    print("LMP DATA MODULE DEMO")
    print("=" * 60)

    # Initialize and load data
    lmp = LMPData()
    lmp.refresh_all()

    # Show available zones
    print("\nAvailable Pricing Zones:")
    zones = lmp.get_zones()
    for region in zones['region'].unique():
        region_zones = zones[zones['region'] == region]
        print(f"\n  {region}:")
        for _, z in region_zones.iterrows():
            print(f"    {z['zone_id']}: {z['zone_name']}")

    # Demo revenue estimation
    print("\n" + "=" * 60)
    print("REVENUE ESTIMATION EXAMPLES")
    print("=" * 60)

    estimator = RevenueEstimator()

    examples = [
        ('ERCOT', 200, 'Solar', 'WEST'),
        ('ERCOT', 300, 'Wind', 'NORTH'),
        ('PJM', 150, 'Solar', 'AEP'),
        ('NYISO', 100, 'Battery', 'NYC'),
        ('CAISO', 250, 'Solar', 'SP15'),
    ]

    for region, mw, tech, zone in examples:
        result = estimator.estimate_annual_revenue(
            region=region,
            capacity_mw=mw,
            technology=tech,
            zone_id=zone
        )
        print(f"\n{region} - {mw} MW {tech} in {zone}:")
        print(f"  Generation: {result['annual_generation_mwh']:,.0f} MWh/year")
        print(f"  Price: ${result['effective_price_mwh']}/MWh")
        print(f"  Revenue: {estimator.format_revenue(result)}")
        if result['notes']:
            for note in result['notes']:
                print(f"  Note: {note}")


if __name__ == "__main__":
    main()
