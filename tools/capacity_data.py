#!/usr/bin/env python3
"""
Capacity Market Data Module

Provides capacity price data and ELCC values for revenue estimation.

Markets Covered:
- PJM: RPM (Reliability Pricing Model)
- NYISO: ICAP (Installed Capacity)
- ISO-NE: FCM (Forward Capacity Market)
- MISO: PRA (Planning Resource Auction)
- CAISO: RA (Resource Adequacy - bilateral)
- ERCOT: No capacity market (energy-only)

Usage:
    from capacity_data import CapacityData, CapacityValue

    cap = CapacityData()

    # Get capacity price for a zone
    price = cap.get_capacity_price('PJM', 'EMAAC', delivery_year='2025/2026')

    # Calculate capacity value for a project
    cv = CapacityValue()
    value = cv.calculate_capacity_value(
        region='PJM',
        zone='EMAAC',
        technology='Solar',
        capacity_mw=200
    )
"""

import pandas as pd
import numpy as np
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List

DATA_DIR = Path(__file__).parent / '.data'
DB_PATH = DATA_DIR / 'queue.db'


# ELCC (Effective Load Carrying Capability) by technology
# These are typical values - actual values vary by region and vintage
ELCC_VALUES = {
    'PJM': {
        'Solar': 0.38,  # PJM CIRs for solar
        'Wind': 0.13,   # PJM CIRs for wind
        'Battery': 0.85,  # 4-hour duration
        'Gas': 0.95,
        'Nuclear': 0.97,
        'Hydro': 0.50,
        'Other': 0.50,
    },
    'NYISO': {
        'Solar': 0.25,  # Lower in NY due to winter peak
        'Wind': 0.10,
        'Battery': 0.90,
        'Gas': 0.95,
        'Nuclear': 0.97,
        'Hydro': 0.45,
        'Other': 0.50,
    },
    'ISO-NE': {
        'Solar': 0.20,  # Winter peaking - low solar credit
        'Wind': 0.15,
        'Battery': 0.90,
        'Gas': 0.95,
        'Nuclear': 0.97,
        'Hydro': 0.40,
        'Other': 0.50,
    },
    'MISO': {
        'Solar': 0.50,  # Higher in summer-peaking MISO
        'Wind': 0.15,
        'Battery': 0.85,
        'Gas': 0.95,
        'Nuclear': 0.97,
        'Hydro': 0.50,
        'Other': 0.50,
    },
    'CAISO': {
        'Solar': 0.30,  # NQC values (declining)
        'Wind': 0.20,
        'Battery': 0.95,  # High value in CA
        'Gas': 0.95,
        'Nuclear': 0.97,
        'Hydro': 0.60,
        'Other': 0.50,
    },
}

# Default ELCC for regions not listed
DEFAULT_ELCC = {
    'Solar': 0.35,
    'Wind': 0.15,
    'Battery': 0.85,
    'Gas': 0.95,
    'Nuclear': 0.97,
    'Hydro': 0.50,
    'Other': 0.50,
}

# Technology aliases
TECH_ALIASES = {
    'SOL': 'Solar', 'Solar': 'Solar', 'PV': 'Solar',
    'WIN': 'Wind', 'Wind': 'Wind', 'WND': 'Wind',
    'BAT': 'Battery', 'Battery': 'Battery', 'Storage': 'Battery', 'BESS': 'Battery',
    'GAS': 'Gas', 'Gas': 'Gas', 'CCGT': 'Gas', 'CT': 'Gas', 'NG': 'Gas',
    'NUC': 'Nuclear', 'Nuclear': 'Nuclear',
    'HYD': 'Hydro', 'Hydro': 'Hydro',
}


class CapacityData:
    """Load and manage capacity market data."""

    def __init__(self):
        self.db_path = DB_PATH
        self._init_tables()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_tables(self):
        """Create capacity market tables."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Capacity zones
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS capacity_zones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT NOT NULL,
                zone_id TEXT NOT NULL,
                zone_name TEXT,
                parent_zone TEXT,
                UNIQUE(region, zone_id)
            )
        ''')

        # Capacity prices by delivery year
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS capacity_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT NOT NULL,
                zone_id TEXT NOT NULL,
                delivery_year TEXT NOT NULL,
                price_mw_day REAL,
                price_kw_month REAL,
                auction_date TEXT,
                clearing_type TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(region, zone_id, delivery_year)
            )
        ''')

        # ELCC values by technology and year
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS elcc_values (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT NOT NULL,
                technology TEXT NOT NULL,
                year INTEGER NOT NULL,
                elcc_percent REAL,
                notes TEXT,
                UNIQUE(region, technology, year)
            )
        ''')

        conn.commit()
        conn.close()

    def refresh_all(self):
        """Refresh capacity market data."""
        print("Refreshing capacity market data...")
        self._load_benchmark_data()
        print("Capacity data refresh complete.")

    def _load_benchmark_data(self):
        """Load benchmark capacity prices."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # PJM RPM prices ($/MW-day)
        pjm_data = [
            # Zone, 2024/25, 2025/26, 2026/27
            ('RTO', 'PJM RTO', 28.92, 269.92, 280.00),
            ('EMAAC', 'Eastern MAAC', 54.95, 269.92, 290.00),
            ('SWMAAC', 'Southwest MAAC', 54.95, 269.92, 285.00),
            ('MAAC', 'MAAC', 49.49, 269.92, 285.00),
            ('DPL_SOUTH', 'DPL South', 69.95, 285.00, 295.00),
            ('ATSI', 'ATSI', 28.92, 269.92, 280.00),
            ('COMED', 'ComEd', 28.92, 269.92, 275.00),
            ('AEP', 'AEP', 28.92, 269.92, 275.00),
            ('DOM', 'Dominion', 28.92, 269.92, 280.00),
        ]

        for zone_id, zone_name, p2425, p2526, p2627 in pjm_data:
            cursor.execute('''
                INSERT OR REPLACE INTO capacity_zones (region, zone_id, zone_name)
                VALUES ('PJM', ?, ?)
            ''', (zone_id, zone_name))

            for dy, price in [('2024/2025', p2425), ('2025/2026', p2526), ('2026/2027', p2627)]:
                cursor.execute('''
                    INSERT OR REPLACE INTO capacity_prices
                    (region, zone_id, delivery_year, price_mw_day, price_kw_month)
                    VALUES ('PJM', ?, ?, ?, ?)
                ''', (zone_id, dy, price, price * 30.4167 / 1000))  # Convert to $/kW-month

        # NYISO ICAP prices ($/kW-month)
        nyiso_data = [
            ('NYCA', 'NYCA (Statewide)', 3.50, 4.20, 5.00),
            ('LHV', 'Lower Hudson Valley', 5.50, 6.80, 8.00),
            ('NYC', 'New York City', 12.50, 14.00, 15.50),
            ('LI', 'Long Island', 8.50, 10.00, 11.50),
        ]

        for zone_id, zone_name, p2024, p2025, p2026 in nyiso_data:
            cursor.execute('''
                INSERT OR REPLACE INTO capacity_zones (region, zone_id, zone_name)
                VALUES ('NYISO', ?, ?)
            ''', (zone_id, zone_name))

            for dy, price in [('2024', p2024), ('2025', p2025), ('2026', p2026)]:
                cursor.execute('''
                    INSERT OR REPLACE INTO capacity_prices
                    (region, zone_id, delivery_year, price_mw_day, price_kw_month)
                    VALUES ('NYISO', ?, ?, ?, ?)
                ''', (zone_id, dy, price * 1000 / 30.4167, price))  # Store both

        # ISO-NE FCM prices ($/kW-month)
        isone_data = [
            ('SYSTEM', 'ISO-NE System', 2.50, 3.20, 4.00),
            ('SEMA', 'SE Massachusetts', 4.50, 5.50, 6.50),
            ('BOSTON', 'Boston', 5.00, 6.00, 7.00),
        ]

        for zone_id, zone_name, p2024, p2025, p2026 in isone_data:
            cursor.execute('''
                INSERT OR REPLACE INTO capacity_zones (region, zone_id, zone_name)
                VALUES ('ISO-NE', ?, ?)
            ''', (zone_id, zone_name))

            for dy, price in [('2024', p2024), ('2025', p2025), ('2026', p2026)]:
                cursor.execute('''
                    INSERT OR REPLACE INTO capacity_prices
                    (region, zone_id, delivery_year, price_mw_day, price_kw_month)
                    VALUES ('ISO-NE', ?, ?, ?, ?)
                ''', (zone_id, dy, price * 1000 / 30.4167, price))

        # MISO PRA prices ($/MW-day)
        miso_data = [
            ('ZONE1', 'MISO Zone 1', 30.00, 45.00, 60.00),
            ('ZONE4', 'MISO Zone 4', 35.00, 50.00, 65.00),
            ('ZONE6', 'MISO Zone 6', 25.00, 40.00, 55.00),
        ]

        for zone_id, zone_name, p2024, p2025, p2026 in miso_data:
            cursor.execute('''
                INSERT OR REPLACE INTO capacity_zones (region, zone_id, zone_name)
                VALUES ('MISO', ?, ?)
            ''', (zone_id, zone_name))

            for dy, price in [('2024', p2024), ('2025', p2025), ('2026', p2026)]:
                cursor.execute('''
                    INSERT OR REPLACE INTO capacity_prices
                    (region, zone_id, delivery_year, price_mw_day, price_kw_month)
                    VALUES ('MISO', ?, ?, ?, ?)
                ''', (zone_id, dy, price, price * 30.4167 / 1000))

        # CAISO RA prices ($/kW-month) - bilateral market estimates
        caiso_data = [
            ('SYSTEM', 'CAISO System', 5.50, 7.00, 8.50),
            ('LOCAL', 'Local RA', 8.00, 10.00, 12.00),
        ]

        for zone_id, zone_name, p2024, p2025, p2026 in caiso_data:
            cursor.execute('''
                INSERT OR REPLACE INTO capacity_zones (region, zone_id, zone_name)
                VALUES ('CAISO', ?, ?)
            ''', (zone_id, zone_name))

            for dy, price in [('2024', p2024), ('2025', p2025), ('2026', p2026)]:
                cursor.execute('''
                    INSERT OR REPLACE INTO capacity_prices
                    (region, zone_id, delivery_year, price_mw_day, price_kw_month)
                    VALUES ('CAISO', ?, ?, ?, ?)
                ''', (zone_id, dy, price * 1000 / 30.4167, price))

        # Load ELCC values
        for region, tech_elcc in ELCC_VALUES.items():
            for tech, elcc in tech_elcc.items():
                for year in [2024, 2025, 2026]:
                    # ELCC degrades over time for solar
                    if tech == 'Solar':
                        adj_elcc = elcc * (1 - 0.03 * (year - 2024))  # 3% decline per year
                    else:
                        adj_elcc = elcc

                    cursor.execute('''
                        INSERT OR REPLACE INTO elcc_values
                        (region, technology, year, elcc_percent)
                        VALUES (?, ?, ?, ?)
                    ''', (region, tech, year, adj_elcc))

        conn.commit()
        conn.close()
        print(f"  Loaded capacity prices for PJM, NYISO, ISO-NE, MISO, CAISO")

    def get_capacity_price(
        self,
        region: str,
        zone_id: str = None,
        delivery_year: str = None
    ) -> Dict:
        """Get capacity price for a zone."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Default to upcoming delivery year
        if not delivery_year:
            delivery_year = '2025/2026' if region == 'PJM' else '2025'

        # Default to system/RTO zone
        if not zone_id:
            zone_id = 'RTO' if region == 'PJM' else 'SYSTEM'

        cursor.execute('''
            SELECT * FROM capacity_prices
            WHERE region = ? AND zone_id = ? AND delivery_year = ?
        ''', (region, zone_id, delivery_year))

        row = cursor.fetchone()
        conn.close()

        if row:
            return {
                'region': row['region'],
                'zone_id': row['zone_id'],
                'delivery_year': row['delivery_year'],
                'price_mw_day': row['price_mw_day'],
                'price_kw_month': row['price_kw_month'],
                'annual_price_kw': row['price_kw_month'] * 12,
            }

        # Return default if no data
        return self._get_default_price(region)

    def _get_default_price(self, region: str) -> Dict:
        """Return default capacity price."""
        defaults = {
            'PJM': 270.0,       # $/MW-day
            'NYISO': 5.0,       # $/kW-month
            'ISO-NE': 4.0,
            'MISO': 50.0,       # $/MW-day
            'CAISO': 7.0,       # $/kW-month
        }

        price = defaults.get(region, 50.0)

        # Determine if $/MW-day or $/kW-month based on region
        if region in ['PJM', 'MISO']:
            return {
                'region': region,
                'zone_id': 'DEFAULT',
                'delivery_year': '2025',
                'price_mw_day': price,
                'price_kw_month': price * 30.4167 / 1000,
                'annual_price_kw': price * 365 / 1000,
            }
        else:
            return {
                'region': region,
                'zone_id': 'DEFAULT',
                'delivery_year': '2025',
                'price_mw_day': price * 1000 / 30.4167,
                'price_kw_month': price,
                'annual_price_kw': price * 12,
            }

    def get_elcc(self, region: str, technology: str, year: int = 2025) -> float:
        """Get ELCC value for a technology in a region."""
        tech = TECH_ALIASES.get(technology, technology)

        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT elcc_percent FROM elcc_values
            WHERE region = ? AND technology = ? AND year = ?
        ''', (region, tech, year))

        row = cursor.fetchone()
        conn.close()

        if row:
            return row[0]

        # Return default
        region_elcc = ELCC_VALUES.get(region, DEFAULT_ELCC)
        return region_elcc.get(tech, DEFAULT_ELCC.get(tech, 0.50))

    def has_capacity_market(self, region: str) -> bool:
        """Check if region has a capacity market."""
        return region in ['PJM', 'NYISO', 'ISO-NE', 'MISO', 'CAISO']


class CapacityValue:
    """Calculate capacity value for projects."""

    def __init__(self):
        self.cap = CapacityData()

    def calculate_capacity_value(
        self,
        region: str,
        capacity_mw: float,
        technology: str,
        zone_id: str = None,
        delivery_year: str = None
    ) -> Dict[str, Any]:
        """
        Calculate annual capacity value for a project.

        Args:
            region: RTO/ISO region
            capacity_mw: Project nameplate capacity
            technology: Technology type
            zone_id: Optional specific zone
            delivery_year: Target delivery year

        Returns:
            Dict with capacity value estimates
        """
        # Check if region has capacity market
        if not self.cap.has_capacity_market(region):
            return {
                'region': region,
                'has_capacity_market': False,
                'capacity_mw': capacity_mw,
                'technology': technology,
                'message': f'{region} is an energy-only market (no capacity payments)',
                'annual_capacity_value': 0,
            }

        # Get ELCC
        tech = TECH_ALIASES.get(technology, technology)
        year = int(delivery_year[:4]) if delivery_year else 2025
        elcc = self.cap.get_elcc(region, tech, year)

        # Calculate accredited capacity
        accredited_mw = capacity_mw * elcc

        # Get capacity price
        price_data = self.cap.get_capacity_price(region, zone_id, delivery_year)

        # Calculate annual value
        if region in ['PJM', 'MISO']:
            # $/MW-day markets
            annual_value = accredited_mw * price_data['price_mw_day'] * 365
        else:
            # $/kW-month markets
            annual_value = accredited_mw * 1000 * price_data['price_kw_month'] * 12

        return {
            'region': region,
            'has_capacity_market': True,
            'zone_id': price_data['zone_id'],
            'delivery_year': price_data['delivery_year'],
            'capacity_mw': capacity_mw,
            'technology': tech,
            'elcc': round(elcc, 2),
            'accredited_mw': round(accredited_mw, 1),
            'price_mw_day': round(price_data['price_mw_day'], 2),
            'price_kw_month': round(price_data['price_kw_month'], 2),
            'annual_capacity_value': round(annual_value, 0),
            'value_per_kw': round(annual_value / (capacity_mw * 1000), 2) if capacity_mw > 0 else 0,
            'notes': self._generate_notes(region, tech, elcc),
        }

    def _generate_notes(self, region: str, tech: str, elcc: float) -> List[str]:
        """Generate notes about capacity value."""
        notes = []

        if tech == 'Solar' and elcc < 0.30:
            notes.append("Low solar ELCC due to declining capacity credit (marginal saturation)")

        if tech == 'Wind' and elcc < 0.20:
            notes.append("Wind receives limited capacity credit in this region")

        if tech == 'Battery':
            notes.append("Battery capacity credit assumes 4-hour duration")

        if region == 'PJM' and tech in ['Solar', 'Wind']:
            notes.append("PJM uses CIR (Capacity Interconnection Rights) for capacity accreditation")

        return notes

    def format_value(self, result: Dict) -> str:
        """Format capacity value for display."""
        if not result.get('has_capacity_market'):
            return "N/A (energy-only market)"

        value = result['annual_capacity_value'] / 1_000_000
        return f"${value:.2f}M/year"


def main():
    """Demo the capacity data module."""
    print("=" * 60)
    print("CAPACITY MARKET DATA DEMO")
    print("=" * 60)

    # Initialize and load data
    cap = CapacityData()
    cap.refresh_all()

    # Show sample prices
    print("\nSample Capacity Prices (2025/2026):")
    for region in ['PJM', 'NYISO', 'ISO-NE', 'MISO', 'CAISO']:
        price = cap.get_capacity_price(region)
        if region in ['PJM', 'MISO']:
            print(f"  {region}: ${price['price_mw_day']:.2f}/MW-day")
        else:
            print(f"  {region}: ${price['price_kw_month']:.2f}/kW-month")

    # Demo capacity value calculations
    print("\n" + "=" * 60)
    print("CAPACITY VALUE EXAMPLES")
    print("=" * 60)

    cv = CapacityValue()

    examples = [
        ('PJM', 200, 'Solar', 'EMAAC'),
        ('PJM', 300, 'Wind', 'RTO'),
        ('NYISO', 100, 'Battery', 'NYC'),
        ('ERCOT', 200, 'Solar', None),  # No capacity market
        ('CAISO', 250, 'Solar', 'SYSTEM'),
    ]

    for region, mw, tech, zone in examples:
        result = cv.calculate_capacity_value(
            region=region,
            capacity_mw=mw,
            technology=tech,
            zone_id=zone
        )

        print(f"\n{region} - {mw} MW {tech}:")
        if result.get('has_capacity_market'):
            print(f"  ELCC: {result['elcc']*100:.0f}%")
            print(f"  Accredited: {result['accredited_mw']:.1f} MW")
            print(f"  Value: {cv.format_value(result)}")
            print(f"  Per kW: ${result['value_per_kw']:.2f}/kW-year")
            if result['notes']:
                for note in result['notes']:
                    print(f"  Note: {note}")
        else:
            print(f"  {result['message']}")


if __name__ == "__main__":
    main()
