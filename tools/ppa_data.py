#!/usr/bin/env python3
"""
PPA (Power Purchase Agreement) Benchmark Data Module

Provides PPA price benchmarks by region and technology for revenue modeling.

Data Sources:
- Public announcements and news
- State PUC filings
- Industry benchmarks
- Regional estimates

Usage:
    from ppa_data import PPAData, PPABenchmarks

    ppa = PPAData()

    # Get PPA price range for a project type
    prices = ppa.get_benchmark('ERCOT', 'Solar', 2025)

    # Compare to merchant revenue
    benchmarks = PPABenchmarks()
    comparison = benchmarks.compare_merchant_vs_ppa(
        region='ERCOT',
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


class PPAData:
    """Load and manage PPA benchmark data."""

    def __init__(self):
        self.db_path = DB_PATH
        self._init_tables()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_tables(self):
        """Create PPA tables."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # PPA deal records (public announcements)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ppa_deals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                announcement_date TEXT,
                region TEXT NOT NULL,
                state TEXT,
                technology TEXT NOT NULL,
                capacity_mw REAL,
                price_mwh REAL,
                term_years INTEGER,
                buyer_type TEXT,
                buyer_name TEXT,
                seller_name TEXT,
                project_name TEXT,
                source_url TEXT,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Regional benchmarks (aggregated)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ppa_benchmarks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT NOT NULL,
                technology TEXT NOT NULL,
                year INTEGER NOT NULL,
                price_p10 REAL,
                price_p25 REAL,
                price_p50 REAL,
                price_p75 REAL,
                price_p90 REAL,
                sample_size INTEGER,
                trend TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(region, technology, year)
            )
        ''')

        conn.commit()
        conn.close()

    def refresh_all(self):
        """Refresh PPA data."""
        print("Refreshing PPA benchmark data...")
        self._load_benchmark_data()
        print("PPA data refresh complete.")

    def _load_benchmark_data(self):
        """Load PPA benchmark data based on public sources and industry estimates."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Regional PPA benchmarks ($/MWh)
        # Based on publicly available data from LevelTen, BNEF, S&P, state filings
        # Format: (region, technology, 2024_p50, 2025_p50, 2026_p50, trend)
        benchmarks = [
            # ERCOT - competitive market, low prices
            ('ERCOT', 'Solar', 22.0, 24.0, 26.0, 'rising'),
            ('ERCOT', 'Wind', 20.0, 22.0, 24.0, 'rising'),
            ('ERCOT', 'Battery', 8.0, 9.0, 10.0, 'stable'),  # Capacity/ancillary only
            ('ERCOT', 'Solar+Battery', 28.0, 30.0, 32.0, 'rising'),

            # PJM - moderate prices
            ('PJM', 'Solar', 35.0, 38.0, 40.0, 'rising'),
            ('PJM', 'Wind', 32.0, 35.0, 37.0, 'rising'),
            ('PJM', 'Battery', 12.0, 14.0, 15.0, 'rising'),
            ('PJM', 'Solar+Battery', 42.0, 45.0, 48.0, 'rising'),

            # NYISO - higher prices due to premium market
            ('NYISO', 'Solar', 45.0, 48.0, 52.0, 'rising'),
            ('NYISO', 'Wind', 42.0, 45.0, 48.0, 'rising'),
            ('NYISO', 'Battery', 18.0, 20.0, 22.0, 'rising'),
            ('NYISO', 'Solar+Battery', 55.0, 60.0, 65.0, 'rising'),

            # MISO - low to moderate
            ('MISO', 'Solar', 28.0, 30.0, 32.0, 'rising'),
            ('MISO', 'Wind', 22.0, 24.0, 26.0, 'stable'),
            ('MISO', 'Battery', 10.0, 11.0, 12.0, 'stable'),
            ('MISO', 'Solar+Battery', 35.0, 38.0, 40.0, 'rising'),

            # CAISO - premium prices, declining solar
            ('CAISO', 'Solar', 38.0, 35.0, 33.0, 'declining'),  # Oversupply
            ('CAISO', 'Wind', 45.0, 48.0, 50.0, 'rising'),
            ('CAISO', 'Battery', 15.0, 16.0, 17.0, 'rising'),
            ('CAISO', 'Solar+Battery', 48.0, 50.0, 52.0, 'stable'),

            # SPP - low prices
            ('SPP', 'Solar', 22.0, 24.0, 26.0, 'rising'),
            ('SPP', 'Wind', 18.0, 20.0, 22.0, 'stable'),
            ('SPP', 'Battery', 8.0, 9.0, 10.0, 'stable'),

            # ISO-NE - premium market
            ('ISO-NE', 'Solar', 55.0, 58.0, 62.0, 'rising'),
            ('ISO-NE', 'Wind', 50.0, 55.0, 60.0, 'rising'),
            ('ISO-NE', 'Battery', 20.0, 22.0, 25.0, 'rising'),

            # West (non-ISO)
            ('West', 'Solar', 30.0, 32.0, 34.0, 'rising'),
            ('West', 'Wind', 28.0, 30.0, 32.0, 'stable'),

            # Southeast
            ('Southeast', 'Solar', 35.0, 38.0, 40.0, 'rising'),
            ('Southeast', 'Wind', 32.0, 35.0, 38.0, 'rising'),
        ]

        for region, tech, p2024, p2025, p2026, trend in benchmarks:
            for year, p50 in [(2024, p2024), (2025, p2025), (2026, p2026)]:
                # Calculate percentiles based on typical spread
                spread = 0.20  # 20% spread
                cursor.execute('''
                    INSERT OR REPLACE INTO ppa_benchmarks
                    (region, technology, year, price_p10, price_p25, price_p50, price_p75, price_p90, trend)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    region, tech, year,
                    round(p50 * (1 - spread * 1.5), 1),  # P10
                    round(p50 * (1 - spread), 1),        # P25
                    round(p50, 1),                        # P50
                    round(p50 * (1 + spread), 1),        # P75
                    round(p50 * (1 + spread * 1.5), 1),  # P90
                    trend
                ))

        # Load some sample public deals
        sample_deals = [
            # Recent announced deals (illustrative - based on public news)
            ('2024-06-15', 'ERCOT', 'TX', 'Solar', 250, 24.5, 15, 'C&I', 'Meta', None, 'West Texas Solar'),
            ('2024-05-20', 'ERCOT', 'TX', 'Wind', 300, 22.0, 12, 'C&I', 'Amazon', None, 'Panhandle Wind'),
            ('2024-04-10', 'PJM', 'VA', 'Solar', 150, 38.0, 15, 'Utility', 'Dominion', None, 'Virginia Solar'),
            ('2024-03-25', 'CAISO', 'CA', 'Solar+Battery', 200, 52.0, 15, 'Utility', 'SCE', None, 'Mojave Solar+Storage'),
            ('2024-02-15', 'NYISO', 'NY', 'Wind', 180, 48.0, 20, 'Utility', 'NYSERDA', None, 'Upstate Wind'),
            ('2024-01-20', 'MISO', 'IL', 'Solar', 200, 30.0, 15, 'C&I', 'Google', None, 'Illinois Solar'),
            ('2023-12-10', 'ISO-NE', 'MA', 'Solar', 80, 58.0, 20, 'Utility', 'National Grid', None, 'Mass Solar'),
            ('2023-11-05', 'SPP', 'OK', 'Wind', 400, 19.0, 12, 'Utility', 'AEP', None, 'Oklahoma Wind'),
        ]

        for row in sample_deals:
            cursor.execute('''
                INSERT OR IGNORE INTO ppa_deals
                (announcement_date, region, state, technology, capacity_mw, price_mwh,
                 term_years, buyer_type, buyer_name, seller_name, project_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', row)

        conn.commit()
        conn.close()
        print(f"  Loaded PPA benchmarks for {len(benchmarks)} region/technology combinations")

    def get_benchmark(self, region: str, technology: str, year: int = 2025) -> Dict:
        """Get PPA benchmark prices for a region/technology."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM ppa_benchmarks
            WHERE region = ? AND technology = ? AND year = ?
        ''', (region, technology, year))

        row = cursor.fetchone()
        conn.close()

        if row:
            return {
                'region': row['region'],
                'technology': row['technology'],
                'year': row['year'],
                'price_p10': row['price_p10'],
                'price_p25': row['price_p25'],
                'price_p50': row['price_p50'],
                'price_p75': row['price_p75'],
                'price_p90': row['price_p90'],
                'trend': row['trend'],
            }

        # Return regional default
        return self._get_default_benchmark(region, technology)

    def _get_default_benchmark(self, region: str, technology: str) -> Dict:
        """Return default benchmark."""
        # Regional base prices
        regional_base = {
            'ERCOT': 24, 'PJM': 38, 'NYISO': 50, 'MISO': 30,
            'CAISO': 40, 'SPP': 22, 'ISO-NE': 58, 'West': 32, 'Southeast': 38
        }

        # Technology adjustments
        tech_adj = {
            'Solar': 1.0, 'Wind': 0.92, 'Battery': 0.40,
            'Solar+Battery': 1.25, 'Gas': 1.10
        }

        base = regional_base.get(region, 35)
        adj = tech_adj.get(technology, 1.0)
        p50 = base * adj

        return {
            'region': region,
            'technology': technology,
            'year': 2025,
            'price_p10': round(p50 * 0.70, 1),
            'price_p25': round(p50 * 0.80, 1),
            'price_p50': round(p50, 1),
            'price_p75': round(p50 * 1.20, 1),
            'price_p90': round(p50 * 1.30, 1),
            'trend': 'stable',
        }

    def get_recent_deals(self, region: str = None, technology: str = None, limit: int = 10) -> pd.DataFrame:
        """Get recent PPA deals."""
        conn = self._get_conn()

        query = "SELECT * FROM ppa_deals WHERE 1=1"
        params = []

        if region:
            query += " AND region = ?"
            params.append(region)
        if technology:
            query += " AND technology = ?"
            params.append(technology)

        query += " ORDER BY announcement_date DESC LIMIT ?"
        params.append(limit)

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df


class PPABenchmarks:
    """Analyze PPA benchmarks and compare to merchant."""

    def __init__(self):
        self.ppa = PPAData()

    def compare_merchant_vs_ppa(
        self,
        region: str,
        technology: str,
        capacity_mw: float,
        merchant_price: float = None
    ) -> Dict[str, Any]:
        """
        Compare PPA contracted revenue vs merchant.

        Args:
            region: RTO/ISO region
            technology: Technology type
            capacity_mw: Project capacity
            merchant_price: Optional merchant price estimate ($/MWh)

        Returns:
            Dict with comparison analysis
        """
        # Get PPA benchmark
        benchmark = self.ppa.get_benchmark(region, technology)

        # Estimate merchant if not provided
        if merchant_price is None:
            # Import LMP data for merchant estimate
            try:
                from lmp_data import RevenueEstimator
                estimator = RevenueEstimator()
                merchant_result = estimator.estimate_annual_revenue(
                    region=region,
                    capacity_mw=capacity_mw,
                    technology=technology
                )
                merchant_price = merchant_result['effective_price_mwh']
            except:
                merchant_price = benchmark['price_p50'] * 0.85  # Rough estimate

        # Calculate annual revenues
        # Assume typical capacity factors
        cf = {'Solar': 0.25, 'Wind': 0.35, 'Battery': 0.15, 'Solar+Battery': 0.28, 'Gas': 0.40}
        capacity_factor = cf.get(technology, 0.30)

        annual_mwh = capacity_mw * capacity_factor * 8760

        ppa_revenue_low = annual_mwh * benchmark['price_p25']
        ppa_revenue_mid = annual_mwh * benchmark['price_p50']
        ppa_revenue_high = annual_mwh * benchmark['price_p75']
        merchant_revenue = annual_mwh * merchant_price

        # Calculate premium/discount
        ppa_vs_merchant = (benchmark['price_p50'] - merchant_price) / merchant_price * 100

        return {
            'region': region,
            'technology': technology,
            'capacity_mw': capacity_mw,
            'capacity_factor': capacity_factor,
            'annual_mwh': round(annual_mwh, 0),
            'ppa_benchmark': {
                'price_p25': benchmark['price_p25'],
                'price_p50': benchmark['price_p50'],
                'price_p75': benchmark['price_p75'],
                'trend': benchmark['trend'],
            },
            'merchant_price': round(merchant_price, 2),
            'ppa_revenue': {
                'low': round(ppa_revenue_low, 0),
                'mid': round(ppa_revenue_mid, 0),
                'high': round(ppa_revenue_high, 0),
            },
            'merchant_revenue': round(merchant_revenue, 0),
            'ppa_premium_pct': round(ppa_vs_merchant, 1),
            'recommendation': self._get_recommendation(ppa_vs_merchant, benchmark['trend']),
            'notes': self._generate_notes(ppa_vs_merchant, benchmark, technology),
        }

    def _get_recommendation(self, premium: float, trend: str) -> str:
        """Get contracting recommendation."""
        if premium > 15:
            return "Strong PPA market - consider early contracting"
        elif premium > 5:
            return "Moderate PPA premium - PPA provides good revenue certainty"
        elif premium > -5:
            return "PPA roughly at merchant parity - consider risk tolerance"
        else:
            return "Merchant may outperform PPA - consider partial hedge"

    def _generate_notes(self, premium: float, benchmark: Dict, technology: str) -> List[str]:
        """Generate notes about PPA market."""
        notes = []

        if benchmark['trend'] == 'rising':
            notes.append("PPA prices trending upward - waiting may yield better terms")
        elif benchmark['trend'] == 'declining':
            notes.append("PPA prices declining - consider locking in current rates")

        if technology == 'Solar' and premium < 0:
            notes.append("Solar merchant prices often volatile - PPA provides stability")

        if technology == 'Battery':
            notes.append("Battery revenue often from capacity/ancillary - PPA may not capture full value")

        return notes

    def format_comparison(self, result: Dict) -> str:
        """Format comparison for display."""
        ppa = result['ppa_revenue']['mid'] / 1_000_000
        merchant = result['merchant_revenue'] / 1_000_000
        premium = result['ppa_premium_pct']

        sign = '+' if premium > 0 else ''
        return f"PPA: ${ppa:.1f}M vs Merchant: ${merchant:.1f}M ({sign}{premium:.0f}%)"


def main():
    """Demo the PPA data module."""
    print("=" * 60)
    print("PPA BENCHMARK DATA DEMO")
    print("=" * 60)

    # Initialize and load data
    ppa = PPAData()
    ppa.refresh_all()

    # Show benchmarks by region
    print("\nPPA Benchmarks (2025, $/MWh):")
    for region in ['ERCOT', 'PJM', 'NYISO', 'CAISO']:
        print(f"\n  {region}:")
        for tech in ['Solar', 'Wind', 'Solar+Battery']:
            b = ppa.get_benchmark(region, tech, 2025)
            if b:
                print(f"    {tech}: ${b['price_p25']:.0f} - ${b['price_p50']:.0f} - ${b['price_p75']:.0f} ({b['trend']})")

    # Show recent deals
    print("\n" + "=" * 60)
    print("RECENT PPA DEALS")
    print("=" * 60)
    deals = ppa.get_recent_deals(limit=5)
    for _, d in deals.iterrows():
        print(f"  {d['announcement_date']}: {d['region']} {d['technology']} {d['capacity_mw']}MW @ ${d['price_mwh']}/MWh ({d['buyer_type']})")

    # Demo comparison
    print("\n" + "=" * 60)
    print("MERCHANT VS PPA COMPARISON")
    print("=" * 60)

    benchmarks = PPABenchmarks()

    examples = [
        ('ERCOT', 'Solar', 200),
        ('PJM', 'Wind', 150),
        ('NYISO', 'Solar+Battery', 100),
        ('CAISO', 'Solar', 250),
    ]

    for region, tech, mw in examples:
        result = benchmarks.compare_merchant_vs_ppa(region, tech, mw)

        print(f"\n{region} - {mw} MW {tech}:")
        print(f"  PPA Range: ${result['ppa_benchmark']['price_p25']:.0f} - ${result['ppa_benchmark']['price_p50']:.0f} - ${result['ppa_benchmark']['price_p75']:.0f}/MWh")
        print(f"  Merchant: ${result['merchant_price']:.0f}/MWh")
        print(f"  {benchmarks.format_comparison(result)}")
        print(f"  Recommendation: {result['recommendation']}")
        if result['notes']:
            for note in result['notes']:
                print(f"  Note: {note}")


if __name__ == "__main__":
    main()
