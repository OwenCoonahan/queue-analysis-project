#!/usr/bin/env python3
"""
Transmission Constraint Data Module

Provides transmission congestion data and constraint analysis.

Data Includes:
- Historical congestion costs by zone
- Known transmission constraints and flowgates
- Upgrade project status
- POI-level risk assessment

Usage:
    from transmission_data import TransmissionData, ConstraintAnalysis

    tx = TransmissionData()

    # Get congestion info for a zone
    congestion = tx.get_zone_congestion('ERCOT', 'WEST')

    # Analyze constraints near a POI
    analysis = ConstraintAnalysis()
    risk = analysis.assess_poi_risk(region='PJM', poi='Example Substation 345kV')
"""

import pandas as pd
import numpy as np
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List

DATA_DIR = Path(__file__).parent / '.data'
DB_PATH = DATA_DIR / 'queue.db'


class TransmissionData:
    """Load and manage transmission constraint data."""

    def __init__(self):
        self.db_path = DB_PATH
        self._init_tables()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_tables(self):
        """Create transmission tables."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Transmission zones/areas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tx_zones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT NOT NULL,
                zone_id TEXT NOT NULL,
                zone_name TEXT,
                zone_type TEXT,
                congestion_level TEXT,
                UNIQUE(region, zone_id)
            )
        ''')

        # Zone-level congestion statistics
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tx_congestion (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT NOT NULL,
                zone_id TEXT NOT NULL,
                year INTEGER NOT NULL,
                avg_congestion_cost REAL,
                max_congestion_cost REAL,
                congested_hours INTEGER,
                pct_hours_congested REAL,
                total_congestion_cost_m REAL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(region, zone_id, year)
            )
        ''')

        # Known constraints/flowgates
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tx_constraints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT NOT NULL,
                constraint_name TEXT NOT NULL,
                constraint_type TEXT,
                from_area TEXT,
                to_area TEXT,
                limit_mw REAL,
                binding_hours INTEGER,
                shadow_price_avg REAL,
                status TEXT,
                notes TEXT,
                UNIQUE(region, constraint_name)
            )
        ''')

        # Planned upgrades
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tx_upgrades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT NOT NULL,
                project_name TEXT NOT NULL,
                project_type TEXT,
                affected_zones TEXT,
                capacity_increase_mw REAL,
                estimated_cost_m REAL,
                target_cod TEXT,
                status TEXT,
                notes TEXT,
                UNIQUE(region, project_name)
            )
        ''')

        conn.commit()
        conn.close()

    def refresh_all(self):
        """Refresh transmission data."""
        print("Refreshing transmission constraint data...")
        self._load_benchmark_data()
        print("Transmission data refresh complete.")

    def _load_benchmark_data(self):
        """Load benchmark transmission data."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Zone-level congestion data
        # Format: (region, zone_id, zone_name, congestion_level, avg_cost, congested_hours_pct)
        zone_data = [
            # ERCOT - significant congestion in West
            ('ERCOT', 'WEST', 'West Texas', 'high', 8.50, 0.18),
            ('ERCOT', 'PANHANDLE', 'Panhandle', 'very_high', 12.30, 0.25),
            ('ERCOT', 'NORTH', 'North Texas', 'medium', 3.20, 0.08),
            ('ERCOT', 'SOUTH', 'South Texas', 'medium', 4.10, 0.10),
            ('ERCOT', 'HOUSTON', 'Houston', 'low', 1.80, 0.04),
            ('ERCOT', 'COAST', 'Coastal', 'low', 2.10, 0.05),

            # PJM - congestion varies by LDA
            ('PJM', 'WEST', 'Western PJM', 'low', 1.50, 0.03),
            ('PJM', 'AEP', 'AEP Zone', 'low', 1.80, 0.04),
            ('PJM', 'COMED', 'ComEd Zone', 'low', 2.10, 0.05),
            ('PJM', 'EMAAC', 'Eastern MAAC', 'medium', 4.50, 0.10),
            ('PJM', 'SWMAAC', 'SW MAAC', 'medium', 3.80, 0.08),
            ('PJM', 'DOM', 'Dominion', 'medium', 3.20, 0.07),
            ('PJM', 'DPL', 'Delmarva', 'high', 6.20, 0.14),

            # NYISO - significant import constraints
            ('NYISO', 'WEST', 'Western NY', 'medium', 3.50, 0.08),
            ('NYISO', 'CENTRAL', 'Central NY', 'medium', 4.20, 0.09),
            ('NYISO', 'HUDSON', 'Hudson Valley', 'high', 8.50, 0.18),
            ('NYISO', 'NYC', 'New York City', 'very_high', 15.20, 0.28),
            ('NYISO', 'LI', 'Long Island', 'high', 12.80, 0.22),

            # MISO - congestion at seams
            ('MISO', 'NORTH', 'MISO North', 'medium', 3.80, 0.08),
            ('MISO', 'CENTRAL', 'MISO Central', 'medium', 2.90, 0.06),
            ('MISO', 'SOUTH', 'MISO South', 'high', 6.50, 0.14),
            ('MISO', 'WEST', 'MISO West', 'medium', 4.20, 0.09),

            # CAISO - significant renewable curtailment
            ('CAISO', 'SP15', 'Southern CA', 'high', 7.80, 0.16),
            ('CAISO', 'NP15', 'Northern CA', 'medium', 5.20, 0.11),
            ('CAISO', 'ZP26', 'Central CA', 'very_high', 11.50, 0.22),

            # SPP - wind-rich areas congested
            ('SPP', 'NORTH', 'SPP North', 'high', 6.80, 0.15),
            ('SPP', 'SOUTH', 'SPP South', 'medium', 3.50, 0.08),
            ('SPP', 'WEST', 'SPP West', 'very_high', 9.80, 0.20),

            # ISO-NE - import constrained
            ('ISO-NE', 'MAINE', 'Maine', 'high', 7.20, 0.15),
            ('ISO-NE', 'NH', 'New Hampshire', 'medium', 4.50, 0.10),
            ('ISO-NE', 'SEMA', 'SE Massachusetts', 'high', 6.80, 0.14),
            ('ISO-NE', 'BOSTON', 'Boston', 'medium', 5.20, 0.11),
        ]

        for region, zone_id, zone_name, level, avg_cost, congested_pct in zone_data:
            cursor.execute('''
                INSERT OR REPLACE INTO tx_zones
                (region, zone_id, zone_name, zone_type, congestion_level)
                VALUES (?, ?, ?, 'zone', ?)
            ''', (region, zone_id, zone_name, level))

            # Add congestion stats for 2023-2024
            for year in [2023, 2024]:
                year_factor = 1.0 if year == 2024 else 0.95
                cursor.execute('''
                    INSERT OR REPLACE INTO tx_congestion
                    (region, zone_id, year, avg_congestion_cost, congested_hours,
                     pct_hours_congested, total_congestion_cost_m)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    region, zone_id, year,
                    avg_cost * year_factor,
                    int(8760 * congested_pct),
                    congested_pct,
                    avg_cost * congested_pct * 1000 * year_factor  # Rough total
                ))

        # Known major constraints
        constraints = [
            # ERCOT
            ('ERCOT', 'West Texas Export', 'interface', 'WEST', 'NORTH', 8000, 2100, 25.50, 'active'),
            ('ERCOT', 'Panhandle Export', 'interface', 'PANHANDLE', 'NORTH', 4500, 1800, 35.20, 'active'),
            ('ERCOT', 'Houston Import', 'interface', 'COAST', 'HOUSTON', 12000, 800, 15.80, 'active'),

            # PJM
            ('PJM', 'AP South Interface', 'interface', 'AP', 'DOM', 5500, 600, 12.50, 'active'),
            ('PJM', 'COMED-PJM Interface', 'interface', 'COMED', 'EMAAC', 3200, 400, 8.20, 'active'),
            ('PJM', 'Delmarva Export', 'interface', 'DPL', 'EMAAC', 1800, 900, 18.50, 'active'),

            # NYISO
            ('NYISO', 'Central-East Interface', 'interface', 'CENTRAL', 'HUDSON', 5650, 1500, 22.30, 'active'),
            ('NYISO', 'UPNY-SENY Interface', 'interface', 'HUDSON', 'NYC', 5150, 2000, 35.80, 'active'),
            ('NYISO', 'Long Island Cable', 'interface', 'NYC', 'LI', 1800, 1200, 28.50, 'active'),

            # CAISO
            ('CAISO', 'Path 15', 'interface', 'NP15', 'SP15', 5400, 1100, 18.90, 'active'),
            ('CAISO', 'Path 26', 'interface', 'ZP26', 'SP15', 4000, 1400, 24.50, 'active'),

            # MISO
            ('MISO', 'MISO-SPP Interface', 'interface', 'MISO', 'SPP', 2500, 800, 12.30, 'active'),
            ('MISO', 'MISO South Import', 'interface', 'CENTRAL', 'SOUTH', 3000, 1000, 15.80, 'active'),
        ]

        for region, name, ctype, from_a, to_a, limit, hours, price, status in constraints:
            cursor.execute('''
                INSERT OR REPLACE INTO tx_constraints
                (region, constraint_name, constraint_type, from_area, to_area,
                 limit_mw, binding_hours, shadow_price_avg, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (region, name, ctype, from_a, to_a, limit, hours, price, status))

        # Planned upgrades
        upgrades = [
            ('ERCOT', 'West Texas Export Expansion', 'interface', 'WEST,NORTH', 3000, 2500, '2026', 'under_construction'),
            ('ERCOT', 'Panhandle-South Plains', 'new_line', 'PANHANDLE', 1500, 800, '2027', 'approved'),
            ('PJM', 'Artificial Island Project', 'reliability', 'DPL,EMAAC', 500, 400, '2025', 'under_construction'),
            ('NYISO', 'AC Transmission Upgrade', 'interface', 'CENTRAL,HUDSON', 1000, 1200, '2026', 'approved'),
            ('NYISO', 'Champlain Hudson Power Express', 'hvdc', 'QUEBEC,NYC', 1250, 4000, '2026', 'under_construction'),
            ('CAISO', 'Tehachapi Expansion', 'new_line', 'SP15', 1500, 1800, '2027', 'planned'),
        ]

        for region, name, ptype, zones, cap, cost, cod, status in upgrades:
            cursor.execute('''
                INSERT OR REPLACE INTO tx_upgrades
                (region, project_name, project_type, affected_zones,
                 capacity_increase_mw, estimated_cost_m, target_cod, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (region, name, ptype, zones, cap, cost, cod, status))

        conn.commit()
        conn.close()
        print(f"  Loaded transmission data for {len(zone_data)} zones")

    def get_zone_congestion(self, region: str, zone_id: str = None) -> Dict:
        """Get congestion data for a zone."""
        conn = self._get_conn()
        cursor = conn.cursor()

        if zone_id:
            cursor.execute('''
                SELECT z.*, c.avg_congestion_cost, c.pct_hours_congested
                FROM tx_zones z
                LEFT JOIN tx_congestion c ON z.region = c.region AND z.zone_id = c.zone_id AND c.year = 2024
                WHERE z.region = ? AND z.zone_id = ?
            ''', (region, zone_id))
        else:
            # Return region average
            cursor.execute('''
                SELECT z.region, 'AVERAGE' as zone_id, 'Regional Average' as zone_name,
                       AVG(c.avg_congestion_cost) as avg_congestion_cost,
                       AVG(c.pct_hours_congested) as pct_hours_congested
                FROM tx_zones z
                LEFT JOIN tx_congestion c ON z.region = c.region AND z.zone_id = c.zone_id AND c.year = 2024
                WHERE z.region = ?
                GROUP BY z.region
            ''', (region,))

        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)

        return self._get_default_congestion(region)

    def _get_default_congestion(self, region: str) -> Dict:
        """Return default congestion data."""
        defaults = {
            'ERCOT': ('medium', 5.0, 0.12),
            'PJM': ('low', 3.0, 0.06),
            'NYISO': ('high', 8.0, 0.15),
            'MISO': ('medium', 4.0, 0.09),
            'CAISO': ('high', 8.0, 0.16),
            'SPP': ('medium', 5.5, 0.12),
            'ISO-NE': ('medium', 5.5, 0.12),
        }

        level, cost, pct = defaults.get(region, ('medium', 4.0, 0.10))

        return {
            'region': region,
            'zone_id': 'DEFAULT',
            'zone_name': f'{region} Average',
            'congestion_level': level,
            'avg_congestion_cost': cost,
            'pct_hours_congested': pct,
        }

    def get_constraints(self, region: str = None) -> pd.DataFrame:
        """Get known constraints."""
        conn = self._get_conn()
        query = "SELECT * FROM tx_constraints"
        if region:
            query += f" WHERE region = '{region}'"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def get_upgrades(self, region: str = None) -> pd.DataFrame:
        """Get planned upgrades."""
        conn = self._get_conn()
        query = "SELECT * FROM tx_upgrades"
        if region:
            query += f" WHERE region = '{region}'"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df


class ConstraintAnalysis:
    """Analyze transmission constraints for projects."""

    def __init__(self):
        self.tx = TransmissionData()

    def assess_poi_risk(
        self,
        region: str,
        poi: str = None,
        state: str = None,
        zone_id: str = None
    ) -> Dict[str, Any]:
        """
        Assess transmission constraint risk for a POI.

        Args:
            region: RTO/ISO region
            poi: Point of interconnection name
            state: State (for zone mapping)
            zone_id: Optional specific zone

        Returns:
            Dict with risk assessment
        """
        # Map POI to zone if not specified
        if not zone_id:
            zone_id = self._map_poi_to_zone(region, poi, state)

        # Get congestion data
        congestion = self.tx.get_zone_congestion(region, zone_id)

        # Determine risk level
        level = congestion.get('congestion_level', 'medium')
        if level == 'very_high':
            risk_score = 0.85
            risk_rating = 'HIGH'
            risk_color = 'red'
        elif level == 'high':
            risk_score = 0.65
            risk_rating = 'ELEVATED'
            risk_color = 'orange'
        elif level == 'medium':
            risk_score = 0.40
            risk_rating = 'MODERATE'
            risk_color = 'yellow'
        else:
            risk_score = 0.20
            risk_rating = 'LOW'
            risk_color = 'green'

        # Get relevant constraints
        constraints = self.tx.get_constraints(region)
        relevant_constraints = constraints[
            constraints['from_area'].str.contains(zone_id, case=False, na=False) |
            constraints['to_area'].str.contains(zone_id, case=False, na=False)
        ]

        # Get relevant upgrades
        upgrades = self.tx.get_upgrades(region)
        relevant_upgrades = upgrades[
            upgrades['affected_zones'].str.contains(zone_id, case=False, na=False)
        ]

        # Calculate cost impact
        avg_congestion = congestion.get('avg_congestion_cost', 0) or 0
        pct_congested = congestion.get('pct_hours_congested', 0) or 0
        annual_congestion_impact = avg_congestion * pct_congested * 8760

        return {
            'region': region,
            'zone_id': zone_id,
            'zone_name': congestion.get('zone_name', zone_id),
            'risk_score': risk_score,
            'risk_rating': risk_rating,
            'risk_color': risk_color,
            'congestion_level': level,
            'avg_congestion_cost': round(avg_congestion, 2),
            'pct_hours_congested': round(pct_congested * 100, 1),
            'annual_congestion_impact_kwh': round(annual_congestion_impact, 2),
            'relevant_constraints': len(relevant_constraints),
            'planned_upgrades': len(relevant_upgrades),
            'constraints': relevant_constraints[['constraint_name', 'limit_mw', 'binding_hours']].to_dict('records') if not relevant_constraints.empty else [],
            'upgrades': relevant_upgrades[['project_name', 'target_cod', 'status']].to_dict('records') if not relevant_upgrades.empty else [],
            'notes': self._generate_notes(congestion, relevant_constraints, relevant_upgrades),
        }

    def _map_poi_to_zone(self, region: str, poi: str, state: str = None) -> str:
        """Map POI to transmission zone."""
        poi_lower = (poi or '').lower()
        state_upper = (state or '').upper()

        # ERCOT zone mapping
        if region == 'ERCOT':
            if any(x in poi_lower for x in ['panhandle', 'amarillo', 'lubbock']):
                return 'PANHANDLE'
            elif any(x in poi_lower for x in ['houston', 'harris', 'galveston']):
                return 'HOUSTON'
            elif any(x in poi_lower for x in ['dallas', 'fort worth', 'denton']):
                return 'NORTH'
            elif any(x in poi_lower for x in ['corpus', 'brownsville', 'mcallen']):
                return 'SOUTH'
            elif any(x in poi_lower for x in ['midland', 'odessa', 'permian', 'pecos']):
                return 'WEST'
            else:
                return 'WEST'  # Default to West (most congested)

        # NYISO
        elif region == 'NYISO':
            if any(x in poi_lower for x in ['nyc', 'new york city', 'brooklyn', 'queens']):
                return 'NYC'
            elif any(x in poi_lower for x in ['long island', 'nassau', 'suffolk']):
                return 'LI'
            elif any(x in poi_lower for x in ['hudson', 'westchester', 'rockland']):
                return 'HUDSON'
            elif any(x in poi_lower for x in ['buffalo', 'rochester', 'niagara']):
                return 'WEST'
            else:
                return 'CENTRAL'

        # PJM
        elif region == 'PJM':
            if state_upper in ['DE', 'MD'] or 'delmarva' in poi_lower:
                return 'DPL'
            elif state_upper in ['VA', 'NC'] or 'dominion' in poi_lower:
                return 'DOM'
            elif state_upper in ['IL'] or 'comed' in poi_lower:
                return 'COMED'
            elif state_upper in ['OH', 'WV', 'KY'] or 'aep' in poi_lower:
                return 'AEP'
            elif 'emaac' in poi_lower or state_upper in ['NJ', 'PA']:
                return 'EMAAC'
            else:
                return 'WEST'

        # CAISO
        elif region == 'CAISO':
            if any(x in poi_lower for x in ['san diego', 'imperial', 'riverside']):
                return 'SP15'
            elif any(x in poi_lower for x in ['san francisco', 'oakland', 'sacramento']):
                return 'NP15'
            else:
                return 'ZP26'

        # Default
        return 'CENTRAL'

    def _generate_notes(self, congestion: Dict, constraints: pd.DataFrame, upgrades: pd.DataFrame) -> List[str]:
        """Generate notes about transmission risk."""
        notes = []

        level = congestion.get('congestion_level', 'medium')

        if level == 'very_high':
            notes.append("Very high congestion zone - expect significant curtailment risk and potential upgrade costs")
        elif level == 'high':
            notes.append("Elevated congestion - may face interconnection delays or upgrade requirements")

        if not constraints.empty:
            top_constraint = constraints.iloc[0]
            notes.append(f"Key constraint: {top_constraint['constraint_name']} ({top_constraint['binding_hours']} binding hours/year)")

        if not upgrades.empty:
            active_upgrades = upgrades[upgrades['status'].isin(['under_construction', 'approved'])]
            if not active_upgrades.empty:
                notes.append(f"Relief expected: {len(active_upgrades)} transmission upgrade(s) in progress")

        pct = congestion.get('pct_hours_congested', 0) or 0
        if pct > 0.15:
            notes.append(f"Congested {pct*100:.0f}% of hours - factor into revenue projections")

        return notes

    def format_risk(self, result: Dict) -> str:
        """Format risk assessment for display."""
        return f"{result['risk_rating']} ({result['pct_hours_congested']:.0f}% congested hours)"


def main():
    """Demo the transmission data module."""
    print("=" * 60)
    print("TRANSMISSION CONSTRAINT DATA DEMO")
    print("=" * 60)

    # Initialize and load data
    tx = TransmissionData()
    tx.refresh_all()

    # Show congestion by region
    print("\nCongestion Levels by Zone:")
    for region in ['ERCOT', 'PJM', 'NYISO', 'CAISO']:
        print(f"\n  {region}:")
        conn = tx._get_conn()
        zones = pd.read_sql_query(f"""
            SELECT z.zone_id, z.zone_name, z.congestion_level,
                   c.avg_congestion_cost, c.pct_hours_congested
            FROM tx_zones z
            LEFT JOIN tx_congestion c ON z.region = c.region AND z.zone_id = c.zone_id AND c.year = 2024
            WHERE z.region = '{region}'
            ORDER BY c.avg_congestion_cost DESC
        """, conn)
        conn.close()

        for _, z in zones.iterrows():
            pct = z['pct_hours_congested'] or 0
            print(f"    {z['zone_id']}: {z['congestion_level']} (${z['avg_congestion_cost']:.1f}/MWh, {pct*100:.0f}% congested)")

    # Demo risk assessment
    print("\n" + "=" * 60)
    print("POI RISK ASSESSMENT EXAMPLES")
    print("=" * 60)

    analysis = ConstraintAnalysis()

    examples = [
        ('ERCOT', 'Midland 345kV', 'TX'),
        ('ERCOT', 'Houston North 138kV', 'TX'),
        ('PJM', 'Dominion Virginia 500kV', 'VA'),
        ('NYISO', 'NYC Substation', 'NY'),
        ('CAISO', 'Imperial Valley 230kV', 'CA'),
    ]

    for region, poi, state in examples:
        result = analysis.assess_poi_risk(region=region, poi=poi, state=state)

        print(f"\n{region} - {poi}:")
        print(f"  Zone: {result['zone_name']}")
        print(f"  Risk: {analysis.format_risk(result)}")
        print(f"  Congestion cost: ${result['avg_congestion_cost']}/MWh")
        if result['relevant_constraints'] > 0:
            print(f"  Known constraints: {result['relevant_constraints']}")
        if result['planned_upgrades'] > 0:
            print(f"  Planned upgrades: {result['planned_upgrades']}")
        if result['notes']:
            for note in result['notes']:
                print(f"  Note: {note}")


if __name__ == "__main__":
    main()
