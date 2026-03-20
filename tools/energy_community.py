"""
Energy Community Eligibility Checker.

Checks if queue projects are located in IRA Energy Community zones,
qualifying them for a 10% bonus on ITC or PTC tax credits.

Two types of Energy Communities:
1. Coal Closure Communities - Census tracts with coal mine/plant closures
2. Statistical Area Communities - MSAs/non-MSAs with fossil fuel employment

Data source: DOE NETL IRA Energy Community Data Layers (2024)
https://zenodo.org/records/14757122
"""

import csv
import sqlite3
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
from collections import defaultdict


# Paths
CACHE_DIR = Path(__file__).parent / '.cache' / 'energy_communities'
COAL_CLOSURE_CSV = CACHE_DIR / 'Coal_Closures_EnergyComm_v2024_1' / 'IRA_EnergyComm_CTracts_CoalClosures_v2024_1.csv'
MSA_FFE_CSV = CACHE_DIR / 'MSA_NMSA_EC_FFE_v2024_1' / 'MSA_NonMSA_EnergyCommunities_FossilFuelEmp_v2024_1.csv'
V2_PATH = Path(__file__).parent / '.data' / 'master.db'

# State name to abbreviation mapping
STATE_ABBREV = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'District of Columbia': 'DC', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI',
    'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME',
    'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN',
    'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE',
    'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM',
    'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
    'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI',
    'South Carolina': 'SC', 'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX',
    'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA',
    'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY',
    'Puerto Rico': 'PR', 'Virgin Islands': 'VI', 'Guam': 'GU',
}


@dataclass
class EnergyCommunityResult:
    """Result of energy community eligibility check."""
    is_energy_community: bool
    coal_closure: bool = False
    mine_closure: bool = False
    generator_closure: bool = False
    adjacent_to_closure: bool = False
    ffe_qualified: bool = False  # Fossil fuel employment
    msa_area_name: Optional[str] = None
    county_name: Optional[str] = None
    state: Optional[str] = None


class EnergyCommunityChecker:
    """Check project locations against Energy Community zones."""

    def __init__(self):
        self.coal_closures = {}  # (state_abbrev, county_name) -> list of tract info
        self.ffe_counties = {}   # (state_abbrev, county_name) -> FFE info
        self.loaded = False

    def _normalize_county(self, county_name: str) -> str:
        """Normalize county name for matching."""
        if not county_name:
            return ''
        # Remove "County", "Parish", etc. suffixes and normalize case
        s = county_name.strip()
        for suffix in [' County', ' Parish', ' Borough', ' Census Area', ' Municipality']:
            if s.endswith(suffix):
                s = s[:-len(suffix)]
        return s.lower().strip()

    def load_data(self):
        """Load energy community data from CSV files."""
        print("Loading Energy Community data...")

        # Load coal closure data (census tract level)
        if COAL_CLOSURE_CSV.exists():
            with open(COAL_CLOSURE_CSV, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    state_name = row['State_Name']
                    county_raw = row['County_Name']

                    state_abbrev = STATE_ABBREV.get(state_name, state_name)
                    county_norm = self._normalize_county(county_raw)

                    key = (state_abbrev, county_norm)

                    # Only add if there's an actual closure or adjacent status
                    mine = row['Mine_Closure'] == 'Yes'
                    generator = row['Generator_Closure'] == 'Yes'
                    adjacent = row['Adjacent_to_Closure'] == 'Yes'

                    if mine or generator or adjacent:
                        if key not in self.coal_closures:
                            self.coal_closures[key] = []
                        self.coal_closures[key].append({
                            'tract_id': row['geoid_tract_2020'],
                            'mine_closure': mine,
                            'generator_closure': generator,
                            'adjacent': adjacent,
                        })

            print(f"  Loaded {len(self.coal_closures)} coal closure counties")

        # Load MSA/FFE data (county level)
        if MSA_FFE_CSV.exists():
            with open(MSA_FFE_CSV, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    state_name = row['State_Name']
                    county_raw = row['County_Name']

                    state_abbrev = STATE_ABBREV.get(state_name, state_name)
                    county_norm = self._normalize_county(county_raw)

                    key = (state_abbrev, county_norm)

                    # Only store if FFE or EC qualified
                    if row['ffe_qual_status'] == 'Yes' or row['ec_qual_status'] == 'Yes':
                        self.ffe_counties[key] = {
                            'ffe_qualified': row['ffe_qual_status'] == 'Yes',
                            'ec_qualified': row['ec_qual_status'] == 'Yes',
                            'msa_area_name': row['msa_nmsa_area_name'],
                            'msa_type': row['msa_nmsa'],
                        }

            print(f"  Loaded {len(self.ffe_counties)} FFE-qualified counties")

        self.loaded = True
        return len(self.coal_closures) + len(self.ffe_counties)

    def check_location(self, state: str, county: str) -> EnergyCommunityResult:
        """Check if a state/county location is in an Energy Community."""
        if not self.loaded:
            self.load_data()

        state_upper = state.upper() if state else ''
        county_norm = self._normalize_county(county)
        key = (state_upper, county_norm)

        result = EnergyCommunityResult(
            is_energy_community=False,
            state=state,
            county_name=county
        )

        # Check coal closures
        if key in self.coal_closures:
            tracts = self.coal_closures[key]
            result.coal_closure = True
            result.is_energy_community = True

            # Check if any tract has mine/generator closure
            for tract in tracts:
                if tract['mine_closure']:
                    result.mine_closure = True
                if tract['generator_closure']:
                    result.generator_closure = True
                if tract['adjacent']:
                    result.adjacent_to_closure = True

        # Check FFE status
        if key in self.ffe_counties:
            ffe_info = self.ffe_counties[key]
            if ffe_info['ec_qualified']:
                result.is_energy_community = True
                result.ffe_qualified = ffe_info['ffe_qualified']
                result.msa_area_name = ffe_info['msa_area_name']

        return result


def enrich_queue_with_energy_community(save: bool = True):
    """Add energy community eligibility to queue projects."""
    print("=" * 60)
    print("Energy Community Enrichment")
    print("=" * 60)

    checker = EnergyCommunityChecker()
    checker.load_data()

    if not V2_PATH.exists():
        raise FileNotFoundError(f"V2 database not found: {V2_PATH}")

    conn = sqlite3.connect(V2_PATH)
    conn.row_factory = sqlite3.Row

    # Get projects with locations
    query = """
        SELECT id, queue_id, state, county
        FROM projects
        WHERE state IS NOT NULL AND county IS NOT NULL
    """

    cursor = conn.execute(query)
    projects = cursor.fetchall()
    print(f"\nChecking {len(projects):,} projects with state/county data...")

    eligible_count = 0
    coal_count = 0
    ffe_count = 0

    for i, project in enumerate(projects):
        if (i + 1) % 5000 == 0:
            print(f"  Progress: {i+1:,}/{len(projects):,}")

        result = checker.check_location(project['state'], project['county'])

        if result.is_energy_community:
            eligible_count += 1

            ec_type = []
            if result.coal_closure:
                ec_type.append('coal_closure')
                coal_count += 1
            if result.ffe_qualified:
                ec_type.append('ffe')
                ffe_count += 1

            ec_type_str = ','.join(ec_type)

            if save:
                conn.execute("""
                    UPDATE projects
                    SET energy_community_eligible = 1,
                        energy_community_type = ?
                    WHERE id = ?
                """, (ec_type_str, project['id']))

    # Set non-eligible projects explicitly
    if save:
        conn.execute("""
            UPDATE projects
            SET energy_community_eligible = 0
            WHERE energy_community_eligible IS NULL
        """)
        conn.commit()

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"\nTotal projects checked: {len(projects):,}")
    print(f"Energy Community eligible: {eligible_count:,} ({100*eligible_count/len(projects):.1f}%)")
    print(f"  - Coal closure: {coal_count:,}")
    print(f"  - Fossil fuel employment: {ffe_count:,}")

    conn.close()
    return eligible_count


def get_energy_community_stats():
    """Get summary statistics of energy community eligible projects."""
    if not V2_PATH.exists():
        raise FileNotFoundError(f"V2 database not found: {V2_PATH}")

    conn = sqlite3.connect(V2_PATH)

    # Check if column exists
    cursor = conn.execute("PRAGMA table_info(projects)")
    columns = [row[1] for row in cursor.fetchall()]

    if 'energy_community_eligible' not in columns:
        print("Energy community data not yet enriched. Run enrich_queue_with_energy_community() first.")
        conn.close()
        return None

    # Get stats by region
    query = """
        SELECT
            region,
            COUNT(*) as total,
            SUM(CASE WHEN energy_community_eligible = 1 THEN 1 ELSE 0 END) as eligible,
            SUM(CASE WHEN energy_community_type LIKE '%coal%' THEN 1 ELSE 0 END) as coal,
            SUM(CASE WHEN energy_community_type LIKE '%ffe%' THEN 1 ELSE 0 END) as ffe
        FROM projects
        GROUP BY region
        ORDER BY total DESC
    """

    cursor = conn.execute(query)
    results = cursor.fetchall()

    print("\n" + "=" * 60)
    print("ENERGY COMMUNITY ELIGIBILITY BY REGION")
    print("=" * 60)
    print(f"\n{'Region':<12} {'Total':>10} {'Eligible':>10} {'Rate':>8} {'Coal':>8} {'FFE':>8}")
    print("-" * 60)

    total_all = 0
    eligible_all = 0

    for row in results:
        region, total, eligible, coal, ffe = row
        rate = 100 * eligible / total if total > 0 else 0
        print(f"{region:<12} {total:>10,} {eligible:>10,} {rate:>7.1f}% {coal:>8,} {ffe:>8,}")
        total_all += total
        eligible_all += eligible

    print("-" * 60)
    overall_rate = 100 * eligible_all / total_all if total_all > 0 else 0
    print(f"{'TOTAL':<12} {total_all:>10,} {eligible_all:>10,} {overall_rate:>7.1f}%")

    conn.close()
    return results


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Energy Community Eligibility Checker')
    parser.add_argument('--enrich', action='store_true',
                        help='Enrich queue database with energy community eligibility')
    parser.add_argument('--stats', action='store_true',
                        help='Show energy community statistics')
    parser.add_argument('--check', nargs=2, metavar=('STATE', 'COUNTY'),
                        help='Check if a specific location is in an energy community')
    parser.add_argument('--dry-run', action='store_true',
                        help='Do not save changes to database')

    args = parser.parse_args()

    if args.check:
        state, county = args.check
        checker = EnergyCommunityChecker()
        checker.load_data()
        result = checker.check_location(state, county)

        print(f"\nEnergy Community Check: {state}, {county}")
        print("-" * 40)
        print(f"Is Energy Community: {result.is_energy_community}")
        if result.coal_closure:
            print(f"  Coal Closure: Yes")
            print(f"    Mine Closure: {result.mine_closure}")
            print(f"    Generator Closure: {result.generator_closure}")
            print(f"    Adjacent to Closure: {result.adjacent_to_closure}")
        if result.ffe_qualified:
            print(f"  FFE Qualified: Yes")
            print(f"    MSA Area: {result.msa_area_name}")

    elif args.enrich:
        enrich_queue_with_energy_community(save=not args.dry_run)

    elif args.stats:
        get_energy_community_stats()

    else:
        parser.print_help()
