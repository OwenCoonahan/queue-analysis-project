#!/usr/bin/env python3
"""
Extract lat/lon coordinates for projects in master.db.

Geocoding sources (applied in order of precision):
1. County centroids — US Census Bureau Gazetteer (state + county → centroid lat/lon)
   Covers ~42K projects with state+county fields.
   Source: https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/

NOTE: grid.db substations table lacks lat/lon columns, so substation-based
geocoding is not yet possible. When substation coords are added to grid.db,
a --substations mode can be implemented to match project POI → substation coords.

Usage:
    python3 extract_coordinates.py --add-columns    # Add lat/lon columns to master.db
    python3 extract_coordinates.py --geocode         # Run county centroid geocoding
    python3 extract_coordinates.py --stats           # Show coordinate coverage
    python3 extract_coordinates.py --audit           # Scan raw_data for any coordinate data
"""

import argparse
import csv
import io
import json
import os
import re
import sqlite3
import sys
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

# Paths — consistent with existing Queue Analysis conventions
TOOLS_DIR = Path(__file__).parent
DATA_DIR = TOOLS_DIR / '.data'
CACHE_DIR = TOOLS_DIR / '.cache'
DB_PATH = Path(os.environ.get('QUEUE_DB_PATH', str(DATA_DIR / 'master.db')))
GRID_DB_PATH = DATA_DIR / 'grid.db'

GAZETTEER_URL = 'https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_counties_national.zip'
GAZETTEER_CACHE_DIR = CACHE_DIR / 'county_centroids'
GAZETTEER_ZIP = GAZETTEER_CACHE_DIR / '2024_Gaz_counties_national.zip'


# =============================================================================
# County name normalization
# =============================================================================

def normalize_county(name: str) -> str:
    """Normalize county name for fuzzy matching.

    Handles: case, 'County'/'Parish'/'Borough' suffix, St./Saint,
    'City of X' prefix, punctuation, whitespace.
    """
    if not name:
        return ''
    s = name.strip().lower()
    # Remove "city of" prefix (Virginia independent cities: "City of Chesapeake" -> "chesapeake")
    s = re.sub(r'^city of\s+', '', s)
    # Remove common suffixes
    for suffix in [' county', ' parish', ' borough', ' census area',
                   ' municipality', ' city and borough', ' city',
                   ' planning region', ' cty', ' co']:
        if s.endswith(suffix):
            s = s[:-len(suffix)].strip()
    # Normalize Saint variants
    s = re.sub(r'\bst\.?\s', 'saint ', s)
    s = re.sub(r'\bste\.?\s', 'sainte ', s)
    # Remove punctuation except spaces
    s = re.sub(r"[''.\-]", '', s)
    # Collapse whitespace
    s = re.sub(r'\s+', ' ', s).strip()
    return s


# Connecticut county-to-planning-region mapping (CT abolished counties in 1960;
# Census 2024 gazetteer uses planning regions instead)
CT_COUNTY_TO_REGION = {
    'hartford': 'capitol',
    'tolland': 'capitol',
    'new haven': 'south central connecticut',
    'fairfield': 'western connecticut',
    'new london': 'southeastern connecticut',
    'windham': 'northeastern connecticut',
    'middlesex': 'lower connecticut river valley',
    'litchfield': 'northwest hills',
}

# Virginia independent cities → county/city mapping for gazetteer
VA_INDEPENDENT_CITIES = {
    'chesapeake': 'chesapeake',
    'suffolk': 'suffolk',
    'virginia beach': 'virginia beach',
    'hopewell': 'hopewell',
    'hampton': 'hampton',
    'newport news': 'newport news',
    'norfolk': 'norfolk',
    'portsmouth': 'portsmouth',
    'richmond': 'richmond',
    'alexandria': 'alexandria',
    'fredericksburg': 'fredericksburg',
    'lynchburg': 'lynchburg',
    'roanoke': 'roanoke',
    'danville': 'danville',
}

# Common misspellings and alternate names
COUNTY_SPELLING_FIXES = {
    'northhampton': 'northampton',
    'la salle': 'lasalle',
    'de kalb': 'dekalb',
    'de witt': 'dewitt',
    'du page': 'dupage',
    'brooklyn': 'kings',
    'manhattan': 'new york',
    'staten island': 'richmond',
    'the bronx': 'bronx',
    'pittsburgh': 'pittsburg',       # OK has Pittsburg County (no h)
    'tyrell': 'tyrrell',             # NC Tyrrell County
}

# City-to-county mappings for common city-as-county errors
CITY_TO_COUNTY = {
    ('NE', 'plattsmouth'): ('NE', 'cass'),
    ('NE', 'omaha'): ('NE', 'douglas'),
    ('NE', 'lincoln'): ('NE', 'lancaster'),
    ('ME', 'lewiston'): ('ME', 'androscoggin'),
    ('TX', 'denver'): ('TX', 'yoakum'),    # Denver City, TX -> Yoakum County
    ('OK', 'apache'): ('OK', 'caddo'),
    ('NY', 'ny'): ('NY', 'new york'),      # NYC -> New York County (Manhattan)
    ('RI', 'north kingstown'): ('RI', 'washington'),
    ('ME', 'augusta'): ('ME', 'kennebec'),
}

# State name to abbreviation (for non-standard state values)
STATE_NAME_TO_ABBR = {
    'michigan': 'MI',
}


# =============================================================================
# Gazetteer download & parse
# =============================================================================

def download_gazetteer() -> Path:
    """Download Census Bureau county gazetteer if not cached."""
    GAZETTEER_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Check for already-extracted txt file
    txt_files = list(GAZETTEER_CACHE_DIR.glob('*.txt'))
    if txt_files:
        print(f"  Using cached gazetteer: {txt_files[0].name}")
        return txt_files[0]

    if not GAZETTEER_ZIP.exists():
        print(f"  Downloading county gazetteer from Census Bureau...")
        urlretrieve(GAZETTEER_URL, GAZETTEER_ZIP)
        print(f"  Downloaded: {GAZETTEER_ZIP.name}")

    # Extract
    with zipfile.ZipFile(GAZETTEER_ZIP, 'r') as zf:
        txt_names = [n for n in zf.namelist() if n.endswith('.txt')]
        if not txt_names:
            raise RuntimeError("No .txt file found in gazetteer ZIP")
        zf.extract(txt_names[0], GAZETTEER_CACHE_DIR)
        print(f"  Extracted: {txt_names[0]}")
        return GAZETTEER_CACHE_DIR / txt_names[0]


def load_centroids(gazetteer_path: Path) -> dict:
    """Load county centroids from gazetteer file.

    Returns dict: (state_abbr, normalized_county) -> (lat, lon)
    """
    centroids = {}
    with open(gazetteer_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        header = next(reader)
        # Strip whitespace from headers
        header = [h.strip() for h in header]

        # Find column indices
        try:
            usps_idx = header.index('USPS')
            name_idx = header.index('NAME')
            lat_idx = header.index('INTPTLAT')
            lon_idx = header.index('INTPTLONG')
        except ValueError as e:
            # Try alternative column names
            print(f"  Header columns: {header}")
            raise RuntimeError(f"Missing expected gazetteer column: {e}")

        for row in reader:
            if len(row) <= max(usps_idx, name_idx, lat_idx, lon_idx):
                continue
            state = row[usps_idx].strip()
            county_raw = row[name_idx].strip()
            try:
                lat = float(row[lat_idx].strip())
                lon = float(row[lon_idx].strip())
            except (ValueError, IndexError):
                continue

            key = (state, normalize_county(county_raw))
            centroids[key] = (lat, lon)

    print(f"  Loaded {len(centroids):,} county centroids from gazetteer")
    return centroids


# =============================================================================
# Geocoding
# =============================================================================

def _resolve_centroid(state, norm_county, raw_county, centroids):
    """Try multiple strategies to resolve a county to centroid coords.

    Returns (lat, lon) or None.
    """
    # Skip obvious non-county values
    if norm_county in ('na', 'unknown', '', 'various', 'multiple', 'tbd', 'n/a',
                        'baja california', 'sonora', 'chihuahua', 'tamaulipas'):
        return None

    # Strategy 1: Direct match
    coords = centroids.get((state, norm_county))
    if coords:
        return coords

    # Strategy 2: Connecticut planning region lookup
    if state == 'CT' and norm_county in CT_COUNTY_TO_REGION:
        region = CT_COUNTY_TO_REGION[norm_county]
        coords = centroids.get(('CT', region))
        if coords:
            return coords

    # Strategy 3: Virginia independent cities (gazetteer lists them separately)
    if state == 'VA' and norm_county in VA_INDEPENDENT_CITIES:
        city = VA_INDEPENDENT_CITIES[norm_county]
        coords = centroids.get(('VA', city))
        if coords:
            return coords

    # Strategy 4: Spelling fixes
    if norm_county in COUNTY_SPELLING_FIXES:
        fixed = COUNTY_SPELLING_FIXES[norm_county]
        coords = centroids.get((state, fixed))
        if coords:
            return coords

    # Strategy 5: Cross-state county (county belongs to neighboring state)
    # e.g., CA/CLARK -> NV/Clark, CA/MARICOPA -> AZ/Maricopa, OR/Klickitat -> WA
    cross_state_map = {
        'CA': ['NV', 'AZ', 'OR', 'UT', 'WA'],
        'OR': ['WA', 'CA', 'ID', 'NV'],
        'WA': ['OR', 'ID'],
        'NM': ['TX', 'AZ'],
    }
    if state in cross_state_map:
        for try_state in cross_state_map[state]:
            coords = centroids.get((try_state, norm_county))
            if coords:
                return coords

    # Strategy 6: Multi-county (take first county from "X-Y" or "X, Y")
    if '-' in raw_county or ',' in raw_county:
        parts = re.split(r'[-,]', raw_county)
        first = normalize_county(parts[0].strip())
        coords = centroids.get((state, first))
        if coords:
            return coords

    # Strategy 7: City-to-county explicit mapping
    city_key = (state, norm_county)
    if city_key in CITY_TO_COUNTY:
        mapped_state, mapped_county = CITY_TO_COUNTY[city_key]
        coords = centroids.get((mapped_state, mapped_county))
        if coords:
            return coords

    # Strategy 8: Multi-county with slash separator ("Morrow / Umatilla")
    if '/' in raw_county:
        parts = raw_county.split('/')
        first = normalize_county(parts[0].strip())
        coords = centroids.get((state, first))
        if coords:
            return coords

    # Strategy 9: Try with/without diacritics (Doña Ana ↔ Dona Ana, etc.)
    import unicodedata
    stripped = unicodedata.normalize('NFD', norm_county)
    stripped = ''.join(c for c in stripped if unicodedata.category(c) != 'Mn')
    if stripped != norm_county:
        coords = centroids.get((state, stripped))
        if coords:
            return coords
    # Also try: if our input lacks diacritics, search all centroids for this state
    for (cs, cc), cv in centroids.items():
        if cs != state:
            continue
        cc_stripped = unicodedata.normalize('NFD', cc)
        cc_stripped = ''.join(c for c in cc_stripped if unicodedata.category(c) != 'Mn')
        if cc_stripped == norm_county:
            return cv

    return None


def geocode_counties():
    """Geocode projects using county centroids from Census Bureau gazetteer."""
    print("=" * 60)
    print("COUNTY CENTROID GEOCODING")
    print("=" * 60)

    # Step 1: Download/load gazetteer
    print("\n[1/3] Loading county centroid data...")
    gazetteer_path = download_gazetteer()
    centroids = load_centroids(gazetteer_path)

    # Step 2: Load projects needing coordinates
    print("\n[2/3] Loading projects needing coordinates...")
    conn = sqlite3.connect(DB_PATH)

    # Ensure columns exist
    cursor = conn.execute("PRAGMA table_info(projects)")
    cols = {row[1] for row in cursor.fetchall()}
    for col in ['latitude', 'longitude']:
        if col not in cols:
            conn.execute(f"ALTER TABLE projects ADD COLUMN {col} REAL")
            print(f"  Added column: {col}")
    conn.commit()

    # Get projects with state+county but no coordinates
    rows = conn.execute("""
        SELECT id, state, county
        FROM projects
        WHERE latitude IS NULL
          AND state IS NOT NULL
          AND county IS NOT NULL
          AND LENGTH(state) = 2
    """).fetchall()

    print(f"  Projects needing geocoding (with state+county): {len(rows):,}")

    if not rows:
        print("  Nothing to geocode.")
        conn.close()
        return

    # Step 3: Match and update
    print("\n[3/3] Matching projects to county centroids...")
    matched = 0
    unmatched_counties = {}
    batch = []
    batch_size = 1000

    for row_id, state, county in rows:
        norm = normalize_county(county)
        coords = _resolve_centroid(state, norm, county, centroids)

        if coords:
            batch.append((coords[0], coords[1], row_id))
            matched += 1
        else:
            uc_key = (state, county)
            unmatched_counties[uc_key] = unmatched_counties.get(uc_key, 0) + 1

        # Batch update
        if len(batch) >= batch_size:
            conn.executemany(
                "UPDATE projects SET latitude = ?, longitude = ? WHERE id = ?",
                batch
            )
            conn.commit()
            batch = []
            if matched % 5000 == 0:
                print(f"    ... {matched:,} matched so far")

    # Final batch
    if batch:
        conn.executemany(
            "UPDATE projects SET latitude = ?, longitude = ? WHERE id = ?",
            batch
        )
        conn.commit()

    conn.close()

    # Summary
    print(f"\n  Matched: {matched:,} / {len(rows):,} ({matched/len(rows)*100:.1f}%)")
    if unmatched_counties:
        # Show top unmatched
        top_unmatched = sorted(unmatched_counties.items(), key=lambda x: -x[1])[:15]
        print(f"\n  Top unmatched counties ({len(unmatched_counties)} unique):")
        for (state, county), count in top_unmatched:
            print(f"    {state} / {county}: {count} projects")


def geocode():
    """Run all geocoding strategies."""
    geocode_counties()
    print("\n" + "=" * 60)
    print("GEOCODING COMPLETE")
    print("=" * 60)
    print("\nNote: grid.db substations lack lat/lon columns.")
    print("When substation coordinates are added, POI-based geocoding")
    print("can provide more precise locations for projects with POI data.")


# =============================================================================
# Existing functions
# =============================================================================

def add_columns():
    """Add latitude and longitude columns to master.db if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("PRAGMA table_info(projects)")
    cols = {row[1] for row in cursor.fetchall()}

    added = []
    for col in ['latitude', 'longitude']:
        if col not in cols:
            conn.execute(f"ALTER TABLE projects ADD COLUMN {col} REAL")
            added.append(col)

    conn.commit()
    conn.close()

    if added:
        print(f"Added columns: {', '.join(added)}")
    else:
        print("Columns already exist")


def show_stats():
    """Show coordinate coverage stats."""
    conn = sqlite3.connect(DB_PATH)

    cursor = conn.execute("PRAGMA table_info(projects)")
    cols = {row[1] for row in cursor.fetchall()}

    if 'latitude' not in cols:
        print("latitude/longitude columns not yet added. Run with --add-columns first.")
        conn.close()
        return

    total = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    with_lat = conn.execute("SELECT COUNT(*) FROM projects WHERE latitude IS NOT NULL").fetchone()[0]
    with_lon = conn.execute("SELECT COUNT(*) FROM projects WHERE longitude IS NOT NULL").fetchone()[0]
    with_state_county = conn.execute(
        "SELECT COUNT(*) FROM projects WHERE state IS NOT NULL AND county IS NOT NULL"
    ).fetchone()[0]
    null_but_has_county = conn.execute(
        "SELECT COUNT(*) FROM projects WHERE latitude IS NULL AND state IS NOT NULL AND county IS NOT NULL AND LENGTH(state) = 2"
    ).fetchone()[0]

    print(f"\nTotal projects:           {total:,}")
    print(f"With coordinates:         {with_lat:,} ({with_lat/total*100:.1f}%)")
    print(f"With state+county:        {with_state_county:,} ({with_state_county/total*100:.1f}%)")
    print(f"Still need geocoding:     {null_but_has_county:,}")

    if with_lat > 0:
        cursor = conn.execute("""
            SELECT region, COUNT(*) as total,
                   SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) as with_coords
            FROM projects GROUP BY region ORDER BY total DESC
        """)
        print("\nBy region:")
        for row in cursor.fetchall():
            pct = row[2] / row[1] * 100 if row[1] > 0 else 0
            print(f"  {row[0]:12s}: {row[2]:>6,}/{row[1]:>6,} ({pct:5.1f}%)")

        # By source
        cursor = conn.execute("""
            SELECT source, COUNT(*) as total,
                   SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) as with_coords
            FROM projects GROUP BY source ORDER BY total DESC
        """)
        print("\nBy source:")
        for row in cursor.fetchall():
            pct = row[2] / row[1] * 100 if row[1] > 0 else 0
            print(f"  {row[0]:25s}: {row[2]:>6,}/{row[1]:>6,} ({pct:5.1f}%)")

    conn.close()


def audit_raw_data():
    """Scan raw_data JSON for any coordinate-like fields."""
    conn = sqlite3.connect(DB_PATH)

    # Check each source for coordinate fields
    cursor = conn.execute("""
        SELECT source, COUNT(*) FROM projects
        WHERE raw_data IS NOT NULL GROUP BY source ORDER BY COUNT(*) DESC
    """)
    sources = cursor.fetchall()

    print("Scanning raw_data for coordinate fields...\n")

    coord_keywords = ['lat', 'lon', 'coord', 'x_coord', 'y_coord', 'geom', 'location']
    total_found = 0

    for source, count in sources:
        cursor = conn.execute(
            "SELECT raw_data FROM projects WHERE source = ? AND raw_data IS NOT NULL LIMIT 5",
            (source,)
        )
        sample_keys = set()
        coord_keys = set()

        for (raw_data,) in cursor.fetchall():
            try:
                data = json.loads(raw_data)
                sample_keys.update(data.keys())
                for key in data.keys():
                    if any(kw in key.lower() for kw in coord_keywords):
                        coord_keys.add(key)
            except (json.JSONDecodeError, TypeError):
                pass

        if coord_keys:
            print(f"  {source} ({count:,} records): FOUND coordinate keys: {coord_keys}")
            total_found += count
        else:
            print(f"  {source} ({count:,} records): no coordinate fields")

    print(f"\nTotal records with potential coordinates: {total_found:,}")
    if total_found == 0:
        print("\nConclusion: No extractable coordinates in raw_data.")
        print("Next steps: geocoding from substation names, county centroids, or external APIs.")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description='Extract/manage lat/lon coordinates')
    parser.add_argument('--add-columns', action='store_true', help='Add lat/lon columns to master.db')
    parser.add_argument('--geocode', action='store_true', help='Run county centroid geocoding')
    parser.add_argument('--stats', action='store_true', help='Show coordinate coverage')
    parser.add_argument('--audit', action='store_true', help='Scan raw_data for coordinates')

    args = parser.parse_args()

    if not any([args.add_columns, args.geocode, args.stats, args.audit]):
        parser.print_help()
        return

    if args.add_columns:
        add_columns()
    if args.geocode:
        geocode()
    if args.stats:
        show_stats()


if __name__ == '__main__':
    main()
