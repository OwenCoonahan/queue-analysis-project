# Interconnection Queue Database Documentation

**Last Updated:** 2026-01-26
**Database Version:** V2 (Normalized Star Schema)
**Total Projects:** 35,469 | **Active Projects:** 7,888 | **Active Capacity:** 1,423 GW

---

## Table of Contents

1. [Overview](#overview)
2. [Data Sources](#data-sources)
3. [Database Architecture](#database-architecture)
4. [ETL Pipeline](#etl-pipeline)
5. [Data Coverage & Quality](#data-coverage--quality)
6. [Querying the Database](#querying-the-database)
7. [Maintenance & Updates](#maintenance--updates)
8. [Known Limitations](#known-limitations)
9. [Appendix](#appendix)

---

## Overview

This database consolidates interconnection queue data from all major US ISOs/RTOs, enriched with EIA Form 860 ownership data, market prices, and other sources. The primary use case is analyzing the renewable energy development pipeline, developer portfolios, and market trends.

### Key Metrics (as of 2026-01-26)

| Metric | Value |
|--------|-------|
| Total Projects | 35,469 |
| Active Projects | 7,888 |
| Active Capacity | 1,423 GW |
| Developer Coverage | 98.6% |
| Regions Validated | 6/6 ISOs |
| Data Sources | 7 primary + 2 enrichment |
| PPA Matches | 18,671 (FERC Form 1) |
| Energy Community Eligible | 16,248 (45.8%) |
| Queue Date Coverage | 96.9% |
| COD Date Coverage | 79.7% |

---

## Data Sources

### Primary Sources (Integrated)

| Source | Type | Records | Update Frequency | Developer Data |
|--------|------|---------|------------------|----------------|
| **LBL Queued Up** | Historical | 33,702 | Annual | Yes (93%) |
| **ERCOT GIS** | Live API | 1,258 | Daily | Yes (100%) |
| **MISO API** | Live API | 1,678 | Daily | Yes (99%) |
| **NYISO** | Excel | 160 | Weekly | Yes (100%) |
| **CAISO** | Excel | 325 | Weekly | Yes (100%) |
| **SPP** | Excel | 281 | Weekly | Yes (100%) |
| **ISO-NE** | Excel | 79 | Weekly | Yes (82%) |

### Enrichment Sources (Integrated)

| Source | Records | Purpose |
|--------|---------|---------|
| **EIA Form 860** | 965 matches | Owner/developer enrichment |
| **GridStatus EIA** | 538 matches | Additional EIA matching |
| **Market Data** | ~500 | LMP prices, capacity prices, ELCC |
| **PUDL FERC PPA** | 18,671 matches | PPA/offtake relationship discovery |
| **Energy Community** | 16,248 eligible | IRA/ITC 10% bonus eligibility |

### Available But Not Fully Integrated

| Source | Status | Action Required |
|--------|--------|-----------------|
| **PJM Data Miner 2** | Code ready | Need API key |
| **OASIS ATC** | Framework ready | Need NAESB credentials |
| **FERC Filings** | Parser exists | Need to run extraction |

### Source Details

#### Lawrence Berkeley Lab (LBL) "Queued Up"
- **URL:** https://emp.lbl.gov/queues
- **Format:** Excel
- **Coverage:** All US regions, historical back to 2010
- **Fields:** queue_id, project_name, developer, capacity, type, status, state, county, dates
- **Limitations:** Annual snapshot, may have stale records
- **Cache:** `.cache/lbl_queued_up.xlsx`

#### ERCOT Generation Interconnection Status (GIS)
- **URL:** Dynamic (fetched via `direct_fetcher.py`)
- **Format:** Excel via API
- **Coverage:** ERCOT region only, live queue
- **Fields:** INR, Project Name, Interconnecting Entity, Capacity, Fuel, Status, POI
- **Refresh:** Real-time queue status
- **Cache:** `.cache/ercot_gis_report.xlsx`

#### MISO Interconnection Queue API
- **URL:** `https://www.misoenergy.org/api/giqueue/getprojects`
- **Format:** JSON
- **Coverage:** MISO region, live queue
- **Fields:** projectNumber, transmissionOwner, summerNetMW, fuelType, applicationStatus
- **Developer Coverage:** 96.8% (transmissionOwner field)
- **Cache:** `.cache/miso_queue_direct.parquet`

#### NYISO Interconnection Queue
- **URL:** Dynamic Excel download
- **Format:** Excel
- **Coverage:** NYISO region, live queue
- **Cache:** `.cache/nyiso_queue_direct.xlsx`

#### CAISO Generation Queue
- **URL:** `http://www.caiso.com/Documents/PublicQueueReport.xlsx`
- **Format:** Excel (header row 4)
- **Coverage:** CAISO region, active projects
- **Cache:** `.cache/caiso_queue_direct.xlsx`

#### SPP Generator Interconnection Queue
- **URL:** Via OpsPortal
- **Format:** Excel
- **Coverage:** SPP region
- **Cache:** `.cache/spp_queue_direct.parquet`

#### ISO-NE Interconnection Queue
- **URL:** Via IRTT Portal
- **Format:** Excel
- **Coverage:** ISO-NE region
- **Cache:** `.cache/isone_queue_direct.parquet`

#### PUDL FERC Form 1 Purchased Power (PPA Discovery)
- **Source:** Public Utility Data Liberation (PUDL) project
- **Table:** `out_ferc1__yearly_purchased_power_and_exchanges_sched326`
- **Format:** SQLite (19 GB database)
- **Records:** 218,882 purchase power records (2010-2024)
- **Fields:** seller_name, utility_buyer, purchased_mwh, purchase_type_code
- **Purpose:** Match queue developers with FERC PPA sellers to discover offtake
- **Script:** `ppa_discovery.py`
- **Cache:** `.cache/pudl/pudl.sqlite`

#### DOE Energy Community Data (IRA/ITC Eligibility)
- **Source:** DOE NETL via Zenodo (https://zenodo.org/records/14757122)
- **Format:** CSV/Shapefile (ZIP)
- **Coverage:** 818 coal closure counties, 1,949 FFE-qualified counties
- **Purpose:** Identify projects eligible for 10% IRA/ITC bonus tax credit
- **Script:** `energy_community.py`
- **Cache:** `.cache/energy_communities/`

---

## Database Architecture

### Database Files

```
.data/
├── queue.db        # V1 - Flat denormalized (52 MB)
├── queue_v2.db     # V2 - Normalized star schema (7 MB) [PRIMARY]
├── enrichment.db   # Enrichment data (1.5 MB)
└── pe_firms.db     # PE firm tracking (40 KB)

.cache/
├── eia/            # EIA 860 Excel files
├── pudl/           # PUDL SQLite database (19 GB)
├── ferc/           # FERC filing cache
└── [iso]_*.xlsx    # ISO queue caches
```

### V2 Schema (Primary Database)

```
┌─────────────────────────────────────────────────────────────┐
│                    DIMENSION TABLES                          │
├─────────────────────────────────────────────────────────────┤
│  dim_regions (9)        - ISO/RTO regions                   │
│  dim_developers (6,284) - Developer registry                │
│  dim_technologies (17)  - Generation types                  │
│  dim_statuses (26)      - Project status codes              │
│  dim_locations (3,878)  - State/county geography            │
│  dim_substations (0)    - POI substations (not populated)   │
├─────────────────────────────────────────────────────────────┤
│                      FACT TABLES                             │
├─────────────────────────────────────────────────────────────┤
│  fact_projects (35,469) - Main project records              │
│    └─ energy_community_eligible, energy_community_type      │
│  ppa_matches (18,671)   - FERC PPA relationship matches     │
│  fact_project_history   - Change tracking (not populated)   │
│  fact_lmp_prices        - LMP data (not populated in V2)    │
│  fact_capacity_prices   - Capacity market (not populated)   │
│  fact_elcc              - ELCC values (not populated)       │
├─────────────────────────────────────────────────────────────┤
│                        MARTS                                 │
├─────────────────────────────────────────────────────────────┤
│  mart_developer_portfolios - Aggregated developer stats     │
└─────────────────────────────────────────────────────────────┘
```

### Key Tables Schema

#### fact_projects
```sql
CREATE TABLE fact_projects (
    project_id INTEGER PRIMARY KEY,
    queue_id TEXT NOT NULL,
    region_id INTEGER REFERENCES dim_regions,
    project_name TEXT,
    developer_id INTEGER REFERENCES dim_developers,
    location_id INTEGER REFERENCES dim_locations,
    technology_id INTEGER REFERENCES dim_technologies,
    status_id INTEGER REFERENCES dim_statuses,
    capacity_mw REAL,
    queue_date DATE,
    cod_proposed DATE,
    cod_actual DATE,
    withdrawal_date DATE,
    data_source TEXT,
    last_updated_date DATE,
    energy_community_eligible BOOLEAN,  -- IRA/ITC 10% bonus eligible
    energy_community_type TEXT,         -- 'coal_closure', 'ffe', or both
    UNIQUE(queue_id, region_id)
);
```

#### ppa_matches
```sql
CREATE TABLE ppa_matches (
    queue_id TEXT,
    project_name TEXT,
    developer TEXT,
    region TEXT,
    capacity_mw REAL,
    seller_name TEXT,           -- FERC Form 1 seller
    utility_buyer TEXT,         -- Utility purchasing power
    report_year INTEGER,
    purchased_mwh REAL,
    purchase_type TEXT,
    match_type TEXT,            -- exact_developer, fuzzy_developer, project_name, fuzzy_project
    confidence REAL,            -- 0.0 to 1.0
    created_at TEXT,
    PRIMARY KEY (queue_id, seller_name, report_year)
);
```

#### dim_developers
```sql
CREATE TABLE dim_developers (
    developer_id INTEGER PRIMARY KEY,
    canonical_name TEXT NOT NULL UNIQUE,
    display_name TEXT,
    parent_company TEXT,
    parent_canonical_name TEXT,  -- Canonical parent (e.g., "NextEra Energy" for all NextEra variants)
    developer_type TEXT,  -- 'IPP', 'Utility', 'Investor'
    is_active BOOLEAN
);
```

**Developer Canonicalization:** 184 name variations have been mapped to 20 canonical parent groups:
- NextEra Energy (16 variations)
- Invenergy (18 variations)
- EDF Renewables (11 variations)
- Entergy (14 variations)
- Duke Energy (25 variations)
- And 15 more major developers

### Status Categories

| Category | Statuses | Description |
|----------|----------|-------------|
| **Active** | Active, Done, Pending Transfer, Study phases | In queue, not yet operational |
| **Completed** | Operational, Completed | Reached COD |
| **Withdrawn** | Withdrawn, Cancelled, Archived | Left queue |
| **Suspended** | Suspended, On Hold | Temporarily paused |

**Important:** MISO's "Done" status means studies complete, awaiting COD - counted as **Active**.

---

## ETL Pipeline

### Pipeline Overview

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  ISO APIs/Files  │────▶│  direct_fetcher  │────▶│   .cache/        │
│  (External)      │     │  (Download)      │     │   (24h cache)    │
└──────────────────┘     └──────────────────┘     └──────────────────┘
                                                           │
                                                           ▼
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  queue.db (V1)   │◀────│  refresh_data    │◀────│  Normalize       │
│  (Flat table)    │     │  (Upsert)        │     │  columns         │
└──────────────────┘     └──────────────────┘     └──────────────────┘
         │
         ▼
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Sync stale      │────▶│  Deduplicate     │────▶│  queue_v2.db     │
│  records         │     │  (prefer API)    │     │  (Star schema)   │
└──────────────────┘     └──────────────────┘     └──────────────────┘
         │                                                 │
         ▼                                                 ▼
┌──────────────────┐                              ┌──────────────────┐
│  validate_data   │◀─────────────────────────────│  Live API check  │
│  (Benchmarks)    │                              │  (6/6 regions)   │
└──────────────────┘                              └──────────────────┘
```

### Key Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `direct_fetcher.py` | Download from ISO APIs | `DirectFetcher().fetch_ercot()` |
| `refresh_data.py` | Refresh V1 from sources | `python3 refresh_data.py` |
| `refresh_v2.py` | Sync & rebuild V2 | `python3 refresh_v2.py --quick` |
| `validate_data.py` | Validate against live | `python3 validate_data.py --live` |
| `eia_loader.py` | EIA 860 enrichment | `EIAMatcher().enrich_database()` |
| `queue_db.py` | Data access layer | `QueueDB().get_projects()` |

### Refresh Process

#### Quick Refresh (Daily)
```bash
cd tools
python3 refresh_v2.py --quick
```
This will:
1. Sync stale records against live ISO data
2. Mark withdrawn projects not in live queues
3. Rebuild V2 from V1

#### Full Refresh (Weekly)
```bash
cd tools
python3 refresh_data.py           # Refresh V1 from all sources
python3 refresh_v2.py --quick     # Rebuild V2
python3 validate_data.py --live   # Validate
```

#### EIA Enrichment (Monthly)
```python
from eia_loader import EIAMatcher
matcher = EIAMatcher()
matcher.load_eia_data(year=2024)
matcher.enrich_database(min_confidence=0.7)
```

### Deduplication Rules

When the same project exists in multiple sources, priority order:
1. `miso_api` (direct API)
2. `ercot` (direct GIS)
3. `nyiso` (direct Excel)
4. `caiso` (direct Excel)
5. `spp` (direct Excel)
6. `isone` (direct Excel)
7. `lbl` (historical data - lowest priority)

### Stale Record Handling

Records are marked as "Withdrawn" if:
- They exist in our database as Active
- They are NOT found in the live ISO queue data
- They are not already marked as Withdrawn/Operational

This prevents over-counting from stale historical data.

---

## Data Coverage & Quality

### Validation Results (2026-01-25)

| Region | Live Source | DB Count | DB GW | Diff | Status |
|--------|-------------|----------|-------|------|--------|
| ERCOT | GIS API | 1,258 | 235.9 | 0.0% | ✓ |
| MISO | JSON API | 1,678 | 325.1 | 0.8% | ✓ |
| NYISO | Excel | 160 | 24.7 | 0.0% | ✓ |
| CAISO | Excel | 325 | 89.6 | 0.0% | ✓ |
| SPP | Excel | 281 | 71.2 | 0.0% | ✓ |
| ISO-NE | Excel | 79 | 15.3 | 0.1% | ✓ |
| PJM | LBL only | 1,942 | 187.5 | N/A | ○ |
| Southeast | LBL only | 710 | 117.3 | N/A | ○ |
| West | LBL only | 1,455 | 356.3 | N/A | ○ |

### Developer Coverage by Region

| Region | Active | With Developer | Coverage |
|--------|--------|----------------|----------|
| ERCOT | 1,258 | 1,258 | 100.0% |
| NYISO | 160 | 160 | 100.0% |
| SPP | 281 | 281 | 100.0% |
| PJM | 1,942 | 1,938 | 99.8% |
| CAISO | 325 | 324 | 99.7% |
| MISO | 1,678 | 1,664 | 99.2% |
| West | 1,455 | 1,428 | 98.1% |
| Southeast | 710 | 663 | 93.4% |
| ISO-NE | 79 | 65 | 82.3% |
| **TOTAL** | **7,888** | **7,781** | **98.6%** |

### Energy Community Eligibility by Region

Projects in IRA Energy Community zones qualify for 10% ITC/PTC bonus.

| Region | Total | EC Eligible | Rate |
|--------|-------|-------------|------|
| ERCOT | 3,768 | 2,517 | 66.8% |
| CAISO | 2,837 | 1,662 | 58.6% |
| PJM | 8,152 | 4,681 | 57.4% |
| West | 6,008 | 2,545 | 42.4% |
| MISO | 5,435 | 2,178 | 40.1% |
| Southeast | 2,879 | 1,111 | 38.6% |
| SPP | 2,841 | 750 | 26.4% |
| NYISO | 1,948 | 471 | 24.2% |
| ISO-NE | 1,601 | 333 | 20.8% |
| **TOTAL** | **35,469** | **16,248** | **45.8%** |

### Date Coverage

#### Date Columns in fact_projects

| Column | Description | Coverage | Date Range |
|--------|-------------|----------|------------|
| `queue_date` | When project entered queue | **96.9%** | 1996-2026 |
| `cod_proposed` | Proposed Commercial Operation Date | **79.7%** | 2000-2050 |
| `cod_actual` | Actual COD (operational date) | 0% | Not populated |
| `withdrawal_date` | When project withdrew | 0% | Not populated |
| `first_seen_date` | First seen in our system | 0% | Not populated |
| `last_updated_date` | Last ETL refresh | 100% | Current |

#### Queue Date Coverage by Region

| Region | Coverage | Earliest | Latest |
|--------|----------|----------|--------|
| PJM | 100% | 1997 | 2023 |
| CAISO | 100% | 1999 | 2023 |
| NYISO | 100% | 1998 | 2025 |
| ISO-NE | 100% | 1996 | 2025 |
| MISO | 99.7% | 1997 | 2025 |
| SPP | 99.1% | 1998 | 2026 |
| West | 98.5% | 1997 | 2024 |
| Southeast | 91.7% | 1998 | 2024 |
| ERCOT | 80.6% | 2016 | 2024 |

#### COD Proposed Coverage by Region

| Region | Coverage | Range |
|--------|----------|-------|
| PJM | 97.6% | 1981-2031 |
| MISO | 97.1% | 2000-2032 |
| CAISO | 92.0% | 2006-2036 |
| ISO-NE | 92.5% | 2003-2033 |
| West | 88.0% | 1987-2035 |
| ERCOT | 68.0% | 2021-2050 |
| SPP | 64.4% | 2002-2035 |
| Southeast | 27.7% | 2010-2047 |
| NYISO | 24.4% | 2025-2034 |

#### Date Quality Notes
- **181 projects** have `1970-01-01` queue dates (Unix epoch - parsing failures)
- **9 projects** have pre-1990 COD dates (likely invalid)
- **224 projects** have COD dates beyond 2040 (aggressive estimates)

### Data Quality Checks

Run validation suite:
```bash
python3 validate_data.py --live    # Compare against live APIs
python3 validate_data.py --cross   # Cross-source reconciliation
python3 validate_data.py --quality # Data quality metrics
```

---

## Querying the Database

### Using Python (Recommended)

```python
from queue_db import QueueDB

db = QueueDB()

# Get all active projects
projects = db.get_projects(status='active')

# Get projects by region
ercot = db.get_projects(region='ERCOT', status='active')

# Get projects by developer
nextera = db.get_projects(developer='NextEra')

# Get developer portfolio
portfolio = db.get_developer_portfolio('NextEra Energy')
```

### Direct SQL Queries

Connect to V2 database:
```python
import sqlite3
conn = sqlite3.connect('.data/queue_v2.db')
```

#### Active Projects by Region
```sql
SELECT
    r.region_code,
    COUNT(*) as projects,
    ROUND(SUM(p.capacity_mw)/1000, 1) as gw
FROM fact_projects p
JOIN dim_regions r ON p.region_id = r.region_id
JOIN dim_statuses s ON p.status_id = s.status_id
WHERE s.status_category = 'Active'
GROUP BY r.region_code
ORDER BY gw DESC;
```

#### Projects by Developer
```sql
SELECT
    d.canonical_name,
    COUNT(*) as projects,
    ROUND(SUM(p.capacity_mw)/1000, 1) as gw,
    GROUP_CONCAT(DISTINCT r.region_code) as regions
FROM fact_projects p
JOIN dim_developers d ON p.developer_id = d.developer_id
JOIN dim_regions r ON p.region_id = r.region_id
JOIN dim_statuses s ON p.status_id = s.status_id
WHERE s.status_category = 'Active'
GROUP BY d.developer_id
ORDER BY gw DESC
LIMIT 20;
```

#### Projects by Technology
```sql
SELECT
    t.technology_name,
    COUNT(*) as projects,
    ROUND(SUM(p.capacity_mw)/1000, 1) as gw
FROM fact_projects p
JOIN dim_technologies t ON p.technology_id = t.technology_id
JOIN dim_statuses s ON p.status_id = s.status_id
WHERE s.status_category = 'Active'
GROUP BY t.technology_id
ORDER BY gw DESC;
```

#### Queue Trends by Year
```sql
SELECT
    strftime('%Y', p.queue_date) as year,
    COUNT(*) as projects,
    ROUND(SUM(p.capacity_mw)/1000, 1) as gw
FROM fact_projects p
WHERE p.queue_date IS NOT NULL
GROUP BY year
ORDER BY year;
```

#### Developer with Location Details
```sql
SELECT
    d.canonical_name as developer,
    p.project_name,
    r.region_code,
    l.state,
    l.county,
    t.technology_name,
    p.capacity_mw,
    s.status_name,
    p.queue_date,
    p.cod_proposed
FROM fact_projects p
JOIN dim_developers d ON p.developer_id = d.developer_id
JOIN dim_regions r ON p.region_id = r.region_id
LEFT JOIN dim_locations l ON p.location_id = l.location_id
LEFT JOIN dim_technologies t ON p.technology_id = t.technology_id
JOIN dim_statuses s ON p.status_id = s.status_id
WHERE d.canonical_name LIKE '%NextEra%'
ORDER BY p.capacity_mw DESC;
```

### Using pandas

```python
import pandas as pd
import sqlite3

conn = sqlite3.connect('.data/queue_v2.db')

# Load all active projects with joins
df = pd.read_sql("""
    SELECT
        p.queue_id,
        p.project_name,
        d.canonical_name as developer,
        r.region_code as region,
        l.state,
        t.technology_name as technology,
        p.capacity_mw,
        s.status_name as status,
        p.queue_date,
        p.cod_proposed
    FROM fact_projects p
    JOIN dim_regions r ON p.region_id = r.region_id
    LEFT JOIN dim_developers d ON p.developer_id = d.developer_id
    LEFT JOIN dim_locations l ON p.location_id = l.location_id
    LEFT JOIN dim_technologies t ON p.technology_id = t.technology_id
    JOIN dim_statuses s ON p.status_id = s.status_id
    WHERE s.status_category = 'Active'
""", conn)

print(f"Loaded {len(df)} active projects")
```

### Using PPA Discovery

```python
from ppa_discovery import PPADiscovery

# Run full PPA discovery
discovery = PPADiscovery()
discovery.connect()
discovery.load_ferc_sellers(min_year=2015)
discovery.load_queue_projects()
matches = discovery.find_matches(min_confidence=0.8)
discovery.save_matches()
discovery.close()

# Or use CLI:
# python3 ppa_discovery.py --min-confidence 0.8
# python3 ppa_discovery.py --developer "NextEra Energy"
# python3 ppa_discovery.py --utility "Duke Energy"
```

### Using Energy Community Checker

```python
from energy_community import EnergyCommunityChecker, enrich_queue_with_energy_community

# Check specific location
checker = EnergyCommunityChecker()
checker.load_data()
result = checker.check_location('TX', 'Harris')
print(f"Energy Community: {result.is_energy_community}")
print(f"Coal Closure: {result.coal_closure}")
print(f"FFE Qualified: {result.ffe_qualified}")

# Enrich entire database
enrich_queue_with_energy_community(save=True)

# Or use CLI:
# python3 energy_community.py --enrich
# python3 energy_community.py --stats
# python3 energy_community.py --check TX Harris
```

---

## Maintenance & Updates

### Regular Maintenance Schedule

| Task | Frequency | Command |
|------|-----------|---------|
| Quick refresh | Daily | `python3 refresh_v2.py --quick` |
| Full refresh | Weekly | `python3 refresh_data.py && python3 refresh_v2.py --quick` |
| Validation | Weekly | `python3 validate_data.py --live` |
| EIA enrichment | Monthly | See EIA section above |
| PPA discovery | Monthly | `python3 ppa_discovery.py --min-confidence 0.8` |
| Energy Community | Quarterly | `python3 energy_community.py --enrich` |
| Cache cleanup | Monthly | Delete `.cache/*.xlsx` older than 30 days |

### Adding New Data Sources

1. Add fetch method to `direct_fetcher.py`
2. Add refresh method to `refresh_data.py`
3. Add sync logic to `refresh_v2.py` (if live validation needed)
4. Add validation to `validate_data.py`
5. Update this documentation

### Monitoring Data Quality

Key metrics to track:
- **Validation diff %** - Should be <5% for all validated regions
- **Developer coverage %** - Currently 98.6%, should stay >95%
- **Stale records synced** - Watch for large spikes (data issues)
- **Total active GW** - ~1,400 GW, significant changes warrant investigation

### Troubleshooting

#### Over-counting (DB > Live)
- Likely stale records not synced
- Run `refresh_v2.py --quick` to sync
- Check if source's Status column changed format

#### Under-counting (DB < Live)
- Check if status mapping is correct
- MISO "Done" should map to Active
- NULL status records may be excluded

#### Missing developers
- Run EIA enrichment: `EIAMatcher().enrich_database()`
- Check if source column name changed

---

## Known Limitations

### Data Limitations

1. **PJM not validated** - Need Data Miner 2 API key
2. **Southeast/West not validated** - No central API (multiple utilities)
3. **Historical data may be stale** - LBL is annual snapshot
4. **SPP completion tracking** - SPP direct feed removes completed projects (only shows Active/Withdrawn). The 1.7% completion rate is artificially low; 44 known completions come from LBL historical data only. Could be enriched via EIA Form 860 cross-reference (3,089 SPP plants available).
5. **Developer canonicalization partial** - 184 variations mapped to 20 parent groups. ~6,000 developers still not grouped.

### Technical Limitations

1. **No incremental updates** - Full rebuild on each refresh
2. **No change tracking** - `fact_project_history` not populated
3. **PUDL not integrated** - 19GB database available but not used
4. **Cache expiry** - 24 hours, may miss intraday updates

---

## Analysis Capabilities

### Implemented Features

| Module | Class/Function | Description | Status |
|--------|----------------|-------------|--------|
| `scoring.py` | `FeasibilityScorer` | Project completion probability (0-100 score, A-F grade) | ✓ Ready |
| `intelligence.py` | `MonteCarloSimulator` | P10/P50/P90 COD distributions | ✓ Ready |
| `intelligence.py` | `DeveloperAnalyzer` | Developer track record, success rates, portfolio stats | ✓ Ready |
| `intelligence.py` | `POIAnalyzer` | POI-specific success/failure rates | ✓ Ready |
| `intelligence.py` | `CostAnalyzer` | Interconnection cost estimation | ✓ Ready |
| `intelligence.py` | `ModelValidator` | Backtesting validation, Proposed vs Realistic COD | ✓ Ready |

### Using Analysis Tools

```python
from scoring import FeasibilityScorer
from intelligence import (
    DataLoader, MonteCarloSimulator,
    DeveloperAnalyzer, POIAnalyzer, CostAnalyzer
)

# Load data
loader = DataLoader()
df = loader.load_lbl_historical()

# Score a project
scorer = FeasibilityScorer(df, region='MISO')
result = scorer.score_project(queue_id='J1234')
print(f"Score: {result['score']['total']}, Grade: {result['score']['grade']}")

# Monte Carlo simulation
mc = MonteCarloSimulator(loader)
sim_result = mc.simulate_project(queue_id='J1234', n_simulations=10000)
print(f"P50 COD: {sim_result.p50_cod}, P90 COD: {sim_result.p90_cod}")

# Developer analysis
dev_analyzer = DeveloperAnalyzer(loader)
dev_intel = dev_analyzer.analyze_developer('NextEra Energy')
print(f"Success rate: {dev_intel.success_rate:.1%}")

# POI analysis
poi_analyzer = POIAnalyzer(loader)
poi_intel = poi_analyzer.analyze_poi('Substation XYZ')
print(f"POI success rate: {poi_intel.success_rate:.1%}")

# Cost estimation
cost_analyzer = CostAnalyzer(loader)
cost_intel = cost_analyzer.estimate_costs(
    capacity_mw=100, project_type='Solar', region='MISO'
)
print(f"Estimated cost: ${cost_intel.total_estimate_m:.1f}M")
```

---

## Gap Analysis & Roadmap

### Current State vs Recommendations

| Capability | Status | Notes |
|------------|--------|-------|
| **Completion Probability** | ✓ Implemented | `FeasibilityScorer` |
| **Timeline Risk (P50/P90)** | ✓ Implemented | `MonteCarloSimulator` |
| **Developer Track Record** | ✓ Implemented | `DeveloperAnalyzer` |
| **POI Intelligence** | ✓ Implemented | `POIAnalyzer` |
| **Proposed vs Realistic COD** | ✓ Implemented | `ModelValidator` |
| **Cost Estimates** | ⚠ Data exists | Cost files in cache, `CostAnalyzer` exists |
| **PPA Discovery** | ✓ Implemented | 18,671 matches via `ppa_discovery.py` |
| **Network Constraints** | ⚠ Partial | 13 records, need OASIS integration |
| **IRA/ITC Zones** | ✓ Implemented | 16,248 eligible via `energy_community.py` |
| **Portfolio Risk** | ○ Not implemented | Need correlated risk model |
| **OASIS ATC Data** | ⚠ Framework ready | `oasis_fetcher.py` - needs NAESB credentials |
| **PJM Direct API** | ○ Blocked | Code ready, need API key |

### Available Data Not Yet Integrated

| Data Source | Records | Integration Effort |
|-------------|---------|-------------------|
| PUDL EIA Ownership | 100,613 | Low - extend EIA loader |
| Cost Study Files | 4 files | Low - already have CostAnalyzer |
| OASIS ATC Data | Variable | Medium - need NAESB credentials |

### Roadmap

#### Phase 1: Quick Wins (Data exists, need integration)
- [x] Integrate cost study data into scoring (`CostAnalyzer`)
- [x] PUDL FERC PPA matching to queue projects (`ppa_discovery.py` - 18,671 matches)
- [x] Energy Community (IRA/ITC) zone mapping (`energy_community.py` - 45.8% eligible)
- [x] Date backfill from V1 standardized dates (queue: 21%→97%, COD: 26%→80%)
- [x] Technology reclassification ("Other" reduced from 4,993 to 1,966 projects)
- [x] Developer canonicalization (184 variations → 20 parent groups)

#### Phase 2: Data Gaps
- [ ] PJM API key acquisition
- [x] OASIS ATC data fetchers framework (`oasis_fetcher.py` - needs credentials)
- [ ] NERC reliability assessment integration
- [ ] SPP completion enrichment via EIA Form 860 cross-reference

#### Phase 3: Premium Features
- [ ] Portfolio risk analysis (correlated risk model)
- [ ] Enhanced competitive landscape ("who will win at POI")
- [ ] Cost variance tracking and analysis
- [ ] Full developer canonicalization (~6,000 remaining)

---

## Appendix

### File Inventory

```
tools/
├── DATABASE_DOCUMENTATION.md  # This file
│
├── # ETL Pipeline
├── direct_fetcher.py      # ISO API fetchers (ERCOT, MISO, NYISO, CAISO, SPP, ISO-NE, PJM)
├── refresh_data.py        # V1 refresh orchestration
├── refresh_v2.py          # V2 sync and rebuild (--dry-run flag available)
├── validate_data.py       # Data validation suite
├── eia_loader.py          # EIA 860 enrichment (EIAMatcher)
├── data_enrichment.py     # Additional enrichment logic
├── developer_registry.py  # Name canonicalization (not populated)
├── ppa_discovery.py       # FERC PPA matching from PUDL (18,671 matches)
├── energy_community.py    # IRA/ITC Energy Community eligibility (45.8%)
├── oasis_fetcher.py       # OASIS ATC data framework (CAISO, PJM, MISO)
│
├── # Analysis & Scoring
├── scoring.py             # FeasibilityScorer - project completion probability
├── intelligence.py        # MonteCarloSimulator, DeveloperAnalyzer, POIAnalyzer, CostAnalyzer
├── pe_analytics.py        # PE firm portfolio analysis
│
├── # Data Access
├── queue_db.py            # QueueDB data access layer
├── data_store.py          # DataStore for V1
├── schema_v2.sql          # V2 database schema
│
├── # Visualization & Reporting
├── app.py                 # Streamlit web application
├── charts_altair.py       # Chart generation
├── visualizations.py      # Additional visualizations
├── portfolio_report.py    # Portfolio report generation
│
└── # Market Data
    ├── lmp_data.py        # LMP price handling
    ├── capacity_data.py   # Capacity market data
    ├── ppa_data.py        # PPA deal tracking
    └── transmission_data.py # Transmission constraints
```

### Environment Requirements

```
Python 3.9+
pandas
openpyxl
requests
sqlite3 (built-in)
```

### Contact & Support

For issues or questions:
- Check refresh logs: `.data/queue.db` → `refresh_log` table
- Validation output: `python3 validate_data.py --json`
- Cache status: `ls -la .cache/`

---

*Document updated: 2026-01-26*
*Session changes: Date backfill (queue 97%, COD 80%), Technology reclassification (3,027 fixed), Developer canonicalization (184→20 groups), SPP completion rate analysis*
*Previous: PPA Discovery (18,671 matches), Energy Community (45.8% eligible), OASIS ATC framework*
