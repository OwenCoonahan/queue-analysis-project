# Queue Analysis - Comprehensive Data Overview

*Last updated: 2026-02-17*

## Executive Summary

This platform aggregates interconnection queue data from all 7 major US ISOs plus supplementary market and reference data.

| Metric | Value |
|--------|-------|
| **Unique Projects** | **36,612** |
| Total Records | 54,496 |
| Total Capacity | 6,563 GW |
| Active Projects | 6,284 (17%) |
| Withdrawn Projects | 25,046 (69%) |
| Operational Projects | 3,995 (11%) |
| Developer Coverage | 94.7% |
| Queue Date Coverage | 97.3% |

### Unique Projects by Region

| Region | Unique Projects | Capacity | Developer Coverage | Queue Date Coverage |
|--------|-----------------|----------|-------------------|-------------------|
| PJM | 8,350 | 904 GW | 99% | 100% |
| West | 6,008 | 1,133 GW | 89% | 100% |
| MISO | 5,445 | 1,009 GW | 98% | 91% |
| ERCOT | 3,789 | 809 GW | 100% | 87% |
| Southeast | 2,879 | 652 GW | 78% | 100% |
| NYISO | 2,861 | 556 GW | 100% | 100% |
| SPP | 2,842 | 581 GW | 97% | 100% |
| CAISO | 2,837 | 710 GW | 99% | 100% |
| ISO-NE | 1,601 | 209 GW | 80% | 100% |
| **TOTAL** | **36,612** | **6,563 GW** | | |

> **Note:** Run `python3 db_status.py` to get current stats. Use `--markdown` flag to update this section.

---

## Data Architecture

### Storage Layer

```
.data/
├── queue.db          # MAIN DATABASE (74 MB) - Single source of truth
│                     #   54,496 records, 36,612 unique projects
│                     #   22 tables including market data
├── queue_v2.db       # Legacy (deprecated)
├── enrichment.db     # Legacy (deprecated)
├── nyiso_sections.db # Legacy (deprecated)
└── pe_firms.db       # Legacy (deprecated)

.cache/
├── pudl/             # PUDL database (23 GB) - EIA/FERC data
├── energy_communities/ # IRA bonus zones (378 MB)
├── nyiso_sections/   # Planning documents (302 MB)
├── nyiso_historical/ # 132 queue snapshots (28 MB)
├── eia/              # EIA 860 data (25 MB)
├── miso_api_cache.json # MISO API cache
├── pjm_*.xlsx        # PJM queue files
└── [iso]_*.xlsx      # Other cached ISO files
```

### Database Schema (queue.db)

**Core Tables:**
- `projects` - 54,496 records (36,612 unique projects)
- `snapshots` - 167,996 historical snapshots
- `changes` - 65,212 detected changes
- `qualified_developers` - 26 NYISO qualified developers

**Market Data Tables:**
- `lmp_zones` / `lmp_prices` / `lmp_annual` - Energy prices
- `capacity_zones` / `capacity_prices` / `elcc_values` - Capacity markets
- `tx_zones` / `tx_congestion` / `tx_constraints` / `tx_upgrades` - Transmission
- `ppa_deals` / `ppa_benchmarks` - PPA pricing
- `permit_requirements` / `permit_stats` / `permit_issues` - Permitting

---

## Data Sources by ISO

### 1. PJM (16,439 projects, 1,527 GW)

| Source | Projects | Capacity | Developer Coverage | Notes |
|--------|----------|----------|-------------------|-------|
| `pjm_direct` | 8,287 | 651 GW | 18% | Manual download from PJM |
| `lbl` | 8,152 | 876 GW | 99% | LBL Queued Up historical |

**Loader:** `pjm_loader.py`
- Downloads: `PlanningQueues.xlsx` (8,241 projects)
- Downloads: `CycleProjects-All.xlsx` (966 with developers)
- Matches developers by project name (1,485 matched)

**Status:**
- Queue dates: Not available in current files
- Developer data: Only 18% from direct source, 99% from LBL
- Refresh: Manual download required from PJM website

---

### 2. MISO (8,684 projects, 1,558 GW)

| Source | Projects | Capacity | Developer Coverage | Notes |
|--------|----------|----------|-------------------|-------|
| `miso_api` | 3,706 | 656 GW | 99% | **Live public API** |
| `lbl` | 4,978 | 901 GW | 99% | LBL historical |

**Loader:** `miso_loader.py` (NEW)
- **API Endpoint:** `https://www.misoenergy.org/api/giqueue/getprojects`
- No authentication required
- Returns JSON with full project details
- Fields: projectNumber, transmissionOwner, county, state, fuelType, applicationStatus, summerNetMW, inService, withdrawnDate, studyCycle, studyPhase

**Status:**
- Queue dates: Available (2015-present)
- Developer data: 99% coverage via transmissionOwner
- Refresh: Automatic via API (recommended daily)

---

### 3. ERCOT (5,157 projects, 1,095 GW)

| Source | Projects | Capacity | Developer Coverage | Notes |
|--------|----------|----------|-------------------|-------|
| `ercot` | 1,875 | 413 GW | 100% | GIS report |
| `lbl` | 3,282 | 682 GW | 100% | LBL historical |

**Loader:** `direct_fetcher.py` → `refresh_ercot()`
- Source: ERCOT GIS Report (monthly Excel)
- No queue dates available (ERCOT doesn't publish IR dates)
- Current queue: 1,999 active projects, 432 GW

**Status:**
- Queue dates: **NOT AVAILABLE** (ERCOT policy)
- Developer data: 100% coverage
- Refresh: Manual download or gridstatus

---

### 4. SPP (5,289 projects, 1,063 GW)

| Source | Projects | Capacity | Developer Coverage | Notes |
|--------|----------|----------|-------------------|-------|
| `spp` | 2,820 | 580 GW | 56% | gridstatus API |
| `lbl` | 2,469 | 483 GW | 100% | LBL historical |

**Loader:** Uses `gridstatus` library
- Requires Python 3.10+
- Latest queue date: 2026-01-07

**Status:**
- Queue dates: Available (2005-present)
- Developer data: 56% from live, 100% from LBL
- Refresh: Automatic via gridstatus

---

### 5. CAISO (3,162 projects, 798 GW)

| Source | Projects | Capacity | Developer Coverage | Notes |
|--------|----------|----------|-------------------|-------|
| `caiso` | 325 | 89 GW | 99% | Direct Excel download |
| `lbl` | 2,837 | 708 GW | 99% | LBL historical |

**Loader:** `direct_fetcher.py` → `fetch_caiso()`
- URL: `http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx`
- **CRITICAL ISSUE:** Queue dates frozen at April 2021

**Status:**
- Queue dates: **FROZEN** at 2021-04-15 (known CAISO data issue)
- Developer data: 99% coverage
- Refresh: Automatic but dates are stale
- API: User signed up for CAISO developer API (pending approval)

---

### 6. ISO-NE (2,882 projects, 339 GW)

| Source | Projects | Capacity | Developer Coverage | Notes |
|--------|----------|----------|-------------------|-------|
| `isone` | 1,601 | 188 GW | 74% | gridstatus |
| `lbl` | 1,281 | 151 GW | 99% | LBL historical |

**Loader:** Uses `gridstatus` library
- Latest queue date: 2022-09-09 (13+ months behind)

**Status:**
- Queue dates: Stale (2022 latest)
- Developer data: 74% from live
- Refresh: Via gridstatus but data is behind

---

### 7. NYISO (1,976 projects, 388 GW)

| Source | Projects | Capacity | Developer Coverage | Notes |
|--------|----------|----------|-------------------|-------|
| `nyiso` | 160 | 25 GW | 100% | Direct download |
| `lbl` | 1,816 | 364 GW | 100% | LBL historical |

**Loader:** `nyiso_loader.py`
- URL: NYISO Interconnection Queue Excel
- 7 sheets: Active, Cluster, Affected System, Withdrawn, In Service
- **Full historical archive:** 132 monthly snapshots (2014-2025)

**Additional Tools:**
- `nyiso_historical_analysis.py` - Track project progression
- `nyiso_section_scrapers.py` - Download planning documents
- `nyiso_section_ingest.py` - Parse qualified developers

**Status:**
- Queue dates: Available (2008-present)
- Developer data: 100% coverage
- Historical: 132 snapshots downloaded (28 MB)
- Planning docs: 63 documents downloaded (302 MB)
- Qualified developers: 26 parsed and ingested

---

### 8. West/Southeast (Combined from LBL)

| Region | Projects | Capacity | Source |
|--------|----------|----------|--------|
| West | 6,008 | 1,133 GW | LBL only |
| Southeast | 2,879 | 652 GW | LBL only |

These regions don't have unified queue systems - data comes exclusively from LBL Queued Up annual report.

---

## Data Quality Analysis

### Completeness by Field

| Field | Coverage | Notes |
|-------|----------|-------|
| Name | 94.3% | Project identifier |
| Developer | 96.7% | Varies by source (18-100%) |
| Capacity | 98.5% | MW value |
| Queue Date | **21.5%** | Major gap - ERCOT, CAISO issues |
| Location | 99.3% | State/County |
| Technology | 100% | Fuel type |
| Status | 100% | Active/Withdrawn/Operational |

### Freshness by Source

| Source | Records | Latest Queue Date | Last Ingested |
|--------|---------|-------------------|---------------|
| lbl | 33,702 | N/A (annual) | 2026-01-25 |
| pjm_direct | 8,287 | N/A | 2026-02-17 |
| miso_api | 3,706 | 2025-11-20 | 2026-02-15 |
| spp | 2,820 | 2026-01-07 | 2026-02-15 |
| ercot | 1,875 | N/A | 2026-02-11 |
| isone | 1,601 | 2022-09-09 | 2026-01-24 |
| caiso | 325 | **2021-04-15** | 2026-01-24 |
| nyiso | 160 | 2025-12-18 | 2026-01-16 |

### Known Data Issues

1. **CAISO queue dates frozen** at April 2021 - Source data issue
2. **ERCOT no queue dates** - Policy decision by ERCOT
3. **ISO-NE 13 months behind** - gridstatus data lag
4. **LBL is 76% of data** - Annual historical, not real-time

---

## Refresh Process

### Automatic (Daily Recommended)

```bash
python3 refresh_data.py --all
```

Refreshes:
- NYISO (direct download)
- ERCOT (GIS report)
- MISO (API)
- CAISO (direct download)
- SPP (gridstatus)
- ISO-NE (gridstatus)
- LBL (annual file)
- Market data (LMP, capacity, transmission, PPA, permits)

### Manual Downloads Required

| ISO | File | URL |
|-----|------|-----|
| PJM | PlanningQueues.xlsx | PJM website portal |
| PJM | CycleProjects-All.xlsx | PJM website portal |
| ERCOT | GIS Report | ERCOT data portal |

### New Loaders (Recently Added)

```bash
# MISO - Live API (no auth needed)
python3 miso_loader.py --stats
python3 miso_loader.py --refresh

# NYISO - Comprehensive loader (all 7 sheets)
python3 nyiso_loader.py --stats
python3 nyiso_loader.py --refresh

# NYISO - Historical analysis (132 snapshots)
python3 nyiso_historical_analysis.py --summary
python3 nyiso_historical_analysis.py --withdrawals

# NYISO - Section documents
python3 nyiso_section_scrapers.py --download-all
python3 nyiso_section_ingest.py --developers
```

---

## Technology Breakdown

| Technology | Projects | Capacity (GW) | Active % |
|------------|----------|---------------|----------|
| Solar | 18,795 | 1,811 | 23% |
| Wind | 8,276 | 1,442 | 10% |
| Battery | 5,574 | 899 | 39% |
| Gas | 4,570 | 1,419 | 8% |
| Solar+Battery | 3,515 | 754 | 36% |
| Other | 1,509 | 291 | 2% |
| Storage | 1,080 | 72 | 25% |
| Coal | 533 | 177 | 2% |
| Hydro | 451 | 30 | 9% |

---

## Regional Benchmarks (from LBL)

### Completion Rates

| Region | Rate | Timeline (months) |
|--------|------|-------------------|
| ERCOT | 33.9% | 36 |
| ISO-NE | 24.8% | 54 |
| PJM | 19.7% | 48 |
| MISO | 17.8% | 45 |
| West | 16.5% | 48 |
| SPP | 15.8% | 42 |
| Southeast | 15.2% | 44 |
| CAISO | 10.4% | 52 |
| NYISO | 7.9% | 56 |

### NYISO Historical Analysis (132 Snapshots)

- **2,136 projects tracked** (2014-2025)
- **84% withdrawal rate** (1,794 withdrawn)
- **9.4% completion rate** (201 to In Service)
- **319 GW withdrawn**, 11 GW completed
- **Avg 447 days** to withdrawal
- **Avg 857 days** to completion

---

## Supplementary Data

### EIA/PUDL Data (23 GB)
- EIA Form 860: Generator inventory
- FERC Form 1: Utility financials
- Stored in `.cache/pudl/`

### Energy Communities (378 MB)
- IRA bonus eligibility zones
- Coal closure communities
- Brownfield sites

### Interconnection Costs
- MISO, NYISO, ISO-NE, PJM, SPP cost studies
- Historical cost benchmarks by region/technology

### NYISO Planning Documents (302 MB)
- Gold Book (generator data)
- Reliability Plans (STAR reports)
- Qualified Developers (26 companies)
- Local Transmission Plans
- Congestion Reports
- DER Aggregation Data

---

## Recommendations

### High Priority
1. **Fix CAISO queue dates** - Await API approval or find alternate source
2. **Update ISO-NE data** - gridstatus is 13 months behind
3. **Automate PJM downloads** - Currently manual

### Medium Priority
4. **Add queue dates to ERCOT** - May not be possible (policy)
5. **Integrate MISO API into main refresh** - Already built
6. **Build historical tracking** - NYISO done, extend to others

### Data Quality
7. **Improve queue_date coverage** - Currently only 21.5%
8. **Resolve 7,787 status conflicts** - Cross-source validation
9. **Address 1,299 capacity conflicts** - Cross-source validation
