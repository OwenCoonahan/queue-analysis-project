# Interconnection Queue Data Sources & Architecture

**Last Updated:** January 2026

## Quick Status

| Category | Status | Coverage |
|----------|--------|----------|
| **Queue Data** | 5/7 ISOs integrated | 39,367 projects |
| **Developer Names** | 99.5% coverage | 5,971 unique |
| **PPA Detection** | 38% flagged | 14,950 projects |
| **Parent Companies** | Major utilities mapped | 18 parents |

---

# Interconnection Queue Data Sources

## ISO/RTO Integration Status

| ISO/RTO | Direct API | Status | Developer Data | Refresh Frequency |
|---------|------------|--------|----------------|-------------------|
| **NYISO** | ✅ Excel download | **Active** | ✅ 100% | Daily |
| **ERCOT** | ✅ GIS Report API | **Active** | ✅ 95% | Daily |
| **MISO** | ✅ JSON API | **Active** | ✅ 97% | Daily |
| **PJM** | ⏳ Data Miner 2 API | **Waiting on API key** | Will have | Daily |
| **CAISO** | ✅ Excel download | **Needs integration** | ❌ Not in file | Daily |
| **SPP** | ⏳ OpsPortal | **Needs integration** | Unknown | Daily |
| **ISO-NE** | ⏳ IRTT Portal | **Needs integration** | Unknown | Daily |
| **West** | ❌ Multiple utilities | LBL only | Via EIA | Annual |
| **Southeast** | ❌ Multiple utilities | LBL only | Via EIA | Annual |

## Data Source Details

### Primary Queue Data

#### 1. NYISO (New York ISO)
- **URL:** `https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx`
- **Format:** Excel
- **Fields:** Queue Pos, Developer, Project Name, Capacity, Type, County, State, POI, Queue Date, COD
- **Developer field:** `Developer/Interconnection Customer` ✅
- **Update frequency:** Real-time (daily download recommended)
- **Integration:** `direct_fetcher.py::fetch_nyiso()`

#### 2. ERCOT (Texas)
- **URL:** `https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=15933`
- **Format:** Excel (GIS Report)
- **Fields:** INR, Project Name, Interconnecting Entity, Capacity, Fuel, County, Status, COD
- **Developer field:** `Interconnecting Entity` ✅
- **Update frequency:** Weekly GIS report updates
- **Integration:** `direct_fetcher.py::fetch_ercot()`

#### 3. MISO (Midwest)
- **URL:** `https://www.misoenergy.org/api/giqueue/getprojects`
- **Format:** JSON API
- **Fields:** projectNumber, transmissionOwner, summerNetMW, fuelType, state, county, poiName, status
- **Developer field:** `transmissionOwner` ✅ (96.8% populated)
- **Update frequency:** Real-time API
- **Integration:** `direct_fetcher.py::fetch_miso()`

#### 4. PJM (Mid-Atlantic/Northeast)
- **URL:** `https://api.pjm.com/api/v1/gen_queues`
- **Format:** JSON API (requires API key)
- **Fields:** queue_number, developer_name, mw, fuel_type, status, county, state
- **Developer field:** `developer_name` ✅
- **API Key:** Required from https://dataminer2.pjm.com/
- **Status:** ⏳ Waiting on API key
- **Integration:** `direct_fetcher.py::fetch_pjm()` (ready, needs key)

#### 5. CAISO (California)
- **URL:** `http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx`
- **Format:** Excel
- **Fields:** Queue Position, Project Name, Capacity, Fuel, County, State, POI, Status
- **Developer field:** ❌ Not in public file (need EIA matching)
- **Update frequency:** Monthly
- **Integration:** `direct_fetcher.py::fetch_caiso()` (implemented but not in refresh)

#### 6. SPP (Southwest Power Pool)
- **URL:** `https://opsportal.spp.org/Studies/GIActive`
- **Format:** Web portal (needs scraper or API discovery)
- **Developer field:** Unknown - needs investigation
- **Status:** ⏳ Needs integration
- **Fallback:** gridstatus library

#### 7. ISO-NE (New England)
- **URL:** `https://irtt.iso-ne.com/reports/external`
- **Format:** Excel export from web portal
- **Developer field:** Unknown - needs investigation
- **Status:** ⏳ Needs integration
- **Fallback:** gridstatus library

#### 8. West (WECC - non-CAISO)
- **Source:** Multiple utilities (NV Energy, PacifiCorp, Arizona utilities, etc.)
- **No centralized queue**
- **Current data:** LBL historical only
- **Developer data:** Via EIA Form 860 matching

#### 9. Southeast (Non-ISO utilities)
- **Source:** Duke, Southern Company, TVA, Entergy territories
- **No centralized queue**
- **Current data:** LBL historical only
- **Developer data:** Via EIA Form 860 matching

### Enrichment Data Sources

#### EIA Form 860 (Annual)
- **URL:** `https://www.eia.gov/electricity/data/eia860/`
- **Files:** Plant, Generator, Owner data
- **Purpose:** Developer/owner name matching by location
- **Update:** Annual (September release)
- **Integration:** `eia_loader.py`

#### FERC Form 1 Schedule 326 (Annual)
- **Source:** PUDL / Zenodo
- **Purpose:** PPA/purchased power contract detection
- **Records:** 40,183 contracts, 8,451 sellers
- **Update:** Annual
- **Integration:** `ferc_ppa.py`

#### LBL Queued Up (Annual)
- **URL:** `https://emp.lbl.gov/queues`
- **Purpose:** Historical queue data, completion rates, costs
- **Coverage:** All ISOs 2000-2024
- **Update:** Annual (manual download)
- **Integration:** `refresh_data.py::refresh_lbl()`

#### PUDL Database (Quarterly) - NOW AVAILABLE
- **Source:** Zenodo (Catalyst Cooperative)
- **Size:** 18 GB (uncompressed)
- **Contains:**
  - EIA 860 Generators: 628,389 records (proposed, existing, retired)
  - EIA 860 Plants: 234,012 records with locations
  - EIA 923 Generation: 767,081 monthly generation records
  - FERC Form 1 Purchased Power: 218,882 PPA records
  - EIA Entity Plants: 18,631 with state/county/coordinates
- **Key Use Cases:**
  - Verify which queue projects became operational (match to EIA 923)
  - Track proposed → operational conversion rates
  - Cross-reference PPA contracts with queue projects
- **Location:** `.cache/pudl/pudl.sqlite`

## Current Data Gaps

### Missing Direct Integrations
1. **PJM** - Have code, waiting on API key
2. **CAISO** - Have fetcher, need to add to refresh pipeline
3. **SPP** - Need to build scraper or find API
4. **ISO-NE** - Need to build scraper or find API

### Missing Data Fields
- **Interconnection costs** - Only estimates, not actual study costs
- **Transmission constraints** - Need OASIS data integration
- **Permitting status** - State-level data needed
- **Financial backing** - No public source

## Refresh Strategy

### Daily Automated Refresh
```bash
# Cron job (6 AM daily)
0 6 * * * cd /path/to/tools && python3 refresh_data.py --cron >> .data/refresh.log 2>&1
```

### Current Refresh Pipeline (`refresh_data.py`)
1. **NYISO** - Direct Excel download ✅
2. **ERCOT** - GIS Report API ✅
3. **MISO** - JSON API ✅ (new)
4. **LBL** - Manual/annual

### Recommended Additions
1. Add CAISO to daily refresh
2. Add PJM when API key arrives
3. Add SPP/ISO-NE when scrapers ready
4. Quarterly EIA 860 refresh (after September release)
5. Quarterly FERC/PUDL refresh

## File Locations

### Code
- `direct_fetcher.py` - ISO queue fetchers
- `refresh_data.py` - Refresh orchestration
- `eia_loader.py` - EIA 860 matching
- `ferc_ppa.py` - FERC PPA matching
- `developer_registry.py` - Name canonicalization

### Data Cache
- `.cache/` - Raw downloaded files
- `.cache/eia/` - EIA Form 860 files
- `.cache/pudl/` - PUDL database files

### Database
- `.data/queue.db` - Main SQLite database
- `.data/developer_registry.csv` - Exported developer list

## API Keys Required

| API | Status | How to Get |
|-----|--------|------------|
| PJM Data Miner 2 | ⏳ Pending | https://dataminer2.pjm.com/ → "Obtain API Key" |
| ERCOT | ✅ Not needed | Public GIS report |
| MISO | ✅ Not needed | Public API |
| NYISO | ✅ Not needed | Public download |
| CAISO | ✅ Not needed | Public download |
