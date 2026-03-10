# Data Status Report - CRITICAL REVIEW

**Report Date:** February 15, 2026
**Purpose:** Ground-truth assessment of all data sources, freshness, and accuracy issues

---

## EXECUTIVE SUMMARY: CRITICAL ISSUES

| Issue | Severity | Impact |
|-------|----------|--------|
| **CAISO queue dates frozen at April 2021** | CRITICAL | No new project dates for ~5 years |
| **PJM has NO live data source** | CRITICAL | 8,152 projects rely solely on LBL historical |
| **ERCOT GIS report has NO queue dates** | HIGH | Cannot track time-in-queue |
| **ISO-NE queue dates stop at Jan 2025** | HIGH | 13 months behind |
| **LBL is 76% of all data** | HIGH | Most data is annual historical, not live |
| **Developer coverage varies 0-100%** | MEDIUM | SPP/ISO-NE via gridstatus have gaps |

### Documentation vs Reality

The existing `DATA_SOURCES.md` claims **"99.5% developer coverage"** and **"Daily refresh"**.

**Reality:**
- Developer coverage ranges from **0% to 100%** depending on source
- Most data comes from **annual LBL historical file** (last updated January 2026)
- Live ISO refreshes are working but have significant data gaps

---

## DATA SOURCE INVENTORY

### Overview by Source

| Source | Projects | % of Total | Developer Coverage | Queue Date Coverage | Freshness |
|--------|----------|------------|-------------------|---------------------|-----------|
| **LBL Historical** | 25,242 | 71.1% | 94.4% | 100% (Excel serials) | Annual (Jan 2026) |
| **MISO API** | 3,706 | 10.4% | 99.0% | 100% | Live (Nov 2025) |
| **SPP (gridstatus)** | 2,592 | 7.3% | 60.9% | 100% | Live (Jan 2026) |
| **ERCOT GIS** | 1,875 | 5.3% | 100% | **0%** | Live (Dec 2024) |
| **ISO-NE (gridstatus)** | 1,601 | 4.5% | 74.0% | 100% | **Stale (Jan 2025)** |
| **CAISO Public** | 325 | 0.9% | 99.4% | 100% | **FROZEN (Apr 2021)** |
| **NYISO Direct** | 160 | 0.5% | 100% | 100% | Live (Dec 2025) |

**Total: 35,501 projects in V2 database**

---

## DETAILED SOURCE ANALYSIS

### 1. NYISO - New York ISO

| Attribute | Value |
|-----------|-------|
| **Source URL** | `https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx` |
| **Method** | Direct Excel download |
| **Projects** | 160 (active queue only) |
| **Developer Coverage** | 100% |
| **Queue Date Range** | 1970 - Dec 2025 |
| **Last Cache Update** | Feb 15, 2026 |
| **Status** | WORKING |

**Notes:**
- Best data quality of all sources
- Direct download, no scraping needed
- Only shows active projects (withdrawn not included)

---

### 2. ERCOT - Electric Reliability Council of Texas

| Attribute | Value |
|-----------|-------|
| **Source URL** | `https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=15933` (GIS Report) |
| **Method** | Download latest GIS report from MIS portal |
| **Projects** | 1,875 |
| **Developer Coverage** | 100% |
| **Queue Date Range** | **NONE - GIS report does not include queue dates** |
| **Last Cache Update** | Feb 15, 2026 |
| **Status** | PARTIAL - Missing queue dates |

**CRITICAL ISSUE:**
```
ERCOT GIS report columns: ['Queue ID', 'Project Name', 'Status', 'Developer',
'POI Location', 'County', 'CDR Reporting Zone', 'Proposed COD', 'Generation Type',
'Technology'...]

NO QUEUE DATE COLUMN EXISTS IN THE SOURCE FILE
```

Queue dates are backfilled from LBL historical data where available (71.3% coverage).

---

### 3. MISO - Midcontinent ISO

| Attribute | Value |
|-----------|-------|
| **Source URL** | `https://www.misoenergy.org/api/giqueue/getprojects` |
| **Method** | Direct JSON API |
| **Projects** | 3,706 |
| **Developer Coverage** | 99.0% |
| **Queue Date Range** | Apr 2015 - Nov 2025 |
| **Last Cache Update** | Feb 15, 2026 |
| **Status** | WORKING |

**Notes:**
- Best live API - returns full queue with developer data
- Developer field is `transmissionOwner` (actually the developer, not TO)
- Latest queue date is Nov 2025 - could be more current

---

### 4. CAISO - California ISO

| Attribute | Value |
|-----------|-------|
| **Source URL** | `http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx` |
| **Method** | Direct Excel download |
| **Projects** | 325 (active only) |
| **Developer Coverage** | 99.4% (but no developer names in file) |
| **Queue Date Range** | Nov 2003 - **April 2021** |
| **Last Cache Update** | Feb 15, 2026 |
| **Status** | CRITICAL - Queue dates frozen |

**CRITICAL ISSUE:**
```python
# Analysis of CAISO data:
Total rows: 330
Queue Position range: 22 to 2188  # Higher positions = newer projects
Latest queue date in file: 2021-04-15 07:00:00  # NEARLY 5 YEARS OLD

# Sample of newest projects by queue position:
Queue Pos   Queue Date           Project Name
2188        2021-04-15           GERANIUM ENERGY STORAGE
2186        2021-04-15           SANDBAR ENERGY STORAGE
2185        2021-04-15           GATEWAY ENERGY STORAGE 2
```

**Root Cause:** CAISO's public queue report (`PublicQueueReport.xlsx`) appears to have stopped updating queue dates after April 2021, even though the file itself is current (downloaded Feb 15, 2026). This means:
- All projects queued after April 2021 show the same frozen date
- Time-in-queue analysis for CAISO is impossible
- No way to distinguish projects by entry date

**Developer Coverage Note:** The CAISO public file does NOT contain developer names. The 99.4% developer coverage comes from EIA Form 860 matching or LBL backfill.

---

### 5. SPP - Southwest Power Pool

| Attribute | Value |
|-----------|-------|
| **Source URL** | `https://opsportal.spp.org/Studies/GIActive` (via gridstatus) |
| **Method** | gridstatus library |
| **Projects** | 2,592 |
| **Developer Coverage** | 60.9% |
| **Queue Date Range** | Sep 1999 - Jan 2026 |
| **Last Cache Update** | Feb 15, 2026 |
| **Status** | WORKING but low developer coverage |

**Issue:** gridstatus library returns developer data in `Interconnecting Entity` field, but only 60.9% populated. The rest have no developer information.

---

### 6. ISO-NE - ISO New England

| Attribute | Value |
|-----------|-------|
| **Source URL** | `https://irtt.iso-ne.com/` (via gridstatus) |
| **Method** | gridstatus library |
| **Projects** | 1,601 |
| **Developer Coverage** | 74.0% |
| **Queue Date Range** | Jun 1996 - **Jan 2025** |
| **Last Cache Update** | Feb 15, 2026 |
| **Status** | STALE - 13 months behind |

**ISSUE:**
```
Latest queue date in ISO-NE data: January 14, 2025
Current date: February 15, 2026
Gap: 13 MONTHS
```

No projects queued after January 2025 are in the data. Either:
1. gridstatus library has a bug/limitation
2. ISO-NE portal data export is lagging
3. The source isn't being refreshed properly

---

### 7. PJM - PJM Interconnection

| Attribute | Value |
|-----------|-------|
| **Source URL** | `https://api.pjm.com/api/v1/gen_queues` (requires API key) |
| **Method** | API (NOT CONFIGURED) |
| **Projects** | 8,152 (ALL from LBL) |
| **Developer Coverage** | 99.0% (LBL data) |
| **Queue Date Range** | Via LBL only |
| **API Key Status** | NOT OBTAINED |
| **Status** | CRITICAL - No live data |

**CRITICAL ISSUE:**
```
PJM cache files: ['pjm_costs_2022_clean_data.xlsx']  # Cost data only, no queue
PJM projects by source:
source  cnt
   lbl 8152  # 100% FROM LBL HISTORICAL
```

PJM has **8,152 projects** (23% of total) with **NO LIVE DATA SOURCE**. All data comes from LBL annual historical file. The code for PJM API exists in `direct_fetcher.py` but requires an API key from https://dataminer2.pjm.com/ which has never been obtained.

---

### 8. LBL Historical (Lawrence Berkeley Lab)

| Attribute | Value |
|-----------|-------|
| **Source URL** | `https://emp.lbl.gov/queues` (manual download) |
| **Method** | Manual Excel download |
| **Projects** | 36,441 (raw), 25,242 (after dedup in V2) |
| **Developer Coverage** | 94.4% |
| **Queue Date Range** | 1970 - Dec 2024 |
| **File Date** | January 14, 2026 |
| **Status** | WORKING but annual only |

**Important Context:**
- LBL "Queued Up" data is the **backbone of this database** (76% of records)
- Updated annually (typically in January after calendar year ends)
- Covers ALL US ISOs including West/Southeast non-ISO regions
- Queue dates are Excel serial numbers (need conversion)
- This is historical research data, not real-time queue data

---

### 9. West & Southeast (Non-ISO Regions)

| Attribute | Value |
|-----------|-------|
| **Source** | LBL Historical only |
| **Projects** | West: 6,008 / Southeast: 2,879 |
| **Developer Coverage** | West: 89.2% / Southeast: 78.2% |
| **Status** | No live source possible |

These regions have no centralized ISO queue. Data comes only from:
- LBL historical compilation
- EIA Form 860 matching
- Individual utility OASIS filings (not integrated)

---

## DATA QUALITY MATRIX

### Queue Date Availability

| Source | Has Queue Dates | Latest Queue Date | Gap from Today |
|--------|-----------------|-------------------|----------------|
| NYISO | Yes | Dec 2025 | ~2 months |
| MISO API | Yes | Nov 2025 | ~3 months |
| SPP | Yes | Jan 2026 | ~1 month |
| CAISO | **Frozen** | Apr 2021 | **~5 years** |
| ISO-NE | Yes | Jan 2025 | **~13 months** |
| ERCOT | **No** | N/A | **Never had dates** |
| PJM | Via LBL | Dec 2024 | ~14 months |

### Developer Name Availability

| Source | Coverage | Notes |
|--------|----------|-------|
| NYISO | 100% | `Developer/Interconnection Customer` field |
| ERCOT | 100% | `Interconnecting Entity` field |
| MISO | 99% | `transmissionOwner` field (misnamed) |
| CAISO | 0% | **Not in public file** - must match via EIA |
| SPP | 61% | gridstatus limitation |
| ISO-NE | 74% | gridstatus limitation |
| PJM | 99% | Via LBL only |
| LBL | 95% | Historical research compilation |

---

## CRITICAL ACTION ITEMS

### Immediate (Blocking Issues)

1. **CAISO Queue Dates**
   - **Problem:** Queue dates frozen at April 2021
   - **Impact:** Cannot analyze CAISO project timing
   - **Action:** Investigate alternative data sources:
     - CAISO OASIS filings
     - CAISO Resource Adequacy Portal
     - Direct CAISO contact for updated queue export
   - **Owner:** [TBD]

2. **PJM API Key**
   - **Problem:** 8,152 projects have no live data
   - **Impact:** PJM data is 14+ months stale
   - **Action:** Obtain API key from https://dataminer2.pjm.com/
   - **Owner:** [TBD]

3. **ISO-NE Data Gap**
   - **Problem:** 13-month gap in queue data
   - **Impact:** Missing all projects since Jan 2025
   - **Action:**
     - Investigate gridstatus library issue
     - Consider direct ISO-NE IRTT portal scraping
   - **Owner:** [TBD]

### High Priority

4. **ERCOT Queue Dates**
   - **Problem:** GIS report has no queue dates
   - **Impact:** Cannot track ERCOT project age
   - **Action:**
     - Continue LBL backfill for historical
     - Investigate ERCOT alternative reports
   - **Workaround:** Using LBL historical dates (71.3% coverage)

5. **Developer Coverage Gaps**
   - **Problem:** SPP (61%) and ISO-NE (74%) have gaps
   - **Impact:** Developer tracking incomplete
   - **Action:** Supplement with EIA Form 860 matching

### Medium Priority

6. **Documentation Accuracy**
   - **Problem:** `DATA_SOURCES.md` claims don't match reality
   - **Impact:** False confidence in data quality
   - **Action:** Update documentation to reflect actual state

7. **LBL Annual Dependency**
   - **Problem:** 76% of data is annual historical
   - **Impact:** Delayed project visibility
   - **Action:** Shift to live sources where possible

---

## COMPARISON: DOCUMENTATION VS REALITY

### From `DATA_SOURCES.md` (January 2026)

| Claim | Reality |
|-------|---------|
| "99.5% developer coverage" | **Varies 0-100% by source** |
| "Daily refresh" | **Most data is annual (LBL)** |
| "5/7 ISOs integrated" | **Only 3 have usable live data (NYISO, MISO, SPP)** |
| "CAISO: Needs integration" | **Integrated but data frozen since 2021** |
| "SPP: Needs integration" | **Integrated via gridstatus but 61% dev coverage** |
| "ISO-NE: Needs integration" | **Integrated but 13 months stale** |
| "PJM: Waiting on API key" | **Still waiting - 8,152 projects with no live data** |

### From `DATA_AUDIT.md` (January 2026)

| Claim | Status |
|-------|--------|
| "LBL data HIGH quality" | **Accurate but annual only** |
| "39,367 projects" | Now 35,501 in V2 (deduplication) |
| Completion rates audited | **Still valid** |
| Cost benchmarks audited | **Still valid** |

---

## RECOMMENDATIONS

### Short-Term (Next 30 Days)

1. Obtain PJM API key - single biggest impact
2. Investigate ISO-NE gridstatus issue
3. Contact CAISO about queue date issue
4. Update all documentation to reflect reality

### Medium-Term (Next Quarter)

5. Build direct scrapers for SPP/ISO-NE to bypass gridstatus
6. Implement EIA Form 860 developer backfill for gaps
7. Set up automated data quality monitoring
8. Create data freshness dashboard in app

### Long-Term

9. Establish direct relationships with ISO data contacts
10. Consider commercial data sources (S&P Global, etc.)
11. Build redundant data pipelines for reliability

---

## APPENDIX: RAW DATA ANALYSIS

### V1 Database Project Distribution

```
source    region  projects  dev_pct  queue_date_pct
lbl       PJM     8,152     99.0%    100%
lbl       West    6,008     89.2%    100%
lbl       MISO    4,978     99.5%    100%
miso_api  MISO    3,706     99.0%    100%
lbl       ERCOT   3,282     100%     100%
lbl       SE      2,879     78.2%    100%
lbl       CAISO   2,837     99.0%    100%
spp       SPP     2,820     56.0%    100%
lbl       SPP     2,469     99.7%    100%
ercot     ERCOT   1,875     100%     71.3%
lbl       NYISO   1,816     100%     100%
isone     ISO-NE  1,601     74.0%    100%
lbl       ISO-NE  1,281     99.4%    100%
caiso     CAISO   325       99.4%    100%
nyiso     NYISO   160       100%     100%
```

### V2 Database Status Distribution

```
source     total   active  withdrawn  completed  active_pct
lbl        25,242  4,104   17,426     3,401      16.3%
miso_api   3,706   1,109   2,027      570        29.9%
spp        2,592   279     2,069      17         10.8%
ercot      1,875   1,285   590        0          68.5%
isone      1,601   79      1,226      296        4.9%
caiso      325     322     3          0          99.1%
nyiso      160     145     6          9          90.6%
```

---

*Report generated by Claude Code, February 15, 2026*
*Based on actual database analysis, not documentation claims*
