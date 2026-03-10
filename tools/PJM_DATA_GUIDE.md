# PJM Data Ingestion Guide

**Last Updated:** February 2026
**Author:** Claude Code

---

## Overview

PJM interconnection queue data is ingested from two Excel files downloaded from PJM's website. Unlike other ISOs, PJM requires manual file downloads (no public API without registration).

### Data Sources

| File | Source URL | Records | Key Data |
|------|------------|---------|----------|
| **PlanningQueues.xlsx** | PJM Queue Page | ~8,200+ | Full queue history, status, dates, capacity |
| **CycleProjects-All.xlsx** | PJM Transition Cycle | ~1,000 | Developer names (100% coverage) |

---

## Step 1: Download PJM Data Files

### PlanningQueues.xlsx (Required)

1. Go to: **https://www.pjm.com/planning/services-requests/interconnection-queues**
2. Look for the **"Queue (Excel)"** download link
3. Download the file
4. Rename to `pjm_planning_queues.xlsx`
5. Copy to: `tools/.cache/pjm_planning_queues.xlsx`

### CycleProjects-All.xlsx (Optional but Recommended)

This file contains developer names that aren't in the main queue file.

1. Go to: **https://www.pjm.com/planning/services-requests/interconnection-queues**
2. Look for **Transition Cycle** reports section
3. Download the "All Projects" or "Cycle Projects" Excel file
4. Rename to `pjm_cycle_projects.xlsx`
5. Copy to: `tools/.cache/pjm_cycle_projects.xlsx`

---

## Step 2: Verify Files

```bash
cd tools
ls -la .cache/pjm*.xlsx
```

Expected output:
```
pjm_planning_queues.xlsx   # ~1.5 MB, 8,000+ projects
pjm_cycle_projects.xlsx    # ~200 KB, 900+ projects (optional)
```

---

## Step 3: Run PJM Loader

### Option A: Load and View Stats Only

```bash
cd tools
source .venv/bin/activate
python3 pjm_loader.py --stats
```

Expected output:
```
File Status:
  planning_queues: FOUND
  cycle_projects: FOUND

Loading PJM data...
  Loaded 8,241 projects from PlanningQueues.xlsx
  Loaded 966 projects from CycleProjects.xlsx (with developers)
  Matched 1,485 developers by name

PJM Data Statistics:
  Total projects: 8,287
  Developer coverage: 18.5%
  Queue date coverage: 99.5%
  Total capacity: 651.0 GW
  Active projects: 1,429
  Active capacity: 104.9 GW
  Latest queue date: 2026-02-13
```

### Option B: Load and Update Database

```bash
python3 pjm_loader.py --refresh
```

This will:
1. Load both Excel files
2. Merge developer data where names match
3. Normalize to standard schema
4. Upsert all projects to the V1 database (`queue.db`)

### Option C: Export to CSV for Review

```bash
python3 pjm_loader.py --export pjm_export.csv
```

---

## Step 4: Rebuild V2 Database

After loading PJM data into V1, rebuild the normalized V2 database:

```bash
python3 refresh_v2.py --quick
```

The `--quick` flag skips re-downloading from other ISOs and just rebuilds V2 from V1.

---

## Step 5: Verify Integration

```bash
python3 refresh_data.py --status
```

Check that PJM shows updated counts:
```
By region:
  PJM: 8,350 projects
  ...
```

---

## Full Refresh (All ISOs Including PJM)

To refresh everything at once:

```bash
python3 refresh_data.py
```

Or refresh only PJM:

```bash
python3 refresh_data.py --source pjm
```

---

## File Structure

```
tools/
├── pjm_loader.py              # PJM data loading module
├── refresh_data.py            # Main refresh orchestrator (includes PJM)
├── refresh_v2.py              # V2 database rebuilder
├── .cache/
│   ├── pjm_planning_queues.xlsx    # Downloaded PJM queue file
│   └── pjm_cycle_projects.xlsx     # Downloaded PJM cycle file (optional)
└── .data/
    ├── queue.db               # V1 database (raw data)
    └── queue_v2.db            # V2 database (normalized)
```

---

## Data Schema

### PlanningQueues.xlsx Columns

| PJM Column | Our Schema | Notes |
|------------|------------|-------|
| Project ID | queue_id | e.g., "A01", "AF2-010" |
| Name | name | Project name |
| Commercial Name | commercial_name | Marketing name |
| State | state | Two-letter code |
| County | county | County name |
| Status | status | Active, Withdrawn, In Service, etc. |
| MW Capacity | capacity_mw | Primary capacity value |
| MW Energy | mw_energy | Fallback if MW Capacity missing |
| Fuel | type | Solar, Wind, Storage, etc. |
| Submitted Date | queue_date | **This is the queue entry date** |
| Projected In Service Date | cod | Commercial operation date |
| Withdrawal Date | withdrawal_date | If withdrawn |

### CycleProjects-All.xlsx Columns

Same as above, plus:
| PJM Column | Our Schema | Notes |
|------------|------------|-------|
| Developer | developer | **Developer/owner name** |
| Cycle | cycle | TC1, TC2, C01 (Transition Cycle) |

---

## Status Mapping

| PJM Status | Our Status |
|------------|------------|
| Active | Active |
| Engineering and Procurement | Active |
| EP | Active |
| Under Construction | Active |
| Partially in Service - Under Construction | Active |
| Suspended | Suspended |
| Withdrawn | Withdrawn |
| Deactivated | Withdrawn |
| Canceled | Withdrawn |
| In Service | Operational |

---

## Troubleshooting

### "PJM PlanningQueues.xlsx not found"

Download the file from PJM's website and save to `.cache/pjm_planning_queues.xlsx`

### Low Developer Coverage

The main queue file doesn't include developer names. To improve coverage:
1. Download CycleProjects-All.xlsx (has 100% developer coverage)
2. The loader automatically matches by project name

### Duplicate Projects After Refresh

The V2 rebuild deduplicates by (queue_id, region). If you see duplicates in V1, that's expected - V2 handles deduplication.

### Queue Dates Not Showing in V2

The V2 rebuild prefers records with more complete data. If LBL records are being preferred over pjm_direct, the dates may not transfer. Run a full rebuild:

```bash
python3 refresh_v2.py
```

---

## Data Quality Notes

### What PJM Data Provides

- **99.5% queue date coverage** - "Submitted Date" is reliable
- **Full status history** - Including withdrawn projects
- **Geographic data** - State and county for all projects
- **Capacity data** - MW Energy and MW Capacity
- **Study phase tracking** - Feasibility, SIS, Facilities Study status

### What PJM Data Lacks (Without Cycle File)

- **Developer names** - Not in main queue file
- Must be matched from CycleProjects or enriched from EIA

### Comparison to Other ISOs

| ISO | Queue Date | Developer | Method |
|-----|------------|-----------|--------|
| PJM | 99.5% | 18.5% | Manual Excel |
| MISO | 100% | 99% | Direct API |
| NYISO | 100% | 100% | Direct Excel |
| ERCOT | 0% | 100% | GIS Report |
| CAISO | 100%* | 0% | Direct Excel |
| SPP | 100% | 61% | gridstatus |
| ISO-NE | 100% | 74% | gridstatus |

*CAISO dates frozen at April 2021

---

## Automation Notes

### Cron Job Setup

PJM files must be downloaded manually, but you can automate the processing:

```bash
# After manually updating files, run:
0 7 * * * cd /path/to/tools && python3 pjm_loader.py --refresh && python3 refresh_v2.py --quick
```

### PJM API (Future)

PJM has a Data Miner 2 API that could automate downloads:
- URL: `https://api.pjm.com/api/v1/gen_queues`
- Requires API key from: https://dataminer2.pjm.com/

The code for API access exists in `direct_fetcher.py::fetch_pjm()` but requires obtaining an API key.

---

## Change Log

| Date | Change |
|------|--------|
| 2026-02-17 | Initial PJM loader created |
| 2026-02-17 | Integrated into refresh_data.py |
| 2026-02-17 | Added CycleProjects developer matching |

---

*Guide created by Claude Code*
