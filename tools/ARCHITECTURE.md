# Queue Analysis - Data Architecture

## The Problem (Before)

We had fragmented data across multiple databases and loaders:

```
5 DATABASES:
├── queue.db          # Main (but incomplete)
├── queue_v2.db       # Duplicate normalized schema
├── enrichment.db     # Developer matching (separate)
├── nyiso_sections.db # NYISO docs (orphaned)
└── pe_firms.db       # PE tracking (orphaned)

8+ LOADERS:
├── direct_fetcher.py      # NYISO, CAISO
├── miso_loader.py         # MISO API
├── nyiso_loader.py        # NYISO comprehensive
├── pjm_loader.py          # PJM Excel
├── eia_loader.py          # EIA data
├── nyiso_historical_*.py  # 3 NYISO-specific files
└── ...more

RESULT: Inconsistent data, unclear entry points, duplicate records
```

## The Solution (Canonical Architecture)

### Single Source of Truth

```
ONE DATABASE: queue.db
├── projects          # 52K interconnection queue projects
├── snapshots         # Historical snapshots for change tracking
├── changes           # Detected changes over time
├── developers        # Developer registry (consolidated)
├── [market tables]   # LMP, capacity, transmission, PPA, permits
└── [reference]       # Qualified developers, planning docs metadata
```

### Single Entry Point

```bash
# ONE command to refresh everything
python3 refresh_data.py --all

# Or specific sources
python3 refresh_data.py --source miso
python3 refresh_data.py --source nyiso
python3 refresh_data.py --source pjm
```

### Loader Hierarchy

```
refresh_data.py (ORCHESTRATOR)
    │
    ├── Queue Data Sources
    │   ├── miso_loader.py      → MISO API (live)
    │   ├── nyiso_loader.py     → NYISO Excel (7 sheets)
    │   ├── pjm_loader.py       → PJM Excel (manual download)
    │   ├── direct_fetcher.py   → CAISO, ERCOT direct downloads
    │   └── [gridstatus]        → SPP, ISO-NE
    │
    ├── Historical/Reference
    │   ├── lbl_queued_up.xlsx  → Annual LBL data
    │   └── nyiso_historical/   → 132 NYISO snapshots (analysis only)
    │
    ├── Market Data
    │   ├── lmp_data.py
    │   ├── capacity_data.py
    │   ├── transmission_data.py
    │   └── ppa_data.py
    │
    └── Enrichment
        ├── eia_loader.py       → Generator data
        └── energy_community.py → IRA bonus zones
            │
            ▼
        data_store.py → queue.db
```

## File Roles (Canonical)

### Core (Use These)

| File | Purpose | Run Via |
|------|---------|---------|
| `refresh_data.py` | **Main entry point** - orchestrates all refreshes | `python3 refresh_data.py --all` |
| `data_store.py` | Database abstraction layer | Imported by loaders |
| `unified_data.py` | Query interface for applications | Imported by app.py |

### ISO Loaders (Called by refresh_data.py)

| File | ISO | Data Source | Status |
|------|-----|-------------|--------|
| `miso_loader.py` | MISO | Public API | Active |
| `nyiso_loader.py` | NYISO | Excel (7 sheets) | Active |
| `pjm_loader.py` | PJM | Excel (manual) | Active |
| `direct_fetcher.py` | CAISO, ERCOT | Direct downloads | Active |

### Analysis Tools (Standalone)

| File | Purpose | Output |
|------|---------|--------|
| `nyiso_historical_analysis.py` | Track project progression | Reports only |
| `nyiso_section_scrapers.py` | Download planning docs | .cache/ files |
| `scoring.py` | Project viability scoring | In-memory |

### Deprecated/Archive

| File | Status | Reason |
|------|--------|--------|
| `refresh_v2.py` | Deprecated | Use queue.db, not queue_v2.db |
| `nyiso_section_ingest.py` | Merge into nyiso_loader.py | Orphaned database |
| `nyiso_historical_downloader.py` | Keep for downloads | Analysis tool |
| `oasis_fetcher.py` | Low priority | CAISO market data |

## Database Schema (queue.db)

### Core Tables

```sql
projects (52K rows)
├── queue_id        -- ISO-specific project ID
├── name            -- Project name
├── developer       -- Developer/owner name
├── capacity_mw     -- Summer capacity
├── type            -- Technology (Solar, Wind, etc.)
├── status          -- Active, Withdrawn, Operational
├── region          -- ISO region
├── state, county   -- Location
├── queue_date      -- Date entered queue
├── cod             -- Commercial operation date
├── source          -- Data source (miso_api, nyiso, lbl, etc.)
└── created_at, updated_at

snapshots (168K rows)
├── snapshot_date   -- Date of snapshot
├── queue_id, region
├── [project fields]
└── row_hash        -- For change detection

changes (63K rows)
├── detected_at
├── queue_id, region
├── change_type     -- new, updated, status_change
├── field_name      -- Which field changed
├── old_value, new_value
└── project_name
```

### Reference Tables

```sql
qualified_developers  -- From NYISO (26 records)
├── name
├── region
├── qualification_date
└── source

planning_documents   -- Metadata for downloaded docs
├── section          -- gold_book, reliability, etc.
├── filename
├── file_path
├── download_date
└── size_kb
```

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                            │
├─────────────┬─────────────┬─────────────┬──────────────────┤
│ MISO API    │ NYISO Excel │ PJM Excel   │ CAISO/ERCOT      │
│ (live)      │ (7 sheets)  │ (manual)    │ (direct)         │
└──────┬──────┴──────┬──────┴──────┬──────┴────────┬─────────┘
       │             │             │               │
       ▼             ▼             ▼               ▼
┌─────────────────────────────────────────────────────────────┐
│                   refresh_data.py                           │
│  • Calls each loader                                        │
│  • Normalizes data                                          │
│  • Validates before insert                                  │
│  • Creates snapshots                                        │
│  • Detects changes                                          │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   data_store.py                             │
│  • upsert_projects()                                        │
│  • create_snapshot()                                        │
│  • detect_changes()                                         │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                      queue.db                               │
│  • projects (52K)                                           │
│  • snapshots (168K)                                         │
│  • changes (63K)                                            │
│  • market data tables                                       │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   unified_data.py                           │
│  • UnifiedQueue.search()                                    │
│  • RegionalBenchmarks                                       │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                       app.py                                │
│  • Streamlit dashboard                                      │
│  • Project search                                           │
│  • Analytics                                                │
└─────────────────────────────────────────────────────────────┘
```

## Quick Reference

### Daily Operations

```bash
# Refresh all queue data
python3 refresh_data.py --all

# Check data status
python3 refresh_data.py --status

# View recent changes
python3 refresh_data.py --changes
```

### Manual Downloads (Required)

```bash
# PJM - Download from website, then:
python3 pjm_loader.py --refresh

# NYISO historical (one-time)
python3 nyiso_historical_downloader.py --download
```

### Analysis

```bash
# NYISO historical tracking
python3 nyiso_historical_analysis.py --summary
python3 nyiso_historical_analysis.py --track Q1234

# Data validation
python3 data_validation.py --full-report
```

### Dashboard

```bash
streamlit run app.py
```
