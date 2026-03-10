# Queue Analysis - Project Instructions

## Overview

This is an **interconnection queue analysis platform** for power grid project development. It aggregates data from all major US ISOs (PJM, MISO, SPP, ERCOT, CAISO, NYISO, ISO-NE) plus non-ISO regions (West, Southeast) and provides analytics for evaluating project viability, tracking developers, and monitoring market trends.

### Current Database Stats (2026-02-17)

| Metric | Value |
|--------|-------|
| **Unique Projects** | **36,612** |
| Total Records | 54,496 |
| Total Capacity | 6,563 GW |
| Developer Coverage | 94.7% |
| Queue Date Coverage | 97.3% |

Run `python3 db_status.py` for current stats anytime.

### Unique Projects by Region

| Region | Projects | Capacity | Developer % | Queue Date % |
|--------|----------|----------|-------------|--------------|
| PJM | 8,350 | 904 GW | 99% | 100% |
| West | 6,008 | 1,133 GW | 89% | 100% |
| MISO | 5,445 | 1,009 GW | 98% | 91% |
| ERCOT | 3,789 | 809 GW | 100% | 87% |
| Southeast | 2,879 | 652 GW | 78% | 100% |
| NYISO | 2,861 | 556 GW | 100% | 100% |
| SPP | 2,842 | 581 GW | 97% | 100% |
| CAISO | 2,837 | 710 GW | 99% | 100% |
| ISO-NE | 1,601 | 209 GW | 80% | 100% |

---

## Workspace Context

This project is part of the **World Domination** workspace. For cross-project work, start Claude Code from the parent directory:

```bash
cd "/Users/owencoonahan/Documents/World Domination"
# Then open Claude Code
```

**Parent workspace:** `../../CLAUDE.md`
**Agent coordination:** `../../.agents/` (Claude Code ↔ OpenClaw communication)

---

## Architecture

### Single Source of Truth

All queue data is consolidated into **one database**: `queue.db`

```
.data/queue.db (74 MB)
├── projects          # 54,496 records (36,612 unique)
├── snapshots         # 167,996 historical snapshots
├── changes           # 65,212 detected changes
├── qualified_developers  # 26 NYISO qualified devs
├── refresh_log       # Refresh history
└── [market tables]   # LMP, capacity, transmission, PPA, permits
```

### Single Entry Point

```bash
python3 refresh_data.py --all      # Refresh everything
python3 refresh_data.py --source miso   # Single source
python3 refresh_data.py --status   # Check refresh status
```

### Multi-Agent Setup

This codebase is accessed by two AI agents:
- **Laptop (Claude Code)**: Primary development, feature work, analysis
- **Mac Mini (24/7 Server)**: Automated data refresh, cron jobs, scraping

Both agents push/pull from GitHub. See `MULTI_AGENT_SETUP.md` for coordination details.

---

## File Structure

### Core Files (Use These)

```
tools/
├── refresh_data.py      # MAIN ENTRY POINT - orchestrates all data refreshes
├── data_store.py        # Database abstraction layer (upsert, query, snapshots)
├── unified_data.py      # Query interface for applications
├── data_validation.py   # Schema validation before ingestion
├── db_status.py         # Database status reports (run for current stats)
├── app.py               # Streamlit dashboard
└── analytics.py         # Consolidated analytics functions
```

### ISO Loaders (Called by refresh_data.py)

| File | ISO | Source | Auth | Records |
|------|-----|--------|------|---------|
| `miso_loader.py` | MISO | Public API | None | 3,706 |
| `nyiso_loader.py` | NYISO | Excel (7 sheets) | None | 2,020 |
| `pjm_loader.py` | PJM | Excel (manual download) | None | 8,287 |
| `direct_fetcher.py` | ERCOT, CAISO, SPP, ISO-NE | Various | None | 6,621 |

### Analysis Tools

| File | Purpose |
|------|---------|
| `nyiso_historical_analysis.py` | Track project progression across 132 monthly snapshots |
| `nyiso_section_scrapers.py` | Download NYISO planning documents (Gold Book, etc.) |
| `nyiso_section_ingest.py` | Parse qualified developers from PDFs |
| `scoring.py` | Project viability scoring algorithms |
| `intelligence.py` | Market intelligence module |

### Market Data Modules

| File | Data Type |
|------|-----------|
| `lmp_data.py` | Energy prices by zone |
| `capacity_data.py` | Capacity market prices, ELCC values |
| `transmission_data.py` | Congestion, constraints, upgrades |
| `ppa_data.py` | PPA benchmark prices |
| `permitting_data.py` | State permitting requirements |

### Supporting Files

| File | Purpose |
|------|---------|
| `developer_registry.py` | Developer name normalization |
| `developer_matcher.py` | Match developers across sources |
| `energy_community.py` | IRA bonus eligibility zones |
| `eia_loader.py` | EIA 860 generator data |
| `charts_altair.py` | Visualization components |

### Documentation

| File | Contents |
|------|----------|
| `ARCHITECTURE.md` | Canonical architecture, data flow diagrams |
| `DATA_OVERVIEW.md` | Comprehensive data stats (update with db_status.py --markdown) |
| `DATA_SOURCES.md` | Detailed source documentation |
| `ANALYTICS.md` | Analytics function documentation |

### Live Documentation (Vercel)

**Live Site**: https://docs-self-tau-68.vercel.app

| URL | File | Purpose |
|-----|------|---------|
| `/` | `docs/index.html` | Queue Explorer - public stats dashboard |
| `/data-architecture.html` | `docs/data-architecture.html` | Data Architecture diagram (endpoints, schemas, matching) |

**Source file**: `tools/data_ingestion_diagram.html` (development version)

**Deploy updates**: `cd docs && vercel --prod`

---

## Data Sources

### Live ISO Sources (Refreshed Automatically)

| Source | Type | URL/Method | Frequency |
|--------|------|------------|-----------|
| **MISO API** | JSON API | `misoenergy.org/api/giqueue/getprojects` | Daily |
| **NYISO** | Excel | 7 sheets (Active, Cluster, Withdrawn, In Service) | Daily |
| **ERCOT** | Excel | GIS Report | Weekly |
| **CAISO** | Excel | Public Queue Report | Daily |
| **SPP** | gridstatus | Python library | Daily |
| **ISO-NE** | gridstatus | Python library | Daily |

### Manual Downloads Required

| Source | File | Download From |
|--------|------|---------------|
| **PJM** | PlanningQueues.xlsx | pjm.com/planning/services-requests/interconnection-queues |
| **PJM** | CycleProjects-All.xlsx | PJM Transition Cycle reports |
| **LBL** | lbl_queued_up.xlsx | emp.lbl.gov/queues (annual) |

### Historical/Reference Data

| Source | Records | Purpose |
|--------|---------|---------|
| **LBL Queued Up** | 33,702 | Historical queue data, completion rates |
| **NYISO Historical** | 132 snapshots | Monthly queue snapshots 2014-2025 |
| **EIA 860** | ~25,000 | Generator inventory, ownership |
| **Energy Communities** | US counties | IRA bonus eligibility |
| **NYISO Planning Docs** | 63 files (302 MB) | Gold Book, reliability, qualified developers |

### Data Quality by Source

| Source | Developer % | Queue Date % | Notes |
|--------|-------------|--------------|-------|
| lbl | 99% | 100% | Historical baseline |
| miso_api | 96% | 0% | No queue dates in API |
| nyiso_direct | 100% | 100% | All 7 sheets |
| pjm_direct | 18% | 100% | Developer from CycleProjects only |
| ercot | 100% | 71% | Some dates backfilled from LBL |
| caiso | 99% | 100% | Dates frozen at 2021 (known issue) |
| spp | 56% | 100% | gridstatus |
| isone | 74% | 100% | gridstatus |

---

## Data Flow

```
DATA SOURCES                          ENTRY POINT                    DATABASE
─────────────────────────────────────────────────────────────────────────────

MISO Public API ──────────┐
  (3,706 projects)        │
                          │
NYISO Excel (7 sheets) ───┼──→ refresh_data.py ──→ data_store.py ──→ queue.db
  (2,020 projects)        │         │
                          │         ├── Normalizes columns
PJM Excel (manual) ───────┤         ├── Validates schema
  (8,287 projects)        │         ├── Detects changes
                          │         └── Creates snapshots
ERCOT GIS Report ─────────┤
  (1,875 projects)        │
                          │
CAISO / SPP / ISO-NE ─────┤
  (4,746 projects)        │
                          │
LBL Historical ───────────┘
  (33,702 projects)
                                           │
                                           ▼
                                    unified_data.py
                                           │
                                           ▼
                                        app.py
                                    (Streamlit Dashboard)
```

---

## Daily Operations

### Refresh Data
```bash
cd tools
python3 refresh_data.py --all           # Refresh all sources
python3 refresh_data.py --source miso   # Single source
python3 refresh_data.py --status        # Check last refresh times
python3 refresh_data.py --changes 7     # Show changes in last 7 days
```

### Check Database Status
```bash
python3 db_status.py                    # Full report
python3 db_status.py --brief            # One-liner
python3 db_status.py --markdown         # For documentation
```

### Run Dashboard
```bash
streamlit run app.py
```

### Manual PJM Update
```bash
# 1. Download files from PJM website
# 2. Copy to cache:
cp ~/Downloads/PlanningQueues.xlsx .cache/pjm_planning_queues.xlsx
cp ~/Downloads/CycleProjects-All.xlsx .cache/pjm_cycle_projects.xlsx
# 3. Refresh:
python3 refresh_data.py --source pjm
```

---

## Analytics Infrastructure

**IMPORTANT**: All analytics logic lives in `analytics.py`. Do NOT create scattered analysis functions elsewhere.

### QueueAnalytics Class - Complete Reference

```python
from analytics import QueueAnalytics
qa = QueueAnalytics()
```

### Tier 1: Feasibility Analysis

| Function | Purpose | Key Output |
|----------|---------|------------|
| `get_completion_probability(region, tech, mw)` | Historical completion rates | `combined_rate`, `confidence` |
| `get_developer_track_record(developer, region)` | Developer history + EIA verification | `completed`, `withdrawn`, `completion_rate` |
| `get_poi_congestion_score(poi, region, project_id)` | Queue depth at POI | `risk_level`, `projects_ahead` |
| `get_cost_percentile(region, tech, mw, cost)` | Cost ranking vs historicals | `project_percentile`, `histogram` |
| `get_timeline_benchmarks(region, tech)` | Time to COD estimates | `p25/p50/p75_months` |
| `get_ira_eligibility(state, county)` | IRA bonus eligibility | `eligible`, `bonus_adder` |

### Tier 2: Revenue & Market Analysis

| Function | Purpose | Key Output |
|----------|---------|------------|
| `get_revenue_estimate(region, tech, mw, zone)` | Annual energy revenue | `annual_revenue_millions`, `avg_lmp` |
| `get_capacity_value(region, tech, mw, year)` | Capacity market value | `annual_value_millions`, `elcc_percent` |
| `get_transmission_risk(region, zone, poi)` | Congestion risk scoring | `risk_rating`, `avg_congestion_cost` |
| `get_ppa_benchmarks(region, tech, year)` | PPA price ranges | `price_low/mid/high`, `trend` |
| `get_full_revenue_stack(region, tech, mw)` | Combined revenue estimate | `total_revenue_millions`, `revenue_per_kw` |

### Convenience Method

```python
# Get ALL analytics for a project in one call
analysis = qa.get_project_analysis(
    project_id='J1234',
    region='PJM',
    technology='Solar',
    capacity_mw=200,
    developer='NextEra',
    poi='Smithburg 345kV',
    state='PA',
    county='Cambria',
    include_tier2=True  # Include revenue/capacity/transmission
)
```

### Sample Outputs

**Completion Probability (PJM Solar 200MW):**
```python
{
    'region_rate': 0.192,        # 19.2% PJM completion rate
    'technology_rate': 0.109,    # 10.9% Solar completion rate
    'combined_rate': 0.144,      # 14.4% weighted average
    'confidence': 'high',        # Based on 4,501 samples
    'sample_size': 4501
}
```

**Revenue Stack (PJM Solar 200MW):**
```python
{
    'energy_revenue_millions': 18.4,     # $18.4M/year energy
    'capacity_revenue_millions': 6.9,    # $6.9M/year capacity
    'ancillary_revenue_millions': 0.55,  # $0.55M/year ancillary
    'total_revenue_millions': 25.85,     # $25.85M/year total
    'revenue_per_kw': 129,               # $129/kW-year
    'energy_pct': 71.2,                  # 71% from energy
    'capacity_pct': 26.7,                # 27% from capacity
}
```

**Developer Track Record:**
```python
{
    'total_projects': 82,
    'completed': 13,
    'withdrawn': 31,
    'completion_rate': 0.295,    # 29.5%
    'completed_mw': 2101,
    'eia_verified_plants': 8,    # Cross-referenced with EIA 860
    'assessment': 'Good track record: 13 completed, 31 withdrawn'
}
```

### Data Sources

| Analytics | Primary Source | Records | Quality |
|-----------|---------------|---------|---------|
| Completion rates | LBL Queued Up | 36,441 | Production |
| Developer history | LBL + EIA 860 | 50,000+ | Production |
| Cost benchmarks | LBL IC Costs | 690-1800/region | Production |
| LMP prices | Benchmarks | 35 zones | Benchmark |
| Capacity prices | Benchmarks | 63 records | Benchmark |
| Transmission | Benchmarks | 64 zones | Benchmark |
| PPA prices | Benchmarks | 105 records | Benchmark |

### CLI Usage

```bash
# Test analytics from command line
python3 analytics.py --completion PJM Solar 200
python3 analytics.py --developer "NextEra" --region PJM
python3 analytics.py --poi "Dayton" PJM
python3 analytics.py --stats --region PJM
```

### PDF Report Generation

```python
from reports import generate_deal_report

# Generate comprehensive deal report (auto-numbered if no output_path)
pdf_path = generate_deal_report(
    project_id='AB2-037',
    client_name='Acme Capital'
)
# Creates: reports/output/report_007_AB2-037_PJM.pdf
```

**Report Numbering**: Reports are auto-numbered sequentially (001, 002, etc.) when no `output_path` is specified. Counter stored in `reports/output/.report_counter`.

Report includes:
- Executive summary with score gauge
- Completion probability analysis
- Developer track record with actual projects
- Cost percentile ranking with histogram
- Timeline benchmarks
- Revenue stack (energy + capacity + ancillary)
- Transmission risk assessment
- IRA eligibility status
- Comparable project outcomes

---

## Known Data Issues

| Issue | Status | Workaround |
|-------|--------|------------|
| CAISO queue dates frozen at 2021 | Known CAISO issue | Using LBL dates |
| ERCOT no queue dates | Policy decision | Backfill from LBL |
| ISO-NE data 13+ months stale | gridstatus lag | None |
| MISO API has no queue dates | API limitation | Use LBL for historical |
| PJM developer coverage low (18%) | Only in CycleProjects | LBL has 99% |

---

## Roadmap / Plan

### Completed
- [x] Consolidated to single database (queue.db)
- [x] MISO public API loader
- [x] NYISO comprehensive loader (all 7 sheets)
- [x] NYISO historical analysis (132 snapshots)
- [x] NYISO planning document scrapers
- [x] Qualified developers parsing and ingestion
- [x] Database status reporting tool

### In Progress
- [ ] Improve CAISO queue date freshness (awaiting API approval)
- [ ] Automate PJM downloads
- [ ] Developer entity resolution across sources

### Future
- [ ] Real-time change notifications
- [ ] ML-based completion probability
- [ ] Cost estimation models
- [ ] Permit timeline predictions

---

## Code Style

- Python 3.12+
- Use existing patterns in codebase
- SQLite for storage via `data_store.py`
- Streamlit for UI
- Altair for charts
- Always validate data before ingestion

## Agent Coordination Rules

### Analytics Changes (Laptop Only)
- Only the Laptop agent modifies `analytics.py`
- Always update `ANALYTICS.md` when adding new functions

### Data Refresh (Mac Mini Only)
- Mac Mini runs scheduled data refreshes
- Check `refresh_data.py --status` before triggering manual refreshes

### Data Architecture Diagram Updates (IMPORTANT)
**When to update `tools/data_ingestion_diagram.html`:**
- Adding a new data source or ISO loader
- Changing API endpoints or URLs
- Modifying database schema (new tables, columns)
- Adding new enrichment sources (EIA, FERC, etc.)
- Changing cache file paths or formats
- Updating column mappings between sources and database

**How to update:**
1. Edit `tools/data_ingestion_diagram.html` (development version)
2. Copy to `docs/data-architecture.html` for GitHub Pages
3. Commit both files together

**What's documented in the diagram:**
- All ISO endpoints with exact URLs
- Cache file paths (`.cache/` directory)
- Database table mappings
- Column mappings (source → normalized)
- Enrichment matching logic (EIA tiers, fuzzy matching thresholds)
- Parser file locations with line numbers

### Code Changes Protocol
1. Pull latest from GitHub before starting
2. Run `python3 db_status.py --brief` to understand current state
3. Make changes in appropriate files
4. Test locally
5. Commit with clear message: `feat(loader): add X` or `fix(validation): Y`
6. Push to GitHub

---

## Quick Reference

| Task | Command |
|------|---------|
| Check DB status | `python3 db_status.py` |
| Refresh all data | `python3 refresh_data.py --all` |
| Refresh single source | `python3 refresh_data.py --source miso` |
| Run dashboard | `streamlit run app.py` |
| NYISO historical analysis | `python3 nyiso_historical_analysis.py --summary` |
| Update DATA_OVERVIEW.md | `python3 db_status.py --markdown` |
| View data architecture | `open tools/data_ingestion_diagram.html` |
| Deploy diagram to GH Pages | `cp tools/data_ingestion_diagram.html docs/data-architecture.html` |
