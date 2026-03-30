# Queue Analysis - Project Instructions

## ⚠ COORDINATION REQUIRED

You are one of multiple Claude Code instances working on this codebase.

**Before starting work:**
1. Read `../../TODO.md` — check what's in progress, claim your tasks
2. Read `../../CHANGELOG.md` — see what other instances did recently
3. `git pull` — get latest changes

**Before ending your conversation:**
1. Append to `../../CHANGELOG.md` what you did (date, directory, summary)
2. Update `../../TODO.md` (mark done, add new tasks, note blockers)
3. Commit and push your work so other instances can see it

**If a task in TODO.md is marked `[~]` (in progress), do not work on it** — another instance is handling it.

**Instance naming:** Owen assigns you a name (Dev1, Dev2, Dev3…). Use it in all CHANGELOG/TODO entries. If you don't have a name yet, ask Owen.

---

## Overview

This is an **interconnection queue analysis platform** for power grid project development. It aggregates data from all major US ISOs (PJM, MISO, SPP, ERCOT, CAISO, NYISO, ISO-NE) plus non-ISO regions (West, Southeast) and provides analytics for evaluating project viability, tracking developers, and monitoring market trends.

### Current Database Stats (2026-03-25)

| Metric | Value |
|--------|-------|
| **Database** | **master.db** (golden record) |
| **Unique Projects** | **47,594** |
| Total Capacity | ~7,000+ GW |
| Size | 238 MB |
| Columns | 115 |
| Enrichment | Tax credits 78%, developer canonical 79%, EIA match 39%, parent company 28% |

Run `python3 db_status.py` for current stats anytime.

### Unique Projects by Region

| Region | Projects | Primary Sources |
|--------|----------|-----------------|
| CAISO | 10,397 | caiso, interconnection_fyi, lbl, ca_dg_stats (>=1MW) |
| PJM | 10,243 | pjm_direct, lbl, interconnection_fyi |
| West | 6,628 | lbl, interconnection_fyi |
| MISO | 5,514 | miso_api, lbl, interconnection_fyi |
| ERCOT | 3,871 | ercot, lbl, interconnection_fyi |
| Southeast | 3,178 | lbl, interconnection_fyi |
| NYISO | 3,008 | nyiso_direct, lbl, interconnection_fyi |
| SPP | 2,842 | spp, lbl |
| ISO-NE | 1,912 | isone, interconnection_fyi, ma_doer (>=1MW) |

### DG Database (dg.db)

| Metric | Value |
|--------|-------|
| **Total projects** | **4,843,018** |
| Size | 2.7 GB |
| Sources | 10 state/utility DG programs |
| DG stage enrichment | 247K classified (NJ + NY raw_status) |

See `../../DATABASES.md` for full inventory and `../../DG_STAGE_STRATEGY.md` for DG stage classification plan.

---

## Workspace Context

This project is part of the **End Suffering** workspace (Prospector Labs). For cross-project work, start Claude Code from the parent directory:

```bash
cd "/Users/owencoonahan/Documents/Grand Library/End Suffering"
# Then open Claude Code
```

**Parent workspace:** `../../CLAUDE.md`
**Agent coordination:** `../../.agents/` (Claude Code ↔ OpenClaw communication)

---

## Architecture

### Single Source of Truth

All utility-scale queue data is consolidated into **one database**: `master.db` (golden record). DG projects go to `dg.db`.

```
.data/master.db (238 MB, 115 columns)
├── projects              # 47,594 unique projects
├── project_sources       # Provenance tracking per source
├── snapshots             # Historical snapshots
├── changes               # Detected changes
├── qualified_developers  # 26 NYISO qualified devs
├── refresh_log           # Refresh history
└── developer_registry    # 6,593 canonical developers

.data/dg.db (2.7 GB)
├── projects              # 4,843,018 DG projects (<1MW)
└── [10 state/utility programs]

.data/grid.db (48 MB)
├── wind_turbines         # USWTDB
├── substations           # HIFLD
└── transmission_lines    # HIFLD
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

### Enrichment Tools (Category B — enrich existing rows, never create)

| File | Purpose | Coverage |
|------|---------|----------|
| `tax_credits.py` | ITC/PTC eligibility engine with bonus adders | 78% |
| `energy_community.py` | IRA energy community bonus zones | 73% |
| `low_income_community.py` | Low-income community bonus credit (DOE 48e) | 73% |
| `enrich_corporate.py` | Parent company from corporate.db | 28% |
| `enrich_ferc_epa.py` | FERC capex + EPA emissions cross-ref | 7-9% |
| `enrich_grid_context.py` | Wind turbine + substation context from grid.db | 42% |
| `enrich_utility_intelligence.py` | Utility financial data via EIA→PUDL→FERC | 41% |
| `eia_match_enhance.py` | EIA-860 plant ID matching | 39% |
| `developer_registry.py` | Developer name normalization (6,593 canonical) | 79% |
| `scoring.py` | Project viability scoring | All |
| `itc_deal_finder.py` | ITC deal sourcing engine (98K deals) | All |
| `validate_enrichments.py` | Cross-validation checks (8 checks) | — |

### DG Tools

| File | Purpose |
|------|---------|
| `nj_dg_scraper.py` | NJ Clean Energy 6-program scraper (247K projects, Playwright) |
| `ny_sun_loader.py` | NY-SUN via Socrata API (189K projects) |
| `dg_stage.py` | DG development stage classifier (applied→operational) |
| `dg_loader.py` | Other state DG loaders (CA, MA, CT, IL) |

### Supporting Files

| File | Purpose |
|------|---------|
| `developer_matcher.py` | Match developers across sources |
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
NYISO Excel (7 sheets) ───┼──→ refresh_data.py ──→ data_store.py ──→ master.db
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
| ISO-NE data was 13+ months stale | **Fixed** — IRTT scraper added | direct_fetcher.py scrapes IRTT directly |
| MISO API has no queue dates | API limitation | Use LBL for historical |
| PJM developer coverage low (18%) | Only in CycleProjects | LBL has 99% |

---

## Roadmap / Plan

### Completed
- [x] Consolidated to single golden record (master.db — 47,594 projects, 115 columns)
- [x] All 7 ISO loaders live (MISO, NYISO, PJM, ERCOT, CAISO, SPP, ISO-NE)
- [x] ISO-NE IRTT scraper (replaced stale gridstatus data)
- [x] NYISO historical analysis (132 snapshots)
- [x] Developer entity resolution (6,593 canonical developers, 79% coverage)
- [x] PJM auto-download via API key
- [x] Full enrichment suite (tax credits, energy community, low-income, corporate, FERC/EPA, grid context, utility intel)
- [x] DG database (dg.db — 4.8M projects, 10 state/utility programs)
- [x] DG stage classifier (NJ + NY raw_status → 7 standardized stages)
- [x] Railway deployment (10 DBs, 50+ API endpoints, APScheduler)
- [x] ITC deal finder engine (98K deals)
- [x] Validation framework (8 cross-checks)

### In Progress
- [ ] Investability tagging pipeline (construction stage + developer classification + composite score)
- [ ] Lat/lon geocoding (from substations, county centroids, or external sources)
- [ ] DG stage expansion (MA SMART, IL Shines, NY DPS SIR loaders)

### Future
- [ ] Real-time change notifications
- [ ] Report generation API
- [ ] Developer CRM/alerts
- [ ] Marketplace

---

## Code Style

- Python 3.12+
- Use existing patterns in codebase
- SQLite for storage via `data_store.py`
- Streamlit for UI
- Plotly for charts (Altair legacy in some files)
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
| Run enrichment only | `python3 refresh_data.py --enrich` |
| Tax credit check | `python3 tax_credits.py --check Solar 200 TX Pecos 2027` |
| Tax credit stats | `python3 tax_credits.py --stats` |
| Low-income check | `python3 low_income_community.py --check WV McDowell` |
| Refresh single source | `python3 refresh_data.py --source miso` |
| Run dashboard | `streamlit run app.py` |
| NYISO historical analysis | `python3 nyiso_historical_analysis.py --summary` |
| Update DATA_OVERVIEW.md | `python3 db_status.py --markdown` |
| View data architecture | `open tools/data_ingestion_diagram.html` |
| Deploy diagram to GH Pages | `cp tools/data_ingestion_diagram.html docs/data-architecture.html` |
