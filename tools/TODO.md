# Queue Analysis - Outstanding Tasks

Last Updated: 2026-03-10

## High Priority

### System Maintenance

- [x] **Upgrade Claude Code**: `brew upgrade claude-code` ✓ (2.1.4 → 2.1.72, completed 2026-03-11)

### Manual Downloads Required

| Task | Source | URL | Save To |
|------|--------|-----|---------|
| [ ] CPUC RPS Excel | California CPUC | https://www.cpuc.ca.gov/rps_reports_data/ | `.cache/permits/california/cpuc_rps_raw.xlsx` |
| [ ] NREL SolarTRACE | NREL Data | https://data.nrel.gov/submissions/160 | `.cache/permits/nrel/solartrace_raw.xlsx` |
| [ ] PJM Planning Queues | PJM | https://www.pjm.com/planning/services-requests/interconnection-queues | `.cache/pjm_planning_queues.xlsx` |
| [ ] PJM Cycle Projects | PJM | PJM Transition Cycle reports | `.cache/pjm_cycle_projects.xlsx` |

### Data Quality Improvements

- [ ] **Improve permit-to-queue matching** (currently 58.6%)
  - Add project name fuzzy matching (currently only uses developer/location/capacity)
  - Consider adding POI matching tier
  - Review unmatched permits for patterns

- [ ] **Link NYSERDA to NYISO queue**
  - 186 NYSERDA projects have `interconnection_queue_number` field
  - Create linkage table or add `nyserda_project_id` to NYISO records
  - Would provide permit/contract status for NYISO projects

---

## Medium Priority

### Documentation Updates

- [ ] **Update data architecture diagram** (`tools/data_ingestion_diagram.html`)
  - Add permit data sources (EIA, CEC, CPUC, NYSERDA)
  - Add permit table schema
  - Document permit matching flow

- [ ] **Update CLAUDE.md** with permit refresh commands
  - Add `--source permits` to quick reference
  - Document new permitting-scrapers directory

### Data Integration

- [ ] **Review openclaw's merged dataset**
  - `data/master_projects_eia_full_enriched.csv` - 41,750 projects, 65 columns
  - `data/bess_permitting_data.csv` - 559 BESS projects
  - `data/bess_permitting_matched.csv` - 3,489 matched records
  - Determine what should be integrated into main database

- [ ] **Integrate interconnection.fyi data**
  - Already scraped by openclaw (38,951 records)
  - Contains permit milestones not in our database
  - Consider adding `interconnection_fyi_id` linkage

### New Data Sources to Evaluate

- [ ] **ERCOT queue dates** - Currently backfilling from LBL (71% coverage)
  - Check if ERCOT has newer public data with dates

- [ ] **CAISO queue dates** - Frozen at 2021 (known issue)
  - Monitor for CAISO API approval status

---

## Low Priority / Future

### Additional State Sources

| State | Source | Data Quality | Effort | Notes |
|-------|--------|--------------|--------|-------|
| TX | PUC Filings | Poor (PDFs) | High | Skip - document repository |
| FL | PSC Docket | Poor (PDFs) | High | Skip for now |
| AZ | ACC Filings | Medium | Medium | Could be valuable |
| NV | PUC | Medium | Medium | Solar-heavy state |

### Feature Enhancements

- [ ] **Permit timeline predictions**
  - Use SolarTRACE data as baseline for jurisdiction timelines
  - Build regression model based on state/tech/size

- [ ] **Developer permit history**
  - Track developer success rates through permitting
  - Add to developer track record in analytics

- [ ] **Automated PJM downloads**
  - Explore Selenium/Playwright for PJM queue downloads
  - Currently requires manual download

---

## Recurring Tasks

### Daily (Automated via Cron)

```bash
# Add to crontab: crontab -e
0 6 * * * cd /path/to/tools && python3 refresh_data.py --cron >> .data/refresh.log 2>&1
```

### Weekly (Manual Check)

| Day | Task | Command |
|-----|------|---------|
| Mon | Refresh permits | `python3 refresh_data.py --source permits` |
| Mon | Check PJM for updates | Manual download if new |
| Fri | Review changes | `python3 refresh_data.py --changes 7` |

### Monthly

- [ ] Check for new LBL Queued Up release (https://emp.lbl.gov/queues)
- [ ] Review CPUC RPS updates
- [ ] Check NREL SolarTRACE for new data

---

## Recently Completed

### 2026-03-10: Permitting Data Infrastructure

- [x] Added `permits` table to database schema
- [x] Built EIA Form 860 Proposed loader (2,408 permits)
- [x] Built California CEC loader (2,060 permits)
- [x] Built California CPUC loader (manual download)
- [x] Built NY NYSERDA loader (311 projects)
- [x] Built NREL SolarTRACE loader (manual download)
- [x] Built permit matcher (58.6% match rate)
- [x] Integrated with `refresh_data.py --source permits`

### Previous

- [x] Consolidated to single database (queue.db)
- [x] MISO public API loader with developer data
- [x] NYISO comprehensive loader (all 7 sheets)
- [x] NYISO historical analysis (132 snapshots)
- [x] Database status reporting tool

---

## Quick Commands

```bash
# Check database status
python3 db_status.py

# Refresh all data
python3 refresh_data.py --all

# Refresh just permits
python3 refresh_data.py --source permits

# Show recent changes
python3 refresh_data.py --changes 7

# Test individual loaders
cd permitting-scrapers
python3 california_cec_loader.py --stats
python3 nyserda_loader.py --stats
python3 eia_planned_loader.py --stats
```

---

## Notes

- **Texas PUC**: Deprioritized - it's a legal document filing system requiring PDF parsing
- **EIA Form 860**: Best federal source - status codes (P, L, T, U, V, TS) serve as permit proxies
- **NYSERDA**: Has direct queue number linkage to NYISO - high value for NY projects
- **SolarTRACE**: Residential rooftop data, but useful as jurisdiction permitting climate proxy
