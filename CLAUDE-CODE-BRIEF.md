# Claude Code Brief — Prospector Labs Data Infrastructure
*Updated: 2026-03-10 2:23 PM EDT*

---

## What Is Prospector Labs?

Prospector Labs builds **data infrastructure for critical technologies** — primarily energy/grid, critical minerals, and manufacturing. The core product is interconnection queue intelligence: tracking every energy project waiting for grid access across all US ISOs.

**Business model:**
- Short-term revenue: PE due diligence reports + developer data subscriptions
- Long-term: API platform (per-request pricing) + MCP server for agent discovery
- Target customers: PE firms, energy developers, utilities, consultants, lenders

**Live site:** https://prospectorlabs.io/
**API (broken):** https://prospector-api.fly.dev/ (DuckDB files missing on Fly.io — all 88 endpoints return 500)
**GitHub:** github.com/OwenCoonahan/prospector-data-tools

---

## Current Data Assets

### Master Interconnection Dataset
- **Location:** `/workspace/tools/master-interconnection-dataset/`
- **File:** `master_projects_eia_full_enriched.csv` (latest, most enriched)
- **Records:** 41,750 unique projects
- **Sources merged:**
  1. Owen's original queue database (~35K projects, 26 fields)
  2. interconnection.fyi scrape (58,643 projects, 24 fields) — scraped March 9
  3. EIA Form 860 operating generators (27,720 records, matched 19,124)
  4. EIA Form 860 proposed BESS (559 records, 307 permitted)

**Field coverage:**
| Field | Coverage |
|-------|----------|
| status | 100% |
| region/ISO | 100% |
| capacity_mw | 97.9% |
| state | 97.7% |
| county | 91.8% |
| type (generation/load/transmission) | 90.6% |
| point_of_interconnection | 85.8% |
| transmission_owner | 83.7% |
| name | 78.2% |
| developer | 77.7% |
| proposed_completion_date | 71.0% |
| EIA enrichment (status, technology, etc.) | 45.8% |
| interconnecting_entity | 14.5% |

**Status breakdown:**
- Active: 13,806
- Withdrawn: 34,984
- Operational: 8,549
- Suspended: 916

**Generation type breakdown (active projects):**
- Solar: 6,280
- Battery/Storage: 5,487
- Wind: ~3,000
- Gas: ~1,500
- Other: ~1,500

### BESS Permitting Dataset
- **Location:** `/workspace/tools/master-interconnection-dataset/bess_permitting_data.csv`
- **Records:** 559 proposed BESS projects from EIA Form 860
- **Permitted (L/T/U/TS status):** 307 projects (74.6 GW pipeline)
- **Planned (P status):** 252 projects
- **Matched to master dataset:** 3,489 entries (`bess_permitting_matched.csv`)
- **Top states:** TX (191), CA (137)

**EIA Status Codes (important — these are permitting proxies):**
| Code | Meaning | What it tells us |
|------|---------|-----------------|
| P | Planned | Early stage, regulatory filing submitted |
| L | Regulatory Approved | Approved but construction NOT started |
| T | Regulatory Approved | Approved AND construction started |
| U | Under Construction | Actively being built |
| TS | Testing/Commissioning | Almost operational, testing phase |
| OP | Operating | Built and running |
| SB | Standby | Operational but not currently producing |
| OS | Out of Service | Temporarily offline |
| RE | Retired | Permanently shut down |

### Raw Data Files
- **Scraper output:** `/workspace/tools/interconnection-fyi-scraper/projects.csv` (58,643 rows)
- **EIA raw (operating):** `/workspace/tools/master-interconnection-dataset/eia_860_full.csv` (27,720 rows)
- **EIA raw (BESS proposed):** `/workspace/tools/master-interconnection-dataset/bess_permitting_data.csv` (559 rows)
- **Merge report:** `/workspace/tools/master-interconnection-dataset/MERGE_REPORT.md`
- **BESS report:** `/workspace/tools/master-interconnection-dataset/BESS_PERMITTING_REPORT.md`

---

## Current Priorities (March 10, 2026)

### Priority 1: Permitting Data Infrastructure
**Goal:** Build scrapers to collect permitting data from state and federal sources, creating the most comprehensive permitting dataset for energy projects in the US.

**Why:** A prospect (POSH Energy / Harshwardhan Wadikar) is specifically looking for permitted clean tech projects ready for investment. Permitting data is our biggest gap and biggest opportunity — nobody has this aggregated.

**The project lifecycle we're tracking:**
```
1. Queue application ←── WE HAVE THIS (41,750 projects)
2. Feasibility study ←── Partially in queue data
3. System impact study ←── Partially in queue data  
4. Facilities study ←── Partially in queue data
5. Interconnection agreement signed ←── Some ISOs report this
6. LOCAL permits (county/city building) ←── WE DON'T HAVE — hardest to get
7. STATE permits (PUC approval) ←── NEED TO BUILD SCRAPERS
8. FEDERAL permits (NEPA, if federal land) ←── EPA ECHO not useful for BESS
9. Regulatory approved ←── EIA status "L" or "T" — WE HAVE FOR SOME
10. Under construction ←── EIA status "U" — WE HAVE FOR SOME
11. Testing ←── EIA status "TS" — WE HAVE FOR SOME
12. Operational ←── EIA status "OP" — WE HAVE
```

**Scraper infrastructure to build:**
```
/tools/permitting-scrapers/
├── eia/                    ← API-based (partially done)
│   ├── pull_operating.py   ← Done (27,720 records)
│   ├── pull_proposed.py    ← Done for BESS (559 records), need ALL types
│   └── pull_planned.py     ← TODO: Find correct EIA endpoint for planned generators
├── state-puc/
│   ├── texas.py            ← PUC of Texas docket scraper (PRIORITY — 191 BESS projects)
│   ├── california.py       ← CPUC + CEC scraper (PRIORITY — 137 BESS projects)
│   ├── new_york.py         ← NY DPS scraper
│   ├── virginia.py         ← VA SCC scraper
│   ├── illinois.py         ← IL ICC scraper
│   └── README.md           ← Source URLs, data dictionaries per state
├── ferc/
│   └── elibrary.py         ← FERC eLibrary scraper (construction permits, EIS)
├── federal/
│   ├── blm.py              ← Bureau of Land Management permits (Western US)
│   └── usfs.py             ← US Forest Service permits
├── scheduler.py            ← Cron-based re-scraping on cadence
├── merge.py                ← Merges all sources into master dataset
└── README.md               ← Architecture, data dictionary, source docs
```

**Re-scraping cadence:**
- EIA 860: Monthly (they update monthly)
- State PUCs: Weekly (filings happen frequently)
- FERC: Weekly
- Queue data (interconnection.fyi): Weekly

**API credentials:**
- EIA API key: stored in `/workspace/.env` as EIA_API_KEY
- EPA ECHO: No auth needed
- State PUCs: No auth needed (public records)
- FERC: No auth needed

### Priority 2: Email Gate on Dashboards
**Goal:** Add blurred data sections + email capture to Queue Lookup and Developer Scorecard.

**Tools to gate:**
1. **Queue Lookup** — `/workspace/queue-lookup/index.html` + `/workspace/prospector-labs-site/queue-lookup/index.html`
   - Show 5 free searches, then blur results and prompt sign-up
   - Sign-up modal: Google OAuth or work email
2. **Developer Scorecard** — multiple versions exist:
   - `/workspace/developer-scorecard/index.html`
   - `/workspace/prospector-data-tools/developer-scorecard/index.html`
   - `/workspace/prospector-labs-site/developer-scorecard/index.html`
   - Show score but blur detailed breakdown

**Design system:** Geist + Geist Mono fonts, shadcn/ui aesthetic, dark/light mode toggle, minimal > decorative.

### Priority 3: Update Live Tools with Merged Data
- Queue Lookup needs to serve 41,750 projects (currently only ~35K)
- Developer Scorecard needs recalculation with merged data
- Critical Minerals + Transformer Tracker need UI polish

### Priority 4: Fix Prospector API
- All 88 endpoints returning 500 on Fly.io
- DuckDB files missing on deployment
- Hosting: Fly.io (~$3.50/mo), flyctl installed on Mac mini
- GitHub: OwenCoonahan/prospector-api (private)

---

## State PUC Research Needed

For each state, we need to find:
1. **URL** of the docket/filing search system
2. **What data is available** (project name, type, capacity, status, dates, developer)
3. **How structured** the data is (HTML table? PDF? API?)
4. **How to filter** for energy generation projects specifically
5. **Update frequency**

### Texas PUC (PRIORITY)
- **URL:** https://www.puc.texas.gov/
- **Docket search:** https://interchange.puc.texas.gov/search/
- 191 BESS projects in our dataset are in Texas
- ERCOT also publishes queue data: http://www.ercot.com/gridinfo/resource

### California PUC (PRIORITY)
- **CPUC:** https://www.cpuc.ca.gov/
- **CEC (California Energy Commission):** https://www.energy.ca.gov/ — tracks power plant licensing
- **CEC SPPE (Small Power Plant Exemption):** Has structured project tracking
- 137 BESS projects in our dataset are in California

### New York DPS
- **URL:** https://www3.dps.ny.gov/
- **Case search:** https://documents.dps.ny.gov/public/Common/CaseSearch.aspx

### Virginia SCC
- **URL:** https://www.scc.virginia.gov/
- **Case search available**

### Illinois ICC
- **URL:** https://www.icc.illinois.gov/
- **E-Docket search available**

---

## Technical Environment

- **Machine:** Owen's Mac mini (Apple Silicon, macOS)
- **Python:** Available, use for scrapers
- **Node.js:** v22.22.0
- **Key tools:** pandas, requests, beautifulsoup4, playwright (for JS-rendered pages)
- **Design preferences:** Geist + Geist Mono, shadcn/ui, dark/light mode, single HTML files with CDN libs for quick deploys
- **Deployment:** GitHub Pages (dashboards), Fly.io (API), Vercel (personal site)

---

## What NOT to Do

- Don't overclaim data we don't have — a previous reply said "we track permitting milestones" when we didn't. Be accurate.
- Don't use seed/fake data in production tools — only real, verified data
- Don't build new namespaces/tools before existing ones are polished
- Don't spend time on redesigns before the email gate is up and replies are sent
- Check that data is real before making any claims about what we track

---

## Files to Read for More Context
- `/workspace/PROJECT-STATUS.md` — full status of all projects
- `/workspace/MEMORY.md` — Owen's background, goals, patterns
- `/workspace/research/data-readiness-assessment.md` — data source evaluation
- `/workspace/research/deep-dive-data-sources.md` — detailed source analysis
- `/workspace/tools/master-interconnection-dataset/MERGE_REPORT.md` — merge details
- `/workspace/tools/master-interconnection-dataset/BESS_PERMITTING_REPORT.md` — BESS data details
