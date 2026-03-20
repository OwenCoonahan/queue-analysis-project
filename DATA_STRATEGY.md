# Data Strategy — Top-Down Framework

## The Question
What data do we need to build, and why? Start from the buyer and work backward.

---

## 1. Buyers & Outcomes

### Buyer A: Pre-Operational Project Acquirer (Sardar)
**Outcome:** Find sub-5 MW solar/battery projects to acquire before placed-in-service for ITC credits.

**Decision they need to make:** Should I acquire this project? Is the developer willing to sell? What's the ITC value?

**Data that drives this decision:**

| Decision Factor | Data Needed | We Have? | Source Gap |
|-----------------|------------|----------|-----------|
| Is it pre-operational? | Project status (TS, U, V, T) | ✅ EIA-860/860M | Good for >1 MW. Missing <1 MW |
| How close to completion? | Construction stage, timeline | ⚠️ Status codes only | No granular milestones (permitting dates, PTO filings) |
| Is the developer small enough to negotiate? | Developer size, project count, revenue | ⚠️ Partial | Need Crunchbase/LinkedIn enrichment |
| What's the ITC value? | Energy community, brownfield, low-income, domestic content | ⚠️ EC only | Need brownfield, CEJST, domestic content overlays |
| Where is it? | State, county, lat/lon | ✅ | Good |
| Can I contact them? | Phone, email, key person | ❌ | Need scraping, data providers |
| Is the data current? | Filing date, last update | ✅ EIA-860M monthly | Jan 2026 is latest |

### Buyer B: PE Firm Evaluating Large Projects (Glass Energy / Existing Product)
**Outcome:** Assess risk/viability of 50-500 MW transmission-level projects in ISO queues.

**Decision:** Should we invest in this project? What's the completion probability?

| Decision Factor | Data Needed | We Have? |
|-----------------|------------|----------|
| Queue position & history | ISO queue data, snapshots | ✅ 36K projects, 9 ISOs |
| Developer track record | Historical completions | ✅ LBL + EIA cross-ref |
| Cost benchmarks | IC cost data | ✅ LBL cost data |
| Revenue potential | LMP, capacity prices, PPA | ✅ Benchmark data |
| Congestion risk | POI analysis, queue depth | ✅ Built |
| IRA eligibility | Energy community overlay | ✅ Built |

**This product is largely built.** The gap is freshness (some ISOs are stale) and automation.

### Buyer C: (Future) Solar Developer / Site Selector
**Outcome:** Find optimal locations for new projects.

**Data needs:** Grid capacity, land availability, permitting speed by county, LMP prices, ITC bonuses, labor costs. Most of this we have pieces of but haven't packaged.

---

## 2. Data Types — What Matters Most

Ranked by value to our current buyers:

### Tier 1: Must Have (Revenue-Generating)
1. **Project inventory** — What exists, where, what stage, who owns it
   - Source: EIA-860, EIA-860M, ISO queues, state DG programs
   - Refresh: Monthly (860M), daily (ISO queues)

2. **Developer profiles** — Who is this company, how big, track record
   - Source: EIA historical, LinkedIn, Crunchbase, state filings, web scraping
   - Refresh: Weekly-monthly

3. **ITC/incentive eligibility** — What % ITC does this project qualify for
   - Source: DOE energy community data, EPA brownfields, CEJST, census tracts
   - Refresh: Annual (DOE updates yearly)

### Tier 2: Differentiating (Competitive Advantage)
4. **Construction timelines** — How fast does a project go from filing to COD
   - Source: EIA-860M historical snapshots (we have 4 years of quarterly data)
   - Refresh: Derived from Tier 1 data

5. **Contact information** — Phone, email, key personnel
   - Source: Company websites, LinkedIn, state business registries
   - Refresh: As-needed

6. **Sub-1 MW project data** — The gap Sardar identified
   - Source: State utility DG queues (CA Rule 21, MA DOER, NYSERDA, NJ SRP)
   - Refresh: Varies by state program

### Tier 3: Nice to Have (Future Value)
7. **Permitting data** — County-level construction permits, timelines
8. **PPA/offtake data** — Who has contracts, at what price
9. **Grid congestion** — Transmission constraints, curtailment risk
10. **Land use/zoning** — What's buildable where

---

## 3. Source Architecture — Where Data Lives

### Federal (Best Data, Easiest Access)
| Source | Data | Access | Refresh | Coverage |
|--------|------|--------|---------|----------|
| EIA-860 (Annual) | All generators >1 MW | Free download | Annual (March) | 100% of >1 MW |
| EIA-860M (Monthly) | Pre-operational generators | Free download | Monthly | Pre-op >1 MW |
| EIA API v2 | Generation, capacity, prices | Free API key | Varies | National |
| DOE Energy Communities | Coal closure + FFE overlays | Free download | Annual | National |
| EPA Brownfields | Contaminated site locations | Free API | Quarterly | National |
| CEJST | Low-income community mapping | Free download | Annual | National |

### ISO/RTO (Good Data, Medium Effort)
| Source | Data | Access | Refresh | Coverage |
|--------|------|--------|---------|----------|
| PJM Queue | Transmission-level projects | Excel download | Weekly | PJM footprint |
| MISO Queue | Transmission-level projects | Public API | Daily | MISO footprint |
| CAISO Queue | Transmission-level projects | Excel download | Daily | CAISO footprint |
| ERCOT GIS | Transmission-level projects | Excel download | Weekly | Texas |
| NYISO Queue | Transmission-level projects | Excel download | Monthly | NY |
| SPP/ISO-NE | Transmission-level projects | gridstatus library | Daily | SPP/NE |

### State Programs (Hard Data, High Value for Sub-5 MW)
| Source | Data | Access | Refresh | Coverage |
|--------|------|--------|---------|----------|
| CA DG Stats (Rule 21) | Distribution-level interconnections | Public portal | Weekly | CA |
| MA DOER SMART | Solar incentive program projects | Public portal | Monthly | MA |
| NYSERDA NY-SUN | Solar incentive program projects | Public portal | Monthly | NY |
| NJ SRP/TPS | Solar renewable energy certificates | Public portal | Monthly | NJ |
| IL Adjustable Block | Community solar program | Public portal | Quarterly | IL |
| CT LREC/ZREC | Renewable energy credits | Public portal | Quarterly | CT |

### Enrichment (Developer Intelligence)
| Source | Data | Access | Cost |
|--------|------|--------|------|
| LinkedIn | Employee count, company info | Scraping / Sales Nav | $80-800/mo |
| Crunchbase | Funding, revenue estimates | API | $300/mo |
| State SOS filings | Business registration, officers | Per-state scraping | Free |
| Company websites | Contact info, project portfolio | Scraping | Free |
| OpenCorporates | Corporate structure, subsidiaries | API | Free tier available |

---

## 4. Ingestion Priority — What to Build Next

### Sprint 1: Sub-1 MW Data (Sardar's Gap)
**Goal:** Get 500 kW - 5 MW commercial solar/battery projects from state programs
- [ ] Pull from OpenClaw's small_projects.db (CA DG Stats, MA DOER, NYSERDA already loaded)
- [ ] Filter: commercial only, pre-operational, 500 kW - 5 MW
- [ ] Add IL Adjustable Block program data
- [ ] Add NJ SRP program data
- [ ] Merge with EIA-860 data, dedup on plant name + location

### Sprint 2: Developer Size Enrichment
**Goal:** Flag small vs large developers across all projects
- [ ] Count projects per developer across EIA-860 + state programs
- [ ] Scrape LinkedIn for employee counts (top 50 developers)
- [ ] Check Crunchbase for funding data
- [ ] Flag: "Small Independent" (<10 projects, <50 employees), "Mid-size" (10-50), "Large/Corporate" (50+)

### Sprint 3: ITC Bonus Completion
**Goal:** Full ITC percentage for every project (not just energy community)
- [ ] Brownfield overlay (EPA API, cross-reference lat/lon)
- [ ] Low-income community overlay (CEJST data, census tract mapping)
- [ ] Domestic content flag (harder — manufacturer data is sparse)
- [ ] Show max potential ITC: 30% base + each applicable bonus

### Sprint 4: Contact Enrichment
**Goal:** Phone/email for developers in the dataset
- [ ] Scrape company websites (already have URLs for ~50%)
- [ ] State business registry lookups for registered agent info
- [ ] LinkedIn key contact identification

---

## 5. Infrastructure Principles

OpenClaw's report is a good technical reference. Here's what matters for us RIGHT NOW:

1. **SQLite is fine.** We don't need DuckDB or Postgres yet. Our data fits in a single file. Don't over-engineer the storage layer until we have paying customers.

2. **Scripts over frameworks.** Python scripts + cron beats Prefect/Dagster at our scale. We have <20 sources. Add orchestration when we have >50 sources or need multi-team coordination.

3. **Dedup at ingest.** The hardest problem is entity resolution — the same project appears in EIA-860, state programs, and ISO queues with different names. Invest time here, not in infrastructure.

4. **HTML dashboards ship fastest.** Single-file HTML with embedded data deploys to GitHub Pages in seconds. No build step, no framework, no server. This is our delivery layer for now.

5. **Freshness matters more than completeness.** A dataset that's 80% complete but updated monthly beats a 95% complete dataset updated annually. EIA-860M monthly is our best friend.

---

## 6. What This Means for the Business

| Product | Buyer | Data Foundation | Revenue |
|---------|-------|-----------------|---------|
| **ITC Target Dashboard** | Sardar / project acquirers | EIA-860M + state DG + developer enrichment | Per-engagement ($5-10K) |
| **Glass Energy Reports** | PE firms | ISO queues + analytics + scoring | Per-report ($4-5K) or retainer ($6-8K/mo) |
| **Prospector API** | Developers, analysts, AI agents | All data via REST/MCP | Subscription ($500-2K/mo) |

The Sardar engagement is proving out a NEW buyer type (project acquirer) that we didn't originally target. If this works, it's a second product line alongside Glass Energy.
