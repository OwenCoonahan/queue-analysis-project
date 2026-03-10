# BESS Permitting Data Pull — Report
**Date:** 2026-03-10

## Summary

Pulled **559 proposed/planned battery energy storage (BESS) projects** from EIA Form 860 (2024 data) with permitting status indicators. Matched **3,489 interconnection queue entries** from our master dataset to these EIA projects.

## What We Got

### EIA Form 860 — Energy Storage (Proposed Sheet)
- **Source:** `https://www.eia.gov/electricity/data/eia860/xls/eia8602024.zip` → `3_4_Energy_Storage_Y2024.xlsx`
- **559 proposed BESS projects** with detailed permitting status
- **74,638 MW total pipeline capacity**

#### Status Breakdown:
| Status | Code | Count | Capacity Implication |
|--------|------|-------|---------------------|
| Planned | P | 157 | Permit pending or early stage |
| Regulatory Approved (not under construction) | L | 112 | **PERMITTED — ready for investment** |
| Under Construction | U | 103 | **PERMITTED — actively building** |
| Inactive | V | 95 | Stalled or mothballed |
| Regulatory Approved | T | 51 | **PERMITTED — awaiting construction start** |
| Testing/Commissioning | TS | 41 | **PERMITTED — near commercial operation** |

**Key for POSH Energy:** Status codes **U, T, TS, L** = 307 projects that are PERMITTED. These are the investable pipeline.

#### Top States:
- TX: 191 projects
- CA: 137 projects  
- AZ: 44 projects
- NY: 24 projects
- MA: 17 projects

### Data Fields Available:
- Plant name, utility, state, county
- Nameplate capacity (MW), energy capacity (MWh)
- Max charge/discharge rates
- Storage technology type (lithium-ion, etc.)
- Whether co-located with renewables
- Use cases (arbitrage, frequency regulation, peak shaving, etc.)

## Match Results

- **10,690** battery/storage projects found in master interconnection dataset
- **3,489** matched to EIA 860 proposed BESS projects (by name+state or capacity+state)
- Many master entries map to same EIA plant (multiple queue entries per project is normal)
- **227 unique EIA plants matched** (out of 559)
- **332 EIA projects** had no match in our queue data (may be behind-the-meter or different naming)

## EPA ECHO Database
- Searched for "energy storage" facilities: **372 results** found
- API was extremely slow/timing out on detail retrieval
- These are mostly existing industrial facilities with "energy storage" in the name — NOT necessarily BESS projects
- **Recommendation:** EPA ECHO is not useful for BESS permitting. Battery storage projects rarely need RCRA/CWA/CAA permits unless very large. Not worth pursuing further.

## Files Created

| File | Description |
|------|-------------|
| `bess_permitting_data.csv` | 559 EIA 860 proposed BESS projects with permitting status |
| `bess_permitting_matched.csv` | 3,489 master dataset entries matched to EIA permitting data |

## Coverage Gaps

1. **Behind-the-meter projects** — EIA 860 only covers utility-scale (1 MW+). Smaller commercial/industrial BESS not captured.
2. **Local/municipal permits** — No centralized database exists. Would require county-by-county FOIA or scraping.
3. **State environmental review** — CEQA (CA), SEQRA (NY) etc. are state-level and not in any single API.
4. **Construction permits** — Building permits are local government data, no federal source.
5. **Queue-only projects** — Many of our 5,487 queue projects haven't filed EIA 860 yet (early stage).

## Recommendations

1. **Immediate value:** Use the 307 PERMITTED projects (U/T/TS/L status) as the "investment-ready" dataset for POSH Energy pitch
2. **CAISO/ERCOT/NYISO queues:** We already have this data in our master dataset — the queue status IS the permitting indicator for many projects
3. **State-level enrichment:** For CA specifically, CAISO's interconnection queue has detailed milestone tracking (Phase I/II study completion = proxy for permitting progress)
4. **FERC Form 556:** Available but covers qualifying facilities only — limited BESS coverage. Low priority.
5. **For comprehensive permitting:** Would need to build a scraper for individual state public utility commission dockets (expensive but high value)

## Key Insight

**EIA 860 status codes ARE the best available proxy for permitting status at the federal level.** A project can't reach "U" (Under Construction) or "T" (Regulatory Approved) without having secured necessary permits. This is the dataset to use for the POSH Energy pitch.
