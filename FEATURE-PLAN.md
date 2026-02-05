# Feature Implementation Plan: Tier 1 & 2

## Overview

Six new data capabilities to enhance deal analysis for PE firms.

| # | Feature | Tier | Data Source | Complexity |
|---|---------|------|-------------|------------|
| 1 | Study Document Parser | 1 | Client-provided PDFs | Medium |
| 2 | Transmission Constraints | 1 | RTO planning data | High |
| 3 | LMP/Pricing Data | 1 | RTO market data | Medium |
| 4 | PPA Database | 2 | Public + estimates | Medium |
| 5 | Permitting Tracker | 2 | State PUC filings | High |
| 6 | Capacity Market Data | 2 | RTO capacity auctions | Medium |

---

## Tier 1: Game Changers

### 1. Study Document Parser

**Purpose:** Extract actual cost estimates from interconnection study PDFs and compare to benchmarks.

**Input:** Client uploads Feasibility Study, System Impact Study, or Facilities Study PDF

**Output:**
- Extracted cost breakdown (network upgrades, POI costs, etc.)
- Comparison to our P25/P50/P75 benchmarks
- Flags for unusually high/low estimates
- Confidence score based on study phase

**Approach:**
```
Option A: PDF text extraction + regex patterns
- PyPDF2 or pdfplumber for text extraction
- Regex patterns for common cost table formats
- Works for ~60-70% of studies

Option B: LLM-assisted extraction
- Send extracted text to Claude API
- Structured extraction prompt
- Higher accuracy, handles varied formats
- Cost: ~$0.10-0.50 per document

Recommendation: Start with Option A, fall back to Option B for complex docs
```

**Data Model:**
```python
class StudyDocument:
    project_id: str
    study_type: str  # feasibility, sis, facilities
    study_date: date
    total_cost: float
    cost_breakdown: dict  # {category: amount}
    upgrade_requirements: list[str]
    timeline_estimate: str
    raw_text: str
    extraction_confidence: float
```

**Files to Create:**
- `study_parser.py` - PDF extraction and parsing
- `study_compare.py` - Comparison to benchmarks

---

### 2. Transmission Constraint Layer

**Purpose:** Identify congested transmission zones and flag high-risk POIs.

**Data Sources:**
| RTO | Data Type | URL/Source | Update Freq |
|-----|-----------|------------|-------------|
| PJM | Congestion costs | PJM DataMiner | Monthly |
| ERCOT | Constraint data | ERCOT MIS | Daily |
| MISO | Binding constraints | MISO OASIS | Daily |
| CAISO | Transmission constraints | CAISO OASIS | Daily |
| NYISO | TCC data | NYISO OASIS | Monthly |
| SPP | Flowgate data | SPP OASIS | Daily |

**Output:**
- Congestion cost at/near POI ($/MWh)
- Historical constraint frequency
- Known upgrade projects in the area
- Risk rating (Low/Medium/High)

**Approach:**
```
Phase 1: Static mapping
- Download historical congestion data
- Map POIs to transmission zones
- Calculate zone-level congestion metrics

Phase 2: Dynamic updates
- API integration for real-time data
- Track constraint changes over time
- Alert on new constraints near projects
```

**Data Model:**
```python
class TransmissionZone:
    zone_id: str
    region: str
    avg_congestion_cost: float  # $/MWh
    constraint_hours: int  # hours/year
    known_upgrades: list[str]
    risk_rating: str

class POIConstraint:
    poi_name: str
    zone_id: str
    distance_miles: float
    congestion_exposure: float
```

**Files to Create:**
- `transmission_data.py` - Data loading and zone mapping
- `congestion_analysis.py` - Constraint analysis

---

### 3. LMP/Pricing Data

**Purpose:** Show historical and expected energy prices at project location.

**Data Sources:**
| RTO | Data Type | Access | Format |
|-----|-----------|--------|--------|
| PJM | Nodal LMPs | DataMiner API | CSV |
| ERCOT | SPPs | ERCOT MIS | CSV |
| MISO | LMPs | MISO API | CSV |
| CAISO | LMPs | OASIS | CSV |
| NYISO | LBMPs | NYISO | CSV |

**Output:**
- Average LMP at nearest node ($/MWh)
- Peak vs off-peak pricing
- Price volatility (std dev)
- Year-over-year trends
- Estimated annual revenue (capacity * CF * LMP)

**Approach:**
```
Phase 1: Historical data
- Download 2-3 years of hourly LMP data
- Map POIs to nearest pricing nodes
- Calculate summary statistics

Phase 2: Revenue modeling
- Apply technology-specific capacity factors
- Model hourly generation vs prices
- Calculate expected revenue range
```

**Data Model:**
```python
class PricingNode:
    node_id: str
    node_name: str
    region: str
    avg_lmp: float
    peak_lmp: float
    offpeak_lmp: float
    volatility: float

class ProjectRevenue:
    project_id: str
    node_id: str
    capacity_mw: float
    capacity_factor: float
    annual_revenue_low: float
    annual_revenue_mid: float
    annual_revenue_high: float
```

**Files to Create:**
- `lmp_data.py` - LMP data loading
- `revenue_model.py` - Revenue calculations

---

## Tier 2: Significant Value

### 4. PPA Database

**Purpose:** Provide PPA price benchmarks for revenue assumptions.

**Data Sources:**
- LevelTen Energy (subscription)
- S&P Global (subscription)
- Public announcements (free)
- State PUC filings (free)

**Output:**
- Recent PPA prices by region/technology
- Price trends over time
- Contracted vs merchant comparison
- Deal volume by region

**Approach:**
```
Phase 1: Public data aggregation
- Scrape announced PPA deals from news
- Extract prices from state PUC filings
- Build database of ~200-500 deals

Phase 2: Benchmark estimates
- Calculate regional averages
- Adjust for technology type
- Provide ranges for deal modeling
```

**Data Model:**
```python
class PPADeal:
    deal_id: str
    announcement_date: date
    region: str
    state: str
    technology: str
    capacity_mw: float
    price_per_mwh: float  # if disclosed
    term_years: int
    buyer_type: str  # utility, C&I, aggregator
    source_url: str

class PPABenchmark:
    region: str
    technology: str
    year: int
    price_p25: float
    price_p50: float
    price_p75: float
    sample_size: int
```

**Files to Create:**
- `ppa_data.py` - PPA deal database
- `ppa_scraper.py` - News/filing scraper

---

### 5. Permitting Tracker

**Purpose:** Track state and local permitting status for projects.

**Data Sources:**
| Type | Source | Coverage |
|------|--------|----------|
| State siting | State PUC/energy office | Varies by state |
| Environmental | NEPA/state EIS databases | Federal lands |
| Local permits | County records | Limited |

**Key States to Cover:**
- Texas (ERCOT) - PUCT filings
- California (CAISO) - CEC siting
- New York (NYISO) - ORES/Article 10
- PJM states - Varies by state

**Output:**
- Permit status by type
- Known opposition or delays
- Timeline estimates
- Risk flags

**Approach:**
```
Phase 1: Manual tracking
- Track major permits for active projects
- Flag known issues from news
- Build template for common permits

Phase 2: Automated monitoring
- Set up scrapers for key state databases
- Alert on status changes
- Integration with project database
```

**Data Model:**
```python
class Permit:
    permit_id: str
    project_id: str
    permit_type: str  # state_siting, environmental, local
    jurisdiction: str
    status: str  # pending, approved, denied, appealed
    filed_date: date
    decision_date: date
    notes: str

class PermitRisk:
    project_id: str
    overall_risk: str  # low, medium, high
    pending_permits: list[str]
    known_opposition: bool
    timeline_risk_months: int
```

**Files to Create:**
- `permit_data.py` - Permit database
- `permit_scraper.py` - State database scrapers

---

### 6. Capacity Market Data

**Purpose:** Show capacity market value for projects.

**Data Sources:**
| RTO | Capacity Market | Data Source |
|-----|-----------------|-------------|
| PJM | RPM | PJM DataMiner |
| NYISO | ICAP | NYISO |
| ISO-NE | FCM | ISO-NE |
| MISO | PRA | MISO |
| CAISO | RA (bilateral) | CPUC filings |
| ERCOT | None (energy-only) | N/A |

**Output:**
- Capacity price by zone ($/MW-day or $/kW-month)
- Historical price trends
- Delivery year obligations
- ELCC/capacity credit by technology
- Value stack (energy + capacity)

**Approach:**
```
Phase 1: Historical data
- Download capacity auction results
- Map zones to project locations
- Calculate technology-specific capacity value

Phase 2: Forward curves
- Model expected future capacity prices
- Apply technology degradation (solar ELCC declining)
- Project lifetime capacity revenue
```

**Data Model:**
```python
class CapacityZone:
    zone_id: str
    zone_name: str
    region: str

class CapacityPrice:
    zone_id: str
    delivery_year: str
    price_per_mw_day: float
    clearing_date: date

class CapacityCredit:
    region: str
    technology: str
    year: int
    elcc_percent: float  # effective load carrying capability
```

**Files to Create:**
- `capacity_data.py` - Capacity market data
- `capacity_value.py` - Capacity revenue calculations

---

## Implementation Order

Recommended sequence based on value and dependencies:

### Week 1-2: Foundation
1. **LMP/Pricing Data** - Most straightforward, immediate value
2. **Capacity Market Data** - Complements LMP for full revenue picture

### Week 3-4: Core Differentiators
3. **Study Document Parser** - High client value, PDF handling
4. **PPA Database** - Start with public data aggregation

### Week 5-6: Advanced Analytics
5. **Transmission Constraints** - Complex but high value
6. **Permitting Tracker** - Start with major states

---

## Data Storage

All new data will be added to SQLite database:

```sql
-- New tables to add
CREATE TABLE lmp_nodes (...);
CREATE TABLE lmp_prices (...);
CREATE TABLE capacity_zones (...);
CREATE TABLE capacity_prices (...);
CREATE TABLE ppa_deals (...);
CREATE TABLE study_documents (...);
CREATE TABLE transmission_constraints (...);
CREATE TABLE permits (...);
```

---

## Report Integration

Each feature adds new sections to the HTML report:

1. **Study Parser** → "Actual vs Benchmark Costs" section
2. **Transmission** → "Transmission Risk" section with map
3. **LMP Data** → "Expected Revenue" section
4. **PPA Data** → "Market Pricing Context" section
5. **Permitting** → "Permitting Status" section
6. **Capacity** → Added to "Expected Revenue" section

---

## Questions to Resolve

1. **LMP granularity** - Hourly vs daily vs monthly averages?
2. **PPA sources** - Pay for LevelTen/S&P or scrape public only?
3. **Permit coverage** - All states or focus on top 5-10?
4. **Study parser** - Use LLM extraction or regex only?
5. **Update frequency** - Daily, weekly, or monthly refreshes?
