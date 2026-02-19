# Analytics Infrastructure

## Overview

This document describes the consolidated analytics infrastructure for interconnection queue analysis. All analytics logic lives in **one place** (`analytics.py`) to prevent code fragmentation across agents and modules.

## Architecture

```
tools/
├── analytics.py          # SINGLE SOURCE OF TRUTH for all analytics
├── reports/
│   ├── deal_report.py    # Uses analytics.py for calculations
│   └── cluster_report.py # Uses analytics.py for calculations
├── .data/
│   ├── queue_v2.db       # Primary normalized database (star schema)
│   ├── queue.db          # Legacy V1 database
│   └── enrichment.db     # Developer/entity enrichment
└── .cache/
    ├── lbl_queued_up.xlsx    # LBL historical data
    ├── eia_*.parquet         # EIA 860 generator data
    └── *_costs_*.xlsx        # Regional IC cost studies
```

## Data Flow

```
Raw ISO Data → queue.db → queue_v2.db (normalized) → analytics.py → reports/
                              ↑
                        enrichment.db
                              ↑
                    .cache/ (LBL, EIA, costs)
```

## Analytics Module (`analytics.py`)

### Design Principles

1. **Single Source of Truth** - All calculation logic in one file
2. **Database-First** - Read from SQLite, not scattered CSV/Excel files
3. **Cacheable** - Results can be cached to avoid recomputation
4. **Documented** - Every function has clear docstrings
5. **Testable** - Pure functions where possible

### Class Structure

```python
class QueueAnalytics:
    """Main analytics class - use this for all calculations."""

    # Tier 1: Feasibility Analysis
    def get_completion_probability(region, technology, capacity_mw) -> dict
    def get_developer_track_record(developer_name, region=None) -> dict
    def get_poi_congestion_score(poi_name, region) -> dict
    def get_cost_percentile(region, technology, capacity_mw) -> dict
    def get_timeline_benchmarks(region, technology) -> dict
    def get_ira_eligibility(state, county) -> dict

    # Tier 2: Revenue & Market Analysis
    def get_revenue_estimate(region, technology, capacity_mw, zone=None) -> dict
    def get_capacity_value(region, technology, capacity_mw, delivery_year=None) -> dict
    def get_transmission_risk(region, zone=None, poi=None) -> dict
    def get_ppa_benchmarks(region, technology, year=None) -> dict
    def get_full_revenue_stack(region, technology, capacity_mw, zone=None) -> dict

    # Convenience
    def get_project_analysis(..., include_tier2=True) -> dict  # All-in-one
```

### Data Sources by Function

| Function | Primary Source | Fallback |
|----------|---------------|----------|
| `get_completion_probability` | queue_v2.db + LBL | queue.db |
| `get_developer_track_record` | queue_v2.db + EIA 860 | LBL |
| `get_poi_congestion_score` | queue_v2.db | queue.db |
| `get_cost_percentile` | LBL IC costs | queue.db estimates |
| `get_timeline_benchmarks` | LBL + queue_v2.db | queue.db |
| `get_ira_eligibility` | energy_communities.csv | None |
| `get_revenue_estimate` | lmp_data.py / benchmarks | Fallback |
| `get_capacity_value` | capacity_data.py / benchmarks | Fallback |
| `get_transmission_risk` | transmission_data.py / benchmarks | Fallback |
| `get_ppa_benchmarks` | ppa_data.py / benchmarks | Fallback |
| `get_full_revenue_stack` | Combined Tier 2 | Fallback |

## Tier 1 Analytics (Implemented)

### 1. Completion Probability

Calculates probability a project reaches COD based on:
- Region historical completion rates
- Technology completion rates
- Capacity band completion rates (small/medium/large)
- Combined weighted probability

**Output:**
```python
{
    'region_rate': 0.21,           # 21% for PJM
    'technology_rate': 0.14,       # 14% for Solar
    'capacity_band_rate': 0.18,    # 18% for 100-500 MW
    'combined_rate': 0.17,         # Weighted average
    'confidence': 'high',          # Based on sample size
    'sample_size': 1234,
    'methodology': 'LBL historical data 2010-2024'
}
```

### 2. Developer Track Record

Analyzes developer's historical performance:
- Projects in queue (all regions)
- Completed (reached COD)
- Withdrawn
- Completion rate
- Cross-reference with EIA 860 operational plants

**Output:**
```python
{
    'total_projects': 45,
    'completed': 12,
    'withdrawn': 28,
    'active': 5,
    'completion_rate': 0.30,       # 30%
    'eia_verified_plants': 8,      # Verified in EIA 860
    'total_operational_mw': 1250,  # From EIA
    'assessment': 'Experienced developer with verified track record',
    'confidence': 'high',
    'completed_projects': [        # Actual project list
        {'name': 'Solar Farm 1', 'mw': 150, 'cod': '2023-06', 'state': 'TX'},
        ...
    ]
}
```

### 3. POI Congestion Score

Analyzes queue depth and competition at Point of Interconnection:
- Total projects at POI
- Active vs withdrawn
- Capacity queued
- This project's position
- Historical withdrawal rate at POI

**Output:**
```python
{
    'poi_name': 'Smithburg 345kV',
    'total_projects': 15,
    'active_projects': 6,
    'withdrawn_projects': 9,
    'total_capacity_mw': 2500,
    'queue_position': 3,           # This project is #3
    'projects_ahead': 2,
    'capacity_ahead_mw': 450,
    'withdrawal_rate': 0.60,       # 60% withdrew
    'risk_level': 'ELEVATED',
    'risk_reason': '2 projects ahead, 60% historical withdrawal'
}
```

### 4. Cost Percentile Ranking

Ranks estimated IC cost against historical actuals:
- Pull from LBL IC cost studies
- Filter by region and technology
- Calculate percentile position

**Output:**
```python
{
    'estimated_cost_per_kw': 125,
    'percentile': 65,              # 65th percentile (higher than 65%)
    'p25': 80,
    'p50': 110,
    'p75': 145,
    'sample_size': 234,
    'interpretation': 'Above median - elevated cost risk',
    'histogram': [                 # For visualization
        {'range': '0-50', 'count': 23, 'contains_project': False},
        {'range': '50-100', 'count': 89, 'contains_project': False},
        {'range': '100-150', 'count': 67, 'contains_project': True},
        ...
    ]
}
```

### 5. Timeline Benchmarks

Historical time-to-COD for similar projects:
- Median, P25, P75 timelines
- By region and technology
- Adjusted for current queue position

**Output:**
```python
{
    'p25_months': 36,
    'p50_months': 48,
    'p75_months': 64,
    'sample_size': 156,
    'region': 'PJM',
    'technology': 'Solar',
    'methodology': 'Completed projects 2018-2024'
}
```

### 6. IRA Energy Communities Eligibility

Checks if project location qualifies for IRA bonus credits:
- Coal closure communities
- Fossil fuel employment areas
- Brownfield sites

**Output:**
```python
{
    'eligible': True,
    'category': 'coal_closure',
    'bonus_adder': 0.10,           # 10% ITC/PTC bonus
    'community_name': 'Martin County Coal Community',
    'qualifying_criteria': 'Coal plant closed 2018'
}
```

## Tier 2 Analytics (Implemented)

### 1. Revenue Estimate

Calculates annual energy revenue based on LMP prices and capacity factors.

**Output:**
```python
{
    'annual_revenue_millions': 18.4,   # $18.4M/year
    'revenue_low_millions': 12.88,
    'revenue_high_millions': 23.91,
    'revenue_per_kw': 92,              # $/kW-year
    'avg_lmp': 42,                     # $/MWh
    'capacity_factor': 0.25,           # 25%
    'annual_generation_mwh': 438000,
    'data_source': 'benchmark',        # or 'live' when available
}
```

### 2. Capacity Value

Calculates capacity market revenue based on ELCC and auction prices.

**Output:**
```python
{
    'elcc_percent': 0.35,              # 35% for solar
    'accredited_mw': 70.0,             # 200 MW × 35%
    'price_mw_day': 270,               # PJM RPM price
    'annual_value_millions': 6.9,      # $6.9M/year
    'value_per_kw': 34.49,             # $/kW-year
    'market_type': 'RPM (Reliability Pricing Model)',
}
```

### 3. Transmission Risk

Assesses congestion and constraint risk at project location.

**Output:**
```python
{
    'risk_score': 0.70,                # 0-1 scale
    'risk_rating': 'ELEVATED',         # LOW/MODERATE/ELEVATED/HIGH
    'congestion_level': 'high',
    'avg_congestion_cost': 12.0,       # $/MWh
    'pct_hours_congested': 0.20,       # 20% of hours
    'relevant_constraints': [...],
    'planned_upgrades': [...],
}
```

### 4. PPA Benchmarks

Returns market PPA price ranges for region and technology.

**Output:**
```python
{
    'price_low': 32,                   # $/MWh
    'price_mid': 38,
    'price_high': 44,
    'trend': 'stable',                 # rising/stable/declining
    'sample_deals': [...],             # Recent transaction examples
}
```

### 5. Full Revenue Stack

Combines energy, capacity, and ancillary revenue.

**Output:**
```python
{
    'energy_revenue_millions': 18.4,
    'capacity_revenue_millions': 6.9,
    'ancillary_revenue_millions': 0.55,
    'total_revenue_millions': 25.85,
    'revenue_per_kw': 129,             # $/kW-year
    'energy_pct': 71.2,                # Revenue mix
    'capacity_pct': 26.7,
    'ancillary_pct': 2.1,
}
```

## Data Sources for Tier 2

| Metric | Source | Status | Quality |
|--------|--------|--------|---------|
| LMP prices | lmp_data.py benchmarks | Fallback | Benchmark |
| Capacity prices | capacity_data.py | 63 records | Benchmark |
| ELCC values | capacity_data.py | 105 records | Benchmark |
| Congestion | transmission_data.py | 64 zones | Benchmark |
| Constraints | transmission_data.py | 13 major | Benchmark |
| PPA prices | ppa_data.py | 105 records | Benchmark |

**Note:** All Tier 2 analytics currently use benchmark data, not live market feeds.
Future enhancement: Connect to ISO OASIS APIs for real-time pricing.

## Usage in Reports

```python
from analytics import QueueAnalytics

# Initialize once
qa = QueueAnalytics()

# Tier 1: Feasibility Analysis
completion = qa.get_completion_probability('PJM', 'Solar', 200)
developer = qa.get_developer_track_record('Invenergy')
poi = qa.get_poi_congestion_score('Smithburg 345kV', 'PJM')
cost = qa.get_cost_percentile('PJM', 'Solar', 200)
timeline = qa.get_timeline_benchmarks('PJM', 'Solar')
ira = qa.get_ira_eligibility('IN', 'Martin')

# Tier 2: Revenue & Market Analysis
revenue = qa.get_revenue_estimate('PJM', 'Solar', 200)
capacity = qa.get_capacity_value('PJM', 'Solar', 200)
tx_risk = qa.get_transmission_risk('PJM', zone='WEST')
ppa = qa.get_ppa_benchmarks('PJM', 'Solar')
stack = qa.get_full_revenue_stack('PJM', 'Solar', 200)

# All-in-one analysis
full_analysis = qa.get_project_analysis(
    project_id='J1234',
    region='PJM',
    technology='Solar',
    capacity_mw=200,
    developer='Invenergy',
    poi='Smithburg 345kV',
    state='IN',
    county='Martin',
    include_tier2=True  # Include revenue/capacity/transmission
)
```

## Multi-Agent Coordination

### For Mac Mini (Server Agent)

The server agent should:
1. Run data refreshes (`refresh_data.py`)
2. NOT modify `analytics.py` - only Laptop agent does analytics changes
3. Pull latest from GitHub before running reports
4. Push refresh logs but not analytics changes

### For Laptop (Development Agent)

The laptop agent should:
1. Modify `analytics.py` for new calculations
2. Update this documentation when adding features
3. Test changes before pushing
4. Coordinate with server via GitHub

### Change Protocol

When adding new analytics:
1. Add function to `analytics.py`
2. Document in this file (ANALYTICS.md)
3. Add tests if complex
4. Update `deal_report.py` to use new function
5. Commit with clear message: "feat(analytics): add X calculation"

## Database Schema Reference

### queue_v2.db (Primary)

```sql
-- Core dimensions
dim_projects: project_id, name, capacity_mw, queue_date, cod_date, status
dim_developers: developer_id, canonical_name, parent_company_id
dim_technologies: tech_id, name, category
dim_regions: region_id, name, iso_code
dim_locations: location_id, state, county, lat, lon

-- Fact tables
fact_projects: Links dimensions with foreign keys
fact_project_history: Historical snapshots
```

### LBL Data Fields

```
q_id, q_status, q_date, on_date, wd_date, ia_date
region, state, county, type_clean, mw1
developer, poi_name
```

## Maintenance

### Weekly
- [ ] Verify data freshness (check last_refresh.json)
- [ ] Run validation (`python validate_data.py`)

### Monthly
- [ ] Check for new LBL Queued Up release
- [ ] Update EIA 860 when new year available
- [ ] Review analytics accuracy against known outcomes

### Quarterly
- [ ] Audit completion rate calculations against actuals
- [ ] Update cost benchmarks with new study data
- [ ] Review and update documentation
