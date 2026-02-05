# Data Sources Analysis

## Current Data Inventory

| Dataset | Rows | Source | Updates | Coverage |
|---------|------|--------|---------|----------|
| NYISO Queue | 179 | NYISO Direct | Manual | Current active queue |
| LBL Queued Up | 36,441 | LBL/Berkeley | Annual | All RTOs 2000-2024 |
| NYISO Costs | 294 | LBL | Annual | Historical w/ $/kW |
| PJM Costs | 1,127 | LBL | Annual | Historical w/ $/kW |
| MISO Costs | 922 | LBL | Annual | Historical w/ $/kW |
| SPP Costs | 845 | LBL | Annual | Historical w/ $/kW |
| ISO-NE Costs | 194 | LBL | Annual | Historical w/ $/kW |

**Total: ~40K historical projects + 3,382 with cost data**

---

## Data We Have vs. Don't Have

### What We Have (Strong)
- Historical queue data for all RTOs (2000-2024)
- Completion rates by region and project type
- Timeline benchmarks (IR to COD)
- Interconnection cost per kW benchmarks
- Project status tracking (active/withdrawn/completed)

### What We're Missing (Gaps)
- **Live queue data** for non-NYISO RTOs
- **Actual study costs** (we estimate, they have real numbers)
- **Transmission constraint data** (congestion, upgrade triggers)
- **Developer financial data** (backing, track record)
- **Permitting status** (state/local approvals)
- **PPA/offtake status** (contracted vs merchant)

---

## Data Sources by RTO

### Tier 1: Live Queue Data (High Priority)

| RTO | Queue Size | Data Source | Format | Access |
|-----|------------|-------------|--------|--------|
| **PJM** | ~2,500 | [pjm.com/planning](https://www.pjm.com/planning/services-requests/interconnection-queues) | Excel | Public |
| **ERCOT** | ~1,800 | [ercot.com/gridinfo](https://www.ercot.com/gridinfo/resource) | Excel | Public |
| **MISO** | ~1,500 | [misoenergy.org](https://www.misoenergy.org/planning/generator-interconnection/) | Excel | Public |
| **CAISO** | ~800 | [caiso.com](http://www.caiso.com/planning/Pages/GeneratorInterconnection/Default.aspx) | Excel | Public |
| NYISO | 179 | nyiso.com | Excel | **Done** |

### Tier 2: Aggregators (Medium Priority)

| Source | Coverage | Update Freq | Access |
|--------|----------|-------------|--------|
| [Interconnection.fyi](https://www.interconnection.fyi) | All RTOs + utilities | Daily | Contact for API |
| [LBL Queued Up](https://emp.lbl.gov/queues) | All RTOs | Annual | Public Excel |
| [GridStatus.io](https://www.gridstatus.io) | All RTOs | Daily | API ($) |

### Tier 3: Supplemental Data (Nice to Have)

| Source | Data Type | Value Add |
|--------|-----------|-----------|
| EIA Form 860 | Existing generators | Competitive analysis |
| FERC Form 714 | Transmission planning | Constraint context |
| State PUC filings | Permitting status | Timeline risk |
| S&P/Platts | PPA prices | Revenue assumptions |

---

## Value Assessment by Data Type

### High Value (Directly impacts report quality)

| Data | Current State | With Addition | Effort |
|------|---------------|---------------|--------|
| **Live PJM Queue** | LBL annual | Real-time status | Medium |
| **Actual Study Costs** | Estimated | From study docs | High |
| **Transmission Constraints** | None | Congestion maps | Medium |
| **Developer Track Record** | Basic count | Success rates | Low |

### Medium Value (Improves depth)

| Data | Current State | With Addition | Effort |
|------|---------------|---------------|--------|
| Multi-RTO search | NYISO + MISO | All 7 RTOs | Medium |
| Comparable projects | None | 5-10 similar deals | Medium |
| Milestone tracking | Snapshot | Timeline history | Medium |

### Lower Value (Nice to have)

| Data | Current State | With Addition | Effort |
|------|---------------|---------------|--------|
| Permitting data | None | State approvals | High |
| PPA status | None | Offtake info | Very High |
| Weather/resource | None | Capacity factors | Low |

---

## Recommended Data Additions

### Phase 1: Quick Wins (1-2 days each)

1. **Add PJM Live Queue**
   - Download from pjm.com
   - Same format as NYISO
   - Enables PJM reports immediately

2. **Add ERCOT Live Queue**
   - Download from ercot.com
   - Different format, needs mapping
   - High demand market

3. **Improve Developer Research**
   - Cross-reference all RTOs in LBL data
   - Calculate actual success rates per developer
   - Already have the data, just need to query it

### Phase 2: Medium Effort (1 week each)

4. **Transmission Constraint Layer**
   - Map POIs to transmission zones
   - Pull congestion data from RTOs
   - Flag high-risk interconnection points

5. **Study Document Parser**
   - If client provides Feasibility/SIS/Facilities study
   - Extract actual cost estimates
   - Compare to our benchmarks

6. **Comparable Projects Module**
   - Find similar completed projects
   - Same region, type, size range
   - Show their actual outcomes

### Phase 3: Longer Term

7. **Real-time Data Pipeline**
   - Daily queue data refresh
   - Track status changes over time
   - Alert on significant movements

8. **API Integration**
   - Interconnection.fyi API (if available)
   - GridStatus.io API
   - Automated data collection

---

## Data Quality Issues

### Known Issues in Current Data

| Issue | Impact | Mitigation |
|-------|--------|------------|
| LBL data is annual | May miss recent changes | Add live RTO data |
| Cost data varies by study type | Wide ranges | Filter by study type |
| Project names inconsistent | Hard to track | Fuzzy matching |
| Developer names vary | Under-counts experience | Normalize names |
| Some projects lack key fields | Missing estimates | Flag low confidence |

### Data Validation Needs

- [ ] Cross-check LBL vs live queue counts
- [ ] Validate cost benchmarks against recent studies
- [ ] Verify completion rate calculations
- [ ] Check timeline outliers (500+ month projects)

---

## Next Steps

1. **Today**: Download PJM and ERCOT queue data
2. **This Week**: Build loaders for new RTOs
3. **This Week**: Improve cross-RTO developer search using LBL data
4. **Next Week**: Add transmission constraint analysis
5. **Ongoing**: Explore API access to Interconnection.fyi

---

## Data Source Links

### Official RTO Sources
- PJM: https://www.pjm.com/planning/services-requests/interconnection-queues
- ERCOT: https://www.ercot.com/gridinfo/resource
- MISO: https://www.misoenergy.org/planning/generator-interconnection/
- CAISO: http://www.caiso.com/planning/Pages/GeneratorInterconnection/
- NYISO: https://www.nyiso.com/interconnections
- ISO-NE: https://www.iso-ne.com/system-planning/interconnection-service/
- SPP: https://www.spp.org/engineering/generator-interconnection/

### Aggregators
- LBL Queued Up: https://emp.lbl.gov/queues
- Interconnection.fyi: https://www.interconnection.fyi
- GridStatus: https://www.gridstatus.io

### Supplemental
- EIA 860: https://www.eia.gov/electricity/data/eia860/
- FERC 714: https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-no-714-annual-electric
