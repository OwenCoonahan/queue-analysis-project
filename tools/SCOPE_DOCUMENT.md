# Interconnection Queue Analysis Service
## Scope Document: PE/Infrastructure Investment Due Diligence

**Document Version:** 1.0
**Date:** January 2026
**Purpose:** Define what matters for PE firms evaluating interconnection queue projects

---

## Executive Summary

This document outlines the critical metrics, data sources, and analysis that PE firms need when evaluating interconnection queue projects for data center, renewable energy, or power infrastructure investments. Based on industry research (Berkeley Lab, Morgan Lewis, DNV, FTI Consulting) and PE due diligence best practices.

---

## 1. NEED-TO-HAVE vs NICE-TO-HAVE Analysis

### TIER 1: NEED-TO-HAVE (Critical for Investment Decision)

| Metric | Why It Matters | Data Source | Format |
|--------|---------------|-------------|--------|
| **Queue Position & Date** | Earlier = higher probability of completion, lower cost | Public (ISO queue) | Table |
| **Interconnection Study Phase** | IA Signed > Facilities > SIS > Feasibility (completion correlates to phase) | Public (ISO queue) | Badge/Status |
| **Interconnection Cost Estimate ($/kW)** | Primary driver of project economics; median $102/kW for completed, $599/kW for withdrawn | Studies (Data Room) | Table + Chart |
| **Network Upgrade Requirements** | Single biggest cost variable; can be $0 or $500M+ | Studies (Data Room) | Table |
| **Capacity (MW)** | Size impacts cost structure, timeline, and upgrade requirements | Public (ISO queue) | Metric |
| **POI Congestion** | # of projects at same POI; high congestion = delays & cost sharing | Public (ISO queue) | Metric + Chart |
| **Developer Track Record** | Experienced developers have 2-3x higher completion rates | Public + Research | Badge/Score |
| **Project Type** | Gas: 32% completion, Solar: 14%, Storage: 30% | Public (ISO queue) | Badge |
| **Estimated Timeline to COD** | Median now 4+ years (doubled from 2007) | Calculated | Timeline |
| **Completion Probability** | Only 13-14% of projects reach COD; critical risk factor | Calculated from LBL data | Percentage |

### TIER 2: IMPORTANT (Supports Investment Thesis)

| Metric | Why It Matters | Data Source | Format |
|--------|---------------|-------------|--------|
| **LMP/Nodal Pricing** | Revenue indicator; higher LMP = better economics | Public (ISO data) | Chart |
| **Capacity Market Prices** | Additional revenue stream; varies by region | Public (ISO data) | Table |
| **Transmission Constraints** | Congestion affects both cost and revenue | Public (ISO data) | Map/Table |
| **Withdrawal Rate at POI** | High withdrawal = problem POI or excessive costs | Public (ISO queue) | Percentage |
| **Regional Load Growth** | Data center demand driving 22-32% growth in some regions | Public (EIA/ISO) | Chart |
| **PPA Market Conditions** | Offtake availability and pricing | Market Research | Table |
| **Permitting Status** | Environmental/local approvals can delay or kill projects | Data Room | Checklist |

### TIER 3: NICE-TO-HAVE (Context & Depth)

| Metric | Why It Matters | Data Source | Format |
|--------|---------------|-------------|--------|
| **Affected System Studies** | Can add months/years and significant costs | Data Room | Text |
| **Equipment Procurement** | Supply chain constraints for transformers (18-24mo lead times) | Research | Notes |
| **Grid Operator Policy Changes** | FERC Order 2023 cluster study impacts | Research | Notes |
| **Developer Financial Backing** | Ability to fund IC costs and construction | Data Room | Notes |
| **Land/Site Control** | Required for study advancement | Data Room | Checklist |
| **Environmental Considerations** | Wetlands, endangered species, etc. | Data Room | Checklist |

---

## 2. PUBLIC DATA vs DATA ROOM

### Publicly Available Data (Can be automated)

| Data | Source | Reliability | Refresh Frequency |
|------|--------|-------------|-------------------|
| Queue position, capacity, type, developer | ISO Queue Files | HIGH | Weekly-Monthly |
| Queue date, study phase status | ISO Queue Files | HIGH | Weekly-Monthly |
| POI/Substation | ISO Queue Files | MEDIUM | Weekly-Monthly |
| Historical interconnection costs | LBL/Berkeley Lab | HIGH | Annual |
| LMP/nodal prices | ISO OASIS/Data | HIGH | Daily |
| Capacity market prices | ISO Data | HIGH | Annual auction |
| Transmission constraints | ISO Planning | MEDIUM | Quarterly |
| Historical completion rates | LBL Studies | HIGH | Annual |

### Data Room Only (Requires developer access)

| Data | Why Not Public | Critical? |
|------|----------------|-----------|
| Actual IC cost estimates from studies | Confidential developer data | YES |
| Network upgrade breakdown | Study documents | YES |
| Affected system requirements | Study documents | YES |
| PPA terms (if any) | Commercial confidential | YES |
| Site control documentation | Legal documents | YES |
| Developer financials | Private company data | YES |
| Detailed engineering studies | Technical confidential | MEDIUM |
| Permitting status/timeline | Project specific | MEDIUM |

---

## 3. VISUALIZATION vs TEXT

### Should Be Visualized (Charts/Graphs)

| Metric | Visualization Type | Rationale |
|--------|-------------------|-----------|
| Score Breakdown | Bar Chart / Radar | Quick assessment of strengths/weaknesses |
| Cost Comparison | Scatter Plot | Position vs. historical benchmarks |
| Queue Completion Funnel | Funnel Chart | Show attrition at each stage |
| Timeline Distribution | Box Plot | Show uncertainty range |
| POI Congestion | Heat Map or Bar | Compare concentration |
| LMP History | Line Chart | Revenue trends |
| Risk Profile | Radar Chart | Multi-dimensional risk view |

### Should Be Tables/Text

| Metric | Format | Rationale |
|--------|--------|-----------|
| Project Details | Table | Reference information |
| Cost Estimates (Low/Med/High) | Table | Precise numbers matter |
| Study Phase Status | Badge + Text | Status clarity |
| Red/Green Flags | Bulleted List | Actionable items |
| Due Diligence Checklist | Checkbox List | Track completion |
| Risk Matrix | Table | Categorical assessment |

---

## 4. CURRENT STATE vs NEEDED

### What We Have Now

| Feature | Status | Accuracy | Notes |
|---------|--------|----------|-------|
| NYISO Queue Data | Working | HIGH | From official source |
| Basic Scoring Model | Working | MEDIUM | Needs validation |
| Cost Estimates | Working | LOW | Uses generic benchmarks, not actual study data |
| Timeline Estimates | Working | LOW | Generic, needs regional calibration |
| Completion Rate | Working | MEDIUM | Based on LBL data but simplified |
| POI Congestion | Working | HIGH | Direct from queue |
| Developer Scoring | Working | LOW | Only counts projects, no external data |
| LMP Data | Partial | - | Module exists, needs integration |
| Capacity Market | Partial | - | Module exists, needs integration |
| PDF Export | Working | - | Dark mode (needs light mode) |

### What Needs Improvement

| Gap | Priority | Effort | Impact |
|-----|----------|--------|--------|
| **Cost benchmarks by region/type** | HIGH | Medium | Accuracy |
| **Actual study cost integration** | HIGH | High | Accuracy |
| **Developer database (external)** | HIGH | High | Accuracy |
| **Light mode PDF report** | HIGH | Low | Professionalism |
| **LBL data integration** | MEDIUM | Medium | Validation |
| **Multi-ISO support** | MEDIUM | High | Coverage |
| **LMP integration in report** | MEDIUM | Low | Completeness |
| **Transmission constraint mapping** | LOW | High | Context |
| **Real-time queue monitoring** | LOW | Medium | Freshness |

---

## 5. DATA ACCURACY VALIDATION NEEDED

### Current Data Sources - Validation Status

| Source | File | Last Updated | Validation |
|--------|------|--------------|------------|
| NYISO Queue | `nyiso_queue.xlsx` | Jan 19, 2026 | NEEDS VERIFICATION |
| NYISO Costs | `nyiso_interconnection_cost_data.xlsx` | Jan 14, 2026 | NEEDS VERIFICATION |
| PJM Costs | `pjm_costs_2022_clean_data.xlsx` | 2022 data | OUTDATED |
| MISO Costs | `miso_costs_2021_clean_data.xlsx` | 2021 data | OUTDATED |
| SPP Costs | `spp_costs_2023_clean_data.xlsx` | 2023 data | NEEDS UPDATE |
| ISO-NE Costs | `isone_interconnection_cost_data.xlsx` | Jan 14, 2026 | NEEDS VERIFICATION |
| LBL Queued Up | `lbl_queued_up.xlsx` | Jan 14, 2026 | NEEDS VERIFICATION |

### Key Numbers to Verify Against Industry Sources

| Metric | Our Value | LBL/Industry Value | Status |
|--------|-----------|-------------------|--------|
| Median IC Cost (completed) | $102/kW | $102/kW (LBL 2024) | FIXED |
| Median IC Cost (active) | $156/kW | $156/kW (LBL 2024) | FIXED |
| Median IC Cost (withdrawn) | N/A | $452-599/kW | ADD |
| Overall completion rate | 14% | 13-14% (LBL 2024) | FIXED |
| Solar completion rate | 14% | 14% (LBL 2024) | FIXED |
| Gas completion rate | 32% | 32% (LBL 2024) | FIXED |
| Storage completion rate | 30% | 30% (LBL 2024) | FIXED |
| Wind completion rate | 21% | 21% (LBL 2024) | FIXED |
| Median time to COD | 48 months | 48-56 months (LBL 2024) | OK |

---

## 6. RECOMMENDED IMMEDIATE ACTIONS

### Sprint 1: Foundation (Current Sprint)

1. **Fix PDF to Light Mode** - Professional white background for client-facing reports
2. **Verify Cost Benchmarks** - Align with LBL 2024 data
3. **Correct Completion Rates** - Use actual LBL statistics
4. **Simplify UI** - Focus on clarity over flashiness
5. **Document Data Sources** - Transparency builds trust

### Sprint 2: Accuracy

1. **Integrate LBL Dataset** - Use as primary benchmark source
2. **Regional Cost Calibration** - Different $/kW by ISO
3. **Study Phase Detection** - Better parsing of status fields
4. **Developer Scoring Enhancement** - Add external data sources

### Sprint 3: Completeness

1. **Multi-ISO Support** - PJM, MISO, ERCOT, CAISO
2. **LMP Integration** - Revenue context
3. **Capacity Market Data** - Additional revenue stream
4. **Export Options** - Excel, detailed PDF

---

## 7. REPORT DELIVERABLE STRUCTURE

### Executive Summary (1 page)
- GO / CONDITIONAL / NO-GO recommendation
- Feasibility Score with grade
- Key metrics: Cost estimate, Timeline, Completion probability
- Top 3 risks, Top 3 strengths

### Project Overview (1 page)
- Queue details: ID, date, capacity, type, developer
- Location: State, county, POI
- Current study phase status

### Cost Analysis (1 page)
- Estimated IC costs (Low/Med/High)
- Comparison to regional benchmarks
- Key cost drivers identified
- Network upgrade risk assessment

### Timeline Analysis (1 page)
- Estimated time to COD (scenarios)
- Historical context for similar projects
- Key milestones and dependencies

### Risk Assessment (1 page)
- Score breakdown by category
- Red flags / Green flags
- Risk matrix (Technical, Cost, Timeline, Developer, POI)

### Due Diligence Checklist (1 page)
- Data room items to obtain
- Questions for developer
- Independent verification steps

### Appendix
- Methodology notes
- Data sources
- Glossary

---

## 8. KEY SOURCES

- [Berkeley Lab - Queued Up Reports](https://emp.lbl.gov/queues)
- [Berkeley Lab - Interconnection Cost Analysis](https://emp.lbl.gov/interconnection_costs)
- [Morgan Lewis - PE in Data Centers](https://www.morganlewis.com/pubs/2025/09/in-the-know-private-equity-in-data-centers-growth-risks-and-opportunities)
- [Data Center Frontier - PE & Infrastructure](https://www.datacenterfrontier.com/colocation/article/55288208/private-equity-and-infrastructure-what-the-new-investment-landscape-means-for-data-centers)
- [FTI Consulting - Energy Transition Due Diligence](https://www.fticonsulting.com/insights/articles/due-diligence-considerations-investing-energy-transition-part-1)
- [DNV - Technical Due Diligence](https://www.dnv.com/services/technical-and-commercial-due-diligence-of-renewable-projects-2595/)

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | Jan 2026 | AI Assistant | Initial scope document |
