# Interconnection Queue Data Sources

## Summary

| Source | Cost | Coverage | Update Frequency | Best For |
|--------|------|----------|------------------|----------|
| GridStatus (open source) | Free | 9 ISOs | Live API | Automated analysis, scripting |
| Lawrence Berkeley Lab | Free | ~97% of US capacity | Annual (Excel) | Historical benchmarks, research |
| Interconnection.fyi | Subscription | 50+ operators | Daily | Comprehensive tracking |
| RTO Websites | Free | Individual RTO | Varies | Primary source verification |

---

## 1. GridStatus (Open Source Python Library)

**Website:** https://opensource.gridstatus.io
**PyPI:** https://pypi.org/project/gridstatus/
**GitHub:** https://github.com/gridstatus/gridstatus

### Installation
```bash
pip install gridstatus
```

### Supported ISOs
- CAISO (California)
- PJM (Mid-Atlantic/Midwest)
- ERCOT (Texas)
- MISO (Midwest)
- NYISO (New York)
- ISONE (New England)
- SPP (Southwest Power Pool)
- IESO (Ontario)
- EIA (aggregate)

### Basic Usage
```python
import gridstatus

# Get queue for a specific ISO
pjm = gridstatus.PJM()
queue = pjm.get_interconnection_queue()

# Get queues from ALL ISOs at once
all_queues = gridstatus.get_interconnection_queues()
```

### Data Returned
- Returns pandas DataFrame
- Includes active, completed, and withdrawn projects
- Standardized column names across ISOs (where possible)
- ISO-specific fields appended to end of dataframe

### Typical Queue Fields (standardized)
- Queue ID / Request ID
- Project Name
- Developer / Entity Name
- Fuel Type / Generation Type
- Capacity (MW)
- Point of Interconnection (POI)
- County / State
- Queue Date / Request Date
- Target COD / Proposed In-Service Date
- Status (Active, Withdrawn, Complete)
- Study Phase
- Transmission Owner

### Commercial API
GridStatus also offers a commercial hosted API at https://www.gridstatus.io/pricing with additional features. Contact for pricing.

---

## 2. Lawrence Berkeley National Lab - "Queued Up"

**Website:** https://emp.lbl.gov/queues
**Direct Download:** Available at website (Excel file)

### What's Included
1. **Full project-level dataset** - All interconnection requests through end of 2024
2. **Codebook** - Data dictionary explaining each field
3. **35 summary tabs** - Pre-built analysis tables

### Coverage
- 7 ISO/RTOs
- 49 non-ISO utilities
- ~97% of US installed electric generating capacity

### 2025 Edition Key Stats (data through 2024)
- ~10,300 active projects
- 1,400 GW generation capacity in queue
- 890 GW storage capacity in queue
- 12% decrease in queue volume YoY (due to withdrawals)
- Natural gas: 136 GW (+72% YoY)
- Solar: 956 GW (-12% YoY)
- Storage: 890 GW (-13% YoY)
- Wind: 271 GW (-26% YoY)

### Best Use Cases
- Historical trend analysis
- Withdrawal rate benchmarks
- Cross-RTO comparisons
- Research and reports

---

## 3. Interconnection.fyi

**Website:** https://www.interconnection.fyi
**Contact:** contact@interconnection.fyi

### Coverage
- 41,653 queue requests tracked (1995-2025)
- ~9,000 active generation requests
- 1.82 TW total active capacity
- All major US ISOs + Canadian markets
- Non-ISO utilities across all 50 states

### Data Fields Tracked
- Status (Active, Withdrawn, Operational, Suspended)
- Energy type (Solar, Wind, Battery, Gas, etc.)
- Geographic location (state, county)
- Transmission owner
- Year enqueued
- Capacity (MW)

### Access
- Web interface with filters/charts (free)
- Full data download requires subscription
- Updated daily (latest: checked daily)

### Best Use Cases
- Quick project lookups
- Visual exploration
- Daily monitoring

---

## 4. RTO Primary Sources

### PJM
**Queue Portal:** https://www.pjm.com/planning/service-requests
**Tools:**
- Queue Scope (interactive map)
- Resource Tracker (Excel export)
- Planning Center portal

**Access Levels:**
- Public Version (no login, some restrictions)
- Secure Version (login required)
- Secure Version + CEII (additional sensitive data)

**Key Reports:**
- Serial Service Request Status
- Cycle Service Request Status
- Interconnection Study Statistics (PDF)

### ERCOT
**Queue Portal:** https://www.ercot.com/gridinfo/resource
**Key Report:** Monthly Generator Interconnection Status Report (GIS Report, PG7-200-ER)

**Additional Data:**
- Capacity Changes by Fuel Type Charts (Excel)
- Historical additions and planned projects

### MISO
**Queue Portal:** https://www.misoenergy.org/planning/generator-interconnection/GI_Queue/
**API Endpoint:** https://www.misoenergy.org/api/giqueue/getprojects

### CAISO
**Queue Portal:** Available through CAISO OASIS
**Studies:** Posted publicly for each project

### NYISO
**Queue Portal:** Available as Excel download
**URL:** https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx

---

## 5. Additional Sources

### FERC eLibrary
**URL:** https://elibrary.ferc.gov
**Content:** Interconnection agreements, FERC filings, regulatory orders
**Cost:** Free
**Use Case:** Verify interconnection agreement terms, track regulatory issues

### OASIS (Open Access Same-Time Information System)
**Content:** Transmission availability, ATC data
**Access:** Through individual RTO websites
**Use Case:** Understand transmission constraints

### SEC Filings
**URL:** https://www.sec.gov/edgar
**Content:** Public company disclosures
**Use Case:** Developer financial health, project announcements

---

## Recommended Data Stack

### For Starting Out (Free)
1. **GridStatus open source** - Automated data pulls
2. **LBL Queued Up Excel** - Historical benchmarks
3. **RTO websites** - Verification and study documents

### For Scale ($500-1000/month)
1. **GridStatus commercial API** - Enhanced data, better support
2. **Interconnection.fyi subscription** - Daily updates, full export
3. **RTO websites** - Study documents, CEII access

---

## Data Quality Notes

### Common Issues
- **Name variations** - Same developer appears under different entity names
- **POI naming** - Substations have multiple naming conventions
- **Status lag** - RTO data may not reflect recent withdrawals immediately
- **Capacity changes** - Projects often resize during study process

### Validation Best Practices
1. Cross-reference multiple sources
2. Verify against RTO primary source for official status
3. Check FERC filings for interconnection agreements
4. Track study documents for cost estimates

---

*Last updated: January 2026*
