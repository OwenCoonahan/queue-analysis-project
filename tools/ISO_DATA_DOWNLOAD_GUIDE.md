# ISO Data Download Quick Reference

**Last Updated:** February 2026

Use this guide when manually downloading queue data from each ISO.

---

## Quick Reference Table

| ISO | Download Method | Auto-Refresh? | File to Save As |
|-----|-----------------|---------------|-----------------|
| **PJM** | Manual Excel | No | `.cache/pjm_planning_queues.xlsx` |
| **MISO** | Auto (API) | Yes | Auto-cached |
| **NYISO** | Auto (Excel) | Yes | Auto-cached |
| **ERCOT** | Auto (GIS) | Yes | Auto-cached |
| **CAISO** | Auto (Excel) | Yes* | Auto-cached |
| **SPP** | Auto (gridstatus) | Yes | Auto-cached |
| **ISO-NE** | Auto (gridstatus) | Yes* | Auto-cached |

*Has known data quality issues

---

## PJM - Manual Download Required

**URL:** https://www.pjm.com/planning/services-requests/interconnection-queues

**Steps:**
1. Click "Queue (Excel)" to download main queue
2. Save as: `.cache/pjm_planning_queues.xlsx`
3. (Optional) Download Cycle Projects for developer names
4. Save as: `.cache/pjm_cycle_projects.xlsx`

**Refresh Command:**
```bash
python3 pjm_loader.py --refresh
```

---

## MISO - Automatic API

**URL:** https://www.misoenergy.org/planning/generator-interconnection/GI_Queue/

The system auto-fetches from MISO's JSON API.

**Refresh Command:**
```bash
python3 refresh_data.py --source miso
```

**Manual Download (if needed):**
1. Go to the URL above
2. Export queue to Excel
3. Save as: `.cache/miso_queue_manual.xlsx`

---

## NYISO - Automatic Excel

**URL:** https://www.nyiso.com/interconnections

**Direct Download:** https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx

The system auto-downloads this file.

**Refresh Command:**
```bash
python3 refresh_data.py --source nyiso
```

**Manual Download (if needed):**
1. Go to the Interconnections page
2. Download "NYISO Interconnection Queue" Excel file
3. Save as: `.cache/nyiso_queue.xlsx`

---

## ERCOT - Automatic GIS Report

**URL:** https://www.ercot.com/gridinfo/generation

The system auto-fetches the latest GIS report from ERCOT's MIS portal.

**Refresh Command:**
```bash
python3 refresh_data.py --source ercot
```

**Manual Download (if needed):**
1. Go to Grid Information > Generation
2. Find "Generation Interconnection Status" (GIS) report
3. Download the latest Excel file
4. Save as: `.cache/ercot_gis_report.xlsx`

**Note:** ERCOT GIS report does NOT contain queue dates. Dates are backfilled from LBL historical data.

---

## CAISO - Automatic Excel (Has Issues)

**URL:** http://www.caiso.com/planning/Pages/GeneratorInterconnection/Default.aspx

**Direct Download:** http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx

The system auto-downloads this file, BUT queue dates are frozen at April 2021.

**Refresh Command:**
```bash
python3 refresh_data.py --source caiso
```

**Known Issue:**
- Queue dates in the public file stop at April 2021
- All newer projects show the same frozen date
- This is a CAISO data source issue, not our code

**Alternative Sources to Investigate:**
- CAISO OASIS: http://oasis.caiso.com/
- CAISO Resource Adequacy portal
- Direct contact with CAISO

---

## SPP - Automatic via gridstatus

**URL:** https://opsportal.spp.org/Studies/GIActive

The system uses the `gridstatus` Python library to fetch SPP data.

**Refresh Command:**
```bash
python3 refresh_data.py --source spp
```

**Manual Download (if needed):**
1. Go to SPP OASIS portal
2. Navigate to Studies > GI Active
3. Export to Excel
4. Save as: `.cache/spp_queue_manual.xlsx`

---

## ISO-NE - Automatic via gridstatus (Has Issues)

**URL:** https://irtt.iso-ne.com/reports/external

The system uses the `gridstatus` Python library, BUT data stops at January 2025.

**Refresh Command:**
```bash
python3 refresh_data.py --source isone
```

**Known Issue:**
- gridstatus data for ISO-NE is 13+ months behind
- Latest queue date: January 2025

**Alternative Sources to Investigate:**
- ISO-NE IRTT Portal: https://irtt.iso-ne.com/
- ISO-NE Web Services API (no queue endpoint found)
- Direct file download from IRTT reports

**Manual Download:**
1. Go to https://irtt.iso-ne.com/reports/external
2. Export interconnection queue report
3. Save as: `.cache/isone_queue_manual.xlsx`

---

## After Downloading Files

### Refresh Individual Source
```bash
python3 refresh_data.py --source [iso_name]
```

### Refresh All Sources
```bash
python3 refresh_data.py
```

### Rebuild V2 Database (Quick)
```bash
python3 refresh_v2.py --quick
```

### Full Refresh + V2 Rebuild
```bash
python3 refresh_v2.py
```

---

## File Locations Summary

```
tools/.cache/
├── pjm_planning_queues.xlsx     # PJM main queue (manual)
├── pjm_cycle_projects.xlsx      # PJM cycle data (manual)
├── miso_queue_direct.parquet    # MISO (auto)
├── nyiso_queue.xlsx             # NYISO (auto)
├── ercot_gis_raw.xlsx           # ERCOT (auto)
├── caiso_queue_direct.xlsx      # CAISO (auto)
├── spp_queue_direct.parquet     # SPP (auto)
├── isone_queue_direct.parquet   # ISO-NE (auto)
└── lbl_queued_up.xlsx           # LBL historical (manual, annual)
```

---

## Data Quality Status

| ISO | Queue Dates | Developer Names | Last Good Date |
|-----|-------------|-----------------|----------------|
| **PJM** | 99.5% | 18.5% | Feb 2026 |
| **MISO** | 100% | 99% | Nov 2025 |
| **NYISO** | 100% | 100% | Dec 2025 |
| **ERCOT** | 35%* | 100% | Dec 2024 |
| **CAISO** | 100%** | 0% | Apr 2021 |
| **SPP** | 100% | 61% | Jan 2026 |
| **ISO-NE** | 100% | 74% | Jan 2025 |

*ERCOT dates backfilled from LBL
**CAISO dates frozen at April 2021

---

## Troubleshooting

### "Using cached data" message
The system caches downloads for 24 hours. To force fresh download:
```bash
python3 refresh_data.py --source [iso] --force
```

Or delete the cache file:
```bash
rm .cache/[iso]_queue*.xlsx
```

### gridstatus errors (SPP/ISO-NE)
Ensure Python 3.10+ and gridstatus is installed:
```bash
pip install gridstatus
```

### Excel parsing errors
Some ISOs change their Excel formats. Check:
1. Header row location (some have metadata rows at top)
2. Column name changes
3. Sheet name changes

---

*Guide created by Claude Code, February 2026*
