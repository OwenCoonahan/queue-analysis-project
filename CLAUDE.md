# Queue Analysis - Project Instructions

## Overview
This is an interconnection queue analysis platform for power grid project development. It aggregates data from all major US ISOs (PJM, MISO, SPP, ERCOT, CAISO, NYISO, ISO-NE) and provides analytics for evaluating project viability.

## Architecture

### Multi-Agent Setup
This codebase is accessed by two AI agents:
- **Laptop (Claude Code)**: Primary development, feature work, analysis
- **Mac Mini (24/7 Server)**: Automated data refresh, cron jobs, scraping, server operations

Both agents push/pull from GitHub. See `MULTI_AGENT_SETUP.md` for coordination details.

### Key Directories
```
/tools/              # Main application code
  app.py             # Streamlit dashboard (main entry point)
  refresh_data.py    # Data refresh orchestration
  unified_data.py    # Cross-ISO data normalization
  scoring.py         # Project scoring algorithms
  intelligence.py    # Market intelligence module
  queue_db.py        # Database operations

/tools/.data/        # SQLite databases (not in git)
  queue.db           # Main queue database
  queue_v2.db        # V2 schema
  enrichment.db      # Enriched data

/tools/.cache/       # Downloaded ISO files (not in git)
```

### Running the Dashboard
```bash
cd tools
source .venv/bin/activate
streamlit run app.py
```

### Refreshing Data
```bash
cd tools
python refresh_data.py --all  # Refresh all ISOs
python refresh_data.py --iso miso  # Single ISO
```

## Data Sources
- **ISO Queues**: Direct from ISO websites/APIs
- **EIA 860**: Generator data from Energy Information Administration
- **PUDL**: Public Utility Data Liberation project
- **FERC**: Federal Energy Regulatory Commission filings
- **Energy Communities**: IRA bonus eligibility zones

## Code Style
- Python 3.12+
- Use existing patterns in codebase
- SQLite for storage (not DuckDB yet)
- Streamlit for UI
- Altair for charts

## Important Notes
- Database files (.data/, .cache/) are NOT in git - transfer separately
- The Mac Mini runs scheduled refreshes; avoid conflicting writes
- Check `last_refresh.json` before triggering manual refreshes
