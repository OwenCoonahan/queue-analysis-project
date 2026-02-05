# Multi-Agent Development Setup

## Overview

This project uses a distributed AI agent architecture:

| Machine | Role | Agent | Availability |
|---------|------|-------|--------------|
| **MacBook (Laptop)** | Development | Claude Code | On-demand |
| **Mac Mini (Server)** | Operations | LLM + Coding Agents | 24/7 |

Both agents access this codebase via GitHub and can make changes.

---

## Machine Responsibilities

### MacBook (Laptop) - Development
- Feature development and code changes
- Data analysis and exploration
- Dashboard UI work
- Ad-hoc queries and research
- Push changes to GitHub

### Mac Mini (Server) - Operations
- **24/7 availability**
- Scheduled data refreshes (cron jobs)
- ISO queue scraping
- Database maintenance
- Server hosting (Streamlit, APIs)
- Automated monitoring
- Pull changes from GitHub, run updated code

---

## Coordination Protocol

### Git Workflow
```
┌─────────────┐     push      ┌──────────┐     pull      ┌─────────────┐
│   Laptop    │ ───────────▶  │  GitHub  │  ◀─────────── │  Mac Mini   │
│ Claude Code │ ◀───────────  │   Repo   │  ───────────▶ │   Server    │
└─────────────┘     pull      └──────────┘     push      └─────────────┘
```

### Avoiding Conflicts
1. **Laptop**: Focuses on `/tools/*.py` code changes
2. **Mac Mini**: Focuses on `/tools/.data/` and operational tasks
3. **Database writes**: Mac Mini owns scheduled refreshes
4. **Code changes**: Laptop develops, Mac Mini pulls and runs

### Communication Files
- `tools/.data/last_refresh.json` - Last refresh timestamp (Mac Mini writes)
- `tools/.data/change_log.json` - Data change tracking

---

## Mac Mini Cron Schedule

```cron
# Daily data refresh at 6am
0 6 * * * cd /path/to/queue-analysis-project/tools && python refresh_data.py --all

# Pull latest code at 5:55am (before refresh)
55 5 * * * cd /path/to/queue-analysis-project && git pull

# Weekly full database backup on Sunday
0 3 * * 0 cd /path/to/queue-analysis-project/tools && python -c "from queue_db import backup_database; backup_database()"
```

---

## Setup Instructions

### Mac Mini Initial Setup

1. **Clone the repo**
   ```bash
   gh repo clone OwenCoonahan/queue-analysis-project
   cd queue-analysis-project
   ```

2. **Unzip data transfer**
   ```bash
   # After receiving queue-data.zip via AirDrop
   unzip ~/Downloads/queue-data.zip -d .
   ```

3. **Set up Python environment**
   ```bash
   cd tools
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

4. **Verify database**
   ```bash
   sqlite3 .data/queue_v2.db "SELECT COUNT(*) FROM projects;"
   ```

5. **Set up cron jobs**
   ```bash
   crontab -e
   # Add the cron schedule above
   ```

6. **Test dashboard**
   ```bash
   streamlit run app.py --server.headless true
   ```

---

## Data Flow

```
                    ┌─────────────────────────────────────┐
                    │           External Sources          │
                    │  (ISO websites, EIA, FERC, PUDL)    │
                    └───────────────┬─────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────┐
│                         MAC MINI (SERVER)                             │
│  ┌─────────────┐    ┌──────────────┐    ┌─────────────────────────┐   │
│  │ Cron Jobs   │───▶│ refresh_data │───▶│ .data/queue_v2.db       │   │
│  │ (6am daily) │    │    .py       │    │ .cache/iso_files        │   │
│  └─────────────┘    └──────────────┘    └─────────────────────────┘   │
│                                                   │                    │
│                                                   ▼                    │
│                                          ┌──────────────┐              │
│                                          │ Streamlit    │              │
│                                          │ Dashboard    │              │
│                                          │ (port 8501)  │              │
│                                          └──────────────┘              │
└───────────────────────────────────────────────────────────────────────┘
                    │
                    │ git push (operational fixes)
                    ▼
            ┌──────────────┐
            │    GitHub    │
            └──────────────┘
                    ▲
                    │ git push (features)
                    │
┌───────────────────────────────────────────────────────────────────────┐
│                       LAPTOP (DEVELOPMENT)                            │
│  ┌─────────────────┐    ┌──────────────────────────────────────────┐  │
│  │  Claude Code    │───▶│  Code changes, analysis, feature work    │  │
│  └─────────────────┘    └──────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────┘
```

---

## Agent Instructions

### For Laptop Agent (Claude Code)
- You are the primary development agent
- Push code changes to GitHub
- Notify when pushing changes that affect data refresh
- Check `last_refresh.json` before triggering manual data loads

### For Mac Mini Agent
- You are the operations/server agent
- Pull code changes before running refreshes
- Own the database write operations for scheduled tasks
- Push only operational fixes or generated reports
- Keep the Streamlit dashboard running
- Monitor for scraping failures and retry

---

## Shared Settings

Claude Code settings are in `.claude/settings.json` (committed to git).
Each machine can have local overrides in `.claude/settings.local.json` (not in git).
