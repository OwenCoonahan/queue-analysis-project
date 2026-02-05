# Interconnection Queue Analysis Tools

Fast, data-driven feasibility assessments for interconnection queue projects.

## Quick Start

```bash
# Install dependencies
pip3 install -r requirements.txt

# Generate an HTML report for a project
python3 report.py 1738 --client "KPMG" --html

# Generate a markdown report
python3 report.py 1738 --client "KPMG" --markdown

# List all projects in queue
python3 report.py --list

# Rank all projects by feasibility score
python3 report.py --rank -o rankings.csv
```

## Directory Structure

```
tools/
├── report.py           # Main CLI entry point
├── html_report.py      # HTML report generator
├── deep_report.py      # Markdown report generator
├── pdf_export.py       # PDF report generator (WeasyPrint)
├── dashboard.py        # Streamlit interactive dashboard
├── unified_data.py     # Cross-RTO unified data query tool
├── refresh_data.py     # Data refresh script (updates SQLite)
├── data_store.py       # SQLite database manager
├── analyze.py          # Data loading and basic analysis
├── scoring.py          # 100-point feasibility scoring model
├── real_data.py        # Historical data estimates (costs, timelines)
├── scrapers.py         # External research (SEC, news, cross-RTO)
├── historical_data.py  # LBL Queued Up data loader
├── charts_altair.py    # Chart generation (Altair - recommended)
├── charts.py           # Chart generation (matplotlib/plotly - legacy)
├── chart_verify.py     # Playwright-based chart verification
├── map_viz.py          # Geographic map visualization (Folium)
├── requirements.txt    # Python dependencies
├── .data/              # SQLite database (queue.db)
├── .cache/             # Downloaded data files (auto-populated)
├── output/             # Generated reports
├── charts/             # Generated chart images
└── maps/               # Generated map HTML files
```

## Usage

### Generate Reports

```bash
# HTML Report (recommended - visual, self-contained)
python3 report.py <QUEUE_ID> --client "<CLIENT_NAME>" --html

# Markdown Report (for further editing)
python3 report.py <QUEUE_ID> --client "<CLIENT_NAME>" --markdown -o report.md

# PDF Report (requires WeasyPrint system dependencies)
python3 pdf_export.py <QUEUE_ID> --client "<CLIENT_NAME>" -o report.pdf

# Examples
python3 report.py 1738 --client "Acme Capital" --html
python3 report.py 1738 --client "KPMG" --markdown -o analysis.md
python3 pdf_export.py 1738 --client "KPMG" -o feasibility_report.pdf
```

### Interactive Dashboard

Launch the Streamlit dashboard for interactive exploration:

```bash
streamlit run dashboard.py
```

Features:
- Filter projects by type, capacity, score, recommendation
- Score distribution and recommendation breakdown charts
- Sortable project list with CSV export
- Deep dive view with executive summary cards
- Traffic light risk indicators

### Geographic Maps

Create interactive maps showing project locations:

```python
from map_viz import create_poi_map, create_congestion_heatmap

# Load your project data
df = ...  # DataFrame with poi, score, recommendation columns

# Create POI map (color by recommendation)
create_poi_map(df, color_by='recommendation')
# Output: maps/poi_map.html

# Create congestion heatmap
create_congestion_heatmap(df)
# Output: maps/congestion_heatmap.html
```

### Batch Analysis

```bash
# List all projects
python3 report.py --list

# Rank all projects and export to CSV
python3 report.py --rank -o rankings.csv

# Show top 50 projects
python3 report.py --rank --top 50
```

### Cross-RTO Queries (unified_data.py)

Query interconnection queues across all RTOs (38,430 projects across 9 regions):

```bash
# Search by developer across all RTOs
python3 unified_data.py --developer "NextEra"

# Search by region, type, and size
python3 unified_data.py --region ERCOT --type Solar --min-mw 100

# Get developer profile with success rate
python3 unified_data.py --profile "Invenergy"

# Compare multiple developers
python3 unified_data.py --compare "NextEra,Invenergy,EDF,Engie"

# View queue statistics
python3 unified_data.py --stats
python3 unified_data.py --stats --region PJM

# List all available regions
python3 unified_data.py --regions

# Export search results to CSV
python3 unified_data.py --developer "NextEra" --output nextera_projects.csv
```

#### Data Coverage

| Region | Projects | Developer Data |
|--------|----------|----------------|
| PJM | 8,152 | Limited (names only) |
| West | 7,675 | 12% |
| ERCOT | 5,092 | 92% |
| MISO | 4,978 | Limited |
| Southeast | 3,950 | 24% |
| CAISO | 2,837 | Limited |
| SPP | 2,469 | Limited |
| NYISO | 1,996 | 100% |
| ISO-NE | 1,281 | Limited |

**Note:** Developer track record analysis works best for ERCOT and NYISO where developer data is complete.

### Direct Module Usage

```python
from analyze import QueueData
from scoring import FeasibilityScorer
from real_data import RealDataEstimator

# Load data
loader = QueueData()
df = loader.load_nyiso()

# Score a project
scorer = FeasibilityScorer(df)
result = scorer.score_project(project_id="1738")
print(f"Score: {result['total_score']}/100")
print(f"Recommendation: {result['recommendation']}")

# Get real data estimates
estimator = RealDataEstimator()
estimates = estimator.estimate_project(
    region="NYISO",
    project_type="L",
    capacity_mw=1000,
    months_in_queue=6
)
print(f"Cost range: {estimator.format_cost_range(estimates['cost'])}")
print(f"Completion rate: {estimator.format_completion_rate(estimates['completion'])}")
```

## Report Contents

### HTML Report Includes:
- Executive summary with score visualization
- Project overview table
- Feasibility score breakdown with risk bar chart
- Cost analysis with scatter plot comparison
- Timeline analysis with box plot
- Historical queue outcomes (donut chart)
- Risk assessment (red/green flags)
- Due diligence checklist

### Data Sources:
- **Queue Data**: NYISO interconnection queue (auto-downloaded)
- **Cost Benchmarks**: LBL + Regional interconnection cost datasets
- **Timeline Data**: LBL Queued Up historical IR-to-COD times
- **Completion Rates**: Historical queue outcomes by region and type
- **External Research**: SEC EDGAR, web search, cross-RTO developer presence

## Scoring Model

100-point feasibility scoring:

| Component | Weight | Description |
|-----------|--------|-------------|
| Queue Position | 25 pts | Position relative to other projects at POI |
| Study Progress | 25 pts | Stage in interconnection study process |
| Developer Track Record | 20 pts | Number and success of other projects |
| POI Congestion | 15 pts | Competition at point of interconnection |
| Project Characteristics | 15 pts | Size, type, and viability factors |

**Recommendation Thresholds:**
- **GO** (70+): Proceed with standard due diligence
- **CONDITIONAL** (50-69): Enhanced due diligence required
- **NO-GO** (<50): Pass or require significant risk mitigation

## Data Storage

### SQLite Database (Primary)

Data is stored in `.data/queue.db` - a SQLite database with:
- 35,500+ projects across 9 RTOs
- Change tracking (new projects, status changes)
- Refresh logging

```bash
# Refresh data from all sources
python3 refresh_data.py

# Refresh specific source
python3 refresh_data.py --source ercot

# Check refresh status
python3 refresh_data.py --status

# View recent changes
python3 refresh_data.py --changes 7
```

### Data Refresh Sources

| Source | Type | Auto-Refresh | Notes |
|--------|------|--------------|-------|
| `nyiso` | Live | Yes | Downloads from NYISO directly |
| `ercot` | Live API | Yes | ERCOT GIS report API |
| `lbl` | Historical | Manual | Annual release from LBL |

### Cache Files (Fallback)

Raw data files are cached in `.cache/`:

| File | Source | Contents |
|------|--------|----------|
| `nyiso_queue.xlsx` | NYISO | Current queue data (179 projects) |
| `ercot_gis_report.xlsx` | ERCOT | Current GIS report (1,810 projects) |
| `lbl_queued_up.xlsx` | LBL | Historical queue analysis (36,441 projects) |
| `nyiso_interconnection_cost_data.xlsx` | LBL | NYISO cost data |
| `pjm_costs_2022_clean_data.xlsx` | LBL | PJM cost data |
| `miso_costs_2021_clean_data.xlsx` | LBL | MISO cost data |
| `spp_costs_2023_clean_data.xlsx` | LBL | SPP cost data |
| `isone_interconnection_cost_data.xlsx` | LBL | ISO-NE cost data |

### Change Tracking

The system tracks:
- New projects entering the queue
- Status changes (e.g., Feasibility Study -> System Impact Study)
- Historical snapshots for trend analysis

```bash
# View changes in last 30 days
python3 refresh_data.py --changes 30
```

## Requirements

- Python 3.9+
- Core: pandas, numpy, requests, openpyxl, python-dateutil
- Charts: altair, vl-convert-python (recommended), plotly, matplotlib
- Dashboard: streamlit, streamlit-folium, folium
- PDF Export: weasyprint (requires system libraries)
- Chart verification: playwright (optional)

Install all dependencies:
```bash
pip3 install -r requirements.txt

# For PDF export on macOS (WeasyPrint system dependencies):
brew install pango gdk-pixbuf libffi

# For chart verification (optional):
playwright install chromium
```

## Output

Reports are saved to `output/` by default:
- `output/report_<ID>.html` - HTML reports
- `output/report_<ID>.md` - Markdown reports
- `output/rankings.csv` - Batch ranking exports

Charts are saved to `charts/` (Altair format):
- `cost_scatter_altair.png` - Cost comparison scatter plot
- `queue_outcomes_altair.png` - Queue status distribution (donut)
- `risk_bars_altair.png` - Risk factor analysis (horizontal bars)
- `timeline_altair.png` - Time to COD distribution
- `completion_rates_altair.png` - Developer completion rates

## Troubleshooting

**"No data loaded" error:**
- Check internet connection (data auto-downloads)
- Clear `.cache/` folder and retry

**Charts not generating:**
- Install Altair: `pip3 install altair vl-convert-python`
- For visual verification: `pip3 install playwright && playwright install chromium`

**PDF export not working (macOS):**
- WeasyPrint requires system libraries: `brew install pango gdk-pixbuf libffi`
- If `libgobject-2.0-0` error persists, try: `brew reinstall glib`
- Alternative: Use HTML export which works without system dependencies

**Streamlit dashboard issues:**
- Ensure `streamlit` is installed: `pip3 install streamlit streamlit-folium`
- If port 8501 is in use: `streamlit run dashboard.py --server.port 8502`

**SSL warnings:**
- These are informational and can be ignored
- If problematic, upgrade OpenSSL or use `--file` with local data
