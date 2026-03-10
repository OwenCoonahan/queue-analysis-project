"""
Minimal CSS styles for PDF reports.

Clean, traditional financial report styling without AI-generated visual patterns.
"""


def get_base_css() -> str:
    """Base CSS used by all report types."""
    return '''
    @page {
        size: letter;
        margin: 0.6in 0.7in 0.7in 0.7in;
        @bottom-center {
            content: counter(page);
            font-size: 9px;
            color: #666;
            font-family: "Times New Roman", Georgia, serif;
        }
    }

    @page :first {
        margin-top: 0;
    }

    * {
        box-sizing: border-box;
    }

    body {
        font-family: "Times New Roman", Georgia, serif;
        font-size: 10px;
        line-height: 1.5;
        color: #1a1a1a;
        background: white;
    }

    p, li, tr {
        orphans: 3;
        widows: 3;
    }
    '''


def get_deal_report_css() -> str:
    """Minimal CSS for deal reports - traditional financial style."""
    return get_base_css() + '''

    /* =============================================
       HEADER - Simple, professional
       ============================================= */
    .header {
        background: #1a1a1a;
        color: white;
        padding: 28px 32px 24px 32px;
        margin: -0.6in -0.7in 24px -0.7in;
        width: calc(100% + 1.4in);
    }

    .header-badge {
        display: inline-block;
        font-size: 9px;
        font-weight: 400;
        letter-spacing: 2px;
        text-transform: uppercase;
        margin-bottom: 10px;
        opacity: 0.7;
    }

    .header h1 {
        font-size: 22px;
        margin: 0 0 4px 0;
        font-weight: 400;
        font-family: "Times New Roman", Georgia, serif;
    }

    .header-project {
        font-size: 13px;
        margin-bottom: 12px;
        font-weight: 400;
    }

    .header-meta {
        display: flex;
        gap: 24px;
        font-size: 9px;
        opacity: 0.7;
    }

    .header-meta span {
        font-family: -apple-system, sans-serif;
    }

    /* =============================================
       EXECUTIVE SUMMARY
       ============================================= */
    .exec-grid {
        display: flex;
        gap: 28px;
        margin-bottom: 20px;
    }

    /* Score Card - Clean */
    .score-card {
        min-width: 160px;
        text-align: center;
        padding: 18px 24px;
        border: 1px solid #ddd;
    }

    .score-card::before { display: none; }

    .score-gauge {
        width: 90px;
        height: 90px;
        margin: 0 auto 10px;
        position: relative;
    }

    .score-gauge svg {
        transform: rotate(-90deg);
    }

    .gauge-bg {
        fill: none;
        stroke: #e5e5e5;
        stroke-width: 6;
    }

    .gauge-fill {
        fill: none;
        stroke-width: 6;
        stroke-linecap: round;
    }

    .gauge-fill.go { stroke: #2d7a2d; }
    .gauge-fill.conditional { stroke: #b8860b; }
    .gauge-fill.nogo { stroke: #a02020; }

    .score-center {
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        text-align: center;
    }

    .score-number {
        font-size: 28px;
        font-weight: 400;
        color: #1a1a1a;
        line-height: 1;
        font-family: "Times New Roman", Georgia, serif;
    }

    .score-max {
        font-size: 10px;
        color: #666;
    }

    .verdict-pill {
        display: inline-block;
        padding: 6px 18px;
        font-weight: 700;
        font-size: 10px;
        color: white;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin: 10px 0 8px 0;
        font-family: -apple-system, sans-serif;
    }

    .verdict-pill.go { background: #2d7a2d; }
    .verdict-pill.conditional { background: #b8860b; }
    .verdict-pill.nogo { background: #a02020; }

    .score-meta {
        font-size: 9px;
        color: #666;
    }

    .score-percentile {
        font-size: 9px;
        color: #666;
        margin-top: 6px;
        font-style: italic;
    }

    /* KPI Grid - Clean boxes */
    .kpi-grid {
        flex: 1;
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 12px;
    }

    .kpi-card {
        padding: 12px 14px;
        border: 1px solid #ddd;
    }

    .kpi-card.cost,
    .kpi-card.prob,
    .kpi-card.cod,
    .kpi-card.comp {
        border-left-width: 1px;
        border-left-color: #ddd;
    }

    .kpi-label {
        font-size: 8px;
        color: #666;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 3px;
        font-family: -apple-system, sans-serif;
    }

    .kpi-value {
        font-size: 16px;
        font-weight: 400;
        color: #1a1a1a;
        line-height: 1.1;
        font-family: "Times New Roman", Georgia, serif;
    }

    .kpi-detail {
        font-size: 8px;
        color: #888;
        margin-top: 2px;
    }

    /* Risk Alert - Subdued */
    .risk-alert {
        display: flex;
        gap: 24px;
        padding: 12px 16px;
        background: #f9f9f9;
        border: 1px solid #ddd;
        margin-top: 16px;
    }

    .risk-alert-item {
        flex: 1;
    }

    .risk-alert-label {
        font-size: 8px;
        font-weight: 700;
        color: #666;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-family: -apple-system, sans-serif;
    }

    .risk-alert-value {
        font-size: 10px;
        color: #333;
        margin-top: 2px;
        line-height: 1.4;
    }

    /* =============================================
       SECTIONS
       ============================================= */
    .section {
        margin-bottom: 24px;
        page-break-inside: avoid;
    }

    h2 {
        font-size: 12px;
        font-weight: 700;
        color: #1a1a1a;
        border-bottom: 1px solid #1a1a1a;
        padding-bottom: 6px;
        margin: 0 0 14px 0;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-family: -apple-system, sans-serif;
    }

    h3 {
        font-size: 11px;
        font-weight: 600;
        color: #333;
        margin: 0 0 10px 0;
    }

    h4 {
        font-size: 10px;
        font-weight: 600;
        color: #333;
        margin: 0 0 8px 0;
    }

    /* =============================================
       TABLES - Traditional
       ============================================= */
    table {
        width: 100%;
        border-collapse: collapse;
        font-size: 9.5px;
    }

    .data-table {
        border: 1px solid #ccc;
    }

    .data-table th, .data-table td {
        padding: 8px 10px;
        text-align: left;
        border-bottom: 1px solid #ddd;
    }

    .data-table th {
        background: #f5f5f5;
        font-weight: 600;
        color: #333;
        font-size: 9px;
        font-family: -apple-system, sans-serif;
    }

    .data-table tbody tr:last-child td {
        border-bottom: none;
    }

    .highlight-row {
        background: #f0f0f0 !important;
    }

    .highlight-row td {
        font-weight: 600;
    }

    /* Score Table */
    .score-table {
        border: 1px solid #ccc;
    }

    .score-table th, .score-table td {
        padding: 8px 10px;
        border-bottom: 1px solid #ddd;
    }

    .score-table th {
        background: #1a1a1a;
        color: white;
        font-weight: 600;
        font-size: 8px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-family: -apple-system, sans-serif;
    }

    .score-table td:first-child {
        font-weight: 500;
    }

    .score-table td:nth-child(2),
    .score-table td:nth-child(3),
    .score-table td:nth-child(4) {
        text-align: center;
    }

    .score-table tbody tr:last-child td {
        border-bottom: none;
    }

    .total-row {
        background: #f0f0f0 !important;
    }

    .total-row td {
        font-weight: 700;
        border-top: 2px solid #1a1a1a;
    }

    /* Indicators - Simple */
    .indicator {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 18px;
        height: 18px;
        border-radius: 50%;
        color: white;
        font-size: 9px;
        font-weight: 700;
    }

    .indicator-green { background: #2d7a2d; }
    .indicator-yellow { background: #b8860b; }
    .indicator-red { background: #a02020; }

    /* Badges - Minimal */
    .badge {
        display: inline-block;
        padding: 3px 10px;
        font-size: 8px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        border: 1px solid;
        font-family: -apple-system, sans-serif;
    }

    .badge-low {
        background: white;
        color: #2d7a2d;
        border-color: #2d7a2d;
    }

    .badge-medium {
        background: white;
        color: #b8860b;
        border-color: #b8860b;
    }

    .badge-high {
        background: white;
        color: #a02020;
        border-color: #a02020;
    }

    .badge-elevated {
        background: white;
        color: #b8860b;
        border-color: #b8860b;
    }

    .badge-moderate {
        background: white;
        color: #666;
        border-color: #666;
    }

    /* =============================================
       LAYOUT
       ============================================= */
    .two-col {
        display: flex;
        gap: 20px;
    }

    .two-col > div,
    .two-col > table {
        flex: 1;
    }

    /* =============================================
       CHARTS
       ============================================= */
    .chart-container {
        text-align: center;
        padding: 12px;
        background: #fafafa;
        border: 1px solid #ddd;
    }

    .chart-container img {
        max-width: 100%;
        max-height: 200px;
    }

    .chart-placeholder {
        padding: 40px 20px;
        color: #999;
        font-size: 10px;
    }

    /* =============================================
       FLAGS - Simple lists
       ============================================= */
    .flags-grid {
        display: flex;
        gap: 24px;
    }

    .flags-col {
        flex: 1;
    }

    .flags-header {
        margin-bottom: 8px;
        padding-bottom: 6px;
        font-size: 10px;
        font-weight: 700;
        font-family: -apple-system, sans-serif;
    }

    .flags-header.red {
        border-bottom: 1px solid #a02020;
        color: #a02020;
    }

    .flags-header.green {
        border-bottom: 1px solid #2d7a2d;
        color: #2d7a2d;
    }

    .flag-list {
        list-style: none;
        padding: 0;
        margin: 0;
    }

    .flag-list li {
        padding: 6px 0;
        font-size: 9.5px;
        line-height: 1.4;
        border-bottom: 1px solid #eee;
    }

    .flag-list li:last-child {
        border-bottom: none;
    }

    .flag-list li.red-flag {
        color: #6b1515;
    }

    .flag-list li.green-flag {
        color: #1a5c1a;
    }

    .flag-list li.no-flag {
        color: #999;
        font-style: italic;
    }

    /* =============================================
       RECOMMENDATION - Clean box
       ============================================= */
    .recommendation-box {
        padding: 20px 24px;
        text-align: center;
        border: 2px solid #1a1a1a;
    }

    .recommendation-box.go {
        border-color: #2d7a2d;
    }

    .recommendation-box.conditional {
        border-color: #b8860b;
    }

    .recommendation-box.nogo {
        border-color: #a02020;
    }

    .recommendation-verdict {
        font-size: 14px;
        font-weight: 700;
        margin-bottom: 10px;
        text-transform: uppercase;
        letter-spacing: 2px;
        font-family: -apple-system, sans-serif;
    }

    .recommendation-box.go .recommendation-verdict { color: #2d7a2d; }
    .recommendation-box.conditional .recommendation-verdict { color: #b8860b; }
    .recommendation-box.nogo .recommendation-verdict { color: #a02020; }

    .recommendation-text {
        font-size: 10px;
        color: #333;
        line-height: 1.6;
        max-width: 500px;
        margin: 0 auto;
    }

    /* =============================================
       CHECKLIST - Simple
       ============================================= */
    .checklist {
        list-style: none;
        padding: 0;
        margin: 0;
        columns: 2;
        column-gap: 24px;
    }

    .checklist li {
        padding: 8px 0 8px 20px;
        margin-bottom: 4px;
        font-size: 9.5px;
        position: relative;
        break-inside: avoid;
        border-bottom: 1px solid #eee;
    }

    .checklist li::before {
        content: "\\2610";
        position: absolute;
        left: 0;
        font-size: 12px;
        color: #888;
    }

    .checklist li.priority {
        font-weight: 600;
        color: #6b1515;
    }

    .checklist li.priority::before {
        content: "\\25A0";
        color: #a02020;
    }

    /* =============================================
       NOTES & FOOTER
       ============================================= */
    .note {
        padding: 10px 12px;
        background: #f9f9f9;
        font-size: 9px;
        color: #555;
        margin-top: 10px;
        border: 1px solid #ddd;
    }

    .note strong {
        color: #333;
    }

    .footer {
        margin-top: 30px;
        padding-top: 16px;
        border-top: 1px solid #ccc;
    }

    .footer-disclaimer {
        font-size: 8px;
        color: #888;
        line-height: 1.5;
        margin-bottom: 10px;
    }

    .footer-generated {
        text-align: center;
        font-size: 8px;
        color: #888;
    }

    .page-break {
        page-break-before: always;
    }

    .no-break {
        page-break-inside: avoid;
    }

    /* =============================================
       THESIS BOX - Simple
       ============================================= */
    .thesis-box {
        border: 1px solid #ccc;
        padding: 14px 18px;
        margin-bottom: 18px;
    }

    .thesis-title {
        font-size: 10px;
        font-weight: 700;
        color: #1a1a1a;
        margin-bottom: 6px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-family: -apple-system, sans-serif;
    }

    .thesis-content {
        font-size: 10px;
        color: #333;
        line-height: 1.5;
    }

    .thesis-content strong {
        font-weight: 600;
    }

    /* =============================================
       MARKET CARD
       ============================================= */
    .market-card {
        border: 1px solid #ddd;
        padding: 12px 14px;
    }

    .market-card h4 {
        margin: 0 0 8px 0;
        font-size: 9px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: #666;
        font-family: -apple-system, sans-serif;
    }

    /* =============================================
       SECTION INTRO TEXT
       ============================================= */
    .section-intro {
        font-size: 10px;
        color: #555;
        margin: 0 0 14px 0;
        line-height: 1.5;
    }

    /* =============================================
       HISTOGRAM TABLE
       ============================================= */
    .histogram-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 9px;
    }

    .histogram-table th {
        text-align: left;
        padding: 6px 8px;
        font-weight: 600;
        color: #666;
        font-size: 8px;
        text-transform: uppercase;
        letter-spacing: 0.3px;
        border-bottom: 1px solid #ccc;
        font-family: -apple-system, sans-serif;
    }

    .histogram-table td {
        padding: 5px 8px;
        vertical-align: middle;
        border-bottom: 1px solid #eee;
    }

    .histogram-table tbody tr:last-child td {
        border-bottom: none;
    }

    /* =============================================
       FUNNEL CONTAINER
       ============================================= */
    .funnel-container {
        margin-bottom: 12px;
    }

    .funnel-container .data-table td:nth-child(2),
    .funnel-container .data-table td:nth-child(3),
    .funnel-container .data-table td:nth-child(4) {
        text-align: right;
    }

    .funnel-container .data-table th:nth-child(2),
    .funnel-container .data-table th:nth-child(3),
    .funnel-container .data-table th:nth-child(4) {
        text-align: right;
    }

    /* =============================================
       OUTCOME SUMMARY
       ============================================= */
    .outcome-summary {
        flex: 1;
    }

    .outcome-summary .data-table td:nth-child(2),
    .outcome-summary .data-table td:nth-child(3) {
        text-align: right;
    }

    .outcome-summary .data-table th:nth-child(2),
    .outcome-summary .data-table th:nth-child(3) {
        text-align: right;
    }
    '''


def get_cluster_report_css() -> str:
    """CSS for cluster (portfolio) reports."""
    return get_base_css() + '''
    /* Cluster styles - minimal */
    .cover-page {
        height: 9.5in;
        display: flex;
        align-items: center;
        justify-content: center;
        background: #1a1a1a;
        margin: -0.6in -0.7in;
        padding: 0.7in;
        page-break-after: always;
    }

    .cover-content { text-align: center; color: white; }
    .cover-content h1 { font-size: 30px; font-weight: 400; margin: 0 0 10px 0; font-family: "Times New Roman", Georgia, serif; }
    .cover-subtitle { font-size: 15px; opacity: 0.8; margin-bottom: 40px; }
    .cover-stats { display: flex; justify-content: center; gap: 50px; margin-bottom: 50px; }
    .cover-stat { text-align: center; }
    .cover-stat-value { display: block; font-size: 30px; font-weight: 400; font-family: "Times New Roman", Georgia, serif; }
    .cover-stat-label { display: block; font-size: 11px; opacity: 0.7; margin-top: 5px; }
    .cover-footer { font-size: 12px; opacity: 0.8; }

    .slide { page-break-before: always; min-height: 8.5in; }
    .slide:first-of-type { page-break-before: auto; }
    .slide-title { font-size: 16px; color: #1a1a1a; border-bottom: 1px solid #1a1a1a; padding-bottom: 8px; margin: 0 0 20px 0; }
    '''


COLORS = {
    'primary': '#1a1a1a',
    'secondary': '#333',
    'accent': '#1a1a1a',
    'success': '#2d7a2d',
    'warning': '#b8860b',
    'danger': '#a02020',
}

RECOMMENDATION_COLORS = {
    'GO': '#2d7a2d',
    'CONDITIONAL': '#b8860b',
    'NO-GO': '#a02020',
}
