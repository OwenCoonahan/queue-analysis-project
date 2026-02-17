"""
Shared CSS styles for PDF reports.

Provides consistent styling across deal and cluster reports.
"""


def get_base_css() -> str:
    """Base CSS used by all report types."""
    return '''
    @page {
        size: letter;
        margin: 0.6in 0.65in;
        @bottom-center {
            content: counter(page);
            font-size: 9px;
            color: #666;
        }
    }

    body {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        font-size: 10px;
        line-height: 1.5;
        color: #1f2937;
    }

    /* Prevent orphans/widows */
    p, li {
        orphans: 3;
        widows: 3;
    }
    '''


def get_deal_report_css() -> str:
    """CSS specific to deal (single project) reports."""
    return get_base_css() + '''

    /* Header */
    .header {
        background: linear-gradient(135deg, #1e3a5f 0%, #2c5282 100%);
        color: white;
        padding: 25px 30px;
        margin: -0.6in -0.65in 25px -0.65in;
        width: calc(100% + 1.3in);
    }

    .header h1 {
        font-size: 22px;
        margin: 0 0 8px 0;
        font-weight: 600;
    }

    .header-meta {
        font-size: 11px;
        opacity: 0.9;
    }

    /* Sections */
    .section {
        margin-bottom: 25px;
        page-break-inside: avoid;
    }

    h2 {
        font-size: 14px;
        color: #1e3a5f;
        border-bottom: 2px solid #3182ce;
        padding-bottom: 6px;
        margin: 0 0 15px 0;
    }

    h3 {
        font-size: 12px;
        color: #374151;
        margin: 0 0 10px 0;
    }

    /* Executive Summary */
    .exec-summary {
        display: flex;
        gap: 25px;
        margin-bottom: 20px;
    }

    .score-card {
        text-align: center;
        padding: 20px;
        background: #f8fafc;
        border-radius: 10px;
        min-width: 150px;
    }

    .score-value {
        font-size: 42px;
        font-weight: 700;
        line-height: 1;
    }

    .score-label {
        font-size: 11px;
        color: #6b7280;
        margin-top: 5px;
    }

    .recommendation-badge {
        display: inline-block;
        padding: 8px 20px;
        border-radius: 20px;
        font-weight: bold;
        font-size: 13px;
        color: white;
        margin-top: 12px;
    }

    .rec-go { background: #22c55e; }
    .rec-conditional { background: #f59e0b; }
    .rec-nogo { background: #ef4444; }

    .grade-badge {
        font-size: 10px;
        color: #6b7280;
        margin-top: 8px;
    }

    /* KPI Grid */
    .kpi-grid {
        display: flex;
        gap: 15px;
        flex: 1;
    }

    .kpi-card {
        flex: 1;
        padding: 15px;
        background: #f8fafc;
        border-radius: 8px;
        text-align: center;
    }

    .kpi-value {
        font-size: 18px;
        font-weight: 700;
        color: #1e3a5f;
    }

    .kpi-label {
        font-size: 9px;
        color: #6b7280;
        margin-top: 4px;
    }

    .kpi-detail {
        font-size: 8px;
        color: #9ca3af;
        margin-top: 2px;
    }

    /* Key Risk Callout */
    .key-risk {
        padding: 12px 18px;
        background: #fef3c7;
        border-left: 4px solid #f59e0b;
        border-radius: 0 8px 8px 0;
        font-size: 11px;
        margin-top: 15px;
    }

    /* Tables */
    table {
        width: 100%;
        border-collapse: collapse;
        font-size: 10px;
    }

    .data-table th, .data-table td {
        padding: 8px 10px;
        text-align: left;
        border-bottom: 1px solid #e5e7eb;
    }

    .data-table th {
        background: #f1f5f9;
        font-weight: 600;
        color: #475569;
    }

    .data-table tbody tr:nth-child(even) {
        background: #f8fafc;
    }

    .highlight-row {
        background: #f0fdf4 !important;
        font-weight: 600;
    }

    /* Score Table */
    .score-table th, .score-table td {
        padding: 8px 12px;
        text-align: center;
        border-bottom: 1px solid #e5e7eb;
    }

    .score-table th {
        background: #f1f5f9;
        font-weight: 600;
    }

    .score-table td:first-child {
        text-align: left;
    }

    .total-row {
        background: #f0f9ff !important;
        font-weight: 600;
    }

    /* Traffic Lights */
    .traffic {
        display: inline-block;
        width: 14px;
        height: 14px;
        border-radius: 50%;
    }

    .traffic-green { background: #22c55e; }
    .traffic-yellow { background: #f59e0b; }
    .traffic-red { background: #ef4444; }

    /* Risk Badges */
    .badge {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 10px;
        font-size: 9px;
        font-weight: 600;
        color: white;
    }

    .badge-low { background: #22c55e; }
    .badge-medium { background: #f59e0b; }
    .badge-high { background: #ef4444; }

    /* Chart Container */
    .chart-container {
        text-align: center;
        margin: 15px 0;
    }

    .chart-container img {
        max-width: 100%;
        max-height: 280px;
        border-radius: 8px;
    }

    .chart-placeholder {
        background: #f1f5f9;
        border: 2px dashed #cbd5e1;
        border-radius: 8px;
        padding: 40px;
        text-align: center;
        color: #94a3b8;
    }

    /* Two Column Layout */
    .two-col {
        display: flex;
        gap: 25px;
    }

    .two-col > div {
        flex: 1;
    }

    /* Risk Flags */
    .flag-list {
        list-style: none;
        padding: 0;
        margin: 0;
    }

    .flag-list li {
        padding: 6px 0;
        border-bottom: 1px solid #e5e7eb;
        font-size: 10px;
    }

    .flag-list li:last-child {
        border-bottom: none;
    }

    .red-flag::before { content: "! "; color: #dc2626; font-weight: bold; }
    .green-flag::before { content: "* "; color: #16a34a; font-weight: bold; }
    .no-flag { color: #9ca3af; font-style: italic; }

    /* Recommendation Box */
    .recommendation-box {
        padding: 20px;
        border: 2px solid;
        border-radius: 10px;
        text-align: center;
    }

    .rec-text {
        margin: 15px 0 0 0;
        color: #4b5563;
        font-size: 11px;
    }

    /* Checklist */
    .checklist {
        list-style: none;
        padding: 0;
    }

    .checklist li {
        padding: 8px 0;
        border-bottom: 1px solid #e5e7eb;
    }

    .checklist li::before {
        content: "[ ] ";
        color: #3182ce;
        font-family: monospace;
    }

    .checklist .investigate {
        color: #dc2626;
    }

    /* Market Data Cards */
    .market-card {
        background: #f8fafc;
        border-radius: 8px;
        padding: 15px;
        margin-bottom: 15px;
    }

    .market-card h4 {
        margin: 0 0 10px 0;
        font-size: 11px;
        color: #1e3a5f;
    }

    /* Notes/Footer */
    .note {
        padding: 10px 12px;
        background: #f8fafc;
        border-radius: 6px;
        font-size: 9px;
        color: #64748b;
        margin-top: 10px;
    }

    .footer {
        margin-top: 30px;
        padding-top: 15px;
        border-top: 1px solid #e5e7eb;
        font-size: 9px;
        color: #6b7280;
    }

    .footer .disclaimer {
        margin-bottom: 10px;
    }

    .footer .generated {
        text-align: center;
    }

    /* Page Breaks */
    .page-break {
        page-break-before: always;
    }

    .no-break {
        page-break-inside: avoid;
    }
    '''


def get_cluster_report_css() -> str:
    """CSS specific to cluster (portfolio) reports."""
    return get_base_css() + '''

    /* Cover Page */
    .cover-page {
        height: 9.5in;
        display: flex;
        align-items: center;
        justify-content: center;
        background: linear-gradient(135deg, #1e3a5f 0%, #2c5282 50%, #1e3a5f 100%);
        margin: -0.6in -0.65in;
        padding: 0.65in;
        page-break-after: always;
    }

    .cover-content {
        text-align: center;
        color: white;
    }

    .cover-content h1 {
        font-size: 32px;
        font-weight: 700;
        margin: 0 0 12px 0;
        line-height: 1.2;
    }

    .cover-subtitle {
        font-size: 16px;
        opacity: 0.9;
        margin-bottom: 40px;
    }

    .cover-stats {
        display: flex;
        justify-content: center;
        gap: 40px;
        margin-bottom: 50px;
    }

    .cover-stat {
        text-align: center;
    }

    .cover-stat-value {
        display: block;
        font-size: 28px;
        font-weight: 700;
    }

    .cover-stat-label {
        display: block;
        font-size: 11px;
        opacity: 0.8;
        margin-top: 4px;
    }

    .cover-footer {
        font-size: 12px;
        opacity: 0.9;
    }

    /* Slide Style Pages */
    .slide {
        page-break-before: always;
        page-break-after: always;
        page-break-inside: avoid;
        min-height: 8.5in;
        position: relative;
    }

    .slide:first-of-type {
        page-break-before: auto;
    }

    .slide-title {
        font-size: 18px;
        color: #1e3a5f;
        border-bottom: 3px solid #3182ce;
        padding-bottom: 10px;
        margin: 0 0 20px 0;
    }

    .slide-subtitle {
        font-size: 12px;
        color: #64748b;
        margin: -15px 0 20px 0;
    }

    /* Project Table */
    .project-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 9px;
    }

    .project-table th {
        background: #1e3a5f;
        color: white;
        padding: 8px 6px;
        text-align: left;
        font-weight: 600;
    }

    .project-table td {
        padding: 6px;
        border-bottom: 1px solid #e5e7eb;
    }

    .project-table tbody tr:nth-child(even) {
        background: #f8fafc;
    }

    .project-table .score-cell {
        text-align: center;
        font-weight: 600;
    }

    .score-go { color: #22c55e; }
    .score-conditional { color: #f59e0b; }
    .score-nogo { color: #ef4444; }

    /* Summary KPIs */
    .summary-grid {
        display: flex;
        gap: 15px;
        margin-bottom: 25px;
    }

    .summary-card {
        flex: 1;
        background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        border: 1px solid #e2e8f0;
    }

    .summary-value {
        font-size: 28px;
        font-weight: 700;
        color: #1e3a5f;
    }

    .summary-label {
        font-size: 10px;
        color: #64748b;
        margin-top: 5px;
    }

    .summary-detail {
        font-size: 9px;
        color: #9ca3af;
        margin-top: 3px;
    }

    /* Risk Heatmap */
    .risk-heatmap {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(80px, 1fr));
        gap: 8px;
    }

    .risk-cell {
        padding: 10px 8px;
        border-radius: 6px;
        text-align: center;
        font-size: 9px;
    }

    .risk-cell-id {
        font-weight: 600;
        margin-bottom: 3px;
    }

    .risk-cell-score {
        font-size: 14px;
        font-weight: 700;
    }

    .risk-low { background: #dcfce7; color: #166534; }
    .risk-medium { background: #fef3c7; color: #92400e; }
    .risk-high { background: #fee2e2; color: #991b1b; }

    /* Chart containers */
    .chart-container {
        text-align: center;
        margin: 15px 0;
    }

    .chart-container img {
        max-width: 100%;
        max-height: 350px;
    }

    .chart-row {
        display: flex;
        gap: 20px;
    }

    .chart-half {
        flex: 1;
        text-align: center;
    }

    .chart-half img {
        max-width: 100%;
        max-height: 280px;
    }

    /* Notes */
    .cluster-note {
        background: #f0f9ff;
        border-left: 4px solid #3182ce;
        padding: 12px 15px;
        border-radius: 0 8px 8px 0;
        font-size: 10px;
        margin-top: 15px;
    }

    /* Tables */
    .data-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 9px;
    }

    .data-table th, .data-table td {
        padding: 6px 8px;
        text-align: left;
        border-bottom: 1px solid #e5e7eb;
    }

    .data-table th {
        background: #f1f5f9;
        font-weight: 600;
        color: #475569;
    }

    .data-table tbody tr:nth-child(even) {
        background: #f8fafc;
    }

    /* Footer */
    .slide-footer {
        position: absolute;
        bottom: 0;
        left: 0;
        right: 0;
        padding-top: 10px;
        border-top: 1px solid #e5e7eb;
        font-size: 8px;
        color: #9ca3af;
        text-align: center;
    }
    '''


# Color constants
COLORS = {
    'primary': '#1e3a5f',
    'secondary': '#3182ce',
    'success': '#22c55e',
    'warning': '#f59e0b',
    'danger': '#ef4444',
    'light': '#f8fafc',
    'muted': '#6b7280',
}

RECOMMENDATION_COLORS = {
    'GO': '#22c55e',
    'CONDITIONAL': '#f59e0b',
    'NO-GO': '#ef4444',
}
