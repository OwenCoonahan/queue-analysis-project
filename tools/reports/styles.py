"""
Premium CSS styles for PDF reports.

Provides professional, PE-quality styling for deal and cluster reports.
"""


def get_base_css() -> str:
    """Base CSS used by all report types."""
    return '''
    @page {
        size: letter;
        margin: 0.5in 0.6in 0.7in 0.6in;
        @bottom-center {
            content: counter(page);
            font-size: 8px;
            color: #94a3b8;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        }
    }

    @page :first {
        margin-top: 0;
    }

    * {
        box-sizing: border-box;
    }

    body {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        font-size: 9.5px;
        line-height: 1.5;
        color: #1e293b;
        background: white;
    }

    p, li, tr {
        orphans: 3;
        widows: 3;
    }
    '''


def get_deal_report_css() -> str:
    """Premium CSS for deal (single project) reports."""
    return get_base_css() + '''

    /* =============================================
       PREMIUM HEADER
       ============================================= */
    .header {
        background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 60%, #1e40af 100%);
        color: white;
        padding: 32px 40px 28px 40px;
        margin: -0.5in -0.6in 28px -0.6in;
        width: calc(100% + 1.2in);
        position: relative;
    }

    .header::after {
        content: "";
        position: absolute;
        bottom: 0;
        left: 0;
        right: 0;
        height: 4px;
        background: linear-gradient(90deg, #3b82f6, #8b5cf6, #06b6d4);
    }

    .header-badge {
        display: inline-block;
        background: rgba(255,255,255,0.12);
        padding: 5px 14px;
        border-radius: 4px;
        font-size: 9px;
        font-weight: 600;
        letter-spacing: 1.2px;
        text-transform: uppercase;
        margin-bottom: 14px;
        border: 1px solid rgba(255,255,255,0.15);
    }

    .header h1 {
        font-size: 24px;
        margin: 0 0 6px 0;
        font-weight: 700;
        letter-spacing: -0.5px;
    }

    .header-project {
        font-size: 14px;
        opacity: 0.95;
        margin-bottom: 14px;
        font-weight: 500;
    }

    .header-meta {
        display: flex;
        gap: 30px;
        font-size: 10px;
        opacity: 0.85;
    }

    /* =============================================
       EXECUTIVE SUMMARY
       ============================================= */
    .exec-grid {
        display: flex;
        gap: 24px;
        margin-bottom: 20px;
    }

    /* Score Card */
    .score-card {
        min-width: 170px;
        background: linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
        border: 1px solid #e2e8f0;
        border-radius: 14px;
        padding: 22px 28px;
        text-align: center;
        position: relative;
    }

    .score-card::before {
        content: "";
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 4px;
        border-radius: 14px 14px 0 0;
    }

    .score-card.go::before { background: linear-gradient(90deg, #22c55e, #16a34a); }
    .score-card.conditional::before { background: linear-gradient(90deg, #f59e0b, #d97706); }
    .score-card.nogo::before { background: linear-gradient(90deg, #ef4444, #dc2626); }

    .score-gauge {
        width: 100px;
        height: 100px;
        margin: 0 auto 10px;
        position: relative;
    }

    .score-gauge svg {
        transform: rotate(-90deg);
    }

    .gauge-bg {
        fill: none;
        stroke: #e2e8f0;
        stroke-width: 8;
    }

    .gauge-fill {
        fill: none;
        stroke-width: 8;
        stroke-linecap: round;
    }

    .gauge-fill.go { stroke: #22c55e; }
    .gauge-fill.conditional { stroke: #f59e0b; }
    .gauge-fill.nogo { stroke: #ef4444; }

    .score-center {
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        text-align: center;
    }

    .score-number {
        font-size: 30px;
        font-weight: 800;
        color: #0f172a;
        line-height: 1;
    }

    .score-max {
        font-size: 10px;
        color: #64748b;
    }

    .verdict-pill {
        display: inline-block;
        padding: 7px 22px;
        border-radius: 20px;
        font-weight: 700;
        font-size: 11px;
        color: white;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        margin: 10px 0 8px 0;
    }

    .verdict-pill.go { background: linear-gradient(135deg, #22c55e, #16a34a); }
    .verdict-pill.conditional { background: linear-gradient(135deg, #f59e0b, #d97706); }
    .verdict-pill.nogo { background: linear-gradient(135deg, #ef4444, #dc2626); }

    .score-meta {
        font-size: 9px;
        color: #64748b;
    }

    /* KPI Grid */
    .kpi-grid {
        flex: 1;
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 12px;
    }

    .kpi-card {
        padding: 14px 16px;
        background: white;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        border-left: 4px solid;
    }

    .kpi-card.cost { border-left-color: #3b82f6; }
    .kpi-card.prob { border-left-color: #8b5cf6; }
    .kpi-card.cod { border-left-color: #06b6d4; }
    .kpi-card.comp { border-left-color: #10b981; }

    .kpi-label {
        font-size: 8px;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 4px;
    }

    .kpi-value {
        font-size: 18px;
        font-weight: 700;
        color: #0f172a;
        line-height: 1.1;
    }

    .kpi-detail {
        font-size: 8px;
        color: #94a3b8;
        margin-top: 3px;
    }

    /* Risk Alert */
    .risk-alert {
        display: flex;
        gap: 24px;
        padding: 14px 18px;
        background: linear-gradient(135deg, #fffbeb, #fef3c7);
        border-left: 4px solid #f59e0b;
        border-radius: 0 10px 10px 0;
        margin-top: 16px;
    }

    .risk-alert-item {
        flex: 1;
    }

    .risk-alert-label {
        font-size: 8px;
        font-weight: 700;
        color: #92400e;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    .risk-alert-value {
        font-size: 10px;
        color: #78350f;
        margin-top: 2px;
        line-height: 1.4;
    }

    /* =============================================
       SECTIONS
       ============================================= */
    .section {
        margin-bottom: 26px;
        page-break-inside: avoid;
    }

    h2 {
        font-size: 13px;
        font-weight: 700;
        color: #0f172a;
        border-bottom: 2px solid #0f172a;
        padding-bottom: 7px;
        margin: 0 0 16px 0;
        letter-spacing: -0.2px;
    }

    h3 {
        font-size: 11px;
        font-weight: 600;
        color: #334155;
        margin: 0 0 10px 0;
    }

    /* =============================================
       TABLES
       ============================================= */
    table {
        width: 100%;
        border-collapse: collapse;
        font-size: 9.5px;
    }

    .data-table {
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        overflow: hidden;
    }

    .data-table th, .data-table td {
        padding: 9px 12px;
        text-align: left;
        border-bottom: 1px solid #e2e8f0;
    }

    .data-table th {
        background: #f8fafc;
        font-weight: 600;
        color: #475569;
        font-size: 9px;
    }

    .data-table tbody tr:last-child td {
        border-bottom: none;
    }

    .data-table tbody tr:nth-child(even) {
        background: #fafbfc;
    }

    .highlight-row {
        background: #f0fdf4 !important;
    }

    .highlight-row td {
        font-weight: 600;
        color: #166534;
    }

    /* Score Table */
    .score-table {
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        overflow: hidden;
    }

    .score-table th, .score-table td {
        padding: 9px 12px;
        border-bottom: 1px solid #e2e8f0;
    }

    .score-table th {
        background: #0f172a;
        color: white;
        font-weight: 600;
        font-size: 9px;
        text-transform: uppercase;
        letter-spacing: 0.3px;
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
        background: #f0f9ff !important;
    }

    .total-row td {
        font-weight: 700;
        color: #0f172a;
        border-top: 2px solid #0f172a;
    }

    /* Indicators */
    .indicator {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 20px;
        height: 20px;
        border-radius: 50%;
        color: white;
        font-size: 9px;
        font-weight: 700;
    }

    .indicator-green { background: linear-gradient(135deg, #22c55e, #16a34a); }
    .indicator-yellow { background: linear-gradient(135deg, #f59e0b, #d97706); }
    .indicator-red { background: linear-gradient(135deg, #ef4444, #dc2626); }

    /* Badges */
    .badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 8px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    .badge-low {
        background: linear-gradient(135deg, #dcfce7, #bbf7d0);
        color: #166534;
    }

    .badge-medium {
        background: linear-gradient(135deg, #fef3c7, #fde68a);
        color: #92400e;
    }

    .badge-high {
        background: linear-gradient(135deg, #fee2e2, #fecaca);
        color: #991b1b;
    }

    .badge-elevated {
        background: linear-gradient(135deg, #fef3c7, #fde68a);
        color: #92400e;
    }

    .badge-moderate {
        background: linear-gradient(135deg, #dbeafe, #bfdbfe);
        color: #1e40af;
    }

    /* Score percentile text */
    .score-percentile {
        font-size: 9px;
        color: #64748b;
        margin-top: 6px;
        font-style: italic;
    }

    /* =============================================
       LAYOUT
       ============================================= */
    .two-col {
        display: flex;
        gap: 24px;
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
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
    }

    .chart-container img {
        max-width: 100%;
        max-height: 200px;
    }

    .chart-placeholder {
        padding: 40px 20px;
        color: #94a3b8;
        font-size: 10px;
    }

    /* =============================================
       FLAGS
       ============================================= */
    .flags-grid {
        display: flex;
        gap: 24px;
    }

    .flags-col {
        flex: 1;
    }

    .flags-header {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 10px;
        padding-bottom: 8px;
        font-size: 11px;
        font-weight: 700;
    }

    .flags-header.red {
        border-bottom: 2px solid #ef4444;
        color: #dc2626;
    }

    .flags-header.green {
        border-bottom: 2px solid #22c55e;
        color: #16a34a;
    }

    .flag-list {
        list-style: none;
        padding: 0;
        margin: 0;
    }

    .flag-list li {
        padding: 8px 12px;
        margin-bottom: 6px;
        border-radius: 6px;
        font-size: 9.5px;
        line-height: 1.4;
    }

    .flag-list li.red-flag {
        background: #fef2f2;
        border-left: 3px solid #ef4444;
        color: #991b1b;
    }

    .flag-list li.green-flag {
        background: #f0fdf4;
        border-left: 3px solid #22c55e;
        color: #166534;
    }

    .flag-list li.no-flag {
        background: #f8fafc;
        color: #94a3b8;
        font-style: italic;
        border-left: 3px solid #e2e8f0;
    }

    /* =============================================
       RECOMMENDATION
       ============================================= */
    .recommendation-box {
        padding: 24px 28px;
        border-radius: 12px;
        text-align: center;
    }

    .recommendation-box.go {
        background: linear-gradient(135deg, #f0fdf4, #dcfce7);
        border: 2px solid #22c55e;
    }

    .recommendation-box.conditional {
        background: linear-gradient(135deg, #fffbeb, #fef3c7);
        border: 2px solid #f59e0b;
    }

    .recommendation-box.nogo {
        background: linear-gradient(135deg, #fef2f2, #fee2e2);
        border: 2px solid #ef4444;
    }

    .recommendation-verdict {
        font-size: 15px;
        font-weight: 800;
        margin-bottom: 10px;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    .recommendation-box.go .recommendation-verdict { color: #166534; }
    .recommendation-box.conditional .recommendation-verdict { color: #92400e; }
    .recommendation-box.nogo .recommendation-verdict { color: #991b1b; }

    .recommendation-text {
        font-size: 10px;
        color: #475569;
        line-height: 1.6;
        max-width: 480px;
        margin: 0 auto;
    }

    /* =============================================
       CHECKLIST
       ============================================= */
    .checklist {
        list-style: none;
        padding: 0;
        margin: 0;
        columns: 2;
        column-gap: 24px;
    }

    .checklist li {
        padding: 9px 12px 9px 30px;
        margin-bottom: 6px;
        background: #f8fafc;
        border-radius: 6px;
        font-size: 9.5px;
        position: relative;
        break-inside: avoid;
    }

    .checklist li::before {
        content: "";
        position: absolute;
        left: 10px;
        top: 50%;
        transform: translateY(-50%);
        width: 12px;
        height: 12px;
        border: 2px solid #cbd5e1;
        border-radius: 3px;
    }

    .checklist li.priority {
        background: #fef2f2;
        border-left: 3px solid #ef4444;
    }

    .checklist li.priority::before {
        border-color: #ef4444;
    }

    /* =============================================
       NOTES & FOOTER
       ============================================= */
    .note {
        padding: 10px 14px;
        background: #f8fafc;
        border-radius: 6px;
        font-size: 9px;
        color: #64748b;
        margin-top: 12px;
        border-left: 3px solid #3b82f6;
    }

    .footer {
        margin-top: 35px;
        padding-top: 18px;
        border-top: 1px solid #e2e8f0;
    }

    .footer-disclaimer {
        font-size: 8px;
        color: #94a3b8;
        line-height: 1.6;
        margin-bottom: 12px;
        padding: 10px 14px;
        background: #f8fafc;
        border-radius: 6px;
    }

    .footer-generated {
        text-align: center;
        font-size: 8px;
        color: #64748b;
    }

    .page-break {
        page-break-before: always;
    }

    .no-break {
        page-break-inside: avoid;
    }

    /* =============================================
       THESIS BOX
       ============================================= */
    .thesis-box {
        background: linear-gradient(135deg, #f0f9ff, #e0f2fe);
        border: 1px solid #0ea5e9;
        border-radius: 10px;
        padding: 16px 20px;
        margin-bottom: 20px;
    }

    .thesis-title {
        font-size: 11px;
        font-weight: 700;
        color: #0369a1;
        margin-bottom: 8px;
    }

    .thesis-content {
        font-size: 10px;
        color: #0c4a6e;
        line-height: 1.5;
    }
    '''


def get_cluster_report_css() -> str:
    """CSS for cluster (portfolio) reports."""
    return get_base_css() + '''
    /* Cluster styles - keeping existing implementation */
    .cover-page {
        height: 9.5in;
        display: flex;
        align-items: center;
        justify-content: center;
        background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 50%, #1e40af 100%);
        margin: -0.5in -0.6in;
        padding: 0.6in;
        page-break-after: always;
    }

    .cover-content { text-align: center; color: white; }
    .cover-content h1 { font-size: 34px; font-weight: 700; margin: 0 0 12px 0; }
    .cover-subtitle { font-size: 17px; opacity: 0.9; margin-bottom: 45px; }
    .cover-stats { display: flex; justify-content: center; gap: 50px; margin-bottom: 55px; }
    .cover-stat { text-align: center; }
    .cover-stat-value { display: block; font-size: 34px; font-weight: 700; }
    .cover-stat-label { display: block; font-size: 12px; opacity: 0.8; margin-top: 5px; }
    .cover-footer { font-size: 13px; opacity: 0.9; }

    .slide { page-break-before: always; min-height: 8.5in; }
    .slide:first-of-type { page-break-before: auto; }
    .slide-title { font-size: 19px; color: #0f172a; border-bottom: 3px solid #1e3a5f; padding-bottom: 10px; margin: 0 0 22px 0; }
    '''


COLORS = {
    'primary': '#0f172a',
    'secondary': '#1e3a5f',
    'accent': '#3b82f6',
    'success': '#22c55e',
    'warning': '#f59e0b',
    'danger': '#ef4444',
}

RECOMMENDATION_COLORS = {
    'GO': '#22c55e',
    'CONDITIONAL': '#f59e0b',
    'NO-GO': '#ef4444',
}
