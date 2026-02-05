#!/usr/bin/env python3
"""
Interconnection Queue Market Intelligence Dashboard

A clean, modern dashboard for tracking macro trends, changes, and insights
across US interconnection queues. Personal HQ for staying informed on market dynamics.

Run with: streamlit run intel_dashboard.py
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json

# Import our data layer
from market_intel import MarketData, MacroAnalytics, NewsAggregator, ensure_dirs

# Import centralized report manager
try:
    from report_manager import ReportManager, create_portfolio_report, create_valuation_report
    REPORT_MANAGER_AVAILABLE = True
except ImportError:
    REPORT_MANAGER_AVAILABLE = False

# Import PE analytics and report generation
PE_ANALYTICS_AVAILABLE = False
PDF_AVAILABLE = False
analyze_portfolio = None
generate_portfolio_report = None

try:
    from pe_analytics import PEAnalytics, analyze_portfolio
    PE_ANALYTICS_AVAILABLE = True
except ImportError:
    pass

try:
    from portfolio_report import generate_portfolio_report
    PDF_AVAILABLE = True
except (ImportError, OSError):
    # WeasyPrint may fail if system deps not installed
    PDF_AVAILABLE = False

# Page configuration
st.set_page_config(
    page_title="Queue Intel | Market Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for clean, modern look (Electricity Maps inspired)
st.markdown("""
<style>
    /* Main theme - clean white background */
    .stApp {
        background-color: #fafafa;
    }

    /* Remove default padding */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    /* Header styling */
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1a1a1a;
        margin-bottom: 0.25rem;
        letter-spacing: -0.03em;
    }

    .sub-header {
        font-size: 1rem;
        color: #666;
        margin-bottom: 1.5rem;
        font-weight: 400;
    }

    /* KPI Cards */
    .kpi-container {
        background: white;
        border-radius: 16px;
        padding: 1.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
        border: 1px solid #f0f0f0;
        height: 100%;
    }

    .kpi-value {
        font-size: 2.75rem;
        font-weight: 700;
        color: #1a1a1a;
        line-height: 1.1;
        letter-spacing: -0.02em;
    }

    .kpi-label {
        font-size: 0.8rem;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-top: 0.5rem;
        font-weight: 500;
    }

    .kpi-sublabel {
        font-size: 0.85rem;
        color: #666;
        margin-top: 0.25rem;
    }

    /* Section headers */
    .section-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: #1a1a1a;
        margin: 2rem 0 1rem 0;
        letter-spacing: -0.01em;
    }

    /* Cards */
    .card {
        background: white;
        border-radius: 16px;
        padding: 1.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
        border: 1px solid #f0f0f0;
        margin-bottom: 1rem;
    }

    .card-header {
        font-size: 0.8rem;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-weight: 500;
        margin-bottom: 1rem;
    }

    /* Badges */
    .badge {
        display: inline-block;
        padding: 0.35rem 0.85rem;
        border-radius: 20px;
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    .badge-new { background: #dcfce7; color: #166534; }
    .badge-removed { background: #fee2e2; color: #991b1b; }
    .badge-changed { background: #fef3c7; color: #92400e; }
    .badge-info { background: #e0f2fe; color: #0369a1; }

    /* Stat cards row */
    .stat-row {
        display: flex;
        gap: 0.5rem;
        margin-top: 1rem;
    }

    .mini-stat {
        flex: 1;
        text-align: center;
        padding: 0.75rem;
        background: #f8f9fa;
        border-radius: 8px;
    }

    .mini-stat-value {
        font-size: 1.25rem;
        font-weight: 600;
        color: #1a1a1a;
    }

    .mini-stat-label {
        font-size: 0.7rem;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    /* Insight items */
    .insight-item {
        display: flex;
        align-items: flex-start;
        gap: 1rem;
        padding: 1rem 0;
        border-bottom: 1px solid #f5f5f5;
    }

    .insight-item:last-child {
        border-bottom: none;
        padding-bottom: 0;
    }

    .insight-number {
        background: #2563eb;
        color: white;
        border-radius: 50%;
        width: 28px;
        height: 28px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 0.8rem;
        font-weight: 600;
        flex-shrink: 0;
    }

    .insight-text {
        font-size: 0.95rem;
        color: #374151;
        line-height: 1.5;
    }

    /* News items */
    .news-item {
        padding: 1rem 0;
        border-bottom: 1px solid #f5f5f5;
    }

    .news-title {
        font-weight: 600;
        color: #1a1a1a;
        margin-bottom: 0.25rem;
    }

    .news-meta {
        font-size: 0.8rem;
        color: #888;
    }

    /* Refresh button */
    .refresh-btn {
        background: #2563eb;
        color: white;
        border: none;
        padding: 0.5rem 1rem;
        border-radius: 8px;
        font-weight: 500;
        cursor: pointer;
    }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Table styling */
    .dataframe {
        border: none !important;
        font-size: 0.9rem;
    }

    /* Plotly chart containers */
    .js-plotly-plot {
        border-radius: 12px;
    }

    /* Status indicator */
    .status-dot {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-right: 6px;
    }
    .status-live { background: #22c55e; }
    .status-stale { background: #f59e0b; }
    .status-offline { background: #ef4444; }
</style>
""", unsafe_allow_html=True)


# Color palette
COLORS = {
    'primary': '#2563eb',
    'success': '#22c55e',
    'warning': '#f59e0b',
    'danger': '#ef4444',
    'neutral': '#6b7280',
}

TECH_COLORS = {
    'Solar': '#fbbf24',
    'Wind': '#60a5fa',
    'Storage': '#a78bfa',
    'Gas': '#f87171',
    'Hydro': '#34d399',
    'Nuclear': '#f472b6',
    'Solar + Storage': '#f59e0b',
    'Coal': '#374151',
    'Other': '#9ca3af',
    'Unknown': '#6b7280'
}


@st.cache_resource(ttl=300)  # 5 min cache
def get_market_data():
    """Initialize market data."""
    ensure_dirs()
    return MarketData()


@st.cache_resource(ttl=300)
def get_analytics(_market_data):
    """Initialize analytics."""
    return MacroAnalytics(_market_data)


def render_header():
    """Render the main header."""
    col1, col2 = st.columns([4, 1])

    with col1:
        st.markdown('<h1 class="main-header">Queue Intelligence</h1>', unsafe_allow_html=True)

        # Status indicator
        market = get_market_data()
        last_refresh = market.metadata.get('last_refresh')
        if last_refresh:
            st.markdown(f'<p class="sub-header"><span class="status-dot status-live"></span>Last updated: {last_refresh}</p>', unsafe_allow_html=True)
        else:
            st.markdown('<p class="sub-header"><span class="status-dot status-offline"></span>No data loaded - click Refresh</p>', unsafe_allow_html=True)

    with col2:
        if st.button("🔄 Refresh Data", use_container_width=True, type="primary"):
            with st.spinner("Fetching from GridStatus..."):
                market = get_market_data()
                result = market.refresh_data()
                if result['success']:
                    st.success(f"✓ Refreshed: {', '.join(result['refreshed'])}")
                    st.cache_resource.clear()
                    st.rerun()
                else:
                    st.error(f"Errors: {result.get('errors', [])}")


def render_kpis(summary):
    """Render the main KPI cards."""
    cols = st.columns(4)

    total_projects = summary.get('total_projects', 0)
    total_capacity = summary.get('total_capacity_gw', 0)
    num_isos = len(summary.get('by_iso', {}))
    avg_size = (total_capacity * 1000 / total_projects) if total_projects > 0 else 0

    metrics = [
        (f"{total_projects:,}", "Active Projects", "Across all tracked ISOs"),
        (f"{total_capacity:,.0f}", "Total GW", "Queued capacity"),
        (f"{avg_size:,.0f}", "Avg MW", "Per project"),
        (f"{num_isos}", "ISOs", "Being tracked"),
    ]

    for col, (value, label, sublabel) in zip(cols, metrics):
        with col:
            st.markdown(f"""
            <div class="kpi-container">
                <div class="kpi-value">{value}</div>
                <div class="kpi-label">{label}</div>
                <div class="kpi-sublabel">{sublabel}</div>
            </div>
            """, unsafe_allow_html=True)


def render_regional_breakdown(regional_df):
    """Render regional comparison."""
    st.markdown('<div class="section-header">Regional Distribution</div>', unsafe_allow_html=True)

    if regional_df.empty:
        st.info("No regional data. Click Refresh to fetch from GridStatus.")
        return

    col1, col2 = st.columns(2)

    with col1:
        # Projects by ISO
        fig = go.Figure(data=[go.Bar(
            x=regional_df['ISO'],
            y=regional_df['Total Projects'],
            marker_color=COLORS['primary'],
            text=regional_df['Total Projects'],
            textposition='outside',
            textfont=dict(size=11)
        )])

        fig.update_layout(
            title=dict(text='Projects by ISO', font=dict(size=14, color='#1a1a1a')),
            height=320,
            margin=dict(l=40, r=20, t=50, b=40),
            paper_bgcolor='white',
            plot_bgcolor='white',
            font=dict(family='Inter, -apple-system, sans-serif', size=12),
            showlegend=False,
            yaxis=dict(showgrid=True, gridcolor='#f5f5f5', zeroline=False),
            xaxis=dict(showgrid=False)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Capacity by ISO
        fig = go.Figure(data=[go.Bar(
            x=regional_df['ISO'],
            y=regional_df['Capacity (GW)'],
            marker_color=COLORS['success'],
            text=[f"{v:.0f}" for v in regional_df['Capacity (GW)']],
            textposition='outside',
            textfont=dict(size=11)
        )])

        fig.update_layout(
            title=dict(text='Capacity by ISO (GW)', font=dict(size=14, color='#1a1a1a')),
            height=320,
            margin=dict(l=40, r=20, t=50, b=40),
            paper_bgcolor='white',
            plot_bgcolor='white',
            font=dict(family='Inter, -apple-system, sans-serif', size=12),
            showlegend=False,
            yaxis=dict(showgrid=True, gridcolor='#f5f5f5', zeroline=False),
            xaxis=dict(showgrid=False)
        )
        st.plotly_chart(fig, use_container_width=True)


def render_technology_mix(tech_df):
    """Render technology breakdown."""
    st.markdown('<div class="section-header">Technology Mix</div>', unsafe_allow_html=True)

    if tech_df.empty:
        st.info("No technology data available.")
        return

    col1, col2 = st.columns(2)

    with col1:
        # Donut chart
        colors = [TECH_COLORS.get(t, COLORS['neutral']) for t in tech_df['Technology']]

        fig = go.Figure(data=[go.Pie(
            labels=tech_df['Technology'],
            values=tech_df['Capacity (GW)'],
            hole=0.65,
            marker_colors=colors,
            textinfo='percent',
            textposition='outside',
            textfont=dict(size=11),
            pull=[0.02] * len(tech_df)
        )])

        total_gw = tech_df['Capacity (GW)'].sum()
        fig.update_layout(
            title=dict(text='Capacity Share', font=dict(size=14, color='#1a1a1a')),
            height=350,
            margin=dict(l=20, r=20, t=50, b=20),
            paper_bgcolor='white',
            showlegend=True,
            legend=dict(orientation='h', yanchor='bottom', y=-0.15, xanchor='center', x=0.5, font=dict(size=10)),
            font=dict(family='Inter, -apple-system, sans-serif'),
            annotations=[dict(
                text=f"<b>{total_gw:.0f}</b><br>GW",
                x=0.5, y=0.5,
                font=dict(size=20, color='#1a1a1a'),
                showarrow=False
            )]
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Horizontal bars
        tech_sorted = tech_df.sort_values('Projects', ascending=True)
        colors = [TECH_COLORS.get(t, COLORS['neutral']) for t in tech_sorted['Technology']]

        fig = go.Figure(data=[go.Bar(
            y=tech_sorted['Technology'],
            x=tech_sorted['Projects'],
            orientation='h',
            marker_color=colors,
            text=tech_sorted['Projects'],
            textposition='outside',
            textfont=dict(size=11)
        )])

        fig.update_layout(
            title=dict(text='Projects by Technology', font=dict(size=14, color='#1a1a1a')),
            height=350,
            margin=dict(l=20, r=40, t=50, b=20),
            paper_bgcolor='white',
            plot_bgcolor='white',
            font=dict(family='Inter, -apple-system, sans-serif', size=12),
            xaxis=dict(showgrid=True, gridcolor='#f5f5f5', zeroline=False),
            yaxis=dict(showgrid=False)
        )
        st.plotly_chart(fig, use_container_width=True)


def render_benchmarks():
    """Render LBL benchmark context."""
    st.markdown('<div class="section-header">LBL 2024 Benchmarks</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""
        <div class="card">
            <div class="card-header">Completion Rates</div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.75rem;">
                <div><span style="color: #f87171; font-weight: 600;">Gas</span><br><span style="font-size: 1.5rem; font-weight: 700;">32%</span></div>
                <div><span style="color: #a78bfa; font-weight: 600;">Storage</span><br><span style="font-size: 1.5rem; font-weight: 700;">30%</span></div>
                <div><span style="color: #60a5fa; font-weight: 600;">Wind</span><br><span style="font-size: 1.5rem; font-weight: 700;">21%</span></div>
                <div><span style="color: #fbbf24; font-weight: 600;">Solar</span><br><span style="font-size: 1.5rem; font-weight: 700;">14%</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="card">
            <div class="card-header">Interconnection Costs</div>
            <div style="margin-bottom: 0.75rem;">
                <span style="color: #22c55e; font-weight: 600;">Completed Projects</span><br>
                <span style="font-size: 1.75rem; font-weight: 700;">$102/kW</span> <span style="color: #888;">median</span>
            </div>
            <div style="margin-bottom: 0.75rem;">
                <span style="color: #f59e0b; font-weight: 600;">Active Projects</span><br>
                <span style="font-size: 1.75rem; font-weight: 700;">$156/kW</span> <span style="color: #888;">median</span>
            </div>
            <div>
                <span style="color: #ef4444; font-weight: 600;">Withdrawn</span><br>
                <span style="font-size: 1.75rem; font-weight: 700;">$452/kW</span> <span style="color: #888;">median</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown("""
        <div class="card">
            <div class="card-header">Timeline to COD</div>
            <div style="margin-bottom: 1rem;">
                <span style="font-size: 2.5rem; font-weight: 700;">4-5</span>
                <span style="color: #888; font-size: 1.1rem;">years median</span>
            </div>
            <div style="color: #666; font-size: 0.9rem; line-height: 1.5;">
                Doubled since 2007. Queue backlogs continue to grow as interconnection capacity struggles to keep pace with demand.
            </div>
        </div>
        """, unsafe_allow_html=True)


def render_key_insights(summary, tech_df):
    """Render AI-generated insights."""
    st.markdown('<div class="section-header">Key Insights</div>', unsafe_allow_html=True)

    insights = []

    # Technology insight
    if not tech_df.empty:
        top_tech = tech_df.iloc[0]
        insights.append(f"<b>{top_tech['Technology']}</b> leads with {top_tech['Capacity (GW)']:.0f} GW ({top_tech['Share (%)']:.0f}% of all projects)")

    # Scale insight
    total_capacity = summary.get('total_capacity_gw', 0)
    if total_capacity > 0:
        # US has ~1,200 GW installed
        insights.append(f"Queue backlog of <b>{total_capacity:,.0f} GW</b> equals ~{total_capacity/1200*100:.0f}% of total US installed capacity")

    # Completion context
    insights.append("At historical rates, <b>~86% of solar projects will never reach COD</b> - expect significant attrition")

    # Regional
    by_iso = summary.get('by_iso', {})
    if by_iso:
        largest = max(by_iso.items(), key=lambda x: x[1].get('capacity_gw', 0))
        insights.append(f"<b>{largest[0]}</b> has the largest backlog: {largest[1]['capacity_gw']:.0f} GW across {largest[1]['projects']:,} projects")

    st.markdown('<div class="card">', unsafe_allow_html=True)
    for i, insight in enumerate(insights, 1):
        st.markdown(f"""
        <div class="insight-item">
            <div class="insight-number">{i}</div>
            <div class="insight-text">{insight}</div>
        </div>
        """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


def render_changes_feed(market):
    """Render recent changes."""
    st.markdown('<div class="section-header">Recent Changes</div>', unsafe_allow_html=True)

    # Try to get changes
    all_changes = []
    for iso in ['nyiso', 'pjm', 'miso', 'caiso', 'ercot']:
        changes = market.detect_changes(iso, days_back=7)
        if 'error' not in changes:
            all_changes.append(changes)

    if not all_changes:
        st.markdown("""
        <div class="card">
            <div style="text-align: center; padding: 2rem; color: #888;">
                <div style="font-size: 2rem; margin-bottom: 0.5rem;">📊</div>
                <div>Change tracking requires multiple data snapshots.</div>
                <div style="font-size: 0.85rem; margin-top: 0.5rem;">Refresh data periodically to enable week-over-week comparisons.</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        return

    total_added = sum(c.get('added_count', 0) for c in all_changes)
    total_removed = sum(c.get('removed_count', 0) for c in all_changes)
    total_changes = sum(c.get('status_changes_count', 0) for c in all_changes)

    cols = st.columns(3)

    with cols[0]:
        st.markdown(f"""
        <div class="card" style="text-align: center;">
            <span class="badge badge-new">New</span>
            <div style="font-size: 2.5rem; font-weight: 700; margin: 0.5rem 0;">{total_added}</div>
            <div style="color: #888; font-size: 0.85rem;">Projects added this week</div>
        </div>
        """, unsafe_allow_html=True)

    with cols[1]:
        st.markdown(f"""
        <div class="card" style="text-align: center;">
            <span class="badge badge-removed">Withdrawn</span>
            <div style="font-size: 2.5rem; font-weight: 700; margin: 0.5rem 0;">{total_removed}</div>
            <div style="color: #888; font-size: 0.85rem;">Projects removed this week</div>
        </div>
        """, unsafe_allow_html=True)

    with cols[2]:
        st.markdown(f"""
        <div class="card" style="text-align: center;">
            <span class="badge badge-changed">Updated</span>
            <div style="font-size: 2.5rem; font-weight: 700; margin: 0.5rem 0;">{total_changes}</div>
            <div style="color: #888; font-size: 0.85rem;">Status changes this week</div>
        </div>
        """, unsafe_allow_html=True)


def render_data_explorer(market):
    """Render data exploration section."""
    st.markdown('<div class="section-header">Data Explorer</div>', unsafe_allow_html=True)

    iso_options = ['All ISOs'] + [iso.upper() for iso in market.SUPPORTED_ISOS]

    col1, col2 = st.columns([1, 4])
    with col1:
        selected_iso = st.selectbox("Select ISO", iso_options, label_visibility="collapsed")

    df = market.get_latest_data(selected_iso.lower() if selected_iso != 'All ISOs' else None)

    if df.empty:
        st.info(f"No data for {selected_iso}. Click Refresh to fetch.")
        return

    st.markdown(f"**{len(df):,} projects** in queue")

    # Find displayable columns
    display_cols = []
    col_mappings = [
        ('Queue ID', 'queue_id', 'ID'),
        ('Project Name', 'project_name', 'name'),
        ('Capacity (MW)', 'capacity_mw', 'MW'),
        ('Generation Type', 'type', 'fuel', 'Fuel Type'),
        ('Status', 'status', 'Queue Status'),
        ('State', 'state'),
        ('County', 'county'),
    ]

    for candidates in col_mappings:
        for col in candidates:
            if col in df.columns:
                display_cols.append(col)
                break

    if display_cols:
        st.dataframe(
            df[display_cols].head(200),
            use_container_width=True,
            height=400
        )


def render_news_feed():
    """Render energy news feed."""
    st.markdown('<div class="section-header">Energy News</div>', unsafe_allow_html=True)

    news = NewsAggregator()

    col1, col2 = st.columns([4, 1])
    with col2:
        if st.button("🔄 Refresh News", use_container_width=True):
            with st.spinner("Fetching news..."):
                news.fetch_news(force_refresh=True)
                st.rerun()

    articles = news.get_cached_news(limit=10)

    if not articles:
        st.markdown("""
        <div class="card">
            <div style="text-align: center; padding: 2rem; color: #888;">
                <div style="font-size: 2rem; margin-bottom: 0.5rem;">📰</div>
                <div>No news articles cached yet.</div>
                <div style="font-size: 0.85rem; margin-top: 0.5rem;">Click "Refresh News" to fetch latest energy news.</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        return

    # Display news in a clean format
    st.markdown('<div class="card">', unsafe_allow_html=True)
    for i, article in enumerate(articles):
        if i > 0:
            st.markdown('<hr style="border: none; border-top: 1px solid #f0f0f0; margin: 0.75rem 0;">', unsafe_allow_html=True)

        source_badge = f'<span class="badge badge-info">{article.get("source", "News")}</span>'
        title = article.get('title', 'No title')
        link = article.get('link', '#')
        published = article.get('published', '')[:16] if article.get('published') else ''

        st.markdown(f"""
        <div style="padding: 0.5rem 0;">
            {source_badge}
            <div style="margin-top: 0.5rem;">
                <a href="{link}" target="_blank" style="color: #1a1a1a; text-decoration: none; font-weight: 600; font-size: 0.95rem;">{title}</a>
            </div>
            <div style="color: #888; font-size: 0.8rem; margin-top: 0.25rem;">{published}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)


# =============================================================================
# PE VALUATION ESTIMATOR
# =============================================================================

# Base $/W values by stage
STAGE_VALUES = {
    'Pre-Queue': 0.02,
    'Feasibility': 0.05,
    'System Impact': 0.10,
    'Facilities Study': 0.15,
    'IA Pending': 0.22,
    'IA Executed': 0.30,
    'Under Construction': 0.45,
}

# Completion probabilities by stage
STAGE_COMPLETION_PROB = {
    'Pre-Queue': 0.08,
    'Feasibility': 0.12,
    'System Impact': 0.20,
    'Facilities Study': 0.35,
    'IA Pending': 0.50,
    'IA Executed': 0.65,
    'Under Construction': 0.85,
}

# Type adjustments
TYPE_MULTIPLIERS = {
    'Solar': 0.85,
    'Wind': 0.80,
    'Storage': 1.0,
    'Solar + Storage': 0.95,
    'Gas': 1.1,
    'Load': 1.15,
}


def estimate_stage_from_data(row):
    """Estimate development stage from queue data."""
    status = str(row.get('Status', row.get('Queue Status', ''))).lower()
    studies = str(row.get('Availability of Studies', '')).upper()

    if 'construction' in status or 'building' in status:
        return 'Under Construction'
    elif 'ia' in status and 'exec' in status:
        return 'IA Executed'
    elif 'ia' in status or 'agreement' in status:
        return 'IA Pending'
    elif 'FS' in studies or 'facilities' in status:
        return 'Facilities Study'
    elif 'SIS' in studies or 'SRIS' in studies or 'system' in status:
        return 'System Impact'
    elif 'FES' in studies or 'feasibility' in status:
        return 'Feasibility'
    else:
        return 'Feasibility'  # Default for active queue


def calculate_pe_valuation(capacity_mw, stage, project_type, score=None, ic_cost_per_kw=None):
    """
    Calculate PE valuation metrics for a project.

    Returns dict with:
    - base_value_per_w: Base $/W for this stage
    - risk_adjusted_per_w: After applying adjustments
    - total_fair_value: Fair value in $M
    - completion_prob: Probability of reaching COD
    - expected_value: Probability-weighted value
    """
    # Base value
    base_per_w = STAGE_VALUES.get(stage, 0.05)

    # Type adjustment
    type_mult = TYPE_MULTIPLIERS.get(project_type, 0.9)

    # Score adjustment (if provided) - maps 0-100 to 0.6-1.1
    score_mult = 1.0
    if score is not None:
        score_mult = 0.6 + (score / 100) * 0.5

    # IC cost risk adjustment
    ic_mult = 1.0
    if ic_cost_per_kw is not None:
        if ic_cost_per_kw > 300:
            ic_mult = 0.7
        elif ic_cost_per_kw > 200:
            ic_mult = 0.85
        elif ic_cost_per_kw > 100:
            ic_mult = 0.95

    # Size adjustment (larger = slightly lower $/W)
    size_mult = 1.0
    if capacity_mw > 500:
        size_mult = 0.9
    elif capacity_mw > 200:
        size_mult = 0.95
    elif capacity_mw < 50:
        size_mult = 1.05

    # Calculate risk-adjusted value
    risk_adjusted = base_per_w * type_mult * score_mult * ic_mult * size_mult

    # Total fair value
    capacity_w = capacity_mw * 1_000_000
    total_fair_value = (risk_adjusted * capacity_w) / 1_000_000  # $M

    # Completion probability
    base_prob = STAGE_COMPLETION_PROB.get(stage, 0.15)
    # Adjust by score
    if score is not None:
        prob_adj = 0.7 + (score / 100) * 0.6  # Maps 0-100 to 0.7-1.3
        completion_prob = min(0.95, base_prob * prob_adj)
    else:
        completion_prob = base_prob

    # Expected value
    expected_value = total_fair_value * completion_prob

    return {
        'base_value_per_w': base_per_w,
        'type_mult': type_mult,
        'score_mult': score_mult,
        'ic_mult': ic_mult,
        'size_mult': size_mult,
        'risk_adjusted_per_w': risk_adjusted,
        'total_fair_value_m': total_fair_value,
        'completion_prob': completion_prob,
        'expected_value_m': expected_value,
        'stage': stage,
    }


def render_pe_valuation(market):
    """Render PE Valuation Estimator section."""
    st.markdown('<div class="section-header">💰 PE Valuation Estimator</div>', unsafe_allow_html=True)

    # Get data
    df = market.get_latest_data()

    if df.empty:
        st.info("No queue data available. Click Refresh to load.")
        return

    # Project selector
    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        # Build project options
        id_col = None
        name_col = None
        for c in ['Queue ID', 'queue_id', 'ID', 'Queue Pos.']:
            if c in df.columns:
                id_col = c
                break
        for c in ['Project Name', 'project_name', 'name']:
            if c in df.columns:
                name_col = c
                break

        if id_col and name_col:
            df['_display'] = df[id_col].astype(str) + ' - ' + df[name_col].astype(str).str[:40]
        elif id_col:
            df['_display'] = df[id_col].astype(str)
        else:
            df['_display'] = df.index.astype(str)

        selected = st.selectbox("Select Project", df['_display'].tolist(), key='pe_project')
        project_idx = df[df['_display'] == selected].index[0]
        project = df.loc[project_idx]

    with col2:
        # Developer asking price input
        ask_price = st.number_input("Developer Ask ($M)", min_value=0.0, value=10.0, step=1.0)

    with col3:
        # Manual score override
        manual_score = st.number_input("Feasibility Score (0-100)", min_value=0, max_value=100, value=50)

    # Extract project details
    cap_col = None
    for c in ['Capacity (MW)', 'capacity_mw', 'MW', 'SP (MW)', 'Nameplate (MW)']:
        if c in df.columns:
            cap_col = c
            break

    capacity = float(project[cap_col]) if cap_col and pd.notna(project[cap_col]) else 100.0

    type_col = None
    for c in ['Generation Type', 'type', 'Fuel', 'fuel', 'Type/ Fuel']:
        if c in df.columns:
            type_col = c
            break

    project_type = str(project[type_col]) if type_col and pd.notna(project[type_col]) else 'Solar'

    # Map type to our categories
    type_map = {
        'S': 'Solar', 'Solar': 'Solar',
        'W': 'Wind', 'Wind': 'Wind',
        'ES': 'Storage', 'Storage': 'Storage', 'Battery': 'Storage',
        'NG': 'Gas', 'Gas': 'Gas', 'Natural Gas': 'Gas',
        'L': 'Load', 'Load': 'Load',
    }
    project_type = type_map.get(project_type, 'Solar')

    # Estimate stage
    stage = estimate_stage_from_data(project)

    # Calculate valuation
    valuation = calculate_pe_valuation(
        capacity_mw=capacity,
        stage=stage,
        project_type=project_type,
        score=manual_score,
        ic_cost_per_kw=150  # Default estimate
    )

    # Calculate comparison to ask
    fair_value = valuation['total_fair_value_m']
    premium_discount = ((ask_price - fair_value) / fair_value * 100) if fair_value > 0 else 0

    # Display results
    st.markdown("---")

    # Main valuation card
    col1, col2 = st.columns([1.5, 1])

    with col1:
        # Verdict color
        if premium_discount > 30:
            verdict_class = "badge-removed"
            verdict = "OVERPRICED"
            verdict_color = "#ef4444"
        elif premium_discount > 10:
            verdict_class = "badge-changed"
            verdict = "NEGOTIATE"
            verdict_color = "#f59e0b"
        elif premium_discount > -10:
            verdict_class = "badge-info"
            verdict = "FAIR VALUE"
            verdict_color = "#2563eb"
        else:
            verdict_class = "badge-new"
            verdict = "ATTRACTIVE"
            verdict_color = "#22c55e"

        # Calculate display values
        ask_per_w = ask_price / capacity * 1000 if capacity > 0 else 0
        risk_adj_per_w = valuation['risk_adjusted_per_w']
        premium_bg = '#fef2f2' if premium_discount > 0 else '#f0fdf4'
        premium_color = '#991b1b' if premium_discount > 0 else '#166534'
        premium_sign = '+' if premium_discount > 0 else ''
        premium_label = 'Overpriced' if premium_discount > 0 else 'Discount'
        comp_prob = valuation['completion_prob'] * 100
        exp_val = valuation['expected_value_m']
        base_val = valuation['base_value_per_w']
        risk_mult = valuation['type_mult'] * valuation['score_mult'] * valuation['ic_mult']

        # Header
        st.markdown(f'''<div class="card" style="border-left: 4px solid {verdict_color};">
<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
<div>
<div style="font-size: 0.8rem; color: #888; text-transform: uppercase; letter-spacing: 0.05em;">Valuation Analysis</div>
<div style="font-size: 1.4rem; font-weight: 700; color: #1a1a1a;">{project_type} • {capacity:.0f} MW • {stage}</div>
</div>
<span class="badge {verdict_class}" style="font-size: 0.85rem;">{verdict}</span>
</div>
</div>''', unsafe_allow_html=True)

        # Use Streamlit columns for the metrics instead of HTML grid
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Developer Ask", f"${ask_price:.1f}M", f"${ask_per_w:.2f}/W")
        with m2:
            st.metric("Fair Value", f"${fair_value:.1f}M", f"${risk_adj_per_w:.3f}/W")
        with m3:
            st.metric("Premium/Discount", f"{premium_sign}{premium_discount:.0f}%", premium_label)

        # Bottom metrics row
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("Completion Prob", f"{comp_prob:.0f}%")
        with m2:
            st.metric("Expected Value", f"${exp_val:.1f}M")
        with m3:
            st.metric("Base $/W", f"${base_val:.2f}")
        with m4:
            st.metric("Risk Mult", f"{risk_mult:.2f}x")

    with col2:
        # IRR Scenarios
        st.subheader("IRR Scenarios")
        st.caption("If buying at ask price")

        # Calculate IRR scenarios
        exit_value = capacity * 0.40 * 1000 / 1_000_000  # $M at $0.40/W

        if ask_price > 0:
            success_return = ((exit_value / ask_price) - 1) * 100
            failure_return = -75
            prob = valuation['completion_prob']
            blended_return = (prob * success_return) + ((1 - prob) * failure_return)
        else:
            success_return = 0
            failure_return = 0
            blended_return = 0

        # Display IRR scenarios using native Streamlit
        success_pct = valuation['completion_prob'] * 100
        failure_pct = (1 - valuation['completion_prob']) * 100

        st.markdown(f"**Success ({success_pct:.0f}%):** :green[{success_return:+.0f}%]")
        st.markdown(f"**Failure ({failure_pct:.0f}%):** :red[{failure_return}%]")
        st.divider()

        blended_color = "green" if blended_return > 0 else "red"
        st.markdown(f"**Blended Expected:** :{blended_color}[{blended_return:+.0f}%]")

        # Recommendation
        if blended_return > 15:
            rec = "Strong buy at this price"
            st.success(f"**Recommendation:** {rec}")
        elif blended_return > 5:
            rec = "Acceptable risk/reward"
            st.info(f"**Recommendation:** {rec}")
        elif blended_return > -10:
            rec = "Marginal - negotiate lower"
            st.warning(f"**Recommendation:** {rec}")
        else:
            rec = "Pass or significant discount needed"
            st.error(f"**Recommendation:** {rec}")

    # Value adjustments breakdown
    with st.expander("📊 Valuation Adjustments Breakdown"):
        adj_df = pd.DataFrame({
            'Factor': ['Base (Stage)', 'Type Adjustment', 'Score Adjustment', 'IC Cost Risk', 'Size Adjustment'],
            'Multiplier': [
                f"${valuation['base_value_per_w']:.2f}/W",
                f"{valuation['type_mult']:.2f}x",
                f"{valuation['score_mult']:.2f}x",
                f"{valuation['ic_mult']:.2f}x",
                f"{valuation['size_mult']:.2f}x"
            ],
            'Rationale': [
                f"{stage} stage base value",
                f"{project_type} historical completion rates",
                f"Score {manual_score}/100 risk adjustment",
                "Estimated IC cost ~$150/kW",
                f"{capacity:.0f} MW capacity adjustment"
            ]
        })
        st.dataframe(adj_df, use_container_width=True, hide_index=True)


def render_footer():
    """Render footer."""
    st.markdown("---")
    cols = st.columns(3)

    with cols[0]:
        st.caption("Data: GridStatus API, LBL Queued Up 2024")
    with cols[1]:
        st.caption("Built for interconnection queue market intelligence")
    with cols[2]:
        st.caption(f"Dashboard v1.0 | {datetime.now().strftime('%Y-%m-%d')}")


def render_sidebar(market):
    """Render sidebar with report generation and settings."""
    with st.sidebar:
        st.markdown("## Reports & Analytics")

        # Saved Reports Browser (if report manager available)
        if REPORT_MANAGER_AVAILABLE:
            st.markdown("### 📁 Saved Reports")
            rm = ReportManager()
            reports = rm.list_reports(limit=10)

            if reports:
                for r in reports:
                    with st.expander(f"{r['report_type'].upper()}: {r['title'][:30]}..."):
                        st.caption(f"Client: {r['client']}")
                        st.caption(f"Created: {r['created_at'][:10]}")
                        if r.get('files'):
                            for f in r['files']:
                                file_path = Path(r['report_dir']) / f
                                if file_path.exists():
                                    with open(file_path, 'rb') as fp:
                                        st.download_button(
                                            f"📥 {f}",
                                            fp.read(),
                                            file_name=f,
                                            key=f"{r['report_id']}_{f}"
                                        )
            else:
                st.caption("No saved reports yet")

            st.markdown("---")

        # Portfolio Report Section
        st.markdown("### Portfolio Report")
        st.caption("Generate a comprehensive PDF report with market-wide analytics for PE firms.")

        if PDF_AVAILABLE:
            client_name = st.text_input("Client Name", value="Confidential", key="report_client")

            if st.button("Generate Portfolio PDF", type="primary", use_container_width=True):
                with st.spinner("Generating report with charts..."):
                    try:
                        df = market.get_latest_data()
                        if df.empty:
                            st.error("No data available. Refresh data first.")
                        else:
                            pdf_path = generate_portfolio_report(
                                df=df,
                                client_name=client_name,
                                include_charts=True
                            )
                            st.session_state['pdf_path'] = pdf_path

                            # Save to centralized report storage
                            if REPORT_MANAGER_AVAILABLE:
                                rm = ReportManager()
                                total_projects = len(df)
                                total_gw = df['Capacity (MW)'].sum() / 1000 if 'Capacity (MW)' in df.columns else 0
                                report_entry = create_portfolio_report(
                                    rm=rm,
                                    client=client_name,
                                    project_count=total_projects,
                                    total_capacity_gw=total_gw
                                )
                                rm.add_file_to_report(
                                    report_entry['report_id'],
                                    'portfolio',
                                    Path(pdf_path),
                                    'portfolio_report.pdf'
                                )
                                st.success(f"Report saved! ID: {report_entry['report_id'][:20]}...")
                            else:
                                st.success("Report generated!")
                    except Exception as e:
                        st.error(f"Error: {e}")

            # Download button if PDF was generated
            if 'pdf_path' in st.session_state:
                pdf_path = st.session_state['pdf_path']
                try:
                    with open(pdf_path, 'rb') as f:
                        pdf_bytes = f.read()
                    st.download_button(
                        label="Download Portfolio PDF",
                        data=pdf_bytes,
                        file_name=f"portfolio_report_{datetime.now().strftime('%Y%m%d')}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
                except Exception:
                    pass
        else:
            st.warning("PDF generation requires WeasyPrint system dependencies.")
            st.code("brew install cairo pango gdk-pixbuf libffi", language="bash")
            st.caption("Run the above, then restart the dashboard.")

        st.markdown("---")

        # Quick PE Analytics Summary
        if PE_ANALYTICS_AVAILABLE:
            st.markdown("### Quick Analytics")
            if st.button("Run PE Analysis", use_container_width=True):
                with st.spinner("Analyzing..."):
                    df = market.get_latest_data()
                    if not df.empty:
                        results = analyze_portfolio(df)
                        summary = results['summary']

                        st.metric("Total Pipeline", f"{summary['total_mw']/1000:,.0f} GW")
                        st.metric("Expected MW", f"{summary['expected_mw']/1000:,.0f} GW")
                        st.metric("Risk Discount", f"{summary['discount_rate']*100:.0f}%")
                        st.metric("Developer HHI", f"{summary['developer_hhi']:,.0f}")
                    else:
                        st.warning("No data available")
        else:
            st.warning("PE Analytics module not available.")

        st.markdown("---")
        st.markdown("### Settings")
        st.caption("Dashboard refresh: 5 min cache")


def main():
    """Main dashboard."""
    # Load data first for sidebar
    market = get_market_data()

    # Render sidebar
    render_sidebar(market)

    render_header()

    st.markdown("---")

    # Load analytics
    analytics = get_analytics(market)

    summary = analytics.get_summary_stats()
    regional = analytics.get_regional_comparison()
    tech = analytics.get_technology_breakdown()

    # KPIs
    render_kpis(summary)

    st.markdown("<br>", unsafe_allow_html=True)

    # Regional and Tech side by side
    col1, col2 = st.columns([1.1, 0.9])

    with col1:
        render_regional_breakdown(regional)

    with col2:
        render_technology_mix(tech)

    st.markdown("---")

    # Insights and Changes
    col1, col2 = st.columns(2)

    with col1:
        render_key_insights(summary, tech)

    with col2:
        render_changes_feed(market)

    st.markdown("---")

    # Benchmarks
    render_benchmarks()

    st.markdown("---")

    # PE Valuation Estimator
    render_pe_valuation(market)

    st.markdown("---")

    # News and Data Explorer side by side
    col1, col2 = st.columns([1, 1.5])

    with col1:
        render_news_feed()

    with col2:
        render_data_explorer(market)

    render_footer()


if __name__ == '__main__':
    main()
