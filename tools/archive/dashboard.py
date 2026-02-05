#!/usr/bin/env python3
"""
Streamlit Dashboard for Queue Analysis

Interactive exploration of interconnection queue projects.

Usage:
    streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from pathlib import Path
from datetime import datetime

# Import local modules
from analyze import QueueData, QueueAnalyzer
from scoring import FeasibilityScorer
from real_data import RealDataEstimator
from historical_data import HistoricalData

# Page config
st.set_page_config(
    page_title="Queue Analysis Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for cards and styling
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 5px;
    }
    .metric-card.green { background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); }
    .metric-card.yellow { background: linear-gradient(135deg, #F2994A 0%, #F2C94C 100%); }
    .metric-card.red { background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%); }
    .metric-card.blue { background: linear-gradient(135deg, #2193b0 0%, #6dd5ed 100%); }
    .metric-value { font-size: 2.5rem; font-weight: bold; margin: 0; }
    .metric-label { font-size: 0.9rem; opacity: 0.9; margin-top: 5px; }
    .metric-sublabel { font-size: 0.75rem; opacity: 0.7; }

    .traffic-light { display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin-right: 8px; }
    .traffic-green { background: #22c55e; }
    .traffic-yellow { background: #f59e0b; }
    .traffic-red { background: #ef4444; }

    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { padding: 10px 20px; }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=3600)
def load_data():
    """Load and cache queue data."""
    loader = QueueData()
    df = loader.load_nyiso()
    return df


@st.cache_data(ttl=3600)
def load_historical():
    """Load historical data for comparisons."""
    try:
        hd = HistoricalData()
        return hd
    except Exception as e:
        st.warning(f"Historical data not available: {e}")
        return None


@st.cache_data(ttl=3600)
def score_all_projects(_df):
    """Score all projects and cache results."""
    scorer = FeasibilityScorer(_df)
    results = []

    for _, row in _df.iterrows():
        try:
            # Get project ID
            project_id = str(row.get('Queue ID', row.get('queue_id', row.name)))
            result = scorer.score_project(project_id=project_id)
            if 'error' not in result:
                results.append({
                    'project_id': project_id,
                    'name': result['project'].get('name', 'Unknown'),
                    'developer': result['project'].get('developer', 'Unknown'),
                    'type': result['project'].get('type', 'Unknown'),
                    'capacity_mw': result['project'].get('capacity_mw', 0),
                    'state': result['project'].get('state', 'Unknown'),
                    'score': result['total_score'],
                    'grade': result['grade'],
                    'recommendation': result['recommendation'],
                    'queue_position': result['breakdown'].get('queue_position', 0),
                    'study_progress': result['breakdown'].get('study_progress', 0),
                    'developer_track': result['breakdown'].get('developer_track_record', 0),
                    'poi_congestion': result['breakdown'].get('poi_congestion', 0),
                    'characteristics': result['breakdown'].get('project_characteristics', 0),
                    'red_flags': len(result.get('red_flags', [])),
                    'green_flags': len(result.get('green_flags', [])),
                })
        except Exception:
            continue

    return pd.DataFrame(results)


def render_metric_card(value, label, sublabel="", color="blue"):
    """Render a styled metric card."""
    st.markdown(f"""
    <div class="metric-card {color}">
        <p class="metric-value">{value}</p>
        <p class="metric-label">{label}</p>
        <p class="metric-sublabel">{sublabel}</p>
    </div>
    """, unsafe_allow_html=True)


def render_traffic_light(label, level):
    """Render a traffic light indicator."""
    color_class = {'Low': 'green', 'Medium': 'yellow', 'High': 'red'}.get(level, 'yellow')
    st.markdown(f"""
    <div style="padding: 8px 0;">
        <span class="traffic-light traffic-{color_class}"></span>
        <span>{label}: <strong>{level}</strong></span>
    </div>
    """, unsafe_allow_html=True)


def main():
    # Sidebar
    st.sidebar.title("⚡ Queue Analysis")
    st.sidebar.markdown("---")

    # Load data
    with st.spinner("Loading queue data..."):
        df = load_data()
        hd = load_historical()

    if df.empty:
        st.error("No data loaded. Check your data sources.")
        return

    # Score all projects
    with st.spinner("Scoring projects..."):
        scores_df = score_all_projects(df)

    if scores_df.empty:
        st.error("No projects could be scored.")
        return

    # Sidebar filters
    st.sidebar.subheader("Filters")

    # Type filter
    types = ['All'] + sorted(scores_df['type'].unique().tolist())
    selected_type = st.sidebar.selectbox("Project Type", types)

    # Capacity filter
    min_cap, max_cap = st.sidebar.slider(
        "Capacity (MW)",
        min_value=0,
        max_value=int(scores_df['capacity_mw'].max()) + 100,
        value=(0, int(scores_df['capacity_mw'].max()) + 100)
    )

    # Score filter
    min_score = st.sidebar.slider("Minimum Score", 0, 100, 0)

    # Recommendation filter
    recs = ['All'] + sorted(scores_df['recommendation'].unique().tolist())
    selected_rec = st.sidebar.selectbox("Recommendation", recs)

    # Apply filters
    filtered_df = scores_df.copy()
    if selected_type != 'All':
        filtered_df = filtered_df[filtered_df['type'] == selected_type]
    filtered_df = filtered_df[
        (filtered_df['capacity_mw'] >= min_cap) &
        (filtered_df['capacity_mw'] <= max_cap) &
        (filtered_df['score'] >= min_score)
    ]
    if selected_rec != 'All':
        filtered_df = filtered_df[filtered_df['recommendation'] == selected_rec]

    # Main content
    st.title("Interconnection Queue Dashboard")
    st.markdown(f"**Region:** NYISO | **Last Updated:** {datetime.now().strftime('%Y-%m-%d')}")

    # Summary metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        render_metric_card(
            len(filtered_df),
            "Projects",
            f"of {len(scores_df)} total",
            "blue"
        )

    with col2:
        go_count = len(filtered_df[filtered_df['recommendation'] == 'GO'])
        render_metric_card(
            go_count,
            "GO Projects",
            f"{go_count/len(filtered_df)*100:.0f}% of filtered" if len(filtered_df) > 0 else "",
            "green"
        )

    with col3:
        avg_score = filtered_df['score'].mean() if len(filtered_df) > 0 else 0
        render_metric_card(
            f"{avg_score:.0f}",
            "Avg Score",
            "out of 100",
            "yellow" if avg_score < 70 else "green"
        )

    with col4:
        total_mw = filtered_df['capacity_mw'].sum()
        render_metric_card(
            f"{total_mw:,.0f}",
            "Total MW",
            f"{len(filtered_df)} projects",
            "blue"
        )

    st.markdown("---")

    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📈 Charts", "📋 Project List", "🔍 Deep Dive"])

    with tab1:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Score Distribution")

            # Score histogram
            hist = alt.Chart(filtered_df).mark_bar(
                cornerRadiusTopLeft=4,
                cornerRadiusTopRight=4
            ).encode(
                x=alt.X('score:Q', bin=alt.Bin(maxbins=20), title='Score'),
                y=alt.Y('count()', title='Projects'),
                color=alt.condition(
                    alt.datum.score >= 70,
                    alt.value('#22c55e'),
                    alt.condition(
                        alt.datum.score >= 50,
                        alt.value('#f59e0b'),
                        alt.value('#ef4444')
                    )
                ),
                tooltip=['count()']
            ).properties(height=300)

            st.altair_chart(hist, use_container_width=True)

        with col2:
            st.subheader("Recommendation Breakdown")

            # Recommendation donut
            rec_counts = filtered_df['recommendation'].value_counts().reset_index()
            rec_counts.columns = ['recommendation', 'count']

            rec_colors = {'GO': '#22c55e', 'CONDITIONAL': '#f59e0b', 'NO-GO': '#ef4444'}
            rec_counts['color'] = rec_counts['recommendation'].map(rec_colors)

            donut = alt.Chart(rec_counts).mark_arc(innerRadius=60).encode(
                theta=alt.Theta('count:Q'),
                color=alt.Color('recommendation:N', scale=alt.Scale(
                    domain=list(rec_colors.keys()),
                    range=list(rec_colors.values())
                )),
                tooltip=['recommendation', 'count']
            ).properties(height=300)

            st.altair_chart(donut, use_container_width=True)

        # Type breakdown
        st.subheader("Projects by Type")
        type_counts = filtered_df.groupby('type').agg({
            'project_id': 'count',
            'capacity_mw': 'sum',
            'score': 'mean'
        }).reset_index()
        type_counts.columns = ['Type', 'Projects', 'Total MW', 'Avg Score']

        type_bar = alt.Chart(type_counts).mark_bar(
            cornerRadiusTopRight=4,
            cornerRadiusBottomRight=4
        ).encode(
            y=alt.Y('Type:N', sort='-x'),
            x=alt.X('Projects:Q'),
            color=alt.Color('Avg Score:Q', scale=alt.Scale(scheme='viridis')),
            tooltip=['Type', 'Projects', 'Total MW', alt.Tooltip('Avg Score:Q', format='.0f')]
        ).properties(height=200)

        st.altair_chart(type_bar, use_container_width=True)

    with tab2:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Cost vs Capacity")

            # Simulate cost data (in real app, would come from estimates)
            chart_df = filtered_df.copy()
            chart_df['estimated_cost_per_kw'] = np.random.normal(150, 50, len(chart_df)).clip(50, 350)

            scatter = alt.Chart(chart_df).mark_circle(size=80, opacity=0.7).encode(
                x=alt.X('capacity_mw:Q', title='Capacity (MW)', scale=alt.Scale(zero=True)),
                y=alt.Y('estimated_cost_per_kw:Q', title='Est. Cost ($/kW)'),
                color=alt.Color('recommendation:N', scale=alt.Scale(
                    domain=['GO', 'CONDITIONAL', 'NO-GO'],
                    range=['#22c55e', '#f59e0b', '#ef4444']
                )),
                tooltip=['name', 'capacity_mw', 'score', 'recommendation']
            ).properties(height=350)

            st.altair_chart(scatter, use_container_width=True)

        with col2:
            st.subheader("Score Components")

            # Average score breakdown
            avg_breakdown = pd.DataFrame({
                'Component': ['Queue Position', 'Study Progress', 'Developer', 'POI Congestion', 'Characteristics'],
                'Score': [
                    filtered_df['queue_position'].mean(),
                    filtered_df['study_progress'].mean(),
                    filtered_df['developer_track'].mean(),
                    filtered_df['poi_congestion'].mean(),
                    filtered_df['characteristics'].mean()
                ],
                'Max': [25, 25, 20, 15, 15]
            })
            avg_breakdown['Percentage'] = avg_breakdown['Score'] / avg_breakdown['Max'] * 100

            # Risk level colors
            def risk_color(pct):
                if pct >= 75: return '#22c55e'
                elif pct >= 50: return '#f59e0b'
                else: return '#ef4444'

            avg_breakdown['Color'] = avg_breakdown['Percentage'].apply(risk_color)

            bars = alt.Chart(avg_breakdown).mark_bar(
                cornerRadiusTopRight=4,
                cornerRadiusBottomRight=4
            ).encode(
                y=alt.Y('Component:N', sort=alt.EncodingSortField(field='Percentage', order='ascending')),
                x=alt.X('Percentage:Q', scale=alt.Scale(domain=[0, 100]), title='Score (%)'),
                color=alt.Color('Color:N', scale=None),
                tooltip=['Component', alt.Tooltip('Score:Q', format='.1f'), alt.Tooltip('Percentage:Q', format='.0f')]
            ).properties(height=350)

            st.altair_chart(bars, use_container_width=True)

        # Lollipop chart - Top projects
        st.subheader("Top 15 Projects by Score")
        top_15 = filtered_df.nlargest(15, 'score')[['name', 'score', 'recommendation']].copy()

        # Create lollipop chart
        points = alt.Chart(top_15).mark_circle(size=100).encode(
            x=alt.X('score:Q', scale=alt.Scale(domain=[0, 100]), title='Score'),
            y=alt.Y('name:N', sort='-x', title=None),
            color=alt.Color('recommendation:N', scale=alt.Scale(
                domain=['GO', 'CONDITIONAL', 'NO-GO'],
                range=['#22c55e', '#f59e0b', '#ef4444']
            )),
            tooltip=['name', 'score', 'recommendation']
        )

        lines = alt.Chart(top_15).mark_rule(strokeWidth=2).encode(
            x=alt.X('score:Q'),
            x2=alt.value(0),
            y=alt.Y('name:N', sort='-x'),
            color=alt.Color('recommendation:N', scale=alt.Scale(
                domain=['GO', 'CONDITIONAL', 'NO-GO'],
                range=['#22c55e', '#f59e0b', '#ef4444']
            ))
        )

        lollipop = (lines + points).properties(height=400)
        st.altair_chart(lollipop, use_container_width=True)

    with tab3:
        st.subheader(f"Project List ({len(filtered_df)} projects)")

        # Sortable table
        sort_by = st.selectbox("Sort by", ['score', 'capacity_mw', 'name'], index=0)
        sort_order = st.radio("Order", ['Descending', 'Ascending'], horizontal=True)

        display_df = filtered_df.sort_values(
            sort_by,
            ascending=(sort_order == 'Ascending')
        )[['project_id', 'name', 'developer', 'type', 'capacity_mw', 'score', 'grade', 'recommendation']]

        # Style the dataframe
        def color_recommendation(val):
            colors = {'GO': 'background-color: #dcfce7', 'CONDITIONAL': 'background-color: #fef3c7', 'NO-GO': 'background-color: #fee2e2'}
            return colors.get(val, '')

        styled_df = display_df.style.applymap(color_recommendation, subset=['recommendation'])
        st.dataframe(styled_df, use_container_width=True, height=500)

        # Download button
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"queue_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

    with tab4:
        st.subheader("Project Deep Dive")

        # Project selector
        project_options = filtered_df.set_index('project_id')['name'].to_dict()
        selected_id = st.selectbox(
            "Select Project",
            options=list(project_options.keys()),
            format_func=lambda x: f"{x} - {project_options[x]}"
        )

        if selected_id:
            project = filtered_df[filtered_df['project_id'] == selected_id].iloc[0]

            # Executive summary cards
            st.markdown("### Executive Summary")
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                score_color = 'green' if project['score'] >= 70 else ('yellow' if project['score'] >= 50 else 'red')
                render_metric_card(f"{project['score']:.0f}", "Score", f"Grade: {project['grade']}", score_color)

            with col2:
                render_metric_card(f"{project['capacity_mw']:,.0f}", "MW", project['type'], "blue")

            with col3:
                rec_color = {'GO': 'green', 'CONDITIONAL': 'yellow', 'NO-GO': 'red'}.get(project['recommendation'], 'blue')
                render_metric_card(project['recommendation'], "Recommendation", "", rec_color)

            with col4:
                render_metric_card(f"{project['red_flags']}", "Red Flags", f"{project['green_flags']} green flags", "red" if project['red_flags'] > 2 else "yellow")

            st.markdown("---")

            # Score breakdown
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("### Score Breakdown")

                breakdown_data = pd.DataFrame({
                    'Component': ['Queue Position', 'Study Progress', 'Developer Track Record', 'POI Congestion', 'Project Characteristics'],
                    'Score': [project['queue_position'], project['study_progress'], project['developer_track'], project['poi_congestion'], project['characteristics']],
                    'Max': [25, 25, 20, 15, 15]
                })
                breakdown_data['Percentage'] = breakdown_data['Score'] / breakdown_data['Max'] * 100

                for _, row in breakdown_data.iterrows():
                    pct = row['Percentage']
                    level = 'Low' if pct >= 75 else ('Medium' if pct >= 50 else 'High')
                    render_traffic_light(f"{row['Component']} ({row['Score']:.0f}/{row['Max']})", level)

            with col2:
                st.markdown("### Project Details")
                st.markdown(f"**Developer:** {project['developer']}")
                st.markdown(f"**State:** {project['state']}")
                st.markdown(f"**Type:** {project['type']}")
                st.markdown(f"**Capacity:** {project['capacity_mw']:,.0f} MW")

                # Generate report button
                st.markdown("---")
                if st.button("📄 Generate Full Report"):
                    st.info(f"Run: `python3 deep_report.py {selected_id} --client 'Your Client' -o report.md`")

    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666; font-size: 0.85rem;'>"
        "Queue Analysis Dashboard | Data refreshed daily | "
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
