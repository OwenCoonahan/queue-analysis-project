#!/usr/bin/env python3
"""
Altair-based Chart Generation for Queue Analysis

Altair provides cleaner defaults and more intuitive visualizations.
Compare these outputs to the matplotlib/plotly versions in charts.py
"""

import altair as alt
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Optional, Any

# Output directory
CHART_DIR = Path(__file__).parent / 'charts'
CHART_DIR.mkdir(exist_ok=True)

# Professional color scheme
COLORS = {
    'primary': '#2563EB',      # Blue
    'success': '#16A34A',      # Green
    'warning': '#D97706',      # Amber
    'danger': '#DC2626',       # Red
    'muted': '#6B7280',        # Gray
    'this_project': '#DC2626', # Red for highlighting
}

# Project type colors
TYPE_COLORS = {
    'Solar': '#F59E0B',
    'Wind': '#0EA5E9',
    'Storage': '#10B981',
    'Hybrid': '#8B5CF6',
    'Gas': '#6B7280',
}

# Status colors
STATUS_COLORS = {
    'Completed': '#16A34A',
    'Active': '#2563EB',
    'Withdrawn': '#DC2626',
    'Pending': '#D97706',
}


def configure_theme():
    """Set up a clean, professional Altair theme."""
    # Use the new theme API (Altair 5.5+)
    try:
        @alt.theme.register('queue_analysis', enable=True)
        def queue_theme():
            return alt.theme.ThemeConfig({
                'config': {
                    'view': {'strokeWidth': 0},
                    'axis': {
                        'labelFontSize': 12,
                        'titleFontSize': 13,
                        'titleFontWeight': 'normal',
                        'gridColor': '#E5E7EB',
                        'domainColor': '#9CA3AF',
                    },
                    'legend': {
                        'labelFontSize': 11,
                        'titleFontSize': 12,
                    },
                    'title': {
                        'fontSize': 16,
                        'fontWeight': 'bold',
                        'anchor': 'start',
                        'offset': 10,
                    },
                    'mark': {
                        'opacity': 0.85,
                    }
                }
            })
    except (AttributeError, TypeError):
        # Fallback for older Altair versions
        alt.themes.register('queue_analysis', lambda: {
            'config': {
                'view': {'strokeWidth': 0},
                'axis': {
                    'labelFontSize': 12,
                    'titleFontSize': 13,
                    'titleFontWeight': 'normal',
                    'gridColor': '#E5E7EB',
                    'domainColor': '#9CA3AF',
                },
                'legend': {
                    'labelFontSize': 11,
                    'titleFontSize': 12,
                },
                'title': {
                    'fontSize': 16,
                    'fontWeight': 'bold',
                    'anchor': 'start',
                    'offset': 10,
                },
                'mark': {
                    'opacity': 0.85,
                }
            }
        })
        alt.themes.enable('queue_analysis')


def cost_scatter(
    historical_df: pd.DataFrame,
    this_project: Dict,
    title: str = "Interconnection Cost Comparison"
) -> alt.Chart:
    """
    Cost vs capacity scatter plot with project overlay.

    Much cleaner than matplotlib version with automatic legends and tooltips.
    """
    configure_theme()

    # Prepare historical data
    df = historical_df.copy()
    df['type'] = df.get('type', 'Unknown')

    # Create type color scale
    type_domain = list(df['type'].unique())
    type_range = [TYPE_COLORS.get(t, COLORS['muted']) for t in type_domain]

    # Historical scatter
    historical = alt.Chart(df).mark_circle(size=80, opacity=0.6).encode(
        x=alt.X('capacity_mw:Q',
                title='Capacity (MW)',
                scale=alt.Scale(zero=True)),
        y=alt.Y('cost_per_kw:Q',
                title='Interconnection Cost ($/kW)',
                scale=alt.Scale(zero=True)),
        color=alt.Color('type:N',
                       scale=alt.Scale(domain=type_domain, range=type_range),
                       title='Project Type'),
        tooltip=[
            alt.Tooltip('capacity_mw:Q', title='Capacity', format=',.0f'),
            alt.Tooltip('cost_per_kw:Q', title='Cost $/kW', format='$,.0f'),
            alt.Tooltip('type:N', title='Type'),
        ]
    ).properties(
        width=600,
        height=400,
        title=title
    )

    # This project point
    proj_df = pd.DataFrame([{
        'capacity_mw': this_project.get('capacity_mw', 0),
        'cost_per_kw': this_project.get('cost_median', 0),
        'cost_low': this_project.get('cost_low', 0),
        'cost_high': this_project.get('cost_high', 0),
        'label': 'This Project'
    }])

    # Error bar for cost range
    error_bar = alt.Chart(proj_df).mark_rule(
        color=COLORS['this_project'],
        strokeWidth=3
    ).encode(
        x='capacity_mw:Q',
        y='cost_low:Q',
        y2='cost_high:Q'
    )

    # Project point
    project_point = alt.Chart(proj_df).mark_point(
        shape='diamond',
        size=300,
        color=COLORS['this_project'],
        filled=True,
        stroke='white',
        strokeWidth=2
    ).encode(
        x='capacity_mw:Q',
        y='cost_per_kw:Q',
        tooltip=[
            alt.Tooltip('label:N', title=''),
            alt.Tooltip('capacity_mw:Q', title='Capacity', format=',.0f'),
            alt.Tooltip('cost_per_kw:Q', title='Est. Cost', format='$,.0f'),
            alt.Tooltip('cost_low:Q', title='Low', format='$,.0f'),
            alt.Tooltip('cost_high:Q', title='High', format='$,.0f'),
        ]
    )

    # Combine layers
    chart = (historical + error_bar + project_point).configure_view(
        strokeWidth=0
    )

    # Save
    chart.save(str(CHART_DIR / 'cost_scatter_altair.html'))
    chart.save(str(CHART_DIR / 'cost_scatter_altair.png'), scale_factor=2)

    return chart


def risk_bars(
    score_breakdown: Dict[str, float],
    max_scores: Dict[str, float],
    title: str = "Risk Factor Analysis"
) -> alt.Chart:
    """
    Horizontal bar chart showing risk factors with color coding.

    Clean, easy to read design that clearly shows problem areas.
    """
    configure_theme()

    # Calculate percentages
    data = []
    for cat, score in score_breakdown.items():
        max_val = max_scores.get(cat, 1)
        pct = (score / max_val) * 100 if max_val > 0 else 0

        # Determine risk level
        if pct >= 75:
            risk = 'Low Risk'
            color = COLORS['success']
        elif pct >= 50:
            risk = 'Medium Risk'
            color = COLORS['warning']
        else:
            risk = 'High Risk'
            color = COLORS['danger']

        data.append({
            'factor': cat.replace('_', ' ').title(),
            'score': pct,
            'risk': risk,
            'color': color
        })

    df = pd.DataFrame(data)

    # Sort by score (lowest first to highlight risks)
    df = df.sort_values('score')

    # Create color scale
    risk_colors = {
        'Low Risk': COLORS['success'],
        'Medium Risk': COLORS['warning'],
        'High Risk': COLORS['danger']
    }

    chart = alt.Chart(df).mark_bar(
        cornerRadiusEnd=4,
        height=25
    ).encode(
        x=alt.X('score:Q',
                title='Score (%)',
                scale=alt.Scale(domain=[0, 100])),
        y=alt.Y('factor:N',
                title=None,
                sort=alt.EncodingSortField(field='score', order='ascending')),
        color=alt.Color('risk:N',
                       scale=alt.Scale(
                           domain=list(risk_colors.keys()),
                           range=list(risk_colors.values())
                       ),
                       title='Risk Level'),
        tooltip=[
            alt.Tooltip('factor:N', title='Factor'),
            alt.Tooltip('score:Q', title='Score', format='.0f'),
            alt.Tooltip('risk:N', title='Risk Level'),
        ]
    ).properties(
        width=500,
        height=alt.Step(40),
        title=title
    )

    # Add text labels
    text = alt.Chart(df).mark_text(
        align='left',
        dx=5,
        fontSize=12,
        fontWeight='bold'
    ).encode(
        x='score:Q',
        y=alt.Y('factor:N', sort=alt.EncodingSortField(field='score', order='ascending')),
        text=alt.Text('score:Q', format='.0f'),
        color=alt.value('#374151')
    )

    combined = (chart + text)

    # Save
    combined.save(str(CHART_DIR / 'risk_bars_altair.html'))
    combined.save(str(CHART_DIR / 'risk_bars_altair.png'), scale_factor=2)

    return combined


def queue_outcomes(
    outcomes: Dict[str, int],
    this_status: str = None,
    title: str = "Queue Outcomes"
) -> alt.Chart:
    """
    Donut chart showing queue status distribution.

    Uses arc marks with clear labels.
    """
    configure_theme()

    # Prepare data
    data = []
    total = sum(outcomes.values())

    for status, count in outcomes.items():
        pct = (count / total) * 100
        color = STATUS_COLORS.get(status, COLORS['muted'])
        data.append({
            'status': status,
            'count': count,
            'percentage': pct,
            'color': color,
            'highlight': status.lower() == (this_status or '').lower()
        })

    df = pd.DataFrame(data)

    # Sort by count descending
    df = df.sort_values('count', ascending=False)

    # Create color scale
    status_domain = df['status'].tolist()
    status_range = [STATUS_COLORS.get(s, COLORS['muted']) for s in status_domain]

    # Donut chart
    base = alt.Chart(df).encode(
        theta=alt.Theta('count:Q', stack=True),
        color=alt.Color('status:N',
                       scale=alt.Scale(domain=status_domain, range=status_range),
                       title='Status'),
        tooltip=[
            alt.Tooltip('status:N', title='Status'),
            alt.Tooltip('count:Q', title='Count', format=','),
            alt.Tooltip('percentage:Q', title='Percent', format='.1f'),
        ]
    )

    # Arc (donut)
    arc = base.mark_arc(innerRadius=80, outerRadius=140, stroke='white', strokeWidth=2)

    # Text labels
    text = base.mark_text(radius=170, fontSize=12).encode(
        text=alt.Text('percentage:Q', format='.0f')
    )

    chart = (arc + text).properties(
        width=350,
        height=350,
        title=title
    )

    # Save
    chart.save(str(CHART_DIR / 'queue_outcomes_altair.html'))
    chart.save(str(CHART_DIR / 'queue_outcomes_altair.png'), scale_factor=2)

    return chart


def timeline_comparison(
    historical_df: pd.DataFrame,
    this_project: Dict,
    title: str = "Time to Commercial Operation"
) -> alt.Chart:
    """
    Box plot showing timeline distribution with project overlay.

    Clean strip plot alternative to traditional box plot.
    """
    configure_theme()

    df = historical_df.copy()

    # Box plot
    box = alt.Chart(df).mark_boxplot(
        color=COLORS['primary'],
        opacity=0.7,
        size=40
    ).encode(
        x=alt.X('region:N', title=None),
        y=alt.Y('months_to_cod:Q', title='Months to COD')
    )

    # This project
    if this_project:
        proj_df = pd.DataFrame([{
            'region': this_project.get('region', 'This Project'),
            'months_to_cod': this_project.get('timeline_likely', 0),
            'timeline_low': this_project.get('timeline_low', 0),
            'timeline_high': this_project.get('timeline_high', 0),
        }])

        # Error bar
        error = alt.Chart(proj_df).mark_rule(
            color=COLORS['this_project'],
            strokeWidth=3
        ).encode(
            x='region:N',
            y='timeline_low:Q',
            y2='timeline_high:Q'
        )

        # Point
        point = alt.Chart(proj_df).mark_point(
            shape='diamond',
            size=200,
            color=COLORS['this_project'],
            filled=True
        ).encode(
            x='region:N',
            y='months_to_cod:Q',
            tooltip=[
                alt.Tooltip('months_to_cod:Q', title='Expected', format='.0f'),
                alt.Tooltip('timeline_low:Q', title='Best Case', format='.0f'),
                alt.Tooltip('timeline_high:Q', title='Worst Case', format='.0f'),
            ]
        )

        chart = (box + error + point)
    else:
        chart = box

    chart = chart.properties(
        width=400,
        height=300,
        title=title
    )

    # Save
    chart.save(str(CHART_DIR / 'timeline_altair.html'))
    chart.save(str(CHART_DIR / 'timeline_altair.png'), scale_factor=2)

    return chart


def completion_rate_bars(
    rates_by_category: Dict[str, float],
    this_category: str = None,
    title: str = "Completion Rate by Category"
) -> alt.Chart:
    """
    Horizontal bar chart of completion rates.

    Clean design with average line.
    """
    configure_theme()

    # Prepare data
    data = []
    for cat, rate in rates_by_category.items():
        data.append({
            'category': cat,
            'rate': rate,
            'highlight': cat.lower() == (this_category or '').lower()
        })

    df = pd.DataFrame(data)
    df = df.sort_values('rate', ascending=True)

    avg_rate = df['rate'].mean()

    # Bars
    bars = alt.Chart(df).mark_bar(
        cornerRadiusEnd=4,
        height=20
    ).encode(
        x=alt.X('rate:Q',
                title='Completion Rate (%)',
                scale=alt.Scale(domain=[0, 100])),
        y=alt.Y('category:N',
                title=None,
                sort=alt.EncodingSortField(field='rate', order='ascending')),
        color=alt.condition(
            alt.datum.highlight,
            alt.value(COLORS['this_project']),
            alt.value(COLORS['primary'])
        ),
        tooltip=[
            alt.Tooltip('category:N', title='Category'),
            alt.Tooltip('rate:Q', title='Completion Rate', format='.1f'),
        ]
    )

    # Average line
    avg_line = alt.Chart(pd.DataFrame({'avg': [avg_rate]})).mark_rule(
        color=COLORS['muted'],
        strokeDash=[4, 4],
        strokeWidth=2
    ).encode(
        x='avg:Q'
    )

    # Average label
    avg_label = alt.Chart(pd.DataFrame({'avg': [avg_rate], 'label': [f'Avg: {avg_rate:.0f}%']})).mark_text(
        align='left',
        dx=5,
        dy=-10,
        fontSize=11,
        color=COLORS['muted']
    ).encode(
        x='avg:Q',
        text='label:N'
    )

    chart = (bars + avg_line + avg_label).properties(
        width=500,
        height=alt.Step(35),
        title=title
    )

    # Save
    chart.save(str(CHART_DIR / 'completion_rates_altair.html'))
    chart.save(str(CHART_DIR / 'completion_rates_altair.png'), scale_factor=2)

    return chart


def lollipop_chart(
    data: Dict[str, float],
    highlight: str = None,
    title: str = "Project Rankings",
    x_title: str = "Score",
    max_value: float = 100
) -> alt.Chart:
    """
    Create a lollipop chart (cleaner alternative to bar charts for rankings).

    Args:
        data: Dict mapping labels to values
        highlight: Label to highlight
        title: Chart title
        x_title: X-axis title
        max_value: Maximum value for x-axis scale

    Returns:
        Altair Chart object
    """
    configure_theme()

    # Prepare data
    df = pd.DataFrame([
        {'label': k, 'value': v, 'highlight': k == highlight}
        for k, v in data.items()
    ])
    df = df.sort_values('value', ascending=True)

    # Create lollipop lines (stems)
    lines = alt.Chart(df).mark_rule(strokeWidth=2).encode(
        y=alt.Y('label:N', sort=alt.EncodingSortField(field='value', order='ascending'), title=None),
        x=alt.X('value:Q', scale=alt.Scale(domain=[0, max_value]), title=x_title),
        x2=alt.value(0),
        color=alt.condition(
            alt.datum.highlight,
            alt.value(COLORS['this_project']),
            alt.value(COLORS['primary'])
        )
    )

    # Create lollipop circles (heads)
    points = alt.Chart(df).mark_circle(size=120).encode(
        y=alt.Y('label:N', sort=alt.EncodingSortField(field='value', order='ascending')),
        x=alt.X('value:Q'),
        color=alt.condition(
            alt.datum.highlight,
            alt.value(COLORS['this_project']),
            alt.value(COLORS['primary'])
        ),
        tooltip=[
            alt.Tooltip('label:N', title='Project'),
            alt.Tooltip('value:Q', title=x_title, format='.0f')
        ]
    )

    # Value labels
    text = alt.Chart(df).mark_text(
        align='left',
        dx=8,
        fontSize=11
    ).encode(
        y=alt.Y('label:N', sort=alt.EncodingSortField(field='value', order='ascending')),
        x=alt.X('value:Q'),
        text=alt.Text('value:Q', format='.0f'),
        color=alt.value('#374151')
    )

    chart = (lines + points + text).properties(
        width=500,
        height=alt.Step(35),
        title=title
    )

    # Save
    chart.save(str(CHART_DIR / 'lollipop_altair.html'))
    chart.save(str(CHART_DIR / 'lollipop_altair.png'), scale_factor=2)

    return chart


def waterfall_chart(
    components: Dict[str, float],
    title: str = "Cost Breakdown",
    value_label: str = "$M"
) -> alt.Chart:
    """
    Create a waterfall chart showing how components add up to a total.

    Useful for cost breakdowns (base cost + upgrades + contingency = total).

    Args:
        components: Ordered dict of component names to values
        title: Chart title
        value_label: Label for values (e.g., "$M", "months")

    Returns:
        Altair Chart object
    """
    configure_theme()

    # Prepare waterfall data
    data = []
    running_total = 0

    for name, value in components.items():
        data.append({
            'component': name,
            'value': value,
            'start': running_total,
            'end': running_total + value,
            'is_total': False,
            'is_positive': value >= 0
        })
        running_total += value

    # Add total bar
    data.append({
        'component': 'Total',
        'value': running_total,
        'start': 0,
        'end': running_total,
        'is_total': True,
        'is_positive': running_total >= 0
    })

    df = pd.DataFrame(data)

    # Determine colors
    def get_color(row):
        if row['is_total']:
            return COLORS['primary']
        elif row['is_positive']:
            return COLORS['warning']  # Amber for costs
        else:
            return COLORS['success']  # Green for reductions

    df['color'] = df.apply(get_color, axis=1)

    # Create bars
    bars = alt.Chart(df).mark_bar(
        cornerRadiusTopLeft=4,
        cornerRadiusTopRight=4
    ).encode(
        x=alt.X('component:N', sort=None, title=None),
        y=alt.Y('start:Q', title=value_label),
        y2='end:Q',
        color=alt.Color('color:N', scale=None),
        tooltip=[
            alt.Tooltip('component:N', title='Component'),
            alt.Tooltip('value:Q', title='Value', format=',.0f')
        ]
    )

    # Connector lines between bars
    connector_data = []
    for i in range(len(df) - 1):
        if not df.iloc[i]['is_total']:
            connector_data.append({
                'x': df.iloc[i]['component'],
                'x2': df.iloc[i + 1]['component'],
                'y': df.iloc[i]['end']
            })

    if connector_data:
        connector_df = pd.DataFrame(connector_data)
        # Note: Altair doesn't directly support connecting lines between bars
        # We'll add dashed rules at the end values instead
        rules = alt.Chart(df[~df['is_total']]).mark_rule(
            strokeDash=[2, 2],
            strokeWidth=1,
            color='#9ca3af'
        ).encode(
            x='component:N',
            x2=alt.value(50),  # Extend slightly
            y='end:Q'
        )
    else:
        rules = alt.Chart(pd.DataFrame()).mark_rule()

    # Value labels
    text = alt.Chart(df).mark_text(
        dy=-10,
        fontSize=11,
        fontWeight='bold'
    ).encode(
        x='component:N',
        y='end:Q',
        text=alt.Text('value:Q', format=',.0f'),
        color=alt.value('#374151')
    )

    chart = (bars + text).properties(
        width=alt.Step(80),
        height=350,
        title=title
    )

    # Save
    chart.save(str(CHART_DIR / 'waterfall_altair.html'))
    chart.save(str(CHART_DIR / 'waterfall_altair.png'), scale_factor=2)

    return chart


def executive_summary_chart(
    score: float,
    cost_range: str,
    timeline_range: str,
    completion_rate: str,
    recommendation: str
) -> alt.Chart:
    """
    Create a visual executive summary with key metrics.

    Args:
        score: Feasibility score (0-100)
        cost_range: Cost range string (e.g., "$45-78M")
        timeline_range: Timeline range string (e.g., "24-36 months")
        completion_rate: Completion rate string (e.g., "18%")
        recommendation: Recommendation (GO/CONDITIONAL/NO-GO)

    Returns:
        Altair Chart object
    """
    configure_theme()

    # Create gauge-like score visualization
    score_data = pd.DataFrame({
        'category': ['Score', 'Remaining'],
        'value': [score, 100 - score]
    })

    # Determine score color
    if score >= 70:
        score_color = COLORS['success']
    elif score >= 50:
        score_color = COLORS['warning']
    else:
        score_color = COLORS['danger']

    gauge = alt.Chart(score_data).mark_arc(innerRadius=60, outerRadius=80).encode(
        theta=alt.Theta('value:Q', stack=True),
        color=alt.Color('category:N', scale=alt.Scale(
            domain=['Score', 'Remaining'],
            range=[score_color, '#e5e7eb']
        ), legend=None)
    ).properties(width=200, height=200)

    # Center text
    center_text = alt.Chart(pd.DataFrame({'text': [f'{score:.0f}']})).mark_text(
        fontSize=36,
        fontWeight='bold',
        color=score_color
    ).encode(text='text:N')

    label_text = alt.Chart(pd.DataFrame({'text': ['/100']})).mark_text(
        fontSize=14,
        dy=25,
        color='#6b7280'
    ).encode(text='text:N')

    score_chart = (gauge + center_text + label_text).properties(
        title='Feasibility Score'
    )

    # Save
    score_chart.save(str(CHART_DIR / 'exec_summary_altair.html'))
    score_chart.save(str(CHART_DIR / 'exec_summary_altair.png'), scale_factor=2)

    return score_chart


# =========================================================================
# PE PORTFOLIO ANALYTICS CHARTS
# =========================================================================

def technology_mix_donut(
    tech_data: Dict[str, float],
    title: str = "Pipeline by Technology"
) -> alt.Chart:
    """
    Donut chart showing technology mix by MW.

    Args:
        tech_data: Dict of technology -> MW
        title: Chart title

    Returns:
        Altair Chart object
    """
    configure_theme()

    # Prepare data
    total_mw = sum(tech_data.values())
    data = []

    for tech, mw in tech_data.items():
        pct = (mw / total_mw) * 100 if total_mw > 0 else 0
        color = TYPE_COLORS.get(tech, COLORS['muted'])
        data.append({
            'technology': tech,
            'mw': mw,
            'percentage': pct,
            'label': f'{tech}: {mw/1000:.0f} GW ({pct:.0f}%)'
        })

    df = pd.DataFrame(data)
    df = df.sort_values('mw', ascending=False)

    # Color scale
    tech_domain = df['technology'].tolist()
    tech_range = [TYPE_COLORS.get(t, COLORS['muted']) for t in tech_domain]

    # Donut chart
    arc = alt.Chart(df).mark_arc(innerRadius=80, outerRadius=140, stroke='white', strokeWidth=2).encode(
        theta=alt.Theta('mw:Q', stack=True),
        color=alt.Color('technology:N',
                       scale=alt.Scale(domain=tech_domain, range=tech_range),
                       title='Technology'),
        tooltip=[
            alt.Tooltip('technology:N', title='Technology'),
            alt.Tooltip('mw:Q', title='MW', format=',.0f'),
            alt.Tooltip('percentage:Q', title='Share', format='.1f'),
        ]
    )

    # Center text showing total
    center = alt.Chart(pd.DataFrame({'text': [f'{total_mw/1000:.0f}'], 'label': ['GW Total']})).mark_text(
        fontSize=28,
        fontWeight='bold',
        color=COLORS['primary']
    ).encode(text='text:N')

    center_label = alt.Chart(pd.DataFrame({'text': ['GW Total']})).mark_text(
        fontSize=12,
        dy=22,
        color=COLORS['muted']
    ).encode(text='text:N')

    chart = (arc + center + center_label).properties(
        width=400,
        height=400,
        title=title
    )

    # Save
    chart.save(str(CHART_DIR / 'tech_mix_donut.html'))
    chart.save(str(CHART_DIR / 'tech_mix_donut.png'), scale_factor=2)

    return chart


def completion_by_technology_bars(
    tech_data: Dict[str, Dict[str, Any]],
    title: str = "Completion Probability by Technology"
) -> alt.Chart:
    """
    Horizontal bar chart showing completion probability by technology.

    Args:
        tech_data: Dict of technology -> {probability, total_mw, expected_mw}
        title: Chart title

    Returns:
        Altair Chart object
    """
    configure_theme()

    data = []
    for tech, stats in tech_data.items():
        prob = stats.get('probability', 0) * 100
        total_mw = stats.get('total_mw', 0)
        expected_mw = stats.get('expected_mw', 0)
        data.append({
            'technology': tech,
            'probability': prob,
            'total_mw': total_mw,
            'expected_mw': expected_mw,
            'label': f'{prob:.0f}%'
        })

    df = pd.DataFrame(data)
    df = df.sort_values('probability', ascending=True)

    # Color based on probability
    def prob_color(p):
        if p >= 30:
            return COLORS['success']
        elif p >= 20:
            return COLORS['warning']
        else:
            return COLORS['danger']

    df['color'] = df['probability'].apply(prob_color)

    bars = alt.Chart(df).mark_bar(cornerRadiusEnd=4, height=25).encode(
        x=alt.X('probability:Q', title='Completion Probability (%)', scale=alt.Scale(domain=[0, 50])),
        y=alt.Y('technology:N', title=None, sort=alt.EncodingSortField(field='probability', order='ascending')),
        color=alt.Color('color:N', scale=None),
        tooltip=[
            alt.Tooltip('technology:N', title='Technology'),
            alt.Tooltip('probability:Q', title='Probability', format='.0f'),
            alt.Tooltip('total_mw:Q', title='Nominal MW', format=',.0f'),
            alt.Tooltip('expected_mw:Q', title='Expected MW', format=',.0f'),
        ]
    )

    text = alt.Chart(df).mark_text(align='left', dx=5, fontSize=11, fontWeight='bold').encode(
        x='probability:Q',
        y=alt.Y('technology:N', sort=alt.EncodingSortField(field='probability', order='ascending')),
        text='label:N',
        color=alt.value('#374151')
    )

    chart = (bars + text).properties(
        width=500,
        height=alt.Step(40),
        title=title
    )

    chart.save(str(CHART_DIR / 'completion_by_tech.html'))
    chart.save(str(CHART_DIR / 'completion_by_tech.png'), scale_factor=2)

    return chart


def developer_market_share_bars(
    dev_data: Dict[str, Dict[str, Any]],
    title: str = "Developer Market Share (Top 15)"
) -> alt.Chart:
    """
    Horizontal bar chart showing developer market share.

    Args:
        dev_data: Dict of developer -> {mw, market_share, project_count}
        title: Chart title

    Returns:
        Altair Chart object
    """
    configure_theme()

    data = []
    for dev, stats in dev_data.items():
        mw = stats.get('mw', 0)
        share = stats.get('market_share', 0) * 100
        count = stats.get('project_count', 0)

        # Truncate long developer names
        display_name = dev[:30] + '...' if len(dev) > 30 else dev

        data.append({
            'developer': display_name,
            'full_name': dev,
            'mw': mw,
            'share': share,
            'project_count': count,
        })

    df = pd.DataFrame(data)
    df = df.sort_values('mw', ascending=True)

    bars = alt.Chart(df).mark_bar(cornerRadiusEnd=4, height=20, color=COLORS['primary']).encode(
        x=alt.X('mw:Q', title='Capacity (MW)'),
        y=alt.Y('developer:N', title=None, sort=alt.EncodingSortField(field='mw', order='ascending')),
        tooltip=[
            alt.Tooltip('full_name:N', title='Developer'),
            alt.Tooltip('mw:Q', title='MW', format=',.0f'),
            alt.Tooltip('share:Q', title='Market Share', format='.1f'),
            alt.Tooltip('project_count:Q', title='Projects', format=','),
        ]
    )

    # Share labels
    text = alt.Chart(df).mark_text(align='left', dx=5, fontSize=10).encode(
        x='mw:Q',
        y=alt.Y('developer:N', sort=alt.EncodingSortField(field='mw', order='ascending')),
        text=alt.Text('share:Q', format='.1f'),
        color=alt.value(COLORS['muted'])
    )

    chart = (bars + text).properties(
        width=550,
        height=alt.Step(28),
        title=title
    )

    chart.save(str(CHART_DIR / 'developer_share.html'))
    chart.save(str(CHART_DIR / 'developer_share.png'), scale_factor=2)

    return chart


def expected_vs_nominal_mw(
    tech_data: Dict[str, Dict[str, Any]],
    title: str = "Nominal vs Expected MW by Technology"
) -> alt.Chart:
    """
    Grouped bar chart comparing nominal vs expected MW.

    Args:
        tech_data: Dict of technology -> {total_mw, expected_mw}
        title: Chart title

    Returns:
        Altair Chart object
    """
    configure_theme()

    data = []
    for tech, stats in tech_data.items():
        total_mw = stats.get('total_mw', stats.get('mw', 0))
        expected_mw = stats.get('expected_mw', 0)

        data.append({'technology': tech, 'type': 'Nominal', 'mw': total_mw})
        data.append({'technology': tech, 'type': 'Expected', 'mw': expected_mw})

    df = pd.DataFrame(data)

    # Sort by nominal MW
    tech_order = df[df['type'] == 'Nominal'].sort_values('mw', ascending=False)['technology'].tolist()

    chart = alt.Chart(df).mark_bar(cornerRadiusEnd=3).encode(
        x=alt.X('technology:N', title=None, sort=tech_order),
        y=alt.Y('mw:Q', title='Capacity (MW)'),
        color=alt.Color('type:N',
                       scale=alt.Scale(domain=['Nominal', 'Expected'],
                                      range=[COLORS['primary'], COLORS['success']]),
                       title=''),
        xOffset='type:N',
        tooltip=[
            alt.Tooltip('technology:N', title='Technology'),
            alt.Tooltip('type:N', title='Type'),
            alt.Tooltip('mw:Q', title='MW', format=',.0f'),
        ]
    ).properties(
        width=500,
        height=350,
        title=title
    )

    chart.save(str(CHART_DIR / 'expected_vs_nominal.html'))
    chart.save(str(CHART_DIR / 'expected_vs_nominal.png'), scale_factor=2)

    return chart


def time_in_queue_histogram(
    buckets: Dict[str, int],
    title: str = "Time in Queue Distribution"
) -> alt.Chart:
    """
    Histogram showing time-in-queue distribution.

    Args:
        buckets: Dict of bucket name -> project count
        title: Chart title

    Returns:
        Altair Chart object
    """
    configure_theme()

    data = [{'bucket': k, 'count': v} for k, v in buckets.items()]
    df = pd.DataFrame(data)

    # Sort by bucket order
    bucket_order = ['0-12 months', '12-24 months', '24-36 months', '36-48 months', '48-60 months', '60+ months']
    df['order'] = df['bucket'].apply(lambda x: bucket_order.index(x) if x in bucket_order else 99)
    df = df.sort_values('order')

    chart = alt.Chart(df).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color=COLORS['primary']).encode(
        x=alt.X('bucket:N', title='Time in Queue', sort=bucket_order),
        y=alt.Y('count:Q', title='Number of Projects'),
        tooltip=[
            alt.Tooltip('bucket:N', title='Duration'),
            alt.Tooltip('count:Q', title='Projects', format=','),
        ]
    ).properties(
        width=500,
        height=300,
        title=title
    )

    # Add value labels
    text = alt.Chart(df).mark_text(dy=-10, fontSize=11, fontWeight='bold').encode(
        x=alt.X('bucket:N', sort=bucket_order),
        y='count:Q',
        text=alt.Text('count:Q', format=','),
        color=alt.value('#374151')
    )

    combined = (chart + text)

    combined.save(str(CHART_DIR / 'time_in_queue_hist.html'))
    combined.save(str(CHART_DIR / 'time_in_queue_hist.png'), scale_factor=2)

    return combined


def queue_vintage_trend(
    vintage_data: Dict[int, Dict[str, Any]],
    title: str = "Queue Growth by Vintage Year"
) -> alt.Chart:
    """
    Line chart showing queue growth over time.

    Args:
        vintage_data: Dict of year -> {mw, project_count}
        title: Chart title

    Returns:
        Altair Chart object
    """
    configure_theme()

    data = []
    for year, stats in vintage_data.items():
        mw = stats.get('mw', 0)
        count = stats.get('project_count', 0)
        data.append({
            'year': int(year),
            'mw': mw,
            'gw': mw / 1000,
            'project_count': count,
        })

    df = pd.DataFrame(data)
    df = df.sort_values('year')

    # MW line
    line = alt.Chart(df).mark_line(color=COLORS['primary'], strokeWidth=3).encode(
        x=alt.X('year:O', title='Queue Entry Year'),
        y=alt.Y('gw:Q', title='Capacity (GW)'),
    )

    # Points
    points = alt.Chart(df).mark_circle(color=COLORS['primary'], size=80).encode(
        x='year:O',
        y='gw:Q',
        tooltip=[
            alt.Tooltip('year:O', title='Year'),
            alt.Tooltip('gw:Q', title='GW', format=',.1f'),
            alt.Tooltip('project_count:Q', title='Projects', format=','),
        ]
    )

    chart = (line + points).properties(
        width=600,
        height=350,
        title=title
    )

    chart.save(str(CHART_DIR / 'queue_vintage_trend.html'))
    chart.save(str(CHART_DIR / 'queue_vintage_trend.png'), scale_factor=2)

    return chart


def regional_breakdown_bars(
    regional_data: Dict[str, Dict[str, Any]],
    title: str = "Pipeline by Region"
) -> alt.Chart:
    """
    Horizontal bar chart showing regional pipeline breakdown.

    Args:
        regional_data: Dict of region -> {mw, project_count}
        title: Chart title

    Returns:
        Altair Chart object
    """
    configure_theme()

    data = []
    for region, stats in regional_data.items():
        mw = stats.get('mw', 0)
        count = stats.get('project_count', 0)
        data.append({
            'region': region,
            'mw': mw,
            'gw': mw / 1000,
            'project_count': count,
        })

    df = pd.DataFrame(data)
    df = df.sort_values('mw', ascending=True)

    bars = alt.Chart(df).mark_bar(cornerRadiusEnd=4, height=30, color=COLORS['primary']).encode(
        x=alt.X('gw:Q', title='Capacity (GW)'),
        y=alt.Y('region:N', title=None, sort=alt.EncodingSortField(field='mw', order='ascending')),
        tooltip=[
            alt.Tooltip('region:N', title='Region'),
            alt.Tooltip('gw:Q', title='GW', format=',.1f'),
            alt.Tooltip('project_count:Q', title='Projects', format=','),
        ]
    )

    # Labels
    text = alt.Chart(df).mark_text(align='left', dx=5, fontSize=11, fontWeight='bold').encode(
        x='gw:Q',
        y=alt.Y('region:N', sort=alt.EncodingSortField(field='mw', order='ascending')),
        text=alt.Text('gw:Q', format='.0f'),
        color=alt.value('#374151')
    )

    chart = (bars + text).properties(
        width=500,
        height=alt.Step(45),
        title=title
    )

    chart.save(str(CHART_DIR / 'regional_breakdown.html'))
    chart.save(str(CHART_DIR / 'regional_breakdown.png'), scale_factor=2)

    return chart


def ic_cost_benchmark_chart(
    cost_data: Dict[str, Dict[str, Any]],
    title: str = "Interconnection Cost Benchmarks by Region"
) -> alt.Chart:
    """
    Box-like chart showing IC cost ranges by region.

    Args:
        cost_data: Dict of region -> {p25, p50, p75}
        title: Chart title

    Returns:
        Altair Chart object
    """
    configure_theme()

    data = []
    for region, costs in cost_data.items():
        data.append({
            'region': region,
            'p25': costs.get('p25', 0),
            'p50': costs.get('p50', 0),
            'p75': costs.get('p75', 0),
        })

    df = pd.DataFrame(data)
    df = df.sort_values('p50', ascending=True)

    # Range bars (p25 to p75)
    range_bars = alt.Chart(df).mark_bar(color=COLORS['primary'], opacity=0.3).encode(
        x=alt.X('region:N', title=None, sort=alt.EncodingSortField(field='p50', order='ascending')),
        y=alt.Y('p25:Q', title='Interconnection Cost ($/kW)'),
        y2='p75:Q',
    )

    # Median line
    median = alt.Chart(df).mark_tick(color=COLORS['primary'], thickness=3, size=30).encode(
        x=alt.X('region:N', sort=alt.EncodingSortField(field='p50', order='ascending')),
        y='p50:Q',
        tooltip=[
            alt.Tooltip('region:N', title='Region'),
            alt.Tooltip('p25:Q', title='P25 (Low)', format='$,.0f'),
            alt.Tooltip('p50:Q', title='P50 (Median)', format='$,.0f'),
            alt.Tooltip('p75:Q', title='P75 (High)', format='$,.0f'),
        ]
    )

    # P50 value labels
    text = alt.Chart(df).mark_text(dy=-15, fontSize=10, fontWeight='bold').encode(
        x=alt.X('region:N', sort=alt.EncodingSortField(field='p50', order='ascending')),
        y='p75:Q',
        text=alt.Text('p50:Q', format='$,.0f'),
        color=alt.value('#374151')
    )

    chart = (range_bars + median + text).properties(
        width=500,
        height=350,
        title=title
    )

    chart.save(str(CHART_DIR / 'ic_cost_benchmarks.html'))
    chart.save(str(CHART_DIR / 'ic_cost_benchmarks.png'), scale_factor=2)

    return chart


def pipeline_funnel(
    phase_data: Dict[str, Dict[str, Any]],
    title: str = "Pipeline by Study Phase"
) -> alt.Chart:
    """
    Funnel-style chart showing projects by study phase.

    Args:
        phase_data: Dict of phase -> {project_count, total_mw, probability}
        title: Chart title

    Returns:
        Altair Chart object
    """
    configure_theme()

    # Define phase order (early to late)
    phase_order = [
        'Pending', 'Feasibility Study', 'System Impact Study',
        'Facilities Study', 'IA Executed', 'Under Construction', 'Active'
    ]

    data = []
    for phase, stats in phase_data.items():
        mw = stats.get('total_mw', 0)
        count = stats.get('project_count', 0)
        prob = stats.get('probability', 0) * 100

        # Determine order
        order = phase_order.index(phase) if phase in phase_order else 99

        data.append({
            'phase': phase,
            'mw': mw,
            'gw': mw / 1000,
            'project_count': count,
            'probability': prob,
            'order': order,
        })

    df = pd.DataFrame(data)
    df = df.sort_values('order')

    # Color by probability
    def get_color(prob):
        if prob >= 50:
            return COLORS['success']
        elif prob >= 25:
            return COLORS['warning']
        else:
            return COLORS['danger']

    df['color'] = df['probability'].apply(get_color)

    bars = alt.Chart(df).mark_bar(cornerRadiusEnd=4).encode(
        y=alt.Y('phase:N', title=None, sort=alt.SortField('order')),
        x=alt.X('gw:Q', title='Capacity (GW)'),
        color=alt.Color('color:N', scale=None),
        tooltip=[
            alt.Tooltip('phase:N', title='Phase'),
            alt.Tooltip('gw:Q', title='GW', format=',.1f'),
            alt.Tooltip('project_count:Q', title='Projects', format=','),
            alt.Tooltip('probability:Q', title='Completion Rate', format='.0f'),
        ]
    )

    # Probability labels
    text = alt.Chart(df).mark_text(align='left', dx=5, fontSize=10).encode(
        y=alt.Y('phase:N', sort=alt.SortField('order')),
        x='gw:Q',
        text=alt.Text('probability:Q', format='.0f'),
        color=alt.value(COLORS['muted'])
    )

    chart = (bars + text).properties(
        width=500,
        height=alt.Step(40),
        title=title
    )

    chart.save(str(CHART_DIR / 'pipeline_funnel.html'))
    chart.save(str(CHART_DIR / 'pipeline_funnel.png'), scale_factor=2)

    return chart


# Demo / test
if __name__ == "__main__":
    print("Generating Altair chart samples...")

    # Sample data
    np.random.seed(42)

    # 1. Cost scatter
    hist_df = pd.DataFrame({
        'capacity_mw': np.random.exponential(200, 80) + 50,
        'cost_per_kw': np.random.normal(150, 50, 80).clip(50, 350),
        'type': np.random.choice(['Solar', 'Wind', 'Storage', 'Hybrid'], 80)
    })
    this_proj = {'capacity_mw': 500, 'cost_low': 100, 'cost_median': 150, 'cost_high': 220}

    print("  Creating cost scatter...")
    cost_scatter(hist_df, this_proj)

    # 2. Risk bars
    scores = {
        'queue_position': 25,
        'study_progress': 5,
        'developer_track_record': 14,
        'poi_congestion': 15,
        'project_characteristics': 9,
    }
    max_scores = {
        'queue_position': 25,
        'study_progress': 25,
        'developer_track_record': 20,
        'poi_congestion': 15,
        'project_characteristics': 15,
    }

    print("  Creating risk bars...")
    risk_bars(scores, max_scores)

    # 3. Queue outcomes
    outcomes = {'Withdrawn': 50, 'Active': 40, 'Completed': 10}

    print("  Creating queue outcomes...")
    queue_outcomes(outcomes, 'Active')

    # 4. Completion rates
    rates = {
        'Major Utility': 85,
        'IPP (Large)': 72,
        'IPP (Mid-size)': 58,
        'Developer': 52,
        'Community Solar': 38,
    }

    print("  Creating completion rates...")
    completion_rate_bars(rates, 'IPP (Mid-size)')

    print(f"\nCharts saved to: {CHART_DIR}")
    print("Done!")
