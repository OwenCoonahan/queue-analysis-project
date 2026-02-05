#!/usr/bin/env python3
"""
Chart Generation Module for Interconnection Queue Analysis

Generates both static (matplotlib/seaborn) and interactive (plotly)
visualizations for feasibility reports.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

# Try to import plotly for interactive charts
try:
    import plotly.graph_objects as go
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

# Try to import seaborn for enhanced styling
try:
    import seaborn as sns
    SEABORN_AVAILABLE = True
except ImportError:
    SEABORN_AVAILABLE = False

# Chart output directory
CHART_DIR = Path(__file__).parent / 'charts'
CHART_DIR.mkdir(exist_ok=True)

# Color scheme - improved with better contrast and distinction
COLORS = {
    # Project types
    'solar': '#F59E0B',      # Amber
    'wind': '#0EA5E9',       # Sky blue
    'storage': '#10B981',    # Emerald
    'gas': '#6B7280',        # Gray
    'load': '#EF4444',       # Red
    'hybrid': '#8B5CF6',     # Violet
    # Chart elements
    'this_project': '#DC2626',   # Red-600
    'benchmark': '#94A3B8',      # Slate-400
    'completed': '#22C55E',      # Green-500
    'withdrawn': '#EF4444',      # Red-500
    'active': '#3B82F6',         # Blue-500
    'pending': '#F59E0B',        # Amber-500
    # Risk levels
    'risk_low': '#22C55E',       # Green
    'risk_medium': '#F59E0B',    # Amber
    'risk_high': '#EF4444',      # Red
    # Background/grid
    'grid': '#E2E8F0',           # Slate-200
    'background': '#F8FAFC',     # Slate-50
}

# Default figure style - cleaner, more professional
FIGURE_STYLE = {
    'figure.figsize': (10, 6),
    'figure.facecolor': '#FFFFFF',
    'axes.facecolor': '#FFFFFF',
    'font.family': 'sans-serif',
    'font.size': 11,
    'axes.titlesize': 14,
    'axes.titleweight': 'bold',
    'axes.labelsize': 11,
    'axes.spines.top': False,
    'axes.spines.right': False,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'legend.fontsize': 10,
    'legend.frameon': False,
}


def _apply_style():
    """Apply consistent matplotlib style."""
    plt.rcParams.update(FIGURE_STYLE)
    if SEABORN_AVAILABLE:
        sns.set_style("whitegrid")


def _close_figure(fig: plt.Figure) -> None:
    """Safely close a matplotlib figure to prevent memory leaks."""
    if fig is not None:
        plt.close(fig)


def _get_project_color(project_type: str) -> str:
    """Get color for a project type."""
    type_lower = str(project_type).lower()
    if 'solar' in type_lower or type_lower in ['s', 'pv']:
        return COLORS['solar']
    elif 'wind' in type_lower or type_lower in ['w', 'osw']:
        return COLORS['wind']
    elif any(kw in type_lower for kw in ['storage', 'battery', 'bess', 'es']):
        return COLORS['storage']
    elif any(kw in type_lower for kw in ['gas', 'ng', 'cc', 'ct', 'peaker']):
        return COLORS['gas']
    elif 'load' in type_lower:
        return COLORS['load']
    elif 'hybrid' in type_lower:
        return COLORS['hybrid']
    return COLORS['benchmark']


class ChartGenerator:
    """Generate charts for feasibility reports."""

    def __init__(self, output_dir: Path = None):
        """
        Initialize chart generator.

        Args:
            output_dir: Directory to save charts. Defaults to module's charts directory.
        """
        self.output_dir = Path(output_dir) if output_dir else CHART_DIR
        self.output_dir.mkdir(exist_ok=True)
        _apply_style()

    def cost_scatter(
        self,
        historical_df: pd.DataFrame,
        this_project: Dict,
        title: str = "Interconnection Cost Comparison",
        save_static: bool = True,
        save_interactive: bool = True
    ) -> Tuple[Optional[plt.Figure], Optional[object]]:
        """
        Generate cost vs size scatter plot.

        Args:
            historical_df: DataFrame with columns [capacity_mw, cost_per_kw, type, status]
            this_project: Dict with keys [capacity_mw, cost_low, cost_median, cost_high, type]
            title: Chart title
            save_static: Whether to save static matplotlib figure
            save_interactive: Whether to save interactive plotly figure

        Returns:
            Tuple of (matplotlib Figure, plotly Figure) - either may be None
        """
        static_fig = None
        interactive_fig = None

        # Validate inputs
        if historical_df is None or historical_df.empty:
            print("Warning: No historical data provided for cost scatter plot")
            return None, None

        if this_project is None:
            print("Warning: No project data provided for cost scatter plot")
            return None, None

        required_cols = ['capacity_mw', 'cost_per_kw']
        if not all(col in historical_df.columns for col in required_cols):
            print(f"Warning: Historical data missing required columns: {required_cols}")
            return None, None

        try:
            # Clean data
            df = historical_df.dropna(subset=['capacity_mw', 'cost_per_kw']).copy()
            df['capacity_mw'] = pd.to_numeric(df['capacity_mw'], errors='coerce')
            df['cost_per_kw'] = pd.to_numeric(df['cost_per_kw'], errors='coerce')
            df = df.dropna(subset=['capacity_mw', 'cost_per_kw'])

            if df.empty:
                print("Warning: No valid data points for cost scatter plot")
                return None, None

            # Get colors based on type
            if 'type' in df.columns:
                df['color'] = df['type'].apply(_get_project_color)
            else:
                df['color'] = COLORS['benchmark']

            # Static matplotlib figure
            static_fig, ax = plt.subplots(figsize=(10, 7))

            # Plot historical data
            scatter = ax.scatter(
                df['capacity_mw'],
                df['cost_per_kw'],
                c=df['color'],
                alpha=0.5,
                s=50,
                edgecolors='white',
                linewidths=0.5,
                label='Historical Projects'
            )

            # Plot this project
            proj_capacity = this_project.get('capacity_mw', 0)
            proj_cost_low = this_project.get('cost_low', 0)
            proj_cost_median = this_project.get('cost_median', 0)
            proj_cost_high = this_project.get('cost_high', 0)

            if proj_capacity > 0 and proj_cost_median > 0:
                # Plot range
                ax.vlines(
                    proj_capacity, proj_cost_low, proj_cost_high,
                    colors=COLORS['this_project'], linewidths=3, alpha=0.7,
                    label='Estimated Range'
                )
                # Plot median
                ax.scatter(
                    [proj_capacity], [proj_cost_median],
                    c=COLORS['this_project'], s=200, marker='*',
                    edgecolors='black', linewidths=1, zorder=10,
                    label='This Project'
                )

            # Add trend line
            if len(df) >= 5:
                try:
                    z = np.polyfit(df['capacity_mw'], df['cost_per_kw'], 1)
                    p = np.poly1d(z)
                    x_line = np.linspace(df['capacity_mw'].min(), df['capacity_mw'].max(), 100)
                    ax.plot(x_line, p(x_line), '--', color='gray', alpha=0.5, label='Trend')
                except Exception:
                    pass

            ax.set_xlabel('Capacity (MW)')
            ax.set_ylabel('Interconnection Cost ($/kW)')
            ax.set_title(title)
            ax.legend(loc='upper right')
            ax.set_xlim(left=0)
            ax.set_ylim(bottom=0)

            plt.tight_layout()

            if save_static:
                static_path = self.output_dir / 'cost_scatter.png'
                static_fig.savefig(static_path, dpi=150, bbox_inches='tight')

            # Interactive plotly figure
            if PLOTLY_AVAILABLE and save_interactive:
                interactive_fig = go.Figure()

                # Historical data by type
                if 'type' in df.columns:
                    for proj_type in df['type'].unique():
                        mask = df['type'] == proj_type
                        subset = df[mask]
                        interactive_fig.add_trace(go.Scatter(
                            x=subset['capacity_mw'],
                            y=subset['cost_per_kw'],
                            mode='markers',
                            name=str(proj_type),
                            marker=dict(
                                color=_get_project_color(proj_type),
                                size=8,
                                opacity=0.6
                            ),
                            hovertemplate='%{text}<br>Capacity: %{x:.0f} MW<br>Cost: $%{y:.0f}/kW',
                            text=subset.get('name', [''] * len(subset))
                        ))
                else:
                    interactive_fig.add_trace(go.Scatter(
                        x=df['capacity_mw'],
                        y=df['cost_per_kw'],
                        mode='markers',
                        name='Historical',
                        marker=dict(color=COLORS['benchmark'], size=8, opacity=0.6)
                    ))

                # This project
                if proj_capacity > 0 and proj_cost_median > 0:
                    interactive_fig.add_trace(go.Scatter(
                        x=[proj_capacity, proj_capacity],
                        y=[proj_cost_low, proj_cost_high],
                        mode='lines',
                        name='Estimated Range',
                        line=dict(color=COLORS['this_project'], width=4)
                    ))
                    interactive_fig.add_trace(go.Scatter(
                        x=[proj_capacity],
                        y=[proj_cost_median],
                        mode='markers',
                        name='This Project',
                        marker=dict(
                            color=COLORS['this_project'],
                            size=20,
                            symbol='star'
                        )
                    ))

                interactive_fig.update_layout(
                    title=title,
                    xaxis_title='Capacity (MW)',
                    yaxis_title='Interconnection Cost ($/kW)',
                    hovermode='closest'
                )

                interactive_path = self.output_dir / 'cost_scatter.html'
                interactive_fig.write_html(str(interactive_path))

        except Exception as e:
            print(f"Error generating cost scatter plot: {e}")
            if static_fig is not None:
                _close_figure(static_fig)
            return None, None

        return static_fig, interactive_fig

    def completion_funnel(
        self,
        stage_counts: Dict[str, int],
        this_project_stage: str = None,
        title: str = "Project Completion Funnel"
    ) -> Tuple[Optional[plt.Figure], Optional[object]]:
        """
        Generate completion funnel showing attrition through stages.

        Args:
            stage_counts: Dict mapping stage names to project counts
            this_project_stage: Current stage of the project being analyzed
            title: Chart title

        Returns:
            Tuple of (matplotlib Figure, plotly Figure) - either may be None
        """
        static_fig = None
        interactive_fig = None

        if not stage_counts:
            print("Warning: No stage data provided for completion funnel")
            return None, None

        try:
            # Standard stage order
            standard_stages = [
                'Queue Entry',
                'Feasibility Study',
                'System Impact Study',
                'Facilities Study',
                'IA Negotiation',
                'IA Executed',
                'Under Construction',
                'Commercial Operation'
            ]

            # Match provided stages to standard or use as-is
            stages = []
            counts = []
            for stage in standard_stages:
                for key, count in stage_counts.items():
                    if stage.lower() in key.lower() or key.lower() in stage.lower():
                        stages.append(stage)
                        counts.append(count)
                        break

            # If few matches, use provided order directly
            if len(stages) < len(stage_counts) // 2:
                stages = list(stage_counts.keys())
                counts = list(stage_counts.values())

            if not stages:
                print("Warning: Could not parse stage data")
                return None, None

            # Calculate percentages (relative to first stage)
            total = counts[0] if counts[0] > 0 else 1
            percentages = [c / total * 100 for c in counts]

            # Colors for funnel (gradient from blue to green)
            n_stages = len(stages)
            funnel_colors = plt.cm.Blues(np.linspace(0.3, 0.9, n_stages))

            # Highlight this project's stage
            highlight_idx = None
            if this_project_stage:
                for i, stage in enumerate(stages):
                    if this_project_stage.lower() in stage.lower() or stage.lower() in this_project_stage.lower():
                        highlight_idx = i
                        break

            # Static matplotlib figure
            static_fig, ax = plt.subplots(figsize=(10, 8))

            # Create funnel as horizontal bars
            y_positions = range(len(stages) - 1, -1, -1)
            bars = ax.barh(
                y_positions, counts,
                color=[COLORS['this_project'] if i == highlight_idx else funnel_colors[i]
                       for i in range(n_stages)],
                edgecolor='white',
                linewidth=2
            )

            # Add count and percentage labels
            for i, (bar, count, pct) in enumerate(zip(bars, counts, percentages)):
                width = bar.get_width()
                label = f'{count:,} ({pct:.0f}%)'
                ax.text(
                    width + max(counts) * 0.02, bar.get_y() + bar.get_height() / 2,
                    label, va='center', ha='left', fontsize=9
                )

                # Add attrition rate between stages
                if i < len(counts) - 1:
                    attrition = counts[i] - counts[i + 1]
                    attrition_pct = (attrition / counts[i] * 100) if counts[i] > 0 else 0
                    if attrition_pct > 0:
                        ax.annotate(
                            f'-{attrition_pct:.0f}%',
                            xy=(width / 2, bar.get_y()),
                            fontsize=8, color='red', alpha=0.7,
                            ha='center', va='top'
                        )

            ax.set_yticks(y_positions)
            ax.set_yticklabels(stages)
            ax.set_xlabel('Number of Projects')
            ax.set_title(title)
            ax.set_xlim(right=max(counts) * 1.3)

            # Add legend if this project is highlighted
            if highlight_idx is not None:
                legend_patch = mpatches.Patch(color=COLORS['this_project'], label='Current Stage')
                ax.legend(handles=[legend_patch], loc='lower right')

            plt.tight_layout()

            funnel_path = self.output_dir / 'completion_funnel.png'
            static_fig.savefig(funnel_path, dpi=150, bbox_inches='tight')

            # Interactive plotly funnel
            if PLOTLY_AVAILABLE:
                interactive_fig = go.Figure(go.Funnel(
                    y=stages,
                    x=counts,
                    textposition='inside',
                    textinfo='value+percent initial',
                    marker=dict(
                        color=[COLORS['this_project'] if i == highlight_idx else '#3498DB'
                               for i in range(n_stages)]
                    )
                ))

                interactive_fig.update_layout(
                    title=title,
                    funnelmode='stack'
                )

                interactive_path = self.output_dir / 'completion_funnel.html'
                interactive_fig.write_html(str(interactive_path))

        except Exception as e:
            print(f"Error generating completion funnel: {e}")
            if static_fig is not None:
                _close_figure(static_fig)
            return None, None

        return static_fig, interactive_fig

    def queue_outcomes_donut(
        self,
        outcomes: Dict[str, int],
        this_project_status: str = None,
        title: str = "Queue Outcomes"
    ) -> Tuple[Optional[plt.Figure], Optional[object]]:
        """
        Generate a donut chart showing queue outcome distribution.

        More intuitive than funnel for showing final status breakdown.

        Args:
            outcomes: Dict mapping status to count (e.g., {'Active': 40, 'Withdrawn': 50, 'Completed': 10})
            this_project_status: Current status of project being analyzed
            title: Chart title

        Returns:
            Tuple of (matplotlib Figure, plotly Figure)
        """
        static_fig = None
        interactive_fig = None

        if not outcomes:
            print("Warning: No outcome data provided")
            return None, None

        try:
            # Sort by count descending
            sorted_outcomes = sorted(outcomes.items(), key=lambda x: x[1], reverse=True)
            labels = [x[0] for x in sorted_outcomes]
            values = [x[1] for x in sorted_outcomes]
            total = sum(values)

            # Assign colors based on status type
            colors = []
            explode = []
            for label in labels:
                label_lower = label.lower()
                if 'complete' in label_lower or 'operational' in label_lower:
                    colors.append(COLORS['completed'])
                elif 'withdraw' in label_lower or 'cancel' in label_lower:
                    colors.append(COLORS['withdrawn'])
                elif 'active' in label_lower or 'pending' in label_lower:
                    colors.append(COLORS['active'])
                else:
                    colors.append(COLORS['benchmark'])

                # Explode this project's status
                if this_project_status and this_project_status.lower() in label_lower:
                    explode.append(0.05)
                else:
                    explode.append(0)

            # Static matplotlib figure
            static_fig, ax = plt.subplots(figsize=(10, 8))

            wedges, texts, autotexts = ax.pie(
                values,
                labels=None,
                colors=colors,
                explode=explode,
                autopct=lambda p: f'{p:.0f}%' if p > 5 else '',
                startangle=90,
                pctdistance=0.75,
                wedgeprops=dict(width=0.5, edgecolor='white', linewidth=2)
            )

            # Style autopct labels
            for autotext in autotexts:
                autotext.set_fontsize(11)
                autotext.set_fontweight('bold')

            # Add legend
            legend_labels = [f'{l}: {v:,} ({v/total*100:.0f}%)' for l, v in zip(labels, values)]
            ax.legend(
                wedges, legend_labels,
                title="Status",
                loc="center left",
                bbox_to_anchor=(1, 0, 0.5, 1),
                fontsize=10
            )

            # Center text showing total
            ax.text(0, 0, f'{total:,}\nTotal', ha='center', va='center', fontsize=14, fontweight='bold')

            ax.set_title(title, fontsize=14, fontweight='bold', pad=20)

            plt.tight_layout()

            donut_path = self.output_dir / 'queue_outcomes.png'
            static_fig.savefig(donut_path, dpi=150, bbox_inches='tight')

            # Interactive plotly figure
            if PLOTLY_AVAILABLE:
                interactive_fig = go.Figure(data=[go.Pie(
                    labels=labels,
                    values=values,
                    hole=0.5,
                    marker=dict(colors=colors, line=dict(color='white', width=2)),
                    textinfo='percent+label',
                    textposition='outside',
                    pull=explode
                )])

                interactive_fig.update_layout(
                    title=dict(text=title, font=dict(size=16)),
                    annotations=[dict(
                        text=f'{total:,}<br>Total',
                        x=0.5, y=0.5,
                        font_size=16,
                        showarrow=False
                    )],
                    showlegend=True
                )

                interactive_path = self.output_dir / 'queue_outcomes.html'
                interactive_fig.write_html(str(interactive_path))

        except Exception as e:
            print(f"Error generating queue outcomes donut: {e}")
            if static_fig is not None:
                _close_figure(static_fig)
            return None, None

        return static_fig, interactive_fig

    def timeline_boxplot(
        self,
        historical_df: pd.DataFrame,
        this_project: Dict,
        group_by: str = 'region',
        title: str = "Time to Commercial Operation"
    ) -> Tuple[Optional[plt.Figure], Optional[object]]:
        """
        Generate box plot of time-to-COD distribution.

        Args:
            historical_df: DataFrame with columns [months_to_cod, region, type]
            this_project: Dict with keys [timeline_low, timeline_likely, timeline_high, region]
            group_by: Column to group by ('region' or 'type')
            title: Chart title

        Returns:
            Tuple of (matplotlib Figure, plotly Figure) - either may be None
        """
        static_fig = None
        interactive_fig = None

        if historical_df is None or historical_df.empty:
            print("Warning: No historical data provided for timeline boxplot")
            return None, None

        if 'months_to_cod' not in historical_df.columns:
            print("Warning: Historical data missing 'months_to_cod' column")
            return None, None

        try:
            df = historical_df.dropna(subset=['months_to_cod']).copy()
            df['months_to_cod'] = pd.to_numeric(df['months_to_cod'], errors='coerce')
            df = df.dropna(subset=['months_to_cod'])

            if df.empty:
                print("Warning: No valid data points for timeline boxplot")
                return None, None

            # Group data
            if group_by in df.columns:
                groups = df[group_by].unique()
                data_by_group = [df[df[group_by] == g]['months_to_cod'].values for g in groups]
                group_labels = [str(g) for g in groups]
            else:
                data_by_group = [df['months_to_cod'].values]
                group_labels = ['All Projects']

            # Static matplotlib figure
            static_fig, ax = plt.subplots(figsize=(10, 6))

            bp = ax.boxplot(
                data_by_group,
                labels=group_labels,
                patch_artist=True,
                medianprops=dict(color='black', linewidth=2)
            )

            # Color boxes
            for i, patch in enumerate(bp['boxes']):
                patch.set_facecolor(COLORS['benchmark'])
                patch.set_alpha(0.7)

            # Add this project's timeline
            if this_project:
                timeline_low = this_project.get('timeline_low', 0)
                timeline_likely = this_project.get('timeline_likely', 0)
                timeline_high = this_project.get('timeline_high', 0)
                proj_region = this_project.get('region', '')

                # Find the position for this project's region
                x_pos = 0.5
                if proj_region and proj_region in group_labels:
                    x_pos = group_labels.index(proj_region) + 1
                else:
                    x_pos = len(group_labels) / 2 + 0.5

                if timeline_likely > 0:
                    # Plot range
                    ax.vlines(
                        x_pos, timeline_low, timeline_high,
                        colors=COLORS['this_project'], linewidths=4, alpha=0.7
                    )
                    # Plot likely
                    ax.scatter(
                        [x_pos], [timeline_likely],
                        c=COLORS['this_project'], s=150, marker='D',
                        edgecolors='black', linewidths=1, zorder=10,
                        label='This Project'
                    )
                    ax.legend(loc='upper right')

            ax.set_ylabel('Months to COD')
            ax.set_title(title)
            ax.set_ylim(bottom=0)

            plt.tight_layout()

            timeline_path = self.output_dir / 'timeline_boxplot.png'
            static_fig.savefig(timeline_path, dpi=150, bbox_inches='tight')

            # Interactive plotly figure
            if PLOTLY_AVAILABLE:
                interactive_fig = go.Figure()

                for i, (data, label) in enumerate(zip(data_by_group, group_labels)):
                    interactive_fig.add_trace(go.Box(
                        y=data,
                        name=label,
                        marker_color=COLORS['benchmark']
                    ))

                # Add this project marker
                if this_project and this_project.get('timeline_likely', 0) > 0:
                    interactive_fig.add_trace(go.Scatter(
                        x=[proj_region or 'This Project'],
                        y=[this_project.get('timeline_likely')],
                        mode='markers',
                        name='This Project',
                        marker=dict(
                            color=COLORS['this_project'],
                            size=15,
                            symbol='diamond'
                        ),
                        error_y=dict(
                            type='data',
                            symmetric=False,
                            array=[this_project.get('timeline_high', 0) - this_project.get('timeline_likely', 0)],
                            arrayminus=[this_project.get('timeline_likely', 0) - this_project.get('timeline_low', 0)]
                        )
                    ))

                interactive_fig.update_layout(
                    title=title,
                    yaxis_title='Months to COD',
                    showlegend=True
                )

                interactive_path = self.output_dir / 'timeline_boxplot.html'
                interactive_fig.write_html(str(interactive_path))

        except Exception as e:
            print(f"Error generating timeline boxplot: {e}")
            if static_fig is not None:
                _close_figure(static_fig)
            return None, None

        return static_fig, interactive_fig

    def risk_radar(
        self,
        score_breakdown: Dict[str, float],
        max_scores: Dict[str, float],
        benchmark_scores: Dict[str, float] = None,
        title: str = "Risk Profile"
    ) -> Tuple[Optional[plt.Figure], Optional[object]]:
        """
        Generate radar/spider chart of risk components.

        Args:
            score_breakdown: Dict of component scores
            max_scores: Dict of maximum possible scores
            benchmark_scores: Optional dict of benchmark comparison scores
            title: Chart title

        Returns:
            Tuple of (matplotlib Figure, plotly Figure) - either may be None
        """
        static_fig = None
        interactive_fig = None

        if not score_breakdown or not max_scores:
            print("Warning: Missing score data for risk radar")
            return None, None

        try:
            # Normalize scores to 0-1 scale
            categories = list(score_breakdown.keys())
            values = []
            benchmark_values = []

            for cat in categories:
                max_val = max_scores.get(cat, 1)
                if max_val == 0:
                    max_val = 1
                values.append(score_breakdown.get(cat, 0) / max_val)
                if benchmark_scores:
                    benchmark_values.append(benchmark_scores.get(cat, 0) / max_val)

            # Close the radar chart (repeat first value)
            values_closed = values + [values[0]]
            categories_closed = categories + [categories[0]]
            if benchmark_scores:
                benchmark_closed = benchmark_values + [benchmark_values[0]]

            # Calculate angles
            n_cats = len(categories)
            angles = [n / float(n_cats) * 2 * np.pi for n in range(n_cats)]
            angles_closed = angles + [angles[0]]

            # Static matplotlib figure
            static_fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))

            # Plot this project
            ax.plot(angles_closed, values_closed, 'o-', linewidth=2,
                   color=COLORS['this_project'], label='This Project')
            ax.fill(angles_closed, values_closed, alpha=0.25, color=COLORS['this_project'])

            # Plot benchmark if provided
            if benchmark_scores:
                ax.plot(angles_closed, benchmark_closed, 'o-', linewidth=2,
                       color=COLORS['benchmark'], label='Benchmark', linestyle='--')
                ax.fill(angles_closed, benchmark_closed, alpha=0.1, color=COLORS['benchmark'])

            # Format labels - shorten for display
            display_labels = []
            for cat in categories:
                if len(cat) > 15:
                    words = cat.replace('_', ' ').split()
                    if len(words) > 2:
                        cat = ' '.join(words[:2])
                    else:
                        cat = cat[:15]
                display_labels.append(cat.replace('_', '\n').title())

            ax.set_xticks(angles)
            ax.set_xticklabels(display_labels, size=9)
            ax.set_ylim(0, 1)
            ax.set_yticks([0.25, 0.5, 0.75, 1.0])
            ax.set_yticklabels(['25%', '50%', '75%', '100%'], size=8)
            ax.set_title(title, y=1.08, fontsize=12)
            ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.0))

            plt.tight_layout()

            radar_path = self.output_dir / 'risk_radar.png'
            static_fig.savefig(radar_path, dpi=150, bbox_inches='tight')

            # Interactive plotly figure
            if PLOTLY_AVAILABLE:
                interactive_fig = go.Figure()

                interactive_fig.add_trace(go.Scatterpolar(
                    r=values_closed,
                    theta=categories_closed,
                    fill='toself',
                    name='This Project',
                    line_color=COLORS['this_project'],
                    fillcolor=f'rgba(227, 25, 55, 0.25)'
                ))

                if benchmark_scores:
                    interactive_fig.add_trace(go.Scatterpolar(
                        r=benchmark_closed,
                        theta=categories_closed,
                        fill='toself',
                        name='Benchmark',
                        line_color=COLORS['benchmark'],
                        fillcolor=f'rgba(149, 165, 166, 0.1)'
                    ))

                interactive_fig.update_layout(
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            range=[0, 1],
                            tickvals=[0.25, 0.5, 0.75, 1.0],
                            ticktext=['25%', '50%', '75%', '100%']
                        )
                    ),
                    showlegend=True,
                    title=title
                )

                interactive_path = self.output_dir / 'risk_radar.html'
                interactive_fig.write_html(str(interactive_path))

        except Exception as e:
            print(f"Error generating risk radar: {e}")
            if static_fig is not None:
                _close_figure(static_fig)
            return None, None

        return static_fig, interactive_fig

    def risk_bars(
        self,
        score_breakdown: Dict[str, float],
        max_scores: Dict[str, float],
        title: str = "Risk Factor Analysis"
    ) -> Tuple[Optional[plt.Figure], Optional[object]]:
        """
        Generate horizontal bar chart showing risk factors (cleaner than radar).

        Each bar shows the score percentage with color coding:
        - Green (>75%): Low risk
        - Yellow (50-75%): Medium risk
        - Red (<50%): High risk

        Args:
            score_breakdown: Dict of component scores
            max_scores: Dict of maximum possible scores
            title: Chart title

        Returns:
            Tuple of (matplotlib Figure, plotly Figure)
        """
        static_fig = None
        interactive_fig = None

        if not score_breakdown or not max_scores:
            print("Warning: Missing score data for risk bars")
            return None, None

        try:
            # Calculate percentages
            categories = []
            percentages = []
            for cat, score in score_breakdown.items():
                max_val = max_scores.get(cat, 1)
                if max_val == 0:
                    max_val = 1
                pct = (score / max_val) * 100
                # Clean up category names
                display_name = cat.replace('_', ' ').title()
                categories.append(display_name)
                percentages.append(pct)

            # Sort by percentage (worst first for focus)
            sorted_data = sorted(zip(categories, percentages), key=lambda x: x[1])
            categories = [x[0] for x in sorted_data]
            percentages = [x[1] for x in sorted_data]

            # Assign colors based on risk level
            colors = []
            for pct in percentages:
                if pct >= 75:
                    colors.append(COLORS['risk_low'])
                elif pct >= 50:
                    colors.append(COLORS['risk_medium'])
                else:
                    colors.append(COLORS['risk_high'])

            # Static matplotlib figure
            static_fig, ax = plt.subplots(figsize=(10, max(5, len(categories) * 0.8)))

            y_pos = range(len(categories))
            bars = ax.barh(y_pos, percentages, color=colors, edgecolor='white', linewidth=1, height=0.7)

            # Add value labels
            for bar, pct in zip(bars, percentages):
                width = bar.get_width()
                label_x = width + 2 if width < 85 else width - 8
                label_color = 'black' if width < 85 else 'white'
                ax.text(
                    label_x, bar.get_y() + bar.get_height() / 2,
                    f'{pct:.0f}%', va='center', ha='left' if width < 85 else 'right',
                    fontsize=11, fontweight='bold', color=label_color
                )

            ax.set_yticks(y_pos)
            ax.set_yticklabels(categories, fontsize=11)
            ax.set_xlabel('Score (%)', fontsize=11)
            ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
            ax.set_xlim(0, 105)

            # Add risk zone backgrounds
            ax.axvspan(0, 50, alpha=0.05, color=COLORS['risk_high'], zorder=0)
            ax.axvspan(50, 75, alpha=0.05, color=COLORS['risk_medium'], zorder=0)
            ax.axvspan(75, 105, alpha=0.05, color=COLORS['risk_low'], zorder=0)

            # Add risk zone labels at top
            ax.text(25, len(categories) - 0.3, 'High Risk', ha='center', fontsize=9, color=COLORS['risk_high'], alpha=0.7)
            ax.text(62.5, len(categories) - 0.3, 'Medium', ha='center', fontsize=9, color=COLORS['risk_medium'], alpha=0.7)
            ax.text(90, len(categories) - 0.3, 'Low Risk', ha='center', fontsize=9, color=COLORS['risk_low'], alpha=0.7)

            # Remove top/right spines
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)

            plt.tight_layout()

            risk_bar_path = self.output_dir / 'risk_bars.png'
            static_fig.savefig(risk_bar_path, dpi=150, bbox_inches='tight')

            # Interactive plotly figure
            if PLOTLY_AVAILABLE:
                interactive_fig = go.Figure()

                interactive_fig.add_trace(go.Bar(
                    y=categories,
                    x=percentages,
                    orientation='h',
                    marker_color=colors,
                    text=[f'{p:.0f}%' for p in percentages],
                    textposition='outside',
                    textfont=dict(size=12)
                ))

                # Add risk zone shapes
                interactive_fig.add_vrect(x0=0, x1=50, fillcolor=COLORS['risk_high'], opacity=0.05, layer='below')
                interactive_fig.add_vrect(x0=50, x1=75, fillcolor=COLORS['risk_medium'], opacity=0.05, layer='below')
                interactive_fig.add_vrect(x0=75, x1=100, fillcolor=COLORS['risk_low'], opacity=0.05, layer='below')

                interactive_fig.update_layout(
                    title=dict(text=title, font=dict(size=16)),
                    xaxis_title='Score (%)',
                    xaxis_range=[0, 105],
                    showlegend=False,
                    height=max(400, len(categories) * 60)
                )

                interactive_path = self.output_dir / 'risk_bars.html'
                interactive_fig.write_html(str(interactive_path))

        except Exception as e:
            print(f"Error generating risk bars: {e}")
            if static_fig is not None:
                _close_figure(static_fig)
            return None, None

        return static_fig, interactive_fig

    def developer_outcomes_bar(
        self,
        outcomes: Dict[str, float],
        this_developer_category: str,
        title: str = "Completion Rate by Developer Type"
    ) -> Tuple[Optional[plt.Figure], Optional[object]]:
        """
        Generate horizontal bar chart of completion rates by developer type.

        Args:
            outcomes: Dict mapping developer category to completion rate (0-100)
            this_developer_category: Category of the current project's developer
            title: Chart title

        Returns:
            Tuple of (matplotlib Figure, plotly Figure) - either may be None
        """
        static_fig = None
        interactive_fig = None

        if not outcomes:
            print("Warning: No outcome data provided for developer bar chart")
            return None, None

        try:
            # Sort by completion rate
            sorted_outcomes = sorted(outcomes.items(), key=lambda x: x[1], reverse=True)
            categories = [x[0] for x in sorted_outcomes]
            rates = [x[1] for x in sorted_outcomes]

            # Determine colors
            colors = []
            for cat in categories:
                if this_developer_category and (
                    this_developer_category.lower() in cat.lower() or
                    cat.lower() in this_developer_category.lower()
                ):
                    colors.append(COLORS['this_project'])
                else:
                    colors.append(COLORS['benchmark'])

            # Static matplotlib figure
            static_fig, ax = plt.subplots(figsize=(10, max(6, len(categories) * 0.5)))

            y_pos = range(len(categories) - 1, -1, -1)
            bars = ax.barh(y_pos, rates, color=colors, edgecolor='white', linewidth=1)

            # Add value labels
            for bar, rate in zip(bars, rates):
                width = bar.get_width()
                ax.text(
                    width + 1, bar.get_y() + bar.get_height() / 2,
                    f'{rate:.0f}%', va='center', ha='left', fontsize=9
                )

            ax.set_yticks(y_pos)
            ax.set_yticklabels(categories)
            ax.set_xlabel('Completion Rate (%)')
            ax.set_title(title)
            ax.set_xlim(0, 110)

            # Add average line
            avg_rate = np.mean(rates)
            ax.axvline(avg_rate, color='gray', linestyle='--', alpha=0.7, label=f'Average ({avg_rate:.0f}%)')

            # Add legend if this developer is highlighted
            if this_developer_category:
                legend_elements = [
                    mpatches.Patch(color=COLORS['this_project'], label='This Developer'),
                    plt.Line2D([0], [0], color='gray', linestyle='--', label=f'Average ({avg_rate:.0f}%)')
                ]
                ax.legend(handles=legend_elements, loc='lower right')
            else:
                ax.legend(loc='lower right')

            plt.tight_layout()

            bar_path = self.output_dir / 'developer_outcomes.png'
            static_fig.savefig(bar_path, dpi=150, bbox_inches='tight')

            # Interactive plotly figure
            if PLOTLY_AVAILABLE:
                interactive_fig = go.Figure()

                interactive_fig.add_trace(go.Bar(
                    y=categories,
                    x=rates,
                    orientation='h',
                    marker_color=colors,
                    text=[f'{r:.0f}%' for r in rates],
                    textposition='outside'
                ))

                # Add average line
                interactive_fig.add_vline(
                    x=avg_rate,
                    line_dash='dash',
                    line_color='gray',
                    annotation_text=f'Avg: {avg_rate:.0f}%'
                )

                interactive_fig.update_layout(
                    title=title,
                    xaxis_title='Completion Rate (%)',
                    xaxis_range=[0, 110],
                    showlegend=False
                )

                interactive_path = self.output_dir / 'developer_outcomes.html'
                interactive_fig.write_html(str(interactive_path))

        except Exception as e:
            print(f"Error generating developer outcomes bar: {e}")
            if static_fig is not None:
                _close_figure(static_fig)
            return None, None

        return static_fig, interactive_fig

    def save_all_charts(
        self,
        project_id: str,
        historical_data: Any,  # Could be HistoricalData instance or dict
        score_result: Dict,
        cost_estimate: Dict,
        timeline_estimate: Dict,
        developer_info: Dict
    ) -> Dict[str, Path]:
        """
        Generate and save all charts for a project report.

        Args:
            project_id: Unique identifier for the project
            historical_data: Historical comparison data (can be dict with dataframes or object)
            score_result: Scoring results with breakdown
            cost_estimate: Cost estimation results
            timeline_estimate: Timeline estimation results
            developer_info: Developer information and track record

        Returns:
            Dict mapping chart name to file path
        """
        chart_paths = {}

        # Create project-specific output directory
        project_dir = self.output_dir / project_id
        project_dir.mkdir(exist_ok=True)
        original_output = self.output_dir
        self.output_dir = project_dir

        try:
            # 1. Cost Scatter
            try:
                hist_cost_df = None
                if isinstance(historical_data, dict):
                    hist_cost_df = historical_data.get('cost_data')
                elif hasattr(historical_data, 'cost_data'):
                    hist_cost_df = historical_data.cost_data

                if hist_cost_df is not None and not hist_cost_df.empty:
                    this_project = {
                        'capacity_mw': cost_estimate.get('capacity_mw', 0),
                        'cost_low': cost_estimate.get('cost_low', 0),
                        'cost_median': cost_estimate.get('cost_median', 0),
                        'cost_high': cost_estimate.get('cost_high', 0),
                        'type': cost_estimate.get('type', '')
                    }
                    fig, _ = self.cost_scatter(hist_cost_df, this_project)
                    if fig:
                        chart_paths['cost_scatter'] = project_dir / 'cost_scatter.png'
                        _close_figure(fig)
            except Exception as e:
                print(f"Warning: Could not generate cost scatter chart: {e}")

            # 2. Completion Funnel
            try:
                stage_counts = None
                if isinstance(historical_data, dict):
                    stage_counts = historical_data.get('stage_counts')
                elif hasattr(historical_data, 'stage_counts'):
                    stage_counts = historical_data.stage_counts

                if stage_counts:
                    current_stage = score_result.get('project', {}).get('status', '')
                    fig, _ = self.completion_funnel(stage_counts, current_stage)
                    if fig:
                        chart_paths['completion_funnel'] = project_dir / 'completion_funnel.png'
                        _close_figure(fig)
            except Exception as e:
                print(f"Warning: Could not generate completion funnel chart: {e}")

            # 3. Timeline Boxplot
            try:
                hist_timeline_df = None
                if isinstance(historical_data, dict):
                    hist_timeline_df = historical_data.get('timeline_data')
                elif hasattr(historical_data, 'timeline_data'):
                    hist_timeline_df = historical_data.timeline_data

                if hist_timeline_df is not None and not hist_timeline_df.empty:
                    this_project = {
                        'timeline_low': timeline_estimate.get('timeline_low', 0),
                        'timeline_likely': timeline_estimate.get('timeline_likely', 0),
                        'timeline_high': timeline_estimate.get('timeline_high', 0),
                        'region': timeline_estimate.get('region', '')
                    }
                    group_by = 'region' if 'region' in hist_timeline_df.columns else 'type'
                    fig, _ = self.timeline_boxplot(hist_timeline_df, this_project, group_by=group_by)
                    if fig:
                        chart_paths['timeline_boxplot'] = project_dir / 'timeline_boxplot.png'
                        _close_figure(fig)
            except Exception as e:
                print(f"Warning: Could not generate timeline boxplot chart: {e}")

            # 4. Risk Radar
            try:
                breakdown = score_result.get('breakdown', {})
                if breakdown:
                    # Default max scores from FeasibilityScorer
                    max_scores = {
                        'queue_position': 25,
                        'study_progress': 25,
                        'developer_track_record': 20,
                        'poi_congestion': 15,
                        'project_characteristics': 15,
                    }

                    # Get benchmark if available
                    benchmark = None
                    if isinstance(historical_data, dict):
                        benchmark = historical_data.get('benchmark_scores')
                    elif hasattr(historical_data, 'benchmark_scores'):
                        benchmark = historical_data.benchmark_scores

                    fig, _ = self.risk_radar(breakdown, max_scores, benchmark)
                    if fig:
                        chart_paths['risk_radar'] = project_dir / 'risk_radar.png'
                        _close_figure(fig)
            except Exception as e:
                print(f"Warning: Could not generate risk radar chart: {e}")

            # 5. Developer Outcomes
            try:
                developer_outcomes = None
                if isinstance(historical_data, dict):
                    developer_outcomes = historical_data.get('developer_outcomes')
                elif hasattr(historical_data, 'developer_outcomes'):
                    developer_outcomes = historical_data.developer_outcomes

                if developer_outcomes:
                    dev_category = developer_info.get('category', '')
                    fig, _ = self.developer_outcomes_bar(developer_outcomes, dev_category)
                    if fig:
                        chart_paths['developer_outcomes'] = project_dir / 'developer_outcomes.png'
                        _close_figure(fig)
            except Exception as e:
                print(f"Warning: Could not generate developer outcomes chart: {e}")

        finally:
            # Restore original output directory
            self.output_dir = original_output

            # Close any remaining figures
            plt.close('all')

        return chart_paths

    def close_all(self) -> None:
        """Close all matplotlib figures to free memory."""
        plt.close('all')


def generate_report_charts(
    project_id: str,
    region: str,
    project_type: str,
    capacity_mw: float,
    score_breakdown: Dict,
    cost_estimate: Dict,
    timeline_estimate: Dict,
    developer_category: str
) -> Dict[str, str]:
    """
    Generate all charts for a report and return paths.

    Convenience function that creates a ChartGenerator and generates all charts
    from provided parameters rather than raw data.

    Args:
        project_id: Unique identifier for the project
        region: Project region/ISO
        project_type: Type of project (Solar, Wind, etc.)
        capacity_mw: Project capacity in MW
        score_breakdown: Dict of score components
        cost_estimate: Dict with cost_low, cost_median, cost_high
        timeline_estimate: Dict with timeline_low, timeline_likely, timeline_high
        developer_category: Category of the developer

    Returns:
        Dict mapping chart name to file path as string
    """
    generator = ChartGenerator()
    chart_paths = {}

    try:
        # Create project directory
        project_dir = generator.output_dir / project_id
        project_dir.mkdir(exist_ok=True)
        generator.output_dir = project_dir

        # Risk Radar
        if score_breakdown:
            max_scores = {
                'queue_position': 25,
                'study_progress': 25,
                'developer_track_record': 20,
                'poi_congestion': 15,
                'project_characteristics': 15,
            }
            fig, _ = generator.risk_radar(
                score_breakdown,
                max_scores,
                title=f"Risk Profile - {project_id}"
            )
            if fig:
                chart_paths['risk_radar'] = str(project_dir / 'risk_radar.png')
                _close_figure(fig)

        # Note: Other charts require historical data which is not provided in this function
        # They can be generated via ChartGenerator.save_all_charts() when historical data is available

    except Exception as e:
        print(f"Error generating report charts: {e}")
    finally:
        generator.close_all()

    return chart_paths


# Example usage and testing
if __name__ == "__main__":
    print("Chart Generation Module")
    print(f"Output directory: {CHART_DIR}")
    print(f"Plotly available: {PLOTLY_AVAILABLE}")
    print(f"Seaborn available: {SEABORN_AVAILABLE}")

    # Generate sample charts for testing
    generator = ChartGenerator()

    # Test risk radar with sample data
    sample_breakdown = {
        'queue_position': 18,
        'study_progress': 20,
        'developer_track_record': 14,
        'poi_congestion': 10,
        'project_characteristics': 12,
    }
    sample_max = {
        'queue_position': 25,
        'study_progress': 25,
        'developer_track_record': 20,
        'poi_congestion': 15,
        'project_characteristics': 15,
    }
    sample_benchmark = {
        'queue_position': 15,
        'study_progress': 15,
        'developer_track_record': 12,
        'poi_congestion': 10,
        'project_characteristics': 10,
    }

    print("\nGenerating sample risk radar...")
    fig, interactive = generator.risk_radar(
        sample_breakdown,
        sample_max,
        sample_benchmark,
        title="Sample Risk Profile"
    )
    if fig:
        print(f"  Static chart saved to: {generator.output_dir / 'risk_radar.png'}")
        _close_figure(fig)
    if interactive:
        print(f"  Interactive chart saved to: {generator.output_dir / 'risk_radar.html'}")

    # Test completion funnel
    sample_funnel = {
        'Queue Entry': 1000,
        'Feasibility Study': 800,
        'System Impact Study': 500,
        'Facilities Study': 350,
        'IA Negotiation': 200,
        'IA Executed': 150,
        'Under Construction': 100,
        'Commercial Operation': 75
    }

    print("\nGenerating sample completion funnel...")
    fig, interactive = generator.completion_funnel(
        sample_funnel,
        this_project_stage="Facilities Study"
    )
    if fig:
        print(f"  Static chart saved to: {generator.output_dir / 'completion_funnel.png'}")
        _close_figure(fig)
    if interactive:
        print(f"  Interactive chart saved to: {generator.output_dir / 'completion_funnel.html'}")

    # Test developer outcomes
    sample_outcomes = {
        'Major Utility': 85,
        'IPP (Large)': 72,
        'IPP (Mid-size)': 58,
        'IPP (Small)': 45,
        'Developer': 52,
        'Community Solar': 38,
        'Unknown': 25
    }

    print("\nGenerating sample developer outcomes...")
    fig, interactive = generator.developer_outcomes_bar(
        sample_outcomes,
        this_developer_category="IPP (Mid-size)"
    )
    if fig:
        print(f"  Static chart saved to: {generator.output_dir / 'developer_outcomes.png'}")
        _close_figure(fig)
    if interactive:
        print(f"  Interactive chart saved to: {generator.output_dir / 'developer_outcomes.html'}")

    generator.close_all()
    print("\nDone!")
