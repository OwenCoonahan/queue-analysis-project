#!/usr/bin/env python3
"""
Visualization Module for Queue Analysis Reports

Generates embeddable SVG charts for PDF reports:
1. Score Calibration Chart - Shows model predictive power
2. Monte Carlo Distributions - Cost and timeline uncertainty
3. Completion Rate Comparison - Project vs benchmarks
4. Timeline Waterfall - Study phase progression
"""

import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime


def generate_calibration_chart_svg(score_buckets: Dict, width: int = 500, height: int = 200) -> str:
    """
    Generate SVG chart showing score vs completion rate.

    This is the KEY validation chart - proves the model works.
    """
    # Extract data
    buckets = ['0-20', '20-40', '40-60', '60-80', '80-100']
    rates = [score_buckets.get(b, {}).get('completion_rate', 0) * 100 for b in buckets]
    counts = [score_buckets.get(b, {}).get('count', 0) for b in buckets]

    # Chart dimensions
    margin = {'top': 30, 'right': 20, 'bottom': 50, 'left': 60}
    chart_width = width - margin['left'] - margin['right']
    chart_height = height - margin['top'] - margin['bottom']

    # Scale
    max_rate = max(rates) if rates else 100
    bar_width = chart_width / len(buckets) * 0.7
    bar_gap = chart_width / len(buckets) * 0.3

    # Colors based on completion rate
    def get_color(rate):
        if rate >= 50:
            return '#22c55e'  # Green
        elif rate >= 25:
            return '#84cc16'  # Light green
        elif rate >= 15:
            return '#f59e0b'  # Yellow
        else:
            return '#ef4444'  # Red

    # Build SVG
    svg_parts = [
        f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">',
        '<style>',
        '  .axis-label { font-family: sans-serif; font-size: 11px; fill: #6b7280; }',
        '  .chart-title { font-family: sans-serif; font-size: 13px; font-weight: 600; fill: #1f2937; }',
        '  .bar-label { font-family: sans-serif; font-size: 10px; fill: #374151; }',
        '  .value-label { font-family: sans-serif; font-size: 11px; font-weight: 600; fill: white; }',
        '</style>',

        # Title
        f'<text x="{width/2}" y="18" text-anchor="middle" class="chart-title">Score vs Completion Rate (Model Validation)</text>',

        # Y-axis
        f'<line x1="{margin["left"]}" y1="{margin["top"]}" x2="{margin["left"]}" y2="{height - margin["bottom"]}" stroke="#e5e7eb" stroke-width="1"/>',
        f'<text x="{margin["left"] - 45}" y="{height/2}" transform="rotate(-90,{margin["left"] - 45},{height/2})" text-anchor="middle" class="axis-label">Completion Rate (%)</text>',
    ]

    # Y-axis ticks
    for i, val in enumerate([0, 25, 50, 75]):
        y = height - margin['bottom'] - (val / max(75, max_rate)) * chart_height
        svg_parts.append(f'<text x="{margin["left"] - 8}" y="{y + 4}" text-anchor="end" class="axis-label">{val}%</text>')
        svg_parts.append(f'<line x1="{margin["left"]}" y1="{y}" x2="{width - margin["right"]}" y2="{y}" stroke="#f3f4f6" stroke-width="1"/>')

    # Bars
    for i, (bucket, rate, count) in enumerate(zip(buckets, rates, counts)):
        x = margin['left'] + i * (bar_width + bar_gap) + bar_gap/2
        bar_height = (rate / max(75, max_rate)) * chart_height
        y = height - margin['bottom'] - bar_height

        color = get_color(rate)

        # Bar
        svg_parts.append(f'<rect x="{x}" y="{y}" width="{bar_width}" height="{bar_height}" fill="{color}" rx="3"/>')

        # Value label on bar
        if bar_height > 20:
            svg_parts.append(f'<text x="{x + bar_width/2}" y="{y + bar_height/2 + 4}" text-anchor="middle" class="value-label">{rate:.0f}%</text>')
        else:
            svg_parts.append(f'<text x="{x + bar_width/2}" y="{y - 5}" text-anchor="middle" class="bar-label">{rate:.0f}%</text>')

        # X-axis label
        svg_parts.append(f'<text x="{x + bar_width/2}" y="{height - margin["bottom"] + 15}" text-anchor="middle" class="axis-label">{bucket}</text>')

        # Count label
        svg_parts.append(f'<text x="{x + bar_width/2}" y="{height - margin["bottom"] + 28}" text-anchor="middle" class="axis-label" style="font-size:9px;">n={count:,}</text>')

    # X-axis label
    svg_parts.append(f'<text x="{width/2}" y="{height - 5}" text-anchor="middle" class="axis-label">Feasibility Score Range</text>')

    svg_parts.append('</svg>')

    return '\n'.join(svg_parts)


def generate_monte_carlo_chart_svg(mc_result: Dict, chart_type: str = 'cost', width: int = 300, height: int = 150) -> str:
    """
    Generate SVG histogram showing Monte Carlo distribution.

    Args:
        mc_result: Monte Carlo result dictionary
        chart_type: 'cost' or 'timeline'
    """
    if chart_type == 'cost':
        p10 = mc_result.get('cost', {}).get('p10', 0)
        p25 = mc_result.get('cost', {}).get('p25', 0)
        p50 = mc_result.get('cost', {}).get('p50', 0)
        p75 = mc_result.get('cost', {}).get('p75', 0)
        p90 = mc_result.get('cost', {}).get('p90', 0)
        title = 'Cost Distribution ($M)'
        unit = '$M'
    else:
        p10 = mc_result.get('timeline_months', {}).get('p10', 0)
        p25 = mc_result.get('timeline_months', {}).get('p25', 0)
        p50 = mc_result.get('timeline_months', {}).get('p50', 0)
        p75 = mc_result.get('timeline_months', {}).get('p75', 0)
        p90 = mc_result.get('timeline_months', {}).get('p90', 0)
        title = 'Timeline Distribution (Months)'
        unit = 'mo'

    margin = {'top': 25, 'right': 15, 'bottom': 35, 'left': 15}
    chart_width = width - margin['left'] - margin['right']
    chart_height = height - margin['top'] - margin['bottom']

    # Simplified representation: show P10, P25, P50, P75, P90 as box plot style
    min_val = p10
    max_val = p90
    range_val = max_val - min_val if max_val > min_val else 1

    def scale_x(val):
        return margin['left'] + ((val - min_val) / range_val) * chart_width

    center_y = margin['top'] + chart_height / 2
    box_height = chart_height * 0.6

    svg_parts = [
        f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">',
        '<style>',
        '  .axis-label { font-family: sans-serif; font-size: 10px; fill: #6b7280; }',
        '  .chart-title { font-family: sans-serif; font-size: 11px; font-weight: 600; fill: #1f2937; }',
        '  .value-label { font-family: sans-serif; font-size: 9px; fill: #374151; }',
        '</style>',

        # Title
        f'<text x="{width/2}" y="15" text-anchor="middle" class="chart-title">{title}</text>',

        # Whiskers (P10-P25 and P75-P90)
        f'<line x1="{scale_x(p10)}" y1="{center_y}" x2="{scale_x(p25)}" y2="{center_y}" stroke="#94a3b8" stroke-width="2"/>',
        f'<line x1="{scale_x(p75)}" y1="{center_y}" x2="{scale_x(p90)}" y2="{center_y}" stroke="#94a3b8" stroke-width="2"/>',

        # End caps
        f'<line x1="{scale_x(p10)}" y1="{center_y - 8}" x2="{scale_x(p10)}" y2="{center_y + 8}" stroke="#94a3b8" stroke-width="2"/>',
        f'<line x1="{scale_x(p90)}" y1="{center_y - 8}" x2="{scale_x(p90)}" y2="{center_y + 8}" stroke="#94a3b8" stroke-width="2"/>',

        # Box (P25-P75)
        f'<rect x="{scale_x(p25)}" y="{center_y - box_height/2}" width="{scale_x(p75) - scale_x(p25)}" height="{box_height}" fill="#dbeafe" stroke="#3b82f6" stroke-width="2" rx="3"/>',

        # Median line (P50)
        f'<line x1="{scale_x(p50)}" y1="{center_y - box_height/2}" x2="{scale_x(p50)}" y2="{center_y + box_height/2}" stroke="#1d4ed8" stroke-width="3"/>',

        # Labels
        f'<text x="{scale_x(p10)}" y="{center_y + box_height/2 + 15}" text-anchor="middle" class="value-label">P10: {p10:.0f}{unit}</text>',
        f'<text x="{scale_x(p50)}" y="{center_y - box_height/2 - 5}" text-anchor="middle" class="value-label" style="font-weight:600;">P50: {p50:.0f}{unit}</text>',
        f'<text x="{scale_x(p90)}" y="{center_y + box_height/2 + 15}" text-anchor="middle" class="value-label">P90: {p90:.0f}{unit}</text>',

        '</svg>'
    ]

    return '\n'.join(svg_parts)


def generate_completion_comparison_svg(project_rate: float, type_rate: float, region_rate: float,
                                        overall_rate: float = 0.122, width: int = 400, height: int = 150) -> str:
    """
    Generate SVG showing completion rate comparisons.
    """
    margin = {'top': 25, 'right': 100, 'bottom': 20, 'left': 120}
    chart_width = width - margin['left'] - margin['right']
    chart_height = height - margin['top'] - margin['bottom']

    bars = [
        ('This Project', project_rate, '#3b82f6'),
        ('Project Type Avg', type_rate, '#6b7280'),
        ('Region Avg', region_rate, '#6b7280'),
        ('National Avg', overall_rate, '#6b7280'),
    ]

    max_rate = max(r for _, r, _ in bars) if bars else 0.3
    bar_height = chart_height / len(bars) * 0.7
    bar_gap = chart_height / len(bars) * 0.3

    svg_parts = [
        f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">',
        '<style>',
        '  .bar-label { font-family: sans-serif; font-size: 11px; fill: #374151; }',
        '  .chart-title { font-family: sans-serif; font-size: 12px; font-weight: 600; fill: #1f2937; }',
        '  .value-label { font-family: sans-serif; font-size: 11px; font-weight: 600; fill: #1f2937; }',
        '</style>',

        f'<text x="{width/2}" y="15" text-anchor="middle" class="chart-title">Completion Rate Comparison</text>',
    ]

    for i, (label, rate, color) in enumerate(bars):
        y = margin['top'] + i * (bar_height + bar_gap)
        bar_w = (rate / max_rate) * chart_width if max_rate > 0 else 0

        # Label
        svg_parts.append(f'<text x="{margin["left"] - 5}" y="{y + bar_height/2 + 4}" text-anchor="end" class="bar-label">{label}</text>')

        # Bar
        svg_parts.append(f'<rect x="{margin["left"]}" y="{y}" width="{bar_w}" height="{bar_height}" fill="{color}" rx="3"/>')

        # Value
        svg_parts.append(f'<text x="{margin["left"] + bar_w + 5}" y="{y + bar_height/2 + 4}" class="value-label">{rate*100:.1f}%</text>')

    svg_parts.append('</svg>')

    return '\n'.join(svg_parts)


def generate_timeline_phases_svg(current_phase: str, width: int = 500, height: int = 80) -> str:
    """
    Generate SVG showing interconnection study phases.
    """
    phases = [
        ('Queue Entry', 'queue'),
        ('Feasibility', 'feasibility'),
        ('System Impact', 'system_impact'),
        ('Facilities', 'facilities'),
        ('IA Signed', 'ia_signed'),
        ('COD', 'cod'),
    ]

    phase_index = {p[1]: i for i, p in enumerate(phases)}
    current_idx = phase_index.get(current_phase, 0)

    margin = {'left': 10, 'right': 10}
    chart_width = width - margin['left'] - margin['right']
    step_width = chart_width / (len(phases) - 1)

    svg_parts = [
        f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">',
        '<style>',
        '  .phase-label { font-family: sans-serif; font-size: 9px; fill: #6b7280; }',
        '  .phase-label-active { font-family: sans-serif; font-size: 9px; font-weight: 600; fill: #1d4ed8; }',
        '</style>',
    ]

    # Progress line
    svg_parts.append(f'<line x1="{margin["left"]}" y1="30" x2="{width - margin["right"]}" y2="30" stroke="#e5e7eb" stroke-width="4" stroke-linecap="round"/>')

    # Completed portion
    completed_width = current_idx * step_width
    if completed_width > 0:
        svg_parts.append(f'<line x1="{margin["left"]}" y1="30" x2="{margin["left"] + completed_width}" y2="30" stroke="#3b82f6" stroke-width="4" stroke-linecap="round"/>')

    # Phase markers
    for i, (label, phase_id) in enumerate(phases):
        x = margin['left'] + i * step_width

        if i < current_idx:
            # Completed
            color = '#3b82f6'
            fill = '#3b82f6'
        elif i == current_idx:
            # Current
            color = '#3b82f6'
            fill = 'white'
        else:
            # Future
            color = '#d1d5db'
            fill = 'white'

        # Circle
        svg_parts.append(f'<circle cx="{x}" cy="30" r="8" fill="{fill}" stroke="{color}" stroke-width="3"/>')

        if i < current_idx:
            # Checkmark for completed
            svg_parts.append(f'<path d="M{x-3} 30 L{x-1} 33 L{x+4} 27" fill="none" stroke="white" stroke-width="2"/>')

        # Label
        label_class = 'phase-label-active' if i == current_idx else 'phase-label'
        svg_parts.append(f'<text x="{x}" y="55" text-anchor="middle" class="{label_class}">{label}</text>')

    svg_parts.append('</svg>')

    return '\n'.join(svg_parts)


def generate_risk_gauge_svg(score: float, width: int = 150, height: int = 100) -> str:
    """
    Generate SVG gauge showing risk level based on score.
    """
    # Determine risk level
    if score >= 70:
        risk = 'LOW'
        color = '#22c55e'
        angle = 30
    elif score >= 50:
        risk = 'MODERATE'
        color = '#f59e0b'
        angle = 90
    elif score >= 30:
        risk = 'HIGH'
        color = '#f97316'
        angle = 130
    else:
        risk = 'VERY HIGH'
        color = '#ef4444'
        angle = 160

    # Gauge arc
    cx, cy = width / 2, height - 15
    radius = 45

    # Convert angle to radians (0 = left, 180 = right)
    import math
    rad = math.radians(180 - angle)
    end_x = cx + radius * math.cos(rad)
    end_y = cy - radius * math.sin(rad)

    svg_parts = [
        f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">',
        '<style>',
        '  .risk-label { font-family: sans-serif; font-size: 14px; font-weight: 700; }',
        '  .score-label { font-family: sans-serif; font-size: 10px; fill: #6b7280; }',
        '</style>',

        # Background arc
        f'<path d="M {cx - radius} {cy} A {radius} {radius} 0 0 1 {cx + radius} {cy}" fill="none" stroke="#e5e7eb" stroke-width="10" stroke-linecap="round"/>',

        # Colored portion
        f'<path d="M {cx - radius} {cy} A {radius} {radius} 0 0 1 {end_x} {end_y}" fill="none" stroke="{color}" stroke-width="10" stroke-linecap="round"/>',

        # Needle
        f'<line x1="{cx}" y1="{cy}" x2="{end_x}" y2="{end_y}" stroke="#1f2937" stroke-width="3" stroke-linecap="round"/>',
        f'<circle cx="{cx}" cy="{cy}" r="5" fill="#1f2937"/>',

        # Labels
        f'<text x="{cx}" y="{cy - 20}" text-anchor="middle" class="risk-label" fill="{color}">{risk}</text>',
        f'<text x="{cx}" y="{height - 2}" text-anchor="middle" class="score-label">Score: {score:.0f}/100</text>',

        '</svg>'
    ]

    return '\n'.join(svg_parts)


# =============================================================================
# MAIN REPORT VISUALIZATION GENERATOR
# =============================================================================

def generate_report_visualizations(analysis_data: Dict, intelligence_data: Dict) -> Dict[str, str]:
    """
    Generate all visualizations for a report.

    Args:
        analysis_data: Project analysis data
        intelligence_data: Intelligence module data

    Returns:
        Dictionary of SVG strings keyed by chart name
    """
    charts = {}

    # 1. Model Validation / Calibration Chart
    validation = intelligence_data.get('validation', {})
    if validation.get('score_buckets'):
        charts['calibration'] = generate_calibration_chart_svg(validation['score_buckets'])

    # 2. Monte Carlo Charts
    mc = intelligence_data.get('monte_carlo', {})
    if mc and 'cost' in mc:
        charts['monte_carlo_cost'] = generate_monte_carlo_chart_svg(mc, 'cost')
        charts['monte_carlo_timeline'] = generate_monte_carlo_chart_svg(mc, 'timeline')

    # 3. Risk Gauge
    score = analysis_data.get('score_result', {}).get('total_score', 50)
    charts['risk_gauge'] = generate_risk_gauge_svg(score)

    # 4. Completion Comparison
    mc = intelligence_data.get('monte_carlo', {})
    project_rate = mc.get('completion_probability', 0.1)

    # Get type and region rates from validation buckets
    charts['completion_comparison'] = generate_completion_comparison_svg(
        project_rate=project_rate,
        type_rate=0.086,  # Default solar rate
        region_rate=0.062,  # NYISO rate
        overall_rate=0.122
    )

    # 5. Timeline Phases
    study_progress = analysis_data.get('breakdown', {}).get('study_progress', 0)
    if study_progress >= 20:
        phase = 'ia_signed'
    elif study_progress >= 15:
        phase = 'facilities'
    elif study_progress >= 10:
        phase = 'system_impact'
    elif study_progress >= 5:
        phase = 'feasibility'
    else:
        phase = 'queue'
    charts['timeline_phases'] = generate_timeline_phases_svg(phase)

    return charts


if __name__ == '__main__':
    # Test visualizations
    print("Testing visualization generation...")

    # Test calibration chart
    test_buckets = {
        '0-20': {'count': 3, 'completion_rate': 0.0},
        '20-40': {'count': 1329, 'completion_rate': 0.072},
        '40-60': {'count': 8020, 'completion_rate': 0.088},
        '60-80': {'count': 3319, 'completion_rate': 0.35},
        '80-100': {'count': 619, 'completion_rate': 0.616},
    }

    svg = generate_calibration_chart_svg(test_buckets)
    print(f"Calibration chart: {len(svg)} bytes")

    # Test Monte Carlo chart
    test_mc = {
        'cost': {'p10': 5, 'p25': 8, 'p50': 13, 'p75': 22, 'p90': 34},
        'timeline_months': {'p10': 30, 'p25': 38, 'p50': 48, 'p75': 60, 'p90': 78},
    }

    svg = generate_monte_carlo_chart_svg(test_mc, 'cost')
    print(f"Monte Carlo cost chart: {len(svg)} bytes")

    svg = generate_risk_gauge_svg(62)
    print(f"Risk gauge: {len(svg)} bytes")

    print("All visualizations generated successfully!")
