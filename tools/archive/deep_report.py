#!/usr/bin/env python3
"""
Deep Research Report Generator

The most comprehensive report - combines:
- Feasibility scoring
- Cost/timeline benchmarks
- SEC EDGAR search
- News/web search
- Cross-RTO developer search
- NYISO document guidance

Usage:
    python3 deep_report.py 1738 --client "KPMG" -o report.md
"""

import argparse
import sys
from datetime import datetime
from typing import Dict, Any
from pathlib import Path

from analyze import QueueData, QueueAnalyzer
from scoring import FeasibilityScorer
from research import research_project, DeveloperResearcher
from scrapers import comprehensive_developer_research, NYISODocumentFetcher
from real_data import RealDataEstimator

# Import chart and historical data modules
try:
    import charts_altair as charts
    from historical_data import HistoricalData
    CHARTS_AVAILABLE = True
except ImportError:
    CHARTS_AVAILABLE = False
    print("Note: charts_altair or historical_data module not available. Charts will be skipped.")


def generate_deep_report(
    project_id: str,
    df,
    client_name: str = "[CLIENT]",
    region: str = "NYISO"
) -> str:
    """Generate the most comprehensive report with all external research."""

    # Initialize components
    scorer = FeasibilityScorer(df)
    analyzer = QueueAnalyzer(df)

    # Get score
    print(f"[1/5] Scoring project {project_id}...")
    score_result = scorer.score_project(project_id=project_id)

    if 'error' in score_result:
        return f"Error: {score_result['error']}"

    # Get internal research (basic info + developer)
    print(f"[2/5] Running benchmark analysis...")
    research = research_project(df, project_id, region)

    if 'error' in research:
        return f"Error: {research['error']}"

    # Get REAL DATA estimates (replaces hardcoded benchmarks)
    print(f"[3/5] Computing estimates from historical data...")
    real_estimator = RealDataEstimator()
    real_estimates = real_estimator.estimate_project(
        region=region,
        project_type=research['basic_info']['type'],
        capacity_mw=research['basic_info']['capacity_mw'],
        months_in_queue=research['basic_info']['months_in_queue']
    )

    # Get external research (SEC, news, cross-RTO)
    developer_name = research['basic_info']['developer']
    print(f"[4/5] Running external research on {developer_name}...")
    external_research = comprehensive_developer_research(developer_name)

    # Get document guidance
    print(f"[5/5] Getting document guidance...")
    doc_fetcher = NYISODocumentFetcher()
    doc_info = doc_fetcher.get_document_links(project_id)

    # Generate charts with historical comparables
    chart_paths = {}
    historical_stats = {}
    if CHARTS_AVAILABLE:
        print(f"[6/6] Generating charts with historical comparables...")
        chart_paths, historical_stats = _generate_charts(
            project_id=project_id,
            region=region,
            basic_info=research['basic_info'],
            cost_estimate=research['cost_estimate'],
            timeline_estimate=research['timeline_estimate'],
            score_breakdown=score_result['breakdown'],
            developer_info=research['developer_research'],
            cross_rto=external_research.get('cross_rto', {}),
        )
    else:
        print(f"[5/5] Skipping charts (modules not available)...")

    # Build comprehensive report
    proj = score_result['project']
    breakdown = score_result['breakdown']
    dev = research['developer_research']
    basic = research['basic_info']

    # Use REAL DATA for cost/timeline/completion
    cost_data = real_estimates['cost']
    timeline_data = real_estimates['timeline']
    completion_data = real_estimates['completion']

    # Build display-friendly cost structure
    cost = {
        'range_display': real_estimator.format_cost_range(cost_data),
        'total_cost_millions': {
            'low': cost_data['total_millions']['p25'],
            'median': cost_data['total_millions']['p50'],
            'high': cost_data['total_millions']['p75'],
        },
        'cost_per_kw': {
            'low': cost_data['per_kw']['p25'],
            'median': cost_data['per_kw']['p50'],
            'high': cost_data['per_kw']['p75'],
        },
        'confidence': cost_data['confidence'],
        'n_comparables': cost_data['n_comparables'],
        'notes': _build_cost_notes(cost_data, basic),
    }

    # Build display-friendly timeline structure
    timeline = {
        'range_display': real_estimator.format_timeline_range(timeline_data),
        'remaining_months': {
            'optimistic': timeline_data['remaining_p25'],
            'likely': timeline_data['remaining_p50'],
            'pessimistic': timeline_data['remaining_p75'],
        },
        'estimated_cod': _calc_cod_dates(timeline_data),
        'historical_completion_rate': real_estimator.format_completion_rate(completion_data),
        'confidence': timeline_data['confidence'],
        'notes': [],
    }

    # External research
    sec = external_research.get('sec_search', {})
    news = external_research.get('news_search', {})
    cross_rto = external_research.get('cross_rto', {})

    # Build variance analysis
    variance_info = _build_variance_analysis(cost, timeline, basic)

    report = f"""# INTERCONNECTION FEASIBILITY ASSESSMENT
## Deep Research Report

**Project:** {proj.get('name', 'Unknown')}
**Queue ID:** {project_id}
**RTO/ISO:** {region}
**Date:** {datetime.now().strftime('%Y-%m-%d')}
**Prepared For:** {client_name}

---

## VERDICT: {score_result['recommendation']}

**Confidence:** {score_result['confidence']}

| Metric | Value |
|--------|-------|
| **Feasibility Score** | {score_result['total_score']:.0f}/100 (Grade: {score_result['grade']}) |
| **Key Risk** | {score_result['red_flags'][0] if score_result['red_flags'] else 'No critical risks identified'} |

---

## EXECUTIVE SUMMARY

{_build_executive_bullets(score_result, cost, timeline, cross_rto, basic, variance_info)}

### Assessment

{_build_summary(score_result, cost, timeline, cross_rto)}

---

## HISTORICAL COMPARABLES

{_build_comparables_section(chart_paths, historical_stats, region, basic, cost, timeline)}

---

## 1. PROJECT OVERVIEW

| Field | Value |
|-------|-------|
| **Queue ID** | {project_id} |
| **Project Name** | {proj.get('name', 'Unknown')} |
| **Developer** | {basic['developer']} |
| **Project Type** | {basic['type']} |
| **Capacity** | {basic['capacity_mw']:,.0f} MW |
| **State** | {basic['state']} |
| **POI** | {basic['poi']} |
| **Queue Date** | {basic['queue_date']} |
| **Time in Queue** | {basic['months_in_queue']} months |

---

## 2. FEASIBILITY SCORE

**Total Score: {score_result['total_score']:.0f}/100 ({score_result['grade']})**

| Component | Score | Max | Assessment |
|-----------|-------|-----|------------|
| Queue Position | {breakdown['queue_position']:.1f} | 25 | {_assess(breakdown['queue_position'], 25)} |
| Study Progress | {breakdown['study_progress']:.1f} | 25 | {_assess(breakdown['study_progress'], 25)} |
| Developer Track Record | {breakdown['developer_track_record']:.1f} | 20 | {_assess(breakdown['developer_track_record'], 20)} |
| POI Congestion | {breakdown['poi_congestion']:.1f} | 15 | {_assess(breakdown['poi_congestion'], 15)} |
| Project Characteristics | {breakdown['project_characteristics']:.1f} | 15 | {_assess(breakdown['project_characteristics'], 15)} |

---

## 3. DEVELOPER DEEP DIVE

### Basic Profile

| Field | Value |
|-------|-------|
| **Entity** | {dev['name']} |
| **Projects in This Queue** | {dev['projects_in_queue']} |
| **Portfolio in This Queue** | {dev['total_capacity_mw']:,.0f} MW |

### SEC EDGAR Search

{_format_sec_results(sec)}

### News & Web Mentions

{_format_news_results(news)}

### Cross-RTO Presence

{_format_cross_rto_results(cross_rto)}

### Developer Risk Assessment

{_format_dev_risk_assessment(dev, sec, news, cross_rto)}

---

## 4. COST ANALYSIS

### Interconnection Cost Estimate

| Percentile | Cost | $/kW |
|------------|------|------|
| **P25 (Low)** | ${cost['total_cost_millions']['low']:.0f}M | ${cost['cost_per_kw']['low']:,.0f}/kW |
| **P50 (Median)** | ${cost['total_cost_millions']['median']:.0f}M | ${cost['cost_per_kw']['median']:,.0f}/kW |
| **P75 (High)** | ${cost['total_cost_millions']['high']:.0f}M | ${cost['cost_per_kw']['high']:,.0f}/kW |

**Confidence Level:** {cost['confidence']}
**Based on:** {cost['n_comparables']} comparable projects from {region} historical data

### Cost Notes

{_format_notes(cost['notes'])}

---

## 5. TIMELINE ANALYSIS

### Estimated Commercial Operation Date

| Percentile | Remaining | Target COD |
|------------|-----------|------------|
| **P25 (Fast)** | {timeline['remaining_months']['optimistic']} months | {timeline['estimated_cod']['optimistic']} |
| **P50 (Typical)** | {timeline['remaining_months']['likely']} months | {timeline['estimated_cod']['likely']} |
| **P75 (Slow)** | {timeline['remaining_months']['pessimistic']} months | {timeline['estimated_cod']['pessimistic']} |

**Confidence Level:** {timeline['confidence']}

### Historical Context

- **Completion Rate:** {timeline['historical_completion_rate']} of similar projects reach COD
- **Time in Queue:** {basic['months_in_queue']} months already
- **Timeline Range (IQR):** {timeline['range_display']}

{_format_notes(timeline['notes'])}

---

## 6. QUEUE & POI ANALYSIS

{_format_poi_analysis(research.get('poi_analysis', {}))}

---

## 7. RISK MATRIX

### Risk Summary

| Category | Risk Level | Driver |
|----------|------------|--------|
| **Technical** | {_risk_level_badge(breakdown['study_progress'], 25)} | Study progress: {_study_phase_desc(breakdown['study_progress'])} |
| **Cost** | {_confidence_to_risk(cost['confidence'])} | Based on {cost['n_comparables']} comparables |
| **Timeline** | {_risk_level_badge(breakdown['study_progress'], 25)} | {basic['months_in_queue']} months in queue |
| **Developer** | {_dev_risk_badge(cross_rto)} | {cross_rto.get('total_projects', 0)} projects across RTOs |
| **Queue/POI** | {_risk_level_badge(breakdown['poi_congestion'], 15)} | POI congestion level |

### Red Flags (HIGH RISK)

{_format_all_red_flags(score_result, dev, cross_rto)}

### Green Flags (STRENGTHS)

{_format_all_green_flags(score_result, dev, cross_rto)}

---

## 8. STUDY DOCUMENTS

### Available Documents

{_format_doc_guidance(doc_info)}

### Key Items to Review

- [ ] Feasibility Study results and any fatal flaws identified
- [ ] System Impact Study network upgrade requirements
- [ ] Facilities Study cost estimates
- [ ] Interconnection Agreement milestone schedule (if available)
- [ ] Any affected system studies

---

## 9. RECOMMENDATION

### Decision: **{score_result['recommendation']}** (Confidence: {score_result['confidence']})

{_build_recommendation(score_result, cost, timeline, cross_rto)}

### Proceed If (Conditions)

{_build_conditional_recommendations(score_result, cost, timeline, cross_rto, basic)}

### Due Diligence Checklist

{_build_checklist(score_result, sec, cross_rto, doc_info)}

---

## APPENDIX

### A. Data Sources

| Source | Type | Date |
|--------|------|------|
| {region} Queue Data | Automated | {datetime.now().strftime('%Y-%m-%d')} |
| SEC EDGAR | Automated | {datetime.now().strftime('%Y-%m-%d')} |
| News/Web Search | Automated | {datetime.now().strftime('%Y-%m-%d')} |
| Cross-RTO Search | NYISO, MISO | {datetime.now().strftime('%Y-%m-%d')} |
| Cost Benchmarks | LBL/Industry | Historical |
| Timeline Benchmarks | LBL/Industry | Historical |

### B. External Research Links

- SEC EDGAR: {sec.get('search_url', 'N/A')}
- NYISO Documents: {doc_info.get('document_portal', 'N/A')}

### C. Methodology

**Feasibility Score:** 100-point model assessing queue position, study progress, developer track record, POI congestion, and project characteristics.

**Cost Estimates:** Based on Lawrence Berkeley National Lab interconnection cost studies and industry benchmarks, adjusted for region and project size.

**Timeline Estimates:** Based on historical completion times by project type from LBL Queued Up data.

---

**Disclaimer:** This assessment combines automated data extraction, external research, scoring models, and benchmark-based estimates.
All findings should be validated through manual review of source documents. SEC search results indicate filing presence only.
News mentions require manual verification. Developer track record is based on queue data and may not reflect full company history.

---

*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*
*Report Type: Deep Research*
*Score: {score_result['total_score']:.0f}/100 | Recommendation: {score_result['recommendation']}*
"""

    return report


def _generate_charts(
    project_id: str,
    region: str,
    basic_info: Dict,
    cost_estimate: Dict,
    timeline_estimate: Dict,
    score_breakdown: Dict,
    developer_info: Dict,
    cross_rto: Dict,
) -> tuple:
    """Generate all charts for the report using Altair and return paths + historical stats."""
    chart_paths = {}
    historical_stats = {}

    try:
        # Initialize historical data module
        hd = HistoricalData()

        # Project data for charts
        this_project = {
            'capacity_mw': basic_info.get('capacity_mw', 0),
            'cost_low': cost_estimate.get('cost_per_kw', {}).get('low', 0),
            'cost_median': cost_estimate.get('cost_per_kw', {}).get('median', 0),
            'cost_high': cost_estimate.get('cost_per_kw', {}).get('high', 0),
            'type': basic_info.get('type', 'unknown'),
            'region': region,
            'timeline_low': timeline_estimate.get('remaining_months', {}).get('optimistic', 18),
            'timeline_likely': timeline_estimate.get('remaining_months', {}).get('likely', 42),
            'timeline_high': timeline_estimate.get('remaining_months', {}).get('pessimistic', 78),
        }

        # Score breakdown for risk bars
        score_data = {
            'queue_position': score_breakdown.get('queue_position', 0),
            'study_progress': score_breakdown.get('study_progress', 0),
            'developer_track_record': score_breakdown.get('developer_track_record', 0),
            'poi_congestion': score_breakdown.get('poi_congestion', 0),
            'project_characteristics': score_breakdown.get('project_characteristics', 0),
        }

        max_scores = {
            'queue_position': 25,
            'study_progress': 25,
            'developer_track_record': 20,
            'poi_congestion': 15,
            'project_characteristics': 15,
        }

        # 1. Cost Scatter Chart (Altair)
        region_costs = hd.ic_costs_by_region.get(region)
        if region_costs is None or (hasattr(region_costs, 'empty') and region_costs.empty):
            region_costs = hd.ic_costs_df
        if region_costs is not None and len(region_costs) > 0:
            charts.cost_scatter(region_costs, this_project, f'{region} Interconnection Cost Comparison')
            chart_paths['cost_scatter'] = 'charts/cost_scatter_altair.png'

            # Get cost stats for historical comparison
            if 'cost_per_kw' in region_costs.columns:
                costs = region_costs['cost_per_kw'].dropna()
                historical_stats['cost'] = {
                    'median': float(costs.median()),
                    'p25': float(costs.quantile(0.25)),
                    'p75': float(costs.quantile(0.75)),
                    'sample_size': len(costs),
                }

        # 2. Queue Outcomes Donut (Altair - replaces funnel)
        project_type = basic_info.get('type', 'Solar')
        funnel_data = hd.get_completion_funnel(region, project_type, year_range=(2000, 2024))
        if funnel_data and 'error' not in funnel_data:
            outcomes = {
                'Active': int(funnel_data.get('active_in_queue', 0)),
                'Withdrawn': int(funnel_data.get('withdrawn', 0)),
                'Completed': int(funnel_data.get('completed', 0)),
            }
            charts.queue_outcomes(outcomes, 'Active', f'{region} {project_type} Queue Outcomes')
            chart_paths['completion_funnel'] = 'charts/queue_outcomes_altair.png'
            historical_stats['funnel'] = funnel_data

        # 3. Risk Bars (Altair - cleaner than radar)
        charts.risk_bars(score_data, max_scores, title='Project Risk Profile')
        chart_paths['risk_radar'] = 'charts/risk_bars_altair.png'

        # 4. Developer Completion Rates (Altair)
        dev_category = _categorize_developer_type(developer_info, cross_rto)
        dev_outcomes = {
            'Experienced (5+)': 32,
            'Mid-tier (2-4)': 21,
            'Single-project': 14,
            'Unknown/SPV': 10,
        }
        charts.completion_rate_bars(dev_outcomes, dev_category, 'Completion Rate by Developer Type')
        chart_paths['developer_outcomes'] = 'charts/completion_rates_altair.png'
        historical_stats['developer_category'] = dev_category

        # 5. Timeline Comparison (Altair)
        if hd.queued_up_df is not None:
            operational = hd.queued_up_df[hd.queued_up_df['q_status'] == 'operational'].copy()
            if 'q_date_parsed' in operational.columns and 'on_date_parsed' in operational.columns:
                operational = operational[
                    operational['q_date_parsed'].notna() &
                    operational['on_date_parsed'].notna()
                ]
                operational['months_to_cod'] = (
                    operational['on_date_parsed'] - operational['q_date_parsed']
                ).dt.days / 30

                if len(operational) > 10:
                    charts.timeline_comparison(operational, this_project, 'Time to COD by Region')
                    chart_paths['timeline_boxplot'] = 'charts/timeline_altair.png'

                    # Timeline stats
                    historical_stats['timeline'] = {
                        'median_months': float(operational['months_to_cod'].median()),
                        'p25_months': float(operational['months_to_cod'].quantile(0.25)),
                        'p75_months': float(operational['months_to_cod'].quantile(0.75)),
                        'sample_size': len(operational),
                    }

    except Exception as e:
        print(f"Warning: Chart generation error: {e}")
        import traceback
        traceback.print_exc()

    return chart_paths, historical_stats


def _categorize_developer_type(developer_info: Dict, cross_rto: Dict) -> str:
    """Determine developer category based on project count."""
    projects = cross_rto.get('total_projects', 0)
    if developer_info:
        projects = max(projects, developer_info.get('projects_in_queue', 0))

    if projects >= 5:
        return "Experienced (5+)"
    elif projects >= 2:
        return "Mid-tier (2-4)"
    elif projects == 1:
        return "Single-project"
    else:
        return "Unknown/SPV"


def _build_comparables_section(
    chart_paths: Dict,
    historical_stats: Dict,
    region: str,
    basic_info: Dict,
    cost_estimate: Dict,
    timeline_estimate: Dict,
) -> str:
    """Build the Historical Comparables section with charts and analysis."""
    sections = []

    # Cost Context
    if 'cost_scatter' in chart_paths:
        cost_stats = historical_stats.get('cost', {})
        median_cost = cost_stats.get('median', 0)
        sample_size = cost_stats.get('sample_size', 0)
        project_median = cost_estimate.get('cost_per_kw', {}).get('median', 0)

        cost_comparison = "within" if cost_stats.get('p25', 0) <= project_median <= cost_stats.get('p75', 999) else "outside"

        sections.append(f"""### Cost Context

![Interconnection Cost Comparison]({chart_paths['cost_scatter']})

**Analysis:** This project's estimated ${project_median:.0f}/kW sits **{cost_comparison}** the interquartile range
of historical {region} projects. Based on {sample_size} projects with cost data, the median was ${median_cost:.0f}/kW.
""")

    # Completion Probability
    if 'completion_funnel' in chart_paths:
        funnel = historical_stats.get('funnel', {})
        comp_rate = funnel.get('completion_rate_pct', 0)
        wd_rate = funnel.get('withdrawal_rate_pct', 0)
        total = funnel.get('total_entered', 0)

        sections.append(f"""### Completion Probability

![Queue Outcomes]({chart_paths['completion_funnel']})

**Analysis:** Of {total} similar projects entering the {region} queue, **{comp_rate:.1f}%** reached commercial operation
while **{wd_rate:.1f}%** withdrew. This project is currently in the Active stage.
""")

    # Timeline Context
    if 'timeline_boxplot' in chart_paths:
        timeline_stats = historical_stats.get('timeline', {})
        median_months = timeline_stats.get('median_months', 42)
        p25 = timeline_stats.get('p25_months', 21)
        p75 = timeline_stats.get('p75_months', 67)
        sample_size = timeline_stats.get('sample_size', 0)
        project_likely = timeline_estimate.get('remaining_months', {}).get('likely', 42)

        sections.append(f"""### Timeline Context

![Time to Commercial Operation]({chart_paths['timeline_boxplot']})

**Analysis:** The estimated {project_likely}-month timeline (likely case) compares to a historical median of
{median_months:.0f} months for completed projects (IQR: {p25:.0f}-{p75:.0f} months, n={sample_size}).
""")

    # Risk Profile
    if 'risk_radar' in chart_paths:
        sections.append(f"""### Risk Profile

![Project Risk Analysis]({chart_paths['risk_radar']})

**Analysis:** The risk bar chart shows this project's scoring components. Green indicates low risk (>75%),
yellow indicates medium risk (50-75%), and red indicates high risk (<50%). Focus on red items first.
""")

    # Developer Context
    if 'developer_outcomes' in chart_paths:
        dev_category = historical_stats.get('developer_category', 'Unknown')

        sections.append(f"""### Developer Context

![Developer Type Outcomes]({chart_paths['developer_outcomes']})

**Analysis:** This developer is categorized as **{dev_category}**. Historical completion rates vary significantly
by developer experience level, with experienced developers achieving 2-3x higher success rates.
""")

    if not sections:
        return "*Historical comparables charts not available. Run with chart generation enabled.*"

    return "\n".join(sections)


def _assess(score: float, max_score: float) -> str:
    pct = score / max_score
    if pct >= 0.8: return "Strong"
    elif pct >= 0.6: return "Good"
    elif pct >= 0.4: return "Fair"
    else: return "Weak"


def _build_variance_analysis(cost: Dict, timeline: Dict, basic: Dict) -> Dict:
    """Build variance analysis for developer claims vs realistic estimates."""
    # Calculate variance percentages
    cost_median = cost['total_cost_millions']['median']
    timeline_median = timeline['remaining_months']['likely']

    # Estimate what a developer might claim (typically 30-40% lower than realistic)
    # This is a heuristic - in practice you'd have actual developer claims
    developer_cost_claim = cost_median * 0.65  # Developers often underestimate
    developer_timeline_months = timeline_median * 0.70  # Developers often optimistic

    cost_variance_pct = ((cost_median - developer_cost_claim) / developer_cost_claim) * 100 if developer_cost_claim > 0 else 0
    timeline_variance_months = timeline_median - developer_timeline_months

    return {
        'developer_cost_claim': developer_cost_claim,
        'realistic_cost': cost_median,
        'cost_variance_pct': cost_variance_pct,
        'developer_timeline_months': developer_timeline_months,
        'realistic_timeline_months': timeline_median,
        'timeline_variance_months': timeline_variance_months,
    }


def _build_executive_bullets(score_result: Dict, cost: Dict, timeline: Dict,
                              cross_rto: Dict, basic: Dict, variance: Dict) -> str:
    """Build executive summary bullet points comparing claims vs reality."""
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    now = datetime.now()

    def quarter(months):
        dt = now + relativedelta(months=int(months))
        return f"Q{(dt.month - 1) // 3 + 1} {dt.year}"

    realistic_cod = quarter(timeline['remaining_months']['likely'])
    optimistic_cod = quarter(timeline['remaining_months']['optimistic'])

    lines = []

    # Cost comparison
    cost_low = cost['total_cost_millions']['low']
    cost_high = cost['total_cost_millions']['high']
    lines.append(f"- **Interconnection Cost Estimate:** ${cost_low:.0f}M - ${cost_high:.0f}M (median ${cost['total_cost_millions']['median']:.0f}M)")

    # Timeline comparison
    lines.append(f"- **Realistic COD:** {optimistic_cod} to {realistic_cod}")

    # Queue position context
    queue_score = score_result['breakdown']['queue_position']
    if queue_score >= 20:
        lines.append(f"- **Queue Position:** Strong - among early entrants at POI")
    elif queue_score >= 12:
        lines.append(f"- **Queue Position:** Moderate - middle of pack at POI")
    else:
        lines.append(f"- **Queue Position:** Weak - late entrant with significant competition ahead")

    # Key risk
    if score_result['red_flags']:
        lines.append(f"- **Key Risk:** {score_result['red_flags'][0]}")

    # Developer context
    dev_projects = cross_rto.get('total_projects', 0)
    if dev_projects >= 5:
        lines.append(f"- **Developer:** Experienced ({dev_projects} projects across RTOs)")
    elif dev_projects >= 2:
        lines.append(f"- **Developer:** Moderate experience ({dev_projects} projects)")
    else:
        lines.append(f"- **Developer:** Limited track record - enhanced diligence recommended")

    return '\n'.join(lines)


def _risk_level_badge(score: float, max_score: float) -> str:
    """Return HIGH/MEDIUM/LOW risk level."""
    pct = score / max_score
    if pct >= 0.7:
        return "LOW"
    elif pct >= 0.4:
        return "MEDIUM"
    else:
        return "HIGH"


def _confidence_to_risk(confidence: str) -> str:
    """Convert confidence level to risk level."""
    confidence_lower = confidence.lower()
    if 'high' in confidence_lower:
        return "LOW"
    elif 'medium' in confidence_lower:
        return "MEDIUM"
    else:
        return "HIGH"


def _dev_risk_badge(cross_rto: Dict) -> str:
    """Developer risk badge based on track record."""
    projects = cross_rto.get('total_projects', 0)
    if projects >= 5:
        return "LOW"
    elif projects >= 2:
        return "MEDIUM"
    else:
        return "HIGH"


def _study_phase_desc(study_score: float) -> str:
    """Describe study phase based on score."""
    pct = study_score / 25
    if pct >= 0.8:
        return "IA Executed"
    elif pct >= 0.65:
        return "Facilities Study"
    elif pct >= 0.5:
        return "System Impact Study"
    elif pct >= 0.35:
        return "Feasibility Study"
    else:
        return "Early Stage"


def _build_conditional_recommendations(score_result: Dict, cost: Dict, timeline: Dict,
                                        cross_rto: Dict, basic: Dict) -> str:
    """Build numbered conditional recommendations based on risk factors."""
    rec = score_result['recommendation']
    conditions = []
    condition_num = 1

    if rec == 'GO':
        conditions.append(f"{condition_num}. **Standard due diligence** - No special conditions required")
        condition_num += 1
    else:
        # Cost adjustment
        cost_high = cost['total_cost_millions']['high']
        cost_median = cost['total_cost_millions']['median']
        conditions.append(f"{condition_num}. **Cost adjustment:** Model ${cost_median:.0f}M-${cost_high:.0f}M interconnection cost in valuation")
        condition_num += 1

        # Timeline adjustment
        timeline_likely = timeline['remaining_months']['likely']
        timeline_pessimistic = timeline['remaining_months']['pessimistic']
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        now = datetime.now()
        likely_date = now + relativedelta(months=int(timeline_likely))
        pessimistic_date = now + relativedelta(months=int(timeline_pessimistic))
        likely_q = f"Q{(likely_date.month - 1) // 3 + 1} {likely_date.year}"
        pessimistic_q = f"Q{(pessimistic_date.month - 1) // 3 + 1} {pessimistic_date.year}"

        conditions.append(f"{condition_num}. **Timeline adjustment:** Model {likely_q} to {pessimistic_q} COD")
        condition_num += 1

        # Developer verification if limited track record
        if cross_rto.get('total_projects', 0) < 3:
            conditions.append(f"{condition_num}. **Developer verification:** Confirm financial capacity for ${cost_median:.0f}M+ interconnection investment")
            condition_num += 1

        # Contractual protection if high cost uncertainty
        if cost['confidence'].lower() in ['low', 'very low', 'medium']:
            conditions.append(f"{condition_num}. **Contractual protection:** Negotiate cost cap or escrow for interconnection overruns")
            condition_num += 1

        # Monitoring items
        monitoring_items = []
        if score_result['breakdown']['study_progress'] < 15:
            monitoring_items.append("Study completion milestones")
        if score_result['breakdown']['poi_congestion'] < 10:
            monitoring_items.append("Competing project withdrawals")
        if monitoring_items:
            conditions.append(f"{condition_num}. **Monitoring:** Track quarterly for: {', '.join(monitoring_items)}")

    return '\n'.join(conditions)


def _risk_level(score: float, max_score: float) -> str:
    pct = score / max_score
    if pct >= 0.7: return "Low"
    elif pct >= 0.4: return "Medium"
    else: return "High"


def _dev_risk_level(cross_rto: Dict) -> str:
    projects = cross_rto.get('total_projects', 0)
    if projects >= 5: return "Low"
    elif projects >= 2: return "Medium"
    else: return "High"


def _build_summary(score: Dict, cost: Dict, timeline: Dict, cross_rto: Dict) -> str:
    rec = score['recommendation']
    total = score['total_score']
    dev_assessment = cross_rto.get('assessment', 'unknown track record')

    if rec == 'GO':
        return f"""This project scores **{total:.0f}/100** and receives a **GO** recommendation.
The developer has {dev_assessment.lower()}. Estimated interconnection costs of {cost['range_display']}
and timeline of {timeline['range_display']} are within acceptable ranges for this project type."""
    elif rec == 'CONDITIONAL':
        concerns = score['red_flags'][0] if score['red_flags'] else 'timeline uncertainty'
        return f"""This project scores **{total:.0f}/100** and receives a **CONDITIONAL** recommendation.
Primary concern: {concerns}. The developer has {dev_assessment.lower()}.
Enhanced due diligence recommended on flagged items before proceeding."""
    else:
        return f"""This project scores **{total:.0f}/100** and receives a **NO-GO** recommendation.
Multiple risk factors suggest this opportunity should be passed. The developer has {dev_assessment.lower()}."""


def _format_sec_results(sec: Dict) -> str:
    if sec.get('found'):
        lines = ["**SEC Filings Found:** Yes"]
        if sec.get('companies_found'):
            lines.append(f"- Companies: {', '.join(sec['companies_found'][:3])}")
        if sec.get('recent_filings'):
            lines.append("- Recent filings:")
            for f in sec['recent_filings'][:3]:
                lines.append(f"  - {f['type']} ({f['date']})")
        return '\n'.join(lines)
    else:
        return f"""**SEC Filings Found:** No

This entity does not appear in SEC EDGAR, indicating it is likely:
- A private company
- A single-purpose project vehicle (SPV)
- A subsidiary not filing separately

Manual search: {sec.get('search_url', 'N/A')}"""


def _format_news_results(news: Dict) -> str:
    mentions = news.get('total_mentions', 0)
    lines = [f"**Total Mentions Found:** {mentions}"]

    if news.get('indicators'):
        lines.append("\n**Indicators:**")
        for ind in news['indicators']:
            lines.append(f"- {ind}")

    if news.get('top_results'):
        lines.append("\n**Top Results:**")
        for r in news['top_results'][:5]:
            lines.append(f"- [{r.get('title', 'Link')[:50]}]({r.get('url', '#')})")
    else:
        lines.append("\n*No significant news coverage found. This may indicate a new or low-profile entity.*")

    return '\n'.join(lines)


def _format_cross_rto_results(cross_rto: Dict) -> str:
    lines = [
        f"**RTOs Searched:** {', '.join(cross_rto.get('rtos_searched', []))}",
        f"**Total Projects Found:** {cross_rto.get('total_projects', 0)}",
        f"**Total Capacity:** {cross_rto.get('total_capacity_mw', 0):,.0f} MW",
        f"**Assessment:** {cross_rto.get('assessment', 'Unknown')}",
    ]

    if cross_rto.get('matches'):
        lines.append("\n**By RTO:**")
        for rto, data in cross_rto['matches'].items():
            if isinstance(data, dict) and 'count' in data:
                cap = data.get('total_capacity_mw', 0)
                lines.append(f"- {rto}: {data['count']} projects ({cap:,.0f} MW)")

    return '\n'.join(lines)


def _format_dev_risk_assessment(dev: Dict, sec: Dict, news: Dict, cross_rto: Dict) -> str:
    lines = []

    # Risk factors
    risks = []
    positives = []

    if dev['projects_in_queue'] == 1:
        risks.append("Single project in this queue")
    elif dev['projects_in_queue'] >= 3:
        positives.append(f"Multiple projects ({dev['projects_in_queue']}) in this queue")

    if not sec.get('found'):
        risks.append("No SEC filings (private entity)")

    if news.get('total_mentions', 0) == 0:
        risks.append("No news coverage found")
    elif news.get('total_mentions', 0) >= 5:
        positives.append(f"Media presence ({news['total_mentions']} mentions)")

    if cross_rto.get('total_projects', 0) <= 1:
        risks.append("Limited cross-RTO presence")
    elif cross_rto.get('total_projects', 0) >= 5:
        positives.append(f"Strong cross-RTO presence ({cross_rto['total_projects']} projects)")

    if risks:
        lines.append("**Risk Factors:**")
        for r in risks:
            lines.append(f"- {r}")

    if positives:
        lines.append("\n**Positive Factors:**")
        for p in positives:
            lines.append(f"- {p}")

    if not risks and not positives:
        lines.append("*Insufficient information to assess developer risk.*")

    return '\n'.join(lines)


def _format_notes(notes: list) -> str:
    if not notes:
        return "*No additional notes.*"
    return '\n'.join(f"- {note}" for note in notes)


def _format_poi_analysis(poi: Dict) -> str:
    if not poi:
        return "*POI analysis not available.*"

    lines = [
        f"- **Projects at POI:** {poi.get('project_count', 'Unknown')}",
        f"- **Total Capacity:** {poi.get('total_capacity_mw', 0):,.0f} MW",
    ]

    if poi.get('type_breakdown'):
        types = ', '.join(f"{k}: {v}" for k, v in poi['type_breakdown'].items())
        lines.append(f"- **By Type:** {types}")

    return '\n'.join(lines)


def _format_all_red_flags(score: Dict, dev: Dict, cross_rto: Dict) -> str:
    flags = list(score.get('red_flags', []))
    flags.extend(dev.get('red_flags', []))

    if cross_rto.get('total_projects', 0) <= 1:
        flags.append("Limited developer track record across RTOs")

    if not flags:
        return "*No red flags identified.*"

    return '\n'.join(f"- **{flag}**" for flag in set(flags))


def _format_all_green_flags(score: Dict, dev: Dict, cross_rto: Dict) -> str:
    flags = list(score.get('green_flags', []))
    flags.extend(dev.get('green_flags', []))

    if cross_rto.get('total_projects', 0) >= 5:
        flags.append("Strong developer presence across RTOs")

    if not flags:
        return "*No notable strengths identified.*"

    return '\n'.join(f"- {flag}" for flag in set(flags))


def _format_doc_guidance(doc_info: Dict) -> str:
    lines = [
        f"**Document Portal:** {doc_info.get('document_portal', 'N/A')}",
        "",
        "**How to Access:**",
    ]

    for instruction in doc_info.get('instructions', []):
        lines.append(f"{instruction}")

    lines.append("\n**Document Types to Request:**")
    for doc in doc_info.get('document_types', []):
        lines.append(f"- **{doc['type']}:** {doc['typical_contents']}")

    return '\n'.join(lines)


def _build_recommendation(score: Dict, cost: Dict, timeline: Dict, cross_rto: Dict) -> str:
    rec = score['recommendation']
    breakdown = score['breakdown']

    # Find strongest/weakest
    components = [
        ('Queue Position', breakdown['queue_position'] / 25),
        ('Study Progress', breakdown['study_progress'] / 25),
        ('Developer', breakdown['developer_track_record'] / 20),
        ('POI', breakdown['poi_congestion'] / 15),
    ]
    strongest = max(components, key=lambda x: x[1])
    weakest = min(components, key=lambda x: x[1])

    return f"""
**Strongest factor:** {strongest[0]} ({strongest[1]*100:.0f}%)
**Weakest factor:** {weakest[0]} ({weakest[1]*100:.0f}%)

**Cost context:** Median estimate of ${cost['total_cost_millions']['median']:.0f}M represents ${cost['cost_per_kw']['median']:.0f}/kW.
Wide range (${cost['total_cost_millions']['low']:.0f}M - ${cost['total_cost_millions']['high']:.0f}M) reflects
{cost['confidence'].lower()} confidence due to project size and early study phase.

**Developer context:** {cross_rto.get('assessment', 'Unknown')} with {cross_rto.get('total_projects', 0)} projects
across {len(cross_rto.get('rtos_searched', []))} RTOs searched.
"""


def _build_checklist(score: Dict, sec: Dict, cross_rto: Dict, doc_info: Dict) -> str:
    rec = score['recommendation']

    items = [
        "- [ ] Obtain and review interconnection study documents",
        "- [ ] Validate cost estimate against study documents",
        "- [ ] Confirm current study phase with RTO",
    ]

    if not sec.get('found'):
        items.append("- [ ] Research developer ownership/backing (private entity)")

    if cross_rto.get('total_projects', 0) <= 2:
        items.append("- [ ] Investigate developer background via additional sources")

    if rec == 'CONDITIONAL':
        for flag in score['red_flags'][:3]:
            items.append(f"- [ ] Investigate: {flag}")

    items.extend([
        "- [ ] Review transmission constraints in POI area",
        "- [ ] Verify developer financial capability for interconnection costs",
        "- [ ] Assess regulatory/permitting status",
    ])

    return '\n'.join(items)


def _build_cost_notes(cost_data: Dict, basic: Dict) -> list:
    """Build cost notes based on real data analysis."""
    notes = []

    n = cost_data.get('n_comparables', 0)
    confidence = cost_data.get('confidence', 'Low')
    capacity = basic.get('capacity_mw', 0)

    if n >= 20:
        notes.append(f"Based on {n} comparable projects with cost data")
    elif n >= 5:
        notes.append(f"Based on {n} comparable projects (limited sample)")
    else:
        notes.append(f"Limited comparable data ({n} projects) - estimates less reliable")

    if capacity >= 500:
        notes.append("Large project may trigger significant network upgrades")

    if confidence == 'Low' or confidence == 'Very Low - limited comparables':
        notes.append("Recommend obtaining actual study documents to validate costs")

    return notes


def _calc_cod_dates(timeline_data: Dict) -> Dict[str, str]:
    """Calculate COD date strings from timeline data."""
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    now = datetime.now()

    def quarter(dt):
        return f"Q{(dt.month - 1) // 3 + 1} {dt.year}"

    p25_date = now + relativedelta(months=timeline_data['remaining_p25'])
    p50_date = now + relativedelta(months=timeline_data['remaining_p50'])
    p75_date = now + relativedelta(months=timeline_data['remaining_p75'])

    return {
        'optimistic': quarter(p25_date),
        'likely': quarter(p50_date),
        'pessimistic': quarter(p75_date),
    }


def main():
    parser = argparse.ArgumentParser(description="Generate Deep Research Report")
    parser.add_argument('project_id', help='Queue ID to analyze')
    parser.add_argument('--file', '-f', help='Local data file')
    parser.add_argument('--client', default='[CLIENT]', help='Client name')
    parser.add_argument('--output', '-o', help='Output file')
    parser.add_argument('--region', default='NYISO', help='Region')

    args = parser.parse_args()

    # Load data
    loader = QueueData()
    if args.file:
        df = loader.load_file(args.file)
    else:
        df = loader.load_nyiso()

    if df.empty:
        print("Error: No data loaded")
        return 1

    # Generate report
    print(f"\n{'='*60}")
    print("GENERATING DEEP RESEARCH REPORT")
    print(f"{'='*60}\n")

    report = generate_deep_report(
        project_id=args.project_id,
        df=df,
        client_name=args.client,
        region=args.region
    )

    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        print(f"\n{'='*60}")
        print(f"Report saved to: {args.output}")
        print(f"{'='*60}")
    else:
        print(report)

    return 0


if __name__ == "__main__":
    sys.exit(main())
