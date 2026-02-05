#!/usr/bin/env python3
"""
Full Enhanced Report Generator

Combines scoring + automated research into comprehensive feasibility assessment.

Usage:
    python3 full_report.py 1738 --client "Acme PE"
    python3 full_report.py 1738 -o report.md
"""

import argparse
import sys
from datetime import datetime
from typing import Dict, Any

from analyze import QueueData, QueueAnalyzer
from scoring import FeasibilityScorer
from research import research_project, DeveloperResearcher, CostEstimator, TimelineEstimator


def generate_full_report(
    project_id: str,
    df,
    client_name: str = "[CLIENT]",
    region: str = "NYISO"
) -> str:
    """Generate comprehensive report with scoring + research."""

    # Initialize components
    scorer = FeasibilityScorer(df)
    analyzer = QueueAnalyzer(df)

    # Get score
    print(f"Scoring project {project_id}...")
    score_result = scorer.score_project(project_id=project_id)

    if 'error' in score_result:
        return f"Error: {score_result['error']}"

    # Get research
    print(f"Researching project {project_id}...")
    research = research_project(df, project_id, region)

    if 'error' in research:
        return f"Error: {research['error']}"

    # Build report
    proj = score_result['project']
    breakdown = score_result['breakdown']
    cost = research['cost_estimate']
    timeline = research['timeline_estimate']
    dev = research['developer_research']
    basic = research['basic_info']

    report = f"""# INTERCONNECTION FEASIBILITY ASSESSMENT

**Project:** {proj.get('name', 'Unknown')}
**Queue ID:** {project_id}
**RTO/ISO:** {region}
**Date:** {datetime.now().strftime('%Y-%m-%d')}
**Prepared For:** {client_name}

---

## EXECUTIVE SUMMARY

| Attribute | Assessment |
|-----------|------------|
| **Recommendation** | **{score_result['recommendation']}** |
| **Feasibility Score** | {score_result['total_score']:.0f}/100 (Grade: {score_result['grade']}) |
| **Key Risk** | {score_result['red_flags'][0] if score_result['red_flags'] else 'No critical risks identified'} |
| **Estimated Cost** | {cost['range_display']} |
| **Estimated COD** | {timeline['range_display']} |
| **Completion Probability** | {timeline['historical_completion_rate']} |

### Summary

{_build_summary(score_result, cost, timeline)}

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

## 3. DEVELOPER ANALYSIS

| Field | Value |
|-------|-------|
| **Entity** | {dev['name']} |
| **Projects in Queue** | {dev['projects_in_queue']} |
| **Total Portfolio** | {dev['total_capacity_mw']:,.0f} MW |
| **Identified As** | {dev['parent_company'] or 'Unknown / Independent'} |

### Developer Assessment

{_format_dev_assessment(dev)}

### Other Projects by Developer

{_format_other_projects(dev)}

---

## 4. COST ANALYSIS

### Interconnection Cost Estimate

| Scenario | Cost | $/kW |
|----------|------|------|
| **Low** | ${cost['total_cost_millions']['low']:.0f}M | ${cost['cost_per_kw']['low']:,.0f}/kW |
| **Median** | ${cost['total_cost_millions']['median']:.0f}M | ${cost['cost_per_kw']['median']:,.0f}/kW |
| **High** | ${cost['total_cost_millions']['high']:.0f}M | ${cost['cost_per_kw']['high']:,.0f}/kW |

**Estimate Confidence:** {cost['confidence']}

### Cost Notes

{_format_notes(cost['notes'])}

### Methodology

{cost['methodology']}

---

## 5. TIMELINE ANALYSIS

### Estimated Commercial Operation Date

| Scenario | Timeline | Date |
|----------|----------|------|
| **Optimistic** | {timeline['remaining_months']['optimistic']} months | {timeline['estimated_cod']['optimistic']} |
| **Likely** | {timeline['remaining_months']['likely']} months | {timeline['estimated_cod']['likely']} |
| **Pessimistic** | {timeline['remaining_months']['pessimistic']} months | {timeline['estimated_cod']['pessimistic']} |

### Historical Context

- **Completion Rate:** {timeline['historical_completion_rate']} of similar projects reach COD
- **Time in Queue:** {basic['months_in_queue']} months

### Timeline Notes

{_format_notes(timeline['notes'])}

---

## 6. QUEUE POSITION & POI ANALYSIS

### POI Summary

{_format_poi(research.get('poi_analysis', {}))}

### Queue Position Assessment

{_format_queue_notes(score_result)}

---

## 7. RISK ASSESSMENT

### Red Flags

{_format_flags(score_result['red_flags'], dev.get('red_flags', []))}

### Green Flags

{_format_green_flags(score_result['green_flags'], dev.get('green_flags', []))}

### Risk Matrix

| Category | Level | Driver |
|----------|-------|--------|
| Technical | {_risk_level(breakdown['study_progress'], 25)} | Study progress |
| Cost | {cost['confidence']} confidence | Large project size |
| Timeline | {_risk_level(breakdown['study_progress'], 25)} | Study phase, historical rates |
| Developer | {_risk_level(breakdown['developer_track_record'], 20)} | Track record |
| Queue/POI | {_risk_level(breakdown['poi_congestion'], 15)} | Competition |

---

## 8. RECOMMENDATION

### Decision: **{score_result['recommendation']}**

{_build_recommendation(score_result, cost, timeline, dev)}

### Key Actions

{_build_actions(score_result, cost, timeline)}

---

## APPENDIX

### Data Sources

| Source | Date |
|--------|------|
| RTO Queue Data | {datetime.now().strftime('%Y-%m-%d')} |
| Cost Benchmarks | LBL / Industry data |
| Timeline Benchmarks | Historical completion data |

### Scoring Notes

{_format_scoring_notes(score_result)}

---

**Disclaimer:** This assessment combines automated data extraction, scoring models, and benchmark-based estimates.
Cost and timeline estimates are indicative ranges based on historical data and should be validated against
actual interconnection study documents. Developer track record is based on queue data only and may not reflect
full company history.

---

*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*
*Score: {score_result['total_score']:.0f}/100 | Recommendation: {score_result['recommendation']}*
"""

    return report


def _assess(score: float, max_score: float) -> str:
    pct = score / max_score
    if pct >= 0.8:
        return "Strong"
    elif pct >= 0.6:
        return "Good"
    elif pct >= 0.4:
        return "Fair"
    else:
        return "Weak"


def _risk_level(score: float, max_score: float) -> str:
    pct = score / max_score
    if pct >= 0.7:
        return "Low"
    elif pct >= 0.4:
        return "Medium"
    else:
        return "High"


def _build_summary(score: Dict, cost: Dict, timeline: Dict) -> str:
    rec = score['recommendation']
    total = score['total_score']

    if rec == 'GO':
        return f"""This project scores **{total:.0f}/100** and receives a **GO** recommendation.
With estimated interconnection costs of {cost['range_display']} and a target COD of {timeline['range_display']},
the project profile supports proceeding with standard due diligence. Key strengths include {', '.join(score['green_flags'][:2]) if score['green_flags'] else 'favorable queue position'}."""

    elif rec == 'CONDITIONAL':
        concerns = score['red_flags'][0] if score['red_flags'] else 'timeline uncertainty'
        return f"""This project scores **{total:.0f}/100** and receives a **CONDITIONAL** recommendation.
Estimated interconnection costs range from {cost['range_display']} with COD projected for {timeline['range_display']}.
The primary concern is: {concerns}. Enhanced due diligence is recommended on flagged items before proceeding."""

    else:
        return f"""This project scores **{total:.0f}/100** and receives a **NO-GO** recommendation.
The risk profile suggests significant challenges: {', '.join(score['red_flags'][:2]) if score['red_flags'] else 'weak overall score'}.
Estimated costs of {cost['range_display']} and timeline uncertainty further support passing on this opportunity."""


def _format_dev_assessment(dev: Dict) -> str:
    lines = []

    if dev['projects_in_queue'] == 1:
        lines.append("- **Single-project developer** - Limited track record in this queue")
    elif dev['projects_in_queue'] >= 5:
        lines.append(f"- **Experienced developer** with {dev['projects_in_queue']} projects totaling {dev['total_capacity_mw']:,.0f} MW")
    else:
        lines.append(f"- Developer has {dev['projects_in_queue']} projects in queue")

    if dev.get('parent_company'):
        lines.append(f"- Identified association: **{dev['parent_company']}**")

    if dev.get('project_types'):
        types = ', '.join(f"{k}: {v}" for k, v in list(dev['project_types'].items())[:3])
        lines.append(f"- Project types: {types}")

    return '\n'.join(lines) if lines else "*Limited developer information available.*"


def _format_other_projects(dev: Dict) -> str:
    projects = dev.get('other_projects', [])

    if not projects or len(projects) <= 1:
        return "*No other projects by this developer in queue.*"

    lines = ["| Project | Capacity | Type |", "|---------|----------|------|"]
    for p in projects[:5]:
        name = str(p.get('name', 'Unknown'))[:30]
        cap = p.get('capacity', '?')
        ptype = p.get('type', '?')
        lines.append(f"| {name} | {cap} MW | {ptype} |")

    return '\n'.join(lines)


def _format_notes(notes: list) -> str:
    if not notes:
        return "*No additional notes.*"
    return '\n'.join(f"- {note}" for note in notes)


def _format_poi(poi: Dict) -> str:
    if not poi:
        return "*POI analysis not available.*"

    lines = []
    lines.append(f"- **Projects at POI:** {poi.get('project_count', 'Unknown')}")

    if 'total_capacity_mw' in poi:
        lines.append(f"- **Total Capacity:** {poi['total_capacity_mw']:,.0f} MW")

    if 'type_breakdown' in poi:
        types = ', '.join(f"{k}: {v}" for k, v in poi['type_breakdown'].items())
        lines.append(f"- **By Type:** {types}")

    return '\n'.join(lines)


def _format_queue_notes(score: Dict) -> str:
    notes = score.get('scoring_notes', [])
    queue_notes = [n for n in notes if any(kw in n.lower() for kw in ['queue', 'poi', 'position', 'project'])]

    if queue_notes:
        return '\n'.join(f"- {note}" for note in queue_notes)
    return "*Queue position details in scoring notes.*"


def _format_flags(score_flags: list, dev_flags: list) -> str:
    all_flags = list(set(score_flags + dev_flags))

    if not all_flags:
        return "*No red flags identified.*"

    return '\n'.join(f"- **{flag}**" for flag in all_flags)


def _format_green_flags(score_flags: list, dev_flags: list) -> str:
    all_flags = list(set(score_flags + dev_flags))

    if not all_flags:
        return "*No notable strengths identified.*"

    return '\n'.join(f"- {flag}" for flag in all_flags)


def _build_recommendation(score: Dict, cost: Dict, timeline: Dict, dev: Dict) -> str:
    rec = score['recommendation']
    total = score['total_score']

    # Find strongest/weakest
    breakdown = score['breakdown']
    components = [
        ('Queue Position', breakdown['queue_position'] / 25),
        ('Study Progress', breakdown['study_progress'] / 25),
        ('Developer', breakdown['developer_track_record'] / 20),
        ('POI', breakdown['poi_congestion'] / 15),
        ('Characteristics', breakdown['project_characteristics'] / 15),
    ]
    strongest = max(components, key=lambda x: x[1])
    weakest = min(components, key=lambda x: x[1])

    text = f"""The project achieves a feasibility score of **{total:.0f}/100**.

**Strongest factor:** {strongest[0]} ({strongest[1]*100:.0f}%)
**Weakest factor:** {weakest[0]} ({weakest[1]*100:.0f}%)

**Cost context:** At the median estimate of ${cost['total_cost_millions']['median']:.0f}M for {score['project']['capacity_mw']} MW,
this represents approximately ${cost['cost_per_kw']['median']:.0f}/kW - {'within' if cost['confidence'] != 'Low' else 'at the edge of'} typical ranges for this project type.

**Timeline context:** The {timeline['range_display']} estimated COD reflects typical timelines for this project type,
with {timeline['historical_completion_rate']} historical completion rate.
"""

    return text


def _build_actions(score: Dict, cost: Dict, timeline: Dict) -> str:
    rec = score['recommendation']

    if rec == 'GO':
        return """
1. **Proceed with standard due diligence**
2. Obtain interconnection study documents to validate cost estimate
3. Verify developer background through external sources
4. Negotiate standard contract protections
5. Monitor queue status monthly
"""
    elif rec == 'CONDITIONAL':
        actions = ["1. **Conduct enhanced due diligence on flagged items**"]

        for i, flag in enumerate(score['red_flags'][:3], 2):
            actions.append(f"{i}. Investigate: {flag}")

        next_num = len(score['red_flags'][:3]) + 2
        actions.append(f"{next_num}. Model cost sensitivity: ${cost['total_cost_millions']['low']:.0f}M to ${cost['total_cost_millions']['high']:.0f}M")
        actions.append(f"{next_num + 1}. Build timeline contingency: {timeline['remaining_months']['pessimistic']} months worst case")
        actions.append(f"{next_num + 2}. Consider price adjustment or escrow for cost overrun risk")

        return '\n'.join(actions)
    else:
        return """
1. **Do not proceed with current terms**
2. If pursuing despite score, require:
   - Significant price reduction (30%+ from asking)
   - Enhanced risk protections / escrow
   - Milestone-based payments tied to study progress
3. Consider screening alternative projects in queue
"""


def _format_scoring_notes(score: Dict) -> str:
    notes = score.get('scoring_notes', [])
    if not notes:
        return "*See scoring breakdown above.*"
    return '\n'.join(f"- {note}" for note in notes)


def main():
    parser = argparse.ArgumentParser(description="Generate Full Feasibility Report")
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
    report = generate_full_report(
        project_id=args.project_id,
        df=df,
        client_name=args.client,
        region=args.region
    )

    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        print(f"Report saved to: {args.output}")
    else:
        print(report)

    return 0


if __name__ == "__main__":
    sys.exit(main())
