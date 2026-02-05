#!/usr/bin/env python3
"""
Report Generator

Generates draft feasibility assessment reports from queue data and scoring.
Combines automated data extraction with scoring to pre-fill templates.

Usage:
    python3 generate_report.py 0276                      # Generate for project ID
    python3 generate_report.py 0276 --client "Acme PE"   # With client name
    python3 generate_report.py 0276 --output report.md   # Save to file
    python3 generate_report.py --file queue.xlsx 12345   # Use local data
"""

import argparse
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
import json

from analyze import QueueData, QueueAnalyzer
from scoring import FeasibilityScorer, ScoreBreakdown


def generate_assessment_report(
    score_result: Dict[str, Any],
    analyzer: QueueAnalyzer,
    client_name: str = "[CLIENT]",
    additional_notes: Optional[Dict[str, str]] = None
) -> str:
    """
    Generate a full feasibility assessment report.

    Args:
        score_result: Output from FeasibilityScorer.score_project()
        analyzer: QueueAnalyzer with loaded data
        client_name: Name for the report header
        additional_notes: Manual notes to incorporate

    Returns:
        Markdown report content
    """
    proj = score_result['project']
    breakdown = score_result['breakdown']
    notes = additional_notes or {}

    # Determine RTO from data source
    rto = "NYISO"  # Default, could be detected from data

    # Format values with defaults
    def fmt(val, default="[Not available]"):
        if val is None or str(val) == 'nan' or str(val) == '':
            return default
        return str(val)

    def fmt_num(val, default="[TBD]"):
        if val is None:
            return default
        try:
            return f"{float(val):,.0f}"
        except:
            return default

    # Build timeline estimate based on study progress
    study_score = breakdown['study_progress']
    if study_score >= 20:  # Late stage
        timeline_estimate = "12-18 months"
        timeline_note = "Advanced study phase, near completion"
    elif study_score >= 12:  # Mid stage
        timeline_estimate = "18-30 months"
        timeline_note = "Mid-stage studies, typical timeline"
    else:  # Early stage
        timeline_estimate = "30-48+ months"
        timeline_note = "Early stage, significant timeline uncertainty"

    report = f"""# INTERCONNECTION FEASIBILITY ASSESSMENT

**Project:** {fmt(proj.get('name'))}
**Queue ID:** {fmt(proj.get('id'))}
**RTO/ISO:** {rto}
**Date:** {datetime.now().strftime('%Y-%m-%d')}
**Prepared For:** {client_name}

---

## EXECUTIVE SUMMARY

| Attribute | Assessment |
|-----------|------------|
| **Recommendation** | **{score_result['recommendation']}** |
| **Confidence Level** | {_confidence_from_score(score_result['total_score'])} |
| **Feasibility Score** | {score_result['total_score']:.0f}/100 (Grade: {score_result['grade']}) |
| **Key Risk** | {score_result['red_flags'][0] if score_result['red_flags'] else 'No critical risks identified'} |
| **Realistic Timeline** | {notes.get('timeline', timeline_estimate)} |
| **Cost Exposure** | {notes.get('cost_estimate', '[Requires study document review]')} |

### Summary

{_generate_summary(score_result)}

---

## 1. PROJECT OVERVIEW

### Basic Information

| Field | Value |
|-------|-------|
| **Queue ID** | {fmt(proj.get('id'))} |
| **Project Name** | {fmt(proj.get('name'))} |
| **Developer Entity** | {fmt(proj.get('developer'))} |
| **Project Type** | {fmt(proj.get('type'))} |
| **Capacity** | {fmt_num(proj.get('capacity_mw'))} MW |
| **Point of Interconnection** | {fmt(proj.get('poi'))} |
| **State** | {fmt(proj.get('state'))} |

### Feasibility Score Breakdown

| Component | Score | Max | Assessment |
|-----------|-------|-----|------------|
| Queue Position | {breakdown['queue_position']:.1f} | 25 | {_assess_score(breakdown['queue_position'], 25)} |
| Study Progress | {breakdown['study_progress']:.1f} | 25 | {_assess_score(breakdown['study_progress'], 25)} |
| Developer Track Record | {breakdown['developer_track_record']:.1f} | 20 | {_assess_score(breakdown['developer_track_record'], 20)} |
| POI Congestion | {breakdown['poi_congestion']:.1f} | 15 | {_assess_score(breakdown['poi_congestion'], 15)} |
| Project Characteristics | {breakdown['project_characteristics']:.1f} | 15 | {_assess_score(breakdown['project_characteristics'], 15)} |
| **TOTAL** | **{score_result['total_score']:.0f}** | **100** | **{score_result['grade']}** |

---

## 2. QUEUE POSITION ANALYSIS

### Position Assessment

{_queue_position_analysis(score_result, analyzer, proj)}

---

## 3. STUDY PROGRESS

### Current Status

Study Progress Score: **{breakdown['study_progress']:.1f}/25** ({_assess_score(breakdown['study_progress'], 25)})

{_study_progress_notes(score_result)}

### Timeline Assessment

| Phase | Status | Notes |
|-------|--------|-------|
| Feasibility Study | {notes.get('feasibility_status', '[Check RTO records]')} | |
| System Impact Study | {notes.get('sis_status', '[Check RTO records]')} | |
| Facilities Study | {notes.get('facilities_status', '[Check RTO records]')} | |
| Interconnection Agreement | {notes.get('ia_status', '[Check RTO records]')} | |

**Estimated Timeline to COD:** {timeline_estimate}
- *{timeline_note}*

---

## 4. DEVELOPER DILIGENCE

### Developer Profile

| Field | Value |
|-------|-------|
| **Entity** | {fmt(proj.get('developer'))} |
| **Other Projects in Queue** | {notes.get('dev_project_count', '[Research needed]')} |
| **Track Record** | {notes.get('dev_track_record', '[Research needed]')} |

Developer Score: **{breakdown['developer_track_record']:.1f}/20**

{_developer_notes(score_result)}

---

## 5. POI CONGESTION ANALYSIS

POI Congestion Score: **{breakdown['poi_congestion']:.1f}/15** ({_assess_score(breakdown['poi_congestion'], 15)})

{_poi_analysis(score_result, analyzer, proj)}

---

## 6. COST EXPOSURE

### Interconnection Cost Estimate

| Category | Developer Estimate | Our Estimate | Notes |
|----------|-------------------|--------------|-------|
| Interconnection Facilities | {notes.get('dev_ic_cost', '$[TBD]')} | {notes.get('our_ic_cost', '$[TBD]')} | |
| Network Upgrades | {notes.get('dev_nu_cost', '$[TBD]')} | {notes.get('our_nu_cost', '$[TBD]')} | |
| **TOTAL** | {notes.get('dev_total_cost', '$[TBD]')} | {notes.get('our_total_cost', '$[TBD]')} | |

*Cost estimates require review of interconnection study documents.*

---

## 7. RISK ASSESSMENT

### Red Flags

{_format_flags(score_result['red_flags'], 'red')}

### Green Flags

{_format_flags(score_result['green_flags'], 'green')}

### Risk Matrix

| Risk Category | Level | Notes |
|---------------|-------|-------|
| Technical Feasibility | {_risk_level(breakdown['study_progress'], 25)} | Based on study progress |
| Timeline | {_risk_level(breakdown['study_progress'], 25)} | {timeline_note} |
| Cost Exposure | [TBD] | Requires study document review |
| Queue Position | {_risk_level(breakdown['queue_position'], 25)} | Based on POI competition |
| Developer Capability | {_risk_level(breakdown['developer_track_record'], 20)} | Limited track record data |

---

## 8. RECOMMENDATION

### Decision: **{score_result['recommendation']}**

### Rationale

{_generate_rationale(score_result)}

### Recommended Actions

{_generate_actions(score_result)}

### Items Requiring Manual Review

- [ ] Review interconnection study documents for cost estimates
- [ ] Verify developer track record with external sources
- [ ] Check for any regulatory/permitting issues
- [ ] Confirm current study phase with RTO
- [ ] Review transmission constraint studies for POI area

---

## APPENDIX

### A. Scoring Methodology

This assessment uses a 100-point feasibility scoring model:

- **Queue Position (25 pts):** Position relative to other projects at same POI
- **Study Progress (25 pts):** Progress through interconnection study process
- **Developer Track Record (20 pts):** Historical completion rate of developer
- **POI Congestion (15 pts):** Number of competing projects at interconnection point
- **Project Characteristics (15 pts):** Type, size, and historical completion rates

### B. Scoring Notes

{_format_scoring_notes(score_result)}

### C. Data Sources

| Source | Date | Notes |
|--------|------|-------|
| RTO Queue Data | {datetime.now().strftime('%Y-%m-%d')} | Automated extraction |
| Study Documents | [TBD] | Manual review required |

---

**Disclaimer:** This assessment is based on publicly available queue data and automated scoring.
Actual project outcomes may vary. This document requires manual review and supplementation
with interconnection study documents, developer background research, and current RTO filings
before use in investment decisions.

---

*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*
*Feasibility Score: {score_result['total_score']:.0f}/100 | Grade: {score_result['grade']} | Recommendation: {score_result['recommendation']}*
"""

    return report


def _confidence_from_score(score: float) -> str:
    """Determine confidence level from score."""
    if score >= 70:
        return "High"
    elif score >= 50:
        return "Medium"
    else:
        return "Low"


def _assess_score(score: float, max_score: float) -> str:
    """Assess a score component."""
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
    """Convert score to risk level."""
    pct = score / max_score
    if pct >= 0.7:
        return "Low"
    elif pct >= 0.4:
        return "Medium"
    else:
        return "High"


def _generate_summary(result: Dict[str, Any]) -> str:
    """Generate executive summary text."""
    rec = result['recommendation']
    score = result['total_score']
    red_flags = result['red_flags']
    green_flags = result['green_flags']

    if rec == 'GO':
        summary = f"This project scores {score:.0f}/100 and receives a **GO** recommendation. "
        if green_flags:
            summary += f"Key strengths include: {', '.join(green_flags[:2])}. "
        summary += "Proceed with standard due diligence."
    elif rec == 'CONDITIONAL':
        summary = f"This project scores {score:.0f}/100 and receives a **CONDITIONAL** recommendation. "
        if red_flags:
            summary += f"Key concerns: {red_flags[0]}. "
        if green_flags:
            summary += f"However, the project benefits from: {green_flags[0]}. "
        summary += "Recommend additional diligence on flagged items before proceeding."
    else:
        summary = f"This project scores {score:.0f}/100 and receives a **NO-GO** recommendation. "
        if red_flags:
            summary += f"Critical issues: {', '.join(red_flags[:2])}. "
        summary += "Significant risks outweigh potential benefits."

    return summary


def _queue_position_analysis(result: Dict[str, Any], analyzer: QueueAnalyzer, proj: Dict) -> str:
    """Generate queue position analysis text."""
    score = result['breakdown']['queue_position']
    poi = proj.get('poi')

    text = f"Queue Position Score: **{score:.1f}/25** ({_assess_score(score, 25)})\n\n"

    if poi:
        # Get POI analysis
        poi_result = analyzer.analyze_poi(str(poi))
        if 'error' not in poi_result:
            text += f"- **POI:** {poi}\n"
            text += f"- **Projects at this POI:** {poi_result.get('project_count', 'Unknown')}\n"
            if 'total_capacity_mw' in poi_result:
                text += f"- **Total capacity at POI:** {poi_result['total_capacity_mw']:,.0f} MW\n"
        else:
            text += f"- **POI:** {poi}\n"
            text += "- Project appears to have unique POI\n"
    else:
        text += "- POI information not available in queue data\n"

    return text


def _study_progress_notes(result: Dict[str, Any]) -> str:
    """Generate study progress notes."""
    notes = result.get('scoring_notes', [])
    study_notes = [n for n in notes if 'study' in n.lower() or 'phase' in n.lower()]

    if study_notes:
        return "**From queue data:**\n" + "\n".join(f"- {n}" for n in study_notes)
    else:
        return "*Study phase details not available in queue data. Check RTO records.*"


def _developer_notes(result: Dict[str, Any]) -> str:
    """Generate developer notes."""
    notes = result.get('scoring_notes', [])
    dev_notes = [n for n in notes if 'developer' in n.lower() or 'project' in n.lower()]

    if dev_notes:
        return "\n".join(f"- {n}" for n in dev_notes)
    else:
        return "*Developer information limited. Recommend external research.*"


def _poi_analysis(result: Dict[str, Any], analyzer: QueueAnalyzer, proj: Dict) -> str:
    """Generate POI analysis text."""
    poi = proj.get('poi')

    if not poi:
        return "*POI information not available.*"

    poi_result = analyzer.analyze_poi(str(poi))

    if 'error' in poi_result:
        return f"*{poi_result['error']}*"

    text = f"""
### Projects at {poi}

| Metric | Value |
|--------|-------|
| Total Projects | {poi_result.get('project_count', 'Unknown')} |
| Total Capacity | {poi_result.get('total_capacity_mw', 0):,.0f} MW |
| Avg Project Size | {poi_result.get('avg_capacity_mw', 0):,.0f} MW |
"""

    if 'type_breakdown' in poi_result and poi_result['type_breakdown']:
        text += "\n**By Type:**\n"
        for t, count in list(poi_result['type_breakdown'].items())[:5]:
            text += f"- {t}: {count}\n"

    return text


def _format_flags(flags: list, flag_type: str) -> str:
    """Format flags as markdown list."""
    if not flags:
        if flag_type == 'red':
            return "*No critical red flags identified.*"
        else:
            return "*No notable green flags.*"

    prefix = "!" if flag_type == 'red' else "+"
    return "\n".join(f"- {prefix} {flag}" for flag in flags)


def _generate_rationale(result: Dict[str, Any]) -> str:
    """Generate recommendation rationale."""
    rec = result['recommendation']
    score = result['total_score']
    breakdown = result['breakdown']

    # Find strongest and weakest areas
    components = [
        ('Queue Position', breakdown['queue_position'], 25),
        ('Study Progress', breakdown['study_progress'], 25),
        ('Developer Track Record', breakdown['developer_track_record'], 20),
        ('POI Congestion', breakdown['poi_congestion'], 15),
        ('Project Characteristics', breakdown['project_characteristics'], 15),
    ]

    # Sort by percentage score
    sorted_components = sorted(components, key=lambda x: x[1]/x[2], reverse=True)
    strongest = sorted_components[0]
    weakest = sorted_components[-1]

    text = f"The project achieves a feasibility score of {score:.0f}/100. "
    text += f"The strongest area is **{strongest[0]}** ({strongest[1]:.0f}/{strongest[2]}), "
    text += f"while the weakest area is **{weakest[0]}** ({weakest[1]:.0f}/{weakest[2]}). "

    if rec == 'GO':
        text += "The overall profile supports proceeding with investment."
    elif rec == 'CONDITIONAL':
        text += "The profile warrants cautious optimism with additional diligence."
    else:
        text += "The risk profile suggests passing on this opportunity."

    return text


def _generate_actions(result: Dict[str, Any]) -> str:
    """Generate recommended actions."""
    rec = result['recommendation']
    red_flags = result['red_flags']

    if rec == 'GO':
        return """
1. Proceed with standard due diligence process
2. Obtain and review interconnection study documents
3. Verify developer track record
4. Negotiate standard contract protections
"""
    elif rec == 'CONDITIONAL':
        actions = "\n1. Conduct enhanced due diligence on flagged items\n"
        if red_flags:
            for i, flag in enumerate(red_flags[:3], 2):
                actions += f"{i}. Investigate: {flag}\n"
        actions += f"{len(red_flags[:3]) + 2}. Consider pricing adjustments for identified risks\n"
        actions += f"{len(red_flags[:3]) + 3}. Build contingencies into timeline and cost assumptions\n"
        return actions
    else:
        return """
1. **Do not proceed** with current terms
2. If pursuing despite score, require:
   - Significant price reduction
   - Enhanced risk protections
   - Escrow for cost overruns
3. Consider alternative projects in queue
"""


def _format_scoring_notes(result: Dict[str, Any]) -> str:
    """Format all scoring notes."""
    notes = result.get('scoring_notes', [])
    if not notes:
        return "*No additional scoring notes.*"
    return "\n".join(f"- {note}" for note in notes)


def main():
    parser = argparse.ArgumentParser(
        description="Generate Feasibility Assessment Reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 generate_report.py 0276                      # Generate report
    python3 generate_report.py 0276 --client "Acme PE"   # With client name
    python3 generate_report.py 0276 -o report.md         # Save to file
    python3 generate_report.py --file data.xlsx 12345    # Use local data
        """
    )

    parser.add_argument('project_id', help='Queue ID to analyze')
    parser.add_argument('--file', '-f', help='Local Excel/CSV file')
    parser.add_argument('--client', default='[CLIENT]', help='Client name')
    parser.add_argument('--output', '-o', help='Output file path')
    parser.add_argument('--refresh', action='store_true', help='Force refresh data')

    args = parser.parse_args()

    # Load data
    loader = QueueData()

    if args.file:
        df = loader.load_file(args.file)
    else:
        df = loader.load_nyiso(force_refresh=args.refresh)

    if df.empty:
        print("Error: No data loaded")
        return 1

    # Initialize scorer and analyzer
    scorer = FeasibilityScorer(df)
    analyzer = QueueAnalyzer(df)

    # Score the project
    print(f"Scoring project: {args.project_id}")
    score_result = scorer.score_project(project_id=args.project_id)

    if 'error' in score_result:
        print(f"Error: {score_result['error']}")
        return 1

    # Generate report
    print("Generating report...")
    report = generate_assessment_report(
        score_result=score_result,
        analyzer=analyzer,
        client_name=args.client,
    )

    # Output
    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        print(f"Report saved to: {args.output}")
    else:
        print(report)

    return 0


if __name__ == "__main__":
    sys.exit(main())
