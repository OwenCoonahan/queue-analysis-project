#!/usr/bin/env python3
"""
Report Generator for Interconnection Feasibility Assessments.

Generates markdown reports from queue analysis data.
"""

import os
import json
from datetime import datetime
from typing import Dict, Any, Optional
from queue_analyzer import QueueAnalyzer
import pandas as pd


class ReportGenerator:
    """Generate feasibility assessment reports."""

    def __init__(self, template_dir: str = "../templates"):
        """
        Initialize report generator.

        Args:
            template_dir: Directory containing report templates
        """
        self.template_dir = template_dir

    def generate_quick_assessment(self,
                                  iso: str,
                                  queue_id: str,
                                  client_name: str = "[CLIENT]",
                                  notes: Optional[Dict[str, Any]] = None) -> str:
        """
        Generate a quick assessment report.

        Args:
            iso: ISO name
            queue_id: Queue ID to analyze
            client_name: Client name for the report
            notes: Additional notes/overrides for the report

        Returns:
            Markdown report content
        """
        analyzer = QueueAnalyzer(iso)
        project = analyzer.find_project(queue_id=queue_id)

        if len(project) == 0:
            raise ValueError(f"No project found with queue ID: {queue_id}")

        project = project.iloc[0]

        # Extract key fields (handling various column name formats)
        def get_field(patterns, default="[Not found]"):
            for pattern in patterns:
                for col in project.index:
                    if pattern.lower() in col.lower():
                        val = project[col]
                        if pd.notna(val) and str(val) != '':
                            return val
            return default

        # Get project details
        project_name = get_field(['name'], 'Unknown Project')
        developer = get_field(['developer', 'owner', 'entity', 'applicant'])
        capacity = get_field(['capacity', 'mw'])
        fuel_type = get_field(['fuel', 'type', 'generation', 'resource'])
        state = get_field(['state'])
        county = get_field(['county'])
        poi = get_field(['poi', 'substation', 'interconnection point'])
        status = get_field(['status'])
        queue_date = get_field(['queue date', 'request date', 'date'])

        # Get POI analysis
        poi_analysis = None
        if poi and poi != "[Not found]":
            poi_analysis = analyzer.get_poi_analysis(str(poi))

        # Get completion rates for similar projects
        completion_rates = analyzer.calculate_completion_rate(fuel_type=str(fuel_type) if fuel_type != "[Not found]" else None)

        # Calculate months in queue
        months_in_queue = "[Calculate]"
        if queue_date and queue_date != "[Not found]":
            try:
                if isinstance(queue_date, str):
                    qd = pd.to_datetime(queue_date)
                else:
                    qd = queue_date
                months_in_queue = (datetime.now() - qd).days // 30
            except:
                pass

        # Generate report
        report = f"""# QUICK INTERCONNECTION ASSESSMENT

**Project:** {project_name} | **Queue ID:** {queue_id} | **RTO:** {iso}
**Date:** {datetime.now().strftime('%Y-%m-%d')} | **Prepared For:** {client_name}

---

## VERDICT

| | |
|---|---|
| **Recommendation** | **[GO / NO-GO / CONDITIONAL]** |
| **Confidence** | [High / Medium / Low] |
| **Key Risk** | [Analyst to complete] |
| **Realistic COD** | [Analyst to complete] |
| **Cost Exposure** | [Analyst to complete] |

---

## PROJECT SNAPSHOT

| | |
|---|---|
| Developer | {developer} |
| Type | {fuel_type} |
| Capacity | {capacity} MW |
| POI | {poi} |
| Location | {county}, {state} |
| Queue Date | {queue_date} ({months_in_queue} months in queue) |
| Current Phase | {status} |

---

## QUEUE POSITION

- **Projects at same POI:** {poi_analysis.get('total_projects', '[Unknown]') if poi_analysis and 'error' not in poi_analysis else '[Unknown]'} totaling {poi_analysis.get('total_capacity_mw', '[Unknown]'):,.0f if poi_analysis and 'total_capacity_mw' in poi_analysis else '[Unknown]'} MW
- **Position in queue:** [Analyst to determine]
- **Regional completion rate:** {completion_rates.get('completion_rate', 0):.0f}% for similar {fuel_type} projects
- **Average time to COD:** [Research needed]

---

## COST ANALYSIS

| | Developer | Our Estimate |
|---|---|---|
| Interconnection | $[X]M | $[X]M |
| Network Upgrades | $[X]M | $[X]M |
| **Total** | **$[X]M** | **$[X-Y]M** |

**Variance driver:** [Analyst to complete]

---

## TIMELINE

| Milestone | Developer | Our Estimate |
|---|---|---|
| Studies complete | [Date] | [Date] |
| IA executed | [Date] | [Date] |
| COD | [Date] | [Range] |

**Delay risk:** [Primary delay factor]

---

## DEVELOPER CHECK

| | |
|---|---|
| Track record | [Research needed] |
| Red flags | [None / List concerns] |
| Financial backing | [Research needed] |

---

## KEY RISKS

| Risk | Level | Note |
|---|---|---|
| Cost overrun | [H/M/L] | [One line] |
| Timeline slip | [H/M/L] | [One line] |
| Queue competition | [H/M/L] | {poi_analysis.get('total_projects', 'Unknown') if poi_analysis else 'Unknown'} projects at same POI |
| Technical | [H/M/L] | [One line] |

---

## RECOMMENDATION

**[GO / NO-GO / CONDITIONAL]**

[Analyst to complete: 2-3 sentences on recommendation and key actions]

**If proceeding:**
1. [Action 1]
2. [Action 2]
3. [Action 3]

---

## RAW DATA EXTRACTED

```
ISO: {iso}
Queue ID: {queue_id}
Project Name: {project_name}
Developer: {developer}
Capacity: {capacity} MW
Fuel Type: {fuel_type}
State: {state}
County: {county}
POI: {poi}
Status: {status}
Queue Date: {queue_date}

POI Analysis:
- Total projects at POI: {poi_analysis.get('total_projects', 'N/A') if poi_analysis else 'N/A'}
- Total capacity at POI: {poi_analysis.get('total_capacity_mw', 'N/A') if poi_analysis else 'N/A'} MW
- Active projects: {poi_analysis.get('active_projects', 'N/A') if poi_analysis else 'N/A'}

Completion Rates ({fuel_type}):
- Completion rate: {completion_rates.get('completion_rate', 'N/A')}%
- Withdrawal rate: {completion_rates.get('withdrawal_rate', 'N/A')}%
- Total projects analyzed: {completion_rates.get('total_projects', 'N/A')}
```

---

*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*
"""

        return report

    def save_report(self,
                    report_content: str,
                    output_path: str) -> str:
        """
        Save a report to file.

        Args:
            report_content: Report markdown content
            output_path: Path to save the report

        Returns:
            Path to saved file
        """
        with open(output_path, 'w') as f:
            f.write(report_content)

        print(f"Report saved to: {output_path}")
        return output_path


def main():
    """Example usage."""
    import argparse

    parser = argparse.ArgumentParser(description="Generate feasibility assessment reports")
    parser.add_argument('iso', help='ISO name (PJM, ERCOT, etc.)')
    parser.add_argument('queue_id', help='Queue ID to analyze')
    parser.add_argument('--client', default='[CLIENT]', help='Client name')
    parser.add_argument('--output', '-o', help='Output file path')

    args = parser.parse_args()

    generator = ReportGenerator()

    report = generator.generate_quick_assessment(
        iso=args.iso,
        queue_id=args.queue_id,
        client_name=args.client
    )

    if args.output:
        generator.save_report(report, args.output)
    else:
        print(report)


if __name__ == "__main__":
    main()
