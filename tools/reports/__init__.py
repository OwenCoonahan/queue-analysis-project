"""
Reports Module - Consolidated PDF Report Generation

This module provides two main report types:
1. Deal Report - Single project feasibility assessment
2. Cluster Report - Group/portfolio analysis

Usage:
    from reports import generate_deal_report, generate_cluster_report

    # Single project
    pdf_path = generate_deal_report(
        project_id="J1234",
        client_name="Acme Capital",
        output_path="deal_report.pdf"
    )

    # Cluster of projects
    pdf_path = generate_cluster_report(
        project_ids=["J1234", "J1235", "J1236"],
        cluster_name="XYZ Substation Portfolio",
        client_name="Acme Capital",
        output_path="cluster_report.pdf"
    )
"""

from .deal_report import generate_deal_report
from .cluster_report import generate_cluster_report
from .deal_sheet import generate_deal_sheet_pdf, generate_deal_sheets

__all__ = ['generate_deal_report', 'generate_cluster_report',
           'generate_deal_sheet_pdf', 'generate_deal_sheets']
