#!/usr/bin/env python3
"""
Centralized Report Manager

Handles all report generation, storage, and retrieval across dashboards.
Reports are stored in organized directories with metadata for easy browsing.

Usage:
    from report_manager import ReportManager

    rm = ReportManager()

    # Create a project report
    report = rm.create_project_report(
        project_id="1738",
        project_name="1 Gig Data Center",
        client="KPMG",
        report_type="feasibility"
    )

    # List all reports
    reports = rm.list_reports()

    # Get a specific report
    report = rm.get_report(report_id)
"""

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import hashlib


@dataclass
class ReportMetadata:
    """Metadata for a report."""
    report_id: str
    report_type: str  # 'project', 'portfolio', 'comparison'
    client: str
    created_at: str
    title: str
    description: str
    project_id: Optional[str] = None
    project_name: Optional[str] = None
    parameters: Optional[Dict] = None
    files: Optional[List[str]] = None
    tags: Optional[List[str]] = None


class ReportManager:
    """
    Centralized report management system.

    Directory structure:
    output/
    ├── reports/
    │   ├── manifest.json          # Index of all reports
    │   ├── project/
    │   │   ├── 2026-01-22_KPMG_1738_feasibility/
    │   │   │   ├── metadata.json
    │   │   │   ├── report.pdf
    │   │   │   ├── report.html
    │   │   │   └── report.md
    │   ├── portfolio/
    │   │   ├── 2026-01-22_ClientName_market_overview/
    │   │   │   ├── metadata.json
    │   │   │   ├── report.pdf
    │   │   │   └── charts/
    │   └── comparison/
    │       └── ...
    """

    REPORT_TYPES = ['project', 'portfolio', 'comparison', 'valuation']

    def __init__(self, base_dir: Optional[Path] = None):
        """Initialize report manager."""
        if base_dir is None:
            base_dir = Path(__file__).parent / 'output' / 'reports'

        self.base_dir = Path(base_dir)
        self.manifest_path = self.base_dir / 'manifest.json'

        # Ensure directories exist
        self._ensure_dirs()

        # Load or create manifest
        self.manifest = self._load_manifest()

    def _ensure_dirs(self):
        """Create directory structure if it doesn't exist."""
        for report_type in self.REPORT_TYPES:
            (self.base_dir / report_type).mkdir(parents=True, exist_ok=True)

    def _load_manifest(self) -> Dict:
        """Load the reports manifest."""
        if self.manifest_path.exists():
            with open(self.manifest_path, 'r') as f:
                return json.load(f)
        return {'reports': [], 'last_updated': None}

    def _save_manifest(self):
        """Save the reports manifest."""
        self.manifest['last_updated'] = datetime.now().isoformat()
        with open(self.manifest_path, 'w') as f:
            json.dump(self.manifest, f, indent=2, default=str)

    def _generate_report_id(self, report_type: str, client: str, identifier: str) -> str:
        """Generate a unique report ID."""
        date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        # Clean up strings for filesystem
        client_clean = "".join(c for c in client if c.isalnum() or c in ' -_').strip().replace(' ', '_')[:20]
        id_clean = "".join(c for c in identifier if c.isalnum() or c in ' -_').strip().replace(' ', '_')[:30]

        return f"{date_str}_{client_clean}_{id_clean}"

    def _get_report_dir(self, report_type: str, report_id: str) -> Path:
        """Get the directory path for a report."""
        return self.base_dir / report_type / report_id

    def create_report(
        self,
        report_type: str,
        client: str,
        title: str,
        description: str = "",
        project_id: Optional[str] = None,
        project_name: Optional[str] = None,
        parameters: Optional[Dict] = None,
        tags: Optional[List[str]] = None
    ) -> Dict:
        """
        Create a new report entry and return its metadata.

        Returns dict with:
        - report_id: Unique identifier
        - report_dir: Path to store report files
        - metadata: Full metadata object
        """
        if report_type not in self.REPORT_TYPES:
            raise ValueError(f"Invalid report type. Must be one of: {self.REPORT_TYPES}")

        # Generate identifier based on type
        if report_type == 'project' and project_id:
            identifier = f"{project_id}_{report_type}"
        elif report_type == 'portfolio':
            identifier = "portfolio_analysis"
        elif report_type == 'comparison':
            identifier = "comparison"
        elif report_type == 'valuation':
            identifier = f"valuation_{project_id or 'batch'}"
        else:
            identifier = report_type

        report_id = self._generate_report_id(report_type, client, identifier)
        report_dir = self._get_report_dir(report_type, report_id)
        report_dir.mkdir(parents=True, exist_ok=True)

        # Create metadata
        metadata = ReportMetadata(
            report_id=report_id,
            report_type=report_type,
            client=client,
            created_at=datetime.now().isoformat(),
            title=title,
            description=description,
            project_id=project_id,
            project_name=project_name,
            parameters=parameters or {},
            files=[],
            tags=tags or []
        )

        # Save metadata to report directory
        metadata_path = report_dir / 'metadata.json'
        with open(metadata_path, 'w') as f:
            json.dump(asdict(metadata), f, indent=2, default=str)

        # Add to manifest
        self.manifest['reports'].append({
            'report_id': report_id,
            'report_type': report_type,
            'client': client,
            'title': title,
            'created_at': metadata.created_at,
            'project_id': project_id,
            'project_name': project_name,
        })
        self._save_manifest()

        return {
            'report_id': report_id,
            'report_dir': report_dir,
            'metadata': metadata
        }

    def add_file_to_report(self, report_id: str, report_type: str,
                          file_path: Path, file_name: Optional[str] = None) -> Path:
        """
        Add a file to an existing report.

        Args:
            report_id: The report's unique ID
            report_type: Type of report (project, portfolio, etc.)
            file_path: Source file path
            file_name: Optional new filename (defaults to original)

        Returns:
            Path to the file in the report directory
        """
        report_dir = self._get_report_dir(report_type, report_id)

        if not report_dir.exists():
            raise ValueError(f"Report not found: {report_id}")

        # Determine destination filename
        if file_name is None:
            file_name = Path(file_path).name

        dest_path = report_dir / file_name

        # Copy file
        shutil.copy2(file_path, dest_path)

        # Update metadata
        metadata_path = report_dir / 'metadata.json'
        if metadata_path.exists():
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)

            if 'files' not in metadata:
                metadata['files'] = []

            if file_name not in metadata['files']:
                metadata['files'].append(file_name)

            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)

        return dest_path

    def save_content_to_report(self, report_id: str, report_type: str,
                               content: str, file_name: str) -> Path:
        """
        Save string content directly to a report.

        Args:
            report_id: The report's unique ID
            report_type: Type of report
            content: String content to save
            file_name: Filename (e.g., 'report.md', 'report.html')

        Returns:
            Path to the saved file
        """
        report_dir = self._get_report_dir(report_type, report_id)

        if not report_dir.exists():
            raise ValueError(f"Report not found: {report_id}")

        dest_path = report_dir / file_name

        with open(dest_path, 'w') as f:
            f.write(content)

        # Update metadata
        metadata_path = report_dir / 'metadata.json'
        if metadata_path.exists():
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)

            if 'files' not in metadata:
                metadata['files'] = []

            if file_name not in metadata['files']:
                metadata['files'].append(file_name)

            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)

        return dest_path

    def list_reports(self,
                    report_type: Optional[str] = None,
                    client: Optional[str] = None,
                    limit: int = 50,
                    newest_first: bool = True) -> List[Dict]:
        """
        List reports with optional filtering.

        Args:
            report_type: Filter by type (project, portfolio, etc.)
            client: Filter by client name
            limit: Max number of reports to return
            newest_first: Sort by date descending

        Returns:
            List of report summary dicts
        """
        reports = self.manifest.get('reports', [])

        # Filter
        if report_type:
            reports = [r for r in reports if r.get('report_type') == report_type]

        if client:
            reports = [r for r in reports if client.lower() in r.get('client', '').lower()]

        # Sort
        reports = sorted(
            reports,
            key=lambda x: x.get('created_at', ''),
            reverse=newest_first
        )

        # Limit
        reports = reports[:limit]

        # Add file paths
        for report in reports:
            report_dir = self._get_report_dir(report['report_type'], report['report_id'])
            report['report_dir'] = str(report_dir)
            report['exists'] = report_dir.exists()

            # List available files
            if report_dir.exists():
                report['files'] = [f.name for f in report_dir.iterdir()
                                  if f.is_file() and f.name != 'metadata.json']

        return reports

    def get_report(self, report_id: str) -> Optional[Dict]:
        """
        Get full details of a specific report.

        Returns:
            Dict with metadata and file list, or None if not found
        """
        # Find in manifest
        for report in self.manifest.get('reports', []):
            if report.get('report_id') == report_id:
                report_type = report['report_type']
                report_dir = self._get_report_dir(report_type, report_id)

                if not report_dir.exists():
                    return None

                # Load full metadata
                metadata_path = report_dir / 'metadata.json'
                if metadata_path.exists():
                    with open(metadata_path, 'r') as f:
                        metadata = json.load(f)
                else:
                    metadata = report

                # List files
                files = {}
                for f in report_dir.iterdir():
                    if f.is_file() and f.name != 'metadata.json':
                        files[f.name] = {
                            'path': str(f),
                            'size': f.stat().st_size,
                            'modified': datetime.fromtimestamp(f.stat().st_mtime).isoformat()
                        }

                return {
                    'metadata': metadata,
                    'report_dir': str(report_dir),
                    'files': files
                }

        return None

    def delete_report(self, report_id: str) -> bool:
        """
        Delete a report and all its files.

        Returns:
            True if deleted, False if not found
        """
        for i, report in enumerate(self.manifest.get('reports', [])):
            if report.get('report_id') == report_id:
                report_type = report['report_type']
                report_dir = self._get_report_dir(report_type, report_id)

                # Delete directory
                if report_dir.exists():
                    shutil.rmtree(report_dir)

                # Remove from manifest
                self.manifest['reports'].pop(i)
                self._save_manifest()

                return True

        return False

    def get_report_file(self, report_id: str, file_name: str) -> Optional[Path]:
        """
        Get the path to a specific file in a report.

        Returns:
            Path to file or None if not found
        """
        report = self.get_report(report_id)
        if report and file_name in report['files']:
            return Path(report['files'][file_name]['path'])
        return None


# Convenience functions for common report types

def create_project_feasibility_report(
    rm: ReportManager,
    project_id: str,
    project_name: str,
    client: str,
    score: float,
    recommendation: str,
    parameters: Optional[Dict] = None
) -> Dict:
    """Create a project feasibility report entry."""
    return rm.create_report(
        report_type='project',
        client=client,
        title=f"Feasibility Assessment: {project_name}",
        description=f"Queue position {project_id} - Score: {score}/100 - {recommendation}",
        project_id=project_id,
        project_name=project_name,
        parameters=parameters,
        tags=['feasibility', recommendation.lower()]
    )


def create_portfolio_report(
    rm: ReportManager,
    client: str,
    project_count: int,
    total_capacity_gw: float,
    parameters: Optional[Dict] = None
) -> Dict:
    """Create a portfolio analysis report entry."""
    return rm.create_report(
        report_type='portfolio',
        client=client,
        title=f"Portfolio Analysis - {client}",
        description=f"{project_count:,} projects, {total_capacity_gw:.1f} GW total capacity",
        parameters=parameters,
        tags=['portfolio', 'market-overview']
    )


def create_valuation_report(
    rm: ReportManager,
    project_id: str,
    project_name: str,
    client: str,
    fair_value_m: float,
    recommendation: str,
    parameters: Optional[Dict] = None
) -> Dict:
    """Create a PE valuation report entry."""
    return rm.create_report(
        report_type='valuation',
        client=client,
        title=f"PE Valuation: {project_name}",
        description=f"Fair value: ${fair_value_m:.1f}M - {recommendation}",
        project_id=project_id,
        project_name=project_name,
        parameters=parameters,
        tags=['valuation', 'pe', recommendation.lower().replace(' ', '-')]
    )


# CLI for testing
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Report Manager CLI')
    parser.add_argument('command', choices=['list', 'create', 'get', 'delete'])
    parser.add_argument('--type', '-t', help='Report type')
    parser.add_argument('--client', '-c', help='Client name')
    parser.add_argument('--id', help='Report ID')
    parser.add_argument('--limit', '-n', type=int, default=10)

    args = parser.parse_args()

    rm = ReportManager()

    if args.command == 'list':
        reports = rm.list_reports(report_type=args.type, client=args.client, limit=args.limit)
        print(f"\nFound {len(reports)} reports:\n")
        for r in reports:
            print(f"  {r['report_id']}")
            print(f"    Type: {r['report_type']} | Client: {r['client']}")
            print(f"    Title: {r['title']}")
            print(f"    Created: {r['created_at']}")
            if r.get('files'):
                print(f"    Files: {', '.join(r['files'])}")
            print()

    elif args.command == 'get':
        if not args.id:
            print("Error: --id required")
        else:
            report = rm.get_report(args.id)
            if report:
                print(json.dumps(report, indent=2, default=str))
            else:
                print(f"Report not found: {args.id}")

    elif args.command == 'delete':
        if not args.id:
            print("Error: --id required")
        else:
            if rm.delete_report(args.id):
                print(f"Deleted: {args.id}")
            else:
                print(f"Not found: {args.id}")

    elif args.command == 'create':
        # Example create
        report = rm.create_report(
            report_type=args.type or 'project',
            client=args.client or 'Test Client',
            title='Test Report',
            description='Created via CLI'
        )
        print(f"Created: {report['report_id']}")
        print(f"Directory: {report['report_dir']}")
