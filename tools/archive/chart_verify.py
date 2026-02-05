#!/usr/bin/env python3
"""
Chart Verification Module

Uses Playwright to render HTML charts and capture screenshots for visual verification.
This allows Claude to actually see and verify the generated charts.

Usage:
    from chart_verify import ChartVerifier

    verifier = ChartVerifier()

    # Verify a single chart
    result = verifier.verify_chart('charts/cost_scatter.html')

    # Verify all charts in a directory
    results = verifier.verify_all_charts('charts/')

    # Get a viewable screenshot (for Claude to see)
    screenshot_path = verifier.capture_chart('charts/cost_scatter.html')
"""

import os
import base64
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

# Try to import Playwright
try:
    from playwright.sync_api import sync_playwright, Browser, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("Warning: Playwright not installed. Run: pip install playwright && playwright install chromium")


class ChartVerifier:
    """Verify and capture screenshots of HTML charts using Playwright."""

    def __init__(self, output_dir: str = None):
        """
        Initialize the chart verifier.

        Args:
            output_dir: Directory to save verification screenshots
        """
        self.output_dir = Path(output_dir) if output_dir else Path(__file__).parent / 'chart_screenshots'
        self.output_dir.mkdir(exist_ok=True)

        if not PLAYWRIGHT_AVAILABLE:
            raise RuntimeError("Playwright is not installed. Run: pip install playwright && playwright install chromium")

    def capture_chart(
        self,
        html_path: str,
        output_name: str = None,
        width: int = 1200,
        height: int = 800,
        wait_ms: int = 1000,
    ) -> str:
        """
        Capture a screenshot of an HTML chart.

        Args:
            html_path: Path to the HTML chart file
            output_name: Name for the output screenshot (without extension)
            width: Viewport width
            height: Viewport height
            wait_ms: Time to wait for chart to render (ms)

        Returns:
            Path to the saved screenshot
        """
        html_path = Path(html_path)
        if not html_path.exists():
            raise FileNotFoundError(f"Chart not found: {html_path}")

        # Generate output name
        if output_name is None:
            output_name = f"verify_{html_path.stem}_{datetime.now().strftime('%H%M%S')}"

        output_path = self.output_dir / f"{output_name}.png"

        with sync_playwright() as p:
            # Launch headless browser
            browser = p.chromium.launch(headless=True)

            # Create page with specific viewport
            page = browser.new_page(viewport={'width': width, 'height': height})

            # Navigate to the local HTML file
            file_url = f"file://{html_path.absolute()}"
            page.goto(file_url)

            # Wait for chart to render
            page.wait_for_timeout(wait_ms)

            # Capture screenshot
            page.screenshot(path=str(output_path), full_page=False)

            browser.close()

        return str(output_path)

    def capture_chart_as_base64(
        self,
        html_path: str,
        width: int = 1200,
        height: int = 800,
        wait_ms: int = 1000,
    ) -> str:
        """
        Capture a chart screenshot and return as base64 string.

        Useful for embedding in reports or sending to APIs.
        """
        html_path = Path(html_path)
        if not html_path.exists():
            raise FileNotFoundError(f"Chart not found: {html_path}")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={'width': width, 'height': height})

            file_url = f"file://{html_path.absolute()}"
            page.goto(file_url)
            page.wait_for_timeout(wait_ms)

            # Get screenshot as bytes
            screenshot_bytes = page.screenshot(full_page=False)

            browser.close()

        return base64.b64encode(screenshot_bytes).decode('utf-8')

    def verify_chart(
        self,
        html_path: str,
        checks: List[str] = None,
    ) -> Dict:
        """
        Verify a chart by capturing it and running basic checks.

        Args:
            html_path: Path to the HTML chart file
            checks: List of checks to run (default: all)

        Returns:
            Dictionary with verification results
        """
        html_path = Path(html_path)
        result = {
            'chart': html_path.name,
            'path': str(html_path),
            'exists': html_path.exists(),
            'checks': {},
            'screenshot': None,
            'status': 'unknown',
        }

        if not html_path.exists():
            result['status'] = 'error'
            result['error'] = 'File not found'
            return result

        # File size check
        file_size = html_path.stat().st_size
        result['file_size_kb'] = file_size / 1024
        result['checks']['file_not_empty'] = file_size > 1000  # At least 1KB

        # Capture screenshot
        try:
            screenshot_path = self.capture_chart(html_path)
            result['screenshot'] = screenshot_path

            # Check screenshot was created and has content
            screenshot_size = Path(screenshot_path).stat().st_size
            result['screenshot_size_kb'] = screenshot_size / 1024
            result['checks']['screenshot_created'] = screenshot_size > 5000  # At least 5KB

        except Exception as e:
            result['checks']['screenshot_created'] = False
            result['error'] = str(e)

        # Read HTML and check for key elements
        try:
            html_content = html_path.read_text()

            # Check for Plotly-specific elements
            result['checks']['has_plotly'] = 'plotly' in html_content.lower()
            result['checks']['has_data'] = 'data' in html_content and 'trace' in html_content.lower()
            result['checks']['has_layout'] = 'layout' in html_content.lower()

            # Check for common error indicators
            result['checks']['no_error_message'] = 'error' not in html_content.lower() or 'error' in html_content.count < 3

        except Exception as e:
            result['error'] = str(e)

        # Overall status
        all_checks_passed = all(result['checks'].values())
        result['status'] = 'pass' if all_checks_passed else 'fail'

        return result

    def verify_all_charts(
        self,
        chart_dir: str = None,
        pattern: str = "*.html",
    ) -> Dict:
        """
        Verify all HTML charts in a directory.

        Args:
            chart_dir: Directory containing charts (default: charts/)
            pattern: Glob pattern for chart files

        Returns:
            Dictionary with results for all charts
        """
        if chart_dir is None:
            chart_dir = Path(__file__).parent / 'charts'
        else:
            chart_dir = Path(chart_dir)

        results = {
            'directory': str(chart_dir),
            'timestamp': datetime.now().isoformat(),
            'charts': [],
            'summary': {
                'total': 0,
                'passed': 0,
                'failed': 0,
            }
        }

        # Find all HTML files
        html_files = list(chart_dir.glob(pattern))
        results['summary']['total'] = len(html_files)

        for html_file in html_files:
            chart_result = self.verify_chart(html_file)
            results['charts'].append(chart_result)

            if chart_result['status'] == 'pass':
                results['summary']['passed'] += 1
            else:
                results['summary']['failed'] += 1

        return results

    def create_verification_report(
        self,
        chart_dir: str = None,
        output_file: str = None,
    ) -> str:
        """
        Create a markdown verification report with embedded screenshots.

        Args:
            chart_dir: Directory containing charts
            output_file: Path for the output report

        Returns:
            Path to the created report
        """
        results = self.verify_all_charts(chart_dir)

        if output_file is None:
            output_file = self.output_dir / f"verification_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

        report_lines = [
            "# Chart Verification Report",
            f"\n**Generated:** {results['timestamp']}",
            f"**Directory:** {results['directory']}",
            f"\n## Summary",
            f"- **Total Charts:** {results['summary']['total']}",
            f"- **Passed:** {results['summary']['passed']}",
            f"- **Failed:** {results['summary']['failed']}",
            "\n---\n",
        ]

        for chart in results['charts']:
            status_emoji = "✅" if chart['status'] == 'pass' else "❌"
            report_lines.append(f"## {status_emoji} {chart['chart']}")
            report_lines.append(f"\n**Status:** {chart['status'].upper()}")
            report_lines.append(f"**File Size:** {chart.get('file_size_kb', 0):.1f} KB")

            if chart.get('screenshot'):
                report_lines.append(f"\n**Screenshot:** {chart['screenshot']}")
                # Use relative path for markdown
                rel_path = os.path.relpath(chart['screenshot'], Path(output_file).parent)
                report_lines.append(f"\n![{chart['chart']}]({rel_path})")

            report_lines.append("\n**Checks:**")
            for check, passed in chart.get('checks', {}).items():
                check_emoji = "✓" if passed else "✗"
                report_lines.append(f"- {check_emoji} {check}")

            if chart.get('error'):
                report_lines.append(f"\n**Error:** {chart['error']}")

            report_lines.append("\n---\n")

        report_content = "\n".join(report_lines)

        output_path = Path(output_file)
        output_path.write_text(report_content)

        return str(output_path)


def verify_charts(chart_dir: str = None) -> Dict:
    """
    Convenience function to verify all charts.

    Args:
        chart_dir: Directory containing charts

    Returns:
        Verification results
    """
    verifier = ChartVerifier()
    return verifier.verify_all_charts(chart_dir)


def capture_for_review(html_path: str) -> str:
    """
    Capture a chart screenshot for Claude to review.

    Args:
        html_path: Path to HTML chart

    Returns:
        Path to the screenshot
    """
    verifier = ChartVerifier()
    return verifier.capture_chart(html_path)


# CLI interface
if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Verify HTML charts using Playwright")
    parser.add_argument('--dir', '-d', default='charts', help='Directory containing charts')
    parser.add_argument('--chart', '-c', help='Verify a specific chart file')
    parser.add_argument('--report', '-r', action='store_true', help='Generate verification report')
    parser.add_argument('--json', '-j', action='store_true', help='Output as JSON')

    args = parser.parse_args()

    try:
        verifier = ChartVerifier()

        if args.chart:
            # Verify single chart
            result = verifier.verify_chart(args.chart)

            if args.json:
                print(json.dumps(result, indent=2))
            else:
                status = "✅ PASS" if result['status'] == 'pass' else "❌ FAIL"
                print(f"\n{status}: {result['chart']}")
                print(f"  Screenshot: {result.get('screenshot', 'N/A')}")
                for check, passed in result.get('checks', {}).items():
                    print(f"  {'✓' if passed else '✗'} {check}")

        elif args.report:
            # Generate full report
            report_path = verifier.create_verification_report(args.dir)
            print(f"\nVerification report created: {report_path}")

        else:
            # Verify all charts
            results = verifier.verify_all_charts(args.dir)

            if args.json:
                print(json.dumps(results, indent=2))
            else:
                print(f"\n{'='*60}")
                print("CHART VERIFICATION RESULTS")
                print(f"{'='*60}")
                print(f"\nDirectory: {results['directory']}")
                print(f"Total: {results['summary']['total']} | "
                      f"Passed: {results['summary']['passed']} | "
                      f"Failed: {results['summary']['failed']}")

                for chart in results['charts']:
                    status = "✅" if chart['status'] == 'pass' else "❌"
                    print(f"\n{status} {chart['chart']}")
                    if chart.get('screenshot'):
                        print(f"   Screenshot: {chart['screenshot']}")

    except Exception as e:
        print(f"Error: {e}")
        exit(1)
