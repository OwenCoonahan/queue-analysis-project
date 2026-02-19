#!/usr/bin/env python3
"""
NYISO High-Value Section Scrapers

Downloads and parses data from key NYISO planning sections:
- List of Qualified Developers
- Generator Status Updates
- Congested Elements Reports
- Generator Deactivation Notices
- Local Transmission Plans
- DER Geolocation Data

Usage:
    python3 nyiso_section_scrapers.py --list              # List available sections
    python3 nyiso_section_scrapers.py --download-all     # Download all sections
    python3 nyiso_section_scrapers.py --developers       # Download qualified developers
    python3 nyiso_section_scrapers.py --status-updates   # Download generator status
    python3 nyiso_section_scrapers.py --congestion       # Download congestion reports
    python3 nyiso_section_scrapers.py --deactivation     # Download deactivation notices
    python3 nyiso_section_scrapers.py --ltp              # Download local transmission plans
    python3 nyiso_section_scrapers.py --der              # Download DER geolocation data
"""

import asyncio
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import argparse
import requests
import pandas as pd

try:
    from playwright.async_api import async_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False
    print("Warning: Playwright not installed. Some scrapers may not work.")
    print("Run: pip install playwright && playwright install")

CACHE_DIR = Path(__file__).parent / '.cache'
NYISO_DATA_DIR = CACHE_DIR / 'nyiso_sections'

# Section configurations - Updated with correct NYISO URLs
SECTIONS = {
    'developers': {
        'name': 'List of Qualified Developers',
        'url': 'https://www.nyiso.com/developer-qualification-process',
        'description': 'Pre-qualified interconnection developers',
        'file_pattern': r'.*qualified.*developer.*\.(xlsx|xls|pdf)',
        'subdir': 'qualified_developers',
        # Known direct document URLs
        'direct_files': [
            {
                'name': 'List of Qualified Developers',
                'url': 'https://www.nyiso.com/documents/20142/1395552/List-of-Qualified-Developers.pdf',
                'filename': 'list_of_qualified_developers.pdf',
            },
            {
                'name': 'Developer Qualification Process',
                'url': 'https://www.nyiso.com/documents/20142/1395552/Developer-Qualification-Process.pdf',
                'filename': 'developer_qualification_process.pdf',
            },
        ],
    },
    'gold_book': {
        'name': 'Gold Book (Load & Capacity Data)',
        'url': 'https://www.nyiso.com/planning',
        'description': 'Annual load and capacity data report with generator inventory',
        'file_pattern': r'.*gold.*book.*\.(xlsx|xls|pdf)',
        'subdir': 'gold_book',
        'direct_files': [
            {
                'name': '2025 Gold Book',
                'url': 'https://www.nyiso.com/documents/20142/2226333/2025-Gold-Book-Public.pdf',
                'filename': '2025_gold_book.pdf',
            },
        ],
    },
    'reliability': {
        'name': 'Comprehensive Reliability Plan',
        'url': 'https://www.nyiso.com/planning',
        'description': 'Long-term system reliability assessments',
        'file_pattern': r'.*(reliability|crp).*\.(xlsx|xls|pdf)',
        'subdir': 'reliability_plans',
        'direct_files': [
            {
                'name': '2025-2034 Comprehensive Reliability Plan',
                'url': 'https://www.nyiso.com/documents/20142/2248481/2025-2034-Comprehensive-Reliability-Plan.pdf',
                'filename': '2025_2034_comprehensive_reliability_plan.pdf',
            },
        ],
    },
    'congestion': {
        'name': 'Congestion Reports',
        'url': 'https://www.nyiso.com/transmission-congestion-contracts-tcc',
        'description': 'Transmission congestion analysis and binding constraints',
        'file_pattern': r'.*(congestion|constraint|binding|tcc).*\.(xlsx|xls|csv|pdf)',
        'subdir': 'congestion_reports',
    },
    'reliability_compliance': {
        'name': 'Reliability Compliance Data',
        'url': 'https://www.nyiso.com/planning-reliability-compliance',
        'description': 'NERC reliability compliance reports and data',
        'file_pattern': r'.*(reliability|compliance|nerc).*\.(xlsx|xls|pdf)',
        'subdir': 'reliability_compliance',
    },
    'ltp': {
        'name': 'Local Transmission Plans',
        'url': 'https://www.nyiso.com/short-term-reliability-process',
        'description': 'Short-term reliability and local transmission planning',
        'file_pattern': r'.*(local.*transmission|ltp|reliability).*\.(xlsx|xls|pdf)',
        'subdir': 'local_transmission_plans',
    },
    'der': {
        'name': 'DER & Aggregations',
        'url': 'https://www.nyiso.com/der-aggregations',
        'description': 'Distributed energy resources and aggregation data',
        'file_pattern': r'.*(der|distributed|aggregation).*\.(xlsx|xls|csv|json)',
        'subdir': 'der_data',
    },
}


class NYISOSectionScraper:
    """Scrape data from NYISO planning sections."""

    def __init__(self):
        self.data_dir = NYISO_DATA_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)

    async def scrape_section(self, section_key: str) -> List[Dict]:
        """
        Scrape a specific section for downloadable files.

        Returns list of file info dicts.
        """
        if section_key not in SECTIONS:
            print(f"Unknown section: {section_key}")
            return []

        config = SECTIONS[section_key]
        print(f"\nScraping: {config['name']}")
        print(f"URL: {config['url']}")

        files = []

        if HAS_PLAYWRIGHT:
            files = await self._scrape_with_playwright(config)
        else:
            files = self._scrape_with_requests(config)

        print(f"  Found {len(files)} files")
        return files

    async def _scrape_with_playwright(self, config: Dict) -> List[Dict]:
        """Scrape using Playwright for JS-heavy pages."""
        files = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                await page.goto(config['url'], wait_until='networkidle', timeout=60000)
                # Wait longer for NYISO's document library to load
                await page.wait_for_timeout(5000)

                # Try to click on any "Documents" or "Files" tabs/sections
                for selector in [
                    'text=Documents',
                    'text=Files',
                    'text=Download',
                    '.document-library',
                    '[class*="document"]',
                    '[class*="portlet"]',
                ]:
                    try:
                        elem = page.locator(selector).first
                        if await elem.count() > 0:
                            await elem.click()
                            await page.wait_for_timeout(2000)
                    except:
                        pass

                # Scroll down to trigger lazy loading
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await page.wait_for_timeout(2000)

                # Find all document links - more comprehensive selectors
                links = await page.locator('a[href*=".xlsx"], a[href*=".xls"], a[href*=".pdf"], a[href*=".csv"], a[href*="/documents/"], a[href*="/document_library/"]').all()

                for link in links:
                    try:
                        href = await link.get_attribute('href')
                        text = await link.inner_text()

                        if not href:
                            continue

                        # Build full URL
                        if href.startswith('/'):
                            url = f"https://www.nyiso.com{href}"
                        elif not href.startswith('http'):
                            url = f"https://www.nyiso.com/{href}"
                        else:
                            url = href

                        # Get file extension
                        ext_match = re.search(r'\.(xlsx|xls|pdf|csv)(\?|$)', url.lower())
                        ext = ext_match.group(1) if ext_match else 'unknown'

                        # Generate filename
                        filename = self._generate_filename(text or href, ext)

                        files.append({
                            'name': text.strip() if text else filename,
                            'url': url,
                            'filename': filename,
                            'extension': ext,
                        })
                    except Exception as e:
                        continue

                # Also try to expand any collapsible sections and NYISO's Liferay panels
                try:
                    expandables = await page.locator('.accordion-button, .collapse-trigger, [data-toggle="collapse"], .panel-heading, .panel-title a, .toggle-content').all()
                    for exp in expandables:
                        try:
                            await exp.click()
                            await page.wait_for_timeout(500)
                        except:
                            pass

                    # Re-scan for links with broader selectors
                    links = await page.locator('a[href*=".xlsx"], a[href*=".xls"], a[href*=".pdf"], a[href*=".csv"], a[href*="/documents/"], a.document-link, a.icon-download').all()
                    for link in links:
                        try:
                            href = await link.get_attribute('href')
                            text = await link.inner_text()
                            if href:
                                url = href if href.startswith('http') else f"https://www.nyiso.com{href}"
                                ext_match = re.search(r'\.(xlsx|xls|pdf|csv)(\?|$)', url.lower())
                                ext = ext_match.group(1) if ext_match else 'unknown'
                                filename = self._generate_filename(text or href, ext)

                                # Avoid duplicates
                                if not any(f['url'] == url for f in files):
                                    files.append({
                                        'name': text.strip() if text else filename,
                                        'url': url,
                                        'filename': filename,
                                        'extension': ext,
                                    })
                        except:
                            continue
                except:
                    pass

                # Try to find NYISO Liferay document library links
                try:
                    # NYISO uses Liferay CMS - look for document entries
                    doc_entries = await page.locator('.document-entry, .entry-title a, [class*="document"] a').all()
                    for entry in doc_entries:
                        try:
                            href = await entry.get_attribute('href')
                            text = await entry.inner_text()
                            if href and ('/documents/' in href or '.xls' in href.lower() or '.pdf' in href.lower()):
                                url = href if href.startswith('http') else f"https://www.nyiso.com{href}"
                                ext_match = re.search(r'\.(xlsx|xls|pdf|csv)(\?|$)', url.lower())
                                ext = ext_match.group(1) if ext_match else 'xlsx'
                                filename = self._generate_filename(text or href, ext)

                                if not any(f['url'] == url for f in files):
                                    files.append({
                                        'name': text.strip() if text else filename,
                                        'url': url,
                                        'filename': filename,
                                        'extension': ext,
                                    })
                        except:
                            continue
                except:
                    pass

            except Exception as e:
                print(f"  Error scraping page: {e}")

            await browser.close()

        return files

    def _scrape_with_requests(self, config: Dict) -> List[Dict]:
        """Scrape using requests for simple pages."""
        files = []

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Queue-Analysis/1.0'
            }
            response = requests.get(config['url'], headers=headers, timeout=30)
            response.raise_for_status()

            # Simple regex to find document links
            pattern = r'href=["\']([^"\']*\.(?:xlsx|xls|pdf|csv)[^"\']*)["\']'
            matches = re.findall(pattern, response.text, re.IGNORECASE)

            for href in matches:
                if href.startswith('/'):
                    url = f"https://www.nyiso.com{href}"
                elif not href.startswith('http'):
                    url = f"https://www.nyiso.com/{href}"
                else:
                    url = href

                ext_match = re.search(r'\.(xlsx|xls|pdf|csv)(\?|$)', url.lower())
                ext = ext_match.group(1) if ext_match else 'unknown'
                filename = self._generate_filename(href, ext)

                files.append({
                    'name': filename,
                    'url': url,
                    'filename': filename,
                    'extension': ext,
                })

        except Exception as e:
            print(f"  Error: {e}")

        return files

    def _generate_filename(self, text: str, ext: str) -> str:
        """Generate a clean filename from text."""
        # Clean up the text
        clean = re.sub(r'[^\w\s\-_]', '', text)
        clean = re.sub(r'\s+', '_', clean)
        clean = clean[:100]  # Limit length

        if not clean:
            clean = f"nyiso_doc_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        return f"{clean}.{ext}"

    def download_file(self, url: str, filepath: Path) -> bool:
        """Download a file from URL."""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Queue-Analysis/1.0'
            }
            response = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
            response.raise_for_status()

            # Try to determine correct extension from Content-Type
            content_type = response.headers.get('Content-Type', '').lower()
            if filepath.suffix == '.unknown':
                ext_map = {
                    'application/pdf': '.pdf',
                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
                    'application/vnd.ms-excel': '.xls',
                    'text/csv': '.csv',
                    'application/json': '.json',
                }
                for ct, ext in ext_map.items():
                    if ct in content_type:
                        filepath = filepath.with_suffix(ext)
                        break

            filepath.write_bytes(response.content)
            return True

        except Exception as e:
            print(f"    Error downloading: {e}")
            return False

    async def download_section(self, section_key: str, force: bool = False) -> Dict:
        """Download all files from a section."""
        if section_key not in SECTIONS:
            return {'error': f'Unknown section: {section_key}'}

        config = SECTIONS[section_key]
        section_dir = self.data_dir / config['subdir']
        section_dir.mkdir(parents=True, exist_ok=True)

        # Start with known direct files if available
        files = config.get('direct_files', []).copy()

        # Then scrape for additional files
        scraped_files = await self.scrape_section(section_key)
        files.extend(scraped_files)

        if not files:
            print("  No files found to download")
            return {'downloaded': 0, 'skipped': 0, 'failed': 0}

        stats = {'downloaded': 0, 'skipped': 0, 'failed': 0}

        for i, file_info in enumerate(files):
            filepath = section_dir / file_info['filename']

            # Skip if exists
            if filepath.exists() and not force:
                print(f"  [{i+1}/{len(files)}] Skipping (exists): {file_info['filename']}")
                stats['skipped'] += 1
                continue

            print(f"  [{i+1}/{len(files)}] Downloading: {file_info['filename']}")

            if self.download_file(file_info['url'], filepath):
                stats['downloaded'] += 1
            else:
                stats['failed'] += 1

        return stats

    async def download_all(self, force: bool = False) -> Dict:
        """Download all sections."""
        total_stats = {'downloaded': 0, 'skipped': 0, 'failed': 0}

        for section_key in SECTIONS:
            stats = await self.download_section(section_key, force=force)
            if 'error' not in stats:
                total_stats['downloaded'] += stats['downloaded']
                total_stats['skipped'] += stats['skipped']
                total_stats['failed'] += stats['failed']

        return total_stats


# Specialized parsers for each section type

class QualifiedDevelopersParser:
    """Parse the qualified developers list."""

    def parse(self, filepath: Path) -> pd.DataFrame:
        """Parse qualified developers Excel file."""
        try:
            df = pd.read_excel(filepath)

            # Standardize columns
            col_map = {
                'Developer Name': 'developer',
                'Company Name': 'developer',
                'Qualification Date': 'qualified_date',
                'Date': 'qualified_date',
                'Status': 'status',
                'Contact': 'contact',
            }

            rename_map = {k: v for k, v in col_map.items() if k in df.columns}
            df = df.rename(columns=rename_map)

            return df

        except Exception as e:
            print(f"Error parsing {filepath}: {e}")
            return pd.DataFrame()


class DeactivationNoticesParser:
    """Parse generator deactivation notices."""

    def parse(self, filepath: Path) -> pd.DataFrame:
        """Parse deactivation notices Excel file."""
        try:
            df = pd.read_excel(filepath)

            # Standardize columns
            col_map = {
                'Generator': 'generator_name',
                'Generator Name': 'generator_name',
                'Unit': 'unit',
                'Capacity (MW)': 'capacity_mw',
                'MW': 'capacity_mw',
                'Deactivation Date': 'deactivation_date',
                'Retirement Date': 'deactivation_date',
                'Zone': 'zone',
                'Utility': 'utility',
                'Reason': 'reason',
            }

            rename_map = {k: v for k, v in col_map.items() if k in df.columns}
            df = df.rename(columns=rename_map)

            return df

        except Exception as e:
            print(f"Error parsing {filepath}: {e}")
            return pd.DataFrame()


class CongestionReportsParser:
    """Parse congestion analysis reports."""

    def parse(self, filepath: Path) -> pd.DataFrame:
        """Parse congestion reports Excel file."""
        try:
            df = pd.read_excel(filepath)

            # Standardize columns
            col_map = {
                'Element': 'element',
                'Constraint': 'constraint',
                'Binding Hours': 'binding_hours',
                'Hours': 'binding_hours',
                'Congestion Cost': 'congestion_cost',
                'Cost ($)': 'congestion_cost',
                'Zone': 'zone',
                'From': 'from_bus',
                'To': 'to_bus',
            }

            rename_map = {k: v for k, v in col_map.items() if k in df.columns}
            df = df.rename(columns=rename_map)

            return df

        except Exception as e:
            print(f"Error parsing {filepath}: {e}")
            return pd.DataFrame()


def list_sections():
    """Print available sections."""
    print("\n" + "="*70)
    print("AVAILABLE NYISO SECTIONS")
    print("="*70)

    for key, config in SECTIONS.items():
        print(f"\n  --{key.replace('_', '-')}")
        print(f"    Name: {config['name']}")
        print(f"    URL: {config['url']}")
        print(f"    Description: {config['description']}")


async def main():
    parser = argparse.ArgumentParser(description='NYISO Section Scrapers')
    parser.add_argument('--list', action='store_true', help='List available sections')
    parser.add_argument('--download-all', action='store_true', help='Download all sections')
    parser.add_argument('--developers', action='store_true', help='Download qualified developers')
    parser.add_argument('--gold-book', action='store_true', help='Download Gold Book (generator data)')
    parser.add_argument('--reliability', action='store_true', help='Download reliability plans')
    parser.add_argument('--congestion', action='store_true', help='Download congestion reports')
    parser.add_argument('--reliability-compliance', action='store_true', help='Download reliability compliance')
    parser.add_argument('--ltp', action='store_true', help='Download local transmission plans')
    parser.add_argument('--der', action='store_true', help='Download DER data')
    parser.add_argument('--section', type=str, help='Download specific section by key')
    parser.add_argument('--force', action='store_true', help='Overwrite existing files')

    args = parser.parse_args()

    if args.list:
        list_sections()
        return 0

    scraper = NYISOSectionScraper()

    if args.download_all:
        print("Downloading all NYISO sections...")
        stats = await scraper.download_all(force=args.force)
        print(f"\nTotal: {stats['downloaded']} downloaded, {stats['skipped']} skipped, {stats['failed']} failed")
        return 0

    # Individual sections
    sections_to_download = []
    if args.developers:
        sections_to_download.append('developers')
    if args.gold_book:
        sections_to_download.append('gold_book')
    if args.reliability:
        sections_to_download.append('reliability')
    if args.congestion:
        sections_to_download.append('congestion')
    if args.reliability_compliance:
        sections_to_download.append('reliability_compliance')
    if args.ltp:
        sections_to_download.append('ltp')
    if args.der:
        sections_to_download.append('der')
    if args.section:
        sections_to_download.append(args.section)

    if sections_to_download:
        for section in sections_to_download:
            stats = await scraper.download_section(section, force=args.force)
            print(f"\n{section}: {stats.get('downloaded', 0)} downloaded, {stats.get('skipped', 0)} skipped, {stats.get('failed', 0)} failed")
        return 0

    # Default: list sections
    list_sections()
    print("\nUse --download-all or specific section flags to download")
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(asyncio.run(main()))
