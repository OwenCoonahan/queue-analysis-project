#!/usr/bin/env python3
"""
NYISO Historical Queue Scraper

Downloads all historical interconnection queue files from NYISO website.
Uses Playwright to handle dynamic JavaScript loading.

Usage:
    python3 nyiso_scraper.py --list           # List available files
    python3 nyiso_scraper.py --download       # Download all historical files
    python3 nyiso_scraper.py --download-latest # Download only the latest file
"""

import asyncio
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import argparse

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("Playwright not installed. Run: pip install playwright && playwright install")
    exit(1)

CACHE_DIR = Path(__file__).parent / '.cache'
NYISO_CACHE_DIR = CACHE_DIR / 'nyiso_historical'
NYISO_URL = "https://www.nyiso.com/interconnections"
BASE_URL = "https://www.nyiso.com"


async def get_queue_files() -> List[Dict]:
    """
    Scrape NYISO interconnections page for all queue file links.

    Returns list of dicts with: name, url, date, type (current/historical)
    """
    files = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        print("Loading NYISO interconnections page...")
        await page.goto(NYISO_URL, wait_until='networkidle')

        # Wait for document library to load
        await page.wait_for_timeout(3000)

        # Click to expand "Prior Interconnection Queues" if collapsed
        try:
            prior_header = page.locator("text=Prior Interconnection Queues")
            if await prior_header.count() > 0:
                await prior_header.click()
                await page.wait_for_timeout(1000)
        except:
            pass

        # Find all xlsx links
        links = await page.locator('a[href*=".xlsx"]').all()

        print(f"Found {len(links)} Excel file links")

        for link in links:
            try:
                href = await link.get_attribute('href')
                text = await link.inner_text()

                if not href or not text:
                    continue

                # Build full URL
                if href.startswith('/'):
                    url = BASE_URL + href
                else:
                    url = href

                # Parse date from filename
                # Format: "NYISO Interconnection Queue 12/31/2025" or similar
                date_match = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{4})', text)
                if date_match:
                    date_str = date_match.group(1).replace('-', '/')
                    try:
                        file_date = datetime.strptime(date_str, '%m/%d/%Y')
                    except:
                        file_date = None
                else:
                    file_date = None

                # Determine type
                if 'Prior' in text or (file_date and file_date < datetime.now()):
                    file_type = 'historical'
                else:
                    file_type = 'current'

                # Clean filename
                filename = text.strip().replace('/', '-').replace(' ', '_') + '.xlsx'
                filename = re.sub(r'[^\w\-_.]', '', filename)

                files.append({
                    'name': text.strip(),
                    'filename': filename,
                    'url': url,
                    'date': file_date,
                    'type': file_type,
                })

            except Exception as e:
                print(f"  Error parsing link: {e}")
                continue

        # Also look for cluster studies
        cluster_links = await page.locator('a[href*="Cluster"]').all()
        for link in cluster_links:
            try:
                href = await link.get_attribute('href')
                text = await link.inner_text()

                if not href or '.xlsx' not in href:
                    continue

                url = BASE_URL + href if href.startswith('/') else href

                files.append({
                    'name': text.strip(),
                    'filename': text.strip().replace(' ', '_') + '.xlsx',
                    'url': url,
                    'date': None,
                    'type': 'cluster',
                })
            except:
                continue

        await browser.close()

    # Sort by date (newest first)
    files.sort(key=lambda x: x['date'] or datetime.min, reverse=True)

    return files


async def download_file(url: str, filepath: Path) -> bool:
    """Download a file using Playwright."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            # Set up download handling
            async with page.expect_download() as download_info:
                await page.goto(url)

            download = await download_info.value
            await download.save_as(filepath)
            await browser.close()
            return True

        except Exception as e:
            print(f"  Error downloading: {e}")
            # Try direct download as fallback
            try:
                import httpx
                async with httpx.AsyncClient(follow_redirects=True) as client:
                    response = await client.get(url, timeout=60)
                    if response.status_code == 200:
                        filepath.write_bytes(response.content)
                        await browser.close()
                        return True
            except:
                pass

            await browser.close()
            return False


async def download_all_files(files: List[Dict], download_type: str = 'all') -> Dict:
    """
    Download queue files.

    Args:
        files: List of file dicts from get_queue_files()
        download_type: 'all', 'historical', or 'latest'

    Returns:
        Dict with download statistics
    """
    NYISO_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    stats = {'downloaded': 0, 'skipped': 0, 'failed': 0}

    if download_type == 'latest':
        files = [f for f in files if f['type'] == 'current'][:1]
    elif download_type == 'historical':
        files = [f for f in files if f['type'] == 'historical']

    for i, file_info in enumerate(files):
        filepath = NYISO_CACHE_DIR / file_info['filename']

        # Skip if already exists
        if filepath.exists():
            print(f"  [{i+1}/{len(files)}] Skipping (exists): {file_info['name']}")
            stats['skipped'] += 1
            continue

        print(f"  [{i+1}/{len(files)}] Downloading: {file_info['name']}")

        success = await download_file(file_info['url'], filepath)

        if success:
            stats['downloaded'] += 1
        else:
            stats['failed'] += 1

    return stats


def list_files(files: List[Dict]):
    """Print list of available files."""
    print("\n" + "="*80)
    print("AVAILABLE NYISO QUEUE FILES")
    print("="*80)

    current = [f for f in files if f['type'] == 'current']
    historical = [f for f in files if f['type'] == 'historical']
    cluster = [f for f in files if f['type'] == 'cluster']

    print(f"\nCurrent Queue ({len(current)} file):")
    for f in current:
        date_str = f['date'].strftime('%Y-%m-%d') if f['date'] else 'Unknown'
        print(f"  - {f['name']} ({date_str})")

    print(f"\nHistorical Queues ({len(historical)} files):")
    for f in historical[:20]:  # Show first 20
        date_str = f['date'].strftime('%Y-%m-%d') if f['date'] else 'Unknown'
        print(f"  - {f['name']} ({date_str})")
    if len(historical) > 20:
        print(f"  ... and {len(historical) - 20} more")

    if cluster:
        print(f"\nCluster Studies ({len(cluster)} files):")
        for f in cluster:
            print(f"  - {f['name']}")


async def main():
    parser = argparse.ArgumentParser(description='NYISO Historical Queue Scraper')
    parser.add_argument('--list', action='store_true', help='List available files')
    parser.add_argument('--download', action='store_true', help='Download all historical files')
    parser.add_argument('--download-latest', action='store_true', help='Download only latest file')
    parser.add_argument('--download-historical', action='store_true', help='Download only historical files')

    args = parser.parse_args()

    # Get list of files
    print("Scanning NYISO website for queue files...")
    files = await get_queue_files()

    if not files:
        print("No files found. The page structure may have changed.")
        return 1

    print(f"\nFound {len(files)} total files:")
    print(f"  - Current: {len([f for f in files if f['type'] == 'current'])}")
    print(f"  - Historical: {len([f for f in files if f['type'] == 'historical'])}")
    print(f"  - Cluster: {len([f for f in files if f['type'] == 'cluster'])}")

    if args.list:
        list_files(files)
        return 0

    if args.download:
        print("\nDownloading all files...")
        stats = await download_all_files(files, 'all')
        print(f"\nDownload complete: {stats['downloaded']} downloaded, {stats['skipped']} skipped, {stats['failed']} failed")
        return 0

    if args.download_latest:
        print("\nDownloading latest file...")
        stats = await download_all_files(files, 'latest')
        print(f"\nDownload complete: {stats['downloaded']} downloaded, {stats['skipped']} skipped, {stats['failed']} failed")
        return 0

    if args.download_historical:
        print("\nDownloading historical files...")
        stats = await download_all_files(files, 'historical')
        print(f"\nDownload complete: {stats['downloaded']} downloaded, {stats['skipped']} skipped, {stats['failed']} failed")
        return 0

    # Default: just list
    list_files(files)
    print("\nUse --download to download files, or --list for detailed listing")
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(asyncio.run(main()))
