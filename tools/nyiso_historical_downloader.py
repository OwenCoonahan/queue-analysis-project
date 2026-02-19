#!/usr/bin/env python3
"""
NYISO Historical Queue Downloader

Downloads all historical NYISO interconnection queue files from scraped URLs.

Usage:
    python3 nyiso_historical_downloader.py --list           # List available files
    python3 nyiso_historical_downloader.py --download       # Download all files
    python3 nyiso_historical_downloader.py --download -n 10 # Download first 10 files
"""

import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
import time
import re
import argparse
from typing import List, Dict

CACHE_DIR = Path(__file__).parent / '.cache'
NYISO_HISTORICAL_DIR = CACHE_DIR / 'nyiso_historical'
SCRAPED_FILE = Path('/Users/owencoonahan/Downloads/nyiso.xlsx')


def load_urls() -> pd.DataFrame:
    """Load scraped URLs from Excel file."""
    if not SCRAPED_FILE.exists():
        raise FileNotFoundError(f"Scraped file not found: {SCRAPED_FILE}")

    df = pd.read_excel(SCRAPED_FILE)

    # Parse date from filename
    def extract_date(filename):
        # Try various date formats in the filename
        patterns = [
            r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',  # M/D/YYYY or M-D-YYYY
            r'(\d{1,2})[/-](\d{1,2})[/-](\d{2})',  # M/D/YY or M-D-YY
            r'(\d{2})(\d{2})(\d{2})',              # MMDDYY
            r'(\d{2})(\d{2})(\d{4})',              # MMDDYYYY
        ]

        for pattern in patterns:
            match = re.search(pattern, str(filename))
            if match:
                groups = match.groups()
                if len(groups[2]) == 2:
                    year = int(groups[2])
                    year = 2000 + year if year < 50 else 1900 + year
                else:
                    year = int(groups[2])
                try:
                    return datetime(year, int(groups[0]), int(groups[1]))
                except:
                    pass
        return None

    df['queue_date'] = df['filename'].apply(extract_date)
    df = df.sort_values('queue_date', ascending=False)

    return df


def generate_local_filename(row) -> str:
    """Generate a consistent local filename."""
    if row['queue_date']:
        date_str = row['queue_date'].strftime('%Y-%m-%d')
    else:
        # Extract from filename
        date_str = re.sub(r'[^\d]', '-', str(row['filename']))[:10]

    ext = row['file-icon'] if pd.notna(row['file-icon']) else 'xlsx'
    return f"nyiso_queue_{date_str}.{ext}"


def download_file(url: str, filepath: Path) -> bool:
    """Download a file from URL."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Queue-Analysis/1.0'
        }

        response = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
        response.raise_for_status()

        filepath.write_bytes(response.content)
        return True

    except Exception as e:
        print(f"    Error: {e}")
        return False


def list_files():
    """List all available files."""
    df = load_urls()

    print("="*80)
    print("AVAILABLE NYISO HISTORICAL QUEUE FILES")
    print("="*80)
    print(f"\nTotal files: {len(df)}")
    print(f"Date range: {df['queue_date'].min()} to {df['queue_date'].max()}")
    print()

    # Group by year
    df['year'] = df['queue_date'].dt.year
    year_counts = df.groupby('year').size()

    print("Files by year:")
    for year, count in year_counts.items():
        print(f"  {int(year)}: {count} files")

    print()
    print("Most recent 10 files:")
    for _, row in df.head(10).iterrows():
        date_str = row['queue_date'].strftime('%Y-%m-%d') if row['queue_date'] else 'Unknown'
        print(f"  {date_str}: {row['filename']}")


def download_files(n: int = None, force: bool = False):
    """Download historical files."""
    df = load_urls()

    NYISO_HISTORICAL_DIR.mkdir(parents=True, exist_ok=True)

    if n:
        df = df.head(n)

    stats = {'downloaded': 0, 'skipped': 0, 'failed': 0}

    print(f"Downloading {len(df)} files to {NYISO_HISTORICAL_DIR}")
    print()

    for i, (_, row) in enumerate(df.iterrows()):
        url = row['filename href']
        local_name = generate_local_filename(row)
        filepath = NYISO_HISTORICAL_DIR / local_name

        # Skip if exists
        if filepath.exists() and not force:
            print(f"[{i+1}/{len(df)}] Skipping (exists): {local_name}")
            stats['skipped'] += 1
            continue

        print(f"[{i+1}/{len(df)}] Downloading: {local_name}")

        success = download_file(url, filepath)

        if success:
            stats['downloaded'] += 1
            # Be nice to the server
            time.sleep(0.5)
        else:
            stats['failed'] += 1

    print()
    print("="*60)
    print("DOWNLOAD SUMMARY")
    print("="*60)
    print(f"  Downloaded: {stats['downloaded']}")
    print(f"  Skipped: {stats['skipped']}")
    print(f"  Failed: {stats['failed']}")

    return stats


def main():
    parser = argparse.ArgumentParser(description='NYISO Historical Queue Downloader')
    parser.add_argument('--list', action='store_true', help='List available files')
    parser.add_argument('--download', action='store_true', help='Download files')
    parser.add_argument('-n', type=int, help='Number of files to download (most recent first)')
    parser.add_argument('--force', action='store_true', help='Overwrite existing files')

    args = parser.parse_args()

    if not SCRAPED_FILE.exists():
        print(f"ERROR: Scraped file not found at {SCRAPED_FILE}")
        print("Please provide the nyiso.xlsx file with scraped URLs")
        return 1

    if args.list:
        list_files()
        return 0

    if args.download:
        download_files(n=args.n, force=args.force)
        return 0

    # Default: list
    list_files()
    print("\nUse --download to download files")
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
