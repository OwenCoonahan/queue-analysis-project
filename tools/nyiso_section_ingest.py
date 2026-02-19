#!/usr/bin/env python3
"""
NYISO Section Data Ingestion

Parses and ingests data from downloaded NYISO planning section documents:
- Qualified Developers list (PDF)
- Gold Book generator data (PDF)
- Reliability assessments (PDF)
- DER aggregation data

Usage:
    python3 nyiso_section_ingest.py --list           # List available documents
    python3 nyiso_section_ingest.py --developers     # Parse qualified developers
    python3 nyiso_section_ingest.py --summary        # Show summary of all sections
    python3 nyiso_section_ingest.py --export-devs    # Export developers to CSV
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import re
import json
import warnings
warnings.filterwarnings('ignore')

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    print("Warning: pdfplumber not installed. PDF parsing disabled.")
    print("Run: pip install pdfplumber")

CACHE_DIR = Path(__file__).parent / '.cache'
NYISO_SECTIONS_DIR = CACHE_DIR / 'nyiso_sections'
DATA_DIR = Path(__file__).parent / '.data'


class NYISOSectionIngest:
    """Parse and ingest NYISO section documents."""

    def __init__(self):
        self.sections_dir = NYISO_SECTIONS_DIR
        self.data_dir = DATA_DIR
        self.data_dir.mkdir(exist_ok=True)

    def list_documents(self) -> Dict[str, List[Dict]]:
        """List all downloaded documents by section."""
        documents = {}

        if not self.sections_dir.exists():
            return documents

        for section_dir in self.sections_dir.iterdir():
            if section_dir.is_dir():
                docs = []
                for f in section_dir.iterdir():
                    if f.is_file():
                        docs.append({
                            'name': f.name,
                            'path': str(f),
                            'size_kb': f.stat().st_size / 1024,
                            'modified': datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d'),
                        })
                if docs:
                    documents[section_dir.name] = sorted(docs, key=lambda x: x['name'])

        return documents

    def parse_qualified_developers(self) -> List[Dict]:
        """
        Parse the qualified developers list from PDF.

        Returns list of developer dicts with name and qualification date.
        """
        if not HAS_PDFPLUMBER:
            return []

        # Find the most recent qualified developers PDF
        dev_dir = self.sections_dir / 'qualified_developers'
        if not dev_dir.exists():
            print("Qualified developers directory not found")
            return []

        pdf_files = list(dev_dir.glob('*qualified*.pdf')) + list(dev_dir.glob('*Qualified*.pdf'))
        if not pdf_files:
            print("No qualified developers PDF found")
            return []

        # Use the most recent one
        pdf_path = max(pdf_files, key=lambda p: p.stat().st_mtime)
        print(f"Parsing: {pdf_path.name}")

        developers = []

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                # Extract date from header
                date_match = re.search(r'([A-Za-z]+\s+\d{1,2},\s+\d{4})', text)
                doc_date = date_match.group(1) if date_match else None

                # Find developer names (bullet points starting with •)
                lines = text.split('\n')
                for line in lines:
                    line = line.strip()
                    # Match bullet points or lines that look like company names
                    if line.startswith('•'):
                        name = line[1:].strip()
                        if name and len(name) > 3:
                            developers.append({
                                'name': name,
                                'region': 'NYISO',
                                'qualification_date': doc_date,
                                'source': pdf_path.name,
                            })

        print(f"  Found {len(developers)} qualified developers")
        return developers

    def get_developers_df(self) -> pd.DataFrame:
        """Get qualified developers as DataFrame."""
        developers = self.parse_qualified_developers()
        return pd.DataFrame(developers)

    def parse_gold_book_generators(self) -> pd.DataFrame:
        """
        Parse generator data from Gold Book PDF.

        Note: Gold Book PDFs have complex tables that may not parse cleanly.
        Returns whatever tabular data can be extracted.
        """
        if not HAS_PDFPLUMBER:
            return pd.DataFrame()

        gold_dir = self.sections_dir / 'gold_book'
        if not gold_dir.exists():
            return pd.DataFrame()

        # Find Gold Book PDF
        pdf_files = [f for f in gold_dir.glob('*.pdf') if 'gold' in f.name.lower()]
        if not pdf_files:
            return pd.DataFrame()

        pdf_path = pdf_files[0]
        print(f"Parsing Gold Book: {pdf_path.name}")

        all_tables = []

        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                for table in tables:
                    if table and len(table) > 1:
                        # Try to create DataFrame
                        try:
                            df = pd.DataFrame(table[1:], columns=table[0])
                            df['page'] = i + 1
                            all_tables.append(df)
                        except:
                            continue

        if all_tables:
            combined = pd.concat(all_tables, ignore_index=True)
            print(f"  Extracted {len(combined)} rows from {len(all_tables)} tables")
            return combined

        return pd.DataFrame()

    def get_section_summary(self) -> Dict:
        """Get summary of all downloaded section data."""
        docs = self.list_documents()

        summary = {
            'sections': len(docs),
            'total_documents': sum(len(d) for d in docs.values()),
            'total_size_mb': sum(
                sum(f['size_kb'] for f in files) for files in docs.values()
            ) / 1024,
            'by_section': {},
        }

        for section, files in docs.items():
            summary['by_section'][section] = {
                'documents': len(files),
                'size_mb': sum(f['size_kb'] for f in files) / 1024,
                'files': [f['name'] for f in files],
            }

        return summary

    def export_developers(self, output_path: Optional[str] = None) -> str:
        """Export qualified developers to CSV."""
        df = self.get_developers_df()

        if df.empty:
            return "No developers found"

        if output_path is None:
            output_path = str(self.data_dir / 'nyiso_qualified_developers.csv')

        df.to_csv(output_path, index=False)
        return output_path

    def save_developers_to_db(self) -> Dict:
        """Save qualified developers to main queue.db database."""
        from data_store import DataStore

        developers = self.parse_qualified_developers()
        if not developers:
            return {'success': False, 'error': 'No developers found'}

        db = DataStore()
        stats = db.upsert_qualified_developers(developers, region='NYISO')

        return {'success': True, **stats, 'db_path': str(db.db_path)}


def main():
    import argparse

    parser = argparse.ArgumentParser(description='NYISO Section Data Ingestion')
    parser.add_argument('--list', action='store_true', help='List available documents')
    parser.add_argument('--developers', action='store_true', help='Parse qualified developers')
    parser.add_argument('--summary', action='store_true', help='Show section summary')
    parser.add_argument('--export-devs', type=str, metavar='FILE', nargs='?', const='', help='Export developers to CSV')
    parser.add_argument('--save-db', action='store_true', help='Save developers to database')

    args = parser.parse_args()

    ingest = NYISOSectionIngest()

    if args.list:
        docs = ingest.list_documents()
        print("\n" + "="*60)
        print("NYISO DOWNLOADED DOCUMENTS")
        print("="*60)
        for section, files in docs.items():
            print(f"\n{section.upper().replace('_', ' ')}:")
            for f in files:
                print(f"  {f['name']} ({f['size_kb']:.0f} KB)")
        return 0

    if args.summary:
        summary = ingest.get_section_summary()
        print("\n" + "="*60)
        print("NYISO SECTION SUMMARY")
        print("="*60)
        print(f"Total sections: {summary['sections']}")
        print(f"Total documents: {summary['total_documents']}")
        print(f"Total size: {summary['total_size_mb']:.1f} MB")
        print("\nBy section:")
        for section, info in summary['by_section'].items():
            print(f"  {section}: {info['documents']} docs ({info['size_mb']:.1f} MB)")
        return 0

    if args.developers:
        devs = ingest.parse_qualified_developers()
        print("\n" + "="*60)
        print("NYISO QUALIFIED DEVELOPERS")
        print("="*60)
        for dev in devs:
            print(f"  {dev['name']}")
        print(f"\nTotal: {len(devs)} developers")
        return 0

    if args.export_devs is not None:
        output = args.export_devs if args.export_devs else None
        path = ingest.export_developers(output)
        print(f"Exported to: {path}")
        return 0

    if args.save_db:
        result = ingest.save_developers_to_db()
        if result['success']:
            print(f"Saved {result['added']} developers to {result['db_path']}")
        else:
            print(f"Error: {result['error']}")
        return 0

    # Default: show summary
    summary = ingest.get_section_summary()
    print(f"\nNYISO Sections: {summary['sections']} sections, {summary['total_documents']} documents ({summary['total_size_mb']:.1f} MB)")
    print("\nUse --list, --developers, --summary, --export-devs, or --save-db")
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
