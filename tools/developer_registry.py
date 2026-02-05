#!/usr/bin/env python3
"""
Developer Name Canonicalization and Registry

Normalizes developer/company names to canonical forms and tracks
parent company relationships for accurate developer analytics.

Features:
- Normalizes case, suffixes (LLC, Inc, Corp)
- Maps subsidiaries to parent companies
- Handles common variations and typos
- Maintains canonical developer registry

Usage:
    from developer_registry import DeveloperRegistry

    registry = DeveloperRegistry()
    registry.load_from_database()

    # Get canonical name
    canonical = registry.canonicalize("Entergy Arkansas LLC")
    # Returns: "Entergy Arkansas"

    # Apply to database
    registry.apply_canonicalization()
"""

import sqlite3
import pandas as pd
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / '.data'


# Known parent company mappings (subsidiary -> parent)
PARENT_COMPANY_MAP = {
    # NextEra / FPL
    'fpl': 'NextEra Energy',
    'fpl energy': 'NextEra Energy',
    'fpl group': 'NextEra Energy',
    'nextera energy resources': 'NextEra Energy',
    'nextera energy power marketing': 'NextEra Energy',
    'nextera energy point beach': 'NextEra Energy',
    'nextera energy transmission': 'NextEra Energy',
    'nextera solar': 'NextEra Energy',

    # Dominion
    'dominion renewable energy': 'Dominion Energy',
    'virginia electric & power': 'Dominion Energy',
    'virginia electric and power': 'Dominion Energy',
    'dominion energy virginia': 'Dominion Energy',
    'dominion energy south carolina': 'Dominion Energy',

    # Duke Energy
    'duke energy indiana': 'Duke Energy',
    'duke energy progress': 'Duke Energy',
    'duke energy carolinas': 'Duke Energy',
    'duke energy florida': 'Duke Energy',
    'duke energy kentucky': 'Duke Energy',
    'duke energy ohio': 'Duke Energy',

    # Entergy
    'entergy arkansas': 'Entergy',
    'entergy louisiana': 'Entergy',
    'entergy mississippi': 'Entergy',
    'entergy texas': 'Entergy',
    'entergy new orleans': 'Entergy',
    'entergy services': 'Entergy',

    # Southern Company
    'alabama power': 'Southern Company',
    'georgia power': 'Southern Company',
    'gulf power': 'Southern Company',
    'mississippi power': 'Southern Company',
    'southern company services': 'Southern Company',

    # AES
    'aes alamitos': 'AES Corporation',
    'aes distributed energy': 'AES Corporation',
    'aes southland': 'AES Corporation',
    'aes redondo beach': 'AES Corporation',
    'indianapolis power & light': 'AES Corporation',
    'dayton power and light': 'AES Corporation',

    # Berkshire Hathaway Energy
    'pacificorp': 'Berkshire Hathaway Energy',
    'pacificorp energy': 'Berkshire Hathaway Energy',
    'midamerican energy': 'Berkshire Hathaway Energy',
    'nv energy': 'Berkshire Hathaway Energy',
    'rocky mountain power': 'Berkshire Hathaway Energy',

    # Xcel Energy
    'northern states power': 'Xcel Energy',
    'southwestern public service': 'Xcel Energy',
    'public service company of colorado': 'Xcel Energy',

    # Evergy
    'evergy metro': 'Evergy',
    'evergy kansas central': 'Evergy',
    'westar energy': 'Evergy',
    'kansas city power & light': 'Evergy',

    # Ameren
    'ameren missouri': 'Ameren',
    'ameren illinois': 'Ameren',
    'union electric': 'Ameren',

    # Invenergy
    'invenergy services': 'Invenergy',
    'invenergy thermal': 'Invenergy',
    'invenergy wind': 'Invenergy',
    'invenergy solar': 'Invenergy',
    'invenergy energy marketing': 'Invenergy',

    # EDF Renewables
    'edf renewables development': 'EDF Renewables',
    'edf renewable energy': 'EDF Renewables',
    'edf renewable development': 'EDF Renewables',

    # Enel
    'enel green power north america': 'Enel',
    'enel green power na': 'Enel',
    'enel x': 'Enel',

    # Avangrid
    'avangrid renewables': 'Avangrid',
    'iberdrola renewables': 'Avangrid',

    # Pattern Energy
    'pattern energy': 'Pattern Energy Group',
    'pattern energy group': 'Pattern Energy Group',

    # Orsted
    'orsted north america': 'Orsted',
    'orsted wind power': 'Orsted',

    # BP
    'bp wind energy north america': 'BP',
    'bp alternative energy': 'BP',
    'lightsource bp': 'BP',

    # First Solar
    'first solar electric': 'First Solar',
    'first solar development': 'First Solar',

    # Recurrent Energy
    'recurrent energy': 'Canadian Solar',

    # TVA
    'tennessee valley authority': 'TVA',
}


@dataclass
class CanonicalDeveloper:
    """Represents a canonical developer entry."""
    canonical_name: str
    parent_company: Optional[str]
    aliases: Set[str]
    project_count: int


class DeveloperRegistry:
    """Developer name canonicalization and registry."""

    # Suffixes to remove for normalization (must have space or comma before)
    SUFFIXES = [
        r',\s*llc\.?$',
        r'\s+llc\.?$',
        r',\s*l\.l\.c\.?$',
        r'\s+l\.l\.c\.?$',
        r',\s*inc\.?$',
        r'\s+inc\.?$',
        r',\s*incorporated$',
        r'\s+incorporated$',
        r',\s*corp\.?$',           # Only remove "corp" after comma
        r'\s+corp\.?$',            # Or after space (not part of word like PacifiCorp)
        r',\s*corporation$',
        r'\s+corporation$',
        r',\s*co\.?$',
        r'\s+co\.?$',
        r',\s*company$',
        r'\s+company$',
        r',\s*ltd\.?$',
        r'\s+ltd\.?$',
        r',\s*limited$',
        r'\s+limited$',
        r',\s*lp\.?$',
        r'\s+lp\.?$',
        r',\s*l\.p\.?$',
        r'\s+l\.p\.?$',
        r',\s*plc\.?$',
        r'\s+plc\.?$',
        r'\s*\([^)]*\)$',  # Remove parenthetical notes
    ]

    # Names that should NOT have suffixes removed (proper names containing suffix-like strings)
    PRESERVE_NAMES = {
        'pacificorp', 'bancorp', 'amcorp', 'gencorp', 'suncorp',
    }

    def __init__(self):
        self.developers: Dict[str, CanonicalDeveloper] = {}
        self.alias_map: Dict[str, str] = {}  # alias -> canonical
        self.parent_map: Dict[str, str] = {}  # canonical -> parent

    def normalize(self, name: str) -> str:
        """
        Normalize a developer name (lowercase, remove suffixes, clean whitespace).

        This is the basic normalization - not the full canonicalization.
        """
        if not name or pd.isna(name):
            return ""

        name = str(name).strip()

        # Lowercase
        normalized = name.lower()

        # Check if this is a preserved name (don't remove suffixes)
        base_name = normalized.split()[0] if normalized else ''
        should_preserve = any(p in normalized.replace(' ', '') for p in self.PRESERVE_NAMES)

        if not should_preserve:
            # Remove suffixes
            for suffix in self.SUFFIXES:
                normalized = re.sub(suffix, '', normalized, flags=re.IGNORECASE)

        # Clean up punctuation
        normalized = re.sub(r'[,.]$', '', normalized)

        # Normalize whitespace
        normalized = ' '.join(normalized.split())

        return normalized.strip()

    def canonicalize(self, name: str) -> str:
        """
        Get canonical name for a developer.

        Returns the standardized canonical name, applying:
        1. Basic normalization
        2. Alias mapping
        3. Title case formatting
        """
        if not name or pd.isna(name):
            return ""

        normalized = self.normalize(name)

        # Check alias map
        if normalized in self.alias_map:
            return self.alias_map[normalized]

        # Check parent company map
        if normalized in PARENT_COMPANY_MAP:
            return PARENT_COMPANY_MAP[normalized]

        # Return title-cased normalized name
        return self._title_case(normalized)

    def get_parent_company(self, name: str) -> Optional[str]:
        """Get parent company for a developer if known."""
        normalized = self.normalize(name)

        if normalized in PARENT_COMPANY_MAP:
            return PARENT_COMPANY_MAP[normalized]

        canonical = self.canonicalize(name)
        if canonical in self.parent_map:
            return self.parent_map[canonical]

        return None

    def _title_case(self, name: str) -> str:
        """Convert to title case with special handling for acronyms."""
        # Words that should stay uppercase
        acronyms = {'llc', 'lp', 'plc', 'usa', 'us', 'pv', 'ppa', 'pge', 'sce',
                    'sdge', 'aps', 'tva', 'aes', 'edf', 'bp', 'nrg', 'rwe'}

        # Words that should stay lowercase
        lowercase = {'of', 'the', 'and', 'for', 'in', 'on', 'at', 'to', 'a', 'an'}

        words = name.split()
        result = []

        for i, word in enumerate(words):
            if word.lower() in acronyms:
                result.append(word.upper())
            elif i > 0 and word.lower() in lowercase:
                result.append(word.lower())
            else:
                result.append(word.capitalize())

        return ' '.join(result)

    def load_from_database(self) -> int:
        """
        Load developers from queue database and build registry.

        Returns number of unique canonical developers.
        """
        db_path = DATA_DIR / 'queue.db'
        if not db_path.exists():
            logger.error(f"Database not found: {db_path}")
            return 0

        conn = sqlite3.connect(db_path)

        # Get all developers with counts
        df = pd.read_sql('''
            SELECT developer, COUNT(*) as cnt
            FROM projects
            WHERE developer IS NOT NULL AND developer != ''
            GROUP BY developer
        ''', conn)
        conn.close()

        logger.info(f"Loaded {len(df):,} unique developer names")

        # Build registry by grouping normalized names
        groups = defaultdict(list)
        for _, row in df.iterrows():
            normalized = self.normalize(row['developer'])
            if normalized:
                groups[normalized].append((row['developer'], row['cnt']))

        # Create canonical entries
        for normalized, variants in groups.items():
            # Pick the most common variant as the display name
            variants.sort(key=lambda x: -x[1])
            primary_name = variants[0][0]
            total_count = sum(cnt for _, cnt in variants)

            # Create canonical name
            canonical = self.canonicalize(primary_name)

            # Get parent company
            parent = self.get_parent_company(primary_name)

            # Store developer
            self.developers[canonical] = CanonicalDeveloper(
                canonical_name=canonical,
                parent_company=parent,
                aliases=set(v[0] for v in variants),
                project_count=total_count
            )

            # Map all variants to canonical
            for variant, _ in variants:
                self.alias_map[self.normalize(variant)] = canonical

        logger.info(f"Built registry with {len(self.developers):,} canonical developers")

        # Count parent company consolidation
        with_parent = sum(1 for d in self.developers.values() if d.parent_company)
        logger.info(f"  {with_parent:,} developers mapped to parent companies")

        return len(self.developers)

    def apply_canonicalization(self, add_columns: bool = True) -> Tuple[int, int]:
        """
        Apply canonicalization to queue database.

        Args:
            add_columns: If True, add canonical_developer and parent_company columns

        Returns:
            Tuple of (updated_count, unique_canonical_count)
        """
        db_path = DATA_DIR / 'queue.db'
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Add columns if needed
        if add_columns:
            cursor.execute("PRAGMA table_info(projects)")
            columns = [col[1] for col in cursor.fetchall()]

            if 'developer_canonical' not in columns:
                cursor.execute("ALTER TABLE projects ADD COLUMN developer_canonical TEXT")
                logger.info("Added developer_canonical column")

            if 'parent_company' not in columns:
                cursor.execute("ALTER TABLE projects ADD COLUMN parent_company TEXT")
                logger.info("Added parent_company column")

            conn.commit()

        # Get all developers
        df = pd.read_sql('''
            SELECT DISTINCT developer
            FROM projects
            WHERE developer IS NOT NULL AND developer != ''
        ''', conn)

        updated = 0
        for _, row in df.iterrows():
            original = row['developer']
            canonical = self.canonicalize(original)
            parent = self.get_parent_company(original)

            cursor.execute("""
                UPDATE projects
                SET developer_canonical = ?, parent_company = ?
                WHERE developer = ?
            """, (canonical, parent, original))
            updated += cursor.rowcount

        conn.commit()

        # Get unique count
        result = cursor.execute("""
            SELECT COUNT(DISTINCT developer_canonical)
            FROM projects
            WHERE developer_canonical IS NOT NULL
        """).fetchone()
        unique_count = result[0] if result else 0

        conn.close()

        logger.info(f"Updated {updated:,} projects")
        logger.info(f"Reduced to {unique_count:,} unique canonical developers")

        return updated, unique_count

    def get_developer_stats(self) -> pd.DataFrame:
        """Get statistics about developers in the registry."""
        if not self.developers:
            self.load_from_database()

        data = []
        for canonical, dev in self.developers.items():
            data.append({
                'canonical_name': dev.canonical_name,
                'parent_company': dev.parent_company or dev.canonical_name,
                'project_count': dev.project_count,
                'alias_count': len(dev.aliases),
            })

        df = pd.DataFrame(data)
        return df.sort_values('project_count', ascending=False)

    def get_parent_company_stats(self) -> pd.DataFrame:
        """Get statistics grouped by parent company."""
        if not self.developers:
            self.load_from_database()

        # Group by parent company
        parent_groups = defaultdict(lambda: {'subsidiaries': [], 'total_projects': 0})

        for canonical, dev in self.developers.items():
            parent = dev.parent_company or dev.canonical_name
            parent_groups[parent]['subsidiaries'].append(dev.canonical_name)
            parent_groups[parent]['total_projects'] += dev.project_count

        data = []
        for parent, info in parent_groups.items():
            data.append({
                'parent_company': parent,
                'subsidiary_count': len(info['subsidiaries']),
                'total_projects': info['total_projects'],
            })

        df = pd.DataFrame(data)
        return df.sort_values('total_projects', ascending=False)

    def export_registry(self, output_path: Path = None) -> Path:
        """Export the developer registry to CSV."""
        if not self.developers:
            self.load_from_database()

        output_path = output_path or DATA_DIR / 'developer_registry.csv'

        data = []
        for canonical, dev in self.developers.items():
            data.append({
                'canonical_name': dev.canonical_name,
                'parent_company': dev.parent_company,
                'project_count': dev.project_count,
                'aliases': '; '.join(sorted(dev.aliases)),
            })

        df = pd.DataFrame(data)
        df.to_csv(output_path, index=False)

        logger.info(f"Exported registry to {output_path}")
        return output_path


def main():
    """CLI for developer registry."""
    import argparse

    parser = argparse.ArgumentParser(description="Developer Name Canonicalization")
    parser.add_argument('--load', action='store_true', help='Load and analyze developers')
    parser.add_argument('--apply', action='store_true', help='Apply canonicalization to database')
    parser.add_argument('--stats', action='store_true', help='Show developer statistics')
    parser.add_argument('--parents', action='store_true', help='Show parent company stats')
    parser.add_argument('--export', action='store_true', help='Export registry to CSV')
    parser.add_argument('--lookup', type=str, help='Look up canonical name for developer')

    args = parser.parse_args()

    registry = DeveloperRegistry()

    if args.load or args.stats or args.parents or args.apply or args.export:
        registry.load_from_database()

    if args.lookup:
        canonical = registry.canonicalize(args.lookup)
        parent = registry.get_parent_company(args.lookup)
        print(f"\nLookup: '{args.lookup}'")
        print(f"  Canonical: {canonical}")
        print(f"  Parent: {parent or 'N/A'}")

    if args.stats:
        df = registry.get_developer_stats()
        print("\n=== Top 30 Developers by Project Count ===")
        print(df.head(30).to_string())

    if args.parents:
        df = registry.get_parent_company_stats()
        print("\n=== Top 30 Parent Companies ===")
        print(df.head(30).to_string())

    if args.apply:
        updated, unique = registry.apply_canonicalization()
        print(f"\nApplied canonicalization:")
        print(f"  Updated {updated:,} project records")
        print(f"  Reduced to {unique:,} unique canonical developers")

    if args.export:
        path = registry.export_registry()
        print(f"\nExported to: {path}")


if __name__ == '__main__':
    main()
