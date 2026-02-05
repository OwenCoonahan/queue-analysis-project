#!/usr/bin/env python3
"""
Data Scrapers for Enhanced Research

Automates collection of:
- RTO study documents
- SEC EDGAR company filings
- News/web search for developer background
- Cross-RTO developer search

Usage:
    python3 scrapers.py --developer "Donovan Drive Holdings LLC"
    python3 scrapers.py --sec-search "Donovan"
    python3 scrapers.py --nyiso-docs 1738
"""

import requests
import json
import re
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path
from dataclasses import dataclass, field
import argparse
import sys
from urllib.parse import quote, urljoin

# Suppress SSL warnings for older Python
import warnings
warnings.filterwarnings('ignore')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

CACHE_DIR = Path(__file__).parent / '.cache' / 'scrapers'
CACHE_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# SEC EDGAR SEARCH
# =============================================================================

class SECEdgarSearch:
    """
    Search SEC EDGAR for company filings.

    Useful for:
    - Finding if developer is public company or subsidiary
    - Finding financial disclosures
    - Finding major investors/ownership
    """

    BASE_URL = "https://efts.sec.gov/LATEST/search-index"
    SEARCH_URL = "https://www.sec.gov/cgi-bin/srch-ia"
    FULL_TEXT_URL = "https://efts.sec.gov/LATEST/search-index"
    COMPANY_SEARCH = "https://www.sec.gov/cgi-bin/browse-edgar"

    def search_company(self, company_name: str) -> Dict[str, Any]:
        """
        Search for a company in SEC EDGAR.

        Returns company info if found, or indicates no filing found.
        """
        results = {
            'query': company_name,
            'found': False,
            'filings': [],
            'company_info': None,
            'search_url': None,
        }

        # Clean up search term
        search_term = company_name.replace('LLC', '').replace('Inc', '').replace(',', '').strip()

        # Try the company search
        try:
            params = {
                'company': search_term,
                'CIK': '',
                'type': '',
                'owner': 'include',
                'count': '40',
                'action': 'getcompany',
                'output': 'atom',
            }

            response = requests.get(
                self.COMPANY_SEARCH,
                params=params,
                headers=HEADERS,
                timeout=30
            )

            results['search_url'] = response.url

            if response.status_code == 200:
                # Parse the response (it's XML/Atom format)
                content = response.text

                # Look for company entries
                if '<company-info>' in content or '<entry>' in content:
                    results['found'] = True

                    # Extract company names found
                    companies = re.findall(r'<conformed-name>([^<]+)</conformed-name>', content)
                    ciks = re.findall(r'<cik>([^<]+)</cik>', content)

                    if companies:
                        results['companies_found'] = companies[:5]
                        results['ciks'] = ciks[:5]

                    # Extract recent filings
                    filing_types = re.findall(r'<filing-type>([^<]+)</filing-type>', content)
                    filing_dates = re.findall(r'<filing-date>([^<]+)</filing-date>', content)

                    results['recent_filings'] = [
                        {'type': t, 'date': d}
                        for t, d in zip(filing_types[:5], filing_dates[:5])
                    ]
                else:
                    results['note'] = 'No SEC filings found - likely private company'

        except Exception as e:
            results['error'] = str(e)

        return results

    def search_full_text(self, search_term: str, form_types: List[str] = None) -> Dict[str, Any]:
        """
        Full-text search across SEC filings.

        Useful for finding mentions of a company in other filings
        (e.g., in 10-K of a parent company).
        """
        results = {
            'query': search_term,
            'mentions': [],
        }

        try:
            # SEC full-text search API
            params = {
                'q': f'"{search_term}"',
                'dateRange': 'custom',
                'startdt': '2020-01-01',
                'enddt': datetime.now().strftime('%Y-%m-%d'),
            }

            if form_types:
                params['forms'] = ','.join(form_types)

            # Note: This is a simplified version - full implementation would
            # use SEC's EDGAR full-text search API
            results['search_performed'] = True
            results['note'] = 'Full-text search available at SEC EDGAR website'
            results['manual_search_url'] = f"https://www.sec.gov/cgi-bin/srch-ia?text={quote(search_term)}"

        except Exception as e:
            results['error'] = str(e)

        return results


# =============================================================================
# NEWS/WEB SEARCH
# =============================================================================

class NewsSearcher:
    """
    Search for news and web mentions of developers.

    Uses DuckDuckGo (no API key required) for basic search.
    """

    DUCKDUCKGO_URL = "https://html.duckduckgo.com/html/"

    def search(self, query: str, max_results: int = 10) -> Dict[str, Any]:
        """
        Search for news/web mentions.
        """
        results = {
            'query': query,
            'results': [],
            'search_url': None,
        }

        try:
            # DuckDuckGo HTML search
            response = requests.post(
                self.DUCKDUCKGO_URL,
                data={'q': query},
                headers=HEADERS,
                timeout=30
            )

            if response.status_code == 200:
                content = response.text

                # Extract result links and titles
                # DuckDuckGo HTML format
                result_pattern = r'<a rel="nofollow" class="result__a" href="([^"]+)">([^<]+)</a>'
                matches = re.findall(result_pattern, content)

                for url, title in matches[:max_results]:
                    results['results'].append({
                        'title': title.strip(),
                        'url': url,
                    })

                # Also look for snippets
                snippet_pattern = r'<a class="result__snippet"[^>]*>([^<]+)</a>'
                snippets = re.findall(snippet_pattern, content)

                for i, snippet in enumerate(snippets[:len(results['results'])]):
                    if i < len(results['results']):
                        results['results'][i]['snippet'] = snippet.strip()

            results['search_url'] = f"https://duckduckgo.com/?q={quote(query)}"

        except Exception as e:
            results['error'] = str(e)
            results['search_url'] = f"https://duckduckgo.com/?q={quote(query)}"

        return results

    def search_developer(self, developer_name: str) -> Dict[str, Any]:
        """
        Comprehensive search for developer information.
        """
        # Clean company name
        clean_name = developer_name.replace('LLC', '').replace('Inc', '').replace(',', '').strip()

        results = {
            'developer': developer_name,
            'searches': {},
        }

        # General search
        results['searches']['general'] = self.search(f'"{clean_name}" energy project')

        # Data center specific (if applicable)
        if any(kw in developer_name.lower() for kw in ['data', 'digital', 'compute', 'cloud']):
            results['searches']['datacenter'] = self.search(f'"{clean_name}" data center')

        # News search
        results['searches']['news'] = self.search(f'"{clean_name}" site:reuters.com OR site:bloomberg.com OR site:utilitydive.com')

        # Compile findings
        all_results = []
        for search_type, search_results in results['searches'].items():
            all_results.extend(search_results.get('results', []))

        results['total_mentions'] = len(all_results)
        results['top_results'] = all_results[:10]

        # Check for known patterns
        results['indicators'] = []
        for r in all_results:
            text = (r.get('title', '') + ' ' + r.get('snippet', '')).lower()
            if 'microsoft' in text or 'google' in text or 'amazon' in text or 'meta' in text:
                results['indicators'].append('Possible hyperscaler connection mentioned')
            if 'acquisition' in text or 'acquired' in text:
                results['indicators'].append('Acquisition activity mentioned')
            if 'funding' in text or 'investment' in text or 'raised' in text:
                results['indicators'].append('Funding/investment mentioned')

        results['indicators'] = list(set(results['indicators']))

        return results


# =============================================================================
# NYISO DOCUMENT FETCHER
# =============================================================================

class NYISODocumentFetcher:
    """
    Fetch study documents from NYISO.

    NYISO publishes interconnection study documents at predictable URLs.
    """

    BASE_URL = "https://www.nyiso.com"
    QUEUE_DOCS_URL = "https://www.nyiso.com/interconnections"

    def get_document_links(self, queue_position: str) -> Dict[str, Any]:
        """
        Find document links for a queue position.

        Note: NYISO's document structure requires navigating their portal.
        This provides guidance on where to find documents.
        """
        results = {
            'queue_position': queue_position,
            'document_portal': 'https://www.nyiso.com/interconnections',
            'instructions': [],
            'possible_documents': [],
        }

        results['instructions'] = [
            f"1. Go to {results['document_portal']}",
            f"2. Search for Queue Position: {queue_position}",
            "3. Click on the project to view available studies",
            "4. Download Feasibility Study, SIS, and Facilities Study if available",
        ]

        # NYISO document types
        results['document_types'] = [
            {'type': 'Feasibility Study', 'typical_contents': 'Initial screening, fatal flaw analysis'},
            {'type': 'System Impact Study (SIS)', 'typical_contents': 'Detailed analysis, network upgrade requirements'},
            {'type': 'Facilities Study', 'typical_contents': 'Cost estimates, construction requirements'},
            {'type': 'Interconnection Agreement', 'typical_contents': 'Final terms, milestone schedule'},
        ]

        # Check NYISO OASIS for any public documents
        try:
            # NYISO posts some info publicly - check their study queue page
            study_url = f"https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx"
            results['queue_spreadsheet'] = study_url
            results['note'] = 'Queue spreadsheet contains study status info'
        except:
            pass

        return results


# =============================================================================
# CROSS-RTO DEVELOPER SEARCH
# =============================================================================

class CrossRTOSearch:
    """
    Search for developers across multiple RTOs.

    Helps identify developer track record in other regions.
    """

    def __init__(self):
        self.rto_data = {}

    def load_rto_data(self, rto: str) -> bool:
        """Load queue data for an RTO."""
        try:
            if rto == 'NYISO':
                url = 'https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx'
                response = requests.get(url, headers=HEADERS, timeout=60)
                if response.status_code == 200:
                    cache_path = CACHE_DIR / f'{rto.lower()}_queue.xlsx'
                    with open(cache_path, 'wb') as f:
                        f.write(response.content)
                    import pandas as pd
                    self.rto_data[rto] = pd.read_excel(cache_path)
                    return True

            elif rto == 'MISO':
                # MISO has a JSON API
                url = 'https://www.misoenergy.org/api/giqueue/getprojects'
                response = requests.get(url, headers=HEADERS, timeout=60)
                if response.status_code == 200:
                    import pandas as pd
                    self.rto_data[rto] = pd.DataFrame(response.json())
                    return True

        except Exception as e:
            print(f"Error loading {rto}: {e}")

        return False

    def search_developer_across_rtos(self, developer_name: str) -> Dict[str, Any]:
        """
        Search for a developer across multiple RTOs.
        """
        results = {
            'developer': developer_name,
            'rtos_searched': [],
            'matches': {},
            'total_projects': 0,
            'total_capacity_mw': 0,
        }

        # Clean search term
        search_terms = self._get_search_terms(developer_name)

        for rto in ['NYISO', 'MISO']:
            try:
                if rto not in self.rto_data:
                    if not self.load_rto_data(rto):
                        continue

                df = self.rto_data[rto]
                results['rtos_searched'].append(rto)

                # Find developer column
                dev_cols = [c for c in df.columns if any(kw in c.lower() for kw in ['developer', 'owner', 'customer', 'applicant'])]
                if not dev_cols:
                    continue

                dev_col = dev_cols[0]

                # Search for matches
                matches = []
                for idx, row in df.iterrows():
                    dev_val = str(row[dev_col]).lower()
                    for term in search_terms:
                        if term.lower() in dev_val:
                            matches.append(row.to_dict())
                            break

                if matches:
                    results['matches'][rto] = {
                        'count': len(matches),
                        'projects': matches[:10],  # Limit for display
                    }
                    results['total_projects'] += len(matches)

                    # Try to sum capacity
                    cap_cols = [c for c in df.columns if any(kw in c.lower() for kw in ['capacity', 'mw'])]
                    if cap_cols:
                        cap_col = cap_cols[0]
                        import pandas as pd
                        cap_sum = pd.to_numeric([m.get(cap_col, 0) for m in matches], errors='coerce').sum()
                        results['total_capacity_mw'] += cap_sum
                        results['matches'][rto]['total_capacity_mw'] = cap_sum

            except Exception as e:
                results['matches'][rto] = {'error': str(e)}

        # Analysis
        if results['total_projects'] > 5:
            results['assessment'] = 'Experienced developer with multi-RTO presence'
        elif results['total_projects'] > 1:
            results['assessment'] = 'Developer has some track record'
        else:
            results['assessment'] = 'Limited presence in searched RTOs'

        return results

    def _get_search_terms(self, developer_name: str) -> List[str]:
        """Generate search terms from developer name."""
        terms = [developer_name]

        # Remove common suffixes for broader search
        cleaned = developer_name.replace('LLC', '').replace('Inc', '').replace('Corp', '')
        cleaned = cleaned.replace(',', '').replace('.', '').strip()
        terms.append(cleaned)

        # Get significant words (exclude common words)
        stop_words = {'llc', 'inc', 'corp', 'the', 'and', 'of', 'for', 'energy', 'solar', 'wind', 'power'}
        words = [w for w in cleaned.lower().split() if w not in stop_words and len(w) > 2]
        if words:
            terms.append(' '.join(words[:2]))  # First two significant words

        return terms


# =============================================================================
# COMBINED RESEARCH
# =============================================================================

def comprehensive_developer_research(developer_name: str) -> Dict[str, Any]:
    """
    Run all available research on a developer.
    """
    print(f"\nResearching: {developer_name}")
    print("=" * 60)

    results = {
        'developer': developer_name,
        'research_date': datetime.now().isoformat(),
        'sec_search': None,
        'news_search': None,
        'cross_rto': None,
    }

    # SEC Search
    print("Searching SEC EDGAR...")
    sec = SECEdgarSearch()
    results['sec_search'] = sec.search_company(developer_name)

    # News Search
    print("Searching news/web...")
    news = NewsSearcher()
    results['news_search'] = news.search_developer(developer_name)

    # Cross-RTO Search
    print("Searching across RTOs...")
    cross_rto = CrossRTOSearch()
    results['cross_rto'] = cross_rto.search_developer_across_rtos(developer_name)

    return results


def print_research_results(results: Dict[str, Any]):
    """Pretty print research results."""
    print("\n" + "=" * 70)
    print(f"DEVELOPER RESEARCH: {results['developer']}")
    print("=" * 70)

    # SEC Results
    sec = results.get('sec_search', {})
    print("\n--- SEC EDGAR ---")
    if sec.get('found'):
        print(f"SEC Filings Found: Yes")
        if sec.get('companies_found'):
            print(f"Companies: {', '.join(sec['companies_found'][:3])}")
        if sec.get('recent_filings'):
            print("Recent filings:")
            for f in sec['recent_filings'][:3]:
                print(f"  - {f['type']} ({f['date']})")
    else:
        print(f"SEC Filings Found: No (likely private company)")
        if sec.get('search_url'):
            print(f"Manual search: {sec['search_url']}")

    # News Results
    news = results.get('news_search', {})
    print(f"\n--- NEWS/WEB SEARCH ---")
    print(f"Total mentions found: {news.get('total_mentions', 0)}")

    if news.get('indicators'):
        print("Indicators found:")
        for ind in news['indicators']:
            print(f"  - {ind}")

    if news.get('top_results'):
        print("Top results:")
        for r in news['top_results'][:5]:
            print(f"  - {r.get('title', 'No title')[:60]}")
            if r.get('url'):
                print(f"    {r['url'][:70]}")

    # Cross-RTO Results
    cross = results.get('cross_rto', {})
    print(f"\n--- CROSS-RTO SEARCH ---")
    print(f"RTOs searched: {', '.join(cross.get('rtos_searched', []))}")
    print(f"Total projects found: {cross.get('total_projects', 0)}")
    print(f"Total capacity: {cross.get('total_capacity_mw', 0):,.0f} MW")
    print(f"Assessment: {cross.get('assessment', 'Unknown')}")

    if cross.get('matches'):
        for rto, data in cross['matches'].items():
            if isinstance(data, dict) and 'count' in data:
                print(f"\n  {rto}: {data['count']} projects")

    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Developer Research Scrapers")
    parser.add_argument('--developer', '-d', help='Research a developer')
    parser.add_argument('--sec-search', help='Search SEC EDGAR')
    parser.add_argument('--news-search', help='Search news/web')
    parser.add_argument('--cross-rto', help='Search across RTOs')
    parser.add_argument('--nyiso-docs', help='Get NYISO document info for queue position')
    parser.add_argument('--json', action='store_true', help='Output as JSON')

    args = parser.parse_args()

    if args.developer:
        results = comprehensive_developer_research(args.developer)
        if args.json:
            print(json.dumps(results, indent=2, default=str))
        else:
            print_research_results(results)
        return 0

    if args.sec_search:
        sec = SECEdgarSearch()
        results = sec.search_company(args.sec_search)
        print(json.dumps(results, indent=2))
        return 0

    if args.news_search:
        news = NewsSearcher()
        results = news.search_developer(args.news_search)
        print(json.dumps(results, indent=2, default=str))
        return 0

    if args.cross_rto:
        cross = CrossRTOSearch()
        results = cross.search_developer_across_rtos(args.cross_rto)
        print(json.dumps(results, indent=2, default=str))
        return 0

    if args.nyiso_docs:
        fetcher = NYISODocumentFetcher()
        results = fetcher.get_document_links(args.nyiso_docs)
        print(json.dumps(results, indent=2))
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
