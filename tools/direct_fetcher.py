#!/usr/bin/env python3
"""
Direct ISO Data Fetcher

Downloads interconnection queue data directly from ISO websites,
bypassing gridstatus parsing issues for more reliable data access.
"""

import os
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import tempfile

CACHE_DIR = Path(__file__).parent / '.cache'
CACHE_DIR.mkdir(exist_ok=True)


class DirectFetcher:
    """Fetch queue data directly from ISO websites."""

    # Direct download URLs for queue data
    QUEUE_URLS = {
        'nyiso': 'https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx',
        'caiso': 'http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx',
        'miso': 'https://www.misoenergy.org/api/giqueue/getprojects',  # JSON API
        'pjm': None,  # Requires API key
        'ercot': None,  # Requires portal login, but we can get GIS report
    }

    # Column standardization mappings
    COLUMN_MAPS = {
        'nyiso': {
            'Queue Pos.': 'Queue ID',
            'Developer/Interconnection Customer': 'Developer',
            'Project Name': 'Project Name',
            'SP (MW)': 'Capacity (MW)',
            'WP (MW)': 'Winter Capacity (MW)',
            'Type/ Fuel': 'Generation Type',
            'County': 'County',
            'State': 'State',
            'Z': 'Zone',
            'Points of Interconnection': 'POI',
            'Date of IR': 'Queue Date',
            'Proposed COD': 'Proposed COD',
            'S': 'Status',
        },
        'caiso': {
            'Queue Position': 'Queue ID',
            'Project Name': 'Project Name',
            'Capacity (MW)': 'Capacity (MW)',
            'Fuel': 'Generation Type',
            'County': 'County',
            'State': 'State',
            'POI': 'POI',
            'Status': 'Status',
            'Application Status': 'Application Status',
        },
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Queue-Analysis/1.0'
        })

    def fetch_nyiso(self, use_cache: bool = True) -> pd.DataFrame:
        """Fetch NYISO interconnection queue."""
        cache_file = CACHE_DIR / 'nyiso_queue_direct.xlsx'

        # Check cache
        if use_cache and cache_file.exists():
            cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
            if cache_age < 86400:  # 24 hours
                print(f"Using cached NYISO data ({cache_age/3600:.1f}h old)")
                return pd.read_excel(cache_file)

        print("Downloading NYISO queue data...")
        try:
            response = self.session.get(self.QUEUE_URLS['nyiso'], timeout=60)
            response.raise_for_status()

            # Save to cache
            with open(cache_file, 'wb') as f:
                f.write(response.content)

            df = pd.read_excel(cache_file)
            df['iso'] = 'NYISO'
            print(f"  Downloaded {len(df)} projects")
            return df

        except Exception as e:
            print(f"  Error: {e}")
            # Fall back to existing cache if available
            if cache_file.exists():
                return pd.read_excel(cache_file)
            return pd.DataFrame()

    def _parse_caiso_excel(self, filepath) -> pd.DataFrame:
        """Parse CAISO Excel file with proper header handling."""
        try:
            df = pd.read_excel(filepath, sheet_name='Grid GenerationQueue', header=3)
        except:
            df = pd.read_excel(filepath, sheet_name=0, header=3)

        # Drop any rows that are all NaN
        df = df.dropna(how='all')

        # Clean up column names (remove newlines)
        df.columns = [str(c).replace('\n', ' ').strip() for c in df.columns]

        df['iso'] = 'CAISO'
        return df

    def fetch_caiso(self, use_cache: bool = True) -> pd.DataFrame:
        """Fetch CAISO interconnection queue."""
        cache_file = CACHE_DIR / 'caiso_queue_direct.xlsx'

        if use_cache and cache_file.exists():
            cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
            if cache_age < 86400:
                print(f"Using cached CAISO data ({cache_age/3600:.1f}h old)")
                return self._parse_caiso_excel(cache_file)

        print("Downloading CAISO queue data...")
        try:
            response = self.session.get(self.QUEUE_URLS['caiso'], timeout=60)
            response.raise_for_status()

            with open(cache_file, 'wb') as f:
                f.write(response.content)

            df = self._parse_caiso_excel(cache_file)
            print(f"  Downloaded {len(df)} projects")
            return df

        except Exception as e:
            print(f"  Error: {e}")
            if cache_file.exists():
                return self._parse_caiso_excel(cache_file)
            return pd.DataFrame()

    def fetch_miso(self, use_cache: bool = True) -> pd.DataFrame:
        """
        Fetch MISO interconnection queue via JSON API.

        API endpoint returns ~3,700 projects with developer data in 'transmissionOwner' field.
        Developer coverage: ~96.8%
        """
        cache_file = CACHE_DIR / 'miso_queue_direct.parquet'

        # Check cache
        if use_cache and cache_file.exists():
            cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
            if cache_age < 86400:  # 24 hours
                print(f"Using cached MISO data ({cache_age/3600:.1f}h old)")
                return pd.read_parquet(cache_file)

        print("Fetching MISO queue via API...")
        try:
            response = self.session.get(self.QUEUE_URLS['miso'], timeout=60)
            response.raise_for_status()

            data = response.json()
            df = pd.DataFrame(data)

            print(f"  Downloaded {len(df)} projects from MISO API")

            # Standardize column names
            col_map = {
                'projectNumber': 'Queue ID',
                'transmissionOwner': 'Developer',  # This is the key field!
                'summerNetMW': 'Capacity (MW)',
                'fuelType': 'Generation Type',
                'applicationStatus': 'Status',
                'state': 'State',
                'county': 'County',
                'poiName': 'POI',
                'inService': 'Proposed COD',
                'queueDate': 'Queue Date',
                'studyPhase': 'Study Phase',
                'studyCycle': 'Study Cycle',
            }
            df = df.rename(columns=col_map)
            df['iso'] = 'MISO'

            # Report developer coverage
            dev_count = df['Developer'].notna() & (df['Developer'] != '')
            print(f"  Developer coverage: {dev_count.sum()}/{len(df)} ({dev_count.sum()/len(df)*100:.1f}%)")

            # Cache the data
            df.to_parquet(cache_file)

            return df

        except Exception as e:
            print(f"  Error: {e}")
            if cache_file.exists():
                print("  Using cached data")
                return pd.read_parquet(cache_file)
            return pd.DataFrame()

    def fetch_pjm(self, api_key: str = None, use_cache: bool = True) -> pd.DataFrame:
        """
        Fetch PJM interconnection queue via Data Miner 2 API.

        Requires API key from https://dataminer2.pjm.com/ (click "Obtain an API Key")
        Set PJM_API_KEY environment variable or pass api_key parameter.

        API Docs: https://dataminer2.pjm.com/api-ref
        Queue endpoint: /gen_queues
        """
        cache_file = CACHE_DIR / 'pjm_queue_direct.parquet'
        api_key = api_key or os.environ.get('PJM_API_KEY')

        # Check cache first
        if use_cache and cache_file.exists():
            cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
            if cache_age < 86400:  # 24 hours
                print(f"Using cached PJM data ({cache_age/3600:.1f}h old)")
                return pd.read_parquet(cache_file)

        if not api_key:
            print("PJM requires API key from https://dataminer2.pjm.com/")
            print("Set PJM_API_KEY environment variable or pass api_key parameter")

            # Return cached data if available
            if cache_file.exists():
                print("  Using cached data")
                return pd.read_parquet(cache_file)
            return pd.DataFrame()

        print("Fetching PJM queue via Data Miner 2 API...")

        try:
            # PJM Data Miner 2 API endpoint for generation queue
            api_url = 'https://api.pjm.com/api/v1/gen_queues'

            headers = {
                'Ocp-Apim-Subscription-Key': api_key,
                'Accept': 'application/json'
            }

            all_records = []
            start_row = 0
            page_size = 1000  # Max rows per request

            while True:
                params = {
                    'rowCount': page_size,
                    'startRow': start_row,
                    'format': 'json'
                }

                response = self.session.get(api_url, headers=headers, params=params, timeout=60)
                response.raise_for_status()

                data = response.json()
                records = data if isinstance(data, list) else data.get('items', [])

                if not records:
                    break

                all_records.extend(records)
                print(f"  Fetched {len(all_records)} records...")

                if len(records) < page_size:
                    break

                start_row += page_size

            if all_records:
                df = pd.DataFrame(all_records)
                df['iso'] = 'PJM'

                # Standardize column names
                col_map = {
                    'queue_number': 'Queue ID',
                    'project_name': 'Project Name',
                    'developer_name': 'Developer',
                    'mw': 'Capacity (MW)',
                    'fuel_type': 'Generation Type',
                    'status': 'Status',
                    'county': 'County',
                    'state': 'State',
                    'queue_date': 'Queue Date',
                    'in_service_date': 'Proposed COD',
                }
                df = df.rename(columns=col_map)

                # Convert object columns to string for parquet
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].astype(str)

                # Cache the data
                df.to_parquet(cache_file)
                print(f"  Downloaded {len(df)} projects")
                return df

            return pd.DataFrame()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print("  Error: Invalid API key. Check your PJM_API_KEY")
            elif e.response.status_code == 429:
                print("  Error: Rate limited. Wait a moment and try again")
            else:
                print(f"  HTTP Error: {e}")

            # Fall back to cache
            if cache_file.exists():
                print("  Using cached data")
                return pd.read_parquet(cache_file)
            return pd.DataFrame()

        except Exception as e:
            print(f"  Error: {e}")
            if cache_file.exists():
                print("  Using cached data")
                return pd.read_parquet(cache_file)
            return pd.DataFrame()

    def fetch_ercot(self, use_cache: bool = True) -> pd.DataFrame:
        """
        Fetch ERCOT interconnection queue from GIS report.

        Downloads directly from ERCOT MIS portal, combining data from
        multiple sheets (Stand-Alone, Co-located with Solar/Wind/Thermal).
        """
        cache_file = CACHE_DIR / 'ercot_queue_direct.parquet'
        raw_file = CACHE_DIR / 'ercot_gis_raw.xlsx'

        # Check cache
        if use_cache and cache_file.exists():
            cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
            if cache_age < 86400:  # 24 hours
                print(f"Using cached ERCOT data ({cache_age/3600:.1f}h old)")
                return pd.read_parquet(cache_file)

        print("Fetching ERCOT GIS report...")

        try:
            # Get document list from ERCOT MIS portal
            list_url = 'https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=15933'
            response = self.session.get(list_url, timeout=30)
            data = response.json()

            docs = data.get('ListDocsByRptTypeRes', {}).get('DocumentList', [])
            if not docs:
                raise Exception("No documents found")

            # Get latest document
            doc_id = docs[0].get('Document', {}).get('DocID')
            download_url = f'https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc_id}'

            response = self.session.get(download_url, timeout=60)
            response.raise_for_status()

            # Save raw file
            with open(raw_file, 'wb') as f:
                f.write(response.content)

            # Parse the multi-sheet file
            xlsx = pd.ExcelFile(raw_file)

            all_dfs = []

            # Sheets with project data (header at row 14)
            data_sheets = ['Stand-Alone', 'Co-located with Solar', 'Co-located with Wind', 'Co-located with Thermal']

            for sheet in data_sheets:
                if sheet in xlsx.sheet_names:
                    try:
                        df = pd.read_excel(xlsx, sheet_name=sheet, header=14)
                        df.columns = [str(c).replace('\n', ' ').strip() for c in df.columns]
                        df = df.dropna(subset=['INR'])  # Keep only rows with INR
                        df['Category'] = sheet
                        if not df.empty:
                            all_dfs.append(df)
                    except Exception as e:
                        print(f"  Warning: Could not parse {sheet}: {e}")

            if all_dfs:
                df = pd.concat(all_dfs, ignore_index=True)
                df['iso'] = 'ERCOT'

                # Standardize column names
                col_map = {
                    'INR': 'Queue ID',
                    'Project Name': 'Project Name',
                    'Interconnecting Entity': 'Developer',
                    'Capacity (MW)': 'Capacity (MW)',
                    'Fuel': 'Generation Type',
                    'Project Status': 'Status',
                    'County': 'County',
                    'Projected COD': 'Proposed COD',
                }
                df = df.rename(columns=col_map)

                # Cache the processed data
                df.to_parquet(cache_file)
                print(f"  Downloaded {len(df)} projects from GIS report")
                return df

            raise Exception("No data found in any sheet")

        except Exception as e:
            print(f"  Error: {e}")

            # Fall back to cache
            if cache_file.exists():
                print("  Using cached data")
                return pd.read_parquet(cache_file)

            return pd.DataFrame()

    def fetch_spp(self, use_cache: bool = True) -> pd.DataFrame:
        """
        Fetch SPP interconnection queue via gridstatus.

        Requires Python 3.10+ for gridstatus.
        Developer data is in 'Interconnecting Entity' field.
        """
        cache_file = CACHE_DIR / 'spp_queue_direct.parquet'

        # Check cache
        if use_cache and cache_file.exists():
            cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
            if cache_age < 86400:  # 24 hours
                print(f"Using cached SPP data ({cache_age/3600:.1f}h old)")
                return pd.read_parquet(cache_file)

        print("Fetching SPP queue via gridstatus...")
        try:
            import gridstatus
            spp = gridstatus.SPP()
            df = spp.get_interconnection_queue()

            print(f"  Downloaded {len(df)} projects from SPP")

            # Standardize column names (gridstatus already provides good names)
            col_map = {
                'Interconnecting Entity': 'Developer',
            }
            df = df.rename(columns=col_map)
            df['iso'] = 'SPP'

            # Report developer coverage
            dev_count = df['Developer'].notna() & (df['Developer'] != '')
            print(f"  Developer coverage: {dev_count.sum()}/{len(df)} ({dev_count.sum()/len(df)*100:.1f}%)")

            # Cache the data
            df.to_parquet(cache_file)

            return df

        except ImportError:
            print("  Error: gridstatus not available. Run with Python 3.10+")
            if cache_file.exists():
                print("  Using cached data")
                return pd.read_parquet(cache_file)
            return pd.DataFrame()
        except Exception as e:
            print(f"  Error: {e}")
            if cache_file.exists():
                print("  Using cached data")
                return pd.read_parquet(cache_file)
            return pd.DataFrame()

    def fetch_isone(self, use_cache: bool = True) -> pd.DataFrame:
        """
        Fetch ISO-NE interconnection queue via gridstatus.

        Requires Python 3.10+ for gridstatus.
        Developer data is in 'Interconnecting Entity' field.
        """
        cache_file = CACHE_DIR / 'isone_queue_direct.parquet'

        # Check cache
        if use_cache and cache_file.exists():
            cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
            if cache_age < 86400:  # 24 hours
                print(f"Using cached ISO-NE data ({cache_age/3600:.1f}h old)")
                return pd.read_parquet(cache_file)

        print("Fetching ISO-NE queue via gridstatus...")
        try:
            import gridstatus
            isone = gridstatus.ISONE()
            df = isone.get_interconnection_queue()

            print(f"  Downloaded {len(df)} projects from ISO-NE")

            # Standardize column names
            col_map = {
                'Interconnecting Entity': 'Developer',
            }
            df = df.rename(columns=col_map)
            df['iso'] = 'ISONE'

            # Report developer coverage
            dev_count = df['Developer'].notna() & (df['Developer'] != '')
            print(f"  Developer coverage: {dev_count.sum()}/{len(df)} ({dev_count.sum()/len(df)*100:.1f}%)")

            # Cache the data
            df.to_parquet(cache_file)

            return df

        except ImportError:
            print("  Error: gridstatus not available. Run with Python 3.10+")
            if cache_file.exists():
                print("  Using cached data")
                return pd.read_parquet(cache_file)
            return pd.DataFrame()
        except Exception as e:
            print(f"  Error: {e}")
            if cache_file.exists():
                print("  Using cached data")
                return pd.read_parquet(cache_file)
            return pd.DataFrame()

    def fetch_all(self, pjm_api_key: str = None) -> Dict[str, pd.DataFrame]:
        """Fetch data from all ISOs."""
        results = {}

        # Direct fetchers (most reliable)
        results['nyiso'] = self.fetch_nyiso()
        results['caiso'] = self.fetch_caiso()
        results['miso'] = self.fetch_miso()  # Direct API with developer data

        # API-based (PJM)
        results['pjm'] = self.fetch_pjm(pjm_api_key)

        # Mixed approach (ERCOT)
        results['ercot'] = self.fetch_ercot()

        # SPP and ISO-NE via gridstatus (requires Python 3.10+)
        results['spp'] = self.fetch_spp()
        results['isone'] = self.fetch_isone()

        return results

    def get_combined_queue(self, pjm_api_key: str = None) -> pd.DataFrame:
        """Get combined queue data from all ISOs."""
        data = self.fetch_all(pjm_api_key)

        # Combine all non-empty dataframes
        dfs = [df for df in data.values() if not df.empty]

        if dfs:
            combined = pd.concat(dfs, ignore_index=True)
            return combined

        return pd.DataFrame()


def standardize_columns(df: pd.DataFrame, iso: str) -> pd.DataFrame:
    """Standardize column names across ISOs."""
    # Standard column names
    standard_cols = [
        'Queue ID', 'Project Name', 'Developer', 'Capacity (MW)',
        'Generation Type', 'Status', 'County', 'State', 'POI',
        'Queue Date', 'Proposed COD', 'iso'
    ]

    # Mapping of common variations
    col_aliases = {
        'Queue ID': ['Queue Pos.', 'Queue Position', 'queue_id', 'Request ID', 'Project ID'],
        'Project Name': ['Project Name', 'project_name', 'Name'],
        'Developer': ['Developer', 'Developer/Interconnection Customer', 'Interconnection Customer', 'developer'],
        'Capacity (MW)': ['Capacity (MW)', 'capacity_mw', 'MW', 'SP (MW)', 'Summer Capacity (MW)', 'Nameplate (MW)'],
        'Generation Type': ['Generation Type', 'Type', 'Fuel', 'Type/ Fuel', 'Resource Type', 'fuel_type'],
        'Status': ['Status', 'Queue Status', 'Application Status', 'S', 'status'],
        'County': ['County', 'county'],
        'State': ['State', 'state'],
        'POI': ['POI', 'Points of Interconnection', 'Point of Interconnection', 'Substation'],
        'Queue Date': ['Queue Date', 'Date of IR', 'queue_date', 'Application Date'],
        'Proposed COD': ['Proposed COD', 'Proposed In-Service Date', 'COD', 'In-Service Date'],
    }

    # Create standardized dataframe
    result = pd.DataFrame()

    for std_col, aliases in col_aliases.items():
        for alias in aliases:
            if alias in df.columns:
                result[std_col] = df[alias]
                break
        if std_col not in result.columns:
            result[std_col] = None

    # Preserve ISO column
    if 'iso' in df.columns:
        result['iso'] = df['iso']

    return result


# Convenience function
def refresh_all_queues(pjm_api_key: str = None) -> pd.DataFrame:
    """Refresh and return combined queue data from all ISOs."""
    fetcher = DirectFetcher()
    return fetcher.get_combined_queue(pjm_api_key)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Fetch ISO Queue Data')
    parser.add_argument('--iso', choices=['nyiso', 'caiso', 'pjm', 'ercot', 'all'],
                       default='all', help='ISO to fetch')
    parser.add_argument('--pjm-key', help='PJM API key')
    parser.add_argument('--no-cache', action='store_true', help='Skip cache')

    args = parser.parse_args()

    fetcher = DirectFetcher()

    if args.iso == 'all':
        data = fetcher.fetch_all(args.pjm_key)
        print("\n" + "="*50)
        print("Summary:")
        total = 0
        for iso, df in data.items():
            count = len(df) if not df.empty else 0
            total += count
            status = "✓" if count > 0 else "✗"
            print(f"  {status} {iso.upper()}: {count:,} projects")
        print(f"\nTotal: {total:,} projects")
    else:
        if args.iso == 'nyiso':
            df = fetcher.fetch_nyiso(use_cache=not args.no_cache)
        elif args.iso == 'caiso':
            df = fetcher.fetch_caiso(use_cache=not args.no_cache)
        elif args.iso == 'pjm':
            df = fetcher.fetch_pjm(args.pjm_key)
        elif args.iso == 'ercot':
            df = fetcher.fetch_ercot()

        print(f"\n{args.iso.upper()}: {len(df)} projects")
        if not df.empty:
            print(f"Columns: {list(df.columns)[:8]}...")
