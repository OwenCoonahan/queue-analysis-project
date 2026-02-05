"""
OASIS ATC (Available Transfer Capability) Data Fetcher.

OASIS (Open Access Same-time Information System) provides transmission
availability data from RTOs/ISOs. This module fetches ATC data to help
assess transmission constraints for queue projects.

NOTE: Most OASIS systems require NAESB WEQ-002 compliant access with
authentication. This module provides the framework and public data access
where available.

Data sources:
- PJM OASIS: pjmoasis.pjm.com (requires auth for full access)
- MISO OASIS: oasis.oati.com/MISO (requires auth)
- CAISO OASIS: oasis.caiso.com (public API available)
- SPP OASIS: oasis.oati.com/SPP (requires auth)

References:
- NAESB WEQ-002: OASIS Business Practice Standards
- PJM OASIS API Guide: pjm.com/markets-and-operations/etools/oasis
"""

import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, Dict, List
import xml.etree.ElementTree as ET
import time


# Paths
CACHE_DIR = Path(__file__).parent / '.cache' / 'oasis'
V2_PATH = Path(__file__).parent / '.data' / 'queue_v2.db'


@dataclass
class ATCRecord:
    """Available Transfer Capability record."""
    source: str          # ISO/RTO source
    path_name: str       # Transmission path
    source_poi: str      # Source point of interconnection
    sink_poi: str        # Sink point of interconnection
    direction: str       # Direction of flow
    timestamp: datetime  # Time of posting
    firm_atc_mw: float   # Firm ATC in MW
    nonfirm_atc_mw: float  # Non-firm ATC in MW
    ttc_mw: Optional[float] = None  # Total Transfer Capability
    afc_mw: Optional[float] = None  # Available Flowgate Capability


class CAISOOASISFetcher:
    """
    Fetch ATC data from CAISO OASIS public API.

    CAISO provides public API access without authentication for certain data.
    API Documentation: https://www.caiso.com/Documents/OASISAPISpecification.pdf
    """

    BASE_URL = "http://oasis.caiso.com/oasisapi/SingleZip"

    # Query names for different data types
    QUERY_NAMES = {
        'atc': 'SLD_ATC',  # ATC for specific paths
        'prices': 'PRC_LMP',  # Locational Marginal Prices
        'load': 'SLD_FCST',  # Load forecast
    }

    def __init__(self):
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def fetch_atc(self, start_date: datetime, end_date: datetime,
                  market_type: str = 'DAM') -> List[ATCRecord]:
        """
        Fetch ATC data from CAISO OASIS.

        Args:
            start_date: Start date for query
            end_date: End date for query
            market_type: Market run ID (DAM=Day-Ahead, HASP=Hour-Ahead)

        Returns:
            List of ATCRecord objects
        """
        params = {
            'queryname': 'SLD_ATC',
            'startdatetime': start_date.strftime('%Y%m%dT07:00-0000'),
            'enddatetime': end_date.strftime('%Y%m%dT07:00-0000'),
            'market_run_id': market_type,
            'version': 1,
        }

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=60)
            response.raise_for_status()

            # Parse XML response
            records = self._parse_atc_response(response.content)
            return records

        except requests.RequestException as e:
            print(f"Error fetching CAISO ATC: {e}")
            return []

    def _parse_atc_response(self, content: bytes) -> List[ATCRecord]:
        """Parse CAISO ATC XML response."""
        records = []

        try:
            # CAISO returns a ZIP file containing XML
            import zipfile
            import io

            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                for name in zf.namelist():
                    if name.endswith('.xml'):
                        xml_content = zf.read(name)
                        root = ET.fromstring(xml_content)

                        # Parse based on CAISO XML schema
                        for item in root.iter('REPORT_DATA'):
                            try:
                                record = ATCRecord(
                                    source='CAISO',
                                    path_name=item.findtext('PATH_NAME', ''),
                                    source_poi=item.findtext('SOURCE_POI', ''),
                                    sink_poi=item.findtext('SINK_POI', ''),
                                    direction=item.findtext('DIRECTION', ''),
                                    timestamp=datetime.strptime(
                                        item.findtext('INTERVALSTARTTIME_GMT', ''),
                                        '%Y-%m-%dT%H:%M:%S-00:00'
                                    ) if item.findtext('INTERVALSTARTTIME_GMT') else datetime.now(),
                                    firm_atc_mw=float(item.findtext('FIRM_ATC', 0) or 0),
                                    nonfirm_atc_mw=float(item.findtext('NONFIRM_ATC', 0) or 0),
                                    ttc_mw=float(item.findtext('TTC', 0) or 0),
                                )
                                records.append(record)
                            except (ValueError, TypeError):
                                continue

        except Exception as e:
            print(f"Error parsing CAISO response: {e}")

        return records


class PJMOASISFetcher:
    """
    Fetch data from PJM OASIS.

    NOTE: Full API access requires NAESB credentials and registration.
    Public data is available through PJM Data Miner for some datasets.

    Registration: https://pjm.my.site.com/publicknowledge/s/article/Requesting-access-to-OASIS-system
    """

    # PJM Data Miner endpoints (public, no auth required)
    DATA_MINER_BASE = "https://api.pjm.com/api/v1"

    # OASIS API (requires auth)
    OASIS_BASE = "https://pjmoasis.pjm.com/oasis/PJM"

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize PJM fetcher.

        Args:
            api_key: PJM Data Miner 2 API key (for non-OASIS data)
        """
        self.api_key = api_key
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def fetch_flowgates(self) -> pd.DataFrame:
        """
        Fetch PJM flowgate data (public).

        Flowgates are monitored transmission elements that can constrain
        power transfers and affect ATC.
        """
        # PJM publishes flowgate info publicly
        url = "https://www.pjm.com/-/media/markets-ops/energy/real-time/flowgates.ashx"

        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()

            # Save to cache
            cache_file = CACHE_DIR / 'pjm_flowgates.xlsx'
            with open(cache_file, 'wb') as f:
                f.write(response.content)

            # Try different engines
            try:
                df = pd.read_excel(cache_file, engine='openpyxl')
            except Exception:
                try:
                    df = pd.read_excel(cache_file, engine='xlrd')
                except Exception:
                    # Try CSV as fallback
                    df = pd.read_csv(cache_file)

            print(f"Loaded {len(df)} PJM flowgates")
            return df

        except Exception as e:
            print(f"Error fetching PJM flowgates: {e}")
            return pd.DataFrame()

    def fetch_with_data_miner(self, endpoint: str, params: dict) -> pd.DataFrame:
        """
        Fetch data from PJM Data Miner 2 API.

        Requires API key registration at:
        https://dataminer2.pjm.com/list

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            DataFrame with results
        """
        if not self.api_key:
            print("PJM Data Miner requires API key. Register at dataminer2.pjm.com")
            return pd.DataFrame()

        headers = {
            'Ocp-Apim-Subscription-Key': self.api_key,
            'Accept': 'application/json'
        }

        url = f"{self.DATA_MINER_BASE}/{endpoint}"

        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                return pd.DataFrame(data)
            return pd.DataFrame([data])

        except Exception as e:
            print(f"Error fetching from PJM Data Miner: {e}")
            return pd.DataFrame()


class MISOOASISFetcher:
    """
    Fetch ATC data from MISO OASIS.

    MISO OASIS is hosted by OATI at oasis.oati.com/MISO.
    Full access requires NAESB WEQ-002 compliant credentials.

    Public real-time data is available through MISO's RT Data APIs.
    Docs: https://www.misoenergy.org/markets-and-operations/rtdataapis/
    """

    # Public real-time data broker
    RT_DATA_BASE = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx"

    def __init__(self):
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def fetch_binding_constraints(self) -> pd.DataFrame:
        """
        Fetch real-time binding transmission constraints from MISO.

        Binding constraints indicate transmission bottlenecks that affect ATC.
        """
        url = f"{self.RT_DATA_BASE}?messageType=getrealtimebindingconstraints&returnType=json"

        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()

            # MISO returns nested structure
            if isinstance(data, dict):
                # Try different possible keys
                for key in ['Constraints', 'BindingConstraints', 'Data', 'data']:
                    if key in data:
                        df = pd.DataFrame(data[key])
                        print(f"Loaded {len(df)} MISO binding constraints")
                        return df

                # If structure is flat, return as-is
                df = pd.DataFrame([data])
                print(f"Loaded MISO constraints (single record)")
                return df

            elif isinstance(data, list):
                df = pd.DataFrame(data)
                print(f"Loaded {len(df)} MISO binding constraints")
                return df

            return pd.DataFrame()

        except Exception as e:
            print(f"Error fetching MISO binding constraints: {e}")
            return pd.DataFrame()

    def fetch_regional_transfers(self) -> pd.DataFrame:
        """Fetch regional directional transfer data."""
        url = f"{self.RT_DATA_BASE}?messageType=getregionaldirectionaltransfer&returnType=json"

        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                return pd.DataFrame(data)
            elif isinstance(data, dict):
                for key in ['Data', 'data', 'Transfers']:
                    if key in data:
                        return pd.DataFrame(data[key])
            return pd.DataFrame()

        except Exception as e:
            print(f"Error fetching MISO regional transfers: {e}")
            return pd.DataFrame()


def get_poi_atc_summary(poi_name: str) -> Dict:
    """
    Get ATC summary for a Point of Interconnection.

    This aggregates available ATC data from multiple sources to provide
    a summary of transmission availability at a specific POI.

    Args:
        poi_name: Name of the Point of Interconnection

    Returns:
        Dictionary with ATC summary
    """
    summary = {
        'poi_name': poi_name,
        'sources_checked': [],
        'firm_atc_mw': None,
        'nonfirm_atc_mw': None,
        'binding_constraints': [],
        'last_updated': None,
    }

    # Try CAISO if applicable
    if 'CAISO' in poi_name.upper() or 'CA' in poi_name.upper():
        try:
            caiso = CAISOOASISFetcher()
            end = datetime.now()
            start = end - timedelta(days=1)
            records = caiso.fetch_atc(start, end)

            matching = [r for r in records if poi_name.lower() in r.path_name.lower()]
            if matching:
                latest = max(matching, key=lambda x: x.timestamp)
                summary['firm_atc_mw'] = latest.firm_atc_mw
                summary['nonfirm_atc_mw'] = latest.nonfirm_atc_mw
                summary['last_updated'] = latest.timestamp

            summary['sources_checked'].append('CAISO')
        except Exception as e:
            print(f"CAISO lookup failed: {e}")

    return summary


def try_gridstatus_fetch():
    """
    Try fetching transmission data using the gridstatus library.

    gridstatus is an open-source library for accessing ISO/RTO data:
    https://github.com/kmax12/gridstatus
    pip install gridstatus
    """
    try:
        import gridstatus

        print("\nUsing gridstatus library...")

        # Try MISO
        miso = gridstatus.MISO()
        print(f"MISO latest LMP: {miso.get_lmp('latest')}")

        # Try PJM
        pjm = gridstatus.PJM()
        print(f"PJM latest LMP available")

        # Try CAISO
        caiso = gridstatus.CAISO()
        print(f"CAISO latest LMP available")

        return True

    except ImportError:
        print("\ngridstatus library not installed.")
        print("Install with: pip install gridstatus")
        return False

    except Exception as e:
        print(f"\nError with gridstatus: {e}")
        return False


def main():
    """Demo the OASIS fetchers."""
    print("=" * 60)
    print("OASIS ATC Data Fetcher Demo")
    print("=" * 60)

    # Try gridstatus first (recommended approach)
    print("\n0. Gridstatus Library (Recommended)")
    print("-" * 40)
    gridstatus_available = try_gridstatus_fetch()

    # Demo CAISO (public API)
    print("\n1. CAISO OASIS (Public API)")
    print("-" * 40)
    caiso = CAISOOASISFetcher()
    end = datetime.now()
    start = end - timedelta(days=1)

    print(f"Fetching ATC data from {start.date()} to {end.date()}...")
    records = caiso.fetch_atc(start, end)
    print(f"Retrieved {len(records)} ATC records")

    if records:
        print("\nSample records:")
        for r in records[:3]:
            print(f"  {r.path_name}: Firm={r.firm_atc_mw}MW, Non-firm={r.nonfirm_atc_mw}MW")

    # Demo PJM Flowgates (public)
    print("\n2. PJM Flowgates (Public)")
    print("-" * 40)
    pjm = PJMOASISFetcher()
    flowgates = pjm.fetch_flowgates()
    if not flowgates.empty:
        print(f"Retrieved {len(flowgates)} flowgates")
        print(f"Columns: {list(flowgates.columns)[:5]}...")

    # Demo MISO Binding Constraints
    print("\n3. MISO Binding Constraints")
    print("-" * 40)
    miso = MISOOASISFetcher()
    constraints = miso.fetch_binding_constraints()
    if not constraints.empty and 'error' not in constraints.columns:
        print(f"Retrieved {len(constraints)} binding constraints")
    else:
        print("MISO API endpoints changed Dec 2025. Use gridstatus library instead.")

    print("\n" + "=" * 60)
    print("RECOMMENDATIONS:")
    print("1. Install gridstatus: pip install gridstatus")
    print("   (Best for real-time LMP, load, and transmission data)")
    print("2. For full OASIS ATC data, register for NAESB credentials:")
    print("   https://pjm.my.site.com/publicknowledge")
    print("=" * 60)


if __name__ == '__main__':
    main()
