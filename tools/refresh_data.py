#!/usr/bin/env python3
# Suppress warnings before any imports for clean cron output
import warnings
warnings.filterwarnings('ignore')

"""
Data Refresh Script - Update all interconnection and market data.

Usage:
    python3 refresh_data.py              # Refresh all sources
    python3 refresh_data.py --source ercot    # Refresh specific source
    python3 refresh_data.py --status          # Show refresh status
    python3 refresh_data.py --changes         # Show recent changes
    python3 refresh_data.py --cron            # Cron-friendly (quiet, exit codes)

Queue Sources:
    - nyiso: NYISO interconnection queue (live)
    - ercot: ERCOT GIS report (live API)
    - lbl: LBL Queued Up historical data (manual/annual)

Market Data:
    - lmp: Energy prices by zone
    - capacity: Capacity market prices and ELCC
    - transmission: Congestion and constraint data
    - ppa: PPA benchmark prices
    - permits: State permitting requirements

Data is stored in .data/queue.db (SQLite)

Cron Setup (daily at 6 AM):
    0 6 * * * cd /path/to/tools && python3 refresh_data.py --cron >> .data/refresh.log 2>&1
"""

import argparse
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
import sys

from data_store import DataStore
from data_validation import DataValidator, ValidationError

CACHE_DIR = Path(__file__).parent / '.cache'
CACHE_DIR.mkdir(exist_ok=True)


class DataRefresher:
    """Refresh data from all sources into SQLite."""

    def __init__(self, strict_validation: bool = False):
        self.db = DataStore()
        self.validator = DataValidator(strict_mode=strict_validation)
        self.validation_errors = []

    def refresh_all(self, force: bool = False, quiet: bool = False):
        """Refresh all data sources."""
        if not quiet:
            print("=" * 60)
            print("REFRESHING ALL DATA SOURCES")
            print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 60)
            print()

        results = {}

        # Queue data
        if not quiet:
            print("--- Queue Data ---")
        results['nyiso'] = self.refresh_nyiso(quiet)
        results['ercot'] = self.refresh_ercot(quiet)
        results['miso'] = self.refresh_miso(quiet)  # Direct API with developer data
        results['pjm'] = self.refresh_pjm(quiet)  # Direct Excel files (manual download)
        results['caiso'] = self.refresh_caiso(quiet)  # Direct Excel download
        results['spp'] = self.refresh_spp(quiet)  # gridstatus (Python 3.10+)
        results['isone'] = self.refresh_isone(quiet)  # gridstatus (Python 3.10+)
        results['lbl'] = self.refresh_lbl(quiet)

        # Market data
        if not quiet:
            print("\n--- Market Data ---")
        results['lmp'] = self.refresh_market_data('lmp', quiet)
        results['capacity'] = self.refresh_market_data('capacity', quiet)
        results['transmission'] = self.refresh_market_data('transmission', quiet)
        results['ppa'] = self.refresh_market_data('ppa', quiet)
        results['permits'] = self.refresh_market_data('permits', quiet)

        # Create snapshot after refresh
        if not quiet:
            print("\nCreating snapshot...")
        rows = self.db.create_snapshot()
        if not quiet:
            if rows:
                print(f"  Snapshot created: {rows:,} rows")
            else:
                print("  Snapshot already exists for today")

        # Summary
        if not quiet:
            print("\n" + "=" * 60)
            print("REFRESH SUMMARY")
            print("=" * 60)
            for source, result in results.items():
                status = "OK" if result.get('success') else "FAILED"
                if source in ['nyiso', 'ercot', 'miso', 'pjm', 'caiso', 'spp', 'isone', 'lbl']:
                    added = result.get('added', 0)
                    updated = result.get('updated', 0)
                    print(f"  {source}: {status} (+{added} new, {updated} updated)")
                else:
                    print(f"  {source}: {status}")

            # Print validation summary
            self.validator.print_summary()

        return results

    def refresh_market_data(self, data_type: str, quiet: bool = False) -> dict:
        """Refresh market data modules."""
        try:
            if data_type == 'lmp':
                from lmp_data import LMPData
                if not quiet:
                    print("Refreshing LMP data...")
                lmp = LMPData()
                lmp.refresh_all()
                return {'success': True}

            elif data_type == 'capacity':
                from capacity_data import CapacityData
                if not quiet:
                    print("Refreshing capacity market data...")
                cap = CapacityData()
                cap.refresh_all()
                return {'success': True}

            elif data_type == 'transmission':
                from transmission_data import TransmissionData
                if not quiet:
                    print("Refreshing transmission data...")
                tx = TransmissionData()
                tx.refresh_all()
                return {'success': True}

            elif data_type == 'ppa':
                from ppa_data import PPAData
                if not quiet:
                    print("Refreshing PPA data...")
                ppa = PPAData()
                ppa.refresh_all()
                return {'success': True}

            elif data_type == 'permits':
                from permitting_data import PermittingData
                if not quiet:
                    print("Refreshing permitting data...")
                permits = PermittingData()
                permits.refresh_all()
                return {'success': True}

            else:
                return {'success': False, 'error': f'Unknown data type: {data_type}'}

        except Exception as e:
            if not quiet:
                print(f"  ERROR: {e}")
            return {'success': False, 'error': str(e)}

    def refresh_nyiso(self, quiet: bool = False) -> dict:
        """Refresh NYISO queue data using comprehensive loader (all 7 sheets)."""
        if not quiet:
            print("Refreshing NYISO (comprehensive - all sheets)...")
        log_id = self.db.log_refresh_start('nyiso')

        try:
            from nyiso_loader import NYISOLoader, refresh_nyiso as nyiso_refresh

            # Use the comprehensive nyiso_loader which processes:
            # - Interconnection Queue (active)
            # - Cluster Projects (active)
            # - Affected System Studies (active)
            # - Withdrawn
            # - Cluster Projects-Withdrawn
            # - Affected System-Withdrawn
            # - In Service (completed)

            result = nyiso_refresh(quiet=quiet)

            if result['success']:
                stats = {
                    'added': result.get('added', 0),
                    'updated': result.get('updated', 0),
                    'unchanged': result.get('unchanged', 0),
                }
                self.db.log_refresh_complete(log_id, stats)
                return {'success': True, **stats}
            else:
                raise Exception(result.get('error', 'Unknown error'))

        except ImportError:
            # Fallback to direct download if nyiso_loader not available
            if not quiet:
                print("  Falling back to direct download...")
            return self._refresh_nyiso_direct(quiet, log_id)

        except Exception as e:
            if not quiet:
                print(f"  ERROR: {e}")
            self.db.log_refresh_complete(log_id, {}, str(e))
            return {'success': False, 'error': str(e)}

    def _refresh_nyiso_direct(self, quiet: bool, log_id: int) -> dict:
        """Fallback: Direct download of NYISO queue (active sheet only)."""
        try:
            url = "https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx"
            cache_path = CACHE_DIR / 'nyiso_queue.xlsx'

            if not quiet:
                print("  Downloading from NYISO...")
            response = requests.get(url, timeout=60, verify=False)
            response.raise_for_status()

            with open(cache_path, 'wb') as f:
                f.write(response.content)

            df = pd.read_excel(cache_path)
            if not quiet:
                print(f"  Loaded {len(df)} projects")

            normalized = self._normalize_nyiso(df)
            normalized = normalized[
                normalized['queue_id'].notna() &
                (normalized['queue_id'] != '') &
                (~normalized['queue_id'].astype(str).str.lower().isin(['nan', 'none', 'notes:']))
            ]

            stats = self.db.upsert_projects(normalized, source='nyiso', region='NYISO')
            if not quiet:
                print(f"  Added: {stats['added']}, Updated: {stats['updated']}, Unchanged: {stats['unchanged']}")

            self.db.log_refresh_complete(log_id, stats)
            return {'success': True, **stats}

        except Exception as e:
            if not quiet:
                print(f"  ERROR: {e}")
            self.db.log_refresh_complete(log_id, {}, str(e))
            return {'success': False, 'error': str(e)}

    def refresh_ercot(self, quiet: bool = False) -> dict:
        """Refresh ERCOT GIS report via direct_fetcher (gets latest document)."""
        if not quiet:
            print("Refreshing ERCOT...")
        log_id = self.db.log_refresh_start('ercot')

        try:
            from direct_fetcher import DirectFetcher
            fetcher = DirectFetcher()

            if not quiet:
                print("  Fetching latest ERCOT GIS report...")
            df = fetcher.fetch_ercot(use_cache=False)

            if df.empty:
                raise Exception("No data returned from ERCOT")

            if not quiet:
                print(f"  Loaded {len(df)} projects")

            # Validate raw data before normalization
            validation = self.validator.validate_source_data(df, 'ercot', raise_on_error=False)
            if not validation.is_valid:
                if not quiet:
                    print(f"  VALIDATION FAILED:")
                    for err in validation.errors:
                        print(f"    - {err}")
                raise ValidationError(f"ERCOT validation failed", validation.errors)

            if validation.warnings and not quiet:
                print(f"  Validation warnings:")
                for warn in validation.warnings[:3]:
                    print(f"    - {warn[:70]}...")

            # Normalize columns
            normalized = self._normalize_ercot(df)

            # Filter out invalid queue_ids
            normalized = normalized[
                normalized['queue_id'].notna() &
                (normalized['queue_id'] != '') &
                (~normalized['queue_id'].astype(str).str.lower().isin(['nan', 'none']))
            ]

            if not quiet:
                print(f"  After filtering: {len(normalized)} valid projects")

            # Upsert to database
            stats = self.db.upsert_projects(normalized, source='ercot', region='ERCOT')
            if not quiet:
                print(f"  Added: {stats['added']}, Updated: {stats['updated']}, Unchanged: {stats['unchanged']}")

            # Backfill queue dates from LBL historical data
            backfill_count = self._backfill_ercot_queue_dates()
            if not quiet and backfill_count > 0:
                print(f"  Backfilled {backfill_count} queue dates from LBL data")
            stats['backfilled_dates'] = backfill_count

            self.db.log_refresh_complete(log_id, stats)
            return {'success': True, **stats}

        except Exception as e:
            if not quiet:
                print(f"  ERROR: {e}")
            self.db.log_refresh_complete(log_id, {}, str(e))
            return {'success': False, 'error': str(e)}

    def refresh_miso(self, quiet: bool = False) -> dict:
        """Refresh MISO queue data via public API (includes developer data)."""
        if not quiet:
            print("Refreshing MISO (public API)...")
        log_id = self.db.log_refresh_start('miso_api')

        try:
            from miso_loader import MISOLoader
            loader = MISOLoader()

            if not quiet:
                print("  Fetching from MISO API...")
            df = loader.load(use_cache=False)

            if df.empty:
                raise Exception("No data returned from MISO API")

            if not quiet:
                print(f"  Loaded {len(df)} projects")

            # Validate raw data before normalization
            validation = self.validator.validate_source_data(df, 'miso', raise_on_error=False)
            if not validation.is_valid:
                if not quiet:
                    print(f"  VALIDATION FAILED:")
                    for err in validation.errors:
                        print(f"    - {err}")
                raise ValidationError(f"MISO validation failed", validation.errors)

            if validation.warnings and not quiet:
                print(f"  Validation warnings:")
                for warn in validation.warnings[:3]:
                    print(f"    - {warn[:70]}...")

            # Normalize columns
            normalized = self._normalize_miso(df)

            # Filter out invalid queue_ids
            normalized = normalized[
                normalized['queue_id'].notna() &
                (normalized['queue_id'] != '') &
                (~normalized['queue_id'].astype(str).str.lower().isin(['nan', 'none']))
            ]

            if not quiet:
                print(f"  After filtering: {len(normalized)} valid projects")

                # Report developer coverage
                dev_count = normalized['developer'].notna() & (normalized['developer'] != '')
                print(f"  Developer coverage: {dev_count.sum()}/{len(normalized)} ({dev_count.sum()/len(normalized)*100:.1f}%)")

            # Upsert to database
            stats = self.db.upsert_projects(normalized, source='miso_api', region='MISO')
            if not quiet:
                print(f"  Added: {stats['added']}, Updated: {stats['updated']}, Unchanged: {stats['unchanged']}")

            # Merge developer data into existing LBL records with matching queue_ids
            merge_count = self._merge_miso_developers()
            if not quiet and merge_count > 0:
                print(f"  Merged developer data into {merge_count} LBL records")
            stats['merged'] = merge_count

            self.db.log_refresh_complete(log_id, stats)
            return {'success': True, **stats}

        except Exception as e:
            if not quiet:
                print(f"  ERROR: {e}")
            self.db.log_refresh_complete(log_id, {}, str(e))
            return {'success': False, 'error': str(e)}

    def refresh_pjm(self, quiet: bool = False) -> dict:
        """Refresh PJM queue data from downloaded Excel files.

        Requires manual download of:
        - PlanningQueues.xlsx from https://www.pjm.com/planning/services-requests/interconnection-queues
        - CycleProjects-All.xlsx from PJM Transition Cycle reports (optional, for developer data)
        """
        if not quiet:
            print("Refreshing PJM (from Excel files)...")
        log_id = self.db.log_refresh_start('pjm_direct')

        try:
            from pjm_loader import PJMLoader, refresh_pjm as pjm_refresh

            loader = PJMLoader()
            files = loader.check_files()

            if not files['planning_queues']:
                raise Exception(
                    "PJM PlanningQueues.xlsx not found. "
                    "Download from https://www.pjm.com/planning/services-requests/interconnection-queues"
                )

            # Use the pjm_loader refresh function
            result = pjm_refresh(quiet=quiet)

            if result['success']:
                stats = {
                    'added': result.get('added', 0),
                    'updated': result.get('updated', 0),
                    'unchanged': result.get('unchanged', 0),
                }
                self.db.log_refresh_complete(log_id, stats)
                return {'success': True, **stats}
            else:
                raise Exception(result.get('error', 'Unknown error'))

        except Exception as e:
            if not quiet:
                print(f"  ERROR: {e}")
            self.db.log_refresh_complete(log_id, {}, str(e))
            return {'success': False, 'error': str(e)}

    def refresh_caiso(self, quiet: bool = False) -> dict:
        """Refresh CAISO queue data via direct Excel download."""
        if not quiet:
            print("Refreshing CAISO...")
        log_id = self.db.log_refresh_start('caiso')

        try:
            from direct_fetcher import DirectFetcher
            fetcher = DirectFetcher()

            if not quiet:
                print("  Downloading from CAISO...")
            df = fetcher.fetch_caiso(use_cache=False)

            if df.empty:
                raise Exception("No data returned from CAISO")

            if not quiet:
                print(f"  Loaded {len(df)} projects")

            # Validate raw data before normalization
            validation = self.validator.validate_source_data(df, 'caiso', raise_on_error=False)
            if not validation.is_valid:
                if not quiet:
                    print(f"  VALIDATION FAILED:")
                    for err in validation.errors:
                        print(f"    - {err}")
                raise ValidationError(f"CAISO validation failed", validation.errors)

            if validation.warnings and not quiet:
                print(f"  Validation warnings:")
                for warn in validation.warnings[:3]:
                    print(f"    - {warn[:70]}...")

            # Normalize columns
            normalized = self._normalize_caiso(df)

            # Filter out invalid queue_ids
            normalized = normalized[
                normalized['queue_id'].notna() &
                (normalized['queue_id'] != '') &
                (~normalized['queue_id'].astype(str).str.lower().isin(['nan', 'none']))
            ]

            if not quiet:
                print(f"  After filtering: {len(normalized)} valid projects")

            # Upsert to database
            stats = self.db.upsert_projects(normalized, source='caiso', region='CAISO')
            if not quiet:
                print(f"  Added: {stats['added']}, Updated: {stats['updated']}, Unchanged: {stats['unchanged']}")

            self.db.log_refresh_complete(log_id, stats)
            return {'success': True, **stats}

        except Exception as e:
            if not quiet:
                print(f"  ERROR: {e}")
            self.db.log_refresh_complete(log_id, {}, str(e))
            return {'success': False, 'error': str(e)}

    def refresh_spp(self, quiet: bool = False) -> dict:
        """Refresh SPP queue data via gridstatus (requires Python 3.10+)."""
        if not quiet:
            print("Refreshing SPP (gridstatus)...")
        log_id = self.db.log_refresh_start('spp')

        try:
            from direct_fetcher import DirectFetcher
            fetcher = DirectFetcher()

            if not quiet:
                print("  Fetching from SPP via gridstatus...")
            df = fetcher.fetch_spp(use_cache=False)

            if df.empty:
                raise Exception("No data returned from SPP")

            if not quiet:
                print(f"  Loaded {len(df)} projects")

            # Validate raw data before normalization
            validation = self.validator.validate_source_data(df, 'spp', raise_on_error=False)
            if not validation.is_valid:
                if not quiet:
                    print(f"  VALIDATION FAILED:")
                    for err in validation.errors:
                        print(f"    - {err}")
                raise ValidationError(f"SPP validation failed", validation.errors)

            if validation.warnings and not quiet:
                print(f"  Validation warnings:")
                for warn in validation.warnings[:3]:
                    print(f"    - {warn[:70]}...")

            # Normalize columns
            normalized = self._normalize_spp(df)

            # Filter out invalid queue_ids
            normalized = normalized[
                normalized['queue_id'].notna() &
                (normalized['queue_id'] != '') &
                (~normalized['queue_id'].astype(str).str.lower().isin(['nan', 'none']))
            ]

            if not quiet:
                print(f"  After filtering: {len(normalized)} valid projects")

                # Report developer coverage
                dev_count = normalized['developer'].notna() & (normalized['developer'] != '')
                print(f"  Developer coverage: {dev_count.sum()}/{len(normalized)} ({dev_count.sum()/len(normalized)*100:.1f}%)")

            # Upsert to database
            stats = self.db.upsert_projects(normalized, source='spp', region='SPP')
            if not quiet:
                print(f"  Added: {stats['added']}, Updated: {stats['updated']}, Unchanged: {stats['unchanged']}")

            self.db.log_refresh_complete(log_id, stats)
            return {'success': True, **stats}

        except Exception as e:
            if not quiet:
                print(f"  ERROR: {e}")
            self.db.log_refresh_complete(log_id, {}, str(e))
            return {'success': False, 'error': str(e)}

    def refresh_isone(self, quiet: bool = False) -> dict:
        """Refresh ISO-NE queue data via gridstatus (requires Python 3.10+)."""
        if not quiet:
            print("Refreshing ISO-NE (gridstatus)...")
        log_id = self.db.log_refresh_start('isone')

        try:
            from direct_fetcher import DirectFetcher
            fetcher = DirectFetcher()

            if not quiet:
                print("  Fetching from ISO-NE via gridstatus...")
            df = fetcher.fetch_isone(use_cache=False)

            if df.empty:
                raise Exception("No data returned from ISO-NE")

            if not quiet:
                print(f"  Loaded {len(df)} projects")

            # Validate raw data before normalization
            validation = self.validator.validate_source_data(df, 'isone', raise_on_error=False)
            if not validation.is_valid:
                if not quiet:
                    print(f"  VALIDATION FAILED:")
                    for err in validation.errors:
                        print(f"    - {err}")
                raise ValidationError(f"ISO-NE validation failed", validation.errors)

            if validation.warnings and not quiet:
                print(f"  Validation warnings:")
                for warn in validation.warnings[:3]:
                    print(f"    - {warn[:70]}...")

            # Normalize columns
            normalized = self._normalize_isone(df)

            # Filter out invalid queue_ids
            normalized = normalized[
                normalized['queue_id'].notna() &
                (normalized['queue_id'] != '') &
                (~normalized['queue_id'].astype(str).str.lower().isin(['nan', 'none']))
            ]

            if not quiet:
                print(f"  After filtering: {len(normalized)} valid projects")

                # Report developer coverage
                dev_count = normalized['developer'].notna() & (normalized['developer'] != '')
                print(f"  Developer coverage: {dev_count.sum()}/{len(normalized)} ({dev_count.sum()/len(normalized)*100:.1f}%)")

            # Upsert to database
            stats = self.db.upsert_projects(normalized, source='isone', region='ISO-NE')
            if not quiet:
                print(f"  Added: {stats['added']}, Updated: {stats['updated']}, Unchanged: {stats['unchanged']}")

            self.db.log_refresh_complete(log_id, stats)
            return {'success': True, **stats}

        except Exception as e:
            if not quiet:
                print(f"  ERROR: {e}")
            self.db.log_refresh_complete(log_id, {}, str(e))
            return {'success': False, 'error': str(e)}

    def refresh_lbl(self, quiet: bool = False) -> dict:
        """Load LBL historical data (from cache - manual download)."""
        if not quiet:
            print("Loading LBL historical data...")
        log_id = self.db.log_refresh_start('lbl')

        try:
            cache_path = CACHE_DIR / 'lbl_queued_up.xlsx'

            if not cache_path.exists():
                if not quiet:
                    print("  LBL data not found in cache.")
                    print("  Download manually from: https://emp.lbl.gov/queues")
                    print(f"  Save to: {cache_path}")
                self.db.log_refresh_complete(log_id, {}, "File not found")
                return {'success': False, 'error': 'File not found'}

            # Load main data sheet
            df = pd.read_excel(cache_path, sheet_name='03. Complete Queue Data', header=1)
            if not quiet:
                print(f"  Loaded {len(df):,} historical projects")

            # Validate raw data before normalization
            validation = self.validator.validate_source_data(df, 'lbl', raise_on_error=False)
            if not validation.is_valid:
                if not quiet:
                    print(f"  VALIDATION FAILED:")
                    for err in validation.errors:
                        print(f"    - {err}")
                raise ValidationError(f"LBL validation failed", validation.errors)

            if validation.warnings and not quiet:
                print(f"  Validation warnings:")
                for warn in validation.warnings[:3]:
                    print(f"    - {warn[:70]}...")

            # Normalize columns
            normalized = self._normalize_lbl(df)

            # Upsert to database (by region)
            total_stats = {'added': 0, 'updated': 0, 'unchanged': 0}

            for region in normalized['region'].unique():
                region_df = normalized[normalized['region'] == region]
                stats = self.db.upsert_projects(region_df, source='lbl', region=region)
                total_stats['added'] += stats['added']
                total_stats['updated'] += stats['updated']
                total_stats['unchanged'] += stats['unchanged']

            if not quiet:
                print(f"  Added: {total_stats['added']}, Updated: {total_stats['updated']}, Unchanged: {total_stats['unchanged']}")

            self.db.log_refresh_complete(log_id, total_stats)
            return {'success': True, **total_stats}

        except Exception as e:
            if not quiet:
                print(f"  ERROR: {e}")
            self.db.log_refresh_complete(log_id, {}, str(e))
            return {'success': False, 'error': str(e)}

    def _normalize_nyiso(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize NYISO column names."""
        # NYISO uses different column names than expected
        # Queue Pos. (not Queue Position)
        # Developer/Interconnection Customer (not Developer)
        # SP (MW) for summer peak capacity
        # Points of Interconnection (not POI Location)
        # Date of IR (not Queue Date)
        # Proposed COD (not Projected COD)
        return pd.DataFrame({
            'queue_id': df.get('Queue Pos.', df.get('Queue Position', '')),
            'name': df.get('Project Name', ''),
            'developer': df.get('Developer/Interconnection Customer', df.get('Developer', '')),
            'capacity_mw': pd.to_numeric(df.get('SP (MW)', df.get('Capacity (MW)', 0)), errors='coerce'),
            'type': df.get('Type/ Fuel', ''),
            'status': df.get('S', ''),
            'state': df.get('State', ''),
            'county': df.get('County', ''),
            'poi': df.get('Points of Interconnection', df.get('POI Location', '')),
            'queue_date': df.get('Date of IR', df.get('Queue Date', '')),
            'cod': df.get('Proposed COD', df.get('Projected COD', '')),
            'region': 'NYISO',
        })

    def _normalize_ercot(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize ERCOT column names (handles both old and new formats)."""
        return pd.DataFrame({
            'queue_id': df.get('Queue ID', df.get('INR', '')),
            'name': df.get('Project Name', ''),
            'developer': df.get('Developer', df.get('Interconnecting Entity', '')),
            'capacity_mw': pd.to_numeric(df.get('Capacity (MW)', 0), errors='coerce'),
            'type': df.get('Generation Type', df.get('Fuel', '')),
            'status': df.get('Status', df.get('GIM Study Phase', '')),
            'state': 'TX',
            'county': df.get('County', ''),
            'poi': df.get('POI Location', ''),
            'queue_date': '',
            'cod': df.get('Proposed COD', df.get('Projected COD', '')),
            'region': 'ERCOT',
        })

    def _normalize_lbl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize LBL column names."""
        return pd.DataFrame({
            'queue_id': df.get('q_id', ''),
            'name': df.get('project_name', ''),
            'developer': df.get('developer', df.get('entity', '')),
            'capacity_mw': pd.to_numeric(df.get('mw1', 0), errors='coerce'),
            'type': df.get('type_clean', ''),
            'status': df.get('q_status', ''),
            'state': df.get('state', ''),
            'county': df.get('county', ''),
            'poi': df.get('poi_name', ''),
            'queue_date': df.get('q_date', ''),
            'cod': df.get('prop_date', ''),
            'region': df.get('region', 'Unknown'),
        })

    def _normalize_miso(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize MISO API column names.

        Handles both:
        - Raw API data (from direct_fetcher with columns like projectNumber, summerNetMW)
        - Pre-normalized data (from miso_loader with columns like queue_id, capacity_mw)
        """
        # Check if data is already normalized (from miso_loader)
        if 'queue_id' in df.columns:
            # Data is already normalized, just ensure all required columns exist
            return pd.DataFrame({
                'queue_id': df.get('queue_id', ''),
                'name': df.get('name', ''),
                'developer': df.get('utility', df.get('developer', '')),  # miso_loader uses 'utility'
                'capacity_mw': pd.to_numeric(df.get('capacity_mw', 0), errors='coerce'),
                'type': df.get('type', ''),
                'status': df.get('status', ''),
                'state': df.get('state', ''),
                'county': df.get('county', ''),
                'poi': df.get('poi', ''),
                'queue_date': '',  # MISO API doesn't provide queue dates
                'cod': df.get('cod', '').astype(str) if 'cod' in df.columns else '',
                'region': 'MISO',
            })

        # Raw API data - map to normalized schema
        return pd.DataFrame({
            'queue_id': df.get('Queue ID', df.get('projectNumber', '')),
            'name': '',  # MISO API doesn't have project names
            'developer': df.get('Developer', df.get('transmissionOwner', '')),
            'capacity_mw': pd.to_numeric(df.get('Capacity (MW)', df.get('summerNetMW', 0)), errors='coerce'),
            'type': df.get('Generation Type', df.get('fuelType', '')),
            'status': df.get('Status', df.get('applicationStatus', '')),
            'state': df.get('State', df.get('state', '')),
            'county': df.get('County', df.get('county', '')),
            'poi': df.get('POI', df.get('poiName', '')),
            'queue_date': df.get('Queue Date', df.get('queueDate', '')),
            'cod': df.get('Proposed COD', df.get('inService', '')),
            'region': 'MISO',
        })

    def _normalize_caiso(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize CAISO column names."""
        # CAISO public queue doesn't include developer names
        return pd.DataFrame({
            'queue_id': df.get('Queue Position', ''),
            'name': df.get('Project Name', ''),
            'developer': '',  # Not in CAISO public file - enriched via EIA
            'capacity_mw': pd.to_numeric(df.get('Net MWs to Grid', df.get('On-Peak MWs Deliverability', 0)), errors='coerce'),
            'type': df.get('Fuel-1', df.get('Type-1', '')),
            'status': df.get('Application Status', ''),
            'state': 'CA',  # All CAISO projects are in CA
            'county': df.get('County', ''),
            'poi': df.get('Full Capacity, Pair-wise, Off-Peak Deliverability Option', ''),
            'queue_date': df.get('Queue Date', df.get('Interconnection Request Receive Date', '')),
            'cod': df.get('Actual or Expected On-line Date', ''),
            'region': 'CAISO',
        })

    def _normalize_spp(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize SPP column names from gridstatus."""
        return pd.DataFrame({
            'queue_id': df.get('Queue ID', ''),
            'name': df.get('Project Name', ''),
            'developer': df.get('Developer', df.get('Interconnecting Entity', '')),
            'capacity_mw': pd.to_numeric(df.get('Capacity (MW)', 0), errors='coerce'),
            'type': df.get('Generation Type', ''),
            'status': df.get('Status', ''),
            'state': df.get('State', ''),
            'county': df.get('County', ''),
            'poi': df.get('Interconnection Location', ''),
            'queue_date': df.get('Queue Date', ''),
            'cod': df.get('Proposed Completion Date', df.get('Commercial Operation Date', '')),
            'region': 'SPP',
        })

    def _normalize_isone(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize ISO-NE column names from gridstatus."""
        return pd.DataFrame({
            'queue_id': df.get('Queue ID', ''),
            'name': df.get('Project Name', ''),
            'developer': df.get('Developer', df.get('Interconnecting Entity', '')),
            'capacity_mw': pd.to_numeric(df.get('Capacity (MW)', 0), errors='coerce'),
            'type': df.get('Generation Type', ''),
            'status': df.get('Status', df.get('Project Status', '')),
            'state': df.get('State', ''),
            'county': df.get('County', ''),
            'poi': df.get('Interconnection Location', ''),
            'queue_date': df.get('Queue Date', ''),
            'cod': df.get('Proposed Completion Date', df.get('Op Date', '')),
            'region': 'ISO-NE',
        })

    def _merge_miso_developers(self) -> int:
        """
        Merge developer data from MISO API records into LBL records.

        Returns the number of records updated.
        """
        conn = self.db._get_conn()
        cursor = conn.cursor()

        # Get developer data from API records
        cursor.execute('''
            SELECT queue_id, developer
            FROM projects
            WHERE region = 'MISO' AND source = 'miso_api'
            AND developer IS NOT NULL AND developer != ''
        ''')
        api_devs = cursor.fetchall()

        # Update LBL records with matching queue_ids that are missing developers
        updated = 0
        for queue_id, developer in api_devs:
            cursor.execute('''
                UPDATE projects
                SET developer = ?
                WHERE region = 'MISO' AND source = 'lbl'
                AND queue_id = ?
                AND (developer IS NULL OR developer = '')
            ''', (developer, queue_id))
            updated += cursor.rowcount

        conn.commit()
        conn.close()
        return updated

    def _backfill_ercot_queue_dates(self) -> int:
        """
        Backfill ERCOT queue dates from LBL historical data.

        ERCOT GIS report doesn't include queue dates, but LBL has 92%+ coverage
        for historical ERCOT projects. This merges those dates into ERCOT records.

        Returns the number of records updated.
        """
        import pandas as pd
        from datetime import datetime, timedelta

        cache_path = CACHE_DIR / 'lbl_queued_up.xlsx'
        if not cache_path.exists():
            return 0

        try:
            # Load LBL ERCOT records with queue dates
            df = pd.read_excel(cache_path, sheet_name='03. Complete Queue Data', header=1)
            ercot_lbl = df[df['region'] == 'ERCOT'][['q_id', 'q_date']].dropna(subset=['q_date'])

            if ercot_lbl.empty:
                return 0

            # Convert Excel serial dates to ISO format
            def excel_to_date(serial):
                if pd.isna(serial):
                    return None
                try:
                    # Excel date serial number starts from 1900-01-01
                    return (datetime(1899, 12, 30) + timedelta(days=int(serial))).strftime('%Y-%m-%d')
                except:
                    return None

            ercot_lbl['queue_date'] = ercot_lbl['q_date'].apply(excel_to_date)
            ercot_lbl = ercot_lbl[ercot_lbl['queue_date'].notna()]

            conn = self.db._get_conn()
            cursor = conn.cursor()

            # Update ERCOT records with matching queue_ids that are missing dates
            updated = 0
            for _, row in ercot_lbl.iterrows():
                cursor.execute('''
                    UPDATE projects
                    SET queue_date = ?
                    WHERE region = 'ERCOT'
                    AND queue_id = ?
                    AND (queue_date IS NULL OR queue_date = '')
                ''', (row['queue_date'], row['q_id']))
                updated += cursor.rowcount

            conn.commit()
            conn.close()
            return updated

        except Exception as e:
            print(f"  Warning: Could not backfill ERCOT dates: {e}")
            return 0


def show_status(db: DataStore):
    """Show refresh status."""
    print("=" * 60)
    print("REFRESH STATUS")
    print("=" * 60)

    status = db.get_refresh_status()

    if not status:
        print("\nNo refresh history. Run: python3 refresh_data.py")
        return

    print("\nLast successful refresh by source:")
    for s in status:
        print(f"  {s['source']}: {s['last_success']} ({s['last_status']})")

    stats = db.get_stats()
    print(f"\nDatabase contains:")
    print(f"  {stats['total_projects']:,} projects")
    print(f"  {stats['total_capacity_gw']:.1f} GW total capacity")

    if stats['by_region']:
        print(f"\nBy region:")
        for r in stats['by_region']:
            print(f"  {r['region']}: {r['count']:,} projects")


def show_changes(db: DataStore, days: int = 7):
    """Show recent changes."""
    print("=" * 60)
    print(f"CHANGES IN LAST {days} DAYS")
    print("=" * 60)

    changes = db.get_changes(since_days=days)

    if changes.empty:
        print("\nNo changes detected in this period.")
        return

    # Group by change type
    new_projects = changes[changes['change_type'] == 'new_project']
    status_changes = changes[changes['change_type'] == 'status_change']

    if not new_projects.empty:
        print(f"\nNew Projects ({len(new_projects)}):")
        for _, row in new_projects.head(20).iterrows():
            name = row['project_name'][:40] if row['project_name'] else 'Unknown'
            print(f"  [{row['region']}] {row['queue_id']}: {name}")

    if not status_changes.empty:
        print(f"\nStatus Changes ({len(status_changes)}):")
        for _, row in status_changes.head(20).iterrows():
            name = row['project_name'][:30] if row['project_name'] else 'Unknown'
            print(f"  [{row['region']}] {name}: {row['old_value']} -> {row['new_value']}")


def main():
    parser = argparse.ArgumentParser(
        description="Refresh interconnection queue data from all sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 refresh_data.py                # Refresh all sources
  python3 refresh_data.py --source ercot # Refresh ERCOT only
  python3 refresh_data.py --status       # Show refresh status
  python3 refresh_data.py --changes 30   # Show changes in last 30 days
        """
    )

    parser.add_argument('--source', '-s', choices=['nyiso', 'ercot', 'miso', 'pjm', 'caiso', 'spp', 'isone', 'lbl', 'all'],
                       default='all', help='Data source to refresh')
    parser.add_argument('--status', action='store_true', help='Show refresh status')
    parser.add_argument('--changes', type=int, metavar='DAYS',
                       help='Show changes in last N days')
    parser.add_argument('--force', '-f', action='store_true',
                       help='Force refresh even if recently updated')
    parser.add_argument('--cron', action='store_true',
                       help='Cron-friendly mode (quiet, exit codes only)')

    args = parser.parse_args()

    db = DataStore()

    if args.status:
        show_status(db)
        return 0

    if args.changes:
        show_changes(db, args.changes)
        return 0

    # Run refresh
    refresher = DataRefresher()
    quiet = args.cron

    try:
        if args.source == 'all':
            results = refresher.refresh_all(force=args.force, quiet=quiet)
        elif args.source == 'nyiso':
            results = {'nyiso': refresher.refresh_nyiso(quiet=quiet)}
        elif args.source == 'ercot':
            results = {'ercot': refresher.refresh_ercot(quiet=quiet)}
        elif args.source == 'miso':
            results = {'miso': refresher.refresh_miso(quiet=quiet)}
        elif args.source == 'pjm':
            results = {'pjm': refresher.refresh_pjm(quiet=quiet)}
        elif args.source == 'caiso':
            results = {'caiso': refresher.refresh_caiso(quiet=quiet)}
        elif args.source == 'spp':
            results = {'spp': refresher.refresh_spp(quiet=quiet)}
        elif args.source == 'isone':
            results = {'isone': refresher.refresh_isone(quiet=quiet)}
        elif args.source == 'lbl':
            results = {'lbl': refresher.refresh_lbl(quiet=quiet)}

        # Check for failures in cron mode
        if quiet:
            failures = [k for k, v in results.items() if not v.get('success')]
            if failures:
                print(f"FAILED: {', '.join(failures)}", file=sys.stderr)
                return 1
        return 0

    except Exception as e:
        if quiet:
            print(f"FATAL: {e}", file=sys.stderr)
        else:
            print(f"\nFATAL ERROR: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
