#!/usr/bin/env python3
"""
Market Intelligence Data Layer

Handles data refresh, change tracking, and trend analysis for the
interconnection queue market intelligence dashboard.
"""

import os
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np

try:
    import gridstatus
    HAS_GRIDSTATUS = True
except (ImportError, TypeError):
    # TypeError can occur with Python < 3.10 due to type hints in gridstatus
    HAS_GRIDSTATUS = False
    gridstatus = None

# Data storage paths
DATA_DIR = Path(__file__).parent / '.data' / 'market_intel'
SNAPSHOTS_DIR = DATA_DIR / 'snapshots'
CACHE_DIR = Path(__file__).parent / '.cache'


def ensure_dirs():
    """Create necessary directories."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)


class MarketData:
    """Manages queue data snapshots and change tracking."""

    SUPPORTED_ISOS = ['nyiso', 'pjm', 'miso', 'caiso', 'ercot', 'spp', 'isone']

    # Map lowercase ISO names to actual gridstatus class names
    ISO_CLASS_MAP = {
        'nyiso': 'NYISO',
        'pjm': 'PJM',
        'miso': 'MISO',
        'caiso': 'CAISO',
        'ercot': 'Ercot',  # Note: CamelCase, not all caps
        'spp': 'SPP',
        'isone': 'ISONE',
    }

    def __init__(self):
        ensure_dirs()
        self._current_data: Dict[str, pd.DataFrame] = {}
        self._metadata_file = DATA_DIR / 'metadata.json'
        self._load_metadata()

    def _load_metadata(self):
        """Load tracking metadata."""
        if self._metadata_file.exists():
            with open(self._metadata_file) as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {
                'last_refresh': None,
                'snapshots': [],
                'iso_status': {}
            }

    def _save_metadata(self):
        """Save tracking metadata."""
        with open(self._metadata_file, 'w') as f:
            json.dump(self.metadata, f, indent=2, default=str)

    def refresh_data(self, isos: List[str] = None, pjm_api_key: str = None) -> Dict[str, Any]:
        """
        Refresh queue data from ISOs.

        Uses DirectFetcher for NYISO, CAISO, ERCOT (bypasses gridstatus bugs).
        Uses gridstatus for MISO, SPP, ISONE.
        PJM requires API key.

        Returns dict with refresh status and any errors.
        """
        isos = isos or self.SUPPORTED_ISOS
        results = {'success': True, 'refreshed': [], 'errors': []}
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # ISOs that need direct fetching due to gridstatus parsing bugs
        DIRECT_FETCH_ISOS = {'nyiso', 'caiso', 'ercot'}

        for iso_name in isos:
            iso_lower = iso_name.lower()

            try:
                df = None

                # Use DirectFetcher for problematic ISOs
                if iso_lower in DIRECT_FETCH_ISOS:
                    try:
                        from direct_fetcher import DirectFetcher
                        fetcher = DirectFetcher()

                        if iso_lower == 'nyiso':
                            df = fetcher.fetch_nyiso(use_cache=False)
                        elif iso_lower == 'caiso':
                            df = fetcher.fetch_caiso(use_cache=False)
                        elif iso_lower == 'ercot':
                            df = fetcher.fetch_ercot(use_cache=False)

                    except ImportError:
                        results['errors'].append(f'{iso_name}: DirectFetcher not available')
                        continue

                # Use gridstatus for PJM (requires API key)
                elif iso_lower == 'pjm':
                    if not HAS_GRIDSTATUS:
                        results['errors'].append(f'{iso_name}: gridstatus not installed')
                        continue

                    api_key = pjm_api_key or os.environ.get('PJM_API_KEY')
                    if not api_key:
                        results['errors'].append(f'{iso_name}: PJM_API_KEY required')
                        continue

                    pjm = gridstatus.PJM(api_key=api_key)
                    df = pjm.get_interconnection_queue()

                # Use gridstatus for others (MISO, SPP, ISONE work well)
                else:
                    if not HAS_GRIDSTATUS:
                        results['errors'].append(f'{iso_name}: gridstatus not installed')
                        continue

                    class_name = self.ISO_CLASS_MAP.get(iso_lower, iso_name.upper())
                    iso_class = getattr(gridstatus, class_name, None)
                    if iso_class is None:
                        results['errors'].append(f'{iso_name}: ISO not found')
                        continue

                    iso_instance = iso_class()
                    df = iso_instance.get_interconnection_queue()

                if df is not None and not df.empty:
                    # Ensure iso column exists
                    if 'iso' not in df.columns:
                        df['iso'] = iso_name.upper()

                    # Convert object columns to string to avoid parquet issues
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            df[col] = df[col].astype(str)

                    # Save snapshot
                    snapshot_path = SNAPSHOTS_DIR / f'{iso_lower}_{timestamp}.parquet'
                    df.to_parquet(snapshot_path)

                    # Update current data
                    self._current_data[iso_lower] = df

                    # Update metadata
                    self.metadata['iso_status'][iso_lower] = {
                        'last_refresh': timestamp,
                        'project_count': len(df),
                        'snapshot_file': str(snapshot_path.name)
                    }

                    results['refreshed'].append(iso_name)
                else:
                    results['errors'].append(f'{iso_name}: No data returned')

            except Exception as e:
                results['errors'].append(f'{iso_name}: {str(e)}')

        # Update global metadata
        self.metadata['last_refresh'] = timestamp
        if timestamp not in self.metadata['snapshots']:
            self.metadata['snapshots'].append(timestamp)
        self._save_metadata()

        if results['errors']:
            results['success'] = len(results['refreshed']) > 0

        return results

    def get_latest_data(self, iso: str = None) -> pd.DataFrame:
        """Get the most recent data for an ISO or all ISOs combined."""
        if iso:
            return self._load_latest_snapshot(iso)

        # Combine all ISOs
        all_data = []
        for iso_name in self.SUPPORTED_ISOS:
            df = self._load_latest_snapshot(iso_name)
            if df is not None and not df.empty:
                df['iso'] = iso_name.upper()
                all_data.append(df)

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def _load_latest_snapshot(self, iso: str) -> Optional[pd.DataFrame]:
        """Load the most recent snapshot for an ISO."""
        if iso in self._current_data:
            return self._current_data[iso]

        # Find latest snapshot file (parquet)
        pattern = f'{iso.lower()}_*.parquet'
        snapshots = sorted(SNAPSHOTS_DIR.glob(pattern), reverse=True)

        if snapshots:
            df = pd.read_parquet(snapshots[0])
            self._current_data[iso] = df
            return df

        # Fall back to cache directory - check parquet first
        cache_file = CACHE_DIR / f'{iso.lower()}_queue.parquet'
        if cache_file.exists():
            df = pd.read_parquet(cache_file)
            self._current_data[iso] = df
            return df

        # Fall back to Excel file in cache
        xlsx_file = CACHE_DIR / f'{iso.lower()}_queue.xlsx'
        if xlsx_file.exists():
            try:
                df = pd.read_excel(xlsx_file)
                self._current_data[iso] = df
                return df
            except Exception:
                pass

        return None

    def get_snapshot(self, iso: str, timestamp: str) -> Optional[pd.DataFrame]:
        """Load a specific historical snapshot."""
        snapshot_path = SNAPSHOTS_DIR / f'{iso.lower()}_{timestamp}.parquet'
        if snapshot_path.exists():
            return pd.read_parquet(snapshot_path)
        return None

    def get_available_snapshots(self, iso: str = None) -> List[str]:
        """Get list of available snapshot timestamps."""
        if iso:
            pattern = f'{iso.lower()}_*.parquet'
        else:
            pattern = '*.parquet'

        snapshots = SNAPSHOTS_DIR.glob(pattern)
        timestamps = set()
        for s in snapshots:
            # Extract timestamp from filename: iso_YYYYMMDD_HHMMSS.parquet
            parts = s.stem.split('_')
            if len(parts) >= 3:
                ts = '_'.join(parts[1:3])
                timestamps.add(ts)

        return sorted(timestamps, reverse=True)

    def detect_changes(self, iso: str, days_back: int = 7) -> Dict[str, Any]:
        """
        Detect changes between current data and historical snapshot.

        Returns dict with new projects, withdrawn, status changes, etc.
        """
        current = self._load_latest_snapshot(iso)
        if current is None or current.empty:
            return {'error': 'No current data available'}

        # Find snapshot from ~days_back ago
        snapshots = sorted(SNAPSHOTS_DIR.glob(f'{iso.lower()}_*.parquet'))
        if len(snapshots) < 2:
            return {'error': 'Not enough historical data for comparison'}

        # Find oldest snapshot within the window
        cutoff = datetime.now() - timedelta(days=days_back)
        historical = None
        historical_date = None

        for snap in snapshots:
            try:
                ts_str = '_'.join(snap.stem.split('_')[1:3])
                ts = datetime.strptime(ts_str, '%Y%m%d_%H%M%S')
                if ts < cutoff:
                    historical = pd.read_parquet(snap)
                    historical_date = ts
                    break
            except:
                continue

        if historical is None:
            # Use oldest available
            historical = pd.read_parquet(snapshots[0])

        return self._compare_snapshots(historical, current, iso)

    def _compare_snapshots(self, old: pd.DataFrame, new: pd.DataFrame, iso: str) -> Dict[str, Any]:
        """Compare two snapshots and identify changes."""
        # Identify the project ID column
        id_col = self._find_id_column(new)
        if id_col is None:
            return {'error': 'Could not identify project ID column'}

        old_ids = set(old[id_col].dropna().astype(str))
        new_ids = set(new[id_col].dropna().astype(str))

        # New projects
        added_ids = new_ids - old_ids
        added = new[new[id_col].astype(str).isin(added_ids)]

        # Withdrawn/removed projects
        removed_ids = old_ids - new_ids
        removed = old[old[id_col].astype(str).isin(removed_ids)]

        # Status changes for existing projects
        common_ids = old_ids & new_ids
        status_changes = []

        status_col = self._find_status_column(new)
        if status_col:
            old_status = old[old[id_col].astype(str).isin(common_ids)][[id_col, status_col]].copy()
            new_status = new[new[id_col].astype(str).isin(common_ids)][[id_col, status_col]].copy()

            merged = old_status.merge(new_status, on=id_col, suffixes=('_old', '_new'))
            changed = merged[merged[f'{status_col}_old'] != merged[f'{status_col}_new']]

            for _, row in changed.iterrows():
                status_changes.append({
                    'project_id': row[id_col],
                    'old_status': row[f'{status_col}_old'],
                    'new_status': row[f'{status_col}_new']
                })

        return {
            'iso': iso.upper(),
            'added_count': len(added_ids),
            'removed_count': len(removed_ids),
            'status_changes_count': len(status_changes),
            'added_projects': added.to_dict('records') if not added.empty else [],
            'removed_projects': removed.to_dict('records') if not removed.empty else [],
            'status_changes': status_changes,
            'total_current': len(new),
            'total_previous': len(old),
            'net_change': len(new) - len(old)
        }

    def _find_id_column(self, df: pd.DataFrame) -> Optional[str]:
        """Find the project ID column."""
        candidates = ['Queue ID', 'queue_id', 'Project ID', 'project_id',
                     'Request ID', 'request_id', 'ID', 'id', 'Queue Number']
        for col in candidates:
            if col in df.columns:
                return col
        # Try partial match
        for col in df.columns:
            if 'id' in col.lower() or 'queue' in col.lower():
                return col
        return None

    def _find_status_column(self, df: pd.DataFrame) -> Optional[str]:
        """Find the status column."""
        candidates = ['Status', 'status', 'Queue Status', 'Study Status',
                     'Interconnection Status', 'Phase', 'Study Phase']
        for col in candidates:
            if col in df.columns:
                return col
        for col in df.columns:
            if 'status' in col.lower():
                return col
        return None


class MacroAnalytics:
    """Compute macro-level trends and statistics."""

    def __init__(self, market_data: MarketData):
        self.market_data = market_data

    def get_summary_stats(self) -> Dict[str, Any]:
        """Get high-level summary statistics across all ISOs."""
        df = self.market_data.get_latest_data()
        if df.empty:
            return {'error': 'No data available'}

        stats = {
            'total_projects': len(df),
            'total_capacity_gw': self._sum_capacity(df) / 1000,
            'by_iso': {},
            'by_type': {},
            'by_status': {},
            'last_refresh': self.market_data.metadata.get('last_refresh')
        }

        # By ISO
        if 'iso' in df.columns:
            for iso in df['iso'].unique():
                iso_df = df[df['iso'] == iso]
                stats['by_iso'][iso] = {
                    'projects': len(iso_df),
                    'capacity_gw': self._sum_capacity(iso_df) / 1000
                }

        # By type
        type_col = self._find_type_column(df)
        if type_col:
            type_counts = df[type_col].value_counts()
            for t, count in type_counts.head(10).items():
                if pd.notna(t):
                    type_df = df[df[type_col] == t]
                    stats['by_type'][str(t)] = {
                        'projects': count,
                        'capacity_gw': self._sum_capacity(type_df) / 1000
                    }

        # By status
        status_col = self.market_data._find_status_column(df)
        if status_col:
            status_counts = df[status_col].value_counts()
            for s, count in status_counts.items():
                if pd.notna(s):
                    stats['by_status'][str(s)] = count

        return stats

    def get_trends(self, iso: str = None, months: int = 12) -> Dict[str, Any]:
        """
        Analyze trends over time.

        Returns queue size trends, type mix changes, etc.
        """
        snapshots = self.market_data.get_available_snapshots(iso)
        if len(snapshots) < 2:
            return {'error': 'Not enough historical data for trends'}

        trend_data = []
        for ts in snapshots[:12]:  # Last 12 snapshots
            if iso:
                df = self.market_data.get_snapshot(iso, ts)
            else:
                # Would need to aggregate all ISOs for each timestamp
                continue

            if df is not None and not df.empty:
                trend_data.append({
                    'timestamp': ts,
                    'total_projects': len(df),
                    'total_capacity_gw': self._sum_capacity(df) / 1000
                })

        return {
            'iso': iso,
            'data_points': trend_data,
            'trend_direction': self._calculate_trend(trend_data)
        }

    def get_regional_comparison(self) -> pd.DataFrame:
        """Compare metrics across ISOs."""
        df = self.market_data.get_latest_data()
        if df.empty or 'iso' not in df.columns:
            return pd.DataFrame()

        comparison = []
        for iso in df['iso'].unique():
            iso_df = df[df['iso'] == iso]
            comparison.append({
                'ISO': iso,
                'Total Projects': len(iso_df),
                'Capacity (GW)': round(self._sum_capacity(iso_df) / 1000, 1),
                'Avg Project Size (MW)': round(self._sum_capacity(iso_df) / len(iso_df), 1) if len(iso_df) > 0 else 0,
            })

        return pd.DataFrame(comparison).sort_values('Capacity (GW)', ascending=False)

    def get_technology_breakdown(self, iso: str = None) -> pd.DataFrame:
        """Get breakdown by technology type."""
        df = self.market_data.get_latest_data(iso)
        if df.empty:
            return pd.DataFrame()

        type_col = self._find_type_column(df)
        if not type_col:
            return pd.DataFrame()

        # Normalize type names
        df = df.copy()
        df['type_normalized'] = df[type_col].apply(self._normalize_type)

        breakdown = []
        for tech in df['type_normalized'].unique():
            if pd.isna(tech):
                continue
            tech_df = df[df['type_normalized'] == tech]
            breakdown.append({
                'Technology': tech,
                'Projects': len(tech_df),
                'Capacity (GW)': round(self._sum_capacity(tech_df) / 1000, 1),
                'Share (%)': round(len(tech_df) / len(df) * 100, 1)
            })

        return pd.DataFrame(breakdown).sort_values('Capacity (GW)', ascending=False)

    def _sum_capacity(self, df: pd.DataFrame) -> float:
        """Sum capacity from a dataframe."""
        # Priority order: standard names first, then ISO-specific
        cap_cols = [
            'Capacity (MW)',           # Standard gridstatus column
            'capacity_mw',
            'Summer Capacity (MW)',
            'Winter Capacity (MW)',
            'SP (MW)',                 # NYISO Summer Peak
            'WP (MW)',                 # NYISO Winter Peak
            'MW', 'mw',
            'Nameplate',
            'Capacity'
        ]

        for col in cap_cols:
            if col in df.columns:
                # Make sure there's actually data in this column (not all NaN)
                values = pd.to_numeric(df[col], errors='coerce')
                if values.notna().any():
                    return values.sum()
        return 0

    def _find_type_column(self, df: pd.DataFrame) -> Optional[str]:
        """Find the project type column."""
        candidates = ['Generation Type', 'Fuel Type', 'Resource Type', 'Type',
                     'type', 'fuel', 'resource', 'Technology', 'Type/ Fuel']
        for col in candidates:
            if col in df.columns:
                return col
        for col in df.columns:
            if 'type' in col.lower() or 'fuel' in col.lower():
                return col
        return None

    def _normalize_type(self, type_str: str) -> str:
        """Normalize technology type names."""
        if pd.isna(type_str):
            return 'Unknown'

        t = str(type_str).lower().strip()

        # NYISO specific codes
        if t == 's':
            return 'Solar'
        elif t == 'w':
            return 'Wind'
        elif t == 'osw':
            return 'Wind'  # Offshore Wind
        elif t == 'es':
            return 'Storage'
        elif t == 'l':
            return 'Load'  # Load interconnections
        elif t in ('ac', 'dc'):
            return 'Transmission'
        elif t == 'cr':
            return 'Co-located Resources'
        elif t == 'ng' or t == 'gt':
            return 'Gas'

        # General patterns
        if 'solar' in t and ('storage' in t or 'battery' in t):
            return 'Solar + Storage'
        elif 'solar' in t or 'pv' in t:
            return 'Solar'
        elif 'wind' in t:
            return 'Wind'
        elif 'battery' in t or 'storage' in t or 'bess' in t:
            return 'Storage'
        elif 'gas' in t or 'natural' in t or 'ct' in t or 'cc' in t:
            return 'Gas'
        elif 'nuclear' in t:
            return 'Nuclear'
        elif 'hydro' in t:
            return 'Hydro'
        elif 'coal' in t:
            return 'Coal'
        else:
            return 'Other'

    def _calculate_trend(self, data: List[Dict]) -> str:
        """Calculate if trend is up, down, or flat."""
        if len(data) < 2:
            return 'insufficient_data'

        values = [d['total_projects'] for d in data]
        if values[0] > values[-1] * 1.05:
            return 'increasing'
        elif values[0] < values[-1] * 0.95:
            return 'decreasing'
        else:
            return 'stable'


class NewsAggregator:
    """Aggregate energy/interconnection news from various sources."""

    # RSS feeds for energy news
    NEWS_SOURCES = {
        'Utility Dive': 'https://www.utilitydive.com/feeds/news/',
        'RTO Insider': 'https://www.rtoinsider.com/feed/',
        'Canary Media': 'https://www.canarymedia.com/feed',
        'Energy Storage News': 'https://www.energy-storage.news/feed/',
        'PV Magazine': 'https://pv-magazine-usa.com/feed/',
        'Solar Power World': 'https://www.solarpowerworldonline.com/feed/',
    }

    # Keywords to filter for relevance
    RELEVANT_KEYWORDS = [
        'interconnection', 'queue', 'grid', 'ferc', 'pjm', 'miso', 'caiso',
        'ercot', 'nyiso', 'spp', 'iso-ne', 'transmission', 'solar', 'wind',
        'storage', 'battery', 'renewable', 'generation', 'capacity', 'mw',
        'power plant', 'utility', 'developer', 'infrastructure'
    ]

    def __init__(self):
        self._cache_file = DATA_DIR / 'news_cache.json'
        self._cache = self._load_cache()

    def _load_cache(self) -> Dict:
        """Load cached news."""
        if self._cache_file.exists():
            try:
                with open(self._cache_file) as f:
                    return json.load(f)
            except:
                pass
        return {'articles': [], 'last_fetch': None}

    def _save_cache(self):
        """Save news cache."""
        ensure_dirs()
        with open(self._cache_file, 'w') as f:
            json.dump(self._cache, f, indent=2, default=str)

    def fetch_news(self, keywords: List[str] = None, force_refresh: bool = False) -> List[Dict]:
        """
        Fetch recent news articles from RSS feeds.

        Args:
            keywords: Filter articles containing these keywords (uses defaults if None)
            force_refresh: Force fetch even if cache is fresh

        Returns:
            List of article dicts with title, link, source, published, summary
        """
        # Check if cache is fresh (less than 1 hour old)
        if not force_refresh and self._cache.get('last_fetch'):
            try:
                last_fetch = datetime.fromisoformat(self._cache['last_fetch'])
                if datetime.now() - last_fetch < timedelta(hours=1):
                    return self.get_cached_news()
            except:
                pass

        try:
            import feedparser
        except ImportError:
            return self._cache.get('articles', [])

        keywords = keywords or self.RELEVANT_KEYWORDS
        all_articles = []

        for source_name, feed_url in self.NEWS_SOURCES.items():
            try:
                feed = feedparser.parse(feed_url)
                for entry in feed.entries[:20]:  # Limit per source
                    # Extract article data
                    article = {
                        'title': entry.get('title', 'No title'),
                        'link': entry.get('link', ''),
                        'source': source_name,
                        'published': entry.get('published', entry.get('updated', '')),
                        'summary': entry.get('summary', entry.get('description', ''))[:300],
                    }

                    # Filter by keywords
                    text = f"{article['title']} {article['summary']}".lower()
                    if any(kw.lower() in text for kw in keywords):
                        all_articles.append(article)

            except Exception as e:
                continue

        # Sort by published date (most recent first)
        all_articles.sort(key=lambda x: x.get('published', ''), reverse=True)

        # Update cache
        self._cache = {
            'articles': all_articles[:50],  # Keep top 50
            'last_fetch': datetime.now().isoformat()
        }
        self._save_cache()

        return all_articles[:50]

    def get_cached_news(self, limit: int = 20) -> List[Dict]:
        """Get cached news articles."""
        return self._cache.get('articles', [])[:limit]

    def get_news_by_topic(self, topic: str, limit: int = 10) -> List[Dict]:
        """Get news filtered by a specific topic."""
        articles = self._cache.get('articles', [])
        filtered = []

        for article in articles:
            text = f"{article['title']} {article['summary']}".lower()
            if topic.lower() in text:
                filtered.append(article)
                if len(filtered) >= limit:
                    break

        return filtered


# Convenience functions
def refresh_all_data() -> Dict[str, Any]:
    """Refresh data from all ISOs."""
    market = MarketData()
    return market.refresh_data()

def get_dashboard_data() -> Dict[str, Any]:
    """Get all data needed for the dashboard."""
    market = MarketData()
    analytics = MacroAnalytics(market)

    return {
        'summary': analytics.get_summary_stats(),
        'regional': analytics.get_regional_comparison().to_dict('records'),
        'technology': analytics.get_technology_breakdown().to_dict('records'),
        'metadata': market.metadata
    }


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Market Intelligence Data Manager')
    parser.add_argument('command', choices=['refresh', 'stats', 'changes'],
                       help='Command to run')
    parser.add_argument('--iso', help='Specific ISO to query')

    args = parser.parse_args()

    market = MarketData()
    analytics = MacroAnalytics(market)

    if args.command == 'refresh':
        print("Refreshing data from GridStatus...")
        result = market.refresh_data([args.iso] if args.iso else None)
        print(f"Refreshed: {result['refreshed']}")
        if result['errors']:
            print(f"Errors: {result['errors']}")

    elif args.command == 'stats':
        stats = analytics.get_summary_stats()
        print(f"\n📊 Market Summary")
        print(f"{'='*50}")
        print(f"Total Projects: {stats.get('total_projects', 'N/A'):,}")
        print(f"Total Capacity: {stats.get('total_capacity_gw', 0):.1f} GW")
        print(f"\nBy ISO:")
        for iso, data in stats.get('by_iso', {}).items():
            print(f"  {iso}: {data['projects']:,} projects, {data['capacity_gw']:.1f} GW")

    elif args.command == 'changes':
        iso = args.iso or 'nyiso'
        changes = market.detect_changes(iso)
        print(f"\n🔄 Changes for {iso.upper()}")
        print(f"{'='*50}")
        print(f"New projects: {changes.get('added_count', 0)}")
        print(f"Removed: {changes.get('removed_count', 0)}")
        print(f"Status changes: {changes.get('status_changes_count', 0)}")
