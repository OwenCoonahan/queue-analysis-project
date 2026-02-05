#!/usr/bin/env python3
"""
Data Validation Suite

Comprehensive validation to ensure data accuracy across sources.

Usage:
    python3 validate_data.py              # Full validation report
    python3 validate_data.py --live       # Compare against live ISO APIs
    python3 validate_data.py --cross      # Cross-source reconciliation
    python3 validate_data.py --quality    # Data quality metrics only
    python3 validate_data.py --json       # Output as JSON for automation
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import argparse
import json
import sys

# Suppress warnings
import warnings
warnings.filterwarnings('ignore')

TOOLS_DIR = Path(__file__).parent
V1_PATH = TOOLS_DIR / '.data' / 'queue.db'
V2_PATH = TOOLS_DIR / '.data' / 'queue_v2.db'


class DataValidator:
    """Comprehensive data validation for queue database."""

    def __init__(self):
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'live_comparison': {},
            'cross_source': {},
            'quality_metrics': {},
            'anomalies': [],
            'recommendations': [],
            'overall_score': None
        }

    def run_all_validations(self, include_live=True):
        """Run all validation checks."""
        print("=" * 70)
        print("DATA VALIDATION REPORT")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        # 1. Live API comparison
        if include_live:
            self.validate_against_live()

        # 2. Cross-source reconciliation
        self.cross_source_validation()

        # 3. Data quality metrics
        self.data_quality_check()

        # 4. Anomaly detection
        self.detect_anomalies()

        # 5. Calculate overall score
        self.calculate_score()

        # 6. Generate recommendations
        self.generate_recommendations()

        return self.results

    def validate_against_live(self):
        """Compare database against live ISO API data."""
        print("\n" + "=" * 70)
        print("1. LIVE API VALIDATION")
        print("=" * 70)

        from direct_fetcher import DirectFetcher
        fetcher = DirectFetcher()

        conn = sqlite3.connect(V2_PATH)
        comparisons = {}

        # ERCOT
        print("\nERCOT...")
        try:
            live = fetcher.fetch_ercot(use_cache=False)
            if not live.empty:
                live_count = len(live)
                live_gw = live['Capacity (MW)'].sum() / 1000

                db = pd.read_sql("""
                    SELECT COUNT(*) as cnt, SUM(capacity_mw)/1000 as gw
                    FROM fact_projects p
                    JOIN dim_regions r ON p.region_id = r.region_id
                    JOIN dim_statuses s ON p.status_id = s.status_id
                    WHERE r.region_code = 'ERCOT' AND s.status_category = 'Active'
                """, conn)

                db_count = db['cnt'].values[0]
                db_gw = db['gw'].values[0] or 0

                diff_pct = abs(db_gw - live_gw) / live_gw * 100 if live_gw > 0 else 0

                comparisons['ERCOT'] = {
                    'live_projects': int(live_count),
                    'live_gw': round(live_gw, 1),
                    'db_projects': int(db_count),
                    'db_gw': round(db_gw, 1),
                    'diff_pct': round(diff_pct, 1),
                    'status': 'OK' if diff_pct < 10 else 'WARNING' if diff_pct < 25 else 'ERROR'
                }
                print(f"  Live: {live_count} projects, {live_gw:.1f} GW")
                print(f"  DB:   {db_count} projects, {db_gw:.1f} GW")
                print(f"  Diff: {diff_pct:.1f}% - {comparisons['ERCOT']['status']}")
        except Exception as e:
            print(f"  Error: {e}")
            comparisons['ERCOT'] = {'error': str(e)}

        # MISO
        print("\nMISO...")
        try:
            live = fetcher.fetch_miso(use_cache=False)
            if not live.empty:
                # Filter for active (not Withdrawn)
                active_mask = ~live['Status'].str.lower().str.contains('withdrawn', na=False)
                live_active = live[active_mask]
                live_count = len(live_active)
                live_gw = live_active['Capacity (MW)'].sum() / 1000

                db = pd.read_sql("""
                    SELECT COUNT(*) as cnt, SUM(capacity_mw)/1000 as gw
                    FROM fact_projects p
                    JOIN dim_regions r ON p.region_id = r.region_id
                    JOIN dim_statuses s ON p.status_id = s.status_id
                    WHERE r.region_code = 'MISO' AND s.status_category = 'Active'
                """, conn)

                db_count = db['cnt'].values[0]
                db_gw = db['gw'].values[0] or 0

                diff_pct = abs(db_gw - live_gw) / live_gw * 100 if live_gw > 0 else 0

                comparisons['MISO'] = {
                    'live_projects': int(live_count),
                    'live_gw': round(live_gw, 1),
                    'db_projects': int(db_count),
                    'db_gw': round(db_gw, 1),
                    'diff_pct': round(diff_pct, 1),
                    'status': 'OK' if diff_pct < 10 else 'WARNING' if diff_pct < 25 else 'ERROR'
                }
                print(f"  Live: {live_count} projects, {live_gw:.1f} GW")
                print(f"  DB:   {db_count} projects, {db_gw:.1f} GW")
                print(f"  Diff: {diff_pct:.1f}% - {comparisons['MISO']['status']}")
        except Exception as e:
            print(f"  Error: {e}")
            comparisons['MISO'] = {'error': str(e)}

        # NYISO
        print("\nNYISO...")
        try:
            live = fetcher.fetch_nyiso(use_cache=False)
            if not live.empty:
                # Filter for active projects (NYISO uses numeric status codes)
                # Status 11.0 = In Service (Completed), 12.0 = Withdrawn
                if 'S' in live.columns:
                    status_col = live['S']
                    # Active = not 11.0 (completed) and not 12.0 (withdrawn)
                    active_mask = ~status_col.isin([11.0, 12.0, '11.0', '12.0'])
                    live_active = live[active_mask]
                else:
                    live_active = live

                live_count = len(live_active)
                cap_col = 'SP (MW)' if 'SP (MW)' in live_active.columns else 'Capacity (MW)'
                live_gw = pd.to_numeric(live_active[cap_col], errors='coerce').sum() / 1000

                db = pd.read_sql("""
                    SELECT COUNT(*) as cnt, SUM(capacity_mw)/1000 as gw
                    FROM fact_projects p
                    JOIN dim_regions r ON p.region_id = r.region_id
                    JOIN dim_statuses s ON p.status_id = s.status_id
                    WHERE r.region_code = 'NYISO' AND s.status_category = 'Active'
                """, conn)

                db_count = db['cnt'].values[0]
                db_gw = db['gw'].values[0] or 0

                diff_pct = abs(db_gw - live_gw) / live_gw * 100 if live_gw > 0 else 0

                comparisons['NYISO'] = {
                    'live_projects': int(live_count),
                    'live_gw': round(live_gw, 1),
                    'db_projects': int(db_count),
                    'db_gw': round(db_gw, 1),
                    'diff_pct': round(diff_pct, 1),
                    'status': 'OK' if diff_pct < 10 else 'WARNING' if diff_pct < 25 else 'ERROR'
                }
                print(f"  Live (active): {live_count} projects, {live_gw:.1f} GW")
                print(f"  DB (active):   {db_count} projects, {db_gw:.1f} GW")
                print(f"  Diff: {diff_pct:.1f}% - {comparisons['NYISO']['status']}")
        except Exception as e:
            print(f"  Error: {e}")
            comparisons['NYISO'] = {'error': str(e)}

        # CAISO
        print("\nCAISO...")
        try:
            live = fetcher.fetch_caiso(use_cache=False)
            if not live.empty:
                # Filter for active
                if 'Application Status' in live.columns:
                    active_mask = ~live['Application Status'].str.lower().str.contains('withdrawn', na=False)
                    live_active = live[active_mask]
                else:
                    live_active = live

                live_count = len(live_active)

                # Find capacity column
                cap_col = None
                for col in ['Net MWs to Grid', 'On-Peak MWs Deliverability', 'Capacity (MW)']:
                    if col in live_active.columns:
                        cap_col = col
                        break

                live_gw = pd.to_numeric(live_active[cap_col], errors='coerce').sum() / 1000 if cap_col else 0

                db = pd.read_sql("""
                    SELECT COUNT(*) as cnt, SUM(capacity_mw)/1000 as gw
                    FROM fact_projects p
                    JOIN dim_regions r ON p.region_id = r.region_id
                    JOIN dim_statuses s ON p.status_id = s.status_id
                    WHERE r.region_code = 'CAISO' AND s.status_category = 'Active'
                """, conn)

                db_count = db['cnt'].values[0]
                db_gw = db['gw'].values[0] or 0

                comparisons['CAISO'] = {
                    'live_projects': int(live_count),
                    'live_gw': round(live_gw, 1),
                    'db_projects': int(db_count),
                    'db_gw': round(db_gw, 1),
                    'status': 'CHECK' if live_gw == 0 else 'OK'
                }
                print(f"  Live: {live_count} projects, {live_gw:.1f} GW")
                print(f"  DB:   {db_count} projects, {db_gw:.1f} GW")
        except Exception as e:
            print(f"  Error: {e}")
            comparisons['CAISO'] = {'error': str(e)}

        conn.close()
        self.results['live_comparison'] = comparisons

        # Summary
        print("\n" + "-" * 50)
        print("Live Validation Summary:")
        ok_count = sum(1 for v in comparisons.values() if isinstance(v, dict) and v.get('status') == 'OK')
        total = len([v for v in comparisons.values() if isinstance(v, dict) and 'status' in v])
        print(f"  Passed: {ok_count}/{total} regions within 10% of live data")

    def cross_source_validation(self):
        """Check consistency across data sources for same projects."""
        print("\n" + "=" * 70)
        print("2. CROSS-SOURCE RECONCILIATION")
        print("=" * 70)

        conn = sqlite3.connect(V1_PATH)

        # Find projects that exist in multiple sources
        query = """
        SELECT
            queue_id,
            region,
            GROUP_CONCAT(source) as sources,
            COUNT(DISTINCT source) as source_count,
            COUNT(DISTINCT status) as status_variations,
            COUNT(DISTINCT capacity_mw) as capacity_variations,
            COUNT(DISTINCT developer) as developer_variations
        FROM projects
        WHERE queue_id IS NOT NULL AND queue_id != ''
        GROUP BY queue_id, region
        HAVING source_count > 1
        """
        multi_source = pd.read_sql(query, conn)

        cross_source = {
            'projects_in_multiple_sources': len(multi_source),
            'status_conflicts': int((multi_source['status_variations'] > 1).sum()),
            'capacity_conflicts': int((multi_source['capacity_variations'] > 1).sum()),
            'developer_conflicts': int((multi_source['developer_variations'] > 1).sum()),
        }

        print(f"\nProjects in multiple sources: {cross_source['projects_in_multiple_sources']}")
        print(f"  Status conflicts:    {cross_source['status_conflicts']}")
        print(f"  Capacity conflicts:  {cross_source['capacity_conflicts']}")
        print(f"  Developer conflicts: {cross_source['developer_conflicts']}")

        # Show examples of conflicts
        if cross_source['status_conflicts'] > 0:
            print("\n  Sample status conflicts:")
            conflicts = multi_source[multi_source['status_variations'] > 1].head(5)
            for _, row in conflicts.iterrows():
                detail = pd.read_sql(f"""
                    SELECT queue_id, source, status, capacity_mw
                    FROM projects
                    WHERE queue_id = '{row['queue_id']}' AND region = '{row['region']}'
                """, conn)
                print(f"    {row['queue_id']} ({row['region']}): {row['sources']}")
                for _, d in detail.iterrows():
                    print(f"      - {d['source']}: {d['status']}")

        # Check for duplicate handling
        print("\n  Deduplication check:")
        v1_total = pd.read_sql("SELECT COUNT(*) as cnt FROM projects", conn)['cnt'].values[0]
        v2_conn = sqlite3.connect(V2_PATH)
        v2_total = pd.read_sql("SELECT COUNT(*) as cnt FROM fact_projects", v2_conn)['cnt'].values[0]
        v2_conn.close()

        dedup_removed = v1_total - v2_total
        cross_source['v1_total'] = int(v1_total)
        cross_source['v2_total'] = int(v2_total)
        cross_source['duplicates_removed'] = int(dedup_removed)

        print(f"    V1 total: {v1_total:,}")
        print(f"    V2 total: {v2_total:,}")
        print(f"    Duplicates removed: {dedup_removed:,}")

        conn.close()
        self.results['cross_source'] = cross_source

    def data_quality_check(self):
        """Check data quality metrics."""
        print("\n" + "=" * 70)
        print("3. DATA QUALITY METRICS")
        print("=" * 70)

        conn = sqlite3.connect(V2_PATH)

        # Completeness
        query = """
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN project_name IS NOT NULL AND project_name != '' THEN 1 ELSE 0 END) as has_name,
            SUM(CASE WHEN developer_id IS NOT NULL THEN 1 ELSE 0 END) as has_developer,
            SUM(CASE WHEN capacity_mw IS NOT NULL AND capacity_mw > 0 THEN 1 ELSE 0 END) as has_capacity,
            SUM(CASE WHEN queue_date IS NOT NULL THEN 1 ELSE 0 END) as has_queue_date,
            SUM(CASE WHEN location_id IS NOT NULL THEN 1 ELSE 0 END) as has_location,
            SUM(CASE WHEN technology_id IS NOT NULL THEN 1 ELSE 0 END) as has_technology,
            SUM(CASE WHEN status_id IS NOT NULL THEN 1 ELSE 0 END) as has_status
        FROM fact_projects
        """
        completeness = pd.read_sql(query, conn).iloc[0]

        total = completeness['total']
        quality = {
            'total_records': int(total),
            'completeness': {
                'name': round(completeness['has_name'] / total * 100, 1),
                'developer': round(completeness['has_developer'] / total * 100, 1),
                'capacity': round(completeness['has_capacity'] / total * 100, 1),
                'queue_date': round(completeness['has_queue_date'] / total * 100, 1),
                'location': round(completeness['has_location'] / total * 100, 1),
                'technology': round(completeness['has_technology'] / total * 100, 1),
                'status': round(completeness['has_status'] / total * 100, 1),
            }
        }

        print("\nCompleteness (% of records with field populated):")
        for field, pct in quality['completeness'].items():
            status = "OK" if pct >= 95 else "WARN" if pct >= 80 else "LOW"
            bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
            print(f"  {field:15} {bar} {pct:5.1f}% [{status}]")

        # Validity checks
        print("\nValidity Checks:")

        # Invalid capacities
        invalid_cap = pd.read_sql("""
            SELECT COUNT(*) as cnt FROM fact_projects
            WHERE capacity_mw IS NOT NULL AND (capacity_mw < 0 OR capacity_mw > 20000)
        """, conn)['cnt'].values[0]
        quality['invalid_capacity'] = int(invalid_cap)
        print(f"  Invalid capacity (< 0 or > 20,000 MW): {invalid_cap}")

        # Future queue dates
        future_dates = pd.read_sql("""
            SELECT COUNT(*) as cnt FROM fact_projects
            WHERE queue_date > date('now')
        """, conn)['cnt'].values[0]
        quality['future_queue_dates'] = int(future_dates)
        print(f"  Future queue dates: {future_dates}")

        # Very old queue dates (before 2000)
        old_dates = pd.read_sql("""
            SELECT COUNT(*) as cnt FROM fact_projects
            WHERE queue_date < '2000-01-01'
        """, conn)['cnt'].values[0]
        quality['old_queue_dates'] = int(old_dates)
        print(f"  Queue dates before 2000: {old_dates}")

        # Freshness
        print("\nData Freshness:")
        freshness = pd.read_sql("""
            SELECT
                data_source,
                COUNT(*) as cnt,
                MAX(queue_date) as latest_queue_date
            FROM fact_projects
            GROUP BY data_source
            ORDER BY cnt DESC
        """, conn)
        quality['freshness'] = freshness.to_dict('records')

        for _, row in freshness.iterrows():
            print(f"  {row['data_source']:12} {row['cnt']:>6,} records, latest queue: {row['latest_queue_date'] or 'N/A'}")

        conn.close()
        self.results['quality_metrics'] = quality

    def detect_anomalies(self):
        """Detect statistical anomalies in the data."""
        print("\n" + "=" * 70)
        print("4. ANOMALY DETECTION")
        print("=" * 70)

        conn = sqlite3.connect(V2_PATH)
        anomalies = []

        # Large capacity outliers
        print("\nCapacity Outliers (> 3 std dev from mean):")
        stats = pd.read_sql("""
            SELECT AVG(capacity_mw) as mean,
                   AVG(capacity_mw * capacity_mw) - AVG(capacity_mw) * AVG(capacity_mw) as var
            FROM fact_projects
            WHERE capacity_mw > 0
        """, conn).iloc[0]

        mean = stats['mean']
        std = np.sqrt(stats['var']) if stats['var'] > 0 else 0
        threshold = mean + 3 * std

        outliers = pd.read_sql(f"""
            SELECT p.queue_id, r.region_code as region, p.capacity_mw,
                   d.canonical_name as developer, t.technology_code as tech
            FROM fact_projects p
            JOIN dim_regions r ON p.region_id = r.region_id
            LEFT JOIN dim_developers d ON p.developer_id = d.developer_id
            LEFT JOIN dim_technologies t ON p.technology_id = t.technology_id
            WHERE p.capacity_mw > {threshold}
            ORDER BY p.capacity_mw DESC
            LIMIT 10
        """, conn)

        if len(outliers) > 0:
            print(f"  Mean: {mean:.1f} MW, Std: {std:.1f} MW, Threshold: {threshold:.1f} MW")
            for _, row in outliers.iterrows():
                print(f"  - {row['queue_id']} ({row['region']}): {row['capacity_mw']:,.0f} MW - {row['tech']}")
                anomalies.append({
                    'type': 'capacity_outlier',
                    'queue_id': row['queue_id'],
                    'region': row['region'],
                    'value': row['capacity_mw'],
                    'threshold': threshold
                })

        # Status distribution anomalies by region
        print("\nStatus Distribution by Region:")
        status_dist = pd.read_sql("""
            SELECT
                r.region_code as region,
                s.status_category,
                COUNT(*) as cnt,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY r.region_code), 1) as pct
            FROM fact_projects p
            JOIN dim_regions r ON p.region_id = r.region_id
            LEFT JOIN dim_statuses s ON p.status_id = s.status_id
            GROUP BY r.region_code, s.status_category
            ORDER BY r.region_code, cnt DESC
        """, conn)

        # Check for regions with unusual withdrawal rates
        withdrawals = status_dist[status_dist['status_category'] == 'Withdrawn']
        for _, row in withdrawals.iterrows():
            if row['pct'] > 80:
                print(f"  WARNING: {row['region']} has {row['pct']}% withdrawn rate")
                anomalies.append({
                    'type': 'high_withdrawal_rate',
                    'region': row['region'],
                    'withdrawal_rate': row['pct']
                })

        # Check for suspiciously low active rates
        actives = status_dist[status_dist['status_category'] == 'Active']
        for _, row in actives.iterrows():
            if row['pct'] < 5 and row['region'] not in ['ISO-NE']:  # ISO-NE genuinely has low active
                print(f"  WARNING: {row['region']} has only {row['pct']}% active rate")
                anomalies.append({
                    'type': 'low_active_rate',
                    'region': row['region'],
                    'active_rate': row['pct']
                })

        conn.close()
        self.results['anomalies'] = anomalies
        print(f"\nTotal anomalies detected: {len(anomalies)}")

    def calculate_score(self):
        """Calculate overall data quality score (0-100)."""
        score = 100

        # Deduct for live comparison failures
        live = self.results.get('live_comparison', {})
        for region, data in live.items():
            if isinstance(data, dict):
                if data.get('status') == 'ERROR':
                    score -= 10
                elif data.get('status') == 'WARNING':
                    score -= 5

        # Deduct for low completeness
        quality = self.results.get('quality_metrics', {}).get('completeness', {})
        for field, pct in quality.items():
            if pct < 80:
                score -= 5
            elif pct < 95:
                score -= 2

        # Deduct for anomalies
        anomalies = len(self.results.get('anomalies', []))
        score -= min(anomalies * 2, 20)

        # Deduct for cross-source conflicts
        cross = self.results.get('cross_source', {})
        if cross.get('status_conflicts', 0) > 100:
            score -= 5

        self.results['overall_score'] = max(0, min(100, score))

    def generate_recommendations(self):
        """Generate actionable recommendations based on validation results."""
        print("\n" + "=" * 70)
        print("5. RECOMMENDATIONS")
        print("=" * 70)

        recommendations = []

        # Check live comparison
        for region, data in self.results.get('live_comparison', {}).items():
            if isinstance(data, dict) and data.get('status') == 'ERROR':
                recommendations.append({
                    'priority': 'HIGH',
                    'issue': f'{region} data is {data.get("diff_pct", 0):.0f}% off from live API',
                    'action': f'Run: python3 refresh_v2.py to sync {region} data'
                })

        # Check completeness
        quality = self.results.get('quality_metrics', {}).get('completeness', {})
        for field, pct in quality.items():
            if pct < 80:
                recommendations.append({
                    'priority': 'MEDIUM',
                    'issue': f'{field} field is only {pct:.0f}% complete',
                    'action': f'Review data sources for {field} enrichment'
                })

        # Check anomalies
        for anomaly in self.results.get('anomalies', []):
            if anomaly['type'] == 'capacity_outlier':
                recommendations.append({
                    'priority': 'LOW',
                    'issue': f"Capacity outlier: {anomaly['queue_id']} ({anomaly['value']:,.0f} MW)",
                    'action': 'Verify this project capacity against source documents'
                })

        self.results['recommendations'] = recommendations

        if recommendations:
            print("\n")
            for rec in recommendations:
                print(f"[{rec['priority']}] {rec['issue']}")
                print(f"       Action: {rec['action']}\n")
        else:
            print("\nNo critical issues found.")

        # Overall score
        print("\n" + "=" * 70)
        print(f"OVERALL DATA QUALITY SCORE: {self.results['overall_score']}/100")
        print("=" * 70)

        if self.results['overall_score'] >= 90:
            print("Status: EXCELLENT - Data is highly reliable")
        elif self.results['overall_score'] >= 75:
            print("Status: GOOD - Data is reliable with minor issues")
        elif self.results['overall_score'] >= 60:
            print("Status: FAIR - Some data quality issues need attention")
        else:
            print("Status: POOR - Significant data quality issues detected")


def main():
    parser = argparse.ArgumentParser(description='Data Validation Suite')
    parser.add_argument('--live', action='store_true', help='Only run live API comparison')
    parser.add_argument('--cross', action='store_true', help='Only run cross-source validation')
    parser.add_argument('--quality', action='store_true', help='Only run quality metrics')
    parser.add_argument('--json', action='store_true', help='Output results as JSON')

    args = parser.parse_args()

    validator = DataValidator()

    # Run specific checks or all
    if args.live:
        validator.validate_against_live()
    elif args.cross:
        validator.cross_source_validation()
    elif args.quality:
        validator.data_quality_check()
    else:
        validator.run_all_validations(include_live=True)

    if args.json:
        print(json.dumps(validator.results, indent=2, default=str))

    score = validator.results.get('overall_score')
    if score is None:
        return 0  # Partial validation completed successfully
    return 0 if score >= 75 else 1


if __name__ == '__main__':
    sys.exit(main())
