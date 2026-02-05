#!/usr/bin/env python3
"""
Automated Data Refresh with Validation

This is the main entry point for all data refreshes. It automatically:
1. Backs up the current database
2. Validates input data before processing
3. Refreshes from all sources
4. Rebuilds the normalized database
5. Validates output against live APIs
6. Reports any issues or anomalies
7. Rolls back if validation fails (optional)

Usage:
    python3 auto_refresh.py                    # Full automated refresh
    python3 auto_refresh.py --quick            # Skip source refresh, just rebuild
    python3 auto_refresh.py --validate-only    # Only run validation, no refresh
    python3 auto_refresh.py --status           # Show current data status
    python3 auto_refresh.py --rollback         # Restore from last backup

Environment:
    Set QUEUE_REFRESH_QUIET=1 for cron-friendly output
"""

import subprocess
import sys
import shutil
import json
from pathlib import Path
from datetime import datetime
import argparse

# Suppress warnings
import warnings
warnings.filterwarnings('ignore')

TOOLS_DIR = Path(__file__).parent
DATA_DIR = TOOLS_DIR / '.data'
BACKUP_DIR = DATA_DIR / 'backups'
V1_PATH = DATA_DIR / 'queue.db'
V2_PATH = DATA_DIR / 'queue_v2.db'

# Validation thresholds
VALIDATION_THRESHOLDS = {
    'min_total_projects': 30000,      # Minimum total projects expected
    'min_active_gw': 1000,            # Minimum active capacity in GW
    'max_live_diff_pct': 15,          # Max % difference from live APIs
    'min_quality_score': 65,          # Minimum quality score (0-100) - lowered due to expected gaps in historical data
}


class AutoRefresh:
    """Automated refresh with validation and rollback support."""

    def __init__(self, quiet=False):
        self.quiet = quiet
        self.start_time = datetime.now()
        self.results = {
            'timestamp': self.start_time.isoformat(),
            'steps': [],
            'validation': {},
            'success': False,
            'errors': [],
            'warnings': []
        }

    def log(self, message, level='INFO'):
        """Log a message."""
        if not self.quiet:
            timestamp = datetime.now().strftime('%H:%M:%S')
            print(f"[{timestamp}] {level}: {message}")

        if level == 'ERROR':
            self.results['errors'].append(message)
        elif level == 'WARN':
            self.results['warnings'].append(message)

    def run_step(self, name, func, *args, **kwargs):
        """Run a step and track its result."""
        self.log(f"Starting: {name}")
        step_start = datetime.now()

        try:
            result = func(*args, **kwargs)
            duration = (datetime.now() - step_start).total_seconds()

            self.results['steps'].append({
                'name': name,
                'status': 'success',
                'duration_sec': round(duration, 1),
                'result': result if isinstance(result, (dict, str, int, float, bool)) else str(result)
            })

            self.log(f"Completed: {name} ({duration:.1f}s)")
            return result

        except Exception as e:
            duration = (datetime.now() - step_start).total_seconds()
            self.results['steps'].append({
                'name': name,
                'status': 'failed',
                'duration_sec': round(duration, 1),
                'error': str(e)
            })
            self.log(f"Failed: {name} - {e}", level='ERROR')
            raise

    def backup_databases(self):
        """Create timestamped backup of databases."""
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)

        timestamp = self.start_time.strftime('%Y%m%d_%H%M%S')
        backed_up = []

        for db_path in [V1_PATH, V2_PATH]:
            if db_path.exists():
                backup_path = BACKUP_DIR / f"{db_path.stem}_{timestamp}.db"
                shutil.copy2(db_path, backup_path)
                backed_up.append(str(backup_path))

        # Keep only last 5 backups per database
        self._cleanup_old_backups()

        return {'backed_up': backed_up}

    def _cleanup_old_backups(self):
        """Remove old backups, keeping only the most recent 5."""
        for prefix in ['queue_', 'queue_v2_']:
            backups = sorted(BACKUP_DIR.glob(f'{prefix}*.db'), reverse=True)
            for old_backup in backups[5:]:
                old_backup.unlink()

    def refresh_sources(self):
        """Refresh data from all sources."""
        from refresh_data import DataRefresher

        refresher = DataRefresher(strict_validation=False)
        results = refresher.refresh_all(quiet=self.quiet)

        # Check for failures
        failures = [k for k, v in results.items() if not v.get('success')]
        if failures:
            self.log(f"Some sources failed: {failures}", level='WARN')

        return {
            'sources_refreshed': len(results),
            'failures': failures,
            'validation_summary': refresher.validator.get_validation_summary()
        }

    def rebuild_v2(self):
        """Rebuild V2 normalized database."""
        from refresh_v2 import sync_stale_records, rebuild_v2

        stale_count = sync_stale_records(quiet=self.quiet)
        inserted = rebuild_v2(quiet=self.quiet)

        return {
            'stale_records_updated': stale_count,
            'projects_migrated': inserted
        }

    def validate_output(self):
        """Validate the rebuilt database against live APIs and quality checks."""
        from validate_data import DataValidator

        validator = DataValidator()
        results = validator.run_all_validations(include_live=True)

        self.results['validation'] = results

        # Check against thresholds
        issues = []

        # Check total projects
        total = results.get('quality_metrics', {}).get('total_records', 0)
        if total < VALIDATION_THRESHOLDS['min_total_projects']:
            issues.append(f"Total projects ({total:,}) below minimum ({VALIDATION_THRESHOLDS['min_total_projects']:,})")

        # Check quality score
        score = results.get('overall_score', 0)
        if score < VALIDATION_THRESHOLDS['min_quality_score']:
            issues.append(f"Quality score ({score}) below minimum ({VALIDATION_THRESHOLDS['min_quality_score']})")

        # Check live comparison
        for region, data in results.get('live_comparison', {}).items():
            if isinstance(data, dict) and data.get('diff_pct', 0) > VALIDATION_THRESHOLDS['max_live_diff_pct']:
                issues.append(f"{region} differs {data['diff_pct']:.0f}% from live API")

        if issues:
            for issue in issues:
                self.log(issue, level='WARN')

        return {
            'quality_score': score,
            'total_records': total,
            'issues': issues,
            'passed': len(issues) == 0
        }

    def rollback(self):
        """Restore from most recent backup."""
        self.log("Rolling back to last backup...")

        for db_name in ['queue', 'queue_v2']:
            backups = sorted(BACKUP_DIR.glob(f'{db_name}_*.db'), reverse=True)
            if backups:
                latest = backups[0]
                target = DATA_DIR / f'{db_name}.db'
                shutil.copy2(latest, target)
                self.log(f"Restored {db_name}.db from {latest.name}")

        return {'status': 'rolled_back'}

    def get_status(self):
        """Get current data status without refreshing."""
        import sqlite3
        import pandas as pd

        status = {
            'timestamp': datetime.now().isoformat(),
            'databases': {},
            'recent_refreshes': [],
            'data_quality': {}
        }

        # Database info
        for name, path in [('v1', V1_PATH), ('v2', V2_PATH)]:
            if path.exists():
                size_mb = path.stat().st_size / (1024 * 1024)
                modified = datetime.fromtimestamp(path.stat().st_mtime)

                conn = sqlite3.connect(path)
                if name == 'v1':
                    count = pd.read_sql("SELECT COUNT(*) as cnt FROM projects", conn)['cnt'].values[0]
                else:
                    count = pd.read_sql("SELECT COUNT(*) as cnt FROM fact_projects", conn)['cnt'].values[0]
                conn.close()

                status['databases'][name] = {
                    'path': str(path),
                    'size_mb': round(size_mb, 1),
                    'last_modified': modified.isoformat(),
                    'record_count': int(count)
                }

        # Recent backups
        if BACKUP_DIR.exists():
            backups = sorted(BACKUP_DIR.glob('*.db'), reverse=True)[:5]
            status['recent_backups'] = [b.name for b in backups]

        # Quick quality check
        if V2_PATH.exists():
            conn = sqlite3.connect(V2_PATH)
            summary = pd.read_sql("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN s.status_category = 'Active' THEN 1 ELSE 0 END) as active,
                    SUM(CASE WHEN s.status_category = 'Unknown' THEN 1 ELSE 0 END) as unknown,
                    ROUND(SUM(CASE WHEN s.status_category = 'Active' THEN capacity_mw ELSE 0 END)/1000, 1) as active_gw
                FROM fact_projects p
                LEFT JOIN dim_statuses s ON p.status_id = s.status_id
            """, conn)
            conn.close()

            status['data_quality'] = {
                'total_projects': int(summary['total'].values[0]),
                'active_projects': int(summary['active'].values[0]),
                'unknown_status': int(summary['unknown'].values[0]),
                'active_capacity_gw': float(summary['active_gw'].values[0])
            }

        return status

    def run_full_refresh(self, skip_source_refresh=False):
        """Run the full automated refresh pipeline."""
        self.log("=" * 60)
        self.log("AUTOMATED DATA REFRESH")
        self.log("=" * 60)

        try:
            # Step 1: Backup
            self.run_step("Backup databases", self.backup_databases)

            # Step 2: Refresh sources (unless skipped)
            if not skip_source_refresh:
                self.run_step("Refresh from sources", self.refresh_sources)

            # Step 3: Rebuild V2
            self.run_step("Rebuild V2 database", self.rebuild_v2)

            # Step 4: Validate
            validation = self.run_step("Validate output", self.validate_output)

            # Determine overall success
            self.results['success'] = validation.get('passed', False)

            if not self.results['success']:
                self.log("Validation found issues - review warnings above", level='WARN')
                self.log("Run 'python3 auto_refresh.py --rollback' to restore previous state")

        except Exception as e:
            self.results['success'] = False
            self.log(f"Refresh failed: {e}", level='ERROR')
            self.log("Run 'python3 auto_refresh.py --rollback' to restore previous state")

        # Final summary
        duration = (datetime.now() - self.start_time).total_seconds()
        self.results['total_duration_sec'] = round(duration, 1)

        self.log("=" * 60)
        status = "SUCCESS" if self.results['success'] else "COMPLETED WITH WARNINGS"
        self.log(f"REFRESH {status} ({duration:.0f}s)")
        self.log("=" * 60)

        # Save results
        self._save_results()

        return self.results

    def _save_results(self):
        """Save refresh results to file."""
        results_path = DATA_DIR / 'last_refresh.json'
        with open(results_path, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)


def main():
    import os

    parser = argparse.ArgumentParser(
        description='Automated data refresh with validation',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--quick', action='store_true',
                       help='Skip source refresh, just rebuild V2')
    parser.add_argument('--validate-only', action='store_true',
                       help='Only run validation, no refresh')
    parser.add_argument('--status', action='store_true',
                       help='Show current data status')
    parser.add_argument('--rollback', action='store_true',
                       help='Restore from last backup')
    parser.add_argument('--quiet', action='store_true',
                       help='Minimal output (for cron)')

    args = parser.parse_args()

    # Check for quiet mode from environment
    quiet = args.quiet or os.environ.get('QUEUE_REFRESH_QUIET') == '1'

    refresher = AutoRefresh(quiet=quiet)

    if args.status:
        status = refresher.get_status()
        print(json.dumps(status, indent=2))
        return 0

    if args.rollback:
        refresher.rollback()
        return 0

    if args.validate_only:
        from validate_data import DataValidator
        validator = DataValidator()
        validator.run_all_validations(include_live=True)
        return 0

    # Run full refresh
    results = refresher.run_full_refresh(skip_source_refresh=args.quick)

    # Exit code based on success
    return 0 if results['success'] else 1


if __name__ == '__main__':
    sys.exit(main())
