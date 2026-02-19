#!/usr/bin/env python3
"""
Data Validation Module

Provides input validation, schema checking, and accuracy verification
for the data ingestion pipeline.

Usage:
    from data_validation import DataValidator, ValidationError

    validator = DataValidator()
    validator.validate_source_data(df, 'ercot')  # Raises ValidationError if invalid
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
import json


class ValidationError(Exception):
    """Raised when data validation fails."""
    def __init__(self, message: str, errors: List[str] = None):
        self.message = message
        self.errors = errors or []
        super().__init__(self.format_message())

    def format_message(self) -> str:
        if self.errors:
            return f"{self.message}\n  - " + "\n  - ".join(self.errors)
        return self.message


@dataclass
class ValidationResult:
    """Result of a validation check."""
    source: str
    is_valid: bool
    row_count: int
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    column_coverage: Dict[str, float] = field(default_factory=dict)
    missing_columns: List[str] = field(default_factory=list)
    extra_columns: List[str] = field(default_factory=list)
    null_rates: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            'source': self.source,
            'is_valid': self.is_valid,
            'row_count': self.row_count,
            'errors': self.errors,
            'warnings': self.warnings,
            'column_coverage': self.column_coverage,
            'missing_columns': self.missing_columns,
            'extra_columns': self.extra_columns,
            'null_rates': self.null_rates,
            'timestamp': datetime.now().isoformat()
        }


# =============================================================================
# EXPECTED SCHEMAS PER SOURCE
# =============================================================================

# Required columns that MUST exist (validation fails if missing)
REQUIRED_COLUMNS = {
    'ercot': ['Queue ID', 'Capacity (MW)'],
    'miso': ['Queue ID', 'Capacity (MW)'],
    'nyiso': ['Queue Pos.', 'SP (MW)'],
    'caiso': ['Queue Position'],
    'spp': ['Queue ID'],
    'isone': ['Queue ID'],
    'lbl': ['q_id', 'mw1', 'region'],
}

# Expected columns (warnings if missing, not errors)
EXPECTED_COLUMNS = {
    'ercot': [
        'Queue ID', 'Project Name', 'Capacity (MW)', 'Generation Type',
        'Status', 'County', 'POI Location', 'Projected COD', 'Developer',
        'Interconnecting Entity', 'INR', 'Fuel', 'GIM Study Phase', 'Proposed COD'
    ],
    'miso': [
        'Queue ID', 'Capacity (MW)', 'Developer', 'Generation Type', 'Status',
        'State', 'County', 'POI', 'Queue Date', 'Proposed COD',
        'projectNumber', 'summerNetMW', 'transmissionOwner', 'fuelType',
        'applicationStatus', 'state', 'county', 'poiName', 'queueDate', 'inService'
    ],
    'nyiso': [
        'Queue Pos.', 'Queue Position', 'Project Name', 'SP (MW)', 'Capacity (MW)',
        'Developer/Interconnection Customer', 'Developer', 'Type/ Fuel',
        'S', 'State', 'County', 'Points of Interconnection', 'POI Location',
        'Date of IR', 'Queue Date', 'Proposed COD', 'Projected COD'
    ],
    'caiso': [
        'Queue Position', 'Project Name', 'Net MWs to Grid',
        'On-Peak MWs Deliverability', 'Application Status', 'Fuel-1', 'Type-1',
        'County', 'Queue Date', 'Interconnection Request Receive Date',
        'Actual or Expected On-line Date'
    ],
    'spp': [
        'Queue ID', 'Project Name', 'Capacity (MW)', 'Generation Type',
        'Status', 'State', 'County', 'Interconnection Location',
        'Queue Date', 'Proposed Completion Date', 'Commercial Operation Date',
        'Developer', 'Interconnecting Entity'
    ],
    'isone': [
        'Queue ID', 'Project Name', 'Capacity (MW)', 'Generation Type',
        'Status', 'Project Status', 'State', 'County', 'Interconnection Location',
        'Queue Date', 'Proposed Completion Date', 'Op Date',
        'Developer', 'Interconnecting Entity'
    ],
    'lbl': [
        'q_id', 'project_name', 'developer', 'entity', 'mw1', 'type_clean',
        'q_status', 'state', 'county', 'poi_name', 'q_date', 'prop_date', 'region'
    ],
}

# Minimum expected row counts per source (sanity check)
MIN_ROW_COUNTS = {
    'ercot': 500,      # ERCOT typically has 1000+ projects
    'miso': 800,       # MISO typically has 1500+ projects
    'nyiso': 50,       # NYISO is smaller, ~150 projects
    'caiso': 200,      # CAISO typically has 500+ projects
    'spp': 200,        # SPP typically has 500+ projects
    'isone': 30,       # ISO-NE is smaller
    'lbl': 10000,      # LBL historical has 30k+ projects
}

# Maximum expected row counts (detect duplicates/errors)
MAX_ROW_COUNTS = {
    'ercot': 5000,
    'miso': 5000,
    'nyiso': 1000,
    'caiso': 3000,
    'spp': 3000,
    'isone': 3000,   # Increased - ISO-NE has ~1750 projects
    'lbl': 100000,
}

# Critical fields that should have low null rates
# NOTE: Relaxed thresholds - some sources legitimately have nulls
CRITICAL_FIELDS = {
    'queue_id': 0.15,      # Max 15% null (NYISO often has 10%+ nulls for certain statuses)
    'capacity_mw': 0.20,   # Max 20% null (some projects don't have capacity yet)
    'region': 0.0,         # Must be 0% null
}


class DataValidator:
    """Validates incoming data before ingestion."""

    def __init__(self, strict_mode: bool = False):
        """
        Initialize validator.

        Args:
            strict_mode: If True, warnings become errors
        """
        self.strict_mode = strict_mode
        self.validation_history: List[ValidationResult] = []

    def validate_source_data(
        self,
        df: pd.DataFrame,
        source: str,
        raise_on_error: bool = True
    ) -> ValidationResult:
        """
        Validate raw data from a source before normalization.

        Args:
            df: Raw DataFrame from source
            source: Source identifier ('ercot', 'miso', etc.)
            raise_on_error: If True, raises ValidationError on failure

        Returns:
            ValidationResult with details

        Raises:
            ValidationError: If validation fails and raise_on_error=True
        """
        errors = []
        warnings = []

        source_lower = source.lower()

        # 1. Check for empty data
        if df is None or df.empty:
            result = ValidationResult(
                source=source,
                is_valid=False,
                row_count=0,
                errors=[f"Empty or null DataFrame received from {source}"]
            )
            if raise_on_error:
                raise ValidationError(f"No data from {source}", result.errors)
            return result

        row_count = len(df)

        # 2. Check row count bounds
        min_rows = MIN_ROW_COUNTS.get(source_lower, 10)
        max_rows = MAX_ROW_COUNTS.get(source_lower, 100000)

        if row_count < min_rows:
            errors.append(
                f"Row count {row_count} is below minimum {min_rows} for {source}. "
                f"This may indicate a fetch failure or API change."
            )

        if row_count > max_rows:
            warnings.append(
                f"Row count {row_count} exceeds expected maximum {max_rows} for {source}. "
                f"This may indicate duplicate data."
            )

        # 3. Check required columns
        required = REQUIRED_COLUMNS.get(source_lower, [])
        actual_columns = set(df.columns)
        missing_required = [col for col in required if col not in actual_columns]

        if missing_required:
            # Check for alternative column names
            alternatives_found = self._check_alternative_columns(df, source_lower, missing_required)
            still_missing = [col for col in missing_required if col not in alternatives_found]

            if still_missing:
                errors.append(
                    f"Missing required columns: {still_missing}. "
                    f"Available columns: {sorted(actual_columns)[:20]}..."
                )

        # 4. Check expected columns (warnings only)
        expected = EXPECTED_COLUMNS.get(source_lower, [])
        missing_expected = [col for col in expected if col not in actual_columns]
        extra_columns = [col for col in actual_columns if col not in expected]

        # Calculate coverage
        matched = len([c for c in expected if c in actual_columns])
        coverage = matched / len(expected) if expected else 1.0

        if coverage < 0.5:
            warnings.append(
                f"Low column coverage ({coverage:.0%}). "
                f"Expected columns may have been renamed."
            )

        if extra_columns and len(extra_columns) > 5:
            warnings.append(
                f"Found {len(extra_columns)} unexpected columns. "
                f"Schema may have changed: {extra_columns[:5]}..."
            )

        # 5. Check null rates for critical fields
        null_rates = {}
        for col in df.columns:
            null_rate = df[col].isna().sum() / len(df)
            null_rates[col] = round(null_rate, 4)

        # Check queue_id null rate (using various possible column names)
        queue_id_cols = ['Queue ID', 'Queue Pos.', 'Queue Position', 'q_id', 'INR']
        for qcol in queue_id_cols:
            if qcol in df.columns:
                qnull_rate = null_rates.get(qcol, 1.0)
                if qnull_rate > CRITICAL_FIELDS['queue_id']:
                    errors.append(
                        f"Queue ID column '{qcol}' has {qnull_rate:.1%} null rate "
                        f"(max allowed: {CRITICAL_FIELDS['queue_id']:.1%})"
                    )
                break

        # Check capacity null rate
        capacity_cols = ['Capacity (MW)', 'SP (MW)', 'mw1', 'summerNetMW', 'Net MWs to Grid']
        for ccol in capacity_cols:
            if ccol in df.columns:
                cnull_rate = null_rates.get(ccol, 1.0)
                if cnull_rate > CRITICAL_FIELDS['capacity_mw']:
                    warnings.append(
                        f"Capacity column '{ccol}' has {cnull_rate:.1%} null rate "
                        f"(expected max: {CRITICAL_FIELDS['capacity_mw']:.1%})"
                    )
                break

        # 6. Check for duplicate queue IDs
        for qcol in queue_id_cols:
            if qcol in df.columns:
                dup_count = df[qcol].dropna().duplicated().sum()
                if dup_count > 0:
                    dup_rate = dup_count / len(df)
                    if dup_rate > 0.10:  # More than 10% duplicates is suspicious
                        warnings.append(
                            f"Found {dup_count} duplicate queue IDs ({dup_rate:.1%}). "
                            f"This may be intentional (multiple phases) or an error."
                        )
                break

        # 7. Validate capacity values
        for ccol in capacity_cols:
            if ccol in df.columns:
                cap_series = pd.to_numeric(df[ccol], errors='coerce')
                negative_count = (cap_series < 0).sum()
                huge_count = (cap_series > 10000).sum()  # > 10 GW is suspicious

                # Negative values are warnings, not errors - they can be cleaned up
                if negative_count > 0:
                    warnings.append(f"Found {negative_count} negative capacity values (will be set to NULL)")

                if huge_count > 10:  # A few large projects are OK
                    warnings.append(f"Found {huge_count} projects > 10 GW capacity")
                break

        # Compile result
        is_valid = len(errors) == 0
        if self.strict_mode:
            is_valid = is_valid and len(warnings) == 0

        result = ValidationResult(
            source=source,
            is_valid=is_valid,
            row_count=row_count,
            errors=errors,
            warnings=warnings,
            column_coverage={source: coverage},
            missing_columns=missing_expected,
            extra_columns=extra_columns,
            null_rates=null_rates
        )

        self.validation_history.append(result)

        if raise_on_error and not is_valid:
            raise ValidationError(
                f"Validation failed for {source} ({row_count} rows)",
                errors
            )

        return result

    def _check_alternative_columns(
        self,
        df: pd.DataFrame,
        source: str,
        missing: List[str]
    ) -> Set[str]:
        """Check if missing columns have alternative names present."""
        # Include both raw API column names AND normalized column names
        # since some loaders (like miso_loader) return pre-normalized data
        alternatives = {
            'Queue ID': ['INR', 'Queue Pos.', 'Queue Position', 'q_id', 'projectNumber', 'queue_id'],
            'Capacity (MW)': ['SP (MW)', 'mw1', 'summerNetMW', 'Net MWs to Grid', 'MW', 'capacity_mw'],
            'Developer': ['Developer/Interconnection Customer', 'Interconnecting Entity',
                         'transmissionOwner', 'entity', 'developer', 'utility'],
            'Status': ['S', 'q_status', 'applicationStatus', 'GIM Study Phase',
                      'Application Status', 'Project Status', 'status', 'status_raw'],
            'Queue Date': ['Date of IR', 'q_date', 'queueDate',
                          'Interconnection Request Receive Date', 'queue_date'],
        }

        found = set()
        actual_columns = set(df.columns)

        for missing_col in missing:
            alts = alternatives.get(missing_col, [])
            for alt in alts:
                if alt in actual_columns:
                    found.add(missing_col)
                    break

        return found

    def validate_normalized_data(
        self,
        df: pd.DataFrame,
        source: str,
        raise_on_error: bool = True
    ) -> ValidationResult:
        """
        Validate data after normalization but before database insert.

        Args:
            df: Normalized DataFrame with standard columns
            source: Source identifier
            raise_on_error: If True, raises ValidationError on failure
        """
        errors = []
        warnings = []

        expected_normalized = ['queue_id', 'region', 'capacity_mw', 'status']
        actual = set(df.columns)
        missing = [c for c in expected_normalized if c not in actual]

        if missing:
            errors.append(f"Missing normalized columns: {missing}")

        # Check queue_id
        if 'queue_id' in df.columns:
            null_qid = df['queue_id'].isna().sum()
            empty_qid = (df['queue_id'] == '').sum()
            invalid_qid = null_qid + empty_qid

            if invalid_qid > 0:
                warnings.append(f"Found {invalid_qid} records with null/empty queue_id")

        # Check capacity
        if 'capacity_mw' in df.columns:
            cap = pd.to_numeric(df['capacity_mw'], errors='coerce')
            invalid_cap = cap.isna().sum() + (cap <= 0).sum()
            if invalid_cap / len(df) > 0.20:
                warnings.append(f"{invalid_cap} records ({invalid_cap/len(df):.1%}) have invalid capacity")

        is_valid = len(errors) == 0

        result = ValidationResult(
            source=source,
            is_valid=is_valid,
            row_count=len(df),
            errors=errors,
            warnings=warnings
        )

        if raise_on_error and not is_valid:
            raise ValidationError(f"Normalized data validation failed for {source}", errors)

        return result

    def get_validation_summary(self) -> dict:
        """Get summary of all validations in this session."""
        if not self.validation_history:
            return {'status': 'no_validations', 'results': []}

        passed = sum(1 for r in self.validation_history if r.is_valid)
        failed = len(self.validation_history) - passed
        total_warnings = sum(len(r.warnings) for r in self.validation_history)

        return {
            'status': 'all_passed' if failed == 0 else 'some_failed',
            'total_validations': len(self.validation_history),
            'passed': passed,
            'failed': failed,
            'total_warnings': total_warnings,
            'results': [r.to_dict() for r in self.validation_history]
        }

    def print_summary(self):
        """Print a formatted validation summary."""
        summary = self.get_validation_summary()

        print("\n" + "=" * 60)
        print("VALIDATION SUMMARY")
        print("=" * 60)

        if summary['status'] == 'no_validations':
            print("No validations performed.")
            return

        status_icon = "PASS" if summary['status'] == 'all_passed' else "FAIL"
        print(f"\nStatus: {status_icon}")
        print(f"Validations: {summary['passed']}/{summary['total_validations']} passed")
        print(f"Warnings: {summary['total_warnings']}")

        print("\nBy Source:")
        for result in summary['results']:
            icon = "OK" if result['is_valid'] else "FAIL"
            print(f"  [{icon}] {result['source']}: {result['row_count']:,} rows")

            if result['errors']:
                for err in result['errors']:
                    print(f"       ERROR: {err[:70]}...")

            if result['warnings']:
                for warn in result['warnings'][:3]:
                    print(f"       WARN: {warn[:70]}...")
                if len(result['warnings']) > 3:
                    print(f"       ... and {len(result['warnings']) - 3} more warnings")


def validate_before_refresh(df: pd.DataFrame, source: str) -> Tuple[bool, ValidationResult]:
    """
    Convenience function to validate data before refresh.

    Returns:
        Tuple of (is_valid, validation_result)
    """
    validator = DataValidator()
    try:
        result = validator.validate_source_data(df, source, raise_on_error=False)
        return result.is_valid, result
    except Exception as e:
        return False, ValidationResult(
            source=source,
            is_valid=False,
            row_count=0,
            errors=[str(e)]
        )
