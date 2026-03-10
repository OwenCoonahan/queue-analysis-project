"""
Permitting Scrapers - Data loaders for permit status tracking.

This package provides loaders for collecting permitting data from:
- EIA Form 860 (federal, most reliable)
- California CEC Power Plants
- California CPUC RPS Database
- New York NYSERDA Large-Scale Renewables

Usage:
    from permitting_scrapers import EIAPlannedLoader, PermitMatcher

    # Load EIA proposed generators
    loader = EIAPlannedLoader()
    permits_df = loader.load()

    # Match to queue projects
    matcher = PermitMatcher(queue_df)
    matched = matcher.match_batch(permits_df)

    # Load California data
    from permitting_scrapers import CaliforniaCECLoader, CaliforniaCPUCLoader
    cec_loader = CaliforniaCECLoader()
    cec_df = cec_loader.load()

    # Load New York data
    from permitting_scrapers import NYSERDALoader
    ny_loader = NYSERDALoader()
    ny_df = ny_loader.load()
"""

from .eia_planned_loader import EIAPlannedLoader
from .permit_matcher import PermitMatcher
from .california_cec_loader import CaliforniaCECLoader
from .california_cpuc_loader import CaliforniaCPUCLoader
from .nyserda_loader import NYSERDALoader
from .nrel_solartrace_loader import NRELSolarTraceLoader

__all__ = [
    'EIAPlannedLoader',
    'PermitMatcher',
    'CaliforniaCECLoader',
    'CaliforniaCPUCLoader',
    'NYSERDALoader',
    'NRELSolarTraceLoader',
]
