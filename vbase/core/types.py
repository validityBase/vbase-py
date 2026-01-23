"""
Core types for indexing and matching strategies.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

# If last update of the node transaction is older than this threshold, indexing is considered stale.
# All operations of this indexer will fail.
INDEXING_STALE_THRESHOLD_SECONDS = 30
DAY_HORIZONT = 24 * 60 * 60


@dataclass(frozen=True)
class ObjectAtTime:
    """
    Object at time structure.
    """

    object_cid: str
    timestamp: int


@dataclass(frozen=True)
class SetCandidate:
    """
    SetCandidate structure.
    """

    score: float
    created_at: int
    set_cid: str
    user: str


@dataclass
class SetMatchingCriteria:
    """
    Criteria for matching user sets.

    Attributes:
        objects (list[ObjectAtTime]):
            List of objects with their associated timestamps to match against user sets.
        as_of (pd.Timestamp | None):
            Only consider records with timestamp <= as_of. Should be a pandas.Timestamp (UTC-aware).
            If None, all records are considered.
    """

    objects: list[ObjectAtTime]
    as_of: pd.Timestamp | None = None


@dataclass
class SetMatchingStrategyConfig:
    """
    Configuration for set matching strategies.

    Attributes:
        max_timestamp_diff (pd.Timedelta):
            Maximum allowed absolute difference between object and candidate timestamps for a match.
            Should be a pandas.Timedelta (e.g., pd.Timedelta('1D')).
            Defaults to 1 day.
    """

    max_timestamp_diff: pd.Timedelta = pd.Timedelta("1D")
