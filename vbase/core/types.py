"""
Core types for indexing and matching strategies.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


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
            Only consider records with timestamp <= as_of.
            Always stored as a UTC-aware pandas.Timestamp or None.
    """

    objects: list[ObjectAtTime]
    as_of: pd.Timestamp | int | None = None

    def __post_init__(self) -> None:
        self.as_of = self._normalize_as_of(self.as_of)

    @staticmethod
    def _normalize_as_of(
        as_of: pd.Timestamp | int | None,
    ) -> pd.Timestamp | None:
        if as_of is None:
            return None
        if isinstance(as_of, pd.Timestamp):
            return as_of
        if isinstance(as_of, int):
            return pd.Timestamp(as_of, unit="s", tz="UTC")
        raise TypeError("as_of must be a pandas.Timestamp, int, or None")


@dataclass
class SetMatchingServiceConfig:
    """
    Configuration for set matching strategies.

    Attributes:
        max_timestamp_diff (pd.Timedelta):
            Maximum allowed absolute difference between object and candidate timestamps for a match.
            Should be a pandas.Timedelta (e.g., pd.Timedelta('1D')).
            Defaults to 1 day.
    """

    max_timestamp_diff: pd.Timedelta = pd.Timedelta("1D")
