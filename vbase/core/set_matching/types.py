"""
Core types for indexing and matching strategies.
"""

from dataclasses import dataclass
from vbase.core.models import EventAddSetObject

@dataclass
class SetMatchingCriteriaItem:
    """Pair of object_cid and its timestamp criteria."""

    object_cid: str
    timestamp: int


@dataclass
class SetMatchingCriteria:
    """
    Describes the set of elements for which we are trying to find a matching set on a blockchain.
    Criteria consist of elements where each element pairs an object_cid with its timestamp.
    """

    objects: list[SetMatchingCriteriaItem]

@dataclass(frozen=True)
class SetMatching:
    """
    Represents a successful match of a set of objects to a set on the blockchain, along with metadata about the match.
    """

    score: float # a score representing how well the set matches [0:1], where 1.0 is a perfect match
    set_cid: str
    user: str
    as_of_timestamp: int


@dataclass(frozen=True)
class SetKey:
    """
    Unique identifier for a set, based on its set_cid, user, and chain_id.
    """
    set_cid: str
    user: str
    chain_id: int


@dataclass
class ObjectSetData:
    """
    Holds data for a set of objects, including its key, the objects themselves, and a rank for matching purposes.
    """
    key: SetKey
    objects: list[EventAddSetObject]
    rank: float | None = None


@dataclass(frozen=True)
class LevenshteinDistance:
    """
    Result of Levenshtein distance calculation with detailed operation counts.
    """
    insertions: int  # Number of insertion operations
    deletions: int  # Number of deletion operations
    substitutions: int  # Number of substitution operations
    distance: int  # Total edit distance (insertions + deletions + substitutions)


@dataclass
class FuzzyCheckObjectSetData(ObjectSetData):
    """
    Holds fuzzy match evaluation data for a set of objects.
    """
    lev_result: LevenshteinDistance | None = None
