"""
Core types for indexing and matching strategies.
"""

from dataclasses import dataclass

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

    score: float
    created_at: int
    set_cid: str
    user: str
    as_of_timestamp: int
