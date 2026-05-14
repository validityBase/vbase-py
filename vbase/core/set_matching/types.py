"""
Core types for indexing and matching strategies.
"""

from dataclasses import dataclass, field

from vbase.core.models import EventAddSetObject
@dataclass
class SetMatchingCriteriaItem:
    """Pair of object_cid and its timestamp criteria."""

    object_cid: str
    timestamp: int


@dataclass
class SetMatchingCriteria:
    """
    Describes the set of elements for which we are trying to find a matching set on a
    blockchain. Criteria consist of elements where each element pairs an object_cid
    with its timestamp.
    """

    objects: list[SetMatchingCriteriaItem]

@dataclass(frozen=True)
class SetMatch:
    """
    Represents a successful match of a set of objects to a set on the blockchain,
    along with metadata about the match.
    """

    # A rank representing how well the set matches [0:1], where 1.0 is a perfect match.
    rank: float
    set_cid: str
    user: str
    last_matching_element_timestamp: int
    #: Whether the match is a full match (all criteria objects are in the set and all
    #: set objects are in the criteria), or a partial match where extra objects are in
    #: the set.
    is_full_match: bool
    data_freshness_timestamp: int | None = None


@dataclass(frozen=True)
class SetIdentifier:
    """
    Unique identifier for a set, based on its set_cid and user.
    Sets can span multiple chains (distributed sets).
    """
    set_cid: str
    user: str


@dataclass
class ObjectSetData:
    """
    Holds data for a set of objects, including its key, the objects themselves,
    and a rank for matching purposes.
    """

    key: SetIdentifier
    objects: list[EventAddSetObject]
    rank: float | None = None
    # Length of the complete chain of events for this set, used for tie-breaking.
    set_length: int | None = None


@dataclass(frozen=True)
class LevenshteinDistance:
    """
    Result of Levenshtein distance calculation with detailed operation counts.
    """

    insertions: int  # Number of insertion operations
    deletions: int  # Number of deletion operations
    substitutions: int  # Number of substitution operations
    distance: int  # Total edit distance (insertions + deletions + substitutions)
    # Per-operation record in forward order: each tuple is (op_type, position).
    # 'I'=insertion at position in seq2; 'D'=deletion at position in seq1;
    # 'S'=substitution at position in seq1.
    operations: list[tuple[str, int]] = field(default_factory=list)


@dataclass
class FuzzyCheckObjectSetData(ObjectSetData):
    """
    Holds fuzzy match evaluation data for a set of objects.
    """

    lev_result: LevenshteinDistance | None = None
    projected_last_element_index: int | None = None
