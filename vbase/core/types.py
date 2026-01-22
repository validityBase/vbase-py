# flake8: noqa

from dataclasses import dataclass

# If last update of the node transaction is older than this threshold, indexing is considered stale.
# All operations of this indexer will fail.
INDEXING_STALE_THRESHOLD_SECONDS = 30
DAY_HORIZON = 24 * 60 * 60
DAY_HORIZONT = DAY_HORIZON  # Backwards-compatible alias; prefer DAY_HORIZON


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
class FindBestCandidateRequest:
    """
    Request object for finding best candidate matches.
    """

    objects: list[ObjectAtTime]
    as_of: int | None = None
    max_timestamp_diff: int = DAY_HORIZONT
