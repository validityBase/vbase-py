"""
Set matching strategies for identifying on-chain committed datasets that best
correspond to a caller-supplied list of (object_cid, timestamp) pairs.

Background
----------
vBase users commit individual data objects to the blockchain, grouping them into
named "sets" (datasets). A common analytics need is the reverse lookup: given a
snapshot of objects observed at particular times — for example, the holdings of a
portfolio at a specific date — find which previously-committed on-chain set most
closely matches that snapshot in both content and timing.

This module provides that reverse-lookup capability.

Usage
-----
`SetMatchingService` is the default implementation and is instantiated automatically
by `SQLIndexingService`. Callers that need non-standard matching behaviour (e.g.
stricter timestamp tolerances, alternative scoring) can subclass `BaseMatchingService`
and pass an instance to `SQLIndexingService(matching_service=...)`.

The primary entry point is `SQLIndexingService.find_matching_user_sets()`, which
wraps `SetMatchingCriteria` construction and delegates to the matching service.
Direct use of `SetMatchingService` is only needed in tests or custom integrations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from bisect import bisect_left
from collections import defaultdict
from dataclasses import dataclass, field
from typing import DefaultDict

import pandas as pd
from sqlalchemy import func, tuple_
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

from .models import EventAddSetObject
from .types import (
    ObjectAtTime,
    SetCandidate,
    SetMatchingCriteria,
    SetMatchingServiceConfig,
)


@dataclass(frozen=True, slots=True)
class BucketKey:
    """Key for bucketing sets."""

    set_cid: str
    user: str


@dataclass
class BucketItem:
    """Item in a bucket, holding timestamps and creation time."""

    created_at: int | None = None
    timestamps: DefaultDict[str, list[int]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def add_event(self, object_cid: str, ts: int) -> None:
        """Add an event timestamp for a given object CID."""
        self.timestamps[object_cid].append(ts)

    def set_created_at_once(self, created_ts: int) -> None:
        """Set the creation timestamp if not already set."""
        if self.created_at is None:
            self.created_at = created_ts


class BaseMatchingService(ABC):
    """
    Abstract base class for set matching strategies.

    Subclass this to implement alternative matching logic (e.g. stricter
    timestamp tolerances, weighted scoring, or a non-SQL data source) and
    pass the instance to SQLIndexingService(matching_service=...).
    """

    @abstractmethod
    def find_matching_user_sets(
        self,
        criteria: SetMatchingCriteria,
    ) -> list[SetCandidate]:
        """
        Find committed sets that best match the provided object/timestamp criteria.

        Args:
            criteria: Query objects with timestamps, and an optional as_of cutoff.
        Returns:
            Matching sets ordered by descending similarity score, then ascending
            creation time. Score is the fraction of query objects that have a
            committed counterpart within the configured timestamp tolerance.
        """
        pass

    @staticmethod
    def _normalize_unix_ts(ts: int) -> int:
        """Normalize a UNIX timestamp by converting millisecond values to seconds."""
        return ts // 1000 if ts > 10_000_000_000 else ts

    def _normalize_criteria(
        self,
        criteria: SetMatchingCriteria,
    ) -> tuple[list[ObjectAtTime], int | None]:
        """Normalize the matching criteria."""
        objects = [
            ObjectAtTime(
                object_cid=obj.object_cid,
                timestamp=self._normalize_unix_ts(obj.timestamp),
            )
            for obj in criteria.objects
        ]

        as_of_unix: int | None = None
        if criteria.as_of is not None:
            if not isinstance(criteria.as_of, pd.Timestamp):
                raise TypeError("as_of must be a pandas.Timestamp or None")
            if criteria.as_of.tzinfo is None:
                raise ValueError("as_of must be timezone-aware")
            as_of_unix = int(criteria.as_of.timestamp())

        return objects, as_of_unix


class SetMatchingService(BaseMatchingService):
    """
    SQL-backed implementation of BaseMatchingService.

    Matching algorithm (multi-phase):
    1. Probe: find all (set_cid, user) pairs that share at least one object_cid
       with the query, optionally filtered by an as_of timestamp cutoff.
    2. Load: fetch the full event records for those candidate pairs, restricted
       to the query's object_cids, with each row annotated with its set's
       creation time via a window function.
    3. Bucket: group events by (set_cid, user) and sort per-object timestamp
       lists for binary search.
    4. Score: for each query object, check whether a candidate set committed
       that object within max_timestamp_diff of the query timestamp. Score is
       matched_count / len(query_objects).
    5. Return candidates sorted by descending score, then ascending creation time.

    Configure timestamp tolerance via SetMatchingServiceConfig.max_timestamp_diff
    (default: 1 day).
    """

    def __init__(
        self,
        db_engine: Engine,
        config: SetMatchingServiceConfig | None = None,
    ):
        self.db_engine = db_engine
        self.config = config or SetMatchingServiceConfig()

    @staticmethod
    def _prepare_query_objects(
        objects: list[ObjectAtTime],
    ) -> tuple[list[ObjectAtTime], set[str]]:
        """Normalize query objects into a stable, deduplicated order."""
        normalized_objects = sorted(set(objects), key=lambda o: o.timestamp)
        query_cids = {o.object_cid for o in normalized_objects}
        return normalized_objects, query_cids

    @staticmethod
    def _find_user_sets_by_object_cids(
        session: Session,
        query_cids: set[str],
        as_of_unix: int | None,
    ) -> set[tuple[str, str]]:
        """Find (set_cid, user) pairs that contain any queried object."""
        probe_stmt = select(
            EventAddSetObject.set_cid,
            EventAddSetObject.user,
        ).where(EventAddSetObject.object_cid.in_(query_cids))

        if as_of_unix is not None:
            probe_stmt = probe_stmt.where(EventAddSetObject.timestamp <= as_of_unix)

        return {(r.set_cid, r.user) for r in session.exec(probe_stmt).all()}

    @staticmethod
    def _load_user_sets_objects_matched_by_keys(
        session: Session,
        candidate_keys: set[tuple[str, str]],
        query_cids: set[str],
        as_of_unix: int | None,
    ) -> list:
        """Load EventAddSetObject records matching candidate (set_cid, user) pairs"""
        load_stmt = (
            select(
                EventAddSetObject.set_cid,
                EventAddSetObject.user,
                EventAddSetObject.object_cid,
                EventAddSetObject.timestamp,
                func.min(EventAddSetObject.timestamp)
                .over(
                    partition_by=(
                        EventAddSetObject.set_cid,
                        EventAddSetObject.user,
                    )
                )
                .label("created_at"),
            )
            .where(
                tuple_(
                    EventAddSetObject.set_cid,
                    EventAddSetObject.user,
                ).in_(candidate_keys)
            )
            .where(EventAddSetObject.object_cid.in_(query_cids))
            .order_by(EventAddSetObject.timestamp)
        )

        if as_of_unix is not None:
            load_stmt = load_stmt.where(EventAddSetObject.timestamp <= as_of_unix)

        return session.exec(load_stmt).all()

    def _build_buckets(self, rows: list) -> dict[BucketKey, BucketItem]:
        """Build per-(set_cid, user) buckets of timestamps, normalized to seconds."""
        buckets: dict[BucketKey, BucketItem] = defaultdict(BucketItem)

        for r in rows:
            key = BucketKey(set_cid=r.set_cid, user=r.user)
            ts = self._normalize_unix_ts(r.timestamp)
            created_ts = self._normalize_unix_ts(r.created_at)

            buckets[key].add_event(object_cid=r.object_cid, ts=ts)
            buckets[key].set_created_at_once(created_ts)

        for bucket in buckets.values():
            for ts_list in bucket.timestamps.values():
                ts_list.sort()

        return buckets

    @staticmethod
    def _has_match(ts_list: list[int], t: int, max_diff_sec: int) -> bool:
        """Check if ts_list contains a timestamp within max_diff_sec of t."""
        i = bisect_left(ts_list, t)
        candidates = []
        if i > 0:
            candidates.append(ts_list[i - 1])
        if i < len(ts_list):
            candidates.append(ts_list[i])

        return any(abs(ts - t) <= max_diff_sec for ts in candidates)

    def _count_matches(
        self,
        buckets: dict[BucketKey, BucketItem],
        objects: list[ObjectAtTime],
        max_diff_sec: int,
    ) -> dict[BucketKey, int]:
        """Count how many query objects match each bucket."""
        matched_counts: dict[BucketKey, int] = defaultdict(int)

        for key, bucket in buckets.items():
            timestamps_by_object: dict[str, list[int]] = bucket.timestamps
            for query_obj in objects:
                object_cid: str = query_obj.object_cid
                query_ts: int = query_obj.timestamp
                ts_list: list[int] | None = timestamps_by_object.get(object_cid)
                if ts_list is None:
                    continue
                if self._has_match(ts_list, query_ts, max_diff_sec):
                    matched_counts[key] += 1

        return matched_counts

    @staticmethod
    def _build_results(
        buckets: dict[BucketKey, BucketItem],
        matched_counts: dict[BucketKey, int],
        query_len: int,
    ) -> list[SetCandidate]:
        """Build and sort SetCandidate results from matched counts."""
        results: list[SetCandidate] = []

        for key, matched in matched_counts.items():
            if matched == 0:
                continue

            score = matched / query_len
            bucket = buckets[key]

            results.append(
                SetCandidate(
                    set_cid=key.set_cid,
                    user=key.user,
                    score=score,
                    created_at=bucket.created_at,
                )
            )

        results.sort(key=lambda r: (-r.score, r.created_at))
        return results

    def find_matching_user_sets(
        self,
        criteria: SetMatchingCriteria,
    ) -> list[SetCandidate]:
        """
        Find user sets that best match the given object/timestamp criteria.

        This method performs a multi-phase SQL-backed match:
        1) Normalize inputs (timestamps and as_of) and deduplicate query objects.
        2) Probe for candidate (set_cid, user) pairs containing any query object.
        3) Load ordered events for candidates and bucket them per set/user.
        4) For each query object, check if a candidate has a nearby timestamp
           within max_timestamp_diff, then score by match ratio.

        Matching semantics:
        - set_cid: exact
        - user: exact
        - object_cid: exact
        - timestamp: abs(diff) <= max_timestamp_diff

        Args:
            criteria (SetMatchingCriteria): Criteria for matching user sets.
        Returns:
            list[SetCandidate]: List of candidate sets matching the criteria.
        """
        objects, as_of_unix = self._normalize_criteria(criteria)
        max_timestamp_diff = self.config.max_timestamp_diff

        if not objects:
            return []

        objects, query_cids = self._prepare_query_objects(objects)
        query_len = len(objects)

        with Session(self.db_engine) as session:
            candidate_keys = self._find_user_sets_by_object_cids(
                session=session,
                query_cids=query_cids,
                as_of_unix=as_of_unix,
            )

            if not candidate_keys:
                return []

            rows = self._load_user_sets_objects_matched_by_keys(
                session=session,
                candidate_keys=candidate_keys,
                query_cids=query_cids,
                as_of_unix=as_of_unix,
            )

        buckets = self._build_buckets(rows)
        max_diff_sec = int(max_timestamp_diff.total_seconds())
        matched_counts = self._count_matches(
            buckets=buckets,
            objects=objects,
            max_diff_sec=max_diff_sec,
        )

        return self._build_results(
            buckets=buckets,
            matched_counts=matched_counts,
            query_len=query_len,
        )
