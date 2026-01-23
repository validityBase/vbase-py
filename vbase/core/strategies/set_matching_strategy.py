"""
Matching strategies for finding best candidate sets.
"""

from abc import ABC, abstractmethod
from bisect import bisect_left
from collections import defaultdict
from dataclasses import dataclass, field
from typing import DefaultDict

from sqlalchemy import func, tuple_
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

from ..models import event_add_set_object
from ..types import SetCandidate, SetMatchingCriteria


@dataclass(frozen=True, slots=True)
class BucketKey:
    set_cid: str
    user: str


@dataclass
class BucketItem:
    created_at: int | None = None
    timestamps: DefaultDict[str, list[int]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def add_event(self, object_cid: str, ts: int) -> None:
        self.timestamps[object_cid].append(ts)

    def set_created_at_once(self, created_ts: int) -> None:
        if self.created_at is None:
            self.created_at = created_ts


class BaseMatchingStrategy(ABC):
    """Base class for matching strategies."""

    @abstractmethod
    def find_matching_user_sets(
        self,
        request: SetMatchingCriteria,
    ) -> list[SetCandidate]:
        """
        Find the best candidate sets based on the provided request.
        Args:
            request (SetMatchingCriteria): Criteria for matching user sets.
        Returns:
            list[SetCandidate]: List of candidate sets matching the criteria.
        """
        pass


class SetMatchingStrategy(BaseMatchingStrategy):
    """Set matching strategy implementation using SQL database."""

    def __init__(self, db_engine: Engine):
        self.db_engine = db_engine

    def find_matching_user_sets(
        self,
        request: SetMatchingCriteria,
    ) -> list[SetCandidate]:
        """
        Find the best candidate sets based on the provided request.
        Args:
            request (SetMatchingCriteria): Criteria for matching user sets.
        Returns:
            list[SetCandidate]: List of candidate sets matching the criteria.
        Matching semantics:
            - set_cid: exact
            - user: exact
            - object_cid: exact
            - timestamp: abs(diff) <= max_timestamp_diff
        """
        import pandas as pd

        objects = request.objects
        as_of = request.as_of
        max_timestamp_diff = request.max_timestamp_diff

        if not objects:
            return []

        # ------------------------------------------------------------
        # PHASE 0: NORMALIZE INPUT
        # ------------------------------------------------------------
        objects = sorted(set(objects), key=lambda o: o.timestamp)
        query_len = len(objects)
        query_cids = {o.object_cid for o in objects}

        with Session(self.db_engine) as session:
            # ------------------------------------------------------------
            # PHASE 1: PROBE (discover candidate sets)
            # ------------------------------------------------------------
            probe_stmt = select(
                event_add_set_object.set_cid,
                event_add_set_object.user,
            ).where(event_add_set_object.object_cid.in_(query_cids))

            if as_of is not None:
                # Convert pd.Timestamp to int (seconds since epoch)
                as_of_unix = int(as_of.timestamp())
                probe_stmt = probe_stmt.where(
                    event_add_set_object.timestamp <= as_of_unix
                )

            candidate_keys = {
                (r.set_cid, r.user) for r in session.exec(probe_stmt).all()
            }

            if not candidate_keys:
                return []

            # ------------------------------------------------------------
            # PHASE 2: LOAD ALL CANDIDATE OBJECTS
            # ------------------------------------------------------------
            load_stmt = (
                select(
                    event_add_set_object.set_cid,
                    event_add_set_object.user,
                    event_add_set_object.object_cid,
                    event_add_set_object.timestamp,
                    func.min(event_add_set_object.timestamp)
                    .over(
                        partition_by=(
                            event_add_set_object.set_cid,
                            event_add_set_object.user,
                        )
                    )
                    .label("created_at"),
                )
                .where(
                    tuple_(
                        event_add_set_object.set_cid,
                        event_add_set_object.user,
                    ).in_(candidate_keys)
                )
                .where(event_add_set_object.object_cid.in_(query_cids))
                .order_by(event_add_set_object.timestamp)
            )

            if as_of is not None:
                # Convert pd.Timestamp to int (seconds since epoch)
                as_of_unix = int(as_of.timestamp())
                load_stmt = load_stmt.where(
                    event_add_set_object.timestamp <= as_of_unix
                )

            rows = session.exec(load_stmt).all()

        # ------------------------------------------------------------
        # PHASE 3: BUILD TIME BUCKETS (ordered timestamps)
        # ------------------------------------------------------------
        buckets: dict[BucketKey, BucketItem] = defaultdict(BucketItem)

        for r in rows:
            key = BucketKey(set_cid=r.set_cid, user=r.user)
            ts = self._normalize_ts(r.timestamp)
            created_ts = self._normalize_ts(r.created_at)

            buckets[key].add_event(object_cid=r.object_cid, ts=ts)
            buckets[key].set_created_at_once(created_ts)

        # ------------------------------------------------------------
        # PHASE 4: ORDERED MATCHING
        # ------------------------------------------------------------
        def has_match(ts_list: list[int], t: int) -> bool:
            """Check if there is a timestamp in ts_list within max_timestamp_diff of t.
            ts_list:  [ ... , L , R , ... ] - should be always sorted
            t:        target timestamp
            R = ts_list[i] is the smallest value ≥ t
            L = ts_list[i - 1] is the largest value ≤ t

            """
            max_diff_sec = int(max_timestamp_diff.total_seconds())
            i = bisect_left(ts_list, t)
            candidates = []
            if i > 0:
                candidates.append(ts_list[i - 1])
            if i < len(ts_list):
                candidates.append(ts_list[i])

            return any(abs(ts - t) <= max_diff_sec for ts in candidates)

        matched_counts: dict[BucketKey, int] = defaultdict(int)

        for key, bucket in buckets.items():
            timestamps_by_object: dict[str, list[int]] = bucket.timestamps
            for query_obj in objects:
                object_cid: str = query_obj.object_cid
                query_ts: int = query_obj.timestamp
                ts_list: list[int] | None = timestamps_by_object.get(object_cid)
                if ts_list is None:
                    continue
                if has_match(ts_list, query_ts):
                    matched_counts[key] += 1

        # ------------------------------------------------------------
        # PHASE 5: BUILD RESULTS
        # ------------------------------------------------------------
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

    def _normalize_ts(self, ts: int) -> int:
        """Normalize a UNIX timestamp by converting millisecond values to seconds.

        Timestamps greater than 10_000_000_000 are assumed to be in milliseconds
        and are converted to seconds by integer division; smaller values are
        treated as already being in seconds and returned unchanged.
        """
        return ts // 1000 if ts > 10_000_000_000 else ts
