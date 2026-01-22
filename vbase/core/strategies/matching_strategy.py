# flake8: noqa

from abc import ABC, abstractmethod
from bisect import bisect_left
from collections import defaultdict
from typing import List

from sqlalchemy import func, tuple_
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

from ..models import event_add_set_object
from ..types import DAY_HORIZONT, FindBestCandidateRequest, ObjectAtTime, SetCandidate


class BaseMatchingStrategy(ABC):
    @abstractmethod
    def find_best_candidate(
        self,
        request: FindBestCandidateRequest,
    ) -> list[SetCandidate]:
        pass


class SQLMatchingStrategy(BaseMatchingStrategy):
    def __init__(self, db_engine: Engine):
        self.db_engine = db_engine

    def find_best_candidate(
        self,
        request: FindBestCandidateRequest,
    ) -> list[SetCandidate]:
        """
        Matching semantics:
        - set_cid: exact
        - user: exact
        - object_cid: exact
        - timestamp: abs(diff) <= max_timestamp_diff
        """

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
                probe_stmt = probe_stmt.where(event_add_set_object.timestamp <= as_of)

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
                load_stmt = load_stmt.where(event_add_set_object.timestamp <= as_of)

            rows = session.exec(load_stmt).all()

        # ------------------------------------------------------------
        # PHASE 3: BUILD BUCKETS (ordered timestamps)
        # ------------------------------------------------------------
        buckets: dict[tuple[str, str], dict[str, list[int]]] = defaultdict(
            lambda: defaultdict(list)
        )

        created_at: dict[tuple[str, str], int] = {}

        for r in rows:
            key = (r.set_cid, r.user)
            ts = self._normalize_ts(r.timestamp)
            buckets[key][r.object_cid].append(ts)
            created_at.setdefault(key, self._normalize_ts(r.created_at))

        # ------------------------------------------------------------
        # PHASE 4: ORDERED MATCHING
        # ------------------------------------------------------------
        def has_match(ts_list: list[int], t: int) -> bool:
            i = bisect_left(ts_list, t)

            if i < len(ts_list) and abs(ts_list[i] - t) <= max_timestamp_diff:
                return True
            if i > 0 and abs(ts_list[i - 1] - t) <= max_timestamp_diff:
                return True
            return False

        matched_counts: dict[tuple[str, str], int] = defaultdict(int)

        for key, by_object in buckets.items():
            for q in objects:
                ts_list = by_object.get(q.object_cid)
                if ts_list and has_match(ts_list, q.timestamp):
                    matched_counts[key] += 1

        # ------------------------------------------------------------
        # PHASE 5: BUILD RESULTS
        # ------------------------------------------------------------
        results: list[SetCandidate] = []

        for key, matched in matched_counts.items():
            score = matched / query_len
            if score == 0:
                continue

            set_cid, user = key
            results.append(
                SetCandidate(
                    set_cid=set_cid,
                    user=user,
                    score=score,
                    created_at=created_at[key],
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
