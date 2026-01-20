# flake8: noqa

from bisect import bisect_left
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import List, Union

import pandas as pd
from sqlalchemy import func, tuple_
from sqlalchemy.engine import Engine
from sqlmodel import Field, Session, SQLModel, create_engine, select

from vbase.core.indexing_service import IndexingService

# If last update of the node transaction is older than this threshold, indexing is considered stale.
# All operations of this indexer will fail.
INDEXING_STALE_THRESHOLD_SECONDS = 30
DAY_HORIZONT = 24 * 60 * 60


class event_add_object(SQLModel, table=True):
    __tablename__ = "event_add_object"
    id: str = Field(primary_key=True, index=True)
    user: str = Field(index=False)
    transaction_hash: str = Field(index=False)
    chain_id: int = Field(index=False)
    object_cid: str = Field(index=False)
    timestamp: int = Field(index=False)


class event_add_set_object(SQLModel, table=True):
    __tablename__ = "event_add_set_object"
    id: str = Field(primary_key=True, index=True)
    user: str = Field(index=False)
    set_cid: str = Field(index=False)
    object_cid: str = Field(index=False)
    chain_id: int = Field(index=False)
    transaction_hash: str = Field(index=False)
    timestamp: int = Field(index=False)


class event_add_set(SQLModel, table=True):
    __tablename__ = "event_add_set"
    id: str = Field(primary_key=True, index=True)
    user: str = Field(index=False)
    set_cid: str = Field(index=False)
    chain_id: int = Field(index=False)
    transaction_hash: str = Field(index=False)
    timestamp: int = Field(index=False)


class last_batch_processing_time(SQLModel, table=True):
    __tablename__ = "last_batch_processing_time"
    id: str = Field(primary_key=True, index=True)
    timestamp: int = Field(index=False)


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


class SQLIndexingService(IndexingService):
    """
    Indexing service based on chain indexing data from sql db.
    """

    def __init__(self, db_url: str, engine_kwargs: dict | None = None):
        if engine_kwargs is None:
            engine_kwargs = {}

        self.db_engine = create_engine(db_url, **engine_kwargs)

    def find_user_sets(self, user: str) -> List[dict]:
        """
        Find all sets for a user.
        """
        cs_receipts = []

        # lowercase the user to match the db
        user = user.lower()

        self._fail_if_indexing_stale()

        with Session(self.db_engine) as session:
            statement = (
                select(event_add_set)
                .where(event_add_set.user == user)
                .order_by(event_add_set.timestamp)
            )
            events = session.exec(statement).all()
            cs_receipts = [
                {
                    "chainId": int(event.chain_id),
                    "transactionHash": event.transaction_hash,
                    "user": event.user,
                    "setCid": event.set_cid,
                    "timestamp": self._format_timestamp(event.timestamp),
                }
                for event in events
            ]
        return cs_receipts

    def find_user_objects(self, user: str, return_set_cids=False) -> List[dict]:
        """
        find all event_add_object for a user.
        """

        # lowercase the user to match the db
        user = user.lower()

        self._fail_if_indexing_stale()

        cs_receipts = []
        with Session(self.db_engine) as session:
            statement = (
                select(event_add_object)
                .where(event_add_object.user == user)
                .order_by(event_add_object.timestamp)
            )
            events = session.exec(statement).all()
            cs_receipts = [
                {
                    "chainId": int(event.chain_id),
                    "transactionHash": event.transaction_hash,
                    "user": event.user,
                    "objectCid": event.object_cid,
                    "timestamp": self._format_timestamp(event.timestamp),
                }
                for event in events
            ]
        if return_set_cids and len(cs_receipts) > 0:
            cs_receipts = self._assign_set_cid(cs_receipts)
        return cs_receipts

    def find_user_set_objects(self, user: str, set_cid: str) -> List[dict]:
        """
        Find all objects for a user and set cid.
        """

        # lowercase to match the db
        user = user.lower()
        set_cid = set_cid.lower()

        self._fail_if_indexing_stale()

        cs_receipts = []
        with Session(self.db_engine) as session:
            statement = (
                select(event_add_set_object)
                .where(
                    event_add_set_object.user == user,
                    event_add_set_object.set_cid == set_cid,
                )
                .order_by(event_add_set_object.timestamp)
            )
            events = session.exec(statement).all()
            cs_receipts = [
                {
                    "chainId": int(event.chain_id),
                    "transactionHash": event.transaction_hash,
                    "user": event.user,
                    "setCid": event.set_cid,
                    "objectCid": event.object_cid,
                    "timestamp": self._format_timestamp(event.timestamp),
                }
                for event in events
            ]
        return cs_receipts

    def find_last_user_set_object(self, user: str, set_cid: str) -> Union[dict, None]:
        """
        Find the last object for a user and set cid.
        """

        # lowercase to match the db
        user = user.lower()
        set_cid = set_cid.lower()

        self._fail_if_indexing_stale()

        with Session(self.db_engine) as session:
            statement = (
                select(event_add_set_object)
                .where(
                    event_add_set_object.user == user,
                    event_add_set_object.set_cid == set_cid,
                )
                .order_by(event_add_set_object.timestamp.desc())
            )
            event = session.exec(statement).first()
            if event:
                return {
                    "chainId": int(event.chain_id),
                    "transactionHash": event.transaction_hash,
                    "user": event.user,
                    "setCid": event.set_cid,
                    "objectCid": event.object_cid,
                    "timestamp": self._format_timestamp(event.timestamp),
                }
        return None

    def find_objects(self, object_cids: List[str], return_set_cids=False) -> List[dict]:
        """
        Find all objects for a list of object cids.
        """

        # lowercase the object cids to match the db
        object_cids = [cid.lower() for cid in object_cids]

        self._fail_if_indexing_stale()

        cs_receipts = []
        with Session(self.db_engine) as session:
            statement = (
                select(event_add_object)
                .where(event_add_object.object_cid.in_(object_cids))
                .order_by(event_add_object.timestamp)
            )
            events = session.exec(statement).all()
            cs_receipts = [
                {
                    "chainId": int(event.chain_id),
                    "transactionHash": event.transaction_hash,
                    "user": event.user,
                    "objectCid": event.object_cid,
                    "timestamp": self._format_timestamp(event.timestamp),
                }
                for event in events
            ]
        if return_set_cids and len(cs_receipts) > 0:
            cs_receipts = self._assign_set_cid(cs_receipts)
        return cs_receipts

    def find_object(self, object_cid: str, return_set_cids=False) -> List[dict]:
        # Pass through to find_objects with a single object_cid.
        return self.find_objects([object_cid], return_set_cids)

    def find_last_object(
        self, object_cid: str, return_set_cid=False
    ) -> Union[dict, None]:
        """
        Find the last object for a list of object cids.
        """

        object_cid = object_cid.lower()

        cs_receipts = []

        self._fail_if_indexing_stale()

        with Session(self.db_engine) as session:
            statement = (
                select(event_add_object)
                .where(event_add_object.object_cid == object_cid)
                .order_by(event_add_object.timestamp.desc())
            )
            event = session.exec(statement).first()
            if event:
                cs_receipts = [
                    {
                        "chainId": int(event.chain_id),
                        "transactionHash": event.transaction_hash,
                        "user": event.user,
                        "objectCid": event.object_cid,
                        "timestamp": self._format_timestamp(event.timestamp),
                    }
                ]

        if return_set_cid and len(cs_receipts) > 0:
            cs_receipts = self._assign_set_cid(cs_receipts)

        if len(cs_receipts) > 0:
            return cs_receipts[0]
        else:
            return None

    def _fail_if_indexing_stale(self):
        """
        Checks the latest batch processing timestamp
        Raises an exception if the indexing is stale.
        """
        with Session(self.db_engine) as session:
            statement = select(last_batch_processing_time).order_by(
                last_batch_processing_time.timestamp.desc()
            )
            last_batch = session.exec(statement).first()
            if last_batch is None:
                raise Exception(
                    "No batch processing time found. Indexing might not have started."
                )

            current_time = pd.Timestamp.now(tz="UTC")
            last_time = pd.Timestamp(int(last_batch.timestamp), unit="ms", tz="UTC")
            if (
                current_time - last_time
            ).total_seconds() > INDEXING_STALE_THRESHOLD_SECONDS:
                raise Exception(
                    f"Indexing is stale. Last batch processing time: {last_time} by {last_batch.id}, current time: {current_time}. "
                    f"Stale threshold: {INDEXING_STALE_THRESHOLD_SECONDS} seconds."
                )

    def _format_timestamp(self, timestamp) -> str:
        return str(pd.Timestamp(int(timestamp), unit="ms", tz="UTC"))

    def _assign_set_cid(
        self, cs_receipts: List[dict[str, any]]
    ) -> List[dict[str, any]]:
        """
        Assign set cid to object cid in batches.
        """
        batch_size = 50
        object_cid_to_receipts = {}

        # Group receipts by objectCid
        for r in cs_receipts:
            object_cid_to_receipts.setdefault(r["objectCid"], []).append(r)

        object_cids = list(object_cid_to_receipts.keys())

        # Process in batches
        for i in range(0, len(object_cids), batch_size):
            batch_cids = object_cids[i : i + batch_size]
            with Session(self.db_engine) as session:
                statement = select(event_add_set_object).where(
                    event_add_set_object.object_cid.in_(batch_cids)
                )
                events = session.exec(statement).all()
                set_cids = {
                    (
                        event.object_cid,
                        event.transaction_hash,
                        event.chain_id,
                    ): event.set_cid
                    for event in events
                }

            for receipt in cs_receipts:
                key = (
                    receipt["objectCid"],
                    receipt["transactionHash"],
                    receipt["chainId"],
                )
                if key in set_cids:
                    receipt["setCid"] = set_cids[key]

        return cs_receipts

    def find_best_candidate(
        self,
        objects: list[ObjectAtTime],
        *,
        as_of: int | None = None,
        max_timestamp_diff: int = DAY_HORIZONT,
    ) -> list[SetCandidate]:
        """
        Matching semantics:
        - set_cid: exact
        - user: exact
        - object_cid: exact
        - timestamp: abs(diff) <= max_timestamp_diff
        """

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
        """"""
        return ts // 1000 if ts > 10_000_000_000 else ts
