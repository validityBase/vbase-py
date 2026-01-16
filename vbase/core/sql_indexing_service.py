# flake8: noqa

from collections import Counter
from dataclasses import dataclass
from typing import List, Union

import pandas as pd
from sqlalchemy import func
from sqlmodel import Field, Session, SQLModel, create_engine, select

from vbase.core.indexing_service import IndexingService

# If last update of the node transaction is older than this threshold, indexing is considered stale.
# All operations of this indexer will fail.
INDEXING_STALE_THRESHOLD_SECONDS = 30


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


class SQLIndexingService(IndexingService):
    """
    Indexing service based on chain indexing data from sql db.
    """

    def __init__(self, db_url: str):
        # open connection to db
        self.db_engine = create_engine(db_url)

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


@dataclass(frozen=True)
class SetMatch:
    """
    Set match result structure.
    """

    score: float
    created_at: int
    set_cid: str
    user: str


class SqlSetMatchingService:
    """
    Finds best-matching collections for a given list of object CIDs
    """

    def __init__(self, engine):
        self.engine = engine

    def find_best_candidate(self, object_cids: list[str]) -> list[SetMatch]:
        """
        Given a list of object_cids, find best-matching sets.

        Algorithm:
        1. Probe DB by object_cid to discover candidate set_cids
        2. Count how many query objects matched each set
        3. For candidate sets only, load total object count + metadata
        4. Compute score and rank results
        """

        object_cids = list(set(object_cids))

        with Session(self.engine) as session:

            # ------------------------------------------------------------
            # PHASE 1: PROBE
            #
            # Discover candidate of collections by probing on object_cid.
            # We only need set_cid
            #
            # Native SQL:
            # SELECT set_cid
            # FROM event_add_set_object
            # WHERE object_cid IN (:object_cids);
            # ------------------------------------------------------------
            probe_stmt = select(event_add_set_object.set_cid).where(
                event_add_set_object.object_cid.in_(object_cids)
            )

            probe_rows = session.exec(probe_stmt).all()

            # If no collections share any objects with the query, exit early
            if not probe_rows:
                return []

            # ------------------------------------------------------------
            # PHASE 2: COUNT INTERSECTIONS
            #
            # Count how many query objects matched each set_cid.
            # This is |query ∩ collection|.
            #
            # Example result:
            # { "setA": 3, "setB": 1 }
            # ------------------------------------------------------------
            matched = Counter(probe_rows)

            # ------------------------------------------------------------
            # PHASE 3: AGGREGATE COLLECTION METADATA
            #
            # For candidate sets only:
            # - total number of objects in the collection
            # - earliest timestamp will be used just for ordering
            # - user the user_address who created the collection
            # Group by set_cid and user to handle same set_cid created by different users.
            # other columns should appear in aggregate functions.(total, min)
            # Native SQL:
            # SELECT
            #   set_cid,
            #   COUNT(*) AS total,
            #   MIN(timestamp) AS ts,
            #   "user"
            # FROM event_add_set_object
            # WHERE set_cid IN (:candidate_set_cids)
            # GROUP BY set_cid, "user";
            # ------------------------------------------------------------
            agg_stmt = (
                select(
                    event_add_set_object.set_cid,
                    func.count().label("total"),
                    func.min(event_add_set_object.timestamp).label("ts"),
                    event_add_set_object.user,
                )
                .where(event_add_set_object.set_cid.in_(matched.keys()))
                .group_by(
                    event_add_set_object.set_cid,
                    event_add_set_object.user,
                )
            )

            rows = session.exec(agg_stmt).all()

            # ------------------------------------------------------------
            # PHASE 4: SCORE + SORT
            #
            # score = matched_objects / total_objects
            #
            # Sort order:
            # 1. score DESC  (best match first)
            # 2. timestamp ASC (earliest collection first)
            # ------------------------------------------------------------
            results: list[SetMatch] = []

            for r in rows:
                score = matched[r.set_cid] / r.total
                results.append(
                    SetMatch(
                        set_cid=r.set_cid,
                        user=r.user,
                        score=score,
                        created_at=r.ts,
                    )
                )

            results.sort(key=lambda m: (-m.score, m.created_at))
            return results
