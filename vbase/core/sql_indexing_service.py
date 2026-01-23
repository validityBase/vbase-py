"""SQL indexing service implementation."""

from typing import Any, List, Union

import pandas as pd
from sqlmodel import Session, create_engine, select

from vbase.core.indexing_service import IndexingService

from .models import (
    event_add_object,
    event_add_set,
    event_add_set_object,
    last_batch_processing_time,
)
from .set_matching_service.set_matching_service import (
    BaseMatchingService,
    SetMatchingService,
)
from .types import ObjectAtTime, SetCandidate, SetMatchingCriteria

# If last update of the node transaction is older than this threshold, indexing is considered stale.
# All operations of this indexer will fail.
INDEXING_STALE_THRESHOLD_SECONDS = 30


class SQLIndexingService(IndexingService):
    """
    Indexing service based on chain indexing data from sql db.
    """

    def __init__(self, db_url: str, matching_strategy: BaseMatchingService = None):
        self.db_engine = create_engine(db_url)
        self.best_match_strategy = matching_strategy or SetMatchingService(
            self.db_engine
        )

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

    def find_matching_user_sets(
        self,
        objects: list[ObjectAtTime],
        as_of: pd.Timestamp | int | None = None,
    ) -> list[SetCandidate]:
        criteria = SetMatchingCriteria(
            objects=objects,
            as_of=as_of,
        )
        return self.best_match_strategy.find_matching_user_sets(criteria)
