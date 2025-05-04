# flake8: noqa

from typing import List, Union
from vbase. core.indexing_service import IndexingService
from sqlmodel import Field, SQLModel, Session, create_engine, select
import pandas as pd

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


class SubsquidIndexingService(IndexingService):
    """
    Indexing service based on chain indexing data collected by Subsquid.
    """
    def __init__(self, db_url: str):
        # open connection to db
        self.db_engine = create_engine(db_url)

    def find_user_sets(self, user: str) -> List[dict]:
        """
        Find all sets for a user.
        """
        with Session(self.db_engine) as session:
            statement = select(event_add_set).where(event_add_set.user == user).order_by(event_add_set.timestamp)
            events = session.exec(statement).all()
            cs_receipts = [
                {
                    "chainId": int(event.chain_id),
                    "transactionHash": event.transaction_hash,
                    "user": event.user,
                    "setCid": event.set_cid,
                    "timestamp": self._format_timestamp(event.timestamp)
                }
                for event in events
            ]
        return cs_receipts

    def find_user_objects(self, user: str, return_set_cids=False) -> List[dict]:
        """
        find all event_add_object for a user.
        """

        with Session(self.db_engine) as session:
            statement = select(event_add_object).where(event_add_object.user == user).order_by(event_add_object.timestamp)
            events = session.exec(statement).all()
            cs_receipts = [
                {
                    "chainId": int(event.chain_id),
                    "transactionHash": event.transaction_hash,
                    "user": event.user,
                    "objectCid": event.object_cid,
                    "timestamp": self._format_timestamp(event.timestamp)
                }
                for event in events
            ]
        if return_set_cids:
            cs_receipts = self._assign_set_cid(cs_receipts)
        return cs_receipts

    def find_user_set_objects(self, user: str, set_cid: str) -> List[dict]:
        """
        Find all objects for a user and set cid.
        """
        with Session(self.db_engine) as session:
            statement = select(event_add_set_object).where(
                event_add_set_object.user == user,
                event_add_set_object.set_cid == set_cid
            ).order_by(event_add_set_object.timestamp)
            events = session.exec(statement).all()
            cs_receipts = [
                {
                    "chainId": int(event.chain_id),
                    "transactionHash": event.transaction_hash,
                    "user": event.user,
                    "setCid": event.set_cid,
                    "objectCid": event.object_cid,
                    "timestamp": self._format_timestamp(event.timestamp)
                }
                for event in events
            ]
        return cs_receipts

    def find_last_user_set_object(self, user: str, set_cid: str) -> Union[dict, None]:
        """
        Find the last object for a user and set cid.
        """
        with Session(self.db_engine) as session:
            statement = select(event_add_set_object).where(
                event_add_set_object.user == user,
                event_add_set_object.set_cid == set_cid
            ).order_by(event_add_set_object.timestamp.desc())
            event = session.exec(statement).first()
            if event:
                return {
                    "chainId": int(event.chain_id),
                    "transactionHash": event.transaction_hash,
                    "user": event.user,
                    "setCid": event.set_cid,
                    "objectCid": event.object_cid,
                    "timestamp": self._format_timestamp(event.timestamp)
                }
        return None
    
    def find_objects(self, object_cids: List[str], return_set_cids=False) -> List[dict]:
        """
        Find all objects for a list of object cids.
        """
        with Session(self.db_engine) as session:
            statement = select(event_add_object).where(event_add_object.object_cid.in_(object_cids)).order_by(event_add_object.timestamp)
            events = session.exec(statement).all()
            cs_receipts = [
                {
                    "chainId": int(event.chain_id),
                    "transactionHash": event.transaction_hash,
                    "user": event.user,
                    "objectCid": event.object_cid,
                    "timestamp": self._format_timestamp(event.timestamp)
                }
                for event in events
            ]
        if return_set_cids:
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
        with Session(self.db_engine) as session:
            statement = select(event_add_object).where(event_add_object.object_cid == object_cid).order_by(event_add_object.timestamp.desc())
            event = session.exec(statement).first()
            if event:
                cs_receipts = [{
                    "chainId": event.chain_id,
                    "transactionHash": event.transaction_hash,
                    "user": event.user,
                    "objectCid": event.object_cid,
                    "timestamp": self._format_timestamp(event.timestamp)
                }]

        if return_set_cid:
            cs_receipts = self._assign_set_cid(cs_receipts)
            if cs_receipts:
                return cs_receipts[0]
            else:
                return None

    def _format_timestamp(self, timestamp) -> str:
        return str(pd.Timestamp(int(timestamp), unit="ms", tz="UTC"))

    def _assign_set_cid(self, cs_receipts: List[dict[str, any]]) -> List[dict[str, any]]:
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
            batch_cids = object_cids[i:i + batch_size]
            with Session(self.db_engine) as session:
                statement = select(event_add_set_object).where(
                    event_add_set_object.object_cid.in_(batch_cids)
                )
                events = session.exec(statement).all()
                set_cids = {
                    (event.object_cid, event.transaction_hash, event.chain_id): event.set_cid
                    for event in events
                }

            for receipt in cs_receipts:
                key = (receipt["objectCid"], receipt["transactionHash"], receipt["chainId"])
                if key in set_cids:
                    receipt["setCid"] = set_cids[key]

        return cs_receipts

