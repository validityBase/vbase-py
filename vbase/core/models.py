"""SQL models for indexing events."""

from sqlmodel import Field, SQLModel


class EventAddObject(SQLModel, table=True):
    """ORM model for the event_add_object table, recording per-object commitment events."""

    __tablename__ = "event_add_object"
    id: str = Field(primary_key=True, index=True)
    user: str = Field(index=False)
    transaction_hash: str = Field(index=False)
    chain_id: int = Field(index=False)
    object_cid: str = Field(index=False)
    timestamp: int = Field(index=False)


class EventAddSetObject(SQLModel, table=True):
    """ORM model for the event_add_set_object table, linking objects to their containing sets."""

    __tablename__ = "event_add_set_object"
    id: str = Field(primary_key=True, index=True)
    user: str = Field(index=False)
    set_cid: str = Field(index=False)
    object_cid: str = Field(index=False)
    chain_id: int = Field(index=False)
    transaction_hash: str = Field(index=False)
    timestamp: int = Field(index=False)


class EventAddSet(SQLModel, table=True):
    """ORM model for the event_add_set table, recording set creation events."""

    __tablename__ = "event_add_set"
    id: str = Field(primary_key=True, index=True)
    user: str = Field(index=False)
    set_cid: str = Field(index=False)
    chain_id: int = Field(index=False)
    transaction_hash: str = Field(index=False)
    timestamp: int = Field(index=False)


class LastBatchProcessingTime(SQLModel, table=True):
    """ORM model for the last_batch_processing_time table, tracking indexer heartbeat."""

    __tablename__ = "last_batch_processing_time"
    id: str = Field(primary_key=True, index=True)
    timestamp: int = Field(index=False)
