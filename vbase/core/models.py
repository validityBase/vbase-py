# flake8: noqa

from sqlmodel import Field, SQLModel


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
