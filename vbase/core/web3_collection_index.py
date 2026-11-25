"""Persistent collection-only index of finalized Web3 logs."""

from collections.abc import Callable
from pathlib import Path

from sqlalchemy import (
    URL,
    Boolean,
    Column,
    Connection,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    inspect,
    select,
)
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.pool import NullPool

from vbase.core.collection_cid_page import CollectionCidPage, read_cid_page
from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService

_schema = MetaData()
_sources = Table(
    "vbase_collection_sources",
    _schema,
    Column("source", String, primary_key=True),
    Column("height", Integer, nullable=False),
    Column("block_hash", String),
    Column("ready", Boolean, nullable=False),
)
_cids = Table(
    "vbase_collection_cids",
    _schema,
    Column("user", String, primary_key=True),
    Column("cid", String, primary_key=True),
)


class Web3CollectionIndex:
    """A file belongs to one fixed set of chain IDs and contract addresses."""

    def __init__(self, path: str | Path, services: list[Web3HTTPCommitmentService]):
        self.path = Path(path).resolve()
        self.services = {
            f"{cs.w3.eth.chain_id}:{cs.commitment_service_address.lower()}": cs
            for cs in services
        }
        if not self.services:
            raise ValueError("A collection index requires at least one Web3 source")
        self.engine = create_engine(
            URL.create("sqlite", database=str(self.path)), poolclass=NullPool
        )

    def _check_sources(self, conn: Connection) -> bool:
        rows = dict(
            conn.execute(select(_sources.c.source, _sources.c.ready)).tuples().all()
        )
        if rows.keys() != self.services.keys():
            raise ValueError(
                "Collection index sources do not match the configured sources"
            )
        return all(rows.values())

    def page(self, user: str, limit: int, offset: int) -> CollectionCidPage:
        """Read one committed view; never contact the node or create a missing index."""
        if not self.path.is_file():
            raise RuntimeError("Collection index not ready: run sync_collection_cids")
        with self.engine.connect() as conn:
            conn.exec_driver_sql("BEGIN")
            if not inspect(conn).has_table(_sources.name) or not self._check_sources(
                conn
            ):
                raise RuntimeError(
                    "Collection index not ready: run sync_collection_cids"
                )
            query = select(_cids.c.cid).where(_cids.c.user == user.lower())
            return read_cid_page(conn, query, _cids.c.cid, limit, offset)

    def sync(self, fetch: Callable, batch_size: int) -> None:
        """Commit each range and its checkpoint together, including empty ranges."""
        if (
            not isinstance(batch_size, int)
            or isinstance(batch_size, bool)
            or batch_size <= 0
        ):
            raise ValueError("block_batch_size must be a positive integer")
        with self.engine.begin() as conn:
            conn.exec_driver_sql("BEGIN IMMEDIATE")
            if not inspect(conn).has_table(_sources.name):
                _schema.create_all(conn)
                conn.execute(
                    _sources.insert(),
                    [
                        {"source": key, "height": -1, "ready": False}
                        for key in self.services
                    ],
                )
            self._check_sources(conn)
        for key, cs in self.services.items():
            self._sync_source(key, cs, fetch, batch_size)

    def _sync_source(
        self, key: str, cs: Web3HTTPCommitmentService, fetch: Callable, batch_size: int
    ) -> None:
        source = _sources.c.source == key
        with self.engine.connect() as conn:
            height, saved_hash = conn.execute(
                select(_sources.c.height, _sources.c.block_hash).where(source)
            ).one()
        target = cs.w3.eth.get_block("finalized")["number"]
        if target < height or (
            height >= 0 and cs.w3.eth.get_block(height)["hash"].hex() != saved_hash
        ):
            raise RuntimeError("Node does not match the saved finalized checkpoint")
        while height < target:
            end = min(height + batch_size, target)
            block_hash = cs.w3.eth.get_block(end)["hash"].hex()
            events = fetch(
                cs.csc.events.AddSetObject,
                from_block=height + 1,
                to_block=end,
                argument_filters={},
            )
            rows = [
                {
                    "user": e["args"]["user"].lower(),
                    "cid": "0x" + bytes(e["args"]["setCid"]).hex(),
                }
                for e in events
            ]
            with self.engine.begin() as conn:
                result = conn.execute(
                    _sources.update()
                    .where(source, _sources.c.height == height)
                    .values(
                        height=end,
                        block_hash=block_hash,
                        **({"ready": True} if end == target else {}),
                    )
                )
                if result.rowcount != 1:
                    raise RuntimeError("Concurrent collection sync; retry")
                if rows:
                    conn.execute(insert(_cids).on_conflict_do_nothing(), rows)
            height = end
