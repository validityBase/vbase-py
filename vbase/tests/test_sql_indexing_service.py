"""Tests for SQLIndexingService.

Covers:
- PostgreSQL driver importability (regression for missing psycopg2 dependency)
- All public query methods against an in-process SQLite database
- Stale-indexing and missing-heartbeat error paths
"""

import os
import tempfile
import unittest

import pandas as pd
from sqlmodel import Session, SQLModel, create_engine

from vbase.core.models import (
    EventAddObject,
    EventAddSet,
    EventAddSetObject,
    LastBatchProcessingTime,
)
from vbase.core.sql_indexing_service import SQLIndexingService

_USER = "0xdeadbeef"
_SET_CID = "0xaaaa1111"
_OBJ_CID = "0xbbbb2222"
_TX_HASH = "0xcccc3333"
_CHAIN_ID = 1
_NOW_MS = int(pd.Timestamp.now(tz="UTC").timestamp() * 1000)


def _make_db(rows=None):
    """Return (path, url) for a fresh temp-file SQLite DB, pre-seeded with rows."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    url = f"sqlite:///{path}"
    engine = create_engine(url)
    SQLModel.metadata.create_all(engine)
    if rows:
        with Session(engine) as session:
            for row in rows:
                session.add(row)
            session.commit()
    engine.dispose()
    return path, url


def _seed(url, rows):
    engine = create_engine(url)
    with Session(engine) as session:
        for row in rows:
            session.add(row)
        session.commit()
    engine.dispose()


class TestPostgresDriverImportable(unittest.TestCase):
    """Regression test: psycopg2 must be installed.

    SQLAlchemy imports the dialect driver inside create_engine() — before any connection
    is made.  If psycopg2-binary is removed from requirements.in this test will fail
    immediately and visibly rather than silently breaking in production.
    """

    def test_postgresql_driver_importable(self):
        try:
            svc = SQLIndexingService(db_url="postgresql://fake/db")
            svc.db_engine.dispose()
        except ModuleNotFoundError as exc:
            self.fail(
                f"psycopg2 driver is not installed — ensure psycopg2-binary is in "
                f"requirements.in: {exc}"
            )
        except Exception:
            # Any other error (OperationalError, etc.) is fine — no connection is
            # attempted here, but some SQLAlchemy versions may raise on engine creation
            # for other reasons.  We only care that the driver can be imported.
            pass


class TestSQLIndexingServiceQueries(unittest.TestCase):
    """Unit tests for query methods using an in-process SQLite database."""

    def setUp(self):
        self._db_path, self._db_url = _make_db(
            rows=[LastBatchProcessingTime(id="heartbeat", timestamp=_NOW_MS)]
        )
        self.svc = SQLIndexingService(
            db_url=self._db_url,
            indexing_stale_threshold_seconds=86400,
        )

    def tearDown(self):
        self.svc.db_engine.dispose()
        os.unlink(self._db_path)

    # --- find_user_sets ---

    def test_find_user_sets_returns_matching_row(self):
        _seed(
            self._db_url,
            [
                EventAddSet(
                    id="s1",
                    user=_USER,
                    set_cid=_SET_CID,
                    chain_id=_CHAIN_ID,
                    transaction_hash=_TX_HASH,
                    timestamp=_NOW_MS,
                )
            ],
        )
        results = self.svc.find_user_sets(_USER)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["setCid"], _SET_CID)
        self.assertEqual(results[0]["user"], _USER)
        self.assertEqual(results[0]["chainId"], _CHAIN_ID)

    def test_find_user_sets_lowercases_user(self):
        _seed(
            self._db_url,
            [
                EventAddSet(
                    id="s2",
                    user=_USER.lower(),
                    set_cid=_SET_CID,
                    chain_id=_CHAIN_ID,
                    transaction_hash=_TX_HASH,
                    timestamp=_NOW_MS,
                )
            ],
        )
        results = self.svc.find_user_sets(_USER.upper())
        self.assertEqual(len(results), 1)

    def test_find_user_sets_empty_for_unknown_user(self):
        self.assertEqual(self.svc.find_user_sets("0xunknown"), [])

    # --- find_user_set_objects ---

    def test_find_user_set_objects_returns_matching_row(self):
        _seed(
            self._db_url,
            [
                EventAddSetObject(
                    id="so1",
                    user=_USER,
                    set_cid=_SET_CID,
                    object_cid=_OBJ_CID,
                    chain_id=_CHAIN_ID,
                    transaction_hash=_TX_HASH,
                    timestamp=_NOW_MS,
                )
            ],
        )
        results = self.svc.find_user_set_objects(_USER, _SET_CID)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["objectCid"], _OBJ_CID)
        self.assertEqual(results[0]["setCid"], _SET_CID)

    def test_find_user_set_objects_empty_for_unknown_set(self):
        self.assertEqual(self.svc.find_user_set_objects(_USER, "0xnosuchset"), [])

    # --- find_last_user_set_object ---

    def test_find_last_user_set_object_returns_most_recent(self):
        _seed(
            self._db_url,
            [
                EventAddSetObject(
                    id="so2a",
                    user=_USER,
                    set_cid=_SET_CID,
                    object_cid="0xobj_old",
                    chain_id=_CHAIN_ID,
                    transaction_hash=_TX_HASH,
                    timestamp=_NOW_MS - 1000,
                ),
                EventAddSetObject(
                    id="so2b",
                    user=_USER,
                    set_cid=_SET_CID,
                    object_cid="0xobj_new",
                    chain_id=_CHAIN_ID,
                    transaction_hash=_TX_HASH,
                    timestamp=_NOW_MS,
                ),
            ],
        )
        result = self.svc.find_last_user_set_object(_USER, _SET_CID)
        self.assertIsNotNone(result)
        self.assertEqual(result["objectCid"], "0xobj_new")

    def test_find_last_user_set_object_none_when_empty(self):
        self.assertIsNone(self.svc.find_last_user_set_object(_USER, "0xnosuchset"))

    # --- find_user_objects ---

    def test_find_user_objects_returns_matching_row(self):
        _seed(
            self._db_url,
            [
                EventAddObject(
                    id="o1",
                    user=_USER,
                    object_cid=_OBJ_CID,
                    chain_id=_CHAIN_ID,
                    transaction_hash=_TX_HASH,
                    timestamp=_NOW_MS,
                )
            ],
        )
        results = self.svc.find_user_objects(_USER)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["objectCid"], _OBJ_CID)
        self.assertEqual(results[0]["user"], _USER)

    def test_find_user_objects_empty_for_unknown_user(self):
        self.assertEqual(self.svc.find_user_objects("0xunknown"), [])

    # --- find_objects / find_object ---

    def test_find_objects_by_cid(self):
        _seed(
            self._db_url,
            [
                EventAddObject(
                    id="o2",
                    user=_USER,
                    object_cid=_OBJ_CID,
                    chain_id=_CHAIN_ID,
                    transaction_hash=_TX_HASH,
                    timestamp=_NOW_MS,
                )
            ],
        )
        results = self.svc.find_objects([_OBJ_CID])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["objectCid"], _OBJ_CID)

    def test_find_objects_empty_for_unknown_cid(self):
        self.assertEqual(self.svc.find_objects(["0xnosuchcid"]), [])

    def test_find_object_delegates_to_find_objects(self):
        _seed(
            self._db_url,
            [
                EventAddObject(
                    id="o3",
                    user=_USER,
                    object_cid=_OBJ_CID,
                    chain_id=_CHAIN_ID,
                    transaction_hash=_TX_HASH,
                    timestamp=_NOW_MS,
                )
            ],
        )
        self.assertEqual(
            self.svc.find_object(_OBJ_CID), self.svc.find_objects([_OBJ_CID])
        )

    # --- find_last_object ---

    def test_find_last_object_returns_most_recent(self):
        _seed(
            self._db_url,
            [
                EventAddObject(
                    id="o4a",
                    user=_USER,
                    object_cid=_OBJ_CID,
                    chain_id=_CHAIN_ID,
                    transaction_hash="0xtx_old",
                    timestamp=_NOW_MS - 1000,
                ),
                EventAddObject(
                    id="o4b",
                    user=_USER,
                    object_cid=_OBJ_CID,
                    chain_id=_CHAIN_ID,
                    transaction_hash="0xtx_new",
                    timestamp=_NOW_MS,
                ),
            ],
        )
        result = self.svc.find_last_object(_OBJ_CID)
        self.assertIsNotNone(result)
        self.assertEqual(result["transactionHash"], "0xtx_new")

    def test_find_last_object_none_when_empty(self):
        self.assertIsNone(self.svc.find_last_object("0xnosuchcid"))


class TestSQLIndexingServiceStaleDetection(unittest.TestCase):
    """Tests for _fail_if_indexing_stale."""

    def test_stale_timestamp_raises(self):
        stale_ms = int(pd.Timestamp("2000-01-01", tz="UTC").timestamp() * 1000)
        db_path, db_url = _make_db(
            rows=[LastBatchProcessingTime(id="heartbeat", timestamp=stale_ms)]
        )
        svc = SQLIndexingService(db_url=db_url, indexing_stale_threshold_seconds=60)
        try:
            with self.assertRaises(RuntimeError):
                svc.find_user_sets(_USER)
        finally:
            svc.db_engine.dispose()
            os.unlink(db_path)

    def test_no_batch_time_raises(self):
        db_path, db_url = _make_db()
        svc = SQLIndexingService(db_url=db_url, indexing_stale_threshold_seconds=60)
        try:
            with self.assertRaises(RuntimeError):
                svc.find_user_sets(_USER)
        finally:
            svc.db_engine.dispose()
            os.unlink(db_path)


if __name__ == "__main__":
    unittest.main()
