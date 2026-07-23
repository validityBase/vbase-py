"""Tests for SQLIndexingService.

Covers:
- PostgreSQL driver importability (regression for missing psycopg2 dependency)
- Core query methods against an in-process SQLite database
- Stale-indexing and missing-heartbeat error paths
"""

import importlib
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

    If psycopg2-binary is removed from requirements/test.in this test will fail
    immediately and visibly rather than silently breaking in production.
    """

    def test_postgresql_driver_importable(self):
        """Ensure psycopg2 is installed so PostgreSQL URLs can be used."""
        try:
            importlib.import_module("psycopg2")
        except ImportError as exc:
            self.fail(
                f"psycopg2 driver is not installed — ensure psycopg2-binary is in "
                f"requirements/test.in: {exc}"
            )


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
        if os.path.exists(self._db_path):
            try:
                os.unlink(self._db_path)
            except OSError:
                pass

    # --- find_user_sets ---

    def test_find_user_sets_returns_matching_row(self):
        """Return a set row when the user has a matching EventAddSet record."""
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
        """Match sets regardless of the caller's user address casing."""
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
        """Return an empty list when the user has no sets."""
        self.assertEqual(self.svc.find_user_sets("0xunknown"), [])

    # --- find_user_set_objects ---

    def test_find_user_set_objects_returns_matching_row(self):
        """Return set-object rows for a known user and set CID."""
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
        """Return an empty list when the set CID is unknown."""
        self.assertEqual(self.svc.find_user_set_objects(_USER, "0xnosuchset"), [])

    # --- find_last_user_set_object ---

    def test_find_last_user_set_object_returns_most_recent(self):
        """Return the most recently timestamped object for a user set."""
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
        """Return None when the user set has no objects."""
        self.assertIsNone(self.svc.find_last_user_set_object(_USER, "0xnosuchset"))

    # --- find_user_objects ---

    def test_find_user_objects_returns_matching_row(self):
        """Return object rows for a user with EventAddObject records."""
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
        """Return an empty list when the user has no objects."""
        self.assertEqual(self.svc.find_user_objects("0xunknown"), [])

    # --- find_objects / find_object ---

    def test_find_objects_by_cid(self):
        """Return object rows when searching by object CID."""
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
        """Return an empty list when no objects match the CID."""
        self.assertEqual(self.svc.find_objects(["0xnosuchcid"]), [])

    def test_find_object_delegates_to_find_objects(self):
        """Return the same result as find_objects for a single CID."""
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
        """Return the most recently timestamped record for an object CID."""
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
        """Return None when no records exist for the object CID."""
        self.assertIsNone(self.svc.find_last_object("0xnosuchcid"))


class TestSQLIndexingServiceStaleDetection(unittest.TestCase):
    """Tests for _fail_if_indexing_stale."""

    def test_stale_timestamp_raises(self):
        """Raise RuntimeError when the last batch timestamp is too old."""
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
            if os.path.exists(db_path):
                try:
                    os.unlink(db_path)
                except OSError:
                    pass

    def test_no_batch_time_raises(self):
        """Raise RuntimeError when no LastBatchProcessingTime row exists."""
        db_path, db_url = _make_db()
        svc = SQLIndexingService(db_url=db_url, indexing_stale_threshold_seconds=60)
        try:
            with self.assertRaises(RuntimeError):
                svc.find_user_sets(_USER)
        finally:
            svc.db_engine.dispose()
            if os.path.exists(db_path):
                try:
                    os.unlink(db_path)
                except OSError:
                    pass


if __name__ == "__main__":
    unittest.main()
