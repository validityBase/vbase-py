"""Tests of the vbase_client module
"""

import unittest
from typing import List

import pandas as pd

from vbase.core.vbase_client_test import VBaseClientTest
from vbase.tests.utils import TEST_HASH1, TEST_HASH2, int_to_hash


def _assert_commitment_receipt_fields(cl: dict, expect_set_cid: bool = False):
    """Assert commitment receipt has userAddress, chainId; optionally setCid (E2E)."""
    user_addr = cl.get("userAddress") or cl.get("user")
    assert user_addr, "commitment receipt must have userAddress or user (non-null)"
    assert isinstance(user_addr, str) and len(user_addr) > 0, (
        "userAddress must be non-empty string"
    )
    chain_id = cl.get("chainId") or cl.get("chain_id")
    assert chain_id is not None, "commitment receipt must have chainId"
    assert isinstance(chain_id, int), "chainId must be an int"
    if expect_set_cid:
        set_cid = cl.get("setCid") or cl.get("set_cid")
        assert set_cid is not None, "commitment receipt must have setCid for set object"
        assert isinstance(set_cid, str) and len(set_cid) > 0, (
            "setCid must be non-empty string"
        )


class TestVBaseClient(unittest.TestCase):
    """Test base vBase client functionality."""

    def setUp(self):
        """Set up the tests."""
        self.vbc = VBaseClientTest.create_instance_from_env()
        self.vbc.clear_set_objects(TEST_HASH1)
        self.vbc.clear_sets()
        cl = self.vbc.add_set(TEST_HASH1)
        assert cl["setCid"] == TEST_HASH1

    def test_add_set(self):
        """Test a simple set commitment."""
        self.vbc.clear_sets()
        cl = self.vbc.add_set(TEST_HASH1)
        assert self.vbc.user_set_exists(cl["user"], TEST_HASH1)
        assert not self.vbc.user_set_exists(cl["user"], TEST_HASH2)
        assert self.vbc.verify_user_sets(cl["user"], TEST_HASH1)
        # Check bogus sets.
        assert not self.vbc.verify_user_sets(cl["user"], TEST_HASH2)

    def test_add_named_set(self):
        """Test a simple named set commitment."""
        self.vbc.clear_sets()
        cl = self.vbc.add_named_set("Test Set")
        assert self.vbc.user_named_set_exists(cl["user"], "Test Set")
        assert not self.vbc.user_named_set_exists(cl["user"], "Test Set 2")
        assert self.vbc.verify_user_sets(
            cl["user"], self.vbc.get_named_set_cid("Test Set")
        )
        assert not self.vbc.verify_user_sets(cl["user"], TEST_HASH2)

    def test_add_object(self):
        """Test a simple object commitment."""
        cl = self.vbc.add_object(TEST_HASH1)
        _assert_commitment_receipt_fields(cl, expect_set_cid=False)
        assert self.vbc.verify_user_object(cl["user"], cl["objectCid"], cl["timestamp"])
        # Check bogus timestamp.
        assert not self.vbc.verify_user_object(
            cl["user"],
            cl["objectCid"],
            str(pd.Timestamp(cl["timestamp"]) + pd.Timedelta(hours=1)),
        )
        # Check bogus hash.
        assert not self.vbc.verify_user_object(cl["user"], TEST_HASH2, cl["timestamp"])

    def test_add_set_object(self):
        """Test a simple set object commitment."""
        cl = self.vbc.add_set_object(TEST_HASH1, TEST_HASH2)
        _assert_commitment_receipt_fields(cl, expect_set_cid=True)
        assert self.vbc.verify_user_object(cl["user"], cl["objectCid"], cl["timestamp"])
        assert self.vbc.verify_user_set_objects(cl["user"], TEST_HASH1, TEST_HASH2)
        # Check bogus hashes.
        assert not self.vbc.verify_user_set_objects(cl["user"], TEST_HASH2, TEST_HASH2)
        assert not self.vbc.verify_user_set_objects(cl["user"], TEST_HASH2, TEST_HASH1)

    def test_add_sets_objects_batch(self):
        """Test a batch set object commitment."""
        object_hashes = [int_to_hash(i) for i in range(1, 5)]
        cl = self.vbc.add_sets_objects_batch([TEST_HASH1] * 4, object_hashes)
        assert len(cl) == 4
        for i in range(0, 4):
            assert self.vbc.verify_user_object(
                cl[i]["user"], object_hashes[i], cl[i]["timestamp"]
            )
            assert not self.vbc.verify_user_object(
                cl[i]["user"], int_to_hash(i + 100), cl[i]["timestamp"]
            )
            assert not self.vbc.verify_user_object(
                cl[i]["user"],
                int_to_hash(i + 100),
                pd.Timestamp(cl[i]["timestamp"]) - pd.Timedelta(seconds=1),
            )
            assert self.vbc.verify_user_set_objects(
                cl[i]["user"], TEST_HASH1, int_to_hash(10)
            )
            assert not self.vbc.verify_user_set_objects(
                cl[i]["user"], TEST_HASH1, int_to_hash(10 + 1)
            )

    def test_add_set_object_with_timestamp(self):
        """Test a simple set object commitment with timestamp."""
        cl = self.vbc.add_set_object_with_timestamp(
            TEST_HASH1, TEST_HASH2, pd.Timestamp("2023-01-01")
        )
        _assert_commitment_receipt_fields(cl, expect_set_cid=True)
        assert self.vbc.verify_user_object(
            cl["user"], TEST_HASH2, pd.Timestamp("2023-01-01")
        )
        assert not self.vbc.verify_user_object(
            cl["user"], TEST_HASH2, pd.Timestamp("2023-01-02")
        )
        assert self.vbc.verify_user_set_objects(cl["user"], TEST_HASH1, TEST_HASH2)
        assert not self.vbc.verify_user_set_objects(cl["user"], TEST_HASH2, TEST_HASH2)
        assert not self.vbc.verify_user_set_objects(cl["user"], TEST_HASH2, TEST_HASH1)

    def _verify_sets_objects_batch(
        self, cl: dict, object_hashes: List[str], timestamps: List[str]
    ):
        assert len(cl) == 4
        for i in range(0, 4):
            assert self.vbc.verify_user_object(
                cl[i]["user"], object_hashes[i], timestamps[i]
            )
            assert not self.vbc.verify_user_object(
                cl[i]["user"],
                object_hashes[i],
                str(pd.Timestamp(timestamps[i]) + pd.DateOffset(hours=1)),
            )
            assert not self.vbc.verify_user_object(
                cl[i]["user"], int_to_hash(i + 2), timestamps[i]
            )
            assert self.vbc.verify_user_set_objects(
                cl[i]["user"], TEST_HASH1, int_to_hash(10)
            )
            assert not self.vbc.verify_user_set_objects(
                cl[i]["user"], TEST_HASH1, int_to_hash(10 + 1)
            )

    def test_add_sets_objects_with_timestamps_batch(self):
        """Test a batch sets objects commitment with timestamps."""
        # Use an internal function to quickly build timestamps.
        # noinspection PyUnresolvedReferences
        timestamps = [
            self.vbc.commitment_service.convert_timestamp_chain_to_str(ts)
            for ts in range(1, 5)
        ]
        object_hashes = [int_to_hash(i) for i in range(1, 5)]
        cl = self.vbc.add_sets_objects_with_timestamps_batch(
            [TEST_HASH1] * 4, object_hashes, timestamps
        )
        self._verify_sets_objects_batch(cl, object_hashes, timestamps)

    def test_add_set_objects_with_timestamps_batch(self):
        """Test a batch set objects commitment with timestamps."""
        # Use an internal function to quickly build timestamps.
        # noinspection PyUnresolvedReferences
        timestamps = [
            self.vbc.commitment_service.convert_timestamp_chain_to_str(ts)
            for ts in range(1, 5)
        ]
        object_hashes = [int_to_hash(i) for i in range(1, 5)]
        cl = self.vbc.add_set_objects_with_timestamps_batch(
            TEST_HASH1, object_hashes, timestamps
        )
        self._verify_sets_objects_batch(cl, object_hashes, timestamps)


if __name__ == "__main__":
    unittest.main()
