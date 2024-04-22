"""
Tests of the indexing service for the vbase package
Tests rely on MongoDB for name resolution and data availability.
"""

from typing import cast
import secrets
import unittest

from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService
from vbase.core.indexing_service import Web3HTTPIndexingService

from vbase.tests.utils import (
    init_vbase_client_test_from_mongo,
    int_to_hash,
    TEST_HASH1,
    TEST_HASH2,
    compare_dict_subset,
)


class TestIndexingService(unittest.TestCase):
    """
    Test base vBase indexing functionality using a local test node.
    """

    def setUp(self):
        """
        Set up the tests.
        """
        # Subclasses may initialize vbase client objects themselves,
        # for instance when testing on a public testnet.
        if not hasattr(self, "vbc"):
            self.vbc = init_vbase_client_test_from_mongo()
            self.indexing_service = Web3HTTPIndexingService(
                [cast(Web3HTTPCommitmentService, self.vbc.commitment_service)]
            )
        self.assertTrue(self.indexing_service is not None)
        self.vbc.clear_sets()
        self.vbc.clear_set_objects(TEST_HASH1)
        cl = self.vbc.add_set(TEST_HASH1)
        assert cl["setCid"] == TEST_HASH1

    def test_add_set_object_indexing(self):
        """
        Test a simple set object commitment.
        """
        cl = self.vbc.add_set_object(set_cid=TEST_HASH1, object_cid=TEST_HASH2)
        user = cl["user"]
        commitment_receipts = self.indexing_service.find_user_set_objects(
            user=user, set_cid=TEST_HASH1
        )
        # The node may run multiple tests accumulating multiple events.
        # Validate the tail.
        assert compare_dict_subset(
            commitment_receipts[-1],
            {
                "user": user,
                "setCid": TEST_HASH1,
                "objectCid": TEST_HASH2,
                "timestamp": cl["timestamp"],
            },
        )

    def test_add_set_objects_indexing(self):
        """
        Test a series of simple set object commitments.
        """
        # Use a random set CID to avoid collisions with other tests.
        set_cid = "0x" + secrets.token_bytes(32).hex()
        for i in range(1, 6):
            cl = self.vbc.add_set_object_with_timestamp(
                set_cid=set_cid,
                object_cid=int_to_hash(i),
                timestamp=self.vbc.commitment_service.convert_timestamp_chain_to_str(i),
            )
        user = cl["user"]
        commitment_receipts = self.indexing_service.find_user_set_objects(
            user=user, set_cid=set_cid
        )
        assert compare_dict_subset(
            commitment_receipts[-5],
            {
                "user": user,
                "setCid": set_cid,
                "objectCid": int_to_hash(1),
                "timestamp": self.vbc.commitment_service.convert_timestamp_chain_to_str(
                    1
                ),
            },
        )
        assert compare_dict_subset(
            commitment_receipts[-1],
            {
                "user": user,
                "setCid": set_cid,
                "objectCid": int_to_hash(5),
                "timestamp": self.vbc.commitment_service.convert_timestamp_chain_to_str(
                    5
                ),
            },
        )

    def test_add_object_find_objects(self):
        """
        Test a simple object commitment following by find_objects().
        """
        cl = self.vbc.add_object(object_cid=TEST_HASH2)
        user = cl["user"]
        commitment_receipts = self.indexing_service.find_objects(object_cid=TEST_HASH2)
        # The node may run multiple tests accumulating multiple events.
        # Validate the tail.
        assert compare_dict_subset(
            commitment_receipts[-1],
            {
                "user": user,
                "objectCid": TEST_HASH2,
                "timestamp": cl["timestamp"],
            },
        )
