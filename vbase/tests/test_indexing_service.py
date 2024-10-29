"""
Tests of the indexing service for the vbase package
"""

from typing import cast
import secrets
import unittest

from vbase.core.vbase_client_test import VBaseClientTest
from vbase.core.indexing_service import IndexingService

from vbase.tests.utils import (
    int_to_hash,
    TEST_HASH1,
    TEST_HASH2,
    compare_dict_subset,
)


class TestIndexingService(unittest.TestCase):
    """
    Test base vBase indexing functionality.
    """

    def setUp(self):
        """
        Set up the tests.
        """
        # Subclasses may initialize vbase client objects themselves,
        # for instance when testing on a public testnet.
        if not hasattr(self, "vbc"):
            self.vbc = VBaseClientTest.create_instance_from_env()
            self.indexing_service = IndexingService.create_instance_from_commitment_service(
                self.vbc.commitment_service
            )
        self.assertTrue(self.indexing_service is not None)
        self.assertEqual(len(self.indexing_service.commitment_services), 1)
        self.chain_id = self.indexing_service.commitment_services[0].w3.eth.chain_id
        self.vbc.clear_sets()
        self.vbc.clear_set_objects(TEST_HASH1)
        cl = self.vbc.add_set(TEST_HASH1)
        assert cl["setCid"] == TEST_HASH1

    def test_add_set_indexing(self):
        """
        Test a simple set commitment.
        """
        # Use a random set CID to avoid collisions with other tests.
        set_cid = "0x" + secrets.token_bytes(32).hex()
        cl = self.vbc.add_set(set_cid=set_cid)
        user = cl["user"]
        commitment_receipts = self.indexing_service.find_user_sets(
            user=user,
        )
        # The node may run multiple tests accumulating multiple events.
        # Validate the tail.
        assert compare_dict_subset(
            commitment_receipts[-1],
            {
                "chainId": self.chain_id,
                "user": user,
                "setCid": set_cid,
            },
        )

    def test_add_sets_indexing(self):
        """
        Test a list of set commitments.
        """
        # Use a random set CID to avoid collisions with other tests.
        set_cids = ["0x" + secrets.token_bytes(32).hex() for i in range(5)]
        cls = []
        for set_cid in set_cids:
            cl = self.vbc.add_set(set_cid=set_cid)
            cls.append(cl)
        user = cls[0]["user"]
        commitment_receipts = self.indexing_service.find_user_sets(
            user=user,
        )
        # The node may run multiple tests accumulating multiple events.
        # Validate the tail.
        for i in range(5):
            assert compare_dict_subset(
                commitment_receipts[-5 + i],
                {
                    "chainId": self.chain_id,
                    "user": user,
                    "setCid": set_cids[i],
                },
            )

    # Disable R0801: Similar lines in 2 files for duplicate tests.
    # pylint: disable=R0801
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
                "chainId": self.chain_id,
                "user": user,
                "setCid": TEST_HASH1,
                "objectCid": TEST_HASH2,
                "timestamp": cl["timestamp"],
            },
        )

    # Disable R0801: Similar lines in 2 files for duplicative tests.
    # pylint: disable=R0801
    def test_add_set_objects_indexing(self):
        """
        Test a series of simple set object commitments.
        """
        # Use a random set CID to avoid collisions with other tests.
        set_cid = "0x" + secrets.token_bytes(32).hex()
        cls = []
        for i in range(5):
            cl = self.vbc.add_set_object_with_timestamp(
                set_cid=set_cid,
                object_cid=int_to_hash(i),
                timestamp=self.vbc.commitment_service.convert_timestamp_chain_to_str(i),
            )
            cls.append(cl)
        user = cls[0]["user"]
        commitment_receipts = self.indexing_service.find_user_set_objects(
            user=user, set_cid=set_cid
        )
        for i in range(5):
            assert compare_dict_subset(
                commitment_receipts[-5 + i],
                {
                    "chainId": self.chain_id,
                    "user": user,
                    "setCid": set_cid,
                    "objectCid": int_to_hash(i),
                    "timestamp": self.vbc.commitment_service.convert_timestamp_chain_to_str(
                        i
                    ),
                },
            )

    # Disable R0801: Similar lines in 2 files for duplicative tests.
    # pylint: disable=R0801
    def test_find_last_user_set_object(self):
        """
        Test a series of simple set object commitments
        followed by find_last_user_set_object().
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
        commitment_receipt = self.indexing_service.find_last_user_set_object(
            user=user, set_cid=set_cid
        )
        assert compare_dict_subset(
            commitment_receipt,
            {
                "chainId": self.chain_id,
                "user": user,
                "setCid": set_cid,
                "objectCid": int_to_hash(5),
                "timestamp": self.vbc.commitment_service.convert_timestamp_chain_to_str(
                    5
                ),
            },
        )
        # Check empty receipt.
        set_cid = "0x" + secrets.token_bytes(32).hex()
        commitment_receipt = self.indexing_service.find_last_user_set_object(
            user=user, set_cid=set_cid
        )
        assert commitment_receipt is None

    # Disable R0801: Similar lines in 2 files for duplicative tests.
    # pylint: disable=R0801
    def test_add_object_find_object(self):
        """
        Test a simple object commitment following by find_object().
        """
        cl = self.vbc.add_object(object_cid=TEST_HASH2)
        user = cl["user"]
        commitment_receipts = self.indexing_service.find_object(object_cid=TEST_HASH2)
        # The node may run multiple tests accumulating multiple events.
        # Validate the tail.
        assert compare_dict_subset(
            commitment_receipts[-1],
            {
                "chainId": self.chain_id,
                "user": user,
                "objectCid": TEST_HASH2,
                "timestamp": cl["timestamp"],
            },
        )

    # Disable R0801: Similar lines in 2 files for duplicative tests.
    # pylint: disable=R0801
    def test_add_objects_find_objects(self):
        """
        Test add and find for multiple objects.
        """
        cls = [self.vbc.add_object(object_cid=int_to_hash(i)) for i in range(1, 5)]
        user = cls[0]["user"]
        cl_inds = [1, 2]
        cids = [cls[i]["objectCid"] for i in cl_inds]
        timestamps = [cls[i]["timestamp"] for i in cl_inds]
        commitment_receipts = self.indexing_service.find_objects(object_cids=cids)
        # The node may run multiple tests accumulating multiple events.
        # Validate the tail.
        for i in range(2):
            assert compare_dict_subset(
                commitment_receipts[-2 + i],
                {
                    "chainId": self.chain_id,
                    "user": user,
                    "objectCid": cids[i],
                    "timestamp": timestamps[i],
                },
            )

    # Disable R0801: Similar lines in 2 files for duplicative tests.
    # pylint: disable=R0801
    def test_find_last_object(self):
        """
        Test a simple object commitment following by find_last_object().
        """
        cl = self.vbc.add_object(object_cid=TEST_HASH2)
        user = cl["user"]
        commitment_receipt = self.indexing_service.find_last_object(
            object_cid=TEST_HASH2
        )
        # The node may run multiple tests accumulating multiple events.
        # Validate the tail.
        assert compare_dict_subset(
            commitment_receipt,
            {
                "chainId": self.chain_id,
                "user": user,
                "objectCid": TEST_HASH2,
                "timestamp": cl["timestamp"],
            },
        )
        # Check empty receipt.
        object_cid = "0x" + secrets.token_bytes(32).hex()
        commitment_receipt = self.indexing_service.find_last_object(object_cid)
        assert commitment_receipt is None


if __name__ == "__main__":
    unittest.main()
