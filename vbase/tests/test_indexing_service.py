"""
Tests of the indexing service for the vbase package
"""

import secrets
import unittest

from vbase.core.vbase_client_test import VBaseClientTest
from vbase.core.indexing_service import Web3HTTPIndexingService

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
            self.indexing_service = (
                Web3HTTPIndexingService.create_instance_from_commitment_service(
                    self.vbc.commitment_service
                )
            )
        self.assertTrue(self.indexing_service is not None)
        self.assertEqual(len(self.indexing_service.commitment_services), 1)
        self.chain_id = self.indexing_service.commitment_services[0].w3.eth.chain_id
        self.vbc.clear_sets()
        self.vbc.clear_set_objects(TEST_HASH1)
        cl = self.vbc.add_set(TEST_HASH1)
        self.assertEqual(cl["setCid"], TEST_HASH1)

    # Disable R0801: Similar lines in 2 files for duplicate tests.
    # pylint: disable=R0801
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
        self.assertTrue(
            compare_dict_subset(
                commitment_receipts[-1],
                {
                    "chainId": self.chain_id,
                    "user": user,
                    "setCid": set_cid,
                },
            )
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
            self.assertTrue(
                compare_dict_subset(
                    commitment_receipts[-5:][i],
                    {
                        "chainId": self.chain_id,
                        "user": user,
                        "setCid": set_cids[i],
                    },
                )
            )

    # Disable R0801: Similar lines in 2 files for duplicate tests.
    # pylint: disable=R0801
    def test_add_set_object_indexing(self):
        """
        Test a simple set object commitment.
        """
        cl = self.vbc.add_set_object(set_cid=TEST_HASH1, object_cid=TEST_HASH2)
        user = cl["user"]
        expected_receipt = {
            "chainId": self.chain_id,
            "user": user,
            "setCid": TEST_HASH1,
            "objectCid": TEST_HASH2,
            "timestamp": cl["timestamp"],
        }

        # Verify find_user_set_objects().
        commitment_receipts = self.indexing_service.find_user_set_objects(
            user=user, set_cid=TEST_HASH1
        )
        # The node may run multiple tests accumulating multiple events.
        # Validate the tail.
        self.assertTrue(
            compare_dict_subset(
                commitment_receipts[-1],
                expected_receipt,
            )
        )

        # Verify find_objects().
        commitment_receipts = self.indexing_service.find_objects(
            object_cids=[TEST_HASH2]
        )
        self.assertNotIn("setCid", commitment_receipts[-1])
        self.assertTrue(
            compare_dict_subset(
                commitment_receipts[-1],
                {
                    k: expected_receipt[k]
                    for k in ("chainId", "user", "objectCid", "timestamp")
                },
            )
        )

        # Verify find_objects(return_set_cids=True).
        commitment_receipts = self.indexing_service.find_objects(
            object_cids=[TEST_HASH2], return_set_cids=True
        )
        self.assertTrue(
            compare_dict_subset(
                commitment_receipts[-1],
                expected_receipt,
            )
        )

    # Disable R0801: Similar lines in 2 files for duplicative tests.
    # pylint: disable=R0801
    def test_add_set_objects_indexing(self):
        """
        Test a series of simple set object commitments.
        """
        # Use a random set CID to avoid collisions with other tests.
        set_cid = "0x" + secrets.token_bytes(32).hex()
        self.vbc.add_set(set_cid=set_cid)
        object_cids = [int_to_hash(i) for i in range(5)]
        cls = []
        for i in range(5):
            cl = self.vbc.add_set_object(
                set_cid=set_cid,
                object_cid=object_cids[i],
            )
            cls.append(cl)
        user = cls[0]["user"]
        expected_receipts = [
            {
                "chainId": self.chain_id,
                "user": user,
                "setCid": set_cid,
                "objectCid": object_cids[i],
                "timestamp": cls[i]["timestamp"],
            }
            for i in range(5)
        ]

        # Verify find_user_set_objects().
        commitment_receipts = self.indexing_service.find_user_set_objects(
            user=user, set_cid=set_cid
        )
        for i in range(5):
            self.assertTrue(
                compare_dict_subset(
                    commitment_receipts[-5:][i],
                    expected_receipts[i],
                )
            )

        # Verify find_objects().
        commitment_receipts = self.indexing_service.find_objects(
            object_cids=object_cids
        )
        for i in range(5):
            self.assertTrue(
                compare_dict_subset(
                    commitment_receipts[-5:][i],
                    {
                        k: expected_receipts[i][k]
                        for k in ("chainId", "user", "objectCid", "timestamp")
                    },
                )
            )

        # Verify find_objects(return_set_cids=True).
        commitment_receipts = self.indexing_service.find_objects(
            object_cids=object_cids, return_set_cids=True
        )
        for i in range(5):
            self.assertTrue(
                compare_dict_subset(
                    commitment_receipts[-5:][i],
                    expected_receipts[i],
                )
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
        self.assertTrue(
            compare_dict_subset(
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
        )
        # Check empty receipt.
        set_cid = "0x" + secrets.token_bytes(32).hex()
        commitment_receipt = self.indexing_service.find_last_user_set_object(
            user=user, set_cid=set_cid
        )
        self.assertIsNone(commitment_receipt)

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
        self.assertTrue(
            compare_dict_subset(
                commitment_receipts[-1],
                {
                    "chainId": self.chain_id,
                    "user": user,
                    "objectCid": TEST_HASH2,
                    "timestamp": cl["timestamp"],
                },
            )
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
            self.assertTrue(
                compare_dict_subset(
                    commitment_receipts[-2:][i],
                    {
                        "chainId": self.chain_id,
                        "user": user,
                        "objectCid": cids[i],
                        "timestamp": timestamps[i],
                    },
                )
            )

    # Disable R0801: Similar lines in 2 files for duplicative tests.
    # pylint: disable=R0801
    def test_add_objects_find_user_objects(self):
        """
        Test add and find for multiple user objects.
        """
        for i in range(5):
            self.vbc.add_set(set_cid=int_to_hash(i + 1))
        cls = [self.vbc.add_object(object_cid=int_to_hash(i + 1)) for i in range(5)] + [
            self.vbc.add_set_object(
                set_cid=int_to_hash(i + 1), object_cid=int_to_hash(i + 10)
            )
            for i in range(5)
        ]
        user = cls[0]["user"]
        cl_inds = range(10)
        set_cids = [None] * 5 + [int_to_hash(i + 1) for i in range(5)]
        object_cids = [cls[i]["objectCid"] for i in cl_inds]
        timestamps = [cls[i]["timestamp"] for i in cl_inds]
        commitment_receipts = self.indexing_service.find_user_objects(
            user=user, return_set_cids=True
        )
        # The node may run multiple tests accumulating multiple events.
        # Validate the tail.
        for i in range(5):
            self.assertTrue(
                compare_dict_subset(
                    commitment_receipts[-10:][i],
                    {
                        "chainId": self.chain_id,
                        "user": user,
                        "objectCid": object_cids[i],
                        "timestamp": timestamps[i],
                    },
                )
            )
        for i in range(5):
            self.assertTrue(
                compare_dict_subset(
                    commitment_receipts[-5:][i],
                    {
                        "chainId": self.chain_id,
                        "user": user,
                        "setCid": set_cids[5:][i],
                        "objectCid": object_cids[5:][i],
                        "timestamp": timestamps[5:][i],
                    },
                )
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
        self.assertTrue(
            compare_dict_subset(
                commitment_receipt,
                {
                    "chainId": self.chain_id,
                    "user": user,
                    "objectCid": TEST_HASH2,
                    "timestamp": cl["timestamp"],
                },
            )
        )
        # Check empty receipt.
        object_cid = "0x" + secrets.token_bytes(32).hex()
        commitment_receipt = self.indexing_service.find_last_object(object_cid)
        self.assertIsNone(commitment_receipt)


if __name__ == "__main__":
    unittest.main()
