"""
Tests of the indexing service for the vbase package
using two commitment services for two local smart contracts.
Tests multichain and app compat scenarios for multiple commitment services.
"""

import json
import os
import unittest

from vbase.core.indexing_service import Web3HTTPIndexingService
from vbase.core.vbase_client import VBaseClient
from vbase.core.vbase_client_test import VBaseClientTest

from vbase.tests.utils import (
    int_to_hash,
    TEST_HASH1,
    TEST_HASH2,
    compare_dict_subset,
)


# Test RPC endpoint.
_LOCALHOST_RPC_ENDPOINT = "http://127.0.0.1:8545/"
# Commitment service addresses.
# These are deterministic addresses from deployment on a localhost test node.
_COMMITMENT_SERVICE_ADDR_1 = "0x5FbDB2315678afecb367f032d93F642f64180aa3"
_COMMITMENT_SERVICE_ADDR_2 = "0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512"


class TestIndexingServiceDual(unittest.TestCase):
    """
    Test base vBase indexing functionality with two commitment services
    using a local test node.
    """

    def setUp(self):
        """
        Set up the tests.
        """
        # Create an indexing service using the above commitment service info.
        # Define the indexing service using JSON environment variable
        # to test service start-up path.
        indexing_service_json_str = json.dumps(
            {
                "commitment_services": [
                    {
                        "class": "Web3HTTPCommitmentService",
                        "init_args": {
                            "node_rpc_url": _LOCALHOST_RPC_ENDPOINT,
                            "commitment_service_address": _COMMITMENT_SERVICE_ADDR_1,
                        },
                    },
                    {
                        "class": "Web3HTTPCommitmentServiceTest",
                        "init_args": {
                            "node_rpc_url": _LOCALHOST_RPC_ENDPOINT,
                            "commitment_service_address": _COMMITMENT_SERVICE_ADDR_2,
                        },
                    },
                ]
            }
        )
        os.environ["VBASE_INDEXING_SERVICE_JSON_DESCRIPTOR"] = indexing_service_json_str
        self.indexing_service = (
            Web3HTTPIndexingService.create_instance_from_env_json_descriptor()
        )
        assert self.indexing_service.commitment_services[0].w3.is_connected()
        assert self.indexing_service.commitment_services[1].w3.is_connected()

        # Use these commitment services to define others client state.
        self.cs1 = self.indexing_service.commitment_services[0]
        self.cs2 = self.indexing_service.commitment_services[1]
        self.vbc1 = VBaseClient(self.cs1)
        self.vbc2 = VBaseClientTest(self.cs2)
        self.chain_id = self.vbc1.commitment_service.w3.eth.chain_id
        self.vbc1.add_set(TEST_HASH1)
        self.vbc2.add_set(TEST_HASH1)

    def test_add_set_object_indexing(self):
        """
        Test a simple set object commitment.
        """
        cl1 = self.vbc1.add_set_object(set_cid=TEST_HASH1, object_cid=TEST_HASH1)
        cl2 = self.vbc2.add_set_object(set_cid=TEST_HASH1, object_cid=TEST_HASH2)
        user = cl2["user"]
        commitment_receipts = self.indexing_service.find_user_set_objects(
            user=user, set_cid=TEST_HASH1
        )
        # The node may run multiple tests accumulating multiple events.
        # Validate the tail.
        assert compare_dict_subset(
            commitment_receipts[-2],
            {
                "chainId": self.chain_id,
                "user": user,
                "setCid": TEST_HASH1,
                "objectCid": TEST_HASH1,
                "timestamp": cl1["timestamp"],
            },
        )
        assert compare_dict_subset(
            commitment_receipts[-1],
            {
                "chainId": self.chain_id,
                "user": user,
                "setCid": TEST_HASH1,
                "objectCid": TEST_HASH2,
                "timestamp": cl2["timestamp"],
            },
        )

    def test_add_object_find_object(self):
        """
        Test a simple object commitment followed by find_object().
        """
        self.vbc1.add_object(object_cid=TEST_HASH2)
        cl2 = self.vbc2.add_object(object_cid=TEST_HASH2)
        user = cl2["user"]
        commitment_receipts = self.indexing_service.find_object(object_cid=TEST_HASH2)
        # The node may run multiple tests accumulating multiple events.
        # Validate the tail.
        assert compare_dict_subset(
            commitment_receipts[-1],
            {
                "chainId": self.chain_id,
                "user": user,
                "objectCid": TEST_HASH2,
                "timestamp": cl2["timestamp"],
            },
        )

    # Disable R0801: Similar lines in 2 files for duplicative tests.
    # pylint: disable=R0801
    def test_add_objects_find_objects(self):
        """
        Test add and find for multiple objects.
        """
        cls1 = [self.vbc1.add_object(object_cid=int_to_hash(i)) for i in range(1, 5)]
        cls2 = [self.vbc1.add_object(object_cid=int_to_hash(i)) for i in range(1, 5)]
        user = cls1[0]["user"]
        cl_inds = [1, 2]
        cids = [cls1[i]["objectCid"] for i in cl_inds] + [
            cls2[i]["objectCid"] for i in cl_inds
        ]
        timestamps = [cls1[i]["timestamp"] for i in cl_inds] + [
            cls2[i]["timestamp"] for i in cl_inds
        ]
        commitment_receipts = self.indexing_service.find_objects(object_cids=cids[:2])
        # The node may run multiple tests accumulating multiple events.
        # Validate the tail.
        for i in range(4):
            assert compare_dict_subset(
                commitment_receipts[-4 + i],
                {
                    "chainId": self.chain_id,
                    "user": user,
                    "objectCid": cids[i],
                    "timestamp": timestamps[i],
                },
            )


if __name__ == "__main__":
    unittest.main()
