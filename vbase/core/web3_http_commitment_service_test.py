"""
The vbase commitment service module provides access to various commitment services
such as blockchain-based smart contracts.
This implementation uses Web3.HTTPProvider to access a test commitment service.
"""

import logging
from typing import List, Optional, Union

from vbase.core.commitment_service_test import CommitmentServiceTest
from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService
from vbase.utils.log import get_default_logger


_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


class Web3HTTPCommitmentServiceTest(Web3HTTPCommitmentService, CommitmentServiceTest):
    """
    Test commitment service accessible using Web3.HTTPProvider.
    """

    # pylint: disable-msg=too-many-arguments
    def __init__(
        self,
        node_rpc_url: str = None,
        commitment_service_address: str = None,
        private_key: Optional[str] = None,
        commitment_service_json_file_name: Optional[str] = "CommitmentServiceTest.json",
        inject_geth_poa_middleware: bool = False,
    ):
        super().__init__(
            node_rpc_url,
            commitment_service_address,
            private_key,
            commitment_service_json_file_name,
            inject_geth_poa_middleware,
        )

    @staticmethod
    def create_instance_from_env(
        dotenv_path: Union[str, None] = None
    ) -> "Web3HTTPCommitmentServiceTest":
        return Web3HTTPCommitmentServiceTest(
            **Web3HTTPCommitmentService.get_init_args_from_env(dotenv_path)
        )

    def clear_sets(self):
        self.csc.functions.clearSets().transact()

    def clear_set_objects(self, set_cid: str):
        self.csc.functions.clearSetObjects(set_cid).transact()

    def add_object_with_timestamp(self, object_cid: str, timestamp: str) -> dict:
        _LOG.debug("Sending transaction to addObjectWithTimestamp")
        tx_hash = self.csc.functions.addObjectWithTimestamp(
            object_cid, self.convert_timestamp_str_to_chain(timestamp)
        ).transact()
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return self._add_object_worker(receipt)

    def add_set_object_with_timestamp(
        self, set_cid: str, object_cid: str, timestamp: str
    ) -> dict:
        _LOG.debug("Sending transaction to addSetObjectWithTimestamp")
        tx_hash = self.csc.functions.addSetObjectWithTimestamp(
            # Strings get converted to bytes32 by Web3.
            set_cid,
            object_cid,
            self.convert_timestamp_str_to_chain(timestamp),
        ).transact()
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return self._add_set_object_worker(receipt)

    def add_sets_objects_with_timestamps_batch(
        self, set_cids: List[str], object_cids: List[str], timestamps: List[str]
    ) -> List[dict]:
        _LOG.debug("Sending transaction to addSetObjectWithTimestamp")
        tx_hash = self.csc.functions.addSetsObjectsWithTimestampsBatch(
            # Strings get converted to bytes32 by Web3.
            set_cids,
            object_cids,
            [self.convert_timestamp_str_to_chain(ts) for ts in timestamps],
        ).transact()
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return self._add_sets_objects_batch_worker(receipt)

    def add_set_objects_with_timestamps_batch(
        self, set_cid: str, object_cids: List[str], timestamps: List[str]
    ) -> List[dict]:
        _LOG.debug("Sending transaction to addSetObjectWithTimestamp")
        tx_hash = self.csc.functions.addSetObjectsWithTimestampsBatch(
            # Strings get converted to bytes32 by Web3.
            set_cid,
            object_cids,
            [self.convert_timestamp_str_to_chain(ts) for ts in timestamps],
        ).transact()
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return self._add_sets_objects_batch_worker(receipt)
