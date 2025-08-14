"""The vbase commitment service module provides access to various commitment services
such as blockchain-based smart contracts.
This implementation uses a forwarder to execute meta-transactions on a user's behalf
against a test smart contract.
"""

import logging
from typing import List, Optional, Union

from vbase.core.commitment_service_test import CommitmentServiceTest
from vbase.core.forwarder_commitment_service import ForwarderCommitmentService
from vbase.utils.log import get_default_logger

_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


class ForwarderCommitmentServiceTest(ForwarderCommitmentService, CommitmentServiceTest):
    """Test commitment service accessible using a forwarder API endpoint."""

    # pylint: disable-msg=too-many-arguments
    def __init__(
        self,
        forwarder_url: str,
        api_key: str,
        private_key: Optional[str] = None,
        commitment_service_json_file_name: Optional[str] = "CommitmentServiceTest.json",
    ):
        super().__init__(
            forwarder_url,
            api_key,
            private_key,
            commitment_service_json_file_name,
        )

    @staticmethod
    def create_instance_from_env(
        dotenv_path: Union[str, None] = None
    ) -> "ForwarderCommitmentServiceTest":
        return ForwarderCommitmentServiceTest(
            **ForwarderCommitmentService.get_init_args_from_env(dotenv_path)
        )

    def clear_sets(self):
        self._post_execute(fn_name="clearSets", args=[])

    def clear_set_objects(self, set_cid: str):
        self._post_execute(fn_name="clearSetObjects", args=[set_cid])

    def add_object_with_timestamp(self, object_cid: str, timestamp: str) -> dict:
        _LOG.debug("Sending forward request to addObjectWithTimestamp")
        receipt = self._post_execute(
            fn_name="addObjectWithTimestamp",
            args=[object_cid, self.convert_timestamp_str_to_chain(timestamp)],
        )
        return self._add_object_worker(receipt)

    def add_set_object_with_timestamp(
        self, set_cid: str, object_cid: str, timestamp: str
    ) -> dict:
        _LOG.debug("Sending transaction to addSetObjectWithTimestamp")
        receipt = self._post_execute(
            fn_name="addSetObjectWithTimestamp",
            args=[
                set_cid,
                object_cid,
                self.convert_timestamp_str_to_chain(timestamp),
            ],
        )
        return self._add_set_object_worker(receipt)

    def add_sets_objects_with_timestamps_batch(
        self, set_cids: List[str], object_cids: List[str], timestamps: List[str]
    ) -> List[dict]:
        _LOG.debug("Sending transaction to addSetsObjectsWithTimestampsBatch")
        receipt = self._post_execute(
            fn_name="addSetsObjectsWithTimestampsBatch",
            args=[
                set_cids,
                object_cids,
                [self.convert_timestamp_str_to_chain(ts) for ts in timestamps],
            ],
        )
        return self._add_sets_objects_batch_worker(receipt)

    def add_set_objects_with_timestamps_batch(
        self, set_cid: str, object_cids: List[str], timestamps: List[str]
    ) -> List[dict]:
        _LOG.debug("Sending transaction to addSetObjectsWithTimestampsBatch")
        receipt = self._post_execute(
            fn_name="addSetObjectsWithTimestampsBatch",
            args=[
                set_cid,
                object_cids,
                [self.convert_timestamp_str_to_chain(ts) for ts in timestamps],
            ],
        )
        return self._add_sets_objects_batch_worker(receipt)
