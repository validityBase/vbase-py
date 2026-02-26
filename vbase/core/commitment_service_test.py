"""The vbase commitment service module provides access to various commitment services
such as blockchain-based smart contracts.
"""

import logging
from abc import abstractmethod
from typing import List

from vbase.core.commitment_service import CommitmentService
from vbase.utils.log import get_default_logger

_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


class CommitmentServiceTest(CommitmentService):
    """Interface for base commitment operations"""

    @abstractmethod
    def clear_sets(self):
        """Clear all sets for the user.
        Used to clear state when testing.
        Only supported by test contracts.
        """

    @abstractmethod
    def clear_set_objects(self, set_cid: str):
        """Clear all records (objects) for a user's set.
        Used to clear state when testing.
        Only supported by test contracts.

        :param set_cid: Hash identifying the set.
        """

    @abstractmethod
    def add_object_with_timestamp(self, object_cid: str, timestamp: str) -> dict:
        """Test shim to record an object commitment with a given timestamp.
        Only supported by test contracts.

        :param object_cid: The CID identifying the object.
        :param timestamp: The timestamp to force for the record.
        :return: The commitment log containing commitment receipt info.
        """

    @abstractmethod
    def add_set_object_with_timestamp(
        self, set_cid: str, object_cid: str, timestamp: str
    ) -> dict:
        """Test shim to record an object commitment with a given timestamp.
        Only supported by test contracts.

        :param set_cid: The CID of the set containing the object.
        :param object_cid: The CID to record.
        :param timestamp: The timestamp to force for the record.
        :return: The commitment log containing commitment receipt info.
        """

    @abstractmethod
    def add_sets_objects_with_timestamps_batch(
        self, set_cids: List[str], object_cids: List[str], timestamps: List[str]
    ) -> List[dict]:
        """Test shim to record a batch of object commitment with a timestamps.
        Only supported by test contracts.

        :param set_cids: The hashes of the sets containing the objects.
        :param object_cids: The hashes to record.
        :param timestamps: The timestamps to force for the records.
        :return: The commitment logs containing commitment receipts.
        """

    @abstractmethod
    def add_set_objects_with_timestamps_batch(
        self, set_cid: str, object_cids: List[str], timestamps: List[str]
    ) -> List[dict]:
        """Test shim to record a batch of object commitment with a timestamps.
        Only supported by test contracts.

        :param set_cid: The CID of the set containing the objects.
        :param object_cids: The hashes to record.
        :param timestamps: The timestamps to force for the records.
        :return: The commitment logs containing commitment receipts.
        """
