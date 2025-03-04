"""
The vbase module provides access to base validityBase (vBase) commitments.
The test class provides additional test API hooks.
"""

import logging
import os
from typing import List, Union

import pandas as pd
from dotenv import load_dotenv

from vbase.core.commitment_service_test import CommitmentServiceTest
from vbase.core.forwarder_commitment_service_test import ForwarderCommitmentServiceTest
from vbase.core.vbase_client import VBaseClient
from vbase.core.web3_http_commitment_service_test import Web3HTTPCommitmentServiceTest
from vbase.utils.log import get_default_logger

LOG = get_default_logger(__name__)
LOG.setLevel(logging.INFO)


class VBaseClientTest(VBaseClient):
    """
    Provides Python validityBase (vBase) access with test methods.
    Test methods allow clearing state and bootstrapping objects with pre-defined timestamps.
    """

    def __init__(self, commitment_service: CommitmentServiceTest):
        super().__init__(commitment_service)
        # Init commitment_service as type CommitmentServiceTest
        # to enable type checking in the following call
        # to methods of CommitmentServiceTest.
        self.commitment_service = commitment_service

    @staticmethod
    def create_instance_from_env(
        dotenv_path: Union[str, None] = None
    ) -> "VBaseClientTest":
        if dotenv_path is not None:
            load_dotenv(dotenv_path, verbose=True, override=True)

        # TODO: Handle clients with multiple commitment services.

        # We parameterize the class to construct using commitment_service_class.
        # The class should be imported above, and then we can call methods on this
        # class using string variable as the class name.
        commitment_service_class_name = os.environ.get("VBASE_COMMITMENT_SERVICE_CLASS")
        if commitment_service_class_name is None:
            commitment_service_class_name = "Web3HTTPCommitmentServiceTest"

        # Call methods on this class using string variable as the class name.
        # We could do this cleverly as globals()[commitment_service_class].init_from_env(),
        # but do this the long way for type safety.
        if commitment_service_class_name == "Web3HTTPCommitmentServiceTest":
            commitment_service_class = Web3HTTPCommitmentServiceTest
        elif commitment_service_class_name == "ForwarderCommitmentServiceTest":
            commitment_service_class = ForwarderCommitmentServiceTest
        else:
            raise NotImplementedError()
        # Use the env variables loaded with load_dotenv above.
        return VBaseClientTest(commitment_service_class.create_instance_from_env())

    #####################################################
    # Test object commitments for bootstrapping timestamps
    #####################################################

    @staticmethod
    def normalize_pd_timestamp(timestamp: Union[pd.Timestamp, str]):
        """
        Normalize Pandas timestamp converting it to a string representation
        that is serializable.

        :param timestamp: A representation of a pd.Timestamp object.
        :return: The string representation of a pd.Timestamp.
        """
        if isinstance(timestamp, pd.Timestamp):
            timestamp = str(timestamp)
        return timestamp

    def add_object_with_timestamp(
        self, object_cid: str, timestamp: Union[pd.Timestamp, str]
    ) -> dict:
        """
        Test method to record an object commitment with a given timestamp.
        Only supported by test contracts.

        :param object_cid: The CID identifying the object.
        :param timestamp: The timestamp to force for the record.
        :return: The commitment log containing commitment receipt info.
        """
        return self.commitment_service.add_object_with_timestamp(
            object_cid, self.normalize_pd_timestamp(timestamp)
        )

    def add_set_object_with_timestamp(
        self, set_cid: str, object_cid: str, timestamp: Union[pd.Timestamp, str]
    ) -> dict:
        """
        Test method to record an object commitment with a given timestamp.
        Only supported by test contracts.

        :param set_cid: The CID of the set containing the object.
        :param object_cid: The CID to record.
        :param timestamp: The timestamp to force for the record.
        :return: The commitment log containing commitment receipt info.
        """
        return self.commitment_service.add_set_object_with_timestamp(
            set_cid, object_cid, self.normalize_pd_timestamp(timestamp)
        )

    def add_sets_objects_with_timestamps_batch(
        self,
        set_cids: List[str],
        object_cids: List[str],
        timestamps: List[pd.Timestamp],
    ) -> List[dict]:
        """
        Test method to record a batch of object commitment with a timestamps.
        Only supported by test contracts.

        :param set_cids: The hashes of the sets containing the objects.
        :param object_cids: The hashes to record.
        :param timestamps: The timestamps to force for the records.
        :return: The commitment log list containing commitment receipts.
        """
        return self.commitment_service.add_sets_objects_with_timestamps_batch(
            set_cids,
            object_cids,
            [self.normalize_pd_timestamp(ts) for ts in timestamps],
        )

    def add_set_objects_with_timestamps_batch(
        self,
        set_cid: str,
        object_cids: List[str],
        timestamps: List[pd.Timestamp],
    ) -> List[dict]:
        """
        Test method to record a batch of object commitment with a timestamps.
        Only supported by test contracts.

        :param set_cid: The CID of the set containing the objects.
        :param object_cids: The hashes to record.
        :param timestamps: The timestamps to force for the records.
        :return: The commitment log list containing commitment receipts.
        """
        return self.commitment_service.add_set_objects_with_timestamps_batch(
            set_cid,
            object_cids,
            [self.normalize_pd_timestamp(ts) for ts in timestamps],
        )

    ###################################
    # Test methods to clear commitments
    ###################################

    def clear_sets(self):
        """
        Clear all sets for the user.
        Used to clear state when testing.
        Only supported by test contracts.
        """
        self.commitment_service.clear_sets()

    def clear_set_objects(self, set_cid: str):
        """
        Clear all records (objects) for a user's set.
        Used to clear state when testing.
        Only supported by test contracts.

        :param set_cid: Hash identifying the set.
        """
        self.commitment_service.clear_set_objects(set_cid)

    def clear_named_set_objects(self, name: str):
        """
        Clear all records (objects) for a user's named set.

        :param name: Name of the set to clear.
        """
        self.clear_set_objects(self.get_named_set_cid(name))
