"""
The vbase module provides access to base validityBase (vBase) commitments.
"""

import logging
import os
from typing import Callable, List, Union
from dotenv import load_dotenv
import pandas as pd


from vbase.utils.log import get_default_logger
from vbase.core.commitment_service import CommitmentService
from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService
from vbase.core.web3_http_commitment_service_test import Web3HTTPCommitmentServiceTest
from vbase.core.forwarder_commitment_service import ForwarderCommitmentService
from vbase.core.forwarder_commitment_service_test import ForwarderCommitmentServiceTest
from vbase.utils.crypto_utils import add_uint256_uint256


LOG = get_default_logger(__name__)
LOG.setLevel(logging.INFO)


VBASE_COMMITMENT_SERVICE_TYPES = {
    "Web3HTTPCommitmentService": Web3HTTPCommitmentService,
    "Web3HTTPCommitmentServiceTest": Web3HTTPCommitmentServiceTest,
    "ForwarderCommitmentService": ForwarderCommitmentService,
    "ForwarderCommitmentServiceTest": ForwarderCommitmentServiceTest,
}


class VBaseClient:
    """
    Provides Python validityBase (vBase) access.
    """

    # The class is the entry point to vBase operations.
    # It has lots of public methods by design.
    # Disable R0904: Too many public methods (25/20) (too-many-public-methods):
    # pylint: disable=R0904

    def __init__(self, commitment_service: CommitmentService):
        """
        Initialize a vBase object.

        :param commitment_service: The service for managing commitments.
            The service typically comprises a set of blockchains and smart contracts
            that support commitment operations.
        :param **kwargs: A dictionary with keyword arguments:

        """
        self.commitment_service = commitment_service

        # Init simulation state.
        self._in_sim: bool = False
        # All timestamps are standard UTC times
        # and benefit from pandas scalable timestamp implementation.
        self._sim_t: Union[pd.Timestamp, None] = None

    @staticmethod
    def create_instance_from_env(
        dotenv_path: Union[str, None] = ".env"
    ) -> "VBaseClient":
        """
        Creates an instance initialized from environment variables.
        Syntactic sugar for initializing new commitment objects using settings
        stored in a .env file or in environment variables.

        :param dotenv_path: Path to the .env file.
            Below is the default treatment that should be appropriate in most scenarios:
            - If dotenv_path is specified, attempt to load environment variable from the file.
            Ignore failures and default to the environment variables.
            - If called with no arguments, use default ".env" path.
            - If None dotenv_path is specified, default to the environment variables.
        :return: The constructed vBase client object.
        """

        if dotenv_path is not None:
            load_dotenv(dotenv_path, verbose=True, override=True)

        # TODO: Handle clients with multiple commitment services.

        # We parameterize the class to construct using commitment_service_class.
        # The class should be imported above, and then we can call methods on this
        # class using string variable as the class name.
        commitment_service_class_name = os.environ.get("VBASE_COMMITMENT_SERVICE_CLASS")
        if commitment_service_class_name is None:
            # Use forwarder by default.
            # This minimizes friction in the default use case for end-users.
            commitment_service_class_name = "ForwarderCommitmentService"

        # Call methods on this class using string variable as the class name.
        # We could do this cleverly as globals()[commitment_service_class].init_from_env(),
        # but do this the long way for type safety.
        assert commitment_service_class_name in VBASE_COMMITMENT_SERVICE_TYPES
        commitment_service_class = VBASE_COMMITMENT_SERVICE_TYPES[
            commitment_service_class_name
        ]
        return VBaseClient(commitment_service_class.create_instance_from_env())

    def get_default_user(self) -> str:
        """
        Return the default user address used in vBase transactions.

        :return: The default user address used in vBase transactions.
        """
        return self.commitment_service.get_default_user()

    #################
    # Set commitments
    #################

    def add_set(self, set_cid: str) -> dict:
        """
        Records a set commitment.
        This is a low-level function that operates on set CIDs.
        It does not specify how a hash is built and does not provide
        a schema for hashing complex information.

        :param set_cid: The CID (hash) identifying the set.
        :return: The commitment log containing commitment receipt info.
        """
        return self.commitment_service.add_set(set_cid)

    def user_set_exists(self, user: str, set_cid: str) -> bool:
        """
        Checks whether a given set exists for the calling user.
        This function abstracts the low-level commitment of named set creation.

        :param user: The address for the user who recorded the commitment.
        :param set_cid: The CID (hash) identifying the set.
        :return: True if the set with the given hash exists; False otherwise.
        """
        # Check if the set commitment exists for the given set.
        return self.commitment_service.user_set_exists(user, set_cid)

    def get_named_set_cid(self, name: str) -> str:
        """
        Converts a set name to a hash.
        Abstracts the hashing implementation from the upper layers.

        :param name: The name of the set.
        :return: The CID for the name.
        """
        return self.commitment_service.get_named_set_cid(name)

    def add_named_set(self, name: str) -> dict:
        """
        Creates a commitment for a set with a given name.
        This function abstracts the low-level commitment of set creation.

        :param name: The name of the set.
        :return: The commitment log containing commitment receipt info.
        """
        return self.add_set(self.get_named_set_cid(name))

    def user_named_set_exists(self, user: str, name: str) -> bool:
        """
        Checks whether a set with a given name exists for the calling user.
        This function abstracts the low-level commitment of named set creation.

        :param user: The address for the user who recorded the commitment.
        :param name: The name of the set.
        :return: True if the set with the given name exists; False otherwise.
        """
        # Check if the set commitment exists for the given named set.
        return self.user_set_exists(user, self.get_named_set_cid(name))

    def verify_user_sets(self, user: str, user_sets_cid_sum: str) -> bool:
        """
        Verifies set commitments previously recorded by the user.
        This is a low-level function that operates on object hashes.

        :param user: The address for the user who recorded the commitment.
        :param user_sets_cid_sum: The sum of all set CIDs for the user.
        :return: True if the commitment has been verified successfully;
            False otherwise.
        """
        return self.commitment_service.verify_user_sets(user, user_sets_cid_sum)

    def verify_user_named_sets(self, user: str, names: List[str]) -> bool:
        """
        Verifies the completeness of a list of named sets.

        :param user: Address for the user who recorded the commitment.
        :param names: Names of user sets.
        :return: True if the names comprise all named sets committed by the user;
            False otherwise.
        """
        user_sets_cid_sum = "0x0"
        for name in names:
            set_cid = self.get_named_set_cid(name)
            # Add uint256s with overflow and wrap-around.
            # This replicates the following sol code:
            # unchecked {
            #    userSetCidSums[user] += uint256(setCid);
            # }
            user_sets_cid_sum = add_uint256_uint256(user_sets_cid_sum, set_cid)
        if not self.verify_user_sets(user, user_sets_cid_sum):
            return False
        return True

    ####################
    # Object commitments
    ####################

    def add_object(self, object_cid: str) -> dict:
        """
        Record an object commitment.
        This is a low-level function that operates on object hashes.
        It does not specify how a hash is built and does not provide
        a schema for hashing complex information.

        :param object_cid: The CID identifying the object.
        :return: The commitment log containing commitment receipt info.
        """
        return self.commitment_service.add_object(object_cid)

    def verify_user_object(
        self, user: str, object_cid: str, timestamp: Union[pd.Timestamp, str]
    ) -> bool:
        """
        Verifies an object commitment previously recorded.
        This is a low-level function that operates on object hashes.

        :param user: The address for the user who recorded the commitment.
        :param object_cid: The CID identifying the object.
        :param timestamp: The timestamp of the commitment.
        :return: True if the commitment has been verified successfully;
            False otherwise.
        """
        return self.commitment_service.verify_user_object(user, object_cid, timestamp)

    def add_set_object(self, set_cid: str, object_cid: str) -> dict:
        """
        Records a commitment for an object belonging to a set of objects.
        This is a low-level function that operates on set and object hashes.
        It does not specify how a hash is built and does not provide
        a schema for hashing complex information.

        :param set_cid: The CID for the set containing the object.
        :param object_cid: The object hash to record.
        :return: The commitment log containing commitment receipt info.
        """
        return self.commitment_service.add_set_object(set_cid, object_cid)

    def add_sets_objects_batch(
        self, set_cids: List[str], object_cids: List[str]
    ) -> List[dict]:
        """
        Records a batch of commitments for objects belonging to sets.
        This is a low-level function that operates on set and object hashes.
        It does not specify how a hash is built and does not provide
        a schema for hashing complex information.

        :param set_cids: The hashes of the sets containing the objects.
        :param object_cids: The hashes to record.
        :return: The commitment log list containing commitment receipts.
        """
        return self.commitment_service.add_sets_objects_batch(set_cids, object_cids)

    def add_set_objects_batch(self, set_cid: str, object_cids: List[str]) -> List[dict]:
        """
        Records a batch of commitments for objects belonging to a set.
        This is a low-level function that operates on set and object hashes.
        It does not specify how a hash is built and does not provide
        a schema for hashing complex information.

        :param set_cid: The CID of the set containing the objects.
        :param object_cids: The hashes to record.
        :return: The commitment log list containing commitment receipts.
        """
        return self.commitment_service.add_set_objects_batch(set_cid, object_cids)

    def verify_user_set_objects(
        self, user: str, set_cid: str, user_set_objects_cid_sum: str
    ) -> bool:
        """
        Verifies an object commitment previously recorded.
        This is a low-level function that operates on object hashes.

        :param user: The address for the user who recorded the commitment.
        :param set_cid: The CID for the set containing the object.
        :param user_set_objects_cid_sum: The sum of all object hashes for the user set
        :return: True if the commitment has been verified successfully;
            False otherwise.
        """
        return self.commitment_service.verify_user_set_objects(
            user, set_cid, user_set_objects_cid_sum
        )

    ###########################################
    # Simulation and PIT computation management
    ###########################################

    def in_sim(self) -> bool:
        """
        Get the simulation state.

        :return: True if vBase is in a simulation; False otherwise.
        """
        return self._in_sim

    def get_sim_t(self) -> Union[pd.Timestamp, None]:
        """
        Get the simulation timestamp.

        :return: If in simulation, the sim timestamp; None otherwise.
        """
        return self._sim_t

    def run_pit_sim(
        self,
        ts: pd.DatetimeIndex,
        callback: Callable[[], Union[int, float, dict, pd.DataFrame]],
    ) -> Union[pd.Series, pd.DataFrame]:
        """
        Runs a point-in-time (PIT) simulation.
        PIT simulation executes callback for each t specified
        letting the callback see world state as it existed at that t.

        :param ts: Times/timestamps for which callback should be called
            and PIT world state simulated.
        :param callback: The callback to call.
        :return: The aggregated output of all callback invocations.
        """
        sim_records = []
        self._in_sim = True
        for t in ts:
            self._sim_t = t
            LOG.debug("run_pit_sim(): > callback for t = %s", t)
            ret = callback()
            LOG.debug("run_pit_sim(): < callback for t = %s", t)
            LOG.debug("run_pit_sim(): callback returned: %s", t)
            sim_records.append(ret)
        self._in_sim = False
        self._sim_t = None
        return pd.Series(sim_records, index=ts)
