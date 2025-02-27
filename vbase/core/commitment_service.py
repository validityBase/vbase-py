"""
The vbase commitment service module provides access to various commitment services
such as blockchain-based smart contracts.
"""

import logging
from typing import List, Union
from abc import ABC, abstractmethod

from vbase.utils.log import get_default_logger


_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


class CommitmentService(ABC):
    """
    Interface for base commitment operations    
    """

    @staticmethod
    @abstractmethod
    def get_init_args_from_env(dotenv_path: Union[str, None] = None) -> dict:
        """
        Worker function to load the environment variables.

        :param dotenv_path: The .env file path, if any.
        :return: The dictionary of construction arguments.
        """

    @staticmethod
    @abstractmethod
    def create_instance_from_env(
        dotenv_path: Union[str, None] = None
    ) -> "CommitmentService":
        """
        Creates an instance initialized from environment variables.
        Syntactic sugar for initializing new commitment objects using settings
        stored in a .env file or in environment variables.

        :param dotenv_path: Path to the .env file.
            Below is the default treatment that should be appropriate in most scenarios:
            - If called with no arguments, or if the default None dotenv_path is specified,
                default to the existing environment variables.
            - If dotenv_path is specified, attempt to load environment variables from the file.
        :return: The dictionary of arguments.
        """

    @abstractmethod
    def get_default_user(self) -> str:
        """
        Return the default user address used in vBase transactions.

        :return: The default user address used in vBase transactions.
        """

    @staticmethod
    @abstractmethod
    def convert_timestamp_str_to_chain(ts: str) -> int:
        """
        Convert a string representation of a Pandas timestamp
        to a chain timestamp returned by smart contract calls.

        :param ts: The pandas timestamp in string representation.
        :return: The chain timestamp returned by smart contract calls.
        """

    @staticmethod
    @abstractmethod
    def convert_timestamp_chain_to_str(ts: int) -> str:
        """
        Convert a chain timestamp returned by smart contract calls to a Pandas timestamp.

        :param ts: The chain timestamp returned by smart contract calls.
        :return: The pandas timestamp in string representation.
        """

    @abstractmethod
    def get_named_set_cid(self, name: str) -> str:
        """
        Returns a hash corresponding to a set name.
        Abstracts the hashing implementation from the upper layers

        :param name: The name of the set
        :return: The CID (hash) corresponding to the name.
        """

    @abstractmethod
    def add_set(self, set_cid: str) -> dict:
        """
        Records a set commitment.
        This is a low-level function that operates on set CIDs.
        It does not specify how a hash is built and does not provide
        a schema for hashing complex information.

        :param set_cid: The CID identifying the set.
        :return: The commitment log containing commitment receipt info.
        """

    @abstractmethod
    def user_set_exists(self, user: str, set_cid: str) -> bool:
        """
        Checks whether a given set exists for a user.

        :param user: The address for the user who recorded the commitment.
        :param set_cid: The CID identifying the set.
        :return: True if the set exists for the user; False otherwise.
        """

    @abstractmethod
    def verify_user_sets(self, user: str, user_set_cid_sum: str) -> bool:
        """
        Verifies all set commitments previously recorded by the user.
        This verifies all set commitments for completeness.
        The sum of all set CIDs for the user encodes the collection of all sets.
        This is a low-level function that operates on object hashes.

        :param user: The address for the user who recorded the commitment.
        :param user_set_cid_sum: The sum of all set CIDs for the user.
        :return: True if the commitment has been verified successfully;
            False otherwise.
        """

    @abstractmethod
    def add_object(self, object_cid: str) -> dict:
        """
        Record an object commitment.
        This is a low-level function that operates on object hashes.
        It does not specify how a hash is built and does not provide
        a schema for hashing complex information.

        :param object_cid: The CID identifying the object.
        :return: The commitment log containing commitment receipt info.
        """

    @abstractmethod
    def verify_user_object(self, user: str, object_cid: str, timestamp: str) -> bool:
        """
        Verifies an object commitment previously recorded.
        This is a low-level function that operates on object hashes.

        :param user: The address for the user who recorded the commitment.
        :param object_cid: The CID identifying the object.
        :param timestamp: The timestamp of the commitment.
        :return: True if the commitment has been verified successfully;
            False otherwise.
        """

    @abstractmethod
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

    @abstractmethod
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
        :return: The commitment logs containing commitment receipts.
        """

    @abstractmethod
    def add_set_objects_batch(self, set_cid: str, object_cids: List[str]) -> List[dict]:
        """
        Records a batch of commitments for objects belonging to a set.
        This is a low-level function that operates on set and object hashes.
        It does not specify how a hash is built and does not provide
        a schema for hashing complex information.

        :param set_cid: The hashes of the sets containing the objects.
        :param object_cids: The hashes to record.
        :return: The commitment logs containing commitment receipts.
        """

    @abstractmethod
    def verify_user_set_objects(
        self, user: str, set_cid: str, user_set_object_cid_sum: str
    ) -> bool:
        """
        Verifies an object commitment previously recorded.
        This is a low-level function that operates on object hashes.

        :param user: The address for the user who recorded the commitment.
        :param set_cid: The CID for the set containing the object.
        :param user_set_object_cid_sum: The sum of all object hashes for the user set
        :return: True if the commitment has been verified successfully;
            False otherwise.
        """
