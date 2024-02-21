"""
The vbase indexing service module provides access to various indexing services
for previously submitted commitments.
Such services enable queries of past commitments.
"""

from abc import ABC
import json
import logging
import os
import pprint
from typing import List, Union
from dotenv import load_dotenv

from vbase.utils.crypto_utils import (
    bytes_to_hex_str,
    bytes_to_hex_str_auto,
    hex_str_to_bytes,
)
from vbase.utils.log import get_default_logger
from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService
from vbase.core.web3_http_commitment_service_test import Web3HTTPCommitmentServiceTest


_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


# The indexing service will grow to have more features
# but currently has few methods.
# pylint: disable=too-few-public-methods
class IndexingService(ABC):
    """
    Base indexing operations.
    Various indexing services may provide a subset of the below operations that they support.
    """

    @staticmethod
    def create_instance_from_env_json_descriptor(
        dotenv_path: Union[str, None] = None
    ) -> "IndexingService":
        """
        Creates an instance initialized from an environment variable containing a JSON descriptor.
        Syntactic sugar for initializing a new indexing service object using settings
        stored in a .env file or in environment variables.
        This method is especially useful for constructing complex
        indexers using multiple commitment service defined using complex JSON.

        :param dotenv_path: Path to the .env file.
            If path is not specified, does not load the .env file.
        :return: The dictionary of arguments.
        """
        raise NotImplementedError()

    def find_user_set_objects(self, user: str, set_cid: str) -> List[dict]:
        """
        Returns the list of receipts for user set commitments.

        :param user: The address for the user who recorded the commitment.
        :param set_cid: The CID for the set containing the object.
        :return: The list of commitment receipts for all user set commitments.
        """
        raise NotImplementedError()

    def find_objects(self, object_cid: str) -> List[dict]:
        """
        Returns the list of receipts for object commitments.
        Find and returns individual object commitments irrespective of the set
        they may have been committed to.

        :param object_cid: The CID for the object.
        :return: The list of commitment receipts for all object commitments.
        """
        raise NotImplementedError()


# pylint: disable=too-few-public-methods
class Web3HTTPIndexingService(IndexingService):
    """
    Indexing service accessible using Web3.HTTPProvider.
    Wraps RPC node event indexing to support commitment indexing operations.
    """

    def __init__(self, commitment_services: List[Web3HTTPCommitmentService]):
        self.commitment_services = commitment_services

    @staticmethod
    def create_instance_from_env_json_descriptor(
        dotenv_path: Union[str, None] = None
    ) -> "Web3HTTPIndexingService":
        # Load .env file if it exists.
        if dotenv_path:
            load_dotenv(dotenv_path, verbose=True, override=True)

        # We expect to find the environment variable defining the indexing service.
        is_env_var = os.getenv("INDEXING_SERVICE_JSON_DESCRIPTOR")
        assert is_env_var is not None
        _LOG.info(
            "IndexingService.create_instance_from_env_json_descriptor(): "
            "INDEXING_SERVICE_JSON_DESCRIPTOR =\n%s",
            pprint.pformat(is_env_var),
        )

        # Process the environment variable and create the defined commitment services.
        is_dict = json.loads(is_env_var)
        cs_list = []
        for cs_dict in is_dict["commitment_services"]:
            _LOG.info(
                "IndexingService.create_instance_from_env_json_descriptor(): cs_dict =\n%s",
                pprint.pformat(cs_dict),
            )
            cs_class = cs_dict["class"]
            cs_init_args = cs_dict["init_args"]
            if cs_class == "Web3HTTPCommitmentService":
                cs_list.append(Web3HTTPCommitmentService(**cs_init_args))
            elif cs_class == "Web3HTTPCommitmentServiceTest":
                cs_list.append(Web3HTTPCommitmentServiceTest(**cs_init_args))
            else:
                raise NotImplementedError()

        # Initialize and return an indexing service with the above commitment services.
        indexing_service = Web3HTTPIndexingService(cs_list)
        return indexing_service

    def find_user_set_objects(self, user: str, set_cid: str) -> List[dict]:
        # Find events across all commitment services.
        receipts = []
        for cs in self.commitment_services:
            # Create the event filter for AddSetObject events.
            # For some reason Web3 does not convert set_cid to a byte strings,
            # so we must convert it explicitly.
            event_filter = cs.csc.events.AddSetObject.create_filter(
                fromBlock=0,
                argument_filters={
                    "user": user,
                    "setCid": hex_str_to_bytes(set_cid),
                },
            )
            # Retrieve and parse the events into commitment receipts.
            events = event_filter.get_all_entries()
            # A commitment receipt comprises setCid, objectCid, timestamp fields.
            cs_receipts = [
                {
                    "transactionHash": bytes_to_hex_str_auto(event["transactionHash"]),
                    "user": user,
                    "setCid": set_cid,
                    "objectCid": bytes_to_hex_str(event["args"]["objectCid"]),
                    "timestamp": cs.convert_timestamp_chain_to_str(
                        event["args"]["timestamp"]
                    ),
                }
                for event in events
            ]
            receipts += cs_receipts
        # end for cs in self.commitment_services

        # Sort receipts by timestamp.
        receipts = sorted(receipts, key=lambda x: x["timestamp"])

        return receipts

    def find_objects(self, object_cid: str) -> List[dict]:
        # Find events across all commitment services.
        receipts = []
        for cs in self.commitment_services:
            # Create the event filter for AddObject events.
            # For some reason Web3 does not convert object_cid to a byte strings,
            # so we must convert it explicitly.
            event_filter = cs.csc.events.AddObject.create_filter(
                fromBlock=0,
                argument_filters={
                    "objectCid": hex_str_to_bytes(object_cid),
                },
            )
            # Retrieve and parse the events into commitment receipts.
            events = event_filter.get_all_entries()
            # A commitment receipt comprises setCid, objectCid, timestamp fields.
            cs_receipts = [
                {
                    "transactionHash": bytes_to_hex_str_auto(event["transactionHash"]),
                    "user": event["args"]["user"],
                    "objectCid": bytes_to_hex_str(event["args"]["objectCid"]),
                    "timestamp": cs.convert_timestamp_chain_to_str(
                        event["args"]["timestamp"]
                    ),
                }
                for event in events
            ]
            receipts += cs_receipts
        # end for cs in self.commitment_services
        return receipts
