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
from vbase.core.commitment_service import CommitmentService
from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService
from vbase.core.web3_http_commitment_service_test import Web3HTTPCommitmentServiceTest
from vbase.core.forwarder_commitment_service import ForwarderCommitmentService
from vbase.core.forwarder_commitment_service_test import ForwarderCommitmentServiceTest


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
    def create_instance_from_json_descriptor(is_json: str) -> "IndexingService":
        """
        Creates an instance initialized from a JSON descriptor.
        This method is especially useful for constructing complex
        indexers using multiple commitment service defined using complex JSON.

        :param is_json: The JSON string with the initialization data.
        :return: The IndexingService created.
        """
        raise NotImplementedError()

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
        :return: The IndexingService created.
        """
        raise NotImplementedError()

    @staticmethod
    def create_instance_from_commitment_service(
        commitment_service: CommitmentService,
    ) -> "IndexingService":
        """
        Creates an instance initialized from a commitment service.
        Handles the complexities of initializing an IndexingService
        using a forwarded commitment service.
        We need to query this service for the information needed to connect to a commitment
        service directly, and this method abstracts this initialization.

        :param commitment_service: The commitment service used.
        :return: The IndexingService created.
        """
        if isinstance(
            commitment_service,
            (Web3HTTPCommitmentService, Web3HTTPCommitmentServiceTest),
        ):
            # This is the trivial case where we pass through the commitment_service.
            return Web3HTTPIndexingService([commitment_service])

        assert isinstance(
            commitment_service,
            (ForwarderCommitmentService, ForwarderCommitmentServiceTest),
        )
        # Query the forwarder commitment service for the JSON descriptor
        # required to init the Web3HTTPCommitmentService.
        # This allows us to create a local web3 commitment and indexing services
        # using data provided by the forwarder, eliminating additional settings
        # and ensuring consistency with the forwarder.
        commitment_service_data = commitment_service.get_commitment_service_data()
        if "node_rpc_url" not in commitment_service_data:
            raise ValueError("Forwarder did not return node_rpc_url.")
        if "commitment_service_address" not in commitment_service_data:
            raise ValueError("Forwarder did not return commitment_service_address.")
        return Web3HTTPIndexingService(
            [Web3HTTPCommitmentService(**commitment_service_data)]
        )

    def find_user_sets(self, user: str) -> List[dict]:
        """
        Returns the list of receipts for user set commitments
        for a given user.

        :param user: The address for the user who made the commitments.
        :return: The list of commitment receipts for all user set commitments.
        """
        raise NotImplementedError()

    def find_user_set_objects(self, user: str, set_cid: str) -> List[dict]:
        """
        Returns the list of receipts for user set object commitments
        for a given user and set CID.

        :param user: The address for the user who made the commitments.
        :param set_cid: The CID for the set containing the objects.
        :return: The list of commitment receipts for all user set object commitments.
        """
        raise NotImplementedError()

    def find_last_user_set_object(self, user: str, set_cid: str) -> Union[dict, None]:
        """
        Returns the last/latest receipt, if any, for user set object commitments
        for a given user and set CID.

        :param user: The address for the user who made the commitment.
        :param set_cid: The CID for the set containing the object.
        :return: The commitment receipt for the last/latest user set commitment.
        """
        raise NotImplementedError()

    def find_objects(self, object_cids: List[str], return_set_cids=False) -> List[dict]:
        """
        Returns the list of receipts for object commitments
        for a list of object CIDs.
        Finds and returns individual object commitments irrespective of the set
        they may have been committed to.

        :param object_cids: The CIDs for the objects to search.
        :param return_set_cids: If True, return the set CIDs, if any, for the objects.
        :return: The list of commitment receipts for all object commitments.
        """
        raise NotImplementedError()

    def find_object(self, object_cid: str, return_set_cids=False) -> List[dict]:
        """
        Returns the list of receipts for object commitments
        for a single object CID.
        Finds and returns individual object commitments irrespective of the set
        they may have been committed to.

        :param object_cid: The CID for the objects to search.
        :param return_set_cids: If True, return the set CIDs, if any, for the objects.
        :return: The list of commitment receipts for all object commitments.
        """
        raise NotImplementedError()

    def find_last_object(
        self, object_cid: str, return_set_cid=False
    ) -> Union[dict, None]:
        """
        Returns the last/latest receipt, if any, for object commitments.
        Finds and returns individual object commitment irrespective of the set
        it may have been committed to.

        :param object_cid: The CID for the object for search.
        :param return_set_cid: If True, return the set CIDs, if any, for the object.
        :return: The commitment receipt for the last/latest object commitment.
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
    def create_instance_from_json_descriptor(is_json: str) -> "Web3HTTPIndexingService":
        # Process the environment variable and create the defined commitment services.
        is_dict = json.loads(is_json)
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

    @staticmethod
    def create_instance_from_env_json_descriptor(
        dotenv_path: Union[str, None] = None
    ) -> "Web3HTTPIndexingService":
        # Load .env file if it exists.
        if dotenv_path:
            load_dotenv(dotenv_path, verbose=True, override=True)

        # We expect to find the environment variable defining the indexing service.
        is_json = os.getenv("VBASE_INDEXING_SERVICE_JSON_DESCRIPTOR")
        if is_json is None:
            raise EnvironmentError(
                "Missing required environment variable VBASE_INDEXING_SERVICE_JSON_DESCRIPTOR"
            )
        _LOG.info(
            "IndexingService.create_instance_from_env_json_descriptor(): "
            "VBASE_INDEXING_SERVICE_JSON_DESCRIPTOR =\n%s",
            pprint.pformat(is_json),
        )

        return Web3HTTPIndexingService.create_instance_from_json_descriptor(is_json)

    def find_user_sets(self, user: str) -> List[dict]:
        # Find events across all commitment services.
        receipts = []
        for cs in self.commitment_services:
            # Return chain_id with each receipt.
            # We may have multiple commitment services
            # connected to different chains and clients may not be able to uniquely
            # identify transactions without the chain_id.
            chain_id = cs.w3.eth.chain_id
            # Create the event filter for AddSetObject events.
            # For some reason Web3 does not convert set_cid to a byte strings,
            # so we must convert it explicitly.
            event_filter = cs.csc.events.AddSet.create_filter(
                fromBlock=0,
                argument_filters={
                    "user": user,
                },
            )
            # Retrieve and parse the events into commitment receipts.
            events = event_filter.get_all_entries()
            # A set commitment receipt comprises setCid.
            # Timestamp is is not part of set commitments on-chain data.
            # Data or objects have timestamps, but their container sets do not.
            # The first and last timestamps of a set are the timestamps
            # of the corresponding set object commitments.
            # To return set timestamps for UX and compatibility
            # we retrieve these from the transaction timestamps.
            cs_receipts = [
                {
                    "chainId": chain_id,
                    "transactionHash": bytes_to_hex_str_auto(event["transactionHash"]),
                    "user": user,
                    "setCid": bytes_to_hex_str(event["args"]["setCid"]),
                    # Get block timestamp for the event from the transaction receipt.
                    # This can be done more efficiently if we maintain a cache of block timestamps.
                    "timestamp": cs.convert_timestamp_chain_to_str(
                        cs.w3.eth.get_block(event["blockNumber"])["timestamp"]
                    ),
                }
                for event in events
            ]
            receipts += cs_receipts
        # end for cs in self.commitment_services

        # Sort receipts by timestamp.
        # This is essential since we may have iterated
        # over multiple commitment services above.
        receipts = sorted(receipts, key=lambda x: x["timestamp"])

        return receipts

    def find_user_set_objects(self, user: str, set_cid: str) -> List[dict]:
        # The operation is similar to find_user_sets.
        # We could factor out the common code, but an extra layer of abstraction
        # does not seem worth it for now since the filter and receipt construction
        # are slightly different.
        receipts = []
        for cs in self.commitment_services:
            # Return chain_id with each receipt.
            # We may have multiple commitment services
            # connected to different chains and clients may not be able to uniquely
            # identify transactions without the chain_id.
            chain_id = cs.w3.eth.chain_id
            event_filter = cs.csc.events.AddSetObject.create_filter(
                fromBlock=0,
                argument_filters={
                    "user": user,
                    "setCid": hex_str_to_bytes(set_cid),
                },
            )
            events = event_filter.get_all_entries()
            # A set object commitment receipt comprises setCid, objectCid, timestamp fields.
            cs_receipts = [
                {
                    "chainId": chain_id,
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
        # This is essential since we may have iterated
        # over multiple commitment services above.
        receipts = sorted(receipts, key=lambda x: x["timestamp"])

        return receipts

    def find_last_user_set_object(self, user: str, set_cid: str) -> Union[dict, None]:
        # TODO: This implementation is horribly inefficient.
        # There does not appear to be a simple and clean way
        # to get the latest event for a given filter on EVM blockchains.
        # Long-term, we will have to search for events after a given timestamp.
        # Longer-term, this will be superseded by higher-performance indexing services.
        receipts = self.find_user_set_objects(user, set_cid)
        return receipts[-1] if receipts is not None and len(receipts) > 0 else None

    def find_objects(self, object_cids: List[str], return_set_cids=False) -> List[dict]:
        # The operation is similar to find_user_sets.
        # We could factor out the common code, but an extra layer of abstraction
        # does not seem worth it for now since the filter and receipt construction
        # are slightly different.
        receipts = []
        # Find receipts across all commitment services.
        for cs in self.commitment_services:
            # Return chain_id with each receipt.
            # We may have multiple commitment services
            # connected to different chains and clients may not be able to uniquely
            # identify transactions without the chain_id.
            chain_id = cs.w3.eth.chain_id
            # Create the event filter for AddObject events.
            # For some reason Web3 does not convert object_cid to a byte strings,
            # so we must convert it explicitly.
            event_filter = cs.csc.events.AddObject.create_filter(
                fromBlock=0,
                argument_filters={
                    "objectCid": [
                        hex_str_to_bytes(object_cid) for object_cid in object_cids
                    ],
                },
            )
            events = event_filter.get_all_entries()
            # A commitment receipt comprises setCid, objectCid, timestamp fields.
            cs_receipts = [
                {
                    "chainId": chain_id,
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

        if return_set_cids:
            set_receipts = []
            # Find set commitments across all commitment services.
            # This is a substantially similar loop to the one above.
            for cs in self.commitment_services:
                chain_id = cs.w3.eth.chain_id
                event_filter = cs.csc.events.AddSetObject.create_filter(
                    fromBlock=0,
                    argument_filters={
                        "objectCid": [
                            hex_str_to_bytes(object_cid) for object_cid in object_cids
                        ],
                    },
                )
                events = event_filter.get_all_entries()
                # Retrieve a subset of the fields from the event
                # needed to join the object and set commitments.
                cs_receipts = [
                    {
                        "chainId": chain_id,
                        "transactionHash": bytes_to_hex_str_auto(
                            event["transactionHash"]
                        ),
                        "setCid": bytes_to_hex_str(event["args"]["setCid"]),
                        "objectCid": bytes_to_hex_str(event["args"]["objectCid"]),
                    }
                    for event in events
                ]
                set_receipts += cs_receipts
            # end for cs in self.commitment_services

            # Join the object and set object commitments.
            # Join receipts and set_receipts on (chainId, transactionHash, objectCid).
            # This is a simple O(n^2) join.
            # We could use a more efficient join by pre-sorting.
            # If this becomes a performance bottleneck, we will have to optimize.
            # This should not be necessary as the runtime should be dominated
            # by node RPC calls above.
            for receipt in receipts:
                for set_receipt in set_receipts:
                    # TODO: Consider a single tx with multiple (objectCid, setCid) commitments.
                    # This is not expected in the normal course of operation, but may change.
                    # Strictly speaking, consecutive events can be thus joined
                    # since they are emitted by a single addSetObject() call.
                    # We can handle this with an index for multiple (objectCid, setCid) commitments.
                    if (
                        receipt["chainId"] == set_receipt["chainId"]
                        and receipt["transactionHash"] == set_receipt["transactionHash"]
                        and receipt["objectCid"] == set_receipt["objectCid"]
                    ):
                        receipt["setCid"] = set_receipt["setCid"]
                        break
            # end for receipt in receipts
        # end if return_set_cids

        # Sort receipts by timestamp.
        # This is essential since we may have iterated
        # over multiple commitment services above.
        receipts = sorted(receipts, key=lambda x: x["timestamp"])

        return receipts

    def find_object(self, object_cid: str, return_set_cids=False) -> List[dict]:
        # Pass through to find_objects with a single object_cid.
        return self.find_objects([object_cid], return_set_cids)

    def find_last_object(
        self, object_cid: str, return_set_cid=False
    ) -> Union[dict, None]:
        # TODO: This implementation is horribly inefficient.
        # There does not appear to be a simple and clean way
        # to get the latest event for a given filter on EVM blockchains.
        # Long-term, we will have to search for events after a given timestamp.
        # Longer-term, this will be superseded by higher-performance indexing services.
        receipts = self.find_object(object_cid, return_set_cids=return_set_cid)
        return receipts[-1] if receipts is not None and len(receipts) > 0 else None
