"""The vbase commitment service module captures the common operations
of the various commitment services provided by blockchain-based smart contracts
and the Web3 interface.
Particular implementations may access the blockchain directly
or via a forwarder service.
"""

import logging
import os
import pathlib
import pprint
from abc import ABC
from io import TextIOWrapper
from typing import List, Optional, Type, Union

import pandas as pd
from web3 import Web3
from web3.contract import Contract
from web3.contract.base_contract import EventLogErrorFlags
from web3.types import TxReceipt

from vbase.core.commitment_service import CommitmentService
from vbase.utils.crypto_utils import bytes_to_hex_str, hash_typed_values
from vbase.utils.log import get_default_logger

_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


class Web3CommitmentService(CommitmentService, ABC):
    """Commitment service accessible using Web3 library
    either directly or via a forwarder.
    """

    def __init__(
        self, w3: Web3, commitment_service_contract: Union[Type[Contract], Contract]
    ):
        self.w3 = w3
        self.csc = commitment_service_contract

    @staticmethod
    def get_commitment_service_json_file(
        commitment_service_json_file_name: Optional[str] = "CommitmentService.json",
    ) -> TextIOWrapper:
        """Return the file object for the JSON commitment service ABI.

        :param commitment_service_json_file_name: File name for the JSON file
            containing the CommitmentService smart contract's ABI.
        :return: File object for the JSON file.
        """
        return open(
            os.path.join(
                pathlib.Path(__file__).parent.resolve(),
                "abi",
                commitment_service_json_file_name,
            ),
            encoding="utf-8",
        )

    def get_default_user(self) -> str:
        if self.w3.eth.default_account:
            # If the default_account was initialized using private_key above,
            # it will be used in txs.
            return self.w3.eth.default_account
        # If the default account was not initialized,
        # we are using a test node,
        # and one of the built-in accounts will be used.
        return self.w3.eth.accounts[0]

    @staticmethod
    def get_named_set_cid(name: str) -> str:
        return hash_typed_values(abi_types=["string"], values=[name])

    @staticmethod
    def convert_timestamp_str_to_chain(ts: str) -> int:
        return int(pd.Timestamp(ts).timestamp())

    @staticmethod
    def convert_timestamp_chain_to_str(ts: int) -> str:
        # Ethereum and Web3 use UTC as the time zone.
        return str(pd.Timestamp(ts, unit="s", tz="UTC"))

    def _get_chain_id(self) -> int:
        """Return the chain ID for the connected network.

        Subclasses that do not have direct node access (e.g. ForwarderCommitmentService)
        override this to derive the chain ID from their signature data instead.
        """
        return self.w3.eth.chain_id

    @staticmethod
    def _check_tx_success(receipt):
        if receipt is None:
            msg = "Transaction failed with receipt = None"
            _LOG.error(msg)
            # TODO: Eventually we should add vBase exception classes.
            # These will allow us to handle specific exceptions
            # and potentially retry.
            raise RuntimeError(msg)
        _LOG.debug("Transaction receipt:\n%s", pprint.pformat(dict(receipt)))
        if not receipt["status"]:
            msg = f"Transaction failed with receipt: {pprint.pformat(dict(receipt))}"
            _LOG.error(msg)
            raise RuntimeError(msg)

    def _add_set_worker(self, set_cid: str, receipt: TxReceipt) -> dict:
        """Process results of a addSect transaction.

        :param set_cid: The CID (hash) identifying the set to add.
        :param receipt: The transaction receipt.
        :return: The commitment log containing commitment receipt info.
        """
        self._check_tx_success(receipt)

        # addSet() is idempotent: if already added, no event is emitted.
        # On some chains other events may be emitted (e.g. LogFeeTransfer), so
        # locate AddSet by scanning the receipt; mismatched logs are silently discarded.
        add_set_events = self.csc.events.AddSet().process_receipt(
            receipt, errors=EventLogErrorFlags.Discard
        )
        if add_set_events:
            cl = dict(add_set_events[0]["args"])
            # Convert bytestrings to strings to allow serialization for upper layers.
            cl["setCid"] = bytes_to_hex_str(cl["setCid"])
            cl["transactionHash"] = receipt["transactionHash"]
            # Expose committer as userAddress for API consumers.
            if "user" in cl:
                cl["userAddress"] = str(cl["user"])
            cl["chainId"] = self._get_chain_id()
            # To return set timestamps for UX and compatibility
            # we would need to retrieve these from the transaction timestamps.
            # This worker function is called by the ForwarderCommitmentService
            # that does not have direct access to the node.
            # Thus, getting the timestamp requires additional calls
            # to initialize a w3 instance and access the node.
            # Since there is currently no consumers for this timestamp, we do not return it.
            # Conceptually, sets are containers for objects
            # and we need to expose object, not set, timestamps.
        else:
            # No event means addSet was a no-op (set already existed).
            cl = {}

        # Confirm that the set exists following the completed commitment.
        # This is useful for catching any transaction failures
        # or errors in the node infrastructure to report failures properly.
        # Note that this may be a forwarded transaction.
        # In this case, the receipt["from"] is the forwarder/relayer address
        # and cl["user"] is the user address for the commitment.
        assert self.user_set_exists(self.get_default_user(), set_cid)

        _LOG.debug("Commitment log:\n%s", pprint.pformat(cl))
        return cl

    def _add_object_worker(self, receipt: TxReceipt) -> dict:
        """Process results of a addObject transaction.

        :param receipt: The transaction receipt.
        :return: The commitment log containing commitment receipt info.
        """
        self._check_tx_success(receipt)

        # AddObject event should always be emitted on success.
        # On some chains other events may be emitted (e.g. LogFeeTransfer), so
        # locate AddObject by scanning the receipt; mismatched logs are silently discarded.
        add_object_events = self.csc.events.AddObject().process_receipt(
            receipt, errors=EventLogErrorFlags.Discard
        )
        if not add_object_events:
            raise RuntimeError("AddObject event not found in receipt")
        # process_receipt() returns EventData (AttributeDict) objects;
        # dict-style ["args"] access always works.
        cl = dict(add_object_events[0]["args"])
        # Convert bytestring to string
        # to allow serialization for the upper layers.
        cl["objectCid"] = bytes_to_hex_str(cl["objectCid"])
        # Convert timestamp to the string representation of the Pandas object
        # to allow serialization for the upper layers.
        cl["timestamp"] = self.convert_timestamp_chain_to_str(cl["timestamp"])
        cl["transactionHash"] = receipt["transactionHash"]
        # Expose committer as userAddress for API consumers.
        if "user" in cl:
            cl["userAddress"] = str(cl["user"])
        cl["chainId"] = self._get_chain_id()

        _LOG.debug("Commitment log:\n%s", pprint.pformat(cl))
        return cl

    def _add_set_object_worker(self, receipt: TxReceipt) -> dict:
        """Process addSetObject transaction results.

        :param receipt: Transaction receipt returned by AddSetObject.
        :return: The commitment log containing commitment receipt info.
        """
        self._check_tx_success(receipt)

        # Events should always be emitted on success.

        # The call emits AddSetObject and AddObject; on some chains other events
        # (e.g. LogFeeTransfer) may be emitted, so locate by scanning the receipt;
        # mismatched logs are silently discarded.
        add_set_object_events = self.csc.events.AddSetObject().process_receipt(
            receipt, errors=EventLogErrorFlags.Discard
        )
        add_object_events = self.csc.events.AddObject().process_receipt(
            receipt, errors=EventLogErrorFlags.Discard
        )
        if not add_set_object_events or not add_object_events:
            raise RuntimeError("AddSetObject or AddObject event not found in receipt")
        # process_receipt() returns EventData (AttributeDict) objects;
        # dict-style ["args"] access always works.
        add_set_args = add_set_object_events[0]["args"]

        # Prepare the commitment log using the returned event data.
        cl = dict(add_object_events[0]["args"])
        # Convert bytestring to string
        # to allow serialization for the upper layers.
        cl["objectCid"] = bytes_to_hex_str(cl["objectCid"])
        # Convert timestamp to the string representation of the Pandas object
        # to allow serialization for the upper layers.
        cl["timestamp"] = self.convert_timestamp_chain_to_str(cl["timestamp"])
        cl["transactionHash"] = receipt["transactionHash"]
        # Expose committer as userAddress for API consumers.
        if "user" in cl:
            cl["userAddress"] = str(cl["user"])
        # Include setCid from AddSetObject event for stamp-with-collection responses.
        cl["setCid"] = bytes_to_hex_str(add_set_args["setCid"])
        cl["chainId"] = self._get_chain_id()

        _LOG.debug("Commitment log:\n%s", pprint.pformat(cl))
        return cl

    def _add_sets_objects_batch_worker(self, receipt: TxReceipt) -> List[dict]:
        """Process addSetObjectWithTimestampBatch transaction results.

        :param receipt: Transaction receipt returned by AddSetsObjectsBatch.
        :return: The list of commitment log containing commitment receipts.
        """
        self._check_tx_success(receipt)

        # Events should always be emitted on success.

        # The call emits one AddObject per committed object. On some chains other events
        # (e.g. LogFeeTransfer) may be emitted; process_receipt() filters by event
        # signature so only AddObject events are returned, in log order.
        add_object_events = self.csc.events.AddObject().process_receipt(
            receipt, errors=EventLogErrorFlags.Discard
        )
        # Hoist constants that are the same for every item in the batch.
        tx_hash = receipt["transactionHash"]
        chain_id = self._get_chain_id()
        l_cls = []
        for event_data in add_object_events:
            # process_receipt() returns EventData (AttributeDict) objects;
            # dict-style ["args"] access always works.
            cl = dict(event_data["args"])
            # Convert bytestrings to strings to allow serialization for upper layers.
            cl["objectCid"] = bytes_to_hex_str(cl["objectCid"])
            # Convert timestamp to the string representation of the Pandas object
            # to allow serialization for the upper layers.
            cl["timestamp"] = self.convert_timestamp_chain_to_str(cl["timestamp"])
            cl["transactionHash"] = tx_hash
            # Expose committer as userAddress for API consumers.
            if "user" in cl:
                cl["userAddress"] = str(cl["user"])
            cl["chainId"] = chain_id
            l_cls.append(cl)

        _LOG.debug("Commitment logs:\n%s", pprint.pformat(l_cls))
        return l_cls
