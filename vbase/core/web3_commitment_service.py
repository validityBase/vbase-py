"""
The vbase commitment service module captures the common operations
of the various commitment services provided by blockchain-based smart contracts
and the Web3 interface.
Particular implementations may access the blockchain directly
or via a forwarder service.
"""

import logging
from abc import ABC
from typing import List, Type, Union
from beeprint import pp
import pandas as pd
from web3 import Web3
from web3.contract import Contract
from web3.types import TxReceipt

from vbase.core.commitment_service import CommitmentService
from vbase.utils.log import get_default_logger
from vbase.utils.crypto_utils import bytes_to_hex_str


_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


class Web3CommitmentService(CommitmentService, ABC):
    """
    Commitment service accessible using Web3 library
    either directly or via a forwarder.
    """

    def __init__(
        self, w3: Web3, commitment_service_contract: Union[Type[Contract], Contract]
    ):
        self.w3 = w3
        self.csc = commitment_service_contract

    def get_default_user(self) -> str:
        if self.w3.eth.default_account:
            # If the default_account was initialized using private_key above,
            # it will be used in txs.
            return self.w3.eth.default_account
        # If the default account was not initialized,
        # we are using a test node,
        # and one of the built-in accounts will be used.
        return self.w3.eth.accounts[0]

    def get_named_set_cid(self, name: str) -> str:
        return self.w3.solidity_keccak(abi_types=["string"], values=[name]).hex()

    @staticmethod
    def convert_timestamp_str_to_chain(ts: str) -> int:
        return int(pd.Timestamp(ts).timestamp())

    @staticmethod
    def convert_timestamp_chain_to_str(ts: int) -> str:
        # Ethereum and Web3 use UTC as the time zone.
        return str(pd.Timestamp(ts, unit="s", tz="UTC"))

    @staticmethod
    def _check_tx_success(receipt):
        if receipt is None:
            _LOG.error("Transaction failed with receipt = None")
        assert receipt is not None
        _LOG.debug("Transaction receipt:")
        _LOG.debug(pp(dict(receipt), output=False))
        if not receipt["status"]:
            _LOG.error("Transaction failed with receipt:")
            _LOG.error(pp(dict(receipt), output=False))
        assert receipt["status"]

    def _add_set_worker(self, set_cid: str, receipt: TxReceipt) -> dict:
        """
        Process results of a addSect transaction.

        :param set_cid: The CID (hash) identifying the set to add.
        :param receipt: The transaction receipt.
        :return: The commitment log containing commitment receipt info.
        """
        self._check_tx_success(receipt)
        event_data = self.csc.events.AddSet().process_receipt(receipt)

        # addSet() is idempotent.
        # If the user set has been added in the past,
        # subsequent calls will be ignored, and no event will be generated.
        if len(event_data) > 0:
            # Convert bytestrings to strings to allow serialization for the upper layers.
            # Note that HexBytes is also not JSON serializable.
            cl = dict(event_data[0]["args"])
            cl["setCid"] = bytes_to_hex_str(cl["setCid"])
        else:
            # Return an empty commitment log.
            cl = {}

        # Confirm that the set exists following the completed commitment.
        # This is useful for catching any transaction failures
        # or errors in the node infrastructure to report failures properly.
        # Note that this may be a forwarded transaction.
        # In this case, the receipt["from"] is the forwarder/relayer address
        # and cl["user"] is the user address for the commitment.
        assert self.user_set_exists(self.get_default_user(), set_cid)

        _LOG.debug("Commitment log:")
        _LOG.debug(pp(cl, output=False))
        return cl

    def _add_object_worker(self, receipt: TxReceipt) -> dict:
        """
        Process results of a addObject transaction.

        :param receipt: The transaction receipt.
        :return: The commitment log containing commitment receipt info.
        """
        self._check_tx_success(receipt)

        event_data = self.csc.events.AddObject().process_receipt(receipt)

        cl = dict(event_data[0]["args"])
        # Convert bytestring to string
        # to allow serialization for the upper layers.
        cl["objectCid"] = bytes_to_hex_str(cl["objectCid"])
        # Convert timestamp to the string representation of the Pandas object
        # to allow serialization for the upper layers.
        cl["timestamp"] = self.convert_timestamp_chain_to_str(cl["timestamp"])

        _LOG.debug("Commitment log:")
        _LOG.debug(pp(cl, output=False))
        return cl

    def _add_set_object_worker(self, receipt: TxReceipt) -> dict:
        """
        Process addSetObject transaction results.

        :param receipt: Transaction receipt returned by AddSetObject.
        :return: The commitment log containing commitment receipt info.
        """
        self._check_tx_success(receipt)

        # The call emits the following events:
        # AddSetObject(user, setCid, objectCid, timestamp)
        # AddObject(user, objectCid, timestamp)
        # Return the object commitment log from the 2nd event.
        event_data = self.csc.events.AddObject().process_log(receipt["logs"][1])

        # Prepare the commitment log using the returned event data.
        cl = dict(event_data["args"])
        # Convert bytestring to string
        # to allow serialization for the upper layers.
        cl["objectCid"] = bytes_to_hex_str(cl["objectCid"])
        # Convert timestamp to the string representation of the Pandas object
        # to allow serialization for the upper layers.
        cl["timestamp"] = self.convert_timestamp_chain_to_str(cl["timestamp"])

        _LOG.debug("Commitment log:")
        _LOG.debug(pp(cl, output=False))
        return cl

    def _add_sets_objects_batch_worker(self, receipt: TxReceipt) -> List[dict]:
        """
        Process addSetObjectWithTimestampBatch transaction results.

        :param receipt: Transaction receipt returned by AddSetsObjectsBatch.
        :return: The list of commitment log containing commitment receipts.
        """
        self._check_tx_success(receipt)

        # The commitment calls emit AddUserSetObject and AddObject events.
        # Return the object commitment log from the 2nd event.
        l_cls = []
        for i, log in enumerate(receipt["logs"]):
            if i % 2 == 0:
                continue
            event_data = self.csc.events.AddObject().process_log(log)
            # Convert bytestrings to strings to allow serialization for the
            # upper layers.
            cl = dict(event_data["args"])
            # Convert bytestring to string to allow serialization for the upper
            # layers.
            cl["objectCid"] = bytes_to_hex_str(cl["objectCid"])
            # Convert timestamp to the string representation of the Pandas object
            # to allow serialization for the upper layers.
            cl["timestamp"] = self.convert_timestamp_chain_to_str(cl["timestamp"])
            l_cls.append(cl)

        _LOG.debug("Commitment logs:")
        _LOG.debug(pp(l_cls, output=False))
        return l_cls
