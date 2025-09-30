"""
The vbase commitment service module provides access to various commitment services
such as blockchain-based smart contracts.
This implementation uses Web3.HTTPProvider.
"""

import json
import logging
import os
import pprint
import time
from typing import List, Optional, Union
from dotenv import load_dotenv
from web3 import Web3
from web3.middleware import (
    buffered_gas_estimate_middleware,
    construct_sign_and_send_raw_middleware,
    geth_poa_middleware,
)

from vbase.utils.log import get_default_logger
from vbase.core.web3_commitment_service import Web3CommitmentService
from vbase.utils.crypto_utils import hex_str_to_bytes, hex_str_to_int
from vbase.utils.error_utils import check_for_missing_env_vars


_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


# Settings for the connection retry for Web3.HTTPProvider.
# Maximum number of retries.
_W3_CONNECTION_MAX_RETRIES = 5
# Linear backoff in seconds.
_W3_CONNECTION_BACKOFF = 1


class Web3HTTPCommitmentService(Web3CommitmentService):
    """
    Commitment service accessible using Web3.HTTPProvider.
    Without private key support, this class will only support operations on a test node.
    """

    @staticmethod
    def _get_bool_env_var(var_name, default=False):
        """
        Worker function to get a bool environment variable.

        :param var_name: The environment variable name.
        :param default: The default value to return.
        :return: The environment variable value.
        """
        val = os.environ.get(var_name)
        if val is None:
            return default
        return val.lower() in ["true", "1", "t", "y", "yes"]

    # pylint: disable-msg=too-many-arguments
    def __init__(
        self,
        node_rpc_url: str,
        commitment_service_address: str,
        private_key: Optional[str] = None,
        commitment_service_json_file_name: Optional[str] = "CommitmentService.json",
        inject_geth_poa_middleware: bool = False,
    ):
        """
        Initialize the service object.

        :param node_rpc_url: Node RPC URL.
        :param commitment_service_address: The commitment smart contract address.
        :param private_key: User's private key.
            Can be omitted when using a test node that supports eth_sendTransaction.
            Must be specified when using a node that only supports eth_sendRawTransaction,
            such as hosted node services.
        :param commitment_service_json_file_name: File name for the JSON file
            containing the CommitmentService smart contract's ABI.
        :param inject_geth_poa_middleware: True if geth_poa_middleware W3 option
            is required to connect to the network.
            This option is required for Polygon PoS, BNB, and other chains:
            https://web3py.readthedocs.io/en/stable/middleware.html#proof-of-authority
        """
        self.node_rpc_url = node_rpc_url
        self.commitment_service_address = commitment_service_address

        # Connect to the node with retries and backoff.
        retry_count = 0
        backoff = 0
        while retry_count < _W3_CONNECTION_MAX_RETRIES:
            try:
                w3 = Web3(Web3.HTTPProvider(self.node_rpc_url))
                if w3.is_connected():
                    return w3
                raise ConnectionError(f"is_connected() returned False for {self.node_rpc_url}")
            except ConnectionError as e:
                if retry_count >= _W3_CONNECTION_MAX_RETRIES - 1:
                    _LOG.error(
                        "Web3HTTPCommitmentService.__init__(): "
                        "Exception connecting to %s: %s",
                        self.node_rpc_url,
                        e,
                    )
                retry_count += 1
                backoff += _W3_CONNECTION_BACKOFF
                time.sleep(backoff)

        if not w3.is_connected():
            raise ConnectionError(
                f"Failed to connect to {self.node_rpc_url} after {retry_count} retries"
            )

        if inject_geth_poa_middleware:
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)

        # Initialize the account, if necessary.
        # If the account is not initialized, transaction will be sent using eth_sendTransaction.
        # This method is supported by test nodes.
        # If the private key is specified, transactions will be signed and sent
        # using eth_sendRawTransaction.
        if private_key is not None:
            acct = w3.eth.account.from_key(private_key)
            w3.middleware_onion.add(construct_sign_and_send_raw_middleware(acct))
            w3.eth.default_account = acct.address

        # Add gas buffer middleware to ensure txs do not run out of gas
        # due to a missed estimate.
        # This seems to happen occasionally on Polygon PoS.
        # Gas estimate middleware needs to run before signing.
        # Currently, Web3 has a number of bugs in middleware paths.
        # We work around them by adding middleware after signing.
        w3.middleware_onion.add(buffered_gas_estimate_middleware)

        # Connect to the contract.
        # Web3 library is fussy about the address parameter type.
        # noinspection PyTypeChecker
        with self.get_commitment_service_json_file(
            commitment_service_json_file_name
        ) as f:
            commitment_service_contract = w3.eth.contract(
                address=w3.to_checksum_address(self.commitment_service_address),
                abi=json.load(f)["abi"],
            )

        super().__init__(w3, commitment_service_contract)

    @staticmethod
    def get_init_args_from_env(dotenv_path: Union[str, None] = None) -> dict:
        # Load .env file if it exists.
        if dotenv_path:
            load_dotenv(dotenv_path, verbose=True, override=True)
        init_args = {
            "node_rpc_url": os.getenv("VBASE_COMMITMENT_SERVICE_NODE_RPC_URL"),
            "commitment_service_address": os.getenv("VBASE_COMMITMENT_SERVICE_ADDRESS"),
            "private_key": os.getenv("VBASE_COMMITMENT_SERVICE_PRIVATE_KEY"),
            "inject_geth_poa_middleware": Web3HTTPCommitmentService._get_bool_env_var(
                os.getenv(
                    "VBASE_COMMITMENT_SERVICE_INJECT_GETH_POA_MIDDLEWARE",
                    default="False",
                )
            ),
        }
        # Check for missing environment variables since these are unrecoverable.
        check_for_missing_env_vars(init_args)
        _LOG.debug(
            "Web3HTTPCommitmentService.get_init_args_from_env(): init_args =\n%s",
            pprint.pformat(init_args),
        )
        return init_args

    @staticmethod
    def create_instance_from_env(
        dotenv_path: Union[str, None] = None
    ) -> "Web3HTTPCommitmentService":
        return Web3HTTPCommitmentService(
            **Web3HTTPCommitmentService.get_init_args_from_env(dotenv_path)
        )

    def add_set(self, set_cid: str) -> dict:
        _LOG.debug("Sending transaction to addSet")
        tx_hash = self.csc.functions.addSet(
            # Strings get converted to bytes32 by Web3.
            set_cid
        ).transact()
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return self._add_set_worker(set_cid, receipt)

    def user_set_exists(self, user: str, set_cid: str) -> bool:
        return self.csc.functions.userSetCommitments(
            user, hex_str_to_bytes(set_cid)
        ).call()

    def verify_user_sets(self, user: str, user_set_cid_sum: str) -> bool:
        return self.csc.functions.verifyUserSets(
            user,
            # Convert string to uint256.
            hex_str_to_int(user_set_cid_sum),
        ).call()

    def add_object(self, object_cid: str) -> dict:
        _LOG.debug("Sending transaction to addObject")
        tx_hash = self.csc.functions.addObject(object_cid).transact()
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return self._add_object_worker(receipt)

    def verify_user_object(self, user: str, object_cid: str, timestamp: str) -> bool:
        return self.csc.functions.verifyUserObject(
            user,
            # Convert strings to bytes.
            hex_str_to_bytes(object_cid),
            self.convert_timestamp_str_to_chain(timestamp),
        ).call()

    def add_set_object(self, set_cid: str, object_cid: str) -> dict:
        _LOG.debug("Sending transaction to addSetObject")
        tx_hash = self.csc.functions.addSetObject(
            # Strings get converted to bytes32 by Web3.
            set_cid,
            object_cid,
        ).transact()
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return self._add_set_object_worker(receipt)

    def add_sets_objects_batch(
        self, set_cids: List[str], object_cids: List[str]
    ) -> List[dict]:
        _LOG.debug("Sending transaction to addSetsObjectsBatch")
        tx_hash = self.csc.functions.addSetsObjectsBatch(
            # Strings get converted to bytes32 by Web3.
            set_cids,
            object_cids,
        ).transact()
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return self._add_sets_objects_batch_worker(receipt)

    def add_set_objects_batch(self, set_cid: str, object_cids: List[str]) -> List[dict]:
        _LOG.debug("Sending transaction to addSetObjectsBatch")
        tx_hash = self.csc.functions.addSetObjectsBatch(
            # Strings get converted to bytes32 by Web3.
            set_cid,
            object_cids,
        ).transact()
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return self._add_sets_objects_batch_worker(receipt)

    def verify_user_set_objects(
        self, user: str, set_cid: str, user_set_object_cid_sum: str
    ) -> bool:
        return self.csc.functions.verifyUserSetObjectsCidSum(
            user,
            # Convert strings to bytes.
            hex_str_to_bytes(set_cid),
            hex_str_to_int(user_set_object_cid_sum),
        ).call()
