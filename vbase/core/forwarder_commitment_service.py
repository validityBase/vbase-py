"""
The vbase commitment service module provides access to various commitment services
such as blockchain-based smart contracts.
This implementation uses a forwarder to execute meta-transactions on a user's behalf.
"""

import json
import logging
import os
import pathlib
from enum import Enum
from typing import List, Optional, Union
import pprint
import requests
from eth_account import Account
from eth_account.messages import encode_structured_data
from hexbytes import HexBytes
from dotenv import load_dotenv
from web3 import Web3

from vbase.core.web3_commitment_service import Web3CommitmentService
from vbase.utils.crypto_utils import hex_str_to_bytes
from vbase.utils.log import get_default_logger


_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


# Default timeout for HTTP requests.
# The timeout needs to be long enough to handle tx
# submission and execution.
_REQUEST_TIMEOUT = 30.0


class RequestType(Enum):
    """
    Encodes the request type with type safety.
    """

    GET = "GET"
    POST = "POST"


class ForwarderCommitmentService(Web3CommitmentService):
    """
    Commitment service accessible using a forwarder API endpoint.
    """

    # pylint: disable-msg=too-many-arguments
    def __init__(
        self,
        endpoint_url: str,
        api_key: str,
        private_key: Optional[str] = None,
        commitment_service_json_file_name: Optional[str] = "CommitmentService.json",
    ):
        """
        Initialize the service object.

        :param endpoint_url: The forwarder endpoint URL.
        :param api_key: The API_KEY used to authenticate to the forwarder.
        :param private_key: User's private key.
        :param commitment_service_json_file_name: File name for the JSON file
            containing the CommitmentService smart contract's ABI.
        """
        self.endpoint_url = endpoint_url
        self.api_key = api_key
        self.private_key = private_key

        # Create the w3 object.
        # We will never interact with a node but will use the w3 object
        # to build and sign meta-transactions.
        # Consequently, we do not need to connect to a node.
        # The Web3 object is assigned in the parent constructor.
        w3 = Web3()

        # Initialize the account.
        # We are sending meta-transactions using the forwarder
        # and will use the account to sign the payload.
        acct = w3.eth.account.from_key(private_key)
        w3.eth.default_account = acct.address

        # Connect to the contract.
        # Web3 library is fussy about the address parameter type.
        # noinspection PyTypeChecker
        with open(
            os.path.join(
                pathlib.Path(__file__).parent.resolve(),
                "abi",
                commitment_service_json_file_name,
            ),
            encoding="utf-8",
        ) as f:
            commitment_service_contract = w3.eth.contract(abi=json.load(f)["abi"])

        super().__init__(w3, commitment_service_contract)

    @staticmethod
    def get_init_args_from_env(dotenv_path: Union[str, None] = None) -> dict:
        # Load .env file if it exists.
        if dotenv_path:
            load_dotenv(dotenv_path, verbose=True, override=True)
        init_args = {
            "endpoint_url": os.getenv("FORWARDER_ENDPOINT_URL"),
            "api_key": os.getenv("FORWARDER_API_KEY"),
            "private_key": os.getenv("PRIVATE_KEY"),
        }
        _LOG.debug(
            "ForwarderCommitmentService.get_init_args_from_env(): init_args =\n%s",
            pprint.pformat(init_args),
        )
        return init_args

    @staticmethod
    def create_instance_from_env(
        dotenv_path: Union[str, None] = None
    ) -> "ForwarderCommitmentService":
        return ForwarderCommitmentService(
            **ForwarderCommitmentService.get_init_args_from_env(dotenv_path)
        )

    def _call_forwarder_api(
        self,
        api: str,
        request_type: RequestType = RequestType.GET,
        params: Union[dict, None] = None,
        data: Union[dict, None] = None,
    ) -> Union[dict, str, None]:
        """
        Call a forwarded web API and return the response.

        :param api: The forwarder api.
        :param request_type: The request type.
        :param params: The request parameters.
        :return: The returned data.
        """
        if params is None:
            params = {}
        if data is None:
            data = {}
        try:
            if request_type == RequestType.GET:
                request_function = requests.get
            else:
                assert request_type == RequestType.POST
                request_function = requests.post
            response = request_function(
                url=self.endpoint_url + api,
                params={**{"from": self.get_default_user()}, **params},
                headers={"X-API-KEY": self.api_key},
                json=data,
                timeout=_REQUEST_TIMEOUT,
            )

            # Check if the request was successful.
            response.raise_for_status()
            response_json = response.json()
            if not response_json["success"]:
                raise requests.RequestException(response_json["log"])
            response_data = response_json["data"]

        except requests.HTTPError as http_err:
            _LOG.error("HTTP error occurred: %s", http_err)
            raise http_err
        except requests.RequestException as req_err:
            _LOG.error("Request error occurred: %s", req_err)
            raise req_err
        except ValueError as err:
            _LOG.error("Invalid JSON received!")
            raise err

        return response_data

    @staticmethod
    def _parse_object_pairs(obj: any) -> any:
        """
        Custom iterator to parse transaction receipts.

        :param obj: The object to parse.
        :return: The parsed object.
        """
        obj_dict = dict(obj)
        return {
            k: (int(v[:-1]) if isinstance(v, str) and v.endswith("n") else v)
            for k, v in obj_dict.items()
        }

    @staticmethod
    def _convert_string_numbers(data: any) -> any:
        """
        Recursively convert the object
        replacing numbers stored as strings terminating with "n" to numbers.

        :param data: The object to convert.
        :return: The converted object.
        """
        if isinstance(data, dict):
            # Recursively process each key-value pair of a dict.
            return {
                key: ForwarderCommitmentService._convert_string_numbers(value)
                for key, value in data.items()
            }
        if isinstance(data, list):
            # Recursively process each element of a list.
            return [
                ForwarderCommitmentService._convert_string_numbers(item)
                for item in data
            ]
        if isinstance(data, str) and data.endswith("n"):
            # If the data is a string ending with 'n', try to convert it to a number.
            try:
                return int(data[:-1])
            except ValueError:
                pass
        return data

    @staticmethod
    def _convert_receipt_logs(receipt: dict) -> dict:
        """
        Convert receipt logs.

        :param receipt: Transaction receipt returned by _post_execute().
        :return: The converted receipt data.
        """
        # Convert logs from the unmarshalled format to the format expected by
        # the downstream event parser.
        for i_log, log in enumerate(receipt["logs"]):
            receipt["logs"][i_log]["data"] = HexBytes(log["data"])
            for i_topic, topic in enumerate(log["topics"]):
                receipt["logs"][i_log]["topics"][i_topic] = HexBytes(topic)
        return receipt

    def _post_execute(self, fn_name: str, args: []):
        """
        Call a forwarded web API and return the response.

        :param fn_name: The smart contract function name to call.
        :param args: The arguments to the smart contract function.
        :return: The returned data.
        """
        # Get signature data from the API endpoint.
        signature_data: dict = self._call_forwarder_api("signature-data")
        # Validate that signature_data is as expected for this call.
        if signature_data is None or not isinstance(signature_data, dict):
            raise ValueError("Unexpected signature_data")
        if "domain" not in signature_data:
            raise ValueError("Missing domain field of signature_data")
        if "chainId" not in signature_data["domain"]:
            raise ValueError('Missing chainId field of signature_data["domain"]')

        # Convert chainId to integer to make it compatible with consumer APIs.
        # Technically it is an uint256 and is sent as a string.
        signature_data["domain"]["chainId"] = int(signature_data["domain"]["chainId"])

        # Encode the CommitmentService smart contract call.
        function_data = self.csc.encodeABI(fn_name=fn_name, args=args)
        # Sign the meta-transaction for the call.
        # TODO: Fix: DeprecationWarning: `encode_structured_data` is deprecated
        # and will be removed in a future release. Use encode_typed_data instead.
        signable_message = encode_structured_data(
            {
                "types": {
                    "EIP712Domain": [
                        {"name": "name", "type": "string"},
                        {"name": "version", "type": "string"},
                        {"name": "chainId", "type": "uint256"},
                        {"name": "verifyingContract", "type": "address"},
                    ],
                    "ForwardRequest": [
                        {"name": "from", "type": "address"},
                        {"name": "nonce", "type": "uint256"},
                        {"name": "data", "type": "bytes"},
                    ],
                },
                "primaryType": "ForwardRequest",
                "domain": signature_data["domain"],
                "message": {
                    "from": self.get_default_user(),
                    "nonce": signature_data["nonce"],
                    "data": hex_str_to_bytes(function_data),
                },
            }
        )
        # The following line creates overactive warning
        # because of difficulties with a decorated declaration:
        # No value for argument 'private_key' in unbound method call (no-value-for-parameter)
        # pylint: disable=E1120
        signature = Account.from_key(self.private_key).sign_message(signable_message)

        # Format the ForwardRequest object.
        forward_request = {
            "from": self.get_default_user(),
            "nonce": signature_data["nonce"],
            "data": function_data,
        }

        # Post the forwarded message.
        receipt = self._call_forwarder_api(
            api="execute",
            request_type=RequestType.POST,
            data={
                "forwardRequest": forward_request,
                "signature": signature.signature.hex(),
            },
        )
        if receipt is None:
            _LOG.error("Forwarder execute failed.")
            return None

        receipt = ForwarderCommitmentService._convert_string_numbers(receipt)
        self._check_tx_success(receipt)
        receipt = self._convert_receipt_logs(receipt)

        return receipt

    def add_set(self, set_cid: str) -> dict:
        # Execute the call via the forwarder.
        _LOG.debug("Sending transaction to addSet")
        receipt = self._post_execute(
            fn_name="addSet",
            args=[set_cid],
        )

        # Defer processing to the common worker shared by direct and forwarded paths.
        return self._add_set_worker(set_cid, receipt)

    def user_set_exists(self, user: str, set_cid: str) -> bool:
        # Encode the CommitmentService smart contract call.
        # web3.py requires checksum addresses.
        user = self.w3.to_checksum_address(user)
        function_data = self.csc.encodeABI(
            fn_name="userSetCommitments", args=[user, set_cid]
        )

        # Call userSetCommitments() via the forwarder.
        # The forwarded call return data in the right format
        # that can be passed through.
        ret = self._call_forwarder_api(
            api="call",
            params={
                "data": function_data,
            },
        )
        # The returned data is the JSON representation of userSetCommitments() return.
        # Convert it to a boolean.
        return bool(int(ret, base=16))

    def verify_user_sets(self, user: str, user_set_cid_sum: str) -> bool:
        user = self.w3.to_checksum_address(user)
        # verifyUserSets setHashSum argument is uint256.
        user_set_cid_sum = int(user_set_cid_sum, base=16)
        function_data = self.csc.encodeABI(
            fn_name="verifyUserSets", args=[user, user_set_cid_sum]
        )
        ret = self._call_forwarder_api(
            api="call",
            params={
                "data": function_data,
            },
        )
        return bool(int(ret, base=16))

    def add_object(self, object_cid: str) -> dict:
        _LOG.debug("Sending transaction to addObject")
        receipt = self._post_execute(
            fn_name="addObject",
            args=[object_cid],
        )
        return self._add_object_worker(receipt)

    def verify_user_object(self, user: str, object_cid: str, timestamp: str) -> bool:
        user = self.w3.to_checksum_address(user)
        function_data = self.csc.encodeABI(
            fn_name="verifyUserObject",
            args=[user, object_cid, self.convert_timestamp_str_to_chain(timestamp)],
        )
        ret = self._call_forwarder_api(
            api="call",
            params={
                "data": function_data,
            },
        )
        return bool(int(ret, base=16))

    def add_set_object(self, set_cid: str, object_cid: str) -> dict:
        _LOG.debug("Sending transaction to addSetObject")
        receipt = self._post_execute(
            fn_name="addSetObject",
            args=[set_cid, object_cid],
        )
        return self._add_set_object_worker(receipt)

    def add_sets_objects_batch(
        self, set_cids: List[str], object_cids: List[str]
    ) -> List[dict]:
        _LOG.debug("Sending transaction to addSetsObjectsBatch")
        receipt = self._post_execute(
            fn_name="addSetsObjectsBatch",
            args=[set_cids, object_cids],
        )
        return self._add_sets_objects_batch_worker(receipt)

    def add_set_objects_batch(self, set_cid: str, object_cids: List[str]) -> List[dict]:
        _LOG.debug("Sending transaction to addSetObjectsBatch")
        receipt = self._post_execute(
            fn_name="addSetObjectsBatch",
            args=[set_cid, object_cids],
        )
        return self._add_sets_objects_batch_worker(receipt)

    def verify_user_set_objects(
        self, user: str, set_cid: str, user_set_object_cid_sum: str
    ) -> bool:
        user = self.w3.to_checksum_address(user)
        # verifyUserSetObjectsCidSum setObjectHashSum argument is uint256.
        user_set_object_cid_sum = int(user_set_object_cid_sum, base=16)
        function_data = self.csc.encodeABI(
            fn_name="verifyUserSetObjectsCidSum",
            args=[user, set_cid, user_set_object_cid_sum],
        )
        ret = self._call_forwarder_api(
            api="call",
            params={
                "data": function_data,
            },
        )
        return bool(int(ret, base=16))
