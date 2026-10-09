"""Unit tests for sensitive-value logging safeguards."""

import json
import os
import unittest
from unittest.mock import Mock, patch

import requests

from vbase.core.forwarder_commitment_service import ForwarderCommitmentService
from vbase.core.indexing_service import Web3HTTPIndexingService
from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService
from vbase.utils.log import REDACTED_LOG_VALUE, mask_api_key


class TestSensitiveLogging(unittest.TestCase):
    """Verify credentials cannot be emitted by environment diagnostics."""

    def test_mask_api_key_keeps_only_safe_prefix_and_suffix(self):
        """Long API keys retain only eight characters at each end."""
        api_key = "qHhlk9M6-middle-secretX-2fckvz-A"
        self.assertEqual(len(api_key), 32)

        self.assertEqual(
            mask_api_key(api_key), "qHhlk9M6\N{HORIZONTAL ELLIPSIS}2fckvz-A"
        )

    def test_mask_api_key_fully_redacts_short_keys(self):
        """Short API keys are not partially disclosed."""
        self.assertEqual(mask_api_key("short-api-key"), REDACTED_LOG_VALUE)
        for length in (16, 17, 31):
            with self.subTest(length=length):
                self.assertEqual(mask_api_key("x" * length), REDACTED_LOG_VALUE)
        self.assertIsNone(mask_api_key(None))
        self.assertEqual(mask_api_key(""), "")

    @patch("vbase.core.forwarder_commitment_service._LOG.debug")
    def test_forwarder_environment_log_masks_credentials(self, debug_mock):
        """Forwarder diagnostics mask API keys and omit private values."""
        api_key = "qHhlk9M6-middle-secretX-2fckvz-A"
        private_key = "0x" + "11" * 32
        forwarder_url = "https://forwarder.example/path?token=forwarder-secret"
        environment = {
            "VBASE_FORWARDER_URL": forwarder_url,
            "VBASE_API_KEY": api_key,
            "VBASE_COMMITMENT_SERVICE_PRIVATE_KEY": private_key,
        }

        with patch.dict(os.environ, environment):
            init_args = ForwarderCommitmentService.get_init_args_from_env()

        self.assertEqual(init_args["forwarder_url"], forwarder_url)
        self.assertEqual(init_args["api_key"], api_key)
        self.assertEqual(init_args["private_key"], private_key)
        safe_log_args = debug_mock.call_args.args[1]
        self.assertEqual(
            safe_log_args,
            {
                "forwarder_url_configured": True,
                "api_key": "qHhlk9M6\N{HORIZONTAL ELLIPSIS}2fckvz-A",
                "private_key_configured": True,
            },
        )
        rendered_call = repr(debug_mock.call_args)
        self.assertNotIn(api_key, rendered_call)
        self.assertNotIn(private_key, rendered_call)
        self.assertNotIn(forwarder_url, rendered_call)

    @patch("vbase.core.forwarder_commitment_service._LOG.debug")
    def test_forwarder_environment_log_fully_redacts_17_character_key(self, debug_mock):
        """A short configured API key must never be nearly exposed."""
        api_key = "x" * 17
        environment = {
            "VBASE_FORWARDER_URL": "https://forwarder.example",
            "VBASE_API_KEY": api_key,
            "VBASE_COMMITMENT_SERVICE_PRIVATE_KEY": "0x" + "11" * 32,
        }

        with patch.dict(os.environ, environment):
            init_args = ForwarderCommitmentService.get_init_args_from_env()

        self.assertEqual(init_args["api_key"], api_key)
        self.assertEqual(debug_mock.call_args.args[1]["api_key"], REDACTED_LOG_VALUE)
        self.assertNotIn(api_key, repr(debug_mock.call_args))

    @patch("vbase.core.web3_http_commitment_service._LOG.debug")
    def test_web3_environment_log_omits_private_rpc_values(self, debug_mock):
        """Web3 diagnostics omit private keys and complete RPC URLs."""
        private_key = "0x" + "22" * 32
        node_rpc_url = "https://rpc.example/v2/rpc-provider-api-key"
        service_address = "0x" + "33" * 20
        environment = {
            "VBASE_COMMITMENT_SERVICE_NODE_RPC_URL": node_rpc_url,
            "VBASE_COMMITMENT_SERVICE_ADDRESS": service_address,
            "VBASE_COMMITMENT_SERVICE_PRIVATE_KEY": private_key,
        }

        with patch.dict(os.environ, environment):
            init_args = Web3HTTPCommitmentService.get_init_args_from_env()

        self.assertEqual(init_args["node_rpc_url"], node_rpc_url)
        self.assertEqual(init_args["private_key"], private_key)
        safe_log_args = debug_mock.call_args.args[1]
        self.assertEqual(safe_log_args["node_rpc_url_configured"], True)
        self.assertEqual(safe_log_args["commitment_service_address"], service_address)
        self.assertEqual(safe_log_args["private_key_configured"], True)
        rendered_call = repr(debug_mock.call_args)
        self.assertNotIn(node_rpc_url, rendered_call)
        self.assertNotIn(private_key, rendered_call)

    @patch("vbase.core.web3_http_commitment_service.time.sleep")
    @patch("vbase.core.web3_http_commitment_service._W3_CONNECTION_MAX_RETRIES", 1)
    @patch("vbase.core.web3_http_commitment_service._LOG.error")
    @patch("vbase.core.web3_http_commitment_service.Web3")
    def test_web3_connection_failure_omits_rpc_url(
        self, web3_mock, error_mock, _sleep_mock
    ):
        """Connection errors do not disclose credentials embedded in RPC URLs."""
        node_rpc_url = "https://rpc.example/v2/rpc-provider-api-key"
        web3_mock.return_value.is_connected.return_value = False

        with self.assertRaises(ConnectionError) as raised:
            Web3HTTPCommitmentService(
                node_rpc_url=node_rpc_url,
                commitment_service_address="0x" + "33" * 20,
            )

        self.assertNotIn(node_rpc_url, repr(error_mock.call_args))
        self.assertNotIn(node_rpc_url, str(raised.exception))

    def test_indexing_descriptor_log_omits_private_values(self):
        """Indexer configuration logs only a safe count."""
        node_rpc_url = "https://rpc.example/v2/private-rpc-token"
        private_key = "0x" + "44" * 32
        init_args = {
            "node_rpc_url": node_rpc_url,
            "commitment_service_address": "0x" + "33" * 20,
            "private_key": private_key,
        }
        descriptor = {
            "commitment_services": [
                {"class": "Web3HTTPCommitmentService", "init_args": init_args}
            ]
        }
        environment = {"VBASE_INDEXING_SERVICE_JSON_DESCRIPTOR": json.dumps(descriptor)}

        with (
            patch.dict(os.environ, environment),
            patch("vbase.core.indexing_service._LOG.info") as info_mock,
            patch(
                "vbase.core.indexing_service.Web3HTTPCommitmentService"
            ) as service_mock,
        ):
            indexer = Web3HTTPIndexingService.create_instance_from_env_json_descriptor()

        self.assertEqual(indexer.commitment_services, [service_mock.return_value])
        service_mock.assert_called_once_with(**init_args)
        info_mock.assert_called_once_with(
            "Initializing indexing service. commitment_service_count=%d", 1
        )
        self.assertNotIn(node_rpc_url, repr(info_mock.call_args_list))
        self.assertNotIn(private_key, repr(info_mock.call_args_list))

    @patch("vbase.core.forwarder_commitment_service._LOG.error")
    @patch("vbase.core.forwarder_commitment_service.requests.get")
    def test_forwarder_http_error_log_omits_url(self, get_mock, error_mock):
        """HTTP failures must not log a credential-bearing request URL."""
        secret = "private-forwarder-token"
        service = object.__new__(ForwarderCommitmentService)
        service.forwarder_url = f"https://forwarder.example/?token={secret}"
        service.api_key = "test-api-key"
        get_mock.return_value.raise_for_status.side_effect = requests.HTTPError(
            service.forwarder_url
        )

        with patch.object(service, "get_default_user", return_value="0x" + "11" * 20):
            with self.assertRaises(requests.HTTPError):
                service.get_commitment_service_data()

        error_mock.assert_called_once_with("Forwarder HTTP request failed")
        self.assertNotIn(secret, repr(error_mock.call_args_list))

    @patch("vbase.core.forwarder_commitment_service._LOG.error")
    @patch("vbase.core.forwarder_commitment_service.requests.get")
    def test_forwarder_transport_error_log_omits_url(self, get_mock, error_mock):
        """Transport failures must not log a credential-bearing request URL."""
        secret = "private-forwarder-token"
        service = object.__new__(ForwarderCommitmentService)
        service.forwarder_url = f"https://forwarder.example/?token={secret}"
        service.api_key = "test-api-key"
        get_mock.side_effect = requests.ConnectionError(service.forwarder_url)

        with patch.object(service, "get_default_user", return_value="0x" + "11" * 20):
            with self.assertRaises(requests.ConnectionError):
                service.get_commitment_service_data()

        error_mock.assert_called_once_with("Forwarder request failed")
        self.assertNotIn(secret, repr(error_mock.call_args_list))

    @patch("vbase.core.web3_http_commitment_service.time.sleep")
    @patch("vbase.core.web3_http_commitment_service._W3_CONNECTION_MAX_RETRIES", 1)
    @patch("vbase.core.web3_http_commitment_service._LOG.error")
    @patch("vbase.core.web3_http_commitment_service.Web3")
    def test_web3_connection_exception_log_omits_rpc_url(
        self, web3_mock, error_mock, _sleep_mock
    ):
        """Provider errors may include a credential-bearing RPC URL."""
        node_rpc_url = "https://rpc.example/v2/private-rpc-token"
        web3_mock.return_value.is_connected.side_effect = (
            ConnectionError(node_rpc_url),
            False,
        )

        with self.assertRaises(ConnectionError) as raised:
            Web3HTTPCommitmentService(
                node_rpc_url=node_rpc_url,
                commitment_service_address="0x" + "33" * 20,
            )

        error_mock.assert_called_once_with("Web3 node RPC connection attempt failed")
        self.assertNotIn(node_rpc_url, repr(error_mock.call_args_list))
        self.assertNotIn(node_rpc_url, str(raised.exception))

    @patch("retry.api.time.sleep")
    @patch("vbase.core.indexing_service._LOG.warning")
    def test_indexing_retry_does_not_log_rpc_url(self, warning_mock, _sleep_mock):
        """A transient provider error must not log its credential-bearing URL."""
        node_rpc_url = "https://rpc.example/v2/private-rpc-token"
        event_filter = Mock()
        event_filter.get_all_entries.side_effect = [
            ConnectionError(node_rpc_url),
            [],
        ]
        indexer = Web3HTTPIndexingService([])

        # pylint: disable-next=protected-access
        self.assertEqual(indexer._retry_get_all_entries(event_filter), [])
        self.assertEqual(event_filter.get_all_entries.call_count, 2)
        warning_mock.assert_not_called()

    @patch("retry.api.time.sleep")
    @patch("vbase.core.web3_commitment_service._LOG.warning")
    def test_set_existence_retry_does_not_log_rpc_url(self, warning_mock, _sleep_mock):
        """A transient set lookup error must not log its credential-bearing URL."""
        node_rpc_url = "https://rpc.example/v2/private-rpc-token"
        service = object.__new__(Web3HTTPCommitmentService)
        with patch.object(
            service,
            "user_set_exists",
            side_effect=[ConnectionError(node_rpc_url), True],
        ) as exists_mock:
            # pylint: disable-next=protected-access
            self.assertIsNone(service._user_set_exists_with_retry("0xabc", "0xdef"))

        self.assertEqual(exists_mock.call_count, 2)
        warning_mock.assert_not_called()

    @patch("vbase.core.web3_http_commitment_service.time.sleep")
    @patch("vbase.core.web3_http_commitment_service._W3_CONNECTION_MAX_RETRIES", 2)
    @patch("vbase.core.web3_http_commitment_service._LOG.error")
    @patch("vbase.core.web3_http_commitment_service.Web3")
    def test_web3_repeated_connection_exceptions_omit_rpc_url(
        self, web3_mock, error_mock, _sleep_mock
    ):
        """The final connection failure must not repeat or expose a provider error."""
        node_rpc_url = "https://rpc.example/v2/private-rpc-token"
        web3_mock.return_value.is_connected.side_effect = ConnectionError(node_rpc_url)

        with self.assertRaises(ConnectionError) as raised:
            Web3HTTPCommitmentService(
                node_rpc_url=node_rpc_url,
                commitment_service_address="0x" + "33" * 20,
            )

        self.assertEqual(web3_mock.return_value.is_connected.call_count, 2)
        self.assertEqual(error_mock.call_count, 2)
        self.assertNotIn(node_rpc_url, str(raised.exception))
        self.assertNotIn(node_rpc_url, repr(error_mock.call_args_list))


if __name__ == "__main__":
    unittest.main()
