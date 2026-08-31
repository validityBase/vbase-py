"""Unit tests for sensitive-value logging safeguards."""

import os
import unittest
from unittest.mock import patch

from vbase.core.forwarder_commitment_service import ForwarderCommitmentService
from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService
from vbase.utils.log import REDACTED_LOG_VALUE, mask_api_key


class TestSensitiveLogging(unittest.TestCase):
    """Verify credentials cannot be emitted by environment diagnostics."""

    def test_mask_api_key_keeps_only_safe_prefix_and_suffix(self):
        """Long API keys retain only eight characters at each end."""
        api_key = "qHhlk9M6-middle-secret-2fckvz-A"

        self.assertEqual(
            mask_api_key(api_key), "qHhlk9M6\N{HORIZONTAL ELLIPSIS}2fckvz-A"
        )

    def test_mask_api_key_fully_redacts_short_keys(self):
        """Short API keys are not partially disclosed."""
        self.assertEqual(mask_api_key("short-api-key"), REDACTED_LOG_VALUE)
        self.assertIsNone(mask_api_key(None))
        self.assertEqual(mask_api_key(""), "")

    @patch("vbase.core.forwarder_commitment_service._LOG.debug")
    def test_forwarder_environment_log_masks_credentials(self, debug_mock):
        """Forwarder diagnostics mask API keys and omit private values."""
        api_key = "qHhlk9M6-middle-secret-2fckvz-A"
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


if __name__ == "__main__":
    unittest.main()
