"""Tests for Web3HTTPCommitmentService that need no node."""

import unittest
from unittest.mock import call, patch

import requests
from web3 import Web3

from vbase.core.web3_http_commitment_service import (
    Web3HTTPCommitmentService,
    _caller_retries,
    _RedactingHTTPProvider,
)


class TestWeb3HTTPCommitmentServiceConnection(unittest.TestCase):
    """Test the connection attempts of Web3HTTPCommitmentService."""

    @patch("vbase.core.web3_http_commitment_service.time.sleep")
    @patch("vbase.core.web3_http_commitment_service._W3_CONNECTION_BACKOFF", 1)
    @patch("vbase.core.web3_http_commitment_service._W3_CONNECTION_MAX_ATTEMPTS", 3)
    @patch("vbase.core.web3_http_commitment_service.Web3")
    def test_failed_connection_waits_only_between_attempts(self, web3_mock, sleep_mock):
        """A failed connection waits before each retry, not after the last attempt."""
        web3_mock.return_value.is_connected.return_value = False

        with self.assertLogs("vbase.core.web3_http_commitment_service", level="ERROR"):
            with self.assertRaises(ConnectionError) as raised:
                Web3HTTPCommitmentService(
                    node_rpc_url="http://127.0.0.1:8545",
                    commitment_service_address="0x" + "33" * 20,
                )

        self.assertEqual(web3_mock.return_value.is_connected.call_count, 3)
        self.assertEqual(sleep_mock.call_args_list, [call(1), call(2)])
        self.assertTrue(str(raised.exception).endswith(" after 3 attempts"))


class TestRedactingHTTPProviderRetries(unittest.TestCase):
    """Test when the HTTP provider retries failed node requests."""

    @patch("time.sleep")
    @patch("requests.Session.post")
    def test_caller_retries_turns_off_provider_retries(self, post_mock, _sleep_mock):
        """Requests keep web3's retries except inside _caller_retries()."""
        post_mock.side_effect = requests.exceptions.HTTPError("503 Service Unavailable")
        provider = _RedactingHTTPProvider("http://127.0.0.1:9")
        w3 = Web3(provider)
        web3_attempts = provider.exception_retry_configuration.retries

        for label, expected_posts in (
            ("before", web3_attempts),
            ("inside", 1),
            ("after", web3_attempts),
        ):
            with self.subTest(label=label):
                post_mock.reset_mock()
                with self.assertRaises(requests.exceptions.HTTPError):
                    if label == "inside":
                        with _caller_retries():
                            w3.eth.get_block(10)
                    else:
                        w3.eth.get_block(10)
                self.assertEqual(post_mock.call_count, expected_posts)


if __name__ == "__main__":
    unittest.main()
