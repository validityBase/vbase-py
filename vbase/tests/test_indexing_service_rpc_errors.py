"""Unit tests for safe indexing-service RPC failure diagnostics."""

import unittest
from unittest.mock import Mock

from requests.exceptions import HTTPError

from vbase.core.indexing_service import Web3HTTPIndexingService

# Exercise the RPC retrieval seam without requiring a live blockchain.
# pylint: disable=protected-access


class TestIndexingServiceRpcErrors(unittest.TestCase):
    """Provider errors should be useful without exposing endpoint credentials."""

    def test_block_range_error_omits_rpc_url(self):
        """Classify the provider response without printing its credential URL."""
        response = Mock(status_code=400)
        response.json.return_value = {
            "error": {"code": -32602, "message": "Requested block range is too large"}
        }
        original_error = HTTPError(
            "400 Client Error for https://example.com/v2/example-secret",
            response=response,
        )
        event_filter = Mock()
        event_filter.get_all_entries.side_effect = original_error

        with self.assertRaises(HTTPError) as caught:
            Web3HTTPIndexingService([])._get_all_entries(event_filter)

        self.assertEqual(caught.exception.response, response)
        self.assertIn("HTTP 400", str(caught.exception))
        self.assertIn("RPC -32602", str(caught.exception))
        self.assertIn("block_range_limit", str(caught.exception))
        self.assertNotIn("example-secret", str(caught.exception))

    def test_unknown_message_is_not_echoed(self):
        """Unexpected provider text cannot leak through the diagnostic."""
        response = Mock(status_code=400)
        response.json.return_value = {
            "error": {"code": -32602, "message": "token=example-secret"}
        }
        event_filter = Mock()
        event_filter.get_all_entries.side_effect = HTTPError(
            "400 Client Error for https://example.com/v2/example-secret",
            response=response,
        )

        with self.assertRaises(HTTPError) as caught:
            Web3HTTPIndexingService([])._get_all_entries(event_filter)

        self.assertIn("unclassified", str(caught.exception))
        self.assertNotIn("example-secret", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
