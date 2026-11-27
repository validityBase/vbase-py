"""Tests for Web3HTTPIndexingService that need no node."""

# pylint: disable=protected-access

import unittest
from unittest.mock import MagicMock, call, patch

import requests
from web3 import Web3
from web3.exceptions import Web3RPCError

from vbase.core.indexing_service import Web3HTTPIndexingService
from vbase.core.web3_http_commitment_service import (
    Web3HTTPCommitmentService,
    _RedactingHTTPProvider,
)

_USER = "0x" + "11" * 20
_TIMESTAMP = 1_700_000_000


def _add_set_event(block_number: int, n: int) -> dict:
    """Build an AddSet event in the given block."""
    return {
        "blockNumber": block_number,
        "transactionHash": bytes([n]) * 32,
        "args": {"user": _USER, "setCid": bytes([n]) * 32},
    }


def _commitment_service(events: list, get_block) -> MagicMock:
    """Build a commitment service whose node returns events and blocks."""
    cs = MagicMock()
    cs.csc.events.AddSet.get_logs.return_value = events
    cs.w3.eth.chain_id = 84532
    cs.w3.eth.get_block.side_effect = get_block
    cs.convert_timestamp_chain_to_str.side_effect = (
        Web3HTTPCommitmentService.convert_timestamp_chain_to_str
    )
    return cs


class TestFindUserSetsBlockLookups(unittest.TestCase):
    """Test how find_user_sets() reads block timestamps."""

    def test_reads_each_block_once(self):
        """Events in the same block share one block lookup."""
        events = [_add_set_event(10, 1), _add_set_event(10, 2), _add_set_event(11, 3)]
        cs = _commitment_service(
            events,
            lambda block_identifier: {"timestamp": _TIMESTAMP + block_identifier},
        )

        receipts = Web3HTTPIndexingService([cs]).find_user_sets(_USER)

        self.assertEqual(
            cs.w3.eth.get_block.call_args_list,
            [call(block_identifier=10), call(block_identifier=11)],
        )
        self.assertEqual(
            [(r["setCid"], r["timestamp"]) for r in receipts],
            [
                ("0x" + "01" * 32, "2023-11-14 22:13:30+00:00"),
                ("0x" + "02" * 32, "2023-11-14 22:13:30+00:00"),
                ("0x" + "03" * 32, "2023-11-14 22:13:31+00:00"),
            ],
        )

    @patch("retry.api.time.sleep")
    def test_retries_failed_block_lookups(self, sleep_mock):
        """A refused connection or a JSON-RPC error is retried with backoff."""
        cs = _commitment_service(
            [_add_set_event(10, 1)],
            [
                requests.exceptions.ConnectionError("connection refused"),
                Web3RPCError("header not found"),
                {"timestamp": _TIMESTAMP},
            ],
        )

        receipts = Web3HTTPIndexingService([cs]).find_user_sets(_USER)

        self.assertEqual(cs.w3.eth.get_block.call_count, 3)
        self.assertEqual(
            sleep_mock.call_args_list,
            [
                call(Web3HTTPIndexingService.RETRY_DELAY),
                call(
                    Web3HTTPIndexingService.RETRY_DELAY
                    * Web3HTTPIndexingService.RETRY_BACKOFF
                ),
            ],
        )
        self.assertEqual(receipts[0]["timestamp"], "2023-11-14 22:13:20+00:00")

    @patch("time.sleep")
    @patch("requests.Session.post")
    def test_provider_does_not_retry_indexing_requests(self, post_mock, _sleep_mock):
        """Only the indexing service retries its requests, so attempts do not multiply."""
        post_mock.side_effect = requests.exceptions.HTTPError("429 Too Many Requests")
        w3 = Web3(_RedactingHTTPProvider("http://127.0.0.1:9"))

        with self.assertRaises(requests.exceptions.HTTPError):
            Web3HTTPIndexingService([])._retry_node_call(
                w3.eth.get_block, block_identifier=10
            )

        self.assertEqual(post_mock.call_count, Web3HTTPIndexingService.RETRY_TRIES)


if __name__ == "__main__":
    unittest.main()
