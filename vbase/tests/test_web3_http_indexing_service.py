"""Tests for Web3HTTPIndexingService that need no node."""

# pylint: disable=protected-access

import functools
import io
import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import requests
from web3 import Web3
from web3.exceptions import Web3RPCError

import vbase.core
from vbase.core.indexing_service import Web3HTTPIndexingService
from vbase.core.web3_http_commitment_service import (
    Web3HTTPCommitmentService,
    _RedactingHTTPProvider,
)

_USER = "0x" + "11" * 20
_TIMESTAMP = 1_700_000_000
_ABI_PATH = Path(vbase.core.__file__).parent / "abi" / "CommitmentService.json"


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


class TestIndexingRequestsThroughTheProvider(unittest.TestCase):
    """Each SDK attempt of an indexing request sends one HTTP request.

    The node keeps answering HTTP 429. If the provider also retried, each SDK
    attempt would send several requests, and the counts would multiply.
    RETRY_TRIES is set to 3, unlike web3's 5 attempts, so that the count
    also shows which layer retried.
    """

    def setUp(self):
        self.methods: list = []
        self.answer_chain_id = True
        tries_patcher = patch.object(Web3HTTPIndexingService, "RETRY_TRIES", 3)
        tries_patcher.start()
        self.addCleanup(tries_patcher.stop)
        patcher = patch("requests.Session.post", side_effect=self._post)
        patcher.start()
        self.addCleanup(patcher.stop)
        sleep_patcher = patch("time.sleep")
        sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)
        self.cs = MagicMock()
        self.cs.w3 = Web3(_RedactingHTTPProvider("http://127.0.0.1:9"))
        self.cs.convert_timestamp_chain_to_str.side_effect = (
            Web3HTTPCommitmentService.convert_timestamp_chain_to_str
        )
        self.indexer = Web3HTTPIndexingService([self.cs])

    def _post(self, *_args, **kwargs) -> requests.Response:
        """Answer eth_chainId outside its own test; fail every other request."""
        method = json.loads(kwargs["data"])["method"]
        self.methods.append(method)
        if method == "eth_chainId" and self.answer_chain_id:
            response = requests.Response()
            response.status_code = 200
            response._content = b'{"jsonrpc": "2.0", "id": 0, "result": "0x1"}'
            response.raw = io.BytesIO(b"")
            return response
        raise requests.exceptions.HTTPError("429 Too Many Requests")

    def _assert_attempts(self, method: str, request) -> None:
        self.methods.clear()
        with self.assertRaises(requests.exceptions.HTTPError):
            request()
        self.assertEqual(
            self.methods.count(method), Web3HTTPIndexingService.RETRY_TRIES
        )

    def test_event_query(self):
        """eth_getLogs for an AddSet event query."""
        abi = json.loads(_ABI_PATH.read_text(encoding="utf-8"))["abi"]
        contract = self.cs.w3.eth.contract(address="0x" + "33" * 20, abi=abi)
        self._assert_attempts(
            "eth_getLogs",
            lambda: self.indexer._get_event_logs(
                contract.events.AddSet, 0, {"user": _USER}
            ),
        )

    def test_block_lookup(self):
        """eth_getBlockByNumber for the timestamp of an AddSet event."""
        self._assert_attempts(
            "eth_getBlockByNumber",
            lambda: self.indexer._process_add_set_events(
                self.cs, [_add_set_event(10, 1)]
            ),
        )

    def test_chain_id_and_start_block(self):
        """eth_chainId for receipts, and eth_blockNumber for the start block."""
        self.answer_chain_id = False
        # Each receipt builder reads the chain ID before it reads any event.
        for build in (
            self.indexer._process_add_set_events,
            self.indexer._process_add_object_events,
            self.indexer._process_add_set_object_events,
        ):
            with self.subTest(build=build.__name__):
                self._assert_attempts(
                    "eth_chainId", functools.partial(build, self.cs, [])
                )
        self.indexer.n_last_blocks = 10
        self._assert_attempts(
            "eth_blockNumber", lambda: self.indexer._get_from_block(self.cs)
        )


if __name__ == "__main__":
    unittest.main()
