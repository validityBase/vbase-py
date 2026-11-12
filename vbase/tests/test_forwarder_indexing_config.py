"""Offline tests of the RPC configuration shared by integration tests."""

import os
import unittest
from unittest import mock

from vbase.core.forwarder_commitment_service_test import ForwarderCommitmentServiceTest
from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService
from vbase.tests.utils import create_test_indexing_service

_ARCHIVAL_RPC_URL = "https://archive.example/rpc/test-key"
_CONTRACT_ADDRESS = "0x" + "11" * 20


@mock.patch.dict(os.environ, {}, clear=True)
class TestForwarderIndexingConfig(unittest.TestCase):
    """Keep indexing reads separate from the forwarder's public RPC URL."""

    def setUp(self) -> None:
        self.forwarder = mock.create_autospec(
            ForwarderCommitmentServiceTest, instance=True
        )
        self.forwarder.get_commitment_service_data.return_value = {
            "node_rpc_url": "https://limited-public.example/rpc",
            "commitment_service_address": _CONTRACT_ADDRESS,
            "future_field": "ignored",
        }

    @mock.patch.object(Web3HTTPCommitmentService, "__init__", return_value=None)
    def test_uses_explicit_rpc_and_forwarder_contract(
        self, init: mock.MagicMock
    ) -> None:
        """Only the contract address comes from the forwarder response."""
        os.environ["VBASE_TEST_INDEXING_NODE_RPC_URL"] = _ARCHIVAL_RPC_URL

        indexer = create_test_indexing_service(self.forwarder)

        init.assert_called_once_with(
            node_rpc_url=_ARCHIVAL_RPC_URL,
            commitment_service_address=_CONTRACT_ADDRESS,
        )
        self.assertEqual(len(indexer.commitment_services), 1)
        self.assertIsNone(indexer.n_last_blocks)

    @mock.patch.object(Web3HTTPCommitmentService, "__init__", return_value=None)
    def test_does_not_require_public_rpc(self, init: mock.MagicMock) -> None:
        """An absent public endpoint cannot cause fallback or block indexing."""
        os.environ["VBASE_TEST_INDEXING_NODE_RPC_URL"] = _ARCHIVAL_RPC_URL
        del self.forwarder.get_commitment_service_data.return_value["node_rpc_url"]

        create_test_indexing_service(self.forwarder)

        self.assertEqual(init.call_args.kwargs["node_rpc_url"], _ARCHIVAL_RPC_URL)

    @mock.patch.object(Web3HTTPCommitmentService, "__init__", return_value=None)
    def test_strips_whitespace_from_rpc(self, init: mock.MagicMock) -> None:
        """A secret saved with surrounding whitespace still selects its node."""
        os.environ["VBASE_TEST_INDEXING_NODE_RPC_URL"] = f" {_ARCHIVAL_RPC_URL}\n"

        create_test_indexing_service(self.forwarder)

        self.assertEqual(init.call_args.kwargs["node_rpc_url"], _ARCHIVAL_RPC_URL)

    def test_requires_rpc_before_contacting_forwarder(self) -> None:
        """Missing or blank RPC configuration must never use the public node."""
        for value in (None, "", "   "):
            with self.subTest(value=value):
                if value is not None:
                    os.environ["VBASE_TEST_INDEXING_NODE_RPC_URL"] = value
                with self.assertRaisesRegex(
                    EnvironmentError, "VBASE_TEST_INDEXING_NODE_RPC_URL is required"
                ):
                    create_test_indexing_service(self.forwarder)
        self.forwarder.get_commitment_service_data.assert_not_called()

    def test_direct_service_reuses_its_node(self) -> None:
        """A forwarder test secret must not redirect direct localhost tests."""
        os.environ["VBASE_TEST_INDEXING_NODE_RPC_URL"] = _ARCHIVAL_RPC_URL
        service = Web3HTTPCommitmentService.__new__(Web3HTTPCommitmentService)

        indexer = create_test_indexing_service(service)

        self.assertEqual(indexer.commitment_services, [service])

    def test_applies_test_lookback(self) -> None:
        """The shared helper preserves the configured integration-test window."""
        os.environ["VBASE_TEST_INDEXING_N_LAST_BLOCKS"] = "100"
        service = Web3HTTPCommitmentService.__new__(Web3HTTPCommitmentService)

        indexer = create_test_indexing_service(service)

        self.assertEqual(indexer.n_last_blocks, 100)


if __name__ == "__main__":
    unittest.main()
