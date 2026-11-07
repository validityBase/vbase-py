"""Unit tests of indexing service construction that do not require a node."""

import unittest
from unittest import mock

from vbase.core.forwarder_commitment_service import ForwarderCommitmentService
from vbase.core.indexing_service import IndexingService, Web3HTTPIndexingService
from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService

_FORWARDER_NODE_RPC_URL = "https://public-node.example/rpc"
_OWN_NODE_RPC_URL = "https://own-node.example/rpc"
_COMMITMENT_SERVICE_ADDRESS = "0x" + "11" * 20


def _make_forwarder() -> ForwarderCommitmentService:
    """Return a forwarder stub with commitment service connection data."""
    forwarder = mock.create_autospec(ForwarderCommitmentService, instance=True)
    forwarder.get_commitment_service_data.return_value = {
        "node_rpc_url": _FORWARDER_NODE_RPC_URL,
        "commitment_service_address": _COMMITMENT_SERVICE_ADDRESS,
    }
    return forwarder


class TestCreateInstanceFromCommitmentService(unittest.TestCase):
    """Test IndexingService.create_instance_from_commitment_service()."""

    @mock.patch.object(Web3HTTPCommitmentService, "__init__", return_value=None)
    def test_uses_forwarder_node_by_default(self, init: mock.MagicMock):
        """A forwarder client uses the node URL returned by the forwarder."""
        indexing_service = IndexingService.create_instance_from_commitment_service(
            _make_forwarder()
        )

        self.assertIsInstance(indexing_service, Web3HTTPIndexingService)
        init.assert_called_once_with(
            node_rpc_url=_FORWARDER_NODE_RPC_URL,
            commitment_service_address=_COMMITMENT_SERVICE_ADDRESS,
        )

    @mock.patch.object(Web3HTTPCommitmentService, "__init__", return_value=None)
    def test_uses_supplied_node_for_forwarder(self, init: mock.MagicMock):
        """A supplied node URL overrides the URL returned by the forwarder."""
        IndexingService.create_instance_from_commitment_service(
            _make_forwarder(), node_rpc_url=_OWN_NODE_RPC_URL
        )

        init.assert_called_once_with(
            node_rpc_url=_OWN_NODE_RPC_URL,
            commitment_service_address=_COMMITMENT_SERVICE_ADDRESS,
        )

    @mock.patch.object(Web3HTTPCommitmentService, "__init__", return_value=None)
    def test_ignores_unknown_forwarder_fields(self, init: mock.MagicMock):
        """New forwarder response fields do not break indexer construction."""
        forwarder = _make_forwarder()
        forwarder.get_commitment_service_data.return_value["chain_id"] = 137

        IndexingService.create_instance_from_commitment_service(forwarder)

        init.assert_called_once_with(
            node_rpc_url=_FORWARDER_NODE_RPC_URL,
            commitment_service_address=_COMMITMENT_SERVICE_ADDRESS,
        )

    @mock.patch.object(Web3HTTPCommitmentService, "__init__", return_value=None)
    def test_supplied_node_does_not_need_forwarder_node(self, init: mock.MagicMock):
        """A supplied node URL works when the forwarder returns no node."""
        forwarder = _make_forwarder()
        del forwarder.get_commitment_service_data.return_value["node_rpc_url"]

        IndexingService.create_instance_from_commitment_service(
            forwarder, node_rpc_url=_OWN_NODE_RPC_URL
        )

        init.assert_called_once_with(
            node_rpc_url=_OWN_NODE_RPC_URL,
            commitment_service_address=_COMMITMENT_SERVICE_ADDRESS,
        )

    def test_rejects_missing_forwarder_node(self):
        """Without a supplied node URL, the forwarder must return one."""
        forwarder = _make_forwarder()
        forwarder.get_commitment_service_data.return_value["node_rpc_url"] = ""

        with self.assertRaises(ValueError):
            IndexingService.create_instance_from_commitment_service(forwarder)

    def test_rejects_empty_node_url_for_forwarder(self):
        """An empty node URL is an error, not a fallback to the forwarder's."""
        with self.assertRaises(ValueError):
            IndexingService.create_instance_from_commitment_service(
                _make_forwarder(), node_rpc_url=""
            )

    def test_passes_through_web3_commitment_service(self):
        """A Web3 commitment service is used as is."""
        # Skip __init__, which connects to the node.
        commitment_service = Web3HTTPCommitmentService.__new__(
            Web3HTTPCommitmentService
        )

        indexing_service = IndexingService.create_instance_from_commitment_service(
            commitment_service
        )

        self.assertEqual(indexing_service.commitment_services, [commitment_service])

    def test_rejects_node_url_for_web3_commitment_service(self):
        """A node URL is ambiguous for a Web3 service, which has its own node."""
        commitment_service = Web3HTTPCommitmentService.__new__(
            Web3HTTPCommitmentService
        )

        with self.assertRaises(ValueError):
            IndexingService.create_instance_from_commitment_service(
                commitment_service, node_rpc_url=_OWN_NODE_RPC_URL
            )


class TestGetFromBlock(unittest.TestCase):
    """Test the start block used for event queries."""

    def _get_from_block(self, n_last_blocks, block_number: int) -> int:
        indexing_service = Web3HTTPIndexingService([])
        indexing_service.n_last_blocks = n_last_blocks
        commitment_service = mock.MagicMock()
        commitment_service.w3.eth.block_number = block_number
        # pylint: disable=protected-access
        return indexing_service._get_from_block(commitment_service)

    def test_full_history_by_default(self):
        """Without n_last_blocks, queries start at the genesis block."""
        self.assertEqual(self._get_from_block(None, 500), 0)

    def test_recent_blocks(self):
        """With n_last_blocks, queries start that many blocks back."""
        self.assertEqual(self._get_from_block(100, 500), 400)

    def test_clamps_at_genesis(self):
        """A window longer than the chain starts at the genesis block."""
        self.assertEqual(self._get_from_block(100, 50), 0)


class TestEventQueries(unittest.TestCase):
    """Test event queries that join object and set commitments."""

    def _make_indexing_service(self):
        """Return an indexer whose node advances one block per height read."""
        commitment_service = mock.MagicMock()
        type(commitment_service.w3.eth).block_number = mock.PropertyMock(
            side_effect=[100, 101]
        )
        events = commitment_service.csc.events
        events.AddObject.get_logs.return_value = []
        events.AddSetObject.get_logs.return_value = []
        indexing_service = Web3HTTPIndexingService([commitment_service])
        indexing_service.n_last_blocks = 10
        return indexing_service, events

    def _assert_same_start_block(self, events):
        for event in (events.AddObject, events.AddSetObject):
            self.assertEqual(event.get_logs.call_args.kwargs["from_block"], 90)
            self.assertEqual(event.get_logs.call_args.kwargs["to_block"], "latest")

    def test_find_user_objects_uses_one_start_block(self):
        """Object and set queries cover the same block range."""
        indexing_service, events = self._make_indexing_service()

        indexing_service.find_user_objects("0xuser", return_set_cids=True)

        self._assert_same_start_block(events)

    def test_find_objects_uses_one_start_block(self):
        """Object and set queries cover the same block range."""
        indexing_service, events = self._make_indexing_service()

        indexing_service.find_objects(["0x" + "aa" * 32], return_set_cids=True)

        self._assert_same_start_block(events)


if __name__ == "__main__":
    unittest.main()
