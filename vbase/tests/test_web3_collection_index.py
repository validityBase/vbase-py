"""Collection pagination using a checkpointed Web3 index."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from vbase import Web3HTTPIndexingService


def source(chain_id, entries):
    """Fake a finalized chain; each block has zero or one AddSetObject event."""
    cs = MagicMock()
    cs.commitment_service_address = "0xcontract"
    cs.w3.eth.chain_id = chain_id

    def block(tag):
        height = len(entries) - 1 if tag == "finalized" else tag
        return {"number": height, "hash": height.to_bytes(32, "big")}

    def logs(*, from_block, to_block, argument_filters):
        assert argument_filters == {}
        return [
            {"args": {"user": user, "setCid": bytes.fromhex(cid)}}
            for entry in entries[from_block : to_block + 1]
            if entry
            for user, cid in [entry]
        ]

    cs.w3.eth.get_block.side_effect = block
    cs.csc.events.AddSetObject.get_logs.side_effect = logs
    return cs


class Web3CollectionIndexTests(unittest.TestCase):
    """Exercise the persistent index across independent sync and read instances."""

    def setUp(self):
        # enterContext keeps the directory alive through the whole test.
        # pylint: disable-next=consider-using-with
        tmp = self.enterContext(tempfile.TemporaryDirectory())
        self.path = Path(tmp) / "collections.sqlite"
        self.entries = [("Alice", "bb"), None, ("ALICE", "aa"), ("bob", "ff")]
        self.cs = source(1, self.entries)

    def index(self, *sources):
        """Reopen the same file, as a new process would."""
        return Web3HTTPIndexingService(list(sources), collection_index_path=self.path)

    def test_restart_pages_without_rpc_and_syncs_only_new_blocks(self):
        """Merge distinct CIDs across chains and resume each chain independently."""
        other = source(2, [("alice", "aa"), ("alice", "cc")])
        service = self.index(self.cs, other)
        with self.assertRaisesRegex(RuntimeError, "not ready"):
            service.find_user_collection_cids("alice", limit=2)
        service.sync_collection_cids(block_batch_size=2)
        self.assertEqual(
            [
                (c.kwargs["from_block"], c.kwargs["to_block"])
                for c in self.cs.csc.events.AddSetObject.get_logs.call_args_list
            ],
            [(0, 1), (2, 3)],
        )
        reader = self.index(other, self.cs)
        for cs in (self.cs, other):
            cs.reset_mock()
        for size, offset, expected in (
            (2, 0, ["0xaa", "0xbb"]),
            (2, 2, ["0xcc"]),
            (0, 0, []),
            (2, 10**30, []),
        ):
            page = reader.find_user_collection_cids("ALICE", limit=size, offset=offset)
            self.assertEqual((page.cids, page.total), (expected, 3))
        self.assertFalse(self.cs.mock_calls or other.mock_calls)
        self.entries.append(("alice", "dd"))
        reader.sync_collection_cids()
        self.cs.csc.events.AddSetObject.get_logs.assert_called_once_with(
            from_block=4, to_block=4, argument_filters={}
        )
        other.csc.events.AddSetObject.get_logs.assert_not_called()
        self.assertEqual(reader.find_user_collection_cids("alice", limit=0).total, 4)

    def test_failed_initial_sync_resumes_without_serving_partial_results(self):
        """A failed batch neither advances its checkpoint nor publishes a partial index."""
        node = self.cs.csc.events.AddSetObject.get_logs
        good = node.side_effect

        def interrupted(**kw):
            if kw["from_block"] != 0:
                raise ValueError("sync interrupted")
            return good(**kw)

        node.side_effect = interrupted
        with self.assertRaisesRegex(ValueError, "sync interrupted"):
            self.index(self.cs).sync_collection_cids(block_batch_size=2)
        service = self.index(self.cs)
        with self.assertRaisesRegex(RuntimeError, "not ready"):
            service.find_user_collection_cids("alice", limit=1)
        node.reset_mock()
        node.side_effect = good
        service.sync_collection_cids(block_batch_size=2)
        node.assert_called_once_with(from_block=2, to_block=3, argument_filters={})
        self.assertEqual(service.find_user_collection_cids("alice", limit=2).total, 2)

    def test_rejects_mismatched_source_without_exposing_old_cids(self):
        """Reusing a path with other sources must never mix their collections."""
        self.index(self.cs).sync_collection_cids()
        other = self.index(source(2, [None]))
        with self.assertRaisesRegex(ValueError, "sources"):
            other.sync_collection_cids()
        with self.assertRaisesRegex(ValueError, "sources"):
            other.find_user_collection_cids("alice", limit=2)

    def test_no_page_until_every_source_has_finished_its_first_sync(self):
        """A finished source alone must not produce a plausible but partial total."""
        second = source(2, [None])
        block = second.w3.eth.get_block.side_effect
        second.w3.eth.get_block.side_effect = ValueError("finality unavailable")
        service = self.index(self.cs, second)
        with self.assertRaisesRegex(ValueError, "finality unavailable"):
            service.sync_collection_cids()
        with self.assertRaisesRegex(RuntimeError, "not ready"):
            service.find_user_collection_cids("alice", limit=0)
        second.w3.eth.get_block.side_effect = block
        service.sync_collection_cids()
        self.assertEqual(service.find_user_collection_cids("alice", limit=0).total, 2)

    def test_changed_finalized_checkpoint_stops_before_fetching_logs(self):
        """A changed finalized block requires investigation before appending new CIDs."""
        service = self.index(self.cs)
        service.sync_collection_cids()
        self.cs.reset_mock()
        self.cs.w3.eth.get_block.side_effect = lambda _: {"number": 4, "hash": b"fork"}
        with self.assertRaisesRegex(RuntimeError, "checkpoint"):
            service.sync_collection_cids()
        self.cs.csc.events.AddSetObject.get_logs.assert_not_called()

    def test_bad_batch_size_does_not_create_an_index(self):
        """Invalid ranges fail without initializing a database."""
        service = self.index(self.cs)
        for size in (0, -1, True, 1.5):
            with self.subTest(size=size), self.assertRaises(ValueError):
                service.sync_collection_cids(block_batch_size=size)
        self.assertFalse(self.path.exists())
