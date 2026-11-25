"""Cross-backend pagination and the lifetime of a Web3 indexing session."""

import unittest
from unittest.mock import MagicMock, patch

from sqlmodel import Session, SQLModel

from vbase import SQLIndexingService, Web3HTTPIndexingService
from vbase.core.models import EventAddObject, EventAddSetObject, LastBatchProcessingTime
from vbase.core.web3_commitment_service import Web3CommitmentService
from vbase.core.web3_event_index import Web3EventIndex


def _source(chain: int, blocks: list) -> MagicMock:
    """Each block contains (user, object CID, transaction hash, optional collection) stamps."""
    cs = MagicMock()
    cs.commitment_service_address = "0xcontract"
    cs.w3.eth.chain_id = chain
    cs.convert_timestamp_chain_to_str.side_effect = (
        Web3CommitmentService.convert_timestamp_chain_to_str
    )

    def header(tag):
        n = len(blocks) - 1 if tag == "finalized" else tag
        return {"number": n, "hash": n.to_bytes(32, "big"), "timestamp": 100 + n}

    def logs(collection, from_block, to_block, argument_filters):
        assert not argument_filters
        return [
            {
                "blockNumber": n,
                "args": {
                    "user": user,
                    "objectCid": bytes.fromhex(cid),
                    "timestamp": 42,
                    "setCid": bytes.fromhex(group or ""),
                },
                "transactionHash": bytes.fromhex(tx),
            }
            for n in range(from_block, to_block + 1)
            for user, cid, tx, group in blocks[n]
            if not collection or group is not None
        ]

    cs.w3.eth.get_block.side_effect = header
    cs.csc.events.AddObject.get_logs.side_effect = lambda **kw: logs(False, **kw)
    cs.csc.events.AddSetObject.get_logs.side_effect = lambda **kw: logs(True, **kw)
    return cs


class TestSessionPages(unittest.TestCase):
    """No database, filesystem, or node is accessed by Web3 page reads."""

    def setUp(self):
        self.blocks = [
            [],
            [
                ("Alice", "aa", "02", "ff"),
                ("ALICE", "aa", "01", "ff"),
                ("alice", "ab", "01", "bb"),
                ("bob", "ab", "01", "dd"),
            ],
        ]
        self.cs = _source(1, self.blocks)
        self.other = _source(2, [[], [("alice", "aa", "01", None)]])
        self.node = Web3HTTPIndexingService([self.other, self.cs, self.cs])

    def test_receipts_and_cids_match_sql(self):
        """Same timestamp, repeated stamps, cross-chain ties, mixed case, and count-only reads."""
        sql = SQLIndexingService(
            "sqlite://", indexing_stale_threshold_seconds=float("inf")
        )
        self.addCleanup(sql.db_engine.dispose)
        SQLModel.metadata.create_all(sql.db_engine)
        with Session(sql.db_engine) as db:
            db.add(LastBatchProcessingTime(id="head", timestamp=101000))
            for i, (chain, user, cid, tx, group) in enumerate(
                [
                    (1, "alice", "AA", "02", "FF"),
                    (1, "alice", "aa", "01", "ff"),
                    (1, "alice", "ab", "01", "bb"),
                    (1, "bob", "ab", "01", "dd"),
                    (2, "alice", "aa", "01", None),
                ]
            ):
                fields = {
                    "id": str(i),
                    "chain_id": chain,
                    "user": user,
                    "object_cid": "0x" + cid,
                    "transaction_hash": "0x" + tx,
                    "timestamp": 101000,
                }
                db.add(EventAddObject(**fields))
                if group:
                    db.add(EventAddSetObject(**fields, set_cid="0x" + group))
            db.commit()
        self.node.sync_index(block_batch_size=1)
        self.cs.reset_mock()
        self.other.reset_mock()
        for user in ("ALICE", "bob", "unknown"):
            for size, offset in (
                (1, 0),
                (1, 1),
                (1, 2),
                (2, 3),
                (0, 0),
                (2, 10**30),
                (10**30, 0),
            ):
                with self.subTest(user=user, limit=size, offset=offset):
                    for method in (
                        "find_user_objects_page",
                        "find_user_collection_cids",
                    ):
                        self.assertEqual(
                            getattr(self.node, method)(user, limit=size, offset=offset),
                            getattr(sql, method)(user, limit=size, offset=offset),
                        )
        stamps = self.node.find_user_objects_page("alice", limit=10)
        self.assertEqual(stamps.total, 4)
        self.assertEqual(
            [
                (r["chainId"], r["transactionHash"], r["objectCid"])
                for r in stamps.receipts
            ],
            [
                (1, "0x01", "0xaa"),
                (1, "0x01", "0xab"),
                (1, "0x02", "0xaa"),
                (2, "0x01", "0xaa"),
            ],
        )
        self.assertEqual(stamps.receipts[0]["timestamp"], "1970-01-01 00:01:41+00:00")
        stamps.receipts[0].clear()
        self.assertTrue(self.node.find_user_objects_page("alice", limit=1).receipts[0])
        self.assertEqual(
            self.node.find_user_collection_cids("alice", limit=3).cids, ["0xbb", "0xff"]
        )
        self.assertFalse(self.cs.mock_calls or self.other.mock_calls)

    def test_new_session_and_incremental_refresh(self):
        """Only the owner sees its index; a refresh fetches new ranges once per unique source."""
        self.node.sync_index()
        self.cs.reset_mock()
        fresh = Web3HTTPIndexingService([self.cs])
        for read in (fresh.find_user_collection_cids, fresh.find_user_objects_page):
            with self.assertRaisesRegex(RuntimeError, "not ready"):
                read("alice", limit=1)
        self.assertFalse(self.cs.mock_calls)
        self.blocks.append([("alice", "aa", "03", "aa")])
        self.node.sync_index()
        for event in (self.cs.csc.events.AddObject, self.cs.csc.events.AddSetObject):
            event.get_logs.assert_called_once_with(
                from_block=2, to_block=2, argument_filters={}
            )
        self.assertEqual(self.node.find_user_objects_page("alice", limit=0).total, 5)
        fresh.sync_index()
        self.assertEqual(fresh.find_user_objects_page("alice", limit=0).total, 4)

    def test_failed_sync_does_not_publish_or_duplicate_partial_ranges(self):
        """Resume complete ranges without exposing one source or half a batch."""
        logs = self.cs.csc.events.AddSetObject.get_logs
        good = logs.side_effect
        logs.side_effect = ValueError("interrupted")
        with self.assertRaisesRegex(ValueError, "interrupted"):
            self.node.sync_index()
        with self.assertRaisesRegex(RuntimeError, "not ready"):
            self.node.find_user_objects_page("alice", limit=1)
        self.other.reset_mock()
        logs.side_effect = good
        self.node.sync_index()
        self.other.csc.events.AddObject.get_logs.assert_not_called()
        self.assertEqual(self.node.find_user_objects_page("alice", limit=0).total, 4)
        self.blocks.append([("alice", "aa", "03", "aa")])
        logs.side_effect = ValueError("second interruption")
        with self.assertRaisesRegex(ValueError, "interruption"):
            self.node.sync_index()
        self.assertEqual(self.node.find_user_objects_page("alice", limit=0).total, 4)
        logs.side_effect = good
        self.node.sync_index()
        self.assertEqual(self.node.find_user_objects_page("alice", limit=0).total, 5)

    def test_cancelled_range_or_publication_cannot_duplicate_stamps(self):
        """A notebook interrupt after decoding or during publish must be safe to retry."""
        node = Web3HTTPIndexingService([_source(1, [[("alice", "aa", "01", "ff")]])])
        read = Web3EventIndex._read_range  # pylint: disable=protected-access

        def interrupted(*args):
            read(*args)
            raise KeyboardInterrupt

        with patch.object(Web3EventIndex, "_read_range", side_effect=interrupted):
            with self.assertRaises(KeyboardInterrupt):
                node.sync_index()
        with patch("vbase.core.web3_event_index.sorted", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                node.sync_index()
        with self.assertRaisesRegex(RuntimeError, "not ready"):
            node.find_user_objects_page("alice", limit=0)
        node.sync_index()
        self.assertEqual(node.find_user_objects_page("alice", limit=2).total, 1)
        self.assertEqual(
            node.find_user_collection_cids("alice", limit=2).cids, ["0xff"]
        )

    def test_finality_and_validation(self):
        """A mismatched finalized block or source cannot contaminate an existing index."""
        for size in (0, -1, True, 1.5):
            with self.subTest(size=size), self.assertRaises(ValueError):
                self.node.sync_index(block_batch_size=size)
        self.assertFalse(self.cs.mock_calls)
        self.node.sync_index()
        self.cs.reset_mock()
        self.cs.w3.eth.get_block.side_effect = lambda _: {"number": 2, "hash": b"fork"}
        with self.assertRaisesRegex(RuntimeError, "checkpoint"):
            self.node.sync_index()
        self.cs.csc.events.AddObject.get_logs.assert_not_called()
        self.cs.w3.eth.chain_id = 3
        with self.assertRaisesRegex(ValueError, "sources changed"):
            self.node.sync_index()
