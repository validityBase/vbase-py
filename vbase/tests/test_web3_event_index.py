"""Cross-backend pagination and the lifetime of a Web3 indexing session."""

import unittest
from unittest.mock import MagicMock, patch

from sqlmodel import Session, SQLModel

from vbase import SetReceiptPage, SQLIndexingService, Web3HTTPIndexingService
from vbase.core.models import EventAddObject, EventAddSetObject, LastBatchProcessingTime
from vbase.core.web3_commitment_service import Web3CommitmentService
from vbase.core.web3_event_index import Web3EventIndex


def _source(chain: int, blocks: list) -> MagicMock:
    """Each block contains (user, object CID, transaction hash, optional set CID) commitments."""
    cs = MagicMock()
    cs.commitment_service_address = "0xcontract"
    cs.w3.eth.chain_id = chain
    cs.convert_timestamp_chain_to_str.side_effect = (
        Web3CommitmentService.convert_timestamp_chain_to_str
    )

    def header(tag):
        n = len(blocks) - 1 if tag == "finalized" else tag
        return {
            "number": n,
            "hash": n.to_bytes(32, "big"),
            "timestamp": 100 + n,
            "parentHash": max(0, n - 1).to_bytes(32, "big"),
        }

    def logs(is_membership, from_block, to_block, argument_filters):
        assert not argument_filters
        return [
            {
                "blockNumber": n,
                "blockHash": n.to_bytes(32, "big"),
                "address": "0xcontract",
                "logIndex": i * 2 + int(is_membership),
                "args": {
                    "user": user,
                    "objectCid": bytes.fromhex(cid),
                    "timestamp": 42,
                    "setCid": bytes.fromhex(group or ""),
                },
                "transactionHash": bytes.fromhex(tx),
            }
            for n in range(from_block, to_block + 1)
            for i, (user, cid, tx, group) in enumerate(blocks[n])
            if not is_membership or group is not None
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
        """Cover time ties, repeated commitments, mixed case, and count-only reads."""
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
                        "find_user_set_cids",
                    ):
                        self.assertEqual(
                            getattr(self.node, method)(user, limit=size, offset=offset),
                            getattr(sql, method)(user, limit=size, offset=offset),
                        )
        objects = self.node.find_user_objects_page("alice", limit=10)
        self.assertEqual(objects.total, 4)
        self.assertEqual(
            [
                (r["chainId"], r["transactionHash"], r["objectCid"])
                for r in objects.receipts
            ],
            [
                (1, "0x01", "0xaa"),
                (1, "0x01", "0xab"),
                (1, "0x02", "0xaa"),
                (2, "0x01", "0xaa"),
            ],
        )
        self.assertEqual(objects.receipts[0]["timestamp"], "1970-01-01 00:01:41+00:00")
        objects.receipts[0].clear()
        self.assertTrue(self.node.find_user_objects_page("alice", limit=1).receipts[0])
        sets = self.node.find_user_set_cids("alice", limit=3)
        self.assertIsInstance(sets, SetReceiptPage)
        self.assertEqual(vars(sets), {"cids": ["0xbb", "0xff"], "total": 2})
        self.assertFalse(self.cs.mock_calls or self.other.mock_calls)

    def test_new_session_and_incremental_refresh(self):
        """Only the owner sees its index; a refresh fetches new ranges once per unique source."""
        self.node.sync_index()
        self.cs.reset_mock()
        fresh = Web3HTTPIndexingService([self.cs])
        for read in (fresh.find_user_set_cids, fresh.find_user_objects_page):
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

    def test_cancelled_range_or_publication_cannot_duplicate_commitments(self):
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
        self.assertEqual(node.find_user_set_cids("alice", limit=2).cids, ["0xff"])

    def test_finality_and_validation(self):
        """A mismatched finalized block or source cannot contaminate an existing index."""
        for size in (0, -1, True, 1.5):
            with self.subTest(size=size), self.assertRaises(ValueError):
                self.node.sync_index(block_batch_size=size)
        self.assertFalse(self.cs.mock_calls)
        self.node.sync_index()
        self.cs.reset_mock()
        original_header = self.cs.w3.eth.get_block.side_effect
        self.cs.w3.eth.get_block.side_effect = lambda _: {"number": 2, "hash": b"fork"}
        with self.assertRaisesRegex(RuntimeError, "checkpoint"):
            self.node.sync_index()
        self.cs.csc.events.AddObject.get_logs.assert_not_called()
        self.cs.w3.eth.get_block.side_effect = original_header
        with self.assertRaisesRegex(RuntimeError, "new indexing session"):
            self.node.sync_index()
        for read in (
            self.node.find_user_set_cids,
            self.node.find_user_objects_page,
        ):
            with self.assertRaisesRegex(RuntimeError, "session"):
                read("alice", limit=0)

    def test_changed_sources_do_not_serve_the_old_view(self):
        """Changing an endpoint's identity invalidates both views."""
        self.node.sync_index()
        self.cs.w3.eth.chain_id = 3
        with self.assertRaisesRegex(ValueError, "sources changed"):
            self.node.sync_index()
        with self.assertRaisesRegex(RuntimeError, "session"):
            self.node.find_user_objects_page("alice", limit=1)

    def test_lagging_finalized_height_is_not_a_confirmed_fork(self):
        """Keep the published snapshot after a lagging response, then resume normally."""
        self.node.sync_index()
        header = self.cs.w3.eth.get_block.side_effect
        self.cs.w3.eth.get_block.side_effect = lambda tag: header(
            0 if tag == "finalized" else tag
        )
        with self.assertRaisesRegex(RuntimeError, "behind"):
            self.node.sync_index()
        self.assertEqual(self.node.find_user_objects_page("alice", limit=0).total, 4)
        self.cs.w3.eth.get_block.side_effect = header
        self.node.sync_index()

    def test_history_switch_between_ranges_is_not_published(self):
        """The next range must extend the already accepted history, even in one sync."""
        self.blocks.append([("alice", "cc", "03", "cc")])
        header = self.cs.w3.eth.get_block.side_effect
        switched = False

        def changing_header(tag):
            nonlocal switched
            if tag == 2:
                switched = True
            block = header(tag)
            if switched:
                block["hash"] = b"B" + block["hash"][1:]
                block["parentHash"] = b"B" + (max(0, block["number"] - 1)).to_bytes(
                    31, "big"
                )
            return block

        self.cs.w3.eth.get_block.side_effect = changing_header
        with self.assertRaisesRegex(RuntimeError, "history|checkpoint"):
            self.node.sync_index(block_batch_size=1)
        with self.assertRaises(RuntimeError):
            self.node.find_user_set_cids("alice", limit=1)

    def test_log_from_a_different_block_hash_invalidates_the_view(self):
        """Reject a foreign log from either stream, not just a changed range end."""
        for event_type in ("AddObject", "AddSetObject"):
            with self.subTest(event_type=event_type):
                blocks = [[("alice", "aa", "01", "ff")]]
                cs = _source(1, blocks)
                node = Web3HTTPIndexingService([cs])
                node.sync_index()
                blocks.append([("alice", "bb", "02", "ee")])
                logs = getattr(cs.csc.events, event_type).get_logs
                logs.side_effect = lambda original=logs.side_effect, **kw: [
                    e | {"blockHash": b"other"} for e in original(**kw)
                ]
                with self.assertRaisesRegex(RuntimeError, "history"):
                    node.sync_index()
                with self.assertRaisesRegex(RuntimeError, "session"):
                    node.find_user_objects_page("alice", limit=1)

    def test_empty_range_with_wrong_parent_is_rejected(self):
        """Even when log filters return nothing, a range cannot cross a fork."""
        self.node.sync_index()
        self.blocks.extend([[], []])
        header = self.cs.w3.eth.get_block.side_effect

        def unrelated(tag):
            block = header(tag)
            if tag == 2:
                block["parentHash"] = b"other"
            return block

        self.cs.w3.eth.get_block.side_effect = unrelated
        with self.assertRaisesRegex(RuntimeError, "checkpoint"):
            self.node.sync_index()
        with self.assertRaisesRegex(RuntimeError, "session"):
            self.node.find_user_set_cids("alice", limit=0)

    def test_sparse_history_keeps_range_queries_and_bounded_header_reads(self):
        """Do not walk every empty block just to check a provider's range result."""
        cs = _source(1, [[] for _ in range(101)])
        node = Web3HTTPIndexingService([cs])
        node.sync_index(block_batch_size=32)
        expected = [(0, 31), (32, 63), (64, 95), (96, 100)]
        for stream in (cs.csc.events.AddObject, cs.csc.events.AddSetObject):
            calls = stream.get_logs.call_args_list
            self.assertEqual(len(calls), len(expected))
            self.assertEqual(
                [(c.kwargs["from_block"], c.kwargs["to_block"]) for c in calls],
                expected,
            )
        self.assertLess(cs.w3.eth.get_block.call_count, 5 * len(expected))
        self.assertEqual(node.find_user_set_cids("alice", limit=1).total, 0)

    def test_range_end_changes_during_empty_log_query(self):
        """Recheck the end hash after both range queries, before accepting the batch."""
        self.node.sync_index()
        self.blocks.extend([[], []])
        header = self.cs.w3.eth.get_block.side_effect

        def switch(**_):
            self.cs.w3.eth.get_block.side_effect = lambda tag: (
                header(tag) | {"hash": b"fork"} if tag == 3 else header(tag)
            )
            return []

        self.cs.csc.events.AddSetObject.get_logs.side_effect = switch
        with self.assertRaisesRegex(RuntimeError, "history"):
            self.node.sync_index()
        with self.assertRaisesRegex(RuntimeError, "session"):
            self.node.find_user_objects_page("alice", limit=0)

    def test_foreign_contract_and_duplicate_logs_are_not_indexed(self):
        """A valid block hash alone does not prove an event belongs in this view."""
        for corrupt in (lambda e: [e | {"address": "0xelsewhere"}], lambda e: [e, e]):
            with self.subTest(corrupt=corrupt):
                cs = _source(1, [[("alice", "aa", "01", "ff")]])
                node = Web3HTTPIndexingService([cs])
                logs = cs.csc.events.AddObject.get_logs
                logs.side_effect = (
                    lambda original=logs.side_effect, fn=corrupt, **kw: fn(
                        original(**kw)[0]
                    )
                )
                with self.assertRaisesRegex(RuntimeError, "history"):
                    node.sync_index()
