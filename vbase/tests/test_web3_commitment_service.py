"""Unit tests for Web3 commitment receipt processing."""

# pylint: disable=protected-access

import unittest
from unittest.mock import MagicMock, call, patch

from vbase.core.forwarder_commitment_service import ForwarderCommitmentService
from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService


class TestWeb3CommitmentService(unittest.TestCase):
    """Test behavior shared by direct and forwarded commitment services."""

    def setUp(self):
        self.user = "0x0000000000000000000000000000000000000001"
        self.set_cid = "0x" + "01" * 32
        self.receipt = {
            "status": 1,
            "transactionHash": "0xtransaction",
            "logs": [],
        }
        service = object.__new__(ForwarderCommitmentService)
        service.csc = MagicMock()
        service.get_default_user = MagicMock(return_value=self.user)
        service._get_chain_id = MagicMock(return_value=84532)
        self.service = service

    def test_add_set_trusts_successful_event_when_state_read_is_stale(self):
        """A matching receipt event is authoritative over a lagging state read."""
        self.service.csc.events.AddSet.return_value.process_receipt.return_value = [
            {"args": {"user": self.user, "setCid": bytes.fromhex("01" * 32)}}
        ]
        self.service.user_set_exists = MagicMock(return_value=False)

        commitment = self.service._add_set_worker(self.set_cid, self.receipt)

        self.assertEqual(commitment["setCid"], self.set_cid)
        self.assertEqual(commitment["userAddress"], self.user)
        self.service.user_set_exists.assert_not_called()

    def test_add_set_rejects_event_for_different_user(self):
        """An AddSet event for another user does not prove the requested write."""
        self.service.csc.events.AddSet.return_value.process_receipt.return_value = [
            {
                "args": {
                    "user": "0x0000000000000000000000000000000000000002",
                    "setCid": bytes.fromhex("01" * 32),
                }
            }
        ]

        with self.assertRaisesRegex(
            RuntimeError, "AddSet event user does not match requested user"
        ):
            self.service._add_set_worker(self.set_cid, self.receipt)

    def test_add_set_rejects_event_for_different_set(self):
        """An AddSet event for another set does not prove the requested write."""
        self.service.csc.events.AddSet.return_value.process_receipt.return_value = [
            {"args": {"user": self.user, "setCid": bytes.fromhex("02" * 32)}}
        ]

        with self.assertRaisesRegex(
            RuntimeError, "AddSet event set CID does not match requested set CID"
        ):
            self.service._add_set_worker(self.set_cid, self.receipt)

    @patch("retry.api.time.sleep")
    def test_add_set_retries_eventless_state_confirmation(self, sleep_mock):
        """An eventless add retries while commitment visibility catches up."""
        self.service.csc.events.AddSet.return_value.process_receipt.return_value = []
        self.service.user_set_exists = MagicMock(side_effect=[False, False, True])

        commitment = self.service._add_set_worker(self.set_cid, self.receipt)

        self.assertEqual(commitment, {})
        self.assertEqual(self.service.user_set_exists.call_count, 3)
        self.service.user_set_exists.assert_called_with(self.user, self.set_cid)
        self.assertEqual(sleep_mock.call_count, 2)
        sleep_mock.assert_has_calls(
            [
                call(self.service.RETRY_DELAY),
                call(self.service.RETRY_DELAY * self.service.RETRY_BACKOFF),
            ]
        )

    @patch("retry.api.time.sleep")
    def test_add_set_rejects_eventless_transaction_without_commitment(self, sleep_mock):
        """An eventless add fails when it did not create or find a commitment."""
        self.service.csc.events.AddSet.return_value.process_receipt.return_value = []
        self.service.user_set_exists = MagicMock(return_value=False)

        with self.assertRaisesRegex(
            RuntimeError,
            "AddSet event not found and set commitment does not exist",
        ):
            self.service._add_set_worker(self.set_cid, self.receipt)
        self.assertEqual(self.service.user_set_exists.call_count, 3)
        self.assertEqual(sleep_mock.call_count, 2)

    def _set_add_object_events(self, count):
        """Make the receipt yield count AddObject events."""
        self.service.csc.events.AddObject.return_value.process_receipt.return_value = [
            {
                "args": {
                    "user": self.user,
                    "objectCid": bytes([i + 1]) * 32,
                    "timestamp": 1_700_000_000 + i,
                }
            }
            for i in range(count)
        ]

    def test_batch_returns_one_receipt_per_object(self):
        """A batch receipt with one AddObject event per object is accepted."""
        self._set_add_object_events(2)

        commitments = self.service._add_sets_objects_batch_worker(self.receipt, 2)

        self.assertEqual(
            [c["objectCid"] for c in commitments], ["0x" + "01" * 32, "0x" + "02" * 32]
        )
        self.assertEqual({c["chainId"] for c in commitments}, {84532})

    def test_batch_accepts_empty_batch(self):
        """An empty batch has no AddObject events and no receipts."""
        self._set_add_object_events(0)

        self.assertEqual(
            self.service._add_sets_objects_batch_worker(self.receipt, 0), []
        )

    def test_batch_rejects_event_count_mismatch(self):
        """Missing or extra AddObject events raise instead of partial receipts."""
        hint = ForwarderCommitmentService._NO_EVENTS_HINT
        for n_events in (0, 1, 3):
            with self.subTest(n_events=n_events):
                self._set_add_object_events(n_events)

                with self.assertRaisesRegex(
                    RuntimeError,
                    f"Expected 2 AddObject events in receipt 0xtransaction, "
                    f"found {n_events}\\.",
                ) as raised:
                    self.service._add_sets_objects_batch_worker(self.receipt, 2)
                # A revert discards all of a call's events, so only a receipt
                # without any of them can come from a reverted call.
                self.assertEqual(hint in str(raised.exception), n_events == 0)

    def test_batch_without_events_hints_at_the_likely_cause(self):
        """Each service names the likely cause of a receipt without events."""
        direct = object.__new__(Web3HTTPCommitmentService)
        direct.csc = self.service.csc
        self._set_add_object_events(0)
        for service, cause in (
            (self.service, "forwarded commitment call may have reverted"),
            (direct, "address may not hold the commitment contract"),
        ):
            with self.subTest(service=type(service).__name__):
                with self.assertRaisesRegex(RuntimeError, cause):
                    service._add_sets_objects_batch_worker(self.receipt, 2)

    def test_batch_rejects_missing_object_list(self):
        """None is not an empty batch."""
        self.service._post_execute = MagicMock()
        with self.assertRaises(TypeError):
            self.service.add_set_objects_batch(self.set_cid, None)
        self.service._post_execute.assert_not_called()

    def test_batch_commitment_checks_submitted_object_count(self):
        """The batch methods pass the number of objects they committed."""
        self._set_add_object_events(1)
        self.service._post_execute = MagicMock(return_value=self.receipt)

        with self.assertRaisesRegex(RuntimeError, "Expected 2 AddObject events"):
            self.service.add_set_objects_batch(
                self.set_cid, ["0x" + "0a" * 32, "0x" + "0b" * 32]
            )
        with self.assertRaisesRegex(RuntimeError, "Expected 2 AddObject events"):
            self.service.add_sets_objects_batch(
                [self.set_cid, self.set_cid], ["0x" + "0a" * 32, "0x" + "0b" * 32]
            )

    def test_empty_batch_sends_no_transaction(self):
        """An empty batch returns an empty list without a transaction."""
        direct = object.__new__(Web3HTTPCommitmentService)
        direct.csc = MagicMock()
        self.service._post_execute = MagicMock()
        for service in (self.service, direct):
            with self.subTest(service=type(service).__name__):
                self.assertEqual(service.add_set_objects_batch(self.set_cid, []), [])
                self.assertEqual(service.add_sets_objects_batch([], []), [])
        self.service._post_execute.assert_not_called()
        direct.csc.functions.addSetObjectsBatch.assert_not_called()
        direct.csc.functions.addSetsObjectsBatch.assert_not_called()

    def test_batch_rejects_set_and_object_count_mismatch(self):
        """Set and object lists of different lengths raise before a transaction."""
        direct = object.__new__(Web3HTTPCommitmentService)
        direct.csc = MagicMock()
        self.service._post_execute = MagicMock()
        for service in (self.service, direct):
            for set_cids, object_cids in (
                ([self.set_cid], []),
                ([], ["0x" + "0a" * 32]),
            ):
                with self.subTest(service=type(service).__name__, sets=len(set_cids)):
                    with self.assertRaisesRegex(ValueError, "same length"):
                        service.add_sets_objects_batch(set_cids, object_cids)
        self.service._post_execute.assert_not_called()
        direct.csc.functions.addSetsObjectsBatch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
