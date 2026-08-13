"""Unit tests for Web3 commitment receipt processing."""

# pylint: disable=protected-access

import unittest
from unittest.mock import MagicMock, call, patch

from vbase.core.forwarder_commitment_service import ForwarderCommitmentService


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


if __name__ == "__main__":
    unittest.main()
