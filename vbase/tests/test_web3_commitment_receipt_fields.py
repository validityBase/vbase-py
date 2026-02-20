"""Tests for Web3 commitment receipt fields (userAddress, setCid).

Verify that _add_object_worker and _add_set_object_worker return commitment
logs with userAddress and (for set) setCid, without requiring a live node.
"""

import unittest
from unittest.mock import MagicMock, patch

from hexbytes import HexBytes

from vbase.core.web3_commitment_service import Web3CommitmentService


class _ConcreteCommitmentService(Web3CommitmentService):
    """Concrete subclass to test abstract Web3CommitmentService."""

    @staticmethod
    def get_init_args_from_env(dotenv_path=None) -> dict:
        raise NotImplementedError

    @classmethod
    def create_instance_from_env(cls, dotenv_path=None):
        raise NotImplementedError

    def add_set(self, set_cid: str) -> dict:
        raise NotImplementedError

    def user_set_exists(self, user: str, set_cid: str) -> bool:
        raise NotImplementedError

    def verify_user_sets(self, user: str, user_set_cid_sum: str) -> bool:
        raise NotImplementedError

    def add_object(self, object_cid: str) -> dict:
        raise NotImplementedError

    def verify_user_object(
        self, user: str, object_cid: str, timestamp: str
    ) -> bool:
        raise NotImplementedError

    def add_set_object(self, set_cid: str, object_cid: str) -> dict:
        raise NotImplementedError

    def add_sets_objects_batch(
        self, set_cids: list, object_cids: list
    ) -> list:
        raise NotImplementedError

    def add_set_objects_batch(self, set_cid: str, object_cids: list) -> list:
        raise NotImplementedError

    def verify_user_set_objects(
        self, user: str, set_cid: str, user_set_object_cid_sum: str
    ) -> bool:
        raise NotImplementedError


class TestWeb3CommitmentReceiptFields(unittest.TestCase):
    """Test that commitment log includes userAddress and setCid."""

    def setUp(self):
        """Set up the tests."""
        self.w3 = MagicMock()
        self.csc = MagicMock()
        self.service = _ConcreteCommitmentService(self.w3, self.csc)

    def test_add_object_worker_returns_user_address(self):
        """_add_object_worker must include userAddress in the commitment log."""
        user_addr = "0x1234567890123456789012345678901234567890"
        object_cid_bytes = HexBytes(
            b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"
            b"\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f"
        )
        timestamp_chain = 1700000000  # uint256
        receipt = {
            "status": 1,
            "transactionHash": HexBytes(b"\xab" * 32),
            "logs": [MagicMock()],
        }
        mock_event_args = {
            "user": user_addr,
            "objectCid": object_cid_bytes,
            "timestamp": timestamp_chain,
        }
        mock_process_log = MagicMock(return_value={"args": mock_event_args})

        with patch.object(
            self.csc.events.AddObject.return_value,
            "process_log",
            mock_process_log,
        ):
            # pylint: disable=protected-access
            cl = self.service._add_object_worker(receipt)

        self.assertIn("userAddress", cl)
        self.assertEqual(cl["userAddress"], user_addr)
        self.assertEqual(cl["user"], user_addr)

    def test_add_set_object_worker_returns_user_address_and_set_cid(self):
        """_add_set_object_worker must include userAddress and setCid."""
        user_addr = "0xAbCdEf1234567890AbCdEf1234567890AbCdEf12"
        set_cid_bytes = HexBytes(
            b"\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99"
            b"\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99"
        )
        object_cid_bytes = HexBytes(
            b"\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00"
            b"\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00"
        )
        timestamp_chain = 1700000001
        receipt = {
            "status": 1,
            "transactionHash": HexBytes(b"\xcd" * 32),
            "logs": [MagicMock(), MagicMock()],
        }
        add_set_object_args = {
            "user": user_addr,
            "setCid": set_cid_bytes,
            "objectCid": object_cid_bytes,
            "timestamp": timestamp_chain,
        }
        add_object_args = {
            "user": user_addr,
            "objectCid": object_cid_bytes,
            "timestamp": timestamp_chain,
        }

        with patch.object(
            self.csc.events.AddSetObject.return_value,
            "process_log",
            MagicMock(return_value={"args": add_set_object_args}),
        ), patch.object(
            self.csc.events.AddObject.return_value,
            "process_log",
            MagicMock(return_value={"args": add_object_args}),
        ):
            # pylint: disable=protected-access
            cl = self.service._add_set_object_worker(receipt)

        self.assertIn("userAddress", cl)
        self.assertEqual(cl["userAddress"], user_addr)
        self.assertIn("setCid", cl)
        self.assertEqual(cl["setCid"], "0x" + set_cid_bytes.hex())
