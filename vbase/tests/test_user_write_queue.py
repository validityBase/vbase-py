"""VDT-898: serialize writes before nonce lookup, across client instances."""

import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

from eth_account import Account

from vbase.core.forwarder_commitment_service import ForwarderCommitmentService
from vbase.core.forwarder_commitment_service_test import ForwarderCommitmentServiceTest
from vbase.core.user_write_queue import serialize_user_write
from vbase.core.web3_http_commitment_service_test import Web3HTTPCommitmentServiceTest


class UserWriteQueueTests(unittest.TestCase):
    """Test nonce ownership across service instances."""

    def test_forwarder_instances_refresh_nonce_under_shared_ownership(self):
        """Test forwarder instances refresh nonce under shared ownership."""
        account = Account.create()
        state = {"nonce": 0, "active": 0, "maximum": 0}

        def call(api, **kwargs):
            """Call."""
            if api == "signature-data":
                return {
                    "nonce": max(0, state["nonce"] - 1),
                    "domain": {
                        "name": "test",
                        "version": "1",
                        "chainId": 1,
                        "verifyingContract": "0x" + "00" * 20,
                    },
                }
            state["active"] += 1
            state["maximum"] = max(state["maximum"], state["active"])
            self.assertEqual(kwargs["data"]["forwardRequest"]["nonce"], state["nonce"])
            time.sleep(0.01)
            state["nonce"] += 1
            state["active"] -= 1
            return {"status": 1, "logs": []}

        clients = [
            ForwarderCommitmentService("https://forwarder.test/", "test", account.key)
            for _ in range(2)
        ]
        for client in clients:
            client._call_forwarder_api = Mock(side_effect=call)
        # Alternating instances must not reuse their separate cached nonce.
        for client in [clients[0], clients[1], clients[0]]:
            client._post_execute("addObject", ["0x" + "11" * 32])
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [
                executor.submit(
                    clients[i % 2]._post_execute, "addObject", ["0x" + "11" * 32]
                )
                for i in range(6)
            ]
            for future in futures:
                future.result(timeout=3)
        self.assertEqual(state["nonce"], 9)
        self.assertEqual(state["maximum"], 1)


class QueueBoundaryTests(unittest.TestCase):
    """Other users and failed operations retain forward progress."""

    def test_different_users_and_failure_release(self):
        """A failed holder releases its user while unrelated work progresses."""

        class Service:
            """Small service exposing the actual queue boundary."""

            node_rpc_url = "https://node.test"

            def __init__(self, user):
                self.user = user

            def get_default_user(self):
                """Return the fixture's public identity."""
                return self.user

            @serialize_user_write
            def write(self, operation):
                """Run work under production ownership."""
                return operation()

        entered, release = threading.Event(), threading.Event()
        first, second, other = Service("alice"), Service("alice"), Service("bob")

        def fail():
            entered.set()
            if not release.wait(3):
                raise TimeoutError("test barrier")
            raise ValueError("failed holder")

        with ThreadPoolExecutor(max_workers=3) as executor:
            one = executor.submit(first.write, fail)
            self.assertTrue(entered.wait(3))
            two = executor.submit(second.write, lambda: 2)
            self.assertEqual(
                executor.submit(other.write, lambda: 3).result(timeout=3), 3
            )
            self.assertFalse(two.done())
            release.set()
            with self.assertRaisesRegex(ValueError, "failed holder"):
                one.result(timeout=3)
            self.assertEqual(two.result(timeout=3), 2)


class PublicMutationOwnershipTests(unittest.TestCase):
    """Receipt decoding and test-contract writes retain the same ownership."""

    def test_forwarder_public_mutations_hold_ownership_through_decoding(self):
        """A second write cannot reset metadata while any receipt worker runs."""
        cases = [
            ("add_set", ("cid",)),
            ("add_object", ("cid",)),
            ("add_set_object", ("set", "cid")),
            ("add_sets_objects_batch", (["set"], ["cid"])),
            ("add_set_objects_batch", ("set", ["cid"])),
            ("add_object_with_timestamp", ("cid", "2026-11-06T00:00:00Z")),
            ("add_set_object_with_timestamp", ("set", "cid", "2026-11-06T00:00:00Z")),
            (
                "add_sets_objects_with_timestamps_batch",
                (["set"], ["cid"], ["2026-11-06T00:00:00Z"]),
            ),
            (
                "add_set_objects_with_timestamps_batch",
                ("set", ["cid"], ["2026-11-06T00:00:00Z"]),
            ),
        ]
        for method, args in cases:
            with self.subTest(method=method):
                service = ForwarderCommitmentServiceTest(
                    "https://forwarder.test/", "test", Account().create().key
                )
                entered, release, second_write = (threading.Event() for _ in range(3))
                calls = []

                def execute(**_kwargs):
                    calls.append(True)
                    if len(calls) == 1:
                        service._signature_data = {"domain": {"chainId": 1}}
                    else:
                        service._signature_data = None
                        second_write.set()
                    return {}

                def decode(*_args):
                    if not entered.is_set():
                        entered.set()
                        if not release.wait(3):
                            raise TimeoutError("test barrier")
                        return service._get_chain_id()
                    return 1

                service._post_execute = Mock(side_effect=execute)
                for worker in [
                    "_add_set_worker",
                    "_add_object_worker",
                    "_add_set_object_worker",
                    "_add_sets_objects_batch_worker",
                ]:
                    setattr(service, worker, Mock(side_effect=decode))
                with ThreadPoolExecutor(max_workers=2) as executor:
                    one = executor.submit(getattr(service, method), *args)
                    self.assertTrue(entered.wait(3))
                    two = executor.submit(service.add_object, "other")
                    try:
                        self.assertFalse(second_write.wait(0.05))
                    finally:
                        release.set()
                    self.assertEqual(one.result(timeout=3), 1)
                    two.result(timeout=3)

    def test_direct_test_mutations_cannot_bypass_ordinary_write(self):
        """Timestamped and clearing operations queue behind an ordinary receipt."""
        cases = [
            ("clear_sets", ()),
            ("clear_set_objects", ("set",)),
            ("add_object_with_timestamp", ("cid", "2026-11-06T00:00:00Z")),
            ("add_set_object_with_timestamp", ("set", "cid", "2026-11-06T00:00:00Z")),
            (
                "add_sets_objects_with_timestamps_batch",
                (["set"], ["cid"], ["2026-11-06T00:00:00Z"]),
            ),
            (
                "add_set_objects_with_timestamps_batch",
                ("set", ["cid"], ["2026-11-06T00:00:00Z"]),
            ),
        ]
        for method, args in cases:
            with self.subTest(method=method):
                service = Web3HTTPCommitmentServiceTest.__new__(
                    Web3HTTPCommitmentServiceTest
                )
                service.node_rpc_url = "https://node.test"
                service.get_default_user = Mock(return_value="alice")
                service.w3, service.csc = Mock(), Mock()
                entered, release, second_write = (threading.Event() for _ in range(3))

                def receipt(*_args):
                    if not entered.is_set():
                        entered.set()
                        if not release.wait(3):
                            raise TimeoutError("test barrier")
                    return {"status": 1}

                service.w3.eth.wait_for_transaction_receipt.side_effect = receipt
                for worker in [
                    "_add_object_worker",
                    "_add_set_object_worker",
                    "_add_sets_objects_batch_worker",
                ]:
                    setattr(service, worker, Mock(return_value={}))
                target = getattr(
                    service.csc.functions,
                    {
                        "clear_sets": "clearSets",
                        "clear_set_objects": "clearSetObjects",
                        "add_object_with_timestamp": "addObjectWithTimestamp",
                        "add_set_object_with_timestamp": "addSetObjectWithTimestamp",
                        "add_sets_objects_with_timestamps_batch": "addSetsObjectsWithTimestampsBatch",
                        "add_set_objects_with_timestamps_batch": "addSetObjectsWithTimestampsBatch",
                    }[method],
                )
                target.return_value.transact.side_effect = lambda: (
                    second_write.set(),
                    "hash",
                )[1]
                with ThreadPoolExecutor(max_workers=2) as executor:
                    one = executor.submit(service.add_object, "first")
                    self.assertTrue(entered.wait(3))
                    two = executor.submit(getattr(service, method), *args)
                    try:
                        self.assertFalse(second_write.wait(0.05))
                    finally:
                        release.set()
                    one.result(timeout=3)
                    two.result(timeout=3)
                self.assertEqual(
                    service.w3.eth.wait_for_transaction_receipt.call_count, 2
                )
