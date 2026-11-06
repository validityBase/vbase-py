"""VDT-898: serialize writes before nonce lookup, across client instances."""

import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

from eth_account import Account

from vbase.core.forwarder_commitment_service import ForwarderCommitmentService
from vbase.core.user_write_queue import serialize_user_write


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
