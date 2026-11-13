"""Tests for FailoverIndexingService."""

import traceback
import unittest
from unittest.mock import create_autospec

from vbase.core.failover_indexing_service import FailoverIndexingService
from vbase.core.indexing_service import IndexingService


class TestFailoverIndexingService(unittest.TestCase):
    """Unit tests for FailoverIndexingService."""

    def setUp(self):
        # Create two mock services
        self.service1 = create_autospec(IndexingService)
        self.service2 = create_autospec(IndexingService)
        self.failover_service = FailoverIndexingService([self.service1, self.service2])

    def test_find_user_sets_success_first_service(self):
        """Returns the result from the first service when it succeeds."""
        self.service1.find_user_sets.return_value = [{"id": 1}]
        result = self.failover_service.find_user_sets("user1")
        self.assertEqual(result, [{"id": 1}])
        self.service1.find_user_sets.assert_called_once_with("user1")
        self.service2.find_user_sets.assert_not_called()

    def test_find_user_sets_failover_to_second_service(self):
        """Falls back to the second service when the first raises an exception."""
        self.service1.find_user_sets.side_effect = Exception("fail")
        self.service2.find_user_sets.return_value = [{"id": 2}]
        result = self.failover_service.find_user_sets("user2")
        self.assertEqual(result, [{"id": 2}])
        self.service1.find_user_sets.assert_called_once_with("user2")
        self.service2.find_user_sets.assert_called_once_with("user2")

    def test_failover_log_redacts_rpc_url(self):
        """Fallback logs the error without provider credentials."""
        secret_url = "https://rpc.example/v2/private-rpc-token"
        self.service1.find_user_sets.side_effect = ConnectionError(
            f"Failed to reach {secret_url}"
        )
        self.service1.__str__.return_value = secret_url
        self.service2.find_user_sets.return_value = [{"id": 2}]

        with self.assertLogs(
            "vbase.core.failover_indexing_service", level="ERROR"
        ) as captured:
            result = self.failover_service.find_user_sets("user2")

        self.assertEqual(result, [{"id": 2}])
        self.service2.find_user_sets.assert_called_once_with("user2")
        self.assertEqual(
            captured.records[0].getMessage(),
            f"Service {type(self.service1).__name__} failed to execute "
            "find_user_sets with error: ConnectionError: "
            "Failed to reach https://rpc.example/[REDACTED]",
        )

    def test_find_user_sets_all_services_fail(self):
        """Raises an exception with 'All services failed' when every service fails."""
        last_error = Exception("fail2")
        self.service1.find_user_sets.side_effect = Exception("fail1")
        self.service2.find_user_sets.side_effect = last_error
        with self.assertRaises(Exception) as cm:
            self.failover_service.find_user_sets("user3")
        self.assertIn("All services failed", str(cm.exception))
        self.assertIs(cm.exception.__cause__, last_error)

    def test_all_services_fail_cause_redacts_rpc_url(self):
        """The chained cause keeps its type but not provider credentials."""
        secret = "private-rpc-token"
        last_error = ConnectionError(f"Failed to reach https://rpc.example/v2/{secret}")
        self.service1.find_user_sets.side_effect = Exception("fail1")
        self.service2.find_user_sets.side_effect = last_error

        with self.assertLogs("vbase.core.failover_indexing_service", level="ERROR"):
            with self.assertRaises(RuntimeError) as cm:
                self.failover_service.find_user_sets("user3")

        self.assertIs(cm.exception.__cause__, last_error)
        self.assertEqual(
            str(last_error), "Failed to reach https://rpc.example/[REDACTED]"
        )
        self.assertNotIn(secret, "".join(traceback.format_exception(cm.exception)))

    def test_find_user_objects(self):
        """Delegates to the first available service for find_user_objects."""
        self.service1.find_user_objects.return_value = [{"obj": "a"}]
        result = self.failover_service.find_user_objects("userX")
        self.assertEqual(result, [{"obj": "a"}])
        self.service1.find_user_objects.assert_called_once_with("userX", False)

    def test_find_user_set_objects(self):
        """Delegates to the first available service for find_user_set_objects."""
        self.service1.find_user_set_objects.return_value = {"set": "abc"}
        result = self.failover_service.find_user_set_objects("userY", "set123")
        self.assertEqual(result, {"set": "abc"})
        self.service1.find_user_set_objects.assert_called_once_with("userY", "set123")

    def test_find_last_user_set_object(self):
        """Delegates to the first available service for find_last_user_set_object."""
        self.service1.find_last_user_set_object.return_value = {"last": "obj"}
        result = self.failover_service.find_last_user_set_object("userZ", "set456")
        self.assertEqual(result, {"last": "obj"})
        self.service1.find_last_user_set_object.assert_called_once_with(
            "userZ", "set456"
        )

    def test_find_object_failover(self):
        """Falls back to the second service for find_object when the first fails."""
        self.service1.find_object.side_effect = Exception("fail")
        self.service2.find_object.return_value = {"cid": "c2"}
        result = self.failover_service.find_object("c2", return_set_cids=False)
        self.assertEqual(result, {"cid": "c2"})
        self.service1.find_object.assert_called_once_with("c2", return_set_cids=False)
        self.service2.find_object.assert_called_once_with("c2", return_set_cids=False)


if __name__ == "__main__":
    unittest.main()
