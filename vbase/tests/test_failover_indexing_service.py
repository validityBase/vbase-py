import unittest
from unittest.mock import MagicMock, create_autospec
from vbase.core.failover_indexing_service import FailoverIndexingService
from vbase.core.indexing_service import IndexingService

class TestFailoverIndexingService(unittest.TestCase):
    def setUp(self):
        # Create two mock services
        self.service1 = create_autospec(IndexingService)
        self.service2 = create_autospec(IndexingService)
        self.failover_service = FailoverIndexingService([self.service1, self.service2])

    def test_find_user_sets_success_first_service(self):
        self.service1.find_user_sets.return_value = [{"id": 1}]
        result = self.failover_service.find_user_sets("user1")
        self.assertEqual(result, [{"id": 1}])
        self.service1.find_user_sets.assert_called_once_with("user1")
        self.service2.find_user_sets.assert_not_called()

    def test_find_user_sets_failover_to_second_service(self):
        self.service1.find_user_sets.side_effect = Exception("fail")
        self.service2.find_user_sets.return_value = [{"id": 2}]
        result = self.failover_service.find_user_sets("user2")
        self.assertEqual(result, [{"id": 2}])
        self.service1.find_user_sets.assert_called_once_with("user2")
        self.service2.find_user_sets.assert_called_once_with("user2")

    def test_find_user_sets_all_services_fail(self):
        self.service1.find_user_sets.side_effect = Exception("fail1")
        self.service2.find_user_sets.side_effect = Exception("fail2")
        with self.assertRaises(Exception) as cm:
            self.failover_service.find_user_sets("user3")
        self.assertIn("All services failed", str(cm.exception))

    def test_find_user_objects(self):
        self.service1.find_user_objects.return_value = [{"obj": "a"}]
        result = self.failover_service.find_user_objects("userX")
        self.assertEqual(result, [{"obj": "a"}])
        self.service1.find_user_objects.assert_called_once_with("userX")

    def test_find_user_set_objects(self):
        self.service1.find_user_set_objects.return_value = {"set": "abc"}
        result = self.failover_service.find_user_set_objects("userY", "set123")
        self.assertEqual(result, {"set": "abc"})
        self.service1.find_user_set_objects.assert_called_once_with("userY", "set123")

    def test_find_last_user_set_object(self):
        self.service1.find_last_user_set_object.return_value = {"last": "obj"}
        result = self.failover_service.find_last_user_set_object("userZ", "set456")
        self.assertEqual(result, {"last": "obj"})
        self.service1.find_last_user_set_object.assert_called_once_with("userZ", "set456")

    def test_find_object_failover(self):
        self.service1.find_object.side_effect = Exception("fail")
        self.service2.find_object.return_value = {"cid": "c2"}
        result = self.failover_service.find_object("c2", return_set_cids=False)
        self.assertEqual(result, {"cid": "c2"})
        self.service1.find_object.assert_called_once_with("c2", return_set_cids=False)
        self.service2.find_object.assert_called_once_with("c2", return_set_cids=False)

if __name__ == "__main__":
    unittest.main()
