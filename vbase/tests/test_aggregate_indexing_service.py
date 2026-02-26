"""Tests for AggregateIndexingService."""

import unittest
from unittest.mock import create_autospec

from vbase.core.aggregate_indexing_service import AggregateIndexingService
from vbase.core.indexing_service import IndexingService


class TestAggregateIndexingService(unittest.TestCase):
    """Unit tests for AggregateIndexingService."""

    def setUp(self):
        self.service1 = create_autospec(IndexingService)
        self.service2 = create_autospec(IndexingService)
        self.aggregate_service = AggregateIndexingService(
            [self.service1, self.service2]
        )

    def test_find_user_sets_aggregates_and_deduplicates(self):
        """Results from all services are merged and deduplicated by transactionHash."""
        user = "alice"
        result1 = [
            {"transactionHash": "tx1", "data": 1},
            {"transactionHash": "tx2", "data": 2},
        ]
        result2 = [
            {"transactionHash": "tx2", "data": 2},
            {"transactionHash": "tx3", "data": 3},
        ]
        self.service1.find_user_sets.return_value = result1
        self.service2.find_user_sets.return_value = result2

        result = self.aggregate_service.find_user_sets(user)
        hashes = {r["transactionHash"] for r in result}
        self.assertEqual(hashes, {"tx1", "tx2", "tx3"})
        self.assertEqual(len(result), 3)

    def test_find_user_objects_aggregates_and_deduplicates(self):
        """Object results from all services are merged and deduplicated by transactionHash."""
        user = "bob"
        self.service1.find_user_objects.return_value = [{"transactionHash": "a"}]
        self.service2.find_user_objects.return_value = [
            {"transactionHash": "b"},
            {"transactionHash": "a"},
        ]
        result = self.aggregate_service.find_user_objects(user)
        hashes = {r["transactionHash"] for r in result}
        self.assertEqual(hashes, {"a", "b"})
        self.assertEqual(len(result), 2)

    def test_find_user_set_objects_aggregates_and_deduplicates(self):
        """Set-object results from all services are merged and deduplicated by transactionHash."""
        user = "bob"
        set_cid = "set1"
        self.service1.find_user_set_objects.return_value = [{"transactionHash": "x"}]
        self.service2.find_user_set_objects.return_value = [
            {"transactionHash": "y"},
            {"transactionHash": "x"},
        ]
        result = self.aggregate_service.find_user_set_objects(user, set_cid)
        hashes = {r["transactionHash"] for r in result}
        self.assertEqual(hashes, {"x", "y"})
        self.assertEqual(len(result), 2)

    def test_find_last_user_set_object_returns_latest(self):
        """Returns the object with the highest timestamp across all services."""
        user = "alice"
        set_cid = "set2"
        obj1 = {"timestamp": 100, "transactionHash": "t1"}
        obj2 = {"timestamp": 200, "transactionHash": "t2"}
        self.service1.find_last_user_set_object.return_value = obj1
        self.service2.find_last_user_set_object.return_value = obj2
        result = self.aggregate_service.find_last_user_set_object(user, set_cid)
        self.assertEqual(result, obj2)

    def test_find_objects_aggregates_and_deduplicates(self):
        """Object lookup results from all services are merged
        and deduplicated by transactionHash."""
        cids = ["cid1", "cid2"]
        self.service1.find_objects.return_value = [
            {"transactionHash": "h1"},
            {"transactionHash": "h2"},
        ]
        self.service2.find_objects.return_value = [
            {"transactionHash": "h2"},
            {"transactionHash": "h3"},
        ]
        result = self.aggregate_service.find_objects(cids)
        hashes = {r["transactionHash"] for r in result}
        self.assertEqual(hashes, {"h1", "h2", "h3"})
        self.assertEqual(len(result), 3)

    def test_find_object_returns_first_non_empty(self):
        """Returns the result from the first service that has a non-empty response."""
        cid = "cid1"
        self.service1.find_object.return_value = []
        self.service2.find_object.return_value = [{"transactionHash": "h1"}]
        result = self.aggregate_service.find_object(cid)
        self.assertEqual(result, [{"transactionHash": "h1"}])

    def test_find_object_returns_empty_if_all_empty(self):
        """Returns an empty list when all services return empty results."""
        cid = "cid2"
        self.service1.find_object.return_value = []
        self.service2.find_object.return_value = []
        result = self.aggregate_service.find_object(cid)
        self.assertEqual(result, [])

    def test_find_last_object_returns_latest(self):
        """Returns the object with the highest timestamp across all services."""
        cid = "cid3"
        obj1 = {"timestamp": 10, "transactionHash": "a"}
        obj2 = {"timestamp": 20, "transactionHash": "b"}
        self.service1.find_last_object.return_value = obj1
        self.service2.find_last_object.return_value = obj2
        result = self.aggregate_service.find_last_object(cid)
        self.assertEqual(result, obj2)

    def test_find_last_object_returns_none_if_all_none(self):
        """Returns None when all services return None."""
        cid = "cid4"
        self.service1.find_last_object.return_value = None
        self.service2.find_last_object.return_value = None
        result = self.aggregate_service.find_last_object(cid)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
