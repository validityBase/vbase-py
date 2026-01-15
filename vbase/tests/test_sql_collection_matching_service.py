import unittest
import time
from sqlmodel import create_engine, Session, SQLModel

from vbase.core.sql_indexing_service import (
    SqlCollectionMatchingService,
    event_add_set_object,
)


class TestSqlCollectionMatchingService(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        SQLModel.metadata.create_all(self.engine)
        self.service = SqlCollectionMatchingService(self.engine)

    def _insert_data(self, data):
        with Session(self.engine) as session:
            for item in data:
                event = event_add_set_object(
                    id=item["id"],
                    user=item["user"],
                    set_cid=item["set_cid"],
                    object_cid=item["object_cid"],
                    chain_id=item.get("chain_id", 1),
                    transaction_hash=item.get("transaction_hash", "0x123"),
                    timestamp=item["timestamp"],
                )
                session.add(event)
            session.commit()

    def test_happy_path(self):
        data = [
            {"id": "1", "user": "user1", "set_cid": "set1", "object_cid": "obj1", "timestamp": 1000},
            {"id": "2", "user": "user1", "set_cid": "set1", "object_cid": "obj2", "timestamp": 1001},
            {"id": "3", "user": "user1", "set_cid": "set1", "object_cid": "obj3", "timestamp": 1002},
        ]
        self._insert_data(data)

        results = self.service.find_best_collections(["obj1", "obj2"])
        self.assertEqual(len(results), 1)

        match = results[0]
        self.assertAlmostEqual(match.score, 2 / 3)
        self.assertEqual(match.set_cid, "set1")
        self.assertEqual(match.user, "user1")

    def test_multiple_collections_multiple_users(self):
        data = [
            {"id": "1", "user": "user1", "set_cid": "set1", "object_cid": "obj1", "timestamp": 1000},
            {"id": "2", "user": "user1", "set_cid": "set1", "object_cid": "obj2", "timestamp": 1001},
            {"id": "3", "user": "user2", "set_cid": "set2", "object_cid": "obj2", "timestamp": 1002},
            {"id": "4", "user": "user2", "set_cid": "set2", "object_cid": "obj3", "timestamp": 1003},
            {"id": "5", "user": "user3", "set_cid": "set3", "object_cid": "obj1", "timestamp": 1004},
        ]
        self._insert_data(data)

        results = self.service.find_best_collections(["obj1", "obj2"])
        self.assertEqual(len(results), 3)

        self.assertEqual(results[0].set_cid, "set1")  # score 1.0, ts 1000
        self.assertEqual(results[1].set_cid, "set3")  # score 1.0, ts 1004
        self.assertEqual(results[2].set_cid, "set2")  # score 0.5

    def test_multiple_collections_single_user(self):
        data = [
            {"id": "1", "user": "user1", "set_cid": "set1", "object_cid": "obj1", "timestamp": 1000},
            {"id": "2", "user": "user1", "set_cid": "set1", "object_cid": "obj2", "timestamp": 1001},
            {"id": "3", "user": "user1", "set_cid": "set2", "object_cid": "obj2", "timestamp": 1002},
            {"id": "4", "user": "user1", "set_cid": "set2", "object_cid": "obj3", "timestamp": 1003},
        ]
        self._insert_data(data)

        results = self.service.find_best_collections(["obj1", "obj2"])
        self.assertEqual(len(results), 2)

        self.assertEqual(results[0].set_cid, "set1")
        self.assertEqual(results[1].set_cid, "set2")

    def test_duplicated_entities(self):
        data = [
            {"id": "1", "user": "user1", "set_cid": "set1", "object_cid": "obj1", "timestamp": 1000},
            {"id": "2", "user": "user1", "set_cid": "set1", "object_cid": "obj1", "timestamp": 1001},
            {"id": "3", "user": "user1", "set_cid": "set2", "object_cid": "obj1", "timestamp": 1002},
        ]
        self._insert_data(data)

        results = self.service.find_best_collections(["obj1"])
        self.assertEqual(len(results), 2)

        for match in results:
            self.assertEqual(match.score, 1.0)

    def test_two_collections_equals(self):
        data = [
            {"id": "1", "user": "user1", "set_cid": "set1", "object_cid": "obj1", "timestamp": 1000},
            {"id": "2", "user": "user1", "set_cid": "set1", "object_cid": "obj2", "timestamp": 1001},
            {"id": "3", "user": "user2", "set_cid": "set2", "object_cid": "obj1", "timestamp": 1002},
            {"id": "4", "user": "user2", "set_cid": "set2", "object_cid": "obj2", "timestamp": 1003},
        ]
        self._insert_data(data)

        results = self.service.find_best_collections(["obj1", "obj2"])
        self.assertEqual(len(results), 2)

        self.assertEqual(results[0].set_cid, "set1")
        self.assertEqual(results[1].set_cid, "set2")

    def test_not_found(self):
        data = [
            {"id": "1", "user": "user1", "set_cid": "set1", "object_cid": "obj1", "timestamp": 1000},
        ]
        self._insert_data(data)

        results = self.service.find_best_collections(["obj2"])
        self.assertEqual(results, [])


if __name__ == "__main__":
    unittest.main()
