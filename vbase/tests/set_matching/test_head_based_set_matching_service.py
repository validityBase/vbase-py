"""
Unit tests for HeadBasedSetMatchingService.
"""

import unittest

from vbase.core.set_matching.head_based_set_matching_service import (
    HeadBasedSetMatchingService,
)
from vbase.core.set_matching.types import SetMatchingCriteria, TimestampedCid
from vbase.tests.set_matching.base_sql_matching_test import BaseSQLMatchingTest


class TestHeadBasedSetMatchingService(BaseSQLMatchingTest):
    """
    Tests for HeadBasedSetMatchingService using in-memory database.
    """

    def setUp(self) -> None:
        super().setUp()
        # HeadBasedSetMatchingService queries LastBatchProcessingTime; seed one record.
        self.add_last_batch_processing_time(timestamp=9999999)

    def test_finds_exact_head_match(self) -> None:
        """Test that service finds a set when head objects match exactly."""
        # Add test data: a set with 3 objects
        self.add_test_events(
            [
                {
                    "id": "event-1",
                    "user": "0xAlice",
                    "set_cid": "set-abc",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-2",
                    "user": "0xAlice",
                    "set_cid": "set-abc",
                    "object_cid": "obj-2",
                    "chain_id": 1,
                    "timestamp": 2000,
                },
                {
                    "id": "event-3",
                    "user": "0xAlice",
                    "set_cid": "set-abc",
                    "object_cid": "obj-3",
                    "chain_id": 1,
                    "timestamp": 3000,
                },
            ]
        )

        # Create service with in-memory database
        service = HeadBasedSetMatchingService(db_url=self.db_url)

        # Search for head match (first 2 objects)
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-1", timestamp=1000),
                TimestampedCid(object_cid="obj-2", timestamp=2000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find the set
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-abc")
        self.assertEqual(matches[0].user, "0xAlice")
        self.assertEqual(
            matches[0].rank, 1.0
        )  # perfect match, no timestamp differences
        self.assertEqual(
            matches[0].last_matching_element_timestamp, 2000
        )  # timestamp of last matching element
        self.assertFalse(matches[0].is_full_match)  # set has 3 objects, criteria has 2
        self.assertEqual(matches[0].data_freshness_timestamp, 9999999)

    def test_returns_empty_for_non_matching_head(self) -> None:
        """Test that service returns empty list when head doesn't match."""
        self.add_test_event(
            event_id="event-1",
            user="0xAlice",
            set_cid="set-abc",
            object_cid="obj-1",
            chain_id=1,
            timestamp=1000,
        )

        service = HeadBasedSetMatchingService(db_url=self.db_url)

        criteria = SetMatchingCriteria(
            objects=[TimestampedCid(object_cid="obj-different", timestamp=1000)]
        )

        matches = service.find_matching_sets(criteria)

        self.assertEqual(len(matches), 0)

    def test_returns_empty_for_empty_criteria(self) -> None:
        """Test that service returns empty list for empty search criteria."""
        service = HeadBasedSetMatchingService(db_url=self.db_url)

        criteria = SetMatchingCriteria(objects=[])

        matches = service.find_matching_sets(criteria)

        self.assertEqual(len(matches), 0)

    def test_distinguishes_different_users(self) -> None:
        """Test that sets from different users are kept separate."""
        # Add same set_cid for two different users
        self.add_test_events(
            [
                {
                    "id": "event-1",
                    "user": "0xAlice",
                    "set_cid": "set-abc",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-2",
                    "user": "0xBob",
                    "set_cid": "set-abc",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
            ]
        )

        service = HeadBasedSetMatchingService(db_url=self.db_url)

        criteria = SetMatchingCriteria(
            objects=[TimestampedCid(object_cid="obj-1", timestamp=1000)]
        )

        matches = service.find_matching_sets(criteria)

        # Should find both sets as separate matches
        self.assertEqual(len(matches), 2)
        users = {m.user for m in matches}
        self.assertEqual(users, {"0xAlice", "0xBob"})

    def test_matches_by_cid_despite_timestamp_differences(self) -> None:
        """Test that service matches by CID order even when timestamps differ."""
        # Add test data: a set with objects in a specific timestamp order
        self.add_test_events(
            [
                {
                    "id": "event-1",
                    "user": "0xAlice",
                    "set_cid": "set-xyz",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-2",
                    "user": "0xAlice",
                    "set_cid": "set-xyz",
                    "object_cid": "obj-2",
                    "chain_id": 1,
                    "timestamp": 2000,
                },
                {
                    "id": "event-3",
                    "user": "0xAlice",
                    "set_cid": "set-xyz",
                    "object_cid": "obj-3",
                    "chain_id": 1,
                    "timestamp": 3000,
                },
            ]
        )

        service = HeadBasedSetMatchingService(db_url=self.db_url)

        # Search with same CIDs but different timestamps (will be sorted by timestamp anyway)
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(
                    object_cid="obj-1", timestamp=1500
                ),  # Different from stored 1000
                TimestampedCid(
                    object_cid="obj-2", timestamp=2500
                ),  # Different from stored 2000
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should still find the set because CIDs match in order
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-xyz")
        self.assertEqual(matches[0].user, "0xAlice")
        self.assertLess(
            matches[0].rank, 1.0
        )  # rank should be less than 1.0 due to timestamp differences
        self.assertFalse(matches[0].is_full_match)  # set has 3 objects, criteria has 2

    def test_no_match_when_timestamp_order_differs(self) -> None:
        """Test that service returns empty when CIDs match but timestamp order differs."""
        # Add test data: DB has obj-1 (ts=1000), obj-2 (ts=2000), obj-3 (ts=3000)
        self.add_test_events(
            [
                {
                    "id": "event-1",
                    "user": "0xAlice",
                    "set_cid": "set-xyz",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-2",
                    "user": "0xAlice",
                    "set_cid": "set-xyz",
                    "object_cid": "obj-2",
                    "chain_id": 1,
                    "timestamp": 2000,
                },
                {
                    "id": "event-3",
                    "user": "0xAlice",
                    "set_cid": "set-xyz",
                    "object_cid": "obj-3",
                    "chain_id": 1,
                    "timestamp": 3000,
                },
            ]
        )

        service = HeadBasedSetMatchingService(db_url=self.db_url)

        # Criteria has obj-1 with ts=2000, obj-2 with ts=1000
        # After sorting by timestamp: obj-2 (ts=1000), obj-1 (ts=2000)
        # This order is DIFFERENT from DB order: obj-1 (ts=1000), obj-2 (ts=2000)
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(
                    object_cid="obj-1", timestamp=2000
                ),  # Will be second after sorting
                TimestampedCid(
                    object_cid="obj-2", timestamp=1000
                ),  # Will be first after sorting
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should NOT find a match because the timestamp-ordered CID sequence differs
        # DB order: obj-1, obj-2
        # Criteria order (after sorting): obj-2, obj-1
        self.assertEqual(len(matches), 0)

    # ========== Test is_full_match ==========

    def test_is_full_match_true_when_criteria_equals_set_size(self) -> None:
        """Test that is_full_match is True when criteria length equals the set length."""
        self.add_test_events(
            [
                {
                    "id": "event-1",
                    "user": "0xAlice",
                    "set_cid": "set-full",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-2",
                    "user": "0xAlice",
                    "set_cid": "set-full",
                    "object_cid": "obj-2",
                    "chain_id": 1,
                    "timestamp": 2000,
                },
            ]
        )

        service = HeadBasedSetMatchingService(db_url=self.db_url)

        # Criteria has exactly 2 elements, same as the set
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-1", timestamp=1000),
                TimestampedCid(object_cid="obj-2", timestamp=2000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        self.assertEqual(len(matches), 1)
        self.assertTrue(matches[0].is_full_match)

    def test_is_full_match_false_when_set_longer_than_criteria(self) -> None:
        """Test that is_full_match is False when the set has more objects than criteria."""
        self.add_test_events(
            [
                {
                    "id": "event-1",
                    "user": "0xAlice",
                    "set_cid": "set-partial",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-2",
                    "user": "0xAlice",
                    "set_cid": "set-partial",
                    "object_cid": "obj-2",
                    "chain_id": 1,
                    "timestamp": 2000,
                },
                {
                    "id": "event-3",
                    "user": "0xAlice",
                    "set_cid": "set-partial",
                    "object_cid": "obj-3",
                    "chain_id": 1,
                    "timestamp": 3000,
                },
            ]
        )

        service = HeadBasedSetMatchingService(db_url=self.db_url)

        # Criteria has only 2 elements but the set has 3
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-1", timestamp=1000),
                TimestampedCid(object_cid="obj-2", timestamp=2000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        self.assertEqual(len(matches), 1)
        self.assertFalse(matches[0].is_full_match)

    # ========== Test multi-chain disambiguation ==========

    def test_merges_same_set_from_different_chains(self) -> None:
        """Test that the same set on different chains is treated as one set."""
        self.add_test_events(
            [
                {
                    "id": "event-chain1",
                    "user": "0xAlice",
                    "set_cid": "set-distributed",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-chain2",
                    "user": "0xAlice",
                    "set_cid": "set-distributed",
                    "object_cid": "obj-2",
                    "chain_id": 2,
                    "timestamp": 2000,
                },
            ]
        )

        service = HeadBasedSetMatchingService(db_url=self.db_url)

        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-1", timestamp=1000),
                TimestampedCid(object_cid="obj-2", timestamp=2000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Elements from both chains merge into a single distributed set
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-distributed")
        self.assertEqual(matches[0].user, "0xAlice")
        self.assertTrue(matches[0].is_full_match)


if __name__ == "__main__":
    unittest.main()
