"""
Unit tests for FuzzySetMatchingService.
"""

# pylint: disable=protected-access,too-many-public-methods,too-many-lines

import unittest

from vbase.core.models import EventAddSetObject
from vbase.core.set_matching.fuzzy_set_matching_service import FuzzySetMatchingService
from vbase.core.set_matching.types import (
    FuzzyCheckObjectSetData,
    SetIdentifier,
    SetMatchingCriteria,
    TimestampedCid,
)
from vbase.tests.set_matching.base_sql_matching_test import BaseSQLMatchingTest


class TestFuzzySetMatchingService(BaseSQLMatchingTest):
    """
    Tests for FuzzySetMatchingService using in-memory database.
    """

    def setUp(self) -> None:
        super().setUp()
        # FuzzySetMatchingService queries LastBatchProcessingTime; seed one record.
        self.add_last_batch_processing_time(timestamp=9999999)

    # ========== Test Exact Match for Small Criteria (< 5 elements) ==========

    def test_small_criteria_requires_exact_match(self) -> None:
        """Test that criteria with fewer than 5 elements use exact matching."""
        # Add a set with 4 objects
        self.add_test_events(
            [
                {
                    "id": "event-1",
                    "user": "0xAlice",
                    "set_cid": "set-small",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-2",
                    "user": "0xAlice",
                    "set_cid": "set-small",
                    "object_cid": "obj-2",
                    "chain_id": 1,
                    "timestamp": 2000,
                },
                {
                    "id": "event-3",
                    "user": "0xAlice",
                    "set_cid": "set-small",
                    "object_cid": "obj-3",
                    "chain_id": 1,
                    "timestamp": 3000,
                },
                {
                    "id": "event-4",
                    "user": "0xAlice",
                    "set_cid": "set-small",
                    "object_cid": "obj-4",
                    "chain_id": 1,
                    "timestamp": 4000,
                },
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with 2 elements that match exactly
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-1", timestamp=1000),
                TimestampedCid(object_cid="obj-2", timestamp=1500),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find exact match
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-small")
        self.assertEqual(matches[0].user, "0xAlice")
        self.assertEqual(matches[0].rank, 1.0)  # Perfect match
        self.assertEqual(matches[0].last_matching_element_timestamp, 2000)

    def test_small_criteria_no_match_if_one_position_differs(self) -> None:
        """Test that small criteria (< 5) does not match if even 1 position differs."""
        # Add a set with 4 objects
        self.add_test_events(
            [
                {
                    "id": "event-1",
                    "user": "0xAlice",
                    "set_cid": "set-small",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-2",
                    "user": "0xAlice",
                    "set_cid": "set-small",
                    "object_cid": "obj-2",
                    "chain_id": 1,
                    "timestamp": 2000,
                },
                {
                    "id": "event-3",
                    "user": "0xAlice",
                    "set_cid": "set-small",
                    "object_cid": "obj-3",
                    "chain_id": 1,
                    "timestamp": 3000,
                },
                {
                    "id": "event-4",
                    "user": "0xAlice",
                    "set_cid": "set-small",
                    "object_cid": "obj-4",
                    "chain_id": 1,
                    "timestamp": 4000,
                },
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with 4 elements where last one differs
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-1", timestamp=1000),
                TimestampedCid(object_cid="obj-2", timestamp=2000),
                TimestampedCid(object_cid="obj-3", timestamp=3000),
                TimestampedCid(object_cid="obj-DIFFERENT", timestamp=4000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should NOT find a match (tolerance=0 for < 5 elements)
        self.assertEqual(len(matches), 0)

    # ========== Test Fuzzy Match for Large Criteria (≥ 5 elements) ==========

    def test_large_criteria_exact_match(self) -> None:
        """Test that large criteria (≥ 5) with exact match returns result."""
        # Add a set with 5 objects
        self.add_test_events(
            [
                {
                    "id": f"event-{i}",
                    "user": "0xAlice",
                    "set_cid": "set-large",
                    "object_cid": f"obj-{i}",
                    "chain_id": 1,
                    "timestamp": i * 1000,
                }
                for i in range(1, 6)
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid=f"obj-{i}", timestamp=i * 1000)
                for i in range(1, 6)
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find exact match
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-large")
        self.assertEqual(matches[0].rank, 1.0)  # Perfect match

    def test_large_criteria_fuzzy_match_within_tolerance(self) -> None:
        """Test that large criteria matches when within 20% tolerance (1 out of 5 different)."""
        # Add a set with 5 objects
        self.add_test_events(
            [
                {
                    "id": f"event-{i}",
                    "user": "0xAlice",
                    "set_cid": "set-fuzzy",
                    "object_cid": f"obj-{i}",
                    "chain_id": 1,
                    "timestamp": i * 1000,
                }
                for i in range(1, 6)
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with 5 elements where 1 differs (20% tolerance)
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-1", timestamp=1000),
                TimestampedCid(object_cid="obj-2", timestamp=2000),
                TimestampedCid(object_cid="obj-3", timestamp=3000),
                TimestampedCid(object_cid="obj-4", timestamp=4000),
                TimestampedCid(object_cid="obj-DIFFERENT", timestamp=5000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find the match (1 out of 5 = 20% different, within tolerance)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-fuzzy")

    def test_large_criteria_no_match_beyond_tolerance(self) -> None:
        """Test that large criteria does not match when > 20% positions differ."""
        # Add a set with 5 objects
        self.add_test_events(
            [
                {
                    "id": f"event-{i}",
                    "user": "0xAlice",
                    "set_cid": "set-nofuzzy",
                    "object_cid": f"obj-{i}",
                    "chain_id": 1,
                    "timestamp": i * 1000,
                }
                for i in range(1, 6)
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with 5 elements where 2 differ (40% tolerance, beyond 20%)
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-1", timestamp=1000),
                TimestampedCid(object_cid="obj-2", timestamp=2000),
                TimestampedCid(object_cid="obj-3", timestamp=3000),
                TimestampedCid(object_cid="obj-DIFF1", timestamp=4000),
                TimestampedCid(object_cid="obj-DIFF2", timestamp=5000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should NOT find a match (2 out of 5 = 40% > 20% tolerance)
        self.assertEqual(len(matches), 0)

    def test_boundary_exactly_20_percent_tolerance(self) -> None:
        """Test boundary case: exactly 20% different (2 out of 10)."""
        # Add a set with 10 objects
        self.add_test_events(
            [
                {
                    "id": f"event-{i}",
                    "user": "0xAlice",
                    "set_cid": "set-boundary",
                    "object_cid": f"obj-{i}",
                    "chain_id": 1,
                    "timestamp": i * 1000,
                }
                for i in range(1, 11)
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with 10 elements where exactly 2 differ (20% tolerance boundary)
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(
                    object_cid=(f"obj-{i}" if i <= 8 else f"obj-DIFF{i}"),
                    timestamp=i * 1000,
                )
                for i in range(1, 11)
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find the match (exactly at 20% boundary)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-boundary")

    def test_non_multiple_criteria_size_enforces_ceiling(self) -> None:
        """Test tolerance ceiling with a non-multiple size (len=6, tolerance=0.2).

        math.ceil(6 * 0.8) = 5, so max_allowed_distance = 1.
        1 mismatch (16.7%) must match; 2 mismatches (33.3%) must not.
        """
        self.add_test_events(
            [
                {
                    "id": f"event-{i}",
                    "user": "0xAlice",
                    "set_cid": "set-nonmultiple",
                    "object_cid": f"obj-{i}",
                    "chain_id": 1,
                    "timestamp": i * 1000,
                }
                for i in range(1, 7)
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # 1 mismatch out of 6 (16.7%) — must match
        criteria_one_diff = SetMatchingCriteria(
            objects=[
                TimestampedCid(
                    object_cid=f"obj-{i}" if i < 6 else "obj-DIFF",
                    timestamp=i * 1000,
                )
                for i in range(1, 7)
            ]
        )
        matches = service.find_matching_sets(criteria_one_diff)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-nonmultiple")

        # 2 mismatches out of 6 (33.3%) — must NOT match (exceeds 20% tolerance)
        criteria_two_diff = SetMatchingCriteria(
            objects=[
                TimestampedCid(
                    object_cid=f"obj-{i}" if i < 5 else f"obj-DIFF{i}",
                    timestamp=i * 1000,
                )
                for i in range(1, 7)
            ]
        )
        matches = service.find_matching_sets(criteria_two_diff)
        self.assertEqual(len(matches), 0)

    # ========== Test Position-Based Ordering ==========

    def test_position_order_matters(self) -> None:
        """Test that element ordering by timestamp matters (a,b,c != b,a,c)."""
        # Add a set with objects in order: obj-a (ts=1000), obj-b (ts=2000), obj-c (ts=3000)
        self.add_test_events(
            [
                {
                    "id": "event-1",
                    "user": "0xAlice",
                    "set_cid": "set-order",
                    "object_cid": "obj-a",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-2",
                    "user": "0xAlice",
                    "set_cid": "set-order",
                    "object_cid": "obj-b",
                    "chain_id": 1,
                    "timestamp": 2000,
                },
                {
                    "id": "event-3",
                    "user": "0xAlice",
                    "set_cid": "set-order",
                    "object_cid": "obj-c",
                    "chain_id": 1,
                    "timestamp": 3000,
                },
                {
                    "id": "event-4",
                    "user": "0xAlice",
                    "set_cid": "set-order",
                    "object_cid": "obj-d",
                    "chain_id": 1,
                    "timestamp": 4000,
                },
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.0)

        # Criteria with reversed order: obj-b (ts=1000), obj-a (ts=2000), obj-c (ts=3000)
        # After sorting by timestamp, order is: obj-b, obj-a, obj-c
        # DB order (sorted by timestamp): obj-a, obj-b, obj-c
        # These are DIFFERENT orderings
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-b", timestamp=1000),  # Will be first
                TimestampedCid(object_cid="obj-a", timestamp=2000),  # Will be second
                TimestampedCid(object_cid="obj-c", timestamp=3000),
                TimestampedCid(object_cid="obj-d", timestamp=4000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should NOT find a match due to different position ordering
        self.assertEqual(len(matches), 0)

    def test_timestamp_determines_position_order(self) -> None:
        """Test that timestamps determine element ordering, not insertion order."""
        # Add objects in non-chronological insertion order
        self.add_test_events(
            [
                {
                    "id": "event-3",
                    "user": "0xAlice",
                    "set_cid": "set-ts",
                    "object_cid": "obj-c",
                    "chain_id": 1,
                    "timestamp": 3000,  # Third by timestamp
                },
                {
                    "id": "event-1",
                    "user": "0xAlice",
                    "set_cid": "set-ts",
                    "object_cid": "obj-a",
                    "chain_id": 1,
                    "timestamp": 1000,  # First by timestamp
                },
                {
                    "id": "event-2",
                    "user": "0xAlice",
                    "set_cid": "set-ts",
                    "object_cid": "obj-b",
                    "chain_id": 1,
                    "timestamp": 2000,  # Second by timestamp
                },
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.0)

        # Criteria in chronological timestamp order
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-a", timestamp=1000),
                TimestampedCid(object_cid="obj-b", timestamp=2000),
                TimestampedCid(object_cid="obj-c", timestamp=3000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find the match (timestamp order matches)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-ts")

    # ========== Test Edge Cases ==========

    def test_empty_criteria_returns_empty_list(self) -> None:
        """Test that empty criteria returns empty list."""
        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        criteria = SetMatchingCriteria(objects=[])

        matches = service.find_matching_sets(criteria)

        self.assertEqual(len(matches), 0)

    def test_candidate_shorter_than_criteria(self) -> None:
        """Test handling when candidate set is shorter than criteria."""
        # Add a set with only 3 objects
        self.add_test_events(
            [
                {
                    "id": f"event-{i}",
                    "user": "0xAlice",
                    "set_cid": "set-short",
                    "object_cid": f"obj-{i}",
                    "chain_id": 1,
                    "timestamp": i * 1000,
                }
                for i in range(1, 4)
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with 5 elements
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid=f"obj-{i}", timestamp=i * 1000)
                for i in range(1, 6)
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should not match (candidate too short, only 3/5 = 60% match, needs 80%)
        self.assertEqual(len(matches), 0)

    def test_candidate_longer_than_criteria(self) -> None:
        """Test handling when candidate set is longer than criteria."""
        # Add a set with 10 objects
        self.add_test_events(
            [
                {
                    "id": f"event-{i}",
                    "user": "0xAlice",
                    "set_cid": "set-long",
                    "object_cid": f"obj-{i}",
                    "chain_id": 1,
                    "timestamp": i * 1000,
                }
                for i in range(1, 11)
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with first 5 elements
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid=f"obj-{i}", timestamp=i * 1000)
                for i in range(1, 6)
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find the match (first 5 positions match)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-long")

    def test_different_users_are_separate(self) -> None:
        """Test that sets from different users are kept separate."""
        # Add same set_cid for two different users
        self.add_test_events(
            [
                {
                    "id": "event-alice-1",
                    "user": "0xAlice",
                    "set_cid": "set-shared",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-alice-2",
                    "user": "0xAlice",
                    "set_cid": "set-shared",
                    "object_cid": "obj-2",
                    "chain_id": 1,
                    "timestamp": 2000,
                },
                {
                    "id": "event-bob-1",
                    "user": "0xBob",
                    "set_cid": "set-shared",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-bob-2",
                    "user": "0xBob",
                    "set_cid": "set-shared",
                    "object_cid": "obj-2",
                    "chain_id": 1,
                    "timestamp": 2000,
                },
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-1", timestamp=1000),
                TimestampedCid(object_cid="obj-2", timestamp=2000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find both sets as separate matches
        self.assertEqual(len(matches), 2)
        users = {m.user for m in matches}
        self.assertEqual(users, {"0xAlice", "0xBob"})

    def test_distributed_set_spans_multiple_chains(self) -> None:
        """Test that elements from different chains merge into one distributed set."""
        self.add_test_events(
            [
                # First 3 elements on chain 1
                *[
                    {
                        "id": f"event-chain1-{i}",
                        "user": "0xAlice",
                        "set_cid": "set-distributed",
                        "object_cid": f"obj-{i}",
                        "chain_id": 1,
                        "timestamp": i * 1000,
                    }
                    for i in range(1, 4)
                ],
                # Last 2 elements on chain 2
                *[
                    {
                        "id": f"event-chain2-{i}",
                        "user": "0xAlice",
                        "set_cid": "set-distributed",
                        "object_cid": f"obj-{i}",
                        "chain_id": 2,
                        "timestamp": i * 1000,
                    }
                    for i in range(4, 6)
                ],
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid=f"obj-{i}", timestamp=i * 1000)
                for i in range(1, 6)
            ]
        )

        matches = service.find_matching_sets(criteria)

        # All elements from both chains merge into a single distributed set
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-distributed")
        self.assertEqual(matches[0].user, "0xAlice")
        self.assertEqual(matches[0].rank, 1.0)

    def test_multiple_matches_ranked_by_quality(self) -> None:
        """Test that multiple sets matching the same CID sequence are both returned with rank 1.0.

        Fuzzy ranking is based solely on CID edit distance; timestamp values are
        not used in scoring. Both sets here share the same CID sequence so both
        receive a perfect rank regardless of their actual timestamps.
        """
        # Add two sets: one with timestamps matching the criteria, one with offset timestamps
        self.add_test_events(
            [
                # Set 1: Perfect timestamp match
                {
                    "id": "event-set1-1",
                    "user": "0xAlice",
                    "set_cid": "set-perfect",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-set1-2",
                    "user": "0xAlice",
                    "set_cid": "set-perfect",
                    "object_cid": "obj-2",
                    "chain_id": 1,
                    "timestamp": 2000,
                },
                {
                    "id": "event-set1-3",
                    "user": "0xAlice",
                    "set_cid": "set-perfect",
                    "object_cid": "obj-3",
                    "chain_id": 1,
                    "timestamp": 3000,
                },
                # Set 2: Slightly offset timestamps
                {
                    "id": "event-set2-1",
                    "user": "0xBob",
                    "set_cid": "set-offset",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1500,
                },
                {
                    "id": "event-set2-2",
                    "user": "0xBob",
                    "set_cid": "set-offset",
                    "object_cid": "obj-2",
                    "chain_id": 1,
                    "timestamp": 2500,
                },
                {
                    "id": "event-set2-3",
                    "user": "0xBob",
                    "set_cid": "set-offset",
                    "object_cid": "obj-3",
                    "chain_id": 1,
                    "timestamp": 3500,
                },
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-1", timestamp=1000),
                TimestampedCid(object_cid="obj-2", timestamp=2000),
                TimestampedCid(object_cid="obj-3", timestamp=3000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find both matches
        self.assertEqual(len(matches), 2)
        # Both sets match the CID sequence exactly → both rank 1.0
        # Ordering between equal-ranked results is not guaranteed
        set_cids = {m.set_cid for m in matches}
        self.assertEqual(set_cids, {"set-perfect", "set-offset"})
        self.assertTrue(all(m.rank == 1.0 for m in matches))

    def test_max_5_results_returned(self) -> None:
        """Test that at most 5 matches are returned."""
        # Add 10 sets with same objects
        for i in range(10):
            self.add_test_events(
                [
                    {
                        "id": f"event-set{i}-1",
                        "user": f"0xUser{i}",
                        "set_cid": f"set-{i}",
                        "object_cid": "obj-1",
                        "chain_id": 1,
                        "timestamp": 1000 + i * 10,
                    },
                    {
                        "id": f"event-set{i}-2",
                        "user": f"0xUser{i}",
                        "set_cid": f"set-{i}",
                        "object_cid": "obj-2",
                        "chain_id": 1,
                        "timestamp": 2000 + i * 10,
                    },
                    {
                        "id": f"event-set{i}-3",
                        "user": f"0xUser{i}",
                        "set_cid": f"set-{i}",
                        "object_cid": "obj-3",
                        "chain_id": 1,
                        "timestamp": 3000 + i * 10,
                    },
                ]
            )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-1", timestamp=1000),
                TimestampedCid(object_cid="obj-2", timestamp=2000),
                TimestampedCid(object_cid="obj-3", timestamp=3000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should return at most 5 matches
        self.assertLessEqual(len(matches), 5)

    def test_levenshtein_missing_first_element(self) -> None:
        """Test Levenshtein distance when candidate is missing the first element."""
        # Add a set with 7 objects (e2 through e8)
        self.add_test_events(
            [
                {
                    "id": f"event-{i}",
                    "user": "0xAlice",
                    "set_cid": "set-missing-first",
                    "object_cid": f"e{i}",
                    "chain_id": 1,
                    "timestamp": i * 1000,
                }
                for i in range(2, 9)  # e2 through e8
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with 8 elements (e1 through e8)
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid=f"e{i}", timestamp=i * 1000)
                for i in range(1, 9)  # e1 through e8
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find the match
        # Levenshtein distance = 1 (one deletion at the beginning)
        # required_matches = math.ceil(8 * (1 - 0.2)) = math.ceil(6.4) = 7
        # max_allowed_distance = 8 - 7 = 1
        # distance (1) <= max_allowed_distance (1), so it should match
        # rank = 1 - 1 / 8 = 0.875
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-missing-first")
        self.assertEqual(matches[0].user, "0xAlice")
        self.assertAlmostEqual(matches[0].rank, 0.875, delta=0.1)

    def test_rank_candidate_returns_levenshtein_result(self) -> None:
        """Test that fuzzy ranking returns both the rank and Levenshtein details."""
        candidate = FuzzyCheckObjectSetData(
            key=SetIdentifier(set_cid="set-fuzzy", user="0xAlice"),
            objects=[
                EventAddSetObject(
                    id=f"event-{i}",
                    user="0xAlice",
                    set_cid="set-fuzzy",
                    object_cid=f"obj-{i}",
                    chain_id=1,
                    transaction_hash="0x0",
                    timestamp=i * 1000,
                )
                for i in range(1, 6)
            ],
        )
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-1", timestamp=1000),
                TimestampedCid(object_cid="obj-2", timestamp=2000),
                TimestampedCid(object_cid="obj-3", timestamp=3000),
                TimestampedCid(object_cid="obj-4", timestamp=4000),
                TimestampedCid(object_cid="obj-different", timestamp=5000),
            ]
        )

        candidate.rank, candidate.lev_result, _ = (
            FuzzySetMatchingService._rank_candidate(candidate, criteria, tolerance=0.2)
        )

        self.assertEqual(candidate.rank, 0.8)
        self.assertIsNotNone(candidate.lev_result)
        assert candidate.lev_result is not None
        self.assertEqual(candidate.lev_result.substitutions, 1)
        self.assertEqual(candidate.lev_result.insertions, 0)
        self.assertEqual(candidate.lev_result.deletions, 0)
        self.assertEqual(candidate.lev_result.distance, 1)

    def test_last_matching_element_timestamp_accounts_for_insertions(self) -> None:
        """Test that inserted candidate elements advance the last matching timestamp."""
        self.add_test_events(
            [
                {
                    "id": "event-0",
                    "user": "0xAlice",
                    "set_cid": "set-inserted-prefix",
                    "object_cid": "obj-prefix",
                    "chain_id": 1,
                    "timestamp": 500,
                },
                {
                    "id": "event-1",
                    "user": "0xAlice",
                    "set_cid": "set-inserted-prefix",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-2",
                    "user": "0xAlice",
                    "set_cid": "set-inserted-prefix",
                    "object_cid": "obj-2",
                    "chain_id": 1,
                    "timestamp": 2000,
                },
                {
                    "id": "event-3",
                    "user": "0xAlice",
                    "set_cid": "set-inserted-prefix",
                    "object_cid": "obj-3",
                    "chain_id": 1,
                    "timestamp": 3000,
                },
                {
                    "id": "event-4",
                    "user": "0xAlice",
                    "set_cid": "set-inserted-prefix",
                    "object_cid": "obj-4",
                    "chain_id": 1,
                    "timestamp": 4000,
                },
                {
                    "id": "event-5",
                    "user": "0xAlice",
                    "set_cid": "set-inserted-prefix",
                    "object_cid": "obj-5",
                    "chain_id": 1,
                    "timestamp": 5000,
                },
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid=f"obj-{i}", timestamp=i * 1000)
                for i in range(1, 6)
            ]
        )

        matches = service.find_matching_sets(criteria)

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].last_matching_element_timestamp, 5000)

    def test_insertion_at_head_with_repeated_cids_uses_correct_last_matching_element_timestamp(
        self,
    ) -> None:
        """Regression test for repeated CIDs with an inserted head element.

        The old code truncated the candidate to len(criteria), producing equal-length
        sequences where Levenshtein prefers substitution over insert+delete.
        That caused insertions=0 and matched_length=5, giving the wrong timestamp.
        """
        self.add_test_events(
            [
                {
                    "id": "event-prefix",
                    "user": "0xAlice",
                    "set_cid": "set-repeated",
                    "object_cid": "obj-same",
                    "chain_id": 1,
                    "timestamp": 500,  # extra element at head
                },
                *[
                    {
                        "id": f"event-{i}",
                        "user": "0xAlice",
                        "set_cid": "set-repeated",
                        "object_cid": "obj-same",
                        "chain_id": 1,
                        "timestamp": i * 1000,
                    }
                    for i in range(1, 6)
                ],
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-same", timestamp=i * 1000)
                for i in range(1, 6)
            ]
        )

        matches = service.find_matching_sets(criteria)

        self.assertEqual(len(matches), 1)
        # The insertion of the prefix element shifts the covered window forward:
        # matched_length = 5 + 1 insertion = 6, so last_matching_element_timestamp must be
        # the 6th element (ts=5000), not the 5th (ts=4000).
        self.assertEqual(matches[0].last_matching_element_timestamp, 5000)

    def test_last_matching_element_timestamp_accounts_for_deletions(self) -> None:
        """Test that deleted candidate elements move back the last matching timestamp."""
        self.add_test_events(
            [
                {
                    "id": "event-1",
                    "user": "0xAlice",
                    "set_cid": "set-missing-tail",
                    "object_cid": "obj-1",
                    "chain_id": 1,
                    "timestamp": 1000,
                },
                {
                    "id": "event-2",
                    "user": "0xAlice",
                    "set_cid": "set-missing-tail",
                    "object_cid": "obj-2",
                    "chain_id": 1,
                    "timestamp": 2000,
                },
                {
                    "id": "event-3",
                    "user": "0xAlice",
                    "set_cid": "set-missing-tail",
                    "object_cid": "obj-3",
                    "chain_id": 1,
                    "timestamp": 3000,
                },
                {
                    "id": "event-4",
                    "user": "0xAlice",
                    "set_cid": "set-missing-tail",
                    "object_cid": "obj-4",
                    "chain_id": 1,
                    "timestamp": 4000,
                },
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.25)
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid=f"obj-{i}", timestamp=i * 1000)
                for i in range(1, 6)
            ]
        )

        matches = service.find_matching_sets(criteria)

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].last_matching_element_timestamp, 4000)

    # ========== Test is_full_match ==========

    def test_is_full_match_true_when_perfect_match_and_same_size(self) -> None:
        """Test that is_full_match is true for an exact same-size match."""
        self.add_test_events(
            [
                {
                    "id": f"event-{i}",
                    "user": "0xAlice",
                    "set_cid": "set-exact",
                    "object_cid": f"obj-{i}",
                    "chain_id": 1,
                    "timestamp": i * 1000,
                }
                for i in range(1, 6)
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Criteria has exactly 5 elements, same CIDs as the set
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid=f"obj-{i}", timestamp=i * 1000)
                for i in range(1, 6)
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
                    "id": f"event-{i}",
                    "user": "0xAlice",
                    "set_cid": "set-longer",
                    "object_cid": f"obj-{i}",
                    "chain_id": 1,
                    "timestamp": i * 1000,
                }
                for i in range(1, 9)  # 8 objects in the set
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Criteria has only 5 elements; set has 8
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid=f"obj-{i}", timestamp=i * 1000)
                for i in range(1, 6)
            ]
        )

        matches = service.find_matching_sets(criteria)

        self.assertEqual(len(matches), 1)
        self.assertFalse(matches[0].is_full_match)

    def test_is_full_match_false_when_same_size_but_substitution(self) -> None:
        """Test that is_full_match is false when sizes match but one CID differs."""
        self.add_test_events(
            [
                {
                    "id": f"event-{i}",
                    "user": "0xAlice",
                    "set_cid": "set-sub",
                    "object_cid": f"obj-{i}",
                    "chain_id": 1,
                    "timestamp": i * 1000,
                }
                for i in range(1, 6)
            ]
        )

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Criteria has same size (5) but last CID differs → lev distance = 1
        criteria = SetMatchingCriteria(
            objects=[
                TimestampedCid(object_cid="obj-1", timestamp=1000),
                TimestampedCid(object_cid="obj-2", timestamp=2000),
                TimestampedCid(object_cid="obj-3", timestamp=3000),
                TimestampedCid(object_cid="obj-4", timestamp=4000),
                TimestampedCid(object_cid="obj-DIFFERENT", timestamp=5000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        self.assertEqual(len(matches), 1)
        self.assertFalse(matches[0].is_full_match)


if __name__ == "__main__":
    unittest.main()
