"""
Unit tests for FuzzySetMatchingService.
"""

import unittest

from vbase.core.set_matching.fuzzy_set_matching_service import (
    FuzzySetMatchingService,
)
from vbase.core.set_matching.types import SetMatchingCriteria, SetMatchingCriteriaItem
from vbase.tests.set_matching.base_sql_matching_test import BaseSQLMatchingTest


class TestFuzzySetMatchingService(BaseSQLMatchingTest):
    """
    Tests for FuzzySetMatchingService using in-memory database.
    """

    # ========== Test Exact Match for Small Criteria (< 5 elements) ==========

    def test_small_criteria_requires_exact_match(self) -> None:
        """Test that criteria with < 5 elements uses tolerance=0 (exact matching)."""
        # Add a set with 4 objects
        self.add_test_events([
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
        ])

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with 4 elements that match exactly
        criteria = SetMatchingCriteria(
            objects=[
                SetMatchingCriteriaItem(object_cid="obj-1", timestamp=1000),
                SetMatchingCriteriaItem(object_cid="obj-2", timestamp=1500),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find exact match
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-small")
        self.assertEqual(matches[0].user, "0xAlice")
        self.assertEqual(matches[0].score, 0.0)  # Perfect match
        self.assertEqual(matches[0].as_of_timestamp, 2000)

    def test_small_criteria_no_match_if_one_position_differs(self) -> None:
        """Test that small criteria (< 5) does not match if even 1 position differs."""
        # Add a set with 4 objects
        self.add_test_events([
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
        ])

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with 4 elements where last one differs
        criteria = SetMatchingCriteria(
            objects=[
                SetMatchingCriteriaItem(object_cid="obj-1", timestamp=1000),
                SetMatchingCriteriaItem(object_cid="obj-2", timestamp=2000),
                SetMatchingCriteriaItem(object_cid="obj-3", timestamp=3000),
                SetMatchingCriteriaItem(object_cid="obj-DIFFERENT", timestamp=4000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should NOT find a match (tolerance=0 for < 5 elements)
        self.assertEqual(len(matches), 0)

    # ========== Test Fuzzy Match for Large Criteria (≥ 5 elements) ==========

    def test_large_criteria_exact_match(self) -> None:
        """Test that large criteria (≥ 5) with exact match returns result."""
        # Add a set with 5 objects
        self.add_test_events([
            {
                "id": f"event-{i}",
                "user": "0xAlice",
                "set_cid": "set-large",
                "object_cid": f"obj-{i}",
                "chain_id": 1,
                "timestamp": i * 1000,
            }
            for i in range(1, 6)
        ])

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        criteria = SetMatchingCriteria(
            objects=[
                SetMatchingCriteriaItem(object_cid=f"obj-{i}", timestamp=i * 1000)
                for i in range(1, 6)
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find exact match
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-large")
        self.assertEqual(matches[0].score, 0.0)  # Perfect match

    def test_large_criteria_fuzzy_match_within_tolerance(self) -> None:
        """Test that large criteria matches when within 20% tolerance (1 out of 5 different)."""
        # Add a set with 5 objects
        self.add_test_events([
            {
                "id": f"event-{i}",
                "user": "0xAlice",
                "set_cid": "set-fuzzy",
                "object_cid": f"obj-{i}",
                "chain_id": 1,
                "timestamp": i * 1000,
            }
            for i in range(1, 6)
        ])

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with 5 elements where 1 differs (20% tolerance)
        criteria = SetMatchingCriteria(
            objects=[
                SetMatchingCriteriaItem(object_cid="obj-1", timestamp=1000),
                SetMatchingCriteriaItem(object_cid="obj-2", timestamp=2000),
                SetMatchingCriteriaItem(object_cid="obj-3", timestamp=3000),
                SetMatchingCriteriaItem(object_cid="obj-4", timestamp=4000),
                SetMatchingCriteriaItem(object_cid="obj-DIFFERENT", timestamp=5000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find the match (1 out of 5 = 20% different, within tolerance)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-fuzzy")

    def test_large_criteria_no_match_beyond_tolerance(self) -> None:
        """Test that large criteria does not match when > 20% positions differ."""
        # Add a set with 5 objects
        self.add_test_events([
            {
                "id": f"event-{i}",
                "user": "0xAlice",
                "set_cid": "set-nofuzzy",
                "object_cid": f"obj-{i}",
                "chain_id": 1,
                "timestamp": i * 1000,
            }
            for i in range(1, 6)
        ])

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with 5 elements where 2 differ (40% tolerance, beyond 20%)
        criteria = SetMatchingCriteria(
            objects=[
                SetMatchingCriteriaItem(object_cid="obj-1", timestamp=1000),
                SetMatchingCriteriaItem(object_cid="obj-2", timestamp=2000),
                SetMatchingCriteriaItem(object_cid="obj-3", timestamp=3000),
                SetMatchingCriteriaItem(object_cid="obj-DIFF1", timestamp=4000),
                SetMatchingCriteriaItem(object_cid="obj-DIFF2", timestamp=5000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should NOT find a match (2 out of 5 = 40% > 20% tolerance)
        self.assertEqual(len(matches), 0)

    def test_boundary_exactly_20_percent_tolerance(self) -> None:
        """Test boundary case: exactly 20% different (2 out of 10)."""
        # Add a set with 10 objects
        self.add_test_events([
            {
                "id": f"event-{i}",
                "user": "0xAlice",
                "set_cid": "set-boundary",
                "object_cid": f"obj-{i}",
                "chain_id": 1,
                "timestamp": i * 1000,
            }
            for i in range(1, 11)
        ])

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with 10 elements where exactly 2 differ (20% tolerance boundary)
        criteria = SetMatchingCriteria(
            objects=[
                SetMatchingCriteriaItem(object_cid=f"obj-{i}" if i <= 8 else f"obj-DIFF{i}", timestamp=i * 1000)
                for i in range(1, 11)
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find the match (exactly at 20% boundary)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].set_cid, "set-boundary")

    # ========== Test Position-Based Ordering ==========

    def test_position_order_matters(self) -> None:
        """Test that element ordering by timestamp matters (a,b,c != b,a,c)."""
        # Add a set with objects in order: obj-a (ts=1000), obj-b (ts=2000), obj-c (ts=3000)
        self.add_test_events([
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
        ])

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.0)

        # Criteria with reversed order: obj-b (ts=1000), obj-a (ts=2000), obj-c (ts=3000)
        # After sorting by timestamp, order is: obj-b, obj-a, obj-c
        # DB order (sorted by timestamp): obj-a, obj-b, obj-c
        # These are DIFFERENT orderings
        criteria = SetMatchingCriteria(
            objects=[
                SetMatchingCriteriaItem(object_cid="obj-b", timestamp=1000),  # Will be first
                SetMatchingCriteriaItem(object_cid="obj-a", timestamp=2000),  # Will be second
                SetMatchingCriteriaItem(object_cid="obj-c", timestamp=3000),
                SetMatchingCriteriaItem(object_cid="obj-d", timestamp=4000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should NOT find a match due to different position ordering
        self.assertEqual(len(matches), 0)

    def test_timestamp_determines_position_order(self) -> None:
        """Test that timestamps determine element ordering, not insertion order."""
        # Add objects in non-chronological insertion order
        self.add_test_events([
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
        ])

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.0)

        # Criteria in chronological timestamp order
        criteria = SetMatchingCriteria(
            objects=[
                SetMatchingCriteriaItem(object_cid="obj-a", timestamp=1000),
                SetMatchingCriteriaItem(object_cid="obj-b", timestamp=2000),
                SetMatchingCriteriaItem(object_cid="obj-c", timestamp=3000),
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
        self.add_test_events([
            {
                "id": f"event-{i}",
                "user": "0xAlice",
                "set_cid": "set-short",
                "object_cid": f"obj-{i}",
                "chain_id": 1,
                "timestamp": i * 1000,
            }
            for i in range(1, 4)
        ])

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with 5 elements
        criteria = SetMatchingCriteria(
            objects=[
                SetMatchingCriteriaItem(object_cid=f"obj-{i}", timestamp=i * 1000)
                for i in range(1, 6)
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should not match (candidate too short, only 3/5 = 60% match, needs 80%)
        self.assertEqual(len(matches), 0)

    def test_candidate_longer_than_criteria(self) -> None:
        """Test handling when candidate set is longer than criteria."""
        # Add a set with 10 objects
        self.add_test_events([
            {
                "id": f"event-{i}",
                "user": "0xAlice",
                "set_cid": "set-long",
                "object_cid": f"obj-{i}",
                "chain_id": 1,
                "timestamp": i * 1000,
            }
            for i in range(1, 11)
        ])

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        # Search with first 5 elements
        criteria = SetMatchingCriteria(
            objects=[
                SetMatchingCriteriaItem(object_cid=f"obj-{i}", timestamp=i * 1000)
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
        self.add_test_events([
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
        ])

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        criteria = SetMatchingCriteria(
            objects=[
                SetMatchingCriteriaItem(object_cid="obj-1", timestamp=1000),
                SetMatchingCriteriaItem(object_cid="obj-2", timestamp=2000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find both sets as separate matches
        self.assertEqual(len(matches), 2)
        users = {m.user for m in matches}
        self.assertEqual(users, {"0xAlice", "0xBob"})

    def test_different_chains_are_separate(self) -> None:
        """Test that sets from different chains are kept separate."""
        # Add same set_cid on two different chains
        self.add_test_events([
            {
                "id": "event-chain1-1",
                "user": "0xAlice",
                "set_cid": "set-multichain",
                "object_cid": "obj-1",
                "chain_id": 1,
                "timestamp": 1000,
            },
            {
                "id": "event-chain1-2",
                "user": "0xAlice",
                "set_cid": "set-multichain",
                "object_cid": "obj-2",
                "chain_id": 1,
                "timestamp": 2000,
            },
            {
                "id": "event-chain2-1",
                "user": "0xAlice",
                "set_cid": "set-multichain",
                "object_cid": "obj-1",
                "chain_id": 2,
                "timestamp": 1000,
            },
            {
                "id": "event-chain2-2",
                "user": "0xAlice",
                "set_cid": "set-multichain",
                "object_cid": "obj-2",
                "chain_id": 2,
                "timestamp": 2000,
            },
        ])

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        criteria = SetMatchingCriteria(
            objects=[
                SetMatchingCriteriaItem(object_cid="obj-1", timestamp=1000),
                SetMatchingCriteriaItem(object_cid="obj-2", timestamp=2000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find both sets as separate matches (different chain IDs)
        self.assertEqual(len(matches), 2)

    def test_multiple_matches_ranked_by_quality(self) -> None:
        """Test that multiple matches are ranked by quality (fewer mismatches + better timestamp alignment)."""
        # Add two sets: one with perfect timestamps, one with offset timestamps
        self.add_test_events([
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
        ])

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        criteria = SetMatchingCriteria(
            objects=[
                SetMatchingCriteriaItem(object_cid="obj-1", timestamp=1000),
                SetMatchingCriteriaItem(object_cid="obj-2", timestamp=2000),
                SetMatchingCriteriaItem(object_cid="obj-3", timestamp=3000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should find both matches
        self.assertEqual(len(matches), 2)
        # First match should be the perfect one (lower score)
        self.assertEqual(matches[0].set_cid, "set-perfect")
        self.assertEqual(matches[0].score, 0.0)  # Perfect match
        # Second match should have higher score due to timestamp differences
        self.assertEqual(matches[1].set_cid, "set-offset")
        self.assertEqual(matches[1].score, 0.0)  # perfect match in terms of object order 

    def test_max_5_results_returned(self) -> None:
        """Test that at most 5 matches are returned."""
        # Add 10 sets with same objects
        for i in range(10):
            self.add_test_events([
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
            ])

        service = FuzzySetMatchingService(db_url=self.db_url, tolerance=0.2)

        criteria = SetMatchingCriteria(
            objects=[
                SetMatchingCriteriaItem(object_cid="obj-1", timestamp=1000),
                SetMatchingCriteriaItem(object_cid="obj-2", timestamp=2000),
                SetMatchingCriteriaItem(object_cid="obj-3", timestamp=3000),
            ]
        )

        matches = service.find_matching_sets(criteria)

        # Should return at most 5 matches
        self.assertLessEqual(len(matches), 5)


if __name__ == "__main__":
    unittest.main()
