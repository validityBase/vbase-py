"""Unit tests for ChainSetMatchingService."""

import unittest

from vbase.core.set_matching.chain_set_matching_service import (
    ChainSetMatchingService,
)
from vbase.core.set_matching.base_set_matching_service import BaseSetMatchingService
from vbase.core.set_matching.types import (
    SetMatch,
    SetMatchingCriteria,
    TimestampedCid,
)


class StubSetMatchingService(BaseSetMatchingService):
    """Stub strategy that records calls and returns a fixed result."""

    def __init__(self, matches: list[SetMatch]):
        self.matches = matches
        self.call_count = 0

    def find_matching_sets(self, criteria: SetMatchingCriteria) -> list[SetMatch]:
        self.call_count += 1
        return self.matches


class TestChainSetMatchingService(unittest.TestCase):
    """Tests for sequential strategy evaluation in ChainSetMatchingService."""

    def setUp(self) -> None:
        """Create shared criteria and match fixtures."""
        self.criteria = SetMatchingCriteria(
            objects=[TimestampedCid(object_cid="cid-1", timestamp=1700000000)]
        )
        self.match_a = SetMatch(
            rank=0.9,
            set_cid="set-a",
            user="alice",
            last_matching_element_timestamp=1700000002,
            is_full_match=False,
        )
        self.match_b = SetMatch(
            rank=0.8,
            set_cid="set-b",
            user="bob",
            last_matching_element_timestamp=1700000004,
            is_full_match=False,
        )

    def test_returns_first_non_empty_and_stops(self) -> None:
        """Return the first non-empty strategy result and stop evaluating."""
        first = StubSetMatchingService([self.match_a])
        second = StubSetMatchingService([self.match_b])
        service = ChainSetMatchingService([first, second])

        result = service.find_matching_sets(self.criteria)

        self.assertEqual(result, [self.match_a])
        self.assertEqual(first.call_count, 1)
        self.assertEqual(second.call_count, 0)

    def test_returns_first_non_empty_after_skipping_empty(self) -> None:
        """Skip empty strategies until a non-empty strategy returns matches."""
        empty = StubSetMatchingService([])
        second = StubSetMatchingService([self.match_b])
        service = ChainSetMatchingService([empty, second])

        result = service.find_matching_sets(self.criteria)

        self.assertEqual(result, [self.match_b])
        self.assertEqual(empty.call_count, 1)
        self.assertEqual(second.call_count, 1)

    def test_returns_empty_when_no_strategies_match(self) -> None:
        """Return an empty list when all strategies return no matches."""
        strategies = [StubSetMatchingService([]), StubSetMatchingService([])]
        service = ChainSetMatchingService(strategies)

        result = service.find_matching_sets(self.criteria)

        self.assertEqual(result, [])
        for strategy in strategies:
            self.assertEqual(strategy.call_count, 1)


if __name__ == "__main__":
    unittest.main()
