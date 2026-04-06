import unittest

from vbase.core.set_matching.chain_set_matching_service import (
    ChainSetMatchingService,
)
from vbase.core.set_matching.base_set_matching_service import BaseSetMatchingService
from vbase.core.set_matching.types import (
    SetMatching,
    SetMatchingCriteria,
    SetMatchingCriteriaItem,
)


class StubSetMatchingService(BaseSetMatchingService):
    def __init__(self, matches: list[SetMatching]):
        self.matches = matches
        self.call_count = 0

    def find_matching_sets(self, criteria: SetMatchingCriteria) -> list[SetMatching]:
        self.call_count += 1
        return self.matches


class TestChainSetMatchingService(unittest.TestCase):
    def setUp(self) -> None:
        self.criteria = SetMatchingCriteria(
            objects=[SetMatchingCriteriaItem(object_cid="cid-1", timestamp=1700000000)]
        )
        self.match_a = SetMatching(
            rank=0.9,
            set_cid="set-a",
            user="alice",
            as_of_timestamp=1700000002,
            is_full_match=False,
        )
        self.match_b = SetMatching(
            rank=0.8,
            set_cid="set-b",
            user="bob",
            as_of_timestamp=1700000004,
            is_full_match=False,
        )

    def test_returns_first_non_empty_and_stops(self) -> None:
        first = StubSetMatchingService([self.match_a])
        second = StubSetMatchingService([self.match_b])
        service = ChainSetMatchingService([first, second])

        result = service.find_matching_sets(self.criteria)

        self.assertEqual(result, [self.match_a])
        self.assertEqual(first.call_count, 1)
        self.assertEqual(second.call_count, 0)

    def test_returns_first_non_empty_after_skipping_empty(self) -> None:
        empty = StubSetMatchingService([])
        second = StubSetMatchingService([self.match_b])
        service = ChainSetMatchingService([empty, second])

        result = service.find_matching_sets(self.criteria)

        self.assertEqual(result, [self.match_b])
        self.assertEqual(empty.call_count, 1)
        self.assertEqual(second.call_count, 1)

    def test_returns_empty_when_no_strategies_match(self) -> None:
        strategies = [StubSetMatchingService([]), StubSetMatchingService([])]
        service = ChainSetMatchingService(strategies)

        result = service.find_matching_sets(self.criteria)

        self.assertEqual(result, [])
        for strategy in strategies:
            self.assertEqual(strategy.call_count, 1)


if __name__ == "__main__":
    unittest.main()