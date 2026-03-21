""""
Aggregate set matching strategies.
"""


from vbase.core.set_matching.base_set_matching_service import BaseSetMatchingService
from vbase.core.set_matching.types import SetMatching, SetMatchingCriteria


class AggregateSetMatchingService(BaseSetMatchingService):
    """
    Aggregate multiple set matching strategies to return first non-empty result.
    This service execute matching strategies in order, and returns the first non-empty result. If all strategies return empty results, it returns an empty list.
    """

    def __init__(self, strategies: list[BaseSetMatchingService]):
        self.strategies = strategies

    def find_matching_sets(
        self,
        criteria: SetMatchingCriteria,
    ) -> list[SetMatching]:
        for strategy in self.strategies:
            matches = strategy.find_matching_sets(criteria)
            if matches:
                return matches
        return []