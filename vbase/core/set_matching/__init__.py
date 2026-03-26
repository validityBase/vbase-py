"""vbase.core.set_matching

Core classes for set matching services in the validityBase (vBase) platform Python library
"""

from vbase.core.set_matching.aggregate_set_matching_service import (
    AggregateSetMatchingService,
)
from vbase.core.set_matching.base_set_matching_service import BaseSetMatchingService
from vbase.core.set_matching.fuzzy_set_matching_service import FuzzySetMatchingService
from vbase.core.set_matching.head_based_set_matching_service import (
    HeadBasedSetMatchingService,
)
from vbase.core.set_matching.types import (
    SetMatching,
    SetMatchingCriteria,
    SetMatchingCriteriaItem,
)

__all__ = [
    "AggregateSetMatchingService",
    "BaseSetMatchingService",
    "FuzzySetMatchingService",
    "HeadBasedSetMatchingService",
    "SetMatching",
    "SetMatchingCriteria",
    "SetMatchingCriteriaItem",
]
