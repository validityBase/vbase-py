"""vbase.core.set_matching

Core classes for set matching services in the validityBase (vBase) platform Python library
"""

from vbase.core.set_matching.chain_set_matching_service import (
    ChainSetMatchingService,
)
from vbase.core.set_matching.base_set_matching_service import BaseSetMatchingService
from vbase.core.set_matching.fuzzy_set_matching_service import FuzzySetMatchingService
from vbase.core.set_matching.head_based_set_matching_service import (
    HeadBasedSetMatchingService,
)
from vbase.core.set_matching.types import (
    LevenshteinDistance,
    SetMatching,
    SetMatchingCriteria,
    SetMatchingCriteriaItem,
)

__all__ = [
    "ChainSetMatchingService",
    "BaseSetMatchingService",
    "FuzzySetMatchingService",
    "HeadBasedSetMatchingService",
    "LevenshteinDistance",
    "SetMatching",
    "SetMatchingCriteria",
    "SetMatchingCriteriaItem",
]
