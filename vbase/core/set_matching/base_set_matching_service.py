"""
Base class for set matching strategies.
"""

from abc import ABC, abstractmethod

from vbase.core.set_matching.types import SetMatching, SetMatchingCriteria
class BaseSetMatchingService(ABC):
    """
    Abstract base class for set matching strategies.

    Subclass this to implement alternative matching logic (e.g. stricter
    timestamp tolerances, weighted scoring, or a non-SQL data source)
    """

    @abstractmethod
    def find_matching_sets(
        self,
        criteria: SetMatchingCriteria,
    ) -> list[SetMatching]:
        """
        Find committed sets that best match the provided criteria.

        Args:
            criteria: describes what set we are trying to find a match for
        Returns:
            Matching sets.
        """
        pass
