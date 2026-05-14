"""
Base class for set matching strategies.
"""

from abc import ABC, abstractmethod

from sqlalchemy import and_, or_
from sqlmodel import Session, select

from vbase.core.models import EventAddSetObject, LastBatchProcessingTime
from vbase.core.set_matching.types import SetIdentifier, SetMatch, SetMatchingCriteria


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
    ) -> list[SetMatch]:
        """
        Find committed sets that best match the provided criteria.

        Args:
            criteria: describes what set we are trying to find a match for
        Returns:
            Matching sets.
        """

    @staticmethod
    def _fetch_last_batch_timestamp(session: Session) -> int:
        """
        Return the most recent LastBatchProcessingTime timestamp.

        Raises:
            ValueError: if no records exist.
        """
        statement = select(LastBatchProcessingTime).order_by(
            LastBatchProcessingTime.timestamp.desc()
        )
        record = session.exec(statement).first()
        if record is None:
            raise ValueError(
                "No LastBatchProcessingTime records found; cannot determine"
                " the last batch processing time."
            )
        return record.timestamp

    @staticmethod
    def _build_candidate_filters(candidate_keys: list[SetIdentifier]):
        """Build a SQLAlchemy filter matching any of the given set keys."""
        return or_(
            *[
                and_(
                    EventAddSetObject.set_cid == candidate_key.set_cid,
                    EventAddSetObject.user == candidate_key.user,
                )
                for candidate_key in candidate_keys
            ]
        )

    @staticmethod
    def _load_candidate_events(
        session: Session, candidate_keys: list[SetIdentifier]
    ) -> list[EventAddSetObject]:
        """Load all EventAddSetObject rows for the given candidate keys, ordered by timestamp."""
        return session.exec(
            select(EventAddSetObject)
            .where(BaseSetMatchingService._build_candidate_filters(candidate_keys))
            .order_by(EventAddSetObject.timestamp)
        ).all()
