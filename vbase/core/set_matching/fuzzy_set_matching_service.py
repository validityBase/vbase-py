"""
Fuzzy set matching service implementation.
"""

from sqlalchemy import and_, func, or_
from sqlmodel import Session, create_engine, select

from vbase.core.models import EventAddSetObject
from vbase.core.set_matching.base_set_matching_service import BaseSetMatchingService
from vbase.core.set_matching.types import (
    ObjectSetData,
    SetKey,
    SetMatching,
    SetMatchingCriteria,
)


class FuzzySetMatchingService(BaseSetMatchingService):
    """
    Service for performing fuzzy set matching with tolerance.
    Finds sets whose elements match the criteria within a configurable tolerance.
    Searches the blockchain index SQL table of EventAddSetObject events with the following columns:

    - chainId
    - set_cid
    - user
    - object_cid
    - timestamp

    Each set is identified by its set_cid, user, and chainId, and consists of all
    objects with the same set_cid ordered by timestamp.
    Sets belonging to different users or chains are not mixed together.

    Unlike HeadBasedSetMatchingService which requires exact head matching, this service
    allows a configurable percentage of CIDs to differ.
    """

    MIN_CRITERIA_SIZE_FOR_TOLERANCE = 5

    def __init__(
        self,
        db_url: str,
        tolerance: float = 0.2,
    ):
        """
        Initialize the fuzzy set matching service.

        Args:
            db_url: Database connection URL
            tolerance: Fraction of CIDs that can differ (0.0 to 1.0). Default is 0.2 (20%)
        """
        self.db_url = db_url
        self.db_engine = create_engine(db_url)
        self.tolerance = tolerance

    def find_matching_sets(
        self,
        criteria: SetMatchingCriteria,
    ) -> list[SetMatching]:
        """
        Scans the blockchain index SQL table of EventAddSetObject events to find sets
        whose elements match the criteria within the configured tolerance.
        Access to the database is sqlmodel based.

        Algorithm:
            - If criteria has fewer than MIN_CRITERIA_SIZE_FOR_TOLERANCE elements, use exact matching (tolerance = 0).
            - Find all sets that contain at least (1 - tolerance) * 100% of the criteria elements.
            - For each candidate set, rank by how many CIDs match and timestamp alignment.
            - Use timestamps to determine element ordering.

        Corner Cases:
            - Empty search criteria: return an empty list.
            - Multiple matches: return up to five matches, ranked by match quality.
        """
        if not criteria.objects:
            return []

        # Use exact matching for small criteria sets
        effective_tolerance = (
            0.0
            if len(criteria.objects) < self.MIN_CRITERIA_SIZE_FOR_TOLERANCE
            else self.tolerance
        )

        # Make sure that the criteria objects are ordered by timestamp
        criteria.objects = sorted(criteria.objects, key=lambda item: item.timestamp)

        with Session(self.db_engine) as session:
            # Get narrow selection of most promising candidate sets
            candidate_keys = self._get_candidates(
                session, criteria, effective_tolerance
            )

            if not candidate_keys:
                return []

            # Load all elements for the candidate sets
            event_rows = session.exec(
                select(EventAddSetObject)
                .where(self._build_candidate_filters(candidate_keys))
                .order_by(EventAddSetObject.timestamp)
            ).all()

        # Build ObjectSetData from the event_rows
        candidate_sets_dict: dict[SetKey, ObjectSetData] = {}
        for event_row in event_rows:
            set_key = SetKey(event_row.set_cid, event_row.user, event_row.chain_id)
            if set_key not in candidate_sets_dict:
                candidate_sets_dict[set_key] = ObjectSetData(key=set_key, objects=[])
            candidate_sets_dict[set_key].objects.append(event_row)

        candidate_sets: list[ObjectSetData] = list(candidate_sets_dict.values())
        for candidate_set in candidate_sets:
            candidate_set.rank = self._rank_candidate(
                candidate_set, criteria, effective_tolerance
            )

        # Take everything that meets the tolerance threshold (rank != -1) and sort by rank
        matching_sets = [s for s in candidate_sets if s.rank != -1]
        matching_sets.sort(key=lambda s: s.rank)

        # Return the top 5 matches
        return [
            SetMatching(
                score=s.rank,
                set_cid=s.key.set_cid,
                user=s.key.user,
                as_of_timestamp=s.objects[min(len(s.objects) - 1, len(criteria.objects) - 1)].timestamp,
            )
            for s in matching_sets[:5]
        ]

    @staticmethod
    def _rank_candidate(
        candidate: ObjectSetData,
        criteria: SetMatchingCriteria,
        tolerance: float,
    ) -> int:
        """
        Ranks a candidate set based on how well it matches the criteria with tolerance.

        Compares sequences position-by-position in timestamp order. Element ordering matters:
        if criteria is [a, b, c] and candidate is [b, a, c], positions 0 and 1 differ.

        Returns -1 if the match quality is below the tolerance threshold.
        Otherwise, returns a score based on:
        - Number of matching positions (primary factor)
        - Timestamp alignment for matching positions (secondary factor)

        Lower scores are better.
        """
        ordered_criteria = sorted(criteria.objects, key=lambda item: item.timestamp)
        ordered_candidate = sorted(candidate.objects, key=lambda obj: obj.timestamp)

        # Compare the shorter length to avoid index errors
        comparison_length = min(len(ordered_criteria), len(ordered_candidate))
        
        # Count position-by-position matches
        position_matches = sum(
            1
            for i in range(comparison_length)
            if ordered_criteria[i].object_cid == ordered_candidate[i].object_cid
        )

        # Calculate required matches based on tolerance
        required_matches = int(len(ordered_criteria) * (1.0 - tolerance))

        # Check if we meet the tolerance threshold
        if position_matches < required_matches:
            return -1

        mismatch_count = len(ordered_criteria) - position_matches
        # calculate a rank as a percentage of matching positions
        # so ideal match will be 1.0
        rank = (len(ordered_criteria) - position_matches) / len(ordered_criteria) 
        return rank

    @staticmethod
    def _build_candidate_filters(
        candidate_keys: list[SetKey],
    ):
        """Builds SQLAlchemy filters to find all events belonging to any of the candidate sets."""
        return or_(
            *[
                and_(
                    EventAddSetObject.set_cid == candidate_key.set_cid,
                    EventAddSetObject.user == candidate_key.user,
                    EventAddSetObject.chain_id == candidate_key.chain_id,
                )
                for candidate_key in candidate_keys
            ]
        )

    def _get_candidates(
        self,
        session: Session,
        criteria: SetMatchingCriteria,
        tolerance: float,
    ) -> list[SetKey]:
        """
        Find high-probability candidate sets for the given criteria with tolerance.

        Unlike the head-based approach, this method finds sets that contain at least
        (1 - tolerance) * 100% of the criteria CIDs, not necessarily all of them.

        Returns up to five candidate sets ranked by how many criteria CIDs they contain.
        """
        if not criteria.objects:
            return []

        required_matches = max(1, int(len(criteria.objects) * (1.0 - tolerance)))

        # Collect all criteria CIDs
        criteria_cids = [obj.object_cid for obj in criteria.objects]

        # Find all sets that contain at least one of the criteria CIDs
        # Group by set key and count how many criteria CIDs each set contains
        candidate_stmt = (
            select(
                EventAddSetObject.set_cid,
                EventAddSetObject.user,
                EventAddSetObject.chain_id,
                func.count(EventAddSetObject.object_cid).label(
                    "match_count"
                ),
            )
            .where(EventAddSetObject.object_cid.in_(criteria_cids))
            .group_by(
                EventAddSetObject.set_cid,
                EventAddSetObject.user,
                EventAddSetObject.chain_id,
            )
            .having(
                func.count(EventAddSetObject.object_cid)
                >= required_matches
            )
            .order_by(func.count(EventAddSetObject.object_cid).desc())
            .limit(5)
        )

        rows = session.exec(candidate_stmt).all()

        return [SetKey(row.set_cid, row.user, row.chain_id) for row in rows]
