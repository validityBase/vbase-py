"""
Head-based set matching service implementation.
"""

from sqlmodel import Session, create_engine, select

from vbase.core.models import EventAddSetObject
from vbase.core.set_matching.base_set_matching_service import BaseSetMatchingService
from vbase.core.set_matching.types import (
    ObjectSetData,
    SetIdentifier,
    SetMatch,
    SetMatchingCriteria,
)


class HeadBasedSetMatchingService(
    BaseSetMatchingService
):  # pylint: disable=too-few-public-methods
    """
    Service for performing head-based set matching.
    Finds sets whose head (first elements) matches the elements specified in the criteria.
    Searches the blockchain index SQL table of EventAddSetObject events with the following columns:

    - set_cid
    - user
    - object_cid
    - timestamp

    Each set is identified by its set_cid and user, and consists of all objects with
    the same set_cid ordered by timestamp. Elements may come from multiple chains
    (distributed sets).
    """

    def __init__(self, db_url: str):
        self.db_url = db_url
        self.db_engine = create_engine(db_url)

    def find_matching_sets(
        self,
        criteria: SetMatchingCriteria,
    ) -> list[SetMatch]:
        """
        Scans the blockchain index SQL table of EventAddSetObject events to find sets
        whose head (first elements) matches the elements specified in the criteria.
        Access to the database is sqlmodel based.

        Algorithm:
            - Find all sets that start with the first object in the criteria.
            - For each candidate set, check whether the subsequent objects in the
              criteria match the subsequent objects in the set.
            - Use timestamps only to determine element ordering, not to filter
              out matches.

        Corner Cases:
            - Empty search criteria: return an empty list.
            - Multiple matches: return the first five matches, ranked by how well
              the element timestamps align.
        """
        if not criteria.objects:
            return []

        # Make sure that the criteria objects are ordered by timestamp
        ordered_criteria = SetMatchingCriteria(
            objects=sorted(criteria.objects, key=lambda item: item.timestamp)
        )

        with Session(self.db_engine) as session:
            last_batch = self._fetch_last_batch_timestamp(session)
            candidate_keys = self._get_candidates(session, ordered_criteria)
            if not candidate_keys:
                return []
            event_rows = self._load_candidate_events(session, candidate_keys)

        # Build ObjectSetData from event_rows (already ordered by timestamp)
        candidate_sets_dict: dict[SetIdentifier, ObjectSetData] = {}
        for event_row in event_rows:
            set_key = SetIdentifier(event_row.set_cid, event_row.user)
            if set_key not in candidate_sets_dict:
                candidate_sets_dict[set_key] = ObjectSetData(key=set_key, objects=[])
            candidate_sets_dict[set_key].objects.append(event_row)

        # Update set_length for each candidate set
        for candidate_set in candidate_sets_dict.values():
            candidate_set.set_length = len(candidate_set.objects)

        candidate_sets: list[ObjectSetData] = list(candidate_sets_dict.values())
        for candidate_set in candidate_sets:
            candidate_set.rank = self._get_distance(candidate_set, ordered_criteria)

        # Take everything that matches by CIDs (rank != -1), sort by rank (lower is better)
        matching_sets = [s for s in candidate_sets if s.rank != -1]
        matching_sets.sort(key=lambda s: s.rank)

        # Normalise rank: best match → 1.0, worst → near 0.0
        max_rank = max(s.rank for s in matching_sets) if matching_sets else 1.0
        last_idx = len(ordered_criteria.objects) - 1

        return [
            SetMatch(
                rank=1 - (s.rank / max_rank if max_rank > 0 else 0.0),
                set_cid=s.key.set_cid,
                user=s.key.user,
                # Timestamp of the last criteria-matching element
                last_matching_element_timestamp=s.objects[last_idx].timestamp,
                # Full match when the head covers the entire set
                is_full_match=s.set_length == len(ordered_criteria.objects),
                data_freshness_timestamp=last_batch,
            )
            for s in matching_sets[:5]
        ]

    @staticmethod
    def _get_distance(
        candidate: ObjectSetData,
        criteria: SetMatchingCriteria,
    ) -> float:
        """
        Return a distance for a candidate set based on how well its head matches
        the criteria. Returns -1 if CIDs do not match; otherwise returns the sum
        of absolute timestamp differences for the head elements.
        """
        ordered_criteria = sorted(criteria.objects, key=lambda item: item.timestamp)

        if len(candidate.objects) < len(ordered_criteria):
            return -1

        candidate_head = candidate.objects[: len(ordered_criteria)]
        if any(
            event_row.object_cid != criteria_item.object_cid
            for event_row, criteria_item in zip(candidate_head, ordered_criteria)
        ):
            return -1

        return sum(
            abs(event_row.timestamp - criteria_item.timestamp)
            for event_row, criteria_item in zip(candidate_head, ordered_criteria)
        )

    def _get_candidates(
        self,
        session: Session,
        criteria: SetMatchingCriteria,
    ) -> list[SetIdentifier]:
        """Find high-probability candidate sets for the given criteria.

        Returns up to five candidate sets that contain the most CIDs from the
        criteria. Element ordering is not considered at this stage.
        """
        candidate_keys: set[SetIdentifier] | None = None

        for criteria_object in criteria.objects:
            # Find all sets that contain the current criteria object.
            candidate_stmt = select(
                EventAddSetObject.set_cid,
                EventAddSetObject.user,
            ).where(EventAddSetObject.object_cid == criteria_object.object_cid)

            object_candidate_keys = {
                SetIdentifier(row.set_cid, row.user)
                for row in session.exec(candidate_stmt).all()
            }

            # Intersect the previous results with the current ones.
            # This builds the set of candidates that contain all processed criteria elements.
            if candidate_keys is None:
                candidate_keys = object_candidate_keys
            else:
                candidate_keys &= object_candidate_keys

            # No set contains this object, so no full match is possible.
            if not candidate_keys:
                return []

            if len(candidate_keys) <= 5:
                # The candidate list is now narrow enough for deeper processing,
                # which loads all elements for the remaining sets.
                # At this point, that is likely cheaper than continuing to intersect.
                return sorted(
                    candidate_keys,
                    key=lambda candidate_key: (
                        candidate_key.user,
                        candidate_key.set_cid,
                    ),
                )

        if candidate_keys is None:
            return []

        return sorted(
            candidate_keys,
            key=lambda candidate_key: (
                candidate_key.user,
                candidate_key.set_cid,
            ),
        )[:5]
