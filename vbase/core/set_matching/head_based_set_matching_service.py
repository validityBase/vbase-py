"""
Head-based set matching service implementation.
"""

from sqlalchemy import and_, or_
from sqlmodel import Session, create_engine, select

from vbase.core.models import EventAddSetObject, LastBatchProcessingTime
from vbase.core.set_matching.base_set_matching_service import BaseSetMatchingService
from vbase.core.set_matching.types import ObjectSetData, SetKey, SetMatching, SetMatchingCriteria

class HeadBasedSetMatchingService(BaseSetMatchingService):
    """
    Service for performing head-based set matching.
    Finds sets whose head (first elements) matches the elements specified in the criteria.
    Searches the blockchain index SQL table of EventAddSetObject events with the following columns:

    - chainId
    - set_cid
    - user
    - object_cid
    - timestamp

    Each set is identified by its set_cid, user, and chainId, and consists of all
    objects with the same set_cid ordered by timestamp.
    Sets belonging to different users or chains are not mixed together.
    """

    def __init__(
        self,
        db_url: str
    ):
        self.db_url = db_url
        self.db_engine = create_engine(db_url)

    def find_matching_sets(
        self,
        criteria: SetMatchingCriteria,
    ) -> list[SetMatching]:
        """
        Scans the blockchain index SQL table of EventAddSetObject events to find sets
        whose head (first elements) matches the elements specified in the criteria.
        Access to the database is sqlmodel based.

        Algorithm:
            - Find all sets that start with the first object in the criteria.
            - For each candidate set, check whether the subsequent objects in the criteria
              match the subsequent objects in the set.
            - Use timestamps only to determine element ordering, not to filter out matches.

        Corner Cases:
            - Empty search criteria: return an empty list.
            - Multiple matches: return the first five matches, ranked by how well the
              element timestamps align.
        
        """
        if not criteria.objects:
            return []

        # make sure that the criteria objects are ordered by timestamp, which simplifies head matching
        criteria.objects = sorted(criteria.objects, key=lambda item: item.timestamp)
        
        with Session(self.db_engine) as session:

            last_batch_statement = select(LastBatchProcessingTime).order_by(
                LastBatchProcessingTime.timestamp.desc()
            )
            last_batch_record = session.exec(last_batch_statement).first()
            if last_batch_record is None:
                raise ValueError(
                    "No LastBatchProcessingTime records found; cannot determine the last batch processing time."
                )
            last_batch = last_batch_record.timestamp

            # get narrow selection of most promising candidate sets
            candidate_keys = self._get_candidates(session, criteria)

            if not candidate_keys:
                return []

            # at this point we have up to 5 candidates
            # we load all their elements and perform the in memory final ranking
            # based on head matching and timestamp alignment
            event_rows = session.exec(
                select(EventAddSetObject)
                .where(self._build_candidate_filters(candidate_keys))
                .order_by(EventAddSetObject.timestamp)
            ).all()


        # build ObjectSetData from the event_rows
        # as event rows are ordered by timestamp,
        # objects in the resulting sets will also be ordered by timestamp, which simplifies head matching
        candidate_sets_dict: dict[SetKey, ObjectSetData] = {}
        for event_row in event_rows:
            set_key = SetKey(event_row.set_cid, event_row.user, event_row.chain_id)
            if set_key not in candidate_sets_dict:
                candidate_sets_dict[set_key] = ObjectSetData(key=set_key, objects=[])
            candidate_sets_dict[set_key].objects.append(event_row)

        # update set_length for each candidate set
        for candidate_set in candidate_sets_dict.values():
            candidate_set.set_length = len(candidate_set.objects)
        
        candidate_sets: list[ObjectSetData] = list(candidate_sets_dict.values())
        for candidate_set in candidate_sets:
            candidate_set.rank = self._get_distance(candidate_set, criteria)

        # take everything that matches by CIDs (rank != -1) and sort by rank (lower is better)
        matching_sets = [s for s in candidate_sets if s.rank != -1]
        matching_sets.sort(key=lambda s: s.rank)

        # take the max rank to calculate a score as a percentage of the max rank, so that the best match will have score 1.0 and the worst will have score close to 0.0
        max_rank = max(s.rank for s in matching_sets) if matching_sets else 1.0

        # convert to SetMatching and return the top 5
        return [SetMatching(
            rank= 1 - (s.rank / max_rank if max_rank > 0 else 0.0),  # normalize rank to [0 - worst match, 1 - best match] 
            set_cid=s.key.set_cid,
            user=s.key.user,
            as_of_timestamp=s.objects[len(criteria.objects) - 1].timestamp,  # timestamp of the last matching element
            is_full_match=s.set_length == len(criteria.objects),  # case when the head is full set
            data_freshness_timestamp=last_batch
        ) for s in matching_sets[:5]]

    @staticmethod
    def _get_distance(
        candidate: ObjectSetData,
        criteria: SetMatchingCriteria,
    ) -> float:
        """Get a distance for a candidate set based on how well its head matches the criteria.
        If it doesn't match by CIDs - returns -1. Otherwise, returns the sum of absolute timestamp differences between
        the candidate set and the criteria for the head elements.
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
    ) -> list[SetKey]:
        """Find high-probability candidate sets for the given criteria.

        Returns up to five candidate sets that contain the most CIDs from the
        criteria. Element ordering is not considered at this stage.
        """
        candidate_keys: set[SetKey] | None = None

        for criteria_object in criteria.objects:
            # Find all sets that contain the current criteria object.
            candidate_stmt = select(
                EventAddSetObject.set_cid,
                EventAddSetObject.user,
                EventAddSetObject.chain_id,
            ).where(EventAddSetObject.object_cid == criteria_object.object_cid)

            object_candidate_keys = {
                SetKey(row.set_cid, row.user, row.chain_id)
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
                return candidate_keys

        if candidate_keys is None:
            return []

        return candidate_keys


