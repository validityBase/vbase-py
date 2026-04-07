"""
Fuzzy set matching service implementation.
"""

import math

from sqlalchemy import and_, func, or_
from sqlmodel import Session, create_engine, select

from vbase.core.models import EventAddSetObject
from vbase.core.set_matching.base_set_matching_service import BaseSetMatchingService
from vbase.core.set_matching.types import (
    FuzzyCheckObjectSetData,
    LevenshteinDistance,
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
            - Order criteria and candidate set elements by timestamp to build comparable CID sequences.
            - Rank candidate sets by CID sequence similarity (for example, Levenshtein distance over the ordered CIDs).

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

        # Build FuzzyCheckObjectSetData from the event_rows
        candidate_sets_dict: dict[SetKey, FuzzyCheckObjectSetData] = {}
        for event_row in event_rows:
            set_key = SetKey(event_row.set_cid, event_row.user, event_row.chain_id)
            if set_key not in candidate_sets_dict:
                candidate_sets_dict[set_key] = FuzzyCheckObjectSetData(
                    key=set_key,
                    objects=[],
                )
            candidate_sets_dict[set_key].objects.append(event_row)

        candidate_sets: list[FuzzyCheckObjectSetData] = list(
            candidate_sets_dict.values()
        )

        # update set_length for each candidate set
        for candidate_set in candidate_sets_dict.values():
            candidate_set.set_length = len(candidate_set.objects)

        for candidate_set in candidate_sets:
            candidate_set.rank, candidate_set.lev_result = self._rank_candidate(
                candidate_set, criteria, effective_tolerance
            )

        # Take everything that meets the tolerance threshold (rank != -1) and sort by rank
        matching_sets = [s for s in candidate_sets if s.rank != -1]
        matching_sets.sort(key=lambda s: s.rank, reverse=True)

        # Return the top 5 matches
        results: list[SetMatching] = []
        for candidate_set in matching_sets[:5]:
            # Project the criteria length onto the candidate sequence, adjusted by
            # Levenshtein insertions and deletions, to find the matched timestamp.
            matched_length = len(criteria.objects)
            if candidate_set.lev_result is not None:
                matched_length += candidate_set.lev_result.insertions
                matched_length -= candidate_set.lev_result.deletions

            as_of_index = min(
                len(candidate_set.objects) - 1,
                max(0, matched_length - 1),
            )
            results.append(
                SetMatching(
                    rank=candidate_set.rank,
                    set_cid=candidate_set.key.set_cid,
                    user=candidate_set.key.user,
                    as_of_timestamp=candidate_set.objects[as_of_index].timestamp,
                    is_full_match=(
                        candidate_set.set_length == len(criteria.objects)
                        and candidate_set.lev_result is not None
                        and candidate_set.lev_result.distance == 0
                    )
                )
            )

        return results

    @staticmethod
    def _rank_candidate(
        candidate: FuzzyCheckObjectSetData,
        criteria: SetMatchingCriteria,
        tolerance: float,
    ) -> tuple[float, LevenshteinDistance]:
        """
        Ranks a candidate set based on how well it matches the criteria with tolerance.

        Uses Levenshtein distance to compare sequences of CIDs in timestamp order.
        The Levenshtein distance measures the minimum number of edits (insertions,
        deletions, or substitutions) needed to transform one sequence into another.

        Returns the rank and detailed Levenshtein result for the comparison.
        A rank of -1 means the candidate is below the tolerance threshold.

        Higher ranks are better, with 1.0 representing a perfect match.
        """
        ordered_criteria = sorted(criteria.objects, key=lambda item: item.timestamp)
        ordered_candidate = sorted(candidate.objects, key=lambda obj: obj.timestamp)

        # Extract CID sequences
        criteria_cids = [obj.object_cid for obj in ordered_criteria]
        candidate_cids = [obj.object_cid for obj in ordered_candidate]

        # Truncate the longer sequence to match the criteria length
        # This ensures we only compare up to the criteria length
        if len(candidate_cids) > len(criteria_cids):
            candidate_cids = candidate_cids[:len(criteria_cids)]

        # Calculate Levenshtein distance
        lev_result = FuzzySetMatchingService._levenshtein_distance(
            criteria_cids, candidate_cids
        )

        # Calculate required matches based on tolerance (ceil ensures mismatch fraction never exceeds tolerance)
        required_matches = math.ceil(len(ordered_criteria) * (1.0 - tolerance))
        
        # Maximum allowed distance is the number of allowed mismatches
        max_allowed_distance = len(ordered_criteria) - required_matches

        # Check if we meet the tolerance threshold

        if lev_result.insertions > 0:
            # if we have insertions, it means we are pushing the criteria tail behind the candidate tail,
            # which produce additional deletions, so we need to substract insertions from deletions, and recalculate the total distance
            adjusted_deletions = max(0, lev_result.deletions - lev_result.insertions)
            lev_result = LevenshteinDistance(
                insertions=lev_result.insertions,
                deletions=adjusted_deletions,
                substitutions=lev_result.substitutions,
                distance=lev_result.insertions + adjusted_deletions + lev_result.substitutions
            )

        if lev_result.distance > max_allowed_distance:
            return -1, lev_result

        # Calculate rank as distance normalized by criteria length
        # Perfect match (distance=0) gets rank=1.0, higher distance gets lower rank
        rank = 1 - lev_result.distance / len(ordered_criteria)
        return rank, lev_result

    @staticmethod
    def _levenshtein_distance(seq1: list, seq2: list) -> LevenshteinDistance:
        """
        Calculate the Levenshtein distance between two sequences.

        The Levenshtein distance is the minimum number of single-element edits
        (insertions, deletions, or substitutions) required to change one sequence
        into another.

        Args:
            seq1: First sequence
            seq2: Second sequence

        Returns:
            LevenshteinDistance with detailed operation counts
        """
        len1, len2 = len(seq1), len(seq2)

        # Create a matrix to store distances
        dp = [[0] * (len2 + 1) for _ in range(len1 + 1)]

        # Initialize first column and row
        for i in range(len1 + 1):
            dp[i][0] = i
        for j in range(len2 + 1):
            dp[0][j] = j

        # Fill the matrix
        for i in range(1, len1 + 1):
            for j in range(1, len2 + 1):
                if seq1[i - 1] == seq2[j - 1]:
                    dp[i][j] = dp[i - 1][j - 1]
                else:
                    dp[i][j] = 1 + min(
                        dp[i - 1][j],      # deletion
                        dp[i][j - 1],      # insertion
                        dp[i - 1][j - 1]   # substitution
                    )

        # Backtrack to count operation types
        insertions = 0
        deletions = 0
        substitutions = 0
        i, j = len1, len2
        
        while i > 0 or j > 0:
            if i == 0:
                # Only insertions left
                insertions += j
                break
            if j == 0:
                # Only deletions left
                deletions += i
                break
            
            current = dp[i][j]
            diagonal = dp[i - 1][j - 1]
            left = dp[i][j - 1]
            up = dp[i - 1][j]
            
            if seq1[i - 1] == seq2[j - 1]:
                # No operation needed, move diagonally
                i -= 1
                j -= 1
            elif current == diagonal + 1:
                # Substitution
                substitutions += 1
                i -= 1
                j -= 1
            elif current == left + 1:
                # Insertion
                insertions += 1
                j -= 1
            else:
                # Deletion
                deletions += 1
                i -= 1
        
        total_distance = insertions + deletions + substitutions
        return LevenshteinDistance(
            insertions=insertions,
            deletions=deletions,
            substitutions=substitutions,
            distance=total_distance
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

        required_matches = max(1, math.ceil(len(criteria.objects) * (1.0 - tolerance)))

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
