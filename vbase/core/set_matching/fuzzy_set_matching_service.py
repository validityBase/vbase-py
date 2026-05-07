"""
Fuzzy set matching service implementation.
"""

import math
from collections import Counter

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
    Searches the blockchain index SQL table of EventAddSetObject events with the
    following columns:

    - chainId
    - set_cid
    - user
    - object_cid
    - timestamp

    Each set is identified by its set_cid, user, and chainId, and consists of all
    objects with the same set_cid ordered by timestamp.
    Sets belonging to different users or chains are not mixed together.

    Unlike HeadBasedSetMatchingService which requires exact head matching, this
    service allows a configurable percentage of CIDs to differ.
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
        if tolerance < 0.0 or tolerance > 1.0:
            raise ValueError("tolerance must be between 0.0 and 1.0 inclusive")
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
            - If criteria has fewer than MIN_CRITERIA_SIZE_FOR_TOLERANCE elements,
              use exact matching (tolerance = 0).
            - Find all sets that contain at least (1 - tolerance) * 100% of the
              criteria elements.
            - Order criteria and candidate set elements by timestamp to build
              comparable CID sequences.
            - Rank candidate sets by CID sequence similarity (Levenshtein distance).

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
        sorted_criteria = SetMatchingCriteria(
            objects=sorted(criteria.objects, key=lambda item: item.timestamp)
        )

        with Session(self.db_engine) as session:
            last_batch = self._fetch_last_batch_timestamp(session)
            candidate_keys = self._get_candidates(
                session, sorted_criteria, effective_tolerance
            )
            if not candidate_keys:
                return []
            event_rows = self._load_candidate_events(session, candidate_keys)

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

        # Update set_length for each candidate set
        for candidate_set in candidate_sets_dict.values():
            candidate_set.set_length = len(candidate_set.objects)

        for candidate_set in candidate_sets:
            (
                candidate_set.rank,
                candidate_set.lev_result,
                candidate_set.projected_last_element_index,
            ) = self._rank_candidate(
                candidate_set, sorted_criteria, effective_tolerance
            )

        # Take everything that meets the tolerance threshold (rank != -1) and sort
        matching_sets = [s for s in candidate_sets if s.rank != -1]
        matching_sets.sort(key=lambda s: s.rank, reverse=True)

        # Return the top 5 matches
        return [
            SetMatching(
                rank=s.rank,
                set_cid=s.key.set_cid,
                user=s.key.user,
                as_of_timestamp=s.objects[s.projected_last_element_index].timestamp,
                data_freshness_timestamp=last_batch,
                is_full_match=(
                    s.set_length == len(sorted_criteria.objects)
                    and s.lev_result is not None
                    and s.lev_result.distance == 0
                ),
            )
            for s in matching_sets[:5]
        ]

    @staticmethod
    def _rank_candidate(
        candidate: FuzzyCheckObjectSetData,
        criteria: SetMatchingCriteria,
        tolerance: float,
    ) -> tuple[float, LevenshteinDistance, int]:
        """
        Ranks a candidate set based on how well it matches the criteria with tolerance.

        Uses Levenshtein distance to compare sequences of CIDs in timestamp order.
        The Levenshtein distance measures the minimum number of edits (insertions,
        deletions, or substitutions) needed to transform one sequence into another.

        Returns the rank, detailed Levenshtein result, and projected last element
        index. A rank of -1 means the candidate is below the tolerance threshold.
        Higher ranks are better, with 1.0 representing a perfect match.
        """
        ordered_criteria = sorted(criteria.objects, key=lambda item: item.timestamp)
        criteria_cids = [obj.object_cid for obj in ordered_criteria]
        candidate_cids = [
            obj.object_cid
            for obj in sorted(candidate.objects, key=lambda obj: obj.timestamp)
        ]

        # Maximum allowed edits (ceil ensures mismatch fraction never exceeds tolerance)
        max_allowed_distance = len(ordered_criteria) - math.ceil(
            len(ordered_criteria) * (1.0 - tolerance)
        )

        # Allow up to max_allowed_distance extra elements in the candidate so that
        # Levenshtein can detect head insertions correctly. Truncating to exactly
        # len(criteria_cids) forces equal-length sequences, where Levenshtein
        # prefers substitutions over insert+delete, hiding true insertions and
        # producing incorrect as_of_timestamp values.
        if len(candidate_cids) > len(criteria_cids) + max_allowed_distance:
            candidate_cids = candidate_cids[: len(criteria_cids) + max_allowed_distance]

        lev_result = FuzzySetMatchingService._levenshtein_distance(
            criteria_cids, candidate_cids
        )

        projected_last_element_index = len(ordered_criteria) - 1
        for op, idx in lev_result.operations:
            if op == "D" and idx <= projected_last_element_index:
                projected_last_element_index -= 1
            elif op == "I" and idx <= projected_last_element_index:
                projected_last_element_index += 1

        # Count operations that fall within the projected window
        head_distance = sum(
            1
            for op, idx in lev_result.operations
            if idx <= projected_last_element_index
        )

        if head_distance > max_allowed_distance:
            return -1, lev_result, 0

        # Perfect match (distance=0) gets rank=1.0, higher distance gets lower rank
        rank = 1 - head_distance / len(ordered_criteria)
        return rank, lev_result, projected_last_element_index

    @staticmethod
    def _backtrack_operations(
        dp: list[list[int]], seq1: list, seq2: list
    ) -> tuple[int, int, int, list[tuple[str, int]]]:
        """
        Backtrack through the Levenshtein DP table to record edit operations.

        Returns (insertions, deletions, substitutions, ops) where ops is a list
        of (op_type, position) tuples in forward order.
        """
        insertions = 0
        deletions = 0
        substitutions = 0
        ops: list[tuple[str, int]] = []
        i, j = len(seq1), len(seq2)
        while i > 0 or j > 0:
            if i == 0:
                for k in range(j, 0, -1):
                    ops.append(("I", k - 1))
                insertions += j
                break
            if j == 0:
                for k in range(i, 0, -1):
                    ops.append(("D", k - 1))
                deletions += i
                break
            if seq1[i - 1] == seq2[j - 1]:
                i -= 1
                j -= 1
            elif dp[i][j] == dp[i - 1][j - 1] + 1:
                substitutions += 1
                ops.append(("S", i - 1))
                i -= 1
                j -= 1
            elif dp[i][j] == dp[i][j - 1] + 1:
                insertions += 1
                ops.append(("I", j - 1))
                j -= 1
            else:
                deletions += 1
                ops.append(("D", i - 1))
                i -= 1
        ops.reverse()
        return insertions, deletions, substitutions, ops

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

        # Build the DP cost matrix
        dp = [[0] * (len2 + 1) for _ in range(len1 + 1)]
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
                        dp[i - 1][j - 1],  # substitution
                    )

        insertions, deletions, substitutions, ops = (
            FuzzySetMatchingService._backtrack_operations(dp, seq1, seq2)
        )
        return LevenshteinDistance(
            insertions=insertions,
            deletions=deletions,
            substitutions=substitutions,
            distance=insertions + deletions + substitutions,
            operations=ops,
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

        required_matches = max(
            1, math.ceil(len(criteria.objects) * (1.0 - tolerance))
        )

        # Collect all criteria CIDs
        criteria_cids = [obj.object_cid for obj in criteria.objects]

        # Query all events matching any criteria CID, then aggregate in Python
        rows = session.exec(
            select(
                EventAddSetObject.set_cid,
                EventAddSetObject.user,
                EventAddSetObject.chain_id,
            ).where(EventAddSetObject.object_cid.in_(criteria_cids))
        ).all()

        counts: Counter[SetKey] = Counter(
            SetKey(row.set_cid, row.user, row.chain_id) for row in rows
        )

        return [
            key
            for key, count in sorted(counts.items(), key=lambda x: -x[1])
            if count >= required_matches
        ][:5]
