"""
Unit tests for the Levenshtein distance algorithm used in FuzzySetMatchingService.

The Levenshtein distance is the minimum number of single-element edits
(insertions, deletions, or substitutions) required to change one sequence
into another.
"""

# pylint: disable=protected-access,too-many-public-methods

import unittest

from vbase.core.set_matching.fuzzy_set_matching_service import (
    FuzzySetMatchingService,
)


class TestLevenshteinDistance(unittest.TestCase):
    """
    Tests for the _levenshtein_distance static method.
    """

    # ========== Empty Sequence Tests ==========

    def test_both_sequences_empty(self) -> None:
        """Test that distance between two empty sequences is 0."""
        distance = FuzzySetMatchingService._levenshtein_distance([], [])
        self.assertEqual(distance.distance, 0)

    def test_first_sequence_empty(self) -> None:
        """Test that distance equals length of non-empty sequence when first is empty."""
        distance = FuzzySetMatchingService._levenshtein_distance([], ["a", "b", "c"])
        self.assertEqual(distance.distance, 3)

    def test_second_sequence_empty(self) -> None:
        """Test that distance equals length of non-empty sequence when second is empty."""
        distance = FuzzySetMatchingService._levenshtein_distance(["a", "b", "c"], [])
        self.assertEqual(distance.distance, 3)

    # ========== Identical Sequence Tests ==========

    def test_identical_single_element(self) -> None:
        """Test that distance between identical single-element sequences is 0."""
        distance = FuzzySetMatchingService._levenshtein_distance(["a"], ["a"])
        self.assertEqual(distance.distance, 0)

    def test_identical_multiple_elements(self) -> None:
        """Test that distance between identical multi-element sequences is 0."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c", "d"], ["a", "b", "c", "d"]
        )
        self.assertEqual(distance.distance, 0)

    def test_identical_numeric_sequences(self) -> None:
        """Test that distance between identical numeric sequences is 0."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            [1, 2, 3, 4], [1, 2, 3, 4]
        )
        self.assertEqual(distance.distance, 0)

    # ========== Single Operation Tests ==========

    def test_single_substitution(self) -> None:
        """Test that single character substitution has distance 1."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c"], ["a", "x", "c"]
        )
        self.assertEqual(distance.distance, 1)

    def test_single_insertion_at_end(self) -> None:
        """Test that single insertion at the end has distance 1."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c"], ["a", "b", "c", "d"]
        )
        self.assertEqual(distance.distance, 1)

    def test_single_insertion_at_beginning(self) -> None:
        """Test that single insertion at the beginning has distance 1."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c"], ["x", "a", "b", "c"]
        )
        self.assertEqual(distance.distance, 1)

    def test_single_insertion_in_middle(self) -> None:
        """Test that single insertion in the middle has distance 1."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c"], ["a", "x", "b", "c"]
        )
        self.assertEqual(distance.distance, 1)

    def test_single_deletion_at_end(self) -> None:
        """Test that single deletion at the end has distance 1."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c", "d"], ["a", "b", "c"]
        )
        self.assertEqual(distance.distance, 1)

    def test_single_deletion_at_beginning(self) -> None:
        """Test that single deletion at the beginning has distance 1."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["x", "a", "b", "c"], ["a", "b", "c"]
        )
        self.assertEqual(distance.distance, 1)

    def test_single_deletion_in_middle(self) -> None:
        """Test that single deletion in the middle has distance 1."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "x", "b", "c"], ["a", "b", "c"]
        )
        self.assertEqual(distance.distance, 1)

    # ========== Multiple Operations Tests ==========

    def test_two_substitutions(self) -> None:
        """Test that two substitutions have distance 2."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c", "d"], ["a", "x", "c", "y"]
        )
        self.assertEqual(distance.distance, 2)

    def test_two_insertions(self) -> None:
        """Test that two insertions have distance 2."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b"], ["a", "x", "b", "y"]
        )
        self.assertEqual(distance.distance, 2)

    def test_two_deletions(self) -> None:
        """Test that two deletions have distance 2."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "x", "b", "y"], ["a", "b"]
        )
        self.assertEqual(distance.distance, 2)

    def test_mixed_operations(self) -> None:
        """Test a sequence requiring mixed operations."""
        # Transform "kitten" to "sitting"
        # k→s (substitution), e→i (substitution), insert g
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["k", "i", "t", "t", "e", "n"],
            ["s", "i", "t", "t", "i", "n", "g"],
        )
        self.assertEqual(distance.distance, 3)

    def test_complete_replacement(self) -> None:
        """Test that complete replacement has distance equal to sequence length."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c"], ["x", "y", "z"]
        )
        self.assertEqual(distance.distance, 3)

    # ========== Different Length Sequences ==========

    def test_much_longer_second_sequence(self) -> None:
        """Test with second sequence much longer than first."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a"], ["a", "b", "c", "d", "e"]
        )
        self.assertEqual(distance.distance, 4)

    def test_much_longer_first_sequence(self) -> None:
        """Test with first sequence much longer than second."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c", "d", "e"], ["a"]
        )
        self.assertEqual(distance.distance, 4)

    # ========== String Content Tests ==========

    def test_string_case_sensitivity(self) -> None:
        """Test that algorithm treats different cases as different elements."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "B", "c"], ["a", "b", "c"]
        )
        self.assertEqual(distance.distance, 1)

    def test_string_sequences_common_prefix(self) -> None:
        """Test sequences with common prefix."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c", "d"], ["a", "b", "x", "y"]
        )
        self.assertEqual(distance.distance, 2)

    def test_string_sequences_common_suffix(self) -> None:
        """Test sequences with common suffix."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["x", "y", "c", "d"], ["a", "b", "c", "d"]
        )
        self.assertEqual(distance.distance, 2)

    # ========== Numeric Content Tests ==========

    def test_numeric_sequences(self) -> None:
        """Test with numeric sequences."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            [1, 2, 3], [1, 5, 3]
        )
        self.assertEqual(distance.distance, 1)

    def test_mixed_type_sequences(self) -> None:
        """Test with mixed type sequences."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", 1, "b"], ["a", 2, "b"]
        )
        self.assertEqual(distance.distance, 1)

    # ========== Edge Cases ==========

    def test_single_element_different(self) -> None:
        """Test single element sequences that differ."""
        distance = FuzzySetMatchingService._levenshtein_distance(["a"], ["b"])
        self.assertEqual(distance.distance, 1)

    def test_reversed_sequences(self) -> None:
        """Test that reversing a sequence gives expected distance."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c"], ["c", "b", "a"]
        )
        self.assertEqual(distance.distance, 2)

    def test_repeated_elements(self) -> None:
        """Test sequences with repeated elements."""
        distance = FuzzySetMatchingService._levenshtein_distance(
            ["a", "a", "a"], ["a", "a", "a", "a"]
        )
        self.assertEqual(distance.distance, 1)

    # ========== Symmetry Tests ==========

    def test_symmetry_property(self) -> None:
        """Test that distance(A, B) == distance(B, A)."""
        seq1 = ["a", "b", "c", "d"]
        seq2 = ["x", "b", "y", "d"]
        distance1 = FuzzySetMatchingService._levenshtein_distance(seq1, seq2)
        distance2 = FuzzySetMatchingService._levenshtein_distance(seq2, seq1)
        self.assertEqual(distance1.distance, distance2.distance)

    # ========== Triangle Inequality Tests ==========

    def test_triangle_inequality(self) -> None:
        """Test that distance satisfies triangle inequality."""
        seq_a = ["a", "b", "c"]
        seq_b = ["a", "x", "c"]
        seq_c = ["a", "x", "y"]

        dist_ac = FuzzySetMatchingService._levenshtein_distance(seq_a, seq_c)
        dist_ab = FuzzySetMatchingService._levenshtein_distance(seq_a, seq_b)
        dist_bc = FuzzySetMatchingService._levenshtein_distance(seq_b, seq_c)

        self.assertLessEqual(dist_ac.distance, dist_ab.distance + dist_bc.distance)

    # ========== Real-World Examples ==========

    def test_real_world_object_cids(self) -> None:
        """Test with realistic object CID sequences."""
        # Simulating object CIDs that might appear in the actual system
        seq1 = ["cid-001", "cid-002", "cid-003", "cid-004"]
        seq2 = ["cid-001", "cid-002", "cid-999", "cid-004"]
        distance = FuzzySetMatchingService._levenshtein_distance(seq1, seq2)
        self.assertEqual(distance.distance, 1)

    def test_real_world_missing_head_element(self) -> None:
        """Test a realistic case where the first element is missing."""
        # This simulates the scenario tested in the fuzzy matching service
        seq_with_all = ["e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8"]
        seq_missing_first = ["e2", "e3", "e4", "e5", "e6", "e7", "e8"]
        distance = FuzzySetMatchingService._levenshtein_distance(
            seq_with_all, seq_missing_first
        )
        self.assertEqual(distance.distance, 1)

    def test_real_world_offset_sequence(self) -> None:
        """Test scenario with additional elements interspersed."""
        seq1 = ["obj-1", "obj-2", "obj-3"]
        seq2 = ["obj-1", "obj-extra", "obj-2", "obj-3"]
        distance = FuzzySetMatchingService._levenshtein_distance(seq1, seq2)
        self.assertEqual(distance.distance, 1)

    # ========== Large Sequence Tests ==========

    def test_large_identical_sequences(self) -> None:
        """Test performance with large identical sequences."""
        large_seq = [f"item-{i}" for i in range(100)]
        distance = FuzzySetMatchingService._levenshtein_distance(
            large_seq, large_seq.copy()
        )
        self.assertEqual(distance.distance, 0)

    def test_large_sequences_with_single_difference(self) -> None:
        """Test large sequences with single difference in the middle."""
        seq1 = [f"item-{i}" for i in range(100)]
        seq2 = seq1.copy()
        seq2[50] = "different-item"
        distance = FuzzySetMatchingService._levenshtein_distance(seq1, seq2)
        self.assertEqual(distance.distance, 1)

    # ========== Operation Count Tests ==========

    def test_operation_counts_empty_sequences(self) -> None:
        """Test that empty sequences have zero operations."""
        result = FuzzySetMatchingService._levenshtein_distance([], [])
        self.assertEqual(result.insertions, 0)
        self.assertEqual(result.deletions, 0)
        self.assertEqual(result.substitutions, 0)
        self.assertEqual(result.distance, 0)

    def test_operation_counts_only_insertions(self) -> None:
        """Test that only insertions are counted when second sequence is longer."""
        result = FuzzySetMatchingService._levenshtein_distance(
            ["a"], ["a", "b", "c"]
        )
        self.assertEqual(result.insertions, 2)
        self.assertEqual(result.deletions, 0)
        self.assertEqual(result.substitutions, 0)
        self.assertEqual(result.distance, 2)

    def test_operation_counts_only_deletions(self) -> None:
        """Test that only deletions are counted when first sequence is longer."""
        result = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c"], ["a"]
        )
        self.assertEqual(result.insertions, 0)
        self.assertEqual(result.deletions, 2)
        self.assertEqual(result.substitutions, 0)
        self.assertEqual(result.distance, 2)

    def test_operation_counts_only_substitutions(self) -> None:
        """Test that only substitutions are counted for equal-length differences."""
        result = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c"], ["x", "y", "z"]
        )
        self.assertEqual(result.insertions, 0)
        self.assertEqual(result.deletions, 0)
        self.assertEqual(result.substitutions, 3)
        self.assertEqual(result.distance, 3)

    def test_operation_counts_mixed(self) -> None:
        """Test operation counts for a mix of operations."""
        # Transform ["a", "b", "c"] to ["a", "x", "c", "d"]
        # Keep "a", substitute "b" -> "x", keep "c", insert "d"
        result = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c"], ["a", "x", "c", "d"]
        )
        # Should have 1 substitution and 1 insertion
        self.assertEqual(result.substitutions, 1)
        self.assertEqual(result.insertions, 1)
        self.assertEqual(result.deletions, 0)
        self.assertEqual(result.distance, 2)

    def test_operation_counts_kitten_to_sitting(self) -> None:
        """Test operation counts for classic 'kitten' to 'sitting' example."""
        # k→s (sub), i→i, t→t, t→t, e→i (sub), n→n, insert g
        result = FuzzySetMatchingService._levenshtein_distance(
            ["k", "i", "t", "t", "e", "n"],
            ["s", "i", "t", "t", "i", "n", "g"],
        )
        # Expected: 2 substitutions (k→s, e→i) and 1 insertion (g)
        self.assertEqual(result.substitutions, 2)
        self.assertEqual(result.insertions, 1)
        self.assertEqual(result.deletions, 0)
        self.assertEqual(result.distance, 3)

    def test_operation_counts_sum_equals_distance(self) -> None:
        """Test that sum of operations always equals total distance."""
        test_cases = [
            ([], []),
            (["a"], ["b"]),
            (["a", "b"], ["a", "b", "c"]),
            (["a", "b", "c"], ["a"]),
            (["a", "b", "c"], ["x", "y", "z"]),
            (["a", "b", "c", "d"], ["a", "x", "c", "y"]),
        ]

        for seq1, seq2 in test_cases:
            result = FuzzySetMatchingService._levenshtein_distance(seq1, seq2)
            computed_distance = (
                result.insertions + result.deletions + result.substitutions
            )
            self.assertEqual(
                result.distance,
                computed_distance,
                f"Failed for {seq1} -> {seq2}: distance={result.distance}, "
                f"but ins+del+sub={computed_distance}"
            )

    # ========== Operations List Tests ==========

    def test_operations_both_empty(self) -> None:
        """Test that empty sequences produce an empty operations list."""
        result = FuzzySetMatchingService._levenshtein_distance([], [])
        self.assertEqual(result.operations, [])

    def test_operations_single_insertion_at_end(self) -> None:
        """Test that a single insertion at the end is recorded at the correct position."""
        result = FuzzySetMatchingService._levenshtein_distance(["a"], ["a", "b"])
        self.assertEqual(result.operations, [("I", 1)])

    def test_operations_single_insertion_at_beginning(self) -> None:
        """Test that a single insertion at the beginning is recorded at position 0."""
        result = FuzzySetMatchingService._levenshtein_distance(["b"], ["a", "b"])
        self.assertEqual(result.operations, [("I", 0)])

    def test_operations_single_deletion_at_end(self) -> None:
        """Test that a single deletion at the end is recorded at the correct position."""
        result = FuzzySetMatchingService._levenshtein_distance(["a", "b"], ["a"])
        self.assertEqual(result.operations, [("D", 1)])

    def test_operations_single_deletion_at_beginning(self) -> None:
        """Test that a single deletion at the beginning is recorded at position 0."""
        result = FuzzySetMatchingService._levenshtein_distance(["x", "b"], ["b"])
        self.assertEqual(result.operations, [("D", 0)])

    def test_operations_single_substitution(self) -> None:
        """Test that a single substitution is recorded at the correct position."""
        result = FuzzySetMatchingService._levenshtein_distance(
            ["a", "b", "c"], ["a", "x", "c"]
        )
        self.assertEqual(result.operations, [("S", 1)])

    def test_operations_only_insertions(self) -> None:
        """Test that multiple insertions from an empty first sequence record correct positions."""
        result = FuzzySetMatchingService._levenshtein_distance([], ["a", "b"])
        self.assertEqual(result.operations, [("I", 0), ("I", 1)])

    def test_operations_only_deletions(self) -> None:
        """Test that multiple deletions to an empty second sequence record correct positions."""
        result = FuzzySetMatchingService._levenshtein_distance(["a", "b"], [])
        self.assertEqual(result.operations, [("D", 0), ("D", 1)])

    def test_operations_mixed_kitten_sitting(self) -> None:
        """Test mixed operations for kitten->sitting are recorded correctly."""
        result = FuzzySetMatchingService._levenshtein_distance(
            ["k", "i", "t", "t", "e", "n"],
            ["s", "i", "t", "t", "i", "n", "g"],
        )
        self.assertEqual(result.operations, [("S", 0), ("S", 4), ("I", 6)])

    def test_operations_length_matches_distance(self) -> None:
        """Test that the number of recorded operations always equals the total distance."""
        test_cases = [
            ([], []),
            (["a"], ["b"]),
            (["a", "b"], ["a", "b", "c"]),
            (["a", "b", "c"], ["a"]),
            (["a", "b", "c"], ["x", "y", "z"]),
            (["a", "b", "c", "d"], ["a", "x", "c", "y"]),
        ]
        for seq1, seq2 in test_cases:
            result = FuzzySetMatchingService._levenshtein_distance(seq1, seq2)
            self.assertEqual(
                len(result.operations),
                result.distance,
                f"Failed for {seq1} -> {seq2}: "
                f"len(operations)={len(result.operations)}, distance={result.distance}",
            )
if __name__ == "__main__":
    unittest.main()
