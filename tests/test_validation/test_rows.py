"""Tests for comparing a query result against a certified breakdown answer."""

from __future__ import annotations

from decimal import Decimal

import pytest

from agentic_data_contracts.validation._rows import (
    compare_rows,
    key_positions,
    named_differences,
)

_EXPECTED = [["EMEA", 5000.0], ["APAC", 3000.0], ["AMER", 2700.0]]
_COLUMNS = ["region", "revenue"]


def _run(expected, rows, *, columns=None, ordered=False, rel_tol=1e-9, abs_tol=0.0):
    return compare_rows(
        expected,
        columns or _COLUMNS,
        rows,
        ordered=ordered,
        rel_tol=rel_tol,
        abs_tol=abs_tol,
    )


class TestKeyPositions:
    def test_non_numeric_cells_are_the_key(self) -> None:
        assert key_positions(_EXPECTED) == (0,)

    def test_several_dimensions_all_join_the_key(self) -> None:
        rows = [["EMEA", "2025-Q1", 5000.0]]
        assert key_positions(rows) == (0, 1)

    def test_a_row_with_no_measure_is_all_key(self) -> None:
        assert key_positions([["EMEA"], ["APAC"]]) == (0,)


class TestUnorderedComparison:
    def test_same_groups_in_a_different_order_match(self) -> None:
        result = _run(_EXPECTED, [("AMER", 2700.0), ("EMEA", 5000.0), ("APAC", 3000.0)])
        assert result.matched
        assert result.differences == []
        assert result.actual_row_count == 3

    def test_a_dropped_group_is_named(self) -> None:
        result = _run(_EXPECTED, [("EMEA", 5000.0), ("AMER", 2700.0)])
        assert not result.matched
        assert any("missing" in d and "APAC" in d for d in result.differences)

    def test_an_extra_group_is_named(self) -> None:
        result = _run(
            _EXPECTED,
            [("EMEA", 5000.0), ("APAC", 3000.0), ("AMER", 2700.0), ("LATAM", 400.0)],
        )
        assert not result.matched
        assert any("unexpected" in d and "LATAM" in d for d in result.differences)

    def test_the_right_total_with_the_wrong_split_is_a_mismatch(self) -> None:
        # 10700 either way -- the failure this feature exists to catch.
        result = _run(_EXPECTED, [("EMEA", 5200.0), ("APAC", 2800.0), ("AMER", 2700.0)])
        assert not result.matched
        assert any("EMEA" in d for d in result.differences)
        assert any("APAC" in d for d in result.differences)

    def test_a_value_within_tolerance_matches(self) -> None:
        result = _run(
            _EXPECTED,
            [("EMEA", 5000.4), ("APAC", 3000.0), ("AMER", 2700.0)],
            abs_tol=0.5,
        )
        assert result.matched

    def test_a_decimal_measure_compares_as_a_number(self) -> None:
        result = _run(
            _EXPECTED,
            [
                ("EMEA", Decimal("5000.00")),
                ("APAC", Decimal("3000.00")),
                ("AMER", Decimal("2700.00")),
            ],
        )
        assert result.matched

    def test_a_numeric_key_renders_to_text_before_matching(self) -> None:
        expected = [["2025", 5000.0]]
        result = _run(expected, [(2025, 5000.0)], columns=["year", "revenue"])
        assert result.matched

    def test_a_key_only_answer_asserts_the_groups_alone(self) -> None:
        result = compare_rows(
            [["EMEA"], ["APAC"]],
            ["region"],
            [("APAC",), ("EMEA",)],
            ordered=False,
            rel_tol=1e-9,
            abs_tol=0.0,
        )
        assert result.matched

    def test_counts_are_reported(self) -> None:
        result = _run(_EXPECTED, [("EMEA", 5000.0)])
        assert result.expected_group_count == 3
        assert result.actual_row_count == 1


class TestNullCells:
    def test_null_matches_a_certified_null(self) -> None:
        result = _run([["EMEA", None]], [("EMEA", None)])
        assert result.matched

    def test_null_where_a_number_was_certified_is_a_mismatch_not_an_error(self) -> None:
        result = _run([["EMEA", 5000.0]], [("EMEA", None)])
        assert not result.matched
        assert any("EMEA" in d for d in result.differences)


class TestRefusals:
    def test_a_column_count_mismatch_names_both_counts(self) -> None:
        with pytest.raises(ValueError, match="2 column.*returned 3"):
            compare_rows(
                _EXPECTED,
                ["region", "revenue", "orders"],
                [("EMEA", 5000.0, 12)],
                ordered=False,
                rel_tol=1e-9,
                abs_tol=0.0,
            )

    def test_a_duplicated_group_in_the_result_refuses(self) -> None:
        with pytest.raises(ValueError, match="two rows for group EMEA"):
            _run(_EXPECTED, [("EMEA", 5000.0), ("EMEA", 1.0)])

    def test_a_non_numeric_measure_names_the_column_and_the_value(self) -> None:
        with pytest.raises(ValueError, match="'revenue'.*'N/A'"):
            _run([["EMEA", 5000.0]], [("EMEA", "N/A")])


class TestOrderedComparison:
    def test_the_declared_order_is_the_answer(self) -> None:
        result = _run(
            _EXPECTED,
            [("EMEA", 5000.0), ("APAC", 3000.0), ("AMER", 2700.0)],
            ordered=True,
        )
        assert result.matched

    def test_a_reordering_fails_under_ordered(self) -> None:
        result = _run(
            _EXPECTED,
            [("APAC", 3000.0), ("EMEA", 5000.0), ("AMER", 2700.0)],
            ordered=True,
        )
        assert not result.matched

    def test_a_row_count_difference_is_reported_as_a_count(self) -> None:
        # Position is identity under `ordered`, so "APAC is missing" is a claim
        # the comparison cannot support -- only "a row is missing".
        result = _run(_EXPECTED, [("EMEA", 5000.0), ("APAC", 3000.0)], ordered=True)
        assert not result.matched
        assert any("expected 3 row" in d for d in result.differences)
        assert not any("missing group" in d for d in result.differences)

    def test_a_repeated_key_is_legitimate_under_ordered(self) -> None:
        expected = [["EMEA", 5000.0], ["EMEA", 3000.0]]
        result = _run(expected, [("EMEA", 5000.0), ("EMEA", 3000.0)], ordered=True)
        assert result.matched

    def test_a_wrong_number_names_its_row(self) -> None:
        result = _run(
            _EXPECTED,
            [("EMEA", 9999.0), ("APAC", 3000.0), ("AMER", 2700.0)],
            ordered=True,
        )
        assert any("row 1" in d for d in result.differences)


class TestNullMeasurements:
    """A group whose measurement is NULL -- an ordinary LEFT JOIN breakdown."""

    def test_a_null_measurement_is_a_value_not_a_key(self) -> None:
        # `None` is not a label: a row cannot identify itself by a cell that
        # holds nothing, and treating it as a key silently makes the group
        # named `(EMEA, None)` rather than `EMEA`.
        assert key_positions([["EMEA", None]]) == (0,)

    def test_a_null_measurement_in_one_group_only_is_comparable(self) -> None:
        expected = [["EMEA", 5000.0], ["LATAM", None]]
        result = _run(expected, [("EMEA", 5000.0), ("LATAM", None)])
        assert result.matched

    def test_a_certified_null_does_not_match_the_string_none(self) -> None:
        result = _run([["EMEA", None]], [("EMEA", "None")])
        assert not result.matched

    def test_a_certified_null_is_a_value_difference_against_a_number(self) -> None:
        result = _run([["EMEA", None]], [("EMEA", 0)])
        assert not result.matched
        assert not any("missing group" in d for d in result.differences)
        assert any("EMEA" in d and "0" in d for d in result.differences)


class TestDifferenceOrdering:
    def test_value_mismatches_are_reported_before_group_differences(self) -> None:
        # `summary()` names only the first few differences, and the line it
        # renders already carries the group and row counts -- so a group-set
        # difference is signalled even when unnamed, while a wrong number has
        # no other signal and must not be crowded out of the named slice.
        result = _run(
            [["EMEA", 5000.0], ["APAC", 3000.0]],
            [("EMEA", 9999.0), ("LATAM", 1.0)],
        )
        assert not result.matched
        assert "EMEA" in result.differences[0]
        assert "9999" in result.differences[0]


class TestNamedDifferences:
    """The capped rendering both report summaries share."""

    def test_under_the_cap_names_every_difference(self) -> None:
        assert named_differences(["a", "b", "c"]) == "a; b; c"

    def test_over_the_cap_counts_the_rest(self) -> None:
        assert named_differences(["a", "b", "c", "d"]) == "a; b; c (and 1 more)"

    def test_the_remainder_counts_all_of_them(self) -> None:
        assert named_differences([str(i) for i in range(10)]).endswith("(and 7 more)")

    def test_no_differences_renders_empty(self) -> None:
        assert named_differences([]) == ""
