"""Tests for comparing a query result against a certified breakdown answer."""

from __future__ import annotations

from decimal import Decimal

import pytest

from agentic_data_contracts.validation._rows import (
    compare_rows,
    key_positions,
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
