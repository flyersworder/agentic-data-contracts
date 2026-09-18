"""Sensitivity checks: rewrite mechanics, result types, check_sensitivity."""

from __future__ import annotations

import pytest

from agentic_data_contracts.semantic.base import Shadow
from agentic_data_contracts.validation.sensitivity import (
    _free_alias,
    _norm,
    _Refused,
    _rewrite,
    _table_refs,
)

SHADOW = Shadow(
    table="main.payments",
    sql="SELECT * REPLACE (eur_amount * 10 AS eur_amount) FROM main.payments",
)


def _rw(sql: str) -> str:
    return _rewrite(sql, SHADOW, dialect="duckdb")


class TestLocate:
    def test_counts_a_qualified_reference(self) -> None:
        assert (
            _table_refs(
                "SELECT 1 FROM main.payments", "main.payments", dialect="duckdb"
            )
            == 1
        )

    def test_counts_a_bare_reference(self) -> None:
        assert (
            _table_refs("SELECT 1 FROM payments", "main.payments", dialect="duckdb")
            == 1
        )

    def test_ignores_another_schema(self) -> None:
        assert (
            _table_refs(
                "SELECT 1 FROM other.payments", "main.payments", dialect="duckdb"
            )
            == 0
        )

    def test_ignores_a_cte_of_the_same_name(self) -> None:
        sql = "WITH payments AS (SELECT 1 x) SELECT * FROM payments"
        with pytest.raises(_Refused, match="shadowed by a CTE"):
            _table_refs(sql, "main.payments", dialect="duckdb")

    def test_unparseable_sql_is_refused(self) -> None:
        with pytest.raises(_Refused, match="unparseable"):
            _table_refs("SELECT FROM WHERE )(", "main.payments", dialect="duckdb")


class TestRewrite:
    def test_prepends_a_with_clause(self) -> None:
        out = _rw("SELECT count(*) FROM main.payments")
        assert out.startswith("WITH __sens_0 AS (")
        assert "FROM __sens_0" in out
        assert "FROM main.payments)" in out  # inside the shadow, untouched

    def test_rewrites_a_bare_reference_and_keeps_its_alias(self) -> None:
        out = _rw("SELECT count(*) FROM payments p WHERE p.eur_amount > 10")
        assert "FROM __sens_0 p" in out

    def test_splices_into_an_existing_with(self) -> None:
        sql = "WITH v AS (SELECT 1 FROM payments) SELECT * FROM v"
        out = _rw(sql)
        assert out.startswith("WITH __sens_0 AS (")
        assert "v AS (SELECT 1 FROM __sens_0)" in out

    def test_splices_after_with_recursive(self) -> None:
        sql = (
            "WITH RECURSIVE r(n) AS (SELECT 1) "
            "SELECT (SELECT count(*) FROM payments) FROM r"
        )
        out = _rw(sql)
        assert out.startswith("WITH RECURSIVE __sens_0 AS (")
        assert "FROM __sens_0) FROM r" in out

    def test_rewrites_a_reference_inside_a_correlated_subquery(self) -> None:
        sql = (
            "SELECT (SELECT count(*) FROM payments x WHERE x.merchant=f.merchant) "
            "FROM main.fees f"
        )
        out = _rw(sql)
        assert "FROM __sens_0 x" in out

    def test_leaves_a_string_literal_alone(self) -> None:
        out = _rw("SELECT 'payments' AS label, count(*) FROM main.payments")
        assert "'payments' AS label" in out
        assert "FROM __sens_0" in out

    def test_leaves_a_column_alias_alone(self) -> None:
        # The count guard's false-positive class, measured on the DABStep
        # corpus: four stored queries write this shape and were refused.
        sql = "SELECT count(DISTINCT psp_reference) AS payments FROM main.payments"
        out = _rw(sql)
        assert "AS payments" in out
        assert "FROM __sens_0" in out

    def test_not_applicable_when_the_target_is_absent(self) -> None:
        with pytest.raises(_Refused, match="not_applicable"):
            _rw("SELECT * FROM main.fees")

    def test_alias_collision_picks_the_next_free_name(self) -> None:
        assert _free_alias("SELECT 1") == "__sens_0"
        assert _free_alias("SELECT __sens_0 FROM t") == "__sens_1"
        assert _free_alias("SELECT __sens_0, __sens_1 FROM t") == "__sens_2"


class TestNormalise:
    def test_orders_and_rounds(self) -> None:
        assert _norm([(2, 1.000000001), (1, 2.0)]) == _norm([(1, 2.0), (2, 1.0)])

    def test_distinguishes_different_values(self) -> None:
        assert _norm([(1,)]) != _norm([(2,)])
