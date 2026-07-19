import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.semantic.base import Decomposition, MetricDefinition
from agentic_data_contracts.validation.reconciliation import (
    ReconciliationResult,
    _apply_operator,
    _scalar,
    reconcile_decomposition,
)


@pytest.fixture
def adapter() -> DuckDBAdapter:
    return DuckDBAdapter(":memory:")


def _metric(
    operator: str, operands: list[str], name: str = "parent"
) -> MetricDefinition:
    return MetricDefinition(
        name=name,
        description="",
        sql_expression="<parent expr>",
        decompositions=[Decomposition(operator=operator, operands=operands)],
    )


class TestScalar:
    def test_returns_float(self, adapter: DuckDBAdapter) -> None:
        assert _scalar(adapter, "SELECT 5", "x") == 5.0

    def test_null_value_returns_none(self, adapter: DuckDBAdapter) -> None:
        assert _scalar(adapter, "SELECT NULL", "x") is None

    def test_empty_result_returns_none(self, adapter: DuckDBAdapter) -> None:
        assert _scalar(adapter, "SELECT 1 WHERE false", "x") is None

    def test_multi_column_raises(self, adapter: DuckDBAdapter) -> None:
        with pytest.raises(ValueError, match="exactly one column"):
            _scalar(adapter, "SELECT 1 AS a, 2 AS b", "x")

    def test_multi_row_raises(self, adapter: DuckDBAdapter) -> None:
        with pytest.raises(ValueError, match="at most one row"):
            _scalar(adapter, "SELECT * FROM (VALUES (1), (2)) AS t(x)", "x")


class TestResultType:
    def test_is_frozen(self) -> None:
        r = ReconciliationResult(
            metric="m",
            operator="sum",
            operands={"a": 1.0},
            implied_parent=1.0,
            actual_parent=1.0,
            abs_diff=0.0,
            rel_diff=0.0,
            reconciles=True,
            rel_tol=1e-4,
            abs_tol=0.0,
        )
        assert r.reason is None
        with pytest.raises(AttributeError):
            r.reconciles = False  # type: ignore


class TestApplyOperator:
    def test_sum(self) -> None:
        assert _apply_operator("sum", [1.0, 2.0, 3.0]) == 6.0

    def test_product(self) -> None:
        assert _apply_operator("product", [2.0, 3.0, 4.0]) == 24.0

    def test_ratio(self) -> None:
        assert _apply_operator("ratio", [3.0, 4.0]) == 0.75

    def test_difference(self) -> None:
        assert _apply_operator("difference", [10.0, 4.0]) == 6.0

    def test_unknown_operator_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown decomposition operator"):
            _apply_operator("power", [2.0, 3.0])


class TestReconcileHappyPath:
    def test_product_holds(self, adapter: DuckDBAdapter) -> None:
        metric = _metric("product", ["a", "b"])
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 20",
            operand_sql={"a": "SELECT 4", "b": "SELECT 5"},
            adapter=adapter,
        )
        assert result.reconciles is True
        assert result.implied_parent == 20.0
        assert result.actual_parent == 20.0
        assert result.operands == {"a": 4.0, "b": 5.0}
        assert result.reason is None

    def test_ratio_holds(self, adapter: DuckDBAdapter) -> None:
        metric = _metric("ratio", ["num", "den"])
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 0.75",
            operand_sql={"num": "SELECT 3", "den": "SELECT 4"},
            adapter=adapter,
        )
        assert result.reconciles is True

    def test_breaks_beyond_tolerance(self, adapter: DuckDBAdapter) -> None:
        metric = _metric("product", ["a", "b"])
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 21",
            operand_sql={"a": "SELECT 4", "b": "SELECT 5"},
            adapter=adapter,
        )
        assert result.reconciles is False
        assert result.implied_parent == 20.0
        assert result.actual_parent == 21.0
        assert result.reason == "identity does not hold within tolerance"

    def test_tolerance_boundary_inclusive(self, adapter: DuckDBAdapter) -> None:
        # implied 20, actual 16, abs_diff 4.0 == rel_tol(0.25)*16 -> reconciles.
        # All values are exact binary floats, so the boundary is not subject to
        # rounding (avoid decimals like 0.002 that are inexact in float).
        metric = _metric("product", ["a", "b"])
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 16",
            operand_sql={"a": "SELECT 4", "b": "SELECT 5"},
            adapter=adapter,
            rel_tol=0.25,
        )
        assert result.reconciles is True

    def test_tolerance_boundary_exclusive(self, adapter: DuckDBAdapter) -> None:
        # implied 20, actual 16, abs_diff 4.0 > rel_tol(0.125)*16 = 2.0 -> no
        metric = _metric("product", ["a", "b"])
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 16",
            operand_sql={"a": "SELECT 4", "b": "SELECT 5"},
            adapter=adapter,
            rel_tol=0.125,
        )
        assert result.reconciles is False


class TestReconcileFindings:
    def test_null_operand_is_finding(self, adapter: DuckDBAdapter) -> None:
        metric = _metric("product", ["a", "b"])
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 20",
            operand_sql={"a": "SELECT NULL", "b": "SELECT 5"},
            adapter=adapter,
        )
        assert result.reconciles is False
        assert result.reason is not None and "NULL" in result.reason
        assert "a" in result.reason

    def test_ratio_zero_denominator_is_finding(self, adapter: DuckDBAdapter) -> None:
        metric = _metric("ratio", ["num", "den"])
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 0",
            operand_sql={"num": "SELECT 3", "den": "SELECT 0"},
            adapter=adapter,
        )
        assert result.reconciles is False
        assert result.reason is not None and "denominator" in result.reason


class TestReconcilePreconditions:
    def test_operand_key_mismatch_raises(self, adapter: DuckDBAdapter) -> None:
        metric = _metric("product", ["a", "b"])
        with pytest.raises(ValueError, match="do not match the declared operands"):
            reconcile_decomposition(
                metric,
                parent_sql="SELECT 20",
                operand_sql={"a": "SELECT 4", "WRONG": "SELECT 5"},
                adapter=adapter,
            )

    def test_no_decomposition_raises(self, adapter: DuckDBAdapter) -> None:
        metric = MetricDefinition(name="leaf", description="", sql_expression="x")
        with pytest.raises(ValueError, match="no decompositions"):
            reconcile_decomposition(
                metric, parent_sql="SELECT 1", operand_sql={}, adapter=adapter
            )

    def test_index_out_of_range_raises(self, adapter: DuckDBAdapter) -> None:
        metric = _metric("product", ["a", "b"])
        with pytest.raises(ValueError, match="out of range"):
            reconcile_decomposition(
                metric,
                parent_sql="SELECT 20",
                operand_sql={"a": "SELECT 4", "b": "SELECT 5"},
                adapter=adapter,
                decomposition=1,
            )

    def test_decomposition_index_selects_second(self, adapter: DuckDBAdapter) -> None:
        metric = MetricDefinition(
            name="parent",
            description="",
            sql_expression="x",
            decompositions=[
                Decomposition(operator="product", operands=["a", "b"]),
                Decomposition(operator="sum", operands=["c", "d"]),
            ],
        )
        result = reconcile_decomposition(
            metric,
            parent_sql="SELECT 9",
            operand_sql={"c": "SELECT 4", "d": "SELECT 5"},
            adapter=adapter,
            decomposition=1,
        )
        assert result.operator == "sum"
        assert result.reconciles is True


class TestReconcilePopulationMismatch:
    def test_conversion_rate_population_mismatch(self, adapter: DuckDBAdapter) -> None:
        # Parent joins events->users, so its denominator counts only users who
        # appear in events (1,2,3). cohort_signups counts ALL cohort users
        # (1,2,3,4). The identity conversion_rate = first_purchase_users /
        # cohort_signups therefore does NOT hold: 2/3 (parent) vs 2/4 (implied).
        adapter.connection.execute(
            """
            CREATE SCHEMA analytics;
            CREATE TABLE analytics.users (id INTEGER, cohort_month VARCHAR);
            INSERT INTO analytics.users VALUES
                (1, '2026-05'), (2, '2026-05'), (3, '2026-05'), (4, '2026-05');
            CREATE TABLE analytics.events (user_id INTEGER, event_name VARCHAR);
            INSERT INTO analytics.events VALUES
                (1, 'first_purchase'), (2, 'first_purchase'), (3, 'login');
            """
        )
        metric = _metric(
            "ratio", ["first_purchase_users", "cohort_signups"], name="conversion_rate"
        )
        result = reconcile_decomposition(
            metric,
            parent_sql=(
                "SELECT COUNT(DISTINCT e.user_id) "
                "FILTER (WHERE e.event_name = 'first_purchase') "
                "/ COUNT(DISTINCT u.id)::DOUBLE "
                "FROM analytics.events e JOIN analytics.users u ON e.user_id = u.id "
                "WHERE u.cohort_month = '2026-05'"
            ),
            operand_sql={
                "first_purchase_users": (
                    "SELECT COUNT(DISTINCT e.user_id) "
                    "FILTER (WHERE e.event_name = 'first_purchase') "
                    "FROM analytics.events e JOIN analytics.users u "
                    "ON e.user_id = u.id WHERE u.cohort_month = '2026-05'"
                ),
                "cohort_signups": (
                    "SELECT COUNT(DISTINCT id) FROM analytics.users "
                    "WHERE cohort_month = '2026-05'"
                ),
            },
            adapter=adapter,
        )
        assert result.reconciles is False
        assert result.operands == {"first_purchase_users": 2.0, "cohort_signups": 4.0}
        assert result.implied_parent == 0.5
        assert abs(result.actual_parent - 2 / 3) < 1e-9
