import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.validation.reconciliation import (
    ReconciliationResult,
    _scalar,
)


@pytest.fixture
def adapter() -> DuckDBAdapter:
    return DuckDBAdapter(":memory:")


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
