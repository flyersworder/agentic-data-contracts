import sqlglot

from agentic_data_contracts.validation._timewindow import _relative_time_node


def test_names_a_relative_time_function():
    stmt = sqlglot.parse_one("SELECT 1 WHERE d > CURRENT_DATE - 7")
    assert _relative_time_node(stmt) is not None


def test_returns_none_for_a_pinned_window():
    stmt = sqlglot.parse_one("SELECT 1 WHERE d BETWEEN '2026-01-01' AND '2026-01-31'")
    assert _relative_time_node(stmt) is None
