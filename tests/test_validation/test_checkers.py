from pathlib import Path

import pytest
import sqlglot

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.checkers import (
    BlockedColumnsChecker,
    MaxJoinsChecker,
    NoSelectStarChecker,
    OperationBlocklistChecker,
    RequiredFilterChecker,
    RequireLimitChecker,
    TableAllowlistChecker,
    extract_tables,
)


def _parse(sql: str) -> sqlglot.exp.Expr:
    return sqlglot.parse_one(sql)


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


class TestExtractTables:
    def test_simple_select(self) -> None:
        ast = _parse("SELECT id FROM analytics.orders")
        assert extract_tables(ast) == {"analytics.orders"}

    def test_join(self) -> None:
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.id = c.id"
        )
        assert extract_tables(ast) == {"analytics.orders", "analytics.customers"}

    def test_cte_excluded(self) -> None:
        ast = _parse("WITH cte AS (SELECT id FROM analytics.orders) SELECT id FROM cte")
        assert extract_tables(ast) == {"analytics.orders"}

    def test_subquery(self) -> None:
        ast = _parse("SELECT * FROM (SELECT id FROM secret.data) t")
        assert extract_tables(ast) == {"secret.data"}


class TestTableAllowlistChecker:
    def test_allowed_table_passes(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
        result = TableAllowlistChecker().check_ast(ast, contract)
        assert result.passed

    def test_forbidden_table_blocked(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM raw.payments")
        result = TableAllowlistChecker().check_ast(ast, contract)
        assert not result.passed
        assert "raw.payments" in result.message

    def test_unknown_table_blocked(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM secret.data")
        result = TableAllowlistChecker().check_ast(ast, contract)
        assert not result.passed

    def test_subquery_tables_checked(self, contract: DataContract) -> None:
        ast = _parse("SELECT * FROM (SELECT id FROM secret.data) t")
        result = TableAllowlistChecker().check_ast(ast, contract)
        assert not result.passed

    def test_join_tables_checked(self, contract: DataContract) -> None:
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.id = c.id"
        )
        result = TableAllowlistChecker().check_ast(ast, contract)
        assert result.passed

    def test_cte_tables_checked(self, contract: DataContract) -> None:
        ast = _parse("WITH cte AS (SELECT id FROM analytics.orders) SELECT id FROM cte")
        result = TableAllowlistChecker().check_ast(ast, contract)
        assert result.passed


class TestOperationBlocklistChecker:
    def test_select_passes(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM analytics.orders")
        result = OperationBlocklistChecker().check_ast(ast, contract)
        assert result.passed

    def test_delete_blocked(self, contract: DataContract) -> None:
        ast = _parse("DELETE FROM analytics.orders WHERE id = 1")
        result = OperationBlocklistChecker().check_ast(ast, contract)
        assert not result.passed
        assert "DELETE" in result.message

    def test_drop_blocked(self, contract: DataContract) -> None:
        ast = _parse("DROP TABLE analytics.orders")
        result = OperationBlocklistChecker().check_ast(ast, contract)
        assert not result.passed

    def test_insert_blocked(self, contract: DataContract) -> None:
        ast = _parse("INSERT INTO analytics.orders (id) VALUES (1)")
        result = OperationBlocklistChecker().check_ast(ast, contract)
        assert not result.passed

    def test_update_blocked(self, contract: DataContract) -> None:
        ast = _parse("UPDATE analytics.orders SET id = 1")
        result = OperationBlocklistChecker().check_ast(ast, contract)
        assert not result.passed

    def test_truncate_blocked(self, contract: DataContract) -> None:
        ast = _parse("TRUNCATE TABLE analytics.orders")
        result = OperationBlocklistChecker().check_ast(ast, contract)
        assert not result.passed
        assert "TRUNCATE" in result.message


class TestNoSelectStarChecker:
    def test_explicit_columns_pass(self) -> None:
        ast = _parse("SELECT id, name FROM analytics.orders")
        result = NoSelectStarChecker().check_ast(ast)
        assert result.passed

    def test_select_star_blocked(self) -> None:
        ast = _parse("SELECT * FROM analytics.orders")
        result = NoSelectStarChecker().check_ast(ast)
        assert not result.passed
        assert "SELECT *" in result.message

    def test_select_star_in_subquery_blocked(self) -> None:
        ast = _parse("SELECT id FROM (SELECT * FROM analytics.orders) t")
        result = NoSelectStarChecker().check_ast(ast)
        assert not result.passed


class TestRequiredFilterChecker:
    def test_filter_present_passes(self) -> None:
        ast = _parse("SELECT id FROM analytics.orders WHERE tenant_id = 'acme'")
        result = RequiredFilterChecker("tenant_id").check_ast(ast)
        assert result.passed

    def test_filter_missing_blocked(self) -> None:
        ast = _parse("SELECT id FROM analytics.orders WHERE id = 1")
        result = RequiredFilterChecker("tenant_id").check_ast(ast)
        assert not result.passed
        assert "tenant_id" in result.message

    def test_no_where_clause_blocked(self) -> None:
        ast = _parse("SELECT id FROM analytics.orders")
        result = RequiredFilterChecker("tenant_id").check_ast(ast)
        assert not result.passed

    def test_filter_in_subquery_passes(self) -> None:
        ast = _parse(
            "SELECT id FROM (SELECT id FROM analytics.orders WHERE tenant_id = 'x') t"
        )
        result = RequiredFilterChecker("tenant_id").check_ast(ast)
        assert result.passed


class TestBlockedColumnsChecker:
    def test_safe_columns_pass(self) -> None:
        ast = _parse("SELECT id, name FROM analytics.customers")
        result = BlockedColumnsChecker(["ssn", "email"]).check_ast(ast)
        assert result.passed

    def test_blocked_column_caught(self) -> None:
        ast = _parse("SELECT id, ssn FROM analytics.customers")
        result = BlockedColumnsChecker(["ssn", "email"]).check_ast(ast)
        assert not result.passed
        assert "ssn" in result.message

    def test_blocked_column_case_insensitive(self) -> None:
        ast = _parse("SELECT id, SSN FROM analytics.customers")
        result = BlockedColumnsChecker(["ssn"]).check_ast(ast)
        assert not result.passed

    def test_select_star_caught(self) -> None:
        ast = _parse("SELECT * FROM analytics.customers")
        result = BlockedColumnsChecker(["ssn"]).check_ast(ast)
        assert not result.passed
        assert "SELECT *" in result.message


class TestRequireLimitChecker:
    def test_with_limit_passes(self) -> None:
        ast = _parse("SELECT id FROM analytics.orders LIMIT 10")
        result = RequireLimitChecker().check_ast(ast)
        assert result.passed

    def test_without_limit_blocked(self) -> None:
        ast = _parse("SELECT id FROM analytics.orders")
        result = RequireLimitChecker().check_ast(ast)
        assert not result.passed
        assert "LIMIT" in result.message


class TestMaxJoinsChecker:
    def test_within_limit_passes(self) -> None:
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.id = c.id"
        )
        result = MaxJoinsChecker(3).check_ast(ast)
        assert result.passed

    def test_exceeds_limit_blocked(self) -> None:
        ast = _parse(
            "SELECT a.id FROM t1 a"
            " JOIN t2 b ON a.id = b.id"
            " JOIN t3 c ON b.id = c.id"
            " JOIN t4 d ON c.id = d.id"
        )
        result = MaxJoinsChecker(2).check_ast(ast)
        assert not result.passed
        assert "3" in result.message
        assert "2" in result.message
