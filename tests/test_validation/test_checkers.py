from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.checkers import (
    NoSelectStarChecker,
    OperationBlocklistChecker,
    RequiredFilterChecker,
    TableAllowlistChecker,
)


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


class TestTableAllowlistChecker:
    def test_allowed_table_passes(self, contract: DataContract) -> None:
        result = TableAllowlistChecker().check_sql(
            "SELECT id FROM analytics.orders WHERE tenant_id = 'x'", contract
        )
        assert result.passed

    def test_forbidden_table_blocked(self, contract: DataContract) -> None:
        result = TableAllowlistChecker().check_sql(
            "SELECT id FROM raw.payments", contract
        )
        assert not result.passed
        assert "raw.payments" in result.message

    def test_unknown_table_blocked(self, contract: DataContract) -> None:
        result = TableAllowlistChecker().check_sql(
            "SELECT id FROM secret.data", contract
        )
        assert not result.passed

    def test_subquery_tables_checked(self, contract: DataContract) -> None:
        result = TableAllowlistChecker().check_sql(
            "SELECT * FROM (SELECT id FROM secret.data) t", contract
        )
        assert not result.passed

    def test_join_tables_checked(self, contract: DataContract) -> None:
        sql = (
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.id = c.id"
        )
        result = TableAllowlistChecker().check_sql(sql, contract)
        assert result.passed

    def test_cte_tables_checked(self, contract: DataContract) -> None:
        result = TableAllowlistChecker().check_sql(
            "WITH cte AS (SELECT id FROM analytics.orders) SELECT id FROM cte",
            contract,
        )
        assert result.passed

    def test_malformed_sql_blocked(self, contract: DataContract) -> None:
        result = TableAllowlistChecker().check_sql("NOT VALID SQL AT ALL !!!", contract)
        assert not result.passed
        assert "parse error" in result.message.lower() or not result.passed


class TestOperationBlocklistChecker:
    def test_select_passes(self, contract: DataContract) -> None:
        result = OperationBlocklistChecker().check_sql(
            "SELECT id FROM analytics.orders", contract
        )
        assert result.passed

    def test_delete_blocked(self, contract: DataContract) -> None:
        result = OperationBlocklistChecker().check_sql(
            "DELETE FROM analytics.orders WHERE id = 1", contract
        )
        assert not result.passed
        assert "DELETE" in result.message

    def test_drop_blocked(self, contract: DataContract) -> None:
        result = OperationBlocklistChecker().check_sql(
            "DROP TABLE analytics.orders", contract
        )
        assert not result.passed

    def test_insert_blocked(self, contract: DataContract) -> None:
        result = OperationBlocklistChecker().check_sql(
            "INSERT INTO analytics.orders (id) VALUES (1)", contract
        )
        assert not result.passed

    def test_update_blocked(self, contract: DataContract) -> None:
        result = OperationBlocklistChecker().check_sql(
            "UPDATE analytics.orders SET id = 1", contract
        )
        assert not result.passed

    def test_truncate_blocked(self, contract: DataContract) -> None:
        result = OperationBlocklistChecker().check_sql(
            "TRUNCATE TABLE analytics.orders", contract
        )
        assert not result.passed
        assert "TRUNCATE" in result.message


class TestNoSelectStarChecker:
    def test_explicit_columns_pass(self, contract: DataContract) -> None:
        result = NoSelectStarChecker().check_sql(
            "SELECT id, name FROM analytics.orders", contract
        )
        assert result.passed

    def test_select_star_blocked(self, contract: DataContract) -> None:
        result = NoSelectStarChecker().check_sql(
            "SELECT * FROM analytics.orders", contract
        )
        assert not result.passed
        assert "SELECT *" in result.message

    def test_select_star_in_subquery_blocked(self, contract: DataContract) -> None:
        result = NoSelectStarChecker().check_sql(
            "SELECT id FROM (SELECT * FROM analytics.orders) t", contract
        )
        assert not result.passed


class TestRequiredFilterChecker:
    def test_filter_present_passes(self, contract: DataContract) -> None:
        checker = RequiredFilterChecker(required_filters=["tenant_id"])
        result = checker.check_sql(
            "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'", contract
        )
        assert result.passed

    def test_filter_missing_blocked(self, contract: DataContract) -> None:
        checker = RequiredFilterChecker(required_filters=["tenant_id"])
        result = checker.check_sql(
            "SELECT id FROM analytics.orders WHERE id = 1", contract
        )
        assert not result.passed
        assert "tenant_id" in result.message

    def test_no_where_clause_blocked(self, contract: DataContract) -> None:
        checker = RequiredFilterChecker(required_filters=["tenant_id"])
        result = checker.check_sql("SELECT id FROM analytics.orders", contract)
        assert not result.passed

    def test_filter_in_subquery_passes(self, contract: DataContract) -> None:
        checker = RequiredFilterChecker(required_filters=["tenant_id"])
        result = checker.check_sql(
            "SELECT id FROM (SELECT id FROM analytics.orders WHERE tenant_id = 'x') t",
            contract,
        )
        assert result.passed
