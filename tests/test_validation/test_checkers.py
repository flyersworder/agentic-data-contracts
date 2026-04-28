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
    RequiredFilterValuesChecker,
    RequireLimitChecker,
    TableAllowlistChecker,
    extract_tables,
)


def _parse(sql: str) -> sqlglot.exp.Expression:
    from typing import cast

    return cast(sqlglot.exp.Expression, sqlglot.parse_one(sql))


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

    def test_tautology_is_blocked(self) -> None:
        """`WHERE tenant_id = tenant_id` must not satisfy a blocking
        required_filter — it's the exact bypass governance rules exist to prevent."""
        ast = _parse("SELECT id FROM analytics.orders WHERE tenant_id = tenant_id")
        result = RequiredFilterChecker("tenant_id").check_ast(ast)
        assert not result.passed
        assert "tenant_id" in result.message

    def test_tautology_is_blocked_is_self(self) -> None:
        ast = _parse("SELECT id FROM analytics.orders WHERE tenant_id IS tenant_id")
        result = RequiredFilterChecker("tenant_id").check_ast(ast)
        assert not result.passed

    def test_non_tautological_filter_still_passes(self) -> None:
        """Regression guard: `tenant_id IS NOT NULL` is a legitimate binding."""
        ast = _parse("SELECT id FROM analytics.orders WHERE tenant_id IS NOT NULL")
        result = RequiredFilterChecker("tenant_id").check_ast(ast)
        assert result.passed


class TestRequiredFilterValuesChecker:
    """Per-principal value allowlist for a WHERE-clause column."""

    VALUES = {"partner@co.com": [123, 456], "vip@co.com": [999]}

    def test_in_list_subset_passes(self) -> None:
        ast = _parse("SELECT id FROM sales.opps WHERE account_id IN (123, 456)")
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert result.passed

    def test_in_list_with_extra_value_blocked(self) -> None:
        ast = _parse("SELECT id FROM sales.opps WHERE account_id IN (123, 999)")
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert not result.passed
        assert "999" in result.message
        assert "partner@co.com" in result.message

    def test_single_equality_match_passes(self) -> None:
        ast = _parse("SELECT id FROM sales.opps WHERE account_id = 123")
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert result.passed

    def test_single_equality_miss_blocked(self) -> None:
        ast = _parse("SELECT id FROM sales.opps WHERE account_id = 789")
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert not result.passed
        assert "789" in result.message

    def test_or_with_out_of_set_blocked(self) -> None:
        """Every OR branch must be a subset; `id=123 OR id=999` opens the door."""
        ast = _parse(
            "SELECT id FROM sales.opps WHERE account_id = 123 OR account_id = 999"
        )
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert not result.passed
        assert "999" in result.message

    def test_and_narrowing_passes(self) -> None:
        """AND adds restrictions; `account_id IN (123, 456) AND amount > 0` is fine."""
        ast = _parse(
            "SELECT id FROM sales.opps WHERE account_id IN (123, 456) AND amount > 0"
        )
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert result.passed

    def test_principal_not_in_map_passes(self) -> None:
        """Rule only applies to principals it has values for. Others fall through."""
        ast = _parse("SELECT id FROM sales.opps WHERE account_id = 7")
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="other@co.com"
        )
        assert result.passed

    def test_resolved_principal_none_passes(self) -> None:
        """No identity → rule does not apply. Use allowed_principals for hard fail."""
        ast = _parse("SELECT id FROM sales.opps WHERE account_id = 7")
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal=None
        )
        assert result.passed

    def test_subquery_in_in_blocked(self) -> None:
        """Non-literal predicate can't be statically proven inside the allowed set."""
        ast = _parse(
            "SELECT id FROM sales.opps WHERE account_id IN "
            "(SELECT account_id FROM sales.partners)"
        )
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert not result.passed
        assert "non-literal" in result.message

    def test_column_missing_blocked(self) -> None:
        ast = _parse("SELECT id FROM sales.opps WHERE id = 1")
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert not result.passed
        assert "Missing required filter" in result.message

    def test_tautology_blocked(self) -> None:
        ast = _parse("SELECT id FROM sales.opps WHERE account_id = account_id")
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert not result.passed
        assert "trivially satisfied" in result.message

    def test_string_values_pass(self) -> None:
        values = {"emea@co.com": ["EU", "UK"]}
        ast = _parse("SELECT id FROM sales.opps WHERE region IN ('EU', 'UK')")
        result = RequiredFilterValuesChecker("region", values).check_ast(
            ast, resolved_principal="emea@co.com"
        )
        assert result.passed

    def test_ignores_unknown_kwargs(self) -> None:
        """Validator may pass other kwargs in the future; checker should not crash."""
        ast = _parse("SELECT id FROM sales.opps WHERE account_id = 123")
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com", unrelated="x"
        )
        assert result.passed

    def test_self_join_alias_smuggle_blocked(self) -> None:
        """Bypass: aliased self-join with one branch pinned to a forbidden value.

        Without the literal-set guard, AND coverage intersects {123} ∩ {999}
        to ∅, which the subset check (∅ ⊆ allowed) accepts — but the user
        is constraining t2 to account 999 they don't own. The guard must
        catch any literal value referenced on the target column, regardless
        of AND/OR structure or alias.
        """
        ast = _parse(
            "SELECT t1.id FROM sales.opps t1 "
            "JOIN sales.opps t2 ON t1.id = t2.id "
            "WHERE t1.account_id = 123 AND t2.account_id = 999"
        )
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert not result.passed
        assert "999" in result.message

    def test_same_table_contradiction_blocked(self) -> None:
        """Bypass: contradictory AND constraints. account_id=123 AND account_id=999
        is runtime-impossible but must not be accepted by the validator —
        otherwise post-filter logging hooks may misreport the query."""
        ast = _parse(
            "SELECT id FROM sales.opps WHERE account_id = 123 AND account_id = 999"
        )
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert not result.passed
        assert "999" in result.message

    def test_qualified_column_match(self) -> None:
        """Regression: qualified column refs (`t.account_id`) match by base name."""
        ast = _parse("SELECT t.id FROM sales.opps t WHERE t.account_id IN (123, 456)")
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert result.passed

    def test_int_yaml_matches_decimal_sql(self) -> None:
        """YAML int 123 must match SQL literal 123.0 (and vice-versa) — the
        underlying numeric value is the same; canonical form should win."""
        ast = _parse("SELECT id FROM sales.opps WHERE account_id = 123.0")
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert result.passed

    def test_decimal_yaml_matches_int_sql(self) -> None:
        values = {"alice@co.com": [123.0, 456.0]}
        ast = _parse("SELECT id FROM sales.opps WHERE account_id IN (123, 456)")
        result = RequiredFilterValuesChecker("account_id", values).check_ast(
            ast, resolved_principal="alice@co.com"
        )
        assert result.passed

    def test_string_quotes_normalised(self) -> None:
        """SQL `'EU'` and the YAML string `EU` must compare equal."""
        values = {"emea@co.com": ["EU"]}
        ast = _parse("SELECT id FROM sales.opps WHERE region = 'EU'")
        result = RequiredFilterValuesChecker("region", values).check_ast(
            ast, resolved_principal="emea@co.com"
        )
        assert result.passed

    def test_is_not_null_with_eq_passes(self) -> None:
        """Common defensive pattern. `IS NOT NULL AND = 123` is strictly
        tighter than `= 123` alone — must not be rejected as non-literal."""
        ast = _parse(
            "SELECT id FROM sales.opps "
            "WHERE account_id IS NOT NULL AND account_id = 123"
        )
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert result.passed, result.message

    def test_is_null_alone_blocked_as_unbounded(self) -> None:
        """`IS NULL` doesn't pin the column to a literal; without a sibling
        equality predicate, the rule must block as not-constrained."""
        ast = _parse("SELECT id FROM sales.opps WHERE account_id IS NULL")
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert not result.passed
        # Should report unbounded constraint, not a non-literal predicate.
        assert "not constrained" in result.message

    def test_not_eq_uses_non_literal_message(self) -> None:
        """`NOT (account_id = 999)` is correctly blocked, but the error
        message must NOT imply the user wrote a forbidden EQ — it must
        surface the structural reason (non-literal predicate)."""
        ast = _parse("SELECT id FROM sales.opps WHERE NOT (account_id = 999)")
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert not result.passed
        assert "non-literal" in result.message
        # Anti-assertion: must NOT claim the user wrote `account_id = 999`.
        assert "Values ['999']" not in result.message

    def test_not_in_uses_non_literal_message(self) -> None:
        ast = _parse("SELECT id FROM sales.opps WHERE NOT (account_id IN (999, 1000))")
        result = RequiredFilterValuesChecker("account_id", self.VALUES).check_ast(
            ast, resolved_principal="partner@co.com"
        )
        assert not result.passed
        assert "non-literal" in result.message
        assert "Values ['999'" not in result.message


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
