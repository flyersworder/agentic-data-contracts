import logging
from pathlib import Path

import pytest
import sqlglot

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.validation.checkers import (
    ENFORCEABLE_OPERATIONS,
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
from agentic_data_contracts.validation.validator import Validator


def _parse(sql: str) -> sqlglot.exp.Expression:
    from typing import cast

    return cast(sqlglot.exp.Expression, sqlglot.parse_one(sql))


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


def _contract_forbidding(operations: list[str]) -> DataContract:
    return DataContract(
        schema=DataContractSchema(
            version="1.0",
            name="blocklist-test",
            semantic=SemanticConfig(
                allowed_tables=[
                    AllowedTable.model_validate(
                        {"schema": "analytics", "tables": ["orders", "customers", "t"]}
                    )
                ],
                forbidden_operations=operations,
                rules=[],
            ),
        )
    )


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

    def test_qualified_star_blocked(self) -> None:
        """`t.*` is a star projection wearing a prefix."""
        ast = _parse("SELECT o.* FROM analytics.orders o")
        result = NoSelectStarChecker().check_ast(ast)
        assert not result.passed

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT COUNT(*) FROM analytics.orders",
            "SELECT region, COUNT(*) FROM analytics.orders GROUP BY region",
            "SELECT COUNT(*) OVER () FROM analytics.orders",
            "SELECT COUNT(*) FILTER (WHERE status = 'x') FROM analytics.orders",
        ],
    )
    def test_count_star_is_not_select_star(self, sql: str) -> None:
        """`COUNT(*)` projects no columns at all — it is the opposite of the
        over-broad read this checker exists to stop.

        It used to be rejected, because `find_all(exp.Star)` matches any Star
        anywhere in the tree and `COUNT(*)` parses as `Count(this=Star())`.
        Measured cost in the DABStep sweep: 86 false rejections across 124 of
        401 tasks, each one telling the agent "SELECT * is not allowed" about
        a query containing no SELECT *. An agent cannot comply with a
        diagnosis that is not true of its query, so it burned turns guessing.
        """
        result = NoSelectStarChecker().check_ast(_parse(sql))
        assert result.passed, result.message

    def test_count_star_over_a_starred_subquery_is_still_blocked(self) -> None:
        """The inner `SELECT *` is the violation; the outer COUNT(*) is not.
        Allowing COUNT(*) must not create a wrapper that smuggles one in."""
        ast = _parse("SELECT COUNT(*) FROM (SELECT * FROM analytics.orders) t")
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


class TestOperationBlocklistCoverage:
    """Operations beyond the original DELETE/DROP/INSERT/UPDATE/TRUNCATE set.

    Before this was fixed, `forbidden_operations: [CREATE]` parsed fine, stored
    fine, and enforced nothing — a contract could declare a write forbidden and
    silently permit it, which is the exact "confident wrong answer" failure this
    library exists to prevent.
    """

    # (operation name, a statement of that kind)
    CASES = [
        ("CREATE", "CREATE TABLE analytics.t AS SELECT 1 AS x"),
        ("ALTER", "ALTER TABLE analytics.orders ADD COLUMN c INT"),
        (
            "MERGE",
            "MERGE INTO analytics.orders t USING analytics.customers s"
            " ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.id = s.id",
        ),
        ("GRANT", "GRANT SELECT ON analytics.orders TO analyst"),
        ("REVOKE", "REVOKE SELECT ON analytics.orders FROM analyst"),
        ("COPY", "COPY analytics.orders FROM 'data.csv'"),
    ]

    @pytest.mark.parametrize(("operation", "sql"), CASES)
    def test_blocked_when_forbidden(self, operation: str, sql: str) -> None:
        contract = _contract_forbidding([operation])
        result = OperationBlocklistChecker().check_ast(_parse(sql), contract)
        assert not result.passed
        assert operation in result.message

    @pytest.mark.parametrize(("operation", "sql"), CASES)
    def test_allowed_when_not_forbidden(self, operation: str, sql: str) -> None:
        # The more dangerous regression: over-blocking a statement the contract
        # never forbade would break working agents.
        contract = _contract_forbidding(["DELETE"])
        result = OperationBlocklistChecker().check_ast(_parse(sql), contract)
        assert result.passed, f"{operation} blocked by a contract that never forbade it"

    def test_select_still_passes_with_everything_forbidden(self) -> None:
        contract = _contract_forbidding(sorted(ENFORCEABLE_OPERATIONS))
        result = OperationBlocklistChecker().check_ast(
            _parse("SELECT id FROM analytics.orders"), contract
        )
        assert result.passed


class TestEnforceableOperations:
    def test_derived_from_the_map(self) -> None:
        # Derived, never hand-maintained: a second list would drift from the
        # map, which is the failure mode being fixed.
        # Exact shape, not a subset: `<=` is unconditionally true given the
        # `frozenset(map.values()) | {...}` definition, so it would only catch a
        # hand-retyped replacement — which is the very drift being guarded.
        assert ENFORCEABLE_OPERATIONS == set(
            OperationBlocklistChecker._OPERATION_MAP.values()
        ) | {"TRUNCATE"}

    def test_unenforceable_operation_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        contract = _contract_forbidding(["DELETE", "CALL", "VACUUM"])
        with caplog.at_level(logging.WARNING):
            Validator(contract)
        assert "NOT enforced" in caplog.text
        # Asserted on the rendered unenforceable list rather than bare
        # substrings: the message also names the enforceable operations (to be
        # actionable), so DELETE legitimately appears elsewhere in the text.
        assert "['CALL', 'VACUUM']" in caplog.text

    def test_no_warning_when_all_enforceable(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        contract = _contract_forbidding(sorted(ENFORCEABLE_OPERATIONS))
        with caplog.at_level(logging.WARNING):
            Validator(contract)
        assert "NOT enforced" not in caplog.text

    def test_each_distinct_mistake_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Uncached and stateless: two contracts with different mistakes each
        # report their own, and no global state can swallow the second.
        with caplog.at_level(logging.WARNING):
            Validator(_contract_forbidding(["FIRSTOP"]))
            Validator(_contract_forbidding(["SECONDOP"]))
        assert "FIRSTOP" in caplog.text
        assert "SECONDOP" in caplog.text

    def test_warning_survives_a_later_logging_reconfiguration(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # The reason this is not memoised: a consumer that builds contracts at
        # import time and configures logging in main() must still see the
        # warning on the next Validator, not have it cached away forever.
        contract = _contract_forbidding(["NOSUCHOP"])
        Validator(contract)  # first build
        # caplog's handler is attached for the whole test, so the first build's
        # record is already captured — without this clear() the assertion below
        # passes even if the second build emits nothing, which is exactly what
        # a memoised warning would do.
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            Validator(contract)
        assert "NOSUCHOP" in caplog.text

    def test_every_enforceable_operation_is_actually_blockable(self) -> None:
        # The set exists to answer "what can a contract forbid?", and the
        # warning message advertises it to users. A name in the set with no
        # working map entry would make that answer a lie -- the same
        # declared-but-unenforced shape the set was added to surface.
        statements = {
            "DELETE": "DELETE FROM analytics.orders WHERE id = 1",
            "DROP": "DROP TABLE analytics.orders",
            "INSERT": "INSERT INTO analytics.orders (id) VALUES (1)",
            "UPDATE": "UPDATE analytics.orders SET id = 1",
            "TRUNCATE": "TRUNCATE TABLE analytics.orders",
            "CREATE": "CREATE TABLE analytics.t AS SELECT 1 AS x",
            "ALTER": "ALTER TABLE analytics.orders ADD COLUMN c INT",
            "MERGE": (
                "MERGE INTO analytics.orders t USING analytics.customers s"
                " ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.id = s.id"
            ),
            "GRANT": "GRANT SELECT ON analytics.orders TO analyst",
            "REVOKE": "REVOKE SELECT ON analytics.orders FROM analyst",
            "COPY": "COPY analytics.orders FROM 'data.csv'",
        }
        assert set(statements) == set(ENFORCEABLE_OPERATIONS), (
            "add a statement here when extending ENFORCEABLE_OPERATIONS"
        )
        for operation, sql in statements.items():
            result = OperationBlocklistChecker().check_ast(
                _parse(sql), _contract_forbidding([operation])
            )
            assert not result.passed, f"{operation} is in the set but not blockable"
            assert operation in result.message
