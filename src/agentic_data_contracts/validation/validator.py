"""Validator — orchestrates checkers and aggregates results."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, cast

import sqlglot
from sqlglot import exp

from agentic_data_contracts.adapters._normalizer import SqlNormalizer
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.checkers import (
    BlockedColumnsChecker,
    CheckResult,
    MaxJoinsChecker,
    NoSelectStarChecker,
    OperationBlocklistChecker,
    RequiredFilterChecker,
    RequireLimitChecker,
    ResultCheckRunner,
    TableAllowlistChecker,
    extract_tables,
)
from agentic_data_contracts.validation.explain import ExplainAdapter


class Checker(Protocol):
    """Protocol for SQL AST checkers.

    Two usage patterns:
    - Structural checkers (TableAllowlist, OperationBlocklist) take (ast, contract)
    - Rule-based checkers (RequiredFilter, NoSelectStar, etc.) take (ast) only
    The Validator calls them through separate paths; *args bridges both.
    """

    def check_ast(self, ast: exp.Expression, *args: Any) -> CheckResult: ...


@dataclass
class ValidationResult:
    blocked: bool
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    log_messages: list[str] = field(default_factory=list)
    estimated_cost_usd: float | None = None


class Validator:
    """Runs all applicable checkers against a SQL query."""

    def __init__(
        self,
        contract: DataContract,
        dialect: str | None = None,
        explain_adapter: ExplainAdapter | None = None,
        sql_normalizer: SqlNormalizer | None = None,
    ) -> None:
        self.contract = contract
        self.dialect = dialect
        self.explain_adapter = explain_adapter
        self.sql_normalizer = sql_normalizer
        self._build_checkers()

    def _build_checkers(self) -> None:
        semantic = self.contract.schema.semantic

        self._table_checker = (
            TableAllowlistChecker() if semantic.allowed_tables else None
        )
        self._operation_checker = (
            OperationBlocklistChecker() if semantic.forbidden_operations else None
        )

        self._query_checkers: list[tuple[str, str | None, Any]] = []
        self._result_checkers: list[tuple[str, str | None, ResultCheckRunner]] = []

        for rule in semantic.rules:
            table_scope = rule.table if rule.table and rule.table != "*" else None

            if rule.query_check is not None:
                qc = rule.query_check
                if qc.required_filter is not None:
                    self._query_checkers.append(
                        (
                            rule.enforcement.value,
                            table_scope,
                            RequiredFilterChecker(qc.required_filter),
                        )
                    )
                if qc.no_select_star is True:
                    self._query_checkers.append(
                        (
                            rule.enforcement.value,
                            table_scope,
                            NoSelectStarChecker(),
                        )
                    )
                if qc.blocked_columns is not None:
                    self._query_checkers.append(
                        (
                            rule.enforcement.value,
                            table_scope,
                            BlockedColumnsChecker(qc.blocked_columns),
                        )
                    )
                if qc.require_limit is True:
                    self._query_checkers.append(
                        (
                            rule.enforcement.value,
                            table_scope,
                            RequireLimitChecker(),
                        )
                    )
                if qc.max_joins is not None:
                    self._query_checkers.append(
                        (
                            rule.enforcement.value,
                            table_scope,
                            MaxJoinsChecker(qc.max_joins),
                        )
                    )

            elif rule.result_check is not None:
                runner = ResultCheckRunner(
                    column=rule.result_check.column,
                    min_value=rule.result_check.min_value,
                    max_value=rule.result_check.max_value,
                    not_null=rule.result_check.not_null,
                    min_rows=rule.result_check.min_rows,
                    max_rows=rule.result_check.max_rows,
                    rule_name=rule.name,
                )
                self._result_checkers.append(
                    (
                        rule.enforcement.value,
                        table_scope,
                        runner,
                    )
                )

    def pending_result_check_names(self) -> list[str]:
        """Return names of result checks that will run post-execution."""
        return [runner.rule_name for _, _, runner in self._result_checkers]

    def _is_table_in_scope(
        self, table_scope: str | None, referenced_tables: set[str]
    ) -> bool:
        if table_scope is None:
            return True
        return table_scope in referenced_tables

    def validate(self, sql: str) -> ValidationResult:
        reasons: list[str] = []
        warnings: list[str] = []
        log_messages: list[str] = []
        estimated_cost_usd: float | None = None

        try:
            normalized = (
                self.sql_normalizer.normalize_sql(sql) if self.sql_normalizer else sql
            )
            ast = cast(
                exp.Expression, sqlglot.parse_one(normalized, dialect=self.dialect)
            )
        except sqlglot.errors.ParseError as e:
            return ValidationResult(blocked=True, reasons=[f"SQL parse error: {e}"])

        referenced_tables = extract_tables(ast)

        if self._table_checker is not None:
            result = self._table_checker.check_ast(ast, self.contract)
            if not result.passed:
                reasons.append(result.message)

        if self._operation_checker is not None:
            result = self._operation_checker.check_ast(ast, self.contract)
            if not result.passed:
                reasons.append(result.message)

        for enforcement, table_scope, checker in self._query_checkers:
            if not self._is_table_in_scope(table_scope, referenced_tables):
                continue
            result = checker.check_ast(ast)
            if not result.passed:
                if enforcement == "block":
                    reasons.append(result.message)
                elif enforcement == "warn":
                    warnings.append(result.message)
                else:
                    log_messages.append(result.message)

        if not reasons and self.explain_adapter is not None:
            explain_result = self.explain_adapter.explain(sql)
            if not explain_result.schema_valid:
                reasons.append(
                    f"Schema validation failed: {', '.join(explain_result.errors)}"
                )
            else:
                estimated_cost_usd = explain_result.estimated_cost_usd
                res = self.contract.schema.resources
                if res:
                    if (
                        res.cost_limit_usd is not None
                        and explain_result.estimated_cost_usd is not None
                        and explain_result.estimated_cost_usd > res.cost_limit_usd
                    ):
                        cost = explain_result.estimated_cost_usd
                        limit = res.cost_limit_usd
                        reasons.append(
                            f"Estimated cost ${cost:.2f} exceeds limit ${limit:.2f}"
                        )
                    if (
                        res.max_rows_scanned is not None
                        and explain_result.estimated_rows is not None
                        and explain_result.estimated_rows > res.max_rows_scanned
                    ):
                        rows = explain_result.estimated_rows
                        max_rows = res.max_rows_scanned
                        reasons.append(
                            f"Estimated rows {rows:,} exceeds limit {max_rows:,}"
                        )

        return ValidationResult(
            blocked=len(reasons) > 0,
            reasons=reasons,
            warnings=warnings,
            log_messages=log_messages,
            estimated_cost_usd=estimated_cost_usd,
        )

    def validate_results(
        self, sql: str, columns: list[str], rows: list[tuple]
    ) -> ValidationResult:
        reasons: list[str] = []
        warnings: list[str] = []
        log_messages: list[str] = []

        try:
            normalized = (
                self.sql_normalizer.normalize_sql(sql) if self.sql_normalizer else sql
            )
            ast = cast(
                exp.Expression, sqlglot.parse_one(normalized, dialect=self.dialect)
            )
        except sqlglot.errors.ParseError:
            referenced_tables: set[str] = set()
        else:
            referenced_tables = extract_tables(ast)

        for enforcement, table_scope, runner in self._result_checkers:
            if not self._is_table_in_scope(table_scope, referenced_tables):
                continue
            result = runner.check_results(columns, rows)
            if not result.passed:
                if enforcement == "block":
                    reasons.append(result.message)
                elif enforcement == "warn":
                    warnings.append(result.message)
                else:
                    log_messages.append(result.message)

        return ValidationResult(
            blocked=len(reasons) > 0,
            reasons=reasons,
            warnings=warnings,
            log_messages=log_messages,
        )
