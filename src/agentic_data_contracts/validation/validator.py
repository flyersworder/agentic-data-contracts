"""Validator — orchestrates checkers and aggregates results."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Protocol

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.checkers import (
    CheckResult,
    NoSelectStarChecker,
    OperationBlocklistChecker,
    RequiredFilterChecker,
    TableAllowlistChecker,
)
from agentic_data_contracts.validation.explain import ExplainAdapter


class Checker(Protocol):
    def check_sql(
        self, sql: str, contract: DataContract, dialect: str | None = None
    ) -> CheckResult: ...


@dataclass
class ValidationResult:
    blocked: bool
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    log_messages: list[str] = field(default_factory=list)


class Validator:
    """Runs all applicable checkers against a SQL query."""

    def __init__(
        self,
        contract: DataContract,
        dialect: str | None = None,
        explain_adapter: ExplainAdapter | None = None,
    ) -> None:
        self.contract = contract
        self.dialect = dialect
        self.explain_adapter = explain_adapter
        self._checkers = self._build_checkers()

    def _build_checkers(self) -> list[tuple[str, Checker]]:
        checkers: list[tuple[str, Checker]] = []
        semantic = self.contract.schema.semantic

        if semantic.allowed_tables:
            checkers.append(("block", TableAllowlistChecker()))

        if semantic.forbidden_operations:
            checkers.append(("block", OperationBlocklistChecker()))

        # Build required filters from block rules that mention filter patterns
        required_filters: list[str] = []
        for rule in self.contract.block_rules():
            if rule.filter_column:
                required_filters.append(rule.filter_column)
            else:
                name_lower = rule.name.lower()
                if "isolation" in name_lower or "filter" in name_lower:
                    col = self._extract_filter_column(rule.description)
                    if col:
                        required_filters.append(col)

        if required_filters:
            checkers.append(
                ("block", RequiredFilterChecker(required_filters=required_filters))
            )

        # Check if no_select_star rule exists
        for rule in self.contract.schema.semantic.rules:
            if (
                "select_star" in rule.name.lower()
                or "select *" in rule.description.lower()
            ):
                checkers.append((rule.enforcement.value, NoSelectStarChecker()))
                break

        return checkers

    def _extract_filter_column(self, description: str) -> str | None:
        """Extract column name from rule description like 'must filter by tenant_id'."""
        patterns = [
            r"filter\s+(?:by\s+)?(\w+)",
            r"WHERE\s+(\w+)\s*=",
            r"include\s+(?:a\s+)?(?:WHERE\s+)?(\w+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def validate(self, sql: str) -> ValidationResult:
        reasons: list[str] = []
        warnings: list[str] = []
        log_messages: list[str] = []

        for severity, checker in self._checkers:
            result: CheckResult = checker.check_sql(sql, self.contract, self.dialect)
            if not result.passed:
                if severity == "block":
                    reasons.append(result.message)
                elif severity == "warn":
                    warnings.append(result.message)
                else:
                    log_messages.append(result.message)

        # Layer 2: EXPLAIN checks (only when Layer 1 passes and adapter is provided)
        if not reasons and self.explain_adapter is not None:
            explain_result = self.explain_adapter.explain(sql)
            if not explain_result.schema_valid:
                reasons.append(
                    f"Schema validation failed: {', '.join(explain_result.errors)}"
                )
            else:
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
        )
