"""Built-in SQL checkers using sqlglot."""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from agentic_data_contracts.core.contract import DataContract


@dataclass
class CheckResult:
    passed: bool
    severity: str  # "block" | "warn" | "log"
    message: str


class TableAllowlistChecker:
    """Checks that all referenced tables are in the contract's allowed_tables."""

    def check_sql(
        self, sql: str, contract: DataContract, dialect: str | None = None
    ) -> CheckResult:
        allowed = set(contract.allowed_table_names())
        try:
            parsed = sqlglot.parse_one(sql, dialect=dialect)
        except sqlglot.errors.ParseError as e:
            return CheckResult(
                passed=False, severity="block", message=f"SQL parse error: {e}"
            )

        referenced_tables = self._extract_tables(parsed)
        disallowed = referenced_tables - allowed
        if disallowed:
            return CheckResult(
                passed=False,
                severity="block",
                message=f"Tables not in allowlist: {', '.join(sorted(disallowed))}",
            )
        return CheckResult(passed=True, severity="block", message="")

    def _extract_tables(self, expression: exp.Expr) -> set[str]:
        tables: set[str] = set()
        for table in expression.find_all(exp.Table):
            if isinstance(table.parent, exp.CTE):
                continue
            parts = []
            if table.db:
                parts.append(table.db)
            if table.name:
                parts.append(table.name)
            full_name = ".".join(parts)
            cte_names = {cte.alias for cte in expression.find_all(exp.CTE) if cte.alias}
            if full_name and full_name not in cte_names:
                tables.add(full_name)
        return tables


class OperationBlocklistChecker:
    """Checks that the SQL statement type is not in forbidden_operations."""

    _OPERATION_MAP: dict[type[exp.Expression], str] = {
        exp.Delete: "DELETE",
        exp.Drop: "DROP",
        exp.Insert: "INSERT",
        exp.Update: "UPDATE",
    }

    def check_sql(
        self, sql: str, contract: DataContract, dialect: str | None = None
    ) -> CheckResult:
        forbidden = {op.upper() for op in contract.schema.semantic.forbidden_operations}
        try:
            parsed = sqlglot.parse_one(sql, dialect=dialect)
        except sqlglot.errors.ParseError as e:
            return CheckResult(
                passed=False, severity="block", message=f"SQL parse error: {e}"
            )

        for expr_type, op_name in self._OPERATION_MAP.items():
            if isinstance(parsed, expr_type) and op_name in forbidden:
                return CheckResult(
                    passed=False,
                    severity="block",
                    message=f"Forbidden operation: {op_name}",
                )

        if "TRUNCATE" in forbidden and isinstance(parsed, exp.Command):
            if parsed.this and str(parsed.this).upper() == "TRUNCATE":
                return CheckResult(
                    passed=False,
                    severity="block",
                    message="Forbidden operation: TRUNCATE",
                )

        return CheckResult(passed=True, severity="block", message="")


class NoSelectStarChecker:
    """Checks that no SELECT * appears anywhere in the query."""

    def check_sql(
        self, sql: str, contract: DataContract, dialect: str | None = None
    ) -> CheckResult:
        try:
            parsed = sqlglot.parse_one(sql, dialect=dialect)
        except sqlglot.errors.ParseError as e:
            return CheckResult(
                passed=False, severity="block", message=f"SQL parse error: {e}"
            )

        for star in parsed.find_all(exp.Star):
            return CheckResult(
                passed=False,
                severity="block",
                message="SELECT * is not allowed — specify explicit columns",
            )
        return CheckResult(passed=True, severity="block", message="")


class RequiredFilterChecker:
    """Checks that required WHERE filters are present in the query."""

    def __init__(self, required_filters: list[str]) -> None:
        self.required_filters = required_filters

    def check_sql(
        self, sql: str, contract: DataContract, dialect: str | None = None
    ) -> CheckResult:
        try:
            parsed = sqlglot.parse_one(sql, dialect=dialect)
        except sqlglot.errors.ParseError as e:
            return CheckResult(
                passed=False, severity="block", message=f"SQL parse error: {e}"
            )

        where_columns: set[str] = set()
        for where in parsed.find_all(exp.Where):
            for col in where.find_all(exp.Column):
                where_columns.add(col.name.lower())

        missing = [f for f in self.required_filters if f.lower() not in where_columns]
        if missing:
            return CheckResult(
                passed=False,
                severity="block",
                message=f"Missing required filters: {', '.join(missing)}",
            )
        return CheckResult(passed=True, severity="block", message="")
