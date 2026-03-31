"""Built-in SQL checkers using sqlglot AST."""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import exp

from agentic_data_contracts.core.contract import DataContract


@dataclass
class CheckResult:
    passed: bool
    severity: str  # "block" | "warn" | "log"
    message: str


def extract_tables(expression: exp.Expression) -> set[str]:
    """Extract fully-qualified table names from an AST, excluding CTE definitions."""
    tables: set[str] = set()
    cte_names = {cte.alias for cte in expression.find_all(exp.CTE) if cte.alias}
    for table in expression.find_all(exp.Table):
        if isinstance(table.parent, exp.CTE):
            continue
        parts = []
        if table.db:
            parts.append(table.db)
        if table.name:
            parts.append(table.name)
        full_name = ".".join(parts)
        if full_name and full_name not in cte_names:
            tables.add(full_name)
    return tables


class TableAllowlistChecker:
    """Checks that all referenced tables are in the contract's allowed_tables."""

    def check_ast(self, ast: exp.Expression, contract: DataContract) -> CheckResult:
        allowed = set(contract.allowed_table_names())
        referenced_tables = extract_tables(ast)
        disallowed = referenced_tables - allowed
        if disallowed:
            return CheckResult(
                passed=False,
                severity="block",
                message=f"Tables not in allowlist: {', '.join(sorted(disallowed))}",
            )
        return CheckResult(passed=True, severity="block", message="")


class OperationBlocklistChecker:
    """Checks that the SQL statement type is not in forbidden_operations."""

    _OPERATION_MAP: dict[type[exp.Expression], str] = {
        exp.Delete: "DELETE",
        exp.Drop: "DROP",
        exp.Insert: "INSERT",
        exp.Update: "UPDATE",
    }

    def check_ast(self, ast: exp.Expression, contract: DataContract) -> CheckResult:
        forbidden = {op.upper() for op in contract.schema.semantic.forbidden_operations}

        for expr_type, op_name in self._OPERATION_MAP.items():
            if isinstance(ast, expr_type) and op_name in forbidden:
                return CheckResult(
                    passed=False,
                    severity="block",
                    message=f"Forbidden operation: {op_name}",
                )

        if "TRUNCATE" in forbidden and (
            isinstance(ast, exp.TruncateTable)
            or (
                isinstance(ast, exp.Command)
                and ast.this
                and str(ast.this).upper() == "TRUNCATE"
            )
        ):
            return CheckResult(
                passed=False,
                severity="block",
                message="Forbidden operation: TRUNCATE",
            )

        return CheckResult(passed=True, severity="block", message="")


class NoSelectStarChecker:
    """Checks that no SELECT * appears anywhere in the query."""

    def check_ast(self, ast: exp.Expression) -> CheckResult:
        if any(ast.find_all(exp.Star)):
            return CheckResult(
                passed=False,
                severity="block",
                message="SELECT * is not allowed — specify explicit columns",
            )
        return CheckResult(passed=True, severity="block", message="")


class RequiredFilterChecker:
    """Checks that a required WHERE filter column is present."""

    def __init__(self, column: str) -> None:
        self.column = column

    def check_ast(self, ast: exp.Expression) -> CheckResult:
        where_columns: set[str] = set()
        for where in ast.find_all(exp.Where):
            for col in where.find_all(exp.Column):
                where_columns.add(col.name.lower())

        if self.column.lower() not in where_columns:
            return CheckResult(
                passed=False,
                severity="block",
                message=f"Missing required filter: {self.column}",
            )
        return CheckResult(passed=True, severity="block", message="")


class BlockedColumnsChecker:
    """Checks that blocked columns don't appear in SELECT."""

    def __init__(self, blocked: list[str]) -> None:
        self.blocked = {c.lower() for c in blocked}

    def check_ast(self, ast: exp.Expression) -> CheckResult:
        if any(ast.find_all(exp.Star)):
            return CheckResult(
                passed=False,
                severity="block",
                message=(
                    "SELECT * may expose blocked columns: "
                    f"{', '.join(sorted(self.blocked))}"
                ),
            )

        selected: set[str] = set()
        for select in ast.find_all(exp.Select):
            for expr in select.expressions:
                for col in expr.find_all(exp.Column):
                    selected.add(col.name.lower())

        found = selected & self.blocked
        if found:
            return CheckResult(
                passed=False,
                severity="block",
                message=f"Blocked columns in SELECT: {', '.join(sorted(found))}",
            )
        return CheckResult(passed=True, severity="block", message="")


class RequireLimitChecker:
    """Checks that the query has a LIMIT clause."""

    def check_ast(self, ast: exp.Expression) -> CheckResult:
        if not list(ast.find_all(exp.Limit)):
            return CheckResult(
                passed=False,
                severity="block",
                message="Query must include a LIMIT clause",
            )
        return CheckResult(passed=True, severity="block", message="")


class MaxJoinsChecker:
    """Checks that the number of JOINs doesn't exceed a maximum."""

    def __init__(self, max_joins: int) -> None:
        self.max_joins = max_joins

    def check_ast(self, ast: exp.Expression) -> CheckResult:
        join_count = len(list(ast.find_all(exp.Join)))
        if join_count > self.max_joins:
            return CheckResult(
                passed=False,
                severity="block",
                message=(
                    f"Query has {join_count} JOINs, exceeds maximum of {self.max_joins}"
                ),
            )
        return CheckResult(passed=True, severity="block", message="")


class ResultCheckRunner:
    """Runs result_check validations against query output."""

    def __init__(
        self,
        column: str | None,
        min_value: float | None,
        max_value: float | None,
        not_null: bool | None,
        min_rows: int | None,
        max_rows: int | None,
        rule_name: str,
    ) -> None:
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
        self.not_null = not_null
        self.min_rows = min_rows
        self.max_rows = max_rows
        self.rule_name = rule_name

    def check_results(self, columns: list[str], rows: list[tuple]) -> CheckResult:
        row_count = len(rows)
        if self.min_rows is not None and row_count < self.min_rows:
            return CheckResult(
                passed=False,
                severity="block",
                message=(
                    f"Rule '{self.rule_name}': query returned {row_count} rows, "
                    f"minimum is {self.min_rows}"
                ),
            )
        if self.max_rows is not None and row_count > self.max_rows:
            return CheckResult(
                passed=False,
                severity="block",
                message=(
                    f"Rule '{self.rule_name}': query returned {row_count} rows, "
                    f"maximum is {self.max_rows}"
                ),
            )

        if self.column is not None:
            col_lower = {c.lower(): i for i, c in enumerate(columns)}
            idx = col_lower.get(self.column.lower())
            if idx is None:
                return CheckResult(passed=True, severity="block", message="")

            values = [row[idx] for row in rows]

            if self.not_null and any(v is None for v in values):
                null_count = sum(1 for v in values if v is None)
                return CheckResult(
                    passed=False,
                    severity="block",
                    message=(
                        f"Rule '{self.rule_name}': column '{self.column}' "
                        f"contains {null_count} null values"
                    ),
                )

            numeric_values = [
                v for v in values if v is not None and isinstance(v, (int, float))
            ]
            if numeric_values:
                if self.min_value is not None:
                    actual_min = min(numeric_values)
                    if actual_min < self.min_value:
                        return CheckResult(
                            passed=False,
                            severity="block",
                            message=(
                                f"Rule '{self.rule_name}': column '{self.column}' "
                                f"min value {actual_min} "
                                f"is below limit {self.min_value}"
                            ),
                        )
                if self.max_value is not None:
                    actual_max = max(numeric_values)
                    if actual_max > self.max_value:
                        return CheckResult(
                            passed=False,
                            severity="block",
                            message=(
                                f"Rule '{self.rule_name}': column '{self.column}' "
                                f"max value {actual_max} exceeds limit {self.max_value}"
                            ),
                        )

        return CheckResult(passed=True, severity="block", message="")
