"""Built-in SQL checkers using sqlglot AST."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlglot import exp

from agentic_data_contracts.core.contract import DataContract

if TYPE_CHECKING:
    from agentic_data_contracts.semantic.base import Relationship


@dataclass
class CheckResult:
    passed: bool
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
                message=f"Tables not in allowlist: {', '.join(sorted(disallowed))}",
            )
        return CheckResult(passed=True, message="")


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
                message="Forbidden operation: TRUNCATE",
            )

        return CheckResult(passed=True, message="")


class NoSelectStarChecker:
    """Checks that no SELECT * appears anywhere in the query."""

    def check_ast(self, ast: exp.Expression) -> CheckResult:
        if any(ast.find_all(exp.Star)):
            return CheckResult(
                passed=False,
                message="SELECT * is not allowed — specify explicit columns",
            )
        return CheckResult(passed=True, message="")


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
                message=f"Missing required filter: {self.column}",
            )
        return CheckResult(passed=True, message="")


class BlockedColumnsChecker:
    """Checks that blocked columns don't appear in SELECT.

    Only checks SELECT expressions — references in WHERE/ORDER BY/GROUP BY are
    not blocked. The intent is to prevent data exposure in results, not to prevent
    all SQL references to sensitive columns.
    """

    def __init__(self, blocked: list[str]) -> None:
        self.blocked = {c.lower() for c in blocked}

    def check_ast(self, ast: exp.Expression) -> CheckResult:
        if any(ast.find_all(exp.Star)):
            return CheckResult(
                passed=False,
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
                message=f"Blocked columns in SELECT: {', '.join(sorted(found))}",
            )
        return CheckResult(passed=True, message="")


class RequireLimitChecker:
    """Checks that the query has a LIMIT clause."""

    def check_ast(self, ast: exp.Expression) -> CheckResult:
        if not list(ast.find_all(exp.Limit)):
            return CheckResult(
                passed=False,
                message="Query must include a LIMIT clause",
            )
        return CheckResult(passed=True, message="")


class MaxJoinsChecker:
    """Checks that the number of JOINs doesn't exceed a maximum."""

    def __init__(self, max_joins: int) -> None:
        self.max_joins = max_joins

    def check_ast(self, ast: exp.Expression) -> CheckResult:
        join_count = len(list(ast.find_all(exp.Join)))
        if join_count > self.max_joins:
            return CheckResult(
                passed=False,
                message=(
                    f"Query has {join_count} JOINs, exceeds maximum of {self.max_joins}"
                ),
            )
        return CheckResult(passed=True, message="")


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
                message=(
                    f"Rule '{self.rule_name}': query returned {row_count} rows, "
                    f"minimum is {self.min_rows}"
                ),
            )
        if self.max_rows is not None and row_count > self.max_rows:
            return CheckResult(
                passed=False,
                message=(
                    f"Rule '{self.rule_name}': query returned {row_count} rows, "
                    f"maximum is {self.max_rows}"
                ),
            )

        if self.column is not None:
            col_lower = {c.lower(): i for i, c in enumerate(columns)}
            idx = col_lower.get(self.column.lower())
            if idx is None:
                return CheckResult(passed=True, message="")

            values = [row[idx] for row in rows]

            if self.not_null and any(v is None for v in values):
                null_count = sum(1 for v in values if v is None)
                return CheckResult(
                    passed=False,
                    message=(
                        f"Rule '{self.rule_name}': column '{self.column}' "
                        f"contains {null_count} null values"
                    ),
                )

            numeric_values = [
                v
                for v in values
                if v is not None and isinstance(v, (int, float, Decimal))
            ]
            if numeric_values:
                if self.min_value is not None:
                    actual_min = min(numeric_values)
                    if actual_min < self.min_value:
                        return CheckResult(
                            passed=False,
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
                            message=(
                                f"Rule '{self.rule_name}': column '{self.column}' "
                                f"max value {actual_max} exceeds limit {self.max_value}"
                            ),
                        )

        return CheckResult(passed=True, message="")


class RelationshipChecker:
    """Validates SQL JOINs against declared semantic relationships.

    Produces warnings only — never blocks. Silent on undeclared joins.
    """

    def __init__(self, relationships: list[Relationship]) -> None:
        self._relationships = relationships
        self._relationship_map = self._build_map(relationships)

    @staticmethod
    def _parse_ref(ref: str) -> tuple[str, str]:
        """Parse 'schema.table.column' into (table, column), case-insensitive."""
        parts = ref.lower().split(".")
        if len(parts) == 3:
            return parts[1], parts[2]
        if len(parts) == 2:
            return parts[0], parts[1]
        return parts[0], ""

    @staticmethod
    def _build_map(
        relationships: list[Relationship],
    ) -> dict[tuple[str, str], list[Relationship]]:
        """Build bidirectional lookup: (table_a, table_b) -> [Relationship, ...]."""
        result: dict[tuple[str, str], list[Relationship]] = {}
        for rel in relationships:
            from_table, _ = RelationshipChecker._parse_ref(rel.from_)
            to_table, _ = RelationshipChecker._parse_ref(rel.to)
            key_fwd = (from_table, to_table)
            key_rev = (to_table, from_table)
            result.setdefault(key_fwd, []).append(rel)
            result.setdefault(key_rev, []).append(rel)
        return result

    @staticmethod
    def _build_alias_map(ast: exp.Expression) -> dict[str, str]:
        """Build alias -> table_name map from the AST, case-insensitive."""
        alias_map: dict[str, str] = {}
        for table in ast.find_all(exp.Table):
            bare_name = table.name.lower()
            alias_map[bare_name] = bare_name
            if table.alias:
                alias_map[table.alias.lower()] = bare_name
        return alias_map

    @staticmethod
    def _extract_join_columns(
        join_expr: exp.Join, alias_map: dict[str, str]
    ) -> list[tuple[str, str, str, str]]:
        """Extract join column pairs from a JOIN ON clause.

        Returns (left_table, left_col, right_table, right_col) tuples.
        """
        results: list[tuple[str, str, str, str]] = []
        on_clause = join_expr.args.get("on")
        if on_clause is None:
            return results
        for eq in on_clause.find_all(exp.EQ):
            left = eq.left
            right = eq.right
            if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                l_table = (
                    alias_map.get(left.table.lower(), left.table.lower())
                    if left.table
                    else ""
                )
                r_table = (
                    alias_map.get(right.table.lower(), right.table.lower())
                    if right.table
                    else ""
                )
                results.append(
                    (l_table, left.name.lower(), r_table, right.name.lower())
                )
        return results

    def check_joins(self, ast: exp.Expression) -> list[str]:
        """Check all JOINs in the AST against declared relationships.

        Returns a list of warning strings.
        """
        warnings: list[str] = []
        alias_map = self._build_alias_map(ast)

        for join in ast.find_all(exp.Join):
            join_cols = self._extract_join_columns(join, alias_map)
            for l_table, l_col, r_table, r_col in join_cols:
                if not l_table or not r_table:
                    continue
                key = (l_table, r_table)
                rels = self._relationship_map.get(key)
                if rels is None:
                    continue

                for rel in rels:
                    from_table, from_col = self._parse_ref(rel.from_)
                    to_table, to_col = self._parse_ref(rel.to)
                    correct = {l_col, r_col} == {from_col, to_col}
                    if not correct:
                        warnings.append(
                            f"Join `{l_table}` -> `{r_table}` uses columns "
                            f"`{l_col}`, `{r_col}` but declared relationship "
                            f"specifies `{from_col}` -> `{to_col}`"
                        )

        return warnings
