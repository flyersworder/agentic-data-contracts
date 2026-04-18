"""Built-in SQL checkers using sqlglot AST."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

import sqlglot
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
    def _resolve_join_table(join_expr: exp.Join, alias_map: dict[str, str]) -> str:
        """Resolve the table being joined (the JOIN's 'this' arg) to a bare name."""
        table_node = join_expr.this
        if isinstance(table_node, exp.Table):
            bare = table_node.name.lower()
            return alias_map.get(bare, bare)
        return ""

    @staticmethod
    def _extract_join_columns(
        join_expr: exp.Join, alias_map: dict[str, str]
    ) -> list[tuple[str, str, str, str]]:
        """Extract join column pairs from a JOIN's ON or USING clause.

        Returns (left_table, left_col, right_table, right_col) tuples.
        For USING, both sides share the same column name; we pair the
        FROM table with the joined table.
        """
        results: list[tuple[str, str, str, str]] = []

        # Handle ON clause
        on_clause = join_expr.args.get("on")
        if on_clause is not None:
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

        # Handle USING clause — USING(col) means both sides share the same
        # column name, but we don't know which table is the "left" side.
        # Generate a candidate pair for every other table in the query and
        # let check_joins match against the relationship map.
        using_clause = join_expr.args.get("using")
        if using_clause is not None:
            joined_table = RelationshipChecker._resolve_join_table(join_expr, alias_map)
            other_tables = sorted({t for t in alias_map.values() if t != joined_table})
            for ident in using_clause:
                col_name = ident.name.lower()
                for candidate in other_tables:
                    results.append((candidate, col_name, joined_table, col_name))

        return results

    def check_joins(self, ast: exp.Expression) -> list[str]:
        """Check all JOINs in the AST against declared relationships.

        Returns a list of warning strings.
        """
        warnings: list[str] = []
        alias_map = self._build_alias_map(ast)
        matched_rels: list[Relationship] = []

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
                    else:
                        matched_rels.append(rel)

        # Check required_filter for matched relationships
        warnings.extend(self._check_required_filters(ast, matched_rels))

        # Check fan-out risk for one_to_many matched relationships
        warnings.extend(self._check_fan_out(ast, matched_rels))

        return warnings

    _AGG_TYPES: tuple[type[exp.Expression], ...] = (
        exp.Sum,
        exp.Avg,
        exp.Count,
        exp.Min,
        exp.Max,
    )

    @staticmethod
    def _has_aggregation(ast: exp.Expression) -> bool:
        """Check if the top-level SELECT contains any aggregation functions.

        Ignores aggregations inside subqueries to avoid false positives.
        """
        for select in ast.find_all(exp.Select):
            if select.parent_select is not None:
                continue
            # Check only the SELECT's own expressions; skip aggregations
            # that live inside scalar subqueries (e.g. SELECT (SELECT AVG(...)...))
            for expr in select.expressions:
                for agg in expr.find_all(*RelationshipChecker._AGG_TYPES):
                    if not agg.find_ancestor(exp.Subquery):
                        return True
        return False

    @staticmethod
    def _check_fan_out(
        ast: exp.Expression, matched_rels: list[Relationship]
    ) -> list[str]:
        """Warn if query aggregates across a one_to_many join."""
        if not RelationshipChecker._has_aggregation(ast):
            return []

        warnings: list[str] = []
        seen: set[tuple[str, str]] = set()
        for rel in matched_rels:
            if rel.type != "one_to_many":
                continue
            from_table, _ = RelationshipChecker._parse_ref(rel.from_)
            to_table, _ = RelationshipChecker._parse_ref(rel.to)
            pair = (from_table, to_table)
            if pair in seen:
                continue
            seen.add(pair)
            warnings.append(
                f"Query aggregates across a one_to_many join "
                f"(`{from_table}` -> `{to_table}`). "
                f"Results may be inflated by row multiplication."
            )
        return warnings

    @staticmethod
    def _extract_where_columns(ast: exp.Expression) -> set[str]:
        """Extract all column names referenced in WHERE clauses."""
        columns: set[str] = set()
        for where in ast.find_all(exp.Where):
            for col in where.find_all(exp.Column):
                columns.add(col.name.lower())
        return columns

    @staticmethod
    def _extract_filter_columns(required_filter: str) -> set[str]:
        """Extract column names from a required_filter expression string."""
        try:
            parsed = sqlglot.parse_one(f"SELECT 1 WHERE {required_filter}")
            columns: set[str] = set()
            for where in parsed.find_all(exp.Where):
                for col in where.find_all(exp.Column):
                    columns.add(col.name.lower())
            return columns
        except sqlglot.errors.ParseError:
            import re

            return {
                w.lower()
                for w in re.findall(r"[a-zA-Z_]\w*", required_filter)
                if w.upper()
                not in (
                    "AND",
                    "OR",
                    "NOT",
                    "NULL",
                    "IS",
                    "IN",
                    "LIKE",
                    "BETWEEN",
                    "TRUE",
                    "FALSE",
                )
            }

    _BINARY_COMPARISONS: tuple[type[exp.Expression], ...] = (
        exp.EQ,
        exp.NEQ,
        exp.LT,
        exp.LTE,
        exp.GT,
        exp.GTE,
        exp.Like,
        exp.ILike,
    )

    @staticmethod
    def _extract_bound_columns(ast: exp.Expression) -> set[str]:
        """Return columns that appear in at least one non-tautological predicate.

        A column is "bound" if it appears on one side of a comparison, IN,
        BETWEEN, or IS (NOT) NULL expression where the other side does not
        reference the same column. Catches `WHERE tenant_id = tenant_id`
        style bypasses of required_filter column-presence checks.
        """
        bound: set[str] = set()
        for where in ast.find_all(exp.Where):
            for node in where.find_all(*RelationshipChecker._BINARY_COMPARISONS):
                if not isinstance(node, exp.Binary):
                    continue
                left_cols = {c.name.lower() for c in node.left.find_all(exp.Column)}
                right_cols = {c.name.lower() for c in node.right.find_all(exp.Column)}
                bound |= left_cols - right_cols
                bound |= right_cols - left_cols
            for in_node in where.find_all(exp.In):
                this = in_node.this
                if not isinstance(this, exp.Column):
                    continue
                col_name = this.name.lower()
                other_cols: set[str] = set()
                for expr in in_node.expressions:
                    other_cols |= {c.name.lower() for c in expr.find_all(exp.Column)}
                query = in_node.args.get("query")
                if query is not None:
                    other_cols |= {c.name.lower() for c in query.find_all(exp.Column)}
                if col_name not in other_cols:
                    bound.add(col_name)
            for between in where.find_all(exp.Between):
                this = between.this
                if not isinstance(this, exp.Column):
                    continue
                col_name = this.name.lower()
                other_cols = set()
                for key in ("low", "high"):
                    expr = between.args.get(key)
                    if expr is not None:
                        other_cols |= {
                            c.name.lower() for c in expr.find_all(exp.Column)
                        }
                if col_name not in other_cols:
                    bound.add(col_name)
            for is_node in where.find_all(exp.Is):
                this = is_node.this
                if not isinstance(this, exp.Column):
                    continue
                col_name = this.name.lower()
                other = is_node.expression
                other_cols = (
                    {c.name.lower() for c in other.find_all(exp.Column)}
                    if other is not None
                    else set()
                )
                if col_name not in other_cols:
                    bound.add(col_name)
        return bound

    @staticmethod
    def _check_required_filters(
        ast: exp.Expression, matched_rels: list[Relationship]
    ) -> list[str]:
        """Warn if matched relationships have required_filter but column is
        missing or appears only in trivially-true predicates."""
        warnings: list[str] = []
        where_columns = RelationshipChecker._extract_where_columns(ast)
        bound_columns = RelationshipChecker._extract_bound_columns(ast)
        seen: set[int] = set()

        for rel in matched_rels:
            rel_id = id(rel)
            if rel_id in seen:
                continue
            seen.add(rel_id)
            if rel.required_filter is None:
                continue
            filter_columns = RelationshipChecker._extract_filter_columns(
                rel.required_filter
            )
            missing = filter_columns - where_columns
            unbound = (filter_columns & where_columns) - bound_columns
            from_table, _ = RelationshipChecker._parse_ref(rel.from_)
            to_table, _ = RelationshipChecker._parse_ref(rel.to)
            if missing:
                warnings.append(
                    f"Join `{from_table}` -> `{to_table}` has required filter "
                    f"`{rel.required_filter}` but query does not filter on: "
                    f"{', '.join(sorted(missing))}"
                )
            if unbound:
                warnings.append(
                    f"Join `{from_table}` -> `{to_table}` has required filter "
                    f"`{rel.required_filter}` but predicate on "
                    f"{', '.join(sorted(unbound))} is trivially satisfied "
                    f"(e.g. `col = col`); add a non-trivial condition"
                )

        return warnings
