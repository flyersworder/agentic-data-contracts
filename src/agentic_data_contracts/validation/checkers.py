"""Built-in SQL checkers using sqlglot AST."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

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


def extract_where_columns(ast: exp.Expression) -> set[str]:
    """Extract all column names referenced in WHERE clauses (lower-cased)."""
    columns: set[str] = set()
    for where in ast.find_all(exp.Where):
        for col in where.find_all(exp.Column):
            columns.add(col.name.lower())
    return columns


def extract_bound_columns(ast: exp.Expression) -> set[str]:
    """Return columns that appear in at least one non-tautological predicate.

    A column is "bound" if it appears on one side of a comparison, IN,
    BETWEEN, or IS (NOT) NULL expression where the other side does not
    reference the same column. Catches `WHERE tenant_id = tenant_id`
    style bypasses of column-presence-only filter checks.
    """
    bound: set[str] = set()
    for where in ast.find_all(exp.Where):
        for node in where.find_all(*_BINARY_COMPARISONS):
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
                    other_cols |= {c.name.lower() for c in expr.find_all(exp.Column)}
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


class TableAllowlistChecker:
    """Checks referenced tables against the contract's allowlist, filtered by caller.

    The principal_resolver is called at check time (not construction), so a
    single long-lived checker can serve different callers across sequential
    queries (e.g. a Webex bot with one user per message).
    """

    def __init__(
        self,
        principal_resolver: Callable[[], str | None] | None = None,
    ) -> None:
        self._resolve = principal_resolver or (lambda: None)

    def check_ast(self, ast: exp.Expression, contract: DataContract) -> CheckResult:
        principal = self._resolve()
        allowed = contract.allowed_table_names_for(principal)
        declared = set(contract.allowed_table_names())
        referenced = extract_tables(ast)

        undeclared = referenced - declared
        restricted = (referenced - allowed) - undeclared

        if not undeclared and not restricted:
            return CheckResult(passed=True, message="")

        parts: list[str] = []
        if undeclared:
            parts.append(f"Tables not in allowlist: {', '.join(sorted(undeclared))}")
        if restricted:
            who = principal if principal else "<no caller identified>"
            parts.append(
                f"Tables restricted to other principals "
                f"(caller: {who!r}): {', '.join(sorted(restricted))}"
            )
        return CheckResult(passed=False, message="; ".join(parts))


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

    def check_ast(self, ast: exp.Expression, **_: object) -> CheckResult:
        if any(ast.find_all(exp.Star)):
            return CheckResult(
                passed=False,
                message="SELECT * is not allowed — specify explicit columns",
            )
        return CheckResult(passed=True, message="")


class RequiredFilterChecker:
    """Checks that a required WHERE filter column is present and non-trivial.

    Rejects both absence (``tenant_id`` not in WHERE at all) and
    trivially-satisfied predicates (``WHERE tenant_id = tenant_id``,
    ``WHERE tenant_id IS tenant_id``, etc.) — the latter would otherwise
    bypass a blocking governance rule.
    """

    def __init__(self, column: str) -> None:
        self.column = column

    def check_ast(self, ast: exp.Expression, **_: object) -> CheckResult:
        needle = self.column.lower()
        if needle not in extract_where_columns(ast):
            return CheckResult(
                passed=False,
                message=f"Missing required filter: {self.column}",
            )
        if needle not in extract_bound_columns(ast):
            return CheckResult(
                passed=False,
                message=(
                    f"Required filter on {self.column} is trivially satisfied "
                    f"(e.g. `col = col`); add a non-trivial condition"
                ),
            )
        return CheckResult(passed=True, message="")


class _Coverage:
    """Result of analysing a boolean sub-expression for column-value coverage.

    Three states:
    - ``BOUND`` with ``values``: this node restricts ``column`` to exactly
      these literal values.
    - ``UNBOUND``: this node does not restrict ``column`` (predicate on a
      different column, or a comparison kind we cannot statically reason about).
    - ``VIOLATED`` with ``bad``: this node restricts ``column`` but uses a
      non-literal expression (subquery, function, BETWEEN) or contains
      literals that are outside the allowlist.
    """

    __slots__ = ("state", "values", "bad")

    BOUND = "bound"
    UNBOUND = "unbound"
    VIOLATED = "violated"

    def __init__(
        self,
        state: str,
        values: frozenset[str] = frozenset(),
        bad: frozenset[str] = frozenset(),
    ) -> None:
        self.state = state
        self.values = values
        self.bad = bad

    @classmethod
    def bound(cls, values: set[str] | frozenset[str]) -> _Coverage:
        return cls(cls.BOUND, frozenset(values))

    @classmethod
    def unbound(cls) -> _Coverage:
        return cls(cls.UNBOUND)

    @classmethod
    def violated(cls, bad: set[str] | frozenset[str] = frozenset()) -> _Coverage:
        return cls(cls.VIOLATED, bad=frozenset(bad))


_NON_LITERAL_MARKER = "<non-literal>"
_TAUTOLOGY_MARKER = "<tautology>"


def _canon(v: object) -> str:
    """Canonicalise a literal for set comparison.

    Numeric values collapse to their integer form when integer-valued
    (``123`` and ``123.0`` both → ``"123"``); other floats use ``repr`` so
    representable values round-trip stably. Non-numeric values fall back to
    ``str``. This is intentionally lossy for floats whose decimal form
    isn't the same string as the YAML source — exact-match policy lists
    should use integers or strings, not floats with fractional parts.
    """
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, (int, float)):
        try:
            f = float(v)
        except (TypeError, ValueError):
            return str(v)
        if f.is_integer():
            return str(int(f))
        return repr(f)
    s = str(v)
    try:
        f = float(s)
    except ValueError:
        return s
    if f.is_integer():
        return str(int(f))
    return repr(f)


def _literal_value(node: Any) -> str | None:
    """Return the canonicalised literal under ``node`` or ``None`` if non-literal.

    Strips quotes from string literals so ``'EU'`` and ``"EU"`` both compare
    against allowlist entry ``"EU"``. Numeric literals canonicalise via
    ``_canon`` so ``123`` and ``123.0`` collapse to the same key.
    """
    if isinstance(node, exp.Literal):
        return _canon(node.this) if node.is_number else str(node.this)
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal):
        return _canon(f"-{node.this.this}")
    if isinstance(node, exp.Boolean):
        return str(node.this).lower()
    return None


class RequiredFilterValuesChecker:
    """Per-principal allowlist for the literal values of a WHERE-clause column.

    Walks the WHERE expression as a boolean tree. AND nodes narrow (any branch
    binding the column suffices); OR nodes widen (every branch must bind the
    column to a subset of allowed values). Non-literal predicates on the
    target column (subqueries, function calls, BETWEEN) cannot be proven and
    are rejected.

    Principals absent from the value map fall through (rule does not apply),
    matching the `principal_scope` skip semantics in `Validator`. Pair with
    `allowed_principals` on the rule for a hard deny on unknown callers.
    """

    def __init__(
        self,
        column: str,
        values_by_principal: Mapping[str, Sequence[object]],
    ) -> None:
        self.column = column.lower()
        self.values_by_principal: dict[str, frozenset[str]] = {
            p: frozenset(_canon(v) for v in vs) for p, vs in values_by_principal.items()
        }

    def check_ast(
        self,
        ast: exp.Expression,
        resolved_principal: str | None = None,
        **_: object,
    ) -> CheckResult:
        if (
            resolved_principal is None
            or resolved_principal not in self.values_by_principal
        ):
            return CheckResult(passed=True, message="")

        allowed = self.values_by_principal[resolved_principal]

        if self.column not in extract_where_columns(ast):
            return CheckResult(
                passed=False,
                message=f"Missing required filter: {self.column}",
            )

        # Literal-set guard: every literal value referenced against the target
        # column anywhere in the query must be in the allowlist, regardless of
        # AND/OR structure. Catches cross-alias smuggling (e.g. `t1.account_id
        # = 123 AND t2.account_id = 999`) and contradictions, both of which
        # `_combine_and` would otherwise intersect down to ∅ and accept.
        all_literals, has_non_literal_op = self._collect_column_literals(ast)
        if has_non_literal_op:
            return CheckResult(
                passed=False,
                message=(
                    f"Filter on {self.column} contains a non-literal predicate "
                    f"(subquery, function, BETWEEN, or non-equality comparison); "
                    f"cannot prove it is within the allowed values"
                ),
            )
        smuggled = all_literals - allowed
        if smuggled:
            return CheckResult(
                passed=False,
                message=(
                    f"Values {sorted(smuggled)} for {self.column} not allowed "
                    f"for principal {resolved_principal!r}; allowed: "
                    f"{sorted(allowed)}"
                ),
            )

        # Combine coverage across every WHERE clause in the AST. Pass requires
        # at least one BOUND coverage with all values in the allowlist; any
        # VIOLATED coverage fails fast.
        any_bound_in_allow = False
        for where in ast.find_all(exp.Where):
            cov = self._analyse(where.this)
            if cov.state == _Coverage.VIOLATED:
                return self._violated_result(cov, allowed, resolved_principal)
            if cov.state == _Coverage.BOUND:
                extra = cov.values - allowed
                if extra:
                    return self._violated_result(
                        _Coverage.violated(bad=extra), allowed, resolved_principal
                    )
                any_bound_in_allow = True

        if not any_bound_in_allow:
            return CheckResult(
                passed=False,
                message=(
                    f"Filter on {self.column} is not constrained to a literal "
                    f"value set; cannot prove it is within the allowed values "
                    f"for principal {resolved_principal!r}"
                ),
            )
        return CheckResult(passed=True, message="")

    def _violated_result(
        self,
        cov: _Coverage,
        allowed: frozenset[str],
        principal: str,
    ) -> CheckResult:
        if _TAUTOLOGY_MARKER in cov.bad:
            return CheckResult(
                passed=False,
                message=(
                    f"Required filter on {self.column} is trivially satisfied "
                    f"(e.g. `col = col`); add a non-trivial condition"
                ),
            )
        if _NON_LITERAL_MARKER in cov.bad:
            return CheckResult(
                passed=False,
                message=(
                    f"Filter on {self.column} contains a non-literal predicate "
                    f"(subquery, function, BETWEEN, or non-equality comparison); "
                    f"cannot prove it is within the allowed values"
                ),
            )
        # Both marker branches above already returned, so anything left in
        # cov.bad is a real literal value not in the allowlist.
        bad_display = sorted(cov.bad)
        return CheckResult(
            passed=False,
            message=(
                f"Values {bad_display} for {self.column} not allowed for "
                f"principal {principal!r}; allowed: {sorted(allowed)}"
            ),
        )

    def _collect_column_literals(self, ast: Any) -> tuple[frozenset[str], bool]:
        """Walk the whole AST for predicates pinning the target column.

        Independent of AND/OR structure: any literal value compared to the
        column via EQ or IN-list is collected. Non-literal predicates on the
        column (subqueries, function calls, BETWEEN, range comparisons,
        column-on-both-sides, NOT-wrapped EQ/IN) flip the second return
        value, which the caller translates into the standard non-literal
        block message.

        Tautologies (`col = col`) are caught structurally by `_analyse_eq`
        which flips to VIOLATED with `_TAUTOLOGY_MARKER`; this guard only
        flags predicate kinds whose values we cannot prove safe.

        NOT-wrapped predicates invert membership (NOT (col = 999) selects
        rows where the column is NOT 999), so the literal under the NOT is
        not a smuggled value but a structural non-literal — the caller's
        message wording must reflect that.
        """
        literals: set[str] = set()
        has_non_literal = False

        for eq in ast.find_all(exp.EQ):
            left_match = self._column_matches(eq.left)
            right_match = self._column_matches(eq.right)
            if left_match and right_match:
                continue  # tautology — handled in _analyse_eq
            if not (left_match or right_match):
                continue
            if eq.find_ancestor(exp.Not) is not None:
                has_non_literal = True
                continue
            val_side = eq.right if left_match else eq.left
            lit = _literal_value(val_side)
            if lit is None:
                has_non_literal = True
            else:
                literals.add(lit)

        for in_node in ast.find_all(exp.In):
            if not self._column_matches(in_node.this):
                continue
            if in_node.find_ancestor(exp.Not) is not None:
                has_non_literal = True
                continue
            if in_node.args.get("query") is not None:
                has_non_literal = True
                continue
            for expr in in_node.expressions:
                lit = _literal_value(expr)
                if lit is None:
                    has_non_literal = True
                else:
                    literals.add(lit)

        for between in ast.find_all(exp.Between):
            if self._column_matches(between.this):
                has_non_literal = True

        # Range/inequality comparisons on the column can't be proven inside a
        # discrete allowlist either.
        for cmp_type in (exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE):
            for node in ast.find_all(cmp_type):
                if self._column_matches(node.left) or self._column_matches(node.right):
                    has_non_literal = True

        return frozenset(literals), has_non_literal

    def _analyse(self, node: Any) -> _Coverage:
        if node is None:
            return _Coverage.unbound()
        if isinstance(node, exp.Paren):
            return self._analyse(node.this)
        if isinstance(node, exp.And):
            return self._combine_and(
                self._analyse(node.left), self._analyse(node.right)
            )
        if isinstance(node, exp.Or):
            return self._combine_or(self._analyse(node.left), self._analyse(node.right))
        if isinstance(node, exp.Is):
            # IS NULL on the target column is a presence predicate, not a
            # value pin. It doesn't smuggle a forbidden value, so don't
            # poison sibling branches via a VIOLATED state — return UNBOUND
            # so an `IS NOT NULL AND col = 123` AND can still BIND.
            return _Coverage.unbound()
        if isinstance(node, exp.Not):
            inner = node.this
            if isinstance(inner, exp.Paren):
                inner = inner.this
            if isinstance(inner, exp.Is):
                # NOT(IS NULL) — also a presence predicate, see Is branch above.
                return _Coverage.unbound()
            # NOT(EQ) / NOT(IN) / etc. on the target column inverts membership;
            # cannot statically prove the result is inside the allowlist. Note
            # we deliberately don't recurse through nested NOTs (e.g.
            # NOT(NOT(IS NULL))): each unhandled wrapper falls to this
            # conservative non-literal path. Fail-closed beats over-clever
            # double-negation reasoning that an attacker could exploit.
            return self._unbound_unless_touches(node)
        if isinstance(node, exp.EQ):
            return self._analyse_eq(node)
        if isinstance(node, exp.In):
            return self._analyse_in(node)
        if isinstance(node, exp.Between):
            return self._analyse_between(node)
        # Other comparisons (NEQ, LT, LTE, GT, GTE, LIKE) on the column
        # don't pin it to a discrete value set.
        return self._unbound_unless_touches(node)

    def _column_matches(self, node: Any) -> bool:
        return isinstance(node, exp.Column) and node.name.lower() == self.column

    def _node_touches_column(self, node: Any) -> bool:
        return any(
            self._column_matches(c) for c in node.find_all(exp.Column, exp.Identifier)
        )

    def _unbound_unless_touches(self, node: Any) -> _Coverage:
        # If the node touches the target column via a comparison kind we can't
        # statically reason about (>, <, NOT, LIKE, etc.), reject — the user
        # asked us to confine the column to a discrete allowlist.
        for col in node.find_all(exp.Column):
            if col.name.lower() == self.column:
                return _Coverage.violated(bad={_NON_LITERAL_MARKER})
        return _Coverage.unbound()

    def _analyse_eq(self, node: Any) -> _Coverage:
        left_matches = self._column_matches(node.left)
        right_matches = self._column_matches(node.right)
        if left_matches and right_matches:
            return _Coverage.violated(bad={_TAUTOLOGY_MARKER})
        for col_side, val_side in ((node.left, node.right), (node.right, node.left)):
            if self._column_matches(col_side):
                lit = _literal_value(val_side)
                if lit is None:
                    return _Coverage.violated(bad={_NON_LITERAL_MARKER})
                return _Coverage.bound({lit})
        return _Coverage.unbound()

    def _analyse_in(self, node: Any) -> _Coverage:
        if not self._column_matches(node.this):
            return _Coverage.unbound()
        if node.args.get("query") is not None:
            return _Coverage.violated(bad={_NON_LITERAL_MARKER})
        values: set[str] = set()
        for expr in node.expressions:
            lit = _literal_value(expr)
            if lit is None:
                return _Coverage.violated(bad={_NON_LITERAL_MARKER})
            values.add(lit)
        return _Coverage.bound(values)

    def _analyse_between(self, node: Any) -> _Coverage:
        if not self._column_matches(node.this):
            return _Coverage.unbound()
        # BETWEEN spans a range; we can't enumerate it against a discrete allowlist.
        return _Coverage.violated(bad={_NON_LITERAL_MARKER})

    def _combine_and(self, a: _Coverage, b: _Coverage) -> _Coverage:
        # Any VIOLATED branch fails the AND.
        if a.state == _Coverage.VIOLATED:
            return a
        if b.state == _Coverage.VIOLATED:
            return b
        # AND narrows: if either branch BOUND the column, the conjunction
        # restricts the column to that branch's values. If both BOUND, the
        # effective constraint is the intersection.
        if a.state == _Coverage.BOUND and b.state == _Coverage.BOUND:
            return _Coverage.bound(a.values & b.values)
        if a.state == _Coverage.BOUND:
            return a
        if b.state == _Coverage.BOUND:
            return b
        return _Coverage.unbound()

    def _combine_or(self, a: _Coverage, b: _Coverage) -> _Coverage:
        # Any VIOLATED branch fails the OR.
        if a.state == _Coverage.VIOLATED:
            return a
        if b.state == _Coverage.VIOLATED:
            return b
        # OR widens: an UNBOUND branch lets the column take any value, so the
        # OR is UNBOUND overall (effectively a bypass).
        if a.state == _Coverage.UNBOUND or b.state == _Coverage.UNBOUND:
            return _Coverage.unbound()
        return _Coverage.bound(a.values | b.values)


class BlockedColumnsChecker:
    """Checks that blocked columns don't appear in SELECT.

    Only checks SELECT expressions — references in WHERE/ORDER BY/GROUP BY are
    not blocked. The intent is to prevent data exposure in results, not to prevent
    all SQL references to sensitive columns.
    """

    def __init__(self, blocked: list[str]) -> None:
        self.blocked = {c.lower() for c in blocked}

    def check_ast(self, ast: exp.Expression, **_: object) -> CheckResult:
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

    def check_ast(self, ast: exp.Expression, **_: object) -> CheckResult:
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

    def check_ast(self, ast: exp.Expression, **_: object) -> CheckResult:
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

    @staticmethod
    def _check_required_filters(
        ast: exp.Expression, matched_rels: list[Relationship]
    ) -> list[str]:
        """Warn if matched relationships have required_filter but column is
        missing or appears only in trivially-true predicates."""
        warnings: list[str] = []
        where_columns = extract_where_columns(ast)
        bound_columns = extract_bound_columns(ast)
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
