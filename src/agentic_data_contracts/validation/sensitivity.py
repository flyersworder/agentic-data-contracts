"""Does the caller's query DERIVE what the contract says it depends on?

The two-layer validator sees policy (allowed tables, forbidden operations,
required filters) and plannability (a live EXPLAIN). Neither sees a query that
is authorized, parseable, plannable -- and computes the wrong thing. The
DABStep eval established that this class of defect cannot be found by reading
the query text: detectors for the absence of a clause and for the presence of a
wrong construction are both null across four models.

This module reads BEHAVIOUR instead. It replaces one table with a
contract-authored SELECT, re-runs the caller's own query, and asks whether the
answer MOVED. It never computes an answer, so it cannot say a query is right --
only that it responds, or fails to respond, to an input the contract says it
depends on.

COST. Two base executions per call plus one per property: the base result is
computed once and cached, so a metric with four properties costs six
executions, not twelve. That is why this belongs at a promotion gate rather
than in an agent's hot path.

NO SQL IS REGENERATED. sqlglot decides *what* to rewrite and guards the edit;
the string executed is the caller's own text with spans replaced. That is what
makes this work on a dialect sqlglot can parse but not emit (Denodo/VQL), and
it is the same template-assembly discipline the rest of the library follows.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import sqlglot
from sqlglot import exp

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.base import MetricDefinition, Shadow
from agentic_data_contracts.validation.validator import Validator

if TYPE_CHECKING:
    from agentic_data_contracts.adapters.base import DatabaseAdapter

#: Only an identifier in one of these positions names a table. Without this, a
#: COLUMN ALIAS sharing the table's name (``COUNT(DISTINCT psp_reference) AS
#: payments``, which four stored DABStep queries write) is counted as a
#: reference, the span count exceeds the table-node count, and the guard
#: refuses a query it could have checked. ``unchecked`` fails ``report.ok``, so
#: that is a spurious CI failure rather than a harmless conservatism.
_TABLE_POS = frozenset(
    {
        sqlglot.TokenType.FROM,
        sqlglot.TokenType.JOIN,
        sqlglot.TokenType.COMMA,
        sqlglot.TokenType.L_PAREN,
    }
)
_IDENT = frozenset({sqlglot.TokenType.VAR, sqlglot.TokenType.IDENTIFIER})
_ALIAS_STEM = "__sens_"


class _Refused(Exception):
    """The mechanical reason no verdict could be rendered for one property."""


def _cte_aliases(tree: exp.Expression) -> set[str]:
    return {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}


def _table_refs(sql: str, target: str, *, dialect: str | None) -> int:
    """How many real references to *target* the query holds.

    ``find_all(exp.Table)`` minus the CTE aliases: expression nodes carry no
    source positions, but this correctly separates a base table from a
    reference to a CTE of the same name.
    """
    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
    except Exception as e:  # noqa: BLE001 - any parse failure is one outcome
        raise _Refused(f"unparseable: {e}") from e
    if parsed is None:
        raise _Refused("unparseable: empty statement")
    # sqlglot.parse_one is annotated to return the broader `Expr` base (as of
    # the resolved sqlglot; the pinned floor still returns `Expression`
    # directly). Every real parse produces an `exp.Expression` subtype, so
    # this narrows the static type to match runtime reality.
    tree = cast(exp.Expression, parsed)
    ctes = _cte_aliases(tree)
    db, _, name = target.rpartition(".")
    if name.lower() in ctes:
        raise _Refused(f"target {target!r} is shadowed by a CTE of the same name")
    count = 0
    for node in tree.find_all(exp.Table):
        if node.name.lower() != name.lower():
            continue
        if node.db and node.db.lower() != db.lower():
            continue
        count += 1
    return count


def _in_table_position(toks: list, i: int) -> bool:
    return i > 0 and toks[i - 1].token_type in _TABLE_POS


def _spans(sql: str, target: str, *, dialect: str | None) -> list[tuple[int, int]]:
    """Character spans of *target*, via tokens -- which do carry positions.

    Matches both the qualified spelling and the bare one, as alternating
    identifier/DOT runs. A name inside a string literal or a comment is a
    different token type and is never a candidate.
    """
    toks = list(sqlglot.tokenize(sql, dialect=dialect))
    db, _, name = target.rpartition(".")
    out: list[tuple[int, int]] = []
    for i, tok in enumerate(toks):
        if tok.token_type not in _IDENT:
            continue
        if tok.text.strip('"').lower() != name.lower():
            continue
        prev = toks[i - 1] if i else None
        if prev is not None and prev.token_type == sqlglot.TokenType.DOT:
            prev2 = toks[i - 2] if i > 1 else None
            if prev2 is None or prev2.text.strip('"').lower() != db.lower():
                continue  # qualified by some other schema
            if not _in_table_position(toks, i - 2):
                continue
            out.append((prev2.start, tok.end + 1))
        else:
            if not _in_table_position(toks, i):
                continue
            out.append((tok.start, tok.end + 1))
    return out


def _free_alias(sql: str) -> str:
    """The first ``__sens_N`` the query does not already contain."""
    n = 0
    while f"{_ALIAS_STEM}{n}" in sql:
        n += 1
    return f"{_ALIAS_STEM}{n}"


def _rewrite(sql: str, shadow: Shadow, *, dialect: str | None) -> str:
    """The caller's own text, with *shadow.table* pointed at an injected CTE."""
    refs = _table_refs(sql, shadow.table, dialect=dialect)
    if refs == 0:
        raise _Refused("not_applicable: query does not reference the target")
    spans = _spans(sql, shadow.table, dialect=dialect)
    if len(spans) != refs:
        raise _Refused(
            f"count guard: {len(spans)} spans vs {refs} table nodes; refusing to edit"
        )
    alias = _free_alias(sql)
    out = sql
    for start, end in sorted(spans, reverse=True):  # right-to-left
        out = out[:start] + alias + out[end:]

    toks = list(sqlglot.tokenize(out, dialect=dialect))
    cte = f"{alias} AS ({shadow.sql})"
    if toks and toks[0].token_type == sqlglot.TokenType.WITH:
        head = toks[1] if len(toks) > 1 else None
        cut = (
            head.end + 1
            if head is not None and head.token_type == sqlglot.TokenType.RECURSIVE
            else toks[0].end + 1
        )
        return f"{out[:cut]} {cte}, {out[cut:]}"
    return f"WITH {cte} {out}"


def _norm(rows: list[tuple]) -> list[tuple]:
    """Order- and float-noise-insensitive form.

    So "the answer moved" means the values moved, not that the engine returned
    them in a different order.
    """
    rounded = [
        tuple(round(v, 6) if isinstance(v, float) else v for v in row) for row in rows
    ]
    return sorted(rounded, key=repr)


@dataclass(frozen=True)
class SensitivityResult:
    """The verdict for one property against one query.

    ``status`` (each result has exactly one):
      - ``"pass"``           -- the answer responded the way the contract requires.
      - ``"violation"``      -- it did not.
      - ``"not_applicable"`` -- the query never references the shadowed table.
                                An outcome, not a failure.
      - ``"unchecked"``      -- no verdict was possible: unparseable SQL, the
                                count guard refused the edit, the engine raised,
                                the base query is not deterministic, or the test
                                was vacuous.

    ``moved`` is None when no comparison was made. ``reason`` reports the
    mechanical condition only and never infers a cause -- the same boundary
    ``ReconciliationResult.reason`` draws. The check says "the answer did not
    move", not "you hardcoded the threshold".
    """

    name: str
    metric: str
    status: str
    expected: str
    moved: bool | None = None
    reason: str = ""


@dataclass(frozen=True)
class SensitivityReport:
    results: tuple[SensitivityResult, ...]

    @property
    def violations(self) -> tuple[SensitivityResult, ...]:
        return tuple(r for r in self.results if r.status == "violation")

    @property
    def unchecked(self) -> tuple[SensitivityResult, ...]:
        return tuple(r for r in self.results if r.status == "unchecked")

    @property
    def not_applicable(self) -> tuple[SensitivityResult, ...]:
        return tuple(r for r in self.results if r.status == "not_applicable")

    @property
    def ok(self) -> bool:
        """True when nothing is a violation and nothing is unchecked.

        Safe as a CI gate -- ``if not report.ok: sys.exit(1)``. It is False on
        a ``violation`` and on an ``unchecked``, because "no verdict was
        possible" must not read as "passed". ``not_applicable`` does NOT block:
        a query that never touches the table has nothing to answer for.

        An EMPTY report is ok, which differs from ``ExampleValidationReport``.
        There, zero examples means a corpus failed to load. Here it means the
        metric declares no properties -- nothing was claimed, so nothing failed.
        Test ``report.violations`` directly for a laxer gate.
        """
        return not (self.violations or self.unchecked)

    def summary(self) -> str:
        """A compact markdown report, suitable for an MR comment."""
        if not self.results:
            return "No sensitivity properties declared."
        counts: dict[str, int] = {}
        for r in self.results:
            counts[r.status] = counts.get(r.status, 0) + 1
        head = ", ".join(f"{n} {status}" for status, n in sorted(counts.items()))
        lines = [f"**Sensitivity:** {head}", ""]
        for r in self.results:
            detail = f" — {r.reason}" if r.reason else ""
            lines.append(
                f"- `{r.metric}` / `{r.name}`: **{r.status}** "
                f"(expected {r.expected}, moved={r.moved}){detail}"
            )
        return "\n".join(lines)


#: Base executions per call. Two is a filter, not a proof -- see the note on
#: determinism in ``check_sensitivity``.
DEFAULT_REPEATS = 2


def _shadow_tables(shadow: Shadow, *, dialect: str | None) -> set[str] | None:
    """Qualified names ``shadow.sql`` reads, or None if it does not parse."""
    try:
        parsed = sqlglot.parse_one(shadow.sql, dialect=dialect)
    except Exception:  # noqa: BLE001 - an unparseable shadow is one outcome
        return None
    if parsed is None:
        return None
    # sqlglot.parse_one is annotated to return the broader `Expr` base (as of
    # the resolved sqlglot; the pinned floor still returns `Expression`
    # directly). Every real parse produces an `exp.Expression` subtype, so
    # this narrows the static type to match runtime reality -- same pattern
    # as `_table_refs`.
    tree = cast(exp.Expression, parsed)
    ctes = _cte_aliases(tree)
    names: set[str] = set()
    for node in tree.find_all(exp.Table):
        if node.name.lower() in ctes and not node.db:
            continue
        names.add(f"{node.db}.{node.name}" if node.db else node.name)
    return names


def validate_sensitivity_tables(
    contract: DataContract, metrics: list[MetricDefinition]
) -> list[str]:
    """Problems where a shadow reads a table the contract does not govern.

    A shadow that reads an ungoverned table is a governance hole, not a
    convenience: it would let a contract-authored SELECT reach data no agent
    query may reach. This is the CI-time gate; ``check_sensitivity`` refuses
    the same condition at execution time.

    A shadow sqlglot cannot parse yields no problem here -- it cannot be
    checked, and it degrades to ``unchecked`` at run time rather than being
    silently trusted.
    """
    allowed = {name.lower() for name in contract.allowed_table_names()}
    problems: list[str] = []
    for metric in metrics:
        for prop in metric.sensitivity:
            read = _shadow_tables(prop.shadow, dialect=None)
            if read is None:
                continue
            for name in sorted(read):
                if name.lower() not in allowed:
                    problems.append(
                        f"metric {metric.name!r} sensitivity property "
                        f"{prop.name!r}: shadow reads {name!r}, which the "
                        "contract does not allow"
                    )
    return problems


def check_sensitivity(
    metric: MetricDefinition,
    sql: str,
    *,
    contract: DataContract,
    adapter: DatabaseAdapter,
    properties: Sequence[str] | None = None,
    repeats: int = DEFAULT_REPEATS,
    dialect: str | None = None,
) -> SensitivityReport:
    """Check *sql* against the sensitivity properties *metric* declares.

    ``properties`` selects a subset by name; None runs every declared property.
    An unknown name raises ``ValueError`` -- malformed input raises, data
    conditions are findings, the same split ``reconcile_decomposition`` makes.

    **Step 0: the caller's query must pass Layer 1.** ``sql`` goes through the
    contract's ``Validator`` before anything is executed, and a policy block
    raises. Without this the function is a policy bypass: an entry point that
    runs arbitrary SQL against the adapter with none of the checks every other
    path applies. It is less a re-run of the caller's own validation than a
    refusal to be the weak door.

    An unparseable query is a DIFFERENT outcome from a policy block, though the
    Validator reports both as ``blocked``. There is no verdict to render on SQL
    Layer 1 could not even read, so this degrades to ``unchecked`` for every
    selected property -- the same "no verdict was possible" status a count-guard
    refusal or a nondeterministic base query produces -- and nothing is
    executed, not even the base query, not even the rewrite. That is what makes
    skipping the raise safe: this function never runs SQL Layer 1 did not first
    see and clear.

    ``shadow.sql`` is NOT put through the Validator. It is contract-authored,
    like ``sql_expression``, and most shadows want the ``SELECT *`` the
    validator would reject. It is instead constrained at both ends: its tables
    must be governed (checked here, and by ``validate_sensitivity_tables``),
    and it can only ever be read.

    **Determinism is filtered, not proved.** The base query is run ``repeats``
    times; disagreement makes every property ``unchecked``. A query that is
    merely *usually* stable passes this and then produces a verdict it did not
    earn. Measured on the DABStep corpus, two executions caught 13 of ~14 flaky
    queries -- the survivor differed on every repetition of the experiment and
    was always a ``LIMIT`` with no ``ORDER BY``. No finite number of probes
    closes this, which is why ``repeats`` is a parameter and the limitation is
    stated rather than engineered away.

    A passing property says the query *responds* to an input the contract says
    it depends on. It never says the answer is right.
    """
    selected = list(metric.sensitivity)
    if properties is not None:
        by_name = {p.name: p for p in selected}
        unknown = [n for n in properties if n not in by_name]
        if unknown:
            raise ValueError(
                f"metric {metric.name!r} declares no sensitivity property named "
                f"{unknown[0]!r}; declared: {sorted(by_name) or 'none'}"
            )
        selected = [by_name[n] for n in properties]

    if not selected:
        return SensitivityReport(results=())

    if dialect is None:
        dialect = adapter.dialect

    # Step 0 -- refuse to be the weak door. Layer 1 reports an unparseable
    # query as blocked too, but that is not a policy verdict: it is "no
    # verdict possible", which the spec maps to `unchecked` (the decision-B
    # case for a dialect sqlglot cannot read). Only a POLICY block raises.
    verdict = Validator(contract, dialect=dialect).validate(sql)
    if verdict.blocked and not verdict.parse_error:
        raise ValueError(
            f"query is blocked by the contract and will not be executed: "
            f"{'; '.join(verdict.reasons)}"
        )

    allowed = {name.lower() for name in contract.allowed_table_names()}
    for prop in selected:
        read = _shadow_tables(prop.shadow, dialect=dialect)
        for name in sorted(read or ()):
            if name.lower() not in allowed:
                raise ValueError(
                    f"sensitivity property {prop.name!r} of metric "
                    f"{metric.name!r} has a shadow reading {name!r}, which the "
                    "contract does not allow"
                )

    # An unparseable query is refused a verdict WITHOUT executing anything --
    # not even the rewrite. That is what makes skipping the policy raise safe:
    # if the Validator's parse and `_rewrite`'s ever disagreed, falling through
    # would run SQL Layer 1 never vouched for.
    if verdict.parse_error:
        reason = f"unparseable: {'; '.join(verdict.reasons)}"
        return SensitivityReport(
            results=tuple(
                SensitivityResult(
                    name=prop.name,
                    metric=metric.name,
                    status="unchecked",
                    expected=prop.expect,
                    reason=reason,
                )
                for prop in selected
            )
        )

    def _run(statement: str) -> list[tuple]:
        try:
            return _norm(list(adapter.execute(statement).rows))
        except Exception as e:  # noqa: BLE001 - any engine failure is one outcome
            raise _Refused(f"engine error: {e}") from e

    # The base result is computed at most once per call and reused across every
    # property; a metric with four properties costs six executions, not twelve.
    base: list[tuple] | None = None
    base_refusal: str | None = None

    def _base() -> list[tuple]:
        nonlocal base, base_refusal
        if base_refusal is not None:
            raise _Refused(base_refusal)
        if base is None:
            first = _run(sql)
            for _ in range(max(repeats - 1, 0)):
                if _run(sql) != first:
                    base_refusal = "query is not deterministic"
                    raise _Refused(base_refusal)
            base = first
        return base

    results: list[SensitivityResult] = []
    for prop in selected:
        try:
            mutated_sql = _rewrite(sql, prop.shadow, dialect=dialect)
        except _Refused as e:
            reason = str(e)
            status = (
                "not_applicable" if reason.startswith("not_applicable") else "unchecked"
            )
            results.append(
                SensitivityResult(
                    name=prop.name,
                    metric=metric.name,
                    status=status,
                    expected=prop.expect,
                    reason=reason,
                )
            )
            continue

        try:
            before = _base()
            if not before and prop.expect == "changes":
                raise _Refused("vacuous: an empty base result cannot move")
            after = _run(mutated_sql)
        except _Refused as e:
            results.append(
                SensitivityResult(
                    name=prop.name,
                    metric=metric.name,
                    status="unchecked",
                    expected=prop.expect,
                    reason=str(e),
                )
            )
            continue

        moved = after != before
        expected_move = prop.expect == "changes"
        results.append(
            SensitivityResult(
                name=prop.name,
                metric=metric.name,
                status="pass" if moved == expected_move else "violation",
                expected=prop.expect,
                moved=moved,
                reason=""
                if moved == expected_move
                else "the answer did not respond"
                if expected_move
                else "the answer moved",
            )
        )
    return SensitivityReport(results=tuple(results))
