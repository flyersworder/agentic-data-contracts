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

COST. The base query runs ``repeats`` times (default 2) once per call, and
only when at least one property applies -- a call whose properties are all
``not_applicable`` executes nothing -- plus at most one execution per
applicable property. The base result (or its refusal) is cached, so with the
default a metric with four properties costs six executions, not twelve. That
is why this belongs at a promotion gate rather than in an agent's hot path.

NO SQL IS REGENERATED. sqlglot decides *what* to rewrite and guards the edit;
the string executed is the caller's own text with spans replaced. That is what
makes this work on a dialect sqlglot can parse directly but cannot emit, and it
is the same template-assembly discipline the rest of the library follows.

DIALECTS THAT PARSE ONLY AFTER NORMALIZATION (Denodo VQL, in this library).
References to the target are COUNTED in the normalized text, which sqlglot can
parse, and LOCATED in the original by tokenizing it -- tokenizing is far more
permissive than parsing -- because a ``SqlNormalizer`` returns no offsets and
the original is what executes. The two must agree, and the edit is proved by
normalizing the result: the edited body must hold no reference to the target,
and the final text must parse as one statement. The normalizer must change
syntax only; one that renames the target fails closed as ``unchecked``.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, cast

import sqlglot
from sqlglot import exp

from agentic_data_contracts.adapters._normalizer import SqlNormalizer
from agentic_data_contracts.core.principal import Principal, resolve_principal
from agentic_data_contracts.validation.validator import Validator, _is_multi_statement

if TYPE_CHECKING:
    # Deferred to avoid a circular import: adapters.base imports
    # validation.explain, which initializes this package (validation/__init__)
    # before adapters.base finishes defining DatabaseAdapter, and semantic.base
    # itself imports TableSchema from adapters.base at module level. Safe at
    # runtime because `from __future__ import annotations` keeps annotations
    # unevaluated.
    from agentic_data_contracts.adapters.base import DatabaseAdapter
    from agentic_data_contracts.core.contract import DataContract
    from agentic_data_contracts.semantic.base import (
        MetricDefinition,
        SensitivityProperty,
        Shadow,
    )

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
#: Letter-leading, because some engines require an unquoted identifier to start
#: with a letter (Oracle documents it) and the VQL grammar names the CTE a
#: `<query name>` without spelling out its identifier rules.
_ALIAS_STEM = "sens_shadow_"


class _Refused(Exception):
    """The mechanical reason no verdict could be rendered for one property."""


#: A normalizer as the rewrite sees it: dialect text in, sqlglot-parseable text
#: out. The identity when the caller has no SqlNormalizer.
Normalize = Callable[[str], str]


def _identity(sql: str) -> str:
    return sql


def _normalized(sql: str, normalize: Normalize, *, what: str) -> str:
    """*sql* normalized, or a refusal naming what failed to normalize."""
    try:
        result = normalize(sql)
    except Exception as e:  # noqa: BLE001 - any normalizer failure is one outcome
        raise _Refused(f"normalizer failed on {what}: {e}") from e
    if not isinstance(result, str):
        raise _Refused(
            f"normalizer failed on {what}: returned {type(result).__name__}, not str"
        )
    return result


def _normalizer_fn(sql_normalizer: SqlNormalizer | None) -> Normalize:
    """The normalizer as a callable; the identity when there is none."""
    return sql_normalizer.normalize_sql if sql_normalizer is not None else _identity


def _all_unchecked(
    selected: list[SensitivityProperty], metric: MetricDefinition, reason: str
) -> SensitivityReport:
    """Every selected property refused a verdict for the same whole-call reason."""
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
    try:
        toks = list(sqlglot.tokenize(sql, dialect=dialect))
    except Exception as e:  # noqa: BLE001 - TokenError, whatever the version
        raise _Refused(f"original text could not be tokenized: {e}") from e
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
    """The first ``sens_shadow_N`` the query does not already contain."""
    n = 0
    while f"{_ALIAS_STEM}{n}" in sql:
        n += 1
    return f"{_ALIAS_STEM}{n}"


def _prove_edit(
    body: str, target: str, *, dialect: str | None, normalize: Normalize
) -> None:
    """Refuse unless the edited body holds no real reference to *target*.

    Runs on the body BEFORE the CTE is injected: the shadow itself legitimately
    reads the target, so the final text always contains one. Count equality
    alone can be fooled -- a normalizer that drops one reference while the
    original holds a look-alike in table position gets the wrong span edited
    with the counts still agreeing -- so this checks the outcome, not the
    arithmetic.
    """
    try:
        remaining = _table_refs(
            _normalized(body, normalize, what="the edited query"),
            target,
            dialect=dialect,
        )
    except _Refused as e:
        raise _Refused(f"rewrite could not be proved: {e}") from e
    if remaining:
        raise _Refused(
            f"rewrite could not be proved: {remaining} reference(s) to "
            f"{target!r} remain after the edit"
        )


def _strip_trailing_semicolons(sql: str, *, dialect: str | None) -> str:
    """*sql* with any trailing SEMICOLON tokens -- and anything after them,
    such as a trailing comment -- cut off.

    A shadow is one statement, and ``alias AS (<shadow.sql>\n)`` is not valid
    SQL when ``<shadow.sql>`` ends in ``;``: the CTE's own parentheses close
    over an empty second statement. Tokenize-based so a comment trailing the
    ``;`` (``SELECT ...; -- note``) is cut with it, rather than left dangling
    to swallow the CTE's closing paren. A shadow with no trailing semicolon
    is returned unchanged -- its own trailing comment, if any, is still
    closed by the newline `_inject` puts before the paren.
    """
    try:
        toks = list(sqlglot.tokenize(sql, dialect=dialect))
    except Exception as e:  # noqa: BLE001 - any tokenizer failure is one outcome
        raise _Refused(f"shadow could not be tokenized: {e}") from e
    end = len(toks)
    while end > 0 and toks[end - 1].token_type == sqlglot.TokenType.SEMICOLON:
        end -= 1
    if end == len(toks):
        return sql  # no trailing semicolon at all -- untouched
    return sql[: toks[end - 1].end + 1] if end > 0 else ""


def _inject(body: str, alias: str, shadow: Shadow, *, dialect: str | None) -> str:
    """Put the shadow in front of *body* as a CTE, splicing into its own WITH."""
    try:
        toks = list(sqlglot.tokenize(body, dialect=dialect))
    except Exception as e:  # noqa: BLE001 - the original tokenized, so unexpected
        raise _Refused(f"rewrite could not be proved: {e}") from e
    shadow_sql = _strip_trailing_semicolons(shadow.sql, dialect=dialect)
    # The newline ends any trailing line comment in the shadow before the
    # closing paren, which the comment would otherwise swallow.
    cte = f"{alias} AS ({shadow_sql}\n)"
    if toks and toks[0].token_type == sqlglot.TokenType.WITH:
        head = toks[1] if len(toks) > 1 else None
        cut = (
            head.end + 1
            if head is not None and head.token_type == sqlglot.TokenType.RECURSIVE
            else toks[0].end + 1
        )
        return f"{body[:cut]} {cte}, {body[cut:]}"
    return f"WITH {cte} {body}"


def _prove_parses(final: str, *, dialect: str | None, normalize: Normalize) -> None:
    """Refuse unless the injected text is exactly one statement that parses.

    "Parses" alone is not enough: at the sqlglot 28.6 floor ``parse_one``
    silently keeps only the first statement, so the count is checked the way
    Layer 1 checks it.
    """
    try:
        normalized = _normalized(final, normalize, what="the rewritten query")
        if _is_multi_statement(normalized, dialect):
            raise _Refused("it holds more than one statement")
        sqlglot.parse_one(normalized, dialect=dialect)
    except Exception as e:  # noqa: BLE001 - _Refused, ParseError, TokenError alike
        raise _Refused(f"rewrite could not be proved: {e}") from e


def _rewrite(
    sql: str,
    shadow: Shadow,
    *,
    dialect: str | None,
    normalize: Normalize = _identity,
) -> str:
    """The caller's own text, with *shadow.table* pointed at an injected CTE.

    References are COUNTED in the normalized text, which sqlglot can parse, and
    LOCATED in the original, which is what executes: a ``SqlNormalizer`` returns
    no offsets, and the protocol requires the original to run. The two must
    agree, and the edit is then proved rather than trusted. Raises only
    ``_Refused``.
    """
    normalized_sql = _normalized(sql, normalize, what="the query")
    refs = _table_refs(normalized_sql, shadow.table, dialect=dialect)
    spans = _spans(sql, shadow.table, dialect=dialect)
    # Zero references after normalization is not, by itself, an absence: a
    # column that shares the target's bare name (`SELECT lead_id, touchpoints
    # FROM mkt.lead_scores`) puts a span in table position -- `_spans` cannot
    # tell a column reference from a table one -- with no normalizer involved
    # at all. So this branch decides the whole outcome and always raises: if
    # the ORIGINAL holds no span, there is nothing to edit regardless of what
    # the normalizer did, so it is not_applicable outright. Otherwise compare
    # the span count in the normalized text against the original's: unchanged
    # means whatever look-alikes exist survived normalization untouched,
    # nothing was renamed, and it is still not_applicable. A different count
    # -- a normalizer that renamed or re-qualified the target, or (rarer) one
    # that manufactured a look-alike span of its own -- is the count guard,
    # raised here explicitly rather than falling through to the general
    # `len(spans) != refs` check below, which is `0 != 0` and would not fire.
    if refs == 0:
        if not spans:
            raise _Refused("not_applicable: query does not reference the target")
        normalized_spans = _spans(normalized_sql, shadow.table, dialect=dialect)
        if len(normalized_spans) == len(spans):
            raise _Refused("not_applicable: query does not reference the target")
        raise _Refused(
            f"count guard: {len(spans)} spans in the original text vs "
            f"{len(normalized_spans)} look-alike spans in the normalized text; "
            "refusing to edit"
        )
    if len(spans) != refs:
        raise _Refused(
            f"count guard: {len(spans)} spans in the original text vs {refs} "
            "table references after normalization; refusing to edit"
        )
    # Free in BOTH texts: the shadow is spliced in as a CTE right beside the
    # caller's query, so a name only the shadow contains -- a comment, a CTE
    # of its own -- collides just as surely as one in the caller's own text.
    alias = _free_alias(sql + "\n" + shadow.sql)
    body = sql
    for start, end in sorted(spans, reverse=True):  # right-to-left
        body = body[:start] + alias + body[end:]
    _prove_edit(body, shadow.table, dialect=dialect, normalize=normalize)
    final = _inject(body, alias, shadow, dialect=dialect)
    _prove_parses(final, dialect=dialect, normalize=normalize)
    return final


#: Significant digits kept by `_round_sig`. Chosen well inside float64's ~15-17
#: digit precision, so two engine runs of the same deterministic query agree
#: at this many digits while genuine float noise near the precision floor is
#: dropped.
_SIG_DIGITS = 12

#: Absolute floor below which a finite value snaps to 0.0. A quantity that is
#: mathematically exactly zero -- a net-zero sum, a reconciled balance -- does
#: not come back as literal 0.0 from a float engine: summation order alone
#: puts it a few ULPs either side of zero (DuckDB's own `sum(x)` over rows
#: that cancel gives ~1e-17, and the sign and magnitude of that noise depend
#: on the order the rows arrived in). Significant-digit rounding cannot catch
#: this: it is relative to the value's OWN magnitude, and a value near zero
#: has effectively no magnitude to be relative to, so 12 significant digits
#: of pure noise is still reported as 12 significant digits of pure noise.
#: 1e-9 sits far below any plausible genuine measurement in this library's
#: domain (currency, counts) and far above float64's noise floor.
_ZERO_FLOOR = 1e-9


def _round_sig(v: float) -> float:
    """*v* rounded to `_SIG_DIGITS` significant digits, snapping near-zero
    noise (``abs(v) < _ZERO_FLOOR``) to exactly ``0.0`` first.

    Rounds to a fixed significant-digit COUNT rather than a fixed decimal
    place, so the noise floor this discards tracks the value's own
    magnitude instead of being some absolute amount regardless of it. 0.0
    and non-finite values (``nan``, ``inf``, ``-inf``) pass through
    unchanged -- ``log10(0)`` is undefined, and a non-finite value has no
    meaningful digit count to round to.
    """
    if not math.isfinite(v):
        return v
    if abs(v) < _ZERO_FLOOR:
        return 0.0
    return round(v, _SIG_DIGITS - 1 - math.floor(math.log10(abs(v))))


class _NaN:
    """The one canonical NaN `_norm` substitutes for every float or Decimal NaN.

    ``nan != nan`` -- for ``Decimal('NaN')`` too, which is how psycopg returns
    Postgres ``numeric 'NaN'``, and a signalling ``Decimal('sNaN')`` raises
    ``InvalidOperation`` on ``==`` outright -- so a raw NaN never equals
    itself across two fetches: every NaN answer would read "not
    deterministic", and with ``repeats=1`` would always look moved. The
    single instance below compares equal only to itself (identity equality),
    so it cannot collide with any genuine value -- not a float, not None, not
    the string ``"NaN"`` -- and its fixed repr keeps ``sorted(..., key=repr)``
    deterministic. It is not None, so an
    all-NaN result is a value, never vacuous.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "<NaN>"


_NAN = _NaN()


def _norm_value(v: object) -> object:
    if isinstance(v, Decimal):
        # Quiet and signalling NaN alike; every other Decimal is untouched.
        return _NAN if v.is_nan() else v
    if not isinstance(v, float):
        return v
    return _NAN if math.isnan(v) else _round_sig(v)


def _norm(rows: list[tuple]) -> list[tuple]:
    """Order- and float-noise-insensitive form.

    So "the answer moved" means the values moved, not that the engine returned
    them in a different order. Float or Decimal NaN becomes the canonical
    `_NAN`, so NaN in the same position compares equal; ``inf``/``-inf``
    are kept as-is.
    """
    rounded = [tuple(_norm_value(v) for v in row) for row in rows]
    return sorted(rounded, key=repr)


def _is_vacuous(rows: list[tuple]) -> bool:
    """True when *rows* carry no answer at all: no rows, or only NULLs.

    This NARROWS the vacuous-test gap; it does not close it. An aggregate
    over no rows is not always an absence: ``SUM`` (and ``AVG``, ``MIN``,
    ``MAX``) over no rows is NULL, which this catches, but ``COUNT`` over no
    rows is ``0`` -- indistinguishable from a real answer of zero -- and is
    deliberately NOT treated as vacuous. Calling every zero vacuous would
    refuse a verdict on genuine zeros; a ``COUNT`` whose filter matches
    nothing on both sides therefore still reads ``pass`` under
    ``expect: unchanged``.
    """
    return not rows or all(v is None for row in rows for v in row)


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
                                the base query is not deterministic, or the
                                test was vacuous -- an empty base result for
                                ``expect: changes`` (it cannot move), or an
                                empty base AND an empty shadowed result for
                                ``expect: unchanged`` (nothing to hold still).
                                "Empty" means no rows OR only NULLs, so an
                                aggregate such as ``SUM`` over no rows counts;
                                ``COUNT`` over no rows is ``0`` and does not
                                (see ``_is_vacuous``). A non-empty shadowed
                                result against an empty base under
                                ``expect: unchanged`` is still a real
                                ``"violation"``: the answer moved.

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
        """True when nothing failed AND at least one property was checked.

        Safe as a CI gate -- ``if not report.ok: sys.exit(1)``. It is False on:

        - a ``violation``;
        - an ``unchecked``, because "no verdict was possible" must not read
          as "passed";
        - a report where EVERY result is ``not_applicable``. The query read
          none of the shadowed tables, so nothing was checked -- and the
          purest form of the defect this exists to catch, an answer pasted
          in as a literal, reads no table at all. A run that checked nothing
          must not read like a run that found nothing: the rule
          ``check_schema_drift`` already follows.

        ``not_applicable`` beside at least one ``pass`` does NOT block: a
        metric with properties on several tables legitimately gets it for a
        query that reads only some of them. The floor is one verdict, not all.

        An EMPTY report is ok. It arises two ways, both deliberate: the metric
        declares no properties (nothing was claimed), or the caller passed
        ``properties=[]`` -- the explicit way to say no declared property
        applies to this query. That differs from ``ExampleValidationReport``,
        where zero examples means a corpus failed to load. Test
        ``report.violations`` directly for a laxer gate.
        """
        if self.violations or self.unchecked:
            return False
        return not self.results or any(r.status == "pass" for r in self.results)

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


def _shadow_tables(
    shadow: Shadow, *, dialect: str | None, normalize: Normalize = _identity
) -> set[str] | None:
    """Qualified names ``shadow.sql`` reads, or None when they cannot be known.

    The shadow is normalized first, so a shadow written in a dialect that parses
    only after normalization is governed like any other. None covers every way
    its tables cannot be read off it: the normalizer raised, the normalized text
    does not parse, or it holds more than one statement. The last is not
    exploitable -- the CTE's parentheses make a statement boundary a syntax
    error -- but refusing it here gives both gates a clear finding instead of
    an opaque engine error.
    """
    try:
        text = normalize(shadow.sql)
        if _is_multi_statement(text, dialect):
            return None
        parsed = sqlglot.parse_one(text, dialect=dialect)
    except Exception:  # noqa: BLE001 - every failure means "tables unknown"
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


def _shadow_governance(
    props: Iterable[SensitivityProperty],
    allowed: Iterable[str],
    *,
    dialect: str | None,
    normalize: Normalize,
) -> Iterator[tuple[SensitivityProperty, list[str] | None]]:
    """Each property with the tables its shadow reads that *allowed* does not.

    Yields ``(prop, None)`` when the shadow's tables cannot be read (see
    ``_shadow_tables``), otherwise ``(prop, names)`` with the ungoverned names
    sorted -- an empty list when the shadow is fully governed. Lazy, so a
    caller that stops at the first problem reads no further shadows. The one
    governance rule both gates share; what a problem MEANS -- a raise at run
    time, a reported string in CI -- stays with each caller.
    """
    allowed_lower = {name.lower() for name in allowed}
    for prop in props:
        read = _shadow_tables(prop.shadow, dialect=dialect, normalize=normalize)
        if read is None:
            yield prop, None
            continue
        yield prop, [name for name in sorted(read) if name.lower() not in allowed_lower]


def _why_ungoverned(name: str, declared: set[str], principal: str | None) -> str:
    """Why *name* is refused, finishing a sentence that ends "..., which ".

    A table the contract never declares keeps the 0.52.0 wording; a declared
    table denied to this caller names the caller, in the phrasing the
    Validator's "restricted to other principals" uses -- so an upgrader whose
    default caller is None sees why, not a claim the contract lacks the table.
    *declared* is lowercased.
    """
    if name.lower() not in declared:
        return "the contract does not allow"
    who = principal if principal else "<no caller identified>"
    return (
        f"caller {who!r} may not read (the table is restricted by "
        "allowed_principals/blocked_principals)"
    )


def validate_sensitivity_tables(
    contract: DataContract,
    metrics: list[MetricDefinition],
    *,
    dialect: str | None = None,
    sql_normalizer: SqlNormalizer | None = None,
    caller_principal: str | None = None,
) -> list[str]:
    """Problems where a shadow reads a table the contract does not govern.

    A shadow that reads an ungoverned table is a governance hole, not a
    convenience: it would let a contract-authored SELECT reach data no agent
    query may reach. This is the CI-time gate; ``check_sensitivity`` refuses
    the same condition at execution time.

    A shadow sqlglot cannot parse is ALSO a problem here, not something this
    gate stays silent about: its tables cannot be checked against the
    contract, which is exactly the condition this function exists to catch.
    ``check_sensitivity`` makes the matching call at run time -- a property
    whose shadow does not parse is reported ``unchecked`` and never executed.

    ``dialect`` should match what ``check_sensitivity`` will use (typically
    the adapter's). Left at the default of None, a shadow using syntax only
    that dialect accepts parses at run time but not here, and this gate would
    flag it as unparseable regardless of governance.

    ``sql_normalizer`` should likewise match: a shadow in a dialect that parses
    only after normalization (Denodo VQL) is normalized before its tables are
    read. This gate takes no adapter, so -- unlike ``check_sensitivity`` -- it
    has no adapter to fall back on; pass the normalizer explicitly.

    ``caller_principal`` chooses WHICH tables count as governed. Omitted, the
    check is structural -- every table the contract declares, via
    ``contract.allowed_table_names()`` -- because a CI gate has no caller:
    it asks whether a shadow stays inside the contract at all, not whether
    some particular caller may read what it reads. Given, it checks against
    ``contract.allowed_table_names_for(caller_principal)``, the set
    ``check_sensitivity`` enforces at run time for that caller. Pass it to
    gate a metric that a known principal will check; leave it out and a
    shadow over a principal-restricted table passes here, then is refused at
    run time for any caller denied that table. ``caller_principal=""`` checks
    against the anonymous caller's tables -- exactly what ``check_sensitivity``
    applies by default -- whereas omitting it gives the structural check.
    """
    declared = {name.lower() for name in contract.allowed_table_names()}
    allowed = (
        contract.allowed_table_names()
        if caller_principal is None
        else contract.allowed_table_names_for(caller_principal)
    )
    normalize = _normalizer_fn(sql_normalizer)
    problems: list[str] = []
    for metric in metrics:
        for prop, ungoverned in _shadow_governance(
            metric.sensitivity, allowed, dialect=dialect, normalize=normalize
        ):
            if ungoverned is None:
                problems.append(
                    f"metric {metric.name!r} sensitivity property "
                    f"{prop.name!r}: shadow cannot be parsed, so its tables "
                    "cannot be checked against the contract"
                )
                continue
            for name in ungoverned:
                problems.append(
                    f"metric {metric.name!r} sensitivity property "
                    f"{prop.name!r}: shadow reads {name!r}, which "
                    f"{_why_ungoverned(name, declared, caller_principal)}"
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
    sql_normalizer: SqlNormalizer | None = None,
    caller_principal: Principal = None,
) -> SensitivityReport:
    """Check *sql* against the sensitivity properties *metric* declares.

    ``properties`` selects a subset by name; None runs every declared property.
    An unknown name raises ``ValueError``, and so does ``repeats`` below 1 --
    malformed input raises, data conditions are findings, the same split
    ``reconcile_decomposition`` makes.

    **Step 0: the caller's query must pass Layer 1.** ``sql`` goes through the
    contract's ``Validator`` before anything is executed, and a policy block
    raises -- including a string holding more than one statement. Without
    this the function is a policy bypass: an entry point that runs arbitrary
    SQL against the adapter with none of the checks every other path applies.
    It is less a re-run of the caller's own validation than a refusal to be
    the weak door.

    An unparseable query is a DIFFERENT outcome from a policy block, though the
    Validator reports both as ``blocked``. There is no verdict to render on SQL
    Layer 1 could not even read, so this degrades to ``unchecked`` for every
    selected property -- the same "no verdict was possible" status a
    count-guard refusal or a nondeterministic base query produces -- and
    nothing is executed, not even the base query, not even the rewrite. That
    is what makes skipping the raise safe: this function never runs SQL Layer
    1 did not first see and clear.

    **Principals.** ``caller_principal`` has the type and semantics of
    ``Validator``'s: a string, a zero-arg callable, or None. It is resolved
    ONCE per call, and that one identity is used twice: Step 0's
    ``Validator`` checks the query as that caller, and every shadow must read
    only tables in ``contract.allowed_table_names_for(<that caller>)``, so a
    contract-authored shadow can never reach a table the caller is denied. A
    callable is not re-invoked between the two -- it cannot clear the query
    as one caller and have the shadow governed as another. The default, None,
    is an anonymous caller: fail-closed, it is denied every table restricted
    by ``allowed_principals``/``blocked_principals``, so a query over one is
    blocked at Step 0 and a shadow reading one raises ``ValueError``.

    **Dialects that parse only after normalization.** ``sql_normalizer`` --
    defaulting to ``adapter`` when the adapter implements ``SqlNormalizer`` --
    is applied before anything is parsed: the query for Layer 1 and for
    locating the target, and each shadow for governance. The rewrite still
    edits the ORIGINAL text, which is what executes; the edit is proved by
    normalizing the result. The normalizer must change syntax only: one that
    renames the target makes the counts disagree, and every such query is
    ``unchecked``. A normalizer that raises on the QUERY is "no verdict
    possible" for the whole call; raising on one shadow leaves only that
    property ``unchecked``.

    ``shadow.sql`` is NOT put through the Validator. It is contract-authored,
    like ``sql_expression``, and most shadows want the ``SELECT *`` the
    validator would reject. It is instead constrained at both ends: its tables
    must be governed (checked here, and by ``validate_sensitivity_tables``),
    and it can only ever be read. A shadow sqlglot cannot parse is never
    silently trusted either: its tables cannot be checked, so that property is
    refused a verdict -- ``unchecked``, nothing executed -- the same as an
    unparseable *query*, just scoped to the one property whose shadow failed.

    **Determinism is filtered, not proved.** The base query is run ``repeats``
    times; disagreement makes every property that needs the base result
    ``unchecked``, and so does a base query the engine rejects. Either refusal
    is reached once per call, not once per property. A query that is merely
    *usually* stable passes this and then produces a verdict it did not
    earn. Measured on the DABStep corpus, two executions caught 13 of ~14 flaky
    queries -- the survivor differed on every repetition of the experiment and
    was always a ``LIMIT`` with no ``ORDER BY``. No finite number of probes
    closes this, which is why ``repeats`` is a parameter and the limitation is
    stated rather than engineered away.

    A passing property says the query *responds* to an input the contract says
    it depends on. It never says the answer is right.
    """
    if repeats < 1:
        raise ValueError(
            f"repeats must be at least 1, got {repeats}: fewer base executions "
            "would silently disable the determinism filter"
        )

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
    # An adapter that is itself a SqlNormalizer (the Denodo case) normalizes
    # unless the caller passes one -- the convention `create_tools` follows.
    if sql_normalizer is None and isinstance(adapter, SqlNormalizer):
        sql_normalizer = adapter
    normalize = _normalizer_fn(sql_normalizer)
    # Resolved once: Step 0 and shadow governance must judge the same caller,
    # so the Validator gets the resolved string, never the callable.
    principal = resolve_principal(caller_principal)

    # Step 0 -- refuse to be the weak door. The query is normalized ONCE here
    # and Layer 1 sees the normalized text. That is equivalent to handing the
    # Validator the normalizer -- its only use of the raw text is an EXPLAIN,
    # and this Validator has no explain adapter -- and it keeps a normalizer
    # that raises something other than a parse error from escaping validate().
    # A normalizer failure, like an unparseable query, is "no verdict
    # possible", not a policy block: it is recorded, and nothing executes.
    no_verdict: str | None = None
    try:
        normalized_sql = normalize(sql)
    except Exception as e:  # noqa: BLE001 - any normalizer failure is one outcome
        no_verdict = f"normalizer failed: {e}"
    else:
        if not isinstance(normalized_sql, str):
            # A normalizer returning e.g. None must not escape as TypeError
            # from the Validator call below -- it is one more shape of
            # "normalizer failed", not a crash.
            no_verdict = (
                f"normalizer failed: returned {type(normalized_sql).__name__}, not str"
            )
        else:
            verdict = Validator(
                contract, dialect=dialect, caller_principal=principal
            ).validate(normalized_sql)
            if verdict.blocked and not verdict.parse_error:
                raise ValueError(
                    f"query is blocked by the contract and will not be executed: "
                    f"{'; '.join(verdict.reasons)}"
                )
            if verdict.parse_error:
                no_verdict = f"unparseable: {'; '.join(verdict.reasons)}"

    # A shadow that fails to parse cannot be checked against the contract --
    # `None` is a refusal, never an empty set of tables read. Recorded now and
    # turned into an `unchecked` result (never an execution) in the loop below,
    # so a governance hole never opens just because sqlglot cannot read the
    # dialect a shadow is written in. Governed means governed FOR THIS
    # CALLER: a shadow reading a table the caller is denied is refused just as
    # one reading a table the contract never declared -- with a message that
    # says which of the two it is.
    declared = {name.lower() for name in contract.allowed_table_names()}
    unparseable_shadows: set[str] = set()
    for prop, ungoverned in _shadow_governance(
        selected,
        contract.allowed_table_names_for(principal),
        dialect=dialect,
        normalize=normalize,
    ):
        if ungoverned is None:
            unparseable_shadows.add(prop.name)
        elif ungoverned:
            raise ValueError(
                f"sensitivity property {prop.name!r} of metric "
                f"{metric.name!r} has a shadow reading {ungoverned[0]!r}, which "
                f"{_why_ungoverned(ungoverned[0], declared, principal)}"
            )

    # No verdict for the query -- unparseable, or its normalizer failed -- is
    # refused WITHOUT executing anything, not even the rewrite. That is what
    # makes skipping the policy raise safe: if the Validator's parse and
    # `_rewrite`'s ever disagreed, falling through would run SQL Layer 1 never
    # vouched for.
    if no_verdict is not None:
        return _all_unchecked(selected, metric, no_verdict)

    def _run(statement: str) -> list[tuple]:
        try:
            return _norm(list(adapter.execute(statement).rows))
        except Exception as e:  # noqa: BLE001 - any engine failure is one outcome
            raise _Refused(f"engine error: {e}") from e

    # The base result is computed at most once per call and reused across every
    # property; a metric with four properties costs `repeats` + 4 executions,
    # not 4 * (`repeats` + 1). A refusal while computing it -- nondeterminism
    # or an engine error -- is cached the same way, so a failing base query is
    # not re-run for every property.
    base: list[tuple] | None = None
    base_refusal: str | None = None

    def _base() -> list[tuple]:
        nonlocal base, base_refusal
        if base_refusal is not None:
            raise _Refused(base_refusal)
        if base is None:
            try:
                first = _run(sql)
                for _ in range(repeats - 1):
                    if _run(sql) != first:
                        raise _Refused("query is not deterministic")
            except _Refused as e:
                base_refusal = str(e)
                raise
            base = first
        return base

    results: list[SensitivityResult] = []
    for prop in selected:
        if prop.name in unparseable_shadows:
            # Refused a verdict, not defaulted to "reads nothing": nothing is
            # executed for this property -- not `_rewrite`, not the base
            # query -- because its shadow's tables were never checked against
            # the contract.
            results.append(
                SensitivityResult(
                    name=prop.name,
                    metric=metric.name,
                    status="unchecked",
                    expected=prop.expect,
                    reason=(
                        "unparseable shadow: its tables cannot be checked "
                        "against the contract"
                    ),
                )
            )
            continue

        try:
            mutated_sql = _rewrite(
                sql, prop.shadow, dialect=dialect, normalize=normalize
            )
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

        if _is_vacuous(before) and prop.expect == "changes":
            # An empty (or all-NULL) answer cannot move, so `expect: changes`
            # asserts nothing -- and refusing here, before the mutated query
            # ever runs, keeps that early exit's cost the same as before.
            results.append(
                SensitivityResult(
                    name=prop.name,
                    metric=metric.name,
                    status="unchecked",
                    expected=prop.expect,
                    reason="vacuous: an empty or all-NULL base result cannot move",
                )
            )
            continue

        try:
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

        if _is_vacuous(before) and _is_vacuous(after):
            # `expect: changes` with an empty base was already refused above,
            # so reaching here with an empty base means `expect: unchanged` --
            # and an empty shadowed result too tests nothing: there is no
            # answer that moved or held still, just two absences.
            results.append(
                SensitivityResult(
                    name=prop.name,
                    metric=metric.name,
                    status="unchecked",
                    expected=prop.expect,
                    reason=(
                        "vacuous: an empty or all-NULL base result and an "
                        "empty or all-NULL shadowed result cannot show "
                        "whether the answer would move"
                    ),
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
