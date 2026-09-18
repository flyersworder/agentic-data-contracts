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

from typing import cast

import sqlglot
from sqlglot import exp

from agentic_data_contracts.semantic.base import Shadow

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
