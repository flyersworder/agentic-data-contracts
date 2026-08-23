"""Detect relative (non-deterministic) time windows in parsed SQL.

Split out of ``examples.py`` so ``validator.py`` can use ``_relative_time_node``
too: ``examples.py`` imports ``Validator`` from ``validator.py``, so importing
this helper back from ``examples.py`` into ``validator.py`` would be a circular
import. This module has no dependency on either, mirroring the existing
``_scalar.py`` split in this package.
"""

from __future__ import annotations

from sqlglot import exp

# Arm one: the spellings sqlglot gives a dedicated node. Only bare CURRENT_DATE
# and CURRENT_TIMESTAMP land here in EVERY dialect.
_TIME_FUNCS = (
    exp.CurrentDate,
    exp.CurrentTimestamp,
    exp.CurrentTime,
    exp.CurrentDatetime,
    exp.Localtime,
    exp.Localtimestamp,
    exp.Systimestamp,
    exp.UtcTimestamp,
)

# Arm two: the same idea spelled as a function call sqlglot did not model for
# this dialect. NOW() is CurrentTimestamp under postgres but Anonymous under
# duckdb, mysql, snowflake, bigquery, tsql and oracle; GETDATE() is typed only
# under tsql and snowflake; TODAY() only under duckdb. Without this arm the
# checker would miss the most common relative spelling under the dialect most
# likely to be running it.
#
# ``localtime`` / ``localtimestamp`` are also typed nodes now (see
# ``_TIME_FUNCS`` above), which makes their entries here currently
# unreachable. KEEP them anyway: sqlglot's typing is dialect-dependent and has
# changed before — that is the whole reason this second arm exists — so a
# redundant name today is a cheap backstop against a future dialect that
# stops typing it.
_TIME_FUNC_NAMES = frozenset(
    {
        "now",
        "getdate",
        "sysdate",
        "sysdatetime",
        "today",
        "curdate",
        "curtime",
        "localtime",
        "localtimestamp",
        "current_date",
        "current_timestamp",
        "current_time",
        "unix_timestamp",
        "getutcdate",
        "statement_timestamp",
        "transaction_timestamp",
        "timeofday",
        # get_current_timestamp is DuckDB's own spelling and clock_timestamp is
        # statement_timestamp's Postgres sibling — both are real clock reads
        # that stayed Anonymous and slipped through. current_localtimestamp is
        # already caught by the typed arm; it is listed for the same backstop
        # reason as localtime/localtimestamp above.
        "clock_timestamp",
        "get_current_timestamp",
        "current_localtimestamp",
    }
)


def _relative_time_node(statement: exp.Expression) -> str | None:
    """Name the first non-deterministic time function in *statement*, if any.

    An expected value attached to a relative window decays: correct today,
    wrong in a month, for no reason the corpus author did anything about. Such
    a row is refused rather than executed.

    Matching ``exp.Anonymous`` — a function *call* — rather than any identifier
    is deliberate: a column named ``now_flag`` or ``sysdate`` is not a call and
    must not be flagged, or the checker would refuse valid assertions.
    """
    node = statement.find(*_TIME_FUNCS)
    if node is not None:
        return type(node).__name__
    for call in statement.find_all(exp.Anonymous):
        name = str(call.this)
        if name.lower() in _TIME_FUNC_NAMES and _is_clock_read(call):
            return f"{name.upper()}()"
    return None


def _is_clock_read(call: exp.Anonymous) -> bool:
    """True when a named time call reads the clock rather than converting a value.

    The name alone is not enough, because one spelling can be both. Three cases:

    * **No arguments** — ``NOW()``, ``GETDATE()``. The common form, always a
      clock read.
    * **One integer literal** — ``NOW(3)``, ``SYSDATE(6)``, ``CURTIME(3)``. A
      fractional-seconds precision spec, still a clock read. Note this cannot
      be assumed away by saying the precision spellings are typed nodes: it is
      true of ``CURRENT_TIMESTAMP(6)`` and ``LOCALTIMESTAMP(3)``, but ``NOW``
      and ``SYSDATE`` are exactly the names that only ever reach *this* arm.
    * **Anything else** — ``UNIX_TIMESTAMP(created_at)`` converts the column it
      is handed and is perfectly deterministic. Refusing it would make the
      checker reject a pinnable assertion, whose only escape would be
      ``time_scoped: true`` asserting something untrue.
    """
    args = call.expressions
    if not args:
        return True
    return len(args) == 1 and isinstance(args[0], exp.Literal) and args[0].is_int
