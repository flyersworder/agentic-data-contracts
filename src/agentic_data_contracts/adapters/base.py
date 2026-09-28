"""Database adapter protocol and shared types."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from agentic_data_contracts.validation.explain import ExplainResult


@dataclass
class Column:
    name: str
    type: str
    description: str = ""
    nullable: bool = True


@dataclass
class TableSchema:
    columns: list[Column] = field(default_factory=list)
    # Appended rather than placed before `columns`, so every pre-existing
    # positional construction -- `TableSchema([...])` -- keeps binding to the
    # field it always did. This dataclass is public API and not `kw_only`.
    # Same rule as `Attempt.final_rows` in validation/conformance.py.
    #
    # The granularity between `AllowedTable.description` (a schema *group*) and
    # `Column.description` (one column): what the table means, which is what an
    # agent needs before it writes SQL over it. Semantic sources populate it
    # where their format carries one; a `DatabaseAdapter` may leave it empty.
    description: str = ""


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[tuple[Any, ...]]
    row_count: int = 0
    # Appended so positional construction keeps binding as before. True when
    # the query produced more rows than were fetched: `rows` is a prefix and
    # `row_count` counts only it.
    truncated: bool = False

    def __post_init__(self) -> None:
        if self.row_count == 0:
            self.row_count = len(self.rows)


@runtime_checkable
class DatabaseAdapter(Protocol):
    def execute(self, sql: str) -> QueryResult: ...
    def explain(self, sql: str) -> ExplainResult: ...
    def describe_table(self, schema: str, table: str) -> TableSchema: ...
    def list_tables(self, schema: str) -> list[str]: ...

    @property
    def dialect(self) -> str: ...


class QueryTimeoutError(Exception):
    """A statement ran past ``resources.max_query_time_seconds``.

    ``cancelled`` says whether the database stopped it. A ``TimeoutAdapter``
    raises it with ``cancelled=True``; the query tools' caller-side fallback
    raises it with ``cancelled=False``, because it only stopped waiting.
    """

    def __init__(self, timeout_seconds: float, *, cancelled: bool = True) -> None:
        super().__init__(f"query exceeded {timeout_seconds:g}s")
        self.timeout_seconds = timeout_seconds
        self.cancelled = cancelled


@runtime_checkable
class TimeoutAdapter(Protocol):
    """Optional adapter capability: execute under a time limit, cancelling the
    statement in the database when it is exceeded.

    Detected at runtime, so ``DatabaseAdapter`` stays unchanged. Raise
    ``QueryTimeoutError`` on timeout. Start the clock once the statement owns
    its connection, not while it waits for one -- a query queued behind another
    has not run yet. An adapter without this capability still gets a timeout
    from the query tools, but only on the caller's side: the statement may
    keep running in the database, and the agent's next query can reach the
    adapter while it does -- so such an adapter must be safe to call
    concurrently.
    """

    def execute_with_timeout(self, sql: str, timeout_seconds: float) -> QueryResult: ...


@runtime_checkable
class RowLimitAdapter(Protocol):
    """Optional adapter capability: fetch at most ``max_rows`` rows.

    Detected at runtime, so ``DatabaseAdapter`` stays unchanged. Read at most
    ``max_rows + 1`` rows from the database and return at most ``max_rows``,
    with ``truncated=True`` when the extra row existed; never read the rest.
    DB-API drivers do this with ``cursor.fetchmany(max_rows + 1)``.

    The query tools pass ``timeout_seconds`` only to an adapter that also
    implements ``TimeoutAdapter``, which must then honour it as
    ``execute_with_timeout`` does. Any other adapter receives ``None`` and gets
    the tools' caller-side deadline instead. Without this capability the tools
    still cap what the agent sees, but only after ``execute`` has fetched
    everything.
    """

    def execute_limited(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult: ...


# Re-export SqlNormalizer so consumers can import from adapters.base
from agentic_data_contracts.adapters._normalizer import SqlNormalizer  # noqa: E402

__all__ = [
    "Column",
    "DatabaseAdapter",
    "QueryResult",
    "QueryTimeoutError",
    "RowLimitAdapter",
    "SqlNormalizer",
    "TableSchema",
    "TimeoutAdapter",
]
