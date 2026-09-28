"""DuckDB database adapter."""

from __future__ import annotations

import threading
from collections.abc import Generator
from contextlib import contextmanager, nullcontext
from typing import Any

import duckdb

from agentic_data_contracts.adapters.base import (
    Column,
    QueryResult,
    QueryTimeoutError,
    TableSchema,
)
from agentic_data_contracts.validation.explain import ExplainResult

_REINTERRUPT_SECONDS = 0.05


def _caused_by_interrupt(exc: BaseException) -> bool:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if isinstance(current, duckdb.InterruptException):
            return True
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return False


class DuckDBAdapter:
    """Database adapter for DuckDB.

    A single DuckDB connection is **not** safe for concurrent queries from
    multiple threads. The async tool handlers offload adapter calls via
    ``asyncio.to_thread`` (see ``tools/factory.py``), so concurrent sessions
    can land here on different worker threads at once. ``_lock`` serializes
    every access to ``self.connection`` so the shared connection stays
    consistent. DuckDB still parallelizes the work of an individual query
    internally; the lock only prevents two queries from interleaving on the
    same connection.

    ``memory_limit`` (a DuckDB size string such as ``"512MB"``) caps the
    engine's memory, so a runaway query fails with an out-of-memory error the
    agent sees instead of exhausting the process. It does not bound the Python
    objects built from a result; ``execute_limited`` does that. Unset, DuckDB
    uses its own default of 80% of RAM.
    """

    def __init__(
        self, database: str = ":memory:", *, memory_limit: str | None = None
    ) -> None:
        self.connection = duckdb.connect(database)
        # SET after connect, not connect(config=...): a second in-process
        # connection to the same file with a different config raises.
        if memory_limit is not None:
            self.connection.execute("SET memory_limit = ?", [memory_limit])
        # Reentrant so `execute_with_timeout` can hold it while calling
        # `self.execute`, which takes it again -- routing through `execute`
        # keeps a subclass override (SQL rewriting, auditing) on the path.
        self._lock = threading.RLock()

    @property
    def dialect(self) -> str:
        return "duckdb"

    def execute(self, sql: str) -> QueryResult:
        with self._lock:
            result = self.connection.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
        return QueryResult(columns=columns, rows=rows)

    @contextmanager
    def _interrupt_after(self, timeout_seconds: float) -> Generator[None]:
        """Interrupt the connection if the body runs past ``timeout_seconds``.

        The caller must hold ``_lock``: ``interrupt()`` cancels whatever the
        shared connection is running, so a clock started while still waiting
        for the lock would cancel another caller's statement.
        """
        done = threading.Event()
        timed_out = threading.Event()
        # Held while interrupting and while marking the call done, so no
        # interrupt can land after the lock is released -- on the next
        # caller's statement.
        guard = threading.Lock()

        def _watchdog() -> None:
            # Clamped: a longer wait raises OverflowError in this thread,
            # silently leaving the limit unarmed.
            if done.wait(min(timeout_seconds, threading.TIMEOUT_MAX)):
                return
            timed_out.set()
            # DuckDB drops an interrupt sent while the connection is idle,
            # so one fired during work the body does before its statement
            # starts (a subclass's `execute` rewriting SQL) would be lost.
            # Repeat until the call returns.
            while True:
                with guard:
                    if done.is_set():
                        return
                    self.connection.interrupt()
                if done.wait(_REINTERRUPT_SECONDS):
                    return

        threading.Thread(target=_watchdog, daemon=True).start()
        try:
            yield
        except Exception as e:
            # The timeout is an interrupt after the deadline, even one a
            # subclass's `execute` re-raised as another type. Any other
            # error -- memory_limit, say -- reaches the agent as itself.
            if timed_out.is_set() and _caused_by_interrupt(e):
                raise QueryTimeoutError(timeout_seconds) from None
            raise
        finally:
            with guard:
                done.set()

    def execute_with_timeout(self, sql: str, timeout_seconds: float) -> QueryResult:
        """Run ``sql``, interrupting it after ``timeout_seconds``.

        Runs through ``self.execute``, so a subclass override (SQL rewriting,
        auditing) stays on the path.
        """
        with self._lock, self._interrupt_after(timeout_seconds):
            return self.execute(sql)

    def execute_limited(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult:
        """Run ``sql`` and fetch at most ``max_rows`` rows.

        DuckDB streams the result, so rows past ``max_rows + 1`` are never
        produced. Does not call ``self.execute``: for a subclass that overrides
        only ``execute``, the query tools call ``execute`` instead (results
        capped, memory unbounded), so override this method too.
        """
        if max_rows < 1:
            raise ValueError(f"max_rows must be at least 1, got {max_rows}")
        with self._lock:
            limit = (
                self._interrupt_after(timeout_seconds)
                if timeout_seconds is not None
                else nullcontext()
            )
            with limit:
                result = self.connection.execute(sql)
                columns = [desc[0] for desc in result.description]
                rows = result.fetchmany(max_rows + 1)
        return QueryResult(
            columns=columns,
            rows=rows[:max_rows],
            truncated=len(rows) > max_rows,
        )

    def explain(self, sql: str) -> ExplainResult:
        try:
            with self._lock:
                result = self.connection.execute(f"EXPLAIN {sql}")
                rows = result.fetchall()
            estimated_rows = self._parse_row_estimate(rows)
            return ExplainResult(
                estimated_cost_usd=None,
                estimated_rows=estimated_rows,
                schema_valid=True,
                errors=[],
            )
        except duckdb.Error as e:
            return ExplainResult(
                estimated_cost_usd=None,
                estimated_rows=None,
                schema_valid=False,
                errors=[str(e)],
            )

    def _parse_row_estimate(self, explain_rows: list[tuple[Any, ...]]) -> int | None:
        """Parse DuckDB EXPLAIN output for estimated row count.

        DuckDB EXPLAIN includes lines with ~N indicating estimated cardinality.
        We take the last ~N in the output (top-level node estimate).
        """
        import re

        last_estimate = None
        for row in explain_rows:
            if len(row) > 1:
                text = str(row[1])
            elif len(row) == 1:
                text = str(row[0])
            else:
                continue
            match = re.search(r"~(\d+)", text)
            if match:
                last_estimate = int(match.group(1))
        return last_estimate

    def list_tables(self, schema: str) -> list[str]:
        with self._lock:
            rows = self.connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = ?
                ORDER BY table_name
                """,
                [schema],
            ).fetchall()
        return [row[0] for row in rows]

    def describe_table(self, schema: str, table: str) -> TableSchema:
        with self._lock:
            rows = self.connection.execute(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
                ORDER BY ordinal_position
                """,
                [schema, table],
            ).fetchall()
        columns = [
            Column(
                name=row[0],
                type=row[1],
                nullable=row[2] == "YES",
            )
            for row in rows
        ]
        return TableSchema(columns=columns)
