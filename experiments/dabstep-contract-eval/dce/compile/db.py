"""The compiler's database access: read-only, no file access, bounded.

The compiler may query the data, as an engineer writing a contract would,
but nothing else: the connection is read-only, `enable_external_access` is
off (so `read_text` and `glob` cannot reach the gold files that sit beside
LiveSQLBench's databases), results are capped at `MAX_ROWS`, and every
statement is interrupted at `QUERY_SECONDS`.
"""

from __future__ import annotations

import csv
import io
import threading
from pathlib import Path

import duckdb

MAX_ROWS = 50
QUERY_SECONDS = 60.0


class CompileDB:
    def __init__(self, path: Path, *, init_sql: tuple[str, ...] = ()) -> None:
        self.path = path
        self._con = duckdb.connect(str(path), read_only=True)
        try:
            for statement in (*init_sql, "SET enable_external_access = false"):
                self._con.execute(statement)
        except Exception:
            self._con.close()
            raise

    def close(self) -> None:
        self._con.close()

    def _execute(self, sql: str):
        timer = threading.Timer(QUERY_SECONDS, self._con.interrupt)
        timer.start()
        try:
            return self._con.execute(sql)
        finally:
            timer.cancel()

    def tables(self) -> dict[str, list[tuple[str, str]]]:
        rows = self._con.execute(
            "SELECT table_name, column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'main' ORDER BY table_name, ordinal_position"
        ).fetchall()
        out: dict[str, list[tuple[str, str]]] = {}
        for table, column, data_type in rows:
            out.setdefault(table, []).append((column, data_type))
        return out

    def ddl(self) -> str:
        return "\n\n".join(
            f"CREATE TABLE {table} (\n"
            + ",\n".join(f"  {column} {data_type}" for column, data_type in columns)
            + "\n);"
            for table, columns in self.tables().items()
        )

    def run_sql(self, sql: str) -> str:
        try:
            cursor = self._execute(sql)
            columns = [d[0] for d in cursor.description or []]
            rows = cursor.fetchmany(MAX_ROWS + 1)
        except duckdb.Error as exc:
            return f"ERROR: {exc}"
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(columns)
        writer.writerows(rows[:MAX_ROWS])
        text = buf.getvalue().rstrip("\n")
        if len(rows) > MAX_ROWS:
            text += f"\n... truncated at {MAX_ROWS} rows"
        return text

    def check_metric(self, expr: str, source: str, *, predicate: bool) -> str | None:
        """None when the metric runs over `source`: a calculation as
        `SELECT <expr> FROM <source>`, a predicate as a `WHERE` clause."""
        sql = (
            f"SELECT * FROM {source} WHERE {expr} LIMIT 5"
            if predicate
            else f"SELECT {expr} FROM {source} LIMIT 5"
        )
        try:
            self._execute(sql).fetchall()
        except duckdb.Error as exc:
            return str(exc)
        return None
