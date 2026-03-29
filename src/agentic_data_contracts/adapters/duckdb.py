"""DuckDB database adapter."""

from __future__ import annotations

import duckdb

from agentic_data_contracts.adapters.base import Column, QueryResult, TableSchema
from agentic_data_contracts.validation.explain import ExplainResult


class DuckDBAdapter:
    """Database adapter for DuckDB."""

    def __init__(self, database: str = ":memory:") -> None:
        self.connection = duckdb.connect(database)

    @property
    def dialect(self) -> str:
        return "duckdb"

    def execute(self, sql: str) -> QueryResult:
        result = self.connection.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return QueryResult(columns=columns, rows=rows)

    def explain(self, sql: str) -> ExplainResult:
        try:
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

    def _parse_row_estimate(self, explain_rows: list[tuple]) -> int | None:
        """Parse DuckDB EXPLAIN output for estimated row count.

        DuckDB EXPLAIN includes lines with ~N indicating estimated cardinality.
        We take the last ~N in the output (top-level node estimate).
        """
        import re

        last_estimate = None
        for row in explain_rows:
            text = str(row[1]) if len(row) > 1 else str(row[0])
            match = re.search(r"~(\d+)", text)
            if match:
                last_estimate = int(match.group(1))
        return last_estimate

    def list_tables(self, schema: str) -> list[str]:
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
