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
            self.connection.execute(f"EXPLAIN {sql}")
            return ExplainResult(
                estimated_cost_usd=None,
                estimated_rows=None,
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
