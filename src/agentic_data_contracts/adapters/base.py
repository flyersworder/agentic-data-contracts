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


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[tuple[Any, ...]]
    row_count: int = 0

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


@runtime_checkable
class SqlNormalizer(Protocol):
    """Rewrite database-specific SQL into a form sqlglot can parse.

    Called by the Validator before AST parsing. Adapters for non-standard
    dialects implement this alongside DatabaseAdapter. Standard-dialect
    adapters do not need to implement this — the Validator treats its
    absence as a no-op.

    The original (un-normalized) SQL is still passed to execute() and explain().
    """

    def normalize_sql(self, sql: str) -> str: ...
