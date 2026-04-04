"""SqlNormalizer protocol — standalone module to avoid circular imports."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


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
