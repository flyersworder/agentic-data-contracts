"""EXPLAIN dry-run types and protocol."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@dataclass
class ExplainResult:
    estimated_cost_usd: float | None
    estimated_rows: int | None
    schema_valid: bool
    errors: list[str] = field(default_factory=list)


@runtime_checkable
class ExplainAdapter(Protocol):
    def explain(self, sql: str) -> ExplainResult: ...
