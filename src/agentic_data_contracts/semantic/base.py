"""Semantic source protocol and shared types."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from agentic_data_contracts.adapters.base import TableSchema


@dataclass
class MetricDefinition:
    name: str
    description: str
    sql_expression: str
    source_model: str = ""
    filters: list[str] = field(default_factory=list)


@runtime_checkable
class SemanticSource(Protocol):
    def get_metrics(self) -> list[MetricDefinition]: ...
    def get_metric(self, name: str) -> MetricDefinition | None: ...
    def get_table_schema(self, schema: str, table: str) -> TableSchema | None: ...
