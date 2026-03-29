"""Semantic source protocol and shared types."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from thefuzz import fuzz, process

from agentic_data_contracts.adapters.base import TableSchema


@dataclass
class MetricDefinition:
    name: str
    description: str
    sql_expression: str
    source_model: str = ""
    filters: list[str] = field(default_factory=list)


@dataclass
class Relationship:
    from_: str  # "schema.table.column"
    to: str  # "schema.table.column"
    type: str = "many_to_one"  # many_to_one | one_to_one | many_to_many


@runtime_checkable
class SemanticSource(Protocol):
    def get_metrics(self) -> list[MetricDefinition]: ...
    def get_metric(self, name: str) -> MetricDefinition | None: ...
    def get_table_schema(self, schema: str, table: str) -> TableSchema | None: ...
    def search_metrics(self, query: str) -> list[MetricDefinition]: ...
    def get_relationships(self) -> list[Relationship]: ...


def fuzzy_search_metrics(
    metrics: list[MetricDefinition],
    get_metric: Callable[[str], MetricDefinition | None],
    query: str,
    *,
    score_cutoff: int = 50,
    limit: int = 5,
) -> list[MetricDefinition]:
    """Fuzzy search over metrics using thefuzz token_set_ratio."""
    if not metrics:
        return []
    choices = {m.name: f"{m.name} {m.description}" for m in metrics}
    results = process.extractBests(
        query,
        choices,
        scorer=fuzz.token_set_ratio,
        score_cutoff=score_cutoff,
        limit=limit,
    )
    return [m for _, _, key in results if (m := get_metric(key)) is not None]
