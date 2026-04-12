"""Semantic source protocol and shared types."""

from __future__ import annotations

from collections import deque
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
    description: str = ""
    required_filter: str | None = None


@runtime_checkable
class SemanticSource(Protocol):
    def get_metrics(self) -> list[MetricDefinition]: ...
    def get_metric(self, name: str) -> MetricDefinition | None: ...
    def get_table_schema(self, schema: str, table: str) -> TableSchema | None: ...
    def search_metrics(self, query: str) -> list[MetricDefinition]: ...
    def get_relationships(self) -> list[Relationship]: ...
    def get_relationships_for_table(self, table: str) -> list[Relationship]: ...


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


def build_relationship_index(
    relationships: list[Relationship],
) -> dict[str, list[Relationship]]:
    """Build a table-name -> relationships index for O(1) lookup.

    Each relationship is indexed under both its ``from`` and ``to`` table
    (the table portion of "schema.table.column"), unless they are the same
    table (self-referencing FK).
    """
    index: dict[str, list[Relationship]] = {}
    for r in relationships:
        from_table = r.from_.rsplit(".", 1)[0]
        to_table = r.to.rsplit(".", 1)[0]
        index.setdefault(from_table, []).append(r)
        if from_table != to_table:
            index.setdefault(to_table, []).append(r)
    return index


def find_join_path(
    index: dict[str, list[Relationship]],
    from_table: str,
    to_table: str,
    *,
    max_hops: int = 3,
) -> list[Relationship] | None:
    """BFS shortest path between two tables in the relationship graph.

    Returns the list of Relationship edges forming the path, or ``None``
    if no path exists within *max_hops*.  Returns ``[]`` when
    *from_table* == *to_table*.
    """
    if from_table == to_table:
        return []
    visited: set[str] = {from_table}
    queue: deque[tuple[str, list[Relationship]]] = deque([(from_table, [])])
    while queue:
        current, path = queue.popleft()
        if len(path) >= max_hops:
            continue
        for rel in index.get(current, []):
            from_t = rel.from_.rsplit(".", 1)[0]
            to_t = rel.to.rsplit(".", 1)[0]
            neighbor = to_t if from_t == current else from_t
            if neighbor == to_table:
                return path + [rel]
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, path + [rel]))
    return None
