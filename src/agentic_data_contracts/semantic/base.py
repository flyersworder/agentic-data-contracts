"""Semantic source protocol and shared types."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
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
    domains: list[str] = field(default_factory=list)
    tier: list[str] = field(default_factory=list)
    indicator_kind: str | None = None


@dataclass
class Relationship:
    from_: str  # "schema.table.column"
    to: str  # "schema.table.column"
    type: str = "many_to_one"  # many_to_one | one_to_one | many_to_many
    description: str = ""
    required_filter: str | None = None
    preferred: bool = False


@dataclass
class MetricImpact:
    """A directed, annotated edge in the metric-driver graph."""

    from_metric: str  # source metric name
    to_metric: str  # affected metric name
    direction: str = "positive"  # "positive" | "negative"
    confidence: str = "hypothesized"  # "verified" | "correlated" | "hypothesized"
    evidence: str = ""  # free text, human- and agent-citable
    description: str = ""
    last_reviewed: date | None = None


@runtime_checkable
class SemanticSource(Protocol):
    def get_metrics(self) -> list[MetricDefinition]: ...
    def get_metric(self, name: str) -> MetricDefinition | None: ...
    def get_table_schema(self, schema: str, table: str) -> TableSchema | None: ...
    def search_metrics(self, query: str) -> list[MetricDefinition]: ...
    def get_relationships(self) -> list[Relationship]: ...
    def get_relationships_for_table(self, table: str) -> list[Relationship]: ...
    def get_metric_impacts(self) -> list[MetricImpact]: ...


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

    Each adjacency list is stable-sorted with ``preferred=True`` edges first,
    so BFS path-finding and direct table lookup both surface the canonical
    join when alternatives exist. The flat list returned by
    ``SemanticSource.get_relationships()`` deliberately keeps declaration
    order — that list feeds the prompt renderer, where ``preferred="true"``
    is rendered as a per-edge attribute instead of via reordering.
    """
    index: dict[str, list[Relationship]] = {}
    for r in relationships:
        from_table = r.from_.rsplit(".", 1)[0]
        to_table = r.to.rsplit(".", 1)[0]
        index.setdefault(from_table, []).append(r)
        if from_table != to_table:
            index.setdefault(to_table, []).append(r)
    for edges in index.values():
        edges.sort(key=lambda r: not r.preferred)
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


def build_metric_impact_index(
    impacts: list[MetricImpact],
) -> dict[str, list[MetricImpact]]:
    """Build a metric-name -> impact edges index for O(1) lookup.

    Each impact is indexed under both its ``from_metric`` and ``to_metric``
    (unless they are the same), mirroring :func:`build_relationship_index`.
    Walk direction is disambiguated at traversal time by checking
    ``edge.from_metric`` / ``edge.to_metric`` against the current node.

    Edges within each entry are in declaration order; callers should not
    rely on any stronger ordering.
    """
    index: dict[str, list[MetricImpact]] = {}
    for imp in impacts:
        index.setdefault(imp.from_metric, []).append(imp)
        if imp.from_metric != imp.to_metric:
            index.setdefault(imp.to_metric, []).append(imp)
    return index


def walk_metric_impacts(
    index: dict[str, list[MetricImpact]],
    start: str,
    *,
    direction: str,
    max_depth: int = 2,
) -> list[tuple[int, MetricImpact]]:
    """BFS through the metric impact graph from ``start``.

    ``direction="downstream"`` follows edges where ``edge.from_metric ==
    current`` — returns metrics impacted *by* ``start``.  ``direction=
    "upstream"`` follows edges where ``edge.to_metric == current`` —
    returns metrics that *drive* ``start``.

    Returns ``(depth, edge)`` pairs in BFS order, where depth is the number
    of hops from ``start`` (direct neighbors at depth 1).  Visited tracking
    prevents cycles, so each reachable metric appears at most once.
    """
    if direction not in ("upstream", "downstream"):
        msg = f"direction must be 'upstream' or 'downstream', got {direction!r}"
        raise ValueError(msg)

    visited: set[str] = {start}
    result: list[tuple[int, MetricImpact]] = []
    queue: deque[tuple[str, int]] = deque([(start, 0)])
    while queue:
        current, depth = queue.popleft()
        if depth >= max_depth:
            continue
        for edge in index.get(current, []):
            if direction == "downstream":
                # Only follow edges leaving `current`.
                if edge.from_metric != current:
                    continue
                neighbor = edge.to_metric
            else:
                # Only follow edges arriving at `current`.
                if edge.to_metric != current:
                    continue
                neighbor = edge.from_metric
            if neighbor in visited:
                continue
            result.append((depth + 1, edge))
            visited.add(neighbor)
            queue.append((neighbor, depth + 1))
    return result
