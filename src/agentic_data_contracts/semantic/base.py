"""Semantic source protocol and shared types."""

from __future__ import annotations

from collections import Counter, deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Protocol, runtime_checkable

from thefuzz import fuzz, process

from agentic_data_contracts.adapters.base import TableSchema


@dataclass
class Decomposition:
    """An arithmetic identity: how a metric is reconstructed from other metrics."""

    operator: str  # "sum" | "product" | "ratio" | "difference"
    operands: list[str] = field(default_factory=list)


@dataclass
class DrillDimension:
    """A dimension a metric can be exhaustively sliced by. List order = priority."""

    dimension: str
    column: str  # "schema.table.column" — same convention as Relationship endpoints


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
    # Owners are teams, not individuals (convention, not validated): the
    # business owner owns the definition + review cadence; the operational
    # owner owns data health. ``last_reviewed`` drives staleness detection.
    business_owner: str | None = None
    operational_owner: str | None = None
    last_reviewed: date | None = None
    decompositions: list[Decomposition] = field(default_factory=list)
    drill_by: list[DrillDimension] = field(default_factory=list)


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

    @property
    def kind(self) -> str:
        return "influence"


@dataclass
class IdentityEdge:
    """A directed identity edge parent -> operand in the metric graph."""

    from_metric: str  # parent metric
    to_metric: str  # operand metric
    operator: str  # the decomposition operator that produced this edge

    @property
    def kind(self) -> str:
        return "identity"


MetricEdge = MetricImpact | IdentityEdge


def identity_edges_from_metrics(
    metrics: list[MetricDefinition],
) -> list[IdentityEdge]:
    """Flatten each metric's decompositions into directed identity edges.

    For every operand of every decomposition, emit one edge
    ``parent -> operand`` carrying the decomposition's operator. Leaf metrics
    (no decompositions) contribute nothing.
    """
    edges: list[IdentityEdge] = []
    for metric in metrics:
        for decomp in metric.decompositions:
            for operand in decomp.operands:
                edges.append(
                    IdentityEdge(
                        from_metric=metric.name,
                        to_metric=operand,
                        operator=decomp.operator,
                    )
                )
    return edges


VALID_OPERATORS = frozenset({"sum", "product", "ratio", "difference"})
_BINARY_OPERATORS = frozenset({"ratio", "difference"})


def validate_decompositions(metrics: list[MetricDefinition]) -> None:
    """Validate every declared decomposition; raise ``ValueError`` on any fault.

    Optional to declare, validated only when present. Checks operator, operand
    arity, operand resolution, and that the identity edges form a DAG (a metric
    cannot transitively decompose into itself). Leaf metrics pass untouched.
    """
    names = {m.name for m in metrics}
    adjacency: dict[str, list[str]] = {}
    for metric in metrics:
        for decomp in metric.decompositions:
            if decomp.operator not in VALID_OPERATORS:
                raise ValueError(
                    f"metric {metric.name!r} decomposition has unknown operator "
                    f"{decomp.operator!r}; expected one of {sorted(VALID_OPERATORS)}"
                )
            count = len(decomp.operands)
            if decomp.operator in _BINARY_OPERATORS and count != 2:
                raise ValueError(
                    f"metric {metric.name!r} decomposition {decomp.operator!r} "
                    f"requires exactly 2 operands, got {count}"
                )
            if decomp.operator not in _BINARY_OPERATORS and count < 2:
                raise ValueError(
                    f"metric {metric.name!r} decomposition {decomp.operator!r} "
                    f"requires at least 2 operands, got {count}"
                )
            for operand in decomp.operands:
                if operand == metric.name:
                    raise ValueError(
                        f"metric {metric.name!r} decomposition cannot reference itself"
                    )
                if operand not in names:
                    raise ValueError(
                        f"metric {metric.name!r} decomposition references "
                        f"unknown metric {operand!r}"
                    )
            adjacency.setdefault(metric.name, []).extend(decomp.operands)
    _assert_identity_acyclic(adjacency)


def _assert_identity_acyclic(adjacency: dict[str, list[str]]) -> None:
    """DFS the identity graph; raise ``ValueError`` naming the first cycle found."""
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str, stack: list[str]) -> None:
        if node in visiting:
            cycle = " -> ".join([*stack, node])
            raise ValueError(f"metric decomposition cycle detected: {cycle}")
        if node in visited:
            return
        visiting.add(node)
        for neighbor in adjacency.get(node, []):
            visit(neighbor, [*stack, node])
        visiting.discard(node)
        visited.add(node)

    for node in list(adjacency):
        visit(node, [])


def validate_drill_by(
    metrics: list[MetricDefinition],
    table_schemas: dict[str, TableSchema],
) -> None:
    """Validate drill-by column references.

    A column is ``"schema.table.column"``. The ``schema.table`` portion keys
    into *table_schemas*. Column existence is checked only when that table is
    declared; when the table is absent (schemas are optional in these
    contracts), the check is skipped silently. A malformed reference (no
    ``schema.table.column`` shape) always raises.
    """
    for metric in metrics:
        for drill in metric.drill_by:
            table_key, _, column = drill.column.rpartition(".")
            if not table_key or not column or "." not in table_key:
                raise ValueError(
                    f"metric {metric.name!r} drill_by column {drill.column!r} "
                    f"must be 'schema.table.column'"
                )
            schema = table_schemas.get(table_key)
            if schema is None:
                continue  # table not declared — soft skip
            if not any(col.name == column for col in schema.columns):
                raise ValueError(
                    f"metric {metric.name!r} drill_by references unknown column "
                    f"{drill.column!r}"
                )


@runtime_checkable
class SemanticSource(Protocol):
    def get_metrics(self) -> list[MetricDefinition]: ...
    def get_metric(self, name: str) -> MetricDefinition | None: ...
    def get_table_schema(self, schema: str, table: str) -> TableSchema | None: ...
    def get_table_schemas(self) -> dict[str, TableSchema]: ...
    def search_metrics(self, query: str) -> list[MetricDefinition]: ...
    def get_relationships(self) -> list[Relationship]: ...
    def get_relationships_for_table(self, table: str) -> list[Relationship]: ...
    def get_metric_impacts(self) -> list[MetricImpact]: ...


@runtime_checkable
class ExtensibleSemanticSource(Protocol):
    """A source carrying consumer-authored sections the framework never reads.

    Deliberately a *sibling* of :class:`SemanticSource` rather than an extension
    of it. ``runtime_checkable`` isinstance checks method *presence*, so folding
    ``get_extras`` into ``SemanticSource`` would make every external custom
    source fail ``isinstance(src, SemanticSource)`` on upgrade. A sibling breaks
    nobody and stays fully typed.

    The framework carries extras and, on request, places them in the prompt. It
    does not interpret, validate, index, or compute over their content — the
    boundary the ``verified-examples`` corpus already draws in
    ``validation/examples.py``.
    """

    def get_extras(self) -> dict[str, Any]: ...


def _iso(d: date | None) -> str | None:
    """ISO-format a date, passing ``None`` through."""
    return d.isoformat() if d is not None else None


def dump_semantic_source(source: SemanticSource) -> dict[str, Any]:
    """Serialize a source's enumerable semantics into the YAML-source raw format.

    The inverse of :meth:`YamlSource.from_raw`: lets a contract freeze its
    semantics inline (see :meth:`DataContract.freeze_semantic_source`) and
    rebuild them on a consumer with no file access. Captures metrics,
    relationships, metric impacts, and table column-schemas (name, type, and
    authored description — column ``nullable`` is not carried by the YAML-source
    format and defaults on rehydrate).
    """

    def _dump_metric(m: MetricDefinition) -> dict[str, Any]:
        data: dict[str, Any] = {
            "name": m.name,
            "description": m.description,
            "sql_expression": m.sql_expression,
            "source_model": m.source_model,
            "filters": list(m.filters),
            "domains": list(m.domains),
            "tier": list(m.tier),
            "indicator_kind": m.indicator_kind,
            "business_owner": m.business_owner,
            "operational_owner": m.operational_owner,
            "last_reviewed": _iso(m.last_reviewed),
        }
        # Omit when empty: a leaf metric's dump stays byte-identical to the
        # pre-0.28 format, so a frozen contract's ``contract_digest`` is stable
        # across the upgrade, and it matches the omit-when-empty convention the
        # tools layer already uses (``_metric_details``).
        if m.decompositions:
            data["decompositions"] = [
                {"operator": d.operator, "operands": list(d.operands)}
                for d in m.decompositions
            ]
        if m.drill_by:
            data["drill_by"] = [
                {"dimension": dd.dimension, "column": dd.column} for dd in m.drill_by
            ]
        return data

    tables: list[dict[str, Any]] = []
    for key, ts in source.get_table_schemas().items():
        schema_name, _, table_name = key.partition(".")
        tables.append(
            {
                "schema": schema_name,
                "table": table_name,
                "columns": [
                    {"name": c.name, "type": c.type, "description": c.description}
                    for c in ts.columns
                ],
            }
        )

    payload: dict[str, Any] = {
        "tables": tables,
        "metrics": [_dump_metric(m) for m in source.get_metrics()],
        "relationships": [
            {
                "from": r.from_,
                "to": r.to,
                "type": r.type,
                "description": r.description,
                "required_filter": r.required_filter,
                "preferred": r.preferred,
            }
            for r in source.get_relationships()
        ],
        "metric_impacts": [
            {
                "from": i.from_metric,
                "to": i.to_metric,
                "direction": i.direction,
                "confidence": i.confidence,
                "evidence": i.evidence,
                "description": i.description,
                "last_reviewed": _iso(i.last_reviewed),
            }
            for i in source.get_metric_impacts()
        ],
    }
    # Empty extras update nothing, which is why a contract without them still
    # dumps byte-identically and no published ARD digest moves — the same
    # omit-when-empty reasoning as decompositions.
    #
    # ``YamlSource`` cannot collide here (its extras are the complement of
    # ``SEMANTIC_KEYS``), but ``ExtensibleSemanticSource`` is public and
    # top-level exported, so a third-party implementation returning ``metrics``
    # would silently replace the dumped vocabulary — and ``contract_digest``
    # would then attest to the corrupted payload. Checked against
    # ``payload.keys()`` rather than ``SEMANTIC_KEYS``: that is the invariant
    # that actually matters here, and importing the constant from
    # ``yaml_source`` would be circular.
    if isinstance(source, ExtensibleSemanticSource):
        extras = source.get_extras()
        if clash := sorted(extras.keys() & payload.keys()):
            raise ValueError(
                f"{type(source).__name__}.get_extras() returned key(s) {clash},"
                " which collide with the semantic vocabulary this dump already"
                " carries. Extras are uninterpreted carriage and must not shadow"
                " metrics/tables/relationships/metric_impacts — the frozen"
                " contract and its digest would attest to the overwritten value."
            )
        payload.update(extras)
    return payload


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


def metrics_in_domain(
    metrics: list[MetricDefinition],
    domain_name: str,
) -> list[MetricDefinition]:
    """Metrics that self-declare membership in *domain_name*.

    Domain membership is metric-first: each metric lists its domains
    (``MetricDefinition.domains``), and every adapter (yaml/dbt/cube) populates
    that field from the source's ``meta.domains``.  This reverse-lookup is the
    canonical way to enumerate a domain's members — the contract's ``Domain``
    object carries only catalog metadata (summary, owners, review cadence), not
    a membership list.  Declaration order of *metrics* is preserved.
    """
    return [m for m in metrics if domain_name in m.domains]


def domain_metric_counts(metrics: list[MetricDefinition]) -> Counter[str]:
    """Count members per domain in one pass, for callers needing many domains.

    Each metric contributes at most once per domain (duplicate ``domains`` tags
    on a single metric are de-duplicated), so ``counts[d]`` always equals
    ``len(metrics_in_domain(metrics, d))``.  Returns a :class:`Counter`, so an
    absent domain reads as ``0`` rather than raising.
    """
    counts: Counter[str] = Counter()
    for m in metrics:
        counts.update(set(m.domains))
    return counts


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
    impacts: Sequence[MetricEdge],
) -> dict[str, list[MetricEdge]]:
    """Build a metric-name -> impact edges index for O(1) lookup.

    Each impact is indexed under both its ``from_metric`` and ``to_metric``
    (unless they are the same), mirroring :func:`build_relationship_index`.
    Walk direction is disambiguated at traversal time by checking
    ``edge.from_metric`` / ``edge.to_metric`` against the current node.

    Edges within each entry are in declaration order; callers should not
    rely on any stronger ordering. Accepts a mix of influence (``MetricImpact``)
    and identity (``IdentityEdge``) edges.
    """
    index: dict[str, list[MetricEdge]] = {}
    for imp in impacts:
        index.setdefault(imp.from_metric, []).append(imp)
        if imp.from_metric != imp.to_metric:
            index.setdefault(imp.to_metric, []).append(imp)
    return index


def walk_metric_impacts(
    index: dict[str, list[MetricEdge]],
    start: str,
    *,
    direction: str,
    max_depth: int = 2,
) -> list[tuple[int, MetricEdge]]:
    """BFS through the metric impact graph from ``start``.

    ``direction="downstream"`` follows edges where ``edge.from_metric ==
    current`` — returns metrics impacted *by* ``start``.  ``direction=
    "upstream"`` follows edges where ``edge.to_metric == current`` —
    returns metrics that *drive* ``start``.

    Returns ``(depth, edge)`` pairs in BFS order, where depth is the number
    of hops from ``start`` (direct neighbors at depth 1).  Visited tracking
    prevents cycles, so each reachable metric appears at most once. Works
    over a mixed influence + identity edge index.
    """
    if direction not in ("upstream", "downstream"):
        msg = f"direction must be 'upstream' or 'downstream', got {direction!r}"
        raise ValueError(msg)

    visited: set[str] = {start}
    result: list[tuple[int, MetricEdge]] = []
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
