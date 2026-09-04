"""Semantic source protocol and shared types."""

from __future__ import annotations

from collections import Counter, deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from typing import Any, Protocol, runtime_checkable

from thefuzz import fuzz, process

from agentic_data_contracts.adapters.base import TableSchema


@dataclass
class Decomposition:
    """An arithmetic identity: how a metric is reconstructed from other metrics.

    Operands must be in units the declared operator composes — this is not
    validated, and cannot be without expression semantics the semantic layer
    does not carry. A rate declared as a rounded percentage makes a ``product``
    identity false by ~100x. See ``reconcile_decomposition`` for how an operand
    declared at limited precision interacts with its tolerance.

    ``convention`` names where the cross term goes when the identity's *change*
    is attributed to its factors. It is a non-inferable business fact: an agent
    told only the factors will pick a placement silently and present it as
    canonical, and two such reports are indistinguishable because both sum
    correctly. Only ``product`` and ``ratio`` have a cross term.
    """

    operator: str  # "sum" | "product" | "ratio" | "difference"
    operands: list[str] = field(default_factory=list)
    convention: str | None = None  # None = undeclared, agent picks (status quo)
    convention_operand: str | None = None  # required iff convention == "fold_into"


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
    """A directed identity edge parent -> operand in the metric graph.

    This is the *canonical* orientation, and it is the opposite of a
    ``MetricImpact``'s: an influence edge points driver -> affected, while an
    operand that drives its parent points parent -> operand. Any graph holding
    both kinds must reconcile them before it can answer "what drives X" by
    topology; :meth:`as_driver_edge` is that reconciliation.
    """

    from_metric: str  # parent metric
    to_metric: str  # operand metric
    operator: str  # the decomposition operator that produced this edge
    # Carried from the producing decomposition so a root-cause walk that never
    # calls lookup_metric still learns where the cross term goes. Every edge
    # from one decomposition shares the pair.
    convention: str | None = None
    convention_operand: str | None = None

    @property
    def kind(self) -> str:
        return "identity"

    def as_driver_edge(self) -> IdentityEdge:
        """This edge re-pointed operand -> parent, for a driver-graph walk.

        Returns a new edge; the canonical record is left alone, because
        ``lookup_metric`` and ``reconcile_decomposition`` read the parent off
        ``from_metric``. Only the traversal in ``trace_metric_impacts`` wants
        the other orientation, and only so that ``direction="upstream"`` means
        "drivers" for identity and influence edges alike.
        """
        return replace(self, from_metric=self.to_metric, to_metric=self.from_metric)


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
                        convention=decomp.convention,
                        convention_operand=decomp.convention_operand,
                    )
                )
    return edges


def parse_review_date(value: Any) -> date | None:
    """Accept a YAML-native date/datetime, an ISO-8601 string, or None.

    ``datetime`` is checked before ``date`` because it subclasses ``date`` — a
    YAML scalar with a time component (``2020-01-01 12:00:00``) parses to
    ``datetime`` and must be normalised to ``date``, otherwise downstream
    ``date - datetime`` staleness arithmetic raises ``TypeError``.

    Shared rather than per-source: every format that can carry a review date
    reaches the same ``last_reviewed`` field and the same staleness
    arithmetic, so a second copy of this would be a second chance to get the
    subclass ordering wrong.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(
                f"last_reviewed must be an ISO date (YYYY-MM-DD), got {value!r}"
            ) from exc
    raise TypeError(
        f"last_reviewed must be a date or ISO string, "
        f"got {type(value).__name__}: {value!r}"
    )


def jsonify_extras(extras: dict[str, Any], *, source: str) -> dict[str, Any]:
    """Return *extras* with dates ISO-coerced and JSON-safety enforced.

    Extras ride inside ``SemanticSource.inline`` and therefore through
    ``contract_canonical_bytes``' ``json.dumps``. A YAML-native date is not
    JSON, and the guidance extras exist to carry explicitly wants one
    ("verified against the database on ..."). Checking at load means a bad
    value fails where it was authored, rather than months later inside an ARD
    publish. *source* names the parser in the error, since every source that
    carries extras shares this path.
    """
    return {k: _jsonify(v, (k,), source) for k, v in extras.items()}


def _jsonify(value: Any, path: tuple[str | int, ...], source: str) -> Any:
    """Recursively coerce *value* to JSON-safe types, or raise naming *path*.

    ``datetime`` is checked before ``date`` because it subclasses ``date`` —
    the same ordering trap ``parse_review_date`` documents.
    """
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonify(v, (*path, str(k)), source) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonify(v, (*path, i), source) for i, v in enumerate(value)]
    if value is None or isinstance(value, str | int | float):
        return value
    raise ValueError(
        f"{source}: extras value at {_fmt_path(path)} is not JSON-serializable"
        f" ({type(value).__name__}). Extras are carried into the frozen contract"
        f" and its digest, so they must be JSON-safe."
    )


def _fmt_path(path: tuple[str | int, ...]) -> str:
    """Render a key path as ``section[0].field`` for an actionable error."""
    rendered = str(path[0])
    for part in path[1:]:
        rendered += f"[{part}]" if isinstance(part, int) else f".{part}"
    return rendered


VALID_OPERATORS = frozenset({"sum", "product", "ratio", "difference"})
_BINARY_OPERATORS = frozenset({"ratio", "difference"})
VALID_CONVENTIONS = frozenset({"explicit", "split_evenly", "fold_into"})
#: Only these have a ``ΔC·ΔP`` cross term to place. ``sum`` and ``difference``
#: are linear, so a convention on them states nothing.
_CROSS_TERM_OPERATORS = frozenset({"product", "ratio"})


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
            _validate_convention(metric.name, decomp)
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


def _validate_convention(metric_name: str, decomp: Decomposition) -> None:
    """Validate a decomposition's attribution convention; raise on any fault.

    Undeclared is valid — it is the pre-0.43 state and means the agent picks.
    """
    if decomp.convention is None:
        if decomp.convention_operand is not None:
            raise ValueError(
                f"metric {metric_name!r} decomposition sets 'convention_operand' "
                f"but declares no convention; it is only meaningful with "
                f"convention 'fold_into'"
            )
        return
    if decomp.convention not in VALID_CONVENTIONS:
        raise ValueError(
            f"metric {metric_name!r} decomposition has unknown attribution "
            f"convention {decomp.convention!r}; expected one of "
            f"{sorted(VALID_CONVENTIONS)}"
        )
    if decomp.operator not in _CROSS_TERM_OPERATORS:
        raise ValueError(
            f"metric {metric_name!r} decomposition {decomp.operator!r} has no "
            f"cross term to place, so convention {decomp.convention!r} states "
            f"nothing; conventions apply to {sorted(_CROSS_TERM_OPERATORS)}"
        )
    if decomp.convention == "fold_into":
        if decomp.convention_operand is None:
            raise ValueError(
                f"metric {metric_name!r} decomposition convention 'fold_into' "
                f"requires 'convention_operand' naming which operand absorbs "
                f"the cross term"
            )
        if decomp.convention_operand not in decomp.operands:
            raise ValueError(
                f"metric {metric_name!r} decomposition convention_operand "
                f"{decomp.convention_operand!r} is not one of its operands "
                f"{list(decomp.operands)}"
            )
    elif decomp.convention_operand is not None:
        raise ValueError(
            f"metric {metric_name!r} decomposition sets 'convention_operand' "
            f"with convention {decomp.convention!r}; it is only meaningful "
            f"with 'fold_into'"
        )


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


# Shared by every source that can carry a house convention, for the reason
# ``parse_review_date`` records just above: both ``YamlSource`` and
# ``OssieSource`` resolve a default onto the same ``Decomposition.convention``
# field, and a second copy would be a second chance to get the
# fold_into-is-not-a-default rule wrong.
def _parse_convention_default(raw: Any) -> str | None:
    """Read and validate the source-level ``decomposition_convention`` block.

    ``fold_into`` is rejected: it names an operand, and no operand name is
    meaningful across metrics. Declaring it source-wide is always a mistake.
    """
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError(
            "decomposition_convention must be a mapping with a 'convention' key,"
            f" got {type(raw).__name__}"
        )
    convention = raw.get("convention")
    if convention is None:
        raise ValueError("decomposition_convention must set 'convention'")
    if convention not in VALID_CONVENTIONS:
        raise ValueError(
            f"decomposition_convention has unknown attribution convention"
            f" {convention!r}; expected one of {sorted(VALID_CONVENTIONS)}"
        )
    if convention == "fold_into":
        raise ValueError(
            "convention 'fold_into' cannot be a source-level default: it names"
            " an operand, and no operand name is meaningful across metrics."
            " Declare it per decomposition."
        )
    return convention


def _apply_convention_default(
    metrics: list[MetricDefinition], default: str | None
) -> None:
    """Stamp *default* onto every cross-term decomposition that declares none.

    Resolved at load rather than carried, so the effective value survives
    ``freeze_semantic_source`` (which re-serializes from parsed objects) and a
    frozen contract states its convention outright instead of leaving a
    consumer to re-derive it. Linear operators are skipped: a convention on
    them fails ``validate_decompositions``.
    """
    if default is None:
        return
    for metric in metrics:
        for decomp in metric.decompositions:
            if decomp.convention is None and decomp.operator in _CROSS_TERM_OPERATORS:
                decomp.convention = default


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


def as_text(value: Any, default: str = "") -> str:
    """Coerce a source document's value into the ``str`` its field is annotated.

    ``.get(key, default)`` defends against a key's *absence*, and the hazard is a
    key present with no value: a bare ``description:`` in YAML, an explicit
    ``"description": null`` in a dbt manifest. Both load as ``None`` and land in
    a field annotated ``str``, so a consumer calling ``.strip()`` on public API
    gets an ``AttributeError`` from data that parsed without complaint.

    Non-string scalars are coerced rather than passed through: a YAML
    ``description: 2024`` puts an ``int`` in the same ``str`` field, which is the
    same violation with a different shape.

    Fields annotated ``str | None`` -- ``convention``, ``required_filter``,
    ``indicator_kind``, the owners -- deliberately do **not** go through here.
    There ``None`` is a meaningful value, not a malformed one.
    """
    if value is None:
        return default
    return value if isinstance(value, str) else str(value)


def require_text(value: Any, *, where: str) -> str:
    """Read an *identity* field, refusing a blank one.

    The counterpart to :func:`as_text`, and the distinction is the point. A null
    ``description:`` coerced to ``""`` costs a sentence. A null ``name:`` coerced
    to ``""`` produces a metric that loads clean, renders into the prompt, and is
    unfindable by any name a caller would use — the silent-drop shape #89 exists
    to eliminate, reintroduced by the fix for it.

    Applies to what a thing *is called* or *points at*: a metric or column name,
    a relationship or impact endpoint. *where* names the offending entry, since a
    parser error a consumer cannot locate is barely better than silence.
    """
    text = as_text(value).strip()
    if not text:
        raise ValueError(
            f"{where} must name a non-empty value, got {value!r}. An identity"
            " that is blank cannot be looked up, referenced, or rendered."
        )
    return text


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
            decompositions: list[dict[str, Any]] = []
            for d in m.decompositions:
                entry: dict[str, Any] = {
                    "operator": d.operator,
                    "operands": list(d.operands),
                }
                # Omitted when unset for the same reason ``decompositions``
                # itself is omitted when empty: a contract that declares no
                # convention must keep byte-identical canonical bytes, since
                # ``contract_canonical_bytes`` dumps with no ``exclude_none``
                # and any always-present key moves every published digest.
                if d.convention is not None:
                    entry["convention"] = d.convention
                if d.convention_operand is not None:
                    entry["convention_operand"] = d.convention_operand
                decompositions.append(entry)
            data["decompositions"] = decompositions
        if m.drill_by:
            data["drill_by"] = [
                {"dimension": dd.dimension, "column": dd.column} for dd in m.drill_by
            ]
        return data

    tables: list[dict[str, Any]] = []
    for key, ts in source.get_table_schemas().items():
        schema_name, _, table_name = key.partition(".")
        entry: dict[str, Any] = {
            "schema": schema_name,
            "table": table_name,
            "columns": [
                {"name": c.name, "type": c.type, "description": c.description}
                for c in ts.columns
            ],
        }
        # Omitted when empty, for the reason spelled out on `_dump_metric`'s
        # `decompositions`: `contract_canonical_bytes` dumps with no
        # `exclude_none`, so an always-present key moves every published
        # digest. A contract declaring no table descriptions keeps
        # byte-identical canonical bytes across the release that added them.
        if ts.description:
            entry["description"] = ts.description
        tables.append(entry)

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
    prevents cycles by gating *expansion* only: each reachable metric is
    expanded at most once, while **every declared edge between reached
    metrics within the depth horizon is reported**, including one onto a
    metric another branch already reached. That is what lets a shared driver
    -- one metric that is an operand of two parents -- arrive on both
    branches, carrying the ``operator`` and ``convention`` that only its
    second edge holds.

    "Within the depth horizon" is load-bearing: a node reached at exactly
    ``max_depth`` is dequeued but never expanded, so its outgoing edges are
    not discovered even when the far endpoint was independently reached by
    another path. ``a->b, a->c, b->d, c->e, d->e`` walked from ``a`` at
    ``max_depth=2`` never reports ``d->e``, though both ``d`` and ``e`` are
    reached.

    A consequence worth knowing: cycle-closing edges are reported. In
    ``a -> b -> c -> a`` walked downstream, ``c -> a`` appears. The edge is
    declared and an agent tracing root cause should be told the graph closes.
    Termination is unaffected -- it was never the reporting gate that
    guaranteed it.

    The one exception is *parallel* edges — several edges found while
    expanding a single node that land on the same neighbor. All of them are
    reported, less any that are equal in every field to one already reported
    (identical declarations carry one fact). This matters on a mixed
    influence + identity index, where one
    real relationship can be declared twice (as an impact edge and as a
    decomposition operand): keeping whichever was indexed first would drop
    the operator and attribution convention that only the identity edge
    carries.
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
        # Neighbors reached while expanding `current`, in first-seen order.
        # They join `visited` only after the whole adjacency is scanned, so
        # parallel edges to the same neighbor all report; the dict keeps each
        # neighbor queued once.
        reached: dict[str, None] = {}
        # Edges already reported while expanding `current`, compared by value.
        # Parallel edges are reported for the distinct facts each carries, so
        # two edges equal in every field carry one fact and report once.
        # A list, not a set: MetricEdge dataclasses are unhashable, and one
        # node's adjacency is short.
        seen: list[MetricEdge] = []
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
            if any(edge == prior for prior in seen):
                continue
            result.append((depth + 1, edge))
            seen.append(edge)
            if neighbor not in visited:
                reached[neighbor] = None
        for neighbor in reached:
            visited.add(neighbor)
            queue.append((neighbor, depth + 1))
    return result
