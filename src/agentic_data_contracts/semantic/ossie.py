"""Apache Ossie (incubating) semantic model source.

Reads a model written against the Ossie core spec — the vendor-neutral
YAML/JSON format for metrics, dimensions and joins that began as Open
Semantic Interchange and is now in the Apache Incubator.

Scope, and why it is drawn here
-------------------------------
Ossie standardises what a metric *is*; this library enforces what an agent
may *do* with it. The spec is therefore a strict subset of our semantic
vocabulary: an Ossie ``Metric`` carries only ``name``, ``expression``,
``description``, ``datatype`` and ``ai_context``. Ownership, review dates,
tiers, decompositions, drill-by dimensions and the metric-impact graph have
no home in the spec (they sit on its roadmap as discussions #40 and #53).

Every ``$def`` in ``osi-schema.json`` sets ``additionalProperties: false``,
so those cannot be smuggled in as extra keys. The spec's own escape hatch is
``custom_extensions``, a list of ``{vendor_name, data}`` where ``data`` is a
*JSON string*. We read our own vendor block back into real vocabulary and
carry every other vendor's verbatim through ``get_extras()`` — the same
boundary ``YamlSource`` draws: the framework carries extras and, on request,
places them in the prompt, but never interprets them.

Churn
-----
The spec is ``0.2.0.dev0`` with no tagged releases yet (apache/ossie#102),
and the accepted expression-language proposal adds an ``Ossie_SQL_2026``
dialect that is not in today's enum. So ``dialect`` is treated as an opaque
string here and never validated against the enum — a closed check would
break on the next spec bump for no benefit.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import yaml

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.semantic.base import (
    Decomposition,
    DrillDimension,
    MetricDefinition,
    MetricImpact,
    Relationship,
    build_relationship_index,
    fuzzy_search_metrics,
    jsonify_extras,
    parse_review_date,
    validate_decompositions,
    validate_drill_by,
)
from agentic_data_contracts.semantic.yaml_source import (
    _apply_convention_default,
    _parse_convention_default,
)

logger = logging.getLogger(__name__)

#: Vendor key this project claims in ``custom_extensions``. Ossie's ``Vendor``
#: is a free-form string, so a namespace is claimed by convention, not registry.
OSSIE_VENDOR = "AGENTIC_DATA_CONTRACTS"

#: Dialect preference when the caller names none. ``ANSI_SQL`` is the current
#: portable default; ``Ossie_SQL_2026`` is the one the accepted expression-
#: language proposal promotes to default, listed ahead of its arrival.
_DEFAULT_DIALECT_PREFERENCE = ("ANSI_SQL", "Ossie_SQL_2026")

#: Cardinality by ``(from_columns are a key, to_columns are a key)``. Ossie
#: never states the join type, so it is read off which endpoints are unique.
_CARDINALITY: dict[tuple[bool, bool], str] = {
    (True, True): "one_to_one",
    (False, True): "many_to_one",
    (True, False): "one_to_many",
    (False, False): "many_to_many",
}


def _normalize_ai_context(value: Any) -> dict[str, Any] | None:
    """Coerce Ossie's ``ai_context`` union to its object form.

    The schema allows either a bare string or an object with ``instructions``,
    ``synonyms`` and ``examples``. Normalising at the edge means consumers
    reading ``get_extras()`` never have to branch on the shape.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return {"instructions": value}
    if isinstance(value, dict):
        return dict(value)
    return None


def _as_list(value: Any) -> list[str]:
    """Promote a bare string to a one-element list.

    ``YamlSource`` deliberately accepts ``tier: gold`` alongside
    ``tier: [gold]``; the same authoring slip inside an Ossie vendor block has
    to behave identically. ``list("gold")`` would silently yield
    ``['g', 'o', 'l', 'd']`` and then drive tier policy and domain filtering.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def _looks_like_query(source: str) -> bool:
    """Whether a ``dataset.source`` is a query rather than a table reference.

    Ossie allows either. Whitespace is the discriminator: a table reference is
    a dotted identifier path, and while a quoted identifier *may* contain a
    space, that is rare enough that treating it as a query only costs a
    debug-level log instead of a warning.
    """
    return any(c.isspace() for c in source.strip())


def _table_key(source: str) -> str | None:
    """Reduce an Ossie ``dataset.source`` to our two-part ``schema.table`` key.

    Ossie sources are ``database.schema.table`` or a query. Our keys are
    two-part because ``Relationship`` endpoints are ``schema.table.column``
    and ``build_relationship_index`` recovers the table with one ``rsplit``;
    carrying the database part would make every endpoint four deep and break
    that contract. The database qualifier is therefore dropped, which can
    collide if one model spans two databases with matching schema and table
    names — logged when it happens.

    A query-backed dataset has no physical table to key on and returns None.
    """
    candidate = source.strip()
    if not candidate or any(c.isspace() for c in candidate):
        return None
    parts = candidate.split(".")
    if len(parts) < 2:
        return None
    return ".".join(parts[-2:])


class OssieSource:
    """Loads metric and table definitions from an Apache Ossie semantic model."""

    def __init__(
        self,
        path: str | Path,
        *,
        dialect: str | None = None,
        vendor: str = OSSIE_VENDOR,
    ) -> None:
        """Parse an Ossie model file.

        Args:
            path: Path to the Ossie YAML (or JSON — YAML is a superset).
            dialect: Preferred expression dialect. When a metric or field has
                no expression in it, selection falls back to ``ANSI_SQL``,
                then ``Ossie_SQL_2026``, then the first declared dialect, so a
                model always yields an expression rather than silently none.
            vendor: ``custom_extensions`` vendor name to read back as real
                vocabulary. Everything under any other vendor rides in extras.
        """
        raw = yaml.safe_load(Path(path).read_text()) or {}
        self._dialect_preference: tuple[str, ...] = (
            (dialect, *_DEFAULT_DIALECT_PREFERENCE)
            if dialect
            else _DEFAULT_DIALECT_PREFERENCE
        )
        self._vendor = vendor

        self._metrics: list[MetricDefinition] = []
        self._tables: dict[str, TableSchema] = {}
        self._relationships: list[Relationship] = []
        self._metric_impacts: list[MetricImpact] = []
        # Both keyed by semantic-model name first. Ossie namespaces entity
        # names per model and its top level is a *list*, so two models in
        # one file may each declare a `customer` dataset or a block from
        # the same vendor. A flat key would silently drop the first.
        self._ai_context: dict[str, dict[str, dict[str, Any]]] = {}
        self._foreign_extensions: dict[str, dict[str, Any]] = {}

        for model in raw.get("semantic_model", []) or []:
            self._load_model(model)

        self._rel_index = build_relationship_index(self._relationships)
        validate_decompositions(self._metrics)
        validate_drill_by(self._metrics, self._tables)

    # -- parsing ----------------------------------------------------------

    def _load_model(self, model: dict[str, Any]) -> None:
        """Parse one ``semantic_model`` entry.

        Dataset names are resolved to table keys per model, not globally:
        Ossie scopes names inside a model, so two models in one file may each
        declare a ``customer`` dataset pointing at different tables.
        """
        model_name = str(model.get("name", ""))
        self._record_ai_context(model_name, "models", model_name, model)

        name_to_table: dict[str, str] = {}
        keys_by_dataset: dict[str, list[tuple[str, ...]]] = {}

        for dataset in model.get("datasets", []) or []:
            name = dataset.get("name")
            if not name:
                continue
            self._record_ai_context(model_name, "datasets", name, dataset)
            keys_by_dataset[name] = self._unique_key_sets(dataset)

            for f in dataset.get("fields", []) or []:
                self._record_ai_context(
                    model_name, "fields", f"{name}.{f.get('name', '')}", f
                )

            raw_source = str(dataset.get("source", ""))
            key = _table_key(raw_source)
            if key is None:
                # A query-defined dataset legitimately has no table to key on,
                # so that case stays at debug. A single-part or empty source is
                # an authoring error worth a warning: an undeclared table makes
                # `validate_drill_by` soft-skip, so the typo turns column
                # validation off rather than failing.
                log = logger.debug if _looks_like_query(raw_source) else logger.warning
                log(
                    "OssieSource: dataset %r has no resolvable schema.table in"
                    " its source %r, so no table schema is registered for it."
                    " Expected `database.schema.table` or `schema.table`.",
                    name,
                    raw_source,
                )
                continue
            if key in self._tables:
                logger.warning(
                    "OssieSource: dataset %r maps to table key %r, which is"
                    " already declared; the database qualifier is dropped from"
                    " %r, so two databases sharing a schema and table name"
                    " collide. The later dataset wins.",
                    name,
                    key,
                    dataset.get("source"),
                )
            name_to_table[name] = key
            self._tables[key] = TableSchema(
                columns=[
                    Column(
                        name=f["name"],
                        type=f.get("datatype", ""),
                        description=f.get("description", ""),
                    )
                    for f in dataset.get("fields", []) or []
                    if f.get("name")
                ]
            )

        self._load_relationships(model, model_name, name_to_table, keys_by_dataset)
        overlay = self._vendor_overlay(model, model_name)
        # Scoped to the metrics this model contributes: ``self._metrics``
        # accumulates across the ``semantic_model`` loop, so applying the
        # default to the whole list would stamp one model's house convention
        # onto another's. Ossie namespaces entities per model; so does this.
        first_metric = len(self._metrics)
        self._load_metrics(model, model_name, overlay.get("metrics", {}))
        _apply_convention_default(
            self._metrics[first_metric:],
            _parse_convention_default(overlay.get("decomposition_convention")),
        )
        self._load_metric_impacts(overlay.get("metric_impacts", []))

    @staticmethod
    def _unique_key_sets(dataset: dict[str, Any]) -> list[tuple[str, ...]]:
        """Every column set that uniquely identifies a row in *dataset*."""
        keys: list[tuple[str, ...]] = []
        primary = dataset.get("primary_key") or []
        if primary:
            keys.append(tuple(primary))
        for unique in dataset.get("unique_keys") or []:
            if unique:
                keys.append(tuple(unique))
        return keys

    def _load_relationships(
        self,
        model: dict[str, Any],
        model_name: str,
        name_to_table: dict[str, str],
        keys_by_dataset: dict[str, list[tuple[str, ...]]],
    ) -> None:
        """Translate Ossie relationships into single-column edges.

        Ossie relationships are dataset-to-dataset with parallel
        ``from_columns``/``to_columns`` lists, and cardinality is never
        written down — it is implied by which endpoints are keys, so it is
        derived from *both* sides here.

        The spec documents ``to`` as the one side, but nothing validates that,
        and trusting it is not free: ``RelationshipChecker._check_fan_out``
        fires only on ``one_to_many``, so reading a backwards-declared join as
        ``one_to_one`` silently disables the row-multiplication warning on an
        aggregate. When ``to_columns`` are demonstrably not a key of the
        ``to`` dataset, the join fans out and is reported as such.

        Keys are optional in Ossie, though, and absence of a key is not
        evidence of fan-out. When the ``to`` dataset declares no keys at all
        there is nothing to contradict the spec's declaration, so it stands —
        otherwise every model that simply omits keys would flood the fan-out
        checker with false positives.

        Composite joins are skipped rather than split. Our ``Relationship``
        has single-column endpoints, so emitting one edge per column pair
        would assert two independent joins that are individually wrong — a
        silent correctness bug in any path the join planner walks. Skipping
        loudly is the honest failure, matching ``CubeSource``.
        """
        for rel in model.get("relationships", []) or []:
            rel_name = rel.get("name", "<unnamed>")
            from_cols = rel.get("from_columns") or []
            to_cols = rel.get("to_columns") or []
            if len(from_cols) != 1 or len(to_cols) != 1:
                logger.warning(
                    "OssieSource: relationship %r joins on %d column(s);"
                    " Relationship endpoints are single columns, so this edge"
                    " is skipped rather than split into independently-wrong"
                    " pairs. Declare it in a YamlSource overlay if the agent"
                    " needs it.",
                    rel_name,
                    max(len(from_cols), len(to_cols)),
                )
                continue

            from_table = name_to_table.get(rel.get("from", ""))
            to_table = name_to_table.get(rel.get("to", ""))
            if from_table is None or to_table is None:
                logger.warning(
                    "OssieSource: relationship %r references a dataset with no"
                    " physical table (query-backed or undeclared); skipped.",
                    rel_name,
                )
                continue

            self._record_ai_context(model_name, "relationships", rel_name, rel)
            ai = _normalize_ai_context(rel.get("ai_context")) or {}
            from_keys = keys_by_dataset.get(rel.get("from", ""), [])
            to_keys = keys_by_dataset.get(rel.get("to", ""), [])
            from_unique = tuple(from_cols) in {tuple(k) for k in from_keys}
            # No keys declared on the target: nothing contradicts the spec's
            # "``to`` is the one side", so take it at its word.
            to_unique = not to_keys or tuple(to_cols) in {tuple(k) for k in to_keys}

            self._relationships.append(
                Relationship(
                    from_=f"{from_table}.{from_cols[0]}",
                    to=f"{to_table}.{to_cols[0]}",
                    type=_CARDINALITY[(from_unique, to_unique)],
                    description=str(ai.get("instructions", "")),
                )
            )

    def _load_metrics(
        self, model: dict[str, Any], model_name: str, overlay: dict[str, Any]
    ) -> None:
        for metric in model.get("metrics", []) or []:
            name = metric.get("name")
            if not name:
                continue
            self._record_ai_context(model_name, "metrics", name, metric)
            extra = overlay.get(name, {})
            self._metrics.append(
                MetricDefinition(
                    name=name,
                    description=metric.get("description", ""),
                    sql_expression=self._pick_expression(metric),
                    source_model=extra.get("source_model", ""),
                    filters=_as_list(extra.get("filters")),
                    domains=_as_list(extra.get("domains")),
                    tier=_as_list(extra.get("tier")),
                    indicator_kind=extra.get("indicator_kind"),
                    business_owner=extra.get("business_owner"),
                    operational_owner=extra.get("operational_owner"),
                    last_reviewed=parse_review_date(extra.get("last_reviewed")),
                    decompositions=[
                        Decomposition(
                            operator=d["operator"],
                            operands=list(d.get("operands", [])),
                            convention=d.get("convention"),
                            convention_operand=d.get("convention_operand"),
                        )
                        for d in extra.get("decompositions", [])
                    ],
                    drill_by=[
                        DrillDimension(dimension=dd["dimension"], column=dd["column"])
                        for dd in extra.get("drill_by", [])
                    ],
                )
            )

    def _load_metric_impacts(self, impacts: list[dict[str, Any]]) -> None:
        for impact in impacts:
            self._metric_impacts.append(
                MetricImpact(
                    from_metric=impact["from"],
                    to_metric=impact["to"],
                    direction=impact.get("direction", "positive"),
                    confidence=impact.get("confidence", "hypothesized"),
                    evidence=impact.get("evidence", ""),
                    description=impact.get("description", ""),
                    last_reviewed=parse_review_date(impact.get("last_reviewed")),
                )
            )

    def _pick_expression(self, entity: dict[str, Any]) -> str:
        """Choose one dialect's expression, deterministically.

        The spec requires implementations to resolve multi-dialect
        expressions deterministically. Preference order is the caller's
        dialect, then the portable defaults, then the first declared entry —
        the last step chosen so a model written only in a vendor dialect
        still yields an expression instead of a silent empty string.
        """
        dialects = (entity.get("expression") or {}).get("dialects") or []
        by_dialect = {
            d.get("dialect"): d.get("expression", "")
            for d in dialects
            if d.get("dialect")
        }
        for preferred in self._dialect_preference:
            if preferred in by_dialect:
                return by_dialect[preferred]
        return dialects[0].get("expression", "") if dialects else ""

    # -- extensions -------------------------------------------------------

    def _vendor_overlay(self, model: dict[str, Any], model_name: str) -> dict[str, Any]:
        """Split ``custom_extensions`` into our vocabulary and foreign extras.

        Ossie stores extension payloads as a JSON *string*, so a neighbouring
        vendor's malformed block is a real possibility. It is carried verbatim
        rather than raised on: another vendor's typo must not stop this
        library from enforcing a contract.

        Only string payloads are put through ``json.loads``. Authors do write
        the block as a YAML mapping despite the spec, and that value is
        already usable — parsing it would raise ``TypeError`` and produce a
        "not valid JSON" warning pointing at a non-problem.
        """
        ours: dict[str, Any] = {}
        for extension in model.get("custom_extensions", []) or []:
            vendor = extension.get("vendor_name")
            if not vendor:
                continue
            payload: Any = extension.get("data", "")
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except ValueError:
                    logger.warning(
                        "OssieSource: custom_extensions payload for vendor %r is"
                        " not valid JSON; carried verbatim as a string.",
                        vendor,
                    )
            if vendor == self._vendor:
                if isinstance(payload, dict):
                    ours = payload
                else:
                    logger.warning(
                        "OssieSource: our own custom_extensions block (vendor"
                        " %r) did not parse to an object; ignoring it.",
                        vendor,
                    )
                continue
            self._foreign_extensions.setdefault(model_name, {})[vendor] = payload
        return ours

    def _record_ai_context(
        self, model_name: str, kind: str, name: str, entity: dict[str, Any]
    ) -> None:
        context = _normalize_ai_context(entity.get("ai_context"))
        if context is None or not name:
            return
        model = self._ai_context.setdefault(model_name, {})
        model.setdefault(kind, {})[name] = context

    # -- SemanticSource ---------------------------------------------------

    def get_metrics(self) -> list[MetricDefinition]:
        return list(self._metrics)

    def get_metric(self, name: str) -> MetricDefinition | None:
        for m in self._metrics:
            if m.name == name:
                return m
        return None

    def search_metrics(self, query: str) -> list[MetricDefinition]:
        return fuzzy_search_metrics(self._metrics, self.get_metric, query)

    def get_relationships(self) -> list[Relationship]:
        return list(self._relationships)

    def get_relationships_for_table(self, table: str) -> list[Relationship]:
        return list(self._rel_index.get(table, []))

    def get_table_schema(self, schema: str, table: str) -> TableSchema | None:
        return self._tables.get(f"{schema}.{table}")

    def get_table_schemas(self) -> dict[str, TableSchema]:
        return dict(self._tables)

    def get_metric_impacts(self) -> list[MetricImpact]:
        return list(self._metric_impacts)

    # -- ExtensibleSemanticSource -----------------------------------------

    def get_extras(self) -> dict[str, Any]:
        """Ossie content this library carries but does not interpret.

        Two sections, each keyed by semantic-model name first:

        ``ossie_ai_context``
            Every ``ai_context`` in the file, grouped by model then by entity
            kind. The spec's AI-grounding channel — synonyms, instructions,
            example questions — which has no counterpart in our vocabulary.
            Carried, not indexed: ``search_metrics`` still matches on name and
            description only, so Ossie synonyms never silently change
            retrieval.

        ``ossie_custom_extensions``
            Every vendor block *except* ours, grouped by model then by vendor
            name, parsed from its JSON string where possible.

        A section is omitted entirely when empty rather than emitted as ``{}``.
        Extras ride into the contract's inline snapshot and its canonical
        bytes, so an always-present empty dict would add a noise key to every
        Ossie contract's digest and would trip a contract that declares
        ``expected_extras`` on rehydrate.

        Reachable in a prompt only when named in
        ``XmlPromptRenderer(extra_sections=...)``, like any other extras.
        """
        extras: dict[str, Any] = {}
        if self._ai_context:
            extras["ossie_ai_context"] = self._ai_context
        if self._foreign_extensions:
            extras["ossie_custom_extensions"] = self._foreign_extensions
        # Vendor payloads and ai_context are author-controlled and reach
        # ``contract_canonical_bytes`` through ``json.dumps``, so they get the
        # same coercion YamlSource applies to its own extras. This also
        # deep-copies, keeping ``get_extras`` a non-aliasing read like
        # ``get_metrics`` and ``get_relationships``.
        return jsonify_extras(extras, source="OssieSource")
