"""dbt manifest.json semantic source."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.semantic._assembly import AGGREGATIONS, assemble
from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    MetricImpact,
    Relationship,
    as_list,
    as_mapping,
    as_text,
    build_relationship_index,
    dict_entries,
    entry_mapping,
    fuzzy_search_metrics,
    require_text,
)


class DbtSource:
    """Loads metric and table definitions from a dbt manifest.json."""

    def __init__(self, path: str | Path) -> None:
        raw = json.loads(Path(path).read_text())
        if not isinstance(raw, dict):
            raise ValueError(
                f"A dbt manifest must be a JSON object, got {type(raw).__name__}."
            )
        # Every node and metric must itself be an object: a manifest is keyed by
        # node id, and a scalar under one of those keys is a document this
        # parser cannot read — named here rather than indexed into downstream.
        nodes = entry_mapping(raw.get("nodes"), where="manifest nodes")
        self._metrics = self._parse_metrics(
            entry_mapping(raw.get("metrics"), where="manifest metrics"),
            entry_mapping(raw.get("semantic_models"), where="manifest semantic_models"),
        )
        self._tables = self._parse_models(nodes)
        self._relationships = self._parse_relationships(nodes)
        self._rel_index = build_relationship_index(self._relationships)

    def _parse_metrics(
        self, metrics: dict[str, Any], semantic_models: dict[str, Any]
    ) -> list[MetricDefinition]:
        models = _index_semantic_models(semantic_models)
        result: list[MetricDefinition] = []
        for metric in metrics.values():
            name = require_text(metric.get("name"), where="dbt metric name")
            type_params = as_mapping(
                metric.get("type_params"), where="dbt metric type_params"
            )
            filters: list[str] = []
            if "type" not in metric and "calculation_method" in metric:
                # dbt <= 1.5 (the dbt_metrics package). Its filters and model
                # are carried as they always were; its SQL is not assembled.
                for f in dict_entries(metric.get("filters")):
                    filters.append(
                        f"{as_text(f.get('field'))} {as_text(f.get('operator'))} "
                        f"{as_text(f.get('value'))}"
                    )
                sql_expr, source_model = "", as_text(metric.get("model"))
                untranslated = (
                    "a pre-1.6 dbt_metrics metric (calculation_method "
                    f"{metric.get('calculation_method')!r}); its SQL is not "
                    "assembled -- upgrade to MetricFlow semantic models"
                )
            else:
                try:
                    sql_expr, source_model = _assemble_metric(
                        name, metric, type_params, models
                    )
                    untranslated = None
                except _Untranslatable as exc:
                    sql_expr, source_model = "", exc.source_model
                    untranslated = str(exc)

            meta = as_mapping(metric.get("meta"), where="dbt metric meta")
            tier = as_list(meta.get("tier"))
            domains = as_list(meta.get("domains"))

            result.append(
                MetricDefinition(
                    name=name,
                    description=as_text(metric.get("description")),
                    sql_expression=sql_expr,
                    source_model=source_model,
                    filters=filters,
                    domains=domains,
                    tier=tier,
                    indicator_kind=meta.get("indicator_kind"),
                    untranslated=untranslated,
                )
            )
        return result

    def _parse_models(self, nodes: dict[str, Any]) -> dict[str, TableSchema]:
        tables: dict[str, TableSchema] = {}
        for node in nodes.values():
            if node.get("resource_type") != "model":
                continue
            schema_name = node.get("schema", "")
            table_name = node.get("name", "")
            key = f"{schema_name}.{table_name}"
            columns = [
                Column(
                    name=require_text(
                        col.get("name"), where=f"dbt model {key} column name"
                    ),
                    type=as_text(col.get("data_type")),
                    description=as_text(col.get("description")),
                )
                for col in entry_mapping(
                    node.get("columns"), where=f"dbt model {key} columns"
                ).values()
            ]
            tables[key] = TableSchema(
                columns=columns,
                # dbt models carry a description in the same manifest node the
                # column descriptions come from; it was read for columns only.
                description=as_text(node.get("description")),
            )
        return tables

    def _parse_relationships(self, nodes: dict[str, Any]) -> list[Relationship]:
        """Project dbt's built-in `relationships` schema tests into Relationships.

        A relationships test compiles into a node with ``resource_type == "test"``
        and ``test_metadata.name == "relationships"``; its kwargs carry the FK
        column (``column_name``) and the referenced ``field``. The owner model
        is resolved via ``attached_node`` (manifest v12+); the referenced model
        comes from ``depends_on.nodes`` minus the owner. Tests with missing or
        unresolvable model references are skipped silently — they're either
        compiler artefacts (e.g. tests on seeds/sources we don't model) or
        manifests too old to carry ``attached_node``.

        Reads from the test's ``meta:`` block (matching how ``_parse_metrics``
        consumes ``meta.tier`` / ``meta.domains``):

        - ``meta.preferred`` (bool, default False) — canonical-join hint
        - ``meta.required_filter`` (str, default None) — SQL predicate
        - ``meta.relationship_type`` (str, default "many_to_one")
        """
        relationships: list[Relationship] = []
        for node in nodes.values():
            if node.get("resource_type") != "test":
                continue
            tm = as_mapping(node.get("test_metadata"), where="dbt test_metadata")
            if tm.get("name") != "relationships":
                continue

            kwargs = as_mapping(tm.get("kwargs"), where="dbt test kwargs")
            column_name = kwargs.get("column_name")
            field = kwargs.get("field")
            if not column_name or not field:
                continue

            owner_id = node.get("attached_node")
            owner = nodes.get(owner_id) if owner_id else None
            if owner is None:
                continue

            depends = (
                as_mapping(node.get("depends_on"), where="dbt depends_on").get("nodes")
                or []
            )
            other_ids = [n for n in depends if n != owner_id]
            if other_ids:
                referenced = nodes.get(other_ids[0])
            elif owner_id in depends:
                referenced = owner  # self-referencing FK
            else:
                referenced = None
            if referenced is None or referenced.get("resource_type") != "model":
                continue

            owner_table = f"{owner.get('schema', '')}.{owner.get('name', '')}"
            ref_table = f"{referenced.get('schema', '')}.{referenced.get('name', '')}"
            meta = as_mapping(node.get("meta"), where="dbt test meta")

            relationships.append(
                Relationship(
                    from_=f"{owner_table}.{column_name}",
                    to=f"{ref_table}.{field}",
                    type=as_text(meta.get("relationship_type"), "many_to_one"),
                    description=as_text(node.get("description")),
                    required_filter=meta.get("required_filter"),
                    preferred=bool(meta.get("preferred", False)),
                )
            )
        return relationships

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
        # dbt has no native impact-graph concept; impacts live in the
        # contract YAML (declared via YamlSource) and reference metric names.
        return []


# ── MetricFlow metric assembly ────────────────────────────────────────────────
#
# A MetricFlow metric is not one expression. The standard spec keeps `expr` and
# `agg` on a semantic model's measure, which the metric names; the 1.12 spec
# puts `agg` in `type_params.metric_aggregation_params` and `expr` in
# `type_params.expr`. Filters are Jinja over entity paths
# (`{{ Dimension('order__status') }}`), on the measure reference and on the
# metric. Only what resolves to the metric's own table is translated; anything
# else is refused with a reason, because an expression missing a join, a time
# grain or a window computes something other than the metric.

_DBT_AGGS = {
    "sum": "sum",
    "count": "count",
    "count_distinct": "count_distinct",
    "average": "avg",
    "min": "min",
    "max": "max",
    "sum_boolean": "sum_boolean",
}
assert set(_DBT_AGGS.values()) <= AGGREGATIONS

#: Entity types whose dimensions live on the semantic model that declares them.
#: A `foreign` entity's dimensions are on another model, reached by a join.
_LOCAL_ENTITY_TYPES = frozenset({"primary", "unique", "natural"})

_DIMENSION_REF = re.compile(r"\{\{\s*Dimension\(\s*'([^']*)'\s*\)\s*\}\}")


class _Untranslatable(Exception):
    def __init__(self, reason: str, source_model: str = "") -> None:
        super().__init__(reason)
        self.source_model = source_model


@dataclass
class _SemanticModel:
    relation: str
    measures: dict[str, dict[str, Any]] = field(default_factory=dict)
    dimensions: dict[str, str] = field(default_factory=dict)  # name -> column SQL
    time_dimensions: set[str] = field(default_factory=set)
    local_entities: set[str] = field(default_factory=set)


def _index_semantic_models(raw: dict[str, Any]) -> dict[str, _SemanticModel]:
    models: dict[str, _SemanticModel] = {}
    for entry in raw.values():
        name = as_text(entry.get("name"))
        relation = entry.get("node_relation")
        if not name or not isinstance(relation, dict):
            continue
        schema, alias = relation.get("schema_name"), relation.get("alias")
        model = _SemanticModel(
            relation=f"{schema}.{alias}" if schema and alias else "",
        )
        for measure in dict_entries(entry.get("measures")):
            if isinstance(measure.get("name"), str):
                model.measures[measure["name"]] = measure
        for dim in dict_entries(entry.get("dimensions")):
            dim_name = dim.get("name")
            if isinstance(dim_name, str):
                expr = dim.get("expr")
                # A dimension's `expr` is arbitrary SQL, so it is parenthesized
                # where it lands; a bare name is a column and stays bare.
                model.dimensions[dim_name] = (
                    f"({expr})" if isinstance(expr, str) and expr else dim_name
                )
                if dim.get("type") == "time":
                    model.time_dimensions.add(dim_name)
        for entity in dict_entries(entry.get("entities")):
            kind = entity.get("type")
            if (
                isinstance(kind, str)
                and kind in _LOCAL_ENTITY_TYPES
                and isinstance(entity.get("name"), str)
            ):
                model.local_entities.add(entity["name"])
        models[name] = model
    return models


def _assemble_metric(
    name: str,
    metric: dict[str, Any],
    type_params: dict[str, Any],
    models: dict[str, _SemanticModel],
) -> tuple[str, str]:
    """Return ``(sql_expression, source_model)`` or raise ``_Untranslatable``."""
    kind = metric.get("type")
    if kind != "simple":
        raise _Untranslatable(
            f"a {kind} metric is computed by dbt from other metrics or over a "
            "time window at query time; it has no single expression"
            if kind in ("ratio", "derived", "cumulative", "conversion")
            else f"metric type {kind!r} is not one this source can express"
        )

    measure_ref = type_params.get("measure")
    agg_params = type_params.get("metric_aggregation_params")
    filters = [metric.get("filter")]
    if isinstance(agg_params, dict):  # the 1.12 spec: no measure
        model = models.get(as_text(agg_params.get("semantic_model")))
        if model is None:
            raise _Untranslatable(
                f"semantic model {agg_params.get('semantic_model')!r} is not in "
                "the manifest"
            )
        agg = agg_params.get("agg")
        expr = type_params.get("expr") or name
        non_additive = agg_params.get("non_additive_dimension")
        agg_extra = agg_params.get("agg_params")
    elif isinstance(measure_ref, dict):
        measure_name = measure_ref.get("name")
        model, measure = next(
            (
                (m, m.measures[measure_name])
                for m in models.values()
                if isinstance(measure_name, str) and measure_name in m.measures
            ),
            (None, None),
        )
        if model is None or measure is None:
            raise _Untranslatable(
                f"measure {measure_name!r} is not defined by any semantic model "
                "in the manifest"
            )
        agg = measure.get("agg")
        expr = measure.get("expr") or measure_name
        non_additive = measure.get("non_additive_dimension")
        agg_extra = measure.get("agg_params")
        filters.insert(0, measure_ref.get("filter"))
    else:
        raise _Untranslatable("the metric names no measure or aggregation")

    relation = model.relation
    if non_additive:
        raise _Untranslatable(
            "its measure declares a non_additive_dimension (a semi-additive "
            "aggregate such as a closing balance), which is not a plain "
            f"{str(agg).upper()}",
            relation,
        )
    if not isinstance(agg, str) or agg not in _DBT_AGGS:
        set_params = (
            {k: v for k, v in agg_extra.items() if v not in (None, False)}
            if isinstance(agg_extra, dict)
            else {}
        )
        raise _Untranslatable(
            f"its {agg} aggregation has no portable SQL"
            + (f" (agg_params {set_params!r})" if set_params else ""),
            relation,
        )
    conditions = [
        _translate_filter(template, model, relation)
        for group in filters
        for template in _where_templates(group, relation)
    ]
    if agg == "count":
        # MetricFlow compiles `count` to SUM(CASE WHEN e IS NOT NULL ...), so
        # a count no row passes is NULL there, not COUNT's 0; match it.
        sql = assemble("sum_boolean", f"({as_text(expr)}) IS NOT NULL", conditions)
    else:
        sql = assemble(_DBT_AGGS[agg], as_text(expr), conditions)
    fill = (
        measure_ref.get("fill_nulls_with")
        if isinstance(measure_ref, dict)
        else type_params.get("fill_nulls_with")
    )
    if fill is not None:
        if isinstance(fill, bool) or not isinstance(fill, (int, float)):
            raise _Untranslatable(f"fill_nulls_with {fill!r} is not a number", relation)
        sql = f"COALESCE({sql}, {fill})"
    return sql, relation


def _where_templates(group: Any, relation: str) -> list[str]:
    """The `where_sql_template` strings of one filter, or none."""
    if group is None:
        return []
    wheres = group.get("where_filters") if isinstance(group, dict) else None
    if not isinstance(wheres, list):
        raise _Untranslatable(f"filter {group!r} is not a where-filter list", relation)
    templates = [w.get("where_sql_template") for w in wheres if isinstance(w, dict)]
    if len(templates) != len(wheres) or not all(isinstance(t, str) for t in templates):
        raise _Untranslatable(f"filter {group!r} is malformed", relation)
    return templates


def _translate_filter(template: str, model: _SemanticModel, relation: str) -> str:
    def column(match: re.Match[str]) -> str:
        path = match.group(1).split("__")
        if (
            len(path) == 2
            and path[0] in model.local_entities
            and path[1] in model.time_dimensions
        ):
            raise _Untranslatable(
                f"filter {template!r} compares time dimension {match.group(1)!r}, "
                "which MetricFlow truncates to its grain first",
                relation,
            )
        if (
            len(path) == 2
            and path[0] in model.local_entities
            and path[1] in model.dimensions
        ):
            return model.dimensions[path[1]]
        raise _Untranslatable(
            f"filter {template!r} reads {match.group(1)!r}, which is not a "
            "dimension of the metric's own semantic model (it needs a join)",
            relation,
        )

    sql = _DIMENSION_REF.sub(column, template)
    if "{{" in sql:
        raise _Untranslatable(
            f"filter {template!r} uses Jinja other than a plain Dimension "
            "reference (a TimeDimension grain, an Entity or a Metric)",
            relation,
        )
    return sql
