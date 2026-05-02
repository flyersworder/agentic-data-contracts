"""dbt manifest.json semantic source."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    MetricImpact,
    Relationship,
    build_relationship_index,
    fuzzy_search_metrics,
)


class DbtSource:
    """Loads metric and table definitions from a dbt manifest.json."""

    def __init__(self, path: str | Path) -> None:
        raw = json.loads(Path(path).read_text())
        nodes = raw.get("nodes", {})
        self._metrics = self._parse_metrics(raw.get("metrics", {}))
        self._tables = self._parse_models(nodes)
        self._relationships = self._parse_relationships(nodes)
        self._rel_index = build_relationship_index(self._relationships)

    def _parse_metrics(self, metrics: dict[str, Any]) -> list[MetricDefinition]:
        result: list[MetricDefinition] = []
        for metric in metrics.values():
            sql_expr = ""
            type_params = metric.get("type_params", {})
            measure = type_params.get("measure", {})
            if isinstance(measure, dict):
                sql_expr = measure.get("expr", "")

            filters: list[str] = []
            for f in metric.get("filters", []):
                if isinstance(f, dict):
                    field = f.get("field", "")
                    op = f.get("operator", "")
                    val = f.get("value", "")
                    filters.append(f"{field} {op} {val}")

            meta = metric.get("meta") or {}
            tier_raw = meta.get("tier", [])
            tier = [tier_raw] if isinstance(tier_raw, str) else list(tier_raw)
            domains_raw = meta.get("domains", [])
            domains = (
                [domains_raw] if isinstance(domains_raw, str) else list(domains_raw)
            )

            result.append(
                MetricDefinition(
                    name=metric["name"],
                    description=metric.get("description", ""),
                    sql_expression=sql_expr,
                    source_model=metric.get("model", ""),
                    filters=filters,
                    domains=domains,
                    tier=tier,
                    indicator_kind=meta.get("indicator_kind"),
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
                    name=col["name"],
                    type=col.get("data_type", ""),
                    description=col.get("description", ""),
                )
                for col in node.get("columns", {}).values()
            ]
            tables[key] = TableSchema(columns=columns)
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
            tm = node.get("test_metadata") or {}
            if tm.get("name") != "relationships":
                continue

            kwargs = tm.get("kwargs") or {}
            column_name = kwargs.get("column_name")
            field = kwargs.get("field")
            if not column_name or not field:
                continue

            owner_id = node.get("attached_node")
            owner = nodes.get(owner_id) if owner_id else None
            if owner is None:
                continue

            depends = (node.get("depends_on") or {}).get("nodes") or []
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
            meta = node.get("meta") or {}

            relationships.append(
                Relationship(
                    from_=f"{owner_table}.{column_name}",
                    to=f"{ref_table}.{field}",
                    type=meta.get("relationship_type", "many_to_one"),
                    description=node.get("description", ""),
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

    def get_metric_impacts(self) -> list[MetricImpact]:
        # dbt has no native impact-graph concept; impacts live in the
        # contract YAML (declared via YamlSource) and reference metric names.
        return []
