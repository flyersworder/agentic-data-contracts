"""Cube schema YAML semantic source."""

from __future__ import annotations

from pathlib import Path

import yaml

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    MetricImpact,
    Relationship,
    fuzzy_search_metrics,
)


class CubeSource:
    """Loads metric and table definitions from a Cube schema YAML file."""

    def __init__(self, path: str | Path) -> None:
        raw = yaml.safe_load(Path(path).read_text())
        self._metrics: list[MetricDefinition] = []
        self._tables: dict[str, TableSchema] = {}

        for cube in raw.get("cubes", []):
            sql_table = cube.get("sql_table", "")

            for measure in cube.get("measures", []):
                meta = measure.get("meta") or {}
                tier_raw = meta.get("tier", [])
                tier = [tier_raw] if isinstance(tier_raw, str) else list(tier_raw)
                domains_raw = meta.get("domains", [])
                domains = (
                    [domains_raw] if isinstance(domains_raw, str) else list(domains_raw)
                )
                self._metrics.append(
                    MetricDefinition(
                        name=measure["name"],
                        description=measure.get("description", ""),
                        sql_expression=measure.get("sql", ""),
                        source_model=sql_table,
                        domains=domains,
                        tier=tier,
                        indicator_kind=meta.get("indicator_kind"),
                    )
                )

            if sql_table and "." in sql_table:
                columns = [
                    Column(
                        name=c["name"],
                        type=c.get("type", ""),
                        description=c.get("description", ""),
                    )
                    for c in cube.get("columns", [])
                ]
                self._tables[sql_table] = TableSchema(columns=columns)

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
        return []  # TODO: parse from Cube joins config

    def get_relationships_for_table(self, table: str) -> list[Relationship]:
        return []  # TODO: parse from Cube joins config

    def get_table_schema(self, schema: str, table: str) -> TableSchema | None:
        return self._tables.get(f"{schema}.{table}")

    def get_metric_impacts(self) -> list[MetricImpact]:
        # Cube has no native impact-graph concept; impacts live in the
        # contract YAML (declared via YamlSource) and reference metric names.
        return []
