"""YAML-based semantic source for teams not using dbt or Cube."""

from __future__ import annotations

from pathlib import Path

import yaml

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    Relationship,
    build_relationship_index,
    fuzzy_search_metrics,
)


class YamlSource:
    """Loads metric and table definitions from a YAML file."""

    def __init__(self, path: str | Path) -> None:
        raw = yaml.safe_load(Path(path).read_text())
        self._metrics = [
            MetricDefinition(
                name=m["name"],
                description=m.get("description", ""),
                sql_expression=m.get("sql_expression", ""),
                source_model=m.get("source_model", ""),
                filters=m.get("filters", []),
            )
            for m in raw.get("metrics", [])
        ]
        self._tables: dict[str, TableSchema] = {}
        for t in raw.get("tables", []):
            key = f"{t['schema']}.{t['table']}"
            self._tables[key] = TableSchema(
                columns=[
                    Column(
                        name=c["name"],
                        type=c.get("type", ""),
                        description=c.get("description", ""),
                    )
                    for c in t.get("columns", [])
                ]
            )
        self._relationships = [
            Relationship(
                from_=r["from"],
                to=r["to"],
                type=r.get("type", "many_to_one"),
                description=r.get("description", ""),
                required_filter=r.get("required_filter"),
            )
            for r in raw.get("relationships", [])
        ]
        self._rel_index = build_relationship_index(self._relationships)

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
