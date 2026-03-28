"""dbt manifest.json semantic source."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.semantic.base import MetricDefinition, fuzzy_search_metrics


class DbtSource:
    """Loads metric and table definitions from a dbt manifest.json."""

    def __init__(self, path: str | Path) -> None:
        raw = json.loads(Path(path).read_text())
        self._metrics = self._parse_metrics(raw.get("metrics", {}))
        self._tables = self._parse_models(raw.get("nodes", {}))

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

            result.append(
                MetricDefinition(
                    name=metric["name"],
                    description=metric.get("description", ""),
                    sql_expression=sql_expr,
                    source_model=metric.get("model", ""),
                    filters=filters,
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

    def get_metrics(self) -> list[MetricDefinition]:
        return list(self._metrics)

    def get_metric(self, name: str) -> MetricDefinition | None:
        for m in self._metrics:
            if m.name == name:
                return m
        return None

    def search_metrics(self, query: str) -> list[MetricDefinition]:
        return fuzzy_search_metrics(self._metrics, self.get_metric, query)

    def get_table_schema(self, schema: str, table: str) -> TableSchema | None:
        return self._tables.get(f"{schema}.{table}")
