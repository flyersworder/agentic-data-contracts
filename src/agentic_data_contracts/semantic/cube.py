"""Cube schema YAML semantic source."""

from __future__ import annotations

from pathlib import Path

import yaml

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.semantic.base import MetricDefinition


class CubeSource:
    """Loads metric and table definitions from a Cube schema YAML file."""

    def __init__(self, path: str | Path) -> None:
        raw = yaml.safe_load(Path(path).read_text())
        self._metrics: list[MetricDefinition] = []
        self._tables: dict[str, TableSchema] = {}

        for cube in raw.get("cubes", []):
            sql_table = cube.get("sql_table", "")

            for measure in cube.get("measures", []):
                self._metrics.append(
                    MetricDefinition(
                        name=measure["name"],
                        description=measure.get("description", ""),
                        sql_expression=measure.get("sql", ""),
                        source_model=sql_table,
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

    def get_table_schema(self, schema: str, table: str) -> TableSchema | None:
        return self._tables.get(f"{schema}.{table}")
