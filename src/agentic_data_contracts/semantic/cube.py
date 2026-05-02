"""Cube schema YAML semantic source."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    MetricImpact,
    Relationship,
    build_relationship_index,
    fuzzy_search_metrics,
)

# Maps Cube's `relationship` enum (camelCase v1 + snake_case v2 aliases) to
# our canonical Relationship.type strings. Authors override via meta.relationship_type.
_CUBE_RELATIONSHIP_TYPES: dict[str, str] = {
    "belongsto": "many_to_one",
    "many_to_one": "many_to_one",
    "hasone": "one_to_one",
    "one_to_one": "one_to_one",
    "hasmany": "one_to_many",
    "one_to_many": "one_to_many",
}

# Single-equality join SQL: `{Cube1}.col1 = {Cube2}.col2`. Composite-key joins
# (`AND`-chained equalities) are not parsed by this version — declare them
# as separate join entries or fall back to YamlSource for unusual patterns.
_JOIN_EQ_RE = re.compile(r"\{(\w+)\}\.(\w+)\s*=\s*\{(\w+)\}\.(\w+)")


class CubeSource:
    """Loads metric and table definitions from a Cube schema YAML file."""

    def __init__(self, path: str | Path) -> None:
        raw = yaml.safe_load(Path(path).read_text())
        self._metrics: list[MetricDefinition] = []
        self._tables: dict[str, TableSchema] = {}
        cubes = raw.get("cubes", []) or []

        for cube in cubes:
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

        self._relationships = self._parse_relationships(cubes)
        self._rel_index = build_relationship_index(self._relationships)

    def _parse_relationships(self, cubes: list[dict[str, Any]]) -> list[Relationship]:
        """Parse each cube's `joins:` block into Relationship instances.

        Cube join SQL uses `{CubeName}.column` interpolation, where `{CUBE}`
        is the current cube. We regex out the single-equality form
        ``{X}.col1 = {Y}.col2`` (in either order) and resolve the cube names
        to their `sql_table` values via a name lookup map.

        The Relationship's ``from`` is always the column on the *current*
        cube (the one declaring the join) and ``to`` is the column on the
        joined cube — independent of which side `{CUBE}` appears on in the
        SQL. The ``type`` carries the cardinality, so a ``hasMany`` join on
        cube A produces ``A.pk -> B.fk`` with type ``one_to_many``. This
        keeps the mental model consistent with `YamlSource` (where authors
        write ``from`` as the starting table) and means joins read the same
        regardless of how the equality was written.

        Reads from the join's ``meta:`` block (matching `_parse_metrics`):

        - ``meta.preferred`` (bool, default False)
        - ``meta.required_filter`` (str, default None)
        - ``meta.relationship_type`` (str) — wins over the ``relationship`` field

        Joins whose SQL doesn't match the single-equality pattern, whose
        target cube name isn't in the schema, or whose either-side cube has
        no `sql_table`, are skipped silently.
        """
        name_to_table: dict[str, str] = {}
        for cube in cubes:
            name = cube.get("name")
            sql_table = cube.get("sql_table", "")
            if name and sql_table and "." in sql_table:
                name_to_table[name] = sql_table

        relationships: list[Relationship] = []
        for cube in cubes:
            cube_name = cube.get("name")
            if cube_name not in name_to_table:
                continue
            for join in cube.get("joins", []) or []:
                sql = join.get("sql", "")
                m = _JOIN_EQ_RE.search(sql)
                if not m:
                    continue
                left_ref, left_col, right_ref, right_col = m.groups()
                # Normalise so the column on the current cube is on the
                # `from` side and the joined cube's column is on `to`. Either
                # `{CUBE}` or the cube's literal name may appear on either
                # side of the equality.
                if left_ref in ("CUBE", cube_name):
                    cube_col, other_ref, other_col = left_col, right_ref, right_col
                elif right_ref in ("CUBE", cube_name):
                    cube_col, other_ref, other_col = right_col, left_ref, left_col
                else:
                    continue  # neither side references the declaring cube
                other_name = cube_name if other_ref == "CUBE" else other_ref
                cube_table = name_to_table[cube_name]
                other_table = name_to_table.get(other_name)
                if other_table is None:
                    continue

                meta = join.get("meta") or {}
                rel_field = (join.get("relationship") or "many_to_one").lower()
                canonical_type = meta.get(
                    "relationship_type",
                    _CUBE_RELATIONSHIP_TYPES.get(rel_field, "many_to_one"),
                )

                relationships.append(
                    Relationship(
                        from_=f"{cube_table}.{cube_col}",
                        to=f"{other_table}.{other_col}",
                        type=canonical_type,
                        description=join.get("description", ""),
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
        # Cube has no native impact-graph concept; impacts live in the
        # contract YAML (declared via YamlSource) and reference metric names.
        return []
