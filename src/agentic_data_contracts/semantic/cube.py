"""Cube schema YAML semantic source."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.semantic._assembly import assemble
from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    MetricImpact,
    Relationship,
    as_list,
    as_mapping,
    as_text,
    build_relationship_index,
    entry_list,
    fuzzy_search_metrics,
    require_text,
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
        cubes = entry_list(raw.get("cubes"), where="cubes")

        for cube in cubes:
            sql_table = as_text(cube.get("sql_table"))
            cube_label = cube.get("name", "?")
            # A Cube schema is hand-authored YAML, so a bare `measures:`
            # header is the same "present but empty" shape.
            measures = entry_list(
                cube.get("measures"), where=f"cube {cube_label} measures"
            )
            refs = _CubeRefs(
                cube=as_text(cube.get("name")),
                dimensions={
                    as_text(d.get("name")): as_text(d.get("sql"))
                    for d in entry_list(
                        cube.get("dimensions"), where=f"cube {cube_label} dimensions"
                    )
                },
                measures={as_text(m.get("name")) for m in measures},
            )

            for measure in measures:
                name = require_text(measure.get("name"), where="cube measure name")
                meta = as_mapping(measure.get("meta"), where="measure meta")
                tier = as_list(meta.get("tier"))
                domains = as_list(meta.get("domains"))
                try:
                    sql_expr, untranslated = _assemble_measure(measure, refs), None
                except _Untranslatable as exc:
                    sql_expr, untranslated = "", str(exc)
                self._metrics.append(
                    MetricDefinition(
                        name=name,
                        description=as_text(measure.get("description")),
                        sql_expression=sql_expr,
                        source_model=sql_table,
                        domains=domains,
                        tier=tier,
                        indicator_kind=meta.get("indicator_kind"),
                        untranslated=untranslated,
                    )
                )

            if sql_table and "." in sql_table:
                columns = [
                    Column(
                        name=require_text(
                            c.get("name"), where=f"cube {sql_table} column name"
                        ),
                        type=as_text(c.get("type")),
                        description=as_text(c.get("description")),
                    )
                    for c in entry_list(
                        cube.get("columns"),
                        where=f"cube {cube.get('name', '?')} columns",
                    )
                ]
                self._tables[sql_table] = TableSchema(
                    columns=columns,
                    # A cube's own description is the table-level gloss, the
                    # same fact dbt models and Ossie datasets carry.
                    description=as_text(cube.get("description")),
                )

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
        for cube in entry_list(cubes, where="cubes"):
            name = cube.get("name")
            sql_table = as_text(cube.get("sql_table"))
            if name and sql_table and "." in sql_table:
                name_to_table[name] = sql_table

        relationships: list[Relationship] = []
        for cube in cubes:
            cube_name = cube.get("name")
            if cube_name not in name_to_table:
                continue
            for join in entry_list(cube.get("joins"), where=f"cube {cube_name} joins"):
                sql = as_text(join.get("sql"))
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

                meta = as_mapping(join.get("meta"), where="cube join meta")
                rel_field = (join.get("relationship") or "many_to_one").lower()
                # `.get(k, fallback)` returned None for an explicit
                # `relationship_type:` — the default only fires on absence.
                canonical_type = as_text(
                    meta.get("relationship_type"),
                    _CUBE_RELATIONSHIP_TYPES.get(rel_field, "many_to_one"),
                )

                relationships.append(
                    Relationship(
                        from_=f"{cube_table}.{cube_col}",
                        to=f"{other_table}.{other_col}",
                        type=canonical_type,
                        description=as_text(join.get("description")),
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
        # Cube has no native impact-graph concept; impacts live in the
        # contract YAML (declared via YamlSource) and reference metric names.
        return []


# ── Measure assembly ──────────────────────────────────────────────────────────
#
# A Cube measure is `sql` (per-row) aggregated by `type`, over rows where every
# `filters[].sql` holds. Both use Cube's own references: `{CUBE}.col` (or the
# older `${CUBE}.col`, or the cube's own name) for a column of this cube, and
# `{member}` / `{CUBE.member}` for a dimension or measure. Only references that
# resolve to this cube's columns and dimensions are translated; a reference to
# another cube needs a join and one to a measure needs composition, so either
# is refused with a reason rather than emitted as SQL that cannot run.

_CUBE_AGGS = {
    "count": "count",
    "count_distinct": "count_distinct",
    "sum": "sum",
    "avg": "avg",
    "min": "min",
    "max": "max",
}
#: Multi-stage measure keys: each makes the measure an aggregate over another
#: query's output (a time shift, a regrouping), which one expression cannot be.
_MULTI_STAGE_KEYS = (
    "multi_stage",
    "time_shift",
    "group_by",
    "reduce_by",
    "add_group_by",
)
#: Types whose `sql` is already an aggregate expression, passed through.
_CUBE_PASSTHROUGH = frozenset({"number", "string", "time", "boolean"})

# The optional trailing `.` is captured so `{CUBE}.col` can drop it with the
# prefix; any other reference re-emits it.
_REF_RE = re.compile(r"\$?\{([^{}]*)\}(\.)?")
_BARE_COLUMN_RE = re.compile(r"\w+")


class _Untranslatable(Exception):
    pass


class _CubeRefs:
    def __init__(
        self, cube: str, dimensions: dict[str, str], measures: set[str]
    ) -> None:
        self.cube = cube
        self.dimensions = dimensions
        self.measures = measures

    def resolve(self, sql: str, *, where: str, depth: int = 0) -> str:
        """Replace Cube references in *sql* with plain SQL on this cube."""
        own = {"CUBE", self.cube}

        def one(match: re.Match[str]) -> str:
            ref = match.group(1).strip()
            dot = match.group(2) or ""
            # `{CUBE}.col`: the prefix names this cube's table; drop it.
            if ref in own and dot:
                return ""
            head, _, member = ref.partition(".")
            if member and head in own:
                ref = member
            elif member:
                raise _Untranslatable(
                    f"{where} reads {{{match.group(1)}}}, a member of cube "
                    f"{head!r}; it needs a join"
                )
            if ref in self.dimensions and depth > 0:
                raise _Untranslatable(
                    f"{where} refers to dimension {ref!r}; nested dimension "
                    "references are not translated"
                )
            if ref in self.dimensions and not self.dimensions[ref]:
                raise _Untranslatable(
                    f"{where} refers to dimension {ref!r}, which has no sql "
                    "(a `case:` dimension)"
                )
            if ref in self.dimensions:
                dim_sql = self.resolve(
                    self.dimensions[ref], where=f"dimension {ref!r}", depth=1
                )
                return (
                    dim_sql if _BARE_COLUMN_RE.fullmatch(dim_sql) else f"({dim_sql})"
                ) + dot
            if ref in self.measures:
                raise _Untranslatable(
                    f"{where} refers to measure {ref!r}; composing measures is "
                    "not translated"
                )
            raise _Untranslatable(
                f"{where} reads {{{match.group(1)}}}, which is not a dimension "
                "of this cube (another cube's member needs a join)"
            )

        return _REF_RE.sub(one, sql)


def _assemble_measure(measure: dict[str, Any], refs: _CubeRefs) -> str:
    kind = measure.get("type")
    where = f"measure {measure.get('name')!r}"
    if kind is None:
        # Cube requires `type`, but a schema without one loaded before and
        # its `sql` could be per-row or already aggregated: refuse to guess.
        raise _Untranslatable(
            f"{where} declares no type, so its aggregation is unknown"
        )
    if not isinstance(kind, str):
        raise ValueError(
            f"cube measure {measure.get('name')!r} type must be a string, got "
            f"{type(kind).__name__}"
        )
    if measure.get("rolling_window") is not None:
        raise _Untranslatable(
            f"{where} declares a rolling_window, a window over time that one "
            "expression cannot carry"
        )
    staged = [k for k in _MULTI_STAGE_KEYS if measure.get(k) not in (None, False)]
    if staged:
        raise _Untranslatable(
            f"{where} declares {', '.join(staged)}: a multi-stage measure is "
            "computed over another query's result, not over rows"
        )
    filters = entry_list(measure.get("filters"), where=f"cube {where} filters")
    raw_sql = measure.get("sql")
    sql = None if raw_sql is None else refs.resolve(as_text(raw_sql), where=where)
    if kind in _CUBE_PASSTHROUGH:
        if filters:
            raise _Untranslatable(
                f"{where} is a {kind} measure (already aggregated) with filters, "
                "which have no row-level expression to apply to"
            )
        if not sql:
            raise _Untranslatable(f"{where} is a {kind} measure with no sql")
        return sql
    if kind not in _CUBE_AGGS:
        raise _Untranslatable(
            f"{where} has type {kind}, which has no portable single-expression SQL"
        )
    if sql is None and kind != "count":
        raise _Untranslatable(f"{where} is a {kind} measure with no sql")
    conditions: list[str] = []
    for f in filters:
        condition = f.get("sql")
        if not isinstance(condition, str) or not condition.strip():
            raise _Untranslatable(f"{where} has a filter with no sql")
        conditions.append(refs.resolve(condition, where=f"{where} filter"))
    return assemble(_CUBE_AGGS[kind], sql, conditions)
