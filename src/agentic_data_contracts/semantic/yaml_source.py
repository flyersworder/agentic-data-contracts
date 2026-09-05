"""YAML-based semantic source for teams not using dbt or Cube."""

from __future__ import annotations

import logging
from collections.abc import Collection
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
    _apply_convention_default,
    _parse_convention_default,
    as_list,
    as_text,
    build_relationship_index,
    entry_list,
    fuzzy_search_metrics,
    jsonify_extras,
    parse_review_date,
    require_text,
    validate_decompositions,
    validate_drill_by,
)

logger = logging.getLogger(__name__)

#: Top-level keys ``_load_from_raw`` interprets. Everything else in a semantic
#: YAML is carried verbatim as "extras" — see ``YamlSource.get_extras``. Exported
#: so a consumer can assert against it instead of hardcoding a list that drifts on
#: the next release.
SEMANTIC_KEYS = frozenset(
    {"metrics", "tables", "relationships", "metric_impacts", "decomposition_convention"}
)

#: Keys interpreted *inside* each kind of list entry. Exported for the same
#: reason as ``SEMANTIC_KEYS``, and materialised as named sets for a second
#: reason: a guard cannot be written against a key set that exists only as the
#: literal strings in a constructor call, which is why unknown keys survived one
#: level down long after #60 caught them at the top (#89).
#:
#: The YAML spellings, not the dataclass field names -- ``from``/``to`` here,
#: ``from_``/``from_metric`` on :class:`Relationship` and :class:`MetricImpact`.
TABLE_KEYS = frozenset({"schema", "table", "description", "columns"})
#: Deliberately without ``nullable``. :class:`Column` has the field and
#: ``describe_table`` emits it, so writing it here is a reasonable thing to try
#: -- but the overlay carries only descriptions and ``dump_semantic_source``
#: does not serialize it, so reading it would store a value that is never
#: rendered, never frozen and never used. That is precisely the silent drop #89
#: exists to eliminate, so the key is refused with a message instead. Column
#: nullability comes from the adapter, which is the side that knows.
COLUMN_KEYS = frozenset({"name", "type", "description"})
METRIC_KEYS = frozenset(
    {
        "name",
        "description",
        "sql_expression",
        "source_model",
        "filters",
        "domains",
        "tier",
        "indicator_kind",
        "business_owner",
        "operational_owner",
        "last_reviewed",
        "decompositions",
        "drill_by",
    }
)
DECOMPOSITION_KEYS = frozenset(
    {"operator", "operands", "convention", "convention_operand"}
)
DRILL_BY_KEYS = frozenset({"dimension", "column"})
#: ``decomposition_convention`` is a *mapping*, not a list of entries, and
#: ``_parse_convention_default`` reads exactly one key from it -- so a second key
#: was dropped the way #89 complains about, and it is the one section a walk over
#: the list-valued sections cannot reach.
DECOMPOSITION_CONVENTION_KEYS = frozenset({"convention"})
RELATIONSHIP_KEYS = frozenset(
    {"from", "to", "type", "description", "required_filter", "preferred"}
)
METRIC_IMPACT_KEYS = frozenset(
    {
        "from",
        "to",
        "direction",
        "confidence",
        "evidence",
        "description",
        "last_reviewed",
    }
)


def _sorted_keys(keys: Collection[Any]) -> list[Any]:
    """Order a key set for display without assuming the keys are strings.

    ``yaml.safe_load`` resolves an unquoted ``2024:`` to an ``int`` and ``on:``
    to a ``bool``, so a document can hand us a heterogeneous key set. Sorting it
    directly raises ``TypeError`` from inside the diagnostic — turning a silent
    drop into a crash, which is worse than the defect the diagnostic exists to
    report.
    """
    return sorted(keys, key=str)


def _apply_extras_policy(
    extras: dict[str, Any],
    expected_extras: Collection[str] | None,
) -> None:
    """Warn about, or reject, top-level keys the parser does not interpret.

    A deliberate custom section and a typo (``relationship:`` for
    ``relationships:``) are indistinguishable at this layer, so the default
    warning must serve both: it names the keys, the interpreted set, and the
    remedy. Passing *expected_extras* turns the typo case into a load-time error
    while staying silent for sections the consumer declared — which a bare
    ``strict=True`` flag could not do, since it would fire forever on a
    consumer's own deliberate extras.

    Deliberately not memoised, for the reason recorded on
    ``_warn_unenforceable_operations`` in ``validation/validator.py``: caching a
    logging side effect means a consumer that calls ``logging.basicConfig()``
    after building its contracts loses the diagnostic permanently.
    """
    if not extras:
        return
    if expected_extras is None:
        logger.warning(
            "YamlSource: top-level keys not interpreted as semantic vocabulary:"
            " %s (interpreted keys: %s). They are carried and reachable via"
            " get_extras(), but reach a prompt only if named in"
            " XmlPromptRenderer(extra_sections=...). If one of these is a typo,"
            " that section is not being read at all.",
            _sorted_keys(extras),
            sorted(SEMANTIC_KEYS),
        )
        return
    unexpected = _sorted_keys(set(extras) - set(expected_extras))
    if unexpected:
        raise ValueError(
            f"YamlSource: unexpected top-level keys {unexpected}; declared"
            f" expected_extras={sorted(expected_extras)}, interpreted keys="
            f"{sorted(SEMANTIC_KEYS)}"
        )


def _check_entry_keys(
    entry: dict[str, Any],
    known: Collection[str],
    *,
    where: str,
    strict: bool,
) -> None:
    """Warn about, or reject, keys inside one list entry that are not read.

    The nested counterpart of :func:`_apply_extras_policy`, with one deliberate
    asymmetry: an unknown nested key is **diagnosed but not carried**. A
    top-level key is plausibly a consumer's own section, so it survives into
    ``get_extras()`` and can reach a prompt; a key inside a ``columns:`` entry
    has no addressable home on :class:`Column`, so carrying it would mean
    inventing a nested-extras shape that widens the dump format and moves every
    published ``contract_digest``. Say so, drop it.
    """
    unknown = _sorted_keys(set(entry) - set(known))
    if not unknown:
        return
    if strict:
        raise ValueError(
            f"YamlSource: unexpected keys {unknown} in {where}; interpreted keys"
            f" here are {sorted(known)}. Nested keys are not carried as extras,"
            " so this content would be dropped."
        )
    logger.warning(
        "YamlSource: keys not interpreted in %s: %s (interpreted keys here: %s)."
        " Unlike a top-level section these are NOT carried -- they are dropped,"
        " and reach neither a prompt nor contract_digest(). If one is a typo,"
        " that content is not being read at all.",
        where,
        unknown,
        sorted(known),
    )


def _check_nested_keys(
    raw: dict[str, Any],
    expected_extras: Collection[str] | None,
) -> None:
    """Apply :func:`_check_entry_keys` to every list entry the parser reads.

    ``expected_extras`` names *top-level sections* the consumer authored, which
    says nothing about a key inside a table entry -- so it does not excuse one.
    Its role here is only the mode switch #60 gave it: declaring it at all means
    "fail my build on a key you do not read", and that promise now holds at
    every depth.

    Runs before parsing so a strict-mode document fails on its typo rather than
    on whatever the typo caused downstream.
    """
    strict = expected_extras is not None

    def _entries(value: Any, where: str) -> list[dict[str, Any]]:
        return entry_list(value, where=where)

    for i, m in enumerate(_entries(raw.get("metrics"), "metrics")):
        label = f"metrics[{i}] ({m.get('name', '?')})"
        _check_entry_keys(m, METRIC_KEYS, where=label, strict=strict)
        for j, d in enumerate(
            _entries(m.get("decompositions"), f"{label} decompositions")
        ):
            _check_entry_keys(
                d,
                DECOMPOSITION_KEYS,
                where=f"{label} decompositions[{j}]",
                strict=strict,
            )
        for j, dd in enumerate(_entries(m.get("drill_by"), f"{label} drill_by")):
            _check_entry_keys(
                dd, DRILL_BY_KEYS, where=f"{label} drill_by[{j}]", strict=strict
            )

    for i, t in enumerate(_entries(raw.get("tables"), "tables")):
        label = f"tables[{i}] ({t.get('schema', '?')}.{t.get('table', '?')})"
        _check_entry_keys(t, TABLE_KEYS, where=label, strict=strict)
        for j, c in enumerate(_entries(t.get("columns"), f"{label} columns")):
            _check_entry_keys(
                c,
                COLUMN_KEYS,
                where=f"{label} columns[{j}] ({c.get('name', '?')})",
                strict=strict,
            )

    for i, r in enumerate(_entries(raw.get("relationships"), "relationships")):
        _check_entry_keys(
            r, RELATIONSHIP_KEYS, where=f"relationships[{i}]", strict=strict
        )

    for i, impact in enumerate(_entries(raw.get("metric_impacts"), "metric_impacts")):
        _check_entry_keys(
            impact, METRIC_IMPACT_KEYS, where=f"metric_impacts[{i}]", strict=strict
        )

    convention = raw.get("decomposition_convention")
    if isinstance(convention, dict):
        _check_entry_keys(
            convention,
            DECOMPOSITION_CONVENTION_KEYS,
            where="decomposition_convention",
            strict=strict,
        )


class YamlSource:
    """Loads metric and table definitions from a YAML file."""

    def __init__(
        self,
        path: str | Path,
        *,
        expected_extras: Collection[str] | None = None,
    ) -> None:
        raw = yaml.safe_load(Path(path).read_text())
        self._load_from_raw(
            raw if raw is not None else {}, expected_extras=expected_extras
        )

    @classmethod
    def from_raw(
        cls,
        raw: dict[str, Any],
        *,
        expected_extras: Collection[str] | None = None,
    ) -> YamlSource:
        """Build a source from already-parsed semantic data — no file access.

        The inverse of :func:`dump_semantic_source`; lets a frozen contract carry
        its semantics inline and rebuild them on a consumer with no filesystem.
        """
        obj = cls.__new__(cls)
        obj._load_from_raw(raw, expected_extras=expected_extras)
        return obj

    def _load_from_raw(
        self,
        raw: dict[str, Any],
        *,
        expected_extras: Collection[str] | None = None,
    ) -> None:
        if not isinstance(raw, dict):
            raise ValueError(
                "A semantic source document must be a mapping of sections, got"
                f" {type(raw).__name__}. Expected top-level keys among"
                f" {sorted(SEMANTIC_KEYS)}."
            )
        extras = {k: v for k, v in raw.items() if k not in SEMANTIC_KEYS}
        _apply_extras_policy(extras, expected_extras)
        _check_nested_keys(raw, expected_extras)
        self._extras: dict[str, Any] = jsonify_extras(extras, source="YamlSource")
        default_convention = _parse_convention_default(
            raw.get("decomposition_convention")
        )
        self._metrics = []
        # `or []` at every section: a bare `metrics:` header loads as None,
        # and the nested guard is already defensive about exactly this shape,
        # so without it the guard passes and this loop dies two lines later.
        for m in raw.get("metrics") or []:
            tier = as_list(m.get("tier"))
            domains = as_list(m.get("domains"))
            self._metrics.append(
                MetricDefinition(
                    name=require_text(m.get("name"), where="metrics[] name"),
                    description=as_text(m.get("description")),
                    sql_expression=as_text(m.get("sql_expression")),
                    source_model=as_text(m.get("source_model")),
                    # `as_list`, not `list(... or [])`: the latter turned a
                    # single authored filter string into one bogus filter per
                    # character, and froze them into `contract_digest`.
                    filters=as_list(m.get("filters")),
                    domains=domains,
                    tier=tier,
                    indicator_kind=m.get("indicator_kind"),
                    business_owner=m.get("business_owner"),
                    operational_owner=m.get("operational_owner"),
                    last_reviewed=parse_review_date(m.get("last_reviewed")),
                    decompositions=[
                        Decomposition(
                            operator=require_text(
                                d.get("operator"),
                                where="metrics[] decompositions[] operator",
                            ),
                            operands=as_list(d.get("operands")),
                            convention=d.get("convention"),
                            convention_operand=d.get("convention_operand"),
                        )
                        for d in m.get("decompositions") or []
                    ],
                    drill_by=[
                        DrillDimension(
                            # `require_text`, not `as_text`: a blank dimension
                            # renders into `lookup_metric` as `{"dimension": ""}`
                            # and `validate_drill_by` checks only `column`, so
                            # nothing downstream would ever catch it.
                            dimension=require_text(
                                dd.get("dimension"),
                                where="metrics[] drill_by[] dimension",
                            ),
                            column=require_text(
                                dd.get("column"), where="metrics[] drill_by[] column"
                            ),
                        )
                        for dd in m.get("drill_by") or []
                    ],
                )
            )
        self._tables: dict[str, TableSchema] = {}
        for t in raw.get("tables") or []:
            key = (
                f"{require_text(t.get('schema'), where='tables[] schema')}"
                f".{require_text(t.get('table'), where='tables[] table')}"
            )
            self._tables[key] = TableSchema(
                columns=[
                    Column(
                        name=require_text(
                            c.get("name"), where=f"tables[] {key} columns[] name"
                        ),
                        type=as_text(c.get("type")),
                        description=as_text(c.get("description")),
                    )
                    for c in t.get("columns") or []
                ],
                description=as_text(t.get("description")),
            )
        self._relationships = [
            Relationship(
                from_=require_text(r.get("from"), where="relationships[] from"),
                to=require_text(r.get("to"), where="relationships[] to"),
                type=as_text(r.get("type"), "many_to_one"),
                description=as_text(r.get("description")),
                required_filter=r.get("required_filter"),
                # A bare `preferred:` put None in a bool-annotated field and
                # dumped as `"preferred": null` into the frozen contract. It is
                # falsy, so nothing crashed and the property gate stayed green —
                # which is exactly why it survived four rounds.
                preferred=bool(r.get("preferred") or False),
            )
            for r in raw.get("relationships") or []
        ]
        self._rel_index = build_relationship_index(self._relationships)
        self._metric_impacts = [
            MetricImpact(
                from_metric=require_text(i.get("from"), where="metric_impacts[] from"),
                to_metric=require_text(i.get("to"), where="metric_impacts[] to"),
                direction=as_text(i.get("direction"), "positive"),
                confidence=as_text(i.get("confidence"), "hypothesized"),
                evidence=as_text(i.get("evidence")),
                description=as_text(i.get("description")),
                last_reviewed=parse_review_date(i.get("last_reviewed")),
            )
            for i in raw.get("metric_impacts") or []
        ]
        _apply_convention_default(self._metrics, default_convention)
        validate_decompositions(self._metrics)
        validate_drill_by(self._metrics, self._tables)

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

    def get_extras(self) -> dict[str, Any]:
        """Top-level keys this parser does not interpret, carried verbatim.

        The framework never interprets, validates, indexes, or computes over this
        content — it only carries it and, on request, places it in the prompt.
        Anything needing interpretation is a candidate for real vocabulary, not
        for extras.

        Shallow copy, consistent with ``get_metrics`` and ``get_relationships``.
        """
        return dict(self._extras)
