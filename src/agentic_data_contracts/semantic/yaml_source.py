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
    build_relationship_index,
    fuzzy_search_metrics,
    jsonify_extras,
    parse_review_date,
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

    def _entries(value: Any) -> list[Any]:
        return value if isinstance(value, list) else []

    for i, m in enumerate(_entries(raw.get("metrics"))):
        if not isinstance(m, dict):
            continue
        label = f"metrics[{i}] ({m.get('name', '?')})"
        _check_entry_keys(m, METRIC_KEYS, where=label, strict=strict)
        for j, d in enumerate(_entries(m.get("decompositions"))):
            if isinstance(d, dict):
                _check_entry_keys(
                    d,
                    DECOMPOSITION_KEYS,
                    where=f"{label} decompositions[{j}]",
                    strict=strict,
                )
        for j, dd in enumerate(_entries(m.get("drill_by"))):
            if isinstance(dd, dict):
                _check_entry_keys(
                    dd, DRILL_BY_KEYS, where=f"{label} drill_by[{j}]", strict=strict
                )

    for i, t in enumerate(_entries(raw.get("tables"))):
        if not isinstance(t, dict):
            continue
        label = f"tables[{i}] ({t.get('schema', '?')}.{t.get('table', '?')})"
        _check_entry_keys(t, TABLE_KEYS, where=label, strict=strict)
        for j, c in enumerate(_entries(t.get("columns"))):
            if isinstance(c, dict):
                _check_entry_keys(
                    c,
                    COLUMN_KEYS,
                    where=f"{label} columns[{j}] ({c.get('name', '?')})",
                    strict=strict,
                )

    for i, r in enumerate(_entries(raw.get("relationships"))):
        if isinstance(r, dict):
            _check_entry_keys(
                r, RELATIONSHIP_KEYS, where=f"relationships[{i}]", strict=strict
            )

    for i, impact in enumerate(_entries(raw.get("metric_impacts"))):
        if isinstance(impact, dict):
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
        extras = {k: v for k, v in raw.items() if k not in SEMANTIC_KEYS}
        _apply_extras_policy(extras, expected_extras)
        _check_nested_keys(raw, expected_extras)
        self._extras: dict[str, Any] = jsonify_extras(extras, source="YamlSource")
        default_convention = _parse_convention_default(
            raw.get("decomposition_convention")
        )
        self._metrics = []
        for m in raw.get("metrics", []):
            tier_raw = m.get("tier", [])
            tier = [tier_raw] if isinstance(tier_raw, str) else list(tier_raw)
            domains_raw = m.get("domains", [])
            domains = (
                [domains_raw] if isinstance(domains_raw, str) else list(domains_raw)
            )
            self._metrics.append(
                MetricDefinition(
                    name=m["name"],
                    description=m.get("description", ""),
                    sql_expression=m.get("sql_expression", ""),
                    source_model=m.get("source_model", ""),
                    filters=m.get("filters", []),
                    domains=domains,
                    tier=tier,
                    indicator_kind=m.get("indicator_kind"),
                    business_owner=m.get("business_owner"),
                    operational_owner=m.get("operational_owner"),
                    last_reviewed=parse_review_date(m.get("last_reviewed")),
                    decompositions=[
                        Decomposition(
                            operator=d["operator"],
                            operands=list(d.get("operands", [])),
                            convention=d.get("convention"),
                            convention_operand=d.get("convention_operand"),
                        )
                        for d in m.get("decompositions", [])
                    ],
                    drill_by=[
                        DrillDimension(dimension=dd["dimension"], column=dd["column"])
                        for dd in m.get("drill_by", [])
                    ],
                )
            )
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
                ],
                # `or ""` because a bare `description:` key loads as None,
                # and the field is annotated `str` and is public API.
                description=t.get("description") or "",
            )
        self._relationships = [
            Relationship(
                from_=r["from"],
                to=r["to"],
                type=r.get("type", "many_to_one"),
                description=r.get("description", ""),
                required_filter=r.get("required_filter"),
                preferred=r.get("preferred", False),
            )
            for r in raw.get("relationships", [])
        ]
        self._rel_index = build_relationship_index(self._relationships)
        self._metric_impacts = [
            MetricImpact(
                from_metric=i["from"],
                to_metric=i["to"],
                direction=i.get("direction", "positive"),
                confidence=i.get("confidence", "hypothesized"),
                evidence=i.get("evidence", ""),
                description=i.get("description", ""),
                last_reviewed=parse_review_date(i.get("last_reviewed")),
            )
            for i in raw.get("metric_impacts", [])
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
