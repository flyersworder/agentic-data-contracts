"""YAML-based semantic source for teams not using dbt or Cube."""

from __future__ import annotations

import logging
from collections.abc import Collection
from datetime import date, datetime
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
    build_relationship_index,
    fuzzy_search_metrics,
    parse_review_date,
    validate_decompositions,
    validate_drill_by,
)

logger = logging.getLogger(__name__)

#: Top-level keys ``_load_from_raw`` interprets. Everything else in a semantic
#: YAML is carried verbatim as "extras" — see ``YamlSource.get_extras``. Exported
#: so a consumer can assert against it instead of hardcoding a list that drifts on
#: the next release.
SEMANTIC_KEYS = frozenset({"metrics", "tables", "relationships", "metric_impacts"})


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
            sorted(extras),
            sorted(SEMANTIC_KEYS),
        )
        return
    unexpected = sorted(set(extras) - set(expected_extras))
    if unexpected:
        raise ValueError(
            f"YamlSource: unexpected top-level keys {unexpected}; declared"
            f" expected_extras={sorted(expected_extras)}, interpreted keys="
            f"{sorted(SEMANTIC_KEYS)}"
        )


def _normalize_extras(extras: dict[str, Any]) -> dict[str, Any]:
    """Return *extras* with dates ISO-coerced and JSON-safety enforced.

    Extras ride inside ``SemanticSource.inline`` and therefore through
    ``contract_canonical_bytes``' ``json.dumps``. A YAML-native date is not JSON,
    and the guidance extras exist to carry explicitly wants one ("verified
    against the database on ..."). Checking at load means a bad value fails where
    it was authored, rather than months later inside an ARD publish.
    """
    return {k: _jsonify(v, (k,)) for k, v in extras.items()}


def _jsonify(value: Any, path: tuple[str | int, ...]) -> Any:
    """Recursively coerce *value* to JSON-safe types, or raise naming *path*.

    ``datetime`` is checked before ``date`` because it subclasses ``date`` — the
    same ordering trap ``parse_review_date`` documents.
    """
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonify(v, (*path, str(k))) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonify(v, (*path, i)) for i, v in enumerate(value)]
    if value is None or isinstance(value, str | int | float):
        return value
    raise ValueError(
        f"YamlSource: extras value at {_fmt_path(path)} is not JSON-serializable"
        f" ({type(value).__name__}). Extras are carried into the frozen contract"
        f" and its digest, so they must be JSON-safe."
    )


def _fmt_path(path: tuple[str | int, ...]) -> str:
    """Render a key path as ``section[0].field`` for an actionable error."""
    rendered = str(path[0])
    for part in path[1:]:
        rendered += f"[{part}]" if isinstance(part, int) else f".{part}"
    return rendered


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
        self._extras: dict[str, Any] = _normalize_extras(extras)
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
                ]
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
