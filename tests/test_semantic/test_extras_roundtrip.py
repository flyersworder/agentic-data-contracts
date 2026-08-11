"""Extras survive freeze, rebuild identically, and move the contract digest."""

from __future__ import annotations

from typing import Any

import pytest

from agentic_data_contracts.adapters.base import TableSchema
from agentic_data_contracts.semantic.base import (
    ExtensibleSemanticSource,
    MetricDefinition,
    MetricImpact,
    Relationship,
    dump_semantic_source,
)
from agentic_data_contracts.semantic.cube import CubeSource
from agentic_data_contracts.semantic.dbt import DbtSource
from agentic_data_contracts.semantic.yaml_source import YamlSource

_HINTS = [{"table": "analytics.orders", "prefer": "order_total", "over": "total"}]


def test_dump_carries_extras() -> None:
    src = YamlSource.from_raw({"metrics": [], "column_hints": _HINTS})
    assert dump_semantic_source(src)["column_hints"] == _HINTS


def test_dump_omits_extras_entirely_when_there_are_none() -> None:
    """Load-bearing: every existing frozen contract's digest must not move."""
    src = YamlSource.from_raw({"metrics": []})
    dumped = dump_semantic_source(src)
    assert set(dumped) == {"tables", "metrics", "relationships", "metric_impacts"}


def test_extras_round_trip_through_dump_and_rebuild() -> None:
    src = YamlSource.from_raw({"metrics": [], "column_hints": _HINTS})
    rebuilt = YamlSource.from_raw(dump_semantic_source(src))
    assert rebuilt.get_extras() == src.get_extras()


def test_yaml_source_is_extensible() -> None:
    assert isinstance(YamlSource.from_raw({"metrics": []}), ExtensibleSemanticSource)


class _CollidingSource:
    """A third-party ``ExtensibleSemanticSource`` whose extras shadow vocabulary.

    ``ExtensibleSemanticSource`` is public and top-level exported, so nothing
    stops an outside implementation from returning an interpreted key. It cannot
    happen via ``YamlSource`` (extras there are the complement of
    ``SEMANTIC_KEYS``), which is exactly why the invariant needs asserting rather
    than assuming.
    """

    def __init__(self, extras: dict[str, Any]) -> None:
        self._extras = extras

    def get_metrics(self) -> list[MetricDefinition]:
        return [MetricDefinition(name="revenue", description="", sql_expression="x")]

    def get_metric(self, name: str) -> MetricDefinition | None:
        return None

    def get_table_schema(self, schema: str, table: str) -> TableSchema | None:
        return None

    def get_table_schemas(self) -> dict[str, TableSchema]:
        return {}

    def search_metrics(self, query: str) -> list[MetricDefinition]:
        return []

    def get_relationships(self) -> list[Relationship]:
        return []

    def get_relationships_for_table(self, table: str) -> list[Relationship]:
        return []

    def get_metric_impacts(self) -> list[MetricImpact]:
        return []

    def get_extras(self) -> dict[str, Any]:
        return dict(self._extras)


def test_extras_colliding_with_dumped_vocabulary_raise() -> None:
    """Silently replacing the metrics would let the digest attest to a lie."""
    src = _CollidingSource({"metrics": [{"name": "not_really"}]})
    with pytest.raises(ValueError, match="metrics"):
        dump_semantic_source(src)


def test_non_colliding_third_party_extras_still_dump() -> None:
    """The guard rejects collisions only — it must not reject extras as such."""
    src = _CollidingSource({"column_hints": _HINTS})
    dumped = dump_semantic_source(src)
    assert dumped["column_hints"] == _HINTS
    assert [m["name"] for m in dumped["metrics"]] == ["revenue"]


def test_dbt_and_cube_are_not_extensible() -> None:
    """A sibling protocol, so sources without extras stay valid SemanticSources.

    ``__new__`` without ``__init__`` is enough: a runtime_checkable protocol
    checks method presence on the class, not instance state.
    """
    assert not isinstance(DbtSource.__new__(DbtSource), ExtensibleSemanticSource)
    assert not isinstance(CubeSource.__new__(CubeSource), ExtensibleSemanticSource)
