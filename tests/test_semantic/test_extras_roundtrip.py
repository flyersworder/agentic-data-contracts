"""Extras survive freeze, rebuild identically, and move the contract digest."""

from __future__ import annotations

from agentic_data_contracts.semantic.base import (
    ExtensibleSemanticSource,
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


def test_dbt_and_cube_are_not_extensible() -> None:
    """A sibling protocol, so sources without extras stay valid SemanticSources.

    ``__new__`` without ``__init__`` is enough: a runtime_checkable protocol
    checks method presence on the class, not instance state.
    """
    assert not isinstance(DbtSource.__new__(DbtSource), ExtensibleSemanticSource)
    assert not isinstance(CubeSource.__new__(CubeSource), ExtensibleSemanticSource)
