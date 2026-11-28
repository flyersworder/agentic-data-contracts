"""No-source prompts stay byte-for-byte (#139 anti-goal).

The golden files under ``tests/fixtures/golden_prompts/`` were captured from the
v0.64.0 renderer before the metric index existed. Rendering without a source
(``semantic_source=None``, what ``to_system_prompt`` did by default before
v0.65.0), or with one that has no metrics, must give exactly that output.
Regenerating these files to make a test pass defeats the test: a diff here is a
behaviour change for every caller who opts out of the source.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.prompt import XmlPromptRenderer
from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    MetricImpact,
    Relationship,
)

FIXTURES = Path(__file__).parent.parent / "fixtures"
GOLDEN = FIXTURES / "golden_prompts"

CASES: dict[str, tuple[str, dict[str, Any]]] = {
    "valid_contract": ("valid_contract.yml", {}),
    "valid_contract_no_thresholds": (
        "valid_contract.yml",
        {"metric_detail_threshold": None},
    ),
    "valid_contract_low_thresholds": (
        "valid_contract.yml",
        {"metric_detail_threshold": 0},
    ),
    "roundtrip_contract": ("roundtrip_contract.yml", {}),
    "minimal_contract": ("minimal_contract.yml", {}),
    "principals_contract": ("principals_contract.yml", {}),
}


def _golden(name: str) -> str:
    return (GOLDEN / f"{name}.txt").read_text(encoding="utf-8").removesuffix("\n")


@pytest.mark.parametrize("name", sorted(CASES))
def test_no_source_rendering_matches_v064(name: str) -> None:
    fixture, kwargs = CASES[name]
    contract = DataContract.from_yaml(FIXTURES / fixture)
    assert XmlPromptRenderer(**kwargs).render(contract) == _golden(name)


class _EmptySource:
    """A semantic source that carries no metrics and no relationships."""

    def get_metrics(self) -> list[MetricDefinition]:
        return []

    def get_metric(self, name: str) -> MetricDefinition | None:
        return None

    def get_table_schema(self, schema: str, table: str) -> None:
        return None

    def get_table_schemas(self) -> dict:
        return {}

    def get_relationships(self) -> list[Relationship]:
        return []

    def get_relationships_for_table(self, table: str) -> list[Relationship]:
        return []

    def search_metrics(self, query: str) -> list[MetricDefinition]:
        return []

    def get_metric_impacts(self) -> list[MetricImpact]:
        return []

    def get_extras(self) -> dict[str, Any]:
        return {}


@pytest.mark.parametrize("name", ["valid_contract", "roundtrip_contract"])
def test_source_without_metrics_renders_as_no_source(name: str) -> None:
    fixture, kwargs = CASES[name]
    contract = DataContract.from_yaml(FIXTURES / fixture)
    rendered = XmlPromptRenderer(**kwargs).render(contract, _EmptySource())
    assert rendered == _golden(name)


@pytest.mark.parametrize("name", sorted(CASES))
def test_to_system_prompt_with_none_matches_v064(name: str) -> None:
    # The public opt-out, not just the renderer: semantic_source=None must not
    # load the contract's configured source.
    fixture, kwargs = CASES[name]
    contract = DataContract.from_yaml(FIXTURES / fixture)
    rendered = contract.to_system_prompt(
        semantic_source=None, renderer=XmlPromptRenderer(**kwargs)
    )
    assert rendered == _golden(name)
