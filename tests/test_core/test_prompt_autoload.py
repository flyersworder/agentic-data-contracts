"""to_system_prompt() loads the contract's semantic source by default (#139).

Same default as create_tools(): the source declared in the contract's config is
loaded when the caller passes none, so the prompt can name the metrics the
tools already know. Passing ``semantic_source=None`` explicitly renders without
a source, exactly as v0.64.0 did.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import (
    DataContract,
    SemanticSourceUnavailableError,
)
from agentic_data_contracts.core.prompt import XmlPromptRenderer
from agentic_data_contracts.semantic.yaml_source import YamlSource


def _contract(source_block: str) -> DataContract:
    return DataContract.from_yaml_string(
        f"""
version: "1.0"
name: t
semantic:
{source_block}
  allowed_tables:
    - schema: analytics
      tables: [orders]
  domains:
    - name: revenue
      summary: Revenue
      description: Revenue is recognized at fulfillment.
"""
    )


@pytest.fixture
def configured(fixtures_dir: Path) -> DataContract:
    path = fixtures_dir / "semantic_source.yml"
    return _contract(f"  source:\n    type: yaml\n    path: {path}")


def test_default_loads_the_configured_source(configured: DataContract) -> None:
    prompt = configured.to_system_prompt()
    assert '<metric name="total_revenue" />' in prompt


def test_default_matches_passing_the_loaded_source(configured: DataContract) -> None:
    assert configured.to_system_prompt() == configured.to_system_prompt(
        configured.load_semantic_source()
    )


def test_explicit_none_renders_without_a_source(configured: DataContract) -> None:
    prompt = configured.to_system_prompt(semantic_source=None)
    assert prompt == XmlPromptRenderer().render(configured, None)
    assert "<metric name=" not in prompt
    # Today's tool-naming domain hint, unchanged.
    assert 'Use lookup_domain("...")' in prompt


def test_explicit_source_wins_over_the_configured_one(
    configured: DataContract,
) -> None:
    other = YamlSource.from_raw(
        {
            "metrics": [
                {
                    "name": "other_metric",
                    "description": "Other.",
                    "sql_expression": "1",
                    "domains": ["revenue"],
                }
            ]
        }
    )
    prompt = configured.to_system_prompt(other)
    assert '<metric name="other_metric" />' in prompt
    assert "total_revenue" not in prompt


def test_contract_without_a_source_is_unchanged() -> None:
    contract = _contract("")
    assert contract.to_system_prompt() == XmlPromptRenderer().render(contract, None)


def test_inline_snapshot_is_loaded() -> None:
    contract = _contract(
        """  source:
    type: yaml
    inline:
      metrics:
        - name: frozen_metric
          description: Frozen.
          sql_expression: "1"
          domains: [revenue]"""
    )
    assert '<metric name="frozen_metric" />' in contract.to_system_prompt()


def test_unloadable_source_fails_closed_like_create_tools(tmp_path: Path) -> None:
    contract = _contract(
        f"  source:\n    type: yaml\n    path: {tmp_path / 'missing.yml'}"
    )
    with pytest.raises(SemanticSourceUnavailableError):
        contract.to_system_prompt()
    # The explicit opt-out still renders.
    assert "<data_contract" in contract.to_system_prompt(semantic_source=None)
