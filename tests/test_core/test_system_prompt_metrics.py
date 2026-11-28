"""Tests for to_system_prompt with semantic source and domains."""

from pathlib import Path

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Domain,
    SemanticConfig,
)
from agentic_data_contracts.semantic.yaml_source import YamlSource


def test_system_prompt_without_source_with_domains(fixtures_dir: Path) -> None:
    """Contract with domains but no semantic source renders domain index."""
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    prompt = dc.to_system_prompt(semantic_source=None)
    assert "<available_domains>" in prompt
    assert "revenue" in prompt


def test_system_prompt_without_source_no_domains() -> None:
    """Contract with source config but no domains falls back to semantic_source tag."""
    from agentic_data_contracts.core.schema import (
        SemanticSource as SemanticSourceConfig,
    )

    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "public", "tables": ["t"]})
            ],
            source=SemanticSourceConfig(type="dbt", path="./manifest.json"),
        ),
    )
    dc = DataContract(schema)
    prompt = dc.to_system_prompt(semantic_source=None)
    assert "<semantic_source>" in prompt
    assert "dbt" in prompt
    assert "Consult" in prompt


def test_system_prompt_with_source_and_domains(fixtures_dir: Path) -> None:
    """When contract has domains, domains section is rendered instead of metrics."""
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    source = YamlSource(fixtures_dir / "semantic_source.yml")
    prompt = dc.to_system_prompt(semantic_source=source)
    # valid_contract.yml has domains, so we get <available_domains>
    assert "available_domains" in prompt
    assert "revenue" in prompt
    assert "engagement" in prompt
    # The source's metrics are listed by name under each domain they declare.
    assert '<metric name="total_revenue" />' in prompt
    assert prompt.count('<metric name="active_customers" />') == 2
    assert "Look up the domain and the metric before computing it." in prompt


def test_system_prompt_with_domains(fixtures_dir: Path) -> None:
    source = YamlSource(fixtures_dir / "semantic_source.yml")
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "public", "tables": ["t"]})
            ],
            domains=[
                Domain(
                    name="revenue",
                    summary="Revenue and financial metrics",
                    description="Revenue domain.",
                ),
                Domain(
                    name="engagement",
                    summary="Customer activity metrics",
                    description="Engagement domain.",
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    prompt = dc.to_system_prompt(semantic_source=source)
    assert 'name="revenue"' in prompt
    assert 'name="engagement"' in prompt
    assert '<metric name="total_revenue" />' in prompt
    assert "Look up the domain and the metric before computing it." in prompt


def test_system_prompt_backwards_compatible(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    # semantic_source=None renders exactly as calling without args did before
    # v0.65.0 (the default now loads the configured source).
    prompt_no_source = dc.to_system_prompt(semantic_source=None)
    assert "data_contract" in prompt_no_source
    assert "analytics.orders" in prompt_no_source
