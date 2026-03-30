"""Tests for to_system_prompt with semantic source and domains."""

from pathlib import Path

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.semantic.yaml_source import YamlSource


def test_system_prompt_without_source(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    prompt = dc.to_system_prompt()
    # Without semantic source, falls back to file path pointer
    assert "Consult" in prompt or "Semantic Source" in prompt


def test_system_prompt_with_source_no_domains(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    source = YamlSource(fixtures_dir / "semantic_source.yml")
    prompt = dc.to_system_prompt(semantic_source=source)
    assert "available_metrics" in prompt
    assert "total_revenue" in prompt
    assert "active_customers" in prompt
    assert "lookup_metric" in prompt


def test_system_prompt_with_domains(fixtures_dir: Path) -> None:
    source = YamlSource(fixtures_dir / "semantic_source.yml")
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "public", "tables": ["t"]})
            ],
            domains={
                "revenue": ["total_revenue"],
                "engagement": ["active_customers"],
            },
        ),
    )
    dc = DataContract(schema)
    prompt = dc.to_system_prompt(semantic_source=source)
    assert 'name="revenue"' in prompt
    assert 'name="engagement"' in prompt
    assert "total_revenue" in prompt
    assert "active_customers" in prompt


def test_system_prompt_backwards_compatible(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    # Calling without args should work exactly as before
    prompt_no_source = dc.to_system_prompt()
    assert "data_contract" in prompt_no_source
    assert "analytics.orders" in prompt_no_source
