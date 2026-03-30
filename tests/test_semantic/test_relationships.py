"""Tests for table relationship metadata."""

from pathlib import Path

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.semantic.cube import CubeSource
from agentic_data_contracts.semantic.dbt import DbtSource
from agentic_data_contracts.semantic.yaml_source import YamlSource


def test_yaml_source_loads_relationships(fixtures_dir: Path) -> None:
    source = YamlSource(fixtures_dir / "semantic_source.yml")
    rels = source.get_relationships()
    assert len(rels) == 1
    assert rels[0].from_ == "analytics.orders.customer_id"
    assert rels[0].to == "analytics.customers.id"
    assert rels[0].type == "many_to_one"


def test_yaml_source_no_relationships(tmp_path: Path) -> None:
    (tmp_path / "empty.yml").write_text("metrics: []")
    source = YamlSource(tmp_path / "empty.yml")
    assert source.get_relationships() == []


def test_dbt_source_returns_empty_relationships(
    fixtures_dir: Path,
) -> None:
    source = DbtSource(fixtures_dir / "sample_dbt_manifest.json")
    assert source.get_relationships() == []


def test_cube_source_returns_empty_relationships(
    fixtures_dir: Path,
) -> None:
    source = CubeSource(fixtures_dir / "sample_cube_schema.yml")
    assert source.get_relationships() == []


def test_system_prompt_includes_relationships(
    fixtures_dir: Path,
) -> None:
    source = YamlSource(fixtures_dir / "semantic_source.yml")
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders", "customers"]}
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    prompt = dc.to_system_prompt(semantic_source=source)
    assert "table_relationships" in prompt
    assert "analytics.orders.customer_id" in prompt
    assert "analytics.customers.id" in prompt
    assert "many_to_one" in prompt


def test_system_prompt_no_relationships_when_empty(
    fixtures_dir: Path,
) -> None:
    source = DbtSource(fixtures_dir / "sample_dbt_manifest.json")
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    prompt = dc.to_system_prompt(semantic_source=source)
    assert "Table Relationships" not in prompt
