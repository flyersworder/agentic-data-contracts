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
    assert "exactly one customer" in rels[0].description


def test_yaml_source_relationship_description_defaults_empty(tmp_path: Path) -> None:
    """Relationships without description default to empty string."""
    yml = "relationships:\n  - from: a.b.c\n    to: d.e.f\n"
    (tmp_path / "rel.yml").write_text(yml)
    source = YamlSource(tmp_path / "rel.yml")
    rels = source.get_relationships()
    assert len(rels) == 1
    assert rels[0].description == ""
    assert rels[0].required_filter is None


def test_yaml_source_loads_required_filter(tmp_path: Path) -> None:
    yml = (
        "relationships:\n"
        "  - from: s.bdg_attribution.contact_id\n"
        "    to: s.contacts.contact_id\n"
        "    type: many_to_one\n"
        "    required_filter: \"attribution_model = 'last_touch'\"\n"
    )
    (tmp_path / "rel.yml").write_text(yml)
    source = YamlSource(tmp_path / "rel.yml")
    rels = source.get_relationships()
    assert len(rels) == 1
    assert rels[0].required_filter == "attribution_model = 'last_touch'"


def test_system_prompt_renders_required_filter(tmp_path: Path) -> None:
    yml = (
        "relationships:\n"
        "  - from: s.bdg.contact_id\n"
        "    to: s.contacts.contact_id\n"
        "    type: many_to_one\n"
        "    required_filter: \"model = 'last_touch'\"\n"
    )
    (tmp_path / "rel.yml").write_text(yml)
    source = YamlSource(tmp_path / "rel.yml")
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "s", "tables": ["bdg", "contacts"]}
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    prompt = dc.to_system_prompt(semantic_source=source)
    assert "<required_filter>" in prompt
    assert "model = 'last_touch'" in prompt


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
    assert "exactly one customer" in prompt


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
