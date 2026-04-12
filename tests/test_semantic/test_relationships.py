"""Tests for table relationship metadata."""

from pathlib import Path

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.semantic.base import (
    Relationship,
    build_relationship_index,
    find_join_path,
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


class TestBuildRelationshipIndex:
    def test_indexes_from_and_to_tables(self) -> None:
        rels = [
            Relationship(from_="s.orders.customer_id", to="s.customers.id"),
        ]
        index = build_relationship_index(rels)
        assert len(index["s.orders"]) == 1
        assert len(index["s.customers"]) == 1
        assert index["s.orders"][0] is rels[0]
        assert index["s.customers"][0] is rels[0]

    def test_self_referencing_not_duplicated(self) -> None:
        rels = [
            Relationship(from_="s.employees.manager_id", to="s.employees.id"),
        ]
        index = build_relationship_index(rels)
        assert len(index["s.employees"]) == 1

    def test_empty_relationships(self) -> None:
        assert build_relationship_index([]) == {}

    def test_hub_table_collects_all(self) -> None:
        rels = [
            Relationship(from_="s.orders.customer_id", to="s.customers.id"),
            Relationship(from_="s.reviews.customer_id", to="s.customers.id"),
            Relationship(from_="s.tickets.customer_id", to="s.customers.id"),
        ]
        index = build_relationship_index(rels)
        assert len(index["s.customers"]) == 3


class TestFindJoinPath:
    def _make_index(self) -> dict[str, list[Relationship]]:
        rels = [
            Relationship(from_="s.orders.customer_id", to="s.customers.id"),
            Relationship(from_="s.customers.region_id", to="s.regions.id"),
            Relationship(from_="s.regions.country_id", to="s.countries.id"),
        ]
        return build_relationship_index(rels)

    def test_direct_relationship(self) -> None:
        index = self._make_index()
        path = find_join_path(index, "s.orders", "s.customers")
        assert path is not None
        assert len(path) == 1
        assert path[0].from_ == "s.orders.customer_id"

    def test_two_hop_path(self) -> None:
        index = self._make_index()
        path = find_join_path(index, "s.orders", "s.regions")
        assert path is not None
        assert len(path) == 2

    def test_three_hop_path(self) -> None:
        index = self._make_index()
        path = find_join_path(index, "s.orders", "s.countries")
        assert path is not None
        assert len(path) == 3

    def test_exceeds_max_hops_returns_none(self) -> None:
        index = self._make_index()
        path = find_join_path(index, "s.orders", "s.countries", max_hops=2)
        assert path is None

    def test_no_path_returns_none(self) -> None:
        index = self._make_index()
        path = find_join_path(index, "s.orders", "s.nonexistent")
        assert path is None

    def test_same_table_returns_empty(self) -> None:
        index = self._make_index()
        path = find_join_path(index, "s.orders", "s.orders")
        assert path == []
