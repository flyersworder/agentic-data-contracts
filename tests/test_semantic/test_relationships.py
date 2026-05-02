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


def test_yaml_source_get_relationships_for_table(fixtures_dir: Path) -> None:
    source = YamlSource(fixtures_dir / "semantic_source.yml")
    # "analytics.orders" appears in the `from` side
    rels = source.get_relationships_for_table("analytics.orders")
    assert len(rels) == 1
    assert rels[0].from_ == "analytics.orders.customer_id"

    # "analytics.customers" appears in the `to` side
    rels = source.get_relationships_for_table("analytics.customers")
    assert len(rels) == 1
    assert rels[0].to == "analytics.customers.id"

    # Non-existent table returns empty
    rels = source.get_relationships_for_table("analytics.nonexistent")
    assert rels == []


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


class TestPreferredRelationship:
    """Authoring hint: mark the canonical join when alternatives exist."""

    def test_relationship_default_preferred_is_false(self) -> None:
        rel = Relationship(from_="s.a.x", to="s.b.y")
        assert rel.preferred is False

    def test_yaml_source_loads_preferred_flag(self, fixtures_dir: Path) -> None:
        source = YamlSource(fixtures_dir / "relationships_preferred.yml")
        rels = source.get_relationships()
        preferred = [r for r in rels if r.preferred]
        assert len(preferred) == 1
        assert preferred[0].from_ == "analytics.orders.customer_id"
        # Edges without the key default to False (not None).
        non_preferred = [r for r in rels if not r.preferred]
        assert {r.from_ for r in non_preferred} == {
            "analytics.orders.sales_rep_id",
            "analytics.orders.approver_id",
        }

    def test_yaml_source_preferred_defaults_false_when_absent(
        self, tmp_path: Path
    ) -> None:
        yml = "relationships:\n  - from: s.a.x\n    to: s.b.y\n"
        (tmp_path / "rel.yml").write_text(yml)
        source = YamlSource(tmp_path / "rel.yml")
        assert source.get_relationships()[0].preferred is False

    def test_index_sorts_preferred_first(self) -> None:
        """Adjacency lists put preferred edges ahead of declaration order."""
        rels = [
            Relationship(from_="s.orders.sales_rep_id", to="s.users.id"),
            Relationship(from_="s.orders.customer_id", to="s.users.id", preferred=True),
            Relationship(from_="s.orders.approver_id", to="s.users.id"),
        ]
        index = build_relationship_index(rels)
        # Declaration order is sales_rep, customer, approver.
        # After sort, customer (preferred) is first; the other two retain
        # their relative declaration order (stable sort).
        assert [r.from_ for r in index["s.orders"]] == [
            "s.orders.customer_id",
            "s.orders.sales_rep_id",
            "s.orders.approver_id",
        ]

    def test_index_no_preferred_preserves_declaration_order(self) -> None:
        rels = [
            Relationship(from_="s.orders.sales_rep_id", to="s.users.id"),
            Relationship(from_="s.orders.customer_id", to="s.users.id"),
        ]
        index = build_relationship_index(rels)
        assert [r.from_ for r in index["s.orders"]] == [
            "s.orders.sales_rep_id",
            "s.orders.customer_id",
        ]

    def test_find_join_path_picks_preferred_alternative(self) -> None:
        """When multiple direct edges exist, BFS returns the preferred one."""
        rels = [
            Relationship(from_="s.orders.sales_rep_id", to="s.users.id"),
            Relationship(from_="s.orders.customer_id", to="s.users.id", preferred=True),
            Relationship(from_="s.orders.approver_id", to="s.users.id"),
        ]
        index = build_relationship_index(rels)
        path = find_join_path(index, "s.orders", "s.users")
        assert path is not None
        assert len(path) == 1
        assert path[0].from_ == "s.orders.customer_id"

    def test_get_relationships_for_table_orders_preferred_first(
        self, fixtures_dir: Path
    ) -> None:
        source = YamlSource(fixtures_dir / "relationships_preferred.yml")
        rels = source.get_relationships_for_table("analytics.orders")
        assert len(rels) == 3
        assert rels[0].from_ == "analytics.orders.customer_id"
        assert rels[0].preferred is True

    def test_get_relationships_preserves_declaration_order(
        self, fixtures_dir: Path
    ) -> None:
        """The flat list (used by the prompt renderer) keeps YAML order."""
        source = YamlSource(fixtures_dir / "relationships_preferred.yml")
        rels = source.get_relationships()
        assert [r.from_ for r in rels] == [
            "analytics.orders.sales_rep_id",
            "analytics.orders.customer_id",
            "analytics.orders.approver_id",
        ]
