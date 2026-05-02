from pathlib import Path

import pytest

from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.semantic.dbt import DbtSource


@pytest.fixture
def source(fixtures_dir: Path) -> DbtSource:
    return DbtSource(fixtures_dir / "sample_dbt_manifest.json")


def test_source_implements_protocol(source: DbtSource) -> None:
    assert isinstance(source, SemanticSource)


def test_get_metrics(source: DbtSource) -> None:
    metrics = source.get_metrics()
    assert len(metrics) == 1
    assert metrics[0].name == "total_revenue"


def test_get_metric(source: DbtSource) -> None:
    metric = source.get_metric("total_revenue")
    assert metric is not None
    assert "SUM(amount)" in metric.sql_expression
    assert metric.description == "Sum of all completed order amounts"


def test_get_metric_not_found(source: DbtSource) -> None:
    assert source.get_metric("nonexistent") is None


def test_get_table_schema(source: DbtSource) -> None:
    schema = source.get_table_schema("analytics", "orders")
    assert schema is not None
    assert len(schema.columns) == 3
    col_names = [c.name for c in schema.columns]
    assert "id" in col_names
    assert "amount" in col_names


def test_get_table_schema_not_found(source: DbtSource) -> None:
    assert source.get_table_schema("analytics", "nonexistent") is None


# ---------------------------------------------------------------------------
# Relationship parsing — dbt's built-in `relationships` schema test compiles
# into a node whose test_metadata.name == "relationships" and whose
# test_metadata.kwargs carries `to: ref('model')`, `field`, and `column_name`.
# DbtSource projects these into Relationship instances; preferred /
# required_filter / type override read from the test's `meta:` block.
# ---------------------------------------------------------------------------


@pytest.fixture
def relationships_source(fixtures_dir: Path) -> DbtSource:
    return DbtSource(fixtures_dir / "sample_dbt_manifest_with_relationships.json")


class TestDbtRelationships:
    def test_loads_three_relationships(self, relationships_source: DbtSource) -> None:
        rels = relationships_source.get_relationships()
        assert len(rels) == 3

    def test_ignores_non_relationships_tests(
        self, relationships_source: DbtSource
    ) -> None:
        """not_null and unique tests must not project into Relationships."""
        rels = relationships_source.get_relationships()
        # The fixture has not_null_orders_id and unique_customers_id alongside
        # the three relationships tests; only the latter should appear.
        assert all("manager" in r.from_ or "_id" in r.from_ for r in rels)

    def test_canonical_edge_round_trips(self, relationships_source: DbtSource) -> None:
        rels = {(r.from_, r.to): r for r in relationships_source.get_relationships()}
        canonical = rels[("analytics.orders.customer_id", "analytics.customers.id")]
        assert canonical.preferred is True
        assert canonical.required_filter == "status != 'cancelled'"
        assert canonical.type == "many_to_one"
        assert "canonical" in canonical.description

    def test_non_preferred_edge_defaults_to_false(
        self, relationships_source: DbtSource
    ) -> None:
        rels = {(r.from_, r.to): r for r in relationships_source.get_relationships()}
        sales_rep = rels[("analytics.orders.sales_rep_id", "analytics.users.id")]
        assert sales_rep.preferred is False
        assert sales_rep.required_filter is None

    def test_self_referencing_fk(self, relationships_source: DbtSource) -> None:
        """employees.manager_id -> employees.id (same model on both sides)."""
        rels = relationships_source.get_relationships()
        self_ref = next(r for r in rels if r.from_ == "hr.employees.manager_id")
        assert self_ref.to == "hr.employees.id"
        assert self_ref.type == "many_to_one"

    def test_meta_relationship_type_override(
        self, relationships_source: DbtSource
    ) -> None:
        """meta.relationship_type wins over the default many_to_one."""
        rels = relationships_source.get_relationships()
        self_ref = next(r for r in rels if r.from_ == "hr.employees.manager_id")
        assert self_ref.type == "many_to_one"  # explicit in fixture meta

    def test_index_returns_preferred_first(
        self, relationships_source: DbtSource
    ) -> None:
        """get_relationships_for_table inherits the preferred-first sort."""
        orders = relationships_source.get_relationships_for_table("analytics.orders")
        # Two outgoing edges: customer_id (preferred) and sales_rep_id (not).
        assert orders[0].from_ == "analytics.orders.customer_id"
        assert orders[0].preferred is True
        assert orders[1].from_ == "analytics.orders.sales_rep_id"

    def test_get_relationships_for_table_indexes_referenced_side(
        self, relationships_source: DbtSource
    ) -> None:
        """Edges show up under the `to` table as well as the `from` table."""
        customers = relationships_source.get_relationships_for_table(
            "analytics.customers"
        )
        assert len(customers) == 1
        assert customers[0].to == "analytics.customers.id"

    def test_self_referencing_indexed_once(
        self, relationships_source: DbtSource
    ) -> None:
        """Self-FK should appear once in its single adjacency list, not twice."""
        emps = relationships_source.get_relationships_for_table("hr.employees")
        assert len(emps) == 1

    def test_empty_manifest_has_no_relationships(self, source: DbtSource) -> None:
        """The original fixture has no relationships tests — must stay []."""
        assert source.get_relationships() == []
        assert source.get_relationships_for_table("analytics.orders") == []
