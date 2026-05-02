from pathlib import Path

import pytest

from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.semantic.cube import CubeSource


@pytest.fixture
def source(fixtures_dir: Path) -> CubeSource:
    return CubeSource(fixtures_dir / "sample_cube_schema.yml")


def test_source_implements_protocol(source: CubeSource) -> None:
    assert isinstance(source, SemanticSource)


def test_get_metrics(source: CubeSource) -> None:
    metrics = source.get_metrics()
    assert len(metrics) == 1
    assert metrics[0].name == "total_revenue"
    assert "SUM(amount)" in metrics[0].sql_expression


def test_get_metric(source: CubeSource) -> None:
    metric = source.get_metric("total_revenue")
    assert metric is not None
    assert metric.description == "Total revenue from all orders"


def test_get_metric_not_found(source: CubeSource) -> None:
    assert source.get_metric("nonexistent") is None


def test_get_table_schema(source: CubeSource) -> None:
    schema = source.get_table_schema("analytics", "orders")
    assert schema is not None
    col_names = [c.name for c in schema.columns]
    assert "id" in col_names
    assert "amount" in col_names


def test_get_table_schema_not_found(source: CubeSource) -> None:
    assert source.get_table_schema("analytics", "nonexistent") is None


# ---------------------------------------------------------------------------
# Relationship parsing — Cube's per-cube `joins:` block carries an SQL
# expression like `{CUBE}.col = {Other}.col` and a `relationship` enum
# (belongsTo / hasOne / hasMany or snake_case aliases). CubeSource regexes
# the single-equality pattern in either direction, looks up the target
# cube by name to resolve schema.table, maps the enum to a canonical type,
# and reads preferred / required_filter / type override from `meta:`.
# ---------------------------------------------------------------------------


@pytest.fixture
def joins_source(fixtures_dir: Path) -> CubeSource:
    return CubeSource(fixtures_dir / "sample_cube_schema_with_joins.yml")


class TestCubeRelationships:
    def test_loads_four_relationships(self, joins_source: CubeSource) -> None:
        """Three on Orders, one on Customers (hasMany), one self-FK on Employees.
        The Phantoms join points at a non-existent cube and is skipped."""
        rels = joins_source.get_relationships()
        assert len(rels) == 4

    def test_unresolvable_cube_name_skipped(self, joins_source: CubeSource) -> None:
        rels = joins_source.get_relationships()
        assert all("phantom" not in r.from_.lower() for r in rels)
        assert all("phantom" not in r.to.lower() for r in rels)

    def test_canonical_edge_round_trips(self, joins_source: CubeSource) -> None:
        rels = {(r.from_, r.to): r for r in joins_source.get_relationships()}
        canonical = rels[("analytics.orders.customer_id", "analytics.users.id")]
        assert canonical.preferred is True
        assert canonical.required_filter == "status != 'cancelled'"
        assert canonical.type == "many_to_one"

    def test_non_preferred_edge_defaults_to_false(
        self, joins_source: CubeSource
    ) -> None:
        rels = {(r.from_, r.to): r for r in joins_source.get_relationships()}
        sales_rep = rels[("analytics.orders.sales_rep_id", "analytics.users.id")]
        assert sales_rep.preferred is False
        assert sales_rep.required_filter is None

    def test_reversed_sql_order_parsed(self, joins_source: CubeSource) -> None:
        """The sales_rep edge is written as `{Users}.id = {CUBE}.sales_rep_id`
        (CUBE on the right). The parser must still emit it from CUBE side."""
        rels = {(r.from_, r.to): r for r in joins_source.get_relationships()}
        assert ("analytics.orders.sales_rep_id", "analytics.users.id") in rels

    def test_has_many_alias_emitted_as_one_to_many(
        self, joins_source: CubeSource
    ) -> None:
        """Customers.hasMany Orders -> from `{CUBE}.id = {Orders}.customer_id`,
        emitted in the SQL's written direction with type=one_to_many."""
        rels = {(r.from_, r.to): r for r in joins_source.get_relationships()}
        assert (
            "analytics.customers.id",
            "analytics.orders.customer_id",
        ) in rels
        edge = rels[("analytics.customers.id", "analytics.orders.customer_id")]
        assert edge.type == "one_to_many"

    def test_self_referencing_fk_with_meta_override(
        self, joins_source: CubeSource
    ) -> None:
        """Employees.hasOne with meta.relationship_type=many_to_one wins."""
        rels = joins_source.get_relationships()
        self_ref = next(r for r in rels if r.from_ == "hr.employees.manager_id")
        assert self_ref.to == "hr.employees.id"
        assert self_ref.type == "many_to_one"  # meta override beats hasOne default

    def test_index_returns_preferred_first(self, joins_source: CubeSource) -> None:
        """Same preferred-first guarantee that YamlSource and DbtSource get."""
        orders = joins_source.get_relationships_for_table("analytics.orders")
        assert orders[0].from_ == "analytics.orders.customer_id"
        assert orders[0].preferred is True

    def test_get_relationships_for_table_indexes_referenced_side(
        self, joins_source: CubeSource
    ) -> None:
        users = joins_source.get_relationships_for_table("analytics.users")
        # customer_id and sales_rep_id both target users.
        assert len(users) == 2

    def test_self_referencing_indexed_once(self, joins_source: CubeSource) -> None:
        emps = joins_source.get_relationships_for_table("hr.employees")
        assert len(emps) == 1

    def test_empty_cube_schema_has_no_relationships(self, source: CubeSource) -> None:
        """The original fixture has no `joins:` blocks — must stay []."""
        assert source.get_relationships() == []
        assert source.get_relationships_for_table("analytics.orders") == []
