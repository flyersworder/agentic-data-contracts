from pathlib import Path
from typing import cast

import sqlglot
from sqlglot import exp

from agentic_data_contracts.semantic.base import Relationship
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.validation.checkers import RelationshipChecker


def _parse(sql: str) -> exp.Expression:
    return cast(exp.Expression, sqlglot.parse_one(sql))


def _load_relationships(fixtures_dir: Path) -> list[Relationship]:
    source = YamlSource(fixtures_dir / "relationships_checker.yml")
    return source.get_relationships()


class TestJoinKeyCorrectness:
    """Tests that checker warns when join columns don't match declared relationships."""

    def test_correct_join_key_no_warning(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status != 'cancelled'"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_wrong_join_key_warns(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.email = c.email"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        assert "customer_id" in warnings[0]
        assert "email" in warnings[0]

    def test_undeclared_join_silent(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, p.name FROM analytics.orders o"
            " JOIN analytics.products p ON o.product_id = p.id"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_bare_table_names_match(self, fixtures_dir: Path) -> None:
        """Agent omits schema prefix — should still match relationship."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
            " WHERE o.status != 'cancelled'"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_reversed_join_order_matches(self, fixtures_dir: Path) -> None:
        """FROM customers JOIN orders should still match the relationship."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT c.name, o.id FROM analytics.customers c"
            " JOIN analytics.orders o ON o.customer_id = c.id"
            " WHERE o.status != 'cancelled'"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_case_insensitive_table_match(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id FROM Analytics.Orders o"
            " JOIN Analytics.Customers c ON o.customer_id = c.id"
            " WHERE o.status != 'cancelled'"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []


class TestRequiredFilterEnforcement:
    """Tests that the checker warns when a required_filter is missing."""

    def test_required_filter_present_no_warning(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status != 'cancelled'"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_required_filter_absent_warns(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        assert "status" in warnings[0]
        assert (
            "required_filter" in warnings[0].lower()
            or "required filter" in warnings[0].lower()
        )

    def test_no_required_filter_on_relationship_no_warning(
        self, fixtures_dir: Path
    ) -> None:
        """order_items relationship has no required_filter — should be silent."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, oi.quantity FROM analytics.orders o"
            " JOIN analytics.order_items oi ON o.id = oi.order_id"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_required_filter_with_different_expression_no_warning(
        self, fixtures_dir: Path
    ) -> None:
        """Status filtered with different value — no warning (column presence only)."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status = 'active'"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []
