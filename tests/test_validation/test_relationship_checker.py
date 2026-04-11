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

    def test_using_clause_correct_key_no_warning(self, fixtures_dir: Path) -> None:
        """JOIN ... USING (col) should be handled like ON."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        # customers.id -> addresses.customer_id is one_to_one
        # USING (id) means both sides share 'id' — but declared key is customer_id/id
        # This won't match because USING(id) implies both cols are 'id'
        # Let's test a case where USING matches: orders has customer_id, but USING
        # requires same column name on both sides. Use addresses relationship instead.
        # customers.id = addresses.customer_id — can't use USING here (different names)
        # So test that USING with a wrong column warns correctly
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c USING (customer_id)"
            " WHERE o.status != 'cancelled'"
        )
        warnings = checker.check_joins(ast)
        # USING(customer_id) means both sides use customer_id, but
        # declared relationship is customer_id -> id (different cols)
        assert len(warnings) == 1
        assert "customer_id" in warnings[0]

    def test_using_clause_undeclared_silent(self, fixtures_dir: Path) -> None:
        """USING on undeclared relationship should be silent."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.products p USING (product_id)"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_using_clause_three_table_query(self, fixtures_dir: Path) -> None:
        """USING in a 3-table query should resolve to the correct from_table."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        # orders -> customers via ON, then customers -> addresses via USING
        # The USING join should match (customers, addresses), not (orders, addresses)
        ast = _parse(
            "SELECT o.id, a.city FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " JOIN analytics.addresses a USING (customer_id)"
            " WHERE o.status != 'cancelled'"
        )
        warnings = checker.check_joins(ast)
        # customer_id is used on both sides (USING), but declared relationship
        # is customers.id -> addresses.customer_id (different cols: id vs customer_id)
        # So we expect a join-key warning for the customers->addresses pair
        assert len(warnings) == 1
        assert "addresses" in warnings[0]
        # Crucially, should NOT warn about orders->addresses (undeclared)

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


class TestFanOutDetection:
    """Tests that the checker warns when aggregating across a one_to_many join."""

    def test_aggregation_with_one_to_many_warns(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT SUM(o.amount) FROM analytics.orders o"
            " JOIN analytics.order_items oi ON o.id = oi.order_id"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        assert "one_to_many" in warnings[0]
        assert "order_items" in warnings[0]

    def test_no_aggregation_with_one_to_many_no_warning(
        self, fixtures_dir: Path
    ) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, oi.quantity FROM analytics.orders o"
            " JOIN analytics.order_items oi ON o.id = oi.order_id"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_aggregation_with_many_to_one_no_warning(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT SUM(o.amount) FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status != 'cancelled'"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_aggregation_with_one_to_one_no_warning(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT COUNT(c.id) FROM analytics.customers c"
            " JOIN analytics.addresses a ON c.id = a.customer_id"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_multiple_aggregation_functions_single_warning(
        self, fixtures_dir: Path
    ) -> None:
        """Multiple agg functions with same 1:N join should produce one warning."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT SUM(o.amount), AVG(o.amount), COUNT(*) FROM analytics.orders o"
            " JOIN analytics.order_items oi ON o.id = oi.order_id"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1

    def test_aggregation_in_subquery_only_no_warning(self, fixtures_dir: Path) -> None:
        """Aggregation only in subquery, not outer query — no fan-out warning."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, oi.quantity FROM analytics.orders o"
            " JOIN analytics.order_items oi ON o.id = oi.order_id"
            " WHERE o.id IN (SELECT COUNT(*) FROM analytics.tmp)"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_scalar_subquery_aggregation_no_warning(self, fixtures_dir: Path) -> None:
        """Scalar subquery with aggregation in SELECT should not trigger fan-out."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT (SELECT AVG(price) FROM analytics.tmp), o.id"
            " FROM analytics.orders o"
            " JOIN analytics.order_items oi ON o.id = oi.order_id"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_real_agg_with_scalar_subquery_still_warns(
        self, fixtures_dir: Path
    ) -> None:
        """Real outer aggregation should still warn even if scalar subqueries exist."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT SUM(o.amount), (SELECT AVG(price) FROM analytics.tmp)"
            " FROM analytics.orders o"
            " JOIN analytics.order_items oi ON o.id = oi.order_id"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        assert "one_to_many" in warnings[0]
