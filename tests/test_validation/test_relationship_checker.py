from pathlib import Path
from typing import cast

import pytest
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


class TestMultipleRelationshipsPerPair:
    """A table pair may declare several edges; a join matching any one is correct.

    Regression for #126: the checker used to warn once per declared edge the
    join did not use, so a correct join was always told it was wrong.
    """

    @staticmethod
    def _checker(legacy_filter: str | None = None) -> RelationshipChecker:
        return RelationshipChecker(
            [
                Relationship(
                    from_="analytics.id_map.legacy_id",
                    to="analytics.products.product_id",
                    type="one_to_one",
                    required_filter=legacy_filter,
                ),
                Relationship(
                    from_="analytics.id_map.current_id",
                    to="analytics.products.product_id",
                    type="many_to_one",
                ),
            ]
        )

    @staticmethod
    def _join(on: str) -> exp.Expression:
        return _parse(
            f"SELECT p.name FROM analytics.id_map m JOIN analytics.products p ON {on}"
        )

    def test_first_edge_no_warning(self) -> None:
        warnings = self._checker().check_joins(self._join("m.legacy_id = p.product_id"))
        assert warnings == []

    def test_second_edge_no_warning(self) -> None:
        warnings = self._checker().check_joins(
            self._join("m.current_id = p.product_id")
        )
        assert warnings == []

    def test_undeclared_columns_one_warning_listing_every_edge(self) -> None:
        warnings = self._checker().check_joins(self._join("m.legacy_id = p.name"))
        assert len(warnings) == 1
        assert "`id_map.legacy_id` -> `products.product_id`" in warnings[0]
        assert "`id_map.current_id` -> `products.product_id`" in warnings[0]

    def test_required_filter_follows_the_edge_used(self) -> None:
        """Only the matched edge's required_filter applies, not its sibling's."""
        checker = self._checker(legacy_filter="m.is_active = TRUE")
        assert checker.check_joins(self._join("m.current_id = p.product_id")) == []
        warnings = checker.check_joins(self._join("m.legacy_id = p.product_id"))
        assert len(warnings) == 1
        assert "is_active" in warnings[0]

    def test_edges_with_swapped_column_names_match_by_table(self) -> None:
        """`a.id -> b.ref` and `b.id -> a.ref` share column names; a join on one
        must not also match the other and inherit its filter and fan-out checks.
        """
        checker = RelationshipChecker(
            [
                Relationship(
                    from_="s.a.id",
                    to="s.b.ref",
                    type="one_to_many",
                    required_filter="b.active = TRUE",
                ),
                Relationship(from_="s.b.id", to="s.a.ref", type="many_to_one"),
            ]
        )
        ast = _parse("SELECT COUNT(*) FROM s.a a JOIN s.b b ON b.id = a.ref")
        assert checker.check_joins(ast) == []

    def test_self_join_edge_listed_once(self) -> None:
        """A self-referencing edge is indexed under one key, not twice."""
        checker = RelationshipChecker(
            [
                Relationship(
                    from_="s.employees.manager_id",
                    to="s.employees.id",
                    required_filter="m.active = TRUE",
                )
            ]
        )
        sql = "SELECT 1 FROM s.employees e JOIN s.employees m ON {on}"
        wrong = checker.check_joins(_parse(sql.format(on="e.name = m.id")))
        assert len(wrong) == 1
        assert wrong[0].count("`employees.manager_id` -> `employees.id`") == 1
        unfiltered = checker.check_joins(_parse(sql.format(on="e.manager_id = m.id")))
        assert len(unfiltered) == 1
        assert "active" in unfiltered[0]

    def test_columns_on_the_wrong_tables_warn(self) -> None:
        """Right column names, wrong sides: `o.id = c.customer_id` is not the
        declared `orders.customer_id -> customers.id`.
        """
        checker = RelationshipChecker(
            [Relationship(from_="s.orders.customer_id", to="s.customers.id")]
        )
        ast = _parse(
            "SELECT o.id FROM s.orders o JOIN s.customers c ON o.id = c.customer_id"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        # Tables are named, so the agent can see which side each column is on.
        assert "uses `orders.id`, `customers.customer_id`" in warnings[0]
        assert "`orders.customer_id` -> `customers.id`" in warnings[0]

    def test_swapped_edges_listed_distinctly(self) -> None:
        """`a.id -> b.ref` and `b.id -> a.ref` must not both read `id` -> `ref`."""
        checker = RelationshipChecker(
            [
                Relationship(from_="s.a.id", to="s.b.ref"),
                Relationship(from_="s.b.id", to="s.a.ref"),
            ]
        )
        warnings = checker.check_joins(
            _parse("SELECT 1 FROM s.a a JOIN s.b b ON a.ref = b.ref")
        )
        assert len(warnings) == 1
        assert "`a.id` -> `b.ref` or `b.id` -> `a.ref`" in warnings[0]


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

    def test_required_filter_tautology_warns(self, fixtures_dir: Path) -> None:
        """`WHERE status = status` must not satisfy required_filter — it's a bypass."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status = o.status"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        assert "status" in warnings[0]

    def test_required_filter_tautology_unqualified_warns(
        self, fixtures_dir: Path
    ) -> None:
        """`WHERE status = status` without table qualifier is still a tautology."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE status = status"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        assert "trivially" in warnings[0].lower()

    def test_required_filter_tautology_neq_warns(self, fixtures_dir: Path) -> None:
        """`WHERE status != status` is a self-reference — also not a valid binding."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status != o.status"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        assert "status" in warnings[0]

    def test_required_filter_tautology_in_self_warns(self, fixtures_dir: Path) -> None:
        """`WHERE status IN (status)` self-references and does not bind the column."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status IN (o.status)"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        assert "status" in warnings[0]

    def test_required_filter_real_constraint_beside_tautology_no_warning(
        self, fixtures_dir: Path
    ) -> None:
        """A real predicate satisfies the check even if a tautology also appears."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status = 'active' AND o.status = o.status"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_required_filter_is_null_binds_column_no_warning(
        self, fixtures_dir: Path
    ) -> None:
        """`IS NOT NULL` is a legitimate (non-tautological) binding predicate."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status IS NOT NULL"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_required_filter_tautology_is_self_warns(self, fixtures_dir: Path) -> None:
        """`WHERE status IS status` self-references and is not a valid binding."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status IS o.status"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        assert "status" in warnings[0]

    def test_required_filter_tautology_between_self_warns(
        self, fixtures_dir: Path
    ) -> None:
        """`WHERE status BETWEEN status AND status` is a self-referential tautology."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status BETWEEN o.status AND o.status"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        assert "status" in warnings[0]

    def test_required_filter_tautology_lte_self_warns(self, fixtures_dir: Path) -> None:
        """`WHERE status <= status` must also be rejected (non-= binary operators)."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status <= o.status"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        assert "status" in warnings[0]


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


class TestJoinShapes:
    """A join is recognised whatever its shape, and a required filter is found
    wherever it is written.

    Regression for #128: only `JOIN ... ON col = col` was recognised, so a cast
    or wrapped key, or a join written in WHERE, skipped every check; a
    required filter in the ON clause was reported as missing.
    """

    _FILTER = "source_type = 'item'"

    @classmethod
    def _checker(cls, required_filter: str | None = _FILTER) -> RelationshipChecker:
        return RelationshipChecker(
            [
                Relationship(
                    from_="analytics.links.source_id",
                    to="analytics.items.item_id",
                    type="many_to_one",
                    required_filter=required_filter,
                )
            ]
        )

    @staticmethod
    def _join(on: str, where: str = "") -> exp.Expression:
        sql = f"SELECT COUNT(*) FROM analytics.links l JOIN analytics.items i ON {on}"
        return _parse(f"{sql} WHERE {where}" if where else sql)

    @pytest.mark.parametrize(
        "on",
        [
            "l.source_id = CAST(i.item_id AS VARCHAR)",
            "l.source_id = TRY_CAST(i.item_id AS VARCHAR)",
            "l.source_id = i.item_id::VARCHAR",
            "(l.source_id) = (i.item_id)",
            "LOWER(l.source_id) = i.item_id",
            "TRIM(l.source_id) = i.item_id",
            "COALESCE(l.source_id, '') = i.item_id",
            "l.source_id + 0 = i.item_id",
        ],
    )
    def test_wrapped_key_is_recognised(self, on: str) -> None:
        warnings = self._checker().check_joins(self._join(on))
        assert len(warnings) == 1
        assert "does not filter on: source_type" in warnings[0]

    def test_wrapped_key_on_undeclared_column_warns(self) -> None:
        warnings = self._checker().check_joins(
            self._join("l.target_id = CAST(i.item_id AS VARCHAR)")
        )
        assert len(warnings) == 1
        assert "uses `links.target_id`, `items.item_id`" in warnings[0]

    def test_expression_over_several_columns_is_not_a_key(self) -> None:
        """`CONCAT(i.a, i.b)` has no single column to match, so it is skipped."""
        warnings = self._checker().check_joins(
            self._join("l.source_id = CONCAT(i.prefix, i.item_id)")
        )
        assert warnings == []

    def test_subquery_is_not_a_key(self) -> None:
        warnings = self._checker().check_joins(
            self._join("l.source_id = (SELECT MAX(x.item_id) FROM analytics.items x)")
        )
        assert warnings == []

    def test_comma_join_is_recognised(self) -> None:
        sql = (
            "SELECT COUNT(*) FROM analytics.links l, analytics.items i"
            " WHERE l.source_id = i.item_id"
        )
        warnings = self._checker().check_joins(_parse(sql))
        assert len(warnings) == 1
        assert "does not filter on: source_type" in warnings[0]
        assert self._checker().check_joins(_parse(f"{sql} AND {self._FILTER}")) == []

    def test_comma_join_on_undeclared_column_warns(self) -> None:
        warnings = self._checker().check_joins(
            _parse(
                "SELECT COUNT(*) FROM analytics.links l, analytics.items i"
                " WHERE l.target_id = i.item_id"
            )
        )
        assert len(warnings) == 1
        assert "uses `links.target_id`, `items.item_id`" in warnings[0]

    def test_equality_within_one_table_is_not_a_join(self) -> None:
        """`e.manager_id = e.id` compares two columns of one row."""
        checker = RelationshipChecker(
            [
                Relationship(
                    from_="s.employees.manager_id",
                    to="s.employees.id",
                    required_filter="active = TRUE",
                )
            ]
        )
        ast = _parse("SELECT 1 FROM s.employees e WHERE e.manager_id = e.id")
        assert checker.check_joins(ast) == []

    def test_required_filter_in_on_clause_is_accepted(self) -> None:
        on = f"l.source_id = i.item_id AND l.{self._FILTER}"
        assert self._checker().check_joins(self._join(on)) == []

    def test_required_filter_in_left_join_on_clause_is_accepted(self) -> None:
        """For an outer join the ON clause is where the filter belongs."""
        ast = _parse(
            "SELECT COUNT(*) FROM analytics.items i"
            " LEFT JOIN analytics.links l"
            f" ON l.source_id = i.item_id AND l.{self._FILTER}"
        )
        assert self._checker().check_joins(ast) == []

    def test_required_filter_in_another_join_on_clause_is_not_accepted(
        self,
    ) -> None:
        ast = _parse(
            "SELECT COUNT(*) FROM analytics.links l"
            " JOIN analytics.items i ON l.source_id = i.item_id"
            f" JOIN analytics.tags t ON t.item_id = i.item_id AND l.{self._FILTER}"
        )
        warnings = self._checker().check_joins(ast)
        assert len(warnings) == 1
        assert "does not filter on: source_type" in warnings[0]

    def test_tautology_in_on_clause_warns(self) -> None:
        on = "l.source_id = i.item_id AND l.source_type = l.source_type"
        warnings = self._checker().check_joins(self._join(on))
        assert len(warnings) == 1
        assert "trivially satisfied" in warnings[0]

    def test_join_key_does_not_satisfy_a_filter_on_itself(self) -> None:
        """The key equality binds `item_id` to the other table, not to a value."""
        warnings = self._checker("item_id > 0").check_joins(
            self._join("l.source_id = i.item_id")
        )
        assert len(warnings) == 1
        assert "does not filter on: item_id" in warnings[0]

    def test_extra_on_equality_beside_declared_key_no_warning(self) -> None:
        """A second equality is an extra join condition, not a wrong key."""
        on = f"l.source_id = i.item_id AND l.region = i.region AND l.{self._FILTER}"
        assert self._checker().check_joins(self._join(on)) == []

    def test_extra_where_equality_beside_declared_key_no_warning(self) -> None:
        ast = self._join(
            "l.source_id = i.item_id", where=f"l.region = i.region AND l.{self._FILTER}"
        )
        assert self._checker().check_joins(ast) == []

    def test_declared_key_in_subquery_does_not_excuse_outer_wrong_key(
        self,
    ) -> None:
        ast = self._join(
            "l.target_id = i.item_id",
            where=(
                "l.id IN (SELECT l2.id FROM analytics.links l2"
                " JOIN analytics.items i2 ON l2.source_id = i2.item_id"
                f" WHERE l2.{self._FILTER})"
                f" AND l.{self._FILTER}"
            ),
        )
        warnings = self._checker().check_joins(ast)
        assert len(warnings) == 1
        assert "uses `links.target_id`, `items.item_id`" in warnings[0]
