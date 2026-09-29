"""DbtSource against a manifest dbt actually produced (#123).

``dbt_metricflow_manifest.json`` is ``dbt parse`` output for the project in
``fixtures/dbt_metricflow/`` (standard spec: ``legacy_spec.yml``; the 1.12
spec: ``latest_spec.yml``). A MetricFlow metric keeps its expression and
aggregation on a semantic-model measure (or, in the 1.12 spec, in
``metric_aggregation_params``) and its filters as Jinja, so ``DbtSource`` must
assemble one self-contained expression -- or say, in ``untranslated``, why it
cannot, and leave ``sql_expression`` empty rather than emit SQL that means
something else.

Most assertions execute the assembled expression on DuckDB rather than pin
its text: the claim is what it computes.
"""

import json
from pathlib import Path

import duckdb
import pytest

from agentic_data_contracts.semantic.dbt import DbtSource


@pytest.fixture
def source(fixtures_dir: Path) -> DbtSource:
    return DbtSource(fixtures_dir / "dbt_metricflow_manifest.json")


@pytest.fixture(scope="module")
def db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE orders (order_id INT, customer_id INT, amount DOUBLE,
                             status VARCHAR, is_paid BOOLEAN);
        INSERT INTO orders VALUES
            (1, 1, 10.0,  'completed', true),
            (2, 1, 250.0, 'completed', true),
            (3, 2, 40.0,  'cancelled', false),
            (4, 3, 300.0, 'completed', false),
            (5, 3, NULL,  'completed', true);
        CREATE TABLE payments (payment_id INT, amount DOUBLE, fee DOUBLE,
                               status VARCHAR);
        INSERT INTO payments VALUES
            (1, 10.0, 0.5, 'completed'), (2, 20.0, 1.0, 'refunded');
        """
    )
    return con


def _value(source: DbtSource, db: duckdb.DuckDBPyConnection, name: str) -> object:
    metric = source.get_metric(name)
    assert metric is not None, name
    assert metric.untranslated is None, metric.untranslated
    table = metric.source_model.split(".")[-1]
    row = db.execute(f"SELECT {metric.sql_expression} FROM {table}").fetchone()
    assert row is not None
    return row[0]


class TestStandardSpec:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("revenue", 600.0),
            ("orders_placed", 5),
            ("customers_ordering", 3),
            ("average_order", 150.0),
            ("largest_order", 300.0),
            ("paid_order_count", 3),
            # A measure with no `expr` aggregates the column named after it.
            ("amount_default_expr", 600.0),
        ],
    )
    def test_the_aggregation_wraps_the_measure_expression(
        self, source: DbtSource, db: duckdb.DuckDBPyConnection, name: str, expected
    ) -> None:
        assert _value(source, db, name) == expected

    def test_the_source_model_is_the_semantic_models_relation(
        self, source: DbtSource
    ) -> None:
        metric = source.get_metric("revenue")
        assert metric is not None
        assert metric.source_model == "main.orders"

    def test_meta_is_still_read(self, source: DbtSource) -> None:
        metric = source.get_metric("revenue")
        assert metric is not None
        assert metric.tier == ["north_star"]
        assert metric.domains == ["revenue"]
        assert metric.indicator_kind == "lagging"


class TestFilters:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            # A filter on the measure reference.
            ("completed_revenue", 560.0),
            # A filter on the metric.
            ("completed_orders", 4),
            # Both, one resolving a dimension whose `expr` is not a bare
            # column: completed AND amount > 100 AND is_paid -> order 2 only.
            ("large_paid_orders", 1),
        ],
    )
    def test_a_dimension_on_the_metrics_own_model_is_folded_in(
        self, source: DbtSource, db: duckdb.DuckDBPyConnection, name: str, expected
    ) -> None:
        assert _value(source, db, name) == expected

    def test_the_folded_filter_names_a_column_not_a_metricflow_path(
        self, source: DbtSource
    ) -> None:
        metric = source.get_metric("completed_revenue")
        assert metric is not None
        assert metric.untranslated is None
        assert "status" in metric.sql_expression
        assert "order__status" not in metric.sql_expression
        assert "{{" not in metric.sql_expression
        assert metric.filters == []  # folded, so nothing is left beside it

    def test_fill_nulls_with_replaces_an_empty_result(
        self, source: DbtSource, db: duckdb.DuckDBPyConnection
    ) -> None:
        # No order is refunded: MetricFlow reports 0, not NULL.
        assert _value(source, db, "refunded_revenue_or_zero") == 0

    def test_a_filtered_count_with_no_match_is_null_as_metricflow_gives(
        self, source: DbtSource, db: duckdb.DuckDBPyConnection
    ) -> None:
        # MetricFlow compiles `count` to SUM(CASE WHEN e IS NOT NULL ...), so
        # a count over rows no filter admits is NULL, not COUNT's 0.
        metric = source.get_metric("completed_orders")
        assert metric is not None
        row = db.execute(
            f"SELECT {metric.sql_expression} FROM orders WHERE status = 'cancelled'"
        ).fetchone()
        assert row == (None,)

    def test_no_matching_row_gives_null_like_a_where_clause(
        self, source: DbtSource, db: duckdb.DuckDBPyConnection
    ) -> None:
        metric = source.get_metric("completed_revenue")
        assert metric is not None
        row = db.execute(
            f"SELECT {metric.sql_expression} FROM orders WHERE status = 'cancelled'"
        ).fetchone()
        assert row == (None,)


class TestUntranslated:
    """Refused with a reason, and never a half-translated expression."""

    @pytest.mark.parametrize(
        ("name", "reason"),
        [
            ("eu_revenue", "customer__region"),  # a join to another model
            ("jan_revenue", "TimeDimension"),  # a grain, not a column
            # A time dimension through Dimension() is truncated to its grain
            # by MetricFlow; the raw column would compare differently.
            ("orders_since_mid_jan", "time dimension"),
            ("p90_order", "percentile"),  # dialect-specific SQL
            ("balance", "non_additive"),  # semi-additive: not a plain SUM
            ("order_value", "ratio"),
            ("revenue_less_largest", "derived"),
            ("running_revenue", "cumulative"),
        ],
    )
    def test_the_reason_is_named_and_no_sql_is_emitted(
        self, source: DbtSource, name: str, reason: str
    ) -> None:
        metric = source.get_metric(name)
        assert metric is not None
        assert metric.untranslated is not None
        assert reason in metric.untranslated
        assert metric.sql_expression == ""

    def test_an_untranslated_metric_is_still_listed_and_searchable(
        self, source: DbtSource
    ) -> None:
        # The agent must learn the metric exists and why its SQL is missing.
        assert "order_value" in [m.name for m in source.get_metrics()]


class TestLatestSpec:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("paid_total", 30.0),
            ("completed_paid_total", 10.0),
            # `expr` omitted: defaults to the metric's own name.
            ("fee", 1.5),
        ],
    )
    def test_metric_aggregation_params_are_assembled(
        self, source: DbtSource, db: duckdb.DuckDBPyConnection, name: str, expected
    ) -> None:
        assert _value(source, db, name) == expected

    def test_the_source_model_is_the_nested_semantic_models_relation(
        self, source: DbtSource
    ) -> None:
        metric = source.get_metric("paid_total")
        assert metric is not None
        assert metric.source_model == "main.payments"


def test_unfiltered_metrics_agree_with_dbts_own_osi_export(
    source: DbtSource, db: duckdb.DuckDBPyConnection, fixtures_dir: Path
) -> None:
    """A differential check against dbt's compiler, where dbt is right.

    dbt >= 1.12 also writes ``osi_document.json``, one expression per metric.
    For an additive, unfiltered metric that export is correct, so ours must
    compute the same value. (For filters, semi-additive and cumulative metrics
    it is not: it keeps MetricFlow paths like ``order__status`` and drops
    window semantics, which is why DbtSource does its own assembly.)
    """
    osi = json.loads((fixtures_dir / "dbt_metricflow_osi_document.json").read_text())
    theirs = {
        m["name"]: m["expression"]["dialects"][0]["expression"]
        for m in osi["semantic_model"][0]["metrics"]
    }
    compared = 0
    for name in (
        "revenue",
        "orders_placed",
        "customers_ordering",
        "average_order",
        "largest_order",
        "paid_order_count",
        "amount_default_expr",
        "paid_total",
        "fee",
    ):
        metric = source.get_metric(name)
        assert metric is not None
        table = metric.source_model.split(".")[-1]
        ours = db.execute(f"SELECT {metric.sql_expression} FROM {table}").fetchone()
        dbt = db.execute(f"SELECT {theirs[name]} FROM {table}").fetchone()
        assert ours == dbt, (name, metric.sql_expression, theirs[name])
        compared += 1
    assert compared == 9


class TestOlderAndMalformedManifests:
    def _load(self, tmp_path: Path, metrics: dict, semantic_models: dict | None = None):
        doc = {"nodes": {}, "metrics": metrics}
        if semantic_models is not None:
            doc["semantic_models"] = semantic_models
        path = tmp_path / "manifest.json"
        path.write_text(json.dumps(doc))
        return DbtSource(path)

    def test_a_pre_1_6_dbt_metrics_manifest_keeps_what_it_carried(
        self, tmp_path: Path
    ) -> None:
        # dbt <= 1.5 (the dbt_metrics package): calculation_method/expression,
        # a top-level `filters` list and `model`. Its filters and model were
        # read before #123 and must not be lost; its SQL is not assembled.
        src = self._load(
            tmp_path,
            {
                "metric.p.revenue": {
                    "name": "revenue",
                    "calculation_method": "sum",
                    "expression": "amount",
                    "model": "ref('orders')",
                    "filters": [
                        {"field": "status", "operator": "=", "value": "'completed'"}
                    ],
                }
            },
        )
        metric = src.get_metric("revenue")
        assert metric is not None
        assert metric.filters == ["status = 'completed'"]
        assert metric.source_model == "ref('orders')"
        assert metric.untranslated is not None
        assert "dbt_metrics" in metric.untranslated

    @pytest.mark.parametrize("bad", [["sum"], {"x": 1}])
    def test_an_unhashable_agg_is_untranslated_not_a_type_error(
        self, tmp_path: Path, bad: object
    ) -> None:
        src = self._load(
            tmp_path,
            {
                "metric.p.m": {
                    "name": "m",
                    "type": "simple",
                    "type_params": {"measure": {"name": "x"}},
                }
            },
            {
                "semantic_model.p.s": {
                    "name": "s",
                    "node_relation": {"schema_name": "a", "alias": "t"},
                    "entities": [{"name": "e", "type": ["primary"]}],
                    "measures": [{"name": "x", "agg": bad, "expr": "v"}],
                }
            },
        )
        metric = src.get_metric("m")
        assert metric is not None
        assert metric.untranslated is not None
