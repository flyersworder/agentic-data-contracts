"""CubeSource assembles a measure the way Cube defines it (#123).

A Cube measure is `sql` (a per-row expression) aggregated by `type`, counted
only where every `filters` entry holds, with `{CUBE}` and `{member}`
references in both. CubeSource used to pass `sql` through alone, handing
agents `amount` for a revenue metric. Assertions execute the expression.
"""

from pathlib import Path

import duckdb
import pytest

from agentic_data_contracts.semantic.cube import CubeSource


@pytest.fixture
def source(fixtures_dir: Path) -> CubeSource:
    return CubeSource(fixtures_dir / "cube_measures.yml")


@pytest.fixture(scope="module")
def db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE orders (id INT, customer_id INT, amount DOUBLE,
                             cost DOUBLE, status VARCHAR);
        INSERT INTO orders VALUES
            (1, 1, 10.0,  4.0,  'completed'),
            (2, 1, 250.0, 100.0, 'completed'),
            (3, 2, 40.0,  10.0, 'cancelled'),
            (4, 3, 300.0, 200.0, 'completed'),
            (5, NULL, NULL, NULL, 'completed');
        """
    )
    return con


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("count", 5),  # no `sql`: every row
        ("counted_ids", 5),
        ("revenue", 600.0),
        ("revenue_qualified", 600.0),  # {CUBE}.amount
        ("revenue_legacy_syntax", 600.0),  # ${CUBE}.amount
        ("customers_ordering", 3),
        ("average_order", 150.0),
        ("largest_order", 300.0),
        ("smallest_order", 10.0),
        ("margin", 286.0),  # `number`: already an aggregate, passed through
        ("completed_count", 4),
        # completed AND amount > 100 (a dimension reference): orders 2 and 4.
        ("large_completed_revenue", 550.0),
        ("completed_status_revenue", 560.0),  # {CUBE.status}
    ],
)
def test_the_measure_computes_what_cube_would(
    source: CubeSource, db: duckdb.DuckDBPyConnection, name: str, expected
) -> None:
    metric = source.get_metric(name)
    assert metric is not None
    assert metric.untranslated is None, metric.untranslated
    assert "{" not in metric.sql_expression
    row = db.execute(f"SELECT {metric.sql_expression} FROM orders").fetchone()
    assert row == (expected,)


@pytest.mark.parametrize(
    ("name", "reason"),
    [
        ("conversion", "completed_count"),  # refers to other measures
        ("approx_customers", "count_distinct_approx"),
        ("rolling_revenue", "rolling_window"),
        ("eu_revenue", "customers"),  # a dimension on another cube
        ("filtered_margin", "filters"),  # a `number` is already aggregated
        ("running", "running_total"),
        ("untyped", "no type"),
        ("filter_without_sql", "no sql"),
        ("by_band", "size_band"),  # a `case:` dimension has no sql
        ("multi_stage_revenue", "multi_stage"),
    ],
)
def test_an_untranslatable_measure_says_why(
    source: CubeSource, name: str, reason: str
) -> None:
    metric = source.get_metric(name)
    assert metric is not None
    assert metric.untranslated is not None
    assert reason in metric.untranslated
    assert metric.sql_expression == ""


def test_the_source_model_is_the_cubes_table(source: CubeSource) -> None:
    metric = source.get_metric("revenue")
    assert metric is not None
    assert metric.source_model == "main.orders"
