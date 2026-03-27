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
