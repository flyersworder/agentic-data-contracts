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
