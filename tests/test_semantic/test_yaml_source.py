from pathlib import Path

import pytest

from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.semantic.yaml_source import YamlSource


@pytest.fixture
def source(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


def test_source_implements_protocol(source: YamlSource) -> None:
    assert isinstance(source, SemanticSource)


def test_get_metrics(source: YamlSource) -> None:
    metrics = source.get_metrics()
    assert len(metrics) == 2
    names = [m.name for m in metrics]
    assert "total_revenue" in names
    assert "active_customers" in names


def test_get_metric(source: YamlSource) -> None:
    metric = source.get_metric("total_revenue")
    assert metric is not None
    assert metric.name == "total_revenue"
    assert "SUM(amount)" in metric.sql_expression
    assert metric.source_model == "analytics.orders"


def test_get_metric_not_found(source: YamlSource) -> None:
    metric = source.get_metric("nonexistent")
    assert metric is None


def test_get_table_schema(source: YamlSource) -> None:
    schema = source.get_table_schema("analytics", "orders")
    assert schema is not None
    assert len(schema.columns) == 4
    col_names = [c.name for c in schema.columns]
    assert "id" in col_names
    assert "amount" in col_names


def test_get_table_schema_not_found(source: YamlSource) -> None:
    schema = source.get_table_schema("analytics", "nonexistent")
    assert schema is None
