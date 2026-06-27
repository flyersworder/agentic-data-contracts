from datetime import date, datetime
from pathlib import Path

import pytest

from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.semantic.yaml_source import YamlSource, _parse_date


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
    assert len(schema.columns) == 5
    col_names = [c.name for c in schema.columns]
    assert "id" in col_names
    assert "amount" in col_names


def test_get_table_schema_not_found(source: YamlSource) -> None:
    schema = source.get_table_schema("analytics", "nonexistent")
    assert schema is None


def test_metric_parses_owners_and_last_reviewed(source: YamlSource) -> None:
    """Owners and the review date are read off the metric (ISO string → date)."""
    metric = source.get_metric("total_revenue")
    assert metric is not None
    assert metric.business_owner == "revenue-platform"
    assert metric.operational_owner == "data-eng-finance"
    assert metric.last_reviewed == date(2020, 1, 1)


def test_metric_without_governance_fields_defaults_none(source: YamlSource) -> None:
    """Metrics that omit the new keys keep None defaults — no breakage."""
    metric = source.get_metric("active_customers")
    assert metric is not None
    assert metric.business_owner is None
    assert metric.operational_owner is None
    assert metric.last_reviewed is None


def test_parse_date_normalizes_datetime_to_date() -> None:
    """A YAML scalar with a time component (parsed as datetime) → date.

    datetime subclasses date, so without normalization it would slip through and
    later crash the staleness arithmetic with `date - datetime`.
    """
    assert _parse_date(datetime(2020, 1, 1, 12, 30, 0)) == date(2020, 1, 1)


def test_parse_date_passes_through_native_date() -> None:
    assert _parse_date(date(2020, 1, 1)) == date(2020, 1, 1)


def test_parse_date_none_is_none() -> None:
    assert _parse_date(None) is None


def test_parse_date_malformed_string_raises_clear_error() -> None:
    """A bad ISO string fails fast with a message naming the offending value."""
    with pytest.raises(ValueError, match="last_reviewed must be an ISO date"):
        _parse_date("2020-13-01")
