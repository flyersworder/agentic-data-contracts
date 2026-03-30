"""Tests for scalability improvements: compact prompt, pagination, caching."""

from unittest.mock import MagicMock

from agentic_data_contracts.adapters.base import DatabaseAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.semantic.base import MetricDefinition, Relationship


class FakeSemanticSource:
    """Fake source with configurable metric count."""

    def __init__(self, count: int) -> None:
        self._metrics = [
            MetricDefinition(
                name=f"metric_{i}",
                description=f"Description for metric {i}",
                sql_expression=f"SUM(col_{i})",
            )
            for i in range(count)
        ]

    def get_metrics(self) -> list[MetricDefinition]:
        return list(self._metrics)

    def get_metric(self, name: str) -> MetricDefinition | None:
        for m in self._metrics:
            if m.name == name:
                return m
        return None

    def get_table_schema(self, schema: str, table: str):  # noqa: ANN201
        return None

    def search_metrics(self, query: str) -> list[MetricDefinition]:
        return []

    def get_relationships(self) -> list[Relationship]:
        return []


def _make_contract_with_domains(
    metric_names: list[str],
) -> DataContract:
    domains = {
        "domain_a": metric_names[: len(metric_names) // 2],
        "domain_b": metric_names[len(metric_names) // 2 :],
    }
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "public", "tables": ["t"]}),
            ],
            domains=domains,
        ),
    )
    return DataContract(schema)


class TestCompactMetricPrompt:
    def test_small_set_lists_all_metrics(self) -> None:
        source = FakeSemanticSource(5)
        dc = _make_contract_with_domains([f"metric_{i}" for i in range(5)])
        prompt = dc.to_system_prompt(semantic_source=source)
        # Should list individual metric descriptions
        assert 'name="metric_0"' in prompt
        assert 'name="metric_4"' in prompt

    def test_large_set_shows_domain_counts(self) -> None:
        source = FakeSemanticSource(30)
        dc = _make_contract_with_domains([f"metric_{i}" for i in range(30)])
        prompt = dc.to_system_prompt(semantic_source=source)
        # Should NOT list individual metrics
        assert 'name="metric_0"' not in prompt
        # Should show domain counts
        assert 'name="domain_a" count="15"' in prompt
        assert 'name="domain_b" count="15"' in prompt
        assert "list_metrics" in prompt

    def test_large_set_no_domains_shows_count(self) -> None:
        source = FakeSemanticSource(30)
        schema = DataContractSchema(
            name="test",
            semantic=SemanticConfig(
                allowed_tables=[
                    AllowedTable.model_validate({"schema": "public", "tables": ["t"]}),
                ],
            ),
        )
        dc = DataContract(schema)
        prompt = dc.to_system_prompt(semantic_source=source)
        assert "30 metrics available" in prompt
        assert "metric_0 \u2014" not in prompt

    def test_threshold_boundary(self) -> None:
        # Exactly at threshold — should still list individually
        source = FakeSemanticSource(20)
        schema = DataContractSchema(
            name="test",
            semantic=SemanticConfig(
                allowed_tables=[
                    AllowedTable.model_validate({"schema": "public", "tables": ["t"]}),
                ],
            ),
        )
        dc = DataContract(schema)
        prompt = dc.to_system_prompt(semantic_source=source)
        assert 'name="metric_0"' in prompt

        # One above threshold — compact mode
        source = FakeSemanticSource(21)
        prompt = dc.to_system_prompt(semantic_source=source)
        assert 'name="metric_0"' not in prompt
        assert "21 metrics available" in prompt


class TestWildcardCaching:
    def test_resolve_tables_caches(self) -> None:
        dc = DataContract(
            DataContractSchema(
                name="test",
                semantic=SemanticConfig(
                    allowed_tables=[
                        AllowedTable.model_validate({"schema": "s", "tables": ["*"]}),
                    ],
                ),
            )
        )
        mock_adapter = MagicMock(spec=DatabaseAdapter)
        mock_adapter.list_tables.return_value = ["t1", "t2"]

        dc.resolve_tables(mock_adapter)
        assert "s.t1" in dc.allowed_table_names()
        assert mock_adapter.list_tables.call_count == 1

        # Second call should be a no-op
        dc.resolve_tables(mock_adapter)
        assert mock_adapter.list_tables.call_count == 1
