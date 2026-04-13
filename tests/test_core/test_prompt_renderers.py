"""Tests for PromptRenderer protocol and ClaudePromptRenderer."""

from __future__ import annotations

from pathlib import Path

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.prompt import ClaudePromptRenderer, PromptRenderer
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Domain,
    SemanticConfig,
)
from agentic_data_contracts.semantic.base import MetricDefinition, Relationship
from agentic_data_contracts.semantic.yaml_source import YamlSource

# ---------------------------------------------------------------------------
# FakeSemanticSource — mirrors test_scalability.py
# ---------------------------------------------------------------------------


class FakeSemanticSource:
    """Fake source with configurable metric count."""

    def __init__(
        self, count: int, *, domains: dict[str, list[str]] | None = None
    ) -> None:
        self._metrics = [
            MetricDefinition(
                name=f"metric_{i}",
                description=f"Description for metric {i}",
                sql_expression=f"SUM(col_{i})",
            )
            for i in range(count)
        ]
        self._domains = domains

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

    def get_relationships_for_table(self, table: str) -> list[Relationship]:
        return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


def _make_contract_with_domains(metric_names: list[str]) -> DataContract:
    half = len(metric_names) // 2
    domains = [
        Domain(
            name="domain_a",
            summary="Domain A",
            description="Domain A metrics.",
            metrics=metric_names[:half],
        ),
        Domain(
            name="domain_b",
            summary="Domain B",
            description="Domain B metrics.",
            metrics=metric_names[half:],
        ),
    ]
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


def _make_minimal_contract() -> DataContract:
    schema = DataContractSchema(
        name="minimal",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "public", "tables": ["t"]}),
            ],
        ),
    )
    return DataContract(schema)


# ---------------------------------------------------------------------------
# Test 1 — protocol conformance
# ---------------------------------------------------------------------------


def test_claude_renderer_satisfies_protocol() -> None:
    renderer = ClaudePromptRenderer()
    assert isinstance(renderer, PromptRenderer)


# ---------------------------------------------------------------------------
# Test 2 — allowed_tables section
# ---------------------------------------------------------------------------


def test_claude_renderer_allowed_tables(fixtures_dir: Path) -> None:
    contract = _load(fixtures_dir)
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract)

    assert '<data_contract name="revenue-analysis">' in output
    assert "</data_contract>" in output
    assert "<allowed_tables>" in output
    assert "</allowed_tables>" in output
    assert "Only query these tables:" in output
    assert "analytics.orders" in output
    assert "analytics.customers" in output
    assert "analytics.subscriptions" in output


# ---------------------------------------------------------------------------
# Test 3 — constraints section
# ---------------------------------------------------------------------------


def test_claude_renderer_constraints(fixtures_dir: Path) -> None:
    contract = _load(fixtures_dir)
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract)

    assert "<constraints>" in output
    assert "</constraints>" in output

    # forbidden operations
    assert "DELETE" in output
    assert "DROP" in output
    assert "TRUNCATE" in output
    assert "UPDATE" in output
    assert "INSERT" in output

    # block rules
    assert "violations block execution" in output
    assert "tenant_isolation" in output
    assert "no_select_star" in output

    # warn rules
    assert "violations produce warnings" in output
    assert "use_approved_metrics" in output


# ---------------------------------------------------------------------------
# Test 4 — resource_limits section
# ---------------------------------------------------------------------------


def test_claude_renderer_resource_limits(fixtures_dir: Path) -> None:
    contract = _load(fixtures_dir)
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract)

    assert "<resource_limits>" in output
    assert "</resource_limits>" in output

    # resource values
    assert "5.00" in output  # cost_limit_usd
    assert "30" in output  # max_query_time_seconds
    assert "3" in output  # max_retries
    assert "1000000" in output or "1,000,000" in output  # max_rows_scanned
    assert "50000" in output or "50,000" in output  # token_budget

    # temporal value merged in
    assert "300" in output  # max_duration_seconds


# ---------------------------------------------------------------------------
# Test 5 — no resources omits section
# ---------------------------------------------------------------------------


def test_claude_renderer_no_resources() -> None:
    contract = _make_minimal_contract()
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract)

    assert "<resource_limits>" not in output


def test_claude_renderer_no_constraints() -> None:
    """Contracts with no forbidden ops or rules omit the constraints section."""
    contract = _make_minimal_contract()
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract)

    assert "<constraints>" not in output


# ---------------------------------------------------------------------------
# Test 6 — small metric set with semantic_source.yml
# ---------------------------------------------------------------------------


def test_claude_renderer_metrics_small_set(fixtures_dir: Path) -> None:
    contract = _load(fixtures_dir)
    source = YamlSource(fixtures_dir / "semantic_source.yml")
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract, semantic_source=source)

    assert "<available_metrics>" in output
    assert "</available_metrics>" in output
    assert "total_revenue" in output
    assert "active_customers" in output


# ---------------------------------------------------------------------------
# Test 7 — large metric set (>20) with domains
# ---------------------------------------------------------------------------


def test_claude_renderer_metrics_large_set_with_domains() -> None:
    contract = _make_contract_with_domains([f"metric_{i}" for i in range(30)])
    source = FakeSemanticSource(30)
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract, semantic_source=source)

    assert "<available_metrics>" in output
    # Should NOT list individual metric descriptions
    assert "metric_0 —" not in output
    # Should show domain summaries
    assert "domain_a" in output
    assert "domain_b" in output


# ---------------------------------------------------------------------------
# Test 8 — large metric set (>20) without domains
# ---------------------------------------------------------------------------


def test_claude_renderer_metrics_large_set_no_domains() -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "public", "tables": ["t"]}),
            ],
        ),
    )
    contract = DataContract(schema)
    source = FakeSemanticSource(30)
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract, semantic_source=source)

    assert "<available_metrics>" in output
    assert "30" in output
    # Should NOT list individual metrics
    assert "metric_0 —" not in output


# ---------------------------------------------------------------------------
# Test 9 — no semantic source but contract has source config → fallback tag
# ---------------------------------------------------------------------------


def test_claude_renderer_no_semantic_source_with_config(fixtures_dir: Path) -> None:
    contract = _load(fixtures_dir)
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract, semantic_source=None)

    # Should fall back to <semantic_source> tag
    assert "<semantic_source>" in output
    assert "</semantic_source>" in output
    assert "dbt" in output
    assert "<available_metrics>" not in output


# ---------------------------------------------------------------------------
# Test 10 — relationships present
# ---------------------------------------------------------------------------


def test_claude_renderer_relationships(fixtures_dir: Path) -> None:
    contract = _load(fixtures_dir)
    source = YamlSource(fixtures_dir / "semantic_source.yml")
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract, semantic_source=source)

    assert "<table_relationships>" in output
    assert "</table_relationships>" in output
    assert "analytics.orders.customer_id" in output
    assert "analytics.customers.id" in output


# ---------------------------------------------------------------------------
# Test 11 — no relationships omits section
# ---------------------------------------------------------------------------


def test_claude_renderer_no_relationships() -> None:
    contract = _make_minimal_contract()
    source = FakeSemanticSource(5)  # get_relationships() returns []
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract, semantic_source=source)

    assert "<table_relationships>" not in output


# ---------------------------------------------------------------------------
# Test 12 — custom renderer via to_system_prompt
# ---------------------------------------------------------------------------


def test_custom_renderer_via_to_system_prompt(fixtures_dir: Path) -> None:
    """Users can pass their own renderer to to_system_prompt."""

    class MyRenderer:
        def render(self, contract, semantic_source=None):
            return f"CUSTOM: {contract.name}"

    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    prompt = dc.to_system_prompt(renderer=MyRenderer())
    assert prompt == "CUSTOM: revenue-analysis"


def test_to_system_prompt_defaults_to_claude_renderer(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    prompt = dc.to_system_prompt()
    assert '<data_contract name="revenue-analysis">' in prompt
    assert "<constraints>" in prompt
    assert "</data_contract>" in prompt
