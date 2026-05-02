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
from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    MetricImpact,
    Relationship,
)
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

    def get_metric_impacts(self) -> list[MetricImpact]:
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


def test_claude_renderer_allowed_tables_with_schema_annotations() -> None:
    """description and preferred on AllowedTable should surface in the prompt."""
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {
                        "schema": "analytics",
                        "tables": ["orders"],
                        "description": "Curated analytics layer",
                        "preferred": True,
                    }
                ),
                AllowedTable.model_validate(
                    {
                        "schema": "raw",
                        "tables": ["events"],
                        "description": "Raw ingestion tables",
                    }
                ),
                AllowedTable.model_validate({"schema": "staging", "tables": ["_tmp"]}),
            ],
        ),
    )
    output = ClaudePromptRenderer().render(DataContract(schema))

    # Annotated schemas appear with their metadata.
    assert 'name="analytics"' in output
    assert 'preferred="true"' in output
    assert 'description="Curated analytics layer"' in output
    assert 'name="raw"' in output
    assert 'description="Raw ingestion tables"' in output

    # Schemas without annotations are not listed in the schema-annotation block
    # (they still appear in the flat table list).
    assert 'name="staging"' not in output
    assert "staging._tmp" in output


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

    # valid_contract.yml has domains, so we get <available_domains> instead
    assert "<available_domains>" in output
    assert "</available_domains>" in output
    assert "revenue" in output
    assert "engagement" in output


# ---------------------------------------------------------------------------
# Test 7 — large metric set (>20) with domains
# ---------------------------------------------------------------------------


def test_claude_renderer_metrics_large_set_with_domains() -> None:
    contract = _make_contract_with_domains([f"metric_{i}" for i in range(30)])
    source = FakeSemanticSource(30)
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract, semantic_source=source)

    assert "<available_domains>" in output
    assert "domain_a" in output
    assert "domain_b" in output
    assert "metric_0 —" not in output


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


def test_claude_renderer_no_semantic_source_with_config_and_domains(
    fixtures_dir: Path,
) -> None:
    """Contract with domains + source config but no source object → domain index."""
    contract = _load(fixtures_dir)
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract, semantic_source=None)

    assert "<available_domains>" in output
    assert "revenue" in output
    assert "<available_metrics>" not in output


def test_claude_renderer_no_semantic_source_with_config_no_domains() -> None:
    """Contract with source config but no domains → semantic_source fallback tag."""
    from agentic_data_contracts.core.schema import (
        SemanticSource as SemanticSourceConfig,
    )

    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "public", "tables": ["t"]})
            ],
            source=SemanticSourceConfig(type="dbt", path="./manifest.json"),
        ),
    )
    contract = DataContract(schema)
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract, semantic_source=None)

    assert "<semantic_source>" in output
    assert "</semantic_source>" in output
    assert "dbt" in output
    assert "<available_domains>" not in output
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


def test_claude_renderer_relationship_preferred_attribute(
    fixtures_dir: Path,
) -> None:
    """A Relationship with preferred=True surfaces as preferred="true" in XML."""
    source = YamlSource(fixtures_dir / "relationships_preferred.yml")
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders", "users"]}
                ),
            ],
        ),
    )
    output = ClaudePromptRenderer().render(DataContract(schema), semantic_source=source)

    # Preferred edge carries the attribute on its <relationship> tag.
    assert 'preferred="true"' in output
    # Non-preferred edges in the same fixture do not.
    assert output.count('preferred="true"') == 1


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
        def render(self, contract, semantic_source=None, principal=None):
            return f"CUSTOM: {contract.name}"

    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    prompt = dc.to_system_prompt(renderer=MyRenderer())
    assert prompt == "CUSTOM: revenue-analysis"


def test_to_system_prompt_defaults_to_claude_renderer(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    prompt = dc.to_system_prompt()
    assert '<data_contract name="revenue-analysis">' in prompt
    assert "<constraints>" in prompt


# ---------------------------------------------------------------------------
# Principal-scoped rendering of required_filter_values
# ---------------------------------------------------------------------------


def test_filter_values_rendered_for_caller(fixtures_dir: Path) -> None:
    """When the caller is in the values_by_principal map, render their row inline
    so the agent knows the constraint upfront."""
    dc = DataContract.from_yaml(fixtures_dir / "filter_values_contract.yml")
    prompt = dc.to_system_prompt(principal="partner@co.com")
    assert "partner_account_scope" in prompt
    assert "account_id" in prompt
    # Partner's own values appear:
    assert "123" in prompt
    assert "456" in prompt
    # Other principals' values must NOT leak:
    assert "999" not in prompt
    assert "vip@co.com" not in prompt


def test_filter_values_omitted_for_unmapped_caller(fixtures_dir: Path) -> None:
    """A caller not in the values map should not see any per-principal values
    (the rule does not apply to them)."""
    dc = DataContract.from_yaml(fixtures_dir / "filter_values_contract.yml")
    prompt = dc.to_system_prompt(principal="other@co.com")
    assert "partner_account_scope" in prompt  # rule still listed
    # No principal-specific values leak:
    assert "123" not in prompt
    assert "456" not in prompt
    assert "999" not in prompt


def test_filter_values_omitted_when_no_principal(fixtures_dir: Path) -> None:
    """No identity → render the rule's name/description but not any values."""
    dc = DataContract.from_yaml(fixtures_dir / "filter_values_contract.yml")
    prompt = dc.to_system_prompt()
    assert "partner_account_scope" in prompt
    assert "123" not in prompt
    assert "999" not in prompt
    assert "</data_contract>" in prompt
