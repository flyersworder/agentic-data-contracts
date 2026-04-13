"""Tests for the Domain Pydantic model."""

from pathlib import Path

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Domain,
    SemanticConfig,
)


def test_domain_model_basic():
    d = Domain(
        name="revenue",
        summary="Financial metrics",
        description="Revenue tracks recognized revenue from completed orders.",
        metrics=["total_revenue", "mrr"],
    )
    assert d.name == "revenue"
    assert d.summary == "Financial metrics"
    assert d.description == "Revenue tracks recognized revenue from completed orders."
    assert d.metrics == ["total_revenue", "mrr"]
    assert d.tables == []


def test_domain_model_with_tables():
    d = Domain(
        name="revenue",
        summary="Financial metrics",
        description="Revenue domain.",
        metrics=["total_revenue"],
        tables=["analytics.orders", "analytics.invoices"],
    )
    assert d.tables == ["analytics.orders", "analytics.invoices"]


def test_semantic_config_with_domains():
    config = SemanticConfig(
        allowed_tables=[
            AllowedTable.model_validate({"schema": "analytics", "tables": ["orders"]})
        ],
        domains=[
            Domain(
                name="revenue",
                summary="Financial metrics",
                description="Revenue domain.",
                metrics=["total_revenue"],
            ),
            Domain(
                name="engagement",
                summary="Customer activity",
                description="Engagement domain.",
                metrics=["active_customers"],
            ),
        ],
    )
    assert len(config.domains) == 2
    assert config.domains[0].name == "revenue"


def test_semantic_config_domains_default_empty():
    config = SemanticConfig(
        allowed_tables=[
            AllowedTable.model_validate({"schema": "analytics", "tables": ["orders"]})
        ],
    )
    assert config.domains == []


def test_domain_in_full_contract_schema():
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                )
            ],
            domains=[
                Domain(
                    name="revenue",
                    summary="Financial metrics",
                    description="Revenue domain.",
                    metrics=["total_revenue"],
                ),
            ],
        ),
    )
    assert schema.semantic.domains[0].name == "revenue"


def test_domain_from_yaml(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    domains = dc.schema.semantic.domains
    assert len(domains) == 2

    revenue = domains[0]
    assert revenue.name == "revenue"
    assert revenue.summary != ""
    assert revenue.description != ""
    assert "total_revenue" in revenue.metrics

    engagement = domains[1]
    assert engagement.name == "engagement"
    assert "active_customers" in engagement.metrics


def test_get_domain_exact_match():
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                )
            ],
            domains=[
                Domain(
                    name="revenue",
                    summary="Financial metrics",
                    description="Revenue domain.",
                    metrics=["total_revenue"],
                ),
                Domain(
                    name="engagement",
                    summary="Customer activity",
                    description="Engagement domain.",
                    metrics=["active_customers"],
                ),
            ],
        ),
    )
    dc = DataContract(schema)

    result = dc.get_domain("revenue")
    assert result is not None
    assert result.name == "revenue"

    result = dc.get_domain("engagement")
    assert result is not None
    assert result.name == "engagement"

    result = dc.get_domain("nonexistent")
    assert result is None
