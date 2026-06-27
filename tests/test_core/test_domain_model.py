"""Tests for the Domain Pydantic model."""

from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Domain,
    SemanticConfig,
)


def test_domain_forbids_unknown_field():
    """A stale `metrics:` key from a pre-0.26 contract must fail loudly, not be
    silently dropped (which would leave the domain with no members)."""
    with pytest.raises(ValidationError):
        Domain.model_validate(
            {
                "name": "revenue",
                "summary": "Financial metrics",
                "description": "Revenue domain.",
                "metrics": ["total_revenue"],  # removed in 0.26 — now forbidden
            }
        )


def test_domain_model_basic():
    d = Domain(
        name="revenue",
        summary="Financial metrics",
        description="Revenue tracks recognized revenue from completed orders.",
    )
    assert d.name == "revenue"
    assert d.summary == "Financial metrics"
    assert d.description == "Revenue tracks recognized revenue from completed orders."
    assert d.tables == []


def test_domain_model_with_tables():
    d = Domain(
        name="revenue",
        summary="Financial metrics",
        description="Revenue domain.",
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
            ),
            Domain(
                name="engagement",
                summary="Customer activity",
                description="Engagement domain.",
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
    # Domains carry catalog metadata, not membership (that is metric-first).
    assert revenue.business_owner == "revenue-platform"
    assert revenue.last_reviewed is not None

    engagement = domains[1]
    assert engagement.name == "engagement"
    # engagement intentionally omits owners + last_reviewed (negative case).
    assert engagement.business_owner is None


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
                ),
                Domain(
                    name="engagement",
                    summary="Customer activity",
                    description="Engagement domain.",
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


def test_allowed_table_description_and_preferred():
    entry = AllowedTable.model_validate(
        {
            "schema": "analytics",
            "tables": ["orders"],
            "description": "Curated analytics tables",
            "preferred": True,
        }
    )
    assert entry.description == "Curated analytics tables"
    assert entry.preferred is True


def test_allowed_table_defaults():
    entry = AllowedTable.model_validate({"schema": "raw", "tables": ["events"]})
    assert entry.description is None
    assert entry.preferred is False


def test_domain_last_reviewed_defaults_none():
    """Without last_reviewed, the field is None — callers treat this as 'never'."""
    d = Domain(
        name="revenue",
        summary="Financial metrics",
        description="Revenue domain.",
    )
    assert d.last_reviewed is None


def test_domain_last_reviewed_parses_iso_date():
    """YAML dates arrive as ISO strings via model_validate; Pydantic coerces to date."""
    d = Domain.model_validate(
        {
            "name": "revenue",
            "summary": "Financial metrics",
            "description": "Revenue domain.",
            "last_reviewed": "2026-02-01",
        }
    )
    assert d.last_reviewed == date(2026, 2, 1)


def test_domain_owners_default_none():
    """Without owners, both fields are None — callers treat this as 'unowned'."""
    d = Domain(
        name="revenue",
        summary="Financial metrics",
        description="Revenue domain.",
    )
    assert d.business_owner is None
    assert d.operational_owner is None


def test_domain_owners_set():
    """Owners are team identifiers (business owns the definition)."""
    d = Domain.model_validate(
        {
            "name": "revenue",
            "summary": "Financial metrics",
            "description": "Revenue domain.",
            "business_owner": "revenue-platform",
            "operational_owner": "data-eng-finance",
        }
    )
    assert d.business_owner == "revenue-platform"
    assert d.operational_owner == "data-eng-finance"
