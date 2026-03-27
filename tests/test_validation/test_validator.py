from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.explain import ExplainResult
from agentic_data_contracts.validation.validator import Validator


class FakeExplainAdapter:
    def __init__(self, result: ExplainResult) -> None:
        self._result = result

    def explain(self, sql: str) -> ExplainResult:
        return self._result


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def validator(contract: DataContract) -> Validator:
    return Validator(contract)


def test_valid_query_passes(validator: Validator) -> None:
    result = validator.validate(
        "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"
    )
    assert not result.blocked
    assert result.reasons == []


def test_forbidden_table_blocks(validator: Validator) -> None:
    result = validator.validate("SELECT id FROM raw.payments WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("raw.payments" in r for r in result.reasons)


def test_select_star_blocks(validator: Validator) -> None:
    result = validator.validate("SELECT * FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("SELECT *" in r for r in result.reasons)


def test_missing_filter_blocks(validator: Validator) -> None:
    result = validator.validate("SELECT id FROM analytics.orders")
    assert result.blocked
    assert any("tenant_id" in r for r in result.reasons)


def test_delete_blocks(validator: Validator) -> None:
    result = validator.validate("DELETE FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("DELETE" in r for r in result.reasons)


def test_multiple_violations_all_reported(validator: Validator) -> None:
    result = validator.validate("SELECT * FROM raw.payments")
    assert result.blocked
    assert len(result.reasons) >= 2


def test_warnings_returned(validator: Validator) -> None:
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert not result.blocked
    assert result.warnings == []


def test_minimal_contract_permissive(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")
    validator = Validator(dc)
    result = validator.validate("SELECT * FROM public.users")
    assert not result.blocked


def test_explain_cost_exceeds_limit(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=10.0, estimated_rows=100, schema_valid=True)
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("cost" in r.lower() for r in result.reasons)


def test_explain_rows_exceeds_limit(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=1.0, estimated_rows=2000000, schema_valid=True)
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("rows" in r.lower() for r in result.reasons)


def test_explain_schema_invalid_blocks(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(
            estimated_cost_usd=None,
            estimated_rows=None,
            schema_valid=False,
            errors=["Column not found"],
        )
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked


def test_explain_within_limits_passes(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=1.0, estimated_rows=500, schema_valid=True)
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert not result.blocked


def test_explicit_filter_column() -> None:
    from agentic_data_contracts.core.schema import (
        AllowedTable,
        DataContractSchema,
        Enforcement,
        SemanticConfig,
        SemanticRule,
    )

    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "public", "tables": ["users"]})
            ],
            rules=[
                SemanticRule(
                    name="org_filter",
                    description="Must filter by organization",
                    enforcement=Enforcement.BLOCK,
                    filter_column="org_id",
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    validator = Validator(dc)
    result = validator.validate("SELECT id FROM public.users")
    assert result.blocked
    assert any("org_id" in r for r in result.reasons)

    result = validator.validate("SELECT id FROM public.users WHERE org_id = 'x'")
    assert not result.blocked
