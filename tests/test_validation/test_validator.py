from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.validator import Validator


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
