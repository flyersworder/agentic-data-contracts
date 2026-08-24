from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.validator import Validator


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


def test_validate_names_a_relative_time_node(contract: DataContract) -> None:
    result = Validator(contract).validate(
        "SELECT amount, created_at FROM analytics.orders"
        " WHERE tenant_id = 'acme' AND created_at > CURRENT_DATE - 7"
    )
    assert result.relative_time is not None


def test_validate_reports_none_for_a_pinned_window(contract: DataContract) -> None:
    result = Validator(contract).validate(
        "SELECT amount, created_at FROM analytics.orders"
        " WHERE tenant_id = 'acme' AND created_at > '2026-01-01'"
    )
    assert result.relative_time is None


def test_unparseable_sql_leaves_relative_time_none(contract: DataContract) -> None:
    result = Validator(contract).validate("NOT SQL AT ALL ((")
    assert result.parse_error is True
    assert result.relative_time is None
