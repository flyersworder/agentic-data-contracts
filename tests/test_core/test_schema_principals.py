from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from agentic_data_contracts.core.schema import AllowedTable, DataContractSchema


def test_accepts_allowed_principals() -> None:
    at = AllowedTable.model_validate(
        {"schema": "hr", "tables": ["salaries"], "allowed_principals": ["alice@co.com"]}
    )
    assert at.allowed_principals == ["alice@co.com"]
    assert at.blocked_principals is None


def test_accepts_blocked_principals() -> None:
    at = AllowedTable.model_validate(
        {
            "schema": "raw",
            "tables": ["audit_log"],
            "blocked_principals": ["evil@co.com"],
        }
    )
    assert at.blocked_principals == ["evil@co.com"]
    assert at.allowed_principals is None


def test_rejects_both_fields_set() -> None:
    with pytest.raises(ValidationError, match="cannot set both"):
        AllowedTable.model_validate(
            {
                "schema": "hr",
                "tables": ["salaries"],
                "allowed_principals": ["alice@co.com"],
                "blocked_principals": ["evil@co.com"],
            }
        )


def test_defaults_are_none() -> None:
    at = AllowedTable.model_validate({"schema": "analytics", "tables": ["orders"]})
    assert at.allowed_principals is None
    assert at.blocked_principals is None


def test_empty_list_preserved() -> None:
    # Explicitly empty list must stay [] (meaning "nobody"), not become None.
    at = AllowedTable.model_validate(
        {"schema": "sealed", "tables": ["top_secret"], "allowed_principals": []}
    )
    assert at.allowed_principals == []


def test_principals_contract_fixture_loads(fixtures_dir: Path) -> None:
    raw = yaml.safe_load((fixtures_dir / "principals_contract.yml").read_text())
    schema = DataContractSchema.model_validate(raw)
    tables = {at.schema_: at for at in schema.semantic.allowed_tables}
    assert tables["analytics"].allowed_principals is None
    assert tables["analytics"].blocked_principals is None
    assert tables["hr"].allowed_principals == ["alice@co.com"]
    assert tables["raw"].blocked_principals == ["intern@co.com"]
    assert tables["sealed"].allowed_principals == []
