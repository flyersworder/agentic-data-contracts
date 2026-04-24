from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "principals_contract.yml")


@pytest.mark.parametrize(
    "principal,expected",
    [
        (None, {"analytics.orders"}),
        ("alice@co.com", {"analytics.orders", "hr.salaries", "raw.audit_log"}),
        ("bob@co.com", {"analytics.orders", "raw.audit_log"}),
        ("intern@co.com", {"analytics.orders"}),
        ("", {"analytics.orders"}),
    ],
    ids=["none", "alice-allowed", "bob-neither", "intern-blocked", "empty-string"],
)
def test_allowed_table_names_for(
    contract: DataContract, principal: str | None, expected: set[str]
) -> None:
    assert contract.allowed_table_names_for(principal) == expected


def test_sealed_table_never_accessible(contract: DataContract) -> None:
    # allowed_principals: [] means nobody, no matter who asks.
    for principal in [None, "alice@co.com", "bob@co.com", ""]:
        assert "sealed.top_secret" not in contract.allowed_table_names_for(principal)


def test_unscoped_allowed_table_names_unchanged(contract: DataContract) -> None:
    # The old method returns the full declared union, ignoring principals.
    assert set(contract.allowed_table_names()) == {
        "analytics.orders",
        "hr.salaries",
        "raw.audit_log",
        "sealed.top_secret",
    }
