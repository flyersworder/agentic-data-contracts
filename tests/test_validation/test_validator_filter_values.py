import contextvars
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.validator import Validator


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "filter_values_contract.yml")


class TestRequiredFilterValuesIntegration:
    def test_partner_in_set_passes(self, contract: DataContract) -> None:
        v = Validator(contract, caller_principal="partner@co.com")
        result = v.validate("SELECT id FROM sales.opps WHERE account_id IN (123, 456)")
        assert not result.blocked

    def test_partner_out_of_set_blocks(self, contract: DataContract) -> None:
        v = Validator(contract, caller_principal="partner@co.com")
        result = v.validate("SELECT id FROM sales.opps WHERE account_id IN (123, 999)")
        assert result.blocked
        assert any("999" in r and "partner@co.com" in r for r in result.reasons)

    def test_vip_in_own_set_passes(self, contract: DataContract) -> None:
        v = Validator(contract, caller_principal="vip@co.com")
        result = v.validate("SELECT id FROM sales.opps WHERE account_id = 999")
        assert not result.blocked

    def test_vip_with_partner_value_blocks(self, contract: DataContract) -> None:
        """Map keys are the only-applicable scope. VIP's allow set is {999}."""
        v = Validator(contract, caller_principal="vip@co.com")
        result = v.validate("SELECT id FROM sales.opps WHERE account_id = 123")
        assert result.blocked
        assert any("123" in r and "vip@co.com" in r for r in result.reasons)

    def test_unmapped_principal_falls_through(self, contract: DataContract) -> None:
        """Principals not in the map are not subject to this rule (rule is a no-op
        for them). Use allowed_principals on the rule for hard deny."""
        v = Validator(contract, caller_principal="other@co.com")
        result = v.validate("SELECT id FROM sales.opps WHERE account_id = 7")
        assert not result.blocked

    def test_no_caller_falls_through(self, contract: DataContract) -> None:
        v = Validator(contract)
        result = v.validate("SELECT id FROM sales.opps WHERE account_id = 7")
        assert not result.blocked

    def test_partner_missing_filter_blocks(self, contract: DataContract) -> None:
        v = Validator(contract, caller_principal="partner@co.com")
        result = v.validate("SELECT id FROM sales.opps WHERE id = 1")
        assert result.blocked
        assert any("Missing required filter" in r for r in result.reasons)

    def test_self_join_alias_bypass_blocked(self, contract: DataContract) -> None:
        """Regression for the cross-alias smuggle: t1 pinned to a legal value
        and t2 pinned to a forbidden value must not pass."""
        v = Validator(contract, caller_principal="partner@co.com")
        result = v.validate(
            "SELECT t1.id FROM sales.opps t1 "
            "JOIN sales.opps t2 ON t1.id = t2.id "
            "WHERE t1.account_id = 123 AND t2.account_id = 999"
        )
        assert result.blocked
        assert any("999" in r and "partner@co.com" in r for r in result.reasons)

    def test_contradiction_blocked(self, contract: DataContract) -> None:
        v = Validator(contract, caller_principal="partner@co.com")
        result = v.validate(
            "SELECT id FROM sales.opps WHERE account_id = 123 AND account_id = 999"
        )
        assert result.blocked
        assert any("999" in r for r in result.reasons)

    def test_per_validate_callable_principal(self, contract: DataContract) -> None:
        """One Validator, switching principals via ContextVar — Webex pattern."""
        current: contextvars.ContextVar[str | None] = contextvars.ContextVar(
            "current", default=None
        )
        v = Validator(contract, caller_principal=lambda: current.get())

        current.set("partner@co.com")
        assert not v.validate(
            "SELECT id FROM sales.opps WHERE account_id = 123"
        ).blocked

        current.set("vip@co.com")
        result = v.validate("SELECT id FROM sales.opps WHERE account_id = 123")
        assert result.blocked
        assert any("vip@co.com" in r for r in result.reasons)

        current.set("partner@co.com")
        assert not v.validate(
            "SELECT id FROM sales.opps WHERE account_id = 456"
        ).blocked
