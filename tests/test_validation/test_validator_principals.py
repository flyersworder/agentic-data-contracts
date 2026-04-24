import contextvars
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.validator import Validator


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "principals_contract.yml")


class TestStaticPrincipal:
    def test_alice_can_query_hr(self, contract: DataContract) -> None:
        v = Validator(contract, caller_principal="alice@co.com")
        assert not v.validate("SELECT salary FROM hr.salaries").blocked

    def test_bob_cannot_query_hr(self, contract: DataContract) -> None:
        v = Validator(contract, caller_principal="bob@co.com")
        result = v.validate("SELECT salary FROM hr.salaries")
        assert result.blocked
        assert any("caller: 'bob@co.com'" in r for r in result.reasons)

    def test_no_caller_cannot_query_restricted(self, contract: DataContract) -> None:
        v = Validator(contract)  # no caller_principal
        result = v.validate("SELECT salary FROM hr.salaries")
        assert result.blocked
        assert any("<no caller identified>" in r for r in result.reasons)

    def test_open_table_always_accessible(self, contract: DataContract) -> None:
        v = Validator(contract)
        assert not v.validate("SELECT id FROM analytics.orders").blocked


class TestCallablePrincipal:
    def test_callable_invoked_per_validate(self, contract: DataContract) -> None:
        """The resolver MUST be called each validate(), not cached at init.

        This is the core Webex scenario: one long-lived validator, different
        users per message, with identity held in a contextvars.ContextVar.
        """
        current: contextvars.ContextVar[str | None] = contextvars.ContextVar(
            "current", default=None
        )
        v = Validator(contract, caller_principal=lambda: current.get())

        current.set("alice@co.com")
        assert not v.validate("SELECT salary FROM hr.salaries").blocked

        current.set("bob@co.com")
        result = v.validate("SELECT salary FROM hr.salaries")
        assert result.blocked
        assert any("caller: 'bob@co.com'" in r for r in result.reasons)

        current.set("alice@co.com")
        assert not v.validate("SELECT salary FROM hr.salaries").blocked

    def test_callable_returning_none_fails_closed(self, contract: DataContract) -> None:
        v = Validator(contract, caller_principal=lambda: None)
        assert v.validate("SELECT salary FROM hr.salaries").blocked

    def test_callable_that_raises_propagates(self, contract: DataContract) -> None:
        def broken() -> str:
            raise RuntimeError("boom")

        v = Validator(contract, caller_principal=broken)
        with pytest.raises(RuntimeError, match="boom"):
            v.validate("SELECT salary FROM hr.salaries")
