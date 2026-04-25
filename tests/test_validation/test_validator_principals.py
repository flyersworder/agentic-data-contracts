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


class TestRuleAllowedPrincipals:
    """Per-rule allowed_principals — the rule fires ONLY for listed callers."""

    def test_in_scope_caller_is_subject_to_rule(self, contract: DataContract) -> None:
        # alice is on the rule's allowlist, so she is blocked from pii_email.
        v = Validator(contract, caller_principal="alice@co.com")
        result = v.validate("SELECT pii_email FROM analytics.orders")
        assert result.blocked
        assert any("pii_email" in r for r in result.reasons)

    def test_out_of_scope_caller_skips_rule(self, contract: DataContract) -> None:
        # bob is not on the rule's allowlist, so the column block does not
        # apply — analytics.orders is otherwise open.
        v = Validator(contract, caller_principal="bob@co.com")
        assert not v.validate("SELECT pii_email FROM analytics.orders").blocked

    def test_unauthenticated_caller_skips_rule(self, contract: DataContract) -> None:
        # Restricted rules require identification, same as restricted tables.
        # No caller → rule is skipped; the open table query passes.
        v = Validator(contract)
        assert not v.validate("SELECT pii_email FROM analytics.orders").blocked

    def test_in_scope_caller_can_query_other_columns(
        self, contract: DataContract
    ) -> None:
        v = Validator(contract, caller_principal="alice@co.com")
        assert not v.validate("SELECT id FROM analytics.orders").blocked


class TestRuleBlockedPrincipals:
    """Per-rule blocked_principals — rule fires for everyone EXCEPT listed callers."""

    def test_non_blocked_caller_is_subject_to_rule(
        self, contract: DataContract
    ) -> None:
        v = Validator(contract, caller_principal="bob@co.com")
        result = v.validate("SELECT audit_payload FROM analytics.orders")
        assert result.blocked
        assert any("audit_payload" in r for r in result.reasons)

    def test_blocked_caller_skips_rule(self, contract: DataContract) -> None:
        # The intern is on the rule's blocklist → rule does not apply to them.
        v = Validator(contract, caller_principal="intern@co.com")
        assert not v.validate("SELECT audit_payload FROM analytics.orders").blocked

    def test_unauthenticated_caller_skips_rule(self, contract: DataContract) -> None:
        # Same fail-closed contract as table allowlists: restricted rules
        # require identification, so an unauthenticated caller falls outside
        # the rule's scope and the rule is skipped.
        v = Validator(contract)
        assert not v.validate("SELECT audit_payload FROM analytics.orders").blocked


class TestRuleScopeWithCallablePrincipal:
    """Per-rule principal scope re-reads the callable on every validate()."""

    def test_late_binding_across_messages(self, contract: DataContract) -> None:
        current: contextvars.ContextVar[str | None] = contextvars.ContextVar(
            "current", default=None
        )
        v = Validator(contract, caller_principal=lambda: current.get())

        current.set("alice@co.com")
        assert v.validate("SELECT pii_email FROM analytics.orders").blocked

        current.set("bob@co.com")
        assert not v.validate("SELECT pii_email FROM analytics.orders").blocked

        current.set("alice@co.com")
        assert v.validate("SELECT pii_email FROM analytics.orders").blocked


class TestPendingResultCheckNamesIsSuperset:
    """pending_result_check_names() reports the full declared list — see docstring."""

    def test_principal_scoped_rules_still_listed(
        self, fixtures_dir: Path, tmp_path: Path
    ) -> None:
        # Use a small inline contract with a principal-scoped result_check
        # rule. We assert the rule name is reported in pending list even when
        # the current caller is out of scope (the actual check_results pass
        # will skip the rule — superset contract).
        contract_yaml = """
version: "1.0"
name: pending_test
semantic:
  allowed_tables:
    - schema: analytics
      tables: [orders]
  rules:
    - name: alice_only_row_check
      description: Result check that fires only for alice.
      enforcement: block
      table: analytics.orders
      allowed_principals: [alice@co.com]
      result_check:
        min_rows: 1
"""
        path = tmp_path / "pending_test.yml"
        path.write_text(contract_yaml)
        c = DataContract.from_yaml(path)

        # Caller is bob → rule is out of scope, but the name is still listed.
        v = Validator(c, caller_principal="bob@co.com")
        assert "alice_only_row_check" in v.pending_result_check_names()

        # The rule does NOT actually fire for bob — verify by running an
        # empty result set that would otherwise trip min_rows=1.
        result = v.validate_results(
            "SELECT id FROM analytics.orders", columns=["id"], rows=[]
        )
        assert not result.blocked

        # And it DOES fire for alice on the same empty result set.
        v_alice = Validator(c, caller_principal="alice@co.com")
        result_alice = v_alice.validate_results(
            "SELECT id FROM analytics.orders", columns=["id"], rows=[]
        )
        assert result_alice.blocked
