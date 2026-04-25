"""Tests for principal_in_scope — the shared allow/block-list predicate."""

from __future__ import annotations

import pytest

from agentic_data_contracts.core.principal import principal_in_scope


class TestNoRestriction:
    @pytest.mark.parametrize("principal", [None, "", "alice@co.com"])
    def test_no_lists_means_open(self, principal: str | None) -> None:
        assert principal_in_scope(principal, None, None) is True


class TestAllowedList:
    def test_principal_in_list(self) -> None:
        assert principal_in_scope("alice@co.com", ["alice@co.com"], None) is True

    def test_principal_not_in_list(self) -> None:
        assert principal_in_scope("bob@co.com", ["alice@co.com"], None) is False

    def test_none_principal_denied(self) -> None:
        assert principal_in_scope(None, ["alice@co.com"], None) is False

    def test_empty_string_principal_denied(self) -> None:
        # Empty string means unauthenticated — must fail closed even if "" is
        # technically in the allow list. This is the two-layer empty-string
        # invariant: the resolver passes "" through, the policy collapses it.
        assert principal_in_scope("", ["alice@co.com"], None) is False

    def test_empty_string_in_allowlist_does_not_grant_empty_principal(self) -> None:
        # allowed_principals=[""] is degenerate but must not let an
        # unauthenticated caller through.
        assert principal_in_scope("", [""], None) is False

    def test_empty_allowlist_seals_everyone(self) -> None:
        for principal in (None, "", "alice@co.com"):
            assert principal_in_scope(principal, [], None) is False


class TestBlockedList:
    def test_principal_in_list_denied(self) -> None:
        assert principal_in_scope("bob@co.com", None, ["bob@co.com"]) is False

    def test_principal_not_in_list_allowed(self) -> None:
        assert principal_in_scope("alice@co.com", None, ["bob@co.com"]) is True

    def test_none_principal_with_blocklist_denied(self) -> None:
        # Restricted-with-blocklist still requires identification.
        assert principal_in_scope(None, None, ["bob@co.com"]) is False

    def test_empty_string_principal_with_blocklist_denied(self) -> None:
        assert principal_in_scope("", None, ["bob@co.com"]) is False
