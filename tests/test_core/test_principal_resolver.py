import pytest

from agentic_data_contracts.core.principal import resolve_principal


def test_none_returns_none() -> None:
    assert resolve_principal(None) is None


def test_static_string_returned() -> None:
    assert resolve_principal("alice@co.com") == "alice@co.com"


def test_empty_string_passes_through() -> None:
    # No silent coercion — empty string is a distinct (non-matching) principal.
    assert resolve_principal("") == ""


def test_callable_returning_string() -> None:
    assert resolve_principal(lambda: "bob@co.com") == "bob@co.com"


def test_callable_returning_none() -> None:
    assert resolve_principal(lambda: None) is None


def test_callable_that_raises_propagates() -> None:
    def broken() -> str:
        raise RuntimeError("identity lookup failed")

    with pytest.raises(RuntimeError, match="identity lookup failed"):
        resolve_principal(broken)
