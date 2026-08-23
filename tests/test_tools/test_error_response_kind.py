"""``_kind`` on `_error_response` envelopes.

Classifies *why* a tool returned an error envelope, distinct from
``is_error`` (whether it should be one at all). ``kind="blocked"`` marks a
governance denial — the contract refused the action; the default
``kind="error"`` covers misconfiguration, invalid arguments, and execution
failures. A later task (the conformance recorder) reads ``_kind`` to
classify a tool call as blocked vs. error rather than treating every
non-raising return as a successful call.
"""

from agentic_data_contracts.tools.factory import _error_response


def test_defaults_to_error_kind() -> None:
    assert _error_response("boom")["_kind"] == "error"


def test_blocked_kind_is_carried() -> None:
    assert _error_response("BLOCKED — nope", kind="blocked")["_kind"] == "blocked"


def test_is_error_is_still_set_for_mcp() -> None:
    assert _error_response("boom")["is_error"] is True
