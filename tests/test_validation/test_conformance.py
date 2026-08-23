from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.recorder import ToolCall, ToolRecorder
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.validation.conformance import (
    Attempt,
    _answer_verdict,
    _select_answer,
)
from agentic_data_contracts.validation.examples import VerifiedExample


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


def _example(**kw):
    return VerifiedExample(sql=kw.pop("sql", "SELECT 1"), **kw)


def test_from_session_captures_the_call_log_and_cost(contract):
    rec = ToolRecorder()
    session = ContractSession(contract, recorder=rec)
    rec.log("run_query", {"sql": "SELECT 1"}, "ok", scalar=5.0)
    session.record_cost(0.02)

    attempt = Attempt.from_session(_example(), session, final_text="five")

    assert [c.tool for c in attempt.calls] == ["run_query"]
    assert attempt.cost_usd == 0.02
    assert attempt.elapsed_seconds >= 0.0
    assert attempt.final_text == "five"


def test_from_session_coerces_foreign_tool_calls_to_a_list(contract):
    session = ContractSession(contract, recorder=ToolRecorder())
    attempt = Attempt.from_session(
        _example(), session, foreign_tool_calls=("mcp__bigquery__execute_sql",)
    )

    assert attempt.foreign_tool_calls == ["mcp__bigquery__execute_sql"]
    attempt.foreign_tool_calls.append("another")  # must not raise


def test_from_session_refuses_a_reused_recorder(contract):
    session = ContractSession(contract, recorder=ToolRecorder())
    Attempt.from_session(_example(), session)

    with pytest.raises(ValueError, match="already consumed"):
        Attempt.from_session(_example(), session)


def test_from_session_requires_a_recorder(contract):
    with pytest.raises(ValueError, match="recorder"):
        Attempt.from_session(_example(), ContractSession(contract))


def _q(scalar, outcome="ok", seq=0, **kw):
    return ToolCall(
        sequence=seq, tool="run_query", outcome=outcome, scalar=scalar, **kw
    )


def test_a_single_scalar_is_not_a_guess():
    source, actual, count, anchor = _select_answer(
        Attempt(example=_example(), calls=[_q(42.0)])
    )
    assert (source, actual, count) == ("sole_scalar", 42.0, 1)
    assert anchor is not None


def test_a_declared_answer_wins_and_anchors_on_the_last_query():
    attempt = Attempt(
        example=_example(), calls=[_q(1.0, seq=0), _q(2.0, seq=1)], final_answer=9.0
    )
    source, actual, _, anchor = _select_answer(attempt)
    assert (source, actual) == ("declared", 9.0)
    assert anchor.sequence == 1  # ty: ignore[unresolved-attribute]


def test_reruns_of_the_same_value_stay_unambiguous():
    """A retry after a transient failure must not demote the row."""
    attempt = Attempt(
        example=_example(), calls=[_q(100.0, seq=0), _q(100.00000000000001, seq=1)]
    )
    source, actual, count, _ = _select_answer(attempt)
    assert (source, count) == ("sole_scalar", 1)
    assert actual == 100.0


def test_genuinely_different_values_are_ambiguous():
    attempt = Attempt(example=_example(), calls=[_q(10.0, seq=0), _q(4182.0, seq=1)])
    source, actual, count, _ = _select_answer(attempt)
    assert (source, actual, count) == ("last_scalar", 4182.0, 2)


def test_non_scalar_results_are_never_candidates():
    """The 'last query returned a table' outlier must not score as an error."""
    attempt = Attempt(example=_example(), calls=[_q(42.0, seq=0), _q(None, seq=1)])
    source, actual, count, _ = _select_answer(attempt)
    assert (source, actual, count) == ("sole_scalar", 42.0, 1)


def test_blocked_queries_are_never_candidates():
    attempt = Attempt(example=_example(), calls=[_q(None, outcome="blocked", seq=0)])
    source, actual, count, anchor = _select_answer(attempt)
    assert (source, actual, count, anchor) == ("none", None, 0, None)


def _verdict(example, calls=(), **kw):
    attempt = Attempt(example=example, calls=list(calls), **kw)
    source, actual, _, anchor = _select_answer(attempt)
    return _answer_verdict(attempt, source, actual, anchor)[0]


def test_a_crashed_attempt_is_an_error():
    assert _verdict(_example(expected=1.0), error="agent timed out") == "error"


def test_a_row_without_expected_is_skipped_not_failed():
    assert _verdict(_example(), calls=[_q(42.0)]) == "skipped"


def test_no_scalar_produced_is_an_error():
    assert _verdict(_example(expected=42.0)) == "error"


def test_matching_within_tolerance():
    assert _verdict(_example(expected=42.0), calls=[_q(42.0)]) == "match"


def test_a_mismatch_records_the_diffs_it_missed_by():
    attempt = Attempt(example=_example(expected=40.0), calls=[_q(42.0)])
    source, actual, _, anchor = _select_answer(attempt)
    status, _, abs_diff, rel_diff = _answer_verdict(attempt, source, actual, anchor)
    assert status == "mismatch"
    assert abs_diff == pytest.approx(2.0)
    assert rel_diff == pytest.approx(0.05)


def test_mismatch_is_anchored_on_the_certified_answer():
    """_compare(actual, expected, ...) -- reversing the operands turns this
    mismatch into a match, because the tolerance would anchor on 100."""
    assert (
        _verdict(_example(expected=1.0, rel_tol=1.0), calls=[_q(100.0)]) == "mismatch"
    )


def test_a_relative_window_makes_the_certified_number_unassertable():
    calls = [_q(42.0, relative_time="CURRENT_DATE")]
    assert _verdict(_example(expected=42.0), calls=calls) == "unassertable"


def test_time_scoped_rows_suppress_the_relative_window_refusal():
    calls = [_q(42.0, relative_time="CURRENT_DATE")]
    assert _verdict(_example(expected=42.0, time_scoped=True), calls=calls) == "match"
