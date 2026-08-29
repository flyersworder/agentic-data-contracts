from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.recorder import ToolCall, ToolRecorder
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.validation.conformance import (
    Attempt,
    _answer_verdict,
    _protocol_verdict,
    _select_answer,
    evaluate_conformance,
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
    _, actual, _, anchor = _select_answer(attempt)
    return _answer_verdict(attempt, actual, anchor)[0]


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
    _, actual, _, anchor = _select_answer(attempt)
    status, _, abs_diff, rel_diff = _answer_verdict(attempt, actual, anchor)
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


def _lookup(name, seq, outcome="ok"):
    return ToolCall(
        sequence=seq, tool="lookup_metric", args={"metric_name": name}, outcome=outcome
    )


def _protocol(example, calls=(), **kw):
    attempt = Attempt(example=example, calls=list(calls), **kw)
    _, _, _, anchor = _select_answer(attempt)
    return _protocol_verdict(attempt, anchor)[0]


def test_a_row_activating_no_rule_is_not_applicable():
    assert _protocol(_example(), calls=[_q(42.0)]) == "not_applicable"


def test_a_crashed_attempt_could_not_be_judged():
    assert _protocol(_example(), error="boom") == "unchecked"


def test_declaring_an_answer_with_no_query_is_contaminated():
    assert _protocol(_example(), final_answer=42.0) == "contaminated"


def test_a_foreign_tool_call_is_contaminated():
    assert (
        _protocol(
            _example(),
            calls=[_q(42.0)],
            foreign_tool_calls=["mcp__bigquery__execute_sql"],
        )
        == "contaminated"
    )


def test_consulting_the_declared_metric_first_is_followed():
    calls = [_lookup("CAC", 0), _q(42.0, seq=1)]
    assert _protocol(_example(expects_metrics=["CAC"]), calls=calls) == "followed"


def test_querying_without_the_declared_lookup_is_violated():
    assert _protocol(_example(expects_metrics=["CAC"]), calls=[_q(42.0)]) == "violated"


def test_a_lookup_after_the_answer_does_not_count():
    calls = [_q(42.0, seq=0), _lookup("CAC", 1)]
    assert _protocol(_example(expects_metrics=["CAC"]), calls=calls) == "violated"


def test_a_fuzzy_miss_does_not_satisfy_the_rule():
    calls = [_lookup("CAC", 0, outcome="miss"), _q(42.0, seq=1)]
    assert _protocol(_example(expects_metrics=["CAC"]), calls=calls) == "violated"


def test_expects_metrics_with_no_anchor_cannot_be_judged():
    """A row with a consultation requirement but no successful run_query has
    nothing to order the lookup against -- unchecked, not not_applicable.
    Conflating these would silently turn "could not judge" into "nothing to
    judge", greenlighting exactly the rows a corpus author asked to check."""
    calls = [_lookup("total_revenue", 0)]
    assert (
        _protocol(_example(expects_metrics=["total_revenue"]), calls=calls)
        == "unchecked"
    )


def test_friction_is_recorded_without_failing():
    calls = [
        _lookup("CAC", 0, outcome="miss"),
        _lookup("CAC", 1),
        _q(None, outcome="blocked", seq=2),
        _q(42.0, seq=3),
    ]
    attempt = Attempt(example=_example(expects_metrics=["CAC"]), calls=calls)
    _, _, _, anchor = _select_answer(attempt)
    status, reasons = _protocol_verdict(attempt, anchor)
    assert status == "followed"
    assert any("blocked" in r for r in reasons)
    assert any("miss" in r for r in reasons)


def test_friction_ignores_blocks_from_other_tools():
    """A describe_table block restricted for the principal is not a query
    attempt -- only run_query blocks belong in the friction count."""
    calls = [
        ToolCall(sequence=0, tool="describe_table", outcome="blocked"),
        _q(42.0, seq=1),
    ]
    attempt = Attempt(example=_example(), calls=calls)
    _, _, _, anchor = _select_answer(attempt)
    _, reasons = _protocol_verdict(attempt, anchor)
    assert not any("blocked" in r for r in reasons)


def test_friction_wording_is_honest_when_nothing_was_accepted():
    """A row where every run_query attempt was blocked must not claim an
    accepted query came after -- none did."""
    calls = [_q(None, outcome="blocked", seq=0)]
    attempt = Attempt(example=_example(), calls=calls)
    _, _, _, anchor = _select_answer(attempt)
    _, reasons = _protocol_verdict(attempt, anchor)
    assert any("none accepted" in r for r in reasons)
    assert not any("before an accepted one" in r for r in reasons)


def _attempt(example, calls=(), **kw):
    return Attempt(example=example, calls=list(calls), **kw)


def test_a_clean_run_passes_the_gate():
    report = evaluate_conformance([_attempt(_example(expected=42.0), [_q(42.0)])])
    assert report.ok is True
    assert report.pass_rate() == 1.0


def test_an_empty_report_is_not_ok():
    """An emptied or fully-filtered corpus must surface, not pass a no-op gate."""
    assert evaluate_conformance([]).ok is False


def test_a_protocol_only_row_passes_without_an_expected():
    assert evaluate_conformance([_attempt(_example(), [_q(42.0)])]).ok is True


def test_an_ambiguous_answer_selection_fails_the_gate():
    attempts = [_attempt(_example(expected=42.0), [_q(42.0, seq=0), _q(4182.0, seq=1)])]
    report = evaluate_conformance(attempts)
    assert report.ok is False
    assert len(report.ambiguous) == 1


def test_ambiguity_on_a_protocol_only_row_does_not_fail_the_gate():
    """Nobody asserted an answer, so its ambiguity is irrelevant."""
    attempts = [_attempt(_example(), [_q(42.0, seq=0), _q(4182.0, seq=1)])]
    assert evaluate_conformance(attempts).ok is True


def test_declaring_the_answer_resolves_ambiguity():
    attempts = [
        _attempt(
            _example(expected=42.0),
            [_q(42.0, seq=0), _q(4182.0, seq=1)],
            final_answer=42.0,
        )
    ]
    assert evaluate_conformance(attempts).ok is True


def test_repeats_group_by_example_id():
    ex = _example(id="q1", expected=42.0)
    report = evaluate_conformance([_attempt(ex, [_q(42.0)]) for _ in range(3)])
    assert list(report.by_example()) == ["q1"]
    assert len(report.by_example()["q1"]) == 3


def test_one_flake_in_three_repeats_fails_the_strict_gate():
    ex = _example(id="q1", expected=42.0)
    attempts = [
        _attempt(ex, [_q(42.0)]),
        _attempt(ex, [_q(42.0)]),
        _attempt(ex, [_q(0.0)]),
    ]
    report = evaluate_conformance(attempts)
    assert report.ok is False
    assert report.pass_rate() == pytest.approx(2 / 3)


def test_summary_is_markdown_and_never_prints_raw_sql():
    sql = "SELECT secret_column FROM analytics.orders"
    attempts = [_attempt(_example(expected=42.0), [_q(42.0, args={"sql": sql})])]
    text = evaluate_conformance(attempts).summary()
    assert "|" in text
    assert sql not in text


def test_a_successful_non_scalar_query_still_anchors_the_ordering():
    """A row can ask for metric consultation without asserting a number --
    "revenue by region" returns many rows and no scalar. The ordering rule
    needs a *query*, not a scalar, so a successful run_query anchors it. Left
    unanchored the row reports `unchecked` with the reason "no answering query"
    while an answering query is sitting in the log, and a compliant agent fails
    the gate over a claim that is not true."""
    calls = [_lookup("total_revenue", 0), _q(None, seq=1, row_count=4)]
    attempt = Attempt(
        example=_example(expected=None, expects_metrics=["total_revenue"]), calls=calls
    )
    _, _, _, anchor = _select_answer(attempt)
    assert anchor is not None
    assert _protocol_verdict(attempt, anchor)[0] == "followed"


def test_a_relative_window_that_returned_nothing_is_still_unassertable():
    """Pass 2 diagnoses a decaying window before it ever runs the SQL. Pass 3
    must reach the same diagnosis when the agent's window happens to return
    NULL or no rows -- `SUM(amount)` over an empty trailing 30 days is exactly
    that -- rather than blaming the missing scalar, which is a symptom of the
    window, not an independent fault."""
    calls = [_q(None, seq=0, relative_time="CURRENT_DATE - 30")]
    attempt = Attempt(example=_example(expected=4200.0), calls=calls)
    _, actual, _, anchor = _select_answer(attempt)
    status, reasons, _, _ = _answer_verdict(attempt, actual, anchor)
    assert status == "unassertable"
    assert any("CURRENT_DATE - 30" in r for r in reasons)


def test_friction_wording_does_not_backdate_a_later_block():
    """A block that happened *after* the accepted query must not be described
    as preceding it. These reasons are the input to rewriting contract prose,
    so a false ordering claim sends the fix to the wrong place."""
    calls = [_q(42.0, seq=0), _q(None, outcome="blocked", seq=1)]
    attempt = Attempt(example=_example(), calls=calls)
    _, _, _, anchor = _select_answer(attempt)
    _, reasons = _protocol_verdict(attempt, anchor)
    assert any("after an accepted one" in r for r in reasons)
    assert not any("before an accepted one" in r for r in reasons)


def test_friction_wording_splits_blocks_on_both_sides():
    calls = [
        _q(None, outcome="blocked", seq=0),
        _q(42.0, seq=1),
        _q(None, outcome="blocked", seq=2),
    ]
    attempt = Attempt(example=_example(), calls=calls)
    _, _, _, anchor = _select_answer(attempt)
    _, reasons = _protocol_verdict(attempt, anchor)
    assert any("1 before an accepted one, 1 after" in r for r in reasons)


def test_several_successful_queries_and_no_scalar_cannot_be_anchored():
    """With no scalar and no declared answer, *which* query answered is a
    guess. Anchoring the last one silently lets a lookup that landed after the
    real answering query -- but before a trailing drill-down -- read as
    compliant, which is the one direction that must never happen: a violation
    turning into a pass. Several candidates is `unchecked`, the same verdict
    the sibling `sole_scalar` / `last_scalar` split already draws for scalars."""
    calls = [
        _q(None, seq=0, row_count=12),
        _lookup("CAC", 1),
        _q(None, seq=2, row_count=3),
    ]
    attempt = Attempt(example=_example(expects_metrics=["CAC"]), calls=calls)
    _, _, _, anchor = _select_answer(attempt)
    assert anchor is None
    assert _protocol_verdict(attempt, anchor)[0] == "unchecked"


def test_a_lone_successful_query_still_anchors_after_the_answer():
    """The conservative rule must not cost the ordering rule its teeth: one
    successful query is unambiguous, so a lookup after it is still violated."""
    calls = [_q(None, seq=0, row_count=12), _lookup("CAC", 1)]
    attempt = Attempt(example=_example(expects_metrics=["CAC"]), calls=calls)
    _, _, _, anchor = _select_answer(attempt)
    assert _protocol_verdict(attempt, anchor)[0] == "violated"


def test_a_relative_drill_down_is_not_called_the_answering_query():
    """The window that decays has to be the one that answered. A trailing
    drill-down using a relative window, after an absolute answering query,
    must not be reported as the reason the certified value decayed."""
    calls = [
        _q(None, seq=0, row_count=5),
        _q(None, seq=1, row_count=2, relative_time="CURRENT_DATE - 7"),
    ]
    attempt = Attempt(example=_example(expected=4200.0), calls=calls)
    _, actual, _, anchor = _select_answer(attempt)
    status, reasons, _, _ = _answer_verdict(attempt, actual, anchor)
    assert status == "error"
    assert not any("relative time window" in r for r in reasons)


def test_friction_measures_against_the_answering_query_not_the_last_one():
    """P2 orders against the anchor; P3 must use the same reference or the two
    rules disagree about which query was the answer -- and the backdating this
    wording exists to prevent comes straight back on any attempt whose
    answering query is followed by a later successful one."""
    calls = [
        _q(42.0, seq=0),
        _q(None, outcome="blocked", seq=1),
        _q(None, seq=2, row_count=7),
    ]
    attempt = Attempt(example=_example(expected=42.0), calls=calls)
    _, _, _, anchor = _select_answer(attempt)
    _, reasons = _protocol_verdict(attempt, anchor)
    assert any("after an accepted one" in r for r in reasons)
    assert not any("before an accepted one" in r for r in reasons)


# ── Declared breakdown answers (#85) ──────────────────────────────────────────

_BREAKDOWN = [["EMEA", 5000.0], ["APAC", 3000.0]]
_BD_COLUMNS = ["region", "revenue"]


def _breakdown_example(**kw):
    return _example(
        id="by-region",
        question="revenue by region",
        expected_rows=[list(r) for r in _BREAKDOWN],
        **kw,
    )


def _breakdown_attempt(rows, *, columns=None, calls=None, **kw):
    return Attempt(
        example=_breakdown_example(**kw),
        calls=calls if calls is not None else [ToolCall(0, "run_query", outcome="ok")],
        final_rows=rows,
        final_columns=_BD_COLUMNS if columns is None else columns,
    )


class TestDeclaredBreakdown:
    def test_a_matching_breakdown_reports_match(self) -> None:
        result = evaluate_conformance([_breakdown_attempt(_BREAKDOWN)]).results[0]
        assert result.answer == "match"
        assert result.answer_source == "declared"
        assert result.row_differences == []
        assert result.actual_row_count == 2

    def test_a_wrong_number_reports_mismatch_and_names_its_group(self) -> None:
        result = evaluate_conformance(
            [_breakdown_attempt([["EMEA", 9999.0], ["APAC", 3000.0]])]
        ).results[0]
        assert result.answer == "mismatch"
        assert any("EMEA" in d for d in result.row_differences)

    def test_a_missing_group_reports_mismatch(self) -> None:
        result = evaluate_conformance([_breakdown_attempt([["EMEA", 5000.0]])]).results[
            0
        ]
        assert result.answer == "mismatch"
        assert any("missing group APAC" in d for d in result.row_differences)

    def test_an_undeclared_breakdown_still_skips(self) -> None:
        # The non-breaking half: a host that has not wired `final_rows` sees
        # exactly what it saw before, so this ships as a minor.
        attempt = Attempt(
            example=_breakdown_example(),
            calls=[ToolCall(0, "run_query", outcome="ok")],
        )
        report = evaluate_conformance([attempt])
        assert report.results[0].answer == "skipped"
        assert report.ok

    def test_a_column_count_mismatch_is_an_error_not_a_mismatch(self) -> None:
        # `compare_rows` raises for a fault no pairing can resolve; pass 3
        # converts it the way pass 2's batch guard does.
        result = evaluate_conformance(
            [_breakdown_attempt([["EMEA", 5000.0, 1.0]], columns=["a", "b", "c"])]
        ).results[0]
        assert result.answer == "error"
        assert any("column" in r for r in result.reasons)

    def test_a_relative_window_is_unassertable_before_it_is_compared(self) -> None:
        result = evaluate_conformance(
            [
                _breakdown_attempt(
                    _BREAKDOWN,
                    calls=[
                        ToolCall(
                            0, "run_query", outcome="ok", relative_time="CURRENT_DATE"
                        )
                    ],
                )
            ]
        ).results[0]
        assert result.answer == "unassertable"

    def test_rows_without_columns_is_refused(self) -> None:
        with pytest.raises(ValueError, match="final_columns"):
            Attempt(example=_breakdown_example(), final_rows=_BREAKDOWN)

    def test_from_session_carries_a_declared_breakdown(self, contract) -> None:
        rec = ToolRecorder()
        session = ContractSession(contract, recorder=rec)
        rec.log("run_query", {"sql": "SELECT 1"}, "ok")
        attempt = Attempt.from_session(
            _breakdown_example(),
            session,
            final_rows=_BREAKDOWN,
            final_columns=_BD_COLUMNS,
        )
        assert evaluate_conformance([attempt]).results[0].answer == "match"
