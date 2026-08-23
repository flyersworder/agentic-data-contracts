import pytest

from agentic_data_contracts.core.recorder import ToolCall, ToolRecorder


def test_log_appends_a_call_with_an_incrementing_sequence():
    rec = ToolRecorder()
    rec.log("lookup_metric", {"metric_name": "CAC"}, "ok")
    rec.log("run_query", {"sql": "SELECT 1"}, "ok", scalar=1.0, row_count=1)

    assert [c.sequence for c in rec.calls] == [0, 1]
    assert rec.calls[0] == ToolCall(
        sequence=0, tool="lookup_metric", args={"metric_name": "CAC"}, outcome="ok"
    )
    assert rec.calls[1].scalar == 1.0
    assert rec.calls[1].row_count == 1


def test_unknown_outcome_is_rejected_at_log_time():
    rec = ToolRecorder()
    with pytest.raises(ValueError, match="outcome must be one of"):
        rec.log("run_query", {}, "sort-of-ok")


def test_elapsed_seconds_is_measured_from_construction():
    rec = ToolRecorder()
    assert rec.elapsed_seconds >= 0.0


def test_consume_returns_the_calls_and_refuses_a_second_read():
    rec = ToolRecorder()
    rec.log("run_query", {}, "ok")

    assert len(rec.consume()) == 1
    with pytest.raises(ValueError, match="already consumed"):
        rec.consume()
