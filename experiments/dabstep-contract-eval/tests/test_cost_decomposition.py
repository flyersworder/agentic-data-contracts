import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import cost_decomposition as cd  # noqa: E402


def _row(arm, usd, turns, inp, out, cached=0, verdict="correct"):
    return {
        "arm": arm,
        "usd": usd,
        "turns": turns,
        "input_tokens": inp,
        "cached_tokens": cached,
        "output_tokens": out,
        "verdict": verdict,
    }


def test_operating_numbers_are_means_per_run_and_dollars_per_correct_answer():
    rows = [
        _row("a", 0.10, 4, 1000, 100, cached=600),
        _row("a", 0.30, 8, 3000, 300, verdict="incorrect"),
        _row("b", 0.05, 2, 500, 50, cached=None),
    ]
    out = cd.operating(rows)
    assert out["a"]["runs"] == 2
    assert out["a"]["usd"] == pytest.approx(0.20)
    assert out["a"]["turns"] == 6
    assert out["a"]["input"] == 2000
    assert out["a"]["cached"] == 300
    assert out["a"]["output"] == 200
    assert out["a"]["usd_per_correct"] == pytest.approx(0.40)
    assert out["b"]["cached"] == 0


def test_an_arm_with_no_correct_run_has_no_cost_per_correct_answer():
    out = cd.operating([_row("a", 0.1, 1, 1, 1, verdict="incorrect")])
    assert out["a"]["usd_per_correct"] is None


def test_a_trace_lasts_from_its_first_message_to_its_last():
    messages = [
        {"kind": "request", "timestamp": "2026-09-29T13:44:59.000000Z"},
        {"kind": "response", "timestamp": "2026-09-29T13:45:08.500000Z"},
        {"kind": "request", "timestamp": "2026-09-29T13:45:08.600000Z"},
        {"kind": "response", "timestamp": "2026-09-29T13:45:30Z"},
    ]
    assert cd.trace_seconds(messages) == pytest.approx(31.0)


def test_a_trace_without_timestamps_has_no_duration():
    assert cd.trace_seconds([{"kind": "request"}]) is None
    assert cd.trace_seconds([]) is None
