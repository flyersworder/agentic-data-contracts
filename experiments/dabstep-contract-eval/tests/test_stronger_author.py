import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import stronger_author as sa  # noqa: E402


def _row(arm, task, ok, subset="primary"):
    return {
        "arm": arm,
        "task_id": task,
        "verdict": "correct" if ok else "incorrect",
        "subset": subset,
    }


def test_task_scores_count_correct_runs_over_the_repeats():
    repeats = [
        [_row("a", "t1", True), _row("b", "t1", False), _row("a", "t9", True, "x")],
        [_row("a", "t1", True), _row("b", "t1", True)],
    ]
    score = sa.task_scores(repeats, subset="primary")
    assert score["a"]["t1"] == 2 and score["b"]["t1"] == 1
    assert "t9" not in score["a"]  # another subset is left out
    assert sa.task_scores(repeats, subset=None)["a"]["t9"] == 1


def test_sign_drops_ties_and_reports_the_side():
    score = {
        "a": {"t1": 3, "t2": 2, "t3": 1, "t4": 0},
        "b": {"t1": 0, "t2": 2, "t3": 0, "t4": 1},
    }
    better, worse, p = sa.sign(score, "a", "b", {"t1", "t2", "t3", "t4"})
    assert (better, worse) == (2, 1)
    assert p == pytest.approx(0.5)  # one-sided, 2 of 3
    assert sa.sign(score, "a", "b", {"t2"}) == (0, 0, 1.0)


def test_the_experiments_name_their_run_commit_files():
    assert sa.EXPERIMENTS["dabstep"]["files"](2) == "dabstep-opus-luna-r2.jsonl"
    assert (
        sa.EXPERIMENTS["livesqlbench"]["files"](3)
        == "livesqlbench/lsb-exp2-luna-r3.jsonl"
    )
    assert sa.EXPERIMENTS["livesqlbench"]["subset"] == "primary"
