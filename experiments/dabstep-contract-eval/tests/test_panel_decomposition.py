import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import panel_decomposition as pd  # noqa: E402


def _row(task, arm, verdict, level="hard"):
    return {"task_id": task, "arm": arm, "verdict": verdict, "level": level}


def test_a_task_scores_its_correct_runs_over_the_repeats():
    repeats = [
        [_row("1", "contract", "correct"), _row("2", "contract", "incorrect")],
        [_row("1", "contract", "correct"), _row("2", "contract", "correct")],
    ]
    assert pd.scores(repeats) == {"contract": {"1": 2, "2": 1}}


def test_the_hard_subset_keeps_only_hard_tasks():
    repeats = [
        [_row("1", "contract", "correct"), _row("2", "contract", "correct", "easy")]
    ]
    assert pd.scores(repeats, level="hard") == {"contract": {"1": 1}}


def test_a_sign_test_counts_tasks_each_arm_wins_and_drops_ties():
    score = {"a": {"1": 3, "2": 1, "3": 2}, "b": {"1": 0, "2": 2, "3": 2}}
    up, down, p = pd.sign(score, "a", "b")
    assert (up, down) == (1, 1)
    assert p == 1.0


def test_a_task_missing_from_one_arm_counts_as_zero_correct():
    score = {"a": {"1": 3, "2": 3}, "b": {"1": 3}}
    assert pd.sign(score, "a", "b")[:2] == (1, 0)


def test_mcnemar_pairs_one_repeat_by_task():
    rows = [
        _row("1", "a", "correct"),
        _row("1", "b", "incorrect"),
        _row("2", "a", "incorrect"),
        _row("2", "b", "incorrect"),
        _row("3", "a", "correct"),
        _row("3", "b", "correct"),
    ]
    a_only, b_only, p = pd.mcnemar(rows, "a", "b")
    assert (a_only, b_only) == (1, 0)
    assert p == 1.0


def test_accuracy_is_correct_runs_over_task_runs():
    score = {"a": {"1": 3, "2": 0}}
    assert pd.accuracy(score, "a", repeats=3) == 0.5
