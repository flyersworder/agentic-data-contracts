import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import lsb_root_cause as rc  # noqa: E402

V = {"m1": "faithful", "m2": "faithful", "bad": "wrong", "odd": "unclear"}


def test_a_refused_entry_decides_the_group_before_any_verdict():
    used = [("untranslated", []), ("metric", ["bad"])]
    assert rc.task_group(used, V) == "B"


def test_a_task_with_no_compiled_metric_is_group_d():
    assert rc.task_group([("prose", [])], V) == "D"
    assert rc.task_group([], V) == "D"


def test_one_material_error_makes_the_task_group_c():
    assert rc.task_group([("metric", ["m1", "bad"])], V) == "C"


def test_every_needed_metric_faithful_is_group_a():
    assert rc.task_group([("metric", ["m1"]), ("metric", ["m2"])], V) == "A"


def test_an_unclear_or_unreviewed_metric_is_group_e():
    assert rc.task_group([("metric", ["m1", "odd"])], V) == "E"
    assert rc.task_group([("metric", ["m1", "never_reviewed"])], V) == "E"


def test_an_immaterial_error_counts_as_unclear():
    rows = [
        {"db": "d", "metric": "x", "verdict": "wrong", "material": True},
        {"db": "d", "metric": "y", "verdict": "wrong", "material": False},
        {"db": "d", "metric": "z", "verdict": "faithful", "material": False},
    ]
    assert rc.verdict_map(rows) == {
        ("d", "x"): "wrong",
        ("d", "y"): "unclear",
        ("d", "z"): "faithful",
    }


def test_the_statistic_is_the_compiled_gain_in_a_minus_that_in_b():
    # per task: (contract + manual_compiled) / 2 - manual_prompt, correct runs of 3
    score = {
        "contract": {"a1": 3, "a2": 2, "b1": 0},
        "manual_compiled": {"a1": 3, "a2": 2, "b1": 0},
        "manual_prompt": {"a1": 1, "a2": 2, "b1": 2},
    }
    assert rc.compiled_gain(score, "a1") == 2
    assert rc.interaction(score, ["a1", "a2"], ["b1"]) == 1 - (-2)


def test_the_permutation_test_is_one_sided_and_reproducible():
    up = {"contract": {}, "manual_compiled": {}, "manual_prompt": {}}
    a = [f"a{i}" for i in range(30)]
    b = [f"b{i}" for i in range(30)]
    for t in a:
        up["contract"][t] = up["manual_compiled"][t] = 3
        up["manual_prompt"][t] = 0
    for t in b:
        up["contract"][t] = up["manual_compiled"][t] = 0
        up["manual_prompt"][t] = 3
    p = rc.permutation_p(up, a, b, n=2000, seed=1)
    assert p < 0.01
    assert p == rc.permutation_p(up, a, b, n=2000, seed=1)
    assert rc.permutation_p(up, b, a, n=2000, seed=1) > 0.99
