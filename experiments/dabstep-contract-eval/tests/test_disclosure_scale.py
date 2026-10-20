import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import disclosure_scale as ds  # noqa: E402

TASKS = [f"t{i}" for i in range(12)]


def _score(**arms):
    return {a: Counter(v) for a, v in arms.items()}


def test_the_common_top_is_the_largest_level_every_arm_fits_on_every_model():
    fit = {
        "m1": {f"{k}_{lv}": {"fits": True} for k in ds.KINDS for lv in ds.LEVELS},
        "m2": {f"{k}_{lv}": {"fits": True} for k in ds.KINDS for lv in ds.LEVELS},
    }
    assert ds.common_top(fit) == "all"
    fit["m2"]["contract_text_scaled_8x"]["fits"] = False
    fit["m2"]["contract_text_scaled_all"]["fits"] = False
    assert ds.common_top(fit) == "4x"
    assert ds.own_top(fit, "m1") == "all"
    assert ds.own_top(fit, "m2") == "4x"


def test_a_drop_holds_on_the_sign_test_and_every_model():
    one = {t: 3 for t in TASKS}
    low = {t: 1 for t in TASKS}
    by_model = {
        "m1": _score(contract_text_scaled_1x=one, contract_text_scaled_4x=low),
        "m2": _score(contract_text_scaled_1x=one, contract_text_scaled_4x=low),
    }
    r = ds.drop(by_model, "contract_text_scaled", "4x", TASKS)
    assert r["holds"]
    assert r["up"] == 12 and r["down"] == 0
    assert r["models"] == {"m1": 2 / 3, "m2": 2 / 3}
    by_model["m2"] = _score(contract_text_scaled_1x=one, contract_text_scaled_4x=one)
    assert not ds.drop(by_model, "contract_text_scaled", "4x", TASKS)["holds"]


def test_the_difference_of_drops_compares_each_tasks_two_drops():
    one = {t: 3 for t in TASKS}
    by_model = {
        "m": _score(
            contract_text_scaled_1x=one,
            contract_text_scaled_4x={t: 0 for t in TASKS},
            contract_scaled_1x=one,
            contract_scaled_4x={t: 2 for t in TASKS},
        )
    }
    r = ds.smaller_drop(by_model, "4x", TASKS)
    assert r["holds"] and r["up"] == 12
    by_model["m"]["contract_scaled_4x"] = Counter({t: 0 for t in TASKS})
    assert not ds.smaller_drop(by_model, "4x", TASKS)["holds"]


def test_the_equivalence_interval_is_seeded_and_brackets_the_mean():
    a = {t: 3 for t in TASKS}
    b = {t: 3 if i % 4 else 2 for i, t in enumerate(TASKS)}
    score = _score(x=a, y=b)
    lo, hi = ds.interval(score, "x", "y", TASKS, runs=3, n=500)
    assert (lo, hi) == ds.interval(score, "x", "y", TASKS, runs=3, n=500)
    mean = (1 / 3) * 3 / 12
    assert lo <= mean <= hi
    assert ds.equivalent(lo, hi, bound=0.25)
    assert not ds.equivalent(lo, hi, bound=0.01)


def test_the_governed_arm_holds_where_the_text_cannot_fit():
    one = {t: 3 for t in TASKS}
    score = _score(contract_scaled_1x=one, contract_scaled_8x=one)
    assert ds.holds_up(score, "8x", TASKS, runs=3)
    score["contract_scaled_8x"] = Counter({t: 1 for t in TASKS})
    assert not ds.holds_up(score, "8x", TASKS, runs=3)


def test_the_reading_follows_the_registered_outcomes():
    assert ds.reading(p37=True, eq37=False, p38=True, p38b=False).startswith(
        "(37) and (38) hold"
    )
    assert ds.reading(p37=False, eq37=True, p38=False, p38b=False).startswith(
        "(37) equivalent"
    )
    assert ds.reading(p37=False, eq37=False, p38=False, p38b=False).startswith(
        "(37) inconclusive"
    )
    assert "hurts both" in ds.reading(p37=True, eq37=False, p38=False, p38b=True)
    assert "inconclusive between" in ds.reading(
        p37=True, eq37=False, p38=False, p38b=False
    )
