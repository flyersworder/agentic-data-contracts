import json
import sys
from collections import Counter
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import procedures_gap as pg  # noqa: E402

LABELS = {
    "settled": ["s:g1", "s:g2"],
    "split": ["l:g1", "r:g1", "r:g2"],
    "convention-only": ["r:g3"],
}


def test_each_model_reads_its_own_repeats():
    assert pg.FILES == {
        "gpt-6-luna": "exp4b-luna-r{}.jsonl",
        "qwen3.8-27b": "exp4b-qwen-r{}.jsonl",
        "gpt-6-sol": "exp4b-sol-r{}.jsonl",
    }


def test_the_six_arms_run_together():
    assert pg.ARMS == (
        "schema_only",
        "manual_gap",
        "manual_full",
        "contract_gap",
        "manual_noted",
        "contract_text",
    )


def test_a_question_takes_the_label_of_its_gaps_and_g3_wins():
    assert pg.label_of(["s:g2"], LABELS) == "settled"
    assert pg.label_of(["r:g1"], LABELS) == "split"
    # The other R questions of reverse_logistics carry G1 and G3 together.
    assert pg.label_of(["r:g1", "r:g3"], LABELS) == "convention-only"
    assert pg.label_of([], LABELS) == "no gap"


def test_subsets_group_questions_and_clusters():
    tags = {
        "s_R01": ["s:g1"],
        "s_C01": ["s:g2"],
        "l_R01": ["l:g1"],
        "l_C01": [],
        "r_R06": ["r:g1"],
        "r_R01": ["r:g1", "r:g3"],
    }
    sub = pg.subsets(tags, LABELS)
    assert sub["data-informed"] == ["l_R01", "r_R06", "s_C01", "s_R01"]
    assert sub["settled"] == ["s_C01", "s_R01"]
    assert sub["convention-only"] == ["r_R01"]
    assert sub["no gap"] == ["l_C01"]
    assert pg.clusters(tags, sub["data-informed"]) == {
        "l:g1": ["l_R01"],
        "r:g1": ["r_R06"],
        "s:g1": ["s_R01"],
        "s:g2": ["s_C01"],
    }


def _score(**arms):
    return {a: Counter(v) for a, v in arms.items()}


def test_a_directional_test_needs_the_sign_test_every_model_and_most_clusters():
    tasks = [f"t{i}" for i in range(12)]
    up = _score(a={t: 3 for t in tasks}, b={t: 0 for t in tasks})
    by_model = {"m1": up, "m2": up}
    clusters = {"c1": tasks[:6], "c2": tasks[6:]}
    result = pg.directional(by_model, "a", "b", tasks, clusters)
    assert result["holds"]
    assert result["models"] == {"m1": 1.0, "m2": 1.0}
    # One model against: the pooled sign test may pass, the prediction not.
    down = _score(a={t: 0 for t in tasks}, b={t: 1 for t in tasks})
    result = pg.directional({"m1": up, "m2": down}, "a", "b", tasks, clusters)
    assert not result["holds"]


def test_a_directional_test_fails_on_a_minority_of_clusters():
    tasks = [f"t{i}" for i in range(30)]
    a = {t: 3 if i < 10 else 0 for i, t in enumerate(tasks)}
    b = {t: 0 if i < 10 else 1 for i, t in enumerate(tasks)}
    by_model = {"m": _score(a=a, b=b)}
    clusters = {"c1": tasks[:10], "c2": tasks[10:20], "c3": tasks[20:]}
    result = pg.directional(by_model, "a", "b", tasks, clusters)
    assert result["clusters"] == {"c1": 1.0, "c2": -1 / 3, "c3": -1 / 3}
    assert not result["holds"]


def test_the_knowledge_share():
    tasks = ["t1", "t2"]
    score = _score(
        manual_gap={"t1": 0, "t2": 0},
        contract_gap={"t1": 3, "t2": 3},
        manual_noted={"t1": 3, "t2": 0},
    )
    assert pg.share(score, tasks) == 0.5
    flat = _score(manual_gap={"t1": 1}, contract_gap={"t1": 1}, manual_noted={"t1": 1})
    assert pg.share(flat, ["t1"]) is None


def test_the_share_interval_resamples_clusters_and_is_seeded():
    clusters = {"c1": ["t1"], "c2": ["t2"], "c3": ["t3"]}
    score = _score(
        manual_gap={"t1": 0, "t2": 0, "t3": 0},
        contract_gap={"t1": 3, "t2": 3, "t3": 3},
        manual_noted={"t1": 3, "t2": 0, "t3": 3},
    )
    one = pg.share_interval(score, clusters, n=500)
    assert one == pg.share_interval(score, clusters, n=500)
    lo, hi, dropped = one
    assert 0.0 <= lo <= 2 / 3 <= hi <= 1.0
    assert dropped == 0


def test_the_share_verdict():
    assert pg.share_verdict(0.6, 0.3, 0.9) == "holds"
    assert pg.share_verdict(0.4, 0.3, 0.6) == "inconclusive"
    assert pg.share_verdict(0.2, 0.1, 0.45) == "falsified"
    assert pg.share_verdict(None, None, None) == "undefined"


RECOVERY = {
    "s": {
        d: {"g1": {"category": "recovered"}, "g2": {"category": "recovered"}}
        for d in ("draw-1", "draw-2", "draw-3")
    },
    "l": {d: {"g1": {"category": "recovered"}} for d in ("draw-1", "draw-2", "draw-3")},
    "r": {
        d: {
            "g1": {"category": "partial"},
            "g2": {"category": "recovered"},
            "g3": {"convention": "sum"},
        }
        for d in ("draw-1", "draw-2", "draw-3")
    },
}
RECOVERY["s"]["no-data"] = {"g1": {"category": "missed"}, "g2": {"category": "missed"}}
RECOVERY["l"]["no-data"] = {"g1": {"category": "recovered"}}
RECOVERY["r"]["no-data"] = {
    "g1": {"category": "missed"},
    "g2": {"category": "missed"},
    "g3": {"convention": "most_specific"},
}


def test_recovery_rates_per_label_and_draw_kind():
    rates = pg.recovery_rates(RECOVERY, LABELS)
    assert rates["data"] == {
        "settled": (6, 6),
        "split": (6, 9),
        "convention-only": (0, 3),
    }
    assert rates["no-data"] == {
        "settled": (0, 2),
        "split": (1, 3),
        "convention-only": (1, 1),
    }


def test_recovery_predictions():
    out = pg.recovery_predictions(RECOVERY, LABELS)
    assert out["(32)"] == (True, "12 of 15 data gap-draws recovered")
    assert out["(32g)"][0] is True
    assert out["(32c)"][0] is True  # 1 of 5 against 12 of 15
    assert out["(33)"] == (True, "data draws: sum 3; no-data: most_specific")


def test_recovery_predictions_fail_when_a_draw_takes_most_specific():
    rec = json.loads(json.dumps(RECOVERY))
    rec["r"]["draw-2"]["g3"]["convention"] = "most_specific"
    assert pg.recovery_predictions(rec, LABELS)["(33)"][0] is False
    # (32g) asks for a strict gradient: a split rate equal to the settled one
    # breaks it.
    rec = json.loads(json.dumps(RECOVERY))
    for d in ("draw-1", "draw-2", "draw-3"):
        rec["r"][d]["g1"]["category"] = "recovered"
    assert pg.recovery_predictions(rec, LABELS)["(32g)"][0] is False


@pytest.mark.parametrize("n_rows", [1, 2])
def test_scores_count_correct_runs_per_task_and_model(tmp_path, n_rows):
    base = {"model": "m", "gold": "x", "arm": "manual_gap"}
    rows = [
        {**base, "task_id": "t1", "verdict": "correct"},
        {**base, "task_id": "t2", "verdict": "incorrect"},
    ][:n_rows]
    for i in (1, 2, 3):
        (tmp_path / f"f-r{i}.jsonl").write_text(
            "\n".join(json.dumps({**r, "order": n}) for n, r in enumerate(rows)),
            encoding="utf-8",
        )
    score = pg.scores(tmp_path, "f-r{}.jsonl")
    assert score["manual_gap"]["t1"] == 3
    assert score["manual_gap"]["t2"] == 0


def _write_runs(tmp_path, rows):
    base = {"model": "m", "gold": "x", "cached_tokens": 0, "reasoning_tokens": 0}
    for i in (1, 2, 3):
        (tmp_path / f"f-r{i}.jsonl").write_text(
            "\n".join(
                json.dumps({**base, **r, "order": n}) for n, r in enumerate(rows)
            ),
            encoding="utf-8",
        )


def test_runs_carry_the_cost_of_each_run(tmp_path):
    _write_runs(
        tmp_path,
        [
            {
                "task_id": "t1",
                "arm": "contract_gap",
                "verdict": "correct",
                "turns": 4,
                "tool_calls": ["lookup_metric", "lookup_domain", "run_query"],
                "input_tokens": 1000,
                "cached_tokens": 600,
                "output_tokens": 50,
                "reasoning_tokens": 20,
                "usd": 0.002,
            }
        ],
    )
    (run, *_) = pg.run_costs(tmp_path, "f-r{}.jsonl")["contract_gap"]["t1"]
    assert run == {
        "turns": 4,
        "calls": 3,
        "lookups": 2,
        "fresh": 400,
        "cached": 600,
        "output": 50,
        "reasoning": 20,
        "usd": 0.002,
        "correct": True,
    }


def _run(usd, correct, turns=1):
    return {
        "turns": turns,
        "calls": 0,
        "lookups": 0,
        "fresh": 0,
        "cached": 0,
        "output": 0,
        "reasoning": 0,
        "usd": usd,
        "correct": correct,
    }


def test_efficiency_is_the_mean_run_and_the_cost_of_a_correct_answer():
    by_arm = {
        "a": {"t1": [_run(1.0, True), _run(3.0, False)], "t2": [_run(2.0, True)]},
        "b": {"t1": [_run(1.0, False)], "t2": [_run(1.0, False)]},
    }
    eff = pg.efficiency(by_arm, ["t1", "t2"])
    assert eff["a"]["usd"] == 2.0
    assert eff["a"]["usd_per_correct"] == 3.0
    # Only the tasks asked for count.
    assert pg.efficiency(by_arm, ["t2"])["a"]["usd"] == 2.0
    # No correct answer: no cost per correct answer.
    assert eff["b"]["usd_per_correct"] is None


def test_cheaper_compares_each_tasks_mean_run():
    by_arm = {
        "a": {
            "t1": [_run(1.0, True)],
            "t2": [_run(1.0, True)],
            "t3": [_run(5.0, True)],
        },
        "b": {
            "t1": [_run(2.0, True)],
            "t2": [_run(3.0, True)],
            "t3": [_run(5.0, True)],
        },
    }
    assert pg.cheaper(by_arm, "a", "b", ["t1", "t2", "t3"]) == (2, 0, 0.5)
    by_arm["a"]["t1"] = [_run(1.0, True, turns=9)]
    assert pg.cheaper(by_arm, "a", "b", ["t1", "t2"], "turns")[:2] == (0, 1)
