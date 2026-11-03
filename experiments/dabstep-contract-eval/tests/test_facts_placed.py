"""Experiment 5e's analysis (`analysis/facts_placed.py`), on synthetic rows."""

import gzip
import io
import json
import sys
import tarfile
from collections import Counter
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import facts_placed as fp  # noqa: E402

TASKS = [str(t) for t in range(1, 101)]
#: 25 shapes of four tasks each.
SHAPES = {t: f"s{int(t) % 25}" for t in TASKS}


def test_each_model_reads_its_own_repeats_and_the_scale_files_are_named():
    assert fp.FILES == {"luna": "exp5e-luna-r{}.jsonl", "qwen": "exp5e-qwen-r{}.jsonl"}
    assert fp.SCALE_FILES == "exp5e-scale-qwen-r{}.jsonl"
    assert fp.TRACES == "exp5e-{model}-r{i}-traces.tgz"


def test_the_six_arms_run_together():
    assert fp.ARMS == (
        "contract",
        "contract_text",
        "contract_plain",
        "contract_plain_text",
        "contract_noted",
        "manual_summed",
    )


def test_the_contrasts_are_those_the_design_registers():
    assert fp.CONTRASTS["R"] == ("contract_plain", "contract_plain_text")
    assert fp.CONTRASTS["P"] == ("contract_plain", "contract")
    assert fp.CONTRASTS["H"] == ("manual_summed", "contract")
    assert fp.CONTRASTS["R0"] == ("contract", "contract_text")
    assert fp.CONTRASTS["placed-noted"] == ("contract_plain", "contract_noted")
    assert fp.CONTRASTS["F"] == ("contract_plain_text", "manual_summed")
    assert fp.CONFIRMATORY == ("R", "P", "H")
    assert "RxW" in fp.EXPLORATORY


def test_each_rerun_arm_pairs_with_its_earlier_run():
    assert fp.EARLIER["luna"] == {
        "contract": "exp5d-luna-r{}.jsonl",
        "contract_text": "exp5c-luna-r{}.jsonl",
        "contract_noted": "exp5d-luna-r{}.jsonl",
        "manual_summed": "exp5c-luna-r{}.jsonl",
    }
    assert fp.EARLIER["qwen"]["manual_summed"] == "exp5c-qwen-r{}.jsonl"
    assert fp.DRIFT_POINTS == 0.03


def _score(per_arm: dict[str, dict[str, int]]) -> dict[str, Counter]:
    return {arm: Counter(v) for arm, v in per_arm.items()}


def test_the_interaction_is_a_difference_of_route_differences():
    score = _score(
        {
            "contract_plain": {"1": 3},
            "contract_plain_text": {"1": 1},
            "contract": {"1": 2},
            "contract_text": {"1": 2},
        }
    )
    assert fp.differences(score, "RxW", ["1"]) == pytest.approx({"1": 2 / 3})
    assert fp.differences(score, "R", ["1"]) == pytest.approx({"1": 2 / 3})


def _flat(level: int) -> dict[str, int]:
    return {t: level for t in TASKS}


def _ahead(by: int) -> dict[str, int]:
    """Every odd-numbered task `by` runs ahead of level 1: two tasks of
    each shape."""
    return {t: 1 + (by if int(t) % 2 else 0) for t in TASKS}


def test_49_holds_when_the_route_is_level_and_fails_when_the_governed_arm_leads():
    level = _score({"contract_plain": _flat(2), "contract_plain_text": _flat(2)})
    lead = _score({"contract_plain": _ahead(2), "contract_plain_text": _flat(1)})
    for score, holds in ((level, True), (lead, False)):
        out = fp.prediction("(49)", score, TASKS, SHAPES)
        assert out["holds"] is holds
        assert out["kind"] == "non-superiority"


def test_50q_needs_a_positive_shape_level_effect():
    up = _score({"contract_plain": _ahead(2), "contract": _flat(1)})
    flat = _score({"contract_plain": _flat(1), "contract": _flat(1)})
    assert fp.prediction("(50Q)", up, TASKS, SHAPES)["holds"]
    assert not fp.prediction("(50Q)", flat, TASKS, SHAPES)["holds"]


def test_51_is_non_inferiority_of_the_prose_against_the_governed_contract():
    level = _score({"manual_summed": _flat(2), "contract": _flat(2)})
    worse = _score({"manual_summed": _flat(1), "contract": _ahead(2)})
    assert fp.prediction("(51)", level, TASKS, SHAPES)["holds"]
    assert not fp.prediction("(51)", worse, TASKS, SHAPES)["holds"]


def test_the_estimates_carry_no_verdict():
    score = _score({"contract_plain": _ahead(1), "contract": _flat(1)})
    out = fp.prediction("(50)", score, TASKS, SHAPES)
    assert out["kind"] == "estimate" and out["holds"] is None


def test_the_four_confirmatory_predictions_are_holm_adjusted_together():
    by_model = {
        "luna": _score(
            {
                "contract_plain": _flat(2),
                "contract_plain_text": _flat(2),
                "contract": _flat(2),
                "manual_summed": _flat(2),
            }
        ),
        "qwen": _score(
            {
                "contract_plain": _ahead(2),
                "contract_plain_text": _ahead(2),
                "contract": _flat(1),
                "manual_summed": _flat(1),
            }
        ),
    }
    out = fp.predictions(by_model, TASKS, SHAPES)
    assert [o["label"] for o in out] == [
        "(49)",
        "(49Q)",
        "(50Q)",
        "(51)",
        "(50)",
        "(51Q)",
    ]
    confirmatory = [o for o in out if o["kind"] != "estimate"]
    assert all("holm_p" in o for o in confirmatory)
    assert all(o["holm_p"] >= o["family_p"] for o in confirmatory)
    assert all("holm_p" not in o for o in out if o["kind"] == "estimate")


def test_drift_is_flagged_beyond_three_points_only():
    now = Counter({t: 2 for t in TASKS})
    same = fp.drift(now, Counter({t: 2 for t in TASKS}), set(TASKS))
    assert not same["flagged"] and same["gap"] == 0
    # 10 of 100 tasks one run lower before: 10 / 300 = 3.3 points.
    before = Counter({t: 2 - (int(t) <= 10) for t in TASKS})
    moved = fp.drift(now, before, set(TASKS))
    assert moved["flagged"] and (moved["up"], moved["down"]) == (10, 0)
    # 9 tasks: 3.0 points, not flagged.
    before = Counter({t: 2 - (int(t) <= 9) for t in TASKS})
    assert not fp.drift(now, before, set(TASKS))["flagged"]


def _call(tool: str, /, **args) -> dict:
    return {"part_kind": "tool-call", "tool_name": tool, "args": json.dumps(args)}


def _trace(*calls: dict) -> list[dict]:
    return [{"parts": [c]} for c in calls]


def test_the_lookup_log_reads_what_was_fetched_before_the_first_fee_query():
    trace = _trace(
        _call("run_query", sql="SELECT COUNT(*) FROM main.payments"),
        _call("lookup_domain", name="fees"),
        _call("list_metrics", domain="fees"),
        _call("run_query", sql="SELECT * FROM main.fees f"),
        _call("lookup_metric", metric_name="total_transaction_fees"),
    )
    log = fp.lookup_log(trace)
    assert log["fee_query"]
    assert log["domain"] and log["list"] and not log["metric"]
    assert log["fetched_before"] and log["fetched_ever"]


def test_a_metric_lookup_counts_for_either_fee_metric_and_a_dict_args_trace():
    trace = [
        {
            "parts": [
                {
                    "part_kind": "tool-call",
                    "tool_name": "lookup_metric",
                    "args": {"metric_name": "fee_rule_matches_transaction"},
                },
                {
                    "part_kind": "tool-call",
                    "tool_name": "run_query",
                    "args": {"sql": "select sum(x) from fees"},
                },
            ]
        }
    ]
    log = fp.lookup_log(trace)
    assert log["metric"] and not log["domain"]


def test_a_fee_query_must_name_the_fees_table():
    assert fp.is_fee_query("SELECT * FROM main.fees")
    assert fp.is_fee_query("select 1 from fees f join payments p on true")
    assert not fp.is_fee_query("SELECT total_transaction_fees FROM main.payments")
    assert not fp.is_fee_query("SELECT * FROM main.payments")


def test_lost_runs_split_by_whether_the_fact_was_fetched():
    never = fp.lookup_log(_trace(_call("run_query", sql="SELECT * FROM fees")))
    after = fp.lookup_log(
        _trace(
            _call("run_query", sql="SELECT * FROM fees"),
            _call("lookup_domain", name="fees"),
        )
    )
    before = fp.lookup_log(
        _trace(
            _call("lookup_domain", name="fees"),
            _call("run_query", sql="SELECT * FROM fees"),
        )
    )
    assert fp.loss_kind(never) == "never fetched"
    assert fp.loss_kind(after) == "fetched after the first fee query"
    assert fp.loss_kind(before) == "fetched, not used"


def _archive(path: Path, folder: str, traces: dict[str, list[dict]]) -> None:
    with tarfile.open(path, "w:gz") as tar:
        for name, trace in traces.items():
            data = gzip.compress(json.dumps(trace).encode("utf-8"))
            info = tarfile.TarInfo(f"{folder}/{name}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))


def test_traces_are_read_from_the_repeat_archives(tmp_path: Path):
    _archive(
        tmp_path / "exp5e-luna-r1-traces.tgz",
        "exp5e-luna-r1",
        {
            "7__contract_plain__gpt-6-luna.json.gz": _trace(
                _call("lookup_domain", name="fees")
            ),
            "7__contract_plain_text__gpt-6-luna.json.gz": _trace(),
        },
    )
    logs = fp.read_logs(tmp_path, "luna")
    assert set(logs) == {(1, "7", "contract_plain")}
    assert logs[(1, "7", "contract_plain")]["domain"]


def _row(task: str, arm: str, model: str, correct: bool, **extra) -> dict:
    return {
        "task_id": task,
        "arm": arm,
        "model": model,
        "benchmark": extra.pop("benchmark", "dabstep"),
        "verdict": "correct" if correct else "incorrect",
        "answer": "1" if correct else "2",
        "gold": "1",
        **extra,
    }


def test_no_drop_reads_the_exact_bound_on_lost_runs():
    tasks = [f"db_R{i:02d}" for i in range(30)] + [f"db_C{i:02d}" for i in range(30)]
    piece = {t: t[:4] for t in tasks}
    one = {"manual_scaled_1x": Counter({t: 3 for t in tasks})}
    same = {**one, "manual_scaled_all": Counter({t: 3 for t in tasks})}
    out = fp.no_drop(same, tasks, piece, runs=3)
    assert out["lost"] == 0 and out["holds"]
    worse = {**one, "manual_scaled_all": Counter({t: 2 for t in tasks})}
    out = fp.no_drop(worse, tasks, piece, runs=3)
    assert out["lost"] == 60 and not out["holds"]


def test_the_scale_precondition_counts_overflowed_and_truncated_runs():
    rows = [
        {"arm": "manual_scaled_all", "verdict": "correct"},
        {"arm": "manual_scaled_all", "verdict": "overflow"},
        {"arm": "manual_scaled_1x", "verdict": "truncated"},
    ]
    health = fp.scale_health(rows)
    assert health == {"runs": 2, "overflow": 1, "truncated": 0, "holds": False}


def _run(inp: int, usd: float = 0.0) -> dict:
    return {"fresh": inp // 2, "cached": inp - inp // 2, "output": 100, "usd": usd}


def test_53_compares_mean_input_tokens_per_question():
    tasks = [str(i) for i in range(20)]
    costs = {
        "contract_scaled_all": {t: [_run(1000)] * 3 for t in tasks},
        "manual_scaled_all": {t: [_run(5000)] * 3 for t in tasks},
    }
    out = fp.fewer_tokens(costs, tasks)
    assert (out["up"], out["down"]) == (20, 0)
    assert out["holds"] and out["p"] < 1e-5
    costs["manual_scaled_all"] = {t: [_run(1000)] * 3 for t in tasks}
    assert not fp.fewer_tokens(costs, tasks)["holds"]


def _write(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def test_the_report_runs_end_to_end_on_synthetic_files(tmp_path: Path, capsys):
    results = tmp_path / "results"
    results.mkdir()
    for model, name in (("luna", "gpt-6-luna"), ("qwen", "qwen3.8-27b")):
        for i in (1, 2, 3):
            rows = [
                _row(t, arm, name, (int(t) + i + len(arm)) % 3 != 0)
                for t in TASKS
                for arm in fp.ARMS
            ]
            _write(results / fp.FILES[model].format(i), rows)
            for arm, pattern in fp.EARLIER[model].items():
                path = results / pattern.format(i)
                with path.open("a", encoding="utf-8") as fh:
                    for t in TASKS:
                        fh.write(json.dumps(_row(t, arm, name, int(t) % 2 == 0)) + "\n")
    rated = set(TASKS[:60])
    fp.report(results, shapes=SHAPES, rated=rated, traces=None)
    out = capsys.readouterr().out
    for label in ("(49)", "(49Q)", "(50Q)", "(51)", "(50)", "(51Q)"):
        assert f"{label} " in out
    assert "Holm" in out
    assert "drift" in out
    assert "rule-matching" in out and "all" in out


def test_the_report_says_when_there_are_no_results_yet(tmp_path: Path, capsys):
    fp.report(tmp_path, shapes=SHAPES, rated=set(TASKS), traces=None)
    assert "no experiment 5e results yet" in capsys.readouterr().out
