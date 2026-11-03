"""Experiment 7's registered analysis (`analysis/dataagentbench.py`), on
synthetic rows only: no pilot or run row enters these tests."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import dataagentbench as da  # noqa: E402

LUNA = "gpt-6-luna"
DESC, HINTS, CONTRACT, TEXT, BOTH = da.ARMS


def _row(task, arm, verdict="correct", *, model=LUNA, overlong=False):
    return {
        "task_id": task,
        "arm": arm,
        "model": model,
        "verdict": verdict,
        "overlong": overlong,
        "gold": "sha256-of-the-gold",
        "answer": "x",
    }


def _repeats(spec: dict[str, dict[str, int]], runs: int = 5) -> list[list[dict]]:
    """`spec[arm][task]` passes out of `runs` repeats, the passes first."""
    return [
        [
            _row(task, arm, "correct" if i < passes else "incorrect")
            for arm, per in spec.items()
            for task, passes in per.items()
        ]
        for i in range(runs)
    ]


def _tasks(dataset: str, n: int) -> list[str]:
    return [f"{dataset}/{i}" for i in range(1, n + 1)]


def _flat(tasks, passes):
    return {t: passes for t in tasks}


def _write(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


# -- registration -----------------------------------------------------------


def test_the_files_arms_and_thresholds_are_those_the_design_registers():
    assert da.ARMS == (
        "dab_desc",
        "dab_hints",
        "dab_contract",
        "dab_contract_text",
        "dab_hints_contract",
    )
    assert da.MODELS == ("gpt-6-luna", "qwen3.8-27b")
    assert da.PATTERN == "exp7-{model}-r{i}.jsonl"
    assert da.REPEATS == (1, 2, 3, 4, 5)
    assert da.DIRECTIONAL == {
        "(44)": ("dab_hints", "dab_desc"),
        "(45)": ("dab_contract", "dab_desc"),
        "(48)": ("dab_hints_contract", "dab_hints"),
    }
    assert da.TIE == ("(47)", ("dab_contract", "dab_contract_text"))
    assert (da.ALPHA, da.TIE_P, da.TIE_POINTS) == (0.05, 0.10, 0.05)
    assert (da.SHARE_MIN, da.OVERLONG_POINTS) == (0.5, 0.05)


def test_a_task_belongs_to_the_dataset_before_its_number():
    assert da.dataset_of("music_brainz_20k/3") == "music_brainz_20k"
    assert da.dataset_of("PANCANCER_ATLAS/1") == "PANCANCER_ATLAS"


# -- reading the files ------------------------------------------------------


def test_the_last_row_per_task_arm_model_in_a_repeat_wins(tmp_path: Path):
    _write(
        tmp_path / "exp7-gpt-6-luna-r1.jsonl",
        [
            _row("a/1", DESC, "error"),
            _row("a/1", DESC, "correct"),
            _row("a/1", DESC, "correct", model="qwen3.8-27b"),
        ],
    )
    present, repeats = da.read_repeats(tmp_path, LUNA)
    assert present == [1]
    assert repeats == [[_row("a/1", DESC, "correct")]]


def test_missing_repeats_are_named_and_the_rest_still_read(tmp_path: Path):
    for i in (1, 3):
        _write(tmp_path / f"exp7-gpt-6-luna-r{i}.jsonl", [_row("a/1", DESC)])
    present, repeats = da.read_repeats(tmp_path, LUNA)
    assert present == [1, 3]
    assert len(repeats) == 2


def test_harness_failures_count_against_the_arm():
    t = da.tally([[_row("a/1", DESC, "hit_limit")], [_row("a/1", DESC)]])
    assert t[DESC]["a/1"] == (1, 2)


# -- accuracy -----------------------------------------------------------------


def test_accuracy_is_pooled_over_queries_and_the_leaderboard_form_over_datasets():
    big, small = _tasks("big", 3), _tasks("small", 1)
    t = da.tally(_repeats({DESC: {**_flat(big, 5), **_flat(small, 0)}}))
    tasks = big + small
    assert da.accuracy(t, DESC, tasks) == pytest.approx(0.75)
    assert da.leaderboard(t, DESC, tasks) == pytest.approx(0.5)


def test_excluded_datasets_leave_the_tested_tasks_only():
    tasks = _tasks("a", 2) + _tasks("stockmarket", 3)
    assert da.tested(tasks, {"stockmarket"}) == _tasks("a", 2)


# -- (44), (45), (48): directional --------------------------------------------


def test_a_directional_prediction_holds_on_the_sign_test_and_a_dataset_majority():
    tasks = _tasks("a", 4) + _tasks("b", 4) + _tasks("c", 4)
    t = da.tally(_repeats({HINTS: _flat(tasks, 4), DESC: _flat(tasks, 1)}))
    out = da.directional(t, HINTS, DESC, tasks)
    assert (out["up"], out["down"]) == (12, 0)
    assert out["p"] == pytest.approx(0.5**12)
    assert out["directions"] == pytest.approx({"a": 0.6, "b": 0.6, "c": 0.6})
    assert out["holds"] is True


def test_a_passing_sign_test_fails_when_most_differing_datasets_go_the_other_way():
    big = _tasks("big", 13)
    small = ["s1/1", "s2/1", "s3/1"]
    tie = ["level/1"]
    spec = {
        HINTS: {**_flat(big, 4), **_flat(small, 1), **_flat(tie, 2)},
        DESC: {**_flat(big, 1), **_flat(small, 3), **_flat(tie, 2)},
    }
    tasks = big + small + tie
    out = da.directional(da.tally(_repeats(spec)), HINTS, DESC, tasks)
    assert (out["up"], out["down"]) == (13, 3)
    assert out["p"] < 0.05
    # Four datasets differ (the level one does not); one goes the way predicted.
    assert out["differing"] == 4 and out["agreeing"] == 1
    assert out["directions"]["level"] == 0
    assert out["holds"] is False


def test_a_directional_prediction_fails_on_the_sign_test_alone():
    tasks = _tasks("a", 3)
    t = da.tally(_repeats({HINTS: _flat(tasks, 4), DESC: _flat(tasks, 1)}))
    out = da.directional(t, HINTS, DESC, tasks)
    assert out["p"] == pytest.approx(0.125)
    assert out["holds"] is False


# -- (47): no detectable difference -------------------------------------------


def test_47_holds_when_the_arms_tie():
    tasks = _tasks("a", 6) + _tasks("b", 6)
    t = da.tally(_repeats({CONTRACT: _flat(tasks, 3), TEXT: _flat(tasks, 3)}))
    out = da.tie(t, CONTRACT, TEXT, tasks)
    assert out["difference"] == 0 and out["p"] == 1.0
    assert out["holds"] is True


def test_47_fails_beyond_five_points():
    tasks = _tasks("a", 6) + _tasks("b", 6)
    t = da.tally(_repeats({CONTRACT: _flat(tasks, 3), TEXT: _flat(tasks, 2)}))
    out = da.tie(t, CONTRACT, TEXT, tasks)
    assert out["difference"] == pytest.approx(0.2)
    assert out["holds"] is False


def test_47_needs_the_two_sided_p_too_even_within_five_points():
    tasks = [t for d in "abcd" for t in _tasks(d, 5)]
    # Five of the twenty tasks gain one pass of five: exactly 5 points, all
    # five discordant tasks in one direction.
    ahead = {"a/1", "a/2", "b/1", "c/1", "d/1"}
    spec = {
        CONTRACT: {t: 3 if t in ahead else 2 for t in tasks},
        TEXT: _flat(tasks, 2),
    }
    out = da.tie(da.tally(_repeats(spec)), CONTRACT, TEXT, tasks)
    assert out["difference"] == pytest.approx(0.05)
    assert out["p"] == pytest.approx(0.0625)
    assert out["holds"] is False


# -- (46): the recovered share ---------------------------------------------------


def _share_spec(contract_by_dataset: dict[str, int], n: int = 2) -> tuple:
    tasks, spec = [], {DESC: {}, HINTS: {}, CONTRACT: {}}
    for d, c in contract_by_dataset.items():
        for t in _tasks(d, n):
            tasks.append(t)
            spec[DESC][t], spec[HINTS][t], spec[CONTRACT][t] = 1, 3, c
    return da.tally(_repeats(spec)), tasks


def test_46_holds_at_half_the_hints_gain():
    t, tasks = _share_spec({"a": 2, "b": 2, "c": 2})
    out = da.share(t, tasks)
    assert out["point"] == pytest.approx(0.5)
    assert out["verdict"] == "holds"


def test_46_is_falsified_when_the_interval_lies_below_half():
    t, tasks = _share_spec({"a": 1, "b": 1, "c": 1})
    out = da.share(t, tasks)
    assert out["point"] == 0 and out["upper"] == 0
    assert out["verdict"] == "falsified"


def test_46_is_inconclusive_when_the_interval_spans_half_with_the_point_below():
    t, tasks = _share_spec({"a": 3, "b": 1, "c": 1})
    out = da.share(t, tasks)
    assert out["point"] == pytest.approx(1 / 3)
    assert out["lower"] < 0.5 <= out["upper"]
    assert out["verdict"] == "inconclusive"


def test_46_is_undefined_when_the_hints_do_not_gain():
    tasks = _tasks("a", 2) + _tasks("b", 2)
    spec = {DESC: _flat(tasks, 2), HINTS: _flat(tasks, 2), CONTRACT: _flat(tasks, 3)}
    out = da.share(da.tally(_repeats(spec)), tasks)
    assert out["point"] is None
    assert out["verdict"] == "undefined"


def test_46_resamples_datasets_with_a_fixed_seed_and_counts_dropped_resamples():
    tasks = _tasks("a", 2) + _tasks("b", 2)
    spec = {
        DESC: {**_flat(_tasks("a", 2), 1), **_flat(_tasks("b", 2), 4)},
        HINTS: {**_flat(_tasks("a", 2), 4), **_flat(_tasks("b", 2), 2)},
        CONTRACT: _flat(tasks, 2),
    }
    t = da.tally(_repeats(spec))
    first, second = da.share(t, tasks), da.share(t, tasks)
    assert first == second
    # Resamples drawing b twice have no hints gain: left out and counted.
    assert 0 < first["dropped"] < da.BOOTSTRAP


# -- leave one dataset out --------------------------------------------------------


def test_every_test_is_repeated_leaving_out_one_dataset_at_a_time():
    t, tasks = _share_spec({"a": 3, "b": 1, "c": 1})
    out = da.leave_one_out(t, tasks)
    assert sorted(out) == ["a", "b", "c"]
    assert set(out["a"]) == {"(44)", "(45)", "(46)", "(47)", "(48)"}
    assert out["a"]["(46)"]["point"] == 0
    assert out["b"]["(46)"]["point"] == pytest.approx(0.5)


# -- the leniency check ---------------------------------------------------------


def test_overlong_passes_are_counted_per_arm_and_dropped_in_the_lenient_view():
    rows = [
        [_row("a/1", HINTS, overlong=True), _row("a/1", DESC)],
        [_row("a/1", HINTS), _row("a/1", DESC, "incorrect")],
    ]
    shares = da.overlong_shares(rows, ["a/1"])
    assert shares[HINTS] == pytest.approx(0.5) and shares[DESC] == 0
    lenient = da.tally(rows, lenient=True)
    assert lenient[HINTS]["a/1"] == (1, 1)
    assert lenient[DESC]["a/1"] == (1, 2)


def test_passes_without_an_overlong_flag_are_counted():
    unflagged = _row("a/1", DESC)
    del unflagged["overlong"]
    failed = _row("a/2", DESC, "hit_limit")
    del failed["overlong"]
    assert da.unflagged([[unflagged, failed, _row("a/3", DESC)]]) == 1


def test_a_comparison_is_also_read_lenient_when_its_arms_differ_by_over_five_points():
    # contract against text is exactly 5 points: not more than 5.
    shares = {DESC: 0.0, HINTS: 0.06, CONTRACT: 0.0, TEXT: 0.05, BOTH: 0.12}
    assert da.lenient_labels(shares) == ["(44)", "(46)", "(48)"]
    assert da.lenient_labels(dict.fromkeys(da.ARMS, 0.1)) == []


# -- the report -------------------------------------------------------------------


def _recovery(path: Path) -> Path:
    items = [{"id": "a-1", "cls": "data-discoverable", "category": "recovered"}]
    draws = {
        d: {"items": items, "additions": [], "correctable_errors": []}
        for d in ("draw-a1", "draw-a2", "draw-a3", "draw-b", "no-data")
    }
    draws["no-data"] = {
        "items": [{**items[0], "category": "missed"}],
        "additions": [],
        "correctable_errors": [],
    }
    rec = {"datasets": {"a": {"references": {}, "draws": draws}}}
    path.write_text(json.dumps(rec), encoding="utf-8")
    return path


def _run_files(
    directory: Path, repeats: tuple[int, ...], overlong_hints: bool = False
) -> None:
    tasks = [t for d in ("a", "b", "c", "stockmarket") for t in _tasks(d, 3)]
    for i in repeats:
        rows = []
        for t in tasks:
            floor = da.dataset_of(t) == "stockmarket"
            for arm, passes in ((DESC, 1), (HINTS, 4), (CONTRACT, 3), (TEXT, 3)):
                ok = not floor and i <= passes
                long = overlong_hints and ok and arm == HINTS
                rows.append(
                    _row(t, arm, "correct" if ok else "incorrect", overlong=long)
                )
            rows.append(_row(t, BOTH, "correct" if not floor else "incorrect"))
        _write(directory / f"exp7-gpt-6-luna-r{i}.jsonl", rows)


def test_the_report_runs_end_to_end_on_synthetic_files(tmp_path: Path, capsys):
    _run_files(tmp_path, da.REPEATS)
    da.report(
        tmp_path,
        exclude={"stockmarket"},
        recovery=_recovery(tmp_path / "recovery.json"),
    )
    out = capsys.readouterr().out
    assert "(43)  draw A recovers 3 of 3" in out
    assert "(43c)" in out and "(43w)" in out
    assert "stockmarket" in out and "excluded" in out
    assert "(44) holds" in out
    assert "(45) holds" in out
    assert "(46) holds" in out
    assert "(47) holds" in out
    assert "(48) holds" in out
    assert "leaving out a" in out
    assert "qwen3.8-27b: no results" in out


def test_the_report_rereads_comparisons_without_overlong_passes(tmp_path: Path, capsys):
    _run_files(tmp_path, da.REPEATS, overlong_hints=True)
    da.report(tmp_path, exclude={"stockmarket"}, recovery=tmp_path / "absent.json")
    out = capsys.readouterr().out
    lenient = [line for line in out.splitlines() if "without overlong passes" in line]
    # dab_hints' passes are all overlong: (44), (46) and (48) read dab_hints.
    assert [line.split("passes: ")[1][:4] for line in lenient] == [
        "(44)",
        "(46)",
        "(48)",
    ]


def test_the_report_gives_estimates_only_under_missing_repeats(tmp_path: Path, capsys):
    _run_files(tmp_path, (1, 2))
    da.report(tmp_path, exclude=set(), recovery=tmp_path / "absent.json")
    out = capsys.readouterr().out
    assert "missing repeats r3, r4, r5" in out
    assert "estimate" in out
    for label in ("(44)", "(45)", "(46)", "(47)", "(48)"):
        assert f"{label} holds" not in out and f"{label} fails" not in out
    assert "no recovery file" in out
