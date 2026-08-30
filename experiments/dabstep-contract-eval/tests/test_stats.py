import json

import pytest
from dce.stats import (
    HARNESS_VERDICTS,
    PRIMARY_LEFT_ARM,
    PRIMARY_MODEL,
    PRIMARY_RIGHT_ARM,
    accuracy_by,
    load,
    mcnemar,
    report,
    wilson,
)


def test_wilson_interval_brackets_the_point_estimate():
    lo, hi = wilson(80, 100)
    assert lo < 0.80 < hi
    assert 0.70 < lo and hi < 0.88


def test_wilson_handles_a_perfect_score_without_a_zero_width_interval():
    # At the ceiling a naive normal interval collapses to [1.0, 1.0] and
    # would imply certainty the data does not support.
    lo, hi = wilson(100, 100)
    assert lo < 1.0
    assert hi == pytest.approx(1.0)


def test_mcnemar_reports_discordant_pairs():
    a = {"1": True, "2": False, "3": True, "4": False}
    b = {"1": True, "2": True, "3": True, "4": True}
    result = mcnemar(a, b)
    assert result["b_only"] == 2  # b right where a wrong
    assert result["a_only"] == 0
    assert result["discordant"] == 2


def test_mcnemar_with_no_discordant_pairs_is_not_significant():
    a = {"1": True, "2": False}
    result = mcnemar(a, dict(a))
    assert result["discordant"] == 0
    assert result["p_value"] == 1.0


def test_mcnemar_ignores_tasks_missing_from_either_arm():
    a = {"1": True, "2": False}
    b = {"1": False}
    assert mcnemar(a, b)["n_paired"] == 1


def test_mcnemar_flags_low_power_below_the_discordant_threshold():
    a = {"1": True}
    b = {"1": False}
    result = mcnemar(a, b)
    assert result["discordant"] == 1
    assert result["low_power"] is True


def test_mcnemar_does_not_warn_once_discordant_pairs_are_plentiful():
    # 12 discordant pairs, all b-only, clears LOW_POWER_DISCORDANT_THRESHOLD.
    a = {str(i): False for i in range(12)}
    b = {str(i): True for i in range(12)}
    result = mcnemar(a, b)
    assert result["discordant"] == 12
    assert result["low_power"] is False


def test_accuracy_by_stratum():
    rows = [
        {"level": "easy", "verdict": "correct"},
        {"level": "easy", "verdict": "incorrect"},
        {"level": "hard", "verdict": "correct"},
    ]
    assert accuracy_by(rows, "level") == {"easy": (1, 2), "hard": (1, 1)}


def test_accuracy_by_counts_a_harness_failure_as_neither_correct_nor_absent():
    # A caller who does NOT pre-filter to answer verdicts (a STRICT read)
    # gets a harness failure counted against the denominator as wrong,
    # simply because it isn't "correct" -- never dropped silently.
    rows = [
        {"level": "easy", "verdict": "correct"},
        {"level": "easy", "verdict": "hit_limit"},
    ]
    assert accuracy_by(rows, "level") == {"easy": (1, 2)}


def _row(
    task_id: str,
    arm: str,
    verdict: str,
    *,
    model: str = PRIMARY_MODEL,
    level: str = "easy",
    usd: float = 0.01,
) -> dict:
    return {
        "task_id": task_id,
        "level": level,
        "arm": arm,
        "model": model,
        "verdict": verdict,
        "usd": usd,
    }


def _write(path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")


def test_load_deduplicates_by_task_arm_model(tmp_path):
    # A retried unit: two stale construction_error rows, then a real
    # success, all for the same (task_id, arm, model) key. A naive reader
    # would count 1 correct out of 3; the deduplicated view must see 1/1.
    rows = [
        _row("t1", "contract", "construction_error"),
        _row("t1", "contract", "construction_error"),
        _row("t1", "contract", "correct"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    loaded = load(path)

    assert len(loaded) == 1
    assert loaded[0]["verdict"] == "correct"


def test_report_scores_the_deduped_row_not_all_three_stale_attempts(tmp_path):
    # End-to-end version of the dedup requirement, through report() itself:
    # without dedup this unit would score 1/4 (25%); with it, 1/1 (100%).
    rows = [
        _row("t1", PRIMARY_LEFT_ARM, "correct"),
        _row("t1", PRIMARY_RIGHT_ARM, "construction_error"),
        _row("t1", PRIMARY_RIGHT_ARM, "construction_error"),
        _row("t1", PRIMARY_RIGHT_ARM, "correct"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    assert "scored    1/1" in text
    assert "strict    1/1" in text


def test_report_covers_every_verdict_without_crashing_or_miscounting(tmp_path):
    all_verdicts = sorted(HARNESS_VERDICTS | {"correct", "incorrect"})
    rows = [
        _row(f"t{i}", PRIMARY_LEFT_ARM, verdict, level="easy" if i % 2 else "hard")
        for i, verdict in enumerate(all_verdicts)
    ] + [
        _row(f"t{i}", PRIMARY_RIGHT_ARM, verdict, level="easy" if i % 2 else "hard")
        for i, verdict in enumerate(all_verdicts)
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    # One correct and one incorrect out of the seven verdicts: SCORED is
    # 1/2, STRICT is 1/7, for each arm. (Each arm's summary is printed
    # once in the PRIMARY section and again in its SECONDARY breakdown.)
    assert "scored    1/2" in text
    assert "strict    1/7" in text
    # Every harness verdict is broken out by name somewhere in the report.
    for verdict in HARNESS_VERDICTS:
        assert verdict in text


def test_report_labels_the_primary_section_before_secondary(tmp_path):
    rows = [
        _row("t1", PRIMARY_LEFT_ARM, "correct"),
        _row("t1", PRIMARY_RIGHT_ARM, "correct"),
        _row("t1", "schema_only", "correct"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    primary_idx = text.index("# PRIMARY")
    secondary_idx = text.index("# SECONDARY")
    assert primary_idx < secondary_idx
    assert primary_idx == 0


def test_report_prints_discordant_count_next_to_every_p_value(tmp_path):
    rows = [
        _row("t1", PRIMARY_LEFT_ARM, "incorrect"),
        _row("t1", PRIMARY_RIGHT_ARM, "correct"),
        _row("t2", PRIMARY_LEFT_ARM, "correct"),
        _row("t2", PRIMARY_RIGHT_ARM, "correct"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    assert "p=" in text
    assert "discordant=" in text
    for line in text.splitlines():
        if "p=" in line:
            assert "discordant=" in line


def test_report_warns_on_low_power_when_discordant_pairs_are_scarce(tmp_path):
    rows = [
        _row("t1", PRIMARY_LEFT_ARM, "incorrect"),
        _row("t1", PRIMARY_RIGHT_ARM, "correct"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    assert "LOW POWER" in text


def test_report_flags_harness_limited_when_scored_and_strict_diverge(tmp_path):
    # contract arm: mostly hit_limit (a harness outcome) with one correct
    # answer. SCORED accuracy (1/1 = 100%) and STRICT accuracy (1/6 = 17%)
    # diverge enormously -- this arm's number must be flagged, not silently
    # reported as if it were a clean 100%.
    rows = [_row(f"t{i}", PRIMARY_RIGHT_ARM, "hit_limit") for i in range(5)]
    rows.append(_row("t5", PRIMARY_RIGHT_ARM, "correct"))
    rows.append(_row("t5", PRIMARY_LEFT_ARM, "correct"))
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    assert "HARNESS-LIMITED" in text
    assert "FINDINGS.md must say so" in text


def test_report_never_reports_usd_guard_as_cost(tmp_path):
    row = _row("t1", PRIMARY_LEFT_ARM, "correct", usd=0.03)
    row["usd_guard"] = 99.0  # the cap's pessimistic ledger, not real spend
    other = _row("t1", PRIMARY_RIGHT_ARM, "correct", usd=0.05)
    other["usd_guard"] = 99.0
    path = tmp_path / "results.jsonl"
    _write(path, [row, other])

    text = report(path)

    assert "$0.03" in text
    assert "$99.00" not in text
