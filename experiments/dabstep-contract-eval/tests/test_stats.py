import json

import pytest
from dce.stats import (
    ARM_A,
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


def test_mcnemar_does_not_warn_on_a_significant_result_with_few_discordant_pairs():
    # 6 discordant pairs, all b-only: exact two-sided binomial p = 2*0.5**6
    # = 0.03125 < 0.05. Below LOW_POWER_DISCORDANT_THRESHOLD (10) but
    # already significant -- flagging this "low power" would tell a reader
    # to discard a genuine primary finding, contradicting the module's own
    # rule that the warning means "a non-significant result may just be
    # underpowered", not "fewer than N pairs is always suspect".
    a = {str(i): False for i in range(6)}
    b = {str(i): True for i in range(6)}
    result = mcnemar(a, b)
    assert result["discordant"] == 6
    assert result["p_value"] == pytest.approx(0.03125)
    assert result["p_value"] < 0.05
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
        _row("t1", PRIMARY_RIGHT_ARM, "construction_error"),
        _row("t1", PRIMARY_RIGHT_ARM, "construction_error"),
        _row("t1", PRIMARY_RIGHT_ARM, "correct"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    loaded = load(path)

    assert len(loaded) == 1
    assert loaded[0]["verdict"] == "correct"


def _arm_line(text: str, arm: str) -> str:
    """The first `_format_summary` header line for `arm` in `text` -- e.g.
    `"contract         scored    1/1 ..."`. Scoped to one arm's own line so
    a naive (non-deduplicating) loader's bug shows up as a value mismatch
    on THIS line, rather than an assertion that happens to also match a
    different arm's unrelated, unaffected line (see the regression note
    below).
    """
    prefix = f"{arm:16s}"
    return next(line for line in text.splitlines() if line.startswith(prefix))


def _arm_block(text: str, arm: str) -> str:
    """`_arm_line` plus the continuation lines belonging to it -- failures,
    ungraded, db_corrupted. `_arm_line` alone matches only the header, so an
    assertion about the failure line silently searches the wrong string.
    """
    prefix = f"{arm:16s}"
    lines = text.splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith(prefix))
    end = start + 1
    while end < len(lines) and lines[end].startswith(" " * 16):
        end += 1
    return "\n".join(lines[start:end])


def test_report_scores_the_deduped_row_not_all_three_stale_attempts(tmp_path):
    # A retried unit: two stale construction_error rows, then a real
    # success, all for (task_id="t1", arm=contract, model=PRIMARY_MODEL).
    #
    # SCORED accuracy can't catch a dedup regression on its own here --
    # construction_error is a harness verdict, so it never enters the
    # SCORED numerator or denominator whether or not it's deduplicated;
    # SCORED reads 1/1 either way. STRICT accuracy is the one that moves:
    # deduplicated, this key contributes exactly one row (the final
    # "correct") to STRICT's denominator, so the contract arm's fraction is
    # 1/1 (100%) here, NOT the 1/3 (33%) a naive line-by-line reader would
    # produce by counting all three raw rows against a STRICT denominator.
    #
    # The assertions below are scoped to the CONTRACT arm's own line
    # specifically (not just "somewhere in the text") because the OTHER
    # arm (manual_prompt) has exactly one row and would independently
    # print "strict    1/1" regardless of whether dedup works at all -- an
    # unscoped `"strict    1/1" in text` check would still pass by
    # matching that unrelated line even with dedup completely broken. This
    # was verified directly: monkeypatching `dce.stats.load` to a naive,
    # non-deduplicating reader makes the CONTRACT line read
    # "strict    1/3" and fails the assertions below, exactly as intended.
    rows = [
        _row("t1", PRIMARY_LEFT_ARM, "correct"),
        _row("t1", PRIMARY_RIGHT_ARM, "construction_error"),
        _row("t1", PRIMARY_RIGHT_ARM, "construction_error"),
        _row("t1", PRIMARY_RIGHT_ARM, "correct"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    contract_line = _arm_line(text, PRIMARY_RIGHT_ARM)
    assert "scored    1/1" in contract_line
    assert "strict    1/1" in contract_line
    assert "strict    1/3" not in text


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
        _row("t1", ARM_A, "correct"),
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


def test_report_flags_harness_limited_by_failure_rate_even_at_zero_accuracy(tmp_path):
    # contract arm: 5 hit_limit (harness) + 1 correct out of 6 rows -- an
    # 83% harness-failure RATE. This must be flagged directly off the rate,
    # not off the SCORED/STRICT point-estimate gap: a gap-based flag is
    # algebraically scored_accuracy * failure_rate, so an arm that is
    # MOSTLY broken but scores 0% on the little it did answer would
    # produce a near-zero gap and slip through unflagged -- exactly the
    # arm most likely to be unreliable. Covered directly below.
    rows = [_row(f"t{i}", PRIMARY_RIGHT_ARM, "hit_limit") for i in range(5)]
    rows.append(_row("t5", PRIMARY_RIGHT_ARM, "correct"))
    rows.append(_row("t5", PRIMARY_LEFT_ARM, "correct"))
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    assert "HARNESS-LIMITED" in text
    assert "FINDINGS.md must say so" in text


def test_report_flags_an_arm_with_zero_scored_accuracy_and_high_failure_rate(tmp_path):
    # The case a gap-based (product) flag misses entirely: 9 hit_limit
    # (harness) rows and 1 wrong answer, zero correct. SCORED accuracy is
    # 0/1 = 0% and STRICT accuracy is 0/10 = 0%, so the OLD |scored -
    # strict| gap is 0 -- "not flagged" under the product rule despite a
    # 90% harness-failure rate. The rate-based rule must still catch it.
    rows = [_row(f"t{i}", PRIMARY_RIGHT_ARM, "hit_limit") for i in range(9)]
    rows.append(_row("t9", PRIMARY_RIGHT_ARM, "incorrect"))
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    contract_line = _arm_line(text, PRIMARY_RIGHT_ARM)
    assert "HARNESS-LIMITED" in contract_line


def test_report_flags_two_arms_with_identical_failure_rates_the_same_way(tmp_path):
    # contract: 2 construction_error / 20 rows. schema_only: 2 hit_limit /
    # 20 rows. Same 10% failure rate, different verdict types and
    # different accuracy on the rest -- both must get the same flag,
    # because the rate alone (not the rate multiplied by accuracy) decides
    # it.
    contract_rows = [
        _row(f"c{i}", PRIMARY_RIGHT_ARM, "construction_error") for i in range(2)
    ]
    contract_rows += [_row(f"c{i}", PRIMARY_RIGHT_ARM, "correct") for i in range(2, 20)]
    schema_rows = [_row(f"s{i}", ARM_A, "hit_limit") for i in range(2)]
    schema_rows += [_row(f"s{i}", ARM_A, "incorrect") for i in range(2, 20)]
    path = tmp_path / "results.jsonl"
    _write(path, contract_rows + schema_rows)

    text = report(path)

    contract_line = _arm_line(text, PRIMARY_RIGHT_ARM)
    schema_line = _arm_line(text, ARM_A)
    assert "HARNESS-LIMITED" in contract_line
    assert "HARNESS-LIMITED" in schema_line


def test_report_surfaces_db_corrupted_true_and_none_separately(tmp_path):
    # db_corrupted: True is the experiment's headline governance finding
    # (an ungoverned arm mutated the warehouse); db_corrupted: None means
    # the integrity check itself was untrustworthy -- a materially
    # different and more serious claim than "clean". Both must be counted,
    # not silently defaulted to "not corrupted".
    rows = [_row("t1", ARM_A, "correct"), _row("t2", ARM_A, "incorrect")]
    rows[0]["db_corrupted"] = True
    rows[1]["db_corrupted"] = None
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    schema_line_block = text[text.index(f"{ARM_A:16s}") :]
    assert "db_corrupted: true=1 unknown=1" in schema_line_block


def test_report_older_row_shape_without_db_corrupted_does_not_raise(tmp_path):
    # An older row shape, missing the field entirely, must degrade to the
    # same "unknown" bucket as an explicit None -- not raise mid-report.
    row = _row("t1", ARM_A, "correct")
    assert "db_corrupted" not in row
    path = tmp_path / "results.jsonl"
    _write(path, [row])

    text = report(path)  # must not raise

    schema_line_block = text[text.index(f"{ARM_A:16s}") :]
    assert "db_corrupted: true=0 unknown=1" in schema_line_block


def test_report_prints_both_final_and_total_billed_cost_never_usd_guard(tmp_path):
    # t1: a $0.30 error retried into a $0.01 success -- two raw rows for
    # the same (task_id, arm, model) key, only the second survives dedup.
    # usd_final (the deduplicated/final attempt) must read $0.01; the real
    # total actually billed across both attempts must still read $0.31 --
    # dropping the superseded $0.30 would understate real spend exactly
    # the way `dce.runner.real_spent_so_far` is designed not to.
    error_row = _row("t1", PRIMARY_LEFT_ARM, "error", usd=0.30)
    success_row = _row("t1", PRIMARY_LEFT_ARM, "correct", usd=0.01)
    other = _row("t1", PRIMARY_RIGHT_ARM, "correct", usd=0.05)
    error_row["usd_guard"] = 99.0
    success_row["usd_guard"] = 99.0
    other["usd_guard"] = 99.0
    path = tmp_path / "results.jsonl"
    _write(path, [error_row, success_row, other])

    text = report(path)

    left_line = _arm_line(text, PRIMARY_LEFT_ARM)
    assert "final=$0.01" in left_line
    assert "billed=$0.31" in left_line
    assert "$99.00" not in text


# ── per-slice instrumentation ────────────────────────────────────────────


def _arm_block(text: str, arm: str) -> str:
    """One arm's whole `_format_summary` block -- the header line plus the
    failure, db_corrupted and instrumentation lines that hang off it, which
    are indented continuations rather than lines of their own."""
    lines = text.splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith(f"{arm:16s}"))
    end = start + 1
    while end < len(lines) and lines[end].startswith(" " * 16):
        end += 1
    return "\n".join(lines[start:end])


def test_report_prints_token_and_turn_means_per_slice(tmp_path):
    rows = [
        _row("t1", PRIMARY_LEFT_ARM, "correct")
        | {"input_tokens": 1000, "output_tokens": 100, "cached_tokens": 0, "turns": 3},
        _row("t2", PRIMARY_LEFT_ARM, "correct")
        | {"input_tokens": 3000, "output_tokens": 300, "cached_tokens": 0, "turns": 5},
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    assert "in=2000" in text
    assert "out=200" in text
    assert "turns/row 4.0" in text


def test_report_surfaces_cached_tokens_so_the_caching_gate_can_be_read(tmp_path):
    """The smoke-run gate: if OpenRouter prompt-caches the 22k-char
    manual-in-prompt arm and nothing else, the cost comparison measures the
    provider's caching policy rather than the arms -- and `dce.pricing`
    bills cache reads at the full input rate, so it is invisible in the
    dollar figures. This is the only place it shows."""
    rows = [
        _row("t1", PRIMARY_LEFT_ARM, "correct") | {"cached_tokens": 8000},
        _row("t1", PRIMARY_RIGHT_ARM, "correct") | {"cached_tokens": 0},
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    assert "cached total 8,000" in _arm_block(text, PRIMARY_LEFT_ARM)
    assert "cached total 0" in _arm_block(text, PRIMARY_RIGHT_ARM)


def test_report_prints_governed_tool_counters_for_arm_c_only(tmp_path):
    counters = {
        "inspect_rejections": 4,
        "enforcement_blocks": 2,
        "retry_prompts": 7,
    }
    rows = [
        _row("t1", PRIMARY_RIGHT_ARM, "correct") | counters,
        _row("t1", PRIMARY_LEFT_ARM, "correct") | counters,
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    assert "enforcement_blocks=2" in text
    assert text.count("governed-tool counters") == 2  # PRIMARY + SECONDARY
    assert "descriptive instrumentation, not a governance claim" in text
    # Arm B has no contract to enforce; printing zeros there would dress a
    # structural absence up as a measurement.
    assert "enforcement_blocks" not in _arm_block(text, PRIMARY_LEFT_ARM)


def test_instrumentation_does_not_raise_on_an_older_row_without_the_fields(
    tmp_path,
):
    path = tmp_path / "results.jsonl"
    _write(path, [_row("t1", PRIMARY_RIGHT_ARM, "correct")])
    assert "turns/row 0.0" in report(path)


# ── unequal task sets across arms ────────────────────────────────────────


def test_report_warns_when_the_arms_did_not_see_the_same_tasks(tmp_path):
    """A sweep that stops mid task-group (circuit breaker, leaked
    connection, truncated budget) leaves arms with different denominators.
    McNemar drops the unshared tasks silently; nothing else says a word."""
    rows = [
        _row("t1", PRIMARY_LEFT_ARM, "correct"),
        _row("t1", PRIMARY_RIGHT_ARM, "correct"),
        _row("t2", PRIMARY_LEFT_ARM, "correct"),  # arm C never ran t2
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    assert "did NOT see the same task set" in text
    assert "1 tasks common to all" in text


def test_report_does_not_warn_when_every_arm_saw_every_task(tmp_path):
    rows = [
        _row("t1", PRIMARY_LEFT_ARM, "correct"),
        _row("t1", PRIMARY_RIGHT_ARM, "incorrect"),
        _row("t2", PRIMARY_LEFT_ARM, "incorrect"),
        _row("t2", PRIMARY_RIGHT_ARM, "correct"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    assert "did NOT see the same task set" not in report(path)


def test_report_does_not_raise_on_a_row_with_an_explicit_null_arm(tmp_path):
    """`_unequal_task_set_warning` used to build its arm set with
    `sorted({row.get("arm", "unknown") ...})` -- `.get(key, default)` only
    substitutes the default when the KEY is absent, not when its value is
    explicitly `None`, so a row reading `"arm": null` (a hand-edited or
    malformed row, not merely an old shape missing the field) put `None`
    into a set of strings and `sorted()` raised `TypeError` comparing
    `None` to `str`, killing `report()` entirely. A row's arm may be
    explicitly `None`; `report()` must tolerate it, same as it tolerates
    any other malformed field elsewhere in this file."""
    path = tmp_path / "results.jsonl"
    rows = [
        _row("t1", PRIMARY_LEFT_ARM, "correct"),
        _row("t1", PRIMARY_RIGHT_ARM, "correct"),
        {**_row("t2", PRIMARY_LEFT_ARM, "correct"), "arm": None},
    ]
    _write(path, rows)

    report(path)  # must not raise TypeError


# ── scorer staleness and verified-wrong golds ───────────────────────────────


def _graded_row(task_id, arm, answer, gold, verdict, scorer, **extra):
    return {
        "task_id": task_id,
        "arm": arm,
        "model": "z-ai/glm-5.3-flash",
        "level": "hard",
        "answer": answer,
        "gold": gold,
        "verdict": verdict,
        "scorer": scorer,
        "usd": 0.001,
        "usd_guard": 0.001,
        **extra,
    }


def test_stale_scorer_rows_are_detected():
    """Silence would be a claim that the file's verdicts came from the grading
    rules in force now."""
    from dce.stats import stale_scorer_rows

    rows = [
        _graded_row("1", "contract", "yes", "yes", "correct", "fallback"),
        _graded_row("2", "contract", "yes", "yes", "correct", "official-vendored"),
    ]
    assert stale_scorer_rows(rows) == {"fallback": 1}


def test_rescore_replays_a_scorer_change_from_stored_answers():
    """The exact case that moved a headline: `Yes.` against gold `yes` was
    graded wrong by the old fallback and right by DABStep's own scorer."""
    from dce.stats import rescore

    rows = [_graded_row("30", "schema_only", "Yes.", "yes", "incorrect", "fallback")]
    out = rescore(rows)
    assert out[0]["verdict"] == "correct"
    assert out[0]["scorer"] != "fallback"
    # Non-destructive: the caller's row is untouched.
    assert rows[0]["verdict"] == "incorrect"


def test_rescore_leaves_harness_failures_alone():
    """A `hit_limit` row has no answer to re-grade; inventing one would turn a
    harness failure into a wrong answer and quietly inflate the denominator."""
    from dce.stats import rescore

    rows = [_graded_row("9", "contract", "", "12.91", "hit_limit", "fallback")]
    assert rescore(rows)[0]["verdict"] == "hit_limit"


def test_report_drops_verified_wrong_gold_tasks(tmp_path):
    """They cannot be answered correctly by construction, so counting them
    depresses every arm and penalises hardest the arms most likely to reach the
    true value."""
    import json

    from dce.stats import report

    path = tmp_path / "r.jsonl"
    rows = [
        _graded_row("60", "contract", "PT", "ES", "incorrect", "official-vendored"),
        _graded_row(
            "1712", "contract", "12.91", "12.91", "correct", "official-vendored"
        ),
    ]
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    out = report(path)
    assert "verified-wrong gold" in out
    assert "60" in out.split("\n")[0]
    # The surviving task is the only one counted.
    assert "1/1" in out


# ── ungraded rows: present in the file, absent from every denominator ─────


def test_ungraded_rows_move_neither_accuracy(tmp_path):
    """A task with no reconstructed gold is answered but not scored. It is
    not a wrong answer, so it belongs in no numerator and no denominator —
    including STRICT's, which does count harness failures against an arm.
    """
    rows = [
        _row("t1", PRIMARY_RIGHT_ARM, "correct"),
        _row("t2", PRIMARY_RIGHT_ARM, "ungraded"),
        _row("t3", PRIMARY_RIGHT_ARM, "ungraded"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    line = _arm_line(report(path), PRIMARY_RIGHT_ARM)

    assert "scored    1/1" in line
    assert "strict    1/1" in line


def test_ungraded_rows_are_not_counted_as_harness_failures(tmp_path):
    """`ungraded` is a deliberate non-scoring, not the harness breaking. If
    it landed in the failure numerator every submission run would look
    HARNESS-LIMITED; if it landed only in the denominator it would dilute a
    real failure rate and hide one.
    """
    rows = [_row(f"t{i}", PRIMARY_RIGHT_ARM, "correct") for i in range(9)]
    rows.append(_row("t9", PRIMARY_RIGHT_ARM, "error"))
    rows += [_row(f"u{i}", PRIMARY_RIGHT_ARM, "ungraded") for i in range(90)]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    block = _arm_block(report(path), PRIMARY_RIGHT_ARM)

    # 1 error in 10 graded-or-failed rows: 10%, not 1% of 100.
    assert "failures (10% of rows)" in block
    assert "HARNESS-LIMITED" in block


def test_report_names_the_ungraded_rows_it_set_aside(tmp_path):
    """Dropping rows silently is a claim that they did not exist. The count
    is printed so a reader can see the submission run's 49 unscoreable tasks
    were excluded on purpose rather than lost.
    """
    rows = [
        _row("t1", PRIMARY_RIGHT_ARM, "correct"),
        _row("t2", PRIMARY_RIGHT_ARM, "ungraded"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    assert "ungolded: 1 row(s)" in _arm_block(report(path), PRIMARY_RIGHT_ARM)


def test_report_says_nothing_about_ungraded_rows_when_there_are_none(tmp_path):
    """Every published run has zero. The line must not appear and clutter
    the four runs already in FINDINGS.
    """
    rows = [_row("t1", PRIMARY_RIGHT_ARM, "correct")]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    assert "ungolded" not in report(path)


def test_ungraded_rows_keep_their_cost_and_corruption(tmp_path):
    """`_summarize` drops ungraded rows from the ACCURACY arithmetic only.
    They are real runs: they were billed, and their working copy really was
    checked. Dropping them from `usd_final` makes the cost line disagree with
    `billed` for a reason the module docstring assigns to superseded
    duplicates, and dropping them from `db_corrupted` silently deletes the
    experiment's headline governance finding on 49 of 450 tasks.
    """
    rows = [
        {**_row("t1", PRIMARY_RIGHT_ARM, "correct", usd=0.01), "db_corrupted": False},
        {**_row("t2", PRIMARY_RIGHT_ARM, "ungraded", usd=0.05), "db_corrupted": True},
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    block = _arm_block(report(path), PRIMARY_RIGHT_ARM)

    assert "db_corrupted: true=1" in block
    assert "final=$0.06" in block


def test_per_level_strict_accuracy_excludes_ungraded_rows(tmp_path):
    """The level breakdown reads `arm_rows` directly rather than going
    through `_summarize`, so it is a second denominator that has to exclude
    them — otherwise the header prints `strict 1/1` and the level line
    beneath it prints `strict 1/3` off the same rows.
    """
    rows = [
        _row("t1", PRIMARY_RIGHT_ARM, "correct", level="easy"),
        _row("t2", PRIMARY_RIGHT_ARM, "ungraded", level="easy"),
        _row("t3", PRIMARY_RIGHT_ARM, "ungraded", level="easy"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    assert "strict   1/1" in text
    assert "strict   1/3" not in text


def test_strict_mcnemar_does_not_pair_ungraded_tasks(tmp_path):
    """STRICT pairing counts a harness failure as wrong — a real attempt that
    produced no right answer. An ungraded task is not that: neither arm could
    be graded on it, so pairing them inflates `n_paired` with tasks the test
    says nothing about.
    """
    rows = [
        _row("t1", PRIMARY_LEFT_ARM, "correct"),
        _row("t1", PRIMARY_RIGHT_ARM, "correct"),
        _row("t2", PRIMARY_LEFT_ARM, "ungraded"),
        _row("t2", PRIMARY_RIGHT_ARM, "ungraded"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    assert "n_paired=2" not in text
    assert "n_paired=1" in text


def _ungolded(task_id: str, arm: str, verdict: str, **kw) -> dict:
    """A row for a task admitted by `--ungolded run`: no gold exists."""
    return {**_row(task_id, arm, verdict, **kw), "gold": None}


def test_an_ungolded_task_that_fails_the_harness_stays_out_of_the_arithmetic(
    tmp_path,
):
    """An ungolded task only reaches `verdict: "ungraded"` if `run_task`
    finishes cleanly. Trip a cap or error and the row keeps `hit_limit` /
    `error` with `gold: null` — so a cut made on VERDICT lets it through into
    `strict_n` and `failure_rate`, and `--ungolded run` moves numbers the
    README promises it cannot. The cut has to be "this task has no gold".
    """
    rows = [
        {**_row("t1", PRIMARY_RIGHT_ARM, "correct"), "gold": "a"},
        {**_row("t2", PRIMARY_RIGHT_ARM, "correct"), "gold": "a"},
        _ungolded("t3", PRIMARY_RIGHT_ARM, "hit_limit"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)
    block = _arm_block(text, PRIMARY_RIGHT_ARM)

    assert "strict    2/2" in block
    assert "failures (0% of rows)" in block
    assert "HARNESS-LIMITED" not in block
    assert "strict   2/3" not in text


def test_report_counts_the_harness_failures_among_ungolded_rows(tmp_path):
    """Excluding ungolded rows from the failure rate must not HIDE a cap trip
    on 49 tasks. They leave the rate — a different population — and are
    reported on their own.
    """
    rows = [
        {**_row("t1", PRIMARY_RIGHT_ARM, "correct"), "gold": "a"},
        _ungolded("t2", PRIMARY_RIGHT_ARM, "ungraded"),
        _ungolded("t3", PRIMARY_RIGHT_ARM, "hit_limit"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    block = _arm_block(report(path), PRIMARY_RIGHT_ARM)

    assert "ungolded: 2 row(s)" in block
    assert "1 harness failure(s)" in block


def test_the_ungolded_count_is_not_printed_inside_the_failures_line(tmp_path):
    """`failures (0% of rows): none, ungraded=49` reads as though 49 rows
    were failures inside a line that just said 0%. It gets its own line.
    """
    rows = [
        {**_row("t1", PRIMARY_RIGHT_ARM, "correct"), "gold": "a"},
        _ungolded("t2", PRIMARY_RIGHT_ARM, "ungraded"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    failures_line = next(
        line
        for line in _arm_block(report(path), PRIMARY_RIGHT_ARM).splitlines()
        if "failures (" in line
    )
    assert "ungolded" not in failures_line
    assert "ungraded" not in failures_line


def test_stale_scorer_warning_ignores_rows_that_were_never_graded(tmp_path):
    """`ungraded` rows carry `scorer` like any other, but `rescore` skips
    them. Counting them inflates the stale-scorer warning and makes its
    claim — that every answered row has been re-graded — false for them.
    """
    rows = [
        {**_row("t1", PRIMARY_RIGHT_ARM, "correct"), "gold": "a", "scorer": "fallback"},
        {**_ungolded("t2", PRIMARY_RIGHT_ARM, "ungraded"), "scorer": "fallback"},
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    assert "1 row(s)" in text
    assert "2 row(s)" not in text


def test_strict_mcnemar_drops_an_ungolded_task_that_failed_the_harness(tmp_path):
    """`_strict_bools` made the verdict-keyed cut `_is_ungolded` exists to
    replace. An ungolded task that trips a cap on one arm and errors on the
    other keeps its harness verdicts, so a verdict cut pairs it — and the
    report then prints `strict 1/1` per arm above an `n_paired=2` line.
    """
    rows = [
        {**_row("t1", PRIMARY_LEFT_ARM, "correct"), "gold": "a"},
        {**_row("t1", PRIMARY_RIGHT_ARM, "correct"), "gold": "a"},
        _ungolded("u1", PRIMARY_LEFT_ARM, "hit_limit"),
        _ungolded("u1", PRIMARY_RIGHT_ARM, "error"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    text = report(path)

    assert "n_paired=2" not in text
    assert "n_paired=1" in text


def test_an_ungolded_only_arm_does_not_trip_the_unequal_task_set_warning(tmp_path):
    """`--ungolded run --arms contract` resumed into an existing multi-arm
    results file is the only way to run the 49 without re-paying for the 401.
    It leaves the contract arm holding rows no other arm has — which is
    exactly what this warning looks for, and here it is not a lopsided sweep.
    Firing tells the operator to pay to run 49 unscoreable tasks on three
    more arms, while every denominator above the warning is already correct.
    """
    rows = [
        {**_row(f"t{i}", arm, "correct"), "gold": "a"}
        for i in range(3)
        for arm in (PRIMARY_LEFT_ARM, PRIMARY_RIGHT_ARM)
    ] + [
        _ungolded("u1", PRIMARY_RIGHT_ARM, "ungraded"),
        _ungolded("u2", PRIMARY_RIGHT_ARM, "ungraded"),
    ]
    path = tmp_path / "results.jsonl"
    _write(path, rows)

    assert "did NOT see the same task set" not in report(path)
