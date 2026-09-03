import contextlib
import json
import os
from decimal import Decimal
from pathlib import Path

import dce.runner as runner_module
import dce.stats as stats_module
import pytest
from dce.agent import (
    WORST_CASE_TOKEN_BUDGET_USD,
    AgentConstructionError,
    _token_budget_usd,
    build_result_row,
)
from dce.data import DATASET_REVISION
from dce.golds import PLURALITY_THRESHOLD, golds_sha256
from dce.lockfile import SweepLockedError, lock_path_for, sweep_lock
from dce.runner import (
    CIRCUIT_BREAKER_THRESHOLD,
    MAX_CONSTRUCTION_ATTEMPTS,
    SNAPSHOT_EVERY_ROWS,
    SweepResult,
    _construction_error_row,
    _exit_code_for,
    _load_golds,
    _next_reserve,
    _pessimistic_usd,
    _priced_or_pessimistic,
    _safe_json_dumps,
    _seed_observed_by_model,
    _stratified_sample,
    _validated_usd,
    _working_db_path,
    _worst_case_task_group_usd,
    assert_clean_tree,
    completed_keys,
    gave_up_keys,
    latest_rows,
    pending,
    real_spent_so_far,
    snapshot_path_for,
    spent_so_far,
    sweep,
)

TASKS = [{"task_id": "1", "question": "q", "guidelines": "g", "level": "hard"}]

# A real pinned model id is required everywhere a test goes through `sweep`:
# `_next_reserve`'s floor comes from `dce.agent._token_budget_usd(model)`,
# which does a bare `MODELS[model]` lookup with no fallback for an
# unrecognized id.
GLM = "z-ai/glm-5.3-flash"
DEEPSEEK_FLASH = "deepseek/deepseek-v4-flash-0731"


def _make_pristine(tmp_path: Path, name: str = "d") -> Path:
    """A stand-in pristine "database" file. `check_and_restore` only ever
    compares bytes/digests, so a real DuckDB file is not required to
    exercise the runner's working-copy plumbing."""
    path = tmp_path / name
    path.write_bytes(b"pristine-bytes")
    return path


# ── completed_keys ──────────────────────────────────────────────────────


def test_completed_keys_reads_existing_rows(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(
        json.dumps({"task_id": "1", "arm": "contract", "model": "m"}) + "\n"
    )
    assert completed_keys(path) == {("1", "contract", "m")}


def test_completed_keys_on_missing_file_is_empty(tmp_path: Path):
    assert completed_keys(tmp_path / "absent.jsonl") == set()


def test_completed_keys_retries_construction_error_rows_below_the_cap(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(
        json.dumps(
            {
                "task_id": "1",
                "arm": "contract",
                "model": "m",
                "verdict": "construction_error",
            }
        )
        + "\n"
    )
    assert MAX_CONSTRUCTION_ATTEMPTS > 1  # a single attempt must be below the cap
    assert completed_keys(path) == set()
    assert (
        completed_keys(path, retry_verdicts=("error",)) == set()
    )  # unaffected by the flag


def test_completed_keys_stops_retrying_construction_error_after_the_cap(
    tmp_path: Path,
):
    """'Cheap twice, then loud': once a key has accumulated
    `MAX_CONSTRUCTION_ATTEMPTS` `construction_error` rows, it must be
    treated as terminal (done) so a resume stops hammering a persistent
    problem forever."""
    path = tmp_path / "r.jsonl"
    path.write_text(
        "\n".join(
            json.dumps(
                {
                    "task_id": "1",
                    "arm": "contract",
                    "model": "m",
                    "verdict": "construction_error",
                }
            )
            for _ in range(MAX_CONSTRUCTION_ATTEMPTS)
        )
        + "\n"
    )
    assert completed_keys(path) == {("1", "contract", "m")}


def test_completed_keys_retries_error_rows_only_when_requested(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(
        json.dumps(
            {"task_id": "1", "arm": "contract", "model": "m", "verdict": "error"}
        )
        + "\n"
    )
    assert completed_keys(path) == {("1", "contract", "m")}
    assert completed_keys(path, retry_verdicts=("error",)) == set()


def test_completed_keys_treats_hit_limit_and_scoring_error_as_terminal(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(
        "\n".join(
            json.dumps(
                {"task_id": tid, "arm": "contract", "model": "m", "verdict": verdict}
            )
            for tid, verdict in [("1", "hit_limit"), ("2", "scoring_error")]
        )
        + "\n"
    )
    # Terminal regardless of --retry error: that flag is scoped to "error" only.
    assert completed_keys(path, retry_verdicts=("error",)) == {
        ("1", "contract", "m"),
        ("2", "contract", "m"),
    }


# ── gave_up_keys ─────────────────────────────────────────────────────────


def test_gave_up_keys_is_empty_below_the_cap(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(
        json.dumps(
            {
                "task_id": "1",
                "arm": "contract",
                "model": "m",
                "verdict": "construction_error",
            }
        )
        + "\n"
    )
    assert gave_up_keys(path) == set()


def test_gave_up_keys_reports_units_that_exhausted_the_cap(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(
        "\n".join(
            json.dumps(
                {
                    "task_id": "1",
                    "arm": "contract",
                    "model": "m",
                    "verdict": "construction_error",
                }
            )
            for _ in range(MAX_CONSTRUCTION_ATTEMPTS)
        )
        + "\n"
    )
    assert gave_up_keys(path) == {("1", "contract", "m")}


# ── latest_rows ──────────────────────────────────────────────────────────


def test_latest_rows_keeps_only_the_last_row_per_key(tmp_path: Path):
    """Three failing resumes then a success must dedupe to ONE row for that
    key, not four — a raw iteration would count the stale
    `construction_error` rows as wrong answers alongside the real success."""
    path = tmp_path / "r.jsonl"
    rows = [
        {
            "task_id": "1",
            "arm": "contract",
            "model": "m",
            "verdict": "construction_error",
        },
        {
            "task_id": "1",
            "arm": "contract",
            "model": "m",
            "verdict": "construction_error",
        },
        {"task_id": "1", "arm": "contract", "model": "m", "verdict": "error"},
        {"task_id": "1", "arm": "contract", "model": "m", "verdict": "correct"},
        {"task_id": "2", "arm": "contract", "model": "m", "verdict": "incorrect"},
    ]
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    deduped = latest_rows(path)
    assert len(deduped) == 2  # one per key, not five
    by_key = {(r["task_id"], r["arm"], r["model"]): r for r in deduped}
    assert by_key[("1", "contract", "m")]["verdict"] == "correct"
    assert by_key[("2", "contract", "m")]["verdict"] == "incorrect"


def test_latest_rows_on_missing_file_is_empty(tmp_path: Path):
    assert latest_rows(tmp_path / "absent.jsonl") == []


# ── spent_so_far ─────────────────────────────────────────────────────────


def test_spent_so_far_sums_usd_across_existing_rows(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(
        "\n".join(json.dumps({"usd": usd}) for usd in [0.10, 0.25, 0.05]) + "\n"
    )
    assert spent_so_far(path) == pytest.approx(0.40)


def test_spent_so_far_on_missing_file_is_zero(tmp_path: Path):
    assert spent_so_far(tmp_path / "absent.jsonl") == 0.0


def test_spent_so_far_tolerates_missing_or_null_usd(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(
        "\n".join(
            json.dumps(row)
            for row in [{"usd": 0.10}, {"usd": None}, {"no_usd_field": True}]
        )
        + "\n"
    )
    assert spent_so_far(path) == 0.10


# ── pending ──────────────────────────────────────────────────────────────


def test_pending_skips_completed_work():
    done = {("1", "schema_only", "m")}
    todo = pending(TASKS, ("schema_only", "contract"), ("m",), done)
    assert todo == [("1", "contract", "m")]


def test_pending_groups_consecutive_triples_by_task_id():
    tasks = [{"task_id": str(i)} for i in range(5)]
    todo = pending(tasks, ("schema_only", "contract"), ("m1", "m2"), set())
    task_id_sequence = [t[0] for t in todo]
    # Every task_id's triples must be contiguous: task-major order, not
    # arm-major — collapsing consecutive repeats must reproduce every
    # task_id exactly once, in first-seen order.
    collapsed = []
    for tid in task_id_sequence:
        if not collapsed or collapsed[-1] != tid:
            collapsed.append(tid)
    assert collapsed == [str(i) for i in range(5)]
    assert len(collapsed) == len(set(task_id_sequence))


# ── _next_reserve ────────────────────────────────────────────────────────


def test_next_reserve_uses_floor_before_any_observation():
    assert _next_reserve([], floor=0.30) == 0.30


def test_next_reserve_uses_max_not_mean_of_observed():
    # A mean of [0.10, 0.90] would be 0.50 and under-reserve for the model's
    # actual worst observed call; the max must be used instead.
    assert _next_reserve([0.10, 0.90], floor=0.05) == 0.90


def test_next_reserve_never_reads_zero_as_free():
    assert _next_reserve([0.0, 0.0], floor=0.0) > 0.0


def test_next_reserve_does_not_let_a_cheap_observation_undercut_the_floor():
    # I1a: a cheap early call ($0.02) must not collapse the reservation
    # below the real per-model ceiling ($0.183) for a later, possibly
    # worst-case call — measured without this: an 82% budget overrun.
    assert _next_reserve([0.02], floor=0.183) == 0.183


def test_next_reserve_still_grows_past_the_floor_if_actually_observed():
    assert _next_reserve([0.02, 5.00], floor=0.183) == 5.00


# ── _seed_observed_by_model ──────────────────────────────────────────────


def test_seed_observed_by_model_only_counts_billable_rows(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(
        "\n".join(
            json.dumps(row)
            for row in [
                {"model": "m1", "usd": 0.40},
                {"model": "m1", "usd": 0.0},  # hit_limit/error: not billable
                {"model": "m2", "usd": None},
            ]
        )
        + "\n"
    )
    assert _seed_observed_by_model(path) == {"m1": [0.40]}


# ── _worst_case_task_group_usd ───────────────────────────────────────────


def test_worst_case_task_group_usd_sums_across_arms_and_models():
    # I1b: the real reservation `sweep` checks is per TASK GROUP, not per
    # call — every arm x every model's ceiling, summed. Three arms against
    # one model must be 3x that model's own per-call ceiling, not the
    # per-call figure alone (measured: printing the per-call figure
    # overstated admissible tasks by exactly that factor).
    expected = 3 * _token_budget_usd(GLM)
    assert _worst_case_task_group_usd(("a", "b", "c"), (GLM,)) == pytest.approx(
        expected
    )


def test_worst_case_task_group_usd_sums_across_multiple_models_too():
    expected = 2 * (_token_budget_usd(GLM) + _token_budget_usd(DEEPSEEK_FLASH))
    assert _worst_case_task_group_usd(
        ("a", "b"), (GLM, DEEPSEEK_FLASH)
    ) == pytest.approx(expected)


# ── sweep: the spend cap ─────────────────────────────────────────────────


#: A `--max-spend` sized off the live pessimistic reserve floor rather than a
#: hardcoded dollar figure: one full reserve, plus room for 1.5 calls at
#: `_CALL_USD`. Under `_next_reserve`'s rule that admits exactly two calls and
#: refuses the third, whatever `TOKEN_BUDGET` (and so the floor) happens to be
#: — F6 moved that floor 4.5x, which is precisely how a hardcoded figure here
#: turns a real property into an arithmetic coincidence.
_CALL_USD: float = 0.40


def _budget_for_exactly_two_calls(model: str) -> float:
    return _token_budget_usd(model) + 1.5 * _CALL_USD


def test_sweep_stops_before_exceeding_max_spend(tmp_path: Path):
    calls = []

    def fake_run(task, arm, model, *a, **k):
        calls.append(task["task_id"])
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.40,
            "usd_guard": 0.40,
            "verdict": "correct",
        }

    tasks = [
        {"task_id": str(i), "question": "q", "guidelines": "g", "level": "hard"}
        for i in range(10)
    ]
    golds = {str(i): "g" for i in range(10)}
    db_path = _make_pristine(tmp_path)
    max_spend = _budget_for_exactly_two_calls(GLM)
    result = sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        golds,
        out=tmp_path / "r.jsonl",
        db_path=db_path,
        docs={},
        max_spend=max_spend,
        golds_hash="h",
        run_task_fn=fake_run,
    )
    # Derived from the floor rather than hardcoded, so a change to
    # `TOKEN_BUDGET` (which moved this floor 4.5x at F6) cannot silently turn
    # this into an arithmetic coincidence: with a budget of one pessimistic
    # reserve plus 1.5 calls, exactly two $0.40 calls fit. The first reserves
    # the floor (nothing observed yet); each later one reserves the max
    # observed so far against that same floor. A third would project past the
    # cap, so the guard stops at two.
    assert len(calls) == 2
    assert result.spent <= max_spend
    assert result.truncated is True


def test_sweep_appends_rows_that_can_be_resumed(tmp_path: Path):
    out = tmp_path / "r.jsonl"

    def fake_run(task, arm, model, *a, **k):
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.01,
            "usd_guard": 0.01,
            "verdict": "correct",
        }

    db_path = _make_pristine(tmp_path)
    sweep(
        TASKS,
        ("contract",),
        (GLM,),
        {"1": "g"},
        out=out,
        db_path=db_path,
        docs={},
        max_spend=1.0,
        golds_hash="h",
        run_task_fn=fake_run,
    )
    assert completed_keys(out) == {("1", "contract", GLM)}


def test_sweep_seeds_spent_from_existing_rows_across_resumes(tmp_path: Path):
    """The Critical bug this test guards against: `sweep` used to start
    every invocation from `spent = 0.0`, so `--max-spend 1.00` bounded each
    *process*, not the experiment — four resumes banked $3.20. Simulating
    four separate invocations against the same results file must instead
    hold the cap across all of them."""
    calls = []

    def fake_run(task, arm, model, *a, **k):
        calls.append(task["task_id"])
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.40,
            "usd_guard": 0.40,
            "verdict": "correct",
        }

    tasks = [
        {"task_id": str(i), "question": "q", "guidelines": "g", "level": "hard"}
        for i in range(10)
    ]
    golds = {str(i): "g" for i in range(10)}
    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    max_spend = _budget_for_exactly_two_calls(GLM)

    for _ in range(4):  # four separate "process" invocations
        sweep(
            tasks,
            ("schema_only",),
            (GLM,),
            golds,
            out=out,
            db_path=db_path,
            docs={},
            max_spend=max_spend,
            golds_hash="h",
            run_task_fn=fake_run,
        )

    assert spent_so_far(out) <= max_spend
    # Two, for the reason spelled out in
    # `test_sweep_stops_before_exceeding_max_spend`: the budget is sized off
    # the live floor, not a hardcoded figure. Only the FIRST invocation's two
    # calls ever ran — the point of this test is that the other three
    # invocations resumed into an already-exhausted budget.
    assert len(calls) == 2


def test_sweep_reserves_the_whole_task_group_before_starting_it(tmp_path: Path):
    """A task's (arm, model) group is atomic: either every combo in it runs
    or none does, so a truncation always lands on a task boundary. Two
    arms against one model reserve $0.183 x 2 = $0.366 up front; against a
    $0.30 cap that is over budget even though a single call's own floor
    ($0.183) would fit alone."""
    calls = []

    def fake_run(task, arm, model, *a, **k):
        calls.append(arm)
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.01,
            "usd_guard": 0.01,
            "verdict": "correct",
        }

    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    result = sweep(
        TASKS,
        ("schema_only", "contract"),
        (GLM,),
        {"1": "g"},
        out=out,
        db_path=db_path,
        docs={},
        max_spend=0.30,
        golds_hash="h",
        run_task_fn=fake_run,
    )
    assert calls == []
    assert result.spent == 0.0
    assert result.truncated is True


# ── sweep: working-copy orchestration ────────────────────────────────────


def test_sweep_never_opens_the_pristine_db(tmp_path: Path):
    """`run_task_fn` must receive the working copy, never `db_path` itself —
    the ungoverned arms can genuinely write, so nothing may point at the
    pristine file."""
    seen_db_paths = []

    def fake_run(task, arm, model, db_path, docs, gold, **k):
        seen_db_paths.append(db_path)
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.01,
            "usd_guard": 0.01,
            "verdict": "correct",
        }

    pristine = _make_pristine(tmp_path)
    pristine_bytes_before = pristine.read_bytes()
    sweep(
        TASKS,
        ("contract",),
        (GLM,),
        {"1": "g"},
        out=tmp_path / "r.jsonl",
        db_path=pristine,
        docs={},
        max_spend=1.0,
        golds_hash="h",
        run_task_fn=fake_run,
    )

    assert len(seen_db_paths) == 1
    working = seen_db_paths[0]
    assert working != pristine
    assert working == _working_db_path(pristine)
    assert working.exists()
    assert pristine.read_bytes() == pristine_bytes_before


def test_sweep_records_corruption_and_continues(tmp_path: Path):
    """An ungoverned arm mutating the warehouse is a governance finding to
    record, not a crash that loses the evidence."""

    def fake_run_that_corrupts(task, arm, model, db_path, docs, gold, **k):
        db_path.write_bytes(b"corrupted-by-the-arm")
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.01,
            "usd_guard": 0.01,
            "verdict": "correct",
        }

    tasks = [
        {"task_id": str(i), "question": "q", "guidelines": "g", "level": "hard"}
        for i in range(3)
    ]
    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    result = sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        {str(i): "g" for i in range(3)},
        out=out,
        db_path=db_path,
        docs={},
        max_spend=10.0,
        golds_hash="h",
        run_task_fn=fake_run_that_corrupts,
    )
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(rows) == 3
    assert all(row["db_corrupted"] is True for row in rows)
    assert result.spent == 0.03
    assert result.truncated is False


def test_sweep_guards_construction_failures_and_continues(tmp_path: Path):
    """A factory error (e.g. a missing OPENROUTER_API_KEY) must not escape
    with no row written and abort a partly-paid sweep."""
    calls = []

    def flaky_run(task, arm, model, *a, **k):
        calls.append(task["task_id"])
        if task["task_id"] == "0":
            raise AgentConstructionError("missing OPENROUTER_API_KEY")
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.01,
            "usd_guard": 0.01,
            "verdict": "correct",
        }

    tasks = [
        {"task_id": str(i), "question": "q", "guidelines": "g", "level": "hard"}
        for i in range(2)
    ]
    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    result = sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        {str(i): "g" for i in range(2)},
        out=out,
        db_path=db_path,
        docs={},
        max_spend=10.0,
        golds_hash="h",
        run_task_fn=flaky_run,
    )
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert calls == ["0", "1"]  # the second task still ran
    assert len(rows) == 2
    assert rows[0]["verdict"] == "construction_error"
    assert rows[0]["task_id"] == "0"
    assert rows[0]["level"] == "hard"  # schema-compat: Task 9's accuracy_by needs this
    assert "db_corrupted" in rows[0]
    # Real spend vs guard spend split: a genuine construction failure spent
    # $0.00 for real, but the CAP still charges the pessimistic ceiling —
    # see test_construction_error_row_has_build_result_row_shape.
    assert rows[0]["usd"] == 0.0
    assert rows[0]["usd_guard"] == _token_budget_usd(GLM)
    assert rows[1]["verdict"] == "correct"
    # The guard ledger (what the cap counts) includes the pessimistic
    # charge; the real ledger does not.
    assert result.spent == pytest.approx(_token_budget_usd(GLM) + 0.01)
    assert result.real_spent == pytest.approx(0.01)


def test_sweep_gives_up_after_max_construction_attempts_across_resumes(
    tmp_path: Path,
):
    """'Cheap twice, then loud': a persistently failing unit (e.g. an
    API key that is never fixed) must stop being retried after
    `MAX_CONSTRUCTION_ATTEMPTS` resumes, not hammer the same failure
    forever."""
    calls = []

    def always_fails(task, arm, model, *a, **k):
        calls.append(task["task_id"])
        raise AgentConstructionError("missing OPENROUTER_API_KEY")

    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)

    for _ in range(MAX_CONSTRUCTION_ATTEMPTS + 2):  # more resumes than the cap allows
        sweep(
            TASKS,
            ("contract",),
            (GLM,),
            {"1": "g"},
            out=out,
            db_path=db_path,
            docs={},
            max_spend=100.0,
            golds_hash="h",
            run_task_fn=always_fails,
        )

    assert len(calls) == MAX_CONSTRUCTION_ATTEMPTS
    assert gave_up_keys(out) == {("1", "contract", GLM)}
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(rows) == MAX_CONSTRUCTION_ATTEMPTS
    assert all(r["verdict"] == "construction_error" for r in rows)


def test_sweep_lets_a_real_bug_in_run_task_fn_propagate(tmp_path: Path):
    """Only `AgentConstructionError` is swallowed into a `construction_error`
    row — anything else (a signature mismatch, an unrelated `TypeError`,
    ...) is a real bug and must stop the sweep loudly rather than being
    silently treated as a free, retryable construction failure."""

    def buggy_run(task, arm, model, *a, **k):
        raise TypeError("run_task_fn() got an unexpected keyword argument")

    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    with pytest.raises(TypeError):
        sweep(
            TASKS,
            ("contract",),
            (GLM,),
            {"1": "g"},
            out=out,
            db_path=db_path,
            docs={},
            max_spend=1.0,
            golds_hash="h",
            run_task_fn=buggy_run,
        )
    assert out.read_text() == ""  # nothing written; the bug surfaced, not swallowed


def test_sweep_normalizes_and_writes_the_row_when_usd_is_missing(tmp_path: Path):
    """A1's fourth frame, fixed structurally: a row missing `usd` must
    still reach the one write site (normalized to the pessimistic ceiling,
    with a `pricing_error` note) rather than being lost the way a bare
    `row["usd"]` access used to lose it. The collected exception still
    propagates afterward -- a pricing bug is worth surfacing loudly -- but
    only once the row is safely on disk."""

    def unpriced_run(task, arm, model, *a, **k):
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "verdict": "correct",
        }  # no "usd" or "usd_guard" key at all

    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    with pytest.raises(ValueError):
        sweep(
            TASKS,
            ("contract",),
            (GLM,),
            {"1": "g"},
            out=out,
            db_path=db_path,
            docs={},
            max_spend=1.0,
            golds_hash="h",
            run_task_fn=unpriced_run,
        )
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(rows) == 1  # the row was NOT lost
    assert rows[0]["usd"] == _token_budget_usd(GLM)
    assert rows[0]["usd_guard"] == _token_budget_usd(GLM)
    assert "usd missing" in rows[0]["pricing_error"]
    assert "usd_guard missing" in rows[0]["pricing_error"]


def test_sweep_with_no_pending_work_skips_the_working_copy(tmp_path: Path):
    out = tmp_path / "r.jsonl"
    out.write_text(
        json.dumps({"task_id": "1", "arm": "contract", "model": GLM, "usd": 0.0}) + "\n"
    )
    db_path = _make_pristine(tmp_path)

    def fail_if_called(*a, **k):
        raise AssertionError("run_task_fn must not be called when nothing is pending")

    result = sweep(
        TASKS,
        ("contract",),
        (GLM,),
        {"1": "g"},
        out=out,
        db_path=db_path,
        docs={},
        max_spend=1.0,
        golds_hash="h",
        run_task_fn=fail_if_called,
    )
    assert result.truncated is False
    assert not _working_db_path(db_path).exists()


# ── construction_error row shape ─────────────────────────────────────────


def test_construction_error_row_has_build_result_row_shape():
    real_row = build_result_row(
        task=TASKS[0],
        arm="contract",
        model=GLM,
        answer="a",
        answer_normalized="a",
        gold="g",
        verdict="correct",
        forced_answer=False,
        trace_path=None,
        reasoning_tokens=0,
        in_tok=1,
        out_tok=1,
        cached_tok=0,
        turns=1,
        tool_calls=[],
        inspect_rejections=0,
        enforcement_blocks=0,
        retry_prompts=0,
        request_limit=1,
        token_cap=1,
        golds_hash="h",
    )
    error_row = _construction_error_row(
        TASKS[0],
        "contract",
        GLM,
        "g",
        "h",
        AgentConstructionError("missing OPENROUTER_API_KEY"),
    )
    assert set(real_row.keys()) <= set(error_row.keys())
    assert error_row["level"] == "hard"
    # Real vs guard split: `usd` (real) is $0.00 — a genuine construction
    # failure spent nothing; `usd_guard` (what the cap counts) is the
    # pessimistic ceiling — a deliberate backstop, not an accounting truth.
    assert error_row["usd"] == 0.0
    assert error_row["usd_guard"] == _token_budget_usd(GLM)


# ── stratified --n sampling ────────────────────────────────────────────


def test_stratified_sample_covers_every_level_in_proportion():
    tasks = [{"task_id": str(i), "level": "hard"} for i in range(8)] + [
        {"task_id": f"e{i}", "level": "easy"} for i in range(2)
    ]
    sampled = _stratified_sample(tasks, 5)
    levels = [t["level"] for t in sampled]
    assert levels.count("hard") == 4
    assert levels.count("easy") == 1
    assert len(sampled) == 5


def test_stratified_sample_returns_everything_when_n_is_not_smaller():
    tasks = [{"task_id": str(i), "level": "hard"} for i in range(3)]
    assert _stratified_sample(tasks, 10) == tasks
    assert _stratified_sample(tasks, 0) == tasks


# ── golds envelope ───────────────────────────────────────────────────────


def test_load_golds_returns_map_and_hash(tmp_path: Path):
    path = tmp_path / "golds.json"
    path.write_text(
        json.dumps(
            {
                "revision": DATASET_REVISION,
                "threshold": 0.75,
                "count": 1,
                "submissions_expected": 1,
                "submissions_consumed": 1,
                "manifest_sha256": "deadbeef",
                "golds": {"1": "42"},
            }
        )
    )
    golds, golds_hash = _load_golds(path)
    assert golds == {"1": "42"}
    # NOT `manifest_sha256` ("deadbeef"): that fingerprints the submission
    # CORPUS and is identical for two gold sets that differ in every
    # answer. `golds_hash` must fingerprint the golds themselves.
    assert golds_hash == golds_sha256({"1": "42"})
    assert golds_hash != "deadbeef"


def _envelope(mapping: dict[str, str], **overrides) -> str:
    envelope = {
        "revision": DATASET_REVISION,
        "threshold": PLURALITY_THRESHOLD,
        "count": len(mapping),
        "submissions_expected": 1,
        "submissions_consumed": 1,
        "manifest_sha256": "deadbeef",
        "golds_sha256": golds_sha256(mapping),
        "golds": mapping,
    }
    envelope.update(overrides)
    return json.dumps(envelope)


def test_two_gold_sets_from_one_corpus_get_different_hashes(tmp_path: Path):
    """The confound this closes: Ruling 8 requires re-running `prepare` at
    0.60/0.75/0.90, `data/golds.json` is gitignored, and the two envelopes
    carry an IDENTICAL `manifest_sha256` because they were reconstructed
    from the same submission corpus. Under the old code both sweeps stamped
    the same `golds_hash` into every row while being scored against
    different ground truth."""
    loose = tmp_path / "loose.json"
    tight = tmp_path / "tight.json"
    loose.write_text(_envelope({"1": "42", "2": "43"}))
    tight.write_text(_envelope({"1": "42"}))

    _, loose_hash = _load_golds(loose)
    _, tight_hash = _load_golds(tight)
    assert loose_hash != tight_hash


def test_load_golds_rejects_a_threshold_mismatch(tmp_path: Path):
    path = tmp_path / "golds.json"
    path.write_text(_envelope({"1": "42"}, threshold=0.60))
    with pytest.raises(SystemExit):
        _load_golds(path)


def test_load_golds_rejects_an_edited_envelope(tmp_path: Path):
    """Stored hash kept, answers changed by hand."""
    path = tmp_path / "golds.json"
    path.write_text(
        _envelope({"1": "42"}, golds={"1": "999"}),
    )
    with pytest.raises(SystemExit):
        _load_golds(path)


def test_load_golds_rejects_revision_mismatch(tmp_path: Path):
    path = tmp_path / "golds.json"
    path.write_text(
        json.dumps(
            {
                "revision": "some-other-revision",
                "threshold": 0.75,
                "count": 1,
                "submissions_expected": 1,
                "submissions_consumed": 1,
                "manifest_sha256": "deadbeef",
                "golds": {"1": "42"},
            }
        )
    )
    try:
        _load_golds(path)
        raise AssertionError("expected SystemExit")
    except SystemExit:
        pass


def test_load_golds_rejects_a_bare_mapping(tmp_path: Path):
    """A bare task->answer mapping (someone passed `envelope["golds"]`
    itself, not the envelope) must fail loudly with SystemExit, not with a
    confusing `KeyError('revision')` deep inside this function."""
    path = tmp_path / "golds.json"
    path.write_text(json.dumps({"1": "42", "2": "43"}))
    try:
        _load_golds(path)
        raise AssertionError("expected SystemExit")
    except KeyError:
        raise AssertionError("must raise SystemExit, not KeyError") from None
    except SystemExit:
        pass


# ── torn tail (ENOSPC / killed mid-write) ────────────────────────────────


def _row(task_id: str, **extra) -> str:
    row = {
        "task_id": task_id,
        "arm": "contract",
        "model": GLM,
        "verdict": "correct",
        "usd": 0.01,
        "usd_guard": 0.01,
    }
    row.update(extra)
    return json.dumps(row)


def test_a_torn_final_line_does_not_break_any_reader(tmp_path: Path):
    """A full disk (or a `SIGKILL` mid-append) leaves a truncated LAST
    line. It must cost that one row, not the entire accumulated results
    file: before this, all six readers raised `JSONDecodeError` and the
    paid sweep became unreadable to both resume and analysis until a human
    hand-truncated it."""
    path = tmp_path / "r.jsonl"
    path.write_text(_row("1") + "\n" + _row("2") + "\n" + '{"task_id": "3", "usd')

    assert spent_so_far(path) == pytest.approx(0.02)
    assert real_spent_so_far(path) == pytest.approx(0.02)
    assert completed_keys(path) == {("1", "contract", GLM), ("2", "contract", GLM)}
    assert {r["task_id"] for r in latest_rows(path)} == {"1", "2"}
    assert {r["task_id"] for r in stats_module.load(path)} == {"1", "2"}
    assert {r["task_id"] for r in stats_module._raw_rows(path)} == {"1", "2"}
    stats_module.report(path)  # must not raise


def test_write_side_repairs_a_torn_tail_before_two_resumes_append(
    tmp_path: Path,
):
    """The WRITE-side counterpart to the read-side forgiveness above.
    `sweep` used to open `out` with `out.open("a")` and append straight
    onto a torn tail, which merges the next paid row onto the torn bytes
    instead of starting a fresh line. Reproduced, before the fix, in
    exactly this three-step sequence:

      1. Torn tail present -> read forgives it, rows ['1', '2'].
      2. Resume 1 appends a real, paid row -> rows STILL ['1', '2'],
         `spent` unchanged at 0.02: the new row merged onto the torn line
         and reads back as torn again, so it is silently swallowed and its
         unit (task '3') would be re-attempted, and re-paid, forever.
      3. Resume 2 appends another row -> the still-corrupt merged line is
         no longer the file's LAST line, so every reader now raises
         `JSONDecodeError` -- `spent_so_far` and `dce.stats.report` both
         dead, the whole paid sweep bricked, not just one row.

    `_repair_torn_tail` truncates the torn tail before `sweep`'s first
    append, so the paid row survives and the file stays fully readable
    across both resumes.

    Each `sweep()` call below is deliberately given only as much of the
    task list as would be "pending" at that point of a real two-resume
    session (task '3' first, then task '4' newly added) -- `sweep` itself
    does not stop after one task once it has started; it keeps going
    through every pending unit until the budget or a circuit breaker says
    stop. Widening the task list between calls is what turns two `sweep()`
    calls into two genuinely separate resumes, each appending exactly one
    row, instead of one call quietly finishing all of the pending work."""
    out = tmp_path / "r.jsonl"
    out.write_text(_row("1") + "\n" + _row("2") + "\n" + '{"task_id": "3", "usd')

    def _tasks(*ids: str) -> list[dict]:
        return [
            {"task_id": tid, "question": "q", "guidelines": "g", "level": "hard"}
            for tid in ids
        ]

    db_path = _make_pristine(tmp_path)
    calls: list[str] = []

    def fake_run(task, arm, model, *a, **k):
        calls.append(task["task_id"])
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.02,
            "usd_guard": 0.02,
            "verdict": "correct",
        }

    # Resume 1: task "3" was never actually completed (its only row was
    # torn), so it is the next -- and only -- pending unit, and it must be
    # PAID and LAND, not silently eaten by the torn line.
    tasks = _tasks("1", "2", "3")
    golds = {t["task_id"]: "g" for t in tasks}
    sweep(
        tasks,
        ("contract",),
        (GLM,),
        golds,
        out=out,
        db_path=db_path,
        docs={},
        max_spend=10.0,
        golds_hash="h",
        run_task_fn=fake_run,
    )
    assert calls == ["3"]
    assert {r["task_id"] for r in latest_rows(out)} == {"1", "2", "3"}
    assert spent_so_far(out) == pytest.approx(0.04)  # 0.01 + 0.01 + 0.02
    stats_module.report(out)  # must not raise

    # Resume 2: task "4" is the next pending unit. This call must not
    # re-brick the file the way the original bug did by pushing the
    # still-corrupt merged line off the tail.
    tasks = _tasks("1", "2", "3", "4")
    golds = {t["task_id"]: "g" for t in tasks}
    sweep(
        tasks,
        ("contract",),
        (GLM,),
        golds,
        out=out,
        db_path=db_path,
        docs={},
        max_spend=10.0,
        golds_hash="h",
        run_task_fn=fake_run,
    )
    assert calls == ["3", "4"]
    assert {r["task_id"] for r in latest_rows(out)} == {"1", "2", "3", "4"}
    assert spent_so_far(out) == pytest.approx(0.06)
    stats_module.report(out)  # must not raise


def test_a_corrupt_line_mid_file_still_raises_loudly(tmp_path: Path):
    """Only the TAIL can be torn by an interrupted append. A bad line in
    the middle means something rewrote the file, which is not a truncation
    and must never be silently skipped past."""
    path = tmp_path / "r.jsonl"
    path.write_text(_row("1") + "\n" + "{not json at all" + "\n" + _row("3") + "\n")

    with pytest.raises(json.JSONDecodeError):
        spent_so_far(path)
    with pytest.raises(json.JSONDecodeError):
        latest_rows(path)
    with pytest.raises(json.JSONDecodeError):
        stats_module._raw_rows(path)


def test_spend_summers_skip_a_non_numeric_value(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(_row("1") + "\n" + _row("2", usd="1.20", usd_guard="1.20") + "\n")
    assert spent_so_far(path) == pytest.approx(0.01)
    assert real_spent_so_far(path) == pytest.approx(0.01)


# ── clean-tree guard ─────────────────────────────────────────────────────


def test_assert_clean_tree_raises_on_dirty_tree():
    try:
        assert_clean_tree(git_status_fn=lambda: " M dce/runner.py\n")
        raise AssertionError("expected SystemExit")
    except SystemExit:
        pass


def test_assert_clean_tree_passes_on_clean_tree():
    assert_clean_tree(git_status_fn=lambda: "")  # must not raise


def test_assert_clean_tree_ignores_only_the_exact_output_file(tmp_path: Path):
    out = tmp_path / "results" / "r.jsonl"
    porcelain = "?? results/r.jsonl\n"
    assert_clean_tree(out=out, repo_root=tmp_path, git_status_fn=lambda: porcelain)


def test_assert_clean_tree_still_fails_on_other_dirty_files(tmp_path: Path):
    out = tmp_path / "results" / "r.jsonl"
    porcelain = "?? results/r.jsonl\n M dce/runner.py\n"
    try:
        assert_clean_tree(out=out, repo_root=tmp_path, git_status_fn=lambda: porcelain)
        raise AssertionError("expected SystemExit")
    except SystemExit:
        pass


def test_assert_clean_tree_does_not_exempt_a_different_file_in_the_same_results_dir(
    tmp_path: Path,
):
    """I5 minor: exempting the whole `results/` directory instead of just
    `out` would let a change to a DIFFERENT, previous run's results file —
    an edit or deletion — pass silently. Only the exact `out` path is
    exempt."""
    out = tmp_path / "results" / "r.jsonl"
    porcelain = "?? results/r.jsonl\n?? results/scratch.tmp\n"
    try:
        assert_clean_tree(out=out, repo_root=tmp_path, git_status_fn=lambda: porcelain)
        raise AssertionError("expected SystemExit")
    except SystemExit:
        pass


# ── exit code on truncation ──────────────────────────────────────────────


def test_exit_code_for_truncated_is_nonzero():
    result = SweepResult(spent=1.0, real_spent=1.0, truncated=True)
    assert _exit_code_for(result) != 0


def test_exit_code_for_complete_is_zero():
    result = SweepResult(spent=1.0, real_spent=1.0, truncated=False)
    assert _exit_code_for(result) == 0


def test_exit_code_for_circuit_broken_is_distinct_from_truncated():
    truncated = SweepResult(spent=1.0, real_spent=1.0, truncated=True)
    broken = SweepResult(
        spent=1.0, real_spent=1.0, truncated=False, circuit_broken=True
    )
    assert _exit_code_for(broken) != 0
    assert _exit_code_for(broken) != _exit_code_for(truncated)


# ── _validated_usd ───────────────────────────────────────────────────────


def test_validated_usd_treats_none_as_zero():
    assert _validated_usd(None, "usd") == 0.0


def test_validated_usd_accepts_a_normal_float():
    assert _validated_usd(0.42, "usd") == 0.42


def test_validated_usd_rejects_a_string():
    with pytest.raises(TypeError):
        _validated_usd("1.20", "usd")


def test_validated_usd_rejects_a_bool():
    # bool IS an int in Python -- True must not silently price a row at $1.00.
    with pytest.raises(TypeError):
        _validated_usd(True, "usd")


def test_validated_usd_rejects_a_negative_value():
    with pytest.raises(ValueError):
        _validated_usd(-600.0, "usd")


def test_validated_usd_rejects_nan():
    with pytest.raises(ValueError):
        _validated_usd(float("nan"), "usd")


def test_validated_usd_rejects_infinity():
    # `Infinity` is not valid JSON per RFC 8259, and an accepted `inf`
    # would make spent_so_far permanently `inf`, truncating every future
    # resume to zero pending work -- the same bricking outcome the NaN
    # check already exists to prevent, just via a different value.
    with pytest.raises(ValueError):
        _validated_usd(float("inf"), "usd")


def test_validated_usd_rejects_a_decimal():
    # Decimal is not registered as a subclass of int/float in Python, so
    # it must be rejected by the isinstance check even though it looks
    # numeric.
    with pytest.raises(TypeError):
        _validated_usd(Decimal("1.20"), "usd")


# ── _pessimistic_usd / _priced_or_pessimistic ─────────────────────────────


def test_pessimistic_usd_returns_the_real_ceiling_for_a_known_model():
    assert _pessimistic_usd("z-ai/glm-5.3-flash") == _token_budget_usd(
        "z-ai/glm-5.3-flash"
    )


def test_pessimistic_usd_never_raises_for_an_unknown_model():
    assert _pessimistic_usd("not-a-real-model") == WORST_CASE_TOKEN_BUDGET_USD


def test_priced_or_pessimistic_passes_through_a_valid_value():
    row = {"usd": 0.42}
    value, error = _priced_or_pessimistic(row, "usd", "z-ai/glm-5.3-flash")
    assert value == 0.42
    assert error is None


def test_priced_or_pessimistic_normalizes_a_missing_key():
    row: dict = {}
    value, error = _priced_or_pessimistic(row, "usd", "z-ai/glm-5.3-flash")
    assert value == _token_budget_usd("z-ai/glm-5.3-flash")
    assert error is not None
    assert "missing" in error


def test_priced_or_pessimistic_normalizes_an_invalid_value():
    row = {"usd": Decimal("1.20")}
    value, error = _priced_or_pessimistic(row, "usd", "z-ai/glm-5.3-flash")
    assert value == _token_budget_usd("z-ai/glm-5.3-flash")
    assert error is not None
    assert "TypeError" in error


def test_priced_or_pessimistic_never_raises_even_for_an_unknown_model():
    row: dict = {}
    value, error = _priced_or_pessimistic(row, "usd", "not-a-real-model")
    assert value == WORST_CASE_TOKEN_BUDGET_USD
    assert error is not None


# ── _safe_json_dumps ──────────────────────────────────────────────────────


def test_safe_json_dumps_handles_a_normal_row():
    row = {"task_id": "1", "verdict": "correct", "usd": 0.01}
    assert json.loads(_safe_json_dumps(row)) == row


def test_safe_json_dumps_falls_back_to_repr_for_an_unserializable_value():
    class Unserializable:
        def __repr__(self):
            return "<sentinel>"

    row = {"task_id": "1", "oops": Unserializable()}
    parsed = json.loads(_safe_json_dumps(row))
    assert parsed["oops"] == "<sentinel>"


def test_safe_json_dumps_survives_a_non_str_dict_key():
    # default=repr cannot help here: json.dumps raises on the KEY before
    # `default` is ever consulted, regardless of what `default` does.
    row = {"task_id": "1", "arm": "contract", (1, 2): "bad key"}
    parsed = json.loads(_safe_json_dumps(row))
    assert "unserializable_row" in parsed
    assert parsed["task_id"] == "1"  # best-effort identifying fields survive
    assert parsed["arm"] == "contract"


def test_safe_json_dumps_survives_a_circular_reference():
    row: dict = {"task_id": "1", "verdict": "correct"}
    row["self"] = row
    parsed = json.loads(_safe_json_dumps(row))
    assert "unserializable_row" in parsed
    assert parsed["task_id"] == "1"


def test_safe_json_dumps_envelope_carries_the_price_level_and_corruption():
    """The layer-3 envelope must not be a bare "here is a line" marker: a
    row landing without `usd`/`usd_guard` prices it at $0.00 for every
    reader (the spend cap included), without `level` it is bucketed as
    level="unknown" by `dce.stats.accuracy_by`, and without `db_corrupted`
    the experiment's headline governance finding is misattributed."""
    row: dict = {
        "task_id": "1",
        "arm": "contract",
        "model": GLM,
        "verdict": "correct",
        "level": "hard",
        "db_corrupted": True,
        "usd": 1.20,
        "usd_guard": 1.20,
        (1, 2): "a tuple key: nothing but layer 3 can serialize this row",
    }
    parsed = json.loads(_safe_json_dumps(row))
    assert "unserializable_row" in parsed
    assert parsed["usd"] == pytest.approx(1.20)
    assert parsed["usd_guard"] == pytest.approx(1.20)
    assert parsed["level"] == "hard"
    assert parsed["db_corrupted"] is True


def test_safe_json_dumps_envelope_always_carries_a_task_id(tmp_path: Path):
    """A non-`str` `task_id` used to be filtered out of the envelope by an
    `isinstance` check, leaving the key ABSENT -- after which `latest_rows`
    and `completed_keys` (both of which index `row["task_id"]`) raised
    `KeyError` over the whole file, taking down every future resume and all
    of `dce.stats`. Coerced with `str()`, never filtered."""
    row: dict = {
        "task_id": 7,  # an int, not a str
        "arm": "contract",
        "model": GLM,
        "verdict": "correct",
        (1, 2): "forces layer 3",
    }
    parsed = json.loads(_safe_json_dumps(row))
    assert parsed["task_id"] == "7"
    assert parsed["arm"] == "contract"
    assert parsed["verdict"] == "correct"

    path = tmp_path / "r.jsonl"
    path.write_text(json.dumps(parsed) + "\n")
    assert latest_rows(path)  # must not raise KeyError
    assert completed_keys(path) == {("7", "contract", GLM)}


# ── real_spent_so_far ────────────────────────────────────────────────────


def test_real_spent_so_far_sums_usd_not_usd_guard(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(
        json.dumps({"usd": 0.01, "usd_guard": 0.183, "verdict": "construction_error"})
        + "\n"
    )
    assert real_spent_so_far(path) == pytest.approx(0.01)
    assert spent_so_far(path) == pytest.approx(0.183)


def test_spent_so_far_falls_back_to_usd_for_a_legacy_row_without_usd_guard(
    tmp_path: Path,
):
    path = tmp_path / "r.jsonl"
    path.write_text(json.dumps({"usd": 0.40}) + "\n")  # pre-split row shape
    assert spent_so_far(path) == pytest.approx(0.40)
    assert real_spent_so_far(path) == pytest.approx(0.40)


def test_seed_observed_by_model_prefers_usd_guard_over_usd(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(json.dumps({"model": "m1", "usd": 0.0, "usd_guard": 0.183}) + "\n")
    assert _seed_observed_by_model(path) == {"m1": [0.183]}


# ── sweep: real spend split from guard ledger ─────────────────────────────


def test_sweep_reports_a_ledger_that_separates_real_spend_from_guard_spend(
    tmp_path: Path,
):
    """Pricing a construction error pessimistically is right for the CAP
    but wrong for the LEDGER: a unit with one construction error and one
    real $0.01 success must report real_spent close to $0.01, not the
    guard-inflated figure. (Kept to ONE failure, below
    MAX_CONSTRUCTION_ATTEMPTS, so this exercises the ledger split rather
    than the retry cap.)"""
    attempt = {"n": 0}

    def flaky_then_succeeds(task, arm, model, *a, **k):
        attempt["n"] += 1
        if attempt["n"] == 1:
            raise AgentConstructionError("transient")
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.01,
            "usd_guard": 0.01,
            "verdict": "correct",
        }

    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    for _ in range(2):  # two separate invocations: fail, then succeed
        sweep(
            TASKS,
            ("contract",),
            (GLM,),
            {"1": "g"},
            out=out,
            db_path=db_path,
            docs={},
            max_spend=100.0,
            golds_hash="h",
            run_task_fn=flaky_then_succeeds,
        )

    assert real_spent_so_far(out) == pytest.approx(0.01)
    assert spent_so_far(out) == pytest.approx(_token_budget_usd(GLM) + 0.01)


# ── retry policy: trailing, not lifetime, construction-error counts ──────


def test_completed_keys_does_not_give_up_on_one_fresh_failure_after_a_success(
    tmp_path: Path,
):
    """A file reading construction_error, correct, construction_error for
    the same key has a TRAILING count of 1, not a lifetime count of 2 —
    the intervening success means the most recent failure is fresh, not
    the second half of a persistent problem, and must still be retryable."""
    path = tmp_path / "r.jsonl"
    rows = [
        {
            "task_id": "1",
            "arm": "contract",
            "model": "m",
            "verdict": "construction_error",
        },
        {"task_id": "1", "arm": "contract", "model": "m", "verdict": "correct"},
        {
            "task_id": "1",
            "arm": "contract",
            "model": "m",
            "verdict": "construction_error",
        },
    ]
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    assert MAX_CONSTRUCTION_ATTEMPTS == 2  # this test assumes the current cap
    assert completed_keys(path) == set()  # still retryable, not given up
    assert gave_up_keys(path) == set()


def test_completed_keys_gives_up_on_truly_consecutive_trailing_failures(
    tmp_path: Path,
):
    path = tmp_path / "r.jsonl"
    rows = [
        {"task_id": "1", "arm": "contract", "model": "m", "verdict": "correct"},
        {
            "task_id": "1",
            "arm": "contract",
            "model": "m",
            "verdict": "construction_error",
        },
        {
            "task_id": "1",
            "arm": "contract",
            "model": "m",
            "verdict": "construction_error",
        },
    ]
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    assert completed_keys(path) == {("1", "contract", "m")}
    assert gave_up_keys(path) == {("1", "contract", "m")}


# ── sweep-wide circuit breaker ────────────────────────────────────────────


def test_sweep_circuit_breaker_stops_immediately_on_systemic_failure(
    tmp_path: Path,
):
    """A missing API key fails EVERY unit identically. Without a
    sweep-wide circuit breaker, per-key retry bounding alone would still
    let the sweep grind through every remaining unit at
    MAX_CONSTRUCTION_ATTEMPTS phantom-priced rows apiece before the spend
    cap finally noticed."""
    calls = []

    def always_fails(task, arm, model, *a, **k):
        calls.append(task["task_id"])
        raise AgentConstructionError("missing OPENROUTER_API_KEY")

    tasks = [
        {"task_id": str(i), "question": "q", "guidelines": "g", "level": "hard"}
        for i in range(50)  # far more than the circuit breaker threshold
    ]
    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    result = sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        {str(i): "g" for i in range(50)},
        out=out,
        db_path=db_path,
        docs={},
        max_spend=1000.0,  # generous: the circuit breaker must fire first
        golds_hash="h",
        run_task_fn=always_fails,
    )
    assert result.circuit_broken is True
    assert result.truncated is False
    assert len(calls) == CIRCUIT_BREAKER_THRESHOLD
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(rows) == CIRCUIT_BREAKER_THRESHOLD


def test_circuit_breaker_fires_before_any_key_is_given_up(tmp_path: Path):
    """Cross-validates MAX_CONSTRUCTION_ATTEMPTS and
    CIRCUIT_BREAKER_THRESHOLD directly: within one invocation no key is
    ever attempted twice, so when a systemic failure trips the circuit
    breaker, every one of its failing keys has a TRAILING count of exactly
    1 -- strictly below the per-key cap. The circuit breaker must be the
    FIRST alarm for a systemic problem, not one that co-occurs with (or
    is preceded by) `gave_up_keys` reporting the same units -- otherwise a
    future edit to either constant could silently let per-key give-up mask
    what is actually a systemic failure behind what looks like N
    independent per-task failures.
    """
    # The relationship this test exercises requires this to hold; if it
    # ever doesn't, every failing key's first attempt would ALSO exhaust
    # its retry budget in the same invocation the circuit breaker fires.
    assert MAX_CONSTRUCTION_ATTEMPTS > 1

    def always_fails(task, arm, model, *a, **k):
        raise AgentConstructionError("missing OPENROUTER_API_KEY")

    tasks = [
        {"task_id": str(i), "question": "q", "guidelines": "g", "level": "hard"}
        for i in range(CIRCUIT_BREAKER_THRESHOLD * 3)
    ]
    golds = {str(i): "g" for i in range(len(tasks))}
    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    result = sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        golds,
        out=out,
        db_path=db_path,
        docs={},
        max_spend=1000.0,
        golds_hash="h",
        run_task_fn=always_fails,
    )
    assert result.circuit_broken is True
    # The circuit breaker is the sole, first alarm: not one single key has
    # been given up on yet when it fires.
    assert gave_up_keys(out) == set()


def test_sweep_circuit_breaker_resets_on_a_non_construction_error_row(
    tmp_path: Path,
):
    """The circuit breaker counts CONSECUTIVE construction errors; a
    success in between must reset the count rather than accumulate across
    it."""
    calls = []

    def mostly_fails_but_not_consecutively(task, arm, model, *a, **k):
        calls.append(task["task_id"])
        # Every third task succeeds, breaking up any run of consecutive
        # construction errors before it can reach the threshold.
        if int(task["task_id"]) % 3 == 2:
            return {
                "task_id": task["task_id"],
                "arm": arm,
                "model": model,
                "usd": 0.01,
                "usd_guard": 0.01,
                "verdict": "correct",
            }
        raise AgentConstructionError("missing OPENROUTER_API_KEY")

    tasks = [
        {"task_id": str(i), "question": "q", "guidelines": "g", "level": "hard"}
        for i in range(12)
    ]
    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    result = sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        {str(i): "g" for i in range(12)},
        out=out,
        db_path=db_path,
        docs={},
        max_spend=1000.0,
        golds_hash="h",
        run_task_fn=mostly_fails_but_not_consecutively,
    )
    assert result.circuit_broken is False
    assert len(calls) == 12  # every task ran; the circuit never tripped


# ── check_and_restore failures: the row still reaches disk ────────────────


def test_sweep_records_the_row_and_reraises_when_check_and_restore_fails(
    tmp_path, monkeypatch
):
    """The central invariant: once a priced row exists, it reaches disk
    before anything else may throw. A `check_and_restore` failure (e.g. a
    real PermissionError from `_sha256`) must not discard the row or the
    spend it represents -- it writes the row (db_corrupted=None,
    integrity_error noted), updates spent, and ONLY THEN re-raises."""

    def fake_run(task, arm, model, *a, **k):
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 1.20,
            "usd_guard": 1.20,
            "verdict": "correct",
        }

    def exploding_check_and_restore(working, pristine):
        raise PermissionError("cannot read pristine file")

    monkeypatch.setattr(runner_module, "check_and_restore", exploding_check_and_restore)

    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    with pytest.raises(PermissionError):
        sweep(
            TASKS,
            ("contract",),
            (GLM,),
            {"1": "g"},
            out=out,
            db_path=db_path,
            docs={},
            max_spend=10.0,
            golds_hash="h",
            run_task_fn=fake_run,
        )

    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(rows) == 1  # the row was NOT lost
    assert rows[0]["db_corrupted"] is None
    assert "PermissionError" in rows[0]["integrity_error"]
    assert real_spent_so_far(out) == pytest.approx(1.20)  # spend was recorded


def test_sweep_twenty_resumes_advance_the_cap_when_check_and_restore_fails(
    tmp_path, monkeypatch
):
    """The exact scenario the coordinator asked to see re-run: 20 resumes
    against a real_run_task_fn that spends real money, with
    check_and_restore injected to fail every time. The cap must advance
    (spent_so_far grows, rows land on disk) instead of reading $0.00
    forever while real dollars are burned."""
    calls = []

    def fake_run(task, arm, model, *a, **k):
        calls.append(task["task_id"])
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 1.20,
            "usd_guard": 1.20,
            "verdict": "correct",
        }

    def exploding_check_and_restore(working, pristine):
        raise PermissionError("cannot read pristine file")

    monkeypatch.setattr(runner_module, "check_and_restore", exploding_check_and_restore)

    tasks = [
        {"task_id": str(i), "question": "q", "guidelines": "g", "level": "hard"}
        for i in range(20)
    ]
    golds = {str(i): "g" for i in range(20)}
    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)

    for _ in range(20):
        try:
            sweep(
                tasks,
                ("schema_only",),
                (GLM,),
                golds,
                out=out,
                db_path=db_path,
                docs={},
                max_spend=1.00,
                golds_hash="h",
                run_task_fn=fake_run,
            )
        except PermissionError:
            pass  # the sweep is expected to stop loudly each time

    # The cap must have advanced: real dollars were spent AND recorded.
    assert len(calls) >= 1
    assert real_spent_so_far(out) > 0.0
    assert real_spent_so_far(out) == pytest.approx(len(calls) * 1.20)
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(rows) == len(calls)
    # The cap ($1.00) plus per-call reservation must have stopped further
    # calls once one $1.20 call was already recorded -- not run all 20.
    assert len(calls) < 20


# ── close_error invalidates the integrity check ───────────────────────────


def test_sweep_distrusts_the_integrity_check_when_close_error_is_present(
    tmp_path: Path,
):
    """A `close_error` on the row means `run_task`'s own `setup.close()`
    failed -- the connection may still be open, so `check_and_restore`'s
    result cannot be trusted regardless of what it actually reports."""

    def fake_run_with_close_error(task, arm, model, *a, **k):
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.01,
            "usd_guard": 0.01,
            "verdict": "correct",
            "close_error": "RuntimeError: close exploded",
        }

    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    sweep(
        TASKS,
        ("contract",),
        (GLM,),
        {"1": "g"},
        out=out,
        db_path=db_path,
        docs={},
        max_spend=1.0,
        golds_hash="h",
        run_task_fn=fake_run_with_close_error,
    )
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(rows) == 1
    # Untrustworthy, not corrupted=False -- the check may have run against
    # a still-live connection and cannot be relied on either way.
    assert rows[0]["db_corrupted"] is None
    assert "leaked" in rows[0]["integrity_error"]


def test_sweep_stops_entirely_on_a_leaked_connection_not_just_that_row(
    tmp_path: Path,
):
    """A leaked connection does not just invalidate ONE task's check -- a
    still-open connection can keep mutating the working copy underneath
    whatever runs next, so every later check in the SAME sweep would be
    equally untrustworthy. The sweep must stop immediately: the row for
    the task that leaked is written (the invariant), but no further task
    is attempted this invocation."""
    calls = []

    def fake_run_first_task_leaks(task, arm, model, *a, **k):
        calls.append(task["task_id"])
        row = {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.01,
            "usd_guard": 0.01,
            "verdict": "correct",
        }
        if task["task_id"] == "0":
            row["close_error"] = "RuntimeError: close exploded"
        return row

    tasks = [
        {"task_id": str(i), "question": "q", "guidelines": "g", "level": "hard"}
        for i in range(5)
    ]
    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    result = sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        {str(i): "g" for i in range(5)},
        out=out,
        db_path=db_path,
        docs={},
        max_spend=100.0,
        golds_hash="h",
        run_task_fn=fake_run_first_task_leaks,
    )
    assert calls == ["0"]  # every later task was never even attempted
    assert result.connection_leaked is True
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["task_id"] == "0"
    assert rows[0]["db_corrupted"] is None
    assert _exit_code_for(result) == 4
    assert _exit_code_for(result) != _exit_code_for(
        SweepResult(spent=0, real_spent=0, truncated=True)
    )
    assert _exit_code_for(result) != _exit_code_for(
        SweepResult(spent=0, real_spent=0, truncated=False, circuit_broken=True)
    )


# ── json serialization never loses a row ─────────────────────────────────


def test_sweep_falls_back_to_repr_for_a_non_serializable_field(tmp_path: Path):
    class Unserializable:
        def __repr__(self):
            return "<Unserializable sentinel>"

    def fake_run_with_bad_field(task, arm, model, *a, **k):
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.01,
            "usd_guard": 0.01,
            "verdict": "correct",
            "oops": Unserializable(),
        }

    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    sweep(
        TASKS,
        ("contract",),
        (GLM,),
        {"1": "g"},
        out=out,
        db_path=db_path,
        docs={},
        max_spend=1.0,
        golds_hash="h",
        run_task_fn=fake_run_with_bad_field,
    )
    text = out.read_text()
    assert "<Unserializable sentinel>" in text  # the row was not lost
    rows = [json.loads(line) for line in text.splitlines()]
    assert len(rows) == 1
    assert rows[0]["verdict"] == "correct"


# ── regression pin: the row always lands, no matter what fails in between ─


def _mutate_missing_usd(row):
    row = dict(row)
    del row["usd"]
    return row


def _mutate_missing_usd_guard(row):
    row = dict(row)
    del row["usd_guard"]
    return row


def _mutate_usd_decimal(row):
    row = dict(row)
    row["usd"] = Decimal("1.20")
    return row


def _mutate_usd_nan(row):
    row = dict(row)
    row["usd"] = float("nan")
    return row


def _mutate_usd_infinite(row):
    row = dict(row)
    row["usd"] = float("inf")
    return row


def _mutate_usd_negative(row):
    row = dict(row)
    row["usd"] = -5.0
    return row


def _mutate_non_str_dict_key(row):
    row = dict(row)
    row[(1, 2)] = "bad key"  # a tuple key: json.dumps raises regardless of `default`
    return row


def _mutate_circular_reference(row):
    row = dict(row)
    row["self"] = row
    return row


def _mutate_identity(row):
    return dict(row)


# (case name, row mutator, patch check_and_restore to raise?, sweep must raise?)
_INJECTION_CASES = [
    ("missing_usd", _mutate_missing_usd, False, True),
    ("missing_usd_guard", _mutate_missing_usd_guard, False, True),
    ("usd_decimal", _mutate_usd_decimal, False, True),
    ("usd_nan", _mutate_usd_nan, False, True),
    ("usd_infinite", _mutate_usd_infinite, False, True),
    ("usd_negative", _mutate_usd_negative, False, True),
    ("non_str_dict_key", _mutate_non_str_dict_key, False, False),
    ("circular_reference", _mutate_circular_reference, False, False),
    ("check_and_restore_raises", _mutate_identity, True, True),
]


@pytest.mark.parametrize(
    "mutate,patch_check_and_restore,expect_exception",
    [c[1:] for c in _INJECTION_CASES],
    ids=[c[0] for c in _INJECTION_CASES],
)
def test_sweep_lands_the_row_no_matter_what_fails_between_return_and_write(
    tmp_path: Path,
    monkeypatch,
    mutate,
    patch_check_and_restore,
    expect_exception,
):
    """The regression pin for A1's fourth recurrence: walks every known
    injection point between `run_task_fn` returning and the row landing
    on disk (a bad/missing `usd`/`usd_guard`, an unserializable dict key,
    a circular reference, `check_and_restore` itself raising) and asserts
    the row is ALWAYS on disk afterward. This is the fourth time this bug
    class has recurred in this component (see the module docstring); the
    point of this test is that a FIFTH recurrence has to explicitly break
    a test to ship, not slip past silently the way the first three did.
    """
    base_row = {
        "task_id": "1",
        "arm": "contract",
        "model": GLM,
        "usd": 0.01,
        "usd_guard": 0.01,
        "verdict": "correct",
    }

    def fake_run(task, arm, model, *a, **k):
        return mutate(base_row)

    if patch_check_and_restore:

        def exploding_check_and_restore(working, pristine):
            raise PermissionError("cannot read pristine file")

        monkeypatch.setattr(
            runner_module, "check_and_restore", exploding_check_and_restore
        )

    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)

    ctx = pytest.raises(Exception) if expect_exception else contextlib.nullcontext()
    with ctx:
        sweep(
            TASKS,
            ("contract",),
            (GLM,),
            {"1": "g"},
            out=out,
            db_path=db_path,
            docs={},
            max_spend=100.0,
            golds_hash="h",
            run_task_fn=fake_run,
        )

    lines = [line for line in out.read_text().splitlines() if line.strip()]
    assert len(lines) == 1, "the row was lost"
    row = json.loads(lines[0])
    # Either the row parsed with its real fields, or it landed via the
    # unserializable-row envelope -- either way it reached disk, legibly.
    assert row.get("task_id") == "1" or "unserializable_row" in row

    # THE ROW LANDING IS NOT ENOUGH -- THE PRICE HAS TO LAND WITH IT.
    # A row that reaches disk at $0.00 reproduces the exact failure this
    # whole pin exists to prevent (a resumed sweep spending real money
    # while `spent_so_far` reads zero and `--max-spend` stops binding),
    # just one layer deeper: in `_safe_json_dumps`'s own fallback envelope
    # rather than in the loop that calls it. The envelope used to carry
    # only task_id/arm/model/verdict, so the two serialization cases below
    # (`non_str_dict_key`, `circular_reference`) passed the assertion above
    # while the ledger read $0.00.
    assert spent_so_far(out) > 0, "the row landed but its price did not"
    # And every reader of the file must still work afterwards: both of
    # these index `row["task_id"]`, so an envelope that dropped the key
    # would take down every future resume AND the whole analysis.
    latest_rows(out)
    completed_keys(out)


# ── sweep: concurrency ───────────────────────────────────────────────────
#
# Every test below exists because parallelism reaches four pieces of state
# the serial loop never had to share: the results file handle, the spend
# ledger, the per-model observation history, and the working copy. The
# working copy is the one that is not merely a data race — see
# `test_sweep_gives_each_worker_its_own_working_copy`.


def _fast_row(task, arm, model, *a, **k):
    return {
        "task_id": task["task_id"],
        "arm": arm,
        "model": model,
        "usd": 0.001,
        "usd_guard": 0.001,
        "verdict": "correct",
    }


def _numbered_tasks(n: int) -> list[dict]:
    return [
        {"task_id": str(i), "question": "q", "guidelines": "g", "level": "hard"}
        for i in range(n)
    ]


def test_sweep_at_one_worker_writes_rows_in_pending_order(tmp_path: Path):
    """The default must stay bit-identical to the serial sweep: same rows,
    same order. Every other test in this file asserts against `workers=1`
    implicitly, so a regression here would look like a hundred unrelated
    failures rather than one."""
    out = tmp_path / "r.jsonl"
    tasks = _numbered_tasks(4)
    sweep(
        tasks,
        ("schema_only", "contract"),
        (GLM,),
        {t["task_id"]: "g" for t in tasks},
        out=out,
        db_path=_make_pristine(tmp_path),
        docs={},
        max_spend=100.0,
        golds_hash="h",
        run_task_fn=_fast_row,
    )
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert [(r["task_id"], r["arm"]) for r in rows] == [
        (t, a) for t in "0123" for a in ("schema_only", "contract")
    ]


def test_sweep_runs_groups_concurrently_when_workers_exceeds_one(tmp_path: Path):
    """The point of the whole change: at `workers=N`, N task groups are in
    flight at once. Asserted with a barrier rather than a timing
    measurement — a wall-clock assertion is the classic flaky test, and a
    barrier that never trips fails deterministically."""
    import threading

    barrier = threading.Barrier(3, timeout=10)

    def blocking_row(task, arm, model, *a, **k):
        if arm == "schema_only":
            # Only the first call of each group waits, so the barrier is
            # measuring concurrent GROUPS, not concurrent arms.
            barrier.wait()
        return _fast_row(task, arm, model)

    tasks = _numbered_tasks(3)
    sweep(
        tasks,
        ("schema_only", "contract"),
        (GLM,),
        {t["task_id"]: "g" for t in tasks},
        out=tmp_path / "r.jsonl",
        db_path=_make_pristine(tmp_path),
        docs={},
        max_spend=100.0,
        golds_hash="h",
        run_task_fn=blocking_row,
        workers=3,
    )
    # Reaching here at all means all three groups were inside
    # `run_task_fn` simultaneously; a serial sweep deadlocks on the
    # barrier's timeout instead.


def test_sweep_gives_each_worker_its_own_working_copy(tmp_path: Path):
    """NOT merely a data race. `check_and_restore` repairs a corrupted
    working copy by copying the 26 MB pristine file back over it. Sharing
    one copy across workers would land that `copyfile` in the middle of
    another worker's live query, and — worse for the experiment —
    `db_corrupted` would stop attributing corruption to the arm that
    caused it. That field IS the headline governance finding, so a shared
    copy would not just be slow or flaky; it would be wrong."""
    import threading

    seen: list[Path] = []
    lock = threading.Lock()
    barrier = threading.Barrier(2, timeout=10)

    def record_db(task, arm, model, db, *a, **k):
        with lock:
            seen.append(db)
        if arm == "schema_only":
            barrier.wait()
        return _fast_row(task, arm, model)

    pristine = _make_pristine(tmp_path)
    tasks = _numbered_tasks(2)
    sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        {t["task_id"]: "g" for t in tasks},
        out=tmp_path / "r.jsonl",
        db_path=pristine,
        docs={},
        max_spend=100.0,
        golds_hash="h",
        run_task_fn=record_db,
        workers=2,
    )
    assert pristine not in seen
    assert len(set(seen)) == 2, "each worker must get its own working copy"
    assert set(seen) == {_working_db_path(pristine, 0), _working_db_path(pristine, 1)}
    for path in set(seen):
        assert path.exists()


def test_working_db_path_keeps_the_historical_name_for_worker_zero(tmp_path: Path):
    """A `workers=1` resume must reuse the same file a pre-concurrency
    sweep left behind, not orphan it under a new name."""
    pristine = tmp_path / "d"
    assert _working_db_path(pristine) == _working_db_path(pristine, 0)
    assert _working_db_path(pristine, 0).name == "d.working"
    assert _working_db_path(pristine, 1).name == "d.working-1"


def test_concurrent_writes_land_one_parseable_row_per_unit(tmp_path: Path):
    """The module's central invariant — one row per unit of work, on disk —
    survives eight threads sharing one file handle. An unsynchronized
    `write` + `flush` interleaves partial lines, which reads back as
    corruption in the middle of the file, which `_read_rows` (correctly)
    refuses to forgive."""
    out = tmp_path / "r.jsonl"
    tasks = _numbered_tasks(40)
    sweep(
        tasks,
        ("schema_only", "manual_prompt", "contract"),
        (GLM,),
        {t["task_id"]: "g" for t in tasks},
        out=out,
        db_path=_make_pristine(tmp_path),
        docs={},
        max_spend=100.0,
        golds_hash="h",
        run_task_fn=_fast_row,
        workers=8,
    )
    lines = out.read_text().splitlines()
    assert len(lines) == 120
    rows = [json.loads(line) for line in lines]  # raises if any line is torn
    assert {(r["task_id"], r["arm"]) for r in rows} == {
        (t["task_id"], arm)
        for t in tasks
        for arm in ("schema_only", "manual_prompt", "contract")
    }


def test_sweep_counts_in_flight_reservations_against_the_cap(tmp_path: Path):
    """The serial guard reserved one group, ran it, then reserved the next.
    Concurrently, N groups are dispatched before any of them has banked a
    cost, so a guard that only reads `spent` would admit N groups against a
    budget for one. The reservation must be held from dispatch to
    completion, not merely computed at dispatch."""
    import threading

    started = threading.Semaphore(0)
    release = threading.Event()
    calls: list[str] = []
    lock = threading.Lock()

    def blocking_row(task, arm, model, *a, **k):
        with lock:
            calls.append(task["task_id"])
        started.release()
        release.wait(timeout=10)
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": _CALL_USD,
            "usd_guard": _CALL_USD,
            "verdict": "correct",
        }

    tasks = _numbered_tasks(10)
    max_spend = _budget_for_exactly_two_calls(GLM)

    def unblock() -> None:
        # Let every worker that can start, start; then release them all.
        # `daemon` plus a timeout so a failing sweep cannot leave this
        # thread blocked forever and hang the whole test session.
        started.acquire(timeout=10)
        release.set()

    watcher = threading.Thread(target=unblock, daemon=True)
    watcher.start()
    result = sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        {t["task_id"]: "g" for t in tasks},
        out=tmp_path / "r.jsonl",
        db_path=_make_pristine(tmp_path),
        docs={},
        max_spend=max_spend,
        golds_hash="h",
        run_task_fn=blocking_row,
        workers=8,
    )
    release.set()
    watcher.join(timeout=10)
    # The serial sweep admits exactly two $0.40 calls under this budget.
    # Eight workers must not admit more: the floor reservation for a single
    # in-flight group already consumes most of the cap, so at most two
    # groups can ever be in flight at once.
    assert len(calls) <= 2
    assert result.spent <= max_spend
    assert result.truncated is True


def test_sweep_stops_dispatching_new_groups_once_the_circuit_breaks(tmp_path: Path):
    """A systemic failure (a missing API key) fails every unit identically.
    The serial breaker `break`s out of the loop; the concurrent one has to
    stop the QUEUE, or the remaining groups drain through at full
    guard-ledger cost while the breaker's verdict sits unread."""

    def always_fails(task, arm, model, *a, **k):
        raise AgentConstructionError("no OPENROUTER_API_KEY")

    tasks = _numbered_tasks(60)
    result = sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        {t["task_id"]: "g" for t in tasks},
        out=tmp_path / "r.jsonl",
        db_path=_make_pristine(tmp_path),
        docs={},
        max_spend=1_000_000.0,
        golds_hash="h",
        run_task_fn=always_fails,
        workers=4,
    )
    assert result.circuit_broken is True
    rows = [
        json.loads(line) for line in (tmp_path / "r.jsonl").read_text().splitlines()
    ]
    # The breaker trips at CIRCUIT_BREAKER_THRESHOLD; at `workers` in
    # flight, at most `workers - 1` further groups can already be running
    # when it does. Nothing beyond that may be dispatched.
    assert len(rows) < len(tasks)
    assert len(rows) <= CIRCUIT_BREAKER_THRESHOLD + 4


def test_sweep_reraises_a_worker_bug_after_the_pool_drains(tmp_path: Path):
    """A real bug in `run_task_fn` stops the sweep loudly — that property
    predates concurrency and must survive it. The difference is that the
    exception now has to cross a thread boundary, and the rows already in
    flight must still land before it surfaces."""

    def explodes_on_one_task(task, arm, model, *a, **k):
        if task["task_id"] == "3":
            raise RuntimeError("a real bug")
        return _fast_row(task, arm, model)

    out = tmp_path / "r.jsonl"
    tasks = _numbered_tasks(8)
    with pytest.raises(RuntimeError, match="a real bug"):
        sweep(
            tasks,
            ("schema_only",),
            (GLM,),
            {t["task_id"]: "g" for t in tasks},
            out=out,
            db_path=_make_pristine(tmp_path),
            docs={},
            max_spend=100.0,
            golds_hash="h",
            run_task_fn=explodes_on_one_task,
            workers=4,
        )
    # Whatever else happened, no row was torn or lost on the way out.
    for line in out.read_text().splitlines():
        json.loads(line)


def test_sweep_rejects_a_nonsensical_worker_count(tmp_path: Path):
    with pytest.raises(ValueError, match="workers"):
        sweep(
            TASKS,
            ("schema_only",),
            (GLM,),
            {"1": "g"},
            out=tmp_path / "r.jsonl",
            db_path=_make_pristine(tmp_path),
            docs={},
            max_spend=1.0,
            golds_hash="h",
            run_task_fn=_fast_row,
            workers=0,
        )


def test_a_group_blocked_by_in_flight_reservations_waits_instead_of_truncating(
    tmp_path: Path,
):
    """The bug this pins: a reservation is a worst-case CEILING, and real
    costs run ~100x smaller. At `--workers N` the Nth worker routinely finds
    `spent + reserved + amount > max_spend` purely because N-1 ceilings are
    held right now — on a budget that would fund hundreds more groups in
    sequence. Reading that as truncation stopped the entire sweep after
    `workers - 1` groups, silently, on a budget that was barely touched.

    Here: 8 groups, 8 workers, and a cap sized so at most two ceilings fit
    at once — but each call actually costs a thousandth of its ceiling, so
    every group must still run.
    """
    tasks = _numbered_tasks(8)
    floor = _token_budget_usd(GLM)
    result = sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        {t["task_id"]: "g" for t in tasks},
        out=tmp_path / "r.jsonl",
        db_path=_make_pristine(tmp_path),
        docs={},
        # Room for two concurrent ceilings and nothing like eight.
        max_spend=2.5 * floor,
        golds_hash="h",
        run_task_fn=_fast_row,
        workers=8,
    )
    rows = (tmp_path / "r.jsonl").read_text().splitlines()
    assert len(rows) == 8, "every group must run; a held ceiling is not a cap hit"
    assert result.truncated is False
    assert result.spent < floor


def test_the_budget_still_truncates_when_it_is_genuinely_exhausted(tmp_path: Path):
    """The other half of the same property: waiting for a release must not
    turn the spend cap into a suggestion. With nothing in flight to wait for
    (`reserved == 0`), a group that does not fit still stops the sweep —
    exactly the serial condition."""
    tasks = _numbered_tasks(20)
    max_spend = _budget_for_exactly_two_calls(GLM)

    def real_cost(task, arm, model, *a, **k):
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": _CALL_USD,
            "usd_guard": _CALL_USD,
            "verdict": "correct",
        }

    result = sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        {t["task_id"]: "g" for t in tasks},
        out=tmp_path / "r.jsonl",
        db_path=_make_pristine(tmp_path),
        docs={},
        max_spend=max_spend,
        golds_hash="h",
        run_task_fn=real_cost,
        workers=4,
    )
    assert result.truncated is True
    assert result.spent <= max_spend
    assert len((tmp_path / "r.jsonl").read_text().splitlines()) == 2


# ── sweep: crash-safety on an unattended box ─────────────────────────────


def test_sweep_refuses_to_start_while_another_holds_the_results_file(tmp_path: Path):
    """Two sweeps on one results file duplicate paid work AND share
    `<db>.working`, so one instance's `make_working_copy` lands on top of the
    other's live DuckDB connection — false `db_corrupted` on the experiment's
    headline metric. See `dce/lockfile.py`."""
    out = tmp_path / "r.jsonl"
    with sweep_lock(out):
        with pytest.raises(SweepLockedError):
            sweep(
                TASKS,
                ("schema_only",),
                (GLM,),
                {"1": "g"},
                out=out,
                db_path=_make_pristine(tmp_path),
                docs={},
                max_spend=100.0,
                golds_hash="h",
                run_task_fn=_fast_row,
            )
    assert not out.exists(), "a refused sweep must not have written anything"


def test_sweep_releases_the_lock_so_a_restart_can_resume(tmp_path: Path):
    out = tmp_path / "r.jsonl"
    for _ in range(3):
        sweep(
            TASKS,
            ("schema_only",),
            (GLM,),
            {"1": "g"},
            out=out,
            db_path=_make_pristine(tmp_path),
            docs={},
            max_spend=100.0,
            golds_hash="h",
            run_task_fn=_fast_row,
            retry_verdicts=("correct",),
        )
    assert not lock_path_for(out).exists()
    assert len(out.read_text().splitlines()) == 3


def test_every_row_is_fsynced_before_the_next_one_is_written(
    tmp_path: Path, monkeypatch
):
    """`flush()` hands bytes to the OS page cache: that survives this process
    dying (proven by SIGKILL probes) but not the MACHINE dying. On an
    unattended VPS the machine is exactly what dies. Rows arrive about once a
    minute per worker, so there is no throughput argument for batching this."""
    synced: list[int] = []
    real_fsync = os.fsync
    monkeypatch.setattr(os, "fsync", lambda fd: (synced.append(fd), real_fsync(fd))[1])
    out = tmp_path / "r.jsonl"
    tasks = _numbered_tasks(5)
    sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        {t["task_id"]: "g" for t in tasks},
        out=out,
        db_path=_make_pristine(tmp_path),
        docs={},
        max_spend=100.0,
        golds_hash="h",
        run_task_fn=_fast_row,
    )
    # One per row, plus the lockfile's own and any snapshot's.
    assert len(synced) >= 5


def test_the_snapshot_is_always_a_complete_parseable_file(tmp_path: Path):
    """Insurance against the loss fsync cannot cover: operator error, a bad
    disk, a bug that truncates the results file. Written to a temp name and
    `os.replace`d, so the snapshot is never itself half-written — a torn
    backup is worse than none, because it is trusted."""
    out = tmp_path / "r.jsonl"
    tasks = _numbered_tasks(SNAPSHOT_EVERY_ROWS + 3)
    sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        {t["task_id"]: "g" for t in tasks},
        out=out,
        db_path=_make_pristine(tmp_path),
        docs={},
        max_spend=100.0,
        golds_hash="h",
        run_task_fn=_fast_row,
        workers=4,
    )
    snapshot = snapshot_path_for(out)
    assert snapshot.exists()
    rows = [json.loads(line) for line in snapshot.read_text().splitlines()]
    # The final snapshot is taken after the pool drains, so it is complete.
    assert len(rows) == len(tasks)
    assert snapshot.read_text() == out.read_text()


def test_a_snapshot_exists_even_if_the_sweep_never_finishes(tmp_path: Path):
    """The whole point: the snapshot has to be there when the machine dies
    mid-sweep, not only when the sweep ends politely."""
    out = tmp_path / "r.jsonl"
    tasks = _numbered_tasks(80)

    def explodes_late(task, arm, model, *a, **k):
        if task["task_id"] == "70":
            raise RuntimeError("machine trouble")
        return _fast_row(task, arm, model)

    with pytest.raises(RuntimeError):
        sweep(
            tasks,
            ("schema_only",),
            (GLM,),
            {t["task_id"]: "g" for t in tasks},
            out=out,
            db_path=_make_pristine(tmp_path),
            docs={},
            max_spend=100.0,
            golds_hash="h",
            run_task_fn=explodes_late,
        )
    snapshot = snapshot_path_for(out)
    assert snapshot.exists()
    rows = [json.loads(line) for line in snapshot.read_text().splitlines()]
    assert len(rows) >= SNAPSHOT_EVERY_ROWS


def test_clean_tree_exempts_the_out_files_own_sidecars(tmp_path: Path):
    """The snapshot and the lock are DERIVED from `out` and land beside it.
    Left unexempted they make the tree dirty, so the very next invocation —
    which is to say every restart of an unattended sweep — refuses to start.
    That defeats the entire point of writing them: the crash insurance would
    itself block the crash recovery.

    Caught on the VPS before the first long run, by `git status` and not by a
    test, which is why this one exists.
    """
    out = tmp_path / "results" / "glm-full.jsonl"
    assert_clean_tree(
        out=out,
        repo_root=tmp_path,
        git_status_fn=lambda: (
            "?? results/glm-full.jsonl\n"
            "?? results/glm-full.jsonl.snapshot\n"
            "?? results/glm-full.jsonl.lock\n"
        ),
    )


def test_clean_tree_still_rejects_a_sidecar_of_a_different_run(tmp_path: Path):
    """Exempting `<out>.*` by prefix would let a PREVIOUS run's results file
    through under a name like `glm-full.jsonl.bak`. Only the two sidecars
    this sweep actually writes are exempt, by exact name."""
    out = tmp_path / "results" / "glm-full.jsonl"
    with pytest.raises(SystemExit, match="smoke12"):
        assert_clean_tree(
            out=out,
            repo_root=tmp_path,
            git_status_fn=lambda: (
                "?? results/glm-full.jsonl.snapshot\n M results/smoke12.jsonl\n"
            ),
        )
    with pytest.raises(SystemExit, match="glm-full.jsonl.bak"):
        assert_clean_tree(
            out=out,
            repo_root=tmp_path,
            git_status_fn=lambda: "?? results/glm-full.jsonl.bak\n",
        )
