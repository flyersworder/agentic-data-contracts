import json
from pathlib import Path

import pytest
from dce.agent import build_result_row
from dce.data import DATASET_REVISION
from dce.runner import (
    SweepResult,
    _construction_error_row,
    _exit_code_for,
    _load_golds,
    _next_reserve,
    _seed_observed_by_model,
    _stratified_sample,
    _working_db_path,
    assert_clean_tree,
    completed_keys,
    pending,
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


def test_completed_keys_always_retries_construction_error_rows(tmp_path: Path):
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
    assert completed_keys(path) == set()
    assert completed_keys(path, retry_error=True) == set()  # unaffected by the flag


def test_completed_keys_retries_error_rows_only_when_requested(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(
        json.dumps(
            {"task_id": "1", "arm": "contract", "model": "m", "verdict": "error"}
        )
        + "\n"
    )
    assert completed_keys(path) == {("1", "contract", "m")}
    assert completed_keys(path, retry_error=True) == set()


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
    assert completed_keys(path, retry_error=True) == {
        ("1", "contract", "m"),
        ("2", "contract", "m"),
    }


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


# ── sweep: the spend cap ─────────────────────────────────────────────────


def test_sweep_stops_before_exceeding_max_spend(tmp_path: Path):
    calls = []

    def fake_run(task, arm, model, *a, **k):
        calls.append(task["task_id"])
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.40,
            "verdict": "correct",
        }

    tasks = [
        {"task_id": str(i), "question": "q", "guidelines": "g", "level": "hard"}
        for i in range(10)
    ]
    golds = {str(i): "g" for i in range(10)}
    db_path = _make_pristine(tmp_path)
    result = sweep(
        tasks,
        ("schema_only",),
        (GLM,),
        golds,
        out=tmp_path / "r.jsonl",
        db_path=db_path,
        docs={},
        max_spend=1.00,
        golds_hash="h",
        run_task_fn=fake_run,
    )
    # First call reserves the $0.183 floor (nothing observed yet); each
    # subsequent reserve is the max observed so far ($0.40). Two calls cost
    # $0.80; a third would reserve another $0.40 on top, projecting $1.20 —
    # over the $1.00 cap — so the guard stops at two.
    assert len(calls) == 2
    assert result.spent <= 1.00
    assert result.truncated is True


def test_sweep_appends_rows_that_can_be_resumed(tmp_path: Path):
    out = tmp_path / "r.jsonl"

    def fake_run(task, arm, model, *a, **k):
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.01,
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
            "verdict": "correct",
        }

    tasks = [
        {"task_id": str(i), "question": "q", "guidelines": "g", "level": "hard"}
        for i in range(10)
    ]
    golds = {str(i): "g" for i in range(10)}
    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)

    for _ in range(4):  # four separate "process" invocations
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

    assert spent_so_far(out) <= 1.00
    assert len(calls) == 2  # only the first invocation's two calls ever ran


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
            raise KeyError("OPENROUTER_API_KEY")
        return {
            "task_id": task["task_id"],
            "arm": arm,
            "model": model,
            "usd": 0.01,
            "verdict": "correct",
        }

    tasks = [
        {"task_id": str(i), "question": "q", "guidelines": "g", "level": "hard"}
        for i in range(2)
    ]
    out = tmp_path / "r.jsonl"
    db_path = _make_pristine(tmp_path)
    sweep(
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
    assert rows[1]["verdict"] == "correct"


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
        TASKS[0], "contract", GLM, "g", "h", KeyError("OPENROUTER_API_KEY")
    )
    assert set(real_row.keys()) <= set(error_row.keys())
    assert error_row["level"] == "hard"


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
    assert golds_hash == "deadbeef"


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


# ── clean-tree guard ─────────────────────────────────────────────────────


def test_assert_clean_tree_raises_on_dirty_tree():
    try:
        assert_clean_tree(git_status_fn=lambda: " M dce/runner.py\n")
        raise AssertionError("expected SystemExit")
    except SystemExit:
        pass


def test_assert_clean_tree_passes_on_clean_tree():
    assert_clean_tree(git_status_fn=lambda: "")  # must not raise


def test_assert_clean_tree_ignores_the_sweep_output_path(tmp_path: Path):
    out = tmp_path / "results" / "r.jsonl"
    porcelain = "?? results/r.jsonl\n?? results/scratch.tmp\n"
    assert_clean_tree(out=out, repo_root=tmp_path, git_status_fn=lambda: porcelain)


def test_assert_clean_tree_still_fails_on_other_dirty_files(tmp_path: Path):
    out = tmp_path / "results" / "r.jsonl"
    porcelain = "?? results/r.jsonl\n M dce/runner.py\n"
    try:
        assert_clean_tree(out=out, repo_root=tmp_path, git_status_fn=lambda: porcelain)
        raise AssertionError("expected SystemExit")
    except SystemExit:
        pass


# ── exit code on truncation ──────────────────────────────────────────────


def test_exit_code_for_truncated_is_nonzero():
    assert _exit_code_for(SweepResult(spent=1.0, truncated=True)) != 0


def test_exit_code_for_complete_is_zero():
    assert _exit_code_for(SweepResult(spent=1.0, truncated=False)) == 0
