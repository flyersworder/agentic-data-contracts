import json
from pathlib import Path

from dce.data import DATASET_REVISION
from dce.runner import (
    _load_golds,
    _next_reserve,
    _working_db_path,
    assert_clean_tree,
    completed_keys,
    pending,
    sweep,
)

TASKS = [{"task_id": "1", "question": "q", "guidelines": "g", "level": "hard"}]


def _make_pristine(tmp_path: Path, name: str = "d") -> Path:
    """A stand-in pristine "database" file. `check_and_restore` only ever
    compares bytes/digests, so a real DuckDB file is not required to
    exercise the runner's working-copy plumbing."""
    path = tmp_path / name
    path.write_bytes(b"pristine-bytes")
    return path


def test_completed_keys_reads_existing_rows(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(
        json.dumps({"task_id": "1", "arm": "contract", "model": "m"}) + "\n"
    )
    assert completed_keys(path) == {("1", "contract", "m")}


def test_completed_keys_on_missing_file_is_empty(tmp_path: Path):
    assert completed_keys(tmp_path / "absent.jsonl") == set()


def test_pending_skips_completed_work():
    done = {("1", "schema_only", "m")}
    todo = pending(TASKS, ("schema_only", "contract"), ("m",), done)
    assert todo == [("1", "contract", "m")]


def test_sweep_stops_before_exceeding_max_spend(tmp_path: Path):
    calls = []

    def fake_run(task, arm, model, *a, **k):
        calls.append(arm)
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
    db_path = _make_pristine(tmp_path)
    spent = sweep(
        tasks,
        ("schema_only",),
        ("z-ai/glm-5.3-flash",),
        {str(i): "g" for i in range(10)},
        out=tmp_path / "r.jsonl",
        db_path=db_path,
        docs={},
        max_spend=1.00,
        run_task_fn=fake_run,
    )
    # Three calls cost 1.20, which overruns; the guard must stop at two.
    assert len(calls) == 2
    assert spent <= 1.00


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
        ("m",),
        {"1": "g"},
        out=out,
        db_path=db_path,
        docs={},
        max_spend=1.0,
        run_task_fn=fake_run,
    )
    assert completed_keys(out) == {("1", "contract", "m")}


def test_sweep_never_opens_the_pristine_db(tmp_path: Path):
    """`run_task_fn` must receive the working copy, never `db_path` itself —
    the ungoverned arms can genuinely write, so nothing may point at the
    pristine file. Verified by capturing what path the sweep actually hands
    the (fake) task runner and asserting it is the working copy, distinct
    from and untouched relative to the pristine file."""
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
        ("m",),
        {"1": "g"},
        out=tmp_path / "r.jsonl",
        db_path=pristine,
        docs={},
        max_spend=1.0,
        run_task_fn=fake_run,
    )

    assert len(seen_db_paths) == 1
    working = seen_db_paths[0]
    assert working != pristine
    assert working == _working_db_path(pristine)
    assert working.exists()
    # The pristine file itself was never written to.
    assert pristine.read_bytes() == pristine_bytes_before


def test_sweep_records_corruption_and_continues(tmp_path: Path):
    """An ungoverned arm mutating the warehouse is a governance finding to
    record, not a crash that loses the evidence: the sweep must stamp
    `db_corrupted` into the row and keep going rather than aborting."""

    def fake_run_that_corrupts(task, arm, model, db_path, docs, gold, **k):
        # Simulate an ungoverned arm's DROP TABLE by mutating the working
        # copy the sweep handed us.
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
    spent = sweep(
        tasks,
        ("schema_only",),
        ("m",),
        {str(i): "g" for i in range(3)},
        out=out,
        db_path=db_path,
        docs={},
        max_spend=10.0,
        run_task_fn=fake_run_that_corrupts,
    )
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(rows) == 3  # the sweep did not abort
    assert all(row["db_corrupted"] is True for row in rows)
    assert spent == 0.03


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
        ("m",),
        {str(i): "g" for i in range(2)},
        out=out,
        db_path=db_path,
        docs={},
        max_spend=10.0,
        run_task_fn=flaky_run,
    )
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert calls == ["0", "1"]  # the second task still ran
    assert len(rows) == 2
    assert rows[0]["verdict"] == "construction_error"
    assert rows[0]["task_id"] == "0"
    assert "db_corrupted" in rows[0]
    assert rows[1]["verdict"] == "correct"


def test_next_reserve_uses_floor_before_any_observation():
    assert _next_reserve(0.0, 0, floor=0.30) == 0.30


def test_next_reserve_uses_running_mean_once_observed():
    assert _next_reserve(0.80, 2, floor=0.30) == 0.40


def test_next_reserve_never_reads_zero_as_free():
    # A run of $0.00 rows (hit_limit/error) must not zero out the guard.
    assert _next_reserve(0.0, 5, floor=0.30) > 0.0


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


def test_assert_clean_tree_raises_on_dirty_tree():
    try:
        assert_clean_tree(git_status_fn=lambda: " M dce/runner.py\n")
        raise AssertionError("expected SystemExit")
    except SystemExit:
        pass


def test_assert_clean_tree_passes_on_clean_tree():
    assert_clean_tree(git_status_fn=lambda: "")  # must not raise
