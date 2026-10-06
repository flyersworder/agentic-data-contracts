# LiveSQLBench benchmark (PR B) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add LiveSQLBench as the harness's second benchmark, with its two
contract-free arms, its grader, its prep scripts and the open items from
PR A's review, and end with a two-arm smoke on `gpt-6-luna`.

**Architecture:**
- **Protocol.** The `Benchmark` protocol gains a task `subset`, construction hooks (`add_arguments`, `from_args`), and report labels.
- **Runner and analysis.** These read only the protocol: the runner builds whichever benchmark `--benchmark` names, scopes results files by benchmark, and samples by largest remainder. `dce.stats` and `knowledge_delivery.py` report per subset and per role.
- **LiveSQLBench code.** `dce/benchmarks/livesqlbench.py` loads tasks, gold results and per-database context from `LSB_DATA`. `dce/benchmarks/lsb_grade.py` grades an agent's final SQL block with the official Soft-EX helpers, vendored under MIT.

**Tech Stack:** Python 3.12, uv, pytest, DuckDB, pydantic-ai, sqlglot (prep
only); lint and types through prek (ruff, ty).

**Spec:** `docs/superpowers/specs/2026-10-06-livesqlbench-harness-design.md`.
This plan implements Part 1's "Open for PR B" list (and "How PR B closes
each") and Part 2 except the compiler, which is PR C. Read both sections
first.

All paths are relative to `experiments/dabstep-contract-eval/` unless they
start with `docs/` or `~`. Run commands from that directory.

## Global Constraints

- Run Python through `uv run`. Prefix every `uv run` with `UV_FROZEN=1`, so `uv.lock` is never rewritten (otherwise `uv run` re-pins the library to 0.59.0, which belongs only in the run commit).
- Lint and type-check through `prek run --all-files`, run from the repo root. Never commit with `--no-verify`.
- No AI attribution anywhere: no `Co-Authored-By: Claude`, no "Generated with", no robot emoji.
- No internal names (employer, cluster, namespace, gateway host) in any committed file, commit message or PR.
- LiveSQLBench gold SQL, test cases, knowledge mappings and frozen gold results are never committed or published. They stay under `LSB_DATA` (default `~/data/livesqlbench`). Rows and traces carry `gold_ref`, a sha256, never the gold itself.
- No credentials in committed files. The prep scripts read the PostgreSQL DSN from `LSB_PG_DSN`.
- New code: `encoding="utf-8"` on every file open, no f-strings without a placeholder, no emojis or Unicode symbols in scripts and tests.
- Match the surrounding code: comments explain *why*, at the density of the module being edited.
- DABStep behaviour and output must not change. Task 1's baselines are compared again in Task 9.
- Do not edit `results/`, `contract/`, `data/` or `traces/`.
- Branch: `livesqlbench-b`. Commit after each task. Push only when the user asks, and to the `fork` remote.

## Review Focus

1. **An answer whose last ```sql block is followed by prose, or that has several blocks.** The grader takes the last block, and prose after it does not matter. The test is in Task 6.
2. **A candidate query that returns the right values in a different column order or with extra columns.** Soft-EX compares row tuples, so it is incorrect. That is the official rule, and a test pins it so nobody "fixes" it. The test is in Task 6.
3. **A task whose database file is missing from `LSB_DATA`.** Loading stops with the missing databases named, before any spend. It must not become 381 `scoring_error` rows. The test is in Task 7.
4. **`--n` on LiveSQLBench gives exactly `n` tasks; `--n 12` draws them from 12 different databases.** The test is in Task 3.
5. **A LiveSQLBench sweep pointed at a DABStep results file.** It is refused before any task runs. The test is in Task 3.

---

### Task 1: Equivalence baselines for DABStep

PR B touches the runner, `dce.stats` and `knowledge_delivery.py`. DABStep's
output must not move, so capture it first. Nothing is committed.

**Files:** none.

- [ ] **Step 1: Capture**

Set `EQ` to an `equiv-b/` folder inside the session scratchpad directory
(the system prompt names it; never `/tmp`), then:

```bash
mkdir -p "$EQ/before"
UV_FROZEN=1 uv run python analysis/arm_surface.py > "$EQ/before/arm_surface.json" 2>/dev/null
for f in results/*.jsonl; do
  UV_FROZEN=1 uv run python -m dce.stats "$f" > "$EQ/before/$(basename "$f").stats" 2>&1
done
UV_FROZEN=1 uv run python analysis/knowledge_delivery.py > "$EQ/before/knowledge_delivery.txt" 2>/dev/null
UV_FROZEN=1 uv run pytest -q 2>&1 | tail -2 > "$EQ/before/pytest.txt"
ls "$EQ/before" | wc -l; cat "$EQ/before/pytest.txt"
```

Expected: 42 files, with one `.stats` file per results file. pytest shows
1 failure, `tests/test_replay.py::test_sql_statements_are_ordered_and_only_from_sql_tools`.
It needs a local `traces/glm-full` directory and is environmental;
deselect it in every later run.

Define once, for the comparisons in Tasks 4, 5 and 9. It drops uv's
install-noise lines and traceback line numbers:

```bash
norm() { grep -vE '^(Uninstalled|Installed) [0-9]+ package' "$1" | sed -E 's/(File ".*", line )[0-9]+/\1N/'; }
```

---

### Task 2: Protocol additions: subset, construction hooks, report labels

**Files:**
- Modify: `dce/benchmark.py`
- Modify: `dce/agent.py` (`task_row_fields`)
- Modify: `dce/benchmarks/dabstep.py`
- Modify: `dce/runner.py` (`_construction_error_row` fallback fields and the `_safe_json_dumps` envelope only)
- Test: `tests/test_benchmark.py`, `tests/test_dabstep.py`, `tests/test_agent.py`

**Interfaces:**
- Produces:
  - `Task(task_id, prompt, group, meta={}, subset: str | None = None)`.
  - New `Benchmark` class attributes: `subsets: tuple[str, ...]`, `task_set_label: str`, `excluded_reason: str`, `excluded_source: str`.
  - New `Benchmark` classmethods: `add_arguments(parser) -> None` and `from_args(args) -> Benchmark`.
  - `task_row_fields` appends `"subset"` only when `task.subset is not None`.
  - On `DABStep`: `subsets = ()`, `task_set_label = "reconstructed-gold task set"`, `excluded_reason = "with a verified-wrong gold"`, `excluded_source = "dce.golds.VERIFIED_WRONG_GOLDS"`. `add_arguments` adds `--db`, `--golds`, `--tasks` and `--ungolded` (moved out of `dce.runner.main`); `from_args` calls `from_files`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_benchmark.py`:

```python
def test_a_task_has_no_subset_unless_given():
    assert Task("1", "q", "g").subset is None
    assert Task("1", "q", "g", subset="primary").subset == "primary"
```

Append to `tests/test_dabstep.py`:

```python
def test_dabstep_report_labels_and_no_subsets():
    assert DABStep.subsets == ()
    assert DABStep.task_set_label == "reconstructed-gold task set"
    assert DABStep.excluded_reason == "with a verified-wrong gold"
    assert DABStep.excluded_source == "dce.golds.VERIFIED_WRONG_GOLDS"


def test_dabstep_builds_from_its_own_command_line_flags(tmp_path: Path, monkeypatch):
    import argparse

    from dce.golds import golds_sha256

    golds = {"1": "a"}
    (tmp_path / "golds.json").write_text(
        json.dumps(
            {
                "revision": DATASET_REVISION,
                "threshold": PLURALITY_THRESHOLD,
                "golds": golds,
                "golds_sha256": golds_sha256(golds),
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "tasks.json").write_text(
        json.dumps([{"task_id": "1", "question": "q", "level": "easy"}]),
        encoding="utf-8",
    )
    ctx = tmp_path / "data" / "hf" / "data" / "context"
    ctx.mkdir(parents=True)
    (ctx / "manual.md").write_text("M", encoding="utf-8")
    (ctx / "payments-readme.md").write_text("R", encoding="utf-8")

    parser = argparse.ArgumentParser()
    DABStep.add_arguments(parser)
    args = parser.parse_args(
        [
            "--db",
            str(tmp_path / "d.duckdb"),
            "--golds",
            str(tmp_path / "golds.json"),
            "--tasks",
            str(tmp_path / "tasks.json"),
        ]
    )
    assert args.ungolded == "skip"
    monkeypatch.chdir(tmp_path)  # CONTEXT_DIR is relative to the experiment directory
    bench = DABStep.from_args(args)
    assert [t.task_id for t in bench.tasks()] == ["1"]
```

Append to `tests/test_agent.py`:

```python
def test_a_subset_is_stamped_after_group_and_only_when_set():
    import dataclasses

    from dce.agent import task_row_fields

    plain = task_row_fields(TASK_OBJ, _bench())
    assert "subset" not in plain
    tagged = task_row_fields(dataclasses.replace(TASK_OBJ, subset="primary"), _bench())
    assert list(tagged)[-1] == "subset" and tagged["subset"] == "primary"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_benchmark.py tests/test_dabstep.py tests/test_agent.py -k "subset or labels or own_command" 2>&1 | tail -5`
Expected: 4 FAIL (`TypeError` on `subset=`, `AttributeError` on the new
attributes, no `add_arguments`).

- [ ] **Step 3: Implement**

`dce/benchmark.py`:
- Add `import argparse`.
- Give `Task` a last field: `subset: str | None = None  # a separately reported task set, if any`.
- Add to the `Benchmark` class attributes:

```python
    subsets: tuple[str, ...]  # separately reported task sets, pre-registered first
    task_set_label: str  # how the report names the task set it scores
    excluded_reason: str  # why `excluded_tasks` are dropped, for the report
    excluded_source: str  # where the excluded list is kept
```

- Add to the methods:

```python
    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None: ...
    @classmethod
    def from_args(cls, args: argparse.Namespace) -> Benchmark: ...
```

- Extend the module docstring with one sentence: "`add_arguments`/`from_args` let `dce.runner` build whichever benchmark `--benchmark` names; each adds only its own flags, so flag names must not collide across benchmarks."

`dce/agent.py`, `task_row_fields`:

```python
    fields = {
        "task_id": task.task_id,
        **benchmark.row_fields(task),
        "benchmark": benchmark.name,
        "group": task.group,
    }
    # A separately reported task set (LiveSQLBench's primary / order-only).
    # Absent rather than null when unset, so a DABStep row's keys are unchanged.
    if task.subset is not None:
        fields["subset"] = task.subset
    return fields
```

`dce/benchmarks/dabstep.py`:
- Add `import argparse`.
- Add on `DABStep`, after `excluded_tasks`:

```python
subsets: tuple[str, ...] = ()
task_set_label = "reconstructed-gold task set"
excluded_reason = "with a verified-wrong gold"
excluded_source = "dce.golds.VERIFIED_WRONG_GOLDS"


@classmethod
def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--db", type=Path, default=Path("data/dabstep.duckdb"), help="(dabstep)"
    )
    parser.add_argument(
        "--golds", type=Path, default=Path("data/golds.json"), help="(dabstep)"
    )
    parser.add_argument(
        "--tasks", type=Path, default=Path("data/tasks.json"), help="(dabstep)"
    )
    parser.add_argument(
        "--ungolded",
        choices=UNGOLDED_MODES,
        default="skip",
        help=(
            "(dabstep) what to do with tasks that have no reconstructed "
            "gold: skip them (default, every scoring sweep) or run them "
            "unscored, which a leaderboard submission needs"
        ),
    )


@classmethod
def from_args(cls, args: argparse.Namespace) -> DABStep:
    return cls.from_files(
        db=args.db,
        golds_path=args.golds,
        tasks_path=args.tasks,
        ungolded=args.ungolded,
    )
```

`dce/runner.py`:
- In `_construction_error_row`'s fallback `task_fields` dict, add
  `**({"subset": task.subset} if task.subset is not None else {}),` after `"group"`.
- In `_safe_json_dumps`, extend the loop to `for key in ("benchmark", "group", "subset"):`.
  It already writes `None` for a missing key, which every reader treats as absent.

Do not touch `dce.runner.main` in this task; Task 3 moves the flags.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest -q --deselect tests/test_replay.py::test_sql_statements_are_ordered_and_only_from_sql_tools 2>&1 | tail -2`
Expected: all pass. `main()` still defines DABStep's flags itself, and
`DABStep.add_arguments` is not called yet, so there is no clash.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/benchmark.py dce/agent.py dce/benchmarks/dabstep.py dce/runner.py tests/test_benchmark.py tests/test_dabstep.py tests/test_agent.py
git commit -m "dce: a task subset, construction hooks and report labels on the Benchmark"
```

---

### Task 3: Runner: build any benchmark, scope results by benchmark, largest-remainder sampling

**Files:**
- Modify: `dce/runner.py`
- Test: `tests/test_runner.py`

**Interfaces:**
- Consumes:
  - `Benchmark.add_arguments`/`from_args` (Task 2);
  - `benchmark_class`, `benchmark_of`, `BENCHMARK_NAMES` and `DEFAULT_BENCHMARK` (`dce.benchmark`).
- Produces:
  - `default_out(name: str) -> Path`;
  - `default_trace_dir(name: str, out: Path) -> Path`;
  - `_assert_one_benchmark(out: Path, name: str) -> None`, raising `SystemExit`;
  - `_stratified_sample` by largest remainder;
  - `main()` builds its benchmark with `benchmark_class(args.benchmark).from_args(args)`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_runner.py`:

```python
def test_stratified_sample_gives_exactly_n_over_many_groups():
    """22 databases, `--n 12`: rounding each group's share gave 22 tasks or
    none; the largest-remainder rule gives 12, one from each of the 12
    groups with the largest remainders."""
    records = [
        {"task_id": f"{g}_{i}", "question": "q", "level": f"db{g:02d}"}
        for g in range(22)
        for i in range(10 + g % 7)
    ]
    for n in (1, 5, 12, 22, 30, 100):
        sampled = _stratified_sample(_tasks(records), n)
        assert len(sampled) == n
    picked = _stratified_sample(_tasks(records), 12)
    assert len({t.group for t in picked}) == 12


def test_stratified_sample_matches_rounding_on_two_groups_without_a_tie():
    """DABStep's two levels: largest remainder and rounding agree except on
    an exact .5 tie, where rounding returned n - 1 or n + 1 tasks."""
    records = [
        {"task_id": str(i), "question": "q", "level": "hard" if i < 336 else "easy"}
        for i in range(406)
    ]
    for n in range(1, 60):
        hard, easy = n * 336 / 406, n * 70 / 406
        if hard % 1 == 0.5:
            continue
        picked = _stratified_sample(_tasks(records), n)
        assert sum(t.group == "hard" for t in picked) == round(hard)
        assert sum(t.group == "easy" for t in picked) == round(easy)


def test_default_results_and_traces_are_scoped_by_benchmark():
    from dce.runner import default_out, default_trace_dir

    assert default_out("dabstep") == Path("results/results.jsonl")
    assert default_out("livesqlbench") == Path("results/livesqlbench/results.jsonl")
    assert default_trace_dir("dabstep", Path("results/x.jsonl")) == Path("traces/x")
    assert default_trace_dir(
        "livesqlbench", Path("results/livesqlbench/smoke.jsonl")
    ) == Path("traces/livesqlbench/smoke")


def test_a_results_file_of_another_benchmark_is_refused_before_any_task(
    tmp_path: Path,
):
    out = tmp_path / "r.jsonl"
    out.write_text(
        json.dumps({"task_id": "9", "arm": "a", "model": GLM, "verdict": "correct"})
        + "\n",
        encoding="utf-8",
    )  # no `benchmark` field: a DABStep row

    class Other(DABStep):
        name = "other"

    calls = []

    def fake_run(task, arm, model, *a, **k):
        calls.append(task.task_id)
        return _ok_row(task, arm, model)

    bench = Other(db=_make_pristine(tmp_path), golds={}, golds_hash="h", docs={})
    with pytest.raises(SystemExit, match="dabstep"):
        sweep(
            _tasks(TASKS),
            ("schema_only",),
            (GLM,),
            bench,
            out=out,
            max_spend=5.0,
            run_task_fn=fake_run,
        )
    assert calls == []
```

`_ok_row` and `_tasks` already exist in `tests/test_runner.py` (PR A).
`json` and `pytest` are imported at its top.

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_runner.py -k "exactly_n or without_a_tie or scoped_by or another_benchmark" 2>&1 | tail -6`
Expected:
- `exactly_n`, `scoped_by` and `another_benchmark` fail;
- `without_a_tie` may already pass, since it pins behaviour that must hold after the change.

- [ ] **Step 3: Implement**

`_stratified_sample` body (keep the signature; update the docstring's
first paragraph to "split across groups by the largest-remainder rule"):

```python
if n <= 0 or n >= len(tasks):
    return tasks
by_group: dict[str, list[Task]] = {}
for task in tasks:
    by_group.setdefault(task.group, []).append(task)
total = len(tasks)
# Largest remainder: every group gets the floor of its share, and the
# tasks left over go to the largest fractional parts (first-seen group
# first on a tie). Rounding each share instead gave 22 tasks or none for
# `--n 12` over LiveSQLBench's 22 databases, and n - 1 or n + 1 on any
# exact .5 tie. On DABStep's two levels the two rules agree everywhere
# else, so its smoke samples are unchanged.
quotas = {group: n * len(members) / total for group, members in by_group.items()}
counts = {group: math.floor(quota) for group, quota in quotas.items()}
first_seen = {group: index for index, group in enumerate(by_group)}
leftover = n - sum(counts.values())
for group in sorted(by_group, key=lambda g: (-(quotas[g] - counts[g]), first_seen[g]))[
    :leftover
]:
    counts[group] += 1
sampled: list[Task] = []
for group, members in by_group.items():
    sampled.extend(members[: counts[group]])
return sampled
```

Add near `snapshot_path_for`:

```python
def default_out(name: str) -> Path:
    """DABStep keeps its historical default; every other benchmark writes
    under `results/<name>/`, so two benchmarks never share a default file."""
    if name == DEFAULT_BENCHMARK:
        return Path("results/results.jsonl")
    return Path("results") / name / "results.jsonl"


def default_trace_dir(name: str, out: Path) -> Path:
    """Off the results filename, so two sweeps writing different results
    files cannot interleave their transcripts; under `traces/<name>/` for
    every benchmark but DABStep, whose `traces/<stem>/` FINDINGS.md cites."""
    if name == DEFAULT_BENCHMARK:
        return Path("traces") / out.stem
    return Path("traces") / name / out.stem


def _assert_one_benchmark(out: Path, name: str) -> None:
    """A results file belongs to one benchmark. Appending another's rows
    would pay for runs that `dce.stats` then refuses to report, and resume
    would skip any task whose id happens to collide."""
    others = sorted({benchmark_of(row) for row in _read_rows(out)} - {name})
    if others:
        raise SystemExit(
            f"{out} already holds rows from {others}, not {name!r}; a results "
            "file belongs to one benchmark -- pass a different --out"
        )
```

Import `DEFAULT_BENCHMARK` and `benchmark_of` from `dce.benchmark`.
Call `_assert_one_benchmark(out, benchmark.name)` as the first statement
inside `sweep`'s `with sweep_lock(out):`, before `_sweep_locked`.

`main()`:
- Delete the `--db`, `--golds`, `--tasks` and `--ungolded` `add_argument` calls.
- After the `--retry-pass` argument, add:

```python
    # Each benchmark adds its own flags (DABStep's --db/--golds/--tasks/
    # --ungolded, LiveSQLBench's --lsb-data); `from_args` reads only its own.
    for name in BENCHMARK_NAMES:
        benchmark_class(name).add_arguments(parser)
```

- `--out` gets `default=None` and the help text `"default: results/results.jsonl for dabstep, results/<benchmark>/results.jsonl otherwise"`.
- `--traces` gets the help text `"... (default: traces/<out-stem>/ for dabstep, traces/<benchmark>/<out-stem>/ otherwise) ..."`, keeping the rest of the sentence.
- After `args = parser.parse_args()`, add `out = args.out or default_out(args.benchmark)`. Replace every later `args.out` with `out`: in `assert_clean_tree`, in the `sweep` call, and in `gave_up_keys`.
- Replace `benchmark = DABStep.from_files(...)` with `benchmark = spec.from_args(args)`.
- The trace dir: `trace_dir = None if args.no_traces else (args.traces or default_trace_dir(args.benchmark, out))`.
- Remove the now-unused `DABStep` and `UNGOLDED_MODES` imports from `dce.runner`. `dce.benchmarks.dabstep` still owns `UNGOLDED_MODES`.

- [ ] **Step 4: Run the tests and a DABStep sample check on the real tasks**

Run: `UV_FROZEN=1 uv run pytest -q --deselect tests/test_replay.py::test_sql_statements_are_ordered_and_only_from_sql_tools 2>&1 | tail -2`
Expected: all pass.

```bash
UV_FROZEN=1 uv run python - <<'EOF' 2>/dev/null
import math
from pathlib import Path
from dce.benchmarks.dabstep import DABStep
from dce.runner import _stratified_sample

def old(tasks, n):
    if n <= 0 or n >= len(tasks):
        return tasks
    by = {}
    for t in tasks:
        by.setdefault(t.group, []).append(t)
    out = []
    for g in by.values():
        out.extend(g[: round(n * len(g) / len(tasks))])
    return out

for ungolded in ("skip", "run"):
    tasks = DABStep.from_files(db=Path("data/dabstep.duckdb"), golds_path=Path("data/golds.json"), tasks_path=Path("data/tasks.json"), ungolded=ungolded).tasks()
    for n in (1, 5, 10, 12, 20, 37, 100):
        same = [t.task_id for t in old(tasks, n)] == [t.task_id for t in _stratified_sample(tasks, n)]
        print(ungolded, n, "same" if same else "DIFFERENT")
EOF
```

Expected: every line says `same`. A `DIFFERENT` line is acceptable only
where the old rule hit an exact .5 tie (its sample size is then `n - 1` or
`n + 1`). Record any such `n` in the ledger as a ruling.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/runner.py tests/test_runner.py
git commit -m "dce.runner: build any benchmark, scope results by benchmark, sample by largest remainder"
```

---

### Task 4: dce.stats: subsets, a missing contract role, raw-row mixing, honest wording

**Files:**
- Modify: `dce/stats.py`
- Test: `tests/test_stats.py`

**Interfaces:**
- Consumes: `subsets`, `task_set_label`, `excluded_reason` and `excluded_source` (Task 2).
- Produces:
  - `report(path, *, rescore_stale=True)`, with an unchanged signature;
  - `_benchmark_for(rows, raw_rows=())`;
  - a new helper `_secondary_lines(rows, raw_rows, bench, heading) -> list[str]`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_stats.py`:

```python
def _bench_like(**attrs):
    from dce.benchmarks.dabstep import DABStep

    return type("Bench", (DABStep,), attrs)


def test_each_subset_is_reported_in_its_own_section_in_declared_order(
    tmp_path, monkeypatch
):
    import dce.stats as stats_module

    bench = _bench_like(
        subsets=("primary", "order_only"),
        roles={"baseline": "schema_only", "manual": "manual_prompt"},
        primary=None,
    )
    monkeypatch.setattr(stats_module, "benchmark_class", lambda name: bench)
    path = tmp_path / "r.jsonl"
    _write(
        path,
        [
            {**_row("t1", "schema_only", "correct"), "subset": "order_only"},
            {**_row("t2", "schema_only", "incorrect"), "subset": "primary"},
            {**_row("t2", "manual_prompt", "correct"), "subset": "primary"},
        ],
    )
    text = report(path)
    assert text.index("## subset: primary") < text.index("## subset: order_only")
    primary = text[
        text.index("## subset: primary") : text.index("## subset: order_only")
    ]
    assert "schema_only" in primary and "manual_prompt" in primary
    # No `contract` role, so no pairwise comparison against it.
    assert "McNemar" not in text


def test_a_benchmark_mixed_in_only_among_superseded_rows_is_still_refused(tmp_path):
    path = tmp_path / "r.jsonl"
    _write(
        path,
        [
            {**_row("t1", PRIMARY_LEFT_ARM, "correct"), "benchmark": "other"},
            _row("t1", PRIMARY_LEFT_ARM, "correct"),  # same key, supersedes it
        ],
    )
    with pytest.raises(SystemExit, match="more than one benchmark"):
        report(path)


def test_the_regrade_note_counts_only_the_rows_actually_regraded(tmp_path, monkeypatch):
    import dce.stats as stats_module

    bench = _bench_like(
        scorer=staticmethod(lambda: "new-scorer"),
        rescore=staticmethod(lambda row: None),
    )
    monkeypatch.setattr(stats_module, "benchmark_class", lambda name: bench)
    path = tmp_path / "r.jsonl"
    _write(path, [{**_row("t1", PRIMARY_LEFT_ARM, "correct"), "scorer": "old"}])
    text = report(path)
    assert "0 of 1 answered row(s) were RE-GRADED" in text
    assert "every answered row has been RE-GRADED" not in text


def test_the_excluded_task_note_names_the_benchmarks_reason(tmp_path, monkeypatch):
    import dce.stats as stats_module

    bench = _bench_like(
        excluded_tasks=frozenset({"t1"}),
        excluded_reason="whose gold failed to reproduce",
        excluded_source="the freeze verdicts",
    )
    monkeypatch.setattr(stats_module, "benchmark_class", lambda name: bench)
    path = tmp_path / "r.jsonl"
    _write(
        path,
        [
            _row("t1", PRIMARY_LEFT_ARM, "correct"),
            _row("t2", PRIMARY_LEFT_ARM, "correct"),
        ],
    )
    text = report(path)
    assert (
        "NOTE: dropped 1 task(s) whose gold failed to reproduce (t1) -- see the freeze verdicts."
        in text
    )
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_stats.py -k "subset or superseded or regrade_note or excluded_task_note" 2>&1 | tail -6`
Expected: 4 FAIL.

- [ ] **Step 3: Implement**

1. `_benchmark_for(rows, raw_rows=())`: compute `names` from
   `itertools.chain(rows, raw_rows)`. `report` calls it as
   `_benchmark_for(rows, raw_rows)`. Update the docstring: superseded rows
   count, because the file is what is being reported.

2. The excluded note:

```python
        lines.append(
            f"NOTE: dropped {len(dropped)} task(s) {bench.excluded_reason} "
            f"({', '.join(dropped)}) -- see {bench.excluded_source}."
        )
```

   For DABStep this prints exactly the old text.

3. The re-grade note, inside `if rescore_stale:`:

```python
            answered = [r for r in rows if r.get("verdict") in ANSWER_VERDICTS]
            rescored = rescore(rows)
            # `rescore` hands back the same object for a row it could not
            # re-grade (its benchmark keeps no gold in the row), so identity
            # counts the rows that really were re-graded.
            regraded = sum(
                1
                for before, after in zip(rows, rescored)
                if before is not after and before.get("verdict") in ANSWER_VERDICTS
            )
            rows = rescored
            if regraded == len(answered):
                lines.append(  # the existing NOTE text, unchanged
                    f"NOTE: {detail} were graded by a scorer other than the one "
                    f"installed now ({bench.scorer()!r}); every answered row has "
                    "been RE-GRADED from its stored answer and gold. Pass "
                    "rescore_stale=False to report the stored verdicts verbatim."
                )
            else:
                lines.append(
                    f"NOTE: {detail} were graded by a scorer other than the one "
                    f"installed now ({bench.scorer()!r}); {regraded} of "
                    f"{len(answered)} answered row(s) were RE-GRADED from their "
                    "stored answer and gold, and the rest keep their recorded "
                    "verdicts: this benchmark cannot re-grade from a stored row."
                )
```

4. The primary header: replace the literal `"paired McNemar, reconstructed-gold task set"`
   with `f"paired McNemar, {bench.task_set_label}"`.

5. Move the whole `for model in sorted(...)` loop of the secondary section
   into a helper. Its body is unchanged except for the three lines shown:

```python
def _secondary_lines(rows, raw_rows, bench, heading: str) -> list[str]:
    """Per model: every arm's summary and strata, the lopsided-arms warning,
    and the pairwise tests. `heading` is the Markdown level of each model's
    title (`##`, or `###` inside a subset section)."""
    lines: list[str] = []
    primary_model = bench.primary[0] if bench.primary else None
    for model in sorted({row.get("model", "unknown") for row in rows}):
        lines.append(f"\n{heading} {model}")
        ...  # the existing per-model body, verbatim
        contract = bench.roles.get("contract")
        if contract is not None:
            comparison = tuple(a for a in bench.arms if a != contract)
            ...  # the existing comparison loop, verbatim
        ...  # the existing manual / manual_plus pair, verbatim
    return lines
```

   `primary_model` is used where the old loop compared against the
   primary. Without a `contract` role there is nothing to compare each arm
   against, so the per-arm summaries stand alone; that is LiveSQLBench
   until PR C.

   In `report`:

```python
    lines.append("\n# SECONDARY / EXPLORATORY (not pre-registered)")
    present = {row.get("subset") for row in rows} - {None}
    if bench.subsets and present:
        # Declared order first (the pre-registered subset leads), then any
        # subset a row carries that the benchmark does not declare.
        order = [s for s in bench.subsets if s in present] + sorted(
            present - set(bench.subsets)
        )
        for subset in order:
            lines.append(f"\n## subset: {subset}")
            lines.extend(
                _secondary_lines(
                    [r for r in rows if r.get("subset") == subset],
                    [r for r in raw_rows if r.get("subset") == subset],
                    bench,
                    "###",
                )
            )
    else:
        lines.extend(_secondary_lines(rows, raw_rows, bench, "##"))
    return "\n".join(lines)
```

   Add `import itertools` at the top.

- [ ] **Step 4: Run the tests and the DABStep equivalence**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_stats.py tests/test_runner.py 2>&1 | tail -2`
Expected: all pass.

```bash
mkdir -p "$EQ/after"
for f in results/*.jsonl; do
  UV_FROZEN=1 uv run python -m dce.stats "$f" > "$EQ/after/$(basename "$f").stats" 2>&1
done
n=0; for f in "$EQ"/before/*.stats; do b=$(basename "$f"); diff -q <(norm "$EQ/before/$b") <(norm "$EQ/after/$b") >/dev/null || { echo "DIFF $b"; n=$((n+1)); }; done; echo "differing: $n"
```

Expected: `differing: 0`.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/stats.py tests/test_stats.py
git commit -m "dce.stats: per-subset sections, no comparison without a contract role, honest notes"
```

---

### Task 5: knowledge_delivery.py: pairs by role, subset, excluded tasks

**Files:**
- Modify: `analysis/knowledge_delivery.py`

**Interfaces:**
- Consumes: `role_arms` and `benchmark_class` (`dce.benchmark`); `excluded_tasks`.
- Produces: `CONFIG[name]` gains:
  - `"roles"`, a tuple of three roles printed as columns;
  - `"pairs"`, a tuple of `(better_role, worse_role)`;
  - `"subset"`, a `str` or `None`.

  PR C adds LiveSQLBench's entry: subset `primary`, and pairs including `("contract", "baseline")`.

- [ ] **Step 1: Restructure**

1. The DABStep `CONFIG` entry gains:

```python
        "roles": ("manual", "manual_plus", "contract"),
        "pairs": (("manual_plus", "manual"), ("contract", "manual_plus")),
        "subset": None,
```

2. Replace `arms_of` with:

```python
def arms_of(name: str) -> dict[str, str]:
    """role -> arm, for every role this benchmark's report reads. A role the
    benchmark lacks stops the report by name (`role_arms`): a skipped role
    would silently drop a pre-registered comparison."""
    needed = list(CONFIG[name]["roles"])
    for pair in CONFIG[name]["pairs"]:
        needed.extend(r for r in pair if r not in needed)
    return dict(zip(needed, role_arms(benchmark_class(name), *needed)))
```

3. `rows(name, model, repeat, arms)`: after loading each file's rows,
   drop `excluded_tasks` and keep only `CONFIG[name]["subset"]` when it is
   set:

```python
    bench = benchmark_class(name)
    subset = CONFIG[name]["subset"]
    ...
        for row in _as_e2e(_graded(load(ROOT / "results" / f"{stem}.jsonl"))):
            if row["arm"] not in arms or row["task_id"] in bench.excluded_tasks:
                continue
            if subset is not None and row.get("subset") != subset:
                continue
            out.append({**row, "_traces": ROOT / "traces" / stem})
```

4. In `report(name, model)`:

```python
    by_role = arms_of(name)
    manual, plus, contract = (by_role[r] for r in CONFIG[name]["roles"])
    arms = (manual, plus, contract)
    ...
    pairs = tuple((by_role[a], by_role[b]) for a, b in CONFIG[name]["pairs"])
```

   Everything else in `report` is unchanged. The share line still reads
   `manual`, `plus` and `contract`.

5. Docstring: replace "its runs, how a row maps to a group, and the groups"
   with "its runs, how a row maps to a group, the groups, the roles it
   prints, the role pairs it sign-tests, and the subset it reads".

- [ ] **Step 2: Verify the output is identical**

```bash
UV_FROZEN=1 uv run python analysis/knowledge_delivery.py > "$EQ/after/knowledge_delivery.txt" 2>/dev/null
diff <(norm "$EQ/before/knowledge_delivery.txt") "$EQ/after/knowledge_delivery.txt" && echo KD-SAME
```

Expected: `KD-SAME`. This is the test for this task: `knowledge_delivery.py`
reads the local results and traces, so a unit test would need its whole
input set. Its pairs, roles and subset are covered by identical output for
DABStep and, in PR C, by LiveSQLBench's entry.

- [ ] **Step 3: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add analysis/knowledge_delivery.py
git commit -m "analysis: knowledge_delivery pairs by role, reads one subset, drops excluded tasks"
```

---

### Task 6: Vendored Soft-EX helpers and the LiveSQLBench grader

**Files:**
- Create: `vendor/livesqlbench_test_utils.py`
- Modify: `vendor/README.md` (new section)
- Create: `dce/benchmarks/lsb_grade.py`
- Test: `tests/test_vendored_lsb.py`, `tests/test_lsb_grade.py`

**Interfaces:**
- Consumes: `Grade` (`dce.benchmark`); `HARNESS_MEMORY_LIMIT` (`dce.tools`).
- Produces, in `dce.benchmarks.lsb_grade`:
  - constants `PG_COMPAT: tuple[str, ...]` and `GRADE_SECONDS: float = 120`;
  - `extract_sql(answer: str | None) -> str | None`;
  - `clean(sqls: list[str]) -> list[str]`;
  - `normalise(rows) -> list[tuple]`;
  - `matches(gold: list[tuple], got: list[tuple], ordered: bool) -> bool`;
  - `gold_digest(rows) -> str`;
  - `class GradeTimeout(Exception)`;
  - `connect_readonly(db_path: Path, *, memory_limit: str | None = HARNESS_MEMORY_LIMIT)`;
  - `run_sql(con, sqls: list[str], *, seconds: float) -> list[tuple]`;
  - `grade_answer(answer, *, db_path: Path, gold_rows, ordered: bool, seconds: float = GRADE_SECONDS) -> Grade`.

- [ ] **Step 1: Vendor the helpers**

Run from the experiment directory. It writes the vendored file from
upstream at the pinned commit:

```bash
UV_FROZEN=1 uv run python - <<'EOF'
import ast, base64, hashlib, json, subprocess
from pathlib import Path

COMMIT = "5aab9623d6ce58d32e252f8c307f08fbcbdf4a70"
blob = json.loads(subprocess.check_output(["gh", "api", f"repos/bird-bench/livesqlbench/contents/evaluation/src/test_utils.py?ref={COMMIT}"]))
src = base64.b64decode(blob["content"]).decode("utf-8")
tree = ast.parse(src)
want = ["process_decimals_recursive", "preprocess_results", "remove_distinct", "remove_comments", "remove_round_functions", "remove_round"]
segs = {n.name: ast.get_source_segment(src, n) for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in want}
assert sorted(segs) == sorted(want), sorted(segs)
licence = base64.b64decode(json.loads(subprocess.check_output(["gh", "api", "repos/bird-bench/livesqlbench/license"]))["content"]).decode("utf-8")
header = (
    '"""LiveSQLBench\'s official Soft-EX helpers -- VENDORED VERBATIM. Do not edit.\n\n'
    "Six functions copied byte for byte from bird-bench/livesqlbench,\n"
    f"evaluation/src/test_utils.py at commit {COMMIT}. The\n"
    "upstream module also imports a PostgreSQL driver and its own database\n"
    "helpers, which grading on DuckDB does not use, so only these functions and\n"
    "the standard-library imports they need are kept. tests/test_vendored_lsb.py\n"
    "re-asserts each function's sha256.\n\n"
    "Upstream licence follows.\n\n"
    + "\n".join(("    " + line).rstrip() for line in licence.strip().splitlines())
    + '\n"""\n\n'
    "import json\nimport logging\nimport re\nfrom datetime import date, datetime\n"
    "from decimal import ROUND_HALF_UP, Decimal\n\n\n"
)
body = "\n\n\n".join(segs[n] for n in want) + "\n"
Path("vendor/livesqlbench_test_utils.py").write_text(header + body, encoding="utf-8")
for n in want:
    print(n, hashlib.sha256(segs[n].encode("utf-8")).hexdigest())
EOF
```

Expected, the six hashes (the same as the local copy in
`~/data/livesqlbench/official-eval`):

```
process_decimals_recursive a8d6369d031af19ab83abbf264a4a252ef8af68f460c50192a264451a10a458a
preprocess_results eac3ffe7926d70c7aa5c487daa0bad38e49350c6edab85c8824eb0b2cc4ba8f1
remove_distinct 5d69bd22fd6611da54200d0a9d5aee4747e9a7b882fe238a86b1cdde7df7deeb
remove_comments 0fd625600ab4338141caf97eace39525ce1687b224650b7b58be2fd183c02ed3
remove_round_functions 8379355bbc5cea56697a31203d08bafb8b574b1894fc63985927e188f3dcfc6f
remove_round 7ad9b6e73963e0e276dc6bbb573fa33a64ce98d94eb1e69fb5e57578de93075e
```

If ruff later reformats this file, exclude it from ruff the way
`vendor/dabstep_scorer.py` is excluded; check `pyproject.toml` and
`.pre-commit-config.yaml` for that exclusion. The bodies must stay byte
for byte.

Add to `vendor/README.md` a section `## livesqlbench_test_utils.py` with:
- Source: `https://github.com/bird-bench/livesqlbench`, `evaluation/src/test_utils.py`.
- Upstream commit: `5aab9623d6ce58d32e252f8c307f08fbcbdf4a70`.
- Licence: MIT, `Copyright (c) 2024 bird_sql`, reproduced in the file header.
- Why vendored: the grader must apply the official Soft-EX normalisation, and the upstream module cannot be imported without a PostgreSQL driver and the benchmark's database helpers.
- What is kept: the six functions, and nothing else.

- [ ] **Step 2: Write the failing tests**

`tests/test_vendored_lsb.py`:

```python
"""The vendored LiveSQLBench helpers are upstream's, byte for byte."""

import ast
import hashlib
from pathlib import Path

VENDOR = Path(__file__).resolve().parents[1] / "vendor" / "livesqlbench_test_utils.py"

UPSTREAM_COMMIT = "5aab9623d6ce58d32e252f8c307f08fbcbdf4a70"
UPSTREAM_SHA256 = {
    "process_decimals_recursive": "a8d6369d031af19ab83abbf264a4a252ef8af68f460c50192a264451a10a458a",
    "preprocess_results": "eac3ffe7926d70c7aa5c487daa0bad38e49350c6edab85c8824eb0b2cc4ba8f1",
    "remove_distinct": "5d69bd22fd6611da54200d0a9d5aee4747e9a7b882fe238a86b1cdde7df7deeb",
    "remove_comments": "0fd625600ab4338141caf97eace39525ce1687b224650b7b58be2fd183c02ed3",
    "remove_round_functions": "8379355bbc5cea56697a31203d08bafb8b574b1894fc63985927e188f3dcfc6f",
    "remove_round": "7ad9b6e73963e0e276dc6bbb573fa33a64ce98d94eb1e69fb5e57578de93075e",
}


def test_every_vendored_function_is_upstream_byte_for_byte():
    src = VENDOR.read_text(encoding="utf-8")
    found = {
        node.name: hashlib.sha256(
            ast.get_source_segment(src, node).encode("utf-8")
        ).hexdigest()
        for node in ast.parse(src).body
        if isinstance(node, ast.FunctionDef)
    }
    assert found == UPSTREAM_SHA256


def test_the_header_names_the_commit_and_the_licence():
    head = VENDOR.read_text(encoding="utf-8")[:2000]
    assert UPSTREAM_COMMIT in head
    assert "MIT License" in head
```

`tests/test_lsb_grade.py`:

```python
from pathlib import Path

import duckdb
import pytest
from dce.benchmark import Grade
from dce.benchmarks.lsb_grade import (
    extract_sql,
    gold_digest,
    grade_answer,
    matches,
)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    path = tmp_path / "g.duckdb"
    con = duckdb.connect(str(path))
    con.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES "
        "(1, 'a', 10.004, NULL::JSON), (2, 'b', NULL, '{\"k\": [1, 2]}'), "
        "(3, 'c', 7.0, NULL)) v(id, name, amount, doc)"
    )
    con.close()
    return path


def _answer(sql: str) -> str:
    return f"Here is the query.\n\n```sql\n{sql}\n```"


def test_extract_takes_the_last_sql_block_and_ignores_prose_after_it():
    text = "```sql\nSELECT 1\n```\nthen\n```SQL\nSELECT 2;\n```\nThat is all."
    assert extract_sql(text) == "SELECT 2;"
    assert extract_sql("no block here") is None
    assert extract_sql("```sql\n   \n```") is None
    assert extract_sql("```python\nprint(1)\n```") is None


def test_no_block_is_no_sql(db):
    grade = grade_answer("The answer is 3.", db_path=db, gold_rows=[[3]], ordered=False)
    assert grade == Grade("incorrect", failure="no_sql")


def test_an_engine_error_is_sql_error(db):
    grade = grade_answer(
        _answer("SELECT nope FROM t"), db_path=db, gold_rows=[[1]], ordered=False
    )
    assert grade == Grade("incorrect", failure="sql_error")


def test_a_runaway_query_is_a_timeout(db):
    grade = grade_answer(
        _answer("SELECT count(*) FROM range(100000000000) a"),
        db_path=db,
        gold_rows=[[1]],
        ordered=False,
        seconds=0.2,
    )
    assert grade == Grade("incorrect", failure="timeout")


def test_order_counts_only_when_the_task_is_ordered(db):
    gold = [["a"], ["b"], ["c"]]
    desc = _answer("SELECT name FROM t ORDER BY id DESC")
    assert grade_answer(desc, db_path=db, gold_rows=gold, ordered=False) == Grade(
        "correct"
    )
    assert grade_answer(desc, db_path=db, gold_rows=gold, ordered=True) == Grade(
        "incorrect"
    )


def test_postgres_integer_division_and_null_order_apply(db):
    # integer_division: 7 / 2 is 3, as in PostgreSQL.
    assert grade_answer(
        _answer("SELECT 7 / 2"), db_path=db, gold_rows=[[3]], ordered=False
    ) == Grade("correct")
    # DESC puts NULLs first, as in PostgreSQL.
    gold = [[None], [10.0], [7.0]]
    got = _answer("SELECT amount FROM t ORDER BY amount DESC")
    assert grade_answer(got, db_path=db, gold_rows=gold, ordered=True) == Grade(
        "correct"
    )


def test_round_and_distinct_are_stripped_and_numbers_compare_at_two_places(db):
    # ROUND(amount, 0) is stripped to `amount`, which rounds to 10.0 at two places.
    got = _answer("SELECT DISTINCT ROUND(amount, 0) FROM t WHERE id = 1")
    assert grade_answer(got, db_path=db, gold_rows=[[10.0]], ordered=False) == Grade(
        "correct"
    )


def test_json_text_is_parsed_before_comparing(db):
    got = _answer("SELECT doc FROM t WHERE id = 2")
    assert grade_answer(
        got, db_path=db, gold_rows=[['{"k": [1, 2]}']], ordered=False
    ) == Grade("correct")


def test_extra_or_reordered_columns_are_wrong_as_in_the_official_rule(db):
    gold = [["a", 1]]
    assert grade_answer(
        _answer("SELECT id, name FROM t WHERE id = 1"),
        db_path=db,
        gold_rows=gold,
        ordered=False,
    ) == Grade("incorrect")
    assert grade_answer(
        _answer("SELECT name, id, 0 FROM t WHERE id = 1"),
        db_path=db,
        gold_rows=gold,
        ordered=False,
    ) == Grade("incorrect")


def test_an_empty_result_never_matches():
    assert matches([], [], ordered=False) is False
    assert matches([(1,)], [], ordered=False) is False


def test_the_grader_opens_the_database_read_only(db):
    got = _answer("DELETE FROM t; SELECT count(*) FROM t")
    assert (
        grade_answer(got, db_path=db, gold_rows=[[0]], ordered=False).failure
        == "sql_error"
    )
    con = duckdb.connect(str(db), read_only=True)
    assert con.execute("SELECT count(*) FROM t").fetchone() == (3,)
    con.close()


def test_gold_digest_is_stable_and_does_not_contain_the_values():
    digest = gold_digest([["Ann", 120.5]])
    assert digest == gold_digest([["Ann", 120.5]])
    assert digest != gold_digest([["Ann", 120.51]])
    assert len(digest) == 64 and "Ann" not in digest
```

- [ ] **Step 3: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_vendored_lsb.py tests/test_lsb_grade.py 2>&1 | tail -4`
Expected:
- `test_vendored_lsb.py` passes, since Step 1 wrote the file;
- `test_lsb_grade.py` fails to import with `ModuleNotFoundError: No module named 'dce.benchmarks.lsb_grade'`.

- [ ] **Step 4: Implement `dce/benchmarks/lsb_grade.py`**

```python
"""Grading for LiveSQLBench on DuckDB.

The agent's answer is the last ```sql block of its final message. It runs on
a fresh READ-ONLY connection to the pristine database, with PostgreSQL's
NULL ordering and integer division switched on, a memory limit and a time
limit, and its result is compared with the frozen PostgreSQL gold result by
the official Soft-EX rules: comments, DISTINCT and ROUND are stripped from
the query, dates normalised, numbers rounded to two places, and the rows
compared as a list (an ordered task) or a set.

Two departures from upstream, both because the gold came from PostgreSQL
and the candidate runs in DuckDB, and both established when the task set
was frozen (every frozen task's own gold SQL passes through this module):

* JSON values: DuckDB returns JSON as text, PostgreSQL as parsed objects,
  so text that parses as a JSON object or array is parsed first.
* Numbers: the engines' float arithmetic differs in the last bits, which
  can flip the two-place rounding of large values, so numbers compare with
  rel_tol 1e-5 / abs_tol 0.011 after rounding.

Failure categories, all graded incorrect: `no_sql` (no block), `sql_error`
(DuckDB refused or failed the query) and `timeout`. Any other exception --
a missing database file, say -- propagates, and `run_task` records a
`scoring_error`, never a silent `incorrect`.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import threading
from pathlib import Path

import duckdb

from dce.benchmark import Grade
from dce.tools import HARNESS_MEMORY_LIMIT
from vendor.livesqlbench_test_utils import (
    preprocess_results,
    remove_comments,
    remove_distinct,
    remove_round,
)

#: PostgreSQL semantics DuckDB can switch on per connection. The gold
#: answers come from PostgreSQL; agents (through `init_sql`) and the grader
#: alike run with these, so a query that is right in PostgreSQL stays right.
PG_COMPAT: tuple[str, ...] = (
    "SET default_null_order = 'nulls_last_on_asc_first_on_desc'",
    "SET integer_division = true",
)

#: The candidate's time limit, the same as an agent's own queries get
#: (`dce.tools.HARNESS_QUERY_SECONDS`).
GRADE_SECONDS: float = 120

_SQL_BLOCK = re.compile(r"```sql[ \t]*\r?\n(.*?)```", re.DOTALL | re.IGNORECASE)


class GradeTimeout(Exception):
    """The candidate query ran past its time limit and was interrupted."""


def extract_sql(answer: str | None) -> str | None:
    """The last ```sql block of `answer`, stripped; `None` when there is no
    non-empty one. Prose after the block does not matter."""
    blocks = _SQL_BLOCK.findall(str(answer or ""))
    if not blocks:
        return None
    sql = blocks[-1].strip()
    return sql or None


def clean(sqls: list[str]) -> list[str]:
    return remove_round(remove_distinct(remove_comments(list(sqls))))


def normalise(rows) -> list[tuple]:
    parsed = []
    for row in rows:
        new = []
        for value in row:
            if isinstance(value, str) and value[:1] in "[{":
                try:
                    value = json.loads(value)
                except ValueError:
                    pass
            new.append(value)
        parsed.append(tuple(new))
    return [tuple(r) for r in preprocess_results(parsed)]


def _close(a, b) -> bool:
    numeric = (int, float)
    if isinstance(a, numeric) and isinstance(b, numeric) and not isinstance(a, bool):
        return math.isclose(a, b, rel_tol=1e-5, abs_tol=0.011)
    return str(a) == str(b)


def _rows_close(left: list[tuple], right: list[tuple]) -> bool:
    return len(left) == len(right) and all(
        len(x) == len(y) and all(_close(u, v) for u, v in zip(x, y))
        for x, y in zip(left, right)
    )


def _key(row: tuple) -> tuple:
    return tuple(str(v) for v in row)


def matches(gold: list[tuple], got: list[tuple], ordered: bool) -> bool:
    """True when `got` equals `gold` under Soft-EX plus the numeric
    tolerance. An empty result never matches, as upstream."""
    if not gold or not got:
        return False
    gold = [tuple(r) for r in gold]
    if ordered:
        return gold == got or _rows_close(gold, got)
    if set(gold) == set(got):
        return True
    return _rows_close(sorted(set(gold), key=_key), sorted(set(got), key=_key))


def gold_digest(rows) -> str:
    """What a row stores in `gold`: the sha256 of the frozen gold result, so
    a results file can say which gold it was graded against without
    publishing the gold."""
    text = json.dumps(rows, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def connect_readonly(db_path: Path, *, memory_limit: str | None = HARNESS_MEMORY_LIMIT):
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        for statement in PG_COMPAT:
            con.execute(statement)
        if memory_limit is not None:
            con.execute("SET memory_limit = ?", [memory_limit])
    except Exception:
        con.close()
        raise
    return con


def run_sql(con, sqls: list[str], *, seconds: float) -> list[tuple]:
    """Run `sqls` in order and return the last one's rows, normalised. A
    `threading.Timer` interrupts the connection at `seconds`; the result is
    fetched in full, since the grader compares every row."""
    timer = threading.Timer(seconds, con.interrupt)
    timer.start()
    try:
        result = None
        for sql in sqls:
            result = con.execute(sql)
        rows = result.fetchall() if result is not None else []
    except duckdb.InterruptException as exc:
        raise GradeTimeout(f"interrupted after {seconds}s") from exc
    finally:
        timer.cancel()
    return normalise(rows)


def grade_answer(
    answer, *, db_path: Path, gold_rows, ordered: bool, seconds: float = GRADE_SECONDS
) -> Grade:
    sql = extract_sql(answer)
    if sql is None:
        return Grade("incorrect", failure="no_sql")
    con = connect_readonly(db_path)
    try:
        got = run_sql(con, clean([sql]), seconds=seconds)
    except GradeTimeout:
        return Grade("incorrect", failure="timeout")
    except duckdb.Error:
        return Grade("incorrect", failure="sql_error")
    finally:
        con.close()
    gold = [tuple(r) for r in gold_rows]
    return Grade("correct" if matches(gold, got, ordered) else "incorrect")
```

If `duckdb.InterruptException` is not the class DuckDB raises on
`interrupt()` in the installed version, check with
`UV_FROZEN=1 uv run python -c "import duckdb; print([n for n in dir(duckdb) if 'Interrupt' in n])"`
and use the name it prints. Keep the `GradeTimeout` mapping.

- [ ] **Step 5: Run the tests**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_vendored_lsb.py tests/test_lsb_grade.py 2>&1 | tail -3`
Expected: all pass. If `test_round_and_distinct...` or `test_json_text...`
fails, the expectation in the test is what the official pipeline produces;
print `normalise(...)` of the candidate to see why before changing anything,
and change the test only if the official behaviour differs from the
comment's claim.

- [ ] **Step 6: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add vendor/livesqlbench_test_utils.py vendor/README.md dce/benchmarks/lsb_grade.py tests/test_vendored_lsb.py tests/test_lsb_grade.py
git commit -m "dce: the LiveSQLBench grader, on the official Soft-EX helpers vendored under MIT"
```

---

### Task 7: The LiveSQLBench benchmark, on a synthetic fixture

**Files:**
- Create: `tests/fixtures/lsb/shop.sql`
- Create: `tests/fixtures/lsb/gold_sql.json` (test-only, synthetic)
- Create: `tests/fixtures/lsb/tasks_frozen_full_v1.json`
- Create: `tests/fixtures/lsb/gold_results_full_v1.json`
- Create: `tests/fixtures/lsb/hf-full-v1/livesqlbench_data.jsonl`
- Create: `tests/fixtures/lsb/hf-full-v1/shop/shop_kb.jsonl`
- Create: `tests/fixtures/lsb/hf-full-v1/shop/shop_column_meaning_base.json`
- Create: `tests/fixtures/lsb/hf-full-v1/shop/shop_schema.txt`
- Create: `dce/benchmarks/livesqlbench.py`
- Modify: `dce/benchmark.py` (registry)
- Test: `tests/test_livesqlbench.py`

**Interfaces:**
- Consumes:
  - from Task 6: `grade_answer`, `gold_digest`, `extract_sql`, `PG_COMPAT`;
  - from `dce.tools`: `ArmSetup`, `_ungoverned_tools`;
  - from Task 2: the protocol's new members.
- Produces:
  - `class LiveSQLBench` with `name = "livesqlbench"`;
  - `LiveSQLBench.from_data(data: Path) -> LiveSQLBench`;
  - module constants `BASE_PROMPT`, `ANSWER_INSTRUCTION`, `SCORER`, `DEFAULT_DATA`;
  - `render_column_meanings(meanings: dict) -> str` and `render_manual(entries: list[dict]) -> str`;
  - `BENCHMARK_NAMES = ("dabstep", "livesqlbench")`;
  - `benchmark_class("livesqlbench")`.

- [ ] **Step 1: Write the fixture**

The fixture is synthetic. It has the same file layout as `LSB_DATA`, and
none of it comes from LiveSQLBench. The DuckDB file is built at test time
from `shop.sql`, so no binary is committed.

`tests/fixtures/lsb/shop.sql`:

```sql
CREATE TABLE customers (custid BIGINT PRIMARY KEY, name TEXT, tier TEXT, joined DATE);
CREATE TABLE orders (orderid BIGINT PRIMARY KEY, custid BIGINT, amount DOUBLE, qty INTEGER, status TEXT, details JSON);
INSERT INTO customers VALUES
  (1, 'Ann', 'gold', DATE '2024-01-05'),
  (2, 'Bob', 'silver', DATE '2024-02-10'),
  (3, 'Cy', 'gold', DATE '2024-03-15');
INSERT INTO orders VALUES
  (10, 1, 120.5, 3, 'shipped', '{"gift": true}'),
  (11, 1, 80.0, 1, 'returned', NULL),
  (12, 2, 200.0, 4, 'shipped', NULL),
  (13, 3, 15.25, 1, 'shipped', '{"gift": false}'),
  (14, 3, NULL, 2, 'pending', NULL);
```

`tests/fixtures/lsb/gold_sql.json`:

```json
{
  "shop_1": "SELECT c.name, SUM(o.amount) AS revenue FROM customers c JOIN orders o ON o.custid = c.custid WHERE o.status = 'shipped' GROUP BY c.name ORDER BY revenue DESC",
  "shop_2": "SELECT c.name, SUM(o.amount) AS revenue FROM customers c JOIN orders o ON o.custid = c.custid WHERE o.status = 'shipped' GROUP BY c.name ORDER BY revenue ASC",
  "shop_3": "SELECT COUNT(*) FROM customers WHERE tier = 'gold'",
  "shop_4": "SELECT SUM(qty) / COUNT(*) FROM orders",
  "shop_5": "SELECT amount FROM orders ORDER BY amount DESC"
}
```

`tests/fixtures/lsb/gold_results_full_v1.json`:

```json
{
  "shop_1": {"db": "shop", "order": true, "rows": [["Bob", 200.0], ["Ann", 120.5], ["Cy", 15.25]]},
  "shop_2": {"db": "shop", "order": true, "rows": [["Cy", 15.25], ["Ann", 120.5], ["Bob", 200.0]]},
  "shop_3": {"db": "shop", "order": false, "rows": [[2]]},
  "shop_4": {"db": "shop", "order": false, "rows": [[2]]},
  "shop_5": {"db": "shop", "order": true, "rows": [[null], [200.0], [120.5], [80.0], [15.25]]},
  "shop_6": {"db": "shop", "order": false, "rows": [[1]]}
}
```

`tests/fixtures/lsb/tasks_frozen_full_v1.json`:

```json
{
  "shop_1": {"db": "shop", "order": true, "set": "primary", "via": "as_is"},
  "shop_2": {"db": "shop", "order": true, "set": "order_only", "via": "as_is"},
  "shop_3": {"db": "shop", "order": false, "set": "primary", "via": "as_is"},
  "shop_4": {"db": "shop", "order": false, "set": "primary", "via": "as_is"},
  "shop_5": {"db": "shop", "order": true, "set": "primary", "via": "as_is"},
  "shop_6": {"db": "shop", "order": false, "set": "excluded", "via": "as_is"}
}
```

`tests/fixtures/lsb/hf-full-v1/livesqlbench_data.jsonl`, one JSON object
per line, in this order. `shop_7` is a Management task, which is never
loaded:

```json
{"instance_id": "shop_1", "selected_database": "shop", "query": "Rank customers by how much shipped business they brought, biggest first.", "normal_query": "x", "preprocess_sql": [], "clean_up_sqls": [], "sol_sql": [], "external_knowledge": [], "test_cases": [], "category": "Query", "high_level": false, "conditions": {"decimal": 2, "distinct": false, "order": true}}
{"instance_id": "shop_2", "selected_database": "shop", "query": "Rank customers by shipped revenue, smallest first.", "normal_query": "x", "preprocess_sql": [], "clean_up_sqls": [], "sol_sql": [], "external_knowledge": [], "test_cases": [], "category": "Query", "high_level": false, "conditions": {"decimal": 2, "distinct": false, "order": true}}
{"instance_id": "shop_3", "selected_database": "shop", "query": "How many gold customers are there?", "normal_query": "x", "preprocess_sql": [], "clean_up_sqls": [], "sol_sql": [], "external_knowledge": [], "test_cases": [], "category": "Query", "high_level": true, "conditions": {"decimal": 2, "distinct": false, "order": false}}
{"instance_id": "shop_4", "selected_database": "shop", "query": "What is the basket size across all orders?", "normal_query": "x", "preprocess_sql": [], "clean_up_sqls": [], "sol_sql": [], "external_knowledge": [], "test_cases": [], "category": "Query", "high_level": false, "conditions": {"decimal": 2, "distinct": false, "order": false}}
{"instance_id": "shop_5", "selected_database": "shop", "query": "List order amounts from largest to smallest, unknown amounts first.", "normal_query": "x", "preprocess_sql": [], "clean_up_sqls": [], "sol_sql": [], "external_knowledge": [], "test_cases": [], "category": "Query", "high_level": false, "conditions": {"decimal": 2, "distinct": false, "order": true}}
{"instance_id": "shop_6", "selected_database": "shop", "query": "Excluded task.", "normal_query": "x", "preprocess_sql": [], "clean_up_sqls": [], "sol_sql": [], "external_knowledge": [], "test_cases": [], "category": "Query", "high_level": false, "conditions": {"decimal": 2, "distinct": false, "order": false}}
{"instance_id": "shop_7", "selected_database": "shop", "query": "Delete returned orders.", "normal_query": "x", "preprocess_sql": [], "clean_up_sqls": [], "sol_sql": [], "external_knowledge": [], "test_cases": [], "category": "Management", "high_level": false, "conditions": {"decimal": 2, "distinct": false, "order": false}}
```

`tests/fixtures/lsb/hf-full-v1/shop/shop_kb.jsonl`:

```json
{"id": 1, "knowledge": "Customer Tier", "description": "The loyalty tiers customers belong to.", "definition": "Values are 'gold' and 'silver'.", "type": "value_illustration", "children_knowledge": -1}
{"id": 2, "knowledge": "Shipped Order", "description": "An order that has left the warehouse.", "definition": "An order whose status is 'shipped'.", "type": "domain_knowledge", "children_knowledge": -1}
{"id": 3, "knowledge": "Shipped Revenue", "description": "Revenue a customer has realised.", "definition": "The sum of amount over the customer's Shipped Orders.", "type": "calculation_knowledge", "children_knowledge": [2]}
{"id": 4, "knowledge": "Basket Size", "description": "Items per order.", "definition": "Total qty divided by the number of orders, in integer division.", "type": "calculation_knowledge", "children_knowledge": -1}
{"id": 5, "knowledge": "Gold Customer", "description": "A customer in the top tier.", "definition": "A customer whose tier is 'gold'.", "type": "domain_knowledge", "children_knowledge": [1]}
{"id": 6, "knowledge": "Order Status", "description": "The lifecycle states of an order.", "definition": "Values are 'pending', 'shipped' and 'returned'.", "type": "value_illustration", "children_knowledge": -1}
```

`tests/fixtures/lsb/hf-full-v1/shop/shop_column_meaning_base.json`:

```json
{
  "shop|customers|custid": "Customer identifier.",
  "shop|customers|name": "Customer display name.",
  "shop|customers|tier": "Loyalty tier.",
  "shop|customers|joined": "Date the customer joined.",
  "shop|orders|orderid": "Order identifier.",
  "shop|orders|custid": "The ordering customer.",
  "shop|orders|amount": "Order value; null when not yet priced.",
  "shop|orders|qty": "Items in the order.",
  "shop|orders|status": "Order lifecycle state.",
  "shop|orders|details": {"column_meaning": "JSON column with order extras.", "fields_meaning": {"gift": "BOOLEAN. Whether the order is a gift."}}
}
```

`tests/fixtures/lsb/hf-full-v1/shop/shop_schema.txt`:

```
CREATE TABLE "customers" (
custid bigint NOT NULL,
name text NOT NULL,
tier text NOT NULL,
joined date NOT NULL,
    PRIMARY KEY (custid)
);

CREATE TABLE "orders" (
orderid bigint NOT NULL,
custid bigint NOT NULL,
amount double precision,
qty integer NOT NULL,
status text NOT NULL,
details jsonb,
    PRIMARY KEY (orderid),
    FOREIGN KEY (custid) REFERENCES customers(custid)
);
```

- [ ] **Step 2: Write the failing tests**

`tests/test_livesqlbench.py`:

```python
import json
import shutil
from pathlib import Path

import duckdb
import pytest
from dce.benchmark import Benchmark, Grade, benchmark_class
from dce.benchmarks.livesqlbench import (
    ANSWER_INSTRUCTION,
    BASE_PROMPT,
    SCORER,
    LiveSQLBench,
    render_column_meanings,
    render_manual,
)
from dce.benchmarks.lsb_grade import gold_digest

FIXTURE = Path(__file__).parent / "fixtures" / "lsb"


@pytest.fixture
def data(tmp_path: Path) -> Path:
    """The fixture laid out as `LSB_DATA`, with its DuckDB file built."""
    root = tmp_path / "lsb"
    shutil.copytree(FIXTURE, root)
    (root / "duckdb").mkdir()
    con = duckdb.connect(str(root / "duckdb" / "shop.duckdb"))
    con.execute((FIXTURE / "shop.sql").read_text(encoding="utf-8"))
    con.close()
    return root


@pytest.fixture
def bench(data: Path) -> LiveSQLBench:
    return LiveSQLBench.from_data(data)


def _task(bench, task_id):
    return next(t for t in bench.tasks() if t.task_id == task_id)


def test_registered_and_satisfies_the_protocol(bench):
    assert benchmark_class("livesqlbench") is LiveSQLBench
    assert isinstance(bench, Benchmark)


def test_class_attributes(bench):
    assert LiveSQLBench.arms == ("schema_only", "manual_prompt")
    assert LiveSQLBench.extra_arms == ()
    assert LiveSQLBench.governed_arms == frozenset()
    assert LiveSQLBench.roles == {"baseline": "schema_only", "manual": "manual_prompt"}
    assert LiveSQLBench.primary is None
    assert LiveSQLBench.group_label == "db"
    assert LiveSQLBench.subsets == ("primary", "order_only")
    assert LiveSQLBench.excluded_tasks == frozenset()


def test_loads_the_frozen_query_tasks_only(bench):
    tasks = bench.tasks()
    assert [t.task_id for t in tasks] == [
        "shop_1",
        "shop_2",
        "shop_3",
        "shop_4",
        "shop_5",
    ]
    assert [t.subset for t in tasks] == [
        "primary",
        "order_only",
        "primary",
        "primary",
        "primary",
    ]
    assert {t.group for t in tasks} == {"shop"}


def test_the_prompt_is_the_query_and_the_fixed_instruction(bench):
    task = _task(bench, "shop_3")
    assert task.prompt == "How many gold customers are there?\n\n" + ANSWER_INSTRUCTION
    assert "```sql" in ANSWER_INSTRUCTION


def test_row_fields(bench):
    assert bench.row_fields(_task(bench, "shop_3")) == {
        "db": "shop",
        "high_level": True,
        "order": False,
    }


def test_pristine_db_is_the_tasks_database(bench, data):
    assert bench.pristine_db(_task(bench, "shop_1")) == data / "duckdb" / "shop.duckdb"


def test_both_arms_carry_schema_and_column_meanings_and_only_the_manual_carries_the_kb(
    bench, data
):
    task = _task(bench, "shop_1")
    db = data / "duckdb" / "shop.duckdb"
    schema_only = bench.build_arm("schema_only", task, db)
    manual = bench.build_arm("manual_prompt", task, db)
    schema = (data / "hf-full-v1" / "shop" / "shop_schema.txt").read_text(
        encoding="utf-8"
    )
    for setup in (schema_only, manual):
        assert setup.system_prompt.startswith(BASE_PROMPT)
        assert schema.strip() in setup.system_prompt
        assert (
            "- orders.amount: Order value; null when not yet priced."
            in setup.system_prompt
        )
        assert [t.name for t in setup.tools] == [
            "list_tables",
            "describe_table",
            "execute_sql",
        ]
    assert "Shipped Revenue" not in schema_only.system_prompt
    assert (
        manual.system_prompt
        == schema_only.system_prompt
        + "\n\n## Knowledge base\n\n"
        + render_manual(
            [
                json.loads(line)
                for line in (data / "hf-full-v1" / "shop" / "shop_kb.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
        )
    )


def test_the_arms_tools_run_with_postgres_semantics(bench, data):
    task = _task(bench, "shop_4")
    setup = bench.build_arm("schema_only", task, data / "duckdb" / "shop.duckdb")
    execute = next(t for t in setup.tools if t.name == "execute_sql")
    assert execute.function("SELECT 7 / 2 AS x").splitlines() == ["x", "3"]


def test_an_unknown_or_contract_arm_raises(bench, data):
    with pytest.raises(ValueError, match="unknown arm"):
        bench.build_arm(
            "contract", _task(bench, "shop_1"), data / "duckdb" / "shop.duckdb"
        )


def test_rendering_of_meanings_and_manual():
    text = render_column_meanings(
        {
            "d|t|a": "Plain.",
            "d|t|j": {"column_meaning": "JSON col.", "fields_meaning": {"f": "F."}},
        }
    )
    assert text == '- t.a: Plain.\n- t.j: JSON col. Fields: {"f": "F."}'
    manual = render_manual(
        [
            {"id": 2, "knowledge": "B", "description": "d2", "definition": "f2"},
            {"id": 1, "knowledge": "A", "description": "d1", "definition": "f1"},
        ]
    )
    assert manual == "### A\n\nd1\n\nf1\n\n### B\n\nd2\n\nf2"


def test_every_fixture_tasks_own_gold_sql_grades_correct(bench):
    gold_sql = json.loads((FIXTURE / "gold_sql.json").read_text(encoding="utf-8"))
    for task in bench.tasks():
        answer = f"```sql\n{gold_sql[task.task_id]}\n```"
        assert bench.grade(task, answer) == Grade("correct"), task.task_id


def test_an_order_only_task_is_graded_without_order(bench):
    # shop_2's gold is ascending; a descending answer is still right.
    answer = "```sql\nSELECT c.name, SUM(o.amount) FROM customers c JOIN orders o ON o.custid = c.custid WHERE o.status = 'shipped' GROUP BY c.name ORDER BY 2 DESC\n```"
    assert bench.grade(_task(bench, "shop_2"), answer) == Grade("correct")
    # The same descending answer to shop_1 (primary, ordered, gold descending) is right ...
    assert bench.grade(_task(bench, "shop_1"), answer) == Grade("correct")
    # ... and an ascending one is not.
    asc = answer.replace("DESC", "ASC")
    assert bench.grade(_task(bench, "shop_1"), asc) == Grade("incorrect")


def test_gold_ref_golds_hash_normalize_and_scorer(bench, data):
    gold = json.loads((data / "gold_results_full_v1.json").read_text(encoding="utf-8"))
    task = _task(bench, "shop_3")
    assert bench.gold_ref(task) == gold_digest(gold["shop_3"]["rows"])
    assert len(bench.golds_hash()) == 64
    assert bench.normalize("text\n```sql\nSELECT 1\n```") == "SELECT 1"
    assert bench.normalize("no sql") == ""
    assert LiveSQLBench.scorer() == SCORER
    assert LiveSQLBench.rescore({"answer": "x", "gold": "y"}) is None
    assert LiveSQLBench.e2e_correct({"answer": "x"}) is False


def test_arm_digest_is_the_databases_kb_file(bench, data):
    import hashlib

    kb = (data / "hf-full-v1" / "shop" / "shop_kb.jsonl").read_bytes()
    assert (
        bench.arm_digest("manual_prompt", _task(bench, "shop_1"))
        == hashlib.sha256(kb).hexdigest()
    )


def test_a_missing_database_file_stops_loading_by_name(data):
    (data / "duckdb" / "shop.duckdb").unlink()
    with pytest.raises(SystemExit, match="shop"):
        LiveSQLBench.from_data(data)


def test_a_task_without_a_gold_stops_loading(data):
    gold_path = data / "gold_results_full_v1.json"
    gold = json.loads(gold_path.read_text(encoding="utf-8"))
    del gold["shop_3"]
    gold_path.write_text(json.dumps(gold), encoding="utf-8")
    with pytest.raises(SystemExit, match="shop_3"):
        LiveSQLBench.from_data(data)


def test_builds_from_its_command_line_flag(data):
    import argparse

    parser = argparse.ArgumentParser()
    LiveSQLBench.add_arguments(parser)
    args = parser.parse_args(["--lsb-data", str(data)])
    assert [t.task_id for t in LiveSQLBench.from_args(args).tasks()][0] == "shop_1"


def test_no_gold_reaches_a_row(bench, data, tmp_path):
    """A full run_task row, through a fake model, carries the gold digest and
    never a gold value."""
    from dce.agent import run_task

    class Fake:
        def run_sync(self, *a, usage=None, **k):
            class R:
                output = "```sql\nSELECT c.name, SUM(o.amount) FROM customers c JOIN orders o ON o.custid = c.custid WHERE o.status = 'shipped' GROUP BY 1 ORDER BY 2 DESC\n```"

            return R()

    working = tmp_path / "w.duckdb"
    shutil.copyfile(data / "duckdb" / "shop.duckdb", working)
    row = run_task(
        _task(bench, "shop_1"),
        "schema_only",
        "z-ai/glm-5.3-flash",
        bench,
        working,
        agent_factory=lambda **_: Fake(),
    )
    assert row["verdict"] == "correct"
    assert row["benchmark"] == "livesqlbench" and row["subset"] == "primary"
    assert len(row["gold"]) == 64
    assert "Bob" not in json.dumps({k: v for k, v in row.items() if k != "answer"})
```

- [ ] **Step 3: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_livesqlbench.py 2>&1 | tail -3`
Expected: FAIL with `ModuleNotFoundError: No module named 'dce.benchmarks.livesqlbench'`.

- [ ] **Step 4: Implement `dce/benchmarks/livesqlbench.py`**

```python
"""LiveSQLBench Base-Full-v1, ported to DuckDB, as a `Benchmark`.

Everything private stays in `LSB_DATA` (default `~/data/livesqlbench`) and
is never committed: the frozen gold results, the gold SQL and the 22 DuckDB
files. The public task file and each database's KB, column meanings and DDL
come from the Hugging Face dataset `birdsql/livesqlbench-base-full-v1`, laid
out under `LSB_DATA/hf-full-v1/`. `prep/livesqlbench/` rebuilds the rest.

Tasks are the frozen Query tasks: 309 `primary` and 72 `order_only`, each a
`subset`; the 30 excluded tasks are never loaded. Every arm's system prompt
carries the database's DDL and column meanings, so the arms differ only in
how the KB reaches the agent; until the compiler lands (PR C) the arms are
`schema_only` and `manual_prompt`. All their DuckDB connections, and the
grader's, run PostgreSQL's NULL ordering and integer division
(`lsb_grade.PG_COMPAT`), because the gold came from PostgreSQL.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from functools import cache
from pathlib import Path

from dce.benchmark import Grade, Task
from dce.benchmarks.lsb_grade import (
    PG_COMPAT,
    extract_sql,
    gold_digest,
    grade_answer,
)
from dce.tools import ArmSetup, _ungoverned_tools

DEFAULT_DATA = Path.home() / "data" / "livesqlbench"

#: Names the grading rules on every row. Bump it whenever `lsb_grade` changes
#: what counts as correct.
SCORER = "lsb-soft-ex-duckdb/1"

BASE_PROMPT = (
    "You are a data analyst answering questions over a DuckDB database.\n"
    "Explore the schema, write SQL, and verify your result before answering.\n"
    "The database sorts NULLs and divides integers as PostgreSQL does."
)

#: Appended to every task's query. Fixed before the first run; the grader
#: reads exactly what it asks for.
ANSWER_INSTRUCTION = (
    "End your answer with the final SQL query in a ```sql block. That query "
    "is what is graded: it must run on this DuckDB database and return the "
    "answer."
)


def render_column_meanings(meanings: dict) -> str:
    """One line per column, `- table.column: meaning`, in the file's order. A
    JSON column's meaning is an object; its field meanings follow as JSON."""
    lines = []
    for key, value in meanings.items():
        _, table, column = key.split("|", 2)
        if isinstance(value, dict):
            text = str(value.get("column_meaning", "")).strip()
            fields = value.get("fields_meaning")
            if fields:
                text += " Fields: " + json.dumps(fields, ensure_ascii=False)
        else:
            text = str(value).strip()
        lines.append(f"- {table}.{column}: {text}")
    return "\n".join(lines)


def render_manual(entries: list[dict]) -> str:
    """The KB as a prose manual: entries in `id` order, each its name, its
    description and its definition."""
    return "\n\n".join(
        f"### {e['knowledge']}\n\n{e['description']}\n\n{e['definition']}"
        for e in sorted(entries, key=lambda e: e["id"])
    )


def _read_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


@cache
def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class LiveSQLBench:
    name = "livesqlbench"
    arms: tuple[str, ...] = ("schema_only", "manual_prompt")
    extra_arms: tuple[str, ...] = ()
    governed_arms: frozenset[str] = frozenset()
    roles = {"baseline": "schema_only", "manual": "manual_prompt"}
    primary = None
    group_label = "db"
    excluded_tasks: frozenset[str] = frozenset()
    subsets: tuple[str, ...] = ("primary", "order_only")
    task_set_label = "frozen task set"
    excluded_reason = "excluded"
    excluded_source = "LiveSQLBench.excluded_tasks"

    def __init__(
        self, *, data: Path, records: list[tuple[dict, dict]], gold: dict
    ) -> None:
        self._data = data
        self._records = records  # (public task, frozen entry), in file order
        self._gold = gold

    @classmethod
    def from_data(cls, data: Path) -> LiveSQLBench:
        frozen = json.loads(
            (data / "tasks_frozen_full_v1.json").read_text(encoding="utf-8")
        )
        gold = json.loads(
            (data / "gold_results_full_v1.json").read_text(encoding="utf-8")
        )
        public = _read_jsonl(data / "hf-full-v1" / "livesqlbench_data.jsonl")
        records = [
            (task, frozen[task["instance_id"]])
            for task in public
            if task["instance_id"] in frozen
            and frozen[task["instance_id"]]["set"] in cls.subsets
        ]
        # Checked here, before any spend: a task with no gold would grade as
        # a `scoring_error` on every arm, and a missing database file would
        # fail every task on it the same way.
        no_gold = sorted(
            task["instance_id"]
            for task, _ in records
            if task["instance_id"] not in gold
        )
        if no_gold:
            raise SystemExit(
                f"LiveSQLBench tasks with no frozen gold result: {no_gold}"
            )
        missing = sorted(
            {
                task["selected_database"]
                for task, _ in records
                if not (
                    data / "duckdb" / f"{task['selected_database']}.duckdb"
                ).is_file()
            }
        )
        if missing:
            raise SystemExit(
                f"LiveSQLBench databases missing from {data / 'duckdb'}: {missing}"
            )
        return cls(data=data, records=records, gold=gold)

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--lsb-data",
            type=Path,
            default=Path(os.environ.get("LSB_DATA", DEFAULT_DATA)),
            help="(livesqlbench) the private data directory (default $LSB_DATA or ~/data/livesqlbench)",
        )

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> LiveSQLBench:
        return cls.from_data(args.lsb_data)

    def tasks(self) -> list[Task]:
        return [
            Task(
                task_id=task["instance_id"],
                prompt=f"{task['query']}\n\n{ANSWER_INSTRUCTION}",
                group=task["selected_database"],
                meta={
                    "db": task["selected_database"],
                    "high_level": bool(task.get("high_level", False)),
                    "order": bool(entry["order"]),
                },
                subset=entry["set"],
            )
            for task, entry in self._records
        ]

    def _db_dir(self, db: str) -> Path:
        return self._data / "hf-full-v1" / db

    def pristine_db(self, task: Task) -> Path:
        return self._data / "duckdb" / f"{task.meta['db']}.duckdb"

    def _context(self, db: str) -> str:
        folder = self._db_dir(db)
        schema = (folder / f"{db}_schema.txt").read_text(encoding="utf-8").strip()
        meanings = json.loads(
            (folder / f"{db}_column_meaning_base.json").read_text(encoding="utf-8")
        )
        return (
            f"{BASE_PROMPT}\n\n## Database schema\n\n{schema}\n\n"
            f"## Column meanings\n\n{render_column_meanings(meanings)}"
        )

    def build_arm(self, arm: str, task: Task, db: Path) -> ArmSetup:
        name = task.meta["db"]
        prompt = self._context(name)
        if arm == "schema_only":
            pass
        elif arm == "manual_prompt":
            entries = _read_jsonl(self._db_dir(name) / f"{name}_kb.jsonl")
            prompt += "\n\n## Knowledge base\n\n" + render_manual(entries)
        else:
            raise ValueError(f"unknown arm: {arm!r}; expected one of {self.arms}")
        return ArmSetup(prompt, _ungoverned_tools(db, init_sql=PG_COMPAT), None)

    def arm_digest(self, arm: str, task: Task) -> str:
        # The knowledge this experiment is about, for the task's database:
        # the KB file every arm's knowledge comes from (none, prose, or --
        # in PR C -- compiled).
        name = task.meta["db"]
        return _sha256(self._db_dir(name) / f"{name}_kb.jsonl")

    def _gold_rows(self, task: Task):
        return self._gold[task.task_id]["rows"]

    def gold_ref(self, task: Task) -> str | None:
        return gold_digest(self._gold_rows(task))

    def grade(self, task: Task, answer: str) -> Grade:
        # An order-only task's gold reproduces in DuckDB only as a set, so it
        # is graded without order whatever its `order` flag says.
        ordered = task.subset == "primary" and bool(task.meta["order"])
        return grade_answer(
            answer,
            db_path=self.pristine_db(task),
            gold_rows=self._gold_rows(task),
            ordered=ordered,
        )

    def normalize(self, answer: str) -> str:
        # The SQL the grader ran, so a verdict can be re-checked from the row.
        return extract_sql(answer) or ""

    def golds_hash(self) -> str:
        digests = {
            task["instance_id"]: gold_digest(self._gold[task["instance_id"]]["rows"])
            for task, _ in self._records
        }
        return hashlib.sha256(
            json.dumps(digests, sort_keys=True).encode("utf-8")
        ).hexdigest()

    def row_fields(self, task: Task) -> dict:
        return {
            "db": task.meta["db"],
            "high_level": task.meta["high_level"],
            "order": task.meta["order"],
        }

    @staticmethod
    def scorer() -> str:
        return SCORER

    @staticmethod
    def rescore(row: dict) -> str | None:
        # The gold is not in the row, by design, so a row cannot be re-graded
        # offline; re-grading means re-running `lsb_grade` with LSB_DATA.
        return None

    @staticmethod
    def e2e_correct(row: dict) -> bool:
        # The answer is SQL graded by its result; there is no separate
        # end-to-end reading.
        return False
```

`dce/benchmark.py`:
- `BENCHMARK_NAMES = ("dabstep", "livesqlbench")`.
- In `benchmark_class`, add before the `raise`:

```python
    if name == "livesqlbench":
        from dce.benchmarks.livesqlbench import LiveSQLBench

        return LiveSQLBench
```

- `tests/test_benchmark.py::test_unknown_benchmark_is_refused` still passes, because it uses `"nope"`.

- [ ] **Step 5: Run the tests**

Run: `UV_FROZEN=1 uv run pytest -q --deselect tests/test_replay.py::test_sql_statements_are_ordered_and_only_from_sql_tools 2>&1 | tail -2`
Expected: all pass.

Then check the CLI accepts the benchmark without running anything:
`UV_FROZEN=1 uv run python -m dce.runner --help 2>/dev/null | grep -E "livesqlbench|lsb-data"`
Expected: both appear.

- [ ] **Step 6: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/benchmarks/livesqlbench.py dce/benchmark.py tests/test_livesqlbench.py tests/fixtures/lsb
git commit -m "dce: LiveSQLBench as the second Benchmark, schema_only and manual_prompt arms"
```

---

### Task 8: Prep scripts and the checks on the real data

The scripts that built `LSB_DATA` are committed, reading private data from
`LSB_DATA` and credentials from the environment. Two checks run against the
real data. Neither writes gold into the repository.

**Files:**
- Create: `prep/livesqlbench/README.md`
- Create: `prep/livesqlbench/convert.py`, `prep/livesqlbench/gold.py`, `prep/livesqlbench/freeze.py`, `prep/livesqlbench/verify_translations.py`, `prep/livesqlbench/check_grader.py`

**Interfaces:**
- Consumes:
  - `clean`, `matches`, `normalise`, `connect_readonly` and `run_sql` (`dce.benchmarks.lsb_grade`);
  - `LiveSQLBench.from_data` (Task 7).

- [ ] **Step 1: Port the four existing scripts**

Copy each from `~/data/livesqlbench/prep/`, with only these edits:
- `lsb_convert.py` becomes `convert.py`, `lsb_gold.py` becomes `gold.py`, `lsb_freeze.py` becomes `freeze.py`, and `verify_translations.py` keeps its name.
- `HOME = Path.home() / "data" / "livesqlbench"` becomes `DATA = Path(os.environ.get("LSB_DATA", Path.home() / "data" / "livesqlbench"))`, and every `HOME` becomes `DATA`.
- Every hard-coded PostgreSQL connection string (`host=127.0.0.1 port=55432 user=root password=...`) is replaced by the `LSB_PG_DSN` environment variable. Add near the top of `convert.py` and `gold.py`:

```python
PG_DSN = os.environ.get("LSB_PG_DSN")  # e.g. "host=... port=... user=... password=..."
if not PG_DSN:
    raise SystemExit("set LSB_PG_DSN to the LiveSQLBench PostgreSQL connection string")
```

  In `convert.py`, the `PG` prefix becomes `PG_DSN + " dbname="`. In `gold.py`, use `psycopg2.connect(PG_DSN, dbname=db)`.
- The `sys.path.insert(... "official-eval")` and `from test_utils import ...` lines, and in `freeze.py`/`verify_translations.py` the `from lsb_grade import ...` line, become imports from `dce.benchmarks.lsb_grade`. `gold.py`'s local `clean` and `norm` become `clean` and `normalise` from there.
- `freeze.py` and `verify_translations.py` call `connect(db)`. Define it once in each:

```python
def connect(db: str):
    # No memory limit: the freeze reproduced each gold without one.
    return connect_readonly(DATA / "duckdb" / f"{db}.duckdb", memory_limit=None)
```

  and `run_duckdb(con, sqls)` becomes `run_sql(con, sqls, seconds=600)`.
- `freeze.py` ends by also writing `tasks_frozen_full_v1.json` from its verdicts, the translations and the gold:

```python
translations = json.load(open(DATA / "prep" / "translations.json", encoding="utf-8"))
frozen = {}
for iid, g in sorted(gold.items()):
    if iid in verdicts:
        v = verdicts[iid]
        frozen[iid] = {
            "db": g["db"],
            "order": g["order"],
            "set": v["verdict"],
            "via": v["via"],
        }
    elif translations.get(iid, {}).get("status") == "ok":
        frozen[iid] = {
            "db": g["db"],
            "order": g["order"],
            "set": "primary",
            "via": "hand_translation",
        }
    else:
        frozen[iid] = {
            "db": g["db"],
            "order": g["order"],
            "set": "excluded",
            "via": "untranslatable_tie",
        }
json.dump(
    frozen,
    open(DATA / "tasks_frozen_full_v1.json.rebuilt", "w", encoding="utf-8"),
    indent=1,
    sort_keys=True,
)
```

  It writes `.rebuilt` beside the frozen file and never overwrites it.
- Prefix each script's docstring with its run line:
  `Run: LSB_DATA=... [LSB_PG_DSN=...] uv run --with sqlglot --with psycopg2-binary python prep/livesqlbench/<name>.py`.

`lsb_diag.py` is a one-off diagnostic and is not ported.

- [ ] **Step 2: The grader check script**

`prep/livesqlbench/check_grader.py`:

```python
"""Grade every frozen LiveSQLBench task's own gold SQL through the harness.

The gate before any agent run: the gold SQL, submitted exactly as an agent's
answer is (one ```sql block), must grade correct on all 381 tasks under
`LiveSQLBench.grade` -- the same connection settings, memory limit, time
limit and Soft-EX rules agents are graded by. The DuckDB form of each gold
is the one the freeze established (`via`): as-is, sqlglot's transpilation, or
the verified hand translation.

Run: LSB_DATA=... uv run --with sqlglot python prep/livesqlbench/check_grader.py
"""

from __future__ import annotations

import collections
import json
import os
import sys
from pathlib import Path

import sqlglot

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dce.benchmarks.livesqlbench import LiveSQLBench  # noqa: E402

DATA = Path(os.environ.get("LSB_DATA", Path.home() / "data" / "livesqlbench"))


def main() -> None:
    bench = LiveSQLBench.from_data(DATA)
    frozen = json.load(open(DATA / "tasks_frozen_full_v1.json", encoding="utf-8"))
    gt = {
        r["instance_id"]: r
        for r in map(
            json.loads,
            open(
                DATA / "livesqlbench_base_full_v1_gt_kg_testcases_20260613.jsonl",
                encoding="utf-8",
            ),
        )
    }
    translations = json.load(
        open(DATA / "prep" / "translations.json", encoding="utf-8")
    )
    counts: collections.Counter[str] = collections.Counter()
    failures = []
    for task in bench.tasks():
        via = frozen[task.task_id]["via"]
        sqls = list(gt[task.task_id]["sol_sql"])
        if via == "sqlglot":
            sqls = [
                sqlglot.transpile(s, read="postgres", write="duckdb")[0] for s in sqls
            ]
        elif via == "hand_translation":
            sqls = list(translations[task.task_id]["sql"])
        answer = "```sql\n" + ";\n".join(s.rstrip().rstrip(";") for s in sqls) + "\n```"
        grade = bench.grade(task, answer)
        counts[(task.subset, grade.verdict)] += 1
        if grade.verdict != "correct":
            failures.append((task.task_id, via, grade.failure))
    print(dict(counts))
    for failure in failures:
        print("  NOT CORRECT", failure)
    print(f"{len(failures)} of {sum(counts.values())} gold answers not graded correct")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Run the checks on the real data**

```bash
UV_FROZEN=1 uv run --with sqlglot python prep/livesqlbench/check_grader.py 2>&1 | tail -5
```

Expected: `{('primary', 'correct'): 309, ('order_only', 'correct'): 72}`
and `0 of 381 gold answers not graded correct`.

If some fail, find the cause before changing anything; do not touch the
grader to make the number come out. Likely causes:
- a multi-statement gold that the single-block join changes, from `remove_distinct` collapsing whitespace or a statement ending in a comment;
- the memory limit.

Record the cause and the fix as a ruling. Never relax `matches`.

```bash
UV_FROZEN=1 uv run --with sqlglot python prep/livesqlbench/freeze.py 2>&1 | tail -2
UV_FROZEN=1 uv run python - <<'EOF'
import json, os
from pathlib import Path
d = Path(os.environ.get("LSB_DATA", Path.home() / "data" / "livesqlbench"))
a = json.load(open(d / "tasks_frozen_full_v1.json", encoding="utf-8"))
b = json.load(open(d / "tasks_frozen_full_v1.json.rebuilt", encoding="utf-8"))
diff = sorted(k for k in a.keys() | b.keys() if a.get(k) != b.get(k))
print("identical" if not diff else f"{len(diff)} differ: {diff[:10]}")
EOF
```

Expected: `identical`. The freeze reproduces the frozen task set. If a few
differ, check them against `prep/freeze_verdicts.json`'s `strict_runs`.
Five-run flakiness is the only acceptable cause; record it, and keep the
committed frozen file as it is.

Also run `UV_FROZEN=1 uv run python prep/livesqlbench/verify_translations.py 2>&1 | tail -3`.
Expected: all 22 `ok` translations pass.

`convert.py` and `gold.py` need the PostgreSQL container and are not
re-run. Check they import cleanly up to the DSN guard:
`LSB_PG_DSN= UV_FROZEN=1 uv run --with psycopg2-binary python prep/livesqlbench/gold.py x 2>&1 | tail -1`
prints the "set LSB_PG_DSN" message.

- [ ] **Step 4: README**

`prep/livesqlbench/README.md`, at most 40 lines:
- what each script does, and its order: `convert` -> `gold` -> `freeze` -> `verify_translations` -> `check_grader`;
- the environment: `LSB_DATA`, and `LSB_PG_DSN` for the first two;
- which files under `LSB_DATA` each script reads and writes;
- that nothing they produce is committed;
- the expected outputs: 309/72/30, 381/381 correct.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add prep/livesqlbench
git commit -m "prep: the LiveSQLBench data scripts, reading LSB_DATA; the gold-through-grader check"
```

---

### Task 9: DABStep equivalence gate and the README

**Files:**
- Modify: `README.md` (experiment README: a LiveSQLBench section)

- [ ] **Step 1: The gate**

```bash
UV_FROZEN=1 uv run python analysis/arm_surface.py > "$EQ/after/arm_surface.json" 2>/dev/null
diff "$EQ/before/arm_surface.json" "$EQ/after/arm_surface.json" && echo SURFACE-SAME
for f in results/*.jsonl; do UV_FROZEN=1 uv run python -m dce.stats "$f" > "$EQ/after/$(basename "$f").stats" 2>&1; done
UV_FROZEN=1 uv run python analysis/knowledge_delivery.py > "$EQ/after/knowledge_delivery.txt" 2>/dev/null
n=0; for f in "$EQ"/before/*; do b=$(basename "$f"); [ "$b" = pytest.txt ] && continue; diff -q <(norm "$EQ/before/$b") <(norm "$EQ/after/$b") >/dev/null || { echo "DIFF $b"; n=$((n+1)); }; done; echo "differing: $n"
UV_FROZEN=1 uv run pytest -q --deselect tests/test_replay.py::test_sql_statements_are_ordered_and_only_from_sql_tools 2>&1 | tail -1
(cd ../.. && prek run --all-files)
git status --short uv.lock
```

Expected:
- `SURFACE-SAME` and `differing: 0`;
- all tests pass;
- prek clean;
- `uv.lock` unmodified.

- [ ] **Step 2: README**

Add a `## LiveSQLBench` section after `## Run`, in the README's style:
- `LSB_DATA` and its layout (link `prep/livesqlbench/README.md`), and what is never committed;
- the run command:

```bash
uv run python -m dce.runner --benchmark livesqlbench --n 12 \
  --arms schema_only manual_prompt --models gpt-6-luna --max-spend 15 \
  --out results/livesqlbench/smoke.jsonl
```

- that results and traces default to `results/livesqlbench/` and `traces/livesqlbench/`;
- the grading rule in two sentences, linking `dce/benchmarks/lsb_grade.py`;
- that `dce.stats` reports `primary` and `order_only` separately, with `primary` the pre-registered measure;
- that `manual_compiled` and `contract` come with the compiler (PR C).

Also fix the stale `--arms` row in the `## Run` flag table: "Default: the benchmark's arms (DABStep: the four in `ARMS`)".

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "README: running LiveSQLBench"
```

---

### Task 10: Two-arm LiveSQLBench smoke (spends money: ask first)

12 tasks, one from each of 12 databases, `schema_only` and `manual_prompt`,
on `gpt-6-luna`: 24 rows. It costs cents, but it uses the gateway key.
**Ask the user before launching, and launch only after they say yes.**

- [ ] **Step 1: Run**

```bash
set -a; . ./.env; set +a
UV_FROZEN=1 uv run python -m dce.runner --benchmark livesqlbench --n 12 \
  --arms schema_only manual_prompt --models gpt-6-luna \
  --max-spend 15 --workers 4 --retry-pass \
  --out "$EQ/lsb-smoke.jsonl" --traces "$EQ/lsb-smoke-traces" > "$EQ/lsb-smoke.log" 2>&1
echo "exit=$?"; sed -E 's#https?://[^ /"]+#<host>#g' "$EQ/lsb-smoke.log" | grep -vE '^(Uni|I)nstalled' | tail -4
```

Expected: exit 0 and 24 rows. `--max-spend 15` is a guard: each task group
reserves about $3.30 before it runs (the worst case), and real spend is
cents.

- [ ] **Step 2: Check the rows**

```bash
UV_FROZEN=1 uv run python - "$EQ/lsb-smoke.jsonl" <<'EOF' 2>/dev/null
import json, re, sys
from collections import Counter
from pathlib import Path
rows = [json.loads(l) for l in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()]
print("rows", len(rows), "dbs", len({r["db"] for r in rows}))
print("benchmark", {r["benchmark"] for r in rows}, "group==db", all(r["group"] == r["db"] for r in rows))
print("subsets", Counter(r.get("subset") for r in rows))
print("verdicts", sorted(Counter((r["arm"], r["verdict"]) for r in rows).items()))
print("failures", Counter(r.get("failure") for r in rows))
print("gold is a digest", all(re.fullmatch(r"[0-9a-f]{64}", r["gold"]) for r in rows))
print("answer_normalized is SQL or empty", Counter(bool(r["answer_normalized"]) for r in rows))
print("db_corrupted", Counter(r.get("db_corrupted") for r in rows))
print("spent $%.2f" % sum(r.get("usd") or 0 for r in rows))
EOF
UV_FROZEN=1 uv run python -m dce.stats "$EQ/lsb-smoke.jsonl" 2>/dev/null | grep -E "^## subset|^### |^(schema_only|manual_prompt)|db=" | head -30
```

Expected:
- 24 rows over 12 databases; `benchmark` is `livesqlbench` everywhere and `group` equals `db`;
- subsets are `primary` and/or `order_only`;
- every `gold` is a 64-hex digest;
- `db_corrupted` is `False`, or `True` only where an agent wrote to its working copy (check the trace before accepting a `True`);
- `dce.stats` prints a `## subset:` section for each subset present, with `db=` strata lines.

Then read three traces, one `correct`, one `incorrect` and one with a
`failure` if any, to confirm three things:
- the agent saw the schema, the meanings and, on `manual_prompt`, the KB;
- the final message ends with a ```sql block;
- `answer_normalized` is that block.

- [ ] **Step 3: Report**

Tell the user:
- spend, and verdicts per arm and subset;
- failure categories;
- anything surprising in the traces.

The smoke file stays in the scratchpad and is not committed. Ask whether to
push and open PR B.

---

## Self-review notes

- Spec coverage:
  - **Open for PR B:** task sets (Tasks 2, 4, 7); the baseline role and pairs (Task 5, with the LiveSQLBench entry in PR C); `excluded_tasks` in `knowledge_delivery.py` (Task 5); smoke sampling (Task 3); results scoped by benchmark (Tasks 3 and 4); construction (Tasks 2, 3 and 7); wording (Task 4).
  - **Part 2:**
    - data outside the repo (Tasks 7 and 8);
    - task set (Task 7);
    - arms `schema_only`/`manual_prompt` with DDL, meanings and `PG_COMPAT` (Task 7);
    - grading (Task 6);
    - the gold-through-grader check (Task 8);
    - the vendored helpers (Task 6);
    - prep scripts (Task 8).
  - **Testing:** the fixture with a two-table DuckDB, a six-entry KB and a parent-child pair (Task 7); `gold_ref` never exposing the gold (Task 7).
  - **Deferred to PR C, per the split:**
    - the compiler, both KB adapters and the file-access restriction test;
    - `manual_compiled`, `contract` and `contract_compiled`;
    - LiveSQLBench's `knowledge_delivery.py` entry.
- `Task.meta` for LiveSQLBench holds `db`, `high_level` and `order`. `row_fields` exposes the same three, and `subset` travels as the generic field.
