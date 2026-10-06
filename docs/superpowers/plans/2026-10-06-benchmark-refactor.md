# Benchmark refactor (PR A) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `dce` harness benchmark-agnostic behind a `Benchmark`
protocol, with DABStep as the only implementation and no change in DABStep
behaviour.

**Architecture:** A small protocol module (`dce/benchmark.py`) defines
`Task`, `Grade`, `Benchmark` and a registry. The arm machinery every
benchmark shares moves to `dce/tools.py`; everything DABStep-specific
(arms, prompts, golds, task selection, grading) moves to
`dce/benchmarks/dabstep.py`. `run_task`, the runner and the analysis call
the protocol instead of DABStep code. `dce/arms.py` stays as a re-export.
Equivalence is proven by before/after dumps, not assumed.

**Tech Stack:** Python 3.12, uv, pytest, pydantic-ai, DuckDB, scipy; lint
and types through prek (ruff, ty).

**Spec:** `docs/superpowers/specs/2026-10-06-livesqlbench-harness-design.md`,
Part 1. Read Part 1 before starting; this plan argues from it.

All paths below are relative to `experiments/dabstep-contract-eval/` unless
they start with `docs/`. Run every command from that directory.

## Global Constraints

- Run Python through `uv run`; lint and type-check through `prek run --all-files` (run from the repo root), never bare `ruff`/`ty`.
- Never commit with `--no-verify`; fix whatever the hooks report.
- No AI attribution anywhere: no `Co-Authored-By: Claude`, no "Generated with", no robot emoji, in commits, PR text, code or docs.
- No internal names (employer, cluster, namespace, gateway host) in any committed file, commit message or PR.
- Open files with `encoding="utf-8"` in new code; no f-strings without a placeholder; no emojis or Unicode symbols in scripts or tests.
- Match the surrounding code: its comment density and its habit of explaining *why* in comments.
- DABStep behaviour must not change: same prompts, tool surfaces, verdicts, row values (apart from the two new fields `benchmark` and `group`), stats output and analysis output.
- Readers treat a row with no `benchmark` field as `dabstep`, and a row with no `group` field as having `group` = its `level`.
- Do not edit `results/`, `contract/`, `data/` or `traces/`.
- `uv.lock` must not change in this PR. If `uv sync` rewrites it, run `git restore uv.lock` before committing.
- Branch: `livesqlbench-harness`. Commit after each task; push only when the user asks, and to the `fork` remote.

## Review Focus

1. **A results file with rows from two benchmarks.** `dce.stats` must refuse it with a clear message rather than mix two benchmarks' arms. The test is in Task 7.
2. **A row whose `group` is present but `None`.** `group_of` must fall back to `level` exactly as it does for a missing field, or a salvage-envelope row lands in a `None` stratum. The test is in Task 2.
3. **A worker switching databases with a stale `.wal` beside the old copy.** The old working copy and its sidecar must both go before the new copy is made, so no sidecar replays onto a fresh file. The test is in Task 6.
4. **A benchmark method that raises while a row's provenance is being built.** `gold_ref`, `arm_digest`, `golds_hash` and `scorer` must fail as a free `construction_error`, before `build_arm` opens a connection and before any model call. The test is in Task 5.
5. **A `--n` smoke sample after the change.** It must pick the same DABStep tasks as before the change; `group` must reproduce the old `level` strata and their order. The test is in Task 6.

---

### Task 1: Equivalence baselines

Capture what "unchanged" means before any code moves. The dumps go to the
session scratchpad, not the repo; only the dump script is committed.

**Files:**
- Create: `analysis/arm_surface.py`

**Interfaces:**
- Produces: `analysis/arm_surface.py`, which prints JSON
  `{arm: {"system_prompt": str, "tools": [{"name", "description", "parameters"}]}}`
  for every DABStep arm. Task 9 updates its builder to the new API.

- [ ] **Step 1: Write the dump script (pre-refactor API)**

```python
"""Each DABStep arm's system prompt and tool surface, as JSON.

The equivalence check for the Benchmark refactor: run it before the
refactor and after, and diff the two outputs. Every arm is built on its own
throwaway copy of the database, never on `data/dabstep.duckdb` itself (see
`dce.tools`' module docstring for why an arm must not open the pristine
file).

Run:  uv run python analysis/arm_surface.py > arm_surface.json
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

CONTEXT = ROOT / "data" / "hf" / "data" / "context"
PRISTINE = ROOT / "data" / "dabstep.duckdb"


def _builder():
    """`(arms, build)`, where `build(arm, db)` returns an `ArmSetup`."""
    from dce.arms import ALL_ARMS, build_arm

    docs = {
        "manual": (CONTEXT / "manual.md").read_text(encoding="utf-8"),
        "payments_readme": (CONTEXT / "payments-readme.md").read_text(encoding="utf-8"),
    }
    return ALL_ARMS, lambda arm, db: build_arm(arm, db, docs)


def surface(setup) -> dict:
    return {
        "system_prompt": setup.system_prompt,
        "tools": [
            {
                "name": tool.tool_def.name,
                "description": tool.tool_def.description,
                "parameters": tool.tool_def.parameters_json_schema,
            }
            for tool in setup.tools
        ],
    }


def main() -> None:
    arms, build = _builder()
    out = {}
    with tempfile.TemporaryDirectory() as tmp:
        for arm in arms:
            working = Path(tmp) / f"{arm}.duckdb"
            shutil.copyfile(PRISTINE, working)
            setup = build(arm, working)
            try:
                out[arm] = surface(setup)
            finally:
                setup.close()
    print(json.dumps(out, indent=1, sort_keys=True, ensure_ascii=False))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it and check it covers every arm**

Set `EQ` to an `equiv/` folder inside the session scratchpad directory (the
system prompt names it; never `/tmp`), then:

```bash
mkdir -p "$EQ/before"
uv run python analysis/arm_surface.py > "$EQ/before/arm_surface.json"
uv run python -c "import json,sys; d=json.load(open(sys.argv[1], encoding='utf-8')); print(sorted(d), [len(v['tools']) for v in d.values()])" "$EQ/before/arm_surface.json"
```

Expected: six arms (`contract`, `contract_hollow`, `contract_uninterpreted`,
`manual_prompt`, `manual_resolved`, `schema_only`). The three ungoverned
arms have 3 tools each, and the governed arms have more. If `tool_def` is
missing on a governed tool, stop and report: the dump would be incomplete.

- [ ] **Step 3: Capture the other baselines**

```bash
for f in results/*.jsonl; do
  uv run python -m dce.stats "$f" > "$EQ/before/$(basename "$f").stats" 2>&1
done
uv run python analysis/knowledge_delivery.py > "$EQ/before/knowledge_delivery.txt" 2>&1
uv run pytest -q 2>&1 | tail -3 > "$EQ/before/pytest.txt"
ls "$EQ/before" | wc -l; cat "$EQ/before/pytest.txt"
```

Expected: one `.stats` file per results file, plus the three other files.
pytest reports all tests passing (426 when this plan was written).

- [ ] **Step 4: Commit**

```bash
git add analysis/arm_surface.py
git commit -m "analysis: dump each DABStep arm's prompt and tool surface"
```

---

### Task 2: The Benchmark protocol

**Files:**
- Create: `dce/benchmark.py`
- Test: `tests/test_benchmark.py`

**Interfaces:**
- Produces:
  - `Task(task_id: str, prompt: str, group: str, meta: Mapping[str, Any] = {})`, a frozen dataclass.
  - `Grade(verdict: str, failure: str | None = None)`, a frozen dataclass.
  - `Benchmark`, a `runtime_checkable` Protocol, exactly as in the spec's Part 1 interface.
  - `ROLES = ("baseline", "manual", "manual_plus", "contract")`.
  - `BENCHMARK_NAMES = ("dabstep",)` and `DEFAULT_BENCHMARK = "dabstep"`.
  - `benchmark_class(name: str) -> type[Benchmark]`, which raises `ValueError` for an unknown name.
  - `benchmark_of(row: Mapping) -> str` and `group_of(row: Mapping) -> str`.

- [ ] **Step 1: Write the failing tests**

```python
from dataclasses import FrozenInstanceError

import pytest
from dce.benchmark import (
    DEFAULT_BENCHMARK,
    ROLES,
    Grade,
    Task,
    benchmark_class,
    benchmark_of,
    group_of,
)


def test_task_is_frozen_and_meta_defaults_empty():
    task = Task("1", "q", "hard")
    assert task.meta == {}
    with pytest.raises(FrozenInstanceError):
        task.prompt = "other"  # type: ignore[misc]


def test_grade_failure_defaults_to_none():
    assert Grade("correct").failure is None


def test_roles_are_the_four_the_analysis_reads():
    assert ROLES == ("baseline", "manual", "manual_plus", "contract")


def test_unknown_benchmark_is_refused():
    with pytest.raises(ValueError, match="unknown benchmark"):
        benchmark_class("nope")


def test_a_row_without_a_benchmark_field_is_dabstep():
    assert DEFAULT_BENCHMARK == "dabstep"
    assert benchmark_of({}) == "dabstep"
    assert benchmark_of({"benchmark": None}) == "dabstep"
    assert benchmark_of({"benchmark": "livesqlbench"}) == "livesqlbench"


def test_group_falls_back_to_level_when_absent_or_null():
    assert group_of({"group": "db1", "level": "hard"}) == "db1"
    assert group_of({"level": "hard"}) == "hard"
    # A salvage envelope or an older row can carry `group: null`; it must
    # read as the level, not as a `None` stratum.
    assert group_of({"group": None, "level": "easy"}) == "easy"
    assert group_of({}) == "unknown"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_benchmark.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dce.benchmark'`.

- [ ] **Step 3: Implement `dce/benchmark.py`**

```python
"""What the harness needs from a benchmark, and nothing more.

The runner, `run_task` and the analysis talk to a benchmark only through
`Benchmark`. DABStep (`dce.benchmarks.dabstep`) is the first implementation;
the hardened runner it grew up with -- the budget ledger, the retry pass,
the lock, torn-tail repair, construction retries -- is what every later
benchmark reuses.

Class attributes carry everything the analysis reads, so `dce.stats` can
resolve a row's benchmark (`benchmark_class(benchmark_of(row))`) without
loading golds or tasks. The three static hooks at the end exist for the same
reason: re-grading and the end-to-end view work from a stored row, and the
analysis must not import any one benchmark's scorer to do it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from dce.tools import ArmSetup

#: The arms the analysis compares, by role rather than by name.
#: `manual_plus` is the manual plus whatever knowledge the contract adds,
#: delivered as prose -- `manual_resolved` on DABStep -- so
#: (manual_plus - manual) / (contract - manual) is the share of the
#: contract's lead that is knowledge rather than delivery.
ROLES: tuple[str, ...] = ("baseline", "manual", "manual_plus", "contract")

BENCHMARK_NAMES: tuple[str, ...] = ("dabstep",)

#: What a row with no `benchmark` field is: every row written before the
#: field existed is a DABStep row.
DEFAULT_BENCHMARK = "dabstep"


@dataclass(frozen=True)
class Task:
    task_id: str
    prompt: str  # the full user message
    group: str  # stratum for smoke sampling and per-group stats
    meta: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Grade:
    verdict: str  # "correct" | "incorrect" | "ungraded"
    failure: str | None = None  # grading-side failure category, if any


@runtime_checkable
class Benchmark(Protocol):
    name: str
    arms: tuple[str, ...]
    extra_arms: tuple[str, ...]
    governed_arms: frozenset[str]
    roles: Mapping[str, str]
    primary: tuple[str, str, str] | None
    group_label: str
    excluded_tasks: frozenset[str]

    def tasks(self) -> list[Task]: ...
    def pristine_db(self, task: Task) -> Path: ...
    def build_arm(self, arm: str, task: Task, db: Path) -> ArmSetup: ...
    def arm_digest(self, arm: str, task: Task) -> str: ...
    def gold_ref(self, task: Task) -> str | None: ...
    def grade(self, task: Task, answer: str) -> Grade: ...
    def normalize(self, answer: str) -> str: ...
    def golds_hash(self) -> str: ...
    def row_fields(self, task: Task) -> dict: ...

    @staticmethod
    def scorer() -> str: ...
    @staticmethod
    def rescore(row: dict) -> str | None: ...
    @staticmethod
    def e2e_correct(row: dict) -> bool: ...


def benchmark_class(name: str) -> type[Benchmark]:
    """The class registered under `name`. Imported lazily, so reading one
    benchmark's rows never imports another's scorer."""
    if name == "dabstep":
        from dce.benchmarks.dabstep import DABStep

        return DABStep
    raise ValueError(f"unknown benchmark: {name!r}; expected one of {BENCHMARK_NAMES}")


def benchmark_of(row: Mapping) -> str:
    return row.get("benchmark") or DEFAULT_BENCHMARK


def group_of(row: Mapping) -> str:
    """The row's stratum. `group` when the row has one; otherwise `level`,
    which is what DABStep rows written before `group` existed carry."""
    group = row.get("group")
    if group is None:
        group = row.get("level", "unknown")
    return group
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_benchmark.py -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add dce/benchmark.py tests/test_benchmark.py
git commit -m "dce: the Benchmark protocol, Task, Grade and the registry"
```

---

### Task 3: Shared arm machinery in `dce/tools.py`, with an `init_sql` hook

Move the code that every benchmark's arms share out of `dce/arms.py`,
verbatim except for the hook. DABStep's own arm code stays in `dce/arms.py`
until Task 4.

**Files:**
- Create: `dce/tools.py`
- Modify: `dce/arms.py` (remove the moved code and import it from `dce.tools`)
- Modify: `tests/test_arms.py` (monkeypatch targets only)
- Test: `tests/test_tools.py`

**Interfaces:**
- Produces, in `dce.tools`:
  - `ArmSetup`, `MAX_ROWS`, `HARNESS_QUERY_SECONDS`, `HARNESS_MEMORY_LIMIT`, `IntegrityCheck`;
  - `make_working_copy(pristine_db, working_db) -> Path`;
  - `check_and_restore(working_db, pristine_db) -> IntegrityCheck`;
  - `_BoundedDuckDBAdapter(db_path, *, init_sql: tuple[str, ...] = ())`;
  - `_ungoverned_tools(db_path, *, init_sql: tuple[str, ...] = ()) -> list[Tool]`;
  - `_governed_tools(db_path, *, contract, init_sql: tuple[str, ...] = ()) -> (tools, session, adapter)`. `contract` is now required;
  - `_append_truncation_marker`, `_TRUNCATION_MARKER`, `_RUN_QUERY_PAYLOAD_MARKER`, `_sha256`, `_wal_path`, `_sha256_with_sidecar`, `_connect`.

- [ ] **Step 1: Write the failing tests**

```python
from pathlib import Path

import duckdb
import pytest
from dce.tools import _BoundedDuckDBAdapter, _ungoverned_tools

# DuckDB's `/` is float division unless `integer_division` is set, which
# makes it a one-statement probe for "did init_sql run on this connection".
INTEGER_DIVISION = ("SET integer_division = true",)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    path = tmp_path / "t.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE t AS SELECT 7 AS x")
    con.close()
    return path


def _tool(tools, name: str):
    return next(t for t in tools if t.name == name)


def test_bounded_adapter_runs_init_sql_on_its_connection(db):
    adapter = _BoundedDuckDBAdapter(db, init_sql=INTEGER_DIVISION)
    try:
        result = adapter.execute_limited("SELECT x / 2 AS h FROM t", 5)
    finally:
        adapter.connection.close()
    assert result.rows[0][0] == 3


def test_bounded_adapter_without_init_sql_is_unchanged(db):
    adapter = _BoundedDuckDBAdapter(db)
    try:
        result = adapter.execute_limited("SELECT x / 2 AS h FROM t", 5)
    finally:
        adapter.connection.close()
    assert result.rows[0][0] == 3.5


def test_ungoverned_execute_sql_runs_init_sql(db):
    tools = _ungoverned_tools(db, init_sql=INTEGER_DIVISION)
    out = _tool(tools, "execute_sql").function("SELECT x / 2 AS h FROM t")
    assert out.splitlines() == ["h", "3"]


def test_a_failing_init_sql_closes_the_connection_and_raises(db):
    with pytest.raises(duckdb.Error):
        _BoundedDuckDBAdapter(db, init_sql=("SET no_such_setting = 1",))
    # The file is not left locked by a half-built adapter.
    duckdb.connect(str(db)).close()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_tools.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dce.tools'`.

- [ ] **Step 3: Create `dce/tools.py` by moving code**

Move these from `dce/arms.py` into `dce/tools.py` without changing them:
- the module docstring, with only its first paragraph replaced (below);
- `MAX_ROWS` and its comment block;
- `HARNESS_QUERY_SECONDS`, `HARNESS_MEMORY_LIMIT` and `_TRUNCATION_MARKER`;
- `_BoundedDuckDBAdapter` and the `# CUT FROM 1,000` comment after it;
- `ArmSetup`, `_sha256`, `_wal_path`, `_sha256_with_sidecar`, `make_working_copy`, `IntegrityCheck` and `check_and_restore`;
- `_ungoverned_tools`, `_RUN_QUERY_PAYLOAD_MARKER`, `_append_truncation_marker` and `_governed_tools`.

Keep the imports they need: `csv`, `hashlib`, `io`, `json`, `shutil`,
`dataclass`, `Path`, `duckdb`, `Tool`, `QueryResult` and `DuckDBAdapter`.
Do not move `ARMS`, `EXTRA_ARMS`, `ALL_ARMS`, `DATA_NOTE`, `GOVERNED_ARMS`,
`BASE_PROMPT` or `build_arm`.

Replace the docstring's first paragraph (the three lines starting "The three
context configurations under comparison.") with:

```
"""Arm machinery every benchmark shares: the bounded DuckDB adapter, the
ungoverned and governed tool sets, and the working-copy discipline.

A benchmark decides what context each of its arms carries
(`dce.benchmarks.<name>.build_arm`); everything below is how any arm reaches
the database, and it is the same code for every benchmark so that no arm,
on any benchmark, is bounded differently from another. The examples below
name DABStep's arms, where each rule was learned.
```

Then make four edits for the hook.

`_BoundedDuckDBAdapter.__init__`:

```python
    def __init__(self, db_path: Path, *, init_sql: tuple[str, ...] = ()) -> None:
        super().__init__(str(db_path), memory_limit=HARNESS_MEMORY_LIMIT)
        # Per connection, before any query: a benchmark whose gold SQL was
        # written for another engine sets the dialect switches it needs here
        # (LiveSQLBench's PostgreSQL null ordering and integer division).
        # Every arm passes the same statements, so no arm reads the data
        # under different semantics. DABStep passes none.
        try:
            for statement in init_sql:
                self.connection.execute(statement)
        except Exception:
            self.connection.close()
            raise
```

A new helper, placed just above `_ungoverned_tools`:

```python
def _connect(db_path: Path, init_sql: tuple[str, ...]) -> duckdb.DuckDBPyConnection:
    """`duckdb.connect` plus the benchmark's per-connection statements, so the
    ungoverned arms' metadata tools see the same session settings as their
    queries do."""
    con = duckdb.connect(str(db_path))
    try:
        for statement in init_sql:
            con.execute(statement)
    except Exception:
        con.close()
        raise
    return con
```

In `_ungoverned_tools`, change the signature to
`def _ungoverned_tools(db_path: Path, *, init_sql: tuple[str, ...] = ()) -> list[Tool]:`.
In `list_tables` and `describe_table`, replace
`con = duckdb.connect(str(db_path))` with `con = _connect(db_path, init_sql)`.
In `execute_sql`, replace `adapter = _BoundedDuckDBAdapter(db_path)` with
`adapter = _BoundedDuckDBAdapter(db_path, init_sql=init_sql)`.

In `_governed_tools`, make `contract` required. The default loaded DABStep's
contract, and `dce.tools` must not know DABStep:

```python
def _governed_tools(db_path: Path, *, contract, init_sql: tuple[str, ...] = ()):
    from agentic_data_contracts import create_pydantic_ai_tools
    from agentic_data_contracts.core.session import ContractSession
    from agentic_data_contracts.tools.factory import create_tools

    session = ContractSession(contract)
    adapter = _BoundedDuckDBAdapter(db_path, init_sql=init_sql)
```

The rest of the function body is unchanged.

- [ ] **Step 4: Point `dce/arms.py` at the moved code**

In `dce/arms.py`, delete everything that moved, and its now-unused imports.
Replace the module docstring with:

```python
"""DABStep's arms. The machinery every benchmark shares -- the bounded
adapter, the tool sets, the working-copy discipline and its CALL ORDER --
lives in `dce.tools`; read its module docstring before changing anything an
arm does with the database.
"""
```

Add:

```python
from dce.tools import (  # noqa: F401 - re-exported for existing importers
    HARNESS_MEMORY_LIMIT,
    HARNESS_QUERY_SECONDS,
    MAX_ROWS,
    ArmSetup,
    IntegrityCheck,
    _BoundedDuckDBAdapter,
    _governed_tools,
    _ungoverned_tools,
    check_and_restore,
    make_working_copy,
)
```

`build_arm` calls `_governed_tools(db_path, contract=contract)` and already
passes `contract`, so it needs no change.

- [ ] **Step 5: Move the test monkeypatches to the module that reads the constants**

A monkeypatch on `dce.arms.MAX_ROWS` no longer reaches the code that reads
`MAX_ROWS`, which now lives in `dce.tools`. In `tests/test_arms.py`, in
these tests:
- `test_row_cap_parity_between_ungoverned_and_governed_arms`
- `test_truncation_marker_present_when_a_result_is_cut`
- `test_every_arm_gets_the_same_marker`
- `test_execute_sql_is_bounded_in_time`

replace `import dce.arms as arms_mod` with `import dce.tools as arms_mod`.
Leave every other line as it is.

- [ ] **Step 6: Run the tests**

Run: `uv run pytest tests/test_tools.py tests/test_arms.py tests/test_agent.py tests/test_runner.py -q 2>&1 | tail -3`
Expected: all pass.

Run: `uv run python analysis/arm_surface.py | diff - "$EQ/before/arm_surface.json" && echo SAME`
Expected: `SAME`.

- [ ] **Step 7: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/tools.py dce/arms.py tests/test_tools.py tests/test_arms.py
git commit -m "dce: move the shared arm machinery to dce.tools, with an init_sql hook"
```

---

### Task 4: DABStep as a Benchmark

**Files:**
- Create: `dce/benchmarks/__init__.py` (empty)
- Create: `dce/benchmarks/dabstep.py`
- Modify: `dce/arms.py` (becomes a re-export only)
- Modify: `dce/runner.py` (import `select_tasks`, `_load_golds` and `UNGOLDED_MODES` from the new module)
- Modify: `dce/agent.py` (import `arm_digest` from the new module)
- Test: `tests/test_dabstep.py`

**Interfaces:**
- Consumes: `Task`, `Grade` and `Benchmark` (Task 2); `ArmSetup`, `_ungoverned_tools` and `_governed_tools` (Task 3).
- Produces, in `dce.benchmarks.dabstep`:
  - constants: `BASE_PROMPT`, `ARMS`, `EXTRA_ARMS`, `ALL_ARMS`, `DATA_NOTE`, `GOVERNED_ARMS`, `UNGOLDED_MODES`, `CONTEXT_DIR`;
  - module functions: `build_arm(arm, db_path, docs) -> ArmSetup`, `arm_digest(arm) -> str`, `select_tasks(tasks, golds, ungolded="skip")`, `_load_golds(path) -> (dict, str)`, `load_docs(context_dir) -> dict[str, str]`;
  - `class DABStep`, with:
    - `__init__(self, *, db: Path, golds: dict[str, str], golds_hash: str, docs: dict[str, str], records: list[dict] | tuple = ())`;
    - the classmethod `from_files(*, db, golds_path, tasks_path, context_dir=CONTEXT_DIR, ungolded="skip")`;
    - the staticmethod `task(record: dict) -> Task`;
    - every `Benchmark` member.

- [ ] **Step 1: Write the failing tests**

```python
import json
from pathlib import Path

import duckdb
import pytest
from dce.benchmark import Benchmark, Grade, Task, benchmark_class
from dce.benchmarks.dabstep import ARMS, EXTRA_ARMS, GOVERNED_ARMS, DABStep, arm_digest
from dce.data import DATASET_REVISION
from dce.frozen import digest, hollow_digest, uninterpreted_digest
from dce.golds import PLURALITY_THRESHOLD, VERIFIED_WRONG_GOLDS

DOCS = {"manual": "FEE RULE ALPHA: match on card_scheme.", "payments_readme": "cols"}
RECORD = {
    "task_id": "7",
    "question": "What is X?",
    "guidelines": "A number.",
    "level": "hard",
}


def _bench(golds=None, tmp: Path | None = None) -> DABStep:
    return DABStep(
        db=(tmp or Path(".")) / "d.duckdb",
        golds={"7": "0.12"} if golds is None else golds,
        golds_hash="h",
        docs=DOCS,
    )


def test_the_registry_returns_dabstep():
    assert benchmark_class("dabstep") is DABStep


def test_dabstep_satisfies_the_protocol():
    assert isinstance(_bench(), Benchmark)


def test_class_attributes_the_analysis_reads():
    assert DABStep.name == "dabstep"
    assert DABStep.arms == ARMS and DABStep.extra_arms == EXTRA_ARMS
    assert DABStep.governed_arms == GOVERNED_ARMS
    assert DABStep.roles == {
        "manual": "manual_prompt",
        "manual_plus": "manual_resolved",
        "contract": "contract",
    }
    assert DABStep.primary == (
        "deepseek/deepseek-v4-pro-0813",
        "manual_prompt",
        "contract",
    )
    assert DABStep.group_label == "level"
    assert DABStep.excluded_tasks == frozenset(VERIFIED_WRONG_GOLDS)


def test_task_prompt_is_the_question_and_guidelines_exactly():
    task = DABStep.task(RECORD)
    assert task == Task(
        "7", "What is X?\n\nAnswer guidelines: A number.", "hard", RECORD
    )
    no_guidelines = DABStep.task({"task_id": "8", "question": "Q"})
    assert no_guidelines.prompt == "Q\n\nAnswer guidelines: "
    assert no_guidelines.group == "unknown"


def test_grade_uses_the_dabstep_scorer_and_ungraded_without_gold():
    task = DABStep.task(RECORD)
    assert _bench().grade(task, "0.12") == Grade("correct")
    assert _bench().grade(task, "0.13") == Grade("incorrect")
    assert _bench(golds={}).grade(task, "0.12") == Grade("ungraded")


def test_row_provenance_matches_the_old_row_fields():
    bench, task = _bench(), DABStep.task(RECORD)
    assert bench.gold_ref(task) == "0.12"
    assert _bench(golds={}).gold_ref(task) is None
    assert bench.normalize(" [0.12] ") == "0.12"
    assert bench.row_fields(task) == {"level": "hard"}
    assert bench.golds_hash() == "h"


def test_each_governed_arm_is_stamped_with_the_contract_it_loads():
    task = DABStep.task(RECORD)
    bench = _bench()
    assert bench.arm_digest("contract", task) == arm_digest("contract") == digest()
    assert arm_digest("contract_hollow") == hollow_digest()
    assert arm_digest("contract_uninterpreted") == uninterpreted_digest()
    assert arm_digest("manual_resolved") == digest()
    assert arm_digest("schema_only") == digest()


def test_build_arm_matches_the_module_function(tmp_path: Path):
    from dce.benchmarks.dabstep import build_arm

    db = tmp_path / "d.duckdb"
    duckdb.connect(str(db)).close()
    bench = _bench(tmp=tmp_path)
    via_benchmark = bench.build_arm("manual_prompt", DABStep.task(RECORD), db)
    direct = build_arm("manual_prompt", db, DOCS)
    assert via_benchmark.system_prompt == direct.system_prompt
    assert bench.pristine_db(DABStep.task(RECORD)) == db


def test_rescore_and_e2e_work_from_a_stored_row():
    row = {"answer": "The answer is:\n\n0.12", "gold": "0.12", "verdict": "incorrect"}
    assert DABStep.rescore({"answer": "0.12", "gold": "0.12"}) == "correct"
    assert DABStep.rescore({"answer": "0.13", "gold": "0.12"}) == "incorrect"
    assert DABStep.e2e_correct(row) is True
    assert DABStep.e2e_correct({**row, "answer": "0.13"}) is False


def test_from_files_selects_golded_tasks_and_reads_the_docs(tmp_path: Path):
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
    records = [
        {"task_id": "1", "question": "q1", "guidelines": "g", "level": "easy"},
        {"task_id": "2", "question": "q2", "guidelines": "g", "level": "hard"},
    ]
    (tmp_path / "tasks.json").write_text(json.dumps(records), encoding="utf-8")
    ctx = tmp_path / "ctx"
    ctx.mkdir()
    (ctx / "manual.md").write_text("M", encoding="utf-8")
    (ctx / "payments-readme.md").write_text("R", encoding="utf-8")

    bench = DABStep.from_files(
        db=tmp_path / "d.duckdb",
        golds_path=tmp_path / "golds.json",
        tasks_path=tmp_path / "tasks.json",
        context_dir=ctx,
    )
    assert [t.task_id for t in bench.tasks()] == ["1"]
    assert bench.golds_hash() == golds_sha256(golds)
    run_all = DABStep.from_files(
        db=tmp_path / "d.duckdb",
        golds_path=tmp_path / "golds.json",
        tasks_path=tmp_path / "tasks.json",
        context_dir=ctx,
        ungolded="run",
    )
    assert [t.task_id for t in run_all.tasks()] == ["1", "2"]


def test_unknown_arm_raises(tmp_path: Path):
    with pytest.raises(ValueError, match="unknown arm"):
        _bench().build_arm("nope", DABStep.task(RECORD), tmp_path / "d.duckdb")
```

The e2e test row assumes `score_final_paragraph` reads the last paragraph.
Check that against `dce/grade.py:final_paragraph` before running. If the
rule differs, change only the test's answer text so that its final
paragraph is `0.12`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_dabstep.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dce.benchmarks'`.

- [ ] **Step 3: Create `dce/benchmarks/dabstep.py`**

Move into it, verbatim with their comments:
- from `dce/arms.py`: `ARMS`, `EXTRA_ARMS`, `ALL_ARMS`, `DATA_NOTE`, `GOVERNED_ARMS`, `BASE_PROMPT` and `build_arm`;
- from `dce/agent.py`: `arm_digest`;
- from `dce/runner.py`: `UNGOLDED_MODES`, `select_tasks` and `_load_golds`.

`build_arm` keeps its signature `(arm, db_path, docs)`. Its body calls
`_governed_tools(db_path, contract=contract)` imported from `dce.tools`.
Then add the rest:

```python
"""DABStep (Adyen/DABStep, `dce.data.DATASET_REVISION`) as a `Benchmark`.

Every DABStep-specific decision the harness used to make inline lives here:
the arms and their prompts, the frozen contracts each governed arm loads,
the reconstructed golds and their checks, which tasks a sweep runs, and the
scorer. The modules this draws on (`dce.frozen`, `dce.grade`, `dce.golds`,
`dce.data`, `dce.hollow`, `dce.uninterpreted`) stay where they are, so the
commands in README.md and FINDINGS.md keep working.
"""

from __future__ import annotations

import json
from pathlib import Path

from dce.benchmark import Grade, Task
from dce.data import DATASET_REVISION
from dce.frozen import (
    digest,
    hollow_digest,
    load_contract,
    load_hollow_contract,
    load_uninterpreted_contract,
    uninterpreted_digest,
)
from dce.golds import PLURALITY_THRESHOLD, VERIFIED_WRONG_GOLDS, golds_sha256
from dce.tools import ArmSetup, _governed_tools, _ungoverned_tools

#: Where `dce.prepare` puts the vendor's context files; relative to the
#: experiment directory, as the runner's other default paths are.
CONTEXT_DIR = Path("data/hf/data/context")

# ... the moved constants, build_arm, arm_digest, UNGOLDED_MODES,
# select_tasks and _load_golds go here, unchanged ...


def load_docs(context_dir: Path = CONTEXT_DIR) -> dict[str, str]:
    """The two vendor documents the manual arms carry verbatim."""
    return {
        "manual": (context_dir / "manual.md").read_text(encoding="utf-8"),
        "payments_readme": (context_dir / "payments-readme.md").read_text(
            encoding="utf-8"
        ),
    }


class DABStep:
    name = "dabstep"
    arms = ARMS
    extra_arms = EXTRA_ARMS
    governed_arms = GOVERNED_ARMS
    roles = {
        "manual": "manual_prompt",
        "manual_plus": "manual_resolved",
        "contract": "contract",
    }
    #: The one pre-registered confirmatory comparison.
    primary = ("deepseek/deepseek-v4-pro-0813", "manual_prompt", "contract")
    group_label = "level"
    #: Golds verified wrong cannot be answered correctly by construction;
    #: the analysis drops their tasks before counting anything.
    excluded_tasks = frozenset(VERIFIED_WRONG_GOLDS)

    def __init__(
        self,
        *,
        db: Path,
        golds: dict[str, str],
        golds_hash: str,
        docs: dict[str, str],
        records: list[dict] | tuple = (),
    ) -> None:
        self._db = db
        self._golds = golds
        self._golds_hash = golds_hash
        self._docs = docs
        self._records = list(records)

    @classmethod
    def from_files(
        cls,
        *,
        db: Path,
        golds_path: Path,
        tasks_path: Path,
        context_dir: Path = CONTEXT_DIR,
        ungolded: str = "skip",
    ) -> DABStep:
        golds, golds_hash = _load_golds(golds_path)
        records = select_tasks(
            json.loads(tasks_path.read_text(encoding="utf-8")),
            golds,
            ungolded=ungolded,
        )
        return cls(
            db=db,
            golds=golds,
            golds_hash=golds_hash,
            docs=load_docs(context_dir),
            records=records,
        )

    @staticmethod
    def task(record: dict) -> Task:
        # The exact user message `run_task` sent before the refactor.
        prompt = (
            f"{record['question']}\n\nAnswer guidelines: {record.get('guidelines', '')}"
        )
        return Task(
            task_id=record["task_id"],
            prompt=prompt,
            group=record.get("level", "unknown"),
            meta=record,
        )

    def tasks(self) -> list[Task]:
        return [self.task(record) for record in self._records]

    def pristine_db(self, task: Task) -> Path:
        return self._db

    def build_arm(self, arm: str, task: Task, db: Path) -> ArmSetup:
        return build_arm(arm, db, self._docs)

    def arm_digest(self, arm: str, task: Task) -> str:
        return arm_digest(arm)

    def gold_ref(self, task: Task) -> str | None:
        # `.get` WITHOUT a default: a task with no reconstructed gold must
        # read as `None` (recorded `ungraded`), never as `""`, which would
        # be scored and manufacture an `incorrect`. See `select_tasks`.
        return self._golds.get(task.task_id)

    def grade(self, task: Task, answer: str) -> Grade:
        from dce.grade import score

        gold = self.gold_ref(task)
        if gold is None:
            # No gold EXISTS for this task (49 of DABStep's 450 -- see
            # `dce.golds`). Not a wrong answer: `ungraded` stays out of every
            # accuracy denominator in `dce.stats`.
            return Grade("ungraded")
        return Grade("correct" if score(answer, gold) else "incorrect")

    def normalize(self, answer: str) -> str:
        from dce.grade import _clean

        return _clean(answer)

    def golds_hash(self) -> str:
        return self._golds_hash

    def row_fields(self, task: Task) -> dict:
        return {"level": task.meta.get("level", "unknown")}

    @staticmethod
    def scorer() -> str:
        from dce.grade import active_scorer

        return active_scorer()

    @staticmethod
    def rescore(row: dict) -> str | None:
        from dce.grade import score

        if score(row.get("answer", ""), row.get("gold", "")):
            return "correct"
        return "incorrect"

    @staticmethod
    def e2e_correct(row: dict) -> bool:
        from dce.grade import score_final_paragraph

        return bool(score_final_paragraph(row.get("answer"), row.get("gold", "")))
```

`dce.grade` is imported inside the methods, as `dce.stats` imported it
before, so loading the class for its attributes stays cheap.

`select_tasks` and `_load_golds` reference `DATASET_REVISION`,
`PLURALITY_THRESHOLD` and `golds_sha256`. Keep those imports. Remove any
import ruff reports as unused.

- [ ] **Step 4: Turn `dce/arms.py` into a pure re-export**

```python
"""Compatibility re-exports. DABStep's arms live in `dce.benchmarks.dabstep`;
the machinery every benchmark shares lives in `dce.tools`. Kept so the
imports in `analysis/`, `deploy/README.md` and older commands keep working.
"""

from dce.benchmarks.dabstep import (  # noqa: F401
    ALL_ARMS,
    ARMS,
    BASE_PROMPT,
    DATA_NOTE,
    EXTRA_ARMS,
    GOVERNED_ARMS,
    build_arm,
)
from dce.tools import (  # noqa: F401
    HARNESS_MEMORY_LIMIT,
    HARNESS_QUERY_SECONDS,
    MAX_ROWS,
    ArmSetup,
    IntegrityCheck,
    _BoundedDuckDBAdapter,
    _governed_tools,
    _ungoverned_tools,
    check_and_restore,
    make_working_copy,
)
```

In `dce/agent.py`, replace the moved `arm_digest` definition with
`from dce.benchmarks.dabstep import arm_digest` next to the other `dce`
imports. In `dce/runner.py`, replace the three moved definitions with
`from dce.benchmarks.dabstep import UNGOLDED_MODES, _load_golds, select_tasks`.
Tasks 5 and 6 remove these temporary imports.

- [ ] **Step 5: Run the tests**

Run: `uv run pytest -q 2>&1 | tail -3`
Expected: all pass, plus the new `tests/test_dabstep.py`.

Run: `uv run python analysis/arm_surface.py | diff - "$EQ/before/arm_surface.json" && echo SAME`
Expected: `SAME`.

- [ ] **Step 6: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/benchmarks dce/arms.py dce/agent.py dce/runner.py tests/test_dabstep.py
git commit -m "dce: DABStep as the first Benchmark implementation"
```

---

### Task 5: `run_task` through the protocol

**Files:**
- Modify: `dce/agent.py` (`run_task`, `build_result_row`, `_priced_fallback_row`)
- Modify: `tests/test_agent.py`, `tests/test_trace.py`

**Interfaces:**
- Consumes: `Task` and `Benchmark` (Task 2); `DABStep` (Task 4).
- Produces:
  - `run_task(task: Task, arm: str, model: str, benchmark: Benchmark, db_path: Path, *, max_tool_calls=MAX_TOOL_CALLS, per_task_usd=1.00, agent_factory=None, trace_dir=None) -> dict`.
  - `task_row_fields(task: Task, benchmark: Benchmark) -> dict`, returning `{"task_id", **benchmark.row_fields(task), "benchmark", "group"}` in that order.
  - `build_result_row(*, task_fields: dict, arm, model, answer, answer_normalized, gold, verdict, forced_answer, trace_path, reasoning_tokens, reasoning_chars, in_tok, out_tok, cached_tok, turns, tool_calls, inspect_rejections, enforcement_blocks, retry_prompts, request_limit, token_cap, golds_hash, contract_digest: str, scorer: str, failure: str | None = None) -> dict`.
  - `_priced_fallback_row(*, task_fields, arm, model, gold, golds_hash, contract_digest, scorer, usage, verdict, note) -> dict`.
  - `dce.agent.arm_digest` no longer exists.

- [ ] **Step 1: Migrate the tests to the new signatures (they will fail)**

In `tests/test_agent.py`, under the existing `TASK` dict, add:

```python
from dce.benchmarks.dabstep import DABStep

DOCS = {"manual": "m", "payments_readme": "r"}
TASK_OBJ = DABStep.task(TASK)


def _bench(
    gold: str | None = "0.12", golds_hash: str = "deadbeef", docs=DOCS
) -> DABStep:
    """A DABStep holding one gold (or none) for `TASK`, with no files read."""
    golds = {} if gold is None else {TASK["task_id"]: gold}
    return DABStep(
        db=Path("unused.duckdb"), golds=golds, golds_hash=golds_hash, docs=docs
    )


TASK_FIELDS = {
    "task_id": "7",
    "level": "hard",
    "benchmark": "dabstep",
    "group": "hard",
}
```

Rewrite `ROW_KWARGS`. Replace `task=TASK` with `task_fields=TASK_FIELDS`
and add `contract_digest="cd"` and `scorer="official"`. Leave the other
keys as they are.

Rewrite every `run_task(...)` call in `tests/test_agent.py` and
`tests/test_trace.py` by this rule:

```
run_task(TASK, ARM, MODEL, DB, DOCS_EXPR, gold=G, golds_hash=H, **rest)
  ->  run_task(TASK_OBJ, ARM, MODEL, _bench(gold=G, golds_hash=H, docs=DOCS_EXPR), DB, **rest)
```

Keep `gold=""` as `_bench(gold="")`; it is a gold, not a missing one. Keep
`gold=None` as `_bench(gold=None)`. Where a test passes a non-`TASK` dict,
wrap it in `DABStep.task(...)` and build `_bench` golds keyed by that dict's
`task_id`. In `tests/test_trace.py`, import `TASK_OBJ` and `_bench` from
`tests.test_agent` alongside `ROW_KWARGS`.

Rewrite the `_priced_fallback_row(...)` calls. Replace `task=TASK` with
`task_fields=TASK_FIELDS`, and add `contract_digest="cd", scorer="official"`.

In `test_every_row_shape_carries_forced_answer`, update the
`_construction_error_row` call to its Task 6 signature:
`_construction_error_row(TASK_OBJ, "contract", "z-ai/glm-5.3-flash", _bench(gold="g", golds_hash="h"), RuntimeError("boom"))`.
That test stays red until Task 6 lands, which is expected.

Delete `test_each_governed_arm_is_stamped_with_the_contract_it_loads` from
`tests/test_agent.py`; Task 4 moved it to `tests/test_dabstep.py`.

Add these tests:

```python
def test_rows_carry_benchmark_and_group_after_the_dabstep_fields(tmp_path: Path):
    class Fake:
        def run_sync(self, *a, usage=None, **k):
            return _fake_result("0.12", usage)

    row = run_task(
        TASK_OBJ,
        "schema_only",
        "z-ai/glm-5.3-flash",
        _bench(),
        tmp_path / "x.duckdb",
        agent_factory=lambda **_: Fake(),
    )
    assert list(row)[:5] == ["task_id", "level", "benchmark", "group", "arm"]
    assert row["benchmark"] == "dabstep" and row["group"] == "hard"
    assert "failure" not in row  # DABStep never reports one


def test_a_grader_failure_category_is_recorded_on_the_row(tmp_path: Path):
    from dce.benchmark import Grade

    class Failing(DABStep):
        def grade(self, task, answer):
            return Grade("incorrect", failure="no_sql")

    class Fake:
        def run_sync(self, *a, usage=None, **k):
            return _fake_result("text", usage)

    bench = Failing(db=Path("u"), golds={"7": "g"}, golds_hash="h", docs=DOCS)
    row = run_task(
        TASK_OBJ,
        "schema_only",
        "z-ai/glm-5.3-flash",
        bench,
        tmp_path / "x.duckdb",
        agent_factory=lambda **_: Fake(),
    )
    assert row["verdict"] == "incorrect" and row["failure"] == "no_sql"


def test_a_grader_that_raises_is_a_scoring_error(tmp_path: Path):
    class Raising(DABStep):
        def grade(self, task, answer):
            raise RuntimeError("grader down")

    class Fake:
        def run_sync(self, *a, usage=None, **k):
            return _fake_result("0.12", usage)

    bench = Raising(db=Path("u"), golds={"7": "0.12"}, golds_hash="h", docs=DOCS)
    row = run_task(
        TASK_OBJ,
        "schema_only",
        "z-ai/glm-5.3-flash",
        bench,
        tmp_path / "x.duckdb",
        agent_factory=lambda **_: Fake(),
    )
    assert row["verdict"] == "scoring_error"
    assert row["answer"] == "0.12"


def test_a_provenance_failure_is_a_free_construction_error(tmp_path: Path):
    from dce.agent import AgentConstructionError

    built = []

    class Broken(DABStep):
        def arm_digest(self, arm, task):
            raise OSError("contract file unreadable")

        def build_arm(self, arm, task, db):
            built.append(arm)
            return super().build_arm(arm, task, db)

    def factory(**_):
        raise AssertionError("no agent may be built after a provenance failure")

    bench = Broken(db=Path("u"), golds={"7": "g"}, golds_hash="h", docs=DOCS)
    with pytest.raises(AgentConstructionError, match="unreadable"):
        run_task(
            TASK_OBJ,
            "schema_only",
            "z-ai/glm-5.3-flash",
            bench,
            tmp_path / "x.duckdb",
            agent_factory=factory,
        )
    # Before `build_arm`, so no connection was opened that could leak.
    assert built == []
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_agent.py tests/test_trace.py -q 2>&1 | tail -5`
Expected: FAIL. These are `TypeError`s about `run_task`/`build_result_row`
arguments.

- [ ] **Step 3: Implement**

In `dce/agent.py`:

1. Remove `from dce.arms import build_arm`, the temporary
   `from dce.benchmarks.dabstep import arm_digest`, and
   `from dce.grade import _clean, active_scorer, score`. Add:

```python
from dce.benchmark import Benchmark, Task
```

2. Add, above `build_result_row`:

```python
def task_row_fields(task: Task, benchmark: Benchmark) -> dict:
    """The fields that place a row: its task, the benchmark's own task
    fields (`level` on DABStep), then `benchmark` and `group`. In that order,
    so a DABStep row's leading keys read as they always have."""
    return {
        "task_id": task.task_id,
        **benchmark.row_fields(task),
        "benchmark": benchmark.name,
        "group": task.group,
    }
```

3. `build_result_row`:
   - Replace the `task: dict` parameter with `task_fields: dict`.
   - Add the keyword parameters `contract_digest: str`, `scorer: str` and `failure: str | None = None`.
   - Replace the first two entries of the returned dict (`"task_id"` and `"level"`) with `**task_fields,`.
   - Replace `"contract_digest": arm_digest(arm)` with `"contract_digest": contract_digest`.
   - Replace `"scorer": active_scorer()` with `"scorer": scorer`. Keep the comment above it, and change its first sentence to "WHICH scorer produced `verdict`, as the benchmark names it."
   - Build the dict into a local `row`. After it, add:

```python
    if failure is not None:
        # Grading-side failure category (e.g. LiveSQLBench's `no_sql`).
        # Added only when present, so a DABStep row's key set is unchanged.
        row["failure"] = failure
    return row
```

4. `_priced_fallback_row`:
   - Make the same `task` -> `task_fields` replacement.
   - Add the keyword parameters `contract_digest: str` and `scorer: str`.
   - Replace `"task_id"`/`"level"` with `**task_fields,`, `arm_digest(arm)` with `contract_digest`, and `active_scorer()` with `scorer`.

5. `run_task`:
   - New signature, as in **Interfaces** above. Delete the `docs` and `gold` parameters and `golds_hash`.
   - Before the existing `try: setup = build_arm(...)`, add:

```python
    # Every value a row carries about its task and its provenance, computed
    # BEFORE anything opens a connection or calls a model: a benchmark method
    # that raises here is a construction failure -- free, and retried as one
    # -- not a bookkeeping failure on a row that has already spent money.
    try:
        provenance = dict(
            task_fields=task_row_fields(task, benchmark),
            gold=benchmark.gold_ref(task),
            golds_hash=benchmark.golds_hash(),
            contract_digest=benchmark.arm_digest(arm, task),
            scorer=benchmark.scorer(),
        )
    except Exception as exc:
        raise AgentConstructionError(f"{type(exc).__name__}: {exc}") from exc
```

   - `setup = build_arm(arm, db_path, docs)` becomes `setup = benchmark.build_arm(arm, task, db_path)`.
   - The `prompt = (...)` assignment becomes `prompt = task.prompt`.
   - `write_trace(..., task_id=str(task["task_id"]), ...)` becomes `task_id=str(task.task_id)`.
   - Replace the scoring block (`if verdict == "unset": if gold is None: ... else: try: score ...`) with:

```python
            failure = None
            # Graded only if the model call itself completed -- grading a
            # cap trip or a mid-run error would misrepresent a harness
            # artifact as a graded attempt. A grader that raises gets its
            # own verdict, and the answer this run produced is kept.
            if verdict == "unset":
                try:
                    grade = benchmark.grade(task, answer)
                    verdict, failure = grade.verdict, grade.failure
                except Exception:
                    verdict = "scoring_error"

            answer_normalized = benchmark.normalize(answer)
```

   Keep the existing comment block above the old scoring code, trimmed to
   the parts that still apply. The "no gold EXISTS" explanation now lives
   in `DABStep.grade`.
   - The `build_result_row(...)` call: drop `task=task`, `gold=gold` and `golds_hash=golds_hash`, and pass `**provenance` and `failure=failure`.
   - The `_priced_fallback_row(...)` call: drop the same three arguments and pass `**provenance`.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_agent.py tests/test_trace.py tests/test_dabstep.py -q 2>&1 | tail -5`
Expected: all pass except `test_every_row_shape_carries_forced_answer`,
which waits for Task 6's `_construction_error_row`.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/agent.py tests/test_agent.py tests/test_trace.py
git commit -m "dce: run_task grades and stamps rows through the Benchmark"
```

The hooks run only lint and types, not tests, so this commit goes in with
one known-red test. Task 6 makes it green.

---

### Task 6: The runner on the protocol

**Files:**
- Modify: `dce/runner.py`
- Modify: `tests/test_runner.py`, `tests/test_stats.py` (only where they call `sweep`)

**Interfaces:**
- Consumes:
  - `run_task` (Task 5), whose new positional signature is `run_task_fn(task, arm, model, benchmark, db_path, *, trace_dir)`;
  - `DABStep` and `DABStep.from_files` (Task 4);
  - `task_row_fields` (Task 5).
- Produces:
  - `sweep(tasks: list[Task], arms, models, benchmark: Benchmark, *, out, max_spend, run_task_fn=run_task, retry_verdicts=(), trace_dir=None, workers=1, retry_pass=False) -> SweepResult`. The `golds`, `db_path`, `docs` and `golds_hash` parameters are gone.
  - `_construction_error_row(task: Task, arm, model, benchmark, exc) -> dict`.
  - `_stratified_sample(tasks: list[Task], n) -> list[Task]`, stratified by `task.group`.
  - `pending(tasks: list[Task], arms, models, done)`.
  - CLI: `--benchmark {dabstep}`, default `dabstep`. `--arms` defaults to the benchmark's `arms`.

- [ ] **Step 1: Migrate the runner tests (they will fail)**

At the top of `tests/test_runner.py`:

```python
from dce.benchmark import Task
from dce.benchmarks.dabstep import DABStep


def _tasks(records) -> list[Task]:
    return [DABStep.task(r) for r in records]


def _bench(
    db_path: Path, golds: dict | None = None, golds_hash: str = "h", docs=None
) -> DABStep:
    return DABStep(
        db=db_path, golds=golds or {}, golds_hash=golds_hash, docs=docs or {}
    )
```

Rewrite every `sweep(...)` call in `tests/test_runner.py` and
`tests/test_stats.py` by this rule:

```
sweep(TASKS_EXPR, ARMS, MODELS, GOLDS_EXPR, out=O, db_path=P, docs=D, max_spend=M, golds_hash=H, **rest)
  ->  sweep(_tasks(TASKS_EXPR), ARMS, MODELS, _bench(P, GOLDS_EXPR, H, D), out=O, max_spend=M, **rest)
```

Every fake `run_task_fn` changes in two ways:
- a fake taking `(task, arm, model, db_path, docs, gold, **k)` takes `(task, arm, model, benchmark, db_path, **k)`, and reads its gold as `benchmark.gold_ref(task)`;
- `task["task_id"]` becomes `task.task_id`, and `task["level"]` becomes `task.group`.

Fakes taking `(task, arm, model, *a, **k)` keep that signature.

Change the imports of `_load_golds`, `select_tasks` and `UNGOLDED_MODES`
from `dce.runner` to `dce.benchmarks.dabstep`. Rewrite
`_construction_error_row(TASK_DICT, arm, model, gold, golds_hash, exc)` calls
as `_construction_error_row(DABStep.task(TASK_DICT), arm, model, _bench(db, {id: gold}, golds_hash), exc)`.

Rewrite the two `_stratified_sample` tests to pass `_tasks(...)`, and to
read `.group` where they read `["level"]`. Add:

```python
def test_stratified_sample_is_unchanged_by_the_move_to_group():
    """`group` is DABStep's `level`, so a `--n` smoke picks the same tasks
    in the same order it did before the refactor."""
    records = [
        {"task_id": str(i), "question": "q", "level": "easy" if i % 5 == 0 else "hard"}
        for i in range(40)
    ]
    picked = [t.task_id for t in _stratified_sample(_tasks(records), 10)]
    by_level: dict[str, list[str]] = {}
    for r in records:
        by_level.setdefault(r["level"], []).append(r["task_id"])
    expected = []
    for ids in by_level.values():
        expected.extend(ids[: round(10 * len(ids) / len(records))])
    assert picked == expected


class _TwoDBs(DABStep):
    """Tasks in group `a` read `a.db`, the rest `b.db`."""

    def __init__(self, dbs: dict[str, Path]):
        super().__init__(db=dbs["a"], golds={}, golds_hash="h", docs={})
        self._dbs = dbs

    def pristine_db(self, task):
        return self._dbs[task.group]


def test_one_working_copy_per_worker_recopied_on_a_database_change(tmp_path: Path):
    dbs = {"a": _make_pristine(tmp_path, "a.db"), "b": _make_pristine(tmp_path, "b.db")}
    dbs["b"].write_bytes(b"other-pristine")
    seen = []

    def fake_run(task, arm, model, benchmark, db_path, **k):
        seen.append((task.task_id, db_path, db_path.read_bytes()))
        return {
            "task_id": task.task_id,
            "arm": arm,
            "model": model,
            "usd": 0.01,
            "usd_guard": 0.01,
            "verdict": "correct",
        }

    # Interleaved on purpose: the queue must group them by database.
    records = [
        {"task_id": "1", "question": "q", "level": "a"},
        {"task_id": "2", "question": "q", "level": "b"},
        {"task_id": "3", "question": "q", "level": "a"},
    ]
    sweep(
        _tasks(records),
        ("schema_only",),
        (GLM,),
        _TwoDBs(dbs),
        out=tmp_path / "r.jsonl",
        max_spend=5.0,
        run_task_fn=fake_run,
    )

    assert [t for t, _, _ in seen] == ["1", "3", "2"]
    assert seen[0][2] == seen[1][2] == b"pristine-bytes"
    assert seen[2][2] == b"other-pristine"
    # One copy on disk per worker: the `a` copy went when the worker moved on.
    assert not _working_db_path(dbs["a"]).exists()
    assert _working_db_path(dbs["b"]).exists()


def test_switching_databases_removes_the_old_copys_wal(tmp_path: Path):
    dbs = {"a": _make_pristine(tmp_path, "a.db"), "b": _make_pristine(tmp_path, "b.db")}

    def fake_run(task, arm, model, benchmark, db_path, **k):
        if task.task_id == "1":
            # A stale sidecar left beside the `a` copy, as a killed run leaves.
            db_path.with_name(db_path.name + ".wal").write_bytes(b"stale")
        return {
            "task_id": task.task_id,
            "arm": arm,
            "model": model,
            "usd": 0.01,
            "usd_guard": 0.01,
            "verdict": "correct",
        }

    records = [
        {"task_id": "1", "question": "q", "level": "a"},
        {"task_id": "2", "question": "q", "level": "b"},
    ]
    sweep(
        _tasks(records),
        ("schema_only",),
        (GLM,),
        _TwoDBs(dbs),
        out=tmp_path / "r.jsonl",
        max_spend=5.0,
        run_task_fn=fake_run,
    )
    old = _working_db_path(dbs["a"])
    assert not old.exists()
    assert not old.with_name(old.name + ".wal").exists()


def test_a_single_database_keeps_task_order(tmp_path: Path):
    db = _make_pristine(tmp_path)
    seen = []

    def fake_run(task, arm, model, *a, **k):
        seen.append(task.task_id)
        return {
            "task_id": task.task_id,
            "arm": arm,
            "model": model,
            "usd": 0.01,
            "usd_guard": 0.01,
            "verdict": "correct",
        }

    records = [
        {"task_id": str(i), "question": "q", "level": lv}
        for i, lv in enumerate(["hard", "easy", "hard", "easy"])
    ]
    sweep(
        _tasks(records),
        ("schema_only",),
        (GLM,),
        _bench(db),
        out=tmp_path / "r.jsonl",
        max_spend=5.0,
        run_task_fn=fake_run,
    )
    assert seen == ["0", "1", "2", "3"]
```

In `test_switching_databases_removes_the_old_copys_wal`, the stale `.wal`
also makes `check_and_restore` flag task 1 as corrupted, which is correct.
The test asserts only that the old copy and its sidecar are gone afterwards.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_runner.py tests/test_stats.py -q 2>&1 | tail -5`
Expected: FAIL. These are `TypeError`s on `sweep`/fake signatures and
`AttributeError`s on `Task`.

- [ ] **Step 3: Implement in `dce/runner.py`**

1. Imports. Drop `arm_digest` from the `dce.agent` import and add
   `task_row_fields`. Replace
   `from dce.arms import ALL_ARMS, ARMS, check_and_restore, make_working_copy`
   with `from dce.tools import _wal_path, check_and_restore, make_working_copy`.
   Remove the imports of `DATASET_REVISION`, `PLURALITY_THRESHOLD`,
   `golds_sha256`, `active_scorer`, and the temporary dabstep import. Add:

```python
from dce.benchmark import BENCHMARK_NAMES, Benchmark, Task
from dce.benchmarks.dabstep import UNGOLDED_MODES, DABStep
```

2. `_safe_json_dumps`' envelope carries `level`. Directly after that
   `try/except`, add the same pattern for `benchmark` and `group`. A
   salvaged LiveSQLBench row must still land in its own benchmark and
   stratum:

```python
    for key in ("benchmark", "group"):
        try:
            value = row.get(key)
            envelope[key] = None if value is None else str(value)
        except Exception:
            envelope[key] = None
```

   (`benchmark_of` and `group_of` read `None` as absent, so a DABStep
   salvage row is unaffected.)

3. `_construction_error_row(task: Task, arm, model, benchmark: Benchmark, exc)`:
   - Replace `"task_id"`/`"level"` with `**task_row_fields(task, benchmark),`.
   - `"gold": gold` becomes `"gold": benchmark.gold_ref(task)`.
   - `"contract_digest": arm_digest(arm)` becomes `"contract_digest": benchmark.arm_digest(arm, task)`.
   - `"golds_hash": golds_hash` becomes `"golds_hash": benchmark.golds_hash()`.
   - `"scorer": active_scorer()` becomes `"scorer": benchmark.scorer()`.
   - Update the docstring's mention of `level`: "`level` and `group` matter".

   A benchmark method raising here would lose a free row, not a paid one,
   and `run_task` already called the same methods successfully or raised
   `AgentConstructionError` from them. Leave them unguarded, matching the
   old `arm_digest(arm)` call.

4. `pending(tasks, arms, models, done)`: use `task.task_id`.

5. `_run_group(group, working, ledger, *, by_id, benchmark, run_task_fn, trace_dir, pristine)`:
   - Delete the `gold = golds.get(task_id)` lines and their comment; `DABStep.gold_ref` carries that comment now.
   - The call becomes `run_task_fn(by_id[task_id], arm, model, benchmark, working, trace_dir=trace_dir)`.
   - The construction row becomes `_construction_error_row(by_id[task_id], arm, model, benchmark, exc)`.
   - `check_and_restore(working, db_path)` becomes `check_and_restore(working, pristine)`.

6. `_worker`: the working copy follows the group's database:

```python
def _worker(
    index: int, work_q, ledger: _SweepLedger, *, benchmark, by_id, **group_kwargs
) -> None:
    """...existing docstring, plus this paragraph:

    ONE COPY PER WORKER, WHATEVER THE NUMBER OF DATABASES. A benchmark with
    many databases (LiveSQLBench has 22) would otherwise leave a copy of
    each beside its pristine file for every worker. The queue arrives
    ordered by database (`_sweep_locked`), so a worker re-copies only when
    the database changes, and removes the copy it is leaving -- and its
    `.wal` sidecar, which would otherwise replay onto whatever is copied to
    that path next (see `make_working_copy`). A one-database benchmark never
    switches, so DABStep's copy is made once and left on disk, as before.
    """
    working: Path | None = None
    working_for: Path | None = None
    try:
        while not ledger.stop.is_set():
            try:
                task_id, group = work_q.get_nowait()
            except queue.Empty:
                return
            amount = ledger.try_reserve(group, task_id)
            if amount is None:
                return
            try:
                pristine = benchmark.pristine_db(by_id[task_id])
                if working_for != pristine:
                    if working is not None:
                        _wal_path(working).unlink(missing_ok=True)
                        working.unlink(missing_ok=True)
                    working = make_working_copy(
                        pristine, _working_db_path(pristine, index)
                    )
                    working_for = pristine
                assert working is not None
                _run_group(
                    group,
                    working,
                    ledger,
                    by_id=by_id,
                    benchmark=benchmark,
                    pristine=pristine,
                    **group_kwargs,
                )
            finally:
                ledger.release(amount)
    except BaseException as exc:  # noqa: BLE001 - re-raised by `sweep`
        ledger.fail(exc)
```

7. `sweep` and `_sweep_locked`:
   - Replace the `golds`, `db_path`, `docs` and `golds_hash` parameters with one positional `benchmark`.
   - Update the docstrings: the pristine database is `benchmark.pristine_db(task)`, never opened directly.
   - In `_sweep_locked`, before `pending`:

```python
    # Stable, so a one-database benchmark keeps its task order exactly; a
    # many-database one has each database's tasks contiguous, which is what
    # lets a worker keep a single working copy (see `_worker`).
    tasks = sorted(tasks, key=lambda task: str(benchmark.pristine_db(task)))
    by_id = {t.task_id: t for t in tasks}
```

   - `group_kwargs` becomes `dict(run_task_fn=run_task_fn, trace_dir=trace_dir)`.
   - The thread `kwargs` become `{"benchmark": benchmark, "by_id": by_id, **group_kwargs}`.
   - Drop the `db_path=db_path` keyword everywhere it was threaded through.

8. `_stratified_sample(tasks: list[Task], n)`: group by `task.group`
   instead of `task.get("level", "unknown")`. Update the docstring: "split
   across groups (DABStep's `level`)".

9. `main()`:
   - Add `parser.add_argument("--benchmark", choices=BENCHMARK_NAMES, default="dabstep")`.
   - `--arms` gets `default=None`, and the help text "default: the benchmark's arms".
   - Keep `--db`, `--golds`, `--tasks` and `--ungolded`; their help says "(dabstep)".
   - After `parse_args`, and before the arm check:

```python
    if args.benchmark == "dabstep":
        benchmark = DABStep.from_files(
            db=args.db,
            golds_path=args.golds,
            tasks_path=args.tasks,
            ungolded=args.ungolded,
        )
    arms = tuple(args.arms) if args.arms else benchmark.arms
    known = benchmark.arms + benchmark.extra_arms
    for arm in arms:
        if arm not in known:
            raise SystemExit(f"unknown arm: {arm!r}; expected one of {known}")
```

   Keep the old order of checks: the model check and `assert_clean_tree`
   first, then the benchmark, where `_load_golds` used to run. The arm
   check therefore moves below `assert_clean_tree`, which is harmless: both
   raise `SystemExit` before any model call.
   - `tasks = benchmark.tasks()`, then `_stratified_sample` as before.
   - Delete the `docs = {...}` block and the old `_load_golds`/`select_tasks` lines.
   - The `sweep(...)` call passes `tasks, arms, tuple(args.models), benchmark`.
   - Use `arms` (not `args.arms`) in `_worst_case_task_group_usd` and the reserve message.

- [ ] **Step 4: Run the whole suite**

Run: `uv run pytest -q 2>&1 | tee "$EQ/after-task6-pytest.txt" | tail -3`
Expected: all pass. That includes `test_every_row_shape_carries_forced_answer`
from Task 5.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/runner.py tests/test_runner.py tests/test_stats.py
git commit -m "dce: the runner on the Benchmark protocol, one working copy per worker"
```

---

### Task 7: `dce.stats` by role and group

**Files:**
- Modify: `dce/stats.py`
- Test: `tests/test_stats.py`

**Interfaces:**
- Consumes: `benchmark_class`, `benchmark_of` and `group_of` (Task 2); the `DABStep` class attributes and static hooks (Task 4).
- Produces:
  - `report(path, *, rescore_stale=True) -> str`. Its signature is unchanged, and it resolves the benchmark from the rows.
  - `rescore`, `_as_e2e` and `stale_scorer_rows` dispatch per row through the row's benchmark class.
  - The module constants `ARM_A`, `ARM_B`, `ARM_C`, `ARM_D`, `COMPARISON_ARMS`, `PRIMARY_MODEL`, `PRIMARY_LEFT_ARM` and `PRIMARY_RIGHT_ARM` keep their values, now derived from `DABStep`, for existing importers.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_stats.py`:

```python
def test_a_file_mixing_two_benchmarks_is_refused(tmp_path):
    path = tmp_path / "r.jsonl"
    rows = [
        _row("t1", PRIMARY_LEFT_ARM, "correct"),
        {**_row("t2", PRIMARY_LEFT_ARM, "correct"), "benchmark": "other"},
    ]
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    with pytest.raises(SystemExit, match="more than one benchmark"):
        report(path)


def test_strata_lines_read_group_and_fall_back_to_level(tmp_path):
    path = tmp_path / "r.jsonl"
    rows = [
        {**_row("t1", PRIMARY_LEFT_ARM, "correct", level="hard"), "group": "hard"},
        _row("t2", PRIMARY_LEFT_ARM, "incorrect", level="easy"),  # no group field
    ]
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    text = report(path)
    assert "    level=hard " in text
    assert "    level=easy " in text


def test_the_dabstep_constants_still_name_the_same_arms():
    from dce.stats import ARM_B, ARM_C, ARM_D, COMPARISON_ARMS

    assert (ARM_A, ARM_B, ARM_C, ARM_D) == (
        "schema_only",
        "manual_prompt",
        "contract",
        "contract_hollow",
    )
    assert COMPARISON_ARMS == ("schema_only", "manual_prompt", "contract_hollow")
```

If `_row` in `tests/test_stats.py` does not take `level=`, use the keyword
it does take for the level (it is called with `level=` at line 216, so it
should).

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_stats.py -q -k "mixing or strata_lines or still_name" 2>&1 | tail -5`
Expected: the mixed-file test FAILS (no SystemExit). The other two may
already pass; that is fine, because they pin behaviour the next step must
keep.

- [ ] **Step 3: Implement in `dce/stats.py`**

1. Imports: replace `from dce.arms import ARMS, EXTRA_ARMS, GOVERNED_ARMS`
   with:

```python
from dce.benchmark import benchmark_class, benchmark_of, group_of
from dce.benchmarks.dabstep import DABStep
```

2. The module constants keep their names and values, now read off `DABStep`:

```python
ARM_C = DABStep.roles["contract"]
ARM_A = "schema_only"
ARM_B = DABStep.roles["manual"]
ARM_D = "contract_hollow"
for _name in (ARM_A, ARM_B, ARM_C, ARM_D):
    if _name not in DABStep.arms:
        raise ValueError(...)  # existing message, with DABStep.arms for ARMS

COMPARISON_ARMS: tuple[str, ...] = tuple(a for a in DABStep.arms if a != ARM_C)

PRIMARY_MODEL, PRIMARY_LEFT_ARM, PRIMARY_RIGHT_ARM = DABStep.primary
```

Update their comments. These constants are DABStep's, kept for importers;
`report()` reads the benchmark of the rows it is given.

3. A resolver:

```python
def _benchmark_for(rows: list[dict]):
    """The one benchmark class `rows` belong to. A file mixing benchmarks has
    no single set of arms or roles to compare, so it is refused rather than
    reported as if it had."""
    names = {benchmark_of(row) for row in rows} or {DABStep.name}
    if len(names) > 1:
        raise SystemExit(
            f"rows from more than one benchmark ({sorted(names)}); "
            "report each benchmark's results file separately"
        )
    return benchmark_class(names.pop())
```

4. Per-row dispatch:
   - `rescore`: replace the `score(...)` expression with `verdict = benchmark_class(benchmark_of(row)).rescore(row)`. When that returns `None`, the benchmark cannot re-grade from a stored row; append the row unchanged and `continue`. Otherwise stamp `"scorer": benchmark_class(benchmark_of(row)).scorer()`. Keep the `try/except Exception: verdict = "scoring_error"` around it.
   - `stale_scorer_rows`: `now` is computed per row, as `benchmark_class(benchmark_of(row)).scorer()`.
   - `_as_e2e`: the condition becomes `row.get("verdict") == "incorrect" and benchmark_class(benchmark_of(row)).e2e_correct(row)`.
   - `_governance_counts`: replace `GOVERNED_ARMS` with the union of each row's benchmark's `governed_arms`:

```python
    governed = frozenset().union(
        *(benchmark_class(benchmark_of(row)).governed_arms for row in rows)
    )
    if not rows or not arms <= governed:
        return None
```

5. `report()`:
   - After `rows = load(path)`, add `bench = _benchmark_for(rows)`.
   - The verified-wrong drop uses `bench.excluded_tasks` in place of `VERIFIED_WRONG_GOLDS`. Remove that import, and keep the NOTE text exactly as it is.
   - The `# PRIMARY` section uses `bench.primary`. When it is `None`, emit the `# PRIMARY (pre-registered)` header followed by `(none pre-registered for <name>)`. For DABStep, use `PRIMARY_MODEL, PRIMARY_LEFT_ARM, PRIMARY_RIGHT_ARM = bench.primary` locally so the existing lines stay byte-identical.
   - In the per-arm strata block, add a `group` key to the rows (`[{**r, "group": group_of(r)} for r in rows]`) and call `accuracy_by(..., "group")` in place of `"level"`. The line prefix becomes `f"    {bench.group_label}={level:6s} "`.
   - The comparisons read the benchmark's roles:

```python
        contract = bench.roles["contract"]
        comparison = tuple(a for a in bench.arms if a != contract)
        extra = tuple(a for a in bench.extra_arms if a in present)
        for left in comparison + extra:
            if bench.primary and model == bench.primary[0] and left == bench.primary[1]:
                lines.append(f"  McNemar {left} vs {contract}: see PRIMARY section above")
                continue
            lines.extend(_mcnemar_lines(subset, left, contract))
        manual, plus = bench.roles.get("manual"), bench.roles.get("manual_plus")
        if manual and plus and {manual, plus} <= present:
            lines.extend(_mcnemar_lines(subset, manual, plus))
```

6. Update the module docstring's line 99 ("Arm names are unpacked from
   `dce.arms.ARMS`...") to say that arms and roles come from the rows'
   benchmark class.

- [ ] **Step 4: Run the tests and the equivalence diff**

Run: `uv run pytest tests/test_stats.py tests/test_runner.py -q 2>&1 | tail -3`
Expected: all pass.

```bash
mkdir -p "$EQ/after"
for f in results/*.jsonl; do
  uv run python -m dce.stats "$f" > "$EQ/after/$(basename "$f").stats" 2>&1
done
diff -r "$EQ/before" "$EQ/after" --exclude=arm_surface.json --exclude=knowledge_delivery.txt --exclude=pytest.txt && echo STATS-SAME
```

Expected: `STATS-SAME`. Any difference is a regression; fix the code, never
the baseline.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/stats.py tests/test_stats.py
git commit -m "dce.stats: arms by role and strata by group, from the rows' benchmark"
```

---

### Task 8: `knowledge_delivery.py` by role

**Files:**
- Modify: `analysis/knowledge_delivery.py`

**Interfaces:**
- Consumes: `benchmark_class` (Task 2); `DABStep.roles` (Task 4).
- Produces: `uv run python analysis/knowledge_delivery.py [--benchmark dabstep]`, with output identical to before for DABStep. A `CONFIG[name]` entry holds:
  - `runs`: a model -> `(repeat -> results stems)` mapping;
  - `group(row) -> str`;
  - `groups`, a tuple;
  - `extra(model, per_repeat, arms)`, which prints the benchmark-specific trace section, or is `None`.

- [ ] **Step 1: Restructure**

1. Keep every DABStep-specific piece (`MODELS`, `RULE_SET`, `TOTAL_FEE`, `GROUPS`, `LIST_COLS`, `EMPTY_LIST`, `FAMILY`, `group`, `queries`) where it is, with three changes:
   - `group` takes a row: `def dabstep_group(row: dict) -> str`, keyed on `str(row["task_id"])`.
   - The empty-list block at the end of `report` moves into `def dabstep_empty_list(model, per_repeat, arms) -> None`, unchanged except that it reads `arms` instead of the module `ARMS`.
   - Rename `MODELS` to `DABSTEP_RUNS`.

2. Add the configuration and roles:

```python
from dce.benchmark import benchmark_class  # noqa: E402

CONFIG = {
    "dabstep": {
        "runs": DABSTEP_RUNS,
        "group": dabstep_group,
        "groups": GROUPS,
        "extra": dabstep_empty_list,
    },
}


def arms_of(name: str) -> tuple[str, str, str]:
    """(manual, manual_plus, contract), the three roles the decomposition
    reads, as this benchmark names its arms."""
    roles = benchmark_class(name).roles
    return roles["manual"], roles["manual_plus"], roles["contract"]
```

3. `rows(name, model, repeat, arms)` reads `CONFIG[name]["runs"][model](repeat)`
   and filters `row["arm"] in arms`.

4. `report(name, model)`. Set `manual, plus, contract = arms = arms_of(name)`.
   Replace every literal `"manual_prompt"`, `"manual_resolved"` and
   `"contract"` with `manual`, `plus` and `contract`, and every `ARMS` with
   `arms`. `pairs = ((plus, manual), (contract, plus))`. The group loop uses
   `CONFIG[name]["groups"]` and `CONFIG[name]["group"](row)`. Keep the
   printed text and its order exactly as they are. At the end, call
   `CONFIG[name]["extra"](model, per_repeat, arms)` when it is not `None`,
   then `print()`.

   Per-task grouping needs a row, but `by_group` currently keys on
   `group(r["task_id"])` while iterating rows. Pass `r`. For `among`, build
   a `task -> group` map while iterating:
   `group_of_task[r["task_id"]] = CONFIG[name]["group"](r)`.

5. `main()`:

```python
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(prog="knowledge_delivery")
    parser.add_argument("--benchmark", choices=sorted(CONFIG), default="dabstep")
    name = parser.parse_args().benchmark
    for model in CONFIG[name]["runs"]:
        report(name, model)
```

6. Docstring. Add one paragraph saying that the decomposition reads arms by
   role, that each benchmark contributes a `CONFIG` entry, and that the
   empty-list section is DABStep's own. Keep the `Run:` line. FINDINGS.md
   cites it, and the default benchmark keeps it valid.

- [ ] **Step 2: Verify the output is identical**

```bash
uv run python analysis/knowledge_delivery.py > "$EQ/after/knowledge_delivery.txt" 2>&1
diff "$EQ/before/knowledge_delivery.txt" "$EQ/after/knowledge_delivery.txt" && echo KD-SAME
```

Expected: `KD-SAME`.

- [ ] **Step 3: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add analysis/knowledge_delivery.py
git commit -m "analysis: knowledge_delivery reads arms by role, one CONFIG entry per benchmark"
```

---

### Task 9: Equivalence gate and docs

**Files:**
- Modify: `analysis/arm_surface.py` (build through the Benchmark)
- Modify: `README.md` (the `dce.runner` usage section: `--benchmark`, and where arm code now lives)

- [ ] **Step 1: Build the arm surface through the protocol**

Replace `_builder` in `analysis/arm_surface.py`:

```python
def _builder():
    """`(arms, build)`, where `build(arm, db)` returns an `ArmSetup`. Built
    through `DABStep.build_arm`, the path the runner takes."""
    from dce.benchmarks.dabstep import DABStep, load_docs

    bench = DABStep(db=PRISTINE, golds={}, golds_hash="", docs=load_docs(CONTEXT))
    task = DABStep.task({"task_id": "0", "question": "", "level": "hard"})
    return (
        DABStep.arms + DABStep.extra_arms,
        lambda arm, db: bench.build_arm(arm, task, db),
    )
```

- [ ] **Step 2: Run the full gate**

```bash
uv run python analysis/arm_surface.py > "$EQ/after/arm_surface.json"
diff "$EQ/before/arm_surface.json" "$EQ/after/arm_surface.json" && echo SURFACE-SAME
for f in results/*.jsonl; do
  uv run python -m dce.stats "$f" > "$EQ/after/$(basename "$f").stats" 2>&1
done
uv run python analysis/knowledge_delivery.py > "$EQ/after/knowledge_delivery.txt" 2>&1
diff -r "$EQ/before" "$EQ/after" --exclude=pytest.txt && echo ALL-SAME
uv run pytest -q 2>&1 | tail -3
(cd ../.. && prek run --all-files)
git status --short uv.lock
```

Expected:
- `SURFACE-SAME` and `ALL-SAME`;
- every test passing, at least the baseline count plus the new tests;
- prek clean;
- `uv.lock` unmodified.

The ungoverned arms list their tools in the order `dce.tools` builds them,
which is the same order as before, so the dump must match byte for byte.

- [ ] **Step 3: README**

In the README section that documents `uv run python -m dce.runner`, add
`--benchmark` (default `dabstep`, currently the only one). Add one sentence
on where the code lives: `dce/benchmark.py` defines the protocol,
`dce/tools.py` holds the shared arm machinery, and
`dce/benchmarks/dabstep.py` holds DABStep. Every existing command stays
valid; do not rewrite them.

- [ ] **Step 4: Commit**

```bash
git add analysis/arm_surface.py README.md
git commit -m "analysis, README: arm surface through the Benchmark; document --benchmark"
```

---

### Task 10: Smoke run (merge gate, spends money: ask first)

A real 12-task run on `gpt-6-luna`, three arms. It costs well under $5,
but it uses the gateway key. **Ask the user before launching, and launch
only after they say yes.**

- [ ] **Step 1: Run**

```bash
set -a; . ./.env; set +a
uv run python -m dce.runner --benchmark dabstep --n 12 \
  --arms manual_prompt manual_resolved contract --models gpt-6-luna \
  --max-spend 5 --workers 4 --retry-pass \
  --out "$EQ/smoke.jsonl" --traces "$EQ/smoke-traces" 2>&1 \
  | sed -E 's#https?://[^ /"]+#<host>#g' | tail -5
```

Expected: exit 0, and 36 rows (12 tasks x 3 arms).

- [ ] **Step 2: Compare the row schema with the pre-refactor luna run**

```bash
uv run python - "$EQ/smoke.jsonl" <<'EOF'
import json, sys
from pathlib import Path
new = [json.loads(l) for l in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()]
old = [json.loads(l) for l in Path("results/luna-resolved-r1.jsonl").read_text(encoding="utf-8").splitlines()]
old_keys = set().union(*(r.keys() for r in old if r["verdict"] in ("correct", "incorrect")))
new_keys = set().union(*(r.keys() for r in new if r["verdict"] in ("correct", "incorrect")))
print("added:", sorted(new_keys - old_keys))
print("removed:", sorted(old_keys - new_keys))
print("benchmark/group:", {(r["benchmark"], r["group"] == r["level"]) for r in new})
print("verdicts:", sorted({r["verdict"] for r in new}))
EOF
```

Expected:
- `added: ['benchmark', 'group']`;
- `removed: []`;
- `benchmark/group: {('dabstep', True)}`;
- verdicts mostly `correct`/`incorrect`.

Then confirm that `uv run python -m dce.stats "$EQ/smoke.jsonl"` prints all
three arms with `level=` lines.

- [ ] **Step 3: Report**

Tell the user:
- the smoke's spend and verdict counts;
- the schema diff;
- that every equivalence check passed.

The smoke file stays in the scratchpad and is not committed. Ask whether to
push the branch to `fork` and open PR A.

---

## Self-review notes

- Spec coverage, Part 1:
  - protocol and registry: Task 2;
  - `dce/tools.py` and `init_sql`: Task 3;
  - DABStep module: Task 4;
  - `run_task`: Task 5;
  - runner, `--benchmark`, working copies and queue order: Task 6;
  - rows `benchmark`/`group`: Tasks 5 and 6;
  - analysis by role: Tasks 7 and 8;
  - equivalence checks 1-4: Tasks 1, 7, 8, 9 and 10;
  - `dce/arms.py` re-export: Tasks 3 and 4;
  - PR A unit tests: role mapping (Tasks 7 and 8), the `group`/`level` fallback (Tasks 2 and 7), working copies and queue order (Task 6), `init_sql` (Task 3).
- The spec's interface was revised before this plan, to `Grade`, `gold_ref`, `normalize`, the static row hooks, and the queue ordered by pristine database. Both documents agree.
- Out of scope here, all in PR B: LiveSQLBench itself, the compiler, the `failure` categories, a LiveSQLBench `CONFIG` entry in `knowledge_delivery.py`, and `--benchmark livesqlbench`.
