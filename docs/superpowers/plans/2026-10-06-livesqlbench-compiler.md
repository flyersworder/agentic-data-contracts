# LiveSQLBench compiler (PR C) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the benchmark-agnostic KB compiler, both KB adapters, LiveSQLBench's `manual_compiled` and `contract` arms, DABStep's `contract_compiled` extra arm, and LiveSQLBench's `knowledge_delivery.py` entry. End with a paid compile of one LiveSQLBench database.

**Architecture:**
- **`dce/compile/`** turns a neutral list of KB entries (`KBEntry`) into a frozen contract, one entry at a time, children first.
- **Per entry**, the model gets two tools:
  - `run_sql`: read-only, no file access, at most 50 rows;
  - `submit_entry`: validated before it is accepted, 3 attempts.
- **Every finished entry** is appended to a compile log at once, so a rerun resumes.
- **Emission** builds `contract.yml` and `semantic.yml` from the log's records alone.
- **Benchmark adapters** produce `CompileInputs`. LiveSQLBench reads `*_kb.jsonl`; DABStep splits `manual.md` at its numbered headings.
- **The arms load the frozen output.** Governed arms share one builder (`dce.tools.governed_arm`), so the procedural sentence is written once.

**Tech Stack:** Python 3.12, uv, pytest, DuckDB, pydantic-ai (the model loop, via `dce.agent._default_agent_factory`), PyYAML, the editable `agentic-data-contracts` library (`DataContract`, `contract_digest`). Lint and types through prek (ruff, ty).

**Spec:** `docs/superpowers/specs/2026-10-06-livesqlbench-harness-design.md`. This plan implements:
- "Order of work" item 3 (PR C);
- Part 2's Compiler, Layout and Arms;
- Part 3's tooling (the audit itself is the compile commit's);
- Part 5's adapter and arm;
- the Testing section's PR C items.

Read those sections first.

All paths are relative to `experiments/dabstep-contract-eval/` unless they
start with `docs/` or `~`. Run commands from that directory.

## Global Constraints

- **uv:** run Python through `uv run`, and prefix every `uv run` with `UV_FROZEN=1`, so `uv.lock` is never rewritten. The lock update belongs only in the run commit.
- **Lint:** lint and type-check through `prek run --all-files`, run from the repo root. Never commit with `--no-verify`.
- **No AI attribution anywhere:** no `Co-Authored-By: Claude`, no "Generated with", no robot emoji. This covers commit messages, the PR description and the squash message.
- **No internal names** (employer, cluster, namespace, gateway host) in any committed file, commit message or PR.
- **Never committed or published:** LiveSQLBench gold SQL, test cases, knowledge mappings and frozen gold results. They stay under `LSB_DATA` (default `~/data/livesqlbench`).
- **The compiler never opens a task, gold or test-case file.** A test enforces this (Task 8).
- **No credentials in committed files.** The compiler model is `claudesonnet5` (`dce.pricing.MODELS`), reached through the gateway variables in `.env`. Load them with `set -a; . ./.env; set +a`, never print them, and redact hosts with `sed -E 's#https?://[^ /"]+#<host>#g'`.
- **New code:**
  - `encoding="utf-8"` on every file open;
  - no f-strings without a placeholder;
  - no emojis or Unicode symbols in scripts and tests;
  - comments explain *why*, at the density of the module being edited.
- **DABStep is unchanged:**
  - `contract_compiled` is an extra arm, so `ARMS`, the defaults and every existing output stay as they are;
  - Task 1's baselines are compared again in Task 14.
- **Do not edit** `results/`, `contract/`, `contract_hollow/`, `contract_uninterpreted/`, `data/` or `traces/`. Nothing under `contracts/` is committed in this PR: the compile commit adds the real contracts.
- **Branch:** `livesqlbench-c`. Commit after each task. Push only when the user asks, and to the `fork` remote.

## Review Focus

1. **A KB entry whose SQL never validates.** It is kept, not dropped: a metric with an empty `sql_expression` and `untranslated` set to the last error. Its KB text still reaches the contract arm, and the contract still loads. Tests in Tasks 6 and 7.
2. **A compile that crashes or hits its spend cap mid-database.** The rerun skips finished entries, never re-bills one and ignores a torn last log line. A rerun whose inputs changed is refused. Tests in Task 8.
3. **A sweep asking for `manual_compiled` or `contract` on a database with no compiled contract** (or with one that does not load). The sweep stops before any spend and names the databases. It must not become construction-error rows. Tests in Tasks 9 and 10.
4. **The compiler reaching private files.** Its SQL cannot read files (`read_text` of the gold file fails), and no Python `open` touches the task, gold or test-case files, for either benchmark. Tests in Tasks 4 and 8.
5. **Two KB entries with the same name in one database** (three real databases have one). They get distinct, deterministic metric names. Test in Task 3.

---

### Task 1: Baselines and the spec's open points

**Files:**
- Modify: `docs/superpowers/specs/2026-10-06-livesqlbench-harness-design.md`

Nothing in the experiment changes here. Capture DABStep's outputs, and settle five points the spec leaves open, in the spec itself.

- [ ] **Step 1: Capture the DABStep baselines**

Set `EQ` to an `equiv-c/` folder inside the session scratchpad directory (never `/tmp`), then:

```bash
mkdir -p "$EQ/before"
UV_FROZEN=1 uv run python analysis/arm_surface.py > "$EQ/before/arm_surface.json" 2>/dev/null
for f in results/*.jsonl; do
  UV_FROZEN=1 uv run python -m dce.stats "$f" > "$EQ/before/$(basename "$f").stats" 2>&1
done
UV_FROZEN=1 uv run python analysis/knowledge_delivery.py > "$EQ/before/knowledge_delivery.txt" 2>/dev/null
UV_FROZEN=1 uv run pytest -q --deselect tests/test_replay.py::test_sql_statements_are_ordered_and_only_from_sql_tools 2>&1 | tail -1
ls "$EQ/before" | wc -l
```

Expected:
- all tests pass, 557 at the merge of PR B;
- 41 files: the arm surface, 39 `.stats` files and the knowledge-delivery output.

Deselect `test_replay.py::test_sql_statements_are_ordered_and_only_from_sql_tools` in every run. It needs a local `traces/glm-full` and is environmental.

Define once, for Task 14:

```bash
norm() { grep -vE '^(Uninstalled|Installed) [0-9]+ package' "$1" | sed -E 's/(File ".*", line )[0-9]+/\1N/'; }
```

- [ ] **Step 2: Settle the open points in the spec**

In Part 5, "**Arm.**", replace "the `contract` arm's tools, procedural sentence and `DATA_NOTE`, with the compiled contract in place of the hand-authored one" with:

```markdown
the `contract` arm's tools and procedural sentence, with the compiled
contract in place of the hand-authored one. `DATA_NOTE` is not added: the
`contract` arm's prompt does not carry it either (it is `manual_resolved`'s),
and the arm must differ from `contract` only in the contract (Testing). The
empty-list fact `DATA_NOTE` states is in the data, which the compiler can
query, as the hand author did.
```

In Part 2's Compiler section, after the "Mapping:" list, add:

```markdown
- Domains: on LiveSQLBench, `calculations` holds the calculation metrics,
  `definitions` the predicate metrics, and `reference` any illustration
  that names no single table or column. On DABStep, each manual section is
  a domain whose description is the section's text verbatim, and the
  section's metrics belong to it. A section metric's description is the
  compiler's own, followed by the section it came from: the section text
  is too long to repeat per metric, and its domain carries it.
- Notes: whatever the compiler learned from the data (a JSON path, how a
  value is stored) is written as a final paragraph starting
  "Compiler note: " on the metric, column, table or domain it concerns.
  `manual_compiled`'s appendix carries every such note, so both arms see
  the compiler's knowledge.
- Names: a metric is named after its entry (lower case, non-alphanumerics
  as `_`). Entries whose names collide all get `_<id>` appended.
- Working log: a database in progress is logged to
  `compile-log.jsonl`; when it finishes, the log is gzipped to
  `compile-log.jsonl.gz` and the plain file removed. A finished database is
  skipped on rerun.
- Before any spend, a sweep that asks for `manual_compiled` or `contract`
  checks that every database in its task set has a compiled contract that
  loads (`Benchmark.check_arms`).
```

- [ ] **Step 3: Commit**

```bash
git add ../../docs/superpowers/specs/2026-10-06-livesqlbench-harness-design.md
git commit -m "docs: LiveSQLBench spec, the compiler's domains, notes, names and working log; contract_compiled carries no DATA_NOTE"
```

---

### Task 2: One governed-arm builder, shared

**Files:**
- Modify: `dce/tools.py`
- Modify: `dce/benchmarks/dabstep.py` (`build_arm`'s governed branch)
- Modify: `analysis/arm_surface.py` (optional arm names on the command line)
- Test: `tests/test_tools.py`

**Interfaces:**
- Produces:
  - `dce.tools.CONTRACT_PROCEDURE: str`;
  - `dce.tools.governed_arm(base_prompt: str, db_path: Path, contract, *, init_sql: tuple[str, ...] = ()) -> ArmSetup`. Its prompt is `f"{base_prompt}\n\n{CONTRACT_PROCEDURE}\n\n{contract.to_system_prompt()}"`.
  - `analysis/arm_surface.py [arm ...]` dumps only the arms named, or all of them.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_tools.py`:

```python
def test_governed_arm_is_base_procedure_then_contract(tmp_path):
    from dce.frozen import load_contract
    from dce.tools import CONTRACT_PROCEDURE, governed_arm

    db = tmp_path / "d.duckdb"
    duckdb.connect(str(db)).close()
    contract = load_contract()
    setup = governed_arm("BASE", db, contract)
    try:
        assert setup.system_prompt == (
            f"BASE\n\n{CONTRACT_PROCEDURE}\n\n{contract.to_system_prompt()}"
        )
        assert "inspect_query" in CONTRACT_PROCEDURE
        assert setup.session is not None and setup.adapter is not None
        assert "lookup_metric" in [t.name for t in setup.tools]
    finally:
        setup.close()
```

If `duckdb` is not imported at the top of `tests/test_tools.py`, add `import duckdb`.

- [ ] **Step 2: Run it to verify it fails**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_tools.py -k governed_arm_is 2>&1 | tail -3`
Expected: FAIL, `ImportError: cannot import name 'CONTRACT_PROCEDURE'`.

- [ ] **Step 3: Implement**

In `dce/tools.py`, after `_governed_tools`:

```python
#: The one sentence every governed arm adds before its contract. Written
#: once: it is the thing DABStep's `contract_hollow` exists to control for,
#: so a second copy that drifted would silently reintroduce the confound,
#: and LiveSQLBench's `contract` arm must read exactly as DABStep's does.
CONTRACT_PROCEDURE = (
    "A data contract governs this database. Look up the domain and the "
    "metrics that apply before writing SQL, and validate every query "
    "with inspect_query before running it."
)


def governed_arm(
    base_prompt: str, db_path: Path, contract, *, init_sql: tuple[str, ...] = ()
) -> ArmSetup:
    """A governed arm: the benchmark's base prompt, the procedure, then the
    contract's own rendering, over the governed tools."""
    tools, session, adapter = _governed_tools(
        db_path, contract=contract, init_sql=init_sql
    )
    prompt = f"{base_prompt}\n\n{CONTRACT_PROCEDURE}\n\n{contract.to_system_prompt()}"
    return ArmSetup(prompt, tools, session, adapter)
```

In `dce/benchmarks/dabstep.py`, import `governed_arm` from `dce.tools`, and replace the body of the `if arm in GOVERNED_ARMS:` branch after `contract = {...}[arm]()` with `return governed_arm(BASE_PROMPT, db_path, contract)`. Keep the "IDENTICAL EXCEPT FOR THE CONTRACT OBJECT" comment, and add one line to it: "The procedural sentence is `dce.tools.CONTRACT_PROCEDURE`."

In `analysis/arm_surface.py`, `main()`: after `arms, build = _builder()`, add:

```python
    # Named arms only, when given: an arm whose artifact does not exist yet
    # (`contract_compiled` before the compile commit) cannot be built.
    if len(sys.argv) > 1:
        arms = tuple(a for a in arms if a in sys.argv[1:])
```

Then add one sentence to the module docstring: "Pass arm names to dump only those."

- [ ] **Step 4: Run the tests and the DABStep surface check**

```bash
UV_FROZEN=1 uv run pytest -q --deselect tests/test_replay.py::test_sql_statements_are_ordered_and_only_from_sql_tools 2>&1 | tail -1
UV_FROZEN=1 uv run python analysis/arm_surface.py > "$EQ/arm_surface.task2.json" 2>/dev/null
diff "$EQ/before/arm_surface.json" "$EQ/arm_surface.task2.json" && echo SURFACE-SAME
```

Expected: all tests pass, and `SURFACE-SAME`.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/tools.py dce/benchmarks/dabstep.py analysis/arm_surface.py tests/test_tools.py
git commit -m "dce.tools: one governed-arm builder, the procedural sentence written once"
```

---

### Task 3: Neutral KB entries, compile order, metric names

**Files:**
- Create: `dce/compile/__init__.py`, `dce/compile/entries.py`
- Test: `tests/test_compile_entries.py`

**Interfaces:**
- Produces, in `dce.compile.entries`:
  - `KINDS = ("calculation", "predicate", "illustration", "section")`;
  - `@dataclass(frozen=True) class KBEntry(id: int, kind: str, name: str, text: str, children: tuple[int, ...] = ())`;
  - `slug(text: str) -> str`;
  - `metric_names(entries) -> dict[int, str]`, for calculation and predicate entries;
  - `compile_order(entries) -> list[KBEntry]`;
  - `DOMAIN_BY_KIND: dict[str, str]`;
  - `domain_of(entry: KBEntry) -> str`.

- [ ] **Step 1: Write the failing tests**

`tests/test_compile_entries.py`:

```python
import pytest
from dce.compile.entries import (
    KBEntry,
    compile_order,
    domain_of,
    metric_names,
    slug,
)


def _e(id, kind="calculation", name=None, children=()):
    return KBEntry(id, kind, name or f"Entry {id}", f"text {id}", tuple(children))


def test_children_compile_before_parents_otherwise_id_order():
    entries = [_e(3, children=[2]), _e(1), _e(2), _e(5, children=[1, 4]), _e(4)]
    assert [e.id for e in compile_order(entries)] == [1, 2, 3, 4, 5]


def test_a_cycle_or_a_missing_child_is_refused_by_id():
    with pytest.raises(ValueError, match="cycle"):
        compile_order([_e(1, children=[2]), _e(2, children=[1])])
    with pytest.raises(ValueError, match="9"):
        compile_order([_e(1, children=[9])])
    with pytest.raises(ValueError, match="duplicate"):
        compile_order([_e(1), _e(1)])


def test_an_unknown_kind_is_refused():
    with pytest.raises(ValueError, match="kind"):
        KBEntry(1, "formula", "x", "t")


def test_slug():
    assert slug("Shipped Revenue (EUR)") == "shipped_revenue_eur"
    assert slug("  --  ") == "entry"


def test_colliding_names_all_get_their_id_so_the_choice_is_order_free():
    entries = [
        _e(7, name="Risk Score"),
        _e(2, "predicate", name="risk score"),
        _e(3, name="Other"),
        _e(4, "illustration", name="Risk Score"),
    ]
    assert metric_names(entries) == {7: "risk_score_7", 2: "risk_score_2", 3: "other"}
    assert metric_names(list(reversed(entries))) == metric_names(entries)


def test_domains():
    assert domain_of(_e(1, "calculation")) == "calculations"
    assert domain_of(_e(1, "predicate")) == "definitions"
    assert domain_of(_e(1, "illustration")) == "reference"
    assert (
        domain_of(_e(1, "section", name="5.1 Best Practices")) == "5_1_best_practices"
    )
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_compile_entries.py 2>&1 | tail -3`
Expected: FAIL, `ModuleNotFoundError: No module named 'dce.compile'`.

- [ ] **Step 3: Implement**

`dce/compile/__init__.py`:

```python
"""The benchmark-agnostic KB compiler: written documentation in, a frozen
data contract out. See `dce.compile.run` for the flow and
`python -m dce.compile --help` for the command."""
```

`dce/compile/entries.py`:

```python
"""The compiler's neutral view of a knowledge base.

Each benchmark's adapter maps its documentation onto `KBEntry`s; nothing
past this module knows which benchmark an entry came from. LiveSQLBench's
KB types map to `calculation`, `predicate` and `illustration`; DABStep's
manual sections are `section`s, which may yield several metrics or none.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass

KINDS: tuple[str, ...] = ("calculation", "predicate", "illustration", "section")

#: The domain each kind's output lands in. A section is its own domain.
DOMAIN_BY_KIND: dict[str, str] = {
    "calculation": "calculations",
    "predicate": "definitions",
    "illustration": "reference",
}


@dataclass(frozen=True)
class KBEntry:
    id: int
    kind: str
    name: str
    text: str  # verbatim: LiveSQLBench's description and definition, or a section
    children: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        if self.kind not in KINDS:
            raise ValueError(f"unknown kind {self.kind!r}; expected one of {KINDS}")


def slug(text: str) -> str:
    value = re.sub(r"[^0-9a-z]+", "_", text.lower()).strip("_")
    return value or "entry"


def metric_names(entries) -> dict[int, str]:
    """entry id -> metric name, for every calculation and predicate. The
    slug of the entry's name; entries that share a slug ALL get `_<id>`,
    so no name depends on which entry happened to come first."""
    eligible = [e for e in entries if e.kind in ("calculation", "predicate")]
    counts = Counter(slug(e.name) for e in eligible)
    return {
        e.id: slug(e.name) if counts[slug(e.name)] == 1 else f"{slug(e.name)}_{e.id}"
        for e in eligible
    }


def compile_order(entries) -> list[KBEntry]:
    """Children before parents, so a parent's SQL can build on its children's
    metrics; otherwise ascending id."""
    by_id: dict[int, KBEntry] = {}
    for entry in entries:
        if entry.id in by_id:
            raise ValueError(f"duplicate entry id {entry.id}")
        by_id[entry.id] = entry
    for entry in by_id.values():
        missing = [c for c in entry.children if c not in by_id]
        if missing:
            raise ValueError(f"entry {entry.id} names missing children {missing}")
    order: list[KBEntry] = []
    state: dict[int, str] = {}

    def visit(entry: KBEntry, path: list[int]) -> None:
        if state.get(entry.id) == "done":
            return
        if state.get(entry.id) == "open":
            raise ValueError(f"cycle in children: {path + [entry.id]}")
        state[entry.id] = "open"
        for child in sorted(entry.children):
            visit(by_id[child], path + [entry.id])
        state[entry.id] = "done"
        order.append(entry)

    for entry in sorted(by_id.values(), key=lambda e: e.id):
        visit(entry, [])
    return order


def domain_of(entry: KBEntry) -> str:
    if entry.kind == "section":
        return slug(entry.name)
    return DOMAIN_BY_KIND[entry.kind]
```

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_compile_entries.py 2>&1 | tail -1`
Expected: all pass.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/compile/__init__.py dce/compile/entries.py tests/test_compile_entries.py
git commit -m "dce.compile: neutral KB entries, children-first order, collision-free metric names"
```

---

### Task 4: The compiler's database access

**Files:**
- Create: `dce/compile/db.py`
- Test: `tests/test_compile_db.py`

**Interfaces:**
- Produces, in `dce.compile.db`:
  - `MAX_ROWS = 50`, `QUERY_SECONDS = 60.0`;
  - `class CompileDB(path: Path, *, init_sql: tuple[str, ...] = ())`, with:
    - `.tables() -> dict[str, list[tuple[str, str]]]`: table -> [(column, type)], in DuckDB's order;
    - `.ddl() -> str`;
    - `.run_sql(sql: str) -> str`: CSV, at most `MAX_ROWS` rows, errors returned as `"ERROR: ..."`;
    - `.check_metric(expr: str, source: str, *, predicate: bool) -> str | None`: `None` when the SQL runs, else the error;
    - `.close()`.

- [ ] **Step 1: Write the failing tests**

`tests/test_compile_db.py`:

```python
from pathlib import Path

import duckdb
import pytest
from dce.compile.db import MAX_ROWS, CompileDB


@pytest.fixture
def db(tmp_path: Path):
    path = tmp_path / "c.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE t AS SELECT range AS id, range % 3 AS g FROM range(120)")
    con.execute("CREATE TABLE u (k INTEGER, label VARCHAR)")
    con.close()
    (tmp_path / "secret.json").write_text('{"gold": 42}', encoding="utf-8")
    cdb = CompileDB(path, init_sql=("SET integer_division = true",))
    yield cdb
    cdb.close()


def test_tables_and_ddl(db):
    assert db.tables() == {
        "t": [("id", "BIGINT"), ("g", "BIGINT")],
        "u": [("k", "INTEGER"), ("label", "VARCHAR")],
    }
    assert "CREATE TABLE t (\n  id BIGINT,\n  g BIGINT\n);" in db.ddl()


def test_run_sql_is_csv_capped_at_the_row_limit(db):
    out = db.run_sql("SELECT id FROM t ORDER BY id").splitlines()
    assert out[0] == "id" and out[1] == "0"
    assert len(out) == 1 + MAX_ROWS + 1 and "truncated" in out[-1]
    assert db.run_sql("SELECT 7 / 2 AS x").splitlines() == [
        "x",
        "3",
    ]  # init_sql applies


def test_errors_writes_and_file_reads_come_back_as_text(db, tmp_path):
    assert db.run_sql("SELECT nope FROM t").startswith("ERROR:")
    assert db.run_sql("CREATE TABLE w (a INT)").startswith("ERROR:")
    out = db.run_sql(f"SELECT * FROM read_text('{tmp_path / 'secret.json'}')")
    assert out.startswith("ERROR:") and "42" not in out


def test_check_metric(db):
    assert db.check_metric("SUM(id)", "main.t", predicate=False) is None
    assert db.check_metric("g = 1", "t", predicate=True) is None
    assert "nope" in db.check_metric("SUM(nope)", "t", predicate=False)
    assert db.check_metric("g +", "t", predicate=True) is not None
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_compile_db.py 2>&1 | tail -3`
Expected: FAIL, `ModuleNotFoundError: No module named 'dce.compile.db'`.

- [ ] **Step 3: Implement**

`dce/compile/db.py`:

```python
"""The compiler's database access: read-only, no file access, bounded.

The compiler may query the data, as an engineer writing a contract would,
but nothing else: the connection is read-only, `enable_external_access` is
off (so `read_text` and `glob` cannot reach the gold files that sit beside
LiveSQLBench's databases), results are capped at `MAX_ROWS`, and every
statement is interrupted at `QUERY_SECONDS`.
"""

from __future__ import annotations

import csv
import io
import threading
from pathlib import Path

import duckdb

MAX_ROWS = 50
QUERY_SECONDS = 60.0


class CompileDB:
    def __init__(self, path: Path, *, init_sql: tuple[str, ...] = ()) -> None:
        self.path = path
        self._con = duckdb.connect(str(path), read_only=True)
        try:
            for statement in (*init_sql, "SET enable_external_access = false"):
                self._con.execute(statement)
        except Exception:
            self._con.close()
            raise

    def close(self) -> None:
        self._con.close()

    def _execute(self, sql: str):
        timer = threading.Timer(QUERY_SECONDS, self._con.interrupt)
        timer.start()
        try:
            return self._con.execute(sql)
        finally:
            timer.cancel()

    def tables(self) -> dict[str, list[tuple[str, str]]]:
        rows = self._con.execute(
            "SELECT table_name, column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'main' ORDER BY table_name, ordinal_position"
        ).fetchall()
        out: dict[str, list[tuple[str, str]]] = {}
        for table, column, data_type in rows:
            out.setdefault(table, []).append((column, data_type))
        return out

    def ddl(self) -> str:
        return "\n\n".join(
            f"CREATE TABLE {table} (\n"
            + ",\n".join(f"  {column} {data_type}" for column, data_type in columns)
            + "\n);"
            for table, columns in self.tables().items()
        )

    def run_sql(self, sql: str) -> str:
        try:
            cursor = self._execute(sql)
            columns = [d[0] for d in cursor.description or []]
            rows = cursor.fetchmany(MAX_ROWS + 1)
        except duckdb.Error as exc:
            return f"ERROR: {exc}"
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(columns)
        writer.writerows(rows[:MAX_ROWS])
        text = buf.getvalue().rstrip("\n")
        if len(rows) > MAX_ROWS:
            text += f"\n... truncated at {MAX_ROWS} rows"
        return text

    def check_metric(self, expr: str, source: str, *, predicate: bool) -> str | None:
        """None when the metric runs over `source`: a calculation as
        `SELECT <expr> FROM <source>`, a predicate as a `WHERE` clause."""
        sql = (
            f"SELECT * FROM {source} WHERE {expr} LIMIT 5"
            if predicate
            else f"SELECT {expr} FROM {source} LIMIT 5"
        )
        try:
            self._execute(sql).fetchall()
        except duckdb.Error as exc:
            return str(exc)
        return None
```

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_compile_db.py 2>&1 | tail -1`
Expected: all pass.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/compile/db.py tests/test_compile_db.py
git commit -m "dce.compile: read-only, file-less, bounded database access for the compiler"
```

---

### Task 5: Compile inputs and both adapters

**Files:**
- Create: `dce/compile/inputs.py`
- Modify: `dce/benchmarks/livesqlbench.py` (adapter `compile_inputs`; the meaning text shared with `render_column_meanings`)
- Modify: `dce/benchmarks/dabstep.py` (`manual_sections`, `compile_inputs`)
- Create: `tests/conftest.py` (`lsb_data` fixture)
- Test: `tests/test_compile_inputs.py`

**Interfaces:**
- Consumes: `KBEntry` (Task 3); `CompileDB` (Task 4); `lsb_grade.PG_COMPAT`.
- Produces:
  - `dce.compile.inputs.CompileInputs`, a frozen dataclass with:
    - `benchmark: str`, `db_name: str`, `entries: tuple[KBEntry, ...]`;
    - `ddl: str`, `column_meanings: str`, `dialect_note: str`;
    - `db_path: Path`, `init_sql: tuple[str, ...]`;
    - `sources: tuple[Path, ...]`, `out_subdir: str`;
    - `column_descriptions: dict[tuple[str, str], str]`, keyed by lower-case `(table, column)`;
    - `table_descriptions: dict[str, str]`, keyed by lower-case table.
  - `dce.benchmarks.livesqlbench.compile_inputs(data: Path, db: str) -> CompileInputs`.
  - `dce.benchmarks.livesqlbench.KIND_BY_TYPE`.
  - `dce.benchmarks.dabstep.manual_sections(manual: str) -> list[KBEntry]`.
  - `dce.benchmarks.dabstep.compile_inputs(context_dir: Path = CONTEXT_DIR, db: Path = Path("data/dabstep.duckdb")) -> CompileInputs`.
  - `tests/conftest.py`: fixture `lsb_data`, the LSB fixture laid out as `LSB_DATA`, with `duckdb/shop.duckdb` built.

- [ ] **Step 1: Write the fixture and the failing tests**

`tests/conftest.py`:

```python
import shutil
from pathlib import Path

import duckdb
import pytest

LSB_FIXTURE = Path(__file__).parent / "fixtures" / "lsb"


@pytest.fixture
def lsb_data(tmp_path: Path) -> Path:
    """The LiveSQLBench fixture laid out as `LSB_DATA`, its DuckDB built."""
    root = tmp_path / "lsb"
    shutil.copytree(LSB_FIXTURE, root)
    (root / "duckdb").mkdir()
    con = duckdb.connect(str(root / "duckdb" / "shop.duckdb"))
    con.execute((LSB_FIXTURE / "shop.sql").read_text(encoding="utf-8"))
    con.close()
    return root
```

`tests/test_compile_inputs.py`:

```python
from pathlib import Path

import duckdb
from dce.benchmarks import dabstep, livesqlbench
from dce.benchmarks.lsb_grade import PG_COMPAT
from dce.compile.entries import KBEntry


def test_livesqlbench_adapter_maps_kinds_text_and_children(lsb_data):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    assert inputs.benchmark == "livesqlbench" and inputs.db_name == "shop"
    by_id = {e.id: e for e in inputs.entries}
    assert [by_id[i].kind for i in range(1, 7)] == [
        "illustration",
        "predicate",
        "calculation",
        "calculation",
        "predicate",
        "illustration",
    ]
    assert by_id[3] == KBEntry(
        3,
        "calculation",
        "Shipped Revenue",
        "Revenue a customer has realised.\n\n"
        "The sum of amount over the customer's Shipped Orders.",
        (2,),
    )
    assert by_id[1].children == ()  # -1 in the file means none
    assert inputs.ddl.startswith('CREATE TABLE "customers"')
    assert "- orders.amount: Order value; null when not yet priced." in (
        inputs.column_meanings
    )
    assert inputs.column_descriptions[("orders", "amount")] == (
        "Order value; null when not yet priced."
    )
    assert inputs.init_sql == PG_COMPAT
    assert inputs.db_path == lsb_data / "duckdb" / "shop.duckdb"
    assert inputs.out_subdir == "livesqlbench/shop"
    assert {p.name for p in inputs.sources} == {
        "shop_kb.jsonl",
        "shop_column_meaning_base.json",
        "shop_schema.txt",
        "shop.duckdb",
    }


MANUAL = """# A Guide

## Table of Contents
1. Intro

## 1. Introduction
Hello.

## 2. Fees
fee = fixed + rate * value / 10000

### Pricing Overview
More about fees.

####  2.1. Local Acquiring
Same country is cheaper.
"""


def test_manual_sections_split_at_numbered_headings():
    sections = dabstep.manual_sections(MANUAL)
    assert [(s.id, s.kind, s.name) for s in sections] == [
        (0, "section", "A Guide"),
        (1, "section", "1 Introduction"),
        (2, "section", "2 Fees"),
        (3, "section", "2.1 Local Acquiring"),
    ]
    assert sections[2].text == (
        "## 2. Fees\nfee = fixed + rate * value / 10000\n\n"
        "### Pricing Overview\nMore about fees."
    )
    assert "".join(s.text for s in sections).replace("\n", "") == (
        MANUAL.replace("\n", "")
    )


def test_dabstep_adapter(tmp_path: Path):
    context = tmp_path / "context"
    context.mkdir()
    (context / "manual.md").write_text(MANUAL, encoding="utf-8")
    (context / "payments-readme.md").write_text(
        "Columns: psp_reference.", encoding="utf-8"
    )
    db = tmp_path / "d.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE payments (psp_reference BIGINT)")
    con.close()
    inputs = dabstep.compile_inputs(context, db)
    assert inputs.benchmark == "dabstep" and inputs.out_subdir == "dabstep"
    assert [e.name for e in inputs.entries][1:] == [
        "1 Introduction",
        "2 Fees",
        "2.1 Local Acquiring",
    ]
    assert inputs.column_meanings == "Columns: psp_reference."
    assert inputs.table_descriptions == {"payments": "Columns: psp_reference."}
    assert "CREATE TABLE payments" in inputs.ddl
    assert inputs.init_sql == ()
    assert {p.name for p in inputs.sources} == {
        "manual.md",
        "payments-readme.md",
        "d.duckdb",
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_compile_inputs.py 2>&1 | tail -3`
Expected: FAIL, `ModuleNotFoundError: No module named 'dce.compile.inputs'` (raised through the adapters' imports) or `AttributeError` on `compile_inputs`.

- [ ] **Step 3: Implement**

`dce/compile/inputs.py`:

```python
"""What one compile reads: the entries, the context a data engineer would
have (DDL, column meanings), and the database. Built by each benchmark's
adapter; `sources` lists every file the adapter read, for the log header."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from dce.compile.entries import KBEntry


@dataclass(frozen=True)
class CompileInputs:
    benchmark: str
    db_name: str
    entries: tuple[KBEntry, ...]
    ddl: str
    column_meanings: str  # rendered for the compiler's prompt
    dialect_note: str
    db_path: Path
    init_sql: tuple[str, ...]
    sources: tuple[Path, ...]
    out_subdir: str  # under the contracts directory
    column_descriptions: dict[tuple[str, str], str] = field(default_factory=dict)
    table_descriptions: dict[str, str] = field(default_factory=dict)
```

`dce/benchmarks/livesqlbench.py`:
- Import `KBEntry` from `dce.compile.entries`, and `CompileInputs` from `dce.compile.inputs`.
- Factor the per-value text out of `render_column_meanings`. Its output stays byte-identical:

```python
def _meaning_text(value) -> str:
    if isinstance(value, dict):
        text = str(value.get("column_meaning", "")).strip()
        fields = value.get("fields_meaning")
        if fields:
            text += " Fields: " + json.dumps(fields, ensure_ascii=False)
        return text
    return str(value).strip()


def render_column_meanings(meanings: dict) -> str:
    """One line per column, `- table.column: meaning`, in the file's order. A
    JSON column's meaning is an object; its field meanings follow as JSON."""
    lines = []
    for key, value in meanings.items():
        _, table, column = key.split("|", 2)
        lines.append(f"- {table}.{column}: {_meaning_text(value)}")
    return "\n".join(lines)
```

- Add the adapter:

```python
#: LiveSQLBench's KB types as the compiler's neutral kinds (spec, Compiler).
KIND_BY_TYPE: dict[str, str] = {
    "calculation_knowledge": "calculation",
    "domain_knowledge": "predicate",
    "value_illustration": "illustration",
}

#: How the compiler is told the database's dialect switches.
DIALECT_NOTE = "The database sorts NULLs and divides integers as PostgreSQL does."


def compile_inputs(data: Path, db: str) -> CompileInputs:
    """The compiler's inputs for one database: its KB, column meanings, DDL
    and DuckDB file, and nothing else (no task, gold or test-case file)."""
    folder = data / "hf-full-v1" / db
    kb_path = folder / f"{db}_kb.jsonl"
    meanings_path = folder / f"{db}_column_meaning_base.json"
    schema_path = folder / f"{db}_schema.txt"
    db_path = data / "duckdb" / f"{db}.duckdb"
    entries = []
    for raw in _read_jsonl(kb_path):
        children = raw.get("children_knowledge")
        entries.append(
            KBEntry(
                id=int(raw["id"]),
                kind=KIND_BY_TYPE[raw["type"]],
                name=str(raw["knowledge"]),
                text=f"{raw['description']}\n\n{raw['definition']}",
                # -1 in the file means no children.
                children=tuple(int(c) for c in children)
                if isinstance(children, list)
                else (),
            )
        )
    meanings = json.loads(meanings_path.read_text(encoding="utf-8"))
    return CompileInputs(
        benchmark="livesqlbench",
        db_name=db,
        entries=tuple(entries),
        ddl=schema_path.read_text(encoding="utf-8").strip(),
        column_meanings=render_column_meanings(meanings),
        dialect_note=DIALECT_NOTE,
        db_path=db_path,
        init_sql=PG_COMPAT,
        sources=(kb_path, meanings_path, schema_path, db_path),
        out_subdir=f"livesqlbench/{db}",
        column_descriptions={
            (key.split("|", 2)[1].lower(), key.split("|", 2)[2].lower()): (
                _meaning_text(value)
            )
            for key, value in meanings.items()
        },
    )
```

Import `PG_COMPAT` from `dce.benchmarks.lsb_grade`, beside `SAFE_INIT_SQL`.

`dce/benchmarks/dabstep.py`: add `import re`, import `KBEntry`, `CompileInputs` and `CompileDB`, and add:

```python
#: A numbered heading at any level: "## 5. Fees", "###  5.1 Best",
#: "#### 5.1.2. Choosing", "# 5.1.3 Processing" (the manual's levels are
#: inconsistent; its numbers are not). Unnumbered headings stay inside the
#: numbered section above them.
_SECTION = re.compile(r"^#{1,6}\s+(\d+(?:\.\d+)*)\.?\s+(\S.*?)\s*$")


def manual_sections(manual: str) -> list[KBEntry]:
    """`manual.md` as neutral entries of kind `section`: the text before
    the first numbered heading (title and contents), then one entry per
    numbered heading, each holding its text verbatim."""
    lines = manual.splitlines()
    starts = [i for i, line in enumerate(lines) if _SECTION.match(line)]
    entries = []
    head = "\n".join(lines[: starts[0] if starts else len(lines)]).strip()
    if head:
        title = next(
            (line.lstrip("#").strip() for line in lines if line.startswith("#")),
            "Preamble",
        )
        entries.append(KBEntry(0, "section", title, head))
    for n, (start, end) in enumerate(
        zip(starts, starts[1:] + [len(lines)], strict=True), start=1
    ):
        match = _SECTION.match(lines[start])
        assert match is not None
        text = "\n".join(lines[start:end]).strip()
        entries.append(KBEntry(n, "section", f"{match[1]} {match[2]}", text))
    return entries


def compile_inputs(
    context_dir: Path = CONTEXT_DIR, db: Path = Path("data/dabstep.duckdb")
) -> CompileInputs:
    """DABStep's compiler inputs (spec, Part 5): the manual split into
    sections, `payments-readme.md` as the column meanings, the database.
    Never `tasks.json` or `golds.json`."""
    manual_path = context_dir / "manual.md"
    readme_path = context_dir / "payments-readme.md"
    readme = readme_path.read_text(encoding="utf-8").strip()
    cdb = CompileDB(db)
    try:
        ddl = cdb.ddl()
    finally:
        cdb.close()
    return CompileInputs(
        benchmark="dabstep",
        db_name="dabstep",
        entries=tuple(manual_sections(manual_path.read_text(encoding="utf-8"))),
        ddl=ddl,
        column_meanings=readme,
        dialect_note="DuckDB's own semantics apply.",
        db_path=db,
        init_sql=(),
        sources=(manual_path, readme_path, db),
        out_subdir="dabstep",
        # The readme documents payments.csv; the compiled contract carries
        # it on that table, as the hand-authored one carries it in prose.
        table_descriptions={"payments": readme},
    )
```

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest -q --deselect tests/test_replay.py::test_sql_statements_are_ordered_and_only_from_sql_tools 2>&1 | tail -1`
Expected: all pass, including `tests/test_livesqlbench.py::test_rendering_of_meanings_and_manual`, which pins the refactored renderer.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/compile/inputs.py dce/benchmarks/livesqlbench.py dce/benchmarks/dabstep.py tests/conftest.py tests/test_compile_inputs.py
git commit -m "dce.compile: compile inputs, and the LiveSQLBench and DABStep KB adapters"
```

---

### Task 6: One entry's compile: prompts, tools, submission rules

**Files:**
- Create: `dce/compile/session.py`
- Test: `tests/test_compile_session.py`

**Interfaces:**
- Consumes: `KBEntry`, `metric_names`, `domain_of`, `slug` (Task 3); `CompileDB` (Task 4); `CompileInputs` (Task 5).
- Produces, in `dce.compile.session`:
  - `COMPILER_MODEL = "claudesonnet5"`, `MAX_ATTEMPTS = 3`, `NOTE_MARKER = "Compiler note: "`;
  - `@dataclass DriverResult(messages: list, usage: dict)`. `usage` holds `input_tokens`, `output_tokens`, `cached_tokens`, `usd`, and `error` when the run ended abnormally.
  - `Driver`: a callable `(*, system_prompt: str, user_prompt: str, tools: list[Callable]) -> DriverResult`.
  - `@dataclass EntryRecord`, with:
    - `entry_id`, `kind`, `name`, `domain`;
    - `status`: `"metric" | "untranslated" | "prose"`;
    - `metrics: list[dict]`, `prose: list[dict]`;
    - `note: str`, `attempts: int`, `error: str | None`;
    - `queries: list[dict]`, `usage: dict`, `messages: list`;
    - `.to_dict()`, and the classmethod `.from_dict(d)`.
  - `system_prompt(inputs) -> str`.
  - `compile_entry(entry, inputs, db, driver, *, prior: dict[int, EntryRecord], tables: dict[str, list[tuple[str, str]]]) -> EntryRecord`.
- Record shapes, which emission (Task 7) relies on:
  - a metric dict is `{"name", "description", "sql_expression", "source_model"}`, plus `"untranslated"` when set;
  - a prose dict is `{"target": "domain" | "table" | "column", "table": str | None, "column": str | None, "text": str}`.

- [ ] **Step 1: Write the failing tests**

`tests/test_compile_session.py`:

```python
import pytest
from dce.benchmarks import livesqlbench
from dce.compile.db import CompileDB
from dce.compile.entries import KBEntry
from dce.compile.session import (
    MAX_ATTEMPTS,
    NOTE_MARKER,
    DriverResult,
    EntryRecord,
    compile_entry,
    system_prompt,
)

USAGE = {"input_tokens": 10, "output_tokens": 5, "cached_tokens": 0, "usd": 0.001}


def scripted(*calls):
    """A driver that makes `calls` -- (tool name, kwargs) -- in order and
    keeps each tool's reply, as a model would see them."""
    replies = []

    def driver(*, system_prompt, user_prompt, tools):
        by_name = {t.__name__: t for t in tools}
        assert set(by_name) == {"run_sql", "submit_entry"}
        driver.user_prompt = user_prompt
        for name, kwargs in calls:
            replies.append(by_name[name](**kwargs))
        return DriverResult(messages=[{"scripted": True}], usage=dict(USAGE))

    driver.replies = replies
    return driver


@pytest.fixture
def ctx(lsb_data):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    db = CompileDB(inputs.db_path, init_sql=inputs.init_sql)
    yield inputs, db, db.tables(), {e.id: e for e in inputs.entries}
    db.close()


def test_a_calculation_validates_logs_queries_and_carries_the_text(ctx):
    inputs, db, tables, by_id = ctx
    prior = {
        2: EntryRecord(
            2,
            "predicate",
            "Shipped Order",
            "definitions",
            "metric",
            [
                {
                    "name": "shipped_order",
                    "description": "d",
                    "sql_expression": "status = 'shipped'",
                    "source_model": "main.orders",
                }
            ],
            [],
            "",
            1,
            None,
            [],
            {},
            [],
        )
    }
    driver = scripted(
        ("run_sql", {"sql": "SELECT DISTINCT status FROM orders"}),
        ("submit_entry", {"sql_expression": "SUM(nope)", "source_model": "orders"}),
        (
            "submit_entry",
            {
                "sql_expression": "SUM(CASE WHEN status = 'shipped' THEN amount END)",
                "source_model": "orders",
                "note": "Status values are lower case.\n\nChecked.",
            },
        ),
    )
    record = compile_entry(by_id[3], inputs, db, driver, prior=prior, tables=tables)
    assert (
        driver.replies[1].startswith("ERROR:")
        and "2 attempt(s) left" in driver.replies[1]
    )
    assert driver.replies[2] == "Recorded."
    assert "shipped_order over main.orders" in driver.user_prompt  # its child
    assert record.status == "metric" and record.attempts == 2
    [metric] = record.metrics
    assert metric["name"] == "shipped_revenue"
    assert metric["source_model"] == "main.orders"
    assert metric["description"] == (
        "Revenue a customer has realised.\n\n"
        "The sum of amount over the customer's Shipped Orders.\n\n"
        "Built on: shipped_order.\n\n"
        f"{NOTE_MARKER}Status values are lower case. Checked."
    )
    assert record.queries == [
        {"sql": "SELECT DISTINCT status FROM orders", "error": False}
    ]
    assert record.usage == USAGE and record.domain == "calculations"


def test_a_predicate_is_checked_as_a_where_clause(ctx):
    inputs, db, tables, by_id = ctx
    driver = scripted(
        (
            "submit_entry",
            {"sql_expression": "tier = 'gold'", "source_model": "customers"},
        )
    )
    record = compile_entry(by_id[5], inputs, db, driver, prior={}, tables=tables)
    assert record.status == "metric" and record.domain == "definitions"
    assert record.metrics[0]["description"].endswith("Built on: Customer Tier.")


def test_a_metric_that_never_validates_is_kept_as_untranslated_prose(ctx):
    inputs, db, tables, by_id = ctx
    bad = ("submit_entry", {"sql_expression": "SUM(qty) +", "source_model": "orders"})
    driver = scripted(*[bad] * (MAX_ATTEMPTS + 1))
    record = compile_entry(by_id[4], inputs, db, driver, prior={}, tables=tables)
    assert "No attempts left" in driver.replies[MAX_ATTEMPTS - 1]
    assert driver.replies[MAX_ATTEMPTS] == (
        "No attempts left; the entry is kept as prose. Stop."
    )
    assert record.status == "untranslated" and record.attempts == MAX_ATTEMPTS
    [metric] = record.metrics
    assert metric["sql_expression"] == "" and metric["untranslated"]
    assert metric["description"].startswith("Items per order.")


def test_no_submission_is_untranslated_too(ctx):
    inputs, db, tables, by_id = ctx
    record = compile_entry(by_id[4], inputs, db, scripted(), prior={}, tables=tables)
    assert record.status == "untranslated"
    assert record.metrics[0]["untranslated"] == "no submission"


def test_an_unknown_source_table_is_an_error_not_a_metric(ctx):
    inputs, db, tables, by_id = ctx
    driver = scripted(
        ("submit_entry", {"sql_expression": "COUNT(*)", "source_model": "nowhere"}),
        ("submit_entry", {"sql_expression": "COUNT(*)", "source_model": "MAIN.Orders"}),
    )
    record = compile_entry(by_id[4], inputs, db, driver, prior={}, tables=tables)
    assert "nowhere" in driver.replies[0]
    assert record.metrics[0]["source_model"] == "main.orders"


def test_an_illustration_lands_on_its_column_or_the_reference_domain(ctx):
    inputs, db, tables, by_id = ctx
    driver = scripted(
        ("submit_entry", {"target": "customers.nope"}),
        ("submit_entry", {"target": "Customers.Tier", "note": "Only two values."}),
    )
    record = compile_entry(by_id[1], inputs, db, driver, prior={}, tables=tables)
    assert record.status == "prose" and record.metrics == []
    assert record.prose == [
        {
            "target": "column",
            "table": "customers",
            "column": "tier",
            "text": "Customer Tier: The loyalty tiers customers belong to.\n\n"
            "Values are 'gold' and 'silver'.\n\n"
            f"{NOTE_MARKER}Only two values.",
        }
    ]
    lost = compile_entry(by_id[6], inputs, db, scripted(), prior={}, tables=tables)
    assert lost.prose[0]["target"] == "domain" and lost.domain == "reference"


def test_a_section_yields_named_metrics_and_always_its_domain_text(ctx):
    inputs, db, tables, _ = ctx
    section = KBEntry(9, "section", "5 Fees", "## 5. Fees\nfee = qty * 2")
    taken = {
        1: EntryRecord(
            1,
            "section",
            "1 Intro",
            "1_intro",
            "metric",
            [
                {
                    "name": "fee",
                    "description": "",
                    "sql_expression": "1",
                    "source_model": "main.orders",
                }
            ],
            [],
            "",
            1,
            None,
            [],
            {},
            [],
        )
    }
    metric = {
        "name": "Order Fee",
        "kind": "calculation",
        "sql_expression": "qty * 2",
        "source_model": "orders",
        "description": "The fee on one order.",
    }
    driver = scripted(
        ("submit_entry", {"metrics": [{**metric, "name": "Fee"}]}),
        ("submit_entry", {"metrics": [metric], "note": "qty is never null."}),
    )
    record = compile_entry(section, inputs, db, driver, prior=taken, tables=tables)
    assert "already used" in driver.replies[0]
    assert record.domain == "5_fees" and record.status == "metric"
    assert record.metrics == [
        {
            "name": "order_fee",
            "description": 'The fee on one order.\n\nFrom the manual section "5 Fees".',
            "sql_expression": "qty * 2",
            "source_model": "main.orders",
        }
    ]
    assert record.prose == [
        {
            "target": "domain",
            "table": None,
            "column": None,
            "text": f"## 5. Fees\nfee = qty * 2\n\n{NOTE_MARKER}qty is never null.",
        }
    ]
    empty = compile_entry(
        section,
        inputs,
        db,
        scripted(("submit_entry", {"metrics": []})),
        prior={},
        tables=tables,
    )
    assert empty.status == "prose" and empty.prose[0]["text"].startswith("## 5. Fees")


def test_records_round_trip_and_the_system_prompt_carries_the_context(ctx):
    inputs, db, tables, by_id = ctx
    record = compile_entry(by_id[4], inputs, db, scripted(), prior={}, tables=tables)
    assert EntryRecord.from_dict(record.to_dict()) == record
    prompt = system_prompt(inputs)
    assert inputs.ddl in prompt and inputs.column_meanings in prompt
    assert "PostgreSQL" in prompt and str(MAX_ATTEMPTS) in prompt
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_compile_session.py 2>&1 | tail -3`
Expected: FAIL, `ModuleNotFoundError: No module named 'dce.compile.session'`.

- [ ] **Step 3: Implement**

`dce/compile/session.py`:

```python
"""One KB entry's compile: the prompt, the two tools, the submission rules.

The model gets `run_sql` (read-only, at most 50 rows, every call logged)
and `submit_entry`, whose parameters depend on the entry's kind. A
submission is validated before it is accepted -- a metric's SQL must run
over its source table, an illustration's target must exist -- and the model
has `MAX_ATTEMPTS` tries. An entry that never validates is KEPT: a
calculation or predicate becomes a metric with an empty `sql_expression`
and `untranslated` set to the last error, so its KB text still reaches the
contract arm (the library carries `untranslated` for exactly this).

The driver -- the model loop -- is injected. Tests script the tool calls;
`dce.compile.run.claude_driver` is the only code that spends money.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Literal

from pydantic import BaseModel

from dce.compile.db import CompileDB
from dce.compile.entries import KBEntry, domain_of, metric_names, slug
from dce.compile.inputs import CompileInputs

COMPILER_MODEL = "claudesonnet5"
MAX_ATTEMPTS = 3
#: Starts the paragraph that carries what the compiler learned from the
#: data. `manual_compiled` collects every such paragraph from the contract,
#: so both arms see the compiler's knowledge (spec, Compiler: Notes).
NOTE_MARKER = "Compiler note: "

SYSTEM_PROMPT = """\
You compile a database's written documentation into a data contract, one \
entry at a time. An analyst agent will later answer questions with the \
contract: each metric you write is a SQL expression it can look up by name \
and reuse.

Rules:
- Write DuckDB SQL. {dialect_note}
- A calculation's sql_expression is evaluated as \
`SELECT <sql_expression> FROM <source_model>`; a predicate's as \
`SELECT * FROM <source_model> WHERE <sql_expression>`. Use only columns of \
source_model; reach other tables through scalar subqueries.
- Encode what the entry states. Where the data forces a choice the entry \
does not make (how a value is stored, a JSON path, a unit), check it with \
run_sql, encode it, and say so in `note`. Add nothing else.
- run_sql is read-only and returns at most 50 rows.
- Call submit_entry when you are done. If it returns an error, fix the \
submission and call it again; you have {attempts} attempts. When it \
returns "Recorded.", reply "done".

## Database schema

{ddl}

## Column meanings

{meanings}
"""

INSTRUCTIONS: dict[str, str] = {
    "calculation": (
        "Compile this calculation into one metric: a SQL expression computing "
        "the value over one table. Call submit_entry(sql_expression, "
        "source_model, note)."
    ),
    "predicate": (
        "Compile this definition into one metric whose SQL expression is a "
        "boolean condition over the rows of one table. Call "
        "submit_entry(sql_expression, source_model, note)."
    ),
    "illustration": (
        "This entry explains values. Find the table or column it describes and "
        "call submit_entry(target, note), with target 'table' or "
        "'table.column', or '' if it describes no single one."
    ),
    "section": (
        "This is one section of the manual. Submit every rule in it that can be "
        "computed over the database as a metric (a calculation, or a predicate "
        "over rows), or no metrics if the section is prose only. Call "
        "submit_entry(metrics, note)."
    ),
}


class SectionMetric(BaseModel):
    name: str
    kind: Literal["calculation", "predicate"]
    sql_expression: str
    source_model: str
    description: str


@dataclass
class DriverResult:
    messages: list
    usage: dict


#: (*, system_prompt, user_prompt, tools) -> DriverResult
Driver = Callable[..., DriverResult]


@dataclass
class EntryRecord:
    entry_id: int
    kind: str
    name: str
    domain: str
    status: str  # "metric" | "untranslated" | "prose"
    metrics: list[dict]
    prose: list[dict]
    note: str
    attempts: int
    error: str | None
    queries: list[dict] = field(default_factory=list)
    usage: dict = field(default_factory=dict)
    messages: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"type": "entry", **asdict(self)}

    @classmethod
    def from_dict(cls, data: dict) -> EntryRecord:
        return cls(**{k: v for k, v in data.items() if k != "type"})


def system_prompt(inputs: CompileInputs) -> str:
    return SYSTEM_PROMPT.format(
        dialect_note=inputs.dialect_note,
        attempts=MAX_ATTEMPTS,
        ddl=inputs.ddl,
        meanings=inputs.column_meanings or "(none)",
    )


def _one_paragraph(note: str) -> str:
    # A note is one paragraph, so `manual_compiled` can find it by its marker.
    return " ".join(note.split())


class _EntrySession:
    def __init__(self, entry, inputs, db, *, prior, tables) -> None:
        self.entry = entry
        self.inputs = inputs
        self.db = db
        self.prior = prior
        self.tables = tables
        self.by_id = {e.id: e for e in inputs.entries}
        self.names = metric_names(inputs.entries)
        self.taken = set(self.names.values()) | {
            m["name"] for r in prior.values() for m in r.metrics
        }
        self.attempts = 0
        self.error: str | None = None
        self.accepted: dict | None = None
        self.queries: list[dict] = []

    # -- resolution ---------------------------------------------------------
    def _table(self, name: str) -> str | None:
        want = name.strip().strip('"').lower().removeprefix("main.")
        return next((t for t in self.tables if t.lower() == want), None)

    def _column(self, table: str, name: str) -> str | None:
        want = name.strip().strip('"').lower()
        return next((c for c, _ in self.tables[table] if c.lower() == want), None)

    def _metric(self, expr: str, source: str, *, predicate: bool):
        table = self._table(source)
        if table is None:
            return None, f"no table {source!r}; tables are {sorted(self.tables)}"
        error = self.db.check_metric(expr, f"main.{table}", predicate=predicate)
        if error is not None:
            return None, error
        return {"sql_expression": expr.strip(), "source_model": f"main.{table}"}, None

    # -- the submission rule --------------------------------------------------
    def _submit(self, check: Callable[[], tuple[dict | None, str | None]]) -> str:
        if self.accepted is not None:
            return "Already recorded."
        if self.attempts >= MAX_ATTEMPTS:
            return "No attempts left; the entry is kept as prose. Stop."
        self.attempts += 1
        accepted, error = check()
        if error is None and accepted is not None:
            self.accepted, self.error = accepted, None
            return "Recorded."
        self.error = error
        left = MAX_ATTEMPTS - self.attempts
        if left:
            return f"ERROR: {error}\nFix it and call submit_entry again ({left} attempt(s) left)."
        return f"ERROR: {error}\nNo attempts left; the entry is kept as prose. Stop."

    # -- tools ----------------------------------------------------------------
    def tools(self) -> list[Callable]:
        db, queries = self.db, self.queries

        def run_sql(sql: str) -> str:
            """Run a read-only SQL query on the database; at most 50 rows come back."""
            out = db.run_sql(sql)
            queries.append({"sql": sql, "error": out.startswith("ERROR:")})
            return out

        kind = self.entry.kind
        if kind in ("calculation", "predicate"):
            predicate = kind == "predicate"

            def submit_entry(
                sql_expression: str, source_model: str, note: str = ""
            ) -> str:
                """Submit this entry's metric: a SQL expression over one table, and
                a note on anything you checked in the data to write it."""

                def check():
                    accepted, error = self._metric(
                        sql_expression, source_model, predicate=predicate
                    )
                    return (accepted and {**accepted, "note": note}), error

                return self._submit(check)

        elif kind == "illustration":

            def submit_entry(target: str, note: str = "") -> str:  # type: ignore[no-redef]
                """Submit where this entry belongs: 'table', 'table.column', or ''
                for neither, and a note on anything you checked in the data."""

                def check():
                    if not target.strip():
                        return {"target": "domain", "note": note}, None
                    table_part, _, column_part = target.partition(".")
                    table = self._table(table_part)
                    if table is None:
                        return None, f"no table {table_part!r}"
                    if not column_part:
                        return {"target": "table", "table": table, "note": note}, None
                    column = self._column(table, column_part)
                    if column is None:
                        return None, f"no column {column_part!r} in {table}"
                    return {
                        "target": "column",
                        "table": table,
                        "column": column,
                        "note": note,
                    }, None

                return self._submit(check)

        else:

            def submit_entry(metrics: list[SectionMetric], note: str = "") -> str:  # type: ignore[no-redef]
                """Submit the section's metrics (possibly none), and a note on
                anything you checked in the data to write them."""

                def check():
                    out, seen = [], set()
                    for raw in metrics:
                        m = SectionMetric.model_validate(raw)
                        name = slug(m.name)
                        if name in self.taken or name in seen:
                            return None, f"metric name {name!r} is already used"
                        seen.add(name)
                        accepted, error = self._metric(
                            m.sql_expression,
                            m.source_model,
                            predicate=m.kind == "predicate",
                        )
                        if error is not None or accepted is None:
                            return None, f"metric {name!r}: {error}"
                        out.append(
                            {"name": name, "description": m.description, **accepted}
                        )
                    return {"metrics": out, "note": note}, None

                return self._submit(check)

        return [run_sql, submit_entry]

    # -- the record -----------------------------------------------------------
    def _ref(self, child: int) -> str:
        return self.names.get(child, self.by_id[child].name)

    def _with_note(self, text: str, note: str) -> str:
        note = _one_paragraph(note)
        return f"{text}\n\n{NOTE_MARKER}{note}" if note else text

    def user_prompt(self) -> str:
        entry = self.entry
        parts = [INSTRUCTIONS[entry.kind], f"## Entry: {entry.name}\n\n{entry.text}"]
        built = [
            f"- {m['name']} over {m['source_model']}: {m['sql_expression']}"
            for child in entry.children
            for m in (self.prior[child].metrics if child in self.prior else [])
            if m["sql_expression"]
        ]
        if built:
            parts.append("## Compiled entries it builds on\n\n" + "\n".join(built))
        return "\n\n".join(parts)

    def record(self, result: DriverResult) -> EntryRecord:
        entry, got = self.entry, self.accepted
        note = _one_paragraph(got.get("note", "")) if got else ""
        metrics: list[dict] = []
        prose: list[dict] = []
        if entry.kind in ("calculation", "predicate"):
            text = entry.text
            if entry.children:
                text += (
                    "\n\nBuilt on: "
                    + ", ".join(self._ref(c) for c in entry.children)
                    + "."
                )
            if got:
                status = "metric"
                metrics.append(
                    {
                        "name": self.names[entry.id],
                        "description": self._with_note(text, note),
                        "sql_expression": got["sql_expression"],
                        "source_model": got["source_model"],
                    }
                )
            else:
                status = "untranslated"
                metrics.append(
                    {
                        "name": self.names[entry.id],
                        "description": text,
                        "sql_expression": "",
                        "source_model": "",
                        "untranslated": self.error or "no submission",
                    }
                )
        elif entry.kind == "illustration":
            status = "prose"
            target = got or {"target": "domain"}
            prose.append(
                {
                    "target": target["target"],
                    "table": target.get("table"),
                    "column": target.get("column"),
                    "text": self._with_note(f"{entry.name}: {entry.text}", note),
                }
            )
        else:
            for m in got["metrics"] if got else []:
                metrics.append(
                    {
                        "name": m["name"],
                        "description": f"{m['description'].strip()}\n\n"
                        f'From the manual section "{entry.name}".',
                        "sql_expression": m["sql_expression"],
                        "source_model": m["source_model"],
                    }
                )
            status = "metric" if metrics else "prose"
            prose.append(
                {
                    "target": "domain",
                    "table": None,
                    "column": None,
                    "text": self._with_note(entry.text, note),
                }
            )
        return EntryRecord(
            entry_id=entry.id,
            kind=entry.kind,
            name=entry.name,
            domain=domain_of(entry),
            status=status,
            metrics=metrics,
            prose=prose,
            note=note,
            attempts=self.attempts,
            error=self.error,
            queries=self.queries,
            usage=result.usage,
            messages=result.messages,
        )


def compile_entry(
    entry: KBEntry,
    inputs: CompileInputs,
    db: CompileDB,
    driver: Driver,
    *,
    prior: dict[int, EntryRecord],
    tables: dict[str, list[tuple[str, str]]],
) -> EntryRecord:
    session = _EntrySession(entry, inputs, db, prior=prior, tables=tables)
    result = driver(
        system_prompt=system_prompt(inputs),
        user_prompt=session.user_prompt(),
        tools=session.tools(),
    )
    return session.record(result)
```

A rule the tests pin: a calculation's `note` is not part of the metric dict. It goes into the description, after the marker.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_compile_session.py 2>&1 | tail -1`
Expected: all pass. If a `# type: ignore[no-redef]` is unused under ty, remove it. If ty rejects the redefinition, give the three inner functions distinct Python names and set `__name__ = "submit_entry"` on each before returning; the tests look tools up by `__name__`.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/compile/session.py tests/test_compile_session.py
git commit -m "dce.compile: one entry's compile, validated submissions, prose fallback"
```

---

### Task 7: Emitting the contract

**Files:**
- Create: `dce/compile/emit.py`
- Modify: `dce/frozen.py` (`load_contract_at`)
- Test: `tests/test_compile_emit.py`

**Interfaces:**
- Consumes: `EntryRecord`, `NOTE_MARKER`, `COMPILER_MODEL` (Task 6); `CompileInputs` (Task 5).
- Produces:
  - `dce.compile.emit.build(inputs, records, tables) -> tuple[dict, dict]`, returning `(contract, semantic)`;
  - `dce.compile.emit.write(out_dir: Path, contract: dict, semantic: dict, inputs) -> None`, writing `contract.yml` and `semantic.yml`;
  - `dce.compile.emit.render_compiled(contract_dir: Path) -> str`, `manual_compiled`'s appendix;
  - `dce.compile.emit.FORBIDDEN_OPERATIONS`, `RULES`;
  - `dce.frozen.load_contract_at(path: Path) -> DataContract` (cached).

- [ ] **Step 1: Write the failing tests**

`tests/test_compile_emit.py`:

```python
from pathlib import Path

from agentic_data_contracts import DataContract
from dce.benchmarks import livesqlbench
from dce.compile.emit import build, render_compiled, write
from dce.compile.session import NOTE_MARKER, EntryRecord

TABLES = {
    "customers": [("custid", "BIGINT"), ("tier", "VARCHAR")],
    "orders": [("orderid", "BIGINT"), ("amount", "DOUBLE"), ("status", "VARCHAR")],
}


def _r(entry_id, kind, name, domain, status, metrics=(), prose=()):
    return EntryRecord(
        entry_id, kind, name, domain, status, list(metrics), list(prose), "", 1, None
    )


def records():
    return [
        _r(
            3,
            "calculation",
            "Shipped Revenue",
            "calculations",
            "metric",
            [
                {
                    "name": "shipped_revenue",
                    "description": f"Revenue.\n\n{NOTE_MARKER}Lower-case statuses.",
                    "sql_expression": "SUM(amount)",
                    "source_model": "main.orders",
                }
            ],
        ),
        _r(
            4,
            "calculation",
            "Basket Size",
            "calculations",
            "untranslated",
            [
                {
                    "name": "basket_size",
                    "description": "Items per order.",
                    "sql_expression": "",
                    "source_model": "",
                    "untranslated": "bad SQL",
                }
            ],
        ),
        _r(
            2,
            "predicate",
            "Shipped Order",
            "definitions",
            "metric",
            [
                {
                    "name": "shipped_order",
                    "description": "Shipped.",
                    "sql_expression": "status = 'shipped'",
                    "source_model": "main.orders",
                }
            ],
        ),
        _r(
            1,
            "illustration",
            "Customer Tier",
            "reference",
            "prose",
            prose=[
                {
                    "target": "column",
                    "table": "customers",
                    "column": "tier",
                    "text": f"Customer Tier: gold or silver.\n\n{NOTE_MARKER}Two values.",
                }
            ],
        ),
        _r(
            6,
            "illustration",
            "Order Status",
            "reference",
            "prose",
            prose=[
                {
                    "target": "domain",
                    "table": None,
                    "column": None,
                    "text": "Order Status: pending, shipped, returned.",
                }
            ],
        ),
    ]


def test_build_and_write_a_contract_the_library_loads(lsb_data, tmp_path: Path):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    contract, semantic = build(inputs, records(), TABLES)
    # In entry-id order of first use. Entry 1's illustration lands on a
    # column, so `reference` first appears with entry 6.
    assert [d["name"] for d in contract["semantic"]["domains"]] == [
        "definitions",
        "calculations",
        "reference",
    ]
    assert [m["name"] for m in semantic["metrics"]] == [
        "shipped_order",
        "shipped_revenue",
        "basket_size",
    ]
    assert semantic["metrics"][2]["untranslated"] == "bad SQL"
    tier = next(
        c
        for t in semantic["tables"]
        if t["table"] == "customers"
        for c in t["columns"]
        if c["name"] == "tier"
    )
    assert tier["description"].startswith(
        "Loyalty tier.\n\nCustomer Tier: gold or silver."
    )
    out = tmp_path / "c"
    write(out, contract, semantic, inputs)
    head = (out / "contract.yml").read_text(encoding="utf-8").splitlines()[0]
    assert head.startswith("# Compiled by dce.compile")
    loaded = DataContract.from_yaml(out / "contract.yml")
    assert loaded.name == "livesqlbench-shop"
    prompt = loaded.to_system_prompt()
    assert "calculations" in prompt and "main.orders" in prompt
    reference = next(
        d for d in contract["semantic"]["domains"] if d["name"] == "reference"
    )
    assert "Order Status: pending, shipped, returned." in reference["description"]


def test_render_compiled_lists_translated_metrics_and_every_note(
    lsb_data, tmp_path: Path
):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    out = tmp_path / "c"
    write(out, *build(inputs, records(), TABLES), inputs)
    text = render_compiled(out)
    assert text.startswith(
        "### shipped_order\n\nOver `main.orders`:\n\n```sql\nstatus = 'shipped'\n```"
    )
    assert (
        "### shipped_revenue" in text and f"{NOTE_MARKER}Lower-case statuses." in text
    )
    assert "basket_size" not in text  # untranslated: no SQL to show
    assert "- customers.tier: Two values." in text
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_compile_emit.py 2>&1 | tail -3`
Expected: FAIL, `ModuleNotFoundError: No module named 'dce.compile.emit'`.

- [ ] **Step 3: Implement**

`dce/frozen.py`, after the existing loaders:

```python
@lru_cache(maxsize=None)
def load_contract_at(path: Path) -> DataContract:
    """A compiled contract, by path (`contracts/<benchmark>/...`)."""
    return DataContract.from_yaml(path)
```

`dce/compile/emit.py`:

```python
"""A compiled contract from its compile records: `contract.yml` and
`semantic.yml`, built from the records alone so a log replays to the same
files. Domains appear in entry-id order of first use; metrics in entry-id
order; tables and columns as DuckDB lists them, each column's description
its meaning plus any illustration placed on it."""

from __future__ import annotations

from pathlib import Path

import yaml

from dce.compile.inputs import CompileInputs
from dce.compile.session import COMPILER_MODEL, NOTE_MARKER, EntryRecord

#: As the hand-authored DABStep contract forbids.
FORBIDDEN_OPERATIONS = [
    "DELETE",
    "DROP",
    "TRUNCATE",
    "UPDATE",
    "INSERT",
    "CREATE",
    "ALTER",
    "MERGE",
    "GRANT",
    "REVOKE",
    "COPY",
]

#: The hand-authored contract's two rules, worded for any benchmark.
RULES = [
    {
        "name": "no_select_star",
        "description": "Queries must name their columns; use preview_table to explore",
        "enforcement": "block",
        "query_check": {"no_select_star": True},
    },
    {
        "name": "use_declared_definitions",
        "description": (
            "Computations must follow the metric definitions in the semantic "
            "source rather than an ad-hoc formula."
        ),
        "enforcement": "warn",
    },
]

KIND_DOMAINS: dict[str, tuple[str, str]] = {
    "calculations": (
        "Computed measures defined in the knowledge base",
        "Calculations from the knowledge base, each a metric. Look a metric up "
        "by name for its definition and SQL.",
    ),
    "definitions": (
        "Business conditions defined in the knowledge base",
        "Definitions from the knowledge base, each a metric whose SQL is a "
        "boolean condition over the rows of its table.",
    ),
    "reference": (
        "Value explanations that belong to no single table or column",
        "Explanations of values from the knowledge base.",
    ),
}


def _join(*parts: str) -> str:
    return "\n\n".join(p for p in parts if p)


def build(
    inputs: CompileInputs,
    records: list[EntryRecord],
    tables: dict[str, list[tuple[str, str]]],
) -> tuple[dict, dict]:
    records = sorted(records, key=lambda r: r.entry_id)
    domains: dict[str, dict] = {}
    table_desc = {t.lower(): d for t, d in inputs.table_descriptions.items()}
    column_desc = dict(inputs.column_descriptions)
    metrics = []

    def domain(record: EntryRecord) -> dict:
        if record.domain not in domains:
            summary, description = KIND_DOMAINS.get(record.domain, (record.name, ""))
            domains[record.domain] = {
                "name": record.domain,
                "summary": summary,
                "description": description,
            }
        return domains[record.domain]

    for record in records:
        for metric in record.metrics:
            domain(record)
            out = {
                k: metric[k]
                for k in ("name", "description", "sql_expression", "source_model")
            }
            out["domains"] = [record.domain]
            if metric.get("untranslated"):
                out["untranslated"] = metric["untranslated"]
            metrics.append(out)
        for prose in record.prose:
            if prose["target"] == "column":
                key = (prose["table"].lower(), prose["column"].lower())
                column_desc[key] = _join(column_desc.get(key, ""), prose["text"])
            elif prose["target"] == "table":
                key = prose["table"].lower()
                table_desc[key] = _join(table_desc.get(key, ""), prose["text"])
            else:
                d = domain(record)
                d["description"] = _join(d["description"], prose["text"])

    contract = {
        "version": "1.0",
        "name": f"{inputs.benchmark}-{inputs.db_name}",
        "semantic": {
            "source": {"type": "yaml", "path": "./semantic.yml", "expected_extras": []},
            "allowed_tables": [
                {"schema": "main", "preferred": True, "tables": sorted(tables)}
            ],
            "forbidden_operations": FORBIDDEN_OPERATIONS,
            "domains": list(domains.values()),
            "rules": RULES,
        },
    }
    semantic_tables = []
    for table in sorted(tables):
        entry: dict = {"schema": "main", "table": table}
        if table_desc.get(table.lower()):
            entry["description"] = table_desc[table.lower()]
        entry["columns"] = [
            {
                "name": column,
                "type": data_type,
                "description": column_desc.get((table.lower(), column.lower()), ""),
            }
            for column, data_type in tables[table]
        ]
        semantic_tables.append(entry)
    return contract, {"metrics": metrics, "tables": semantic_tables}


HEADER = (
    "# Compiled by dce.compile -- do not edit; recompile instead.\n"
    "# benchmark: {benchmark}, database: {db}, compiler: {model}\n"
    "# inputs: {inputs}\n"
)


def write(out_dir: Path, contract: dict, semantic: dict, inputs: CompileInputs) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    header = HEADER.format(
        benchmark=inputs.benchmark,
        db=inputs.db_name,
        model=COMPILER_MODEL,
        inputs=", ".join(p.name for p in inputs.sources),
    )
    for name, data in (("contract.yml", contract), ("semantic.yml", semantic)):
        body = yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=100)
        (out_dir / name).write_text(header + body, encoding="utf-8")


def _notes(text: str | None) -> list[str]:
    return [
        p[len(NOTE_MARKER) :].strip()
        for p in (text or "").split("\n\n")
        if p.startswith(NOTE_MARKER)
    ]


def render_compiled(contract_dir: Path) -> str:
    """`manual_compiled`'s appendix: each translated metric's name, table and
    SQL exactly as the contract holds them, then every compiler note on a
    table, column or domain."""
    semantic = yaml.safe_load(
        (contract_dir / "semantic.yml").read_text(encoding="utf-8")
    )
    contract = yaml.safe_load(
        (contract_dir / "contract.yml").read_text(encoding="utf-8")
    )
    blocks = []
    for metric in semantic.get("metrics") or []:
        if not metric.get("sql_expression"):
            continue
        block = (
            f"### {metric['name']}\n\nOver `{metric['source_model']}`:\n\n"
            f"```sql\n{metric['sql_expression']}\n```"
        )
        for note in _notes(metric.get("description")):
            block += f"\n\n{NOTE_MARKER}{note}"
        blocks.append(block)
    notes = []
    for table in semantic.get("tables") or []:
        notes += [f"- {table['table']}: {n}" for n in _notes(table.get("description"))]
        for column in table.get("columns") or []:
            notes += [
                f"- {table['table']}.{column['name']}: {n}"
                for n in _notes(column.get("description"))
            ]
    for d in contract["semantic"].get("domains") or []:
        notes += [f"- {d['name']}: {n}" for n in _notes(d.get("description"))]
    if notes:
        blocks.append("### Notes on the data\n\n" + "\n".join(notes))
    return "\n\n".join(blocks)
```

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_compile_emit.py tests/test_frozen.py 2>&1 | tail -1`
Expected: all pass. If `DataContract.from_yaml` rejects a field, read the library's error and fix the emitted shape, not the library. A domain with an empty `description` is one candidate. Record any such change as a ruling.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/compile/emit.py dce/frozen.py tests/test_compile_emit.py
git commit -m "dce.compile: contract.yml and semantic.yml from the compile records; manual_compiled's appendix"
```

---

### Task 8: Running a compile: log, resume, spend cap, the real driver, the command

**Files:**
- Create: `dce/compile/run.py`, `dce/compile/__main__.py`
- Test: `tests/test_compile_run.py`

**Interfaces:**
- Consumes: everything from Tasks 3 to 7; `dce.agent._default_agent_factory`, `dce.agent._commit_sha`; `dce.pricing.cost`.
- Produces, in `dce.compile.run`:
  - `LOG = "compile-log.jsonl"`, `FINAL_LOG = "compile-log.jsonl.gz"`, `COMPILE_REQUEST_LIMIT = 25`;
  - `read_log(path: Path) -> tuple[dict | None, list[EntryRecord], bool]`, returning `(header, records, torn)`;
  - `log_spend(directory: Path) -> float`, the `usd` of every record under `directory`;
  - `compile_database(inputs, out_dir, driver, *, spend_cap: float, spent: float = 0.0) -> tuple[str, float]`. The status is `"done"`, `"skipped"` or `"capped"`, and the float is the running spend;
  - `claude_driver(*, system_prompt, user_prompt, tools) -> DriverResult`.
- Produces `python -m dce.compile`:
  - `--benchmark {dabstep,livesqlbench}`;
  - `--db NAME [NAME ...] | --all`, for livesqlbench;
  - `--lsb-data PATH`;
  - `--out PATH`, default `contracts`;
  - `--max-spend USD`, required.

- [ ] **Step 1: Write the failing tests**

`tests/test_compile_run.py`:

```python
import gzip
import json
import sys
from pathlib import Path

import duckdb
import pytest
from agentic_data_contracts import DataContract
from dce.benchmarks import dabstep, livesqlbench
from dce.compile import run as run_module
from dce.compile.run import FINAL_LOG, LOG, compile_database, log_spend, read_log
from dce.compile.session import DriverResult

USAGE = {"input_tokens": 10, "output_tokens": 5, "cached_tokens": 0, "usd": 0.25}

# The shop fixture's six entries, as a model that knows the answers would
# submit them.
ANSWERS = {
    "Customer Tier": {"target": "customers.tier"},
    "Shipped Order": {"sql_expression": "status = 'shipped'", "source_model": "orders"},
    "Shipped Revenue": {
        "sql_expression": "SUM(CASE WHEN status = 'shipped' THEN amount END)",
        "source_model": "orders",
    },
    "Basket Size": {"sql_expression": "SUM(qty) / COUNT(*)", "source_model": "orders"},
    "Gold Customer": {"sql_expression": "tier = 'gold'", "source_model": "customers"},
    "Order Status": {"target": "orders.status"},
}


def shop_driver(calls: list):
    def driver(*, system_prompt, user_prompt, tools):
        name = user_prompt.split("## Entry: ", 1)[1].split("\n", 1)[0]
        calls.append(name)
        submit = next(t for t in tools if t.__name__ == "submit_entry")
        submit(**ANSWERS[name])
        return DriverResult(messages=[{"entry": name}], usage=dict(USAGE))

    return driver


def test_a_database_compiles_to_a_loadable_contract_and_a_gzipped_log(
    lsb_data, tmp_path
):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    out = tmp_path / "contracts" / inputs.out_subdir
    calls: list = []
    status, spent = compile_database(inputs, out, shop_driver(calls), spend_cap=10.0)
    assert status == "done" and spent == pytest.approx(1.5)
    assert calls == [
        "Customer Tier",
        "Shipped Order",
        "Shipped Revenue",
        "Basket Size",
        "Gold Customer",
        "Order Status",
    ]
    assert not (out / LOG).exists() and (out / FINAL_LOG).exists()
    lines = gzip.decompress((out / FINAL_LOG).read_bytes()).decode("utf-8").splitlines()
    header = json.loads(lines[0])
    assert header["type"] == "header" and header["model"] == "claudesonnet5"
    assert set(header["inputs"]) == {
        "shop_kb.jsonl",
        "shop_column_meaning_base.json",
        "shop_schema.txt",
        "shop.duckdb",
    }
    contract = DataContract.from_yaml(out / "contract.yml")
    assert contract.name == "livesqlbench-shop"
    assert log_spend(tmp_path / "contracts") == pytest.approx(1.5)
    # A finished database is skipped, and nothing is called.
    again: list = []
    assert (
        compile_database(inputs, out, shop_driver(again), spend_cap=10.0)[0]
        == "skipped"
    )
    assert again == []


def test_a_capped_run_resumes_where_it_stopped_without_rebilling(lsb_data, tmp_path):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    out = tmp_path / "c"
    first: list = []
    status, spent = compile_database(inputs, out, shop_driver(first), spend_cap=0.5)
    assert status == "capped" and first == ["Customer Tier", "Shipped Order"]
    # A torn last line, as a crash mid-write leaves it, is dropped.
    with (out / LOG).open("a", encoding="utf-8") as fh:
        fh.write('{"type": "entry", "entry_id": 3, "kin')
    header, records, torn = read_log(out / LOG)
    assert torn and [r.entry_id for r in records] == [1, 2]
    second: list = []
    status, spent = compile_database(
        inputs, out, shop_driver(second), spend_cap=10.0, spent=spent
    )
    assert status == "done"
    assert second == ["Shipped Revenue", "Basket Size", "Gold Customer", "Order Status"]
    assert spent == pytest.approx(1.5)


def test_a_resume_with_changed_inputs_is_refused(lsb_data, tmp_path):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    out = tmp_path / "c"
    compile_database(inputs, out, shop_driver([]), spend_cap=0.3)
    kb = lsb_data / "hf-full-v1" / "shop" / "shop_kb.jsonl"
    kb.write_text(
        kb.read_text(encoding="utf-8").replace("loyalty", "LOYALTY"), encoding="utf-8"
    )
    changed = livesqlbench.compile_inputs(lsb_data, "shop")
    with pytest.raises(SystemExit, match="inputs changed"):
        compile_database(changed, out, shop_driver([]), spend_cap=10.0)


_OPENED: list[str] | None = None


def _audit(event, args):
    if (
        event == "open"
        and _OPENED is not None
        and isinstance(args[0], (str, bytes, Path))
    ):
        _OPENED.append(str(args[0]))


sys.addaudithook(_audit)


def test_the_compiler_opens_no_task_gold_or_test_case_file(
    lsb_data, tmp_path, monkeypatch
):
    global _OPENED
    monkeypatch.setattr(run_module, "claude_driver", shop_driver([]))
    forbidden = {
        str(lsb_data / "gold_results_full_v1.json"),
        str(lsb_data / "tasks_frozen_full_v1.json"),
        str(lsb_data / "gold_sql.json"),
        str(lsb_data / "hf-full-v1" / "livesqlbench_data.jsonl"),
    }
    _OPENED = []
    try:
        from dce.compile.__main__ import main

        main(
            [
                "--benchmark",
                "livesqlbench",
                "--db",
                "shop",
                "--lsb-data",
                str(lsb_data),
                "--out",
                str(tmp_path / "out"),
                "--max-spend",
                "10",
            ]
        )
        opened = set(_OPENED)
    finally:
        _OPENED = None
    assert (tmp_path / "out" / "livesqlbench" / "shop" / "contract.yml").exists()
    assert not opened & forbidden, opened & forbidden
    assert any(p.endswith("shop_kb.jsonl") for p in opened)  # the hook does see opens


def test_dabstep_compiles_without_opening_tasks_or_golds(tmp_path):
    global _OPENED
    context = tmp_path / "context"
    context.mkdir()
    (context / "manual.md").write_text(
        "# Guide\n\n## 1. Fees\nfee = amount * 2\n", encoding="utf-8"
    )
    (context / "payments-readme.md").write_text("amount: euros.", encoding="utf-8")
    db = tmp_path / "d.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE payments AS SELECT 1.5 AS amount")
    con.close()
    for name in ("tasks.json", "golds.json"):
        (tmp_path / name).write_text("{}", encoding="utf-8")

    def driver(*, system_prompt, user_prompt, tools):
        submit = next(t for t in tools if t.__name__ == "submit_entry")
        metrics = []
        if "## 1. Fees" in user_prompt:
            metrics = [
                {
                    "name": "fee",
                    "kind": "calculation",
                    "description": "The fee.",
                    "sql_expression": "amount * 2",
                    "source_model": "payments",
                }
            ]
        submit(metrics=metrics)
        return DriverResult(messages=[], usage=dict(USAGE))

    _OPENED = []
    try:
        inputs = dabstep.compile_inputs(context, db)
        status, _ = compile_database(inputs, tmp_path / "out", driver, spend_cap=10.0)
        opened = set(_OPENED)
    finally:
        _OPENED = None
    assert status == "done"
    assert not opened & {str(tmp_path / "tasks.json"), str(tmp_path / "golds.json")}
    contract = DataContract.from_yaml(tmp_path / "out" / "contract.yml")
    assert "1_fees" in contract.to_system_prompt()
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_compile_run.py 2>&1 | tail -3`
Expected: FAIL, `ModuleNotFoundError: No module named 'dce.compile.run'`.

- [ ] **Step 3: Implement**

`dce/compile/run.py`:

```python
"""Compile one database: resumable, logged, capped.

Every finished entry is appended to `compile-log.jsonl` (flushed and
fsynced) before the next starts, so a crash costs only the entry in
progress. A rerun reads the log, drops a torn last line, refuses if the
inputs' hashes changed, and continues. When every entry is done the
contract is written and the log gzipped to `compile-log.jsonl.gz`; a
database with that file and no plain log is finished and skipped.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path

from dce.compile.db import CompileDB
from dce.compile.emit import build, write
from dce.compile.entries import compile_order
from dce.compile.inputs import CompileInputs
from dce.compile.session import (
    COMPILER_MODEL,
    MAX_ATTEMPTS,
    DriverResult,
    EntryRecord,
    compile_entry,
)

LOG = "compile-log.jsonl"
FINAL_LOG = "compile-log.jsonl.gz"
#: Model requests per entry. An entry takes a handful (a few queries, one
#: or two submissions); the cap stops a model that loops.
COMPILE_REQUEST_LIMIT = 25


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _header(inputs: CompileInputs) -> dict:
    from dce.agent import _commit_sha

    return {
        "type": "header",
        "benchmark": inputs.benchmark,
        "db": inputs.db_name,
        "model": COMPILER_MODEL,
        "commit_sha": _commit_sha(),
        "inputs": {p.name: _sha256(p) for p in inputs.sources},
    }


def read_log(path: Path) -> tuple[dict | None, list[EntryRecord], bool]:
    """(header, records, torn). Only the LAST line may be torn -- it is the
    one a crash interrupts; a bad line anywhere else is corruption."""
    lines = path.read_text(encoding="utf-8").splitlines()
    header, records, torn = None, [], False
    for i, line in enumerate(lines):
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            if i == len(lines) - 1:
                torn = True
                break
            raise SystemExit(f"{path}: line {i + 1} is not JSON") from None
        if data.get("type") == "header":
            header = data
        else:
            records.append(EntryRecord.from_dict(data))
    return header, records, torn


def _gz_records(path: Path) -> list[dict]:
    lines = gzip.decompress(path.read_bytes()).decode("utf-8").splitlines()
    return [json.loads(line) for line in lines]


def log_spend(directory: Path) -> float:
    """What the logs under `directory` record as spent, finished or not."""
    total = 0.0
    for path in directory.rglob(LOG):
        total += sum(r.usage.get("usd") or 0.0 for r in read_log(path)[1])
    for path in directory.rglob(FINAL_LOG):
        total += sum(
            (r.get("usage") or {}).get("usd") or 0.0
            for r in _gz_records(path)
            if r.get("type") == "entry"
        )
    return total


def _append(path: Path, data: dict) -> None:
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(data, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def compile_database(
    inputs: CompileInputs,
    out_dir: Path,
    driver,
    *,
    spend_cap: float,
    spent: float = 0.0,
) -> tuple[str, float]:
    log, final = out_dir / LOG, out_dir / FINAL_LOG
    if final.exists() and not log.exists():
        return "skipped", spent
    out_dir.mkdir(parents=True, exist_ok=True)
    header = _header(inputs)
    records: list[EntryRecord] = []
    if log.exists():
        old, records, torn = read_log(log)
        if old is None or old["inputs"] != header["inputs"]:
            raise SystemExit(
                f"{log}: inputs changed since this compile started; delete the "
                "log to start over"
            )
        if torn:
            # Rewrite without the torn line, so the next append starts clean.
            log.write_text(
                "".join(
                    json.dumps(d, ensure_ascii=False) + "\n"
                    for d in [old] + [r.to_dict() for r in records]
                ),
                encoding="utf-8",
            )
    else:
        _append(log, header)
    prior = {r.entry_id: r for r in records}
    db = CompileDB(inputs.db_path, init_sql=inputs.init_sql)
    try:
        tables = db.tables()
        for entry in compile_order(inputs.entries):
            if entry.id in prior:
                continue
            if spent >= spend_cap:
                return "capped", spent
            record = compile_entry(
                entry, inputs, db, driver, prior=prior, tables=tables
            )
            _append(log, record.to_dict())
            prior[entry.id] = record
            spent += record.usage.get("usd") or 0.0
    finally:
        db.close()
    write(out_dir, *build(inputs, list(prior.values()), tables), inputs)
    final.write_bytes(gzip.compress(log.read_bytes(), mtime=0))
    log.unlink()
    return "done", spent


def claude_driver(*, system_prompt: str, user_prompt: str, tools) -> DriverResult:
    """The compiler model's loop, through the harness's own agent factory
    (route, reasoning effort and caching as for every agent run). Spends
    money; tests replace it."""
    from pydantic_ai import Tool
    from pydantic_ai.exceptions import UsageLimitExceeded
    from pydantic_ai.messages import ModelMessagesTypeAdapter
    from pydantic_ai.usage import UsageLimits

    from dce.agent import _default_agent_factory
    from dce.pricing import cost

    agent = _default_agent_factory(
        model=COMPILER_MODEL,
        system_prompt=system_prompt,
        tools=[Tool(t) for t in tools],
        retries=MAX_ATTEMPTS + 2,
    )
    try:
        result = agent.run_sync(
            user_prompt, usage_limits=UsageLimits(request_limit=COMPILE_REQUEST_LIMIT)
        )
    except UsageLimitExceeded as exc:
        # The entry keeps whatever it submitted before the cap; its cost is
        # unknown here, so it is recorded as such rather than as zero.
        return DriverResult(messages=[], usage={"usd": None, "error": str(exc)})
    usage = result.usage()
    in_tok = usage.input_tokens or 0
    out_tok = usage.output_tokens or 0
    cached = getattr(usage, "cache_read_tokens", 0) or 0
    return DriverResult(
        messages=ModelMessagesTypeAdapter.dump_python(
            result.all_messages(), mode="json"
        ),
        usage={
            "input_tokens": in_tok,
            "output_tokens": out_tok,
            "cached_tokens": cached,
            "usd": cost(COMPILER_MODEL, in_tok, out_tok, cached),
        },
    )
```

`dce/compile/__main__.py`:

```python
"""python -m dce.compile -- compile a benchmark's KB into frozen contracts.

    uv run python -m dce.compile --benchmark livesqlbench --db households --max-spend 5
    uv run python -m dce.compile --benchmark livesqlbench --all --max-spend 60
    uv run python -m dce.compile --benchmark dabstep --max-spend 10

Output goes to `<out>/<benchmark>/[<db>/]`. `--max-spend` caps the spend
recorded across every log under `<out>/<benchmark>/`, resumes included.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from dce.compile import run as run_module


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="dce.compile")
    parser.add_argument(
        "--benchmark", choices=("dabstep", "livesqlbench"), required=True
    )
    parser.add_argument(
        "--db", nargs="+", default=None, help="(livesqlbench) databases"
    )
    parser.add_argument(
        "--all", action="store_true", help="(livesqlbench) every database"
    )
    parser.add_argument(
        "--lsb-data",
        type=Path,
        default=Path(os.environ.get("LSB_DATA", Path.home() / "data" / "livesqlbench")),
    )
    parser.add_argument("--out", type=Path, default=Path("contracts"))
    parser.add_argument("--max-spend", type=float, required=True)
    args = parser.parse_args(argv)

    if args.benchmark == "livesqlbench":
        from dce.benchmarks.livesqlbench import compile_inputs

        if args.all == bool(args.db):
            raise SystemExit("name databases with --db, or pass --all")
        names = args.db or sorted(
            p.name
            for p in (args.lsb_data / "hf-full-v1").iterdir()
            if (args.lsb_data / "duckdb" / f"{p.name}.duckdb").is_file()
        )
        work = [compile_inputs(args.lsb_data, name) for name in names]
    else:
        from dce.benchmarks.dabstep import compile_inputs as dabstep_inputs

        work = [dabstep_inputs()]

    spent = run_module.log_spend(args.out / args.benchmark)
    for inputs in work:
        status, spent = run_module.compile_database(
            inputs,
            args.out / inputs.out_subdir,
            run_module.claude_driver,
            spend_cap=args.max_spend,
            spent=spent,
        )
        print(
            f"{inputs.db_name}: {status} (spent ${spent:.2f} of ${args.max_spend:.2f})"
        )
        if status == "capped":
            raise SystemExit(2)


if __name__ == "__main__":
    main()
```

`run_module.claude_driver` is looked up at call time, so the file-access test's monkeypatch reaches it.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_compile_run.py 2>&1 | tail -1`
Expected: all pass.

Then check the command's surface without spending:
`UV_FROZEN=1 uv run python -m dce.compile --help 2>/dev/null | grep -E "benchmark|max-spend"`. Expected: both appear.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/compile/run.py dce/compile/__main__.py tests/test_compile_run.py
git commit -m "dce.compile: resumable, logged, capped compiles; the compiler model's driver; python -m dce.compile"
```

---

### Task 9: `check_arms` on the protocol, called before any spend

**Files:**
- Modify: `dce/benchmark.py` (protocol)
- Modify: `dce/runner.py` (`_sweep_tasks`, called from `main`)
- Modify: `dce/benchmarks/dabstep.py` (a no-op `check_arms` for now; Task 11 fills it)
- Test: `tests/test_runner.py`

**Interfaces:**
- Produces:
  - `Benchmark.check_arms(self, arms: tuple[str, ...], tasks: list[Task]) -> None`. It raises `SystemExit` naming what is missing.
  - `dce.runner._sweep_tasks(benchmark, arms: tuple[str, ...], n: int) -> list[Task]`. It runs the scorer preflight, loads the tasks, samples them, then calls `check_arms`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_runner.py`:

```python
def test_sweep_tasks_checks_the_arms_on_the_sampled_tasks_before_any_spend():
    from dce.runner import _sweep_tasks

    seen = {}

    class Bench(DABStep):
        def check_arms(self, arms, tasks):
            seen["arms"], seen["tasks"] = arms, [t.task_id for t in tasks]
            raise SystemExit("no compiled contract for ['x']")

    records = [{"task_id": str(i), "question": "q", "level": "hard"} for i in range(5)]
    bench = Bench(db=Path("d"), golds={}, golds_hash="h", docs={}, records=records)
    with pytest.raises(SystemExit, match="no compiled contract"):
        _sweep_tasks(bench, ("contract",), 2)
    assert seen == {"arms": ("contract",), "tasks": ["0", "1"]}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_runner.py -k sweep_tasks_checks 2>&1 | tail -3`
Expected: FAIL, `ImportError: cannot import name '_sweep_tasks'`.

- [ ] **Step 3: Implement**

`dce/benchmark.py`: add to the protocol's methods

```python
    def check_arms(self, arms: tuple[str, ...], tasks: list[Task]) -> None: ...
```

and one sentence to the module docstring: "`check_arms` runs before any spend, so an arm whose artifact is missing (a contract not yet compiled) stops the sweep by name rather than failing every task it runs on."

`dce/benchmarks/dabstep.py`, a method on `DABStep`:

```python
    def check_arms(self, arms: tuple[str, ...], tasks: list[Task]) -> None:
        # Every DABStep arm's artifact is committed; see Task 11.
        return None
```

`dce/runner.py`, beside `_stratified_sample`:

```python
def _sweep_tasks(benchmark: Benchmark, arms: tuple[str, ...], n: int) -> list[Task]:
    """The tasks a sweep runs, checked before any spend."""
    # Resolve the scorer now: it is imported lazily, and a missing one found
    # mid-sweep turns every task into a `construction_error` row until the
    # circuit breaker trips, instead of one clear error before any spend.
    benchmark.scorer()
    tasks = benchmark.tasks()
    if n:
        tasks = _stratified_sample(tasks, n)
    # The same for an arm's artifact -- a contract not yet compiled.
    benchmark.check_arms(arms, tasks)
    return tasks
```

In `main()`, replace the scorer comment, `benchmark.scorer()`, `tasks = benchmark.tasks()` and the `if args.n:` sampling with `tasks = _sweep_tasks(benchmark, arms, args.n)`.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest -q --deselect tests/test_replay.py::test_sql_statements_are_ordered_and_only_from_sql_tools 2>&1 | tail -1`
Expected: all pass. `LiveSQLBench` has no `check_arms` yet, so `isinstance(bench, Benchmark)` in `tests/test_livesqlbench.py` fails until Task 10. If it does, add the same no-op method to `LiveSQLBench` in this task, and let Task 10 replace it.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/benchmark.py dce/runner.py dce/benchmarks/dabstep.py dce/benchmarks/livesqlbench.py tests/test_runner.py
git commit -m "dce: Benchmark.check_arms, run on the sampled tasks before any spend"
```

---

### Task 10: LiveSQLBench's `manual_compiled` and `contract` arms

**Files:**
- Modify: `dce/benchmarks/livesqlbench.py`
- Test: `tests/test_livesqlbench.py`

**Interfaces:**
- Consumes:
  - `governed_arm` (Task 2);
  - `render_compiled` (Task 7);
  - `load_contract_at` (Task 7);
  - `compile_database` and the `shop_driver` pattern (Task 8);
  - `contract_digest` (library).
- Produces:
  - `LiveSQLBench.arms = ("schema_only", "manual_prompt", "manual_compiled", "contract")`;
  - `governed_arms = frozenset({"contract"})`;
  - `roles = {"baseline": "schema_only", "manual": "manual_prompt", "manual_plus": "manual_compiled", "contract": "contract"}`;
  - `DEFAULT_CONTRACTS`;
  - `LiveSQLBench(..., contracts: Path = DEFAULT_CONTRACTS)` and `LiveSQLBench.from_data(data, contracts=DEFAULT_CONTRACTS)`;
  - `check_arms`.

- [ ] **Step 1: Write the failing tests**

In `tests/test_livesqlbench.py`, change `test_class_attributes` to expect the four arms, `governed_arms == frozenset({"contract"})`, and the four roles above.

Then append:

```python
def _compiled(lsb_data, tmp_path):
    """The shop fixture compiled with a scripted model, as Task 8's test does."""
    from dce.compile.run import compile_database
    from tests.test_compile_run import shop_driver

    contracts = tmp_path / "contracts"
    inputs = livesqlbench_module.compile_inputs(lsb_data, "shop")
    compile_database(inputs, contracts / "shop", shop_driver([]), spend_cap=10.0)
    return LiveSQLBench.from_data(lsb_data, contracts=contracts), contracts


def test_manual_compiled_is_the_manual_then_the_compiled_definitions(
    lsb_data, tmp_path
):
    from dce.compile.emit import render_compiled

    bench, contracts = _compiled(lsb_data, tmp_path)
    task = _task(bench, "shop_1")
    db = lsb_data / "duckdb" / "shop.duckdb"
    manual = bench.build_arm("manual_prompt", task, db)
    compiled = bench.build_arm("manual_compiled", task, db)
    assert compiled.system_prompt == (
        manual.system_prompt
        + "\n\n## Compiled definitions\n\n"
        + render_compiled(contracts / "shop")
    )
    assert "SUM(CASE WHEN status = 'shipped' THEN amount END)" in compiled.system_prompt
    assert [t.name for t in compiled.tools] == [t.name for t in manual.tools]


def test_the_contract_arm_is_context_procedure_contract_over_governed_tools(
    lsb_data, tmp_path
):
    from dce.frozen import load_contract_at
    from dce.tools import CONTRACT_PROCEDURE

    bench, contracts = _compiled(lsb_data, tmp_path)
    task = _task(bench, "shop_4")
    db = lsb_data / "duckdb" / "shop.duckdb"
    schema_only = bench.build_arm("schema_only", task, db)
    setup = bench.build_arm("contract", task, db)
    try:
        contract = load_contract_at(contracts / "shop" / "contract.yml")
        assert setup.system_prompt == (
            f"{schema_only.system_prompt}\n\n{CONTRACT_PROCEDURE}\n\n"
            f"{contract.to_system_prompt()}"
        )
        assert "## Knowledge base" not in setup.system_prompt
        names = [t.name for t in setup.tools]
        assert "lookup_metric" in names and "run_query" in names
        # PG_COMPAT reaches the governed connection too.
        result = setup.adapter.execute_limited("SELECT 7 / 2 AS x", 5)
        assert [tuple(r) for r in result.rows] == [(3,)]
    finally:
        setup.close()


def test_compiled_arms_digest_the_contract(lsb_data, tmp_path):
    bench, _ = _compiled(lsb_data, tmp_path)
    task = _task(bench, "shop_1")
    digests = {arm: bench.arm_digest(arm, task) for arm in LiveSQLBench.arms}
    assert len(set(digests.values())) == 4


def test_check_arms_names_databases_without_a_compiled_contract(lsb_data, tmp_path):
    bench = LiveSQLBench.from_data(lsb_data, contracts=tmp_path / "none")
    tasks = bench.tasks()
    bench.check_arms(("schema_only", "manual_prompt"), tasks)  # nothing compiled needed
    with pytest.raises(SystemExit, match=r"\['shop'\]"):
        bench.check_arms(("contract",), tasks)
    broken = tmp_path / "broken" / "shop"
    broken.mkdir(parents=True)
    (broken / "contract.yml").write_text("name: [", encoding="utf-8")
    bench = LiveSQLBench.from_data(lsb_data, contracts=tmp_path / "broken")
    with pytest.raises(SystemExit, match="does not load"):
        bench.check_arms(("manual_compiled",), tasks)
```

At the top of `tests/test_livesqlbench.py`, add `import dce.benchmarks.livesqlbench as livesqlbench_module`.

The test file's `data` fixture and `conftest.py`'s `lsb_data` build the same layout. The new tests use `lsb_data`.

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_livesqlbench.py 2>&1 | tail -6`
Expected: `test_class_attributes` and the four new tests FAIL (`contracts=` is an unexpected keyword; the arms are unknown).

- [ ] **Step 3: Implement**

`dce/benchmarks/livesqlbench.py`:
- Import `governed_arm` from `dce.tools`, `render_compiled` from `dce.compile.emit`, `load_contract_at` from `dce.frozen`, and `contract_digest` from `agentic_data_contracts`.
- Module docstring: replace "until the compiler lands (PR C) the arms are `schema_only` and `manual_prompt`" with "the arms are `schema_only`, `manual_prompt`, `manual_compiled` (the manual plus the compiled definitions) and `contract` (the compiled contract behind governed tools), the last two loading `contracts/livesqlbench/<db>/`".
- Add:

```python
#: Where the compile commit puts the frozen contracts (spec, Layout).
DEFAULT_CONTRACTS = Path(__file__).resolve().parents[2] / "contracts" / "livesqlbench"
```

- Class attributes:

```python
    arms: tuple[str, ...] = ("schema_only", "manual_prompt", "manual_compiled", "contract")
    governed_arms: frozenset[str] = frozenset({"contract"})
    roles = {
        "baseline": "schema_only",
        "manual": "manual_prompt",
        "manual_plus": "manual_compiled",
        "contract": "contract",
    }
```

- Thread `contracts: Path = DEFAULT_CONTRACTS` through `__init__` (stored as `self._contracts`) and through `from_data(cls, data, contracts=DEFAULT_CONTRACTS)`.
- Add:

```python
def _contract_dir(self, db: str) -> Path:
    return self._contracts / db


def _contract(self, db: str):
    return load_contract_at(self._contract_dir(db) / "contract.yml")
```

- `build_arm`:

```python
    def build_arm(self, arm: str, task: Task, db: Path) -> ArmSetup:
        name = task.meta["db"]
        prompt = self._context(name)
        if arm == "contract":
            # No manual: the contract carries the KB text (spec, Arms).
            return governed_arm(prompt, db, self._contract(name), init_sql=AGENT_INIT_SQL)
        if arm in ("manual_prompt", "manual_compiled"):
            entries = _read_jsonl(self._db_dir(name) / f"{name}_kb.jsonl")
            prompt += "\n\n## Knowledge base\n\n" + render_manual(entries)
            if arm == "manual_compiled":
                prompt += "\n\n## Compiled definitions\n\n" + render_compiled(
                    self._contract_dir(name)
                )
        elif arm != "schema_only":
            raise ValueError(f"unknown arm: {arm!r}; expected one of {self.arms}")
        return ArmSetup(prompt, _ungoverned_tools(db, init_sql=AGENT_INIT_SQL), None)
```

- `arm_digest`: the KB file joins for `manual_prompt` and `manual_compiled`. For `manual_compiled` and `contract`, append one more line to `joined`, `f"contract {contract_digest(self._contract(name))}"`, before hashing.
- `check_arms`:

```python
def check_arms(self, arms: tuple[str, ...], tasks: list[Task]) -> None:
    if not {"manual_compiled", "contract"} & set(arms):
        return
    missing = []
    for db in sorted({t.meta["db"] for t in tasks}):
        path = self._contract_dir(db) / "contract.yml"
        if not path.is_file():
            missing.append(db)
            continue
        try:
            self._contract(db)
        except Exception as exc:
            raise SystemExit(
                f"compiled contract for {db} does not load: {exc}"
            ) from exc
    if missing:
        raise SystemExit(
            f"no compiled contract for {missing} under {self._contracts}; run "
            "`python -m dce.compile --benchmark livesqlbench` first"
        )
```

- Remove the no-op `check_arms` if Task 9 added one.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest -q --deselect tests/test_replay.py::test_sql_statements_are_ordered_and_only_from_sql_tools 2>&1 | tail -1`
Expected: all pass. `execute_limited` is the bounded adapter's method, which `_ungoverned_tools` also uses. If its row shape differs, assert on the real shape and record the change as a ruling.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/benchmarks/livesqlbench.py tests/test_livesqlbench.py
git commit -m "livesqlbench: the manual_compiled and contract arms, on the frozen compiled contracts"
```

---

### Task 11: DABStep's `contract_compiled` extra arm

**Files:**
- Modify: `dce/frozen.py` (`COMPILED_CONTRACT_PATH`, `load_compiled_contract`, `compiled_digest`)
- Modify: `dce/benchmarks/dabstep.py`
- Test: `tests/test_dabstep.py`

**Interfaces:**
- Consumes: `governed_arm` (Task 2); `load_contract_at` (Task 7); `check_arms` (Task 9).
- Produces:
  - `EXTRA_ARMS = ("contract_uninterpreted", "manual_resolved", "contract_compiled")`;
  - `"contract_compiled"` in `GOVERNED_ARMS`;
  - `dce.frozen.COMPILED_CONTRACT_PATH = <experiment>/contracts/dabstep/contract.yml`.

- [ ] **Step 1: Write the failing tests**

Update any test in `tests/test_dabstep.py` that pins `EXTRA_ARMS` or `GOVERNED_ARMS` to include `contract_compiled`. Then append:

```python
def test_contract_compiled_differs_from_contract_only_in_the_contract(
    tmp_path, monkeypatch
):
    import shutil

    import dce.frozen as frozen
    from dce.frozen import CONTRACT_PATH

    # Any loadable contract stands in for the compiled one: the claim is
    # about the arm's construction, not the contract's content.
    stand_in = tmp_path / "contracts" / "dabstep"
    shutil.copytree(CONTRACT_PATH.parent, stand_in)
    text = (stand_in / "contract.yml").read_text(encoding="utf-8")
    (stand_in / "contract.yml").write_text(
        text.replace("name: dabstep-payments", "name: dabstep-compiled"),
        encoding="utf-8",
    )
    monkeypatch.setattr(frozen, "COMPILED_CONTRACT_PATH", stand_in / "contract.yml")
    db = tmp_path / "d.duckdb"
    duckdb.connect(str(db)).close()
    bench = _bench(tmp=tmp_path)
    task = DABStep.task(RECORD)
    hand = bench.build_arm("contract", task, db)
    compiled = bench.build_arm("contract_compiled", task, db)
    try:
        assert compiled.system_prompt == hand.system_prompt.replace(
            "dabstep-payments", "dabstep-compiled"
        )
        assert [t.name for t in compiled.tools] == [t.name for t in hand.tools]
        assert bench.arm_digest("contract_compiled", task) != bench.arm_digest(
            "contract", task
        )
    finally:
        hand.close()
        compiled.close()
    bench.check_arms(("contract_compiled",), [task])  # present: passes


def test_contract_compiled_is_checked_before_any_spend(tmp_path, monkeypatch):
    import dce.frozen as frozen

    monkeypatch.setattr(frozen, "COMPILED_CONTRACT_PATH", tmp_path / "missing.yml")
    bench = _bench(tmp=tmp_path)
    bench.check_arms(DABStep.arms, [])  # the default arms need nothing compiled
    with pytest.raises(SystemExit, match="dce.compile --benchmark dabstep"):
        bench.check_arms(("contract_compiled",), [])
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_dabstep.py -k contract_compiled 2>&1 | tail -3`
Expected: FAIL (`AttributeError` on `COMPILED_CONTRACT_PATH`, or an unknown arm).

- [ ] **Step 3: Implement**

`dce/frozen.py`:

```python
#: DABStep's contract compiled by `dce.compile` from the same two documents
#: (spec, Part 5). Written by the compile commit; `DABStep.check_arms` stops
#: a sweep that asks for `contract_compiled` before it exists.
COMPILED_CONTRACT_PATH = (
    Path(__file__).parent.parent / "contracts" / "dabstep" / "contract.yml"
)


def load_compiled_contract() -> DataContract:
    # Read at call time, so a test can point it elsewhere.
    return load_contract_at(COMPILED_CONTRACT_PATH)


def compiled_digest() -> str:
    return contract_digest(load_compiled_contract())
```

`load_contract_at` is defined in Task 7. Place these below it.

`dce/benchmarks/dabstep.py`:
- Import `load_compiled_contract` and `compiled_digest`. Call them through the module (`frozen.load_compiled_contract`) or as imported names; either way, `load_compiled_contract` reads `frozen.COMPILED_CONTRACT_PATH` at call time.
- `EXTRA_ARMS = ("contract_uninterpreted", "manual_resolved", "contract_compiled")`. Extend the comment above it: "`contract_compiled` is `contract` with the contract `dce.compile` produced from the same two documents in place of the hand-authored one (spec, Part 5): it separates how a contract was made from which benchmark it runs on."
- Add `"contract_compiled"` to `GOVERNED_ARMS`, and `"contract_compiled": load_compiled_contract` to the governed `build_arm` mapping.
- `arm_digest`: `if arm == "contract_compiled": return compiled_digest()`.
- `check_arms`:

```python
    def check_arms(self, arms: tuple[str, ...], tasks: list[Task]) -> None:
        if "contract_compiled" not in arms:
            return
        from dce import frozen

        if not frozen.COMPILED_CONTRACT_PATH.is_file():
            raise SystemExit(
                f"no compiled contract at {frozen.COMPILED_CONTRACT_PATH}; run "
                "`python -m dce.compile --benchmark dabstep` first"
            )
        try:
            frozen.load_compiled_contract()
        except Exception as exc:
            raise SystemExit(f"the compiled DABStep contract does not load: {exc}") from exc
```

- [ ] **Step 4: Run the tests and the surface check**

```bash
UV_FROZEN=1 uv run pytest -q --deselect tests/test_replay.py::test_sql_statements_are_ordered_and_only_from_sql_tools 2>&1 | tail -1
UV_FROZEN=1 uv run python analysis/arm_surface.py schema_only manual_prompt contract contract_hollow contract_uninterpreted manual_resolved > "$EQ/arm_surface.task11.json" 2>/dev/null
diff "$EQ/before/arm_surface.json" "$EQ/arm_surface.task11.json" && echo SURFACE-SAME
```

Expected: all tests pass, and `SURFACE-SAME`.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/frozen.py dce/benchmarks/dabstep.py tests/test_dabstep.py
git commit -m "dabstep: the contract_compiled extra arm, checked before any spend"
```

---

### Task 12: LiveSQLBench in `knowledge_delivery.py`

**Files:**
- Modify: `analysis/knowledge_delivery.py`
- Test: `tests/test_knowledge_delivery.py`

**Interfaces:**
- Produces: `CONFIG["livesqlbench"]`. Run stems are `livesqlbench/<model>-r<i>`; the run commit may rename them.

- [ ] **Step 1: Write the failing test**

`tests/test_knowledge_delivery.py`:

```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import knowledge_delivery as kd  # noqa: E402


def test_livesqlbench_reads_four_roles_on_the_primary_subset():
    config = kd.CONFIG["livesqlbench"]
    assert config["subset"] == "primary"
    assert config["roles"] == ("manual", "manual_plus", "contract")
    assert ("contract", "baseline") in config["pairs"]
    assert kd.arms_of("livesqlbench") == {
        "manual": "manual_prompt",
        "manual_plus": "manual_compiled",
        "contract": "contract",
        "baseline": "schema_only",
    }
    assert config["group"]({"high_level": True}) == "high_level"
    assert config["group"]({"high_level": False}) == "standard"
    assert config["runs"]["gpt-6-luna"](2) == ["livesqlbench/luna-r2"]


def test_dabstep_is_unchanged():
    assert kd.arms_of("dabstep") == {
        "manual": "manual_prompt",
        "manual_plus": "manual_resolved",
        "contract": "contract",
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_knowledge_delivery.py 2>&1 | tail -3`
Expected: FAIL, `KeyError: 'livesqlbench'`. If importing the module fails because `analysis/counterfactuals.py` needs local data at import, make the import lazy inside `dabstep_group` rather than skipping the test, and record that as a ruling.

- [ ] **Step 3: Implement**

In `analysis/knowledge_delivery.py`, before `CONFIG`:

```python
#: LiveSQLBench's runs, one file per model and repeat. The run commit fixes
#: these names; until then the entry exists so the analysis is written
#: before the data.
LSB_RUNS = {
    "gpt-6-sol": lambda i: [f"livesqlbench/sol-r{i}"],
    "qwen3.8-27b": lambda i: [f"livesqlbench/qwen38-r{i}"],
    "gpt-6-luna": lambda i: [f"livesqlbench/luna-r{i}"],
}


def lsb_group(row: dict) -> str:
    """The pre-registered secondary split (spec, Part 4)."""
    return "high_level" if row.get("high_level") else "standard"
```

Add to `CONFIG`:

```python
    "livesqlbench": {
        "runs": LSB_RUNS,
        "group": lsb_group,
        "groups": ("high_level", "standard"),
        "extra": None,
        "roles": ("manual", "manual_plus", "contract"),
        # Part 4's three sign tests: what the compiler added, delivery, and
        # the contract against no knowledge at all.
        "pairs": (
            ("manual_plus", "manual"),
            ("contract", "manual_plus"),
            ("contract", "baseline"),
        ),
        "subset": "primary",
    },
```

In the module docstring, change "for all three models" to "for all three models; `--benchmark livesqlbench` reads LiveSQLBench's runs the same way".

- [ ] **Step 4: Run the tests and the DABStep output check**

```bash
UV_FROZEN=1 uv run pytest -q tests/test_knowledge_delivery.py 2>&1 | tail -1
UV_FROZEN=1 uv run python analysis/knowledge_delivery.py > "$EQ/kd.task12.txt" 2>/dev/null
diff <(norm "$EQ/before/knowledge_delivery.txt") "$EQ/kd.task12.txt" && echo KD-SAME
```

Expected: pass, and `KD-SAME`.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add analysis/knowledge_delivery.py tests/test_knowledge_delivery.py
git commit -m "analysis: LiveSQLBench in knowledge_delivery, the three pre-registered pairs on the primary subset"
```

---

### Task 13: The compile audit tool

**Files:**
- Create: `dce/compile/audit.py`
- Test: `tests/test_compile_audit.py`

**Interfaces:**
- Consumes: `FINAL_LOG` and the `_gz_records` helper (Task 8).
- Produces:
  - `dce.compile.audit.render(bench_dir: Path, *, n: int = 30, seed: int = 2026) -> str`;
  - `python -m dce.compile.audit --benchmark NAME [--out contracts] [--n 30] [--seed 2026]`.

  The output is Markdown. Per database it gives entries by kind, metrics, untranslated, prose, the execution rate, queries and dollars, with a totals row. Then it lists the sample of `n` translated metrics drawn with `seed` (`n=0` lists every one, as DABStep's audit needs). Each sample entry carries its KB text, SQL and a blank verdict for the hand check.

- [ ] **Step 1: Write the failing test**

`tests/test_compile_audit.py`:

```python
from dce.benchmarks import livesqlbench
from dce.compile.audit import render
from dce.compile.run import compile_database
from tests.test_compile_run import shop_driver


def test_the_audit_counts_and_samples_reproducibly(lsb_data, tmp_path):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    bench_dir = tmp_path / "livesqlbench"
    compile_database(inputs, bench_dir / "shop", shop_driver([]), spend_cap=10.0)
    text = render(bench_dir, n=2, seed=7)
    assert "| shop | 6 | 2 | 2 | 2 | 4 | 0 | 2 | 100% | 0 | $1.50 |" in text
    assert text == render(bench_dir, n=2, seed=7)  # the sample is seeded
    assert text.count("Verdict: agrees / disagrees / unclear") == 2
    assert render(bench_dir, n=0, seed=7).count("Verdict:") == 4  # all metrics
```

The header row is `| database | entries | calculation | predicate | illustration | metrics | untranslated | prose | execution | queries | cost |`. The section kind is counted only when present.

- [ ] **Step 2: Run it to verify it fails**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_compile_audit.py 2>&1 | tail -3`
Expected: FAIL, `ModuleNotFoundError: No module named 'dce.compile.audit'`.

- [ ] **Step 3: Implement**

`dce/compile/audit.py`:

```python
"""The compile audit (spec, Part 3), drafted from the compile logs.

Per database: entries by kind, metrics translated and not, prose, the
execution rate (translated over calculation and predicate entries), the
compiler's queries and cost. Then a seeded sample of translated metrics,
each with its KB text and SQL and a blank verdict line for the hand check.
The verdicts are filled in by hand and committed with the contracts.

    uv run python -m dce.compile.audit --benchmark livesqlbench > contracts/livesqlbench/AUDIT.md
"""

from __future__ import annotations

import argparse
import random
from collections import Counter
from pathlib import Path

from dce.compile.run import FINAL_LOG, _gz_records


def _databases(bench_dir: Path) -> list[tuple[str, list[dict]]]:
    out = []
    for log in sorted(bench_dir.rglob(FINAL_LOG)):
        records = [r for r in _gz_records(log) if r.get("type") == "entry"]
        out.append((log.parent.name, records))
    return out


def render(bench_dir: Path, *, n: int = 30, seed: int = 2026) -> str:
    dbs = _databases(bench_dir)
    kinds = [
        k
        for k in ("calculation", "predicate", "illustration", "section")
        if any(r["kind"] == k for _, rs in dbs for r in rs)
    ]
    head = [
        "database",
        "entries",
        *kinds,
        "metrics",
        "untranslated",
        "prose",
        "execution",
        "queries",
        "cost",
    ]
    lines = [
        f"# Compile audit: {bench_dir.name}",
        "",
        "| " + " | ".join(head) + " |",
        "|" + "---|" * len(head),
    ]
    totals: Counter = Counter()
    usd_total = 0.0
    sample_pool = []
    for db, records in dbs:
        by_kind = Counter(r["kind"] for r in records)
        status = Counter(r["status"] for r in records)
        metrics = sum(1 for r in records for m in r["metrics"] if m["sql_expression"])
        untranslated = status["untranslated"]
        prose = status["prose"]
        translatable = by_kind["calculation"] + by_kind["predicate"]
        rate = (
            f"{(translatable - untranslated) / translatable:.0%}"
            if translatable
            else "-"
        )
        queries = sum(len(r["queries"]) for r in records)
        usd = sum((r.get("usage") or {}).get("usd") or 0.0 for r in records)
        usd_total += usd
        row = [
            db,
            str(len(records)),
            *[str(by_kind[k]) for k in kinds],
            str(metrics),
            str(untranslated),
            str(prose),
            rate,
            str(queries),
            f"${usd:.2f}",
        ]
        lines.append("| " + " | ".join(row) + " |")
        totals.update(by_kind)
        totals.update(
            {
                "entries": len(records),
                "metrics": metrics,
                "untranslated": untranslated,
                "prose": prose,
                "queries": queries,
            }
        )
        for r in records:
            for m in r["metrics"]:
                if m["sql_expression"]:
                    sample_pool.append((db, r, m))
    total_translatable = totals["calculation"] + totals["predicate"]
    rate = (
        f"{(total_translatable - totals['untranslated']) / total_translatable:.0%}"
        if total_translatable
        else "-"
    )
    lines.append(
        "| **total** | "
        + " | ".join(
            [
                str(totals["entries"]),
                *[str(totals[k]) for k in kinds],
                str(totals["metrics"]),
                str(totals["untranslated"]),
                str(totals["prose"]),
                rate,
                str(totals["queries"]),
                f"${usd_total:.2f}",
            ]
        )
        + " |"
    )
    pool = sorted(sample_pool, key=lambda x: (x[0], x[2]["name"]))
    picked = pool if n == 0 else random.Random(seed).sample(pool, min(n, len(pool)))
    lines += ["", f"## Sample: {len(picked)} metric(s), seed {seed}", ""]
    for db, record, metric in picked:
        lines += [
            f"### {db} / {metric['name']}",
            "",
            f"Entry: {record['name']} ({record['kind']})",
            "",
            "```text",
            metric["description"],
            "```",
            "",
            f"Over `{metric['source_model']}`:",
            "",
            "```sql",
            metric["sql_expression"],
            "```",
            "",
            "Verdict: agrees / disagrees / unclear -- note:",
            "",
        ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="dce.compile.audit")
    parser.add_argument("--benchmark", required=True)
    parser.add_argument("--out", type=Path, default=Path("contracts"))
    parser.add_argument("--n", type=int, default=30)
    parser.add_argument("--seed", type=int, default=2026)
    args = parser.parse_args(argv)
    print(render(args.out / args.benchmark, n=args.n, seed=args.seed))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the test**

Run: `UV_FROZEN=1 uv run pytest -q tests/test_compile_audit.py 2>&1 | tail -1`
Expected: pass. The expected row follows from the fixture: 6 entries; 2 calculations, 2 predicates, 2 illustrations; 4 metrics, 0 untranslated, 2 prose; 100%; no queries; $1.50.

- [ ] **Step 5: Lint, then commit**

```bash
(cd ../.. && prek run --all-files)
git add dce/compile/audit.py tests/test_compile_audit.py
git commit -m "dce.compile.audit: per-database compile counts and a seeded sample for the hand check"
```

---

### Task 14: DABStep equivalence gate and the README

**Files:**
- Modify: `README.md` (experiment README)

- [ ] **Step 1: The gate**

```bash
UV_FROZEN=1 uv run python analysis/arm_surface.py schema_only manual_prompt contract contract_hollow contract_uninterpreted manual_resolved > "$EQ/after.arm_surface.json" 2>/dev/null
diff "$EQ/before/arm_surface.json" "$EQ/after.arm_surface.json" && echo SURFACE-SAME
mkdir -p "$EQ/after"
for f in results/*.jsonl; do UV_FROZEN=1 uv run python -m dce.stats "$f" > "$EQ/after/$(basename "$f").stats" 2>&1; done
UV_FROZEN=1 uv run python analysis/knowledge_delivery.py > "$EQ/after/knowledge_delivery.txt" 2>/dev/null
n=0; for f in "$EQ"/before/*.stats "$EQ/before/knowledge_delivery.txt"; do b=$(basename "$f"); diff -q <(norm "$f") <(norm "$EQ/after/$b") >/dev/null || { echo "DIFF $b"; n=$((n+1)); }; done; echo "differing: $n"
UV_FROZEN=1 uv run pytest -q --deselect tests/test_replay.py::test_sql_statements_are_ordered_and_only_from_sql_tools 2>&1 | tail -1
(cd ../.. && prek run --all-files)
git status --short uv.lock ../../uv.lock
```

Expected:
- `SURFACE-SAME` and `differing: 0`;
- all tests pass;
- prek clean;
- no lock file modified.

- [ ] **Step 2: README**

In the `## LiveSQLBench` section:
- Replace "`manual_compiled` and `contract` arrive with the compiler." with a short subsection, `### Compiling the contracts`, in the README's style, saying:
  - **The command:** `python -m dce.compile --benchmark livesqlbench --all --max-spend 60`, and `--benchmark dabstep` for Part 5.
  - **What the compiler reads and never reads:** the KB, column meanings, DDL and a read-only, file-less database connection. Never tasks, golds or test cases; a test enforces it.
  - **The output:** `contracts/<benchmark>/<db>/contract.yml`, `semantic.yml` and `compile-log.jsonl.gz`.
  - **Resuming:** rerun the same command. A finished database is skipped, and `--max-spend` covers resumes.
  - **The audit:** `python -m dce.compile.audit --benchmark livesqlbench > contracts/livesqlbench/AUDIT.md`, verdicts filled by hand.
  - **The arms:** `manual_compiled` (the manual, then "Compiled definitions") and `contract` (the compiled contract behind governed tools). A sweep asking for either stops before any spend if a database has no compiled contract.
- In the `## Arms` section, add one line: `contract_compiled` is DABStep's extra arm with the compiled contract (spec, Part 5).
- In the `--arms` row of the flag table, add `contract_compiled` to the DABStep arms run only when named. Change LiveSQLBench's arms to `schema_only manual_prompt manual_compiled contract`.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "README: compiling the contracts, and the compiled arms"
```

---

### Task 15: A paid compile of one LiveSQLBench database (spends money: ask first)

This compiles one database's KB with `claudesonnet5`, the only part of PR C that calls a model. Choose the database with the fewest KB entries. It should cost about a dollar. It uses the gateway key. **Ask the user before launching, and launch only after they say yes.** The output stays in the scratchpad: the compile commit, not this PR, adds the real contracts.

- [ ] **Step 1: Pick the database and run**

```bash
for f in "${LSB_DATA:-$HOME/data/livesqlbench}"/hf-full-v1/*/*_kb.jsonl; do echo "$(wc -l < "$f") $(basename "$(dirname "$f")")"; done | sort -n | head -3
set -a; . ./.env; set +a
UV_FROZEN=1 uv run python -m dce.compile --benchmark livesqlbench --db <smallest> \
  --out "$EQ/compiled" --max-spend 5 > "$EQ/compile.log" 2>&1
echo "exit=$?"; sed -E 's#https?://[^ /"]+#<host>#g' "$EQ/compile.log" | grep -vE '^(Uni|I)nstalled' | tail -5
```

Expected: exit 0, and `<db>: done (spent $x.xx of $5.00)`.

- [ ] **Step 2: Check the output**

```bash
UV_FROZEN=1 uv run python -m dce.compile.audit --benchmark livesqlbench --out "$EQ/compiled" --n 5 > "$EQ/audit.md" 2>/dev/null; head -8 "$EQ/audit.md"
UV_FROZEN=1 uv run python - "$EQ/compiled/livesqlbench" <<'EOF' 2>/dev/null
import sys
from pathlib import Path
from dce.benchmarks.livesqlbench import LiveSQLBench
bench = LiveSQLBench.from_data(Path.home() / "data" / "livesqlbench", contracts=Path(sys.argv[1]))
tasks = [t for t in bench.tasks() if (Path(sys.argv[1]) / t.meta["db"]).is_dir()]
bench.check_arms(LiveSQLBench.arms, tasks)
task = tasks[0]
for arm in LiveSQLBench.arms:
    setup = bench.build_arm(arm, task, bench.pristine_db(task))
    print(arm, len(setup.system_prompt), [t.name for t in setup.tools][:4])
    setup.close()
EOF
```

`build_arm` opens the database for the governed tools. The pristine file is used here only to build the arms, and no query runs. If DuckDB refuses a second connection configuration, copy the file to the scratchpad first and pass the copy.

Then read three compiled metrics against their KB entries, plus one illustration and, if any, one untranslated entry. Check three things:
- each description holds the KB text verbatim;
- each SQL computes what the entry says;
- every note is something the compiler checked with a query (see the record's `queries`).

- [ ] **Step 3: Report**

Tell the user:
- the cost, and the per-database row of the audit;
- the execution rate, and any untranslated entries with their errors;
- what the hand check of the sampled metrics found;
- an estimate for `--all` (22 databases), scaled by entry count.

Ask whether to push and open PR C. The compile commit is the next step after the merge.

---

## Self-review notes

- Spec coverage:
  - **Compiler:**
    - inputs and the only files opened: Tasks 5 and 8, with the file-access tests;
    - topological order: Task 3;
    - resumable log: Task 8;
    - model, tools, the 50-row cap and query logging: Tasks 4, 6 and 8;
    - mapping (calculation, predicate, illustration, section): Task 6;
    - validation and the 3 attempts with prose fallback: Task 6;
    - output and the per-database digest (`contract_digest` in `arm_digest`): Tasks 7, 8 and 10.
  - **Layout:** `contracts/<benchmark>/`, in Tasks 8, 10 and 11.
  - **Arms:**
    - `manual_compiled`, the manual plus the appendix with the SQL exactly as in the contract and every compiler note: Tasks 7 and 10;
    - `contract`, through the shared builder with the same sentence, no manual and `PG_COMPAT`: Tasks 2 and 10.
  - **Part 3:** the tooling, in Task 13; the hand verdicts belong to the compile commit.
  - **Part 5:**
    - the section adapter: Task 5;
    - the `contract_compiled` extra arm, differing only in the contract: Task 11;
    - the file-access test covering `tasks.json` and `golds.json`: Task 8.
  - **Testing:**
    - both adapters: Task 5;
    - `contract_compiled` versus `contract`: Task 11;
    - topological order: Task 3;
    - validation and prose fallback with a stubbed model: Task 6;
    - resume from a partial log: Task 8;
    - file access: Task 8.
  - **Part 4:** the pre-registered sign-test pairs and the decomposition's roles, in Task 12.
- **Deferred to the compile commit:**
  - the 22 contracts and DABStep's;
  - the filled audit;
  - the four-arm smoke;
  - the run commit's `primary` tuple for LiveSQLBench, `None` until then;
  - the run-file names in `LSB_RUNS`.
- `DATA_NOTE`: the spec contradicted itself. Task 1 resolves it in the spec: no note for `contract_compiled`.
