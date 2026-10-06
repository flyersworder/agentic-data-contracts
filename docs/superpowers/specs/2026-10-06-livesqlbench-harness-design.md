# LiveSQLBench harness: design

Date: 2026-10-06. Status: approved in conversation, awaiting spec review.

## Goal

Replicate the DABStep contract evaluation on a second benchmark,
LiveSQLBench Base-Full-v1, ported to DuckDB. The question is the same: does
an agent answer better when a database's business knowledge reaches it as a
data contract (executable definitions behind governed tools) than as a prose
manual, with the knowledge held equal? The DABStep findings to test on new
ground are the knowledge-or-delivery decomposition and the result that the
contract closes the capability gap within a model family.

Success: a pre-registered k=3 run of four arms on three models over the
frozen task set, analysed the way `analysis/knowledge_delivery.py` analyses
DABStep, with every number reproducible from committed files plus the
privately held gold answers.

## Decisions taken

| decision | choice | why |
|---|---|---|
| harness structure | make `dce` benchmark-agnostic behind a `Benchmark` protocol; DABStep becomes the first implementation | the hardened runner (budget ledger, retry pass, lock, torn-tail repair, construction retries, clean-tree check) is most of the value; designing the seam against two real benchmarks avoids a speculative abstraction. DABStep results stay reproducible at their tagged run commits |
| contracts | compiled from each database's KB by an LLM, offline, once, then frozen and audited | 22 databases and 1,369 KB entries rule out hand authoring; "compile the manual once" is itself the practical claim |
| compiler model | `claude-sonnet-5` | not one of the agent models, so no agent reads SQL its own model wrote |
| compiler access | KB, column meanings, DDL, and read-only queries against the DuckDB file; never tasks, golds or test cases | what a data engineer writing a contract would have; whatever it learns reaches `manual_compiled` too |
| arms | `schema_only`, `manual_prompt`, `manual_compiled`, `contract` | `manual_compiled` holds the compiler's output constant and varies delivery only, answering "the compiler did the work" |
| agent models | `gpt-6-sol`, `gpt-6-luna`, `qwen3.8-27b` | the DABStep anchor, its small tier, and the open-weights point |
| repeats | k=3 | as on DABStep |
| compiled contract on DABStep | a `contract_compiled` extra arm, compiled from `manual.md` and `payments-readme.md` by the same compiler | separates how the contract was made from which benchmark it runs on (see "What the design can and cannot claim") |
| package rename | out of scope | a separate mechanical PR if wanted |

## Order of work

1. **PR A: the Benchmark refactor.** DABStep is the only implementation.
   Includes making stats and analysis role-driven. No behaviour change,
   verified by the equivalence checks below.
2. **PR B: the LiveSQLBench implementation and the compiler code,** plus
   DABStep's KB adapter and its `contract_compiled` extra arm. Tested on a
   synthetic fixture database and KB; no real gold in the repo.
3. **Compile commit.** The 22 frozen LiveSQLBench contracts and DABStep's
   compiled contract, their compile logs, the audit.
4. **Smoke.** 12 tasks stratified by database, four arms, `gpt-6-luna`.
5. **Run commit.** The pre-registration (below); tagged, tag pushed upstream
   by the maintainer.
6. **Runs.** One job per model, three repeats in sequence with
   `--retry-pass`, as for the gpt-6-luna DABStep run. The DABStep
   compiled-contract run (Part 5) follows the LiveSQLBench runs.

## Part 1: the Benchmark protocol (PR A)

### Interface

```python
@dataclass(frozen=True)
class Task:
    task_id: str
    prompt: str  # the full user message
    group: str  # stratum for smoke sampling and per-group stats
    meta: dict  # benchmark-specific, read only by the benchmark


@dataclass(frozen=True)
class Verdict:
    verdict: str  # "correct" | "incorrect" | "ungraded"
    gold_ref: str | None  # what the row stores in its `gold` field
    answer_normalized: str
    failure: str | None  # grading-side failure category, if any


class Benchmark(Protocol):
    name: str
    arms: tuple[str, ...]  # run when --arms is not given
    extra_arms: tuple[str, ...]  # run only when named
    roles: dict[str, str]  # analysis role -> arm name (see below)

    def tasks(self) -> list[Task]: ...
    def pristine_db(self, task: Task) -> Path: ...
    def build_arm(self, arm: str, task: Task, db: Path) -> ArmSetup: ...
    def arm_digest(self, arm: str, task: Task) -> str: ...
    def grade(self, task: Task, answer: str, db: Path) -> Verdict: ...
    def golds_hash(self) -> str: ...
    def row_fields(self, task: Task) -> dict: ...
```

### What moves where

- `dce/benchmark.py`: `Task`, `Verdict`, `Benchmark`, and a registry
  (`get_benchmark(name)`).
- `dce/tools.py` (shared arm building): `ArmSetup`, `_ungoverned_tools`,
  `_governed_tools`, `_BoundedDuckDBAdapter`, `_append_truncation_marker`,
  `MAX_ROWS`, `make_working_copy`, `check_and_restore`. The adapter gains an
  `init_sql: tuple[str, ...] = ()` hook run on every connection it opens;
  DABStep passes none.
- `dce/benchmarks/dabstep.py`: DABStep's `build_arm`, `DATA_NOTE`, arm
  tuples, `arm_digest`, grading (wrapping `dce.grade.score`), golds loading
  and `DATASET_REVISION` check, task loading, `row_fields` = `{"level": ...}`,
  `Task.group` = level. The existing modules it draws on (`frozen`, `grade`,
  `golds`, `data`, `hollow`, `uninterpreted`, `submit`) stay where they are,
  so imports elsewhere and the commands in FINDINGS.md and README.md keep
  working.
- `dce/agent.py`: `run_task(task, arm, model, benchmark, db, ...)`. It calls
  `benchmark.build_arm`, sends `task.prompt`, grades through
  `benchmark.grade`, and builds the row from the `Verdict` plus
  `benchmark.row_fields(task)` plus `benchmark=benchmark.name`. The model
  factories, retry and forced-final-answer logic are untouched.
- `dce/runner.py`: `--benchmark {dabstep,livesqlbench}`, default `dabstep`.
  Each worker holds one working copy, created lazily; `check_and_restore`
  runs after every task as now. `_stratified_sample` uses `Task.group`.
  The queue is ordered by `Task.group`, so a worker keeps
  one working copy and re-copies only when the database changes; disk use
  stays fixed however many databases a benchmark has. DABStep has one
  database, so its scheduling is unchanged.
- `dce/arms.py` stays as a thin re-export so existing imports and tests keep
  working.

### Rows

Two fields are added to every new row: `benchmark` and `group` (the task's
`Task.group`). Readers treat a missing `benchmark` as `dabstep` and a
missing `group` as the row's `level`, so all committed results load
unchanged. Stats and analysis read only these generic fields, never a
benchmark's own `row_fields`.

### Analysis by role

`dce.stats` and `analysis/knowledge_delivery.py` stop naming arms. Each
benchmark maps roles to its arms, and the analysis works on roles:

| role | DABStep | LiveSQLBench |
|---|---|---|
| `baseline` | (none) | `schema_only` |
| `manual` | `manual_prompt` | `manual_prompt` |
| `manual_plus` | `manual_resolved` | `manual_compiled` |
| `contract` | `contract` | `contract` |

Both take `--benchmark` (default `dabstep`), print per-`group` strata, and
compute the share decomposition, (manual_plus - manual) / (contract -
manual), from the roles. A role a benchmark lacks is skipped. Comparisons
DABStep reports beyond these roles (the hollow and uninterpreted arms, the
contract vs each other arm) stay as they are, driven by `arms` and
`extra_arms`. A third benchmark then needs a module and a roles table, and
no new analysis code.

### Equivalence checks (gate for merging PR A)

1. All existing tests pass (426 at the time of writing).
2. For every DABStep arm, the system prompt and the tool names, descriptions
   and JSON schemas are byte-identical before and after: a script dumps them
   at the pre-refactor commit and at the PR head and diffs the dumps.
3. `dce.stats` and `analysis/knowledge_delivery.py`, now role-driven,
   produce identical output over every committed results file.
4. A 12-task stratified smoke on `gpt-6-luna`, three arms, completes with
   rows whose fields, apart from the new `benchmark` and `group` fields and
   run-specific values, match the pre-refactor smoke's schema.

## Part 2: the LiveSQLBench implementation (PR B)

### Data, outside the repo

`LSB_DATA` (default `~/data/livesqlbench`) holds the gold answers, test
cases, the 22 DuckDB files, `tasks_frozen_full_v1.json` and the frozen gold
results. None of it is committed. The public task file and the per-database
KB, column meanings and DDL come from the Hugging Face dataset
`birdsql/livesqlbench-base-full-v1`; the compile commit records its
revision. The prep scripts from `~/data/livesqlbench/prep/` (DuckDB
conversion, gold execution, task freezing, translation checks) are committed
under `prep/livesqlbench/`, reading from and writing to `LSB_DATA` only.

### Task set

From `tasks_frozen_full_v1.json`: 309 primary tasks and 72 order-only tasks
run in the same sweep, 381 in all; `Task.group` = database name;
`row_fields` = `{"db", "set", "high_level", "order"}`. The 30 excluded tasks
are never run. `Task.group` is written to each row as `group`.
`Task.prompt` is the task's `query` followed by one fixed
instruction: end the answer with the final SQL in a ```sql block, which must
run on this DuckDB database.

### Compiler

The compiler is benchmark-agnostic and lives in `dce/compile/`. Its input
is a neutral list of KB entries (`id`, `kind`, `name`, `text`, `children`)
plus the DDL, the column meanings and a DuckDB file; each benchmark supplies
a small adapter that produces this list. LiveSQLBench's adapter, in
`dce/benchmarks/livesqlbench.py`, reads `*_kb.jsonl` and maps
`calculation_knowledge`, `domain_knowledge` and `value_illustration` to the
neutral kinds `calculation`, `predicate` and `illustration`. Any later
benchmark with written documentation can reuse the compiler through its own
adapter.

`uv run python -m dce.compile --benchmark livesqlbench --db <name> | --all`,
run once.

- Inputs, and the only files the script opens: the database's
  `*_kb.jsonl`, `*_column_meaning_base.json`, `*_schema.txt`, and its DuckDB
  file (read-only, with `PG_COMPAT`). A test asserts that the compiler's
  file accesses never touch the task, gold or test-case files.
- Order: entries topologically by `children`, so a parent's SQL can
  reference its children's metrics. An entry of kind `section` (DABStep,
  Part 5) may yield several metrics or none.
- Resumable: each finished entry is appended to the compile log at once; a
  rerun reads the log, skips entries already done and continues, so a crash
  at the fifteenth database costs only the entry in progress.
- Model: `claude-sonnet-5` with two tools: `run_sql` (read-only, capped at 50
  rows, every call logged) and `submit_entry`. Temperature 0 where the route
  accepts it.
- Mapping:
  - `calculation` -> metric, `sql_expression` computing the value,
    `source_model` the table it is computed over;
  - `predicate` -> metric whose `sql_expression` is a boolean predicate (the
    precedent is DABStep's `fee_rule_matches_transaction`);
  - `illustration` -> prose on the table or column it describes;
  - every metric's `description` contains the KB entry's description and
    definition verbatim, and names its children.
- Validation: each metric's SQL must execute over its source table
  (`SELECT <expr> FROM <source> LIMIT 5`, or the predicate in a `WHERE`).
  Up to 3 attempts; an entry that still fails is kept as prose with
  `untranslated` set to the last error.
- Output: `contracts/livesqlbench/<db>/contract.yml` and `semantic.yml`, a
  compile log (`contracts/livesqlbench/<db>/compile-log.jsonl.gz`: every
  model turn and query), and a per-database digest used by `arm_digest`.

### Layout

New benchmarks keep their files under their own name: contracts in
`contracts/<benchmark>/`, results in `results/<benchmark>/`, traces in
`traces/<benchmark>/`. DABStep's existing `contract/`, `results/` and
`traces/` paths stay where they are, since FINDINGS.md and README.md cite
them.

### Arms

Every arm's prompt carries the DDL and the column meanings, so the arms
differ only in how the KB reaches the agent.

| arm | adds |
|---|---|
| `schema_only` | nothing |
| `manual_prompt` | the KB rendered as a prose manual: entries in `id` order, each as name, description, definition |
| `manual_compiled` | the manual, then an appendix "Compiled definitions": each compiled metric's name and SQL, exactly as in the contract |
| `contract` | the governed tools and `contract.to_system_prompt()`, through the shared builder DABStep uses, with the same procedural sentence; no manual, since the contract carries the KB text |

All arms' DuckDB connections run `PG_COMPAT`
(`default_null_order='nulls_last_on_asc_first_on_desc'`,
`integer_division=true`).

### Grading

- The answer is the last ```sql block in the final message.
- It runs on a fresh read-only connection to the pristine database with
  `PG_COMPAT` and a statement time limit, and its result is compared with the
  frozen gold result by the existing Soft-EX logic (`lsb_grade.matches`).
- Primary tasks: order-sensitive where the task's `conditions.order` is true.
  Order-only tasks: order-insensitive, reported separately.
- Failure categories, all graded incorrect: `no_sql`, `sql_error`,
  `timeout`.
- `gold_ref` is the sha256 of the normalised gold result. The gold itself is
  never written to a row or a trace, so results files and traces can be
  committed.

## Part 3: compile audit

Committed with the contracts, before any agent run:

- per database: entries by type, compiled vs prose-only, execution rate;
- 30 LiveSQLBench metrics drawn with a fixed seed, each checked by hand
  against its KB text (agrees / disagrees / unclear, with a note), listed so
  a reviewer can repeat the check;
- every metric of DABStep's compiled contract checked the same way against
  `manual.md`, and set beside the hand-authored contract's metric of the
  same name where one exists;
- the compiler's total queries and cost.

The audit's disagree rate is reported next to the agent results: it bounds
how much of the contract's level is the compiler's error rather than
delivery.

The contracts are not edited after the audit. If the audit finds a
systematic compiler fault, the compiler is fixed and everything recompiled,
and the audit records both rounds.

## Part 4: run protocol and analysis

The run commit fixes, before the first full run:

- task sets (309 primary, 72 order-only), k=3, the four arms, the three
  models, the four arms pairing within each repeat;
- primary measure: end-to-end strict accuracy on the primary set; task-level
  sign tests over the three repeats for `manual_prompt` vs `manual_compiled`
  and `manual_compiled` vs `contract`, and `contract` vs `schema_only`;
- the decomposition: share = (compiled - manual) / (contract - manual);
- secondary: the order-only set, the `high_level` split, and per-database
  results;
- written predictions, settled in that commit.

Budget per repeat, from DABStep's cost per task-run: about $55 for
`gpt-6-sol`, $5 for `gpt-6-luna`; `qwen3.8-27b` is self-hosted (about a day
per repeat). The compiler is a one-off of roughly $20-50.

LiveSQLBench runs use the library at main (0.59.0); the run commit carries
the updated experiment `uv.lock`.

## Part 5: the compiled contract on DABStep

DABStep's contract was authored once from `manual.md` and
`payments-readme.md` and frozen before any task was read; LiveSQLBench's
are compiled by an LLM. Without a bridge, any difference between the two
benchmarks' results could come from either the benchmark or the authoring.
This run supplies the bridge.

- **Compile.** DABStep gains a KB adapter: `manual.md` split at its section
  headings into neutral entries of kind `section` (the compiler decides per
  section whether it yields metrics, predicates or prose; the LiveSQLBench
  kinds fix this up front), with `payments-readme.md` as the column
  meanings and `data/dabstep.duckdb` as the database. The compiler opens no
  other file; the file-access test covers `tasks.json` and `golds.json`.
  Output in `contracts/dabstep/`, frozen and audited in the compile commit.
- **Arm.** `contract_compiled`: the `contract` arm's tools, procedural
  sentence and `DATA_NOTE`, with the compiled contract in place of the
  hand-authored one. An extra arm, so DABStep's defaults and the PR A
  equivalence checks are unaffected.
- **Run.** `contract` and `contract_compiled` together, all 450 tasks, k=3,
  pairing within each repeat, on `gpt-6-sol` and `gpt-6-luna`. At the cost
  per task-run above, about $100 and $10 over the three repeats.
- **Measure.** End-to-end strict accuracy; task-level sign test of
  `contract_compiled` vs `contract` over the three repeats; the same
  rule-set / total-fee / rest groups as the gpt-6-luna section of
  FINDINGS.md. Pre-registered in the same run commit as LiveSQLBench, with
  its own written prediction.

## What the design can and cannot claim

- **Within a benchmark, arm comparisons are the claims.** `manual_compiled`
  vs `contract` holds the compiler's output constant and varies delivery
  only; `manual_prompt` vs `manual_compiled` measures what the compiler
  added (resolved ambiguities, facts it learned from querying), the role
  `manual_resolved` played on DABStep.
- **LiveSQLBench results are conditional on the compiler.** The claim is
  about a contract compiled once by `claude-sonnet-5` and frozen; a
  different compiler or a human author could do better or worse. Compiler
  errors reach `manual_compiled` and `contract` alike, so they do not bias
  the delivery comparison, but they lower the contract's absolute level;
  the audit's disagree rate is reported alongside.
- **Across benchmarks, only the pattern is compared, never effect sizes.**
  The benchmarks differ in more than their data (one database against 22,
  free-form answers against SQL graded by Soft-EX, a few dense rules
  against many small definitions), so absolute accuracies, gaps and shares
  are not set side by side as if commensurable. The replicated claims are
  directional: the contract beats the manual, most of the gap is delivery,
  and the small model closes on the large one with the contract.
- **Authoring is separated by Part 5, not assumed away.** If
  `contract_compiled` matches `contract` on DABStep, a weaker LiveSQLBench
  result points at the benchmark; if it falls short, the shortfall measures
  what compiling costs, and the LiveSQLBench results are read with it.
- **Provenance.** DABStep's contract provenance is self-attested.
  LiveSQLBench's and DABStep's compiled contracts are checkable: the compile
  log records every model turn and query, and a test shows the compiler
  never opens a task, gold or test-case file.

The run commit's pre-registration states these limits in the same words.

## Error handling

- Agent and transport errors: the existing error rows, `--retry-pass`, and
  free retries for construction failures.
- A run that leaves its working database modified: the existing
  restore-and-flag (`db_corrupted`).
- Grading failures: the categories above; a grader exception is an
  `ungraded` row with the exception recorded, never a silent `incorrect`.

## Testing

- PR A: the equivalence checks above, plus unit tests for the registry,
  the role mapping (including a missing role), the `group`/`level` fallback,
  per-worker working copies re-copied on a database change, the queue's
  ordering by group, and the adapter's `init_sql` hook.
- PR B, test-first, on `tests/fixtures/lsb/`: a two-table DuckDB fixture, a
  six-entry KB with one parent-child pair, and hand-written gold results.
  Covered: task loading and prompt text, each arm's prompt and tools, SQL
  extraction, Soft-EX grading including order and the failure categories,
  `gold_ref` never exposing the gold, both KB adapters' mapping to neutral
  entries (LiveSQLBench's entry types, DABStep's section split), the
  `contract_compiled` arm differing from `contract` only in the contract, the compiler's topological order, validation and prose fallback
  (with a stubbed model), resuming from a partial compile log, and the
  compiler's file-access restriction.

## Out of scope

- Renaming `experiments/dabstep-contract-eval` or the `dce` package.
- Claude Sonnet 5 as an agent model.
- LiveSQLBench's Management (non-SELECT) tasks and the SQLite variant.
- The prose "Document" KB format, which is not released.
