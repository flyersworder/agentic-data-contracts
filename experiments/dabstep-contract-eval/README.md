# DABStep Contract-Context Eval

A three-arm ablation measuring whether contract-delivered context beats a
manual-in-prompt baseline and a schema-only floor on the
[DABStep](https://huggingface.co/datasets/adyen/DABstep) benchmark.

Design: `docs/superpowers/specs/2026-08-30-dabstep-contract-eval-design.md`.
Results and conclusions: `FINDINGS.md` (written after the sweep).

**Read the [Money](#money-read-this-before-the-first-paid-run) section before
the first paid run.** The reservation arithmetic is the single most surprising
thing about this harness: a `--max-spend` that looks generous can write zero
rows and exit 2.

## Setup

```bash
cd experiments/dabstep-contract-eval
uv sync
uv run pytest -q                     # 327 tests, deterministic and offline
uv run python -m dce.prepare         # downloads DABStep, builds the DuckDB, reconstructs golds
```

`dce.prepare` is the only step that touches the network. It writes everything
under `data/`, which is gitignored — nothing it produces is committed, so it
has to be re-run in a fresh checkout.

```
python -m dce.prepare [--threshold 0.75]
```

`--threshold` is the plurality share required to accept a reconstructed gold.
The default, `0.75`, is the pre-registered value. Any other value is a
**sensitivity run**: it writes `data/golds_threshold_<t>.json` instead of
`data/golds.json`, and the runner refuses to score against it. Run it at
`0.60` and `0.90` to produce the sensitivity table FINDINGS requires; the
per-`level` gold coverage FINDINGS also requires is printed on every run.

## The API key

The runner reads `OPENROUTER_API_KEY` from the environment. Keep the key in a
`.env` that lives **outside this repo** and point `LENS_ENV_FILE` at it:

```bash
set -a; . "$LENS_ENV_FILE"; set +a
```

Never commit a key, and never write one into a file under the repo — the
sweep also refuses to start on a dirty working tree (see below), so a stray
`.env` here would block the run as well as leak the key.

## Run

```bash
set -a; . "$LENS_ENV_FILE"; set +a
uv run python -m dce.runner --n 12 --max-spend 2.00 --models z-ai/glm-5.3-flash \
  --out results/smoke12.jsonl
uv run python -m dce.stats results/smoke12.jsonl
```

| Flag | Default | Meaning |
|---|---|---|
| `--max-spend` | **required** | Cap in USD on the *guard ledger* across every resume of this results file. See [Money](#money-read-this-before-the-first-paid-run). |
| `--n` | `0` (all) | Stratified sample of `n` golded tasks, proportional across `level`. |
| `--arms` | all three | Any subset of `schema_only manual_prompt contract`. |
| `--models` | `z-ai/glm-5.3-flash` | Any subset of the pinned ids in `dce/pricing.py`. An unpinned id is rejected. |
| `--out` | `results/results.jsonl` | One JSON row per `(task, arm, model)`. Appended to, never rewritten. |
| `--db` | `data/dabstep.duckdb` | The pristine warehouse. Each process runs against a working *copy*. |
| `--golds` | `data/golds.json` | The gold envelope. Its `revision`, `threshold` and content hash are all checked. |
| `--tasks` | `data/tasks.json` | Task list; filtered to those that have a gold. |
| `--workers` | `1` | Task groups to run concurrently, each on its own working copy. See [Unattended runs](deploy/README.md) before raising it. |
| `--retry` | none | `error` or `post_run_error` — also re-run rows with that verdict on resume. Both already cost money, which is why neither is retried by default. `construction_error` rows are retried automatically (twice, then given up on loudly). |

**Runs are resumable and resumption is automatic**: re-run the identical
command and every completed `(task, arm, model)` is skipped. `--max-spend` is
a budget for the whole results file, not for one invocation, so a resume of a
`--max-spend 40.00` run stops at $40 total, not at $80.

Verified, not assumed: a `SIGKILL` at 142/180 rows, and separately a
hand-torn tail plus a crash loop, both resumed to exactly 180 rows with
nothing missing and nothing re-run. Every row is `fsync`ed before the next is
written, and the file is snapshotted every 25 rows to
`<out>.jsonl.snapshot`. **Only one sweep at a time may hold a results
file** — a second is refused, because two would duplicate paid work and share
`<db>.working`, corrupting `db_corrupted` for both. For running this
unattended on a VPS, see [`deploy/README.md`](deploy/README.md).

**The sweep refuses to start on a dirty working tree.** Every result row
stamps `commit_sha`, and with an editable path dependency the library under
test *is* the working tree — an uncommitted change makes that stamp a lie and
nothing in the results would show it. The output file itself is exempt.

**Commit `results/smoke12.jsonl` after the smoke run, before launching the
full sweep.** `assert_clean_tree` exempts only the exact `--out` file passed
to *that* invocation — `results/` itself is deliberately not gitignored (the
tamper-evidence claim depends on results being readable in git history), so
an uncommitted `results/smoke12.jsonl` left over from the smoke run is just
another dirty file as far as the *next* invocation (writing to a different
`--out`) is concerned, and it will refuse to start. This fires on the very
first paid session:

```bash
git add results/smoke12.jsonl
git commit -m "smoke run"
```

## Arms

| Arm | Context reaching the model | Tools |
|---|---|---|
| `schema_only` | Table and column names only, discovered through the tools. The floor. | `list_tables`, `describe_table`, `execute_sql` (ungoverned) |
| `manual_prompt` | The above, plus all 22k characters of DABStep's `manual.md` verbatim in the system prompt. The baseline to beat. | same three, ungoverned |
| `contract` | The library's nine governed tools over a frozen, digest-pinned contract. No manual text. | `lookup_metric`, `lookup_domain`, `inspect_query`, `run_query`, … |

The arms share one row cap (50 rows per tool result, applied identically),
one token budget, one working copy of the warehouse, and one task order.

**The arms are not a clean context-only contrast**, and any writeup has to say
so: arm C additionally receives 3,042 characters of *procedural* tool
descriptions from the library (`inspect_query` tells the model it "MUST call
lookup_metric first"), an arm-C-only workflow sentence, and a `SELECT *`
prohibition. The system-prompt size ratio between arms B and C is 9.77x, but
**4.41x** once tool schemas are counted — and tool schemas are always-on
context, so 4.41x is the honest number. See the spec's "What this comparison
is, and is not" section.

## Money (read this before the first paid run)

### Worst case per task, per model

Measured, including a single-request overshoot of the token guard:

| Model | Worst case, one task, one arm |
|---|---|
| `deepseek/deepseek-v4-flash-0731` | $0.14 |
| `deepseek/deepseek-v4-pro-0813` | $1.58 |
| `z-ai/glm-5.3-flash` | $0.20 |
| `openai/gpt-5.6-sol` | **$7.73** |

### Reservation headroom — the number that surprises people

The sweep reserves the **worst case for a whole task group** before starting
that group, and a task group is *every arm × every model for one task*. The
reservation is the permanent ceiling, tightened only by observations from
tasks that have already run — so a sweep needs

> `--max-spend` **strictly greater than one task-group reserve** to run even
> one task.

| Configuration | One task-group reserve |
|---|---|
| 3 arms, `z-ai/glm-5.3-flash` | $0.55 |
| 3 arms, `deepseek/deepseek-v4-pro-0813` (**the primary model**) | **$4.35** |
| 2 arms, `openai/gpt-5.6-sol` | **$14.64** |

Concretely, and reproduced:

* `--n 12 --max-spend 2.00` on **glm** completes all 12 tasks and really
  spends about $0.22.
* `--n 12 --max-spend 2.00` on **deepseek-pro** writes **zero rows and exits
  2**, because $2.00 < $4.35. Nothing is wrong; the cap simply cannot admit a
  single task group.
* **`--arms` defaults to all THREE arms**, not two. The $14.64 figure above
  is for 2 arms only (`manual_prompt` + `contract`, the design's own Sol
  comparison) — the default 3-arm invocation reserves 3 x $7.32 =
  **$21.96/task-group**, so `--max-spend 15.00` with the default arms writes
  **zero rows and exits 2**. To actually get the $14.64 ceiling, name the
  arms explicitly:

  ```bash
  uv run python -m dce.runner --arms manual_prompt contract \
    --models openai/gpt-5.6-sol --max-spend 15.00 --out results/sol.jsonl
  ```

  Even then, **$15.00 admits only one task group** before the next one's
  reservation would exceed it (`admits up to 1`) — the design calls for
  ~60 Sol tasks x 2 arms, so budget accordingly (roughly `60 * $14.64` =
  **$878.40** worst case, tightening as real observations come in) rather
  than treating $15.00 as sufficient for the whole Sol run.

Budget a sweep as `--max-spend` **plus one group's overshoot**: the cap is
checked before a group starts, not while it runs.

The runner prints the reserve on startup — read that line before walking away
from a run:

```
reserve ceiling $4.35/task-group (3 arms x ['deepseek/deepseek-v4-pro-0813']);
$2.00 cap admits up to 0 worst-case task-group(s) before the first observation tightens it
```

`admits up to 0` means no task will run.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Completed cleanly — every pending unit ran. |
| `1` | An uncaught exception (Python's own default). A traceback is the last thing printed; no `spent` line. |
| `2` | **Truncated**: stopped because the next task group's reservation exceeded `--max-spend`. |
| `3` | Circuit broken: 5 consecutive construction failures — systemic (a missing key, a bad model id), not bad luck. |
| `4` | Connection leaked: an arm's DuckDB session failed to close, so no later integrity check in this process can be trusted. Resume in a fresh process. |

**A budget-capped sweep is *expected* to exit 2.** Exit 2 is the normal,
successful end of a run that was deliberately capped — the rows it wrote are
good, and the next resume picks up where it stopped. This breaks the usual
shell idiom:

```bash
# WRONG: `analyze` never runs on a capped sweep, which is the normal case
uv run python -m dce.runner --max-spend 40.00 ... && uv run python -m dce.stats results/full.jsonl

# Right: treat 0 and 2 as success
uv run python -m dce.runner --max-spend 40.00 ...; code=$?
[ $code -eq 0 ] || [ $code -eq 2 ] || exit $code
uv run python -m dce.stats results/full.jsonl
```

Exit 3 and exit 4 both mean *stop and look*, not *resume harder*.

## Analysis

```bash
uv run python -m dce.stats results/full.jsonl
```

Prints the pre-registered primary comparison (arm B vs arm C on
`deepseek/deepseek-v4-pro-0813`, paired McNemar) first and labelled, then
everything else under a SECONDARY / EXPLORATORY heading. Each slice reports
SCORED and STRICT accuracy with Wilson intervals, the harness-failure
breakdown, `db_corrupted` counts, both cost figures, mean input/output/cached
tokens and turns, and — for arm `contract` only — the governed-tool counters,
which are descriptive instrumentation and not a governance claim.

Two things to check at the **smoke** run, not after the sweep:

1. **`cached total` on `manual_prompt`.** Arm B carries the whole manual in
   its prompt. If OpenRouter prompt-caches that arm and not the others, the
   cost comparison measures the provider's caching policy rather than the
   arms — and because `dce.pricing` bills cache reads at the full input rate,
   it is invisible in the dollar figures. This line is the only place it shows.
2. **`did NOT see the same task set`.** A sweep that stopped mid task-group
   leaves the arms with different denominators.

## Accepted gaps

Known, deliberate, and disclosed rather than fixed:

* **`KeyboardInterrupt` during a live model call escapes with no row.** Ctrl-C
  while a request is in flight loses that one unit's result *and* its spend:
  the money was billed, no row was written, and the guard ledger never sees
  it. Every other failure path between "the call returned" and "the row is on
  disk" writes the row first.
* **`flush()` is not `fsync()`.** A written row survives this process dying —
  a crash, an exception, `SIGKILL` — because the bytes are in the OS page
  cache. It does not survive the *machine* dying before the OS flushes that
  cache. "Reaches disk" here means "survives process death", not "survives
  power loss".
* A torn final line (a full disk, a killed write) is skipped by every reader
  with a warning, costing that one row. A corrupt line anywhere else still
  raises loudly.
* Answers are graded by **DABStep's own scorer**, vendored verbatim at a
  pinned revision in `vendor/dabstep_scorer.py` (`dabstep_benchmark` is not on
  PyPI — it exists only inside the leaderboard Space). The local fallback
  normalizer now runs only if upstream's scorer raises. Each row records which
  graded it in its `scorer` field.

  This is load-bearing, not housekeeping. The fallback was *stricter* than the
  benchmark, and on the 12-task smoke run that manufactured a significant
  result in this library's favour which the official rules do not support
  (p=0.0156 against p=0.0703). See `vendor/README.md`.
