# DABStep contract-context evaluation

**Status:** design, awaiting review
**Date:** 2026-08-30
**Location:** `experiments/dabstep-contract-eval/`

## Why

The library's central claim is that governed, contract-delivered context makes an
agent's SQL better. That claim has never been measured end to end. The one
experiment in the repo, `experiments/mermaid-joinpath-eval`, measures a
*rendering format* (XML vs Mermaid vs NL adjacency) for one narrow sub-task —
join-path reconstruction. It says nothing about whether a contract improves
answers.

MotherDuck's [`agentic-sql-context-mcp`](https://github.com/motherduckdb/labs/tree/main/projects/agentic-sql-context-mcp)
published **417/419 (99.5%)** on DABStep at ~$0.020/question, using the same
underlying thesis — curated business context served on demand beats letting the
agent reverse-engineer the warehouse — but delivered as prose guides through an
MCP server, with nothing enforcing them. Their headline finding is that
*high-leverage rules must live in the always-on prompt, not in fetch-gated
context items, or a low-reasoning model skips them.*

This experiment produces the number this library is missing, and situates it
against that published result.

DABStep is a good fit because its difficulty is domain-rule reasoning, not SQL
syntax: the benchmark ships a `manual.md` of fee rules, merchant categories and
account types, and the questions are unanswerable without it. That manual is
precisely the kind of knowledge a contract plus a semantic source is supposed to
encode.

## The claim under test

> On DABStep, an agent given the contract and semantic source through the
> library's nine tools scores at least as well as the same agent given DABStep's
> full `manual.md` in its system prompt, while carrying a smaller always-on
> prompt — and both beat a schema-only floor.

**Non-goals.** Reproducing MotherDuck's harness (their number is cited, not
re-run). Establishing the blocking value of `inspect_query` as a governance
mechanism — DABStep is not built to expose that, and a claim there needs its own
constructed failure cases. The `inspect_query` counts reported under Metrics are
**descriptive instrumentation of arm C's behaviour, not a claim**, and
`FINDINGS.md` must present them that way. Submitting to the DABStep leaderboard.

## Arms

Three arms. Identical model, agent loop, turn cap, retry policy and budget. They
differ **only** in what context reaches the agent.

| Arm | Name | Context given | Role |
|---|---|---|---|
| A | `schema_only` | Table and column names, plus an ungoverned query tool. No manual, no metrics, no contract. | Floor |
| B | `manual_prompt` | Arm A, plus `manual.md` and `payments-readme.md` verbatim in the system prompt. | Strong baseline |
| C | `contract` | The nine tools over a frozen `contract.yml` + `semantic.yml`. No manual in the prompt. Progressive disclosure via `lookup_domain` / `list_metrics` / `lookup_metric`; every query passes `inspect_query` before `run_query`. | The library |

Arms A and B run against a **plain DuckDB tool** — schema listing and query
execution with no contract loaded — not the library's `run_query`. If they used
the library's execution path they would inherit contract enforcement, and arm C
would no longer be the only governed arm. Arms A and B do get the same row cap
and the same result-formatting as arm C, so no arm is advantaged or penalized by
result truncation; the cap is a harness property, not a contract property.

**The turn budget is uniform and counts every tool call.** Arm C's
`inspect_query` validation-and-retry cycles consume its own budget; they are not
free extra attempts. Without this, arm C would effectively get more shots at each
question than arms A and B, and any win would be confounded with attempt count.

Arm B is the load-bearing comparison. Arm A only establishes that context
matters at all, which is not in dispute. Beating or matching B at a smaller
always-on prompt size is the result that belongs to this library.

If C ties B on accuracy, that is a real and reportable outcome: the finding then
becomes token efficiency plus the structural argument that a metric definition
cannot be misread the way a paragraph of prose can. The spec commits to
reporting a tie as a tie.

## Freeze discipline

The contract is authored **only** from `manual.md` and `payments-readme.md`. It
is committed, and its `contract_digest()` recorded, **before the first scored
run**. Arms B and C then carry provably identical knowledge in different form,
so the comparison isolates representation rather than content.

The digest is written into every result row. A post-hoc contract edit is
therefore detectable by anyone reading `results/*.jsonl` against git history —
it is not a promise made in prose. Any edit to the contract after scoring begins
invalidates the run, and the run restarts from scratch.

No question text, no gold answer, and no score is consulted while authoring.

## Data

### Sources

From the HuggingFace dataset `adyen/DABstep`, into a local gitignored DuckDB
file (five tables):

- `payments.csv`, `fees.json`, `merchant_data.json`, `acquirer_countries.csv`,
  `merchant_category_codes.csv`

Plus, as prompt/contract source material only: `manual.md`, `payments-readme.md`.

Tasks come from the `tasks` config: `default` (450 tasks, `answer` field empty)
and `dev` (10 tasks, answers published).

### Gold reconstruction

DABStep withholds gold answers for the 450 test tasks. MotherDuck nonetheless
scored 419 locally, and the available route is the dataset's own leaderboard
artifacts:

1. Download `data/submissions/*.jsonl` (~500 submitted answer files) and
   `data/task_scores/`.
2. For each task, collect the answers from every submission that `task_scores`
   marks **correct** on that task.
3. Accept a gold only when **at least two independent submissions agree** after
   DABStep normalization. One correct-scored submission is not enough — a
   normalized comparison can pass by luck, and MotherDuck's `bad_golds.json` is
   evidence that even official golds contain errors.
4. Tasks without consensus are **excluded and counted explicitly** in
   `FINDINGS.md`. Coverage is reported, never silently absorbed.

Golds freeze to `data/golds.json` with a content hash, recorded in every result
row alongside the contract digest.

### Selection bias — stated, not mitigated

Reconstructed golds exist only for tasks that **at least two submitted agents
already answered correctly**. Tasks no agent ever got right yield no gold and are
excluded — and those are disproportionately the hardest tasks. The scored set is
therefore easier than the true 450, and **every accuracy number this experiment
produces is biased upward relative to the full benchmark.**

This cannot be fixed without the official golds, so it is disclosed instead:
`FINDINGS.md` reports gold coverage broken down by `level` and by template, so a
reader can see exactly which slice was scorable. Accuracy is never quoted against
"DABStep" unqualified — always against "the reconstructed-gold subset (n=…)".

Cross-arm comparisons are unaffected: all arms face the identical task set, so
the paired tests remain valid. Only the absolute number is inflated, which is
also why comparing it directly to MotherDuck's 99.5% would be unsound.

### The gate

Reconstruction must reproduce **all 10 published `dev` answers exactly**.

**This gate's feasibility is itself unverified** and is the first thing the
implementation checks: it requires that the 10 `dev` tasks appear in
`data/submissions/` and `data/task_scores/` at all. If they do not, the gate
cannot run as designed, and the substitute is a **hand-verified sample**: write
SQL by hand for 10 reconstructed golds stratified across templates and confirm
each matches. That is weaker evidence and is labelled as such.

**Fallback if either form of the gate fails:** report on the 10 `dev` tasks only,
and prepare a leaderboard submission for the full set. This yields a much weaker
but honest result, and `FINDINGS.md` says so plainly.

## Harness

Pydantic AI over OpenRouter, driving the library through
`create_pydantic_ai_tools`. Local DuckDB via the existing
`adapters/duckdb.py` — no MotherDuck dependency.

### Models

All model ids are **pinned snapshots**. An unpinned id can silently re-point to
a new snapshot mid-run, putting two different models in one results file with no
column recording it. Model id is stamped into every row.

| Model | In / Out ($/M) | Scope | Est. |
|---|---|---|---|
| `deepseek/deepseek-v4-flash-0731` | 0.065 / 0.18 | Full, weak arm | ~$3 |
| `deepseek/deepseek-v4-pro-0813` | 0.66 / 1.98 | Full, strong arm | ~$32 |
| `z-ai/glm-5.3-flash` | 0.075 / 0.25 | Full, cross-family control | ~$4 |
| `openai/gpt-5.6-sol` | 2.00 / 10.00 | Hard subset only (~60 tasks x arms B,C) | ~$10 |

**~$49 total — but see the asymmetry below; treat this as a floor, not a
forecast.**

**Arm B is structurally the most expensive arm.** `manual.md` sits in its system
prompt and is therefore re-sent on *every* turn of a multi-turn tool loop. At a
plausible ~15k-token manual and ~10 turns, arm B alone can approach ~150k input
tokens per question — roughly 5x the ~30k blended assumption these estimates are
built on. The per-arm split matters more than the total, so the runner tracks
spend per `(arm, model)` and the smoke run reports the three arms' costs
separately before the full sweep is authorized.

Two consequences. First, this asymmetry is not a nuisance — a large part of arm
C's value proposition *is* that it does not re-send a manual every turn, so the
cost gap is itself a headline secondary result. Second, prompt caching would
disproportionately rescue arm B; whether OpenRouter applies it per provider is
checked during the smoke run and recorded, because a silently cached arm B makes
the cost comparison meaningless.

The two DeepSeek snapshots are the capability axis: same family, same tokenizer
and tool-calling conventions, same reasoning-token accounting, so a weak-vs-
strong difference reads as capability rather than as vendor behaviour. This is
the same instinct as the anonymization step in `mermaid-joinpath-eval` — make
sure the axis you vary is the only axis that moved. `glm-5.3-flash` is the
cross-family control confirming the effect is not DeepSeek-specific.

`gpt-5.6-sol` answers only the ceiling question — does contract context still
help a frontier model, or is it a weak-model crutch? — so it runs only on the
hardest tasks and only on the two arms that discriminate. Easy tasks where every
arm scores 100% cost flagship rates to learn nothing; `mermaid-joinpath-eval`
hit exactly this with Spider's shallow joins.

**The subset is selected before any run, by a rule fixed here**: all
reconstructed-gold tasks with `level == "hard"`, and if that exceeds 60, a
deterministic seeded sample of 60 stratified across templates. Selecting the
subset after seeing which tasks the other models missed would turn a ceiling
check into a search for a favourable slice. The chosen task ids are written to
`data/sol_subset.json` and committed before the Sol run.

Reasoning mode is set explicitly per model rather than left to default. The
`-pro` served variants are priced identically to their base variants and differ
only in reasoning mode, which means unpinned reasoning turns per-question cost
into a function of how hard the model decides a question is.

### Determinism and repeats

Temperature 0 and a fixed seed wherever the provider honours them. Reasoning
models remain non-deterministic in practice, so this reduces variance rather than
eliminating it.

Each `(task, arm, model)` runs **once** in the main sweep. Single-sample runs
cannot separate a real arm difference from sampling noise on any individual task,
which is acceptable because the paired tests aggregate over hundreds of tasks —
but it means **no per-task claim is ever made**, only distributional ones.

If the primary B-vs-C comparison lands near significance, the documented response
is a **3-sample repeat of that one pair on the weak model** (~$9), not a
reinterpretation of the single-sample result.

### Per-task caps

Max turns, wall-clock, and per-task USD. A task that trips a cap is recorded as
`HIT_LIMIT`, never as an incorrect answer — cap trips are reported per arm so a
harness artifact cannot masquerade as a reasoning effect.

The answer must follow the task's own `guidelines` string, which DABStep ships
per task.

## Scoring

`dabstep_benchmark.evaluation.scorer.question_scorer` when installable;
otherwise the same fallback normalization MotherDuck implemented — case-
insensitive match, bracket/quote normalization, N/A equivalence, float tolerance
at 1e-9 relative, order-insensitive list comparison, prefix match for
non-numeric singletons.

Both the raw answer and the normalized answer are recorded, so a scoring dispute
can be re-adjudicated without re-running the model.

### Result row

Every row records: `task_id`, `level`, `template`, `arm`, `model`, raw answer,
normalized answer, verdict, turns, prompt tokens, completion tokens, USD,
tool-call sequence, `contract_digest`, `golds_hash`, the experiment's **git
commit sha**, the resolved `agentic-data-contracts` version, and any
`inspect_query` rejections.

The commit sha matters as much as the contract digest: it makes a mid-sweep
harness change — a reworded prompt, a changed cap — visible as a discontinuity in
the results file rather than an invisible one.

Template labels come from MotherDuck's `data/split.json` (T01–T26), cited as an
external mapping.

## Metrics

**The primary comparison is pre-registered here as a single test:** arm B vs
arm C on `deepseek-v4-pro-0813`, paired McNemar, over the reconstructed-gold
task set. Every other comparison — arm A, the other two models, all strata — is
**secondary and exploratory**, and `FINDINGS.md` labels it so.

This matters because the design otherwise runs 3 arms x 3 models x 2 levels x 26
templates worth of possible tests, and at that count something crosses p<0.05 by
chance alone. Naming the one confirmatory test in advance is what keeps the
result from being a search.

**Primary.** Accuracy per arm with Wilson confidence intervals, plus paired
**McNemar** tests for A-vs-C and B-vs-C. Every task is seen by every arm, so the
paired test is the correct one — the same treatment `mermaid-joinpath-eval`
applied across renderings.

**Discordant pairs are reported alongside every McNemar result**, because
McNemar sees only the tasks where two arms disagree. Near the ceiling — and
MotherDuck reached 99.5% on this benchmark — discordant pairs become scarce and
the test loses power fast. A non-significant result at ceiling means "this design
could not tell", not "the arms are equal", and `FINDINGS.md` must say which of
the two it is.

**Secondary.** Tokens and USD per question, and always-on prompt size per arm —
the direct analog of MotherDuck's 3.4x prompt-size reduction claim.

**Stratified.** By DABStep level (easy / hard) and by template.

**Arm-C only.** How often `inspect_query` rejected a candidate query, and
whether the retry then scored correct. This is the one number here that no other
DABStep result can report.

**Hygiene.** Turn-cap and truncation rates per arm, so an asymmetric rate across
arms is visible rather than buried.

## Layout

A standalone `uv` project, mirroring `mermaid-joinpath-eval`:

```
experiments/dabstep-contract-eval/
  pyproject.toml          # path dep on agentic-data-contracts
  README.md               # setup + how to run
  FINDINGS.md             # method, results, conclusion
  contract/
    contract.yml          # frozen, authored from manual.md only
    semantic.yml
  dce/
    data.py               # HF fetch -> DuckDB
    golds.py              # reconstruction + dev-10 gate
    arms.py               # the three context configurations
    agent.py              # Pydantic AI loop, caps, cost accounting
    grade.py              # DABStep scorer + normalization
    runner.py             # CLI
    stats.py              # accuracy, Wilson, McNemar, strata
  results/*.jsonl
  tests/
  data/                   # gitignored: duckdb, golds.json, HF cache
```

## Testing

Deterministic offline `pytest`, no network:

- gold reconstruction (consensus rule, exclusion counting, dev-10 gate)
- scorer normalization edge cases, including the float-tolerance and
  order-insensitive-list paths
- arm prompt/tool assembly — that arm A's prompt contains neither the manual nor
  any metric definition; that arm C's prompt contains no verbatim manual text;
  that arms A and B are wired to the ungoverned DuckDB tool and only arm C
  reaches `inspect_query` / `run_query`; and that all three share one row cap
- cost accounting and `--max-spend` enforcement
- statistics: Wilson intervals and McNemar against hand-computed fixtures

Per the repo's TDD convention, tests precede implementation. Per
`feedback_plan_derived_tests`, one review pass reads the tests against this spec
rather than against the code.

## Spend control

- `--max-spend` is a hard stop, checked before each model call.
- Results are resumable: a completed `(task, arm, model)` row is skipped.
- A smoke run — `--n 12 --max-spend 2.00`, all three arms — precedes the full
  sweep. **The smoke run's measured tokens/question replaces the estimates in
  this document before any full sweep commits money.** The estimates here are
  back-solved from MotherDuck's single published figure (~30k in / 2k out per
  question) and should be treated as a ranking, not a forecast.
- `OPENROUTER_API_KEY` is sourced from a `.env` outside the repo via
  `LENS_ENV_FILE`, as in `mermaid-joinpath-eval`.

## Risks

**Gold reconstruction fails the dev-10 gate.** Mitigated by the documented
fallback; it is the single largest risk and is resolved before any spend.

**`payments.csv` size.** The HF file listing reports an implausible figure for
this file. Actual size is confirmed during `data.py` implementation; if the load
is impractical locally, the DuckDB build filters to the columns the benchmark
uses.

**Arm C loses to arm B.** A real possible outcome, reported as such. The
mechanism to investigate first would be MotherDuck's own finding — that
fetch-gated context gets skipped by weaker models — which the weak/strong
DeepSeek pairing is positioned to detect.

**Ceiling effect.** If all three arms cluster near 100% on the scorable subset,
the experiment cannot discriminate them and the honest conclusion is that DABStep
is saturated for this question — the same outcome Spider's shallow joins produced
in `mermaid-joinpath-eval`. Detected at the smoke run, where near-identical arm
accuracy on 12 tasks is an early warning; the response is to re-cut against the
`hard` stratum only rather than to spend the full sweep.

**Arm B silently benefits from prompt caching.** Would invalidate the cost
comparison that is arm C's main expected win. Checked and recorded at smoke time.

**Contract authoring quality is a confound.** A badly authored contract measures
the author, not the library. Mitigated by freezing before any score is seen, and
by arm B carrying identical source knowledge.

## Reporting

Results live in `experiments/dabstep-contract-eval/FINDINGS.md`. The main README
gets at most a one-line pointer. Unreplicated single-run numbers stay off the
project's marketing surface; promotion is a separate decision made after reading
the results.

Per `feedback_doc_lifecycle`, this spec and its implementation plan are process
scaffolding, removed once the work ships. `FINDINGS.md` is the durable record.
