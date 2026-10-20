# Experiment 6: does progressive disclosure pay as the contract grows?

Status: draft, revised after a builder check and a methods review
(2026-10-20). Closes gap 8 of `docs/paper-plan.md`. Builds on experiment 4
(`prep/procedures/`, run commit `05015db`) and experiment 4b (`a6342d0`,
results `e1467c2`, FINDINGS *Gaps the documentation leaves open*).

## Why

A data contract does two things for an agent. It governs the tools: queries
are checked against what the contract allows. And it discloses
progressively: the prompt carries an index (domains, allowed tables, rules),
and the agent fetches a domain's text or a metric's SQL when a question
needs them (`lookup_domain`, `lookup_metric`, `list_metrics`).

The experiments so far tested the governance and the knowledge, not the
disclosure. With knowledge held equal, the governed arm and the same
contract's files in the prompt (`contract_text`) tie on accuracy: in
experiment 4b on all three models, and in experiment 5 on DABStep. On cost,
disclosure has never paid:

- In 4b the governed arm brought 13-43% less input into context than
  `contract_text`, but took more turns on 39 or more of 41 tasks on every
  model. It cost more in dollars on luna and Qwen and tied on sol.
- Fetched text is billed fresh; a static prompt is a cached prefix. Each
  lookup is a round trip that re-sends the prefix.
- One database's contract is about 20k tokens. Loading all of it up front
  is nearly free, so disclosure has nothing to save.

Disclosure should pay only when the contract is large: one contract
covering many domains, and a question that needs one of them. Then the text
arm carries everything on every turn and must find the relevant rules among
many irrelevant ones, while the governed arm fetches what it needs; its
index grows too, but slowly. Past a model's context window the text arm
cannot run at all. Nothing in the paper measures where any of this starts.

**The question.** As a contract is diluted with real domains a question
does not need, at what size does the contract in the prompt lose accuracy
or cost against the same contract disclosed through the governed tools?

**What it does not ask.** Each question's prompt shows its own database's
schema, so the agent never has to find which domain a question belongs to.
The test is dilution of the context by contract text the question does not
need, with single-domain questions. Finding the domain, and questions that
span domains, are untested and stated as such.

## Goals

- Measure accuracy and cost of the two contract deliveries at five sizes,
  from one database's contract to about twenty, on the three models of 4b.
- Start at the ceiling, so any drop is attributable to size.
- Make the padding indistinguishable in style from the relevant domain.
- Register predictions, their tests and their power before any full run,
  with a reading for each outcome, including size costing the text arm
  nothing.
- Record, per model, the size at which each arm no longer fits.
- Compare against the deployment default, all the prose documentation in
  the prompt, at the base and the largest common size.
- Measure the mechanism: how often each arm reaches into padding.

## Non-goals

- Changing the library for this test. The governed arm is the one of
  experiments 4 and 4b: `contract.to_system_prompt()` without a semantic
  source, so its prompt carries the domain index, the allowed tables and
  the rules, and no metric index. `list_metrics` behaves as shipped. A
  defect the test exposes is a finding and a later fix.
- Padding the schema. Only the contract (and, in the prose arm, the KB)
  grows.
- Retrieval baselines over contract chunks.
- New knowledge or new questions.

## Anti-goals (a failure even if it runs)

- A size effect that is really a style cue, a position effect, a harness
  limit (token cap, trace cap), a scheduling effect on caching, or a
  prompt the route silently truncated.
- Padding that contradicts or changes the relevant domain: name
  collisions, rules that change what is enforced on base queries, tools
  whose answers about base tables change with size.
- Levels that differ in anything but the padding.
- Predictions or tests adjusted after the smoke.

## Materials

### Base: experiment 4, at the ceiling

Experiment 4's three databases (reverse_logistics,
labor_certification_applications, solar_panel), its 60 questions, its
complete KB, and its contracts (`contracts-procedures/livesqlbench/<db>/`,
written by `dce.author` and checked equal to the reference on every
record). Every knowledge arm of experiment 4 scored 98-100% on luna and
sol. The base contract enters every level **unchanged**, so 1x is
experiment 4's contract exactly.

### Padding: the other 19 databases, re-authored

The existing compiled contracts (`contracts/livesqlbench/`) cannot serve:
all 22 share the same two generic domains (`calculations`, `definitions`)
with identical summaries, while the base contracts have 5-10 specific
domains each. The relevant domain would be labelled by its style.

So each of the 19 other LiveSQLBench databases gets a contract from
`dce.author`, the author of the base contracts, with the same model and
settings: one draw from the database's KB and data, no questions shown, no
check. These are padding; their correctness is not measured. They live in
`contracts-scale/padding/<db>/` with their author logs.

### Order and levels

Per base database, the padding order is:

1. the other two base databases' contracts first. Same author, and each
   has a rule-table procedure and composites like the base's: the nearest
   distractors available;
2. then the 19 re-authored contracts, in a seeded permutation per base
   database (seed 2026 and the database name).

Levels take padding in that order until the merged contract reaches its
target, so each level contains the previous one. Targets are multiples of
the base contract's size in tokens, measured with each model's reported
input tokens (not estimated from characters):

| level | target | padding |
|---|---|---|
| 1x | the base contract alone | none |
| 2x | about twice | the two near databases |
| 4x | about four times | plus re-authored, in order |
| 8x | about eight times | plus re-authored, in order |
| all | about 20 times | all 21 |

### Merging

One merged contract per base database and level, as one `contract.yml`
and one `semantic.yml`.

- **Namespaces.** Each padding database is a schema of its own name. Its
  tables are qualified (`<db>.<table>`) in allowed tables, metric SQL
  (sqlglot), relationships and prose; its domain and metric names are
  prefixed `<db>.`, and its own metric names in its prose are rewritten by
  exact identifier match. The base stays in `main`, unqualified and
  unprefixed. This is a cue, but the same one the question's schema gives;
  it is stated with the framing above.
- **Rules.** The padding contracts' rules are dropped. Library rules
  without a table apply to every query, so kept padding rules would change
  what is enforced on base queries from 2x up (`no_select_star` blocks
  `SELECT *` on bases that do not have it at 1x). The base's own rules
  are the only rules at every level.
- **Data.** One working database per base database, with the tables of
  all 21 other databases copied into schemas of their names, the same at
  every level. Not `ATTACH`: an attached database is a catalog whose
  tables sit in `main`, and the library's `describe_table` then merges the
  columns of same-named tables across catalogs into the base's tables
  (reverse_logistics' `products` and `orders` collide). Copied schemas keep
  `main` the base's alone.
- **Position.** The base's sections sit at the boundary between padding
  databases nearest the midpoint by size; at 2x, between the two near
  databases. The middle is where long-context models read worst, so this
  is the harder placement for the text arm; a position control (below)
  measures how much it matters.

### Checks (in the materials commit)

- Each merged contract loads; table, domain, metric and rule names are
  unique.
- Every table the merged contract allows exists in the working database.
  Padding metric SQL is not required to run (the padding is not checked).
- The base sections are byte-identical to experiment 4's contract at every
  level.
- The governed prompt at each level differs from 1x only by the padding's
  index lines; its size per level is recorded.
- `describe_table` on every base table returns the same columns at every
  level, and the validator allows and blocks the same base queries at
  every level (a fixed probe set).
- Each level's size in tokens for each model, from one call per level.

### Context windows

Before the run commit, each route is probed for its input limit: a prompt
of known length, grown until the route refuses or the reported input tokens
fall short of the prompt. An arm-level fits a model if its prompt plus the
95th percentile of `contract_text`'s conversation growth in 4b is under the
limit. One that does not fit is not run and is recorded as **does not
fit**.

During the run, every call asserts that the reported input tokens are at
least the expected prompt tokens; a shortfall fails the run loudly rather
than scoring a truncated prompt.

## Arms

- `contract_scaled`: the governed arm over the merged contract, at all
  five levels.
- `contract_text_scaled`: the merged contract's two files verbatim in the
  prompt over the plain tools, introduced as `contract_text` is, at all
  five levels.
- `manual_scaled`: the deployment default. The base's experiment 4 KB plus
  the padding databases' KBs, rendered as `manual_prompt` renders a KB,
  each database under a heading of its name, in the same order and
  position as the contract. At 1x and at the common top level only.
- `contract_text_first`: the position control. `contract_text_scaled`
  with the base's sections first, at the common top level only.

Up to 13 arm-levels per model. The three contract arms carry the same
knowledge at every level; the prose arm carries the KB the contract was
written from.

## Runs

gpt-6-luna, gpt-6-sol and qwen3.8-27b, k=3, the arm-levels together in each
repeat, graded as LiveSQLBench is with experiment 4's golds. Fixed in the
run commit:

- **Prompt order.** The system prompt is the base context (schema, column
  meanings, procedure) then the contract or KB, identical across a base
  database's questions at a given arm-level; the question is the user
  message. Runs are grouped by database, level and arm, so each prefix is
  reused while it is cached. The worker counts are fixed.
- **Token cap.** The harness's `TOKEN_BUDGET` (50 requests' worth, sized
  for 4b's prompts) would end the text arm's runs at "all" after six or
  seven requests: a harness-made size effect. For this experiment the cap
  is raised so that it never binds before `REQUEST_LIMIT` at any level;
  the turn limit is then the same stop for every arm-level.
- **Errors.** A context overflow, from the prompt or from a tool result (an
  unfiltered `list_metrics()` at "all" returns about 590k characters),
  ends the run, scores it wrong, and is not retried: it is deterministic,
  and it is a finding. Transient errors (rate limits, timeouts) are retried
  as usual. Both are counted per arm-level.
- **Traces.** No trace may be truncated by the 8 MB cap; the mechanism
  counts come from complete traces.
- **Spend cap.** `DCE_MAX_SPEND` at twice the estimate made from the smoke.

Measured per run: verdict, turns, tool calls, fresh and cached input,
output and reasoning tokens, dollars, latency. Mechanism per run:
governed lookups that land in padding (a padding domain or metric name),
and each tool result's size; for the plain-tool arms, submitted SQL that
names a padding table or contains a padding metric's SQL. Cache-hit rate
per arm-level.

## Predictions (fixed in the run commit; the smoke may not change them)

**Level and unit.** The primary level is the **common top**: the largest
level at which every arm fits on every model. The unit is the task, scored
0-9 over models and repeats; one-sided sign tests, ties dropped. Each
model's own top level is reported as secondary.

**Power.** Before the run commit, the minimum detectable drop at the common
top is simulated from experiment 4's per-task outcomes at k=3. If it
exceeds 10 points, k is raised for the text arm at 1x and the common top
until it does not, and the run commit records both.

- **(37) Size costs the text arm accuracy.** `contract_text_scaled` at the
  common top is below itself at 1x (p < 0.05, the direction on each model).
  Its **equivalence** reading: the 95% interval of the mean drop
  (bootstrap over tasks) lies within ±5 points.
- **(38) Disclosure holds it.** Per task, `contract_scaled`'s drop from 1x
  to the common top is smaller than `contract_text_scaled`'s (sign test on
  the difference of drops).
- **(38b) Size costs the governed arm accuracy.** `contract_scaled` at the
  common top is below itself at 1x. Registered as the test that tells
  "size hurts both" from "the difference was too small to show".
- **(39) Disclosure is cheaper at size.** At the common top,
  `contract_scaled` costs fewer dollars per run than
  `contract_text_scaled`, on luna and on sol (each model's sign test over
  each task's mean cost). Qwen's route meters output only, so its prompt
  is free and its dollars measure behaviour; Qwen is reported in input
  tokens processed instead. All three are also reported under one fixed
  price, fully cached and fully uncached, so the cost result does not rest
  on how a route bills.
- **(40) Disclosure runs where the text cannot.** On every model and level
  where `contract_text_scaled` does not fit, `contract_scaled`'s accuracy
  minus its 1x accuracy has a 95% lower bound above -5 points. The 5 points
  are set against the temperature-0 flip rate of run E.

Reported, not predicted: the curves per arm and model; cost and cost per
correct answer per level; `manual_scaled` against both contract arms at 1x
and the common top; `contract_text_first` against `contract_text_scaled`
at the common top; the mechanism counts; overflow and error counts;
cache-hit rates; the crossover level, the smallest at which the governed
arm leads on accuracy or cost.

How the outcomes read, fixed now:

- **(37) equivalent.** Up to the common top, putting the whole contract in
  the prompt costs no accuracy. Disclosure's case is then cost, if (39)
  holds, and the contracts too large to fit, by (40).
- **(37) neither holds nor equivalent.** Inconclusive at this power; the
  paper says so, with the interval.
- **(37) and (38) hold.** Disclosure keeps accuracy where the prompt loses
  it: direct support for the progressive-disclosure claim, at the size
  where it starts.
- **(37) holds, (38) fails, (38b) holds.** Size hurts both deliveries; the
  index and lookups do not isolate the relevant domain. Read from the
  mechanism counts as a finding about the library's index.
- **(37) holds, (38) and (38b) fail.** The governed arm's drop is not
  shown, and neither is its advantage: inconclusive between "holds" and
  "hurts both".
- **(39) fails.** At these sizes caching keeps the static prompt cheaper;
  the cost case for disclosure lies beyond the sizes measured or on routes
  without a cache, as the fixed-price uncached figure will show.
- **Position.** If `contract_text_first` recovers most of a drop in (37),
  the drop is mostly "lost in the middle", and the paper says the text arm
  depends on where the relevant domain sits, which a deployment does not
  control.

## Order of work

1. Probe the three routes' input limits.
2. Author the 19 padding contracts with `dce.author` (one draw each, author
   logs kept).
3. The merge, test-first: namespacing, SQL qualification, rule dropping,
   the working databases, the order and position.
4. Materials commit: merged contracts for 3 databases x 5 levels (plus the
   position control's), the prose arm's KBs, the checks above, the sizes
   per model.
5. The arms and the harness changes, test-first: the token cap, overflow
   scoring without retry, the per-call truncation assertion, trace
   completeness, the mechanism counters, run grouping.
6. The power simulation.
7. Smoke: 12 stratified questions, one model, every arm-level that fits;
   check each arm's prompt in a trace.
8. Run commit with (37)-(40), the tests, the power, the fit table and the
   readings; then the cluster run as for 4b.
9. `analysis/disclosure_scale.py`, FINDINGS, the paper.

## Constraints

As for 4b: `uv run` with `UV_FROZEN=1`; prek on every commit; no gold,
test cases or private LiveSQLBench material committed (the padding
contracts are derived from the public KBs, as `contracts/livesqlbench/`
is); no internal names in publishable files; hosts redacted from logs and
results before commit; job manifests only in `~/data/dce-jobs/`; PVC
deletes only with the user's OK; no rebasing run commits; no AI attribution
anywhere.

## Decisions

Made by the user (2026-10-20):

- The padding is re-authored with `dce.author`, the other two base
  databases first.
- The prose arm runs at 1x and the common top only.

Made in revision, from the builder check and the methods review:

- The base contract is unchanged at every level; only the padding is
  namespaced.
- The padding's rules are dropped.
- The padding data is copied into schemas, not attached.
- The governed arm stays as in 4b (no metric index).
- Overflows score wrong without retry.
- The token cap is raised for this experiment.
- The primary level is the common top.
- (39) is tested on luna and sol only.
- (37) has an equivalence reading.
- (38b) is added.
- A position control is added.
- A seeded padding order per database is used.
- Power is simulated before the run commit.
- Each call is checked for truncation.

## Left to the builder

- The file layout under `contracts-scale/`, and how the harness names
  arm-levels, as long as every row records its arm, level and fit.
- How the probe and the per-call assertion count expected tokens, as long
  as they are checked against reported input tokens.
- The concrete detector for "contains a padding metric's SQL".
- The smoke's question sample, worker counts and the trace-cap fix.
