# DABStep eval — smoke-run findings and cap re-plan

**Date:** 2026-08-30
**Amends:** `docs/superpowers/specs/2026-08-30-dabstep-contract-eval-design.md`
**Evidence:** `experiments/dabstep-contract-eval/results/smoke12.jsonl` (3 rows, task 1712, `z-ai/glm-5.3-flash`, commit 23c4576)

The smoke run was stopped after the first task. All three arms failed on it,
each for a *different* reason — which makes one task a complete diagnostic of
the harness rather than a data point about the arms.

| Arm | verdict | input | cached | ctx/req | tools | turns | answer |
|---|---|---:|---:|---:|---:|---:|---|
| schema_only | hit_limit | 58,076 | 80% | 5,807 | 28 | 10 | *empty* |
| manual_prompt | error | 153,981 | 83% | 11,844 | 21 | 13 | `max_tokens (4000) exceeded` |
| contract | hit_limit | 781,609 | 87% | 45,977 | 22 | 17 | *empty* |

Gold was `12.91`. No arm produced any answer at all.

## F1 — `MAX_OUTPUT_TOKENS_PER_REQUEST = 4_000` is fatal to reasoning models

`z-ai/glm-5.3-flash` is a reasoning model. Measured directly against the
OpenRouter API: a trivial 50-token completion reported
`completion_tokens_details.reasoning_tokens: 49`. Reasoning tokens count
against `max_tokens`, so the cap can fire *before any answer text exists* —
which is exactly the error arm B returned:

> `UnexpectedModelBehavior: Model token limit (4000) exceeded before any response was generated.`

**This is a confound, not merely a bug.** The cap is nominally uniform across
arms, but it killed only arm B, because arm B's larger prompt induced longer
reasoning. A cap that preferentially kills the arm carrying the most context
biases the very comparison the experiment exists to make.

**Fix:** `MAX_OUTPUT_TOKENS_PER_REQUEST: 4_000 -> 16_000`.

## F2 — `hit_limit` rows are paid for and carry no signal

Two of three arms exhausted their budget and returned `""`. `verdict=hit_limit`
is unscoreable: it contributes nothing to accuracy while costing full price.
Across a 1,800-run sweep this is a large, silent waste channel.

**Fix — the forcing turn.** On `UsageLimitExceeded`, re-ask once using the
captured message history with **an empty toolset** and a prompt demanding the
final answer now. `capture_run_messages` is already wired into `run_task`, so
the transcript is in hand at the moment the exception fires. This converts an
unscoreable waste row into a scoreable right-or-wrong one.

The forcing turn is applied identically to every arm, and its extra request is
recorded on the row (`forced_answer: bool`) so no analysis can mistake a forced
answer for an unforced one.

## F3 — OpenRouter provider routing is per-request and unpinned

`z-ai/glm-5.3-flash` exposes **20 endpoints** spanning fp4 / fp8 / unknown
quantization, $0.05–$0.15 per 1M input, and 262K–1.31M context.

Measured: two *identical, back-to-back* calls were served by **Z.AI**, then
**DeepInfra**. Routing is per-request, not per-session — so a single task's
turns can hop providers, and therefore quantizations.

Three consequences:

1. **Quality confound.** fp4 and fp8 are different models in effect. Nothing in
   the results file records which served each call.
2. **Pricing error.** `dce/pricing.py` hardcodes the $0.075/$0.25 fp8 tier. Of
   20 endpoints, one is cheaper and eleven are 2x more expensive.
3. **Cache destruction.** Unpinned, the repeat call reported
   `cached_tokens: 0`. Pinned to Z.AI, the same repeat cached 4,224 of 4,228.

**Fix:** pin `provider: {order: [...], allow_fallbacks: false}` per model, and
record the serving `provider` on every result row so the pin is *verified from
the data*, not assumed.

## F4 — Cached input is billed at ~20% of standard, and our accounting ignores it

Measured against the live API, pinned to one provider:

| call | prompt | cached | actual | our formula | ratio |
|---|---:|---:|---:|---:|---:|
| 1 | 4,228 | 0 | $0.00031467 | $0.00031785 | 0.99 |
| 2 | 4,228 | 4,224 | $0.00006599 | $0.00032010 | 0.21 |
| 3 | 4,228 | 4,224 | $0.00007292 | $0.00032710 | 0.22 |

Cache reads bill at roughly **$0.015/1M against a $0.075/1M standard rate**.
`cost()` charges the full rate on every input token.

**This error is differential, which is what makes it serious.** The inflation
factor scales with cache-hit rate, which scales with conversation length:

| Arm | recorded | corrected | inflation |
|---|---:|---:|---:|
| schema_only | $0.00575 | $0.00296 | 1.94x |
| manual_prompt | $0.01425 | $0.00655 | 2.17x |
| contract | $0.06325 | $0.02255 | 2.81x |

The recorded cost ratio contract:schema_only is **11.0x**; the true ratio is
**7.6x**. Publishing the recorded figure would have overstated our own
library's cost penalty by 45%.

**Fix:** add `price_cached` to `ModelSpec`; bill `cached_tokens` at that rate
and `input_tokens - cached_tokens` at the standard rate.

## F5 — `MAX_ROWS = 1_000` poisons the context, arm-asymmetrically

Arm C averaged 45,977 input tokens per request against arm A's 5,807 — 8x. A
single 1,000-row `run_query` return stays in the conversation and is resent as
input on every subsequent turn. DABStep answers are aggregates; the agent
almost never needs 1,000 raw rows.

**Fix:** `MAX_ROWS: 1_000 -> 50`.

## F6 — `TOKEN_BUDGET` re-introduces the confound N1 was meant to remove

The plan's `GROWTH: int = 4` is a "deliberately blunt safety multiplier."
Measured growth (mean ctx/req over `MAX_ARM_FLOOR`) is arm-dependent:

| Arm | ctx/req | growth vs 6,100 floor |
|---|---:|---:|
| schema_only | 5,807 | 0.95x |
| manual_prompt | 11,844 | 1.94x |
| contract | 45,977 | **7.54x** |

So `TOKEN_BUDGET = 732,000` bound arm C at **22** tool calls while arm A ran to
**28** — the token guard, not `tool_calls_limit`, became the binding iteration
control, and it bound *earlier for the arm carrying more context*. That is
precisely the failure N1 recorded and claimed to have fixed by moving the guard
out of dollars and into tokens: the guard is uniform in tokens but not in
iterations, because growth is a property of the arm.

**Fix:** `tool_calls_limit` must be the single uniform iteration control, and
`TOKEN_BUDGET` must be sized never to bind first. F5 collapses arm C's growth
at the root; `GROWTH` is then raised to **12** (worst observed 7.54x with
margin) so the token guard is a genuine runaway stop rather than a de facto
iteration cap. Both are re-measured in the next smoke run before the sweep.

## F7 — Scope: the sweep fits neither the budget nor the calendar

- The dataset is **378 hard / 72 easy** (84% hard). A flash-tier model floors
  near zero on hard tasks in all three arms — 250+ tasks of 0-vs-0.
- The OpenRouter balance is **$129.60 remaining** ($120.40 of $250 used).
  Corrected full-sweep estimate: **~$208**. It does not fit. Under the *current*
  inflated accounting the `--max-spend` guard would have halted mid-sweep.
- Wall-clock: 4,050 runs x 4.5 min/run (measured: 13m47s for 3 runs including
  dataset load and DB build) = **~14 days serial**.

**Proposed (not yet approved):** stratify to all 72 easy + 128 random hard =
**200 tasks**; 200 x 3 arms x 3 models = 1,800 runs (~$66), plus a 60-task
`gpt-5.6-sol` hard subset x 2 arms (~$36) => **~$102**. Parallelise to 10
concurrent workers — runs are already isolated per-run via `make_working_copy`
and the work is pure network I/O — turning ~6 days into ~14 hours. The serial
spend ledger and single write site need a lock to stay correct under
concurrency; that is the only real work.

Whether to run on a VPS is open. With resume already working, a sleeping laptop
merely resumes, so a VPS is a reliability convenience rather than a
requirement.

### F7a — parallelism: done (`--workers N`)

Cost was never the constraint; wall-clock was. A glm sweep over the 401
golded tasks is 1,203 runs at ~$0.0033 each — about **$4** — and **61 hours
serial**.

`sweep` now dispatches whole *task groups* to a thread pool. The unit of
dispatch is the group, never an individual run, which preserves the two
properties the serial loop had for free: a truncation still lands on a task
boundary, and one task's arms still share one copy of the database. At
`workers=1` the behaviour, row order included, is identical to the serial
sweep.

Four pieces of state became shared. Three were the expected ones — the
results file (one lock-held write site), the spend ledger, the per-model
observation history. The fourth was not:

**The working copy has to be per worker, and that is not an optimisation.**
`check_and_restore` repairs a corrupted copy by copying the whole 26 MB
pristine file back over it. One shared copy would land that `copyfile` inside
another worker's live query — and, worse, `db_corrupted` would stop
attributing corruption to the arm that caused it, since any concurrent
worker's ungoverned arm could have been the one that mutated the shared file.
`db_corrupted` *is* this experiment's headline governance finding, so a
shared copy would not have been merely flaky; it would have been wrong.

**A near-miss worth recording, because it is a class of bug and not a typo.**
The first draft treated "this group does not fit against `spent + reserved`"
as budget exhaustion, exactly as the serial code did. But a reservation is a
worst-case *ceiling* ($0.824/call on glm) and real costs run ~100x smaller
(~$0.003), so at `--workers N` the Nth worker routinely fails that check
purely because N-1 ceilings are held right now. Measured on the regression
test against that draft:

> `stopping: $0.00 spent + $0.82 reserved in flight + $0.82 for task '1'`
> `would exceed $1.42`

— the whole sweep stopped after **one group**, having spent $0.40 of a $1.42
budget. On the real sweep that is a 1,203-run job quietly ending at run 3,
with a plausible-looking "stopped at the cap" message and no other symptom.
The fix: refuse permanently only when there is nothing in flight to wait for
(`reserved == 0`, which is exactly the serial condition), otherwise wait on a
condition variable for a release and re-check.

The general lesson, which is the same one F6 taught: **a guard whose serial
and concurrent readings differ is not a guard until you say which one you
mean.** Both halves are now pinned by tests — a blocked group must still run,
and a genuinely exhausted budget must still truncate.

Validated end-to-end against the live API: 4 tasks x 3 arms x glm at
`--workers 4` completed 12/12 with three workers concurrently active
(`.working`, `.working-1`, `.working-2`), `db_corrupted: False` on every row,
12 traces written without collision, $0.03. Correctness, not throughput —
the speedup itself is worth measuring in the first minutes of the real sweep
rather than inferred from a 12-run sample.

## F8 — raising `TOKEN_BUDGET` raised the spend guard's reserve floor with it

`dce.runner._next_reserve` reserves one call ahead before making it, and its
pre-observation floor is `dce.agent._token_budget_usd(model)` — the real worst
case implied by the token guard. Raising `TOKEN_BUDGET` 4.5x (F6) therefore
raised that floor by the same factor:

| model | old floor | new floor |
|---|---:|---:|
| deepseek-v4-flash-0731 | $0.132 | $0.593 |
| glm-5.3-flash | $0.183 | $0.824 |
| deepseek-v4-pro-0813 | $1.449 | $6.522 |
| gpt-5.6-sol | $7.320 | $32.940 |

The guard is not wrong — the ceiling really did rise — and because it reserves
only one call ahead, the practical effect is that a sweep stops one floor short
of `--max-spend` rather than reserving the floor for every remaining task. But
for `gpt-5.6-sol` the floor now exceeds any sane subset budget: a `--max-spend`
under ~$33 would make zero calls. The Sol subset must either be given a budget
comfortably above its floor or run under a tighter model-specific token guard.

Deliberately NOT fixed by weakening `_next_reserve`: keeping the floor in the
estimate after observations exist is a decision that already has a measured
82% budget overrun behind it.

## Implementation notes — two pydantic-ai behaviours that only a real `Agent` shows

Both were caught by the end-to-end forcing-turn test and passed silently under
fake agents. Recorded because both are the kind of thing a reviewer would
reasonably assume works the other way.

1. **Usage limits are checked against the cumulative `RunUsage`.** Passing the
   main run's `usage` into the forcing turn is what bills it onto the row — but
   pydantic-ai compares limits to that same object, so an absolute
   `request_limit=2` is measured against the requests the main run already
   spent. It raised *"the next request would exceed the request_limit of 2"*
   with `usage.requests` at 4, and the forcing turn never reached the model.
   Both limits must be offsets from the counts already banked.

2. **`run_sync(toolsets=[])` does not remove the agent's own tools.** It clears
   only the extra run-time toolsets; tools registered via `Agent(tools=...)` —
   how every arm here is built — stay callable. With `toolsets=[]` alone the
   model still emitted a `list_tables` call on the forcing turn and died on the
   tool-call limit instead of answering. `_tool_less_twin` builds a fresh
   `Agent` on the same model and settings with no tools at all.

## F9 — `temperature=0.0` was inert for every model, on every run so far

pydantic-ai strips `ModelSettings(temperature=...)` for any model whose profile
has reasoning enabled, warning:

> `Sampling parameters ['temperature'] are not supported when reasoning is enabled. These settings will be ignored.`

Its `SAMPLING_PARAMS` set — temperature, top_p, presence/frequency penalty,
logit_bias, logprobs, top_logprobs — is borrowed from OpenAI's reasoning
models. It is wrong for these OpenRouter models, whose cards list `temperature`
and `reasoning` as simultaneously supported. Reasoning cannot be turned off to
dodge the rule either: `{"enabled": false}` returns HTTP 400, *"Reasoning is
mandatory for this endpoint and cannot be disabled."*

So every run in the smoke sweep sampled at the provider's default temperature,
not 0. `seed` was unaffected — it is not in `SAMPLING_PARAMS`.

**Fix:** send temperature in `extra_body`, which bypasses the strip. Verified
against the live API: a deliberately out-of-range value returned *"Expected
temperature to be at most 2, received 99"* rather than being dropped. Applied
only to the three models whose OpenRouter card lists `temperature`;
`openai/gpt-5.6-sol` does not, so it is omitted there and Sol genuinely runs at
its own default sampling. FINDINGS must say that rather than imply a uniform
temperature=0 across the board.

## F10 — reasoning effort was an unset, per-endpoint default

All four pinned models support `reasoning_effort`, all four reason by default,
and the default differs by endpoint — while the endpoint was, until F3, chosen
per request. Measured on `z-ai/glm-5.3-flash` for one trivial question: **133
reasoning tokens with no parameter set, against 45–55 at any explicit effort.**
Reasoning tokens bill at the OUTPUT rate, the pricier one, so this is a cost
knob as well as a quality knob.

**Fix:** `REASONING_EFFORT = "medium"`, sent explicitly on every call and
stamped on every row. Uniform across arms — it is a model setting, not an arm
setting.

### Refinement: reasoning on the tool-calling path varies by MODEL, not by much else

The 133-vs-45 figure above was measured on a **tool-free** call, and this
experiment never makes one. A first re-probe with a trivial prompt (`17*23`)
found near-zero reasoning under tool use and briefly suggested the knob was
inert here. That was wrong: the prompt was too easy to require any thought.

Given a question that actually needs reasoning, every pinned model emits it
**alongside** its tool calls — confirmed on the raw OpenRouter wire and again
through pydantic-ai, which surfaces it as a `ThinkingPart`:

| model | reasoning tokens | reasoning content |
|---|---:|---:|
| glm-5.3-flash | ~20 | 66 chars |
| gpt-5.6-sol | ~71 | 430 chars |
| deepseek-v4-pro-0813 | **1,123** | **4,543 chars** |

End-to-end on a real hard task (1480, arm C, `deepseek-v4-pro`): 7 thinking
parts, 4,652 chars, `reasoning_tokens=1096` — **49% of that run's 2,232 output
tokens**, billed at the output rate. Verdict `correct`.

So F10's original claim stands for the models that reason: reasoning IS a cost
lever, materially so on the strong model. What the smoke run's empty traces
actually showed is that **`glm-5.3-flash` barely reasons** (~20 tokens), not
that the harness loses reasoning. Since the smoke run used only glm, its
traces were bound to look empty.

Two consequences:

- Traces will be far richer on the primary comparison (`deepseek-v4-pro`) than
  anything the smoke run suggested — ~4.6 KB of the model's own reasoning per
  task, which is the single most useful artifact for diagnosing a wrong answer.
- `reasoning_tokens` is recorded per row so the sweep measures this across
  models and arms instead of inheriting any single probe's answer.

## F11 — the account data policy silently removes endpoints

`provider: {"only": ["deepseek"]}` on `deepseek-v4-pro-0813` returns *"No
endpoints available matching your guardrail restrictions and data policy"*. The
first-party DeepSeek endpoint is unreachable under this account's settings, for
both DeepSeek models. Routing was therefore already constrained by an
account-level setting that nothing in the experiment recorded, and that a
different operator re-running this would not share.

Recorded, not worked around: the pins chosen below are all reachable ones.

### The endpoint pins

Chosen on quantization, price, context, and reachability. Prices are the
**pinned endpoint's**, not the model card's headline rate — the distinction
that makes F4 correct rather than merely closer.

| model | pin | quant | in / out / cache ($/1M) |
|---|---|---|---|
| deepseek-v4-flash-0731 | `deepinfra/fp8` | fp8 | 0.08 / 0.18 / 0.016 |
| deepseek-v4-pro-0813 | `alibaba` | unknown | 0.5808 / 1.7424 / 0.0581 |
| glm-5.3-flash | `z-ai` | fp8 | 0.075 / 0.25 / 0.015 |
| gpt-5.6-sol | `openai` | unknown | 2.00 / 10.00 / 0.20 |

Note the old table priced `deepseek-v4-flash-0731` at 0.065/0.18 — the **fp4**
Sail Research/Relace rate. Endpoint prices for that model span $0.03–$0.44 per
1M input, a **14.7x** spread across 30 endpoints.

**The pin is self-verifying.** `allow_fallbacks: false` returns HTTP 404 rather
than re-routing when it cannot be honoured — verified through pydantic-ai, not
just curl. So a row that exists at all is a row the pin held for, which is why
the observed provider is not recorded: OpenRouter returns it only in the
response body, which pydantic-ai discards (`ModelResponse.provider_name` is the
literal string `"openrouter"`, and no response header carries it).

## F12 — our fallback scorer was stricter than DABStep's, and it flipped a headline

The 12-task re-run graded `Yes.` as **wrong** against a gold of `yes`: the
fallback normalises case but not trailing punctuation. DABStep's own
`compare_strings` strips all non-word characters, so it grades that correct.

Two rows disagreed — both ungoverned arms on task 30 — and they moved the
result across the significance line:

| comparison | our fallback | official scorer |
|---|---|---|
| `schema_only` vs `contract` | **p=0.0156** (significant) | p=0.0703 (not) |
| `manual_prompt` vs `contract` | p=0.0625 | p=0.2188 |
| accuracy | A 1/12, B 3/12, C 8/12 | A 2/12, B 4/12, C 8/12 |

**The stricter scorer produced a false positive in this library's own favour.**
Both corrected rows went against the contract arm; none went for it.

**Fix:** vendor DABStep's scorer verbatim at pinned revision
`d4431c2e4a695cbe43c33aab2adaa304a37ae64a` (sha256 recorded and asserted in
`tests/test_vendored_scorer.py`) and make it the grading path. It is not on
PyPI, so a pinned hashed copy is the only way to have both the official rules
and determinism.

Adopting it cuts **both ways**, which is why it is a correctness fix and not a
loosening:

- *More lenient:* `rel_tol=1e-4` numeric comparison, rounding to the lesser
  decimal precision, single-word subset matching, `SequenceMatcher > 0.95`.
- *Stricter:* `N/A` and `none` no longer satisfy a `Not Applicable` gold — the
  task guidelines ask for that exact phrase, and 6 of 406 golds (1.5%) use it.

Gold reconstruction is unaffected: it consumes the leaderboard's own published
`score` field, and the dev gate uses a separate normaliser.

## F13 — the fallback scorer should never have existed

F12 fixed the symptom. The cause was a design error in the spec itself, which
said the experiment would use
`dabstep_benchmark.evaluation.scorer.question_scorer` **"when installable"**
and otherwise a local normalizer that "mirrors its documented behaviour".

Two things were wrong with that:

1. **"Not on PyPI" was treated as "unavailable."** The scorer is 146 lines of
   public code in a public Space, reachable with one `curl` at a pinned
   revision. Writing an approximation of a readable algorithm we could simply
   copy was never justified.
2. **"Mirrors its documented behaviour" was an unverified claim, and false.**
   No test ever compared the two. They differed on trailing punctuation,
   numeric tolerance, `Not Applicable` synonyms, and fuzzy string matching.

The fallback has therefore been **deleted**, not merely demoted. `score` is a
thin delegation to the vendored official scorer, and an exception from it
propagates so `run_task` records a visible `scoring_error` row.

Keeping a fallback "just for errors" was itself a bug, introduced in the F12
fix and removed here: `active_scorer()` decides its answer once at import by
probing with `("x", "x")`, so a row could record `official-vendored` while a
per-call exception had actually graded it with the stricter local rules. That
is the same "one results file, two grading algorithms, nothing per row to say
which" hazard the field exists to prevent — reintroduced, invisibly, by the
patch meant to fix it.

Only `_clean` survives, and it does not score: it fills `answer_normalized`
so a dispute can be re-adjudicated from a stored row.

## Status

- **Done:** F1, F2, F5, F6 (caps + forcing turn); F3, F4, F9, F10, F11
  (endpoint pin, cache-aware pricing, temperature, reasoning effort); F7a
  (parallelism).
- **Decided:** F7's scope — glm first, over all 401 golded tasks, as a
  scale rehearsal *and* the adversarial test of arm C's fetch-gated context
  (glm is the weakest reasoner of the four, ~20 reasoning tokens under tool
  use; MotherDuck's finding is that low-reasoning models skip fetch-gated
  context). It is explicitly **not** the headline: a flash model floors near
  zero on hard tasks and no leaderboard baseline runs one, so comparability
  needs `deepseek-v4-pro`.
- **Open:** where to run it (a VPS is being provided) and F8's Sol budget,
  which binds nothing until a Sol subset is actually run — glm's $2.47/group
  ceiling is immaterial against any sane cap.

Every result produced before these fixes is void for FINDINGS purposes: the
recorded `usd` was inflated, the serving endpoint was unpinned and unrecorded,
temperature was inert, and reasoning effort was a per-endpoint default. The
frozen contract and the gold set are unaffected.

All four pinned models were verified end-to-end through
`_default_agent_factory` — pin honoured, tool call executed, correct answer,
and no `UserWarning` under `-W error::UserWarning`.

