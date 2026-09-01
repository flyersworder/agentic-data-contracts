# DABStep contract-context eval — findings

**Three independent runs of the same four-arm ablation, on three models
spanning a wide capability range.** Shared across all: **contract digest**
`sha256:e438ecf7…`, **hollow digest** `sha256:c46a767d…` (but see the note on
digest stamping under [Operational findings](#operational-findings) — every
row, arm D included, carries the *real* contract's digest) · **golds**
`a4388e9ee823` (401/450 tasks) · **scorer** DABStep's own, vendored at
`d4431c2e` · `reasoning_effort=medium`.

| run | model | rows | scoreable | cost | raw | traces |
|---|---|---:|---:|---:|---|---|
| **A** | `z-ai/glm-5.3-flash` (`z-ai` fp8) | 1,604 | 401/401 tasks | $3.26 | `results/glm-full.jsonl` | `traces/glm-full/` |
| **B** | `deepseek/deepseek-v4-flash-0731` (`baidu` fp8) | 1,604 | **279/401 tasks** | $6.32 | `results/dsflash-full.jsonl` | `traces/dsflash-full/` |
| **C** | `openai/gpt-5.6-sol` (`openai`) | 1,604 | 401/401 tasks | $72.36 | `results/sol-full.jsonl` | `traces/sol-full/` |

Run A at commit `6836139` (arms A–C) / `d75d269` (arm D); run B at `4d230fa`;
run C at `04bee43`.

Temperature is held at 0 on runs A and B. **Run C does not hold it**:
`gpt-5.6-sol` reports `supports_temperature=False`, so the sampling parameter
is not accepted and the run is at the provider's default. Reasoning effort is
`medium` on all three, but that is a nominal setting and not an equated one —
sol spends 957 reasoning tokens per row against glm's and deepseek's far
larger appetites.

Run B lost 122 of 401 tasks to provider rate-limiting partway through and is
reported on its 279-task complete-case set throughout. That truncation is
itself a finding — see [Operational findings](#operational-findings). It cost
power, not validity: the loss is near-uniform across arms (28–30% of rows
each) and clusters in time rather than by task or arm.

## Headline

**`contract` wins on all three models, by a wide and significant margin.**

Run A — `glm-5.3-flash`, all 401 tasks:

| Arm | tools | knowledge | overall | **hard (n=332)** | easy (n=69) | cost |
|---|:-:|:-:|---:|---:|---:|---:|
| **contract** | governed | contract | **232/401 (57.9%)** | **183 (55.1%)** | 49 (71.0%) | **$0.67** |
| manual_prompt | raw SQL | manual in prompt | 127/401 (31.7%) | 76 (22.9%) | 51 (73.9%) | $0.97 |
| **contract_hollow** | **governed** | **none** | 106/401 (26.4%) | **64 (19.3%)** | 42 (60.9%) | $0.85 |
| schema_only | raw SQL | none | 91/401 (22.7%) | 46 (13.9%) | 45 (65.2%) | $0.76 |

Run B — `deepseek-v4-flash`, the 279 tasks scoreable in all four arms:

| Arm | tools | knowledge | overall | **hard (n=226)** | easy (n=53) | cost |
|---|:-:|:-:|---:|---:|---:|---:|
| **contract** | governed | contract | **171/279 (61.3%)** | **128 (56.6%)** | 43 (81.1%) | **$1.05** |
| manual_prompt | raw SQL | manual in prompt | 145/279 (52.0%) | 97 (42.9%) | 48 (90.6%) | $1.69 |
| schema_only | raw SQL | none | 92/279 (33.0%) | 51 (22.6%) | 41 (77.4%) | $1.79 |
| **contract_hollow** | **governed** | **none** | 88/279 (31.5%) | **51 (22.6%)** | 37 (69.8%) | $1.52 |

Run C — `gpt-5.6-sol`, all 401 tasks:

| Arm | tools | knowledge | overall | **hard (n=332)** | easy (n=69) | cost |
|---|:-:|:-:|---:|---:|---:|---:|
| **contract** | governed | contract | **316/401 (78.8%)** | **257 (77.4%)** | 59 (85.5%) | $21.32 |
| manual_prompt | raw SQL | manual in prompt | 232/401 (57.9%) | 167 (50.3%) | 65 (94.2%) | **$14.42** |
| **contract_hollow** | **governed** | **none** | 223/401 (55.6%) | **170 (51.2%)** | 53 (76.8%) | $22.09 |
| schema_only | raw SQL | none | 176/401 (43.9%) | 123 (37.0%) | 53 (76.8%) | $14.52 |

Every pairwise paired McNemar — same tasks, same model, same scorer. `a_only`
is tasks the left arm alone got right; `b_only` the right arm alone:

| comparison | glm-5.3-flash (n=401) | deepseek-v4-flash (n=279) | gpt-5.6-sol (n=401) |
|---|---|---|---|
| schema_only vs **contract** | p=3e-31 · 14 / **155** | p=2e-15 · 14 / **93** | p=5e-32 · 12 / **152** |
| manual_prompt vs **contract** | p=2e-17 · 29 / **134** | p=**0.0067** · 30 / **56** | p=2e-17 · 12 / **96** |
| contract_hollow vs **contract** | p=9e-24 · 23 / **149** | p=8e-17 · 13 / **96** | p=2e-17 · 18 / **111** |
| schema_only vs manual_prompt | p=7e-06 · 14 / 50 | p=2e-12 · 5 / 58 | p=2e-08 · 22 / 78 |
| manual_prompt vs contract_hollow | p=0.033 · 55 / 34 | p=4e-13 · 63 / 6 | p=0.44 · 58 / 49 |
| **schema_only vs contract_hollow** | **p=0.058** · 20 / 35 | **p=0.61** · 19 / 15 | **p=4e-07** · 20 / **67** |

The last row is the one that carries the ablation, and run C changes what it
says. It is discussed next.

## The ablation is the result

`contract_hollow` is arm C with its knowledge removed and everything else
held fixed: the same nine governed tools, the same procedural instruction
(*"Look up the domain and the metrics that apply before writing SQL, and
validate every query with inspect_query"*), the same allowed tables,
forbidden operations and rules. It exists because arm C otherwise differs
from the baselines in three ways at once, and "you also gave it tools and
told it to look things up" is the first objection any reader will raise.

On hard tasks, the gain splits into a scaffolding step and a content step:

```
                        glm-5.3-flash      deepseek-v4-flash      gpt-5.6-sol
schema_only                 13.9%              22.6%                 37.0%
  + tools and procedure  →  19.3% (+5.4)       22.6% (+0.0)          51.2% (+14.2)  ← contract_hollow
  + contract knowledge   →  55.1% (+35.8)      56.6% (+34.0)         77.4% (+26.2)  ← contract
```

**The content step is the larger of the two on every model. The scaffolding
step is model-dependent: absent on one model, decisive on another.**

This reverses a claim held through two runs of this experiment. With runs A
and B alone the scaffolding step did not survive a significance test, and this
document said so — "not distinguishable from zero", "the content is the whole
effect". Run C falsifies that. Paired McNemar on `contract_hollow` vs
`schema_only`, all 401 tasks:

| | Δ hard | discordant | p |
|---|---:|---:|---:|
| glm-5.3-flash | +5.4 pp | 55 (20 / 35) | 0.058 |
| deepseek-v4-flash | +0.0 pp | 34 (19 / 15) | 0.61 |
| **gpt-5.6-sol** | **+14.2 pp** | **87 (20 / 67)** | **4×10⁻⁷** |

Two of three models favour the scaffolding and one is flat; none favours the
bare schema. On glm the hard-task slice alone is already p=0.015 (16 / 34) —
the p=0.058 above is the all-401 test, which the easy tasks dilute. So the
honest summary is not "scaffolding does nothing" but **"scaffolding does
something on two of three models, and always less than the content does."**

What survives unchanged is the part the fourth arm was built for. The
deflationary reading of arm C — that the contract helps by narrowing the
tables in view, imposing a procedure, or making the agent slow down rather
than by what it says — predicts that arm D should capture most of the gain.
It does not, on any model: the content step is 2× the scaffolding step on sol
and unboundedly larger on the other two. **Arm D is no longer a null result;
it is a smaller effect that the content dominates everywhere.**

**Tooling is not a substitute for content.** On glm, `contract_hollow`
(19.3% hard) scores *below* `manual_prompt` (22.9%); on deepseek the gap is
wider (22.6% vs 42.9%, p=4e-13). On sol the two are statistically
indistinguishable (51.2% vs 50.3%, p=0.44) — empty governed tools finally buy
as much as the whole manual pasted into the prompt, but no more, and both
remain far below the contract's 77.4%.

**The empty tools actively cost something.** On glm it is the most expensive
arm per correct answer — $0.85 for 106 correct against contract's $0.67 for
232 — and burns the most reasoning of any arm (543,565 tokens against
contract's 154,247). The mechanism is visible in the transcripts: the model
calls `lookup_domain`, is told *"No documentation is available for this
domain"*, and searches harder. On deepseek it makes the most tool calls of
any arm (6,375, against contract's 4,155) for the fewest correct answers.
This is also the check that the control did what it claims — see below.

### The control is verified, not assumed

`contract_hollow` is *derived* from the frozen contract by `dce/hollow.py`
rather than hand-authored, so the difference between the two artifacts is
exactly the diff: every field the generator does not touch is identical.
Emptied are domain prose, metric descriptions, `sql_expression` (the fee
formula lives there), and column and relationship descriptions. Kept are
names, structure, types, table allow-lists, forbidden operations, rules, and
the identical nine-tool surface.

`tests/test_hollow.py` asserts both halves, including the pair that makes the
knowledge check meaningful: **no 6-gram of `manual.md` survives into the
hollow contract, and the real contract does share 6-grams with it** — so the
check is known to bite rather than merely to pass.

The transcripts confirm it end to end: on glm, arm D calls `lookup_domain` on
**272 of 401 tasks** and receives the empty placeholder **282 times**; on sol
it calls it on **401 of 401 tasks**, 804 times. The tool surface was
exercised; there was simply nothing behind it. That sol both exercises the
scaffolding hardest and gains the most from it is consistent with the
scaffolding step being real there.

Prompt length is deliberately not held constant (1,984 vs 2,475 chars).
Padding with filler would swap one confound for another — text that reads as
content but carries none. The variable under test is knowledge, not tokens.

## The result is entirely in the hard tasks

On easy tasks the four arms are within noise of each other on all three runs
(glm 61–74%, deepseek 70–91%, overlapping intervals throughout) and the
contract arm is not even the best of them — `manual_prompt` leads on easy
tasks in every run. The contract does nothing for questions that need no
domain knowledge, which is what it should do. Every bit of the effect is in
the hard split.

**`manual_prompt` reproduces the published leaderboard band, which is the
validity check that licenses the comparison at all.** Credible named
baselines sit at ~20–26% hard (Google simple_baseline 26%, Adyen GPT-5.4
22.2%, HF Claude 4 Sonnet 19.8%). Our reimplementation of that same approach
— DABStep's `manual.md` verbatim in the prompt — lands at **22.9%** on glm,
a flash-tier model weaker than any of those. The harness measures what the
leaderboard measures.

On deepseek the same arm reaches **42.9%**, well above the published band.
That is a statement about the model, not the harness: `deepseek-v4-flash` is
simply stronger at this benchmark than the named baselines, and its
bare-schema floor (22.6% hard) already sits where their manual-in-prompt
results do. The harness is identical between the two runs.

## The contract's advantage does not shrink monotonically with capability

Ordered by how much the model can do with a bare schema:

| | schema_only (hard) | manual_prompt (hard) | contract (hard) | **contract − manual_prompt** |
|---|---:|---:|---:|---:|
| glm-5.3-flash | 13.9% | 22.9% | 55.1% | **+32.2 pp** |
| deepseek-v4-flash | 22.6% | 42.9% | 56.6% | **+13.7 pp** |
| gpt-5.6-sol | 37.0% | 50.3% | 77.4% | **+27.1 pp** |

**With two models this looked like a clean interaction — the contract's
advantage shrinking as models strengthen, +32.2 → +13.7. The third model
breaks it: +27.1.** Whatever governs the size of the contract's advantage
over a hand-written prompt, it is not a monotone function of base capability,
and a two-point trend was not evidence for one. This document previously
reported that trend as a finding; it should not have.

Run C does settle the older question, though, and decisively against the
reading this document leaned toward. The two candidate explanations of the
55.1%/56.6% coincidence were:

1. **Structured context substitutes for model capability**, so the gap keeps
   shrinking with a stronger model.
2. **The contract arm sits at a ceiling this benchmark imposes**, pinned near
   56% while `manual_prompt` converges on it.

Reading 2 predicted `contract` stays near 56% on a frontier model. It reached
**77.4%**. The near-identical 55.1%/56.6% was a coincidence after all, and
**there is no benchmark ceiling at 56%.** Reading 1's specific prediction — a
monotonically shrinking gap — is also refuted by the +27.1. Both simple
stories are wrong, and three points are not enough to replace them with a
third.

## Where the effect lives, and why that is a caveat

DABStep's hard set is dominated by one question family: **294 of 332 hard
tasks (89%) mention fees.**

| run | bucket | n | schema_only | contract_hollow | manual_prompt | contract |
|---|---|---:|---:|---:|---:|---:|
| glm | fee (hard) | 294 | 41 (13.9%) | 55 (18.7%) | 72 (24.5%) | **174 (59.2%)** |
| glm | non-fee (hard) | 38 | 5 | 9 | 4 | **9** |
| deepseek | fee (hard) | 196 | 45 (23.0%) | 44 (22.4%) | 88 (44.9%) | **120 (61.2%)** |
| deepseek | non-fee (hard) | 30 | 6 | 7 | **9** | 8 |
| sol | fee (hard) | 294 | 109 (37.1%) | 157 (53.4%) | 157 (53.4%) | **246 (83.7%)** |
| sol | non-fee (hard) | 38 | **14** | 13 | 10 | 11 |

"Hard accuracy" on this benchmark is very close to "fee-question accuracy",
and the frozen contract encodes fee-domain semantics. The honest statement
is: **a contract carrying a domain's semantics produces a large gain on
questions in that domain.** It is not evidence about analytics questions in
general, and this benchmark cannot supply that evidence — 450 tasks are
generated from only 26 templates.

**The non-fee bucket shows no contract advantage on any model** — against
`manual_prompt`, 9 vs 4 on glm, 8 vs 9 on deepseek, 11 vs 10 on sol. With 38,
30 and 38 tasks these are far too small to read as a negative result, but they
are equally unable to support the general claim. On sol the ordering inverts
outright: `schema_only` leads the non-fee bucket at 14/38 while `contract`
takes 11 — the *only* slice anywhere in these three runs where the bare
baseline beats the contract. All three runs are consistent with the effect
being domain-specific by construction: outside the domain the contract
describes, it is inert, and a frontier model does not rescue it. That is the
expected behaviour of a domain contract, and it is also precisely why this
benchmark cannot answer the general question.

A 30-task sub-bucket ("most expensive MCC for a transaction of N euros")
scores 1–2 out of 30 for *every* arm. A template no arm solves is a property
of the benchmark, not a weakness of any arm — recorded because a per-arm
reading of that bucket alone would have looked like a contract failure.

## The contract compiles, so the residual gap is derivation, not information

`analysis/macro.sql` is a pre-computed fee layer — two DuckDB views — built by
transcribing the frozen contract's own `sql_expression` fields and adding one
thing: composition. Every predicate in it comes from `contract/semantic.yml`
(`fee_rule_matches_transaction`, `fee_rule_matches_merchant_month`,
`transaction_natural_month`, `merchant_monthly_volume`,
`merchant_monthly_fraud_level`, `transaction_fee`, `total_transaction_fees`).
Nothing was authored independently of the contract. The only work the file does
on its own is correlate `:volume` and `:fraud_ratio` — the two placeholders that
make `fee_rule_matches_merchant_month` "NOT RUNNABLE AS WRITTEN" — to a
per-merchant natural-month aggregate.

**It reproduces gold exactly on every task it covers: 176/176, 100%**
(`uv run python analysis/coverage.py`, graded by the vendored official scorer).

| family | n | exact |
|---|---:|---:|
| `total_fees_day` / `_month` / `_year` | 45 | 45 |
| `fee_ids_day` / `_month` / `_year` | 45 | 45 |
| `merchants_for_fee` | 7 | 7 |
| `avg_fee_credit` / `_account` / `_account_mcc` | 59 | 59 |
| `fee_ids_by_at_aci` | 20 | 20 |
| **total** | **176** | **176 (100%)** |

This is a statement about the artifact, not about an agent: **the contract
contains complete and correct information for 176 of the 332 hard tasks.** Two
consequences follow.

First, it rules out a reviewer's obvious objection — that the contract arm wins
because the contract is subtly wrong in a benchmark-favouring way. It is not
wrong; it compiles to gold.

Second, and more useful, it turns the contract arm's accuracy into an
*efficiency against a known ceiling*. Where the information is provably
present, whatever the agent fails to get is a failure to derive, not a
consequence of missing context.

### What a pre-computed macro layer can and cannot reach

Classifying all 401 golded tasks by question shape alone — never by gold, never
by any arm's output (`analysis/coverage.py:classify`):

| bucket | tasks | hard | what it means |
|---|---:|---:|---|
| `macro` | 176 | 176 | the compiled contract answers it outright |
| `derived` | 148 | 148 | needs fee semantics *plus* a counterfactual or an optimisation the macro does not encode |
| `unrelated` | 77 | 8 | no fee semantics involved (mostly the easy set) |

So a pre-computed view/macro layer of the kind a context-layer product ships
tops out at **53% of the hard set** on this benchmark, however good it is. The
remaining 148 hard tasks — "what delta would this merchant pay if fee 17's rate
changed", "which ACI should they steer to" — consume the macro's output and
reason on top of it. `macro` is a lower bound on macro coverage: it counts the
families we actually implemented, and some `derived` tasks might yield to a
cleverer artifact.

### The arms, split by bucket

`uv run python analysis/buckets.py`, hard tasks, complete cases:

| | glm `macro` | glm `derived` | ds-flash `macro` | ds-flash `derived` | sol `macro` | sol `derived` |
|---|---:|---:|---:|---:|---:|---:|
| n | 176 | 148 | 118 | 101 | 176 | 148 |
| schema_only | 2.8% | 25.0% | 8.5% | 34.7% | 31.8% | 41.2% |
| contract_hollow | 10.2% | 25.7% | 10.2% | 31.7% | 46.6% | 54.1% |
| manual_prompt | 14.8% | 32.4% | 38.1% | 44.6% | 49.4% | 48.6% |
| **contract** | **60.8%** | **45.9%** | **62.7%** | **46.5%** | **94.9%** | **56.1%** |
| *compiled contract (oracle)* | *100%* | — | *100%* | — | *100%* | — |

Three readings.

**The contract's advantage concentrates where its semantics apply directly.**
Over `schema_only` it is +58.0 pp (glm), +54.2 pp (ds-flash) and +63.1 pp (sol)
on `macro`, against +20.9, +11.8 and +14.9 pp on `derived`. This is what a
domain contract should do, and it is the same domain-specificity the fee/non-fee
split shows, measured on a cleaner boundary.

**The derivation gap collapses with capability — this is the central result of
run C.** On `macro` the information is provably sufficient, so the shortfall
below 100% is the agent failing to apply knowledge it was given and provably
had:

| model | contract on `macro` | derivation gap |
|---|---:|---:|
| glm-5.3-flash | 60.8% | 39.2 pp |
| deepseek-v4-flash | 62.7% | 37.3 pp |
| **gpt-5.6-sol** | **94.9%** | **5.1 pp** |

With two models this looked like a fixed ~38 pp tax, model-invariant to within
2 pp. It is not a tax. **On a frontier model it very nearly disappears**, and
what the contract carries is almost fully recovered. The corresponding
behavioural measure agrees: sol's contract arm writes all six of the macro's
clauses 98% of the time (see below).

**Where reasoning is needed on top of the semantics, capability helps far
less.** The `derived` bucket moves 45.9% → 46.5% → 56.1% across the same three
models, against 60.8% → 62.7% → 94.9% on `macro`. The difficulty that survives
a frontier model is not in knowing the domain — it is in the counterfactual and
optimisation reasoning built on top of it.

**`contract_hollow` no longer tracks `schema_only`.** On glm and ds-flash it
sat on the baseline in both buckets; on sol it is +14.8 pp on `macro` and
+12.9 pp on `derived`, matching the reversal of the scaffolding null above.

### Counterfactual macros: the contract kills the named errors, and the rest are idiosyncratic

Lesion exactly one convention in the compiled macro and it becomes wrong in a
single, nameable way. `analysis/counterfactuals.py` builds nine such lesions —
NULL read as "matches nothing" rather than "applies to all values", an empty
list read the same way, the merchant's raw `capture_delay` compared against the
fee table's bands without mapping days to a band, fraud level as a share of
transaction count rather than of euro volume, monthly volume as a count, bands
read as exclusive, the `day_of_year` reconstruction off by one, and charging one
matching rule per transaction instead of summing over all matched pairs (by
lowest ID, and by highest fee). Each is a textual substitution on `macro.sql`
guarded by an exact-occurrence assertion, so a change to the macro fails loudly
here rather than silently lesioning something else.

The lesions produce distinct, plausible numbers: on task 1748 (gold 15237.49)
`first_match` answers 9211.73, `raw_capture_delay` 14898.61,
`null_not_wildcard` 900.48. A wrong agent answer that matched one would
identify the misconception outright — no LLM judge, no rubric, no hand
labelling. Seven of the nine are diagnostic on 34–95 of the 176 tasks;
`exclusive_bands` is diagnostic on none, because no golded task sits exactly on
a band boundary.

**Wrong answers, restricted to the 97 payments-joined fee tasks** — the only
families a lesion of `macro.sql` can reach. The other four macro families
(`avg_fee_*`, `fee_ids_by_at_aci`) resolve straight off `fees` and never touch
the lesioned views, so every wrong answer on them is undiagnosable by
construction:

| | wrong | single lesion | lesion pair | undiagnosed |
|---|---:|---:|---:|---:|
| glm `schema_only` | 92 | 4 | 0 | 88 |
| glm `contract_hollow` | 80 | 11 | 0 | 68 |
| glm `manual_prompt` | 75 | 4 | 2 | 68 |
| glm `contract` | 12 | **0** | **0** | 12 |
| ds-flash `schema_only` | 59 | 0 | 2 | 57 |
| ds-flash `contract_hollow` | 63 | 2 | 1 | 60 |
| ds-flash `manual_prompt` | 40 | 1 | 0 | 39 |
| ds-flash `contract` | 23 | **0** | **0** | 23 |
| sol `schema_only` | 46 | **27** | 0 | 19 |
| sol `contract_hollow` | 37 | **20** | 0 | 16 |
| sol `manual_prompt` | 26 | 2 | 1 | 22 |
| sol `contract` | **0** | — | — | — |

That restriction is a correction. An earlier version of this section pooled all
176 macro tasks, which inflated every `undiagnosed` count with tasks no lesion
could ever explain — and did so *unevenly*: 71% of contract-arm errors fell on
the unreachable families against 52% elsewhere, biasing the very comparison the
table exists to make. The claim below is the corrected one, and it is about
30x weaker than what this document previously reported.

Three things follow.

**On the two weaker models the method mostly fails, and that is a finding.**
27 of 444 wrong answers (6.1%) are explained by one named convention error, and
all 35 lesion pairs add almost nothing. Those agents do not fail by holding one
crisp misconception — they fail idiosyncratically, in ways a fixed catalogue of
conventions does not enumerate. **The derivation gap is therefore not closable
by documenting five more conventions**, which is the intervention its size
would otherwise invite. (A tenth lesion, dropping rule-matching entirely, was
tested to explain `contract_hollow`'s over-counting on glm: it answers 1227x
gold where the arm answers 87x, so that is not the mechanism either.)

**On sol the method works, and the reason is the interesting part.** Its
diagnosis rate is **45.9%** overall and **59%** for `schema_only` — an order of
magnitude above glm's 8.1% and ds-flash's 3.2%. A frontier model given only a
bare schema writes SQL that is structurally right and wrong in exactly one
nameable convention, so a single lesion reproduces its answer exactly. The
weaker models' SQL is wrong in ways no single convention describes.
**Error legibility is itself a function of capability** — a caution for any
failure-taxonomy method calibrated on weak models, an LLM judge included.

**On every model, the contract arm contributes none of the diagnosable
errors.** Zero of 35 contract-arm errors are a named convention mistake,
against 77 of 518 elsewhere (Fisher exact **p = 0.0091**). The contract
eliminates precisely the misconceptions it documents; what survives is made of
something else.

The cleanest statement of that is sol's, and it needs no test: **on these 97
tasks sol's contract arm is not wrong once** (97/97, against 85/97 on glm and
47/97 on ds-flash). There are no errors left to diagnose.

### How wrong, in orders of magnitude

Same 97 tasks, restricted to the single-number families; the ratio of the
agent's answer to gold:

| | n | median | <2x | 2-10x | 10-100x | >100x |
|---|---:|---:|---:|---:|---:|---:|
| glm `schema_only` | 42 | 0.96 | 38% | 33% | 24% | 5% |
| glm `contract_hollow` | 43 | **64.0** | 7% | 12% | **77%** | 5% |
| glm `manual_prompt` | 35 | 0.61 | 57% | 43% | 0% | 0% |
| glm `contract` | 1 | — | — | — | — | — |
| ds-flash `schema_only` | 20 | 1.08 | 65% | 20% | 5% | 10% |
| ds-flash `contract_hollow` | 18 | 1.39 | 44% | 33% | 22% | 0% |
| ds-flash `manual_prompt` | 23 | 0.57 | 83% | 17% | 0% | 0% |
| ds-flash `contract` | 12 | 20.41 | 17% | 33% | 33% | 17% |
| sol `schema_only` | 27 | 0.97 | **93%** | 0% | 7% | 0% |
| sol `contract_hollow` | 17 | 1.00 | 71% | 29% | 0% | 0% |
| sol `manual_prompt` | 19 | 0.66 | 79% | 5% | 5% | 11% |
| sol `contract` | 0 | — | — | — | — | — |

**One pattern is monotone across all three models.** On the bare-schema arm,
the share of wrong answers within 2x of gold rises 38% → 65% → 93%. A wrong
answer from a capable model is a near-miss; a wrong answer from a weak one can
be off by two orders of magnitude. This is the same fact the diagnosis rate
reports from the other side — capable models fail legibly.

**Nothing else here replicates, and one claim previously made in this document
does not survive the correction.** An earlier version reported that
`manual_prompt` is never off by more than 10x on any model; restricted to the
families the analysis can actually speak to, sol's `manual_prompt` puts 11% of
its wrong answers beyond 100x. glm's `contract_hollow` over-counts by a median
64x while neither other model's does, and the contract-arm rows rest on 1, 12
and 0 answers. **Treat this table as exploratory**; separating these from noise
needs the repeat runs the limitations section already calls for.

### Does the contract's vocabulary reach the SQL? A behavioural check

The two instruments above read agents' *answers*. This one reads the SQL they
actually submitted (`analysis/clauses.py`, over the stored transcripts), and
asks a prior question: do the contract's clauses appear in the query at all?

Six detectors, one per load-bearing clause of the compiled macro — the
NULL-wildcard disjunct, the empty-list wildcard, the capture-delay band
mapping, a per-merchant monthly aggregate, the natural-month reconstruction,
and fraud measured on `has_fraudulent_dispute`. They are deliberately
permissive: the question is whether the agent expressed the idea, so a detector
demanding the contract's exact phrasing would measure copying rather than use.
The comparison is restricted to the families that join payments to fees, so
**every row compared needs all six**.

| | glm (97 tasks) | | ds-flash (69 tasks) | | sol (97 tasks) | |
|---|---:|---:|---:|---:|---:|---:|
| | mean /6 | all 6 | mean /6 | all 6 | mean /6 | all 6 |
| schema_only | 2.92 | 7% | 3.07 | 4% | 3.61 | 8% |
| contract_hollow | 3.86 | 14% | 3.17 | 4% | 3.84 | 8% |
| manual_prompt | 3.41 | 3% | 3.48 | 9% | 3.55 | 4% |
| **contract** | **5.05** | **39%** | **5.36** | **65%** | **5.94** | **98%** |

`contract` vs `manual_prompt`, Fisher exact on wrote-all-six: **p = 2×10⁻¹⁰**
(glm), **3×10⁻¹²** (ds-flash), **2×10⁻⁴⁷** (sol); against `schema_only`,
p = 1×10⁻⁷, 7×10⁻¹⁵ and 1×10⁻⁴².

**This is the delivery result made behavioural rather than inferred, and it
strengthens monotonically with capability.** `manual_prompt` is handed the same
knowledge, as prose, in its system prompt. It writes all six clauses 3%, 9% and
4% of the time; the contract arm writes them 39%, 65% and **98%**. The
knowledge is possessed in both arms and expressed in only one, which is what
"delivery matters as much as possession" has until now been asserting from
accuracy alone.

Note what does *not* move: `manual_prompt` writes no more of the contract's
clauses on a frontier model than on a weak one (3% → 4%), while the contract
arm goes to near-total. Capability does not, on its own, make a model extract
structure from prose. It makes a model much better at *using* structure it is
handed — which is the same asymmetry the derivation-gap collapse shows.

**What it does not measure.** Clause presence says nothing about whether an
attempt succeeds. Within any arm, attempts that got the task right and
attempts that got it wrong write the same clauses — on all three models, every
Fisher p ≥ 0.14 (`--within` prints this). `contract_hollow` makes the point
concretely: on glm it writes more clauses than `manual_prompt` (14% vs 3%) and
still scores at the bare-schema floor. The measure captures whether the
contract's vocabulary reaches the query, not whether the query is any good.

**A confound worth recording, because the uncontrolled version is seductive.**
Pooling across families, contract-arm attempts that wrote the NULL-wildcard
clause were correct 90% of the time against 23% for those that did not — a
+67 pp effect, with similar gaps for four other clauses. It is an artifact.
Different families require different clauses, so "wrote the clause" partly
encodes "drew an easier family". Restricting to a single required-clause set,
as the table above does, removes the confound and the entire within-arm effect
disappears. Only the between-arm comparison holds tasks constant.

One arm-level figure does not replicate and should not be leaned on: glm's
`contract_hollow` writes more clauses than its `manual_prompt`, and ds-flash's
does not.

### A prediction, recorded before the third model landed — and its answer

The `gpt-5.6-sol` sweep was still running when this section was first written,
and it was committed (`7de2bb6`) before any of its rows existed. It read:

> - If the derivation gap is capability-bound, sol's contract arm on `macro`
>   should sit well above 62.7% and move toward the oracle's 100%.
> - If it is a property of the harness or the task format rather than the
>   model, sol should land near 61–63% like the other two.
>
> Recorded here before the data exists, so the answer is a test rather than a
> description.

**Sol's contract arm scores 94.9% on `macro` (167/176).** The first branch is
confirmed and the second is refuted by 32 points.

This matters more than a resolved bet. The derivation gap is the quantity that
separates a declarative contract, which requires the agent to derive the query,
from a pre-computed macro layer, which does not. A gap that is fixed at ~38 pp
would be a standing structural cost of the declarative approach. A gap that
falls to 5.1 pp on a frontier model is a **transient** cost — one that the
trend in model capability is already paying down, while the per-metric
authoring cost of a macro layer is not paid down by anything.

Three points do not establish a trend, and the caveats are real: sol differs
from the other two in provider, in family, and in that temperature is not held
at 0 (see the header). The prediction was nonetheless made in advance, in the
direction the data went, on the sharpest quantity available.

## Efficiency

**`contract` is the cheapest arm on both weaker models while also being the
most accurate; on `gpt-5.6-sol` it is not.** Accuracy and cost move together on
runs A and B, and come apart on run C — see the note under the run C table.

Run A — glm, all 401 tasks:

| | schema_only | manual_prompt | contract_hollow | contract |
|---|---:|---:|---:|---:|
| cost | $0.76 | $0.97 | $0.85 | **$0.67** |
| input tokens / row | 60,139 | 95,449 | 66,250 | **52,412** |
| turns / row | 12.1 | 9.8 | 8.4 | **7.1** |
| tool calls | 5,137 | 4,286 | 4,105 | **3,355** |
| reasoning tokens | 444,541 | 416,298 | **543,565** | **154,247** |
| forced answers | 41 | 18 | 0 | 0 |

Run B — deepseek, the 279 complete tasks:

| | schema_only | manual_prompt | contract_hollow | contract |
|---|---:|---:|---:|---:|
| cost | $1.79 | $1.69 | $1.52 | **$1.05** |
| input tokens / row | 205,133 | 222,440 | 165,733 | **121,378** |
| turns / row | 13.7 | 12.8 | 11.4 | **8.2** |
| tool calls | 5,489 | 4,770 | **6,375** | **4,155** |
| reasoning tokens / row | 10,941 | 9,243 | 10,765 | **4,844** |
| forced answers (all 401) | 55 | 42 | 15 | **0** |

Run C — sol, all 401 tasks:

| | schema_only | manual_prompt | contract_hollow | contract |
|---|---:|---:|---:|---:|
| cost | $14.52 | **$14.42** | $22.09 | $21.32 |
| cost per correct answer | $0.082 | $0.062 | $0.099 | **$0.067** |
| input tokens / row | **23,711** | 52,452 | 68,058 | 58,189 |
| turns / row | 7.9 | 7.6 | 10.4 | **6.5** |
| tool calls | 5,099 | **4,354** | 7,997 | 5,245 |
| reasoning tokens | 357,191 | 277,195 | 310,677 | **140,692** |
| forced answers | 0 | 0 | 8 | **0** |

**The cheapest-arm claim does not hold on run C, and the header should be read
as "on the two weaker models".** On sol the contract arm costs $21.32 against
`manual_prompt`'s $14.42 — the contract's own text is a real input-token bill
that a cheap model amortises and a well-priced model does not. Per *correct
answer* the ordering is closer ($0.067 vs $0.062) but `manual_prompt` still
edges it. What survives on all three models is the work claim: the contract arm
uses the fewest turns and by far the least reasoning (140,692 tokens against
`schema_only`'s 357,191 on sol, under a third as on glm). It is not thinking
harder — it has less to search for. `contract` is also the only arm that never
needed a forced answer on any of the three runs.

Note this reverses the pre-fix smoke run, where the contract arm looked 11x
more expensive; that was an artifact of `MAX_ROWS=1000` poisoning the context
and cost accounting that ignored cache pricing (F4, F5).

## What the transcripts show

Except where noted, this section reports run A; the harness, prompts and
frozen artifacts are identical in run B, and the checks that were re-run
there are marked.

**No gold leakage.** Searching every system and user prompt for its own gold
returns 23 hits, all benign: 15 are multiple-choice questions where the gold
is an option printed in the question and visible to every arm; 8 are in
`manual_prompt`, where benchmark-supplied vocabulary happens to be the
answer. **Zero in either contract arm.**

*Re-run on run B*, with a coarser per-trace check: 8 traces in `schema_only`,
8 in `contract`, 8 in `contract_hollow`, 17 in `manual_prompt`. The three
non-manual arms are identical, which is the signature of a gold printed in
the question itself — `schema_only`'s system prompt is 281 characters and
cannot contain one. **The contract adds zero leakage over the bare-schema
baseline**; only `manual_prompt` adds any, from the manual's vocabulary.

**The arms are separated as designed.** System prompts: schema_only 281
chars, manual_prompt 24,177, contract 2,475, contract_hollow 1,984. Tool
vocabularies are disjoint — the two baselines get `list_tables` /
`describe_table` / `execute_sql`; both contract arms get the same nine
governed tools.

**Row limits and caps are symmetric.** `MAX_ROWS=50` for `execute_sql` and
`run_query` alike, same 40 tool-call cap, same token budget, temperature,
reasoning effort, provider pin and quantization across all four arms.

**The golds are not crowd opinion, and were verified independently.**
Reconstruction admits an answer into the vote only if DABStep's own grader
marked that submission correct on that task, then requires a ≥75% plurality
(median share 0.981). As a direct check, the fee-average template was
recomputed from the database using the documented matching rule:
**59/59 reconstructed golds reproduce exactly.**

### The mechanism, on one task (run A)

Task 1278, *"the average fee NexPay would charge for a credit transaction of
50 EUR"*:

| | answer |
|---|---|
| gold | `0.352294` |
| contract | `0.352294` ✓ |
| manual_prompt | `0.353053` ✗ |
| schema_only | `0.353053` ✗ |

Both figures reproduce directly against the warehouse:

- `WHERE is_credit IS NULL OR is_credit = true` → **0.352294**
- `WHERE is_credit = true` → **0.353053**

A NULL in a `fees` column means *this rule applies to every value*, not
*unknown*. Both baselines dropped the NULL rows and produced the identical
wrong number. The contract arm called `lookup_domain("fees")`, and its
recorded reasoning says so outright: *"rules that apply to credit
transactions: is_credit true or null"*.

**The decisive detail is that manual.md documents this rule too.** Arm B had
it — inside 24,177 characters of prompt. The contract arm's resident prompt
is 2,475 characters, with the rule reachable by one targeted call. The
finding is not that the contract arm was given more information. Both arms
had it. Only one of them used it.

### A library defect the transcripts exposed

`NoSelectStarChecker` used `find_all(exp.Star)`, which matches any `Star`
anywhere in the AST — and `COUNT(*)` parses as `Count(this=Star())`. The
agent was told *"SELECT * is not allowed"* about queries containing no
`SELECT *`: **161 rejections across 124 of 401 tasks (31%)**. Replaying the
160 distinct rejected queries through the fixed checker, **79 were false**
and 81 genuine (`SELECT p.*` inside CTEs, still blocked).

The cost is visible: on task 1278 the model re-issued the identical query,
then guessed at unrelated clauses — *"maybe it flags AVG("*, *"maybe the word
select"* — before landing on `COUNT(ID)`. A rejection whose stated reason is
not true of the query cannot be complied with.

**This handicapped both contract arms only** — the baselines run ungoverned
SQL and never saw the checker. The 55.1% is a floor, not a ceiling. Fixed in
`cde8b20`, which run A predates.

**Run B confirms the diagnosis.** It ran with the fix, and its contract arm's
inspect rejections fall from **218 to 29** — a 87% drop, consistent with ~half
the original rejections having been false and the arm no longer tripping over
`COUNT(*)`. Run B's contract arm also scores slightly higher on hard tasks
(56.6% vs 55.1%) despite the runs differing in model and provider. The
defect's direction is therefore confirmed; its magnitude remains unseparable
from the model change, which is why run A has not been re-run under the fix.

## Where the contract arm loses — 33 tasks (run A)

14 losses to schema_only, 29 to manual_prompt, 10 to both. Two modes:

1. **Set membership under NULL wildcards** — the same rule as the mechanism
   above, failing in the other direction. On task 1500 ("fee IDs that apply
   to account_type = O and aci = C") the contract arm returns both false
   positives and false negatives while manual_prompt matches exactly.
   Applying "NULL means all" to an enumeration is harder than applying it to
   an average: the model must decide, per column, whether a NULL widens the
   match.
2. **Near-miss arithmetic.** Task 1274, gold `0.126459`, contract
   `0.125516` — a real computation with a wrong rounding or filter.

**These are not the MotherDuck fetch-gating failure.** `lookup_domain` is
called on 331/401 tasks in arm C; the 70 that skip it are *easier for every
arm* (schema_only 41% there vs 19% elsewhere), and 26 of the 33 losses are on
tasks where it *was* called. The model fetches the rule and misapplies it.
Hoisting rule bodies into `to_system_prompt()` would not obviously help; the
useful follow-up is to make the wildcard semantics executable — a tool that
resolves "which fee rules match this transaction" rather than prose the model
reimplements in SQL each time.

## Governance: the ungoverned arms write to the warehouse

Run A produced no governance events at all and this document previously
recorded that as "no governance events". Run B produced the first one, and
scanning both runs' transcripts for it turned up a much larger pattern that
the `db_corrupted` flag alone had been missing.

Counting **mutating statements the model actually submitted to a SQL tool**
(`CREATE TABLE/VIEW`, `DROP`, `INSERT`, `UPDATE`, `DELETE`, `ALTER`,
`TRUNCATE`, at a statement position in the `sql` argument):

| run | schema_only | manual_prompt | **contract** | **contract_hollow** |
|---|---:|---:|---:|---:|
| glm | 27 stmts (8 tasks) | 92 stmts (13 tasks) | **0** | **0** |
| deepseek | 24 stmts (2 tasks) | 1 stmt (1 task) | **0** | **0** |
| **sol** | **0** | **0** | **0** | **0** |
| **total** | **51 (10 tasks)** | **93 (14 tasks)** | **0** | **0** |

**144 mutating statements across 24 tasks in the ungoverned arms; zero in
either governed arm, on either model that produced any.** Fisher exact on
traces containing at least one such statement: p=4e-07 for glm alone, p=6e-08
pooled over runs A and B. (Run B's own 3 events give p=0.13 — the run is
underpowered here as everywhere else; the result rests on run A and the
pooling.)

**Run C produces no governance events at all, in any arm — and that is a
limitation of the finding, not a confirmation of it.** `gpt-5.6-sol` handed a
raw `execute_sql` tool and no rules never once attempted a write. The hazard
this result is about did not occur, so run C can neither support nor
contradict the deterrence claim: **there was nothing to deter.** Stated
carefully, what these three runs show is that *weaker models* mutate the
warehouse when ungoverned and do not when governed, and that a frontier model
did not mutate it either way on this benchmark. Whether declared rules still
deter a frontier model is not answered here, and a governance claim that
generalises across capability would need a setting where capable models
actually attempt the behaviour.

(A re-scan of all three runs with a single detector gives glm 28 statements
across 9 tasks where the earlier scan recorded 27 across 8 — a detail of
statement splitting, affecting no conclusion. Run C's zero is unambiguous
under either.)

### The harness converts a harmless idiom into a harmful one

The mutating statements are not malformed SQL. `execute_sql` opens a fresh
read-write DuckDB connection per call and passes the statement straight
through, so a `CREATE TABLE` succeeds silently and changes the database.
Nothing in the ungoverned path ever refuses.

What the transcripts show is an escalation with a mechanical cause. Task 1711
(`schema_only`, glm), three consecutive calls:

```
1. CREATE TEMP TABLE p10 AS SELECT ...; SELECT ... LEFT JOIN ...
   → ERROR: Cannot perform non-inner join on subquery!
2. CREATE OR REPLACE TEMP TABLE p10 AS SELECT * FROM payments WHERE ...
   → Count 37                                     ← succeeded
3. CREATE OR REPLACE TEMP TABLE m10 AS SELECT ... FROM p10 ...
   → ERROR: Table with name p10 does not exist!   ← its own table, gone
```

**`TEMP` tables are connection-scoped and this harness opens a new connection
per tool call, so a temp table never survives to the next call.** The model
gets no signal that it did anything wrong — only that its table vanished — and
the natural repair is to stop using `TEMP`. That is precisely the step task 68
took, and it is the one row in 4,812 that actually corrupted the warehouse.

So the escalation is an **interaction between the model's idiom and the tool's
connection lifecycle**, not model misbehaviour alone. A session-scoped
connection would have let `TEMP` work, and that model would most likely never
have reached for a persistent table. This does not weaken the contrast — the
governed arms faced the identical harness and emitted nothing — but it does
mean **the size of the hazard is partly a property of this harness**, and a
different tool design would produce a different mutation count.

Capability changes the idiom rather than the intent. Sol uses CTEs on 59% /
55% of its ungoverned traces against glm's 32% / 30% and deepseek's 26% / 27%,
and attempts `CREATE TEMP` zero times against glm's 22 and deepseek's 3.
Holding an intermediate result is a need every model has; sol meets it inline,
the weaker models materialise it. (Every `schema_only` task that mutated was
also answered wrong — 9/9 on glm, 2/2 on deepseek — consistent with
struggle-driven escalation, but those arms are wrong ~77% of the time anyway,
so this is suggestive rather than a test.) Nothing here separates capability
from a trained disposition against writing; the idiom substitution is evidence
for the former, not proof against the latter.

**The governed arms did not attempt and get blocked — they did not attempt.**
A rejected tool call would still appear in the transcript as a tool call; the
count is of what the model *emitted*, not what survived. Zero means the
forbidden-operation rules in the contract deterred the behaviour rather than
intercepting it. That is a different and cheaper mechanism than enforcement,
and it is the one the transcripts actually support here.

The governed-tool counters (glm arm C: 218 inspect rejections, 527
enforcement blocks, 529 retry prompts; deepseek arm C: 29 / 328 / 370) are
descriptive instrumentation of *other* rules — projection and row limits,
mostly — and are still deliberately not offered as a governance claim.

### One attempt actually corrupted the warehouse

`db_corrupted` is `True` on exactly one row in 4,812: **task 68,
`schema_only`, deepseek**. Run C adds 1,604 rows and no corruption, in any
arm — see the governance caveat above: sol never attempted a write. It is 1 of the 24 because DuckDB `TEMP` tables die
with the connection; only this run escalated to *persistent* tables.

The escalation was deliberate, and the model narrates it. It first tried
`CREATE TEMP TABLE feats`, and on the next call got
`Table with name feats does not exist!`. Its reasoning:

> *"TEMP table per session? The previous CREATE TEMP TABLE in the same
> connection. The tool might use separate connections per call, so temp
> tables don't persist. Let me create a regular table…"*

and later, having decided tables now persist:

> *"Since tables persist across calls (non-temp), I can do sequential UPDATE
> operations."*

It then hand-rolled Gaussian elimination in SQL across twelve statements —
`DROP TABLE IF EXISTS m1; CREATE TABLE m1 AS …` through `m6` — to solve a
linear system. The task (68) is an *easy*-split question asking which
variables drive fees; the model answered
`monthly_volume, monthly_fraud_level, intracountry` against a gold of
`monthly_fraud_level`, so it was wrong as well as destructive.

This is the failure mode the contract exists to prevent, arriving exactly
where the design predicts and by a route nobody would have written a test
for: not malice, not a jailbreak, but a competent agent routing around a
session boundary to get its work done. Each individual statement is
reasonable. The integrity check caught it (`integrity_error: None`,
`close_error: None` — the check itself was sound), and the run used a
disposable per-worker copy, so nothing real was harmed.

**n=1 for actual corruption is an anecdote, not a rate.** The 144 attempts
are the defensible number.

## Operational findings

### Two harness defects found by a post-hoc code review

Neither changes a number; both undercut something this document asserts, and
both are recorded rather than quietly repaired. The harness is fixed for future
runs; the released rows are left exactly as they ran.

**Arm D's digest stamp is inert.** `build_result_row` stamped `digest()`
unconditionally, so all 1,203 `contract_hollow` rows carry the real contract's
hash instead of the hollow artifact's, and `hollow_digest()` had zero call
sites. Which artifact each arm loaded is not in doubt — it is fixed by the
harness, evidenced by arm D's empty-placeholder tool responses, and checked by
the 6-gram tests — but the per-row tamper-evidence claimed for arm D was not
there.

**The 50-row cap was not applied to `preview_table`.** Only `run_query` was
wrapped, and the library clamps `preview_table` at 100 rows on its own, so a
governed arm could receive up to 100 rows from a preview where an ungoverned
arm gets 50 from `execute_sql`. It happened on **31 preview calls across the
three runs**, out of many thousands of tool calls. Small, but it runs **in the
treatment's favour**, which is the direction that obliges disclosure.


**The forcing turn is nearly worthless for accuracy, and valuable anyway**
(run A). It fired 59 times and produced 1 correct answer. But `hit_limit` rows —
paid-for and unscoreable — went to zero in all four arms. Its real service is
preventing a bias: without it those 59 budget-exhausted runs would have been
dropped from the denominator, and they were 41 `schema_only`, 18
`manual_prompt`, **0 in either governed arm**. Excluding them would have
inflated the two baselines by silently discarding their hardest runs.

**Three error rows in run A's 1,604 (0.2%), all the same F1 failure.** Tasks 2760
(`manual_prompt`), 1765 (`schema_only`) and 2550 (`contract_hollow`) returned
*"Model token limit (16000) exceeded before any response was generated"* —
reasoning tokens consuming the whole output budget, the failure F1 fixed at
4,000 by raising the cap to 16,000. One in each non-contract arm, none in
`contract`. **With one event per arm this is far too small to read a
mechanism into**, and an earlier draft of this document over-read it as an
ungoverned-arm asymmetry; `contract_hollow` is fully governed and hit it too.
The mechanism is rarer, not fixed.

**No governance events in run A**; one in run B, and 144 mutation attempts
across both once the transcripts were scanned rather than only the
`db_corrupted` flag. See [Governance](#governance-the-ungoverned-arms-write-to-the-warehouse)
above — that section supersedes the "no governance events" reading this
document carried after run A.

**The forcing turn behaved identically on deepseek.** 112 forced answers —
55 `schema_only`, 42 `manual_prompt`, 15 `contract_hollow`, **0 `contract`** —
and `hit_limit` again near zero (2 rows). The same bias argument applies:
dropping budget-exhausted runs would have discarded the baselines' hardest
work. `contract` is the only arm that has never needed a forcing turn on
either model.

**A provider rate-limit silently destroyed 29% of run B, and the harness
recorded it as data.** This is the run's most transferable lesson, so it is
written out in full.

At row ~1,000 of 1,604 the pinned `baidu/fp8` endpoint began returning HTTP
429. The harness had no retry for it, so every 429 was written to the results
file as a terminal `error` row:

| rows | error rate |
|---|---|
| 1–1,000 | 0.2% |
| 1,001–1,200 | 31% |
| **1,201–1,604** | **100%** |

466 of the 468 error rows are 429s. The sweep then *finished early* — around
02:00 instead of the projected 04:49 — because a throttled request fails
instantly rather than slowly. **A monitor watching for progress and a clean
exit saw both.** Nothing looked wrong until the results were read.

Three things made this possible, and all three are worth stating:

1. **`allow_fallbacks: false` makes us responsible for the retry.** Pinning a
   single endpoint is correct for validity — it is what keeps quantization,
   price and throughput fixed across arms — but it converts a transient
   provider condition into a hard failure instead of a silent re-route to a
   different endpoint. Having taken that trade, the harness owed the
   condition a backoff and did not have one. Fixed in `9541722`: four
   attempts at 20/60/180/420s, matched on the HTTP status rather than the
   message (each vendor's body is wrapped verbatim), bounded so that a real
   quota exhaustion still terminates rather than hanging.
2. **"Completed" is not "completed successfully".** The supervision checked
   liveness and exit status, neither of which distinguishes a finished sweep
   from a sweep that gave up 400 rows early. A rate-of-scoreable-rows check
   would have caught it in minutes.
3. **A fast provider is a rate-limited provider.** `baidu/fp8` was pinned
   over `deepinfra/fp8` on measured throughput (156.8 vs 77.9 tok/s), which
   took the sweep from 0.24 to 3.37 rows/min. The same capacity that made it
   fast is the capacity that got metered. The endpoint was still returning
   429 on a probe 6.5 hours later — this was a daily quota, not a burst
   window — and `baseten/fp8`, the obvious alternative, had 404'd out of
   existence by then.

**The damage is to power, not to validity.** Errors are near-uniform across
arms (28–30% of rows each: 113 / 116 / 118 / 121) and 114 of the 122 lost
tasks lost all four arms together, because the runner schedules a task's arms
close in time. Independent per-arm failure would have left ~99 complete
tasks; 279 survived. Missingness is a property of *when* a task ran, not of
the task or the arm, so the complete-case set is an unbiased subsample for a
paired comparison — and the per-arm scored figures over each arm's own
denominator (172/283, 145/285, 88/279, 92/287) agree with the complete-case
figures to within a point.

The one residual caveat: the 122 lost tasks are slightly enriched for hard
(87% vs 83% overall), so run B's absolute accuracies are marginally
optimistic. Every arm is affected identically and the contrasts are unmoved.

## Related work: MotherDuck Guides, and what the comparison means

MotherDuck reports **418 of 419 DABStep questions correct (99.8%)** with
Gemini 3 Flash at ~$0.02/question, and separately **98.6% with a local
Qwen3.8 27B** at 4-bit quantization (96.4% at 3-bit), using "Guides" — markdown
context stored in the warehouse and fetched through their MCP server. Their headline is the same claim as ours, measured
the same way: Guides raise accuracy by **72 percentage points** and cut cost
~55% against an agent discovering context on its own.

That is an independent replication of this experiment's finding, at a much
higher absolute score. Two things are worth stating plainly about the gap.

**Most of it is work relocated, not reasoning improved.** DABStep's
difficulty is the fee-matching rule: NULL means "applies to every value",
plus list membership over account type, ACI and MCC. This contract carries
that rule as *prose the model must reimplement in SQL every time*, and the
model gets it wrong on set-membership questions. Encode the same rule once as
an executable macro and the agent's job collapses from *derive the rule* to
*call the macro*. Verified directly: tasks 1500 and 1502, both of which the
contract arm answered incorrectly, are answered **exactly** by

```sql
CREATE MACRO wild(col, val) AS
    (col IS NULL OR len(col) = 0 OR list_contains(col, val));
SELECT string_agg(ID::VARCHAR, ', ' ORDER BY ID) FROM fees
WHERE wild(account_type, 'O') AND wild(aci, 'C');
```

MotherDuck's own guidance lists pre-computed views among its five steps, and a
separate post of theirs reports a **progression**: vector-search retrieval of
context fragments "capped out around 88%"; baking the knowledge into the
warehouse as schema comments, macros and derived tables reached **93%**; a
hierarchical semantic layer over raw data, authored by a large model and
refined iteratively, reached **100%**. Each step moves work out of the agent
and into an artifact prepared in advance, so a large share of 55% -> 99.8% is
the semantic layer doing work the agent no longer has to do.

*(Corrected 2026-09-01: this section previously quoted "93.2% from a simple
prompt with views and macros". That phrasing appears in no MotherDuck post —
the verified figure is the 88 -> 93 -> 100 progression above, which makes the
same point with more of the mechanism visible.)*

**And the benchmark's shape flatters that approach.** 450 questions come from
26 templates; 294 of 332 hard tasks here are fee questions. Pre-built views
are maximally effective when the question space is enumerable in advance,
which is exactly DABStep's construction. A macro contributes nothing to a
question nobody wrote it for. The declarative layer measured in this document
is the layer that has to cover the *unanticipated* question — which makes
55.1% a measurement of a harder thing than the benchmark's ceiling suggests,
not a worse attempt at the same thing.

**Two coupling costs are stated in the sources themselves.** The author of the
100% result writes that *"The recipe is portable [...] The tuned artifact is
coupled. The serving model is a cheap, swappable commodity right up until you
swap it, at which point you're not changing a setting, you're re-running the
loop."* A layer refined against one model is re-tuned when the model changes;
our contract was frozen once and run unchanged against two model families.
Separately, third-party commentary on Guides warns *"Do not hardcode answers to
questions [...] will fail the ones you have not seen, including a held-out test
set"* and urges measuring *"whether the Guide generalizes to new questions, not
whether it clears the benchmark you tuned it against"*.

**Methodology is not disclosed for the 99.8% run.** The post defers to a
repository and states neither the provenance of the Guide content, nor why 419
rather than 450 questions, nor whether the official scorer was used, nor how
many iterations it took. This is not an accusation — but our leaderboard
analysis found a strong tuning gradient (organisations submitting once: 19.6%
median hard; submitting 21+ times: 54.2%), which is equally consistent with
genuine iterative improvement. A reader cannot rule it in or out, and neither
can we.

### What is unique here

Their comparison is Guides vs no-Guides: two accuracy numbers, from which no
mechanism can be recovered. Was it the content, the MCP fetch loop, or the
views? This experiment answers that question and, as far as we know, is the
only one that does:

| | MotherDuck | this experiment |
|---|---|---|
| comparison | Guides vs none | four arms |
| separates content from tooling | no | **yes** (`contract_hollow`) |
| statistical test | two accuracies | paired McNemar, every arm pair |
| replicated on further models | no | **yes**, two more, same direction |
| artifact frozen before seeing questions | undisclosed | **yes**, digest-pinned |
| harness and transcripts published | defers to a repo | full, 4,812 traces |

**The gain is mostly content, and the content term dominates the scaffolding
term on every model** — by 2x on the one model where the scaffolding term is
large, and unboundedly on the other two. That decomposition is the
contribution; the accuracy number is not.

### The deliberate non-response

The obvious reaction is to make this library's metric semantics executable —
a `compute_metric` / `compose_metric_query` tool that runs a declared
`sql_expression` rather than describing it — and re-run. That feature is
worth building, and it is already on the roadmap for independent reasons.

It is deliberately **not** being built before publishing this, because it
would be designed from knowledge of which tasks this run lost. That is
fitting to the test set, and it would spend the strongest methodological
asset here: a contract authored from the vendor manual alone and frozen
before any question was read. The order is measure, ship, re-measure — this
document is the baseline the feature will be judged against.

## What these runs do not show

- **The pre-registered primary comparison was never run.** It names
  `deepseek-v4-pro-0813`; `dce.stats` prints that section empty, by design.
  Run C substitutes `gpt-5.6-sol`, chosen on a 40-task capability probe. Every
  contrast involving run C is therefore secondary and exploratory, including
  the derivation-gap result, however sharp it looks.
- **Three models, one frontier point.** The capability claims rest on a single
  strong model. The one two-point trend this document previously reported (the
  contract's advantage shrinking with capability) was **falsified** by the
  third point, which is the best available evidence for how little a two-point
  trend is worth here. Treat the derivation-gap collapse the same way until it
  has a second frontier model under it.
- **Run C differs from A and B in more than capability.** Different provider,
  different model family, and **temperature is not held at 0** because
  `gpt-5.6-sol` does not accept the parameter. Any run-C-versus-others
  contrast confounds capability with all three.
- **The governance result does not replicate on run C, for lack of a hazard.**
  Sol never attempted a mutating statement in any arm, governed or not. The
  deterrence finding rests on runs A and B.
- **Run B is a 279-task subsample.** Unbiased for the paired contrasts (see
  Operational findings), but its confidence intervals are ~20% wider than run
  A's and its absolute accuracies are marginally optimistic. The 122 missing
  tasks are re-runnable, on a different provider than the file's other 279.
- **Reconstructed golds, not official ones.** 401/450 tasks by plurality
  consensus at threshold 0.75, with 5 verified-wrong golds excluded. Close
  enough to compare against the leaderboard band; not a leaderboard
  submission, which would need all 450.
- **k=1 on all three runs.** No repeat runs, so the flip rate is unmeasured.
  One task (1480) was observed flipping verdict between two identical runs
  during development. With 86–164 discordant pairs the headline is not at
  risk, but no individual task's verdict should be treated as stable. **This
  is the largest remaining gap**: the three runs are different models, not
  replicates, so nothing here estimates within-condition variance — and run C
  is the one that needs it most, since its temperature is not pinned.
- **Not a generalisation about analytics.** See the fee-bucket caveat. The
  non-fee bucket shows no contract advantage on any of the three models, and
  on run C `schema_only` leads it outright.
- **The runs are not perfectly comparable.** Run A's contract arms carried
  a validator defect (`COUNT(*)` rejected as `SELECT *`, fixed in `cde8b20`);
  run B has the fix, and its inspect-rejection count falls from 218 to 29
  accordingly. Run A's governed arms were therefore handicapped and run B's
  were not, which is the direction that would make run A's contract result an
  underestimate. The runs also used different providers, and run B lost 29% of
  its rows. Each run's internal contrasts are sound; **the cross-run
  differences should be read as suggestive only.**
- **The contract's provenance is self-attested.** Its header records that
  every fact came from `manual.md` and `payments-readme.md` only, and it was
  frozen and digest-pinned ~35 commits before run A. A reviewer can verify it
  contains nothing beyond those documents; nobody outside can verify that no
  benchmark question was read while authoring it.

## Reproducing

```bash
uv sync && uv run python -m dce.prepare      # golds must hash to a4388e9ee823
uv run python -m dce.hollow                  # regenerate contract_hollow/

# run A
uv run python -m dce.runner --models z-ai/glm-5.3-flash \
    --workers 6 --max-spend 20 --out results/glm-full.jsonl
# run B
uv run python -m dce.runner --models deepseek/deepseek-v4-flash-0731 \
    --workers 6 --max-spend 20 --out results/dsflash-full.jsonl
# run C -- see the note on --max-spend below
uv run python -m dce.runner --models openai/gpt-5.6-sol \
    --workers 6 --max-spend 910 --out results/sol-full.jsonl

uv run python -m dce.stats results/glm-full.jsonl

# post-hoc analyses (no API calls; need data/ and traces/)
uv run python analysis/coverage.py                       # contract -> gold, 176/176
uv run python analysis/buckets.py results/*.jsonl        # arms by macro/derived
uv run python analysis/clauses.py --within glm-full dsflash-full sol-full
uv run python analysis/counterfactuals.py results/glm-full.jsonl
```

Run A: ~3.5 hours at 6 workers, $3.26. Run B: ~4 hours, $6.32, of which the
last 400 rows are 429s — re-run those with `--retry error` under the backoff
added in `9541722`, and note that `baidu/fp8` may no longer serve them.
Run C: ~7 hours, $72.36, no failures.

**`--max-spend` also caps concurrency, and on an expensive model that bites.**
The ledger reserves a whole task group's worst case before dispatching it:
4 arms x the 3,294,000-token runaway guard x sol's $10/M output rate =
**$131.76 per group**. Run C was first launched at `--max-spend 140`, which
admitted exactly one group at a time — so `--workers 6` ran effectively serial
at 33 rows/hour — and then stopped outright at 175 rows when banked spend
reached $8.55 and `8.55 + 131.76 > 140`. Re-launched at `--max-spend 910`
(6 x 131.76 plus headroom) it ran at ~400 rows/hour and finished for $72.36.
The reservation is ~200x the worst row actually observed, so the cap must be
sized off the *reserve*, not off expected spend.

Resumable: see [`deploy/README.md`](deploy/README.md) for the unattended
setup, which is how all three runs were executed (systemd on a VPS, run A with
a restart mid-flight to verify resume; run C resumed cleanly onto its own
175-row prefix after the cap trip, single commit and digest throughout).
Per-run transcripts including the model's own reasoning are under
`traces/glm-full/`, `traces/dsflash-full/` and `traces/sol-full/`, one gzipped
JSON per row.
