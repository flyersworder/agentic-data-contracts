# DABStep contract-context eval — findings

**Two independent runs of the same four-arm ablation, on two models.**
Shared across both: **contract digest** `sha256:e438ecf7…`, **hollow digest**
`sha256:c46a767d…` · **golds** `a4388e9ee823` (401/450 tasks) · **scorer**
DABStep's own, vendored at `d4431c2e` · `reasoning_effort=medium`,
temperature 0.

| run | model | rows | scoreable | cost | raw | traces |
|---|---|---:|---:|---:|---|---|
| **A** | `z-ai/glm-5.3-flash` (`z-ai` fp8) | 1,604 | 401/401 tasks | $3.26 | `results/glm-full.jsonl` | `traces/glm-full/` |
| **B** | `deepseek/deepseek-v4-flash-0731` (`baidu` fp8) | 1,604 | **279/401 tasks** | $6.32 | `results/dsflash-full.jsonl` | `traces/dsflash-full/` |

Run A at commit `6836139` (arms A–C) / `d75d269` (arm D); run B at `4d230fa`.

Run B lost 122 of 401 tasks to provider rate-limiting partway through and is
reported on its 279-task complete-case set throughout. That truncation is
itself a finding — see [Operational findings](#operational-findings). It cost
power, not validity: the loss is near-uniform across arms (28–30% of rows
each) and clusters in time rather than by task or arm.

## Headline

**`contract` wins on both models, by a wide and significant margin.**

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

Every pairwise paired McNemar — same tasks, same model, same scorer. `a_only`
is tasks the left arm alone got right; `b_only` the right arm alone:

| comparison | glm-5.3-flash (n=401) | deepseek-v4-flash (n=279) |
|---|---|---|
| schema_only vs **contract** | p=3e-31 · 14 / **155** | p=2e-15 · 14 / **93** |
| manual_prompt vs **contract** | p=2e-17 · 29 / **134** | p=**0.0067** · 30 / **56** |
| contract_hollow vs **contract** | p=9e-24 · 23 / **149** | p=8e-17 · 13 / **96** |
| schema_only vs manual_prompt | p=7e-06 · 14 / 50 | p=2e-12 · 5 / 58 |
| manual_prompt vs contract_hollow | p=0.033 · 55 / 34 | p=4e-13 · 63 / 6 |
| **schema_only vs contract_hollow** | **p=0.058** · 20 / 35 | **p=0.61** · 19 / 15 |

The last row is the one that carries the ablation, and it is discussed next.

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
                          glm-5.3-flash          deepseek-v4-flash
schema_only                   13.9%                  22.6%
  + tools and procedure  →    19.3%  (+5.4 pp)       22.6%  (+0.0 pp)   ← contract_hollow
  + contract knowledge   →    55.1%  (+35.8 pp)      56.6%  (+34.0 pp)  ← contract
```

**The content step is the whole effect. The scaffolding step is not
distinguishable from zero.**

That second clause is a correction to an earlier draft of this document,
which read the glm scaffolding step as "+5.4 pp, 13% of the gain" and called
it "real but small". It is a point estimate that does not survive a
significance test. Paired McNemar on `contract_hollow` vs `schema_only`:

| | Δ hard | discordant | p |
|---|---:|---:|---:|
| glm-5.3-flash | +5.4 pp | 55 (20 / 35) | 0.058 |
| deepseek-v4-flash | +0.0 pp | 34 (19 / 15) | 0.61 |
| pooled | — | 89 (39 / 50) | 0.29 |

The two runs do not even agree on the sign. Nine governed tools, the
retrieval instruction, the table allow-list and the forbidden-operation rules
— with no semantics behind them — buy nothing measurable over a bare schema.

This is a *stronger* claim than the one it replaces, because it forecloses
the deflationary reading. A reader's first objection to arm C is that the
contract might be helping through some mechanism other than its knowledge:
narrowing the tables in view, imposing a procedure, or simply making the
agent slow down. Arm D holds every one of those fixed and empties only the
prose. It lands on top of the bare-schema baseline.

**Tooling is not a substitute for content.** On glm, `contract_hollow`
(19.3% hard) scores *below* `manual_prompt` (22.9%); on deepseek the gap is
wider (22.6% vs 42.9%, p=4e-13). Nine governed tools with nothing behind them
are worse than raw SQL with the manual pasted into the prompt.

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

The transcripts confirm it end to end: arm D calls `lookup_domain` on
**272 of 401 tasks** and receives the empty placeholder **282 times**. The
tool surface was exercised; there was simply nothing behind it.

Prompt length is deliberately not held constant (1,984 vs 2,475 chars).
Padding with filler would swap one confound for another — text that reads as
content but carries none. The variable under test is knowledge, not tokens.

## The result is entirely in the hard tasks

On easy tasks the four arms are within noise of each other on both runs
(glm 61–74%, deepseek 70–91%, overlapping intervals throughout) and the
contract arm is not even the best of them — `manual_prompt` leads on easy
tasks in both runs. The contract does nothing for questions that need no
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

## The weaker the model, the more the contract matters

The two runs bracket a clear interaction. Ordered by how much the model can
do with a bare schema:

| | schema_only (hard) | manual_prompt (hard) | contract (hard) | **contract − manual_prompt** |
|---|---:|---:|---:|---:|
| glm-5.3-flash | 13.9% | 22.9% | 55.1% | **+32.2 pp** |
| deepseek-v4-flash | 22.6% | 42.9% | 56.6% | **+13.7 pp** |

Both arms improve with the stronger model, but `manual_prompt` improves far
more (+20.0 pp) than `contract` does (+1.5 pp). The contract arm lands in
almost the same place on both models — 55.1% and 56.6% — while the
prompt-based arm climbs to meet it.

Two readings, and the evidence does not separate them:

1. **Structured context substitutes for model capability.** The contract
   supplies what a weaker model cannot infer, so the weaker the model, the
   more it is worth. A stronger model extracts more of the same semantics
   from unstructured prose on its own.
2. **The contract arm is near a ceiling this benchmark imposes.** Both models
   land at ~56% hard, and the fee-bucket analysis below shows a 30-task
   template that *no* arm on *either* model solves. If the ceiling is the
   binding constraint, the shrinking gap says nothing about capability.

Reading 2 is not idle: the near-identical 55.1%/56.6% is more consistent with
a shared benchmark ceiling than with a coincidence. Distinguishing the two
needs a frontier model, where reading 1 predicts the gap keeps shrinking and
reading 2 predicts `contract` stays pinned near 56% while `manual_prompt`
converges on it. That run has not been done, and **no claim about frontier
models should be made from these two.**

## Where the effect lives, and why that is a caveat

DABStep's hard set is dominated by one question family: **294 of 332 hard
tasks (89%) mention fees.**

| run | bucket | n | schema_only | contract_hollow | manual_prompt | contract |
|---|---|---:|---:|---:|---:|---:|
| glm | fee (hard) | 294 | 41 (13.9%) | 55 (18.7%) | 72 (24.5%) | **174 (59.2%)** |
| glm | non-fee (hard) | 38 | 5 | 9 | 4 | **9** |
| deepseek | fee (hard) | 196 | 45 (23.0%) | 44 (22.4%) | 88 (44.9%) | **120 (61.2%)** |
| deepseek | non-fee (hard) | 30 | 6 | 7 | **9** | 8 |

"Hard accuracy" on this benchmark is very close to "fee-question accuracy",
and the frozen contract encodes fee-domain semantics. The honest statement
is: **a contract carrying a domain's semantics produces a large gain on
questions in that domain.** It is not evidence about analytics questions in
general, and this benchmark cannot supply that evidence — 450 tasks are
generated from only 26 templates.

**The non-fee bucket shows no contract advantage on either model** — 9 vs 4
on glm, 8 vs 9 on deepseek, against `manual_prompt`. With 38 and 30 tasks
these are far too small to read as a negative result, but they are equally
unable to support the general claim, and the deepseek run puts `contract`
fractionally *behind*. Both runs are consistent with the effect being
domain-specific by construction: outside the domain the contract describes,
it is inert. That is the expected behaviour of a domain contract, and it is
also precisely why this benchmark cannot answer the general question.

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

| | glm `macro` | glm `derived` | ds-flash `macro` | ds-flash `derived` |
|---|---:|---:|---:|---:|
| n | 176 | 148 | 118 | 101 |
| schema_only | 2.8% | 25.0% | 8.5% | 34.7% |
| contract_hollow | 10.2% | 25.7% | 10.2% | 31.7% |
| manual_prompt | 14.8% | 32.4% | 38.1% | 44.6% |
| **contract** | **60.8%** | **45.9%** | **62.7%** | **46.5%** |
| *compiled contract (oracle)* | *100%* | — | *100%* | — |

Three readings, in descending order of confidence.

**The contract's advantage concentrates where its semantics apply directly.**
Over `schema_only` it is +58.0 pp (glm) and +54.2 pp (ds-flash) on `macro`,
against +20.9 pp and +11.8 pp on `derived`. Over `manual_prompt` the same shape
is sharper still: +46.0 / +24.6 on `macro`, +13.5 / +1.9 on `derived`. This is
what a domain contract should do, and it is the same domain-specificity the
fee/non-fee split shows, measured on a cleaner boundary.

**The derivation gap is large and nearly model-invariant.** On `macro` the
information is provably sufficient, yet the contract arm recovers 60.8% and
62.7% of it. **Roughly 37–39 pp of the hard set's headroom is neither missing
context nor benchmark difficulty — it is the agent failing to apply knowledge
it was given and provably had.** That the two figures land within 2 pp of each
other, on models a generation apart in capability, is the sharpest version of
the "contract arm lands in almost the same place on both models" observation
above — now localised to the bucket where it means something.

**`contract_hollow` tracks `schema_only` in both buckets**, consistent with the
null result reported earlier; the bucket split gives it nowhere to hide.

### A prediction, recorded before the third model lands

The `gpt-5.6-sol` sweep was still running when this section was written. The
two readings of the capability trend make opposite predictions about it:

- If the derivation gap is capability-bound, sol's contract arm on `macro`
  should sit well above 62.7% and move toward the oracle's 100%.
- If it is a property of the harness or the task format rather than the model,
  sol should land near 61–63% like the other two.

Recorded here before the data exists, so the answer is a test rather than a
description.

## Efficiency

**`contract` is the cheapest arm on both models while also being the most
accurate.** Accuracy and cost move together here; there is no tradeoff to
report.

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

The governed arm wins while doing *less* work: on glm, 35% fewer tool calls
than the floor and under a third of the reasoning; on deepseek, 40% fewer
turns and 56% less reasoning per row. It is not thinking harder — it has less
to search for. The pattern is the same on both models, and `contract` is the
only arm that never needed a forced answer on either run.

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
(56.6% vs 55.1%) despite the two runs differing in model and provider. The
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
| **total** | **51 (10 tasks)** | **93 (14 tasks)** | **0** | **0** |

**144 mutating statements across 24 tasks in the ungoverned arms; zero in
either governed arm, on either model.** Fisher exact on traces containing at
least one such statement: p=4e-07 for glm alone, p=6e-08 pooled. (Run B's own
3 events give p=0.13 — the run is underpowered here as everywhere else; the
result rests on run A and the pooling.)

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

`db_corrupted` is `True` on exactly one row in 3,208: **task 68,
`schema_only`, deepseek**. It is 1 of the 24 because DuckDB `TEMP` tables die
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
| replicated on a second model | no | **yes**, same direction |
| artifact frozen before seeing questions | undisclosed | **yes**, digest-pinned |
| harness and transcripts published | defers to a repo | full, 3,208 traces |

**The gain is content, not scaffolding** — and the scaffolding term is not
distinguishable from zero on either model. That decomposition is the
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

- **Two flash-tier models, no frontier arm.** The pre-registered primary
  comparison is `deepseek-v4-pro-0813` and has not been run — `dce.stats`
  prints that section empty, by design. The
  [capability interaction](#the-weaker-the-model-the-more-the-contract-matters)
  has two live readings and these two models cannot separate them.
- **Run B is a 279-task subsample.** Unbiased for the paired contrasts (see
  Operational findings), but its confidence intervals are ~20% wider than run
  A's and its absolute accuracies are marginally optimistic. The 122 missing
  tasks are re-runnable, on a different provider than the file's other 279.
- **Reconstructed golds, not official ones.** 401/450 tasks by plurality
  consensus at threshold 0.75, with 5 verified-wrong golds excluded. Close
  enough to compare against the leaderboard band; not a leaderboard
  submission, which would need all 450.
- **k=1 on both runs.** No repeat runs, so the flip rate is unmeasured. One
  task (1480) was observed flipping verdict between two identical runs during
  development. With 86–155 discordant pairs the headline is not at risk, but
  no individual task's verdict should be treated as stable. **This is the
  largest remaining gap**: the two runs are different models, not replicates,
  so nothing here estimates within-condition variance.
- **Not a generalisation about analytics.** See the fee-bucket caveat. The
  non-fee bucket shows no contract advantage on either model.
- **The two runs are not perfectly comparable.** Run A's contract arms carried
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

uv run python -m dce.stats results/glm-full.jsonl
```

Run A: ~3.5 hours at 6 workers, $3.26. Run B: ~4 hours, $6.32, of which the
last 400 rows are 429s — re-run those with `--retry error` under the backoff
added in `9541722`, and note that `baidu/fp8` may no longer serve them.

Resumable: see [`deploy/README.md`](deploy/README.md) for the unattended
setup, which is how both runs were executed (systemd on a VPS, run A with a
restart mid-flight to verify resume). Per-run transcripts including the
model's own reasoning are under `traces/glm-full/` and `traces/dsflash-full/`,
one gzipped JSON per row.
