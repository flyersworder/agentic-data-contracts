# DABStep contract-context eval — findings

**Run:** 2026-08-31, **1,604 runs across four arms**, complete.
**Commit** `6836139` (arms A–C) / `d75d269` (arm D) · **contract digest**
`sha256:e438ecf7…`, **hollow digest** `sha256:c46a767d…` · **golds**
`a4388e9ee823` (401/450 tasks) · **scorer** DABStep's own, vendored at
`d4431c2e` · **model** `z-ai/glm-5.3-flash`, pinned to the `z-ai` fp8
endpoint, `reasoning_effort=medium`, temperature 0 · **cost** $3.26 ·
**raw rows** `results/glm-full.jsonl` · **transcripts** `traces/glm-full/`.

## Headline

| Arm | tools | knowledge | overall | **hard (n=332)** | easy (n=69) | cost |
|---|:-:|:-:|---:|---:|---:|---:|
| **contract** | governed | contract | **232/401 (57.9%)** | **183 (55.1%)** | 49 (71.0%) | **$0.67** |
| manual_prompt | raw SQL | manual in prompt | 127/401 (31.7%) | 76 (22.9%) | 51 (73.9%) | $0.97 |
| **contract_hollow** | **governed** | **none** | 106/401 (26.4%) | **64 (19.3%)** | 42 (60.9%) | $0.85 |
| schema_only | raw SQL | none | 91/401 (22.7%) | 46 (13.9%) | 45 (65.2%) | $0.76 |

Paired McNemar, same tasks, same model, same scorer — every arm against
`contract`:

| comparison | p | discordant | contract wins | contract loses |
|---|---:|---:|---:|---:|
| schema_only vs contract | **0.0000** | 168 | 154 | 14 |
| manual_prompt vs contract | **0.0000** | 163 | 134 | 29 |
| contract_hollow vs contract | **0.0000** | 171 | 148 | 23 |

## The ablation is the result

`contract_hollow` is arm C with its knowledge removed and everything else
held fixed: the same nine governed tools, the same procedural instruction
(*"Look up the domain and the metrics that apply before writing SQL, and
validate every query with inspect_query"*), the same allowed tables,
forbidden operations and rules. It exists because arm C otherwise differs
from the baselines in three ways at once, and "you also gave it tools and
told it to look things up" is the first objection any reader will raise.

On hard tasks the gain decomposes cleanly:

```
schema_only               13.9%
  + tools and procedure → 19.3%   (contract_hollow)    +5.4 pp   13% of the gain
  + contract knowledge  → 55.1%   (contract)          +35.8 pp   87% of the gain
```

**The scaffolding is real but small; the content does the work.**

Two further readings sharpen it:

**Tooling is not a substitute for content.** `contract_hollow` (19.3% hard)
scores *below* `manual_prompt` (22.9%). Nine governed tools with nothing
behind them are worse than raw SQL with the manual pasted into the prompt.

**And it is the most expensive arm per correct answer** — $0.85 for 106
correct against contract's $0.67 for 232. It burns the most reasoning of any
arm (543,565 tokens, against contract's 154,247): the model calls
`lookup_domain`, is told *"No documentation is available for this domain"*,
and searches harder. Which is also the check that the control did what it
claims — see below.

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

On easy tasks the four arms are within noise of each other (61–74%,
overlapping intervals). The contract does nothing for questions that need no
domain knowledge — which is what it should do. Every bit of the effect is in
the hard split.

**`manual_prompt` reproduces the published leaderboard band, which is the
validity check that licenses the comparison at all.** Credible named
baselines sit at ~20–26% hard (Google simple_baseline 26%, Adyen GPT-5.4
22.2%, HF Claude 4 Sonnet 19.8%). Our reimplementation of that same approach
— DABStep's `manual.md` verbatim in the prompt — lands at **22.9%**, on a
flash-tier model weaker than any of those. The harness measures what the
leaderboard measures.

## Where the effect lives, and why that is a caveat

DABStep's hard set is dominated by one question family: **294 of 332 hard
tasks (89%) mention fees.**

| bucket | n | schema_only | contract_hollow | manual_prompt | contract |
|---|---:|---:|---:|---:|---:|
| fee questions (hard) | 294 | 41 (13.9%) | 55 (18.7%) | 72 (24.5%) | **174 (59.2%)** |
| non-fee (hard) | 38 | 5 | 9 | 4 | **9** |

"Hard accuracy" on this benchmark is very close to "fee-question accuracy",
and the frozen contract encodes fee-domain semantics. The honest statement
is: **a contract carrying a domain's semantics produces a large gain on
questions in that domain.** It is not evidence about analytics questions in
general, and this benchmark cannot supply that evidence — 450 tasks are
generated from only 26 templates. The non-fee hard bucket (38 tasks, 4–9
correct) is too small to read in either direction.

A 30-task sub-bucket ("most expensive MCC for a transaction of N euros")
scores 1–2 out of 30 for *every* arm. A template no arm solves is a property
of the benchmark, not a weakness of any arm — recorded because a per-arm
reading of that bucket alone would have looked like a contract failure.

## Efficiency

| | schema_only | manual_prompt | contract_hollow | contract |
|---|---:|---:|---:|---:|
| cost | $0.76 | $0.97 | $0.85 | **$0.67** |
| input tokens / row | 60,139 | 95,449 | 66,250 | **52,412** |
| turns / row | 12.1 | 9.8 | 8.4 | **7.1** |
| tool calls | 5,137 | 4,286 | 4,105 | **3,355** |
| reasoning tokens | 444,541 | 416,298 | **543,565** | **154,247** |
| forced answers | 41 | 18 | 0 | 0 |

The governed arm wins while doing *less* work: 35% fewer tool calls than the
floor and under a third of the reasoning. It is not thinking harder — it has
less to search for. Note this reverses the pre-fix smoke run, where the
contract arm looked 11x more expensive; that was an artifact of
`MAX_ROWS=1000` poisoning the context and cost accounting that ignored cache
pricing (F4, F5).

## What the transcripts show

**No gold leakage.** Searching every system and user prompt for its own gold
returns 23 hits, all benign: 15 are multiple-choice questions where the gold
is an option printed in the question and visible to every arm; 8 are in
`manual_prompt`, where benchmark-supplied vocabulary happens to be the
answer. **Zero in either contract arm.**

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

### The mechanism, on one task

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
`cde8b20`; this sweep predates the fix and has not been re-run.

## Where the contract arm loses — 33 tasks

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

## Operational findings

**The forcing turn is nearly worthless for accuracy, and valuable anyway.**
It fired 59 times and produced 1 correct answer. But `hit_limit` rows —
paid-for and unscoreable — went to zero in all four arms. Its real service is
preventing a bias: without it those 59 budget-exhausted runs would have been
dropped from the denominator, and they were 41 `schema_only`, 18
`manual_prompt`, **0 in either governed arm**. Excluding them would have
inflated the two baselines by silently discarding their hardest runs.

**Three error rows in 1,604 (0.2%), all the same F1 failure.** Tasks 2760
(`manual_prompt`), 1765 (`schema_only`) and 2550 (`contract_hollow`) returned
*"Model token limit (16000) exceeded before any response was generated"* —
reasoning tokens consuming the whole output budget, the failure F1 fixed at
4,000 by raising the cap to 16,000. One in each non-contract arm, none in
`contract`. **With one event per arm this is far too small to read a
mechanism into**, and an earlier draft of this document over-read it as an
ungoverned-arm asymmetry; `contract_hollow` is fully governed and hit it too.
The mechanism is rarer, not fixed.

**No governance events.** `db_corrupted` is false on all 1,604 rows: no arm
mutated the warehouse. The governed-tool counters (218 inspect rejections,
527 enforcement blocks, 529 retry prompts in arm C) are descriptive
instrumentation and are deliberately *not* offered as a governance claim.

## What this run does not show

- **One model.** glm-5.3-flash only. The pre-registered primary comparison is
  `deepseek-v4-pro-0813` and has not been run — `dce.stats` prints that
  section empty, by design. Cross-family and frontier arms are also unrun.
- **Reconstructed golds, not official ones.** 401/450 tasks by plurality
  consensus at threshold 0.75, with 5 verified-wrong golds excluded. Close
  enough to compare against the leaderboard band; not a leaderboard
  submission, which would need all 450.
- **k=1.** No repeat runs, so the flip rate is unmeasured. One task (1480)
  was observed flipping verdict between two identical runs during
  development. With 148–154 discordant pairs the headline is not at risk, but
  no individual task's verdict should be treated as stable.
- **Not a generalisation about analytics.** See the fee-bucket caveat.
- **Both contract arms ran with a validator defect** (`COUNT(*)` rejected as
  `SELECT *`, fixed in `cde8b20` after this sweep). It never touched the
  baselines, so the direction is known even though the magnitude is not.
- **The contract's provenance is self-attested.** Its header records that
  every fact came from `manual.md` and `payments-readme.md` only, and it was
  frozen and digest-pinned ~35 commits before the sweep. A reviewer can
  verify it contains nothing beyond those documents; nobody outside can
  verify that no benchmark question was read while authoring it.

## Reproducing

```bash
uv sync && uv run python -m dce.prepare      # golds must hash to a4388e9ee823
uv run python -m dce.hollow                  # regenerate contract_hollow/
uv run python -m dce.runner --models z-ai/glm-5.3-flash \
    --workers 6 --max-spend 20 --out results/glm-full.jsonl
uv run python -m dce.stats results/glm-full.jsonl
```

~3.5 hours at 6 workers, ~$3.26. Resumable: see
[`deploy/README.md`](deploy/README.md) for the unattended setup, which is how
this run was executed (systemd on a VPS, with a restart mid-flight to verify
resume). Per-run transcripts including the model's own reasoning are under
`traces/glm-full/`, one gzipped JSON per row.
