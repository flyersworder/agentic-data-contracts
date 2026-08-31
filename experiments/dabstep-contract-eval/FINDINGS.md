# DABStep contract-context eval — findings

**Run:** 2026-08-31, 1,203 runs, complete. **Commit** `6836139` · **contract digest**
`sha256:e438ecf7…` · **golds** `a4388e9ee823` (401/450 tasks) · **scorer**
DABStep's own, vendored at `d4431c2e` · **model** `z-ai/glm-5.3-flash`, pinned to
the `z-ai` fp8 endpoint, `reasoning_effort=medium`, temperature 0 ·
**cost** $2.40 · **raw rows** `results/glm-full.jsonl`.

Every row carries the same `commit_sha`, contract digest, gold hash, scorer,
provider and quantization. Nothing below mixes configurations.

## Headline

| Arm | overall | **hard (n=332)** | easy (n=69) | cost |
|---|---:|---:|---:|---:|
| **contract** | **232/401 (57.9%)** | **183 (55.1%)** | 49 (71.0%) | **$0.67** |
| manual_prompt | 127/401 (31.7%) | 76 (22.9%) | 51 (73.9%) | $0.97 |
| schema_only | 91/401 (22.7%) | 46 (13.9%) | 45 (65.2%) | $0.76 |

Paired McNemar on the same tasks, same model, same scorer:

| comparison | p | discordant | contract wins | contract loses |
|---|---:|---:|---:|---:|
| schema_only vs contract | **0.0000** | 168 | 154 | 14 |
| manual_prompt vs contract | **0.0000** | 163 | 134 | 29 |

The contract arm is better on **9.4x** as many discordant pairs as the
schema-only floor and **4.6x** as many as the manual-in-prompt baseline.

## The result is entirely in the hard tasks

On easy tasks the three arms are indistinguishable (65–74%, overlapping
intervals). The contract does nothing for questions that need no domain
knowledge — which is what it should do. Every bit of the effect is in the
hard split, where the spread is 13.9% / 22.9% / 55.1%.

**`manual_prompt` reproduces the published leaderboard band, which is the
validity check that matters.** Credible named baselines sit at ~20–26% hard
(Google simple_baseline 26%, Adyen GPT-5.4 22.2%, HF Claude 4 Sonnet 19.8%).
Our reimplementation of that same approach — DABStep's `manual.md` verbatim in
the prompt — lands at **22.9%**, on a flash-tier model weaker than any of
those. The harness is measuring the same thing the leaderboard measures.

## Where the effect actually lives, and why that is a caveat

DABStep's hard set is dominated by one question family: **294 of 332 hard
tasks (89%) mention fees.**

| bucket | n | schema_only | manual_prompt | contract |
|---|---:|---:|---:|---:|
| fee questions (hard) | 294 | 41 (14%) | 72 (24%) | **174 (59%)** |
| non-fee (hard) | 38 | 5 | 4 | **9** |

So "hard accuracy" on this benchmark is very close to "fee-question
accuracy", and the frozen contract encodes fee-domain semantics. The honest
statement is: **a contract carrying a domain's semantics produces a large,
unambiguous gain on questions in that domain.** It is not evidence about
analytics questions in general, and this benchmark cannot supply that
evidence — 450 tasks are generated from only 26 templates.

The non-fee hard bucket is too small (38 tasks, 4–9 correct) to support any
claim at all, in either direction.

A 30-task sub-bucket ("most expensive MCC for a transaction of N euros")
scores **1/30, 2/30 and 1/30** across the three arms. A template no arm
solves is a property of the benchmark, not a weakness of any arm — recorded
here because a per-arm reading of that bucket alone would have looked like a
contract failure.

## The contract arm is cheaper, not merely better

| | schema_only | manual_prompt | contract |
|---|---:|---:|---:|
| cost | $0.76 | $0.97 | **$0.67** |
| input tokens / row | 60,139 | 95,449 | **52,412** |
| turns / row | 12.1 | 9.8 | **7.1** |
| tool calls (total) | 5,137 | 4,286 | **3,355** |
| reasoning tokens (total) | 444,541 | 416,298 | **154,247** |

The governed arm wins while doing *less* work: 31% fewer tool calls than the
floor and about a third of the reasoning tokens. It is not thinking harder —
it has less to search for. Note this reverses the pre-fix smoke run, where
the contract arm looked 11x more expensive; that figure was an artifact of
`MAX_ROWS=1000` poisoning the context and of cost accounting that ignored
cache pricing (findings F4, F5).

## What the transcripts show

All 1,203 runs kept a full transcript (`traces/glm-full/`), including the
model's own reasoning. Three checks, then the mechanism.

**No gold leakage.** Searching every system and user prompt for its own gold
string returns 23 hits, all benign: 15 are multiple-choice questions where the
gold is one of the options printed in the question (`"C. both ip_address and
email_address"`) and is therefore visible to all three arms equally; the other
8 are in `manual_prompt`, where benchmark-supplied vocabulary such as
`intracountry` happens to be the answer. **Zero in the contract arm.**

**The arms are separated as designed.** System prompts: schema_only 281
chars, manual_prompt 24,177, contract 2,475. Tool vocabularies are disjoint —
the baselines get `list_tables` / `describe_table` / `execute_sql`, the
contract arm gets the nine governed tools.

**The fetch-gating worry did not materialise.** `lookup_domain` is called on
331 of 401 tasks. The concern (from MotherDuck's write-up) was that a
low-reasoning model would skip fetch-gated context; glm is the weakest
reasoner of the four pinned models, which is why it was run first. But the 70
tasks that skip the lookup are *easier for every arm* — schema_only scores
41% there against 19% elsewhere — so the model is skipping the lookup when it
does not need domain knowledge, which is the correct behaviour. Only 7 of the
33 contract losses fall in that set.

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
is 2,475 characters, an order of magnitude smaller, with the rule reachable
by one targeted call. So the finding is not that the contract arm was given
more information. Both arms had the information. Only one of them used it.

That also refines the loss analysis below: where the contract arm gets these
same fee-set questions wrong, it is losing to the identical mechanism from
the other side, not to missing knowledge.

### A library defect the transcripts exposed

`NoSelectStarChecker` used `find_all(exp.Star)`, which matches any `Star`
anywhere in the AST — and `COUNT(*)` parses as `Count(this=Star())`. The
agent was told *"SELECT * is not allowed"* about queries containing no
`SELECT *`.

In this sweep: **161 rejections across 124 of 401 tasks (31%)**. Replaying
the 160 distinct rejected queries through the fixed checker, **79 were false
rejections** and 81 were genuine (`SELECT p.*` inside CTEs, still blocked);
none of the 79 contains a real star projection.

The cost is visible in the transcripts. On task 1278 the model re-issued the
identical query, then guessed at unrelated clauses — *"maybe it flags AVG("*,
*"maybe the word select"* — before landing on `COUNT(ID)`. A rejection whose
stated reason is not true of the query cannot be complied with; it spends
turns and teaches nothing.

**This handicapped the contract arm only** — the baselines run ungoverned SQL
and never saw the checker. The 55.1% hard figure is therefore a floor, not a
ceiling. Fixed in `cde8b20`; the sweep reported here predates the fix and has
not been re-run.

## Where the contract arm loses — 33 tasks

14 losses to schema_only, 29 to manual_prompt, 10 to both. Two modes are
visible in the transcripts:

1. **Set membership under NULL wildcards** — the same rule as the mechanism
   above, failing in the other direction. e.g. task 1500, "fee IDs that apply
   to account_type = O and aci = C": the contract arm returns both false
   positives and false negatives, while manual_prompt matches exactly.
   Applying "NULL means all" to an enumeration is harder than applying it to
   an average: the model must decide, per column, whether a NULL widens the
   match or not. That the same rule drives both the largest win and the
   residual losses is the clearest signal available about where to invest.
2. **Near-miss arithmetic.** e.g. task 1274, gold `0.126459`, contract
   `0.125516` — a real computation with a wrong rounding or filter, not a
   failure to find the rule.

**These are not the MotherDuck failure mode.** That prediction — a
low-reasoning model skipping fetch-gated context — is not what the traces
show (see above: the lookup is skipped only on easier tasks, and 26 of the 33
losses are on tasks where it *was* called). The model fetches the rule and
then misapplies it. So hoisting rule bodies into `to_system_prompt()` would
not obviously help; the useful follow-up is to make the wildcard semantics
executable — a tool that resolves "which fee rules match this transaction"
rather than prose the model must reimplement in SQL each time.

On hard tasks the contract arm's wrong answers used **more** reasoning than
its right ones (597 vs 339 tokens/row) at identical turn and tool counts.
Its failures are not laziness, and raising `reasoning_effort` is unlikely to
fix them.

## Operational findings

**The forcing turn is nearly worthless for accuracy, and valuable anyway.**
It fired 59 times and produced **1 correct answer**. But `hit_limit` rows —
paid-for and unscoreable — went to **zero** in all three arms. Its real
service is preventing a bias: without it those 59 budget-exhausted runs
would have been dropped from the denominator, and 41 of them were
`schema_only`, 18 `manual_prompt`, **0 `contract`**. Excluding them would
have inflated the two baselines by silently discarding their hardest runs.

**Two error rows (0.2%), and both are F1 again.** Tasks 2760
(`manual_prompt`) and 1765 (`schema_only`) returned *"Model token limit
(16000) exceeded before any response was generated"* — reasoning tokens
consuming the whole output budget, the same failure that F1 fixed at 4,000
by raising the cap to 16,000. The asymmetry from F1 also repeats: **both
hit the ungoverned arms, neither hit the contract arm**, because the cap
binds first on the arms that reason longest. At 2 of 1,203 rows it moves
nothing (the strict view counts both as wrong and p is unchanged), but the
mechanism is not fixed, only made rarer.

**No governance events.** `db_corrupted` is false on all 1,203 rows: no arm
mutated the warehouse. The governed-tool counters (218 inspect rejections,
527 enforcement blocks, 529 retry prompts) are descriptive instrumentation
and are deliberately *not* offered as a governance claim.

## What this run does not show

- **One model.** glm-5.3-flash only. The pre-registered primary comparison is
  `deepseek-v4-pro-0813` and has not been run — `dce.stats` prints that
  section empty, by design. Cross-family (glm was the control) and frontier
  (`gpt-5.6-sol`) arms are also unrun.
- **Reconstructed golds, not official ones.** 401/450 tasks by plurality
  consensus at threshold 0.75, with 5 verified-wrong golds excluded. Close
  enough to compare against the leaderboard band; not a leaderboard
  submission, which would need all 450.
- **k=1.** No repeat runs, so the flip rate is unmeasured. One task
  (1480) was observed flipping verdict between two identical runs during
  development. With 168 discordant pairs the headline is not at risk, but no
  individual task's verdict should be treated as stable.
- **Not a generalisation about analytics.** See the fee-bucket caveat above.
- **The contract arm ran with a validator defect** (`COUNT(*)` rejected as
  `SELECT *`, fixed in `cde8b20` after this sweep). It only ever cost the
  contract arm, so the direction is known even though the magnitude is not.

## Reproducing

```bash
uv sync && uv run python -m dce.prepare      # golds must hash to a4388e9ee823
uv run python -m dce.runner --models z-ai/glm-5.3-flash \
    --workers 6 --max-spend 20 --out results/glm-full.jsonl
uv run python -m dce.stats results/glm-full.jsonl
```

~2.5 hours at 6 workers, ~$2.40. Resumable: see
[`deploy/README.md`](deploy/README.md) for the unattended setup, which is how
this run was executed (systemd, VPS, one restart mid-flight to verify
resume). Per-run transcripts including the model's own reasoning are under
`traces/glm-full/`, one gzipped JSON per row.
