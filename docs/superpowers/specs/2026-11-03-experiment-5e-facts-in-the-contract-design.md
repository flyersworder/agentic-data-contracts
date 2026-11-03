# Experiment 5e: the two facts written into the contract, looked up or pasted; and the prose KB at full scale

Status: design, 2026-11-03. Added in the paper v3 revision after the review
round (five reviewers, all "major revision").

## Why

Experiment 5d asked whether the route (the contract looked up through the
governed tools, or pasted as text) still matters once both arms state the two
DABStep facts. It pasted the two notes into both arms' prompts, so neither arm
had to reach the facts through the contract: 5d tested governed delivery of
*the rest* of the contract, not whether lookup delivers the deciding facts
better. Both external referees name this as the missing test of the paper's
delivery question. Two further requests come with it:

- the paper's headline comparison, `manual_summed` against `contract` on
  luna (84.1% against 82.6% on the rule-matching tasks), compares separate
  runs; reviewers ask for a co-run block;
- the scale experiment's prose comparator was run at 1x and 4x only, so
  lookup's capacity advantage was shown over the YAML serialisation, not over
  the compact prose that fits at every size.

## Goals

1. A clean route test: the two facts stated plainly *inside the contract*, at
   the point where each is used, in no prompt; the same contract looked up or
   pasted.
2. Whether writing the facts plainly into the contract lifts the governed
   contract without any prompt note (5d's (42)/(42Q) showed it with notes).
3. The headline comparison co-run on luna: `manual_summed` and `contract` in
   one block (on Qwen they already ran together in 5b; 5e replicates it).
4. The prose KB at the full scale (22 databases) on Qwen, co-run with the
   governed contract at the same scale.

## Non-goals

- A retrieval (RAG) arm over the manual: out of scope (decided 2026-10-28).
- GPT-6 sol: not rerun (cost).
- Rewording the facts: the contract carries the notes' exact words.

## Materials

### The contract with the facts in place (`contract_plain`)

Derived from the frozen contract's raw files by text substitution (not a YAML
round-trip, which would drop the comments and reflow every block scalar, so
the pasted arm would differ from `contract_text` far beyond the two edits).
`contract/` is not edited. The contract keeps its name (a suffix would show
in the governed prompt). Two edits, each in the notes' exact words
(`dce.benchmarks.dabstep.DATA_NOTE`, `SUM_NOTE`), each matched across the
folded-scalar line breaks and asserted unique:

- **E1, the wildcard.** In `contract.yml`, the fees domain's MATCHING RULE
  paragraph: `DATA_NOTE` is inserted after the sentence stating the manual's
  null rule ("... a null `aci` means the rule applies for every ACI.").
- **E2, summing.** In `semantic.yml`, `total_transaction_fees`' description:
  its first sentence ("Total fees over a set of transactions: ... the fee rule
  that matches it.") is replaced by `SUM_NOTE`. The next sentence ("Use the
  same matching predicates ...") and the SQL are unchanged.

Tests: the raw-text diff against `contract/` is exactly E1 and E2; the parsed
contracts differ in exactly two fields; the SQL is unchanged. `arm_digest`
stamps the derived contract's own digest (`plain_digest()`), not the
original's. The `contract_text` branch of `build_arm` takes a contract
directory instead of the hard-coded path.

What stays: the contract already stated both facts less plainly (the
wildcard as an INTERPRETATION of `fee_rule_matches_transaction` and in three
column descriptions; summing in the SQL and in "sum only over matched
(transaction, rule) pairs"), and `transaction_fee`'s description still says
"Which rule applies is decided by ...". These are identical in every contract
arm, so they do not touch the route contrast; they are said where P and F are
read.

What the governed arm sees: its system prompt is the index (about 2k
characters: tables, domain summaries, constraints), with no domain text or
metric descriptions, so the facts are reachable only by lookup;
`lookup_domain("fees")` returns both edits. The route contrast still
bundles, as in experiment 5: 9 governed tools against 3 plain ones, the
validator, the look-up instruction against the text note, a 2k index against
60k characters of pasted files, governed `describe_table`'s column
descriptions, and the YAML comments only the pasted arm sees.

### Arms (DABStep, 401 tasks)

| arm | route | wording of the two facts | prompt notes |
|---|---|---|---|
| `contract` | governed | original | none |
| `contract_text` | pasted | original | none |
| `contract_plain` | governed | in place, notes' words | none |
| `contract_plain_text` | pasted | in place, notes' words | none |
| `contract_noted` | governed | original | both notes (5d) |
| `manual_summed` | prose manual | notes | both notes (5b) |

The first four are a 2×2 of route by wording. All six run together in each
repeat on gpt-6-luna and qwen3.8-27b, k=3, the harness and settings of 5d.
Rows stamp the run commit; traces to the PVC with coverage checked before
archiving.

### Scale (LiveSQLBench, the 60 constructed questions of experiment 6)

`manual_scaled_all` (the base KB and all 21 padding KBs as prose, about 99k
tokens), `contract_scaled_all`, `manual_scaled_1x` and `contract_scaled_1x`,
co-run on qwen3.8-27b, k=3. No code change: the scale arms take the level
from the arm name. Run-health precondition: no context overflow and no
truncated request in any `manual_scaled_all` run (about 99k tokens over about
9.5 turns); a run that overflows is reported and counted wrong.

## Contrasts

Primary view: end-to-end on the 199 rule-matching tasks
(`contract_text.rule_matching`); also all 401, and the official and exact
views (`analysis/exact_match.py`), labelled exploratory.

- **R, route, facts in place:** `contract_plain` − `contract_plain_text`.
- **P, plain wording, governed:** `contract_plain` − `contract`.
- **H, headline:** `manual_summed` − `contract`.
- Exploratory: R0, the route with the original wording (`contract` −
  `contract_text`, experiment 5's +10.1 on Qwen re-measured within a run); the
  route × wording interaction (does plain wording help lookup more than
  paste); `contract_plain` − `contract_noted` (facts placed in the contract
  against facts as a prompt note, both governed); F, `contract_plain_text` −
  `manual_summed`; the lookup log.

## Inference (one engine, registered)

- Unit: the task, scored as its mean over the three repeats.
- Statistic: the task-weighted mean difference, the sum over shapes of each
  shape's summed task differences, divided by 199 (401 for the all-task
  view).
- Test: sign-flipping the per-shape sums, 100,000 flips, fixed seed,
  two-sided.
- Intervals: by inverting the same test (shift every task difference by δ,
  flip the shape sums; the 90% and 95% sets of δ not rejected). The
  shape-clustered percentile bootstrap is reported beside and decides
  nothing: with 25 shapes of very unequal size (five of 20 tasks, sixteen of
  2-5; about 15 effective) it undercovers.
- The task-level sign test is reported beside, as in earlier experiments.
- Power, said in advance: with about 15 effective shapes only effects of
  about 7 points or more, concentrated in the large shapes, are detectable;
  a luna contrast with seven or fewer non-zero shapes cannot reach Holm
  significance (minimum p about 0.016).
- A claim of no effect in the paper requires its bound to hold; an interval
  reaching past ±5 is reported as inconclusive and supports no claim of no
  effect.
- Holm over the confirmatory predictions below, at the shape level.

The scale block has no shapes: experiment 6's exact bound on lost runs,
clustered by knowledge piece (six pieces, too few for a bootstrap), and a
paired sign test over the 60 questions.

## Predictions (fixed in the run commit)

Numbered after experiment 7's (43)-(48), which are already in its design.

Confirmatory:
- **(49)** luna, R: the governed route does not lead by 5 points or more.
  Holds when the upper end of the 90% interval is below +5.
- **(49Q)** Qwen, R: the same.
- **(50Q)** Qwen, P: `contract_plain` beats `contract` (shape-level p < 0.05,
  positive estimate).
- **(51)** luna, H: `manual_summed` is not worse than `contract` by 5 points
  or more. Holds when the lower end of the 90% interval is above −5.

Estimates, registered as such (no verdict): (50) luna P, whose analogue in 5d
(+4.0 with the notes pasted) reached shape-level p = 0.062; (51Q) Qwen H,
which on the 5b co-run data gives +6.5, p = 0.27.

Scale:
- **(52)** Qwen: `manual_scaled_all` loses no more than 5 points against
  `manual_scaled_1x` (experiment 6's exact bound). A no-drop check at
  ceiling: prose scored 97.8% at 1x and 4x.
- **(53)** Qwen: `contract_scaled_all` uses fewer input tokens per run
  (fresh plus cached) than `manual_scaled_all`; paired sign test over the 60
  questions on per-question mean input tokens, p < 0.05. Dollars at Qwen's
  meter (output only) and at one fixed reference price reported beside.

Drift check (registered, as in 5c and 5d): each rerun arm (`contract`
against 5d, `contract_text` against 5c, `contract_noted` against 5d,
`manual_summed` against 5c) within 3 points of its earlier run on every
subset; a larger drift is reported and the arm's cross-run comparisons are
not made.

Why these: 5d showed no route difference with the notes pasted in both arms
(−1.8 luna, 90% interval by inversion [−9.1, +5.6]; −2.2 Qwen, [−6.3, +1.7])
and a notes lift (+4.0 luna, p = 0.062; +7.0 Qwen, p = 0.019, under this
engine; `analysis/reread.py`). Said in advance: a repeat of 5d's luna spread
(8 of 25 shapes non-zero) would leave (49)'s upper end above +5, so (49) is
expected to read inconclusive unless 5e's spread is smaller; that outcome
supports no claim of no route effect on luna.

## Analysis

`analysis/facts_placed.py`, test-first: the contrasts, the engine above, the
lookup log (per run: `lookup_domain("fees")`, `list_metrics` for the fees
domain, `lookup_metric` of `total_transaction_fees` or
`fee_rule_matches_transaction`, each before the first fee query; a lost task
split into "fact never fetched" and "fetched, not used"), the drift check
and the scale pair. The engine goes in `dce/stats.py` with its own tests, so
the paper's other contrasts (the 5d ties, the form effect, the held-out
lift) can be re-read with it.

## Cost and time

From 5d (luna $9.01, Qwen $2.29 for three arms): about $18 luna and $5 Qwen
for six; the scale block about $2. Two to three days on the cluster.

## Order of work

1. `dce/plain.py` (the derivation), the two arms, digests, tests; the engine;
   `analysis/facts_placed.py` with tests; a dry run.
2. **Run commit** with (49)-(53), the estimates, the drift check and the
   engine; tag; job manifests in `~/data/dce-jobs/`; traces to the PVC.
3. Copy down, verify, commit results; analysis; FINDINGS; the paper.

## Review (2026-11-03)

One reviewer, before the build. Taken: text substitution, not a YAML
round-trip; the name kept; digests; E2 replacing the whole first sentence;
non-superiority and non-inferiority bounds in place of ties; one registered
engine with intervals by test inversion; (53) on input tokens (Qwen's meter
charges output only); `contract_text` and `contract_noted` co-run; (50) and
(51Q) as estimates; a registered drift check; the finer lookup log; the
scale arms at 1x. Not taken: the scale block on luna (the user chose Qwen).
