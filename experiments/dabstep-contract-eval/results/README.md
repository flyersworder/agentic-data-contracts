# Results files

## `glm-all450.jsonl` — the leaderboard submission sweep, and the only near-replicate

The contract arm alone over **all 450 tasks** (`--ungolded run`), at commit
`46dde879`: 56 minutes at 6 workers, $0.71, exit 0. Temperature 0, endpoint
`z-ai` fp8, contract digest `sha256:e438ecf7…`, scorer `official-vendored`.

Two uses, and they are not the same use:

1. **The submission.** `python -m dce.submit --results results/glm-all450.jsonl
   --arm contract --model z-ai/glm-5.3-flash` builds the 450-line file the
   leaderboard Space takes. The 49 tasks with no reconstructed gold are
   answered here and carry `verdict: "ungraded"` with `gold: null`; they are
   in no accuracy denominator anywhere.
2. **A near-replicate of run A's contract arm** on the 401 shared tasks —
   same model, arm, contract, provider and temperature, differing only by the
   `COUNT(*)` validator fix (`cde8b20`) that run A predates. This is the only
   within-condition variance estimate the experiment has: 94 discordant pairs,
   a 23.4% flip rate. See FINDINGS.md, *Run E*.

**Do not put this file in an arm comparison.** It has one arm. Every table in
FINDINGS.md that compares arms is built from the four-arm files below.

## `gpt6-resolved-r{1,2,3}.jsonl` and `gpt6-fifth*.jsonl` -- one arm each, paired with the panel

Each file holds one extra arm on gpt-6-sol, 401 golded tasks, and means
nothing alone: run N pairs by task with `gpt6-panel-rN.jsonl`, the four-arm
panel repeat of the same index. See FINDINGS.md, *Knowledge or delivery?*

- `gpt6-resolved-r{1,2,3}` -- `manual_resolved` at `d5f0e45`, the arm the
  finding rests on. k=3, 0 failures, about $11.40 each.
- `gpt6-fifth3-r{1,2}` -- `contract_uninterpreted` v3 at `f0d41e6`, stopped at
  k=2 when the design was replaced.
- `gpt6-fifth2-r1` -- v2 at `7785733`; its second run was stopped at 21 rows
  and is not kept here.
- `gpt6-fifth-r1` -- v1 at `fb2579c`, which asserted a wrong rule.

The three `gpt6-fifth*` versions are an exploratory record, not a control;
**do not pool them with each other or report them as one arm.**

## `qwen38-resolved-r{1,2,3}.jsonl` -- three arms, the Qwen 3.8 replication

`manual_prompt`, `manual_resolved` and `contract` on `qwen3.8-27b`, 401 golded
tasks, at `f8c4e32`. Unlike the gpt-6-sol files above, each repeat holds all
three arms and pairs within itself; do not pair them with the
`qwen38-panel-rN` files, which predate the 0.55.0 query bounds and the
response-cache bypass. Each file is the main sweep followed by one
`--retry error` pass, so a retried (task, arm) has two rows; the loaders keep
the last. r2's retry pass hung on one run and was stopped, so that run stays
an error. See FINDINGS.md, *Knowledge or delivery, on a weaker model*.

## `luna-resolved-r{1,2,3}.jsonl` -- three arms, the gpt-6-luna replication

`manual_prompt`, `manual_resolved` and `contract` on `gpt-6-luna`, 401 golded
tasks, at `54636a3`. Laid out like the Qwen 3.8 files: each repeat holds all
three arms and pairs within itself. Each sweep ran with `--retry-pass`, but no
run errored, so every file has exactly one row per (task, arm). See
FINDINGS.md, *Knowledge or delivery, on a smaller model of the same family*.

## `smoke12-pre-fixes.jsonl` — VOID, kept as evidence

Three rows (task 1712, all arms, `z-ai/glm-5.3-flash`) from the first smoke
run, stopped after the first task. **Not usable for FINDINGS**, and not
comparable with any later file. Every one of these defects has since been
fixed; the rows are kept only because they are the measurements the smoke-run
findings (formerly `docs/superpowers/specs/`, now in git history only) argue from.

- `usd` is inflated 1.94x-2.81x: cache reads were billed at the fresh-input
  rate (F4).
- The serving endpoint was unpinned and unrecorded — routing is chosen per
  request across up to 30 endpoints of differing quantization and price (F3).
- `temperature=0.0` was inert; every call sampled at the provider's default
  (F9).
- Reasoning effort was an unset per-endpoint default (F10).
- The caps that produced `hit_limit` and the `max_tokens` error no longer
  exist: 25 tool calls, 4,000 output tokens, 1,000 rows, GROWTH=4 (F1/F2/F5/F6).

Rows written before those fixes carry the old schema, and lack
`forced_answer`, `provider_tag`, `quantization` and `reasoning_effort`.

## `dabstep-compiled-luna-r{1,2,3}.jsonl` -- the compiled DABStep contract, gpt-6-luna

`contract` and `contract_compiled` on `gpt-6-luna`, 401 golded tasks, at the
run commit `1c1e302`. Each repeat holds both arms and pairs within itself.
Each sweep ran with `--retry-pass`, but no run errored, so every file has
exactly one row per (task, arm). See FINDINGS.md, *The compiler on DABStep:
prediction (5)*.

## `livesqlbench/luna-r{1,2,3}.jsonl` -- four arms on LiveSQLBench, gpt-6-luna

`schema_only`, `manual_prompt`, `manual_compiled` and `contract` on
`gpt-6-luna`, all 375 frozen tasks (302 `primary`, 73 `order_only`), at the
run commit `1c1e302`. Each repeat holds all four arms and pairs within
itself. Each sweep ran with `--retry-pass`; five construction errors were
retried once each, so those (task, arm) pairs have two rows and the loaders
keep the last. A row carries `gold`, the sha256 of the gold result, never the
gold itself. See FINDINGS.md, *LiveSQLBench: compiled knowledge, delivered
two ways*. `livesqlbench/smoke.jsonl` is the 12-task smoke run before it.

## `livesqlbench/qwen38-r{1,2,3}.jsonl` -- four arms on LiveSQLBench, qwen3.8-27b

The same four arms and 375 tasks on `qwen3.8-27b`, at the same run commit
`1c1e302`, k=3, `--retry-pass` on every repeat (14 construction errors
retried; the loaders keep the last row per pair). Repeat 2 was resumed once
after the job's volume filled while archiving traces; no row was rerun. See
FINDINGS.md, *Qwen 3.8: the tie replicates, the two effects do not*.

## `dabstep-opus-luna-r{1,2,3}.jsonl` -- experiment 1, a stronger compiler on DABStep

`contract_compiled`, `contract_compiled_opus`, `contract_compiled_opus_joins`
and `contract` on `gpt-6-luna`, 401 golded tasks, at the run commit
`3f60a9c`, k=3, `--retry-pass` on every repeat. Repeats 1 and 2 have one row
per (task, arm); repeat 3 holds 41 gateway authentication failures (HTTP
401, $0) followed by their retried rows, and the loaders keep the last. The
job's volume filled while archiving repeat 3's traces, after its sweep had
finished; no row was lost or rerun. `dabstep-opus-smoke.jsonl` is the
12-task smoke run before it. See FINDINGS.md, *A stronger compiler on
DABStep: predictions (7)-(9)*, and `analysis/stronger_author.py`.

## `livesqlbench/lsb-exp2-luna-r{1,2,3}.jsonl` -- experiment 2, a better-made contract on LiveSQLBench

`manual_prompt`, `contract`, `contract_opus` and `contract_authored` on
`gpt-6-luna`, the tasks of insider_trading, museum_artifact and cross_border
(55 `primary`, 5 `order_only`), at the run commit `3e0915a`, k=3,
`--retry-pass`; one row per (task, arm), no errors. A row carries `gold`, the
sha256 of the gold result, never the gold itself. `livesqlbench/exp2-smoke.jsonl`
is the 12-task smoke run before it. See FINDINGS.md, *A better-made contract
on LiveSQLBench: predictions (10)-(12)*.

## Experiment 3 (`482da68`) -- kept private, identified here

`manual_prompt`, `manual_corrected`, `contract_opus` and `contract_corrected`
on `gpt-6-luna`, experiment 2's databases, k=3, `--retry-pass`; one row per
(task, arm), no errors. The corrected arms read knowledge derived from the
gold and their answers can restate it, so the files stay with the private
data (`~/data/dce-held-results/exp3/`), as do the corrections
(`LSB_DATA/corrected/`). sha256 (first 16 hex): `exp3-luna-r1.jsonl`
f0557d7a467241eb, `exp3-luna-r2.jsonl` 6fb46123fb47a077, `exp3-luna-r3.jsonl`
56fcf98342c585e0, `exp3-smoke.jsonl` 2e69f8f9299985cd. The train / held-out
split is `livesqlbench/exp3-split.json`. See FINDINGS.md, *Completing the
knowledge: predictions (13)-(15)*, and `analysis/lsb_corrected.py`.

## Procedural-load ratings (`143e009`) -- kept private, identified here

Two blind raters per task, 703 tasks (401 DABStep, 302 LiveSQLBench
primary), ten batches each. The LiveSQLBench side lists the KB entries the
private mapping assigns, so the packets, the key and the ratings stay in
`~/data/dce-held-results/procedural-load/`. sha256 (first 16 hex) of the
ratings, each role's ten batches concatenated in order: `rater-r1-c*.jsonl`
b8cf40deb2b16001, `rater-r2-c*.jsonl` fe17649d857f4938. The inputs are
identified in `143e009`. See FINDINGS.md, *Is the gain where the knowledge
carries procedure?*, and `analysis/procedural_load.py`.

## `exp5-luna-r{1,2,3}.jsonl` -- experiment 5, the contract as text or behind its tools

`manual_resolved`, `contract_text` and `contract` on `gpt-6-luna`, the 401
golded DABStep tasks, at the run commit `f499053`, k=3, `--retry-pass`, run
together in each repeat; 1,203 rows each, no errors, about $2.90 a repeat.
`exp5-smoke.jsonl` is the 12-task smoke before it; a local partial first
repeat (269 rows, $0.69) was aborted and is not used. `exp5b-smoke.jsonl` is
the 12-task smoke of `manual_summed` (`14d627d`), the arm experiment 5b adds.
See FINDINGS.md, *Text or tools: predictions (20)-(21)*, and
`analysis/contract_text.py`.

## `exp5b-luna-r{1,2,3}.jsonl`, `exp5-qwen-r{1,2,3}.jsonl` -- experiment 5b, the manual with both facts

`exp5b-luna-r{i}` is `manual_summed` alone on `gpt-6-luna`, the 401 golded
DABStep tasks, at the run commit `c39026f`, k=3, `--retry-pass`; 401 rows
each, no errors, about $0.66 a repeat. Each pairs by task with
`exp5-luna-r{i}` (run at `f499053` a day or so earlier; the harness differs
only by the new arm). `exp5-qwen-r{i}` is `manual_resolved`, `manual_summed`,
`contract_text` and `contract` on `qwen3.8-27b`, run together in each repeat
at `c39026f`, k=3, `--retry-pass`; 1,604 graded rows each plus the retried
ones, about $1.90 a repeat. Five `manual_resolved` runs of 1,203 ended at an
error or the turn limit and count as wrong (three in r1, two in r2); one
`contract_text` run in r1 overran the 262k context and was retried. sha256
(first 16 hex): exp5b-luna r1 0cfbd2470ba1e597, r2 8fdd7a14d381f023, r3
ab30c53a3357acf9; exp5-qwen r1 6cedf0bc458a7b35, r2 11a5700de94d8e00, r3
1959d476b559e0c0. See FINDINGS.md, *The prose lacked a second fact*, and
`analysis/contract_text.py --model luna|qwen`.

## `procedures/exp4-{luna,sol}-r{1,2,3}.jsonl` -- experiment 4, knowledge type and writing style, controlled

`schema_only`, `manual_prompt`, `manual_styled`, `manual_compiled` and
`contract` on `gpt-6-luna` and `gpt-6-sol`, the 60 questions of
`prep/procedures/` on three LiveSQLBench databases (our own pieces of
knowledge and questions; the databases themselves are not committed), at the
run commit `05015db`, k=3, `--retry-pass`; 300 rows each, no errors. About
$0.70 a luna repeat and $12.40 a sol repeat. `procedures/exp4-smoke.jsonl`
and `exp4-smoke2.jsonl` are the two smokes before the run commit. The traces
of the first 116 runs of `exp4-sol-r3` were lost when the job was suspended
after the shared volume filled; their rows are intact and the repeat resumed
from row 117. See FINDINGS.md, *Knowledge type, controlled: predictions
(22)-(25)*, and `analysis/procedures.py`.

`procedures/exp4-qwen-r{1,2,3}.jsonl` are the same five arms on
`qwen3.8-27b` at `c39026f`, k=3, `--retry-pass`; 300 graded rows each (two
errors in r1 and one in r3 were retried), about $0.32 a repeat. sha256 (first
16 hex): r1 23e26129b6d735f3, r2 b41d43ffda57e99e, r3 8fa17263640dad81. See
`analysis/procedures.py --set qwen`.
