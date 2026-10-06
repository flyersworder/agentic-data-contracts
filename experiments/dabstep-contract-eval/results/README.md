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
