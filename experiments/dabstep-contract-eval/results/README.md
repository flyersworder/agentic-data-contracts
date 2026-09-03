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

## `smoke12-pre-fixes.jsonl` — VOID, kept as evidence

Three rows (task 1712, all arms, `z-ai/glm-5.3-flash`) from the first smoke
run, stopped after the first task. **Not usable for FINDINGS**, and not
comparable with any later file. Every one of these defects has since been
fixed; the rows are kept only because they are the measurements that
`docs/superpowers/specs/2026-08-30-dabstep-smoke-findings.md` argues from.

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
