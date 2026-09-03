# Results files

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
