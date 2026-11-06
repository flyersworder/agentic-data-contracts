# Experiment 7: the pilot gate

Run 2026-11-03 before the run commit, per the design's "Pilot gate": one
repeat of `dab_desc` and `dab_hints` on gpt-6-luna over the 40 queries
(80 runs, $0.19), at the recovery commit 898373b (the rows stamp it),
`--retry-pass`. Rows: `results/dataagentbench/pilot-luna.jsonl`. They enter
no test, and the pilot cannot change the predictions or the arms.

Every run reached a scored verdict (no harness failure, no refused
validator, no build problem).

| dataset | queries | `dab_desc` | `dab_hints` |
|---|---:|---:|---:|
| bookreview | 3 | 3 | 2 |
| crmarenapro | 13 | 9 | 8 |
| DEPS_DEV_V1 | 2 | 0 | 1 |
| GITHUB_REPOS | 4 | 3 | 2 |
| googlelocal | 4 | 3 | 4 |
| music_brainz_20k | 3 | 1 | 1 |
| PANCANCER_ATLAS | 3 | 2 | 1 |
| stockindex | 3 | 3 | 2 |
| stockmarket | 5 | 1 | 2 |
| **all** | **40** | **25 (62%)** | **23 (57%)** |

Overlong passes: `dab_desc` 4, `dab_hints` 2.

## The floor rules

- **Overall:** `dab_hints` passes 57%, not fewer than 25%. SQL-only tools are
  not a floor; the Python tool is not built.
- **Per dataset:** no dataset has zero `dab_hints` passes. No dataset is left
  out of (44)-(48).

## Noted, not acted on

On this one repeat `dab_hints` does not lead `dab_desc` on luna (23 against
25). (44), the manipulation check, is read on five repeats and both models;
its registered reading if it fails stands: (45)-(48) are reported but not
read as support.
