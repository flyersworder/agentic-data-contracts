# Lean governed tools (#139) — design

## Intent

Production agents built on the governed tools spend more turns and more
uncached input tokens than the same agent given plain SQL access, for the
same accuracy. On LiveSQLBench (gpt-6-luna, k=3, primary set) the `contract`
arm averages 6.2 turns and 13k uncached input tokens a run, against 4.1 turns
and 3k for `manual_compiled`, at 35.8% vs 36.8% accuracy. Per run it makes
`lookup_domain` 2.06, `lookup_metric` 1.91, `inspect_query` 1.48,
`list_metrics` 0.64 and `run_query` 1.42 calls.

The target is production deployments (for example an analytics agent on
Pydantic AI), not the paper's reported cost. The change lives in the
library: prompt rendering, tool descriptions, and the README's recommended
setup.

## What the evidence says to keep

FINDINGS ("Where the tools help: retrieval at the point of use, not
validation") shows the governed tools' accuracy gain on DABStep came from
`lookup_metric` / `lookup_domain` putting the governing rule in front of the
agent just before it wrote the query. `inspect_query` played no part (no
rejection in any winning run). The text arm, which had every definition in
its prompt, lost those tasks.

So the overhead to cut is the *path* to a definition, not the lookup at the
point of use.

Two causes of the overhead sit outside the library: the harness's
`CONTRACT_PROCEDURE` tells the agent to look up the domain and to inspect
every query, and the governed arm renders the prompt without a semantic
source, so no metric name reaches it.

## Prompting guidance this follows

Read 2026-11-19: Anthropic's prompting best practices, tool-definition
guide, prompt-audit checklist and "Writing tools for agents"; OpenAI's
function-calling guide. The points that bind this design:

- **Plain register.** Current models follow instructions literally;
  `MUST` / `NEVER` / `CRITICAL` cause over-triggering. State a constraint
  once, at normal volume, with its reason.
- **Descriptions are contracts, and detailed.** What the tool does, when to
  use it and when not, what each parameter means, what it does not return;
  3-4+ sentences. Steering, examples and procedures do not belong there.
- **A tool's routing lives in its own description**, not in rivals'
  descriptions and not as tool names in the system prompt (a disabled tool
  would leave a dangling reference).
- **Never discourage tool use** ("minimize tool calls" is followed
  literally). Cut hops by shortening the path instead.
- Fewer parameters the model must fill; parallel calls already fetch
  independent items in one turn.

"Concise" here means less steering, not less contract: some descriptions
grow.

## Goals

1. An agent can go from the system prompt straight to `lookup_metric`,
   without `lookup_domain` / `list_metrics` hops just to learn metric names,
   when the caller passes the semantic source.
2. Tool descriptions are precise contracts in plain register, and stop
   implying that every query should be inspected or that a domain must be
   looked up before querying.
3. The README shows the recommended production setup.
4. Measured: fewer turns and uncached input tokens per run, accuracy level.

## Non-goals

- Putting metric SQL in the system prompt.
- A batch `lookup_metric` (dropped: parallel calls cover it, and it would
  need the conformance check and recorder to learn per-name results).
- `to_system_prompt()` loading the semantic source itself (decided: callers
  opt in; existing prompts and the pre-registered arms stay as they are).
- Changing what any check enforces, or the `DatabaseAdapter` /
  `SemanticSource` protocols.
- Editing `CONTRACT_PROCEDURE`, the pre-registered arms, or frozen contract
  directories.

## Anti-goals

- **Losing accuracy to save tokens.** Nothing may remove or discourage the
  lookup immediately before a metric query.
- **Changing a no-source caller's prompt.** A caller that passes no semantic
  source gets exactly today's rendering.

## Approaches considered

- **A. Shorten the path (chosen).** Metric names in the prompt index;
  descriptions rewritten to the guidance. Keeps retrieval at the point of
  use; moves only names (cheap, cached) into the prompt.
- **B. Metric SQL in the prompt, drop the lookup instruction.** Cheapest per
  token, but it is the text arm's design, which lost 5 points on DABStep
  exactly where retrieval at the point of use mattered.
- **C. `lookup_domain` returns full definitions.** Saves one hop but returns
  every member metric's SQL as fresh, mostly unused tokens.

Chosen by the user: A, without the batch lookup, without auto-loading.

## Design

### 1. Metric names in the prompt index (`core/prompt.py`)

Applies only when the caller passes a semantic source with metrics.

- **With domains** (`_render_domains`): each `<domain>` lists its member
  metrics (membership from `metrics_in_domain`; a metric in several domains
  is listed under each). Metrics in no domain are listed in one extra group.
- **Without domains** (`_render_metrics`): above `metric_detail_threshold`
  the index lists names only, instead of today's count plus "browse with
  list_metrics". At or below it, unchanged (name and description).
- **Descriptions vs names:** at or below `metric_detail_threshold` (default
  20, total metrics) each metric carries its one-line description; above it,
  names only.
- **Size bound:** a new `metric_index_threshold` constructor argument,
  default 200, class constant `METRIC_INDEX_THRESHOLD` (the same pattern as
  the existing thresholds, read through `getattr` for old subclasses). Above
  it, both branches fall back to today's count-only form. `None` means never
  degrade.
- **No tool names in the prompt.** The hints become a plain statement of
  what the index is and that each listed metric has a governed definition
  to compute it from; when to call which tool lives in the tool
  descriptions.
- Without a semantic source, rendering is byte-for-byte today's.

Illustrative shape (builder may adjust tags if consistent with the file):

```xml
<available_domains>
  <domain name="fees" summary="..." metric_count="2">
    <metric name="avg_fee">Average fee per transaction</metric>
    <metric name="fee_rule_matches_transaction" />
  </domain>
  <unassigned_metrics>
    <metric name="row_count_check" />
  </unassigned_metrics>
  <hint>...</hint>
</available_domains>
```

### 2. Tool descriptions (`tools/factory.py`)

Rewrite all nine descriptions to the guidance above. Per tool:

- `describe_table`, `preview_table`: today one-liners; add what is returned
  (columns, types, governed descriptions; sample rows and the row cap), when
  to use them, and what they do not return.
- `lookup_metric`: carries the point-of-use routing: use it before writing
  SQL that computes a metric, because the definition (filters, units,
  wildcard rules) is governed and the query must follow it. Plain register.
- `list_metrics`: for browsing or filtering by domain, tier or kind when the
  prompt does not list the metric needed.
- `lookup_domain`: a domain's business description, tables and member
  metrics, for when that context is needed. No "before querying"
  instruction.
- `run_query`: validates with the same checks as `inspect_query` and does
  not run a blocked query. The metric constraint stays as one plain
  sentence with its reason (`_PROTOCOL_METRIC_ORDERING` rewritten, no
  MUST/never). Keep the precedence clause, plainly worded.
- `inspect_query`: for checking a query without running it, such as an
  expensive query or a cost estimate; a query about to be run does not need
  inspecting first. Keep the return-shape text.
- `lookup_relationships`, `trace_metric_impacts`: audit against the same
  rubric; change only where they fall short.

Input schemas are unchanged.

### 3. README: recommended production setup

A short section: pass the semantic source to `to_system_prompt()` so the
metric index reaches the prompt; a recommended system-prompt sentence (look
up the metrics that apply before writing SQL; run queries with `run_query`,
which validates them; use `inspect_query` only to check without running),
noting that requiring an inspect before every query costs a turn per query
for no added checking.

## Measurement

Run where the private LiveSQLBench data (`LSB_DATA`) lives; it is not on the
main Mac. Two new governed arms, gpt-6-luna and qwen3.8-27b, k=3, primary
set, compared with the existing `contract` arm results
(`results/livesqlbench/{luna,qwen38}-r{1,2,3}.jsonl`):

- `contract_lean_lib`: the new library, the semantic source passed to
  `to_system_prompt`, `CONTRACT_PROCEDURE` unchanged. Isolates the library
  plus its recommended prompt setup.
- `contract_lean`: as above, plus a procedure sentence matching the README
  recommendation. The production-recommended setup.

Report per run: turns, uncached input tokens, tool-call mix, cost, accuracy.
Success: turns and uncached tokens down; accuracy within noise of `contract`
(paired per-task comparison, as FINDINGS does). Predictions are written
down before the runs. The riskiest change is the plain-register rewrite of
the metric-lookup constraint; if accuracy drops, bisect that first.

## Constraints

- TDD (CLAUDE.md). Prompt tests in `tests/test_core`, descriptions in
  `tests/test_tools`.
- Lint through prek. Lowest-floor suite must pass.
- Examples' golden output is diffed in CI; regenerate with
  `scripts/regen_examples.sh` and review the diff.
- User-visible prompt and description changes: CHANGELOG `### Changed`,
  minor version.
- Tests or scripts that match on description or hint text must be updated,
  not loosened (grep before changing a string).

## Left to the builder

- Exact XML tag names and attribute order, if consistent with the file.
- Name of the no-domain group.
- Wording of descriptions, hints and the README section, within the rules
  above.
