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
putting the governing rule in front of the agent just before it wrote the
query. `inspect_query` played no part (no rejection in any winning run). The
text arm, which had every definition in its prompt, lost those tasks. The
rule arrived through `lookup_metric` in most winning runs, but in 16 of 50
through `lookup_domain(fees)` or the governed `describe_table`: the fees
domain description carries the null-means-all rule.

So the overhead to cut is the *path* to a definition, not the lookups at the
point of use, and that includes the domain lookup where a domain's
description carries rules.

Two causes of the overhead sit outside the library: the harness's
`CONTRACT_PROCEDURE` tells the agent to look up the domain and to inspect
every query, and the governed arm renders the prompt without a semantic
source, so no metric name reaches it.

On LiveSQLBench the tools made no accuracy difference (`contract` does not
beat `manual_compiled`), so LiveSQLBench can show the cost saving but cannot
show whether the accuracy mechanism survives. DABStep can.

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
  would leave a dangling reference). One exception, below: the metric
  constraint on `run_query` / `inspect_query`.
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
   looked up before every query.
3. The README shows the recommended production setup.
4. Measured: fewer turns and input tokens per run, accuracy non-inferior,
   including on DABStep where the accuracy effect lives.

## Non-goals

- Putting metric SQL, or metric or domain description bodies, in the
  system prompt (beyond the no-domain detailed listing that exists today).
- A batch `lookup_metric` (dropped: parallel calls cover it, and it would
  need the conformance check and recorder to learn per-name results).
- `to_system_prompt()` loading the semantic source itself (decided: callers
  opt in; existing prompts and the pre-registered arms stay as they are).
- A semantic-source opt-in for `bridge.compile_to_contract` (it calls
  `to_system_prompt()` with no source; a follow-up if a bridge user asks).
- Changing what any check enforces, or the `DatabaseAdapter` /
  `SemanticSource` protocols.
- Editing `CONTRACT_PROCEDURE`, the pre-registered arms, their digests, or
  frozen contract directories.

## Anti-goals

- **Losing accuracy to save tokens.** Nothing may remove or discourage the
  lookup immediately before a metric query, including the domain lookup when
  the domain's rules apply.
- **Changing a no-source caller's prompt.** A caller that passes no semantic
  source gets exactly today's rendering.

## Approaches considered

- **A. Shorten the path (chosen).** Metric names in the prompt index;
  descriptions rewritten to the guidance. Keeps retrieval at the point of
  use; moves only names into the prompt.
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
  metrics by **name only** (membership from `metrics_in_domain`; a metric in
  several domains is listed under each). Metrics in no domain the contract
  catalogs (none, or only uncataloged ones) are listed in one extra group.
  `metric_detail_threshold` no longer affects this branch.
- **Without domains** (`_render_metrics`): at or below
  `metric_detail_threshold`, unchanged (name and description). Above it, the
  index lists names only, instead of today's count.
- Descriptions are never added where they are not today: a metric's
  description is a paragraph (DABStep averages about 700 characters, and
  `fee_rule_matches_transaction` carries the wildcard rule), so rendering
  them per domain would rebuild the text arm.
- **Size bound:** a new `metric_index_threshold` constructor argument, class
  constant `METRIC_INDEX_THRESHOLD = 200`, read through `getattr` for old
  subclasses like the existing thresholds. It counts listed entries (a
  metric listed under three domains counts three times). Above it, the names
  are dropped: domains render as today (with `metric_count`), the no-domain
  branch renders today's count line. Default: 200, except that a caller who
  passed `metric_detail_threshold=None` ("never degrade") and did not set
  `metric_index_threshold` gets `None` too. `None` means never drop names.
- **Hints carry no tool names, and keep the "before computing" constraint**
  (`test_scalability` guards it on purpose). Exact text, with a source:
  - index rendered: `<hint>Each listed metric has a governed SQL
    definition; look it up before computing the metric.</hint>`
  - names dropped: `<hint>{n} metrics have governed SQL definitions (names
    omitted above {threshold}); look a metric up before computing
    it.</hint>`
  When to call which tool lives in the tool descriptions.
- The INFO log at the compact branch is reworded to the new meaning (no
  "agents must call list_metrics").
- Without a semantic source, rendering is byte-for-byte today's, including
  the domain hint that names `lookup_domain` / `lookup_metric`.
- Prompt cost: names are cached where the provider caches; where it does not
  (Qwen runs show `cached_tokens` 0 throughout), the index is paid every
  turn. The size bound caps that.

Illustrative shape (builder may adjust tags if consistent with the file):

```xml
<available_domains>
  <domain name="fees" summary="..." metric_count="2">
    <metric name="avg_fee" />
    <metric name="fee_rule_matches_transaction" />
  </domain>
  <unassigned_metrics>
    <metric name="row_count_check" />
  </unassigned_metrics>
  <hint>Each listed metric has a governed SQL definition; look it up before computing the metric.</hint>
</available_domains>
```

### 2. Tool descriptions (`tools/factory.py`)

Rewrite all nine descriptions to the guidance above. The codebase's rule
holds for every new clause: a clause appears only when the capability it
names exists (as `metric_ordering` is gated on metrics today).

- `describe_table`, `preview_table`: today one-liners; add what is returned,
  when to use them, and what they do not return. "Governed descriptions"
  only with a semantic source; the sample row cap is stated as the actual
  cap (min(100, `max_result_rows`), no cap when that is None).
- `lookup_metric`: carries the point-of-use routing (only when metrics
  exist): use it before writing SQL that computes a metric, because the
  definition (filters, units, wildcard rules) is governed and the query must
  follow it. Plain register.
- `list_metrics`: for browsing or filtering by domain, tier or kind when the
  prompt does not list the metric needed.
- `lookup_domain`: returns the domain's business description, tables and
  member metrics. Say what the description holds: rules that apply across
  the domain's metrics (how missing values or wildcards are read, for
  example), so it is worth fetching when a question depends on them. No
  "before querying" instruction.
- `run_query`: validates with the same checks as `inspect_query` and does
  not run a blocked query. Keeps the metric constraint as one plain sentence
  with its reason (`_PROTOCOL_METRIC_ORDERING` rewritten, no MUST/never).
  This is the one deliberate exception to "routing in its own description":
  descriptions travel with the tools on every path, the prompt is opt-in.
  Keep the precedence clause, plainly worded.
- `inspect_query`: for checking a query without running it, such as an
  expensive query or a cost estimate; a query about to be run does not need
  inspecting first. Keeps the metric constraint (as today, same gating).
  When the contract sets `max_retries`, add that a blocked `run_query`
  spends a retry and `inspect_query` does not. Keep the return-shape text.
- `lookup_relationships`, `trace_metric_impacts`: audit against the same
  rubric; change only where they fall short.

Input schemas are unchanged.

### 3. README: recommended production setup

A short section: pass the semantic source to `to_system_prompt()` so the
metric index reaches the prompt; a recommended system-prompt sentence
(before writing SQL, look up the metrics that apply, and their domain when
the question depends on its rules; run queries with `run_query`, which
validates them; use `inspect_query` to check without running). Note the
trade-off: requiring an inspect before every query costs a turn per query
for no added checking, but with `max_retries` set it keeps blocked drafts
from spending retries.

The quickstart passes the source to `to_system_prompt()` where it has one.

## Measurement

Run where the private LiveSQLBench data (`LSB_DATA`) lives; it is not on the
main Mac. All arms on the same library commit, run together. Existing rows
are `adc_version` 0.59.0 and are not the baseline (the harness treats a new
version as a new treatment for the governed arm).

Arm ladder, each step adding one change:

1. `contract`, re-run: new descriptions, no source, `CONTRACT_PROCEDURE`.
2. `contract_lean_lib`: plus the semantic source passed to
   `to_system_prompt`.
3. `contract_lean`: plus a new `LEAN_PROCEDURE` constant **replacing**
   `CONTRACT_PROCEDURE`, matching the README recommendation. The
   production-recommended setup.

Benchmarks: LiveSQLBench primary set (cost) and DABStep (accuracy, where
retrieval at the point of use mattered), gpt-6-luna and qwen3.8-27b, k=3.
DABStep's three avg_fee families are reported on their own.

Harness changes (`experiments/dabstep-contract-eval/dce/`): a source
parameter on the governed arm builder; `LEAN_PROCEDURE`; the two arms
registered (`LiveSQLBench.extra_arms` / `governed_arms` and the DABStep
equivalent); `arm_digest` covering the procedure text and whether the
source is passed, added only when they differ from the existing arm's
setting, so published digests do not change.

Report per run: turns, uncached and total input tokens (Qwen has no
caching, so judge it on total), tool-call mix, the share of metric queries
preceded by a `lookup_metric` or `lookup_domain`, cost, accuracy.

Success: turns and input tokens down; accuracy non-inferior to step 1,
paired per task as FINDINGS does, with the margin written into the
predictions before the runs (proposed: the paired 95% interval's lower bound
above −3 points, and no lost fee-family wins on DABStep). The riskiest
change is the plain-register rewrite of the metric-lookup constraint; if
accuracy drops, bisect that first.

## Constraints

- TDD (CLAUDE.md). Prompt tests in `tests/test_core`, descriptions in
  `tests/test_tools`.
- Lint through prek. Lowest-floor suite must pass.
- Examples' golden output is diffed in CI; regenerate with
  `scripts/regen_examples.sh` and review the diff. At least one example
  renders the prompt with the source, so the golden output covers the
  recommended setup.
- User-visible prompt and description changes: CHANGELOG `### Changed`,
  minor version.
- Tests, docs and scripts that match on description or hint text are
  updated, not loosened. Known sites:
  - `tests/test_tools/test_tool_protocol.py` (exact composition of the
    `run_query` description, the ordering clause on `inspect_query`, the
    scope guard on other tools);
  - `tests/test_tools/test_row_format.py:233,302`;
  - `tests/test_core/test_scalability.py:102-153` (names absent, the
    `lookup_domain` hint, "before computing");
  - `tests/test_core/test_system_prompt_metrics.py:54,83`;
  - `tests/test_core/test_prompt_renderers.py:649-652` (log text);
  - README: How It Works flow (86-95), Query protocol (495-511, quotes the
    MUST sentence), domain workflow (618-628), threshold table (~1557),
    quickstart (285, 335);
  - `docs/architecture.md`: Query Protocol (631-670), Natural Agent
    Workflow (763-766);
  - `experiments/dabstep-contract-eval/dce/tools.py` docstring (~88-98,
    quotes the old descriptions and their length).

## Left to the builder

- Exact XML tag names and attribute order, if consistent with the file.
- Name of the no-domain group.
- Wording of descriptions and the README section, within the rules above.
