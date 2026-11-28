# Lean governed tools (#139) — design

## Intent

Production agents built on the governed tools spend more turns and more
uncached input tokens than the same agent given plain SQL access, for the
same accuracy. On LiveSQLBench (gpt-6-luna, k=3, primary set) the `contract`
arm averages 6.2 turns and 13k uncached input tokens a run, against 4.1 turns
and 3k for `manual_compiled`, at 35.9% vs 36.8% accuracy (FINDINGS, 325 of
906; the luna-r{1,2,3} rows give 35.8% on 909). Per run it makes
`lookup_domain` 2.06, `lookup_metric` 1.91, `inspect_query` 1.48,
`list_metrics` 0.64 and `run_query` 1.42 calls.

The target is production deployments (for example an analytics agent on
Pydantic AI), not the paper's reported cost. The change lives in the
library: prompt rendering, tool descriptions, and the README's recommended
setup.

## What the evidence says to keep

FINDINGS ("Where the tools help: retrieval at the point of use, not
validation") shows the governed tools' 5-point lead on DABStep's
non-rule-matching tasks (the "other 202") sits in three families that
average fees, and came from putting the governing rule in front of the
agent just before it wrote the query. The evidence is luna-only, task-level
and exploratory: it does not survive clustering over question shapes
(+5.1, shape interval [−1.8, +11.1]), and on Qwen the tools trail the text
arm on the same tasks (−6.9). It is the best mechanism FINDINGS has, not a
settled effect, which is why this design protects it and measures it rather
than relying on it. `inspect_query` played no part (no rejection in any winning run). The
text arm, which had every definition in its prompt, lost those tasks. The
rule arrived through `lookup_metric` in most winning runs, but in 16 of 50
through `lookup_domain(fees)` or the governed `describe_table`: the fees
domain description carries the null-means-all rule. Every governed run in
those families made both lookups before its first fee query, and
`lookup_domain("fees")` preceded the first fee query in 81-83% of governed
runs (FINDINGS, "Lookup log").

So the overhead to cut is the *path* to a definition (finding names,
browsing, inspecting before running), not the domain and metric lookups at
the point of use.

Two causes of the overhead sit outside the library: the harness's
`CONTRACT_PROCEDURE` tells the agent to inspect every query, and the
governed arm renders the prompt without a semantic source, so no metric
name reaches it.

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
  descriptions and not as tool names in the library-rendered prompt (a
  disabled tool would leave a dangling reference). Two exceptions, both
  deliberate: the metric constraint on `run_query` / `inspect_query`
  (below), and a host-written prompt (the README sentence, the harness
  procedure), whose author knows which tools it wired.
- **Never discourage tool use** ("minimize tool calls" is followed
  literally). Cut hops by shortening the path instead.
- Fewer parameters the model must fill; parallel calls already fetch
  independent items in one turn.

"Concise" here means less steering, not less contract: some descriptions
grow.

## Goals

1. An agent can go from the system prompt straight to `lookup_domain` /
   `lookup_metric`, without `list_metrics` hops just to learn metric names,
   when the caller passes the semantic source.
2. Tool descriptions are precise contracts in plain register, and stop
   implying that every query should be inspected.
3. The README shows the recommended production setup.
4. Checked on a live agent: fewer turns and inspect/list hops per question,
   with the domain and metric lookups intact (Acceptance).

## Non-goals

- Putting metric SQL, or metric or domain description bodies, in the
  system prompt (beyond the no-domain detailed listing that exists today).
- A batch `lookup_metric` (dropped: parallel calls cover it, and it would
  need the conformance check and recorder to learn per-name results).
- `to_system_prompt()` loading the semantic source itself (decided: callers
  opt in; existing prompts and the benchmark arms stay as they are).
- A semantic-source opt-in for `bridge.compile_to_contract` (it calls
  `to_system_prompt()` with no source; a follow-up if a bridge user asks).
- Changing what any check enforces, or the `DatabaseAdapter` /
  `SemanticSource` protocols.
- Editing `CONTRACT_PROCEDURE`, the existing arms, their digests, or frozen
  contract directories.
- Benchmark runs as a release gate (Acceptance; the ladder is an optional
  paper follow-up).

## Anti-goals

- **Losing accuracy to save tokens.** Nothing may remove or discourage the
  domain or metric lookup immediately before a metric query.
- **Changing a no-source caller's prompt.** A caller that passes no semantic
  source gets exactly today's rendering.

## Approaches considered

- **A. Shorten the path (chosen).** Metric names in the prompt index;
  descriptions rewritten to the guidance. Keeps retrieval at the point of
  use; moves only names into the prompt.
- **B. Metric SQL in the prompt, drop the lookup instruction.** Cheapest per
  token, but it is the text arm's design, which trailed the tools by 5
  points (luna, task-level) on DABStep's fee-averaging families, exactly
  where retrieval at the point of use mattered.
- **C. `lookup_domain` returns full definitions.** Saves one hop but returns
  every member metric's SQL as fresh, mostly unused tokens.

Chosen by the user: A, without the batch lookup, without auto-loading.

## Design

### 1. Metric names in the prompt index (`core/prompt.py`)

Everything in this section applies only when the caller passes a semantic
source with metrics. Without one, rendering is byte-for-byte today's,
including the domain hint that names `lookup_domain` / `lookup_metric`.

**Rendering.** Let *entries* be the number of metric names the index would
list (a metric listed under three domains counts three times).

- **With domains** (`_render_domains`): each `<domain>` keeps today's
  attributes and lists its member metrics by name only (membership from
  `metrics_in_domain`). Metrics in no domain the contract catalogs (none,
  or only uncataloged ones) are listed in one extra group, not named
  `<available_metrics>`. If entries exceed the index threshold, the names
  are dropped: the `<domain>` elements render as today (with
  `metric_count`), followed by the new "names dropped" hint below.
- **Without domains** (`_render_metrics`): if entries exceed the index
  threshold, a count line, exactly `<count>{m} metrics available; names
  omitted above {t} index entries.</count>` (`{t}` the resolved index
  threshold); else if metrics exceed the detail threshold, names only; else
  today's listing with descriptions.
- Descriptions are never added where they are not today: a metric's
  description is a paragraph (DABStep averages about 700 characters, and
  `fee_rule_matches_transaction` carries the wildcard rule).

**Thresholds.** A new `metric_index_threshold` constructor argument
(default `_UNSET`) and class constant `METRIC_INDEX_THRESHOLD = 200`. One
helper resolves it, used by both the `__init__` path and the `getattr` path
for legacy subclasses that never call `super().__init__()`:

1. the explicit argument, if given (`None` means never drop names);
2. else `None`, if the resolved detail threshold is `None` (today's
   "never degrade" opt-out, from the argument or the class constant);
3. else the class's `METRIC_INDEX_THRESHOLD` (so a subclass override wins).

`metric_detail_threshold` itself keeps its meaning (descriptions in the
no-domain branch); `_render_domains` never read it and still does not,
except through rule 2.

**Hints.** They name no tool and keep the "before computing" constraint
that `test_scalability` guards on purpose. The rule covers the metric and
domain hints only; the relationship hint is unchanged. Exact text:

- domains, names listed: `<hint>Each domain's description holds rules
  that apply to its metrics, and each listed metric has a governed SQL
  definition. Look up the domain and the metric before computing
  it.</hint>`
- domains, names dropped: `<hint>{m} metrics have governed SQL
  definitions; their names are omitted here. Each domain's description
  holds rules that apply to its metrics. Look up the domain and the metric
  before computing it.</hint>`
- no domains, listed (names only or with descriptions): `<hint>Each
  listed metric has a governed SQL definition; look it up before computing
  the metric.</hint>`
- no domains, names dropped: `<hint>{m} metrics have governed SQL
  definitions; their names are omitted here. Look a metric up before
  computing it.</hint>`

`{m}` is the number of distinct metrics, never the threshold.

**Logging.** An INFO log, reworded (no "agents must call list_metrics"),
whenever the index omits something: descriptions (no-domain names-only) or
names (either branch, above the index threshold). It says which, and which
argument restores it. A caller who already passes a source to a no-domain
contract with 21-200 metrics gets names added with no log; the CHANGELOG
says so. The index threshold is resolved at render time, like the detail
threshold, so setting `renderer.metric_detail_threshold = None` after
construction still implies "never drop names".

**Cost.** Names are cached where the provider caches; where it does not
(Qwen runs show `cached_tokens` 0 throughout), the index is paid every
turn. All 22 LiveSQLBench contracts list 20-86 entries and DABStep 22, all
under the 200 bound.

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
  <hint>Each domain's description holds rules that apply to its metrics, and each listed metric has a governed SQL definition. Look up the domain and the metric before computing it.</hint>
</available_domains>
```

### 2. Tool descriptions (`tools/factory.py`)

Rewrite all nine descriptions to the guidance above. Every new clause
appears only when the capability it names exists (as `metric_ordering` is
gated on metrics today). Input schema names, types and required fields are
unchanged; parameter descriptions inside the schema may be rewritten to the
same rubric.

- `describe_table`, `preview_table`: today one-liners; add what is returned,
  when to use them, and what they do not return. Return-shape clauses only
  with an adapter (without one they return an error); "governed
  descriptions" only when the semantic source carries table entries
  (decided at build time). Preview's row cap is stated as it is: at most
  100 rows, and at most `max_result_rows` when that is set.
- `lookup_metric`: carries the point-of-use routing (only when metrics
  exist): use it before writing SQL that computes a metric, because the
  definition (filters, units, wildcard rules) is governed and the query must
  follow it. Plain register. Its own text, not `_PROTOCOL_METRIC_ORDERING`
  (the scope guard in `test_tool_protocol.py` keeps that off other tools).
- `list_metrics`: for browsing or filtering by domain, tier or kind when the
  metric needed is not known by name.
- `lookup_domain`: returns the domain's business description, tables and
  member metrics. Says what the description holds: rules that apply across
  the domain's metrics (how missing values or wildcards are read, for
  example), and keeps one plain point-of-use clause with that reason: fetch
  it before writing SQL that computes one of the domain's metrics, because
  the query must follow those rules. This replaces today's "before
  querying", which read as every query; it is the domain lookup's only
  timing cue for a host whose prompt says nothing about it.
- `run_query`: runs the same validation as `inspect_query` before
  executing and does not run a blocked query. The query timeout (during
  execution) and result checks (after) are mentioned only when the contract
  declares them. Keeps the metric constraint as one plain sentence with its
  reason (`_PROTOCOL_METRIC_ORDERING` rewritten, no MUST/never). This is the
  deliberate exception to "routing in its own description": descriptions
  travel with the tools on every path, the prompt is opt-in. Keep the
  precedence clause (`_PROTOCOL_PRECEDENCE`), plainly worded.
- `inspect_query`: checks a query without running it, for an expensive
  query or (with an adapter, which supplies it) a cost estimate; inspecting
  first is optional, since `run_query`
  applies the same checks. Keeps the metric constraint (as today, same
  gating). When the contract sets `max_retries`, add that a blocked or
  failed `run_query` spends a retry and `inspect_query` does not. Keep the
  return-shape text.
- `lookup_relationships`, `trace_metric_impacts`: audit against the same
  rubric; change only where they fall short.

### 3. README: recommended production setup

A short section: pass the semantic source to `to_system_prompt()` so the
metric index reaches the prompt; a recommended system-prompt sentence
(before writing SQL that computes a metric, look up its domain and the
metric; run queries with `run_query`, which validates them; use
`inspect_query` to check without running). Note the trade-off: requiring an
inspect before every query costs a turn per query for no added checking,
but with `max_retries` set it keeps blocked drafts from spending retries.

The quickstart passes `dc.load_semantic_source()` to `to_system_prompt()`.

## Acceptance

This is a library change for production use, not a paper result, so the
bar is that the change does what it says and does not break the one thing
that matters. The overhead it targets is already measured (Intent), and the
change is easy to revert: the index is opt-in and no-source prompts are
byte-for-byte unchanged.

1. **Tests** (Constraints): no-source golden strings, index rendering and
   thresholds, the rewritten descriptions.
2. **Before/after trace check.** About ten realistic questions that each
   compute a governed metric, on a real agent (the production analytics
   agent, or `examples/revenue_agent` with a live model), run on v0.64.0 and
   on this branch. Both runs pass the semantic source and use the README's
   recommended prompt sentence. Read the traces:
   - **Go/no-go:** before every metric query the agent still calls
     `lookup_domain` and `lookup_metric` (the anti-goal). A miss on more
     than one question means the descriptions need work before release.
   - **Direction:** turns and `inspect_query` / `list_metrics` calls per
     question fall. No significance test; it only has to point the right
     way.

Record the counts in the PR description.

A production host whose own prompt says to inspect every query keeps that
cost; rewording it there is the host's lever, which the README section names.

### Optional paper follow-up

If the change is later wanted as a measured result, the ladder below is the
design, and it is not a release gate: step 0 `contract` on v0.64.0; step 1
`contract` on the new library (descriptions); step 2 `contract_lean_lib`
(plus the semantic source); step 3 `contract_lean` (plus `LEAN_PROCEDURE`
replacing `CONTRACT_PROCEDURE`):

> Before writing SQL that computes a metric, look up its domain with
> lookup_domain and the metric with lookup_metric, and follow both. Run
> queries with run_query, which validates them; use inspect_query only to
> check a query without running it.

on LiveSQLBench (cost) and DABStep (accuracy), gpt-6-luna and qwen3.8-27b,
k=3, step 3 against step 0, accuracy non-inferior at a -5 point margin and
the `lookup_domain("fees")` share before the first fee query within 10
points. Building it needs new arms in `experiments/dabstep-contract-eval/`
(their digests, the arm-list tests) and a ladder analysis script.

## Constraints

- TDD (CLAUDE.md). Prompt tests in `tests/test_core`, descriptions in
  `tests/test_tools`.
- Lint through prek. Lowest-floor suite must pass.
- Examples' golden output is diffed in CI; regenerate with
  `scripts/regen_examples.sh` and review the diff. No example prints the
  index today: `examples/revenue_agent/agent.py` renders with the source
  (around line 341) but prints only `<column_hints>`…`</join_paths>`. The
  builder adds a print of the rendered domain/metric index section there
  and regenerates its `expected_output.txt`, so the golden output covers
  §1. The examples' SDK paths, which render without a source, are left as
  they are; the README is where the recommended setup is shown.
- No-source prompts stay byte-for-byte: a test renders a few no-source
  contracts (with and without domains, above and below the thresholds)
  and compares them with golden strings captured from v0.64.0 before any
  change.
- User-visible prompt and description changes: CHANGELOG `### Changed`,
  minor version.
- Tests, docs and scripts that match on description or hint text are
  updated, not loosened. Known sites:
  - `tests/test_tools/test_tool_protocol.py` (exact composition of the
    `run_query` description, the ordering clause on `inspect_query`, the
    scope guard on other tools);
  - `tests/test_tools/test_row_format.py:233,302`;
  - `tests/test_tools/test_error_signalling.py:430` (imports
    `_PROTOCOL_PRECEDENCE`);
  - `tests/test_core/test_scalability.py:102-160` (names absent, the
    `lookup_domain` hint, "before computing");
  - `tests/test_core/test_system_prompt_metrics.py:54,83`;
  - `tests/test_core/test_prompt_renderers.py`: 585-593 and 796-807 ("5
    metrics available" now renders names; 807 needs a legacy-subclass
    analogue for `METRIC_INDEX_THRESHOLD`), 640-652 (log text), 341-343
    (`<available_metrics>` absence), 291-300 and 308-325 (30 metrics, no
    domains: "30" and "should not list individual metrics" no longer hold;
    rewrite to the names-only expectation);
  - README: How It Works flow (86-95), The 9 Tools table (482-490), Query
    protocol (495-511, quotes the MUST sentence), domain workflow
    (618-628), threshold table and its notes (~1557-1575), quickstart (285,
    335);
  - `docs/architecture.md`: Query Protocol (631-670), Natural Agent
    Workflow (763-766);
  - source comments that describe the old texts: the comment block in
    `tools/factory.py` (~295-330) and the `_render_metrics` docstring.
- Docs that describe the published runs (`dce/tools.py` docstring ~88-98,
  `experiments/dabstep-contract-eval/README.md:381-388`) are annotated "as
  of adc ≤ 0.64", not rewritten: they describe what those runs saw. The
  paper (`docs/paper/sections/03-system.tex:88-90`) is left as is for the
  same reason.

## Left to the builder

- Exact XML tag names and attribute order, if consistent with the file.
- Name of the no-domain group.
- Wording of descriptions and the README section, within the rules above.
