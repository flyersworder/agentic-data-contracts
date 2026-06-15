# Findings: Does Mermaid help an LLM reason over table relationships?

**Date:** 2026-06-07 (revised 2026-06-15 after code review)
**Question:** Is rendering a table-relationship graph as **Mermaid** a more effective "memory carrier"
for an LLM than the library's current **XML**, or than plain **natural-language adjacency** — measured by
join-path reconstruction accuracy?
**Short answer:** **No — keep XML.** But the evidence is subtler than the first cut suggested, and a code
review corrected an overstatement. Mermaid is **not better** than XML on any subset. Its *aggregate* deficit
is **statistically significant but confounded**: it is confined entirely to cyclic-schema items where static
exact-match is unreliable (the model can pick a *valid alternate* join path that grading marks wrong). On the
subset where the metric is trustworthy, all four renderings are **tied**, with Mermaid marginally highest.
**Recommendation stands: do not switch the library's relationship rendering to Mermaid** — XML never loses and
most reliably reproduces the canonical/gold join path, which is what a governance library wants.

Design spec: `../../docs/superpowers/specs/2026-06-07-mermaid-joinpath-eval-design.md` (repo root, not this dir).
Plan: `../../docs/superpowers/plans/2026-06-07-mermaid-joinpath-eval.md`.

## Method

- **Task (join-path reconstruction):** given a rendered schema graph + the set of endpoint tables a gold
  query touches, the model outputs the JOIN conditions connecting them. Grading is static (no DB execution):
  the model's join-edge set is compared to the gold query's join edges (extracted with `sqlglot`).
- **Renderings (information-equivalent, fixed ordering):** `xml`, `mermaid` (erDiagram), `nl_adjacency`,
  and `mermaid_qualified` (erDiagram with fully table-qualified edge labels).
- **Contamination control:** every schema is **opaque-token anonymized** (`orders→t1`, `customer_id→t1.c3`)
  so the model cannot recall memorized gold SQL — essential because both datasets predate the models'
  training cutoff. Anonymization is the decontamination strategy (no public benchmark post-dates the cutoff).
- **Models (via OpenRouter):** `anthropic/claude-sonnet-4.6` (strong) and `deepseek/deepseek-v4-flash`
  (weaker, reasoning) — to test whether any effect grows as capability drops.
- **Metrics:** undirected join-edge **F1** (primary) and **exact-match** of the edge set; paired **McNemar**
  on exact-match across renderings (every item seen under all renderings). Parse-failure rate (empty
  prediction) and output-token truncation rate are tracked per rendering so neither can masquerade as a
  reasoning effect, and so an *asymmetric* rate across renderings is detectable.
- **Datasets:** Spider dev (1034 items) and BIRD mini-dev (500 items), both filtered to gold SQL with
  ≥2 join edges. "clean" numbers below exclude rows truncated by the output-token cap (`--exclude-truncated`).
- **Ambiguity flag:** an item is flagged `ambiguous` if the schema's FK graph contains a cycle (a proxy for
  "more than one plausible join path may exist"). **Caveat:** this is a whole-schema proxy, not a property of
  the specific query path, and in this BIRD sample it correlates with *easier* items (see below). It is used
  to stratify results, because exact-match is least reliable exactly where alternate valid paths exist.

## Headline results

### Spider (shallow joins) — uninformative ceiling
Both models reconstruct 2–3-join paths near-perfectly on Spider regardless of rendering (F1 ≈ 0.96–1.00,
no significant differences). Spider dev is too join-shallow to separate the renderings: of 1034 dev queries,
only 65 have 2 join edges, 6 have 3, and **none** have 4+. This motivated moving to BIRD.

### BIRD mini-dev (deeper joins, larger schemas) — n=98, clean (truncated rows excluded)

**Aggregate** mean F1 / exact-match:

| Rendering | Sonnet 4.6 F1 / exact | DeepSeek V4 Flash F1 / exact |
|---|---|---|
| **xml** | **0.852 / 0.755** | **0.865 / 0.750** |
| **nl_adjacency** | **0.852 / 0.755** | 0.843 / 0.694 |
| mermaid_qualified | 0.827 / 0.694 | 0.848 / 0.684 |
| mermaid (raw) | 0.804 / 0.653 | 0.820 / 0.635 |

Taken alone this says "Mermaid worse." **But the aggregate is Simpson-fragile.** Splitting by the ambiguity
flag flips the story:

**Mean F1 by ambiguity subset:**

| Rendering | Sonnet — unambig (n=41) | Sonnet — ambig (n=57) | DeepSeek — unambig (n≈40) | DeepSeek — ambig (n≈57) |
|---|---|---|---|---|
| xml | 0.707 | **0.956** | 0.748 | **0.946** |
| nl_adjacency | 0.707 | **0.956** | 0.725 | 0.928 |
| mermaid_qualified | 0.732 | 0.895 | 0.748 | 0.921 |
| mermaid (raw) | **0.756** | 0.838 | **0.769** | 0.856 |

On the **unambiguous** subset (metric reliable), Mermaid is *marginally highest*. On the **ambiguous** subset
(metric unreliable), XML/NL are highest and Mermaid lowest. The aggregate is dominated by the 57 ambiguous
items, so XML "wins" overall — but only on the items where the metric is least trustworthy.

**Paired McNemar (exact-match), xml vs mermaid (raw), by subset** — direction shown as
`xml-right/mermaid-wrong : mermaid-right/xml-wrong`:

| Subset | Sonnet 4.6 | DeepSeek V4 Flash |
|---|---|---|
| all items | 12 / 2, **p=0.013** | 11 / 0, **p=0.001** |
| **unambiguous (metric reliable)** | 0 / 1, **p=1.000 (tied)** | 0 / 0, **p=1.000 (tied)** |
| ambiguous (metric unreliable) | 12 / 1, p=0.003 | 11 / 0, p=0.001 |

**Every drop of statistical significance comes from the ambiguous subset.** On the reliable subset the two are
exactly tied. The same holds for `mermaid_qualified` (always between `mermaid` and `xml`; never significantly
worse than XML on any subset) and for `xml` vs `nl_adjacency` (tied throughout).

**Parse-failure and truncation are NOT the driver.** Empty-prediction rates are low and rendering-symmetric
(≤1 item difference between mermaid and xml on each model; e.g. Sonnet xml 2.0% vs mermaid 3.1%). DeepSeek's
xml and raw-mermaid had the *same* truncation count (2 each), so truncation cannot explain the gap. This
robustness check is now reproducible via `stats.py` (it was applied manually in the first writeup but not
shipped in code — the review caught that).

## Conclusion / recommendation

**Mermaid is not a better memory carrier for table relationships, and there is no subset on which it reliably
beats XML.** The honest, review-corrected reading:

1. **No rendering is reliably better; differences are small and subset-dependent.**
2. **The aggregate "Mermaid significantly worse" result is confined to cyclic-schema items and is confounded.**
   On those items the model may output a *valid alternate* join path that static exact-match marks wrong; the
   grader cannot distinguish "wrong" from "valid-but-not-the-gold-path" without execution-based equivalence
   checking. Two non-exclusive explanations (Mermaid genuinely guides to the canonical path less well when
   multiple paths exist, vs. a pure scoring artifact) cannot be separated here.
3. **On the metric-reliable (acyclic) subset, all renderings are statistically tied**, with Mermaid marginally
   highest in mean F1 — so there is *no* evidence Mermaid is worse there.
4. **Keep the existing XML relationship rendering** (`core/prompt.py::_render_relationships`). It is never
   significantly worse than anything, and it most reliably reproduces the canonical/gold join path — exactly
   what a governance library wants (predictable, auditable joins). Switching to Mermaid carries downside risk
   on cyclic schemas with no demonstrated upside. If Mermaid is ever desired for human readability, **fully
   table-qualify the join columns in edge labels** (`mermaid_qualified`) — it recovers most of the gap.

This is consistent with the literature: capable models are increasingly robust to schema *presentation*.
"Death of Schema Linking?" (Maamari et al., 2024, arXiv:2408.07702) shows modern models barely need the
schema filtered down to relevant tables — they generate fine over the full schema; our result adds, one level
down, that they are also largely indifferent to the rendering *notation* once join columns are unambiguous.
The original Mermaid-as-memory idea (TencentDB-Agent-Memory) works for *episodic* task memory via
compression + drill-down; it does not transfer to *semantic* relationship memory here.

## Methodological catches (why smoke-first and code-review mattered)

- **Reasoning-token truncation:** DeepSeek V4 Flash is a reasoning model; with the initial `max_tokens=256`,
  hidden reasoning consumed the whole budget and answers came back empty/truncated. The *first* smoke showed a
  dramatic but **entirely artifactual** "XML collapses" effect. Fix: `max_tokens=4096` + log `raw`/`truncated`.
- **Case-normalization bug:** all-lowercase test fixtures hid a `KeyError` between `edge_key` (lowercases) and
  the anonymization map (original case); caught only by running on real Spider data.
- **Fair-Mermaid confound:** the first BIRD result (Mermaid significantly worse) was partly under-qualified
  edge labels — isolated by the `mermaid_qualified` arm, which recovered ~half the aggregate gap.
- **Ambiguity confound (found in code review):** the "Mermaid significantly worse" headline was driven
  entirely by cyclic-schema items where exact-match is unreliable. The design had called for excluding/flagging
  ambiguous items but `stats.py` never did; stratifying revealed the effect vanishes on the reliable subset.
  This downgraded the claim from "significantly worse" to "not better; aggregate deficit is confounded."

## Limitations

- **The `ambiguous` proxy is whole-schema (FK-graph cycle), not query-path-specific**, and in this sample it
  correlates with *easier* items (ambig F1 ≈ 0.95 vs unambig ≈ 0.71) — likely because cyclic schemas here tend
  to connect endpoint tables more directly. So "ambiguous" really means "schema has a cycle," a noisy stand-in
  for join-path ambiguity. The robust takeaway does not depend on its exact semantics: the significant effect
  is concentrated in one subset and absent in the complement.
- **Static grading cannot credit valid alternate join paths.** Execution/equivalence-based grading would be
  needed to determine whether Mermaid's divergent edges on cyclic schemas are wrong or valid-but-alternate.
- Join depth is modest even in BIRD mini-dev (84×2-edge, 14×3-edge, 2×4-edge); the discriminating 4+ regime is
  tiny. The full BIRD dev (1534 items) would add more deep cases.
- Single sample per item at temperature 0 (deterministic); no within-item variance estimate; no bootstrap CIs.
- Six rendering-pair McNemar tests × 2 models × strata; no family-wise correction applied. The strongest
  result (p=0.001) survives Bonferroni; the borderline ones (p≈0.07, 0.13) are already reported as n.s.
- The task gives the model the endpoint tables, so it tests join-*column* selection + bridge-finding, not
  table discovery. A harder formulation might surface larger rendering effects.
- Two models only; both fairly capable.

## Reproduction

```bash
cd experiments/mermaid-joinpath-eval && uv sync && uv run pytest -q
set -a; . "$LENS_ENV_FILE"; set +a   # provides OPENROUTER_API_KEY (keep the key out of this repo)

# BIRD pilot (xml, mermaid, nl_adjacency), both models, ~$1.0
uv run python -m mje.runner --dataset bird --n 98 --out results/bird_full.jsonl \
  --models anthropic/claude-sonnet-4.6 deepseek/deepseek-v4-flash

# Fair-Mermaid arm (qualified labels), ~$0.3
uv run python -m mje.runner --dataset bird --n 98 --renderings mermaid_qualified \
  --out results/bird_mermaidq.jsonl \
  --models anthropic/claude-sonnet-4.6 deepseek/deepseek-v4-flash

# Combine the two arms, then the stratified + clean stats (reproduces the tables above)
cat results/bird_full.jsonl results/bird_mermaidq.jsonl > results/_bird_combined.jsonl
uv run python -m mje.stats --results results/_bird_combined.jsonl --exclude-truncated
```

`stats.py` prints mean F1 / exact / parse-fail / truncation by model × rendering × {all, join-depth, ambig,
unambig}, then paired McNemar for items = all / unambig / ambig. The `unambig` block is the metric-reliable
one; that is where the renderings are tied.

Total OpenRouter spend for the full study (smokes + Spider + BIRD + fair-Mermaid): ≈ **$1.6**.
