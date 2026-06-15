# Findings: Does Mermaid help an LLM reason over table relationships?

**Date:** 2026-06-07
**Question:** Is rendering a table-relationship graph as **Mermaid** a more effective "memory carrier"
for an LLM than the library's current **XML**, or than plain **natural-language adjacency** — measured by
join-path reconstruction accuracy?
**Short answer:** **No.** Mermaid is at best tied-within-noise and at worst significantly *worse*.
XML and NL-adjacency are equivalent and best. **Do not switch the library's relationship rendering to Mermaid.**

Design spec: `docs/superpowers/specs/2026-06-07-mermaid-joinpath-eval-design.md`.
Plan: `docs/superpowers/plans/2026-06-07-mermaid-joinpath-eval.md`.

## Method

- **Task (join-path reconstruction):** given a rendered schema graph + the set of endpoint tables a gold
  query touches, the model outputs the JOIN conditions connecting them. Grading is static (no DB execution):
  the model's join-edge set is compared to the gold query's join edges (extracted with `sqlglot`).
- **Renderings (information-equivalent, fixed ordering):** `xml`, `mermaid` (erDiagram), `nl_adjacency`,
  and later `mermaid_qualified` (erDiagram with fully table-qualified edge labels).
- **Contamination control:** every schema is **opaque-token anonymized** (`orders→t1`, `customer_id→t1.c3`)
  so the model cannot recall memorized gold SQL — essential because both datasets predate the models'
  training cutoff. Anonymization is the decontamination strategy (no public benchmark post-dates the cutoff).
- **Models (via OpenRouter):** `anthropic/claude-sonnet-4.6` (strong) and `deepseek/deepseek-v4-flash`
  (weaker, reasoning) — to test whether any effect grows as capability drops.
- **Metrics:** undirected join-edge **F1** (primary) and **exact-match** of the edge set; paired **McNemar**
  on exact-match across renderings (every item seen under all renderings).
- **Datasets:** Spider dev (1034 items) and BIRD mini-dev (500 items), both filtered to gold SQL with
  ≥2 join edges. "clean" numbers below exclude rows truncated by the output-token cap.

## Headline results

### Spider (shallow joins) — uninformative ceiling
Both models reconstruct 2–3-join paths near-perfectly on Spider regardless of rendering (F1 ≈ 0.96–1.00,
no significant differences). Spider dev is too join-shallow to separate the renderings: of 1034 dev queries,
only 65 have 2 join edges, 6 have 3, and **none** have 4+. This motivated moving to BIRD.

### BIRD mini-dev (deeper joins, larger schemas) — powered result, n=98
Mean F1 / exact-match, clean (truncated rows excluded):

| Rendering | Sonnet 4.6 F1 / exact | DeepSeek V4 Flash F1 / exact |
|---|---|---|
| **xml** | **0.852 / 0.755** | **0.865 / 0.750** |
| **nl_adjacency** | **0.852 / 0.755** | 0.843 / 0.694 |
| mermaid_qualified | 0.827 / 0.694 | 0.848 / 0.684 |
| mermaid (raw) | 0.804 / 0.653 | 0.820 / 0.635 |

Paired McNemar (exact-match), discordant counts as (a-wrong/b-right : a-right/b-wrong):

| Comparison | Sonnet 4.6 | DeepSeek V4 Flash |
|---|---|---|
| xml vs nl_adjacency | 0 / 0, p=1.000 (tied) | 0 / 4, p=0.125 (n.s.) |
| mermaid (raw) vs xml | 12 / 2, **p=0.013** | 11 / 0, **p=0.001** |
| mermaid_qualified vs xml | 7 / 1, p=0.070 (n.s., trend) | 6 / 1, p=0.125 (n.s.) |
| mermaid (raw) vs mermaid_qualified | 5 / 1, p=0.219 | 9 / 3, p=0.146 |

**Reading:**
1. **Raw Mermaid is significantly worse than XML** on both models (p=0.013 / 0.001).
2. **About half that deficit was a renderer-design flaw**, not the notation: my `mermaid` edge label was
   `"c0 = c2"` (unqualified) vs `t1.c0 = t3.c2` in XML/NL. The `mermaid_qualified` variant (qualified labels)
   recovers roughly half the gap and is **no longer significantly worse than XML** (p=0.07 / 0.125).
3. **A residual deficit remains** even with qualified labels — both Mermaid variants are numerically below
   XML/NL on both models. The erDiagram notation appears marginally harder for the model than flat XML/NL,
   but the effect is small and not statistically significant once labels are fair.
4. **XML and NL-adjacency are tied and best.** The library's current XML rendering is already optimal here.

## Conclusion / recommendation

**Mermaid is not a better memory carrier for table relationships.** No Mermaid variant beat XML on either
model; raw Mermaid was significantly worse, and fully-qualified Mermaid only caught up to "tied within noise,
trending worse." **Keep the existing XML relationship rendering** (`core/prompt.py::_render_relationships`).
If Mermaid is ever desired for human readability, **fully table-qualify the join columns in edge labels** —
that recovers most of the penalty.

This is consistent with the literature: capable models are increasingly robust to schema *presentation*.
"Death of Schema Linking?" (Maamari et al., 2024, arXiv:2408.07702) shows modern models barely need the
schema filtered down to relevant tables — they generate fine over the full schema; our result adds, one level
down, that they are also largely indifferent to the rendering *notation* once join columns are unambiguous.
The original Mermaid-as-memory idea (TencentDB-Agent-Memory) works for *episodic* task memory via
compression + drill-down; it does not transfer to *semantic* relationship memory here.

## Methodological catches (why smoke-first mattered)

- **Reasoning-token truncation:** DeepSeek V4 Flash is a reasoning model; with the initial `max_tokens=256`,
  hidden reasoning consumed the whole budget and answers came back empty/truncated. The *first* smoke showed a
  dramatic but **entirely artifactual** "XML collapses" effect. Fix: `max_tokens=4096` + log `raw`/`truncated`.
- **Case-normalization bug:** all-lowercase test fixtures hid a `KeyError` between `edge_key` (lowercases) and
  the anonymization map (original case); caught only by running on real Spider data.
- **Fair-Mermaid confound:** the first BIRD result (Mermaid significantly worse) was partly my under-qualified
  edge labels — isolated by the `mermaid_qualified` arm.

## Limitations

- Join depth is modest even in BIRD mini-dev (84×2-edge, 14×3-edge, 2×4-edge); the discriminating 4+ regime is
  tiny. The full BIRD dev (1534 items) would add more deep cases.
- Single sample per item at temperature 0 (deterministic); no within-item variance estimate.
- The task gives the model the endpoint tables, so it tests join-*column* selection + bridge-finding, not
  table discovery. A harder formulation might surface larger rendering effects.
- Two models only; both fairly capable. A much weaker model might show bigger format sensitivity.

## Reproduction

```bash
cd experiments/mermaid-joinpath-eval && uv sync && uv run pytest -q
set -a; . /Users/qingye/Documents/lens/.env; set +a   # OPENROUTER_API_KEY

# BIRD pilot (xml, mermaid, nl_adjacency), both models, ~$1.0
uv run python -m mje.runner --dataset bird --n 98 --out results/bird_full.jsonl \
  --models anthropic/claude-sonnet-4.6 deepseek/deepseek-v4-flash

# Fair-Mermaid arm (qualified labels), ~$0.3
uv run python -m mje.runner --dataset bird --n 98 --renderings mermaid_qualified \
  --out results/bird_mermaidq.jsonl \
  --models anthropic/claude-sonnet-4.6 deepseek/deepseek-v4-flash

uv run python -m mje.stats --results results/bird_full.jsonl
```

Total OpenRouter spend for the full study (smokes + Spider + BIRD + fair-Mermaid): ≈ **$1.6**.
