# Mermaid Join-Path Eval

Measures whether rendering an (anonymized) Spider schema as XML / Mermaid / NL-adjacency changes an
LLM's join-path reconstruction accuracy. See `FINDINGS.md` for the method, results, and conclusion.

## Setup
```bash
cd experiments/mermaid-joinpath-eval
uv sync
uv run pytest -q          # all deterministic units, offline
```

## Run the pilot (spends OpenRouter credit)
The key is read from `OPENROUTER_API_KEY`; source it from a `.env` that lives **outside** this repo
(never commit it). Point `LENS_ENV_FILE` at that file:
```bash
set -a; . "$LENS_ENV_FILE"; set +a   # the .env stays outside the repo
uv run python -m mje.runner --n 45 --max-spend 2.00
uv run python -m mje.stats --results results/results.jsonl --exclude-truncated
```

Flags: `--n` items (hardest strata first), `--samples`, `--models`, `--min-joins`, `--max-spend`,
`--max-tokens` (keep high for reasoning models), `--renderings` (subset), and `--tables/--dev` to
point at a manual Spider download if the mirror URL is unavailable.
`stats.py` stratifies by join depth and by the ambiguity (cyclic-schema) proxy, and `--exclude-truncated`
drops output-token-truncated rows; see `FINDINGS.md` for why the ambiguity split matters.

## Notes
- Schemas are opaque-token anonymized to defeat training contamination (Spider predates the model cutoff).
- Grading is static (no DB execution): gold join edges come from gold SQL via sqlglot.
- Data source: the full Spider dev set (166 DBs / 1034 items) is fetched from the `taoyds/spider`
  GitHub raw mirror into `data/` (gitignored). Of the 1034 dev queries, ~71 have >=2 join edges
  (65 with 2, 6 with 3); Spider dev is join-shallow, so deep multi-hop strata require BIRD instead.
- Default models: `anthropic/claude-sonnet-4.6` and `deepseek/deepseek-v4-flash`, via OpenRouter's
  OpenAI-compatible endpoint.
