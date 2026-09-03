# Vendored third-party code

## `dabstep_scorer.py`

DABStep's official answer scorer, copied verbatim from the benchmark's own
leaderboard Space so that scores here are computed by the same rules as the
published leaderboard.

- **Source:** `https://huggingface.co/spaces/adyen/DABstep`,
  `dabstep_benchmark/evaluation/scorer.py`
- **Upstream revision:** `d4431c2e4a695cbe43c33aab2adaa304a37ae64a`
- **sha256 of the copied file:** `229c04ecaa249ccc5b555dee11c85fe93d073d41eee61b6c3584065ae33c9df3`
- **Authors:** Martin Iglesias, Alex Egg, Andreu Mora, Friso Kingma,
  Leandro von Werra (Adyen / Hugging Face)
- **Licence:** the Space card declares `cc-by-4.0`; the Space's own `setup.py`
  carries an OSI MIT classifier. The two disagree, so this copy is retained
  with full attribution and an unmodified body, which satisfies either
  reading. Only a header comment was added; no line of the algorithm was
  changed. Re-check before redistributing this directory on its own.

### Why it is vendored rather than depended on

`dabstep_benchmark` is not published on PyPI — it exists only inside the
leaderboard Space — so there is no version to pin in `pyproject.toml`. Fetching
it at scoring time would put a network call in the grading path and make a
results file unreproducible once the Space moves. A pinned, hashed copy is the
only way to get the official rules AND determinism.

### Why it matters — a false positive we nearly published

This experiment's own fallback scorer is STRICTER than the official one, and
the difference is not cosmetic. On the 12-task smoke run it disagreed on two
rows: both ungoverned arms answered `Yes.` where the gold was `yes`, and the
fallback marked them wrong because it normalises case but not trailing
punctuation. The official `compare_strings` strips all non-word characters, so
both are correct.

Those two rows moved the headline:

| comparison | fallback | official |
|---|---|---|
| `schema_only` vs `contract` | p=0.0156 **significant** | p=0.0703 not significant |
| `manual_prompt` vs `contract` | p=0.0625 | p=0.2188 |

The stricter scorer produced a significant result, in this library's favour,
that the benchmark's own rules do not support. The official scorer is also
materially more lenient in ways that will matter more over 450 tasks than over
12 — `rel_tol=1e-4` numeric comparison, rounding to the lesser decimal
precision of the two values, single-word subset matching, and a
`SequenceMatcher` ratio above 0.95 for strings — so no FINDINGS number may be
computed with the fallback.
