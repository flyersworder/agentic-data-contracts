"""DABStep answer scoring — delegated entirely to DABStep's own scorer.

THERE IS NO SECOND SCORER HERE, DELIBERATELY. This module used to carry a
hand-rolled normalizer whose docstring claimed it "mirrors [the official
scorer's] documented behaviour". Nothing verified that claim and it was false
in at least four ways: trailing punctuation, numeric tolerance, `Not
Applicable` synonyms, and fuzzy string matching. On the first 12-task smoke run
the difference graded `Yes.` wrong against a gold of `yes` and moved
`schema_only vs contract` from p=0.0156 (significant) to p=0.0703 (not) — a
false positive in this library's own favour, produced by grading more strictly
than the benchmark being cited.

The root error was treating "not on PyPI" as "unavailable". `dabstep_benchmark`
lives in the public leaderboard Space and its scorer is 146 readable lines; it
was always one fetch away. It is now vendored verbatim at a pinned revision in
`vendor/dabstep_scorer.py` (see `vendor/README.md`).

So `score` is a thin delegation, and an exception from upstream propagates:
`run_task` turns that into a visible `scoring_error` row. Falling back to
different rules on error would put two grading algorithms in one results file
with nothing per row to say which ran — the very hazard `active_scorer` exists
to expose.
"""

from __future__ import annotations

import re

_BRACKETS = re.compile(r"^[\[\('\"]+|[\]\)'\"]+$")


def _load_official():
    """DABStep's own `question_scorer`, preferring a real installed package
    over the vendored copy.

    The installed-package branch is kept ahead of the vendored one so that an
    environment which HAS the real thing — a future PyPI release, or a Space
    checkout on the path — grades with that rather than with a copy that may
    have gone stale.
    """
    try:
        from dabstep_benchmark.evaluation.scorer import (  # type: ignore[import-not-found]
            question_scorer,
        )

        return question_scorer, "official"
    except ImportError:
        from vendor.dabstep_scorer import question_scorer

        return question_scorer, "official-vendored"


_OFFICIAL, _OFFICIAL_KIND = _load_official()


def active_scorer() -> str:
    """Which scorer graded a row: `"official"` (a real installed
    `dabstep_benchmark`) or `"official-vendored"` (the pinned copy).

    Both are the same algorithm; the distinction is provenance, and it is
    recorded per row because the choice is made at import time by whatever is
    on the path. It can no longer return `"fallback"` — there is nothing left
    to fall back to, which is the point.
    """
    return _OFFICIAL_KIND


def _clean(value: str) -> str:
    """Strip surrounding brackets and quotes.

    NOT part of scoring — `score` does its own normalization upstream. This
    exists only to fill the `answer_normalized` column, so a scoring dispute
    can be re-adjudicated from a stored row without re-running the model.
    """
    return _BRACKETS.sub("", str(value).strip()).strip()


def score(predicted: str, gold: str) -> bool:
    """True when `predicted` matches `gold` under DABStep's own rules.

    Deliberately not defensive: an exception from upstream's scorer propagates
    to `run_task`, which records a `scoring_error` row. That is a visible,
    inspectable failure on one row — strictly better than silently grading it
    by different rules.
    """
    return bool(_OFFICIAL(predicted, gold))
