"""DABStep answer scoring.

Prefers the official scorer when installable so results stay comparable with
the published leaderboard; the fallback mirrors its documented behaviour.
"""

from __future__ import annotations

import re

_NA = {"", "n/a", "na", "none", "null", "not applicable"}
_BRACKETS = re.compile(r"^[\[\('\"]+|[\]\)'\"]+$")
_THOUSANDS = re.compile(r"^-?\d{1,3}(,\d{3})+(\.\d+)?$")


def _load_official():
    """DABStep's own `question_scorer`, preferring a real installed package
    over the vendored copy.

    `dabstep_benchmark` is not on PyPI — it lives only inside the leaderboard
    Space — so `vendor/dabstep_scorer.py` is a pinned, hashed copy of the same
    file and is what actually runs here. The installed-package branch is kept
    ahead of it so that an environment which DOES have the real thing (a future
    release, or a Space checkout on the path) grades with that instead of a
    copy that may have gone stale.
    """
    try:
        from dabstep_benchmark.evaluation.scorer import (  # type: ignore[import-not-found]
            question_scorer,
        )

        return question_scorer, "official"
    except ImportError:
        pass
    from vendor.dabstep_scorer import question_scorer

    return question_scorer, "official-vendored"


_OFFICIAL, _OFFICIAL_KIND = _load_official()


def _official(predicted: str, gold: str) -> bool | None:
    """DABStep's verdict, or None if its scorer raises.

    Upstream's `question_scorer` is not written defensively — `compare_lists`
    recurses, `extract_numeric` regexes arbitrary text — so a pathological
    model answer could raise where our own fallback would simply return False.
    Falling through to the fallback on an exception keeps one bad answer from
    turning into a `scoring_error` row, and is the ONLY case where the fallback
    still grades anything.
    """
    try:
        return bool(_OFFICIAL(predicted, gold))
    except Exception:
        return None


def active_scorer() -> str:
    """Which scorer `score()` will actually use, right now: `"official"`
    when `dabstep_benchmark` is importable, `"fallback"` otherwise.

    Recorded on every result row (`dce.agent.build_result_row`) because the
    two scorers are NOT equivalent and the difference changed a headline. On
    the 12-task smoke run the fallback disagreed with DABStep's own rules on
    two rows (`Yes.` against gold `yes`: the fallback normalises case but not
    trailing punctuation; upstream's `compare_strings` strips all non-word
    characters). Those two rows moved `schema_only vs contract` from p=0.0156
    to p=0.0703 — a significant result, in this library's favour, that the
    benchmark's rules do not support.

    So the fallback is no longer the normal path: it grades only if upstream's
    scorer raises. Returns `"official"` for a real installed
    `dabstep_benchmark`, `"official-vendored"` for the pinned copy in
    `vendor/`, `"fallback"` only if neither can be reached at all.
    """
    return "fallback" if _official("x", "x") is None else _OFFICIAL_KIND


def _clean(value: str) -> str:
    return _BRACKETS.sub("", str(value).strip()).strip()


def _as_float(value: str) -> float | None:
    try:
        return float(value.replace(",", "") if value.count(",") == 1 else value)
    except ValueError:
        return None


def _is_thousands_grouped(value: str) -> bool:
    """True for a single numeric literal like `1,234.56`, not a list of values."""
    return bool(_THOUSANDS.match(value))


def _is_scalar_looking(value: str) -> bool:
    """True when a comma-containing gold should be read as one number, not a list.

    `"709,741,454"` is genuinely ambiguous: it matches `_THOUSANDS` perfectly,
    but it is equally plausible as a 3-element merchant-id list. There is no
    syntactic rule that resolves this — preferring the list reading brings
    back `score("500,10", "10,500") -> True`. See `golds.py::_norm`'s
    docstring: it hit the identical ambiguity and reverted a
    thousands-separator guard there for the same reason.

    The stored gold is the *modal raw submission string*
    (`Counter(plurality).most_common(1)[0][0]` in `golds.py`), not `_norm`'s
    canonical ", "-joined form — so nothing structurally guarantees a numeric
    list gold keeps its spaces. It is only true *empirically*, today: task
    59's gold is the no-space `"IT,ES,FR"`, proof the plurality vote can drop
    the space on a genuine list (that one is safe only because it's
    non-numeric — `_THOUSANDS` never matches letters). This function draws
    the line at what's true today: a value reads as a plain number only when
    it matches `_THOUSANDS` *and* contains no `", "`.
    `test_every_numeric_comma_gold_uses_the_reconstruction_separator` in
    `test_grade.py` checks that boundary against the real corpus on every
    run, so the day a *numeric* list gold's modal rendering drops its
    spaces too, it fails loudly here instead of silently misgrading.
    """
    return _is_thousands_grouped(value) and ", " not in value


def _scalar_match(predicted: str, gold: str) -> bool:
    p, g = _clean(predicted), _clean(gold)
    if p.lower() in _NA and g.lower() in _NA:
        return True
    pf, gf = _as_float(p), _as_float(g)
    if pf is not None and gf is not None:
        scale = max(abs(pf), abs(gf), 1e-12)
        return abs(pf - gf) / scale < 1e-9
    return p.lower() == g.lower()


def score(predicted: str, gold: str) -> bool:
    """True when `predicted` matches `gold` under DABStep's rules."""
    official = _official(predicted, gold)
    if official is not None:
        return official

    p, g = _clean(predicted), _clean(gold)
    if "," in g and not _is_scalar_looking(g):
        pl = sorted(_clean(x).lower() for x in p.split(",") if _clean(x))
        gl = sorted(_clean(x).lower() for x in g.split(",") if _clean(x))
        return pl == gl
    return _scalar_match(p, g)
