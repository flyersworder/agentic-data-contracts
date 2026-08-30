"""DABStep answer scoring.

Prefers the official scorer when installable so results stay comparable with
the published leaderboard; the fallback mirrors its documented behaviour.
"""

from __future__ import annotations

import re

_NA = {"", "n/a", "na", "none", "null", "not applicable"}
_BRACKETS = re.compile(r"^[\[\('\"]+|[\]\)'\"]+$")
_THOUSANDS = re.compile(r"^-?\d{1,3}(,\d{3})+(\.\d+)?$")


def _official(predicted: str, gold: str) -> bool | None:
    try:
        from dabstep_benchmark.evaluation.scorer import question_scorer
    except ImportError:
        return None
    return bool(question_scorer(predicted, gold))


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
