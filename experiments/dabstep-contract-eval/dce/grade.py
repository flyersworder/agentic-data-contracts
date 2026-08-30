"""DABStep answer scoring.

Prefers the official scorer when installable so results stay comparable with
the published leaderboard; the fallback mirrors its documented behaviour.
"""

from __future__ import annotations

import re

_NA = {"", "n/a", "na", "none", "null", "not applicable"}
_BRACKETS = re.compile(r"^[\[\('\"]+|[\]\)'\"]+$")


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
    if "," in g:
        pl = sorted(_clean(x).lower() for x in p.split(",") if _clean(x))
        gl = sorted(_clean(x).lower() for x in g.split(",") if _clean(x))
        return pl == gl
    return _scalar_match(p, g)
