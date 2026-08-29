"""The tolerance rule for comparing a measurement against a certified answer.

Its own module because two comparison modules need it — ``examples`` for a
scalar answer and ``_rows`` for a breakdown — and ``examples`` imports
``_rows``, so leaving it in ``examples`` would make that a cycle.
"""

from __future__ import annotations

import math


def _compare(
    actual: float, expected: float, rel_tol: float, abs_tol: float
) -> tuple[float, float, bool]:
    """Compare a measured value against a certified answer.

    Returns ``(abs_diff, rel_diff, matched)``.

    ``rel_diff`` is guarded against a zero reference — a certified answer of
    zero is legitimate, and dividing by it would raise or report a meaningless
    ``inf`` for an exact match. The three-branch form is the same one
    ``reconcile_decomposition`` uses for a zero parent.

    The relative term is anchored on ``expected``, deliberately unlike
    ``reconcile_decomposition`` (which compares two measurements and has no
    privileged side) and unlike ``math.isclose`` (which anchors on the larger
    magnitude). An assertion *has* a reference: the certified answer is the
    fixed point and the query result is what varies against it. Anchoring on
    ``expected`` keeps the tolerance's meaning stable — "within 0.1% of the
    certified number" — however far the query has drifted.
    """
    abs_diff = abs(actual - expected)
    if abs_diff == 0:
        rel_diff = 0.0
    elif expected != 0:
        rel_diff = abs_diff / abs(expected)
    else:
        rel_diff = math.inf
    matched = abs_diff <= max(abs_tol, rel_tol * abs(expected))
    return abs_diff, rel_diff, matched
