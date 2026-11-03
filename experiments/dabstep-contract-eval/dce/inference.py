"""The registered shape-level inference engine (experiment 5e's design,
*Inference (one engine, registered)*).

DABStep's 401 golded tasks expand about 113 question shapes
(`analysis/templates.py`), the 199 rule-matching ones 25 shapes of very
unequal size (five of 20 tasks, sixteen of 2-5). Tasks within a shape share
their failure modes, so a task-level test overstates the evidence. This
module is the one engine every paired contrast of the paper is read with:

- Unit: the task, scored as its mean over the repeats; a contrast's datum is
  a task's difference, a's mean minus b's (`task_differences`).
- Statistic: the task-weighted mean difference, the sum over shapes of each
  shape's summed task differences, divided by the number of tasks.
- Test: sign-flipping the per-shape sums, two-sided. All 2**m flips when the
  m non-zero shapes are few enough (`EXACT_MAX`), else `FLIPS` random flips
  with a fixed seed, drawn as antithetic pairs so the null is symmetric and a
  one-sided p is exactly half the two-sided one; a sampled p is
  (1 + hits) / (1 + flips). A shape whose sum is zero changes nothing.
- Intervals: by inverting the same test. Shift every task difference by
  delta, flip the shifted shape sums, and keep the deltas not rejected at
  10% and 5% (the 90% and 95% intervals). Found by bisection; clamped to the
  range of the task differences, since the mean cannot leave it.
- Beside, deciding nothing: the shape-clustered percentile bootstrap (with
  about 15 effective shapes it undercovers) and the task-level sign test.
- Verdicts against a 5-point bound (`BOUND`): non-superiority when the 90%
  interval's upper end is below +5, non-inferiority when its lower end is
  above -5; a registered tie reads *equivalent* (90% interval inside the
  bound), *different* (shape-level p < 0.05) or *inconclusive*.
- Holm over a family of p-values (`holm`); `bound_p` gives the one-sided p
  at a bound that a non-superiority or non-inferiority claim enters it with.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from functools import lru_cache

import numpy as np
from scipy.stats import binomtest

FLIPS = 100_000
SEED = 2026
#: Enumerate every flip when this many shapes or fewer are non-zero
#: (2**16 = 65,536 flips, fewer than the sampled 100,000).
EXACT_MAX = 16
RESAMPLES = 10_000
#: The registered bound, in fractions: 5 points.
BOUND = 0.05
#: How finely the inverted interval's ends are located.
TOLERANCE = 1e-7
_EPS = 1e-12


def task_differences(
    a: Mapping[str, int], b: Mapping[str, int], tasks: Iterable[str], *, runs: int
) -> dict[str, float]:
    """task -> a's correct runs minus b's, over `runs`: the difference of the
    two task means. A task an arm lacks counts as zero correct there."""
    return {t: (a.get(t, 0) - b.get(t, 0)) / runs for t in tasks}


def shape_sums(
    diffs: Mapping[str, float], shape_of: Mapping[str, str]
) -> tuple[np.ndarray, np.ndarray]:
    """Each shape's summed task differences and its task count, in a fixed
    order. A task without a shape is a cluster of its own."""
    sums: dict[str, float] = defaultdict(float)
    sizes: Counter[str] = Counter()
    for t, v in diffs.items():
        key = shape_of.get(t, f"solo:{t}")
        sums[key] += v
        sizes[key] += 1
    keys = sorted(sums)
    return (
        np.array([sums[k] for k in keys], dtype=float),
        np.array([sizes[k] for k in keys], dtype=float),
    )


@lru_cache(maxsize=64)
def _signs(m: int, flips: int, seed: int) -> np.ndarray:
    """(flips, m) random signs as antithetic pairs: each row and its
    negation, so the sampled null is symmetric."""
    rng = np.random.default_rng(seed)
    half = rng.integers(0, 2, (flips // 2, m)).astype(float) * 2 - 1
    return np.vstack([half, -half])


@lru_cache(maxsize=32)
def _all_signs(m: int) -> np.ndarray:
    codes = np.arange(2**m)[:, None] >> np.arange(m)
    return (codes & 1).astype(float) * 2 - 1


def flip_p(
    sums: np.ndarray,
    sizes: np.ndarray | None = None,
    *,
    n: int,
    side: str = "two-sided",
    flips: int = FLIPS,
    seed: int = SEED,
    exact_max: int = EXACT_MAX,
) -> tuple[float, bool]:
    """The sign-flip p of the statistic sum(sums) / n, and whether it was
    enumerated exactly. `side` is "two-sided", "less" (the observed value is
    low) or "greater". `sizes` is accepted so a `shape_sums` pair can be
    splatted in; the test does not use it."""
    del sizes
    s = np.asarray(sums, dtype=float)
    s = s[np.abs(s) > _EPS]
    if not len(s):
        return 1.0, True
    observed = s.sum() / n
    exact = len(s) <= exact_max
    signs = _all_signs(len(s)) if exact else _signs(len(s), flips, seed)
    null = signs @ s / n
    tol = 1e-9 * max(1.0, abs(observed))
    if side == "two-sided":
        hits = np.abs(null) >= abs(observed) - tol
    elif side == "less":
        hits = null <= observed + tol
    elif side == "greater":
        hits = null >= observed - tol
    else:
        raise ValueError(f"side must be two-sided, less or greater, got {side!r}")
    if exact:
        return float(hits.mean()), True
    return float((1 + hits.sum()) / (1 + len(null))), False


def _shifted_p(sums: np.ndarray, sizes: np.ndarray, n: int, delta: float) -> float:
    return flip_p(sums - delta * sizes, n=n)[0]


def _edge(
    sums: np.ndarray,
    sizes: np.ndarray,
    n: int,
    alpha: float,
    inside: float,
    limit: float,
) -> float:
    """The end of the set of deltas not rejected at `alpha`, between `inside`
    (not rejected) and `limit`; `limit` itself when it is not rejected."""
    if _shifted_p(sums, sizes, n, limit) >= alpha:
        return limit
    accept, reject = inside, limit
    while abs(reject - accept) > TOLERANCE:
        mid = (accept + reject) / 2
        if _shifted_p(sums, sizes, n, mid) >= alpha:
            accept = mid
        else:
            reject = mid
    return accept


def inverted_interval(
    diffs: Mapping[str, float], shape_of: Mapping[str, str], level: float
) -> tuple[float, float]:
    """The deltas the sign-flip test does not reject at 1 - `level`, shifting
    every task difference by delta."""
    sums, sizes = shape_sums(diffs, shape_of)
    n = len(diffs)
    est = sums.sum() / n
    values = list(diffs.values())
    alpha = 1 - level
    return (
        _edge(sums, sizes, n, alpha, est, min(values)),
        _edge(sums, sizes, n, alpha, est, max(values)),
    )


def shape_bootstrap(
    diffs: Mapping[str, float],
    shape_of: Mapping[str, str],
    level: float,
    *,
    resamples: int = RESAMPLES,
    seed: int = SEED,
) -> tuple[float, float]:
    """Percentile interval resampling whole shapes: each resample's estimate
    is its summed difference over its summed task count."""
    sums, sizes = shape_sums(diffs, shape_of)
    rng = np.random.default_rng(seed)
    pick = rng.integers(0, len(sums), (resamples, len(sums)))
    values = sums[pick].sum(axis=1) / sizes[pick].sum(axis=1)
    tail = 100 * (1 - level) / 2
    lo, hi = np.percentile(values, [tail, 100 - tail])
    return float(lo), float(hi)


def sign_test(diffs: Mapping[str, float]) -> tuple[int, int, float]:
    """Tasks a leads, tasks b leads, and the two-sided binomial p over them."""
    up = sum(v > _EPS for v in diffs.values())
    down = sum(v < -_EPS for v in diffs.values())
    p = binomtest(up, up + down, 0.5).pvalue if up + down else 1.0
    return up, down, float(p)


@dataclass(frozen=True)
class Contrast:
    """One paired contrast, read with the engine. Differences in fractions
    (0.05 is 5 points)."""

    n: int
    shapes: int
    nonzero: int
    estimate: float
    p: float
    exact: bool
    ci90: tuple[float, float]
    ci95: tuple[float, float]
    boot90: tuple[float, float]
    boot95: tuple[float, float]
    up: int
    down: int
    sign_p: float


def contrast(diffs: Mapping[str, float], shape_of: Mapping[str, str]) -> Contrast:
    """The registered reading of task differences `diffs` over shapes."""
    if not diffs:
        raise ValueError("a contrast needs at least one task")
    sums, sizes = shape_sums(diffs, shape_of)
    n = len(diffs)
    p, exact = flip_p(sums, n=n)
    up, down, sign_p = sign_test(diffs)
    return Contrast(
        n=n,
        shapes=len(sums),
        nonzero=int((np.abs(sums) > _EPS).sum()),
        estimate=float(sums.sum() / n),
        p=p,
        exact=exact,
        ci90=inverted_interval(diffs, shape_of, 0.90),
        ci95=inverted_interval(diffs, shape_of, 0.95),
        boot90=shape_bootstrap(diffs, shape_of, 0.90),
        boot95=shape_bootstrap(diffs, shape_of, 0.95),
        up=up,
        down=down,
        sign_p=sign_p,
    )


def bound_p(
    diffs: Mapping[str, float], shape_of: Mapping[str, str], bound: float, side: str
) -> float:
    """The one-sided sign-flip p against a mean of `bound`: "less" for
    non-superiority (H0: the mean is at least `bound`), "greater" for
    non-inferiority (H0: at most `bound`)."""
    sums, sizes = shape_sums(diffs, shape_of)
    return flip_p(sums - bound * sizes, n=len(diffs), side=side)[0]


def non_superior(c: Contrast, bound: float = BOUND) -> bool:
    """a does not lead b by `bound` or more: the 90% upper end is below it."""
    return c.ci90[1] < bound


def non_inferior(c: Contrast, bound: float = BOUND) -> bool:
    """a is not worse than b by `bound` or more: the 90% lower end is above
    minus it."""
    return c.ci90[0] > -bound


def reading(c: Contrast, bound: float = BOUND) -> str:
    """A registered tie, read under the bound."""
    if c.p < 0.05:
        return "different"
    if -bound < c.ci90[0] and c.ci90[1] < bound:
        return "equivalent"
    return "inconclusive"


def holm(ps: Mapping[str, float]) -> dict[str, float]:
    """Holm-adjusted p-values: step down from the smallest, each multiplied
    by the number not yet passed, kept monotone, capped at 1."""
    order = sorted(ps, key=lambda k: ps[k])
    m = len(order)
    out: dict[str, float] = {}
    running = 0.0
    for i, key in enumerate(order):
        running = max(running, min(1.0, (m - i) * ps[key]))
        out[key] = running
    return out


def points(x: float) -> str:
    """A fraction as signed points, one decimal."""
    return f"{100 * x:+.1f}"


def interval(ci: tuple[float, float]) -> str:
    lo, hi = ci
    return f"[{points(lo)}, {points(hi)}]"


def p_text(p: float) -> str:
    return f"{p:.2g}" if p >= 1e-3 or p == 0 else f"{p:.1e}"


def line(label: str, c: Contrast) -> str:
    """One contrast on one line: estimate, the 90% and 95% intervals by
    inversion, shape p, the bootstrap beside, task signs."""
    flag = "exact" if c.exact else "sampled"
    return (
        f"{label}: {points(c.estimate)} pts over {c.n} tasks; "
        f"90% {interval(c.ci90)}, 95% {interval(c.ci95)}; "
        f"shape p = {p_text(c.p)} ({c.nonzero}/{c.shapes} shapes non-zero, {flag}); "
        f"bootstrap 95% {interval(c.boot95)}; "
        f"tasks {c.up} / {c.down}, p = {p_text(c.sign_p)}"
    )


def effective_shapes(sizes: Iterable[float]) -> float:
    """Kish's effective number of clusters, (sum n)^2 / sum n^2."""
    s = [float(x) for x in sizes]
    return math.fsum(s) ** 2 / math.fsum(x * x for x in s) if s else 0.0
