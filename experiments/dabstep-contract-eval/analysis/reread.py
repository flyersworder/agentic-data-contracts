"""The paper's committed DABStep contrasts, re-read with the registered engine.

No model call: every number comes from rows already in `results/`. Each
contrast is read on the 199 rule-matching tasks (both blind raters),
end-to-end strict, k=3, with `dce.inference`: the task-weighted mean
difference in points, the 90% and 95% intervals by inverting the
shape-level sign-flip test, the shape p, and the task-level sign counts.

- route (5), `contract` - `contract_text`, both models: (21) on luna is a
  registered tie, (27) on Qwen directional;
- text against prose (5b), `contract_text` - `manual_summed`: the ties (29)
  and (29Q);
- form (5c), `contract_text_noted` - `manual_summed`: the ties (30), (30Q);
- route with the notes in both (5d), `contract_noted` - `contract_text_noted`:
  the ties (41), (41Q);
- the notes lift (5d), `contract_noted` - `contract`: the tie (42) on luna,
  (42Q) directional on Qwen;
- the held-out lift of the second note, `manual_summed` - `manual_resolved`
  (experiment 5 and 5b's files, as `heldout_note.py` reads them), on the 165
  rule-matching tasks no transcript reading touched, and on the tasks of
  the shapes with no inspected task at all.

A registered tie reads *equivalent* (90% interval inside +/-5 points),
*different* (shape p < 0.05) or *inconclusive*; a directional prediction
reads whether its shape p is below 0.05. A sampled shape p of 1.0e-05 is
the floor of 100,000 flips: no flip was as extreme.

Then the corrupted working copies of every DABStep run of the paper's seven
models (`corruption.strata`), stratified by model: the rules stated (the
governed arms and the pasted contracts) against not stated, the governed
arms alone against not stated, and the pasted contracts (stated, not
enforced) against not stated. Each with the Mantel-Haenszel odds ratio, the
exact conditional test of a common odds ratio of 1 (two-sided, the
hypergeometric distribution of the first cells' sum given every stratum's
margins) and the Cochran-Mantel-Haenszel test beside.

Prints aggregates only; the rule-matching ratings are public
(`ratings/procedural-load`).

Run:  uv run python analysis/reread.py [--all]
"""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from scipy.stats import chi2, hypergeom

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dce import inference as inf  # noqa: E402

RUNS = 3

#: (events in x, runs in x, events in y, runs in y), one per stratum.
Stratum = tuple[int, int, int, int]


@dataclass(frozen=True)
class Row:
    """One contrast: a minus b, read from `files` on `among`."""

    label: str
    model: str
    files: tuple[str, ...]
    a: str
    b: str
    pred: str = ""
    kind: str = ""
    among: str = "rule-matching"

    @property
    def tie(self) -> str | None:
        return self.pred if self.kind == "tie" else None


_LUNA5 = ("exp5-luna-r{}.jsonl", "exp5b-luna-r{}.jsonl")
_QWEN5 = ("exp5-qwen-r{}.jsonl",)

ROWS: tuple[Row, ...] = (
    Row("route (5)", "luna", _LUNA5[:1], "contract", "contract_text", "(21)", "tie"),
    Row("route (5)", "qwen", _QWEN5, "contract", "contract_text", "(27)", "dir"),
    Row(
        "text vs prose (5b)",
        "luna",
        _LUNA5,
        "contract_text",
        "manual_summed",
        "(29)",
        "tie",
    ),
    Row(
        "text vs prose (5b)",
        "qwen",
        _QWEN5,
        "contract_text",
        "manual_summed",
        "(29Q)",
        "tie",
    ),
    *(
        Row(
            "form (5c)",
            m,
            (f"exp5c-{m}-r{{}}.jsonl",),
            "contract_text_noted",
            "manual_summed",
            pred,
            "tie",
        )
        for m, pred in (("luna", "(30)"), ("qwen", "(30Q)"))
    ),
    *(
        Row(
            "route, notes both (5d)",
            m,
            (f"exp5d-{m}-r{{}}.jsonl",),
            "contract_noted",
            "contract_text_noted",
            pred,
            "tie",
        )
        for m, pred in (("luna", "(41)"), ("qwen", "(41Q)"))
    ),
    *(
        Row(
            "notes lift (5d)",
            m,
            (f"exp5d-{m}-r{{}}.jsonl",),
            "contract_noted",
            "contract",
            pred,
            kind,
        )
        for m, pred, kind in (("luna", "(42)", "tie"), ("qwen", "(42Q)", "dir"))
    ),
    *(
        Row(label, m, files, "manual_summed", "manual_resolved", among=among)
        for m, files in (("luna", _LUNA5), ("qwen", _QWEN5))
        for label, among in (
            ("2nd note, unseen tasks", "unseen"),
            ("2nd note, unseen shapes", "unseen shapes"),
        )
    ),
)


def unseen_shapes(
    seen: set[str], among: set[str], shapes: Mapping[str, str]
) -> set[str]:
    """The tasks of `among` whose shape holds no inspected task."""
    touched = {shapes.get(t, f"solo:{t}") for t in seen}
    return {t for t in among if shapes.get(t, f"solo:{t}") not in touched}


def _reading(row: Row, c: inf.Contrast) -> str:
    if row.kind == "tie":
        return inf.reading(c)
    if row.kind == "dir":
        return "shape p < 0.05" if c.p < 0.05 else "shape p >= 0.05"
    return ""


def line(row: Row, c: inf.Contrast) -> str:
    return (
        f"{row.pred or '-':6s}{row.model:5s}{row.label:24s}{c.n:4d} "
        f"{inf.points(c.estimate):>6s}  90% {inf.interval(c.ci90):16s} "
        f"95% {inf.interval(c.ci95):16s} p={inf.p_text(c.p):8s}"
        f"{c.up}/{c.down}  {_reading(row, c)}"
    ).rstrip()


# ── Stratified corruption ───────────────────────────────────────────────────


def _usable(strata: Sequence[Stratum]) -> list[Stratum]:
    return [s for s in strata if s[1] > 0 and s[3] > 0]


def mantel_haenszel(strata: Sequence[Stratum]) -> float:
    """The Mantel-Haenszel common odds ratio of an event in x against y."""
    top = bottom = 0.0
    for ex, nx, ey, ny in _usable(strata):
        n = nx + ny
        top += ex * (ny - ey) / n
        bottom += (nx - ex) * ey / n
    return top / bottom if bottom else float("inf")


def _first_cell(stratum: Stratum) -> tuple[int, np.ndarray]:
    """The lowest possible first cell, and the hypergeometric probabilities
    from there up, given the stratum's margins."""
    ex, nx, ey, ny = stratum
    m = ex + ey
    lo, hi = max(0, m - ny), min(m, nx)
    return lo, hypergeom(nx + ny, m, nx).pmf(np.arange(lo, hi + 1))


def exact_conditional(strata: Sequence[Stratum]) -> float:
    """Two-sided exact conditional p of a common odds ratio of 1: the
    distribution of the summed first cells given every margin, summing the
    outcomes no more likely than the observed one (Fisher's rule)."""
    offset, dist = 0, np.array([1.0])
    for s in _usable(strata):
        lo, pmf = _first_cell(s)
        offset += lo
        dist = np.convolve(dist, pmf)
    observed = dist[sum(s[0] for s in _usable(strata)) - offset]
    return float(min(1.0, dist[dist <= observed * (1 + 1e-7)].sum()))


def cmh(strata: Sequence[Stratum]) -> float:
    """The Cochran-Mantel-Haenszel test with continuity correction."""
    dev = var = 0.0
    for ex, nx, ey, ny in _usable(strata):
        n, m = nx + ny, ex + ey
        dev += ex - nx * m / n
        var += nx * ny * m * (n - m) / (n * n * (n - 1)) if n > 1 else 0.0
    if not var:
        return 1.0
    return float(chi2.sf((abs(dev) - 0.5) ** 2 / var, 1))


#: label -> (the sides pooled as x, the sides pooled as y).
CORRUPTION = {
    "rules stated vs not": (("governed", "text"), ("unstated",)),
    "governed vs not stated": (("governed",), ("unstated",)),
    "pasted (stated, unenforced) vs not": (("text",), ("unstated",)),
}


def corruption_strata(runs, bad, x: tuple[str, ...], y: tuple[str, ...]):
    models = sorted({m for m, _ in runs})
    return [
        (
            sum(bad[(m, s)] for s in x),
            sum(runs[(m, s)] for s in x),
            sum(bad[(m, s)] for s in y),
            sum(runs[(m, s)] for s in y),
        )
        for m in models
    ]


# ── Main ────────────────────────────────────────────────────────────────────


def main() -> None:
    import argparse

    import corruption
    import heldout_note as hn
    import procedural_load as pl
    from contract_text import rule_matching
    from templates import shape_of

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--all", action="store_true", help="also every contrast on all 401 tasks"
    )
    args = parser.parse_args()
    shapes = shape_of()
    first = pl.read_ratings(pl.PUBLIC_RATINGS, "r1")["dabstep"]
    second = pl.read_ratings(pl.PUBLIC_RATINGS, "r2")["dabstep"]
    rated = rule_matching(first, second)
    luna5 = hn.RUNS["gpt-6-luna"]["exp5"]
    rm = rated & {
        str(r["task_id"]) for r in hn._rows(luna5, 1) if r["arm"] == "contract"
    }
    seen = hn.inspected(hn._rows(luna5, 1), hn._rows(luna5, 2), among=rm)
    sets = {
        "rule-matching": rm,
        "unseen": rm - seen,
        "unseen shapes": unseen_shapes(seen, rm, shapes),
    }
    n_shapes = len({shapes[t] for t in rm})
    n_free = len({shapes[t] for t in sets["unseen shapes"]})
    print(
        f"== rule-matching {len(rm)} tasks in {n_shapes} shapes; {len(seen)} "
        f"inspected; {len(sets['unseen'])} unseen; {len(sets['unseen shapes'])} "
        f"in the {n_free} shapes with no inspected task. End-to-end, k=3; "
        "points; intervals by inversion"
    )
    cache: dict[tuple[str, ...], dict] = {}
    for row in ROWS:
        if row.files not in cache:
            cache[row.files] = hn._score(row.files)
        score = cache[row.files]
        tasks = set(score[row.a]) | set(score[row.b])
        for among in (
            (row.among, "all")
            if args.all and row.among == "rule-matching"
            else (row.among,)
        ):
            chosen = tasks if among == "all" else sets[among] & tasks
            d = inf.task_differences(
                score[row.a], score[row.b], sorted(chosen), runs=RUNS
            )
            print(line(row, inf.contrast(d, shapes)))

    runs, bad = corruption.strata(ROOT / "results")
    print(
        "== corrupted copies, the paper's seven models, stratified by model: "
        "x events/runs vs y events/runs, MH odds ratio, exact conditional p, CMH p"
    )
    for label, (x, y) in CORRUPTION.items():
        strata = corruption_strata(runs, bad, x, y)
        use = _usable(strata)
        ex, nx = sum(s[0] for s in use), sum(s[1] for s in use)
        ey, ny = sum(s[2] for s in use), sum(s[3] for s in use)
        exact, approx = exact_conditional(strata), cmh(strata)
        print(
            f"   {label:36s} {ex}/{nx} vs {ey}/{ny} over {len(use)} models; "
            f"OR {mantel_haenszel(strata):.2f}; exact p = {inf.p_text(exact)}; "
            f"CMH p = {inf.p_text(approx)}"
        )


if __name__ == "__main__":
    main()
