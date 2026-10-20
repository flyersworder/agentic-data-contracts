"""Experiment 6: does progressive disclosure pay as the contract grows?

Reproduces FINDINGS.md, *Disclosure at scale: predictions (37)-(40)* (design:
docs/superpowers/specs/2026-10-20-experiment-6-disclosure-scale-design.md).
Experiment 4's three databases and 60 questions; each base contract diluted
with other databases' contracts at five levels (1x, 2x, 4x, 8x, all). The
governed arm (`contract_scaled_<level>`) and the same merged contract's files
in the prompt (`contract_text_scaled_<level>`) at every level that fits;
the prose (`manual_scaled`) at 1x and the common top; the position control
(`contract_text_first`) at the common top. gpt-6-luna, gpt-6-sol and
qwen3.8-27b, k=3. A run that is not `correct` -- an overflow, a truncated
prompt, a turn limit included -- counts as wrong.

The common top is the largest level at which both contract arms fit every
model (contracts-scale/fit.json). On it, tasks scored 0-9 pooled, one-sided
sign tests, ties dropped, the direction on each model:

  (37)  contract_text_scaled at the common top is below itself at 1x.
        Equivalent if the 95% interval of the mean drop (bootstrap over
        tasks) lies within +/-5 points.
  (38)  Per task, contract_scaled's drop from 1x is smaller than
        contract_text_scaled's.
  (38b) contract_scaled at the common top is below itself at 1x.
  (39)  At the common top contract_scaled costs fewer dollars per run than
        contract_text_scaled, on gpt-6-luna and on gpt-6-sol, each by its
        own sign test over tasks' mean cost.
  (40)  Wherever the text arm does not fit a model, contract_scaled's
        accuracy minus its 1x accuracy on that model has a 95% lower bound
        above -5 points.

Reported: every arm-level's accuracy, turns, input (fresh and cached),
dollars per run and per correct answer, and its cost at one fixed price,
fully cached and uncached; the cache-hit rate; the prose and the position
control against the contract arms.

Run:  uv run python analysis/disclosure_scale.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
from scipy.stats import binomtest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import procedures_gap as pg  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results" / "scale"
FIT = ROOT / "contracts-scale" / "fit.json"
FILES = {
    "gpt-6-luna": "exp6-luna-r{}.jsonl",
    "gpt-6-sol": "exp6-sol-r{}.jsonl",
    "qwen3.8-27b": "exp6-qwen-r{}.jsonl",
}
LEVELS = ("1x", "2x", "4x", "8x", "all")
#: The arms whose fit sets the common top.
KINDS = ("contract_scaled", "contract_text_scaled")
COST_MODELS = ("gpt-6-luna", "gpt-6-sol")
BOUND = 0.05
RESAMPLES = 10_000
SEED = 2026
#: One price for every arm and model, USD per million tokens (gpt-6-sol's
#: list prices), so a cost comparison does not rest on how a route bills.
FIXED_IN, FIXED_CACHED, FIXED_OUT = 2.00, 0.20, 10.00

Score = pg.Score


def _fits(fit: dict, model: str, arm: str) -> bool:
    return bool(fit.get(model, {}).get(arm, {}).get("fits"))


def common_top(fit: dict) -> str:
    return [
        lv
        for lv in LEVELS
        if all(_fits(fit, m, f"{k}_{lv}") for m in fit for k in KINDS)
    ][-1]


def own_top(fit: dict, model: str) -> str:
    return [lv for lv in LEVELS if _fits(fit, model, f"contract_text_scaled_{lv}")][-1]


def _one_sided(up: int, down: int) -> float:
    return (
        binomtest(up, up + down, 0.5, alternative="greater").pvalue
        if up + down
        else 1.0
    )


def drop(by_model: dict[str, Score], kind: str, top: str, tasks: list[str]) -> dict:
    """`kind` at 1x beats `kind` at `top`: (37) and (38b)."""
    a, b = f"{kind}_1x", f"{kind}_{top}"
    pooled = pg.pool(by_model)
    up = sum(pooled[a][t] > pooled[b][t] for t in tasks)
    down = sum(pooled[a][t] < pooled[b][t] for t in tasks)
    p = _one_sided(up, down)
    models = {m: pg._gap(s, a, b, tasks) / 3 for m, s in by_model.items()}
    holds = p < 0.05 and all(g > 0 for g in models.values())
    return {"up": up, "down": down, "p": p, "models": models, "holds": holds}


def smaller_drop(by_model: dict[str, Score], top: str, tasks: list[str]) -> dict:
    """(38): per task, the governed arm's drop from 1x is smaller than the
    text arm's."""

    def diff(s: Score, t: str) -> int:
        text = s["contract_text_scaled_1x"][t] - s[f"contract_text_scaled_{top}"][t]
        gov = s["contract_scaled_1x"][t] - s[f"contract_scaled_{top}"][t]
        return text - gov

    pooled = pg.pool(by_model)
    up = sum(diff(pooled, t) > 0 for t in tasks)
    down = sum(diff(pooled, t) < 0 for t in tasks)
    p = _one_sided(up, down)
    models = {
        m: sum(diff(s, t) for t in tasks) / (3 * len(tasks))
        for m, s in by_model.items()
    }
    holds = p < 0.05 and all(g > 0 for g in models.values())
    return {"up": up, "down": down, "p": p, "models": models, "holds": holds}


def interval(
    score: Score, a: str, b: str, tasks: list[str], *, runs: int, n: int = RESAMPLES
) -> tuple[float, float]:
    """95% bootstrap interval over tasks of the mean of (a - b), as a
    fraction of runs."""
    d = np.array([(score[a][t] - score[b][t]) / runs for t in tasks])
    rng = np.random.default_rng(SEED)
    means = d[rng.integers(0, len(d), (n, len(d)))].mean(axis=1)
    lo, hi = np.percentile(means, [2.5, 97.5])
    return float(lo), float(hi)


def equivalent(lo: float, hi: float, bound: float = BOUND) -> bool:
    return -bound <= lo and hi <= bound


def holds_up(score: Score, level: str, tasks: list[str], *, runs: int) -> bool:
    """(40): the governed arm at `level` minus at 1x has a 95% lower bound
    above -5 points."""
    lo, _ = interval(
        score, f"contract_scaled_{level}", "contract_scaled_1x", tasks, runs=runs
    )
    return lo > -BOUND


def reading(*, p37: bool, eq37: bool, p38: bool, p38b: bool) -> str:
    """The outcome's reading, as fixed in the run commit."""
    if not p37:
        if eq37:
            return (
                "(37) equivalent: up to the common top, the whole contract in the "
                "prompt costs no accuracy; disclosure's case is cost and size."
            )
        return "(37) inconclusive at this power."
    if p38:
        return (
            "(37) and (38) hold: disclosure keeps accuracy where the prompt loses it."
        )
    if p38b:
        return (
            "(37) holds, (38) fails, (38b) holds: size hurts both deliveries; "
            "the index and lookups do not isolate the relevant domain."
        )
    return (
        "(37) holds, (38) and (38b) fail: inconclusive between disclosure "
        "holding and size hurting both."
    )


def _fixed_usd(run: dict, cached: bool) -> float:
    inp = run["fresh"] + run["cached"]
    rate = FIXED_CACHED if cached else FIXED_IN
    return (inp * rate + run["output"] * FIXED_OUT) / 1e6


def main() -> None:
    fit = json.loads(FIT.read_text(encoding="utf-8"))
    by_model = {
        m: pg.scores(RESULTS, pattern)
        for m, pattern in FILES.items()
        if (RESULTS / pattern.format(1)).exists()
    }
    if not by_model:
        print("no agent results yet")
        return
    pooled = pg.pool(by_model)
    runs = 3 * len(by_model)
    tasks = sorted({t for s in pooled.values() for t in s})
    top = common_top(fit)
    print(
        f"== common top {top}; own tops "
        + ", ".join(f"{m} {own_top(fit, m)}" for m in by_model)
    )

    print("== accuracy, pooled and per model")
    arms = [a for lv in LEVELS for a in pooled if a.endswith(f"_{lv}")]
    for arm in arms:
        per = "  ".join(
            f"{m} {pg._mean(s, arm, tasks) / 3:.1%}"
            for m, s in by_model.items()
            if arm in s
        )
        print(f"   {arm:26s} {pg._mean(pooled, arm, tasks) / runs:6.1%}   {per}")

    r37 = drop(by_model, "contract_text_scaled", top, tasks)
    lo, hi = interval(
        pooled,
        "contract_text_scaled_1x",
        f"contract_text_scaled_{top}",
        tasks,
        runs=runs,
    )
    eq37 = equivalent(lo, hi)
    r38 = smaller_drop(by_model, top, tasks)
    r38b = drop(by_model, "contract_scaled", top, tasks)
    for label, r in (("(37)", r37), ("(38)", r38), ("(38b)", r38b)):
        print(
            f"{label} {'holds' if r['holds'] else 'fails'}: {r['up']} / {r['down']}, "
            f"one-sided p = {r['p']:.2g}; models "
            + ", ".join(f"{m} {g:+.1%}" for m, g in r["models"].items())
        )
    print(
        f"      (37) mean drop 95% interval [{lo:+.1%}, {hi:+.1%}]: equivalent {eq37}"
    )

    costs = {m: pg.run_costs(RESULTS, FILES[m]) for m in by_model}
    gov, text = f"contract_scaled_{top}", f"contract_text_scaled_{top}"
    held = []
    for m in COST_MODELS:
        if m not in costs:
            continue
        up, down, _ = pg.cheaper(costs[m], gov, text, tasks)
        p = _one_sided(up, down)
        held.append(p < 0.05)
        print(f"(39) {m}: {gov} cheaper on {up} / {down} tasks, one-sided p = {p:.2g}")
    print(f"(39) {'holds' if held and all(held) else 'fails'}")

    for m, s in by_model.items():
        for lv in LEVELS:
            if (
                _fits(fit, m, f"contract_text_scaled_{lv}")
                or f"contract_scaled_{lv}" not in s
            ):
                continue
            ok = holds_up(s, lv, tasks, runs=3)
            print(f"(40) {m} {lv}: {'holds' if ok else 'fails'}")

    print(
        "Reading: "
        + reading(p37=r37["holds"], eq37=eq37, p38=r38["holds"], p38b=r38b["holds"])
    )

    print(
        "== cost per run (mean): turns, input k (cache-hit), m$ route, "
        "m$/correct, m$ fixed cached / uncached"
    )
    for m, by_arm in costs.items():
        print(f"   {m}")
        for arm in arms:
            if arm not in by_arm:
                continue
            e = pg.efficiency(by_arm, tasks)[arm]
            rs = [r for t in tasks for r in by_arm[arm].get(t, [])]
            inp = e["fresh"] + e["cached"]
            hit = e["cached"] / inp if inp else 0.0
            fc = 1000 * sum(_fixed_usd(r, True) for r in rs) / len(rs)
            fu = 1000 * sum(_fixed_usd(r, False) for r in rs) / len(rs)
            pc = e["usd_per_correct"]
            print(
                f"      {arm:26s} {e['turns']:4.1f}  {inp / 1000:6.1f}k ({hit:4.0%})  "
                f"{1000 * e['usd']:7.2f}  "
                + ("-" if pc is None else f"{1000 * pc:7.2f}")
                + f"  {fc:7.2f} / {fu:7.2f}"
            )

    print("== reported, not predicted")
    for a, b in (
        (f"manual_scaled_{top}", f"contract_text_scaled_{top}"),
        (f"manual_scaled_{top}", f"contract_scaled_{top}"),
        ("manual_scaled_1x", "contract_scaled_1x"),
        (f"contract_text_first_{top}", f"contract_text_scaled_{top}"),
    ):
        if a in pooled and b in pooled:
            up, down, p = pg.sign(pooled, a, b, tasks, "two-sided")
            print(
                f"   {a} vs {b}: {pg._gap(pooled, a, b, tasks) / runs:+.1%}, "
                f"{up} / {down}, two-sided p = {p:.2g}"
            )


if __name__ == "__main__":
    main()
