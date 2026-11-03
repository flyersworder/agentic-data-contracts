"""Experiment 7: the registered analysis of the agent runs on DataAgentBench.

Design: docs/superpowers/specs/2026-11-01-experiment-7-dataagentbench-design.md
("Arms", "Models, runs, measure", "Pilot gate", "Predictions"). Five arms
together in each repeat, k=5, on gpt-6-luna and qwen3.8-27b, the 40 phase-1
queries in 9 datasets: `dab_desc`, `dab_hints`, `dab_contract`,
`dab_contract_text`, `dab_hints_contract`. Primary measure: each query's own
`validate.py` (the row's `verdict`); a harness failure counts as a fail.

Everything is read from the results files, `exp7-<model>-r<i>.jsonl`; within a
file the last row per (task, arm, model) wins (`dce.stats.load`), so each
file is one repeat. No question, gold or validator is opened: the rows carry
the grader's `overlong` flag (`dce.benchmarks.dab_grade`), and the gold is on
the row only as a digest.

Accuracy per arm is pooled over queries x repeats; the leaderboard's form (the
mean over datasets of each dataset's pass rate) is printed beside it, never
tested. Registered, per model, over the tested queries (all but the datasets
given to `--exclude`, which the pilot gate's floor rule records; they are
still reported):

  (44) `dab_hints` > `dab_desc`;
  (45) `dab_contract` > `dab_desc`;
  (48) `dab_hints_contract` > `dab_hints`:
       one-sided task-level sign test over the queries on each query's pass
       rate over the repeats (ties dropped), p < 0.05, AND the direction
       holds on a strict majority of the datasets where the two arms'
       pooled pass rates differ. The direction per dataset is printed.
  (46) the recovered share (`dab_contract` - `dab_desc`) / (`dab_hints` -
       `dab_desc`), on pooled accuracy, with a 95% percentile bootstrap
       interval resampling datasets (fixed seed; a resample in which
       `dab_hints` does not beat `dab_desc` has no share and is left out,
       and counted). Undefined if the point's denominator is <= 0; else
       falsified if the upper bound < 0.5; else holds if the point >= 0.5;
       else (spanning 0.5, point below) inconclusive.
  (47) `dab_contract` and `dab_contract_text`, no detectable difference:
       within 5 points AND two-sided sign test p >= 0.10.

Every test is also reported leaving out one tested dataset at a time.
Readings: when (44) fails, (45)-(48) are printed but flagged as not read as
support.

Leniency check: an overlong pass is a pass whose answer is more than 200
characters longer than the gold string (the row's `overlong`). Overlong
passes are reported per arm, as a share of the arm's runs on the tested
queries. Where two arms a comparison reads differ by more than 5 points, that
comparison is also reported without the overlong passes (those rows removed
from every arm).

The offline predictions (43), (43c), (43w) are printed from
`contracts-dataagentbench/recovery.json` by `dab_recovery`, not recomputed.

Under missing results files the script names the missing repeats and runs on
what exists; with fewer than 5 repeats it prints estimates only, no verdict.

Run:  uv run python analysis/dataagentbench.py [--exclude DATASET ...]
          [--results DIR] [--recovery PATH] [--models MODEL ...]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from itertools import combinations
from pathlib import Path

import numpy as np
from scipy.stats import binomtest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import dab_recovery as dr  # noqa: E402
from dce.stats import _graded, load  # noqa: E402

ARMS = (
    "dab_desc",
    "dab_hints",
    "dab_contract",
    "dab_contract_text",
    "dab_hints_contract",
)
DESC, HINTS, CONTRACT, TEXT, BOTH = ARMS
MODELS = ("gpt-6-luna", "qwen3.8-27b")
PATTERN = "exp7-{model}-r{i}.jsonl"
REPEATS = (1, 2, 3, 4, 5)
RESULTS = ROOT / "results" / "dataagentbench"

#: label -> (a, b): a predicted to beat b.
DIRECTIONAL: dict[str, tuple[str, str]] = {
    "(44)": (HINTS, DESC),
    "(45)": (CONTRACT, DESC),
    "(48)": (BOTH, HINTS),
}
TIE = ("(47)", (CONTRACT, TEXT))
SHARE = "(46)"
#: The arms each comparison reads, for the leniency check.
COMPARISONS: dict[str, tuple[str, ...]] = {
    "(44)": (HINTS, DESC),
    "(45)": (CONTRACT, DESC),
    "(46)": (CONTRACT, HINTS, DESC),
    "(47)": (CONTRACT, TEXT),
    "(48)": (BOTH, HINTS),
}

ALPHA = 0.05
TIE_P = 0.10
TIE_POINTS = 0.05
SHARE_MIN = 0.5
OVERLONG_POINTS = 0.05
BOOTSTRAP = 10_000
SEED = 7
_EPS = 1e-9

#: arm -> task -> (passes, runs).
Tally = dict[str, dict[str, tuple[int, int]]]


def dataset_of(task: str) -> str:
    """`<dataset>/<n>` -> `<dataset>`."""
    return task.rsplit("/", 1)[0]


# -- reading the files ---------------------------------------------------------


def read_repeats(results: Path, model: str) -> tuple[list[int], list[list[dict]]]:
    """The repeats present for `model`, and each one's rows (last row per
    task, arm and model; ungolded rows out, as `dce.stats` counts)."""
    present, repeats = [], []
    for i in REPEATS:
        path = results / PATTERN.format(model=model, i=i)
        if path.is_file():
            present.append(i)
            repeats.append([r for r in _graded(load(path)) if r.get("model") == model])
    return present, repeats


def _overlong_pass(row: Mapping) -> bool:
    return row.get("verdict") == "correct" and bool(row.get("overlong"))


def tally(repeats: Iterable[Iterable[dict]], *, lenient: bool = False) -> Tally:
    """Passes and runs per arm and task over the repeats; `lenient` leaves
    the overlong passes out."""
    out: Tally = {arm: {} for arm in ARMS}
    for rows in repeats:
        for r in rows:
            arm, task = r.get("arm"), str(r["task_id"])
            if arm not in out or (lenient and _overlong_pass(r)):
                continue
            passes, runs = out[arm].get(task, (0, 0))
            out[arm][task] = (passes + (r.get("verdict") == "correct"), runs + 1)
    return out


def tested(tasks: Iterable[str], exclude: set[str] | frozenset[str]) -> list[str]:
    return [t for t in tasks if dataset_of(t) not in exclude]


def _by_dataset(tasks: Iterable[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = defaultdict(list)
    for t in tasks:
        out[dataset_of(t)].append(t)
    return dict(sorted(out.items()))


# -- accuracy ------------------------------------------------------------------


def _pooled(t: Tally, arm: str, tasks: Iterable[str]) -> tuple[int, int]:
    cells = [t[arm][x] for x in tasks if x in t[arm]]
    return sum(c[0] for c in cells), sum(c[1] for c in cells)


def accuracy(t: Tally, arm: str, tasks: Iterable[str]) -> float | None:
    """Passes over runs, pooled over `tasks` and the repeats."""
    passes, runs = _pooled(t, arm, tasks)
    return passes / runs if runs else None


def leaderboard(t: Tally, arm: str, tasks: Iterable[str]) -> float | None:
    """The leaderboard's form: the mean over datasets of each one's pass rate."""
    rates = [accuracy(t, arm, ts) for ts in _by_dataset(tasks).values()]
    known = [r for r in rates if r is not None]
    return sum(known) / len(known) if known else None


def _difference(t: Tally, a: str, b: str, tasks: list[str]) -> float | None:
    ra, rb = accuracy(t, a, tasks), accuracy(t, b, tasks)
    return None if ra is None or rb is None else ra - rb


# -- the tests ------------------------------------------------------------------


def _rate(t: Tally, arm: str, task: str) -> float | None:
    passes, runs = t[arm].get(task, (0, 0))
    return passes / runs if runs else None


def signs(t: Tally, a: str, b: str, tasks: Iterable[str]) -> tuple[int, int]:
    """Tasks where a's pass rate is above b's, and below it."""
    up = down = 0
    for x in tasks:
        ra, rb = _rate(t, a, x), _rate(t, b, x)
        if ra is None or rb is None:
            continue
        up += ra > rb + _EPS
        down += rb > ra + _EPS
    return up, down


def sign_p(up: int, down: int, alternative: str) -> float:
    n = up + down
    return float(binomtest(up, n, 0.5, alternative=alternative).pvalue) if n else 1.0


def directions(t: Tally, a: str, b: str, tasks: Iterable[str]) -> dict[str, float]:
    """Per dataset, a's pooled pass rate minus b's (0 where they are level)."""
    out = {}
    for d, ts in _by_dataset(tasks).items():
        diff = _difference(t, a, b, ts)
        if diff is not None:
            out[d] = 0.0 if abs(diff) < _EPS else diff
    return out


def directional(t: Tally, a: str, b: str, tasks: list[str]) -> dict:
    """(44), (45), (48): a beats b."""
    up, down = signs(t, a, b, tasks)
    p = sign_p(up, down, "greater")
    dirs = directions(t, a, b, tasks)
    differing = sum(v != 0 for v in dirs.values())
    agreeing = sum(v > 0 for v in dirs.values())
    return {
        "kind": "directional",
        "arms": (a, b),
        "difference": _difference(t, a, b, tasks),
        "up": up,
        "down": down,
        "p": p,
        "directions": dirs,
        "differing": differing,
        "agreeing": agreeing,
        "holds": p < ALPHA and 2 * agreeing > differing,
    }


def tie(t: Tally, a: str, b: str, tasks: list[str]) -> dict:
    """(47): within 5 points and a two-sided sign test p >= 0.10."""
    up, down = signs(t, a, b, tasks)
    p = sign_p(up, down, "two-sided")
    diff = _difference(t, a, b, tasks)
    return {
        "kind": "tie",
        "arms": (a, b),
        "difference": diff,
        "up": up,
        "down": down,
        "p": p,
        "directions": directions(t, a, b, tasks),
        "holds": diff is not None and abs(diff) <= TIE_POINTS + _EPS and p >= TIE_P,
    }


def _ratio(contract: float, hints: float, desc: float) -> float | None:
    gain = hints - desc
    return None if gain <= _EPS else (contract - desc) / gain


def share(t: Tally, tasks: list[str], *, seed: int = SEED) -> dict:
    """(46): the recovered share, with a bootstrap over datasets."""
    accs = [accuracy(t, arm, tasks) for arm in (CONTRACT, HINTS, DESC)]
    c, h, d = accs
    point = None if c is None or h is None or d is None else _ratio(c, h, d)
    groups = list(_by_dataset(tasks).values())
    lower = upper = None
    dropped = BOOTSTRAP
    if groups:
        cells = [
            [_pooled(t, arm, ts) for ts in groups] for arm in (CONTRACT, HINTS, DESC)
        ]
        passes = np.array([[c[0] for c in row] for row in cells], dtype=float)
        runs = np.array([[c[1] for c in row] for row in cells], dtype=float)
        idx = np.random.default_rng(seed).integers(
            0, len(groups), (BOOTSTRAP, len(groups))
        )
        with np.errstate(divide="ignore", invalid="ignore"):
            acc = passes[:, idx].sum(axis=2) / runs[:, idx].sum(axis=2)
        gain = acc[1] - acc[2]
        ok = gain > _EPS
        dropped = int(BOOTSTRAP - ok.sum())
        if ok.any():
            ratios = (acc[0][ok] - acc[2][ok]) / gain[ok]
            lower, upper = (float(x) for x in np.percentile(ratios, [2.5, 97.5]))
    if point is None:
        verdict = "undefined"
    elif upper is not None and upper < SHARE_MIN:
        verdict = "falsified"
    elif point >= SHARE_MIN - _EPS:
        verdict = "holds"
    else:
        verdict = "inconclusive"
    return {
        "kind": "share",
        "arms": (CONTRACT, HINTS, DESC),
        "accuracies": accs,
        "point": point,
        "lower": lower,
        "upper": upper,
        "dropped": dropped,
        "verdict": verdict,
        "holds": verdict == "holds",
    }


def evaluate(t: Tally, tasks: list[str]) -> dict[str, dict]:
    """(44)-(48) over `tasks`."""
    out = {label: directional(t, a, b, tasks) for label, (a, b) in DIRECTIONAL.items()}
    out[TIE[0]] = tie(t, *TIE[1], tasks)
    out[SHARE] = share(t, tasks)
    return dict(sorted(out.items()))


def leave_one_out(t: Tally, tasks: list[str]) -> dict[str, dict[str, dict]]:
    """dataset -> (44)-(48) over `tasks` without that dataset."""
    return {
        d: evaluate(t, [x for x in tasks if dataset_of(x) != d])
        for d in _by_dataset(tasks)
    }


# -- the leniency check -----------------------------------------------------------


def overlong_shares(
    repeats: Iterable[Iterable[dict]], tasks: Iterable[str]
) -> dict[str, float]:
    """arm -> overlong passes as a share of its runs on `tasks`."""
    among = set(tasks)
    runs: Counter[str] = Counter()
    over: Counter[str] = Counter()
    for rows in repeats:
        for r in rows:
            if r.get("arm") in ARMS and str(r["task_id"]) in among:
                runs[r["arm"]] += 1
                over[r["arm"]] += _overlong_pass(r)
    return {arm: over[arm] / runs[arm] for arm in ARMS if runs[arm]}


def unflagged(repeats: Iterable[Iterable[dict]]) -> int:
    """Passes whose row carries no `overlong` flag (read as not overlong)."""
    return sum(
        r.get("verdict") == "correct" and "overlong" not in r
        for rows in repeats
        for r in rows
    )


def lenient_labels(shares: Mapping[str, float]) -> list[str]:
    """The comparisons two of whose arms differ by more than 5 points in
    overlong passes."""
    return [
        label
        for label, arms in COMPARISONS.items()
        if any(
            abs(shares.get(a, 0.0) - shares.get(b, 0.0)) > OVERLONG_POINTS + _EPS
            for a, b in combinations(arms, 2)
        )
    ]


# -- the report -------------------------------------------------------------------


def _pct(x: float | None) -> str:
    return "  n/a" if x is None else f"{x:5.1%}"


def _pts(x: float | None) -> str:
    return "n/a" if x is None else f"{100 * x:+.1f}"


def _dirs(dirs: Mapping[str, float]) -> str:
    return ", ".join(f"{d} {_pts(v)}" for d, v in dirs.items())


def line(label: str, r: dict, complete: bool) -> str:
    """One prediction's result; `complete` False gives the estimate only."""
    if r["kind"] == "share":
        word = r["verdict"] if complete else "estimate"
        c, h, d = r["accuracies"]
        accs = f"(contract {_pct(c)}, hints {_pct(h)}, desc {_pct(d)})"
        if r["point"] is None:
            return (
                f"{label} {word}: no share, dab_hints does not beat dab_desc "
                f"({_pts(None if h is None or d is None else h - d)} pts) {accs}"
            )
        interval = (
            "no interval"
            if r["lower"] is None
            else f"95% [{r['lower']:.2f}, {r['upper']:.2f}]"
        )
        return (
            f"{label} {word}: recovered share {r['point']:.2f}, {interval} "
            f"resampling datasets, {r['dropped']} of {BOOTSTRAP} resamples with "
            f"no hints gain left out {accs}"
        )
    word = ("holds" if r["holds"] else "fails") if complete else "estimate"
    a, b = r["arms"]
    side = "one-sided" if r["kind"] == "directional" else "two-sided"
    text = (
        f"{label} {word}: {a} - {b} {_pts(r['difference'])} pts; tasks "
        f"{r['up']} up / {r['down']} down, {side} sign p = {r['p']:.4f}"
    )
    if r["kind"] == "directional":
        text += (
            f"; direction holds in {r['agreeing']} of {r['differing']} "
            "differing datasets"
        )
    return text + f" ({_dirs(r['directions'])})"


def offline(recovery: Path) -> None:
    """(43), (43c), (43w) as `dab_recovery` prints them."""
    print(f"== offline predictions, from {recovery}")
    if not recovery.is_file():
        print(f"   no recovery file at {recovery}")
        return
    rec = json.loads(recovery.read_text(encoding="utf-8"))
    for text in dr.summary(rec).splitlines():
        if text.startswith(("(43", "Described")):
            print(text)


def _accuracy_section(
    t: Tally, tasks: list[str], test_tasks: list[str], exclude: set[str]
) -> None:
    print(
        f"   accuracy, pooled over queries x repeats ({len(tasks)} queries; "
        f"{len(test_tasks)} tested); leaderboard form (mean over datasets) beside, "
        "never tested"
    )
    for arm in ARMS:
        runs = _pooled(t, arm, tasks)[1]
        print(
            f"   {arm:20s} all {_pct(accuracy(t, arm, tasks))}  tested "
            f"{_pct(accuracy(t, arm, test_tasks))}  leaderboard "
            f"{_pct(leaderboard(t, arm, tasks))}  ({runs} runs)"
        )
    print("   per dataset (queries), in the arm order above:")
    for d, ts in _by_dataset(tasks).items():
        tag = "  excluded from (44)-(48)" if d in exclude else ""
        print(
            f"   {d:18s} ({len(ts):2d}) "
            + "  ".join(_pct(accuracy(t, arm, ts)) for arm in ARMS)
            + tag
        )


def report_model(results: Path, model: str, exclude: set[str] | frozenset[str]) -> None:
    present, repeats = read_repeats(results, model)
    if not present:
        print(f"== {model}: no results ({PATTERN.format(model=model, i='*')})")
        return
    missing = [i for i in REPEATS if i not in present]
    complete = not missing
    note = (
        ""
        if complete
        else "; missing repeats "
        + ", ".join(f"r{i}" for i in missing)
        + ": estimates only, no verdicts"
    )
    print(f"== {model}: {len(present)} of {len(REPEATS)} repeats{note}")
    t = tally(repeats)
    tasks = sorted({x for per in t.values() for x in per})
    test_tasks = tested(tasks, set(exclude))
    absent = sorted(set(exclude) - {dataset_of(x) for x in tasks})
    if absent:
        print(f"   --exclude names datasets not in the results: {', '.join(absent)}")
    _accuracy_section(t, tasks, test_tasks, set(exclude))

    main = evaluate(t, test_tasks)
    print(f"   registered predictions over {len(test_tasks)} tested queries")
    for label, r in main.items():
        print("   " + line(label, r, complete))
    if complete and not main["(44)"]["holds"]:
        print(
            "   reading: (44) fails, so (45)-(48) are reported but not read as "
            "support; the floor rule and the leniency check are the first suspects"
        )
    for d, results_without in leave_one_out(t, test_tasks).items():
        print(f"   leaving out {d}:")
        for label, r in results_without.items():
            print("      " + line(label, r, complete))

    shares = overlong_shares(repeats, test_tasks)
    print(
        "   leniency: overlong passes as a share of runs, tested queries: "
        + ", ".join(f"{a} {_pct(s).strip()}" for a, s in shares.items())
    )
    blank = unflagged(repeats)
    if blank:
        print(f"   {blank} passes carry no overlong flag; read as not overlong")
    flagged = lenient_labels(shares)
    if flagged:
        strict = evaluate(tally(repeats, lenient=True), test_tasks)
        for label in flagged:
            print("   without overlong passes: " + line(label, strict[label], complete))


def report(
    results: Path = RESULTS,
    *,
    models: Iterable[str] = MODELS,
    exclude: set[str] | frozenset[str] = frozenset(),
    recovery: Path = dr.OUT,
) -> None:
    offline(recovery)
    for model in models:
        report_model(results, model, exclude)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Experiment 7: the agent runs.")
    ap.add_argument("--results", type=Path, default=RESULTS)
    ap.add_argument(
        "--exclude",
        nargs="*",
        default=[],
        metavar="DATASET",
        help="datasets the pilot gate's floor rule leaves out of (44)-(48)",
    )
    ap.add_argument("--recovery", type=Path, default=dr.OUT)
    ap.add_argument("--models", nargs="+", default=list(MODELS))
    args = ap.parse_args(argv)
    report(
        args.results,
        models=args.models,
        exclude=set(args.exclude),
        recovery=args.recovery,
    )


if __name__ == "__main__":
    main()
