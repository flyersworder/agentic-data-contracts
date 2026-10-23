"""Does the KB determine the gold, and is that where agents fail?

Two raters, blind to every run and contract, read each of experiment 2's 55
primary tasks (question, gold SQL and result, the KB entries the gold uses)
and marked whether the question and KB settle every choice that changes the
gold result (`kb_clarity`, `decisive`). The analysis plan was fixed before
any rating was read (FINDINGS.md, *Does the KB determine the gold?*):

  determined    both raters: clear, or nothing open changes the result
  undetermined  both raters: something open changes the result
  split         the raters disagree (reported, not tested)

Test: accuracy over all twelve experiment 2 runs of a task (four arms, three
repeats) is higher on determined than on undetermined tasks -- one-sided
Mann-Whitney U, and Fisher's exact test on "ever solved"; overall and within
the hard (29) and the other (26) tasks. Agreement: Cohen's kappa on
settled / not settled.

The same test over all 302 primary tasks (`--all`): the other 247 were rated
the same way (two raters each, plan extended before any of them was read),
and the outcome is the main runs -- gpt-6-luna and qwen3.8-27b, three
repeats, the three knowledge arms (18 runs a task). Confound check: if
undetermined tasks were merely harder, the KB's own gain (knowledge arms
minus schema_only, per task) would not depend on the group.

Prints aggregates only. Inputs (`$LSB_AUDIT`, default `ratings/lsb-determinacy`,
the public labels `export_ratings.py` cuts from the private
~/data/dce-held-results/failure-audit, which gives the same output): the
raters' labels, the key from anonymous id to task, and the hard set's ids.

Run:  uv run python analysis/lsb_kb_clarity.py [--all]
"""

from __future__ import annotations

import json
import os
import sys
from collections import Counter
from pathlib import Path

from scipy.stats import fisher_exact, mannwhitneyu

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dce.stats import load  # noqa: E402

AUDIT = Path(os.environ.get("LSB_AUDIT", ROOT / "ratings/lsb-determinacy"))
ARMS = ("manual_prompt", "contract", "contract_opus", "contract_authored")


def settled(rating: dict) -> bool:
    return rating["kb_clarity"] == "clear" or not rating["decisive"]


def consensus(a: dict, b: dict) -> str:
    if settled(a) and settled(b):
        return "determined"
    if not settled(a) and not settled(b):
        return "undetermined"
    return "split"


def kappa(a: list[bool], b: list[bool]) -> float:
    n = len(a)
    observed = sum(x == y for x, y in zip(a, b, strict=True)) / n
    pa, pb = sum(a) / n, sum(b) / n
    expected = pa * pb + (1 - pa) * (1 - pb)
    return 1.0 if expected == 1 else (observed - expected) / (1 - expected)


def _ratings(name: str) -> dict[str, dict]:
    path = AUDIT / f"blind-rater-{name}.jsonl"
    return {
        r["id"]: r
        for r in map(json.loads, path.read_text(encoding="utf-8").splitlines())
    }


def _test(acc: dict[str, float], group: dict[str, str], tasks: set[str], label: str):
    det = [acc[t] for t in tasks if group[t] == "determined"]
    und = [acc[t] for t in tasks if group[t] == "undetermined"]
    split = [acc[t] for t in tasks if group[t] == "split"]

    def mean(xs):
        return f"{sum(xs) / len(xs):.0%}" if xs else "-"

    line = (
        f"{label:14s} determined {len(det):2d} tasks {mean(det):>4s}"
        f" (ever solved {sum(x > 0 for x in det)})"
        f" | undetermined {len(und):2d} {mean(und):>4s}"
        f" (ever solved {sum(x > 0 for x in und)})"
        f" | split {len(split)} {mean(split)}"
    )
    if det and und:
        mw = mannwhitneyu(det, und, alternative="greater").pvalue
        ed, eu = sum(x > 0 for x in det), sum(x > 0 for x in und)
        fp = fisher_exact(
            [[ed, len(det) - ed], [eu, len(und) - eu]], alternative="greater"
        )[1]
        line += f" | Mann-Whitney p={mw:.2g}, Fisher p={fp:.2g}"
    print(line)


def main() -> None:
    key = json.loads((AUDIT / "blind-key.json").read_text(encoding="utf-8"))
    a, b = _ratings("A"), _ratings("B")
    group = {t: consensus(a[i], b[i]) for i, t in key.items()}
    ids = sorted(key)
    print(f"== {len(ids)} tasks, two blind raters")
    for name, r in (("A", a), ("B", b)):
        print(
            f"rater {name}: "
            + ", ".join(
                f"{k} {v}"
                for k, v in Counter(x["kb_clarity"] for x in r.values()).most_common()
            )
            + f"; decisive {sum(x['decisive'] for x in r.values())}"
        )
    same4 = sum(a[i]["kb_clarity"] == b[i]["kb_clarity"] for i in ids) / len(ids)
    k = kappa([settled(a[i]) for i in ids], [settled(b[i]) for i in ids])
    print(f"agreement: four values {same4:.0%}; settled or not, kappa {k:.2f}")
    print(
        "groups: " + ", ".join(f"{g} {n}" for g, n in Counter(group.values()).items())
    )

    score: dict[str, Counter[str]] = {arm: Counter() for arm in ARMS}
    for i in (1, 2, 3):
        for r in load(ROOT / f"results/livesqlbench/lsb-exp2-luna-r{i}.jsonl"):
            if r["subset"] == "primary":
                score[r["arm"]][r["task_id"]] += r["verdict"] == "correct"
    acc = {t: sum(score[arm][t] for arm in ARMS) / 12 for t in group}
    index = json.loads((AUDIT / "dossiers/index.json").read_text(encoding="utf-8"))
    hard = {row[0] for row in index}
    tasks = set(group)
    _test(acc, group, tasks, "all")
    _test(acc, group, tasks & hard, "hard")
    _test(acc, group, tasks - hard, "other")
    for arm in ARMS:
        per = {
            g: [score[arm][t] / 3 for t in tasks if group[t] == g]
            for g in ("determined", "undetermined")
        }
        print(
            f"  {arm:18s} "
            + "  ".join(f"{g} {sum(v) / len(v):.0%}" for g, v in per.items())
        )


MAIN_RUNS = {"gpt-6-luna": "luna-r{}", "qwen3.8-27b": "qwen38-r{}"}
KNOWLEDGE_ARMS = ("manual_prompt", "manual_compiled", "contract")


def all_ratings() -> tuple[dict[str, dict], dict[str, dict]]:
    """task -> rating, for each of the two rater roles, over all 302."""
    first, second = {}, {}
    key = json.loads((AUDIT / "blind-key.json").read_text(encoding="utf-8"))
    a, b = _ratings("A"), _ratings("B")
    for i, t in key.items():
        first[t], second[t] = a[i], b[i]
    key = json.loads((AUDIT / "blind302-key.json").read_text(encoding="utf-8"))
    for role, out in (("r1", first), ("r2", second)):
        for path in sorted(AUDIT.glob(f"blind302-{role}-c*.jsonl")):
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    r = json.loads(line)
                    out[key[r["id"]]] = r
    return first, second


def main_runs() -> tuple[dict[str, dict[str, Counter[str]]], dict[str, str]]:
    """The main runs' primary tasks: model -> arm -> task -> correct runs over
    the three repeats; and task -> its database."""
    score: dict[str, dict[str, Counter[str]]] = {}
    db_of: dict[str, str] = {}
    for model, pattern in MAIN_RUNS.items():
        s: dict[str, Counter[str]] = {}
        for i in (1, 2, 3):
            path = ROOT / "results/livesqlbench" / (pattern.format(i) + ".jsonl")
            for r in load(path):
                if r["subset"] == "primary":
                    s.setdefault(r["arm"], Counter())[r["task_id"]] += (
                        r["verdict"] == "correct"
                    )
                    db_of[r["task_id"]] = r["db"]
        score[model] = s
    return score, db_of


def main_all() -> None:
    first, second = all_ratings()
    tasks = sorted(set(first) & set(second))
    group = {t: consensus(first[t], second[t]) for t in tasks}
    print(f"== {len(tasks)} primary tasks, two blind raters each")
    for name, r in (("first", first), ("second", second)):
        print(
            f"{name}: "
            + ", ".join(
                f"{k} {v}"
                for k, v in Counter(r[t]["kb_clarity"] for t in tasks).most_common()
            )
        )
    same4 = sum(first[t]["kb_clarity"] == second[t]["kb_clarity"] for t in tasks)
    k = kappa([settled(first[t]) for t in tasks], [settled(second[t]) for t in tasks])
    print(
        f"agreement: four values {same4 / len(tasks):.0%}; "
        f"settled or not, kappa {k:.2f}"
    )
    print(
        "groups: " + ", ".join(f"{g} {n}" for g, n in Counter(group.values()).items())
    )
    score, _ = main_runs()
    know = {
        t: sum(score[m][a][t] for m in MAIN_RUNS for a in KNOWLEDGE_ARMS) / 18
        for t in tasks
    }
    base = {t: sum(score[m]["schema_only"][t] for m in MAIN_RUNS) / 6 for t in tasks}
    gain = {t: know[t] - base[t] for t in tasks}
    _test(know, group, set(tasks), "knowledge arms")
    for model in MAIN_RUNS:
        acc = {t: sum(score[model][a][t] for a in KNOWLEDGE_ARMS) / 9 for t in tasks}
        _test(acc, group, set(tasks), model)
    det = [t for t in tasks if group[t] == "determined"]
    und = [t for t in tasks if group[t] == "undetermined"]
    p = mannwhitneyu(
        [gain[t] for t in det], [gain[t] for t in und], alternative="greater"
    ).pvalue
    mean = lambda d, ts: sum(d[t] for t in ts) / len(ts)  # noqa: E731
    print(
        f"confound check: schema_only {mean(base, det):.0%} determined, "
        f"{mean(base, und):.0%} undetermined; KB gain {mean(gain, det):+.0%} "
        f"against {mean(gain, und):+.0%}, Mann-Whitney p={p:.2g}"
    )
    for model in MAIN_RUNS:
        for g, ts in (("determined", set(det)), ("undetermined", set(und))):
            better = sum(
                score[model]["contract"][t] > score[model]["manual_prompt"][t]
                for t in ts
            )
            worse = sum(
                score[model]["contract"][t] < score[model]["manual_prompt"][t]
                for t in ts
            )
            print(f"  {model} {g}: contract vs manual_prompt {better} / {worse}")


if __name__ == "__main__":
    main_all() if "--all" in sys.argv[1:] else main()
