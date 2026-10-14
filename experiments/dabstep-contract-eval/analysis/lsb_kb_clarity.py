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

Prints aggregates only. Inputs outside the repository (`$LSB_AUDIT`, default
~/data/dce-held-results/failure-audit): the two raters' files, the key from
anonymous id to task, and the hard set's dossier index.

Run:  uv run python analysis/lsb_kb_clarity.py
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

AUDIT = Path(
    os.environ.get("LSB_AUDIT", Path.home() / "data/dce-held-results/failure-audit")
)
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


if __name__ == "__main__":
    main()
