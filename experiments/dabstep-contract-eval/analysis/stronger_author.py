"""A stronger author: experiments 1 (DABStep) and 2 (LiveSQLBench).

Reproduces FINDINGS.md, *A stronger compiler on DABStep: predictions (7)-(9)*
and *A better-made contract on LiveSQLBench: predictions (10)-(12)*.
Experiment 1 ran at the run commit 3f60a9c, experiment 2 at 3e0915a; each
holds its arms in one file per repeat, paired within it.

For each experiment: correct runs per arm and repeat, each task scored 0-3
over the repeats, the declared sign tests (ties dropped; one-sided where the
prediction names a direction), and the split by task group -- DABStep's
rule-set / total-fee / rest families, LiveSQLBench's database.

Run:  uv run python analysis/stronger_author.py
"""

from __future__ import annotations

import sys
from collections import Counter, defaultdict
from pathlib import Path

from scipy.stats import binomtest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dce.stats import load  # noqa: E402

REPEATS = (1, 2, 3)


def _dabstep_group(row: dict) -> str:
    from knowledge_delivery import dabstep_group

    return dabstep_group(row)


EXPERIMENTS = {
    "dabstep": {
        "files": lambda i: f"dabstep-opus-luna-r{i}.jsonl",
        "subset": None,
        "arms": (
            "contract_compiled",
            "contract_compiled_opus",
            "contract_compiled_opus_joins",
            "contract",
        ),
        "group": _dabstep_group,
        # (arm, against, alternative): predictions (7), (8), (9), then (8)'s
        # second clause.
        "tests": (
            ("contract_compiled_opus_joins", "contract_compiled", "greater"),
            ("contract_compiled_opus_joins", "contract_compiled_opus", "greater"),
            ("contract", "contract_compiled_opus_joins", "greater"),
            ("contract_compiled_opus", "contract_compiled", "two-sided"),
        ),
    },
    "livesqlbench": {
        "files": lambda i: f"livesqlbench/lsb-exp2-luna-r{i}.jsonl",
        "subset": "primary",
        "arms": ("manual_prompt", "contract", "contract_opus", "contract_authored"),
        "group": lambda row: row["db"],
        # Predictions (10) and (11), then the pairs (12) orders.
        "tests": (
            ("contract", "manual_prompt", "two-sided"),
            ("contract_authored", "contract", "greater"),
            ("contract_opus", "contract", "two-sided"),
            ("contract_authored", "contract_opus", "two-sided"),
        ),
    },
}


def task_scores(
    repeats: list[list[dict]], subset: str | None
) -> dict[str, Counter[str]]:
    """arm -> task -> correct runs over the repeats, within `subset`."""
    score: dict[str, Counter[str]] = defaultdict(Counter)
    for rows in repeats:
        for r in rows:
            if subset is None or r["subset"] == subset:
                score[r["arm"]][r["task_id"]] += r["verdict"] == "correct"
    return score


def sign(
    score, a: str, b: str, tasks: set[str], alternative: str = "greater"
) -> tuple[int, int, float]:
    """Tasks where `a` scores above `b`, below it, and the binomial p."""
    better = sum(score[a][t] > score[b][t] for t in tasks)
    worse = sum(score[a][t] < score[b][t] for t in tasks)
    if not better + worse:
        return better, worse, 1.0
    p = binomtest(better, better + worse, 0.5, alternative=alternative).pvalue
    return better, worse, float(p)


def report(name: str) -> None:
    exp = EXPERIMENTS[name]
    repeats = [load(ROOT / "results" / exp["files"](i)) for i in REPEATS]
    subset, arms = exp["subset"], exp["arms"]
    score = task_scores(repeats, subset)
    tasks = set(score[arms[0]])
    runs = len(tasks) * len(REPEATS)
    print(f"== {name}: {len(tasks)} tasks" + (f" ({subset})" if subset else ""))
    for a in arms:
        per = [
            sum(
                r["arm"] == a
                and r["verdict"] == "correct"
                and (subset is None or r["subset"] == subset)
                for r in rows
            )
            for rows in repeats
        ]
        print(f"  {a:30s} {per}  {sum(per) / runs:.1%}")
    for a, b, alt in exp["tests"]:
        better, worse, p = sign(score, a, b, tasks, alt)
        print(f"  sign {a} vs {b}: {better} / {worse}, p={p:.2g} ({alt})")
    group_of = {
        r["task_id"]: exp["group"](r)
        for rows in repeats
        for r in rows
        if r["task_id"] in tasks
    }
    for g in sorted(set(group_of.values())):
        among = {t for t in tasks if group_of[t] == g}
        n = len(among) * len(REPEATS)
        print(
            f"  {g:16s} {len(among):3d} tasks  "
            + "  ".join(f"{a} {sum(score[a][t] for t in among) / n:.1%}" for a in arms)
        )
    print()


if __name__ == "__main__":
    for experiment in EXPERIMENTS:
        report(experiment)
