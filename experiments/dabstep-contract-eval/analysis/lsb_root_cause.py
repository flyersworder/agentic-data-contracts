"""Why compiled knowledge ties the manual on LiveSQLBench: the five-group test.

Each primary task is put in one group by the KB entries its gold uses (the
benchmark's private task-to-KB mapping), the compile outcome of those entries,
and a blind review of the compiled metrics they became (verdicts made without
tasks, golds or results):

  A  every needed compiled metric was judged faithful
  B  a needed entry was refused by the compiler (decided first)
  C  a needed metric is wrong in a way that changes results on the data
  D  no needed entry became a compiled metric
  E  otherwise: a needed metric unclear, immaterially wrong, or not reviewed

The groups depend on the compile, the mapping and the review only, never on
a model's results, so the same grouping applies to every model.

Confirmatory statistic, fixed before the Qwen 3.8 data it is tested on:
per task, `compiled_gain` = mean correct runs of `contract` and
`manual_compiled` minus those of `manual_prompt` (three repeats); the
statistic is its mean over group A minus its mean over group B, tested
one-sided by permuting the A/B labels.

Prints aggregates only. Inputs outside the repository: the mapping
(`$LSB_PRIVATE`, default ~/data/livesqlbench) and the review verdicts
(`$LSB_REVIEW`, default ~/data/dce-held-results/metric-review).

Usage, from the experiment dir: lsb_root_cause.py MODEL   (e.g. gpt-6-luna)
"""

import collections
import gzip
import json
import os
import random
import sys
from collections.abc import Mapping
from pathlib import Path

from scipy.stats import binomtest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dce.stats import _as_e2e, _graded, load  # noqa: E402

PRIVATE = Path(os.environ.get("LSB_PRIVATE", Path.home() / "data/livesqlbench"))
REVIEW = Path(
    os.environ.get("LSB_REVIEW", Path.home() / "data/dce-held-results/metric-review")
)
MAPPING = "livesqlbench_base_full_v1_gt_kg_testcases_20260613.jsonl"
RUNS = {
    "gpt-6-luna": "livesqlbench/luna-r{}",
    "qwen3.8-27b": "livesqlbench/qwen38-r{}",
}
ARMS = ("schema_only", "manual_prompt", "manual_compiled", "contract")
GROUPS = {
    "A": "every needed metric faithful",
    "B": "a needed entry refused",
    "C": "a needed metric wrong (material)",
    "D": "no compiled metric needed",
    "E": "a needed metric unclear",
}
Score = Mapping[str, Mapping[str, int]]
Entries = dict[tuple[str, str], tuple[str, list[str]]]
PERMUTATIONS = 100_000
SEED = 2026


def verdict_map(rows: list[dict]) -> dict[tuple[str, str], str]:
    """(db, metric) -> faithful / wrong / unclear. An error the reviewer
    judged immaterial on the data counts as unclear."""
    out = {}
    for r in rows:
        if r["verdict"] == "wrong" and r["material"]:
            out[(r["db"], r["metric"])] = "wrong"
        elif r["verdict"] != "faithful":
            out[(r["db"], r["metric"])] = "unclear"
        else:
            out[(r["db"], r["metric"])] = "faithful"
    return out


def task_group(used: list[tuple[str, list[str]]], verdicts: dict[str, str]) -> str:
    """`used`: (compile status, translated metric names) of each KB entry the
    task's gold uses; `verdicts`: metric name -> verdict, for its database."""
    if any(status == "untranslated" for status, _ in used):
        return "B"
    names = [n for _, ms in used for n in ms]
    if not names:
        return "D"
    seen = {verdicts.get(n, "unclear") for n in names}
    if "wrong" in seen:
        return "C"
    return "A" if seen == {"faithful"} else "E"


def compiled_gain(score: Score, task: str) -> float:
    compiled = (score["contract"][task] + score["manual_compiled"][task]) / 2
    return compiled - score["manual_prompt"][task]


def interaction(score: Score, a: list[str], b: list[str]) -> float:
    mean = lambda ts: sum(compiled_gain(score, t) for t in ts) / len(ts)  # noqa: E731
    return mean(a) - mean(b)


def permutation_p(
    score: Score,
    a: list[str],
    b: list[str],
    n: int = PERMUTATIONS,
    seed: int = SEED,
) -> float:
    """One-sided: the share of A/B relabellings whose statistic is at least
    the observed one (the observed labelling counted once)."""
    observed = interaction(score, a, b)
    gains = [compiled_gain(score, t) for t in a + b]
    rng = random.Random(seed)
    hits = 1
    for _ in range(n):
        rng.shuffle(gains)
        diff = sum(gains[: len(a)]) / len(a) - sum(gains[len(a) :]) / len(b)
        hits += diff >= observed - 1e-12
    return hits / (n + 1)


def load_inputs() -> tuple[dict[str, list[str]], Entries, dict[str, dict[str, str]]]:
    """The private mapping (task -> KB entry ids its gold uses), each entry's
    compile outcome, and the review verdicts by database."""
    rows = [
        json.loads(line)
        for path in sorted(REVIEW.glob("review-b*.jsonl"))
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    by_db: dict[str, dict[str, str]] = collections.defaultdict(dict)
    for (db, metric), v in verdict_map(rows).items():
        by_db[db][metric] = v
    entries: Entries = {}
    for log in (ROOT / "contracts/livesqlbench").glob("*/compile-log.jsonl.gz"):
        for line in gzip.open(log, "rt", encoding="utf-8"):
            r = json.loads(line)
            if r.get("type") == "entry":  # later records replace earlier ones
                names = [m["name"] for m in r["metrics"] if not m.get("untranslated")]
                entries[(log.parent.name, r["entry_id"])] = (r["status"], names)
    mapping = {}
    with open(PRIVATE / MAPPING, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            mapping[r["instance_id"]] = r["external_knowledge"]
    return mapping, entries, by_db


def scores(model: str) -> tuple[dict[str, collections.Counter], dict[str, str]]:
    """arm -> task -> correct runs over the three repeats (primary set), and
    task -> database."""
    score = {arm: collections.Counter() for arm in ARMS}
    db = {}
    for i in (1, 2, 3):
        path = ROOT / "results" / (RUNS[model].format(i) + ".jsonl")
        for row in _as_e2e(_graded(load(path))):
            if row.get("subset") != "primary" or row["arm"] not in score:
                continue
            score[row["arm"]][row["task_id"]] += row["verdict"] == "correct"
            db[row["task_id"]] = row["db"]
    return score, db


def report(model: str) -> None:
    mapping, entries, by_db = load_inputs()
    score, db = scores(model)
    members: dict[str, list[str]] = collections.defaultdict(list)
    for task, d in db.items():
        used = [entries[(d, e)] for e in mapping[task] if (d, e) in entries]
        members[task_group(used, by_db[d])].append(task)
    print(f"== {model}: primary tasks by group, correct task-runs over three repeats")
    print(
        f"{'group':38s} {'tasks':>5s} "
        + " ".join(f"{a:>15s}" for a in ARMS)
        + "   contract-manual  sign tests vs manual_prompt (compiled; contract)"
    )
    for g in sorted(GROUPS):
        ts = members[g]
        if not ts:
            continue
        rate = {a: sum(score[a][t] for t in ts) / (3 * len(ts)) for a in ARMS}

        def sign(arm: str) -> str:
            up = sum(score[arm][t] > score["manual_prompt"][t] for t in ts)
            down = sum(score[arm][t] < score["manual_prompt"][t] for t in ts)
            p = binomtest(up, up + down, 0.5).pvalue if up + down else 1.0
            return f"{up}-{down} p={p:.2g}"

        print(
            f"{g}. {GROUPS[g]:35s} {len(ts):5d} "
            + " ".join(f"{rate[a]:15.1%}" for a in ARMS)
            + f"   {rate['contract'] - rate['manual_prompt']:+14.1%}"
            + f"  {sign('manual_compiled')}; {sign('contract')}"
        )
    a, b = members["A"], members["B"]
    stat = interaction(score, a, b)
    p = permutation_p(score, a, b)
    print(
        f"confirmatory: mean compiled_gain A - B = {stat:+.3f} correct runs per "
        f"task (of 3), one-sided permutation p = {p:.2g} "
        f"({PERMUTATIONS} relabellings, seed {SEED})"
    )


if __name__ == "__main__":
    report(sys.argv[1] if len(sys.argv) > 1 else "gpt-6-luna")
