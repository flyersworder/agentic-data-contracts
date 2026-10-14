"""How many tasks does one piece of knowledge serve, and is that where a
contract pays?

The working account of the two benchmarks (FINDINGS.md, *Knowledge reuse*):
a contract earns its keep when one piece of knowledge is needed by many
tasks, is easy to get wrong from the prose, and can be held exactly as a
definition. This script measures the first condition on both benchmarks and
sets it beside the arms' accuracy.

DABStep. A task's knowledge is its macro family (`counterfactuals.family_of`,
176 of the 401 golded tasks); every family is answered by the fee-matching
procedure (which fee rules apply to a payment or merchant), so that one
procedure serves all 176. The remaining 225 tasks have no family.

LiveSQLBench. A task's knowledge is the KB entries its gold uses (the
benchmark's private task-to-KB mapping), closed over `children_knowledge`
(the entries an entry is defined in terms of). An entry's reuse is the number
of primary tasks whose closed set contains it; a task's reuse is the largest
reuse among its entries. Each database has its own KB, so reuse is also
shown as a share of the database's primary tasks.

Prints aggregates only. Inputs outside the repository: the mapping and the
KB files (`$LSB_PRIVATE`, default ~/data/livesqlbench).

Run:  uv run python analysis/knowledge_reuse.py
"""

from __future__ import annotations

import json
import os
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

from scipy.stats import binomtest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dce.stats import load  # noqa: E402

PRIVATE = Path(os.environ.get("LSB_PRIVATE", Path.home() / "data/livesqlbench"))
MAPPING = "livesqlbench_base_full_v1_gt_kg_testcases_20260613.jsonl"
REPEATS = (1, 2, 3)
BINS = ("none", "1", "2", "3-4", "5+")
LSB_RUNS = {"gpt-6-luna": "luna-r{}", "qwen3.8-27b": "qwen38-r{}"}
LSB_ARMS = ("schema_only", "manual_prompt", "contract")
EXP2_ARMS = ("manual_prompt", "contract", "contract_opus", "contract_authored")
DABSTEP_RUNS = {
    "luna-resolved-r{}.jsonl": ("manual_prompt", "contract"),
    "dabstep-opus-luna-r{}.jsonl": (
        "contract_compiled",
        "contract_compiled_opus",
        "contract_compiled_opus_joins",
    ),
}
RULE_SET_OR_TOTAL = {
    "avg_fee_account",
    "avg_fee_account_mcc",
    "fee_ids_by_at_aci",
    "total_fees_day",
    "total_fees_month",
    "total_fees_year",
}

Key = tuple[str, int]


def closure(db: str, ids: list[int], children: dict[Key, list[int]]) -> set[int]:
    """`ids` and every entry they are defined in terms of, transitively."""
    seen: set[int] = set()
    todo = list(ids)
    while todo:
        i = todo.pop()
        if i in seen:
            continue
        seen.add(i)
        todo.extend(children.get((db, i), []))
    return seen


def reuse(uses: dict[str, tuple[str, set[int]]]) -> dict[Key, int]:
    """(db, entry) -> number of tasks whose set contains it."""
    count: Counter[Key] = Counter()
    for db, ids in uses.values():
        for i in ids:
            count[(db, i)] += 1
    return dict(count)


def hard_tasks(
    uses: dict[str, tuple[str, set[int]]],
    count: dict[Key, int],
    score,
    min_reuse: int = 3,
    max_manual: float = 0.2,
) -> set[str]:
    """Tasks of an entry that serves `min_reuse` or more tasks and on whose
    tasks `manual_prompt` is right in at most `max_manual` of the runs: the
    shape of DABStep's fee formula (reused, and failed from the prose)."""
    out: set[str] = set()
    for (db, i), n in count.items():
        if n < min_reuse:
            continue
        among = {t for t, (d, ids) in uses.items() if d == db and i in ids}
        if sum(score["manual_prompt"][t] for t in among) <= max_manual * 3 * n:
            out |= among
    return out


def bin_of(n: int) -> str:
    if n == 0:
        return "none"
    if n <= 2:
        return str(n)
    return "3-4" if n <= 4 else "5+"


def _scores(paths: list[Path], subset: str | None = None):
    score: dict[str, Counter[str]] = defaultdict(Counter)
    for path in paths:
        for r in load(path):
            if subset is None or r["subset"] == subset:
                score[r["arm"]][r["task_id"]] += r["verdict"] == "correct"
    return score


def _acc(score, arm: str, tasks: set[str]) -> str:
    if not tasks:
        return "-"
    return f"{sum(score[arm][t] for t in tasks) / (3 * len(tasks)):.1%}"


def _sign(score, a: str, b: str, tasks: set[str]) -> str:
    better = sum(score[a][t] > score[b][t] for t in tasks)
    worse = sum(score[a][t] < score[b][t] for t in tasks)
    n = better + worse
    p = binomtest(better, n, 0.5).pvalue if n else 1.0
    return f"{better}/{worse} p={p:.2g}"


def dabstep() -> None:
    from counterfactuals import family_of

    family = family_of()
    score: dict[str, Counter[str]] = defaultdict(Counter)
    for pattern, arms in DABSTEP_RUNS.items():
        part = _scores([ROOT / "results" / pattern.format(i) for i in REPEATS])
        for arm in arms:
            score[arm] = part[arm]
    tasks = set(score["contract"])
    fee = {t for t in tasks if t in family}
    groups = {
        "fee matching: rule-set / total-fee": {
            t for t in fee if family[t] in RULE_SET_OR_TOTAL
        },
        "fee matching: other families": {
            t for t in fee if family[t] not in RULE_SET_OR_TOTAL
        },
        "no family": tasks - fee,
    }
    arms = [a for arms in DABSTEP_RUNS.values() for a in arms]
    print(f"== DABStep: {len(tasks)} tasks, {len(set(family.values()))} families")
    print(
        f"the fee-matching procedure serves {len(fee)} tasks "
        f"({len(fee) / len(tasks):.0%}); families: "
        + ", ".join(f"{k} {v}" for k, v in Counter(family.values()).most_common())
    )
    print("group                                tasks  " + "  ".join(arms))
    for name, among in groups.items():
        print(
            f"{name:36s} {len(among):5d}  "
            + "  ".join(f"{_acc(score, a, among):>{len(a)}s}" for a in arms)
        )
    for name, among in groups.items():
        print(
            f"  {name}: contract vs manual_prompt "
            f"{_sign(score, 'contract', 'manual_prompt', among)}; "
            "opus_joins vs compiled "
            + _sign(score, "contract_compiled_opus_joins", "contract_compiled", among)
        )
    print()


def lsb_inputs() -> tuple[dict[str, tuple[str, set[int]]], dict[Key, str]]:
    children: dict[Key, list[int]] = {}
    kind: dict[Key, str] = {}
    for path in (PRIVATE / "hf-full-v1").glob("*/*_kb.jsonl"):
        db = path.parent.name
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            c = r["children_knowledge"]
            children[(db, r["id"])] = c if isinstance(c, list) else []
            kind[(db, r["id"])] = r["type"]
    mapping = {}
    with open(PRIVATE / MAPPING, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            mapping[r["instance_id"]] = r["external_knowledge"]
    primary = {
        r["task_id"]: r["db"]
        for r in load(ROOT / "results/livesqlbench/luna-r1.jsonl")
        if r["subset"] == "primary"
    }
    uses = {
        t: (db, closure(db, mapping.get(t, []), children)) for t, db in primary.items()
    }
    return uses, kind


def livesqlbench() -> None:
    uses, kind = lsb_inputs()
    count = reuse(uses)
    direct_tasks = Counter(db for db, _ in uses.values())
    print(f"== LiveSQLBench: {len(uses)} primary tasks, {len(direct_tasks)} databases")
    print(
        f"KB entries {len(kind)}; used by some primary task {len(count)} "
        f"({len(count) / len(kind):.0%})"
    )
    print(
        "tasks per used entry: "
        + ", ".join(
            f"{b} {n}"
            for b, n in sorted(
                Counter(bin_of(v) for v in count.values()).items(),
                key=lambda kv: BINS.index(kv[0]),
            )
        )
        + f"; median {statistics.median(count.values()):g}, max {max(count.values())}"
    )
    top = sorted(count.items(), key=lambda kv: -kv[1])[:10]
    print(
        "the ten most reused entries: "
        + ", ".join(f"{n} tasks ({kind[k].split('_')[0]})" for k, n in top)
    )
    shares = []
    for db, n in direct_tasks.items():
        best = max((v for (d, _), v in count.items() if d == db), default=0)
        shares.append(best / n)
    print(
        "share of a database's primary tasks served by its most reused entry: "
        f"median {statistics.median(shares):.0%}, "
        f"range {min(shares):.0%}-{max(shares):.0%} "
        "(DABStep: the fee-matching procedure, 44%)"
    )
    task_reuse = {
        t: max((count[(db, i)] for i in ids), default=0)
        for t, (db, ids) in uses.items()
    }
    for model, pattern in LSB_RUNS.items():
        score = _scores(
            [
                ROOT / "results/livesqlbench" / (pattern.format(i) + ".jsonl")
                for i in REPEATS
            ],
            subset="primary",
        )
        print(f"-- {model}, by the task's most reused entry")
        print("reuse  tasks  " + "  ".join(LSB_ARMS) + "  contract vs manual")
        for b in BINS:
            among = {t for t, v in task_reuse.items() if bin_of(v) == b}
            if not among:
                continue
            print(
                f"{b:5s} {len(among):6d}  "
                + "  ".join(f"{_acc(score, a, among):>{len(a)}s}" for a in LSB_ARMS)
                + f"  {_sign(score, 'contract', 'manual_prompt', among)}"
            )
        hard = hard_tasks(uses, count, score)
        print(
            f"reused (3+ tasks) and failed from the manual (<= 20% of runs): "
            f"{len(hard)} tasks; "
            + "  ".join(f"{a} {_acc(score, a, hard)}" for a in LSB_ARMS)
        )
        print(
            "  their review groups (lsb_root_cause): "
            + ", ".join(f"{g} {n}" for g, n in sorted(review_groups(hard).items()))
            + "; all primary tasks: "
            + ", ".join(f"{g} {n}" for g, n in sorted(review_groups(set(uses)).items()))
        )
        exp2 = _scores(
            [ROOT / f"results/livesqlbench/lsb-exp2-luna-r{i}.jsonl" for i in REPEATS],
            subset="primary",
        )
        if model == "gpt-6-luna":
            among = hard & set(exp2["contract"])
            print(
                f"  of them in experiment 2's databases: {len(among)} tasks; "
                + "  ".join(f"{a} {_acc(exp2, a, among)}" for a in EXP2_ARMS)
            )
    print()


def review_groups(tasks: set[str]) -> Counter[str]:
    """The five-group label of `lsb_root_cause` for each task."""
    import lsb_root_cause as rc

    mapping, entries, by_db = rc.load_inputs()
    primary = {
        r["task_id"]: r["db"]
        for r in load(ROOT / "results/livesqlbench/luna-r1.jsonl")
        if r["subset"] == "primary"
    }
    out: Counter[str] = Counter()
    for t in tasks:
        d = primary[t]
        used = [entries[(d, e)] for e in mapping[t] if (d, e) in entries]
        out[rc.task_group(used, by_db[d])] += 1
    return out


if __name__ == "__main__":
    dabstep()
    livesqlbench()
