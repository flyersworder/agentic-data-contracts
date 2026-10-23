"""Is the contract's delivery gain where the knowledge carries procedure?

The account's second condition says a contract pays when the knowledge is
easy to get wrong from prose. So far that was inferred from the outcome. This
test measures it without the outcome and asks whether the delivery gain
follows it, within each benchmark.

Exposure, fixed before any rating was made (`procedural_load_rubric.md`):
two blind raters per task rate the procedural load (0-3) of the knowledge the
question needs. They see the question and the prose knowledge only -- for
DABStep the manual, the payments reference and the data note
(`manual_resolved`'s prompt), and they find the parts the question needs; for
LiveSQLBench the KB entries the benchmark's mapping lists, with the entries
they build on. Never an answer, a query, a contract or a result. Tasks from
both benchmarks are shuffled together under anonymous ids, so each rater
uses one scale for both. A task's load is the mean of its two ratings; it is
*high* at 2 or more.

Outcome, per task: the delivery gain, the contract's share of correct runs
minus the knowledge-equal prose arm's (end-to-end strict).

  DABStep       `contract` minus `manual_resolved`; gpt-6-sol, qwen3.8-27b,
                gpt-6-luna, three repeats each (9 runs per arm); 401 tasks.
  LiveSQLBench  `contract` minus `manual_prompt`; gpt-6-luna and qwen3.8-27b,
                three repeats (6 runs); the primary tasks the blind clarity
                test calls determined (`lsb_kb_clarity.py --all`), so the
                knowledge is there to be delivered.

Statistic: mean gain over high-load tasks minus mean gain over the rest,
one-sided permutation test (100,000 relabellings, seed 2026).

Predictions, fixed before any rating was read:

  (16) DABStep: the gain is larger on high-load tasks, p < 0.05.
  (17) DABStep, without the fee-matching families (176 tasks): the same,
       p < 0.05. The contract's lead outside the fee procedure follows load
       too, not only the procedure the account was built from.
  (18) LiveSQLBench, determined tasks: the same, p < 0.10. If fewer than 10
       determined tasks are high-load, (18) is reported but counts neither
       way: the benchmark has too little such knowledge to test.
  (19) High-load tasks are a larger share of DABStep than of LiveSQLBench's
       302 primary tasks, one-sided Fisher p < 0.05.

How the outcomes read, fixed in advance:

  (16) and (18) hold     condition 2 is shown within both benchmarks: the
                         delivery gain follows procedural load wherever it is.
  (16) holds, (18) fails LiveSQLBench's procedural knowledge is not helped by
                         its contract. Condition 2 does not explain its null;
                         the paper says the delivery effect is not explained
                         there (the secondary split below shows whether the
                         compiled contract's own errors mask it).
  (16) fails             procedural load, as rated, does not carry the
                         DABStep gain; condition 2 is not supported.
  (19)                   whether LiveSQLBench simply has less of this kind of
                         knowledge.

Secondary, reported whatever they show: the same statistic on all 302
LiveSQLBench primary tasks, and on determined tasks whose needed compiled
metrics a blind review judged faithful (group A, `lsb_root_cause.py`);
Spearman correlation of load and gain; accuracy per arm by load; sign tests of
contract against prose on high-load tasks; agreement (quadratic-weighted
kappa on load, kappa on high or not); and the item fields by benchmark.

Ratings and the key are private (the LiveSQLBench side encodes the
task-to-KB mapping): `$PROCEDURAL_LOAD`, default
~/data/dce-held-results/procedural-load. The DABStep side is public in
`ratings/procedural-load` (`PUBLIC_RATINGS`). Prints aggregates only.

Run:  uv run python analysis/procedural_load.py
"""

from __future__ import annotations

import json
import os
import random
import sys
from collections import Counter
from collections.abc import Mapping
from pathlib import Path

from scipy.stats import binomtest, fisher_exact, spearmanr

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

RATINGS = Path(
    os.environ.get(
        "PROCEDURAL_LOAD", Path.home() / "data/dce-held-results/procedural-load"
    )
)
#: The DABStep side alone, released in the repository (`export_ratings.py`):
#: enough for every analysis that needs only the rule-matching subset.
PUBLIC_RATINGS = ROOT / "ratings/procedural-load"
PERMUTATIONS = 100_000
SEED = 2026
HIGH = 2.0
DABSTEP_MODELS = ("gpt-6-sol", "qwen3.8-27b", "gpt-6-luna")
LSB_MODELS = ("gpt-6-luna", "qwen3.8-27b")
Score = Mapping[str, Mapping[str, int]]


def task_load(a: dict, b: dict) -> float:
    return (a["load"] + b["load"]) / 2


def is_high(load: float) -> bool:
    return load >= HIGH


def loads(first: dict[str, dict], second: dict[str, dict]) -> dict[str, float]:
    """task -> mean load, over tasks both raters rated."""
    return {t: task_load(first[t], second[t]) for t in first if t in second}


def split(load: dict[str, float], tasks: set[str]) -> tuple[list[str], list[str]]:
    """(high, rest) among `tasks` that have a load, sorted."""
    rated = sorted(t for t in tasks if t in load)
    return [t for t in rated if is_high(load[t])], [
        t for t in rated if not is_high(load[t])
    ]


def weighted_kappa(a: list[int], b: list[int], k: int = 4) -> float:
    """Cohen's kappa with quadratic weights over the levels 0..k-1."""
    n = len(a)
    w = [[(i - j) ** 2 / (k - 1) ** 2 for j in range(k)] for i in range(k)]
    observed = sum(w[x][y] for x, y in zip(a, b, strict=True)) / n
    pa, pb = Counter(a), Counter(b)
    expected = sum(w[i][j] * pa[i] * pb[j] for i in range(k) for j in range(k))
    expected /= n * n
    return 1.0 if expected == 0 else 1 - observed / expected


def gain(score: Score, contract: str, prose: str, task: str, runs: int) -> float:
    return (score[contract][task] - score[prose][task]) / runs


def _diff(gains: Mapping[str, float], hi: list[str], lo: list[str]) -> float:
    return sum(gains[t] for t in hi) / len(hi) - sum(gains[t] for t in lo) / len(lo)


def permutation_p(
    gains: Mapping[str, float],
    hi: list[str],
    lo: list[str],
    n: int = PERMUTATIONS,
    seed: int = SEED,
) -> float:
    """One-sided: the share of high/rest relabellings whose difference in
    mean gain is at least the observed one (the observed labelling counted
    once)."""
    observed = _diff(gains, hi, lo)
    values = [gains[t] for t in hi + lo]
    rng = random.Random(seed)
    hits = 1
    for _ in range(n):
        rng.shuffle(values)
        d = sum(values[: len(hi)]) / len(hi) - sum(values[len(hi) :]) / len(lo)
        hits += d >= observed - 1e-12
    return hits / (n + 1)


def read_ratings(folder: Path, role: str) -> dict[str, dict[str, dict]]:
    """benchmark -> task -> rating, for one rater role (r1 or r2)."""
    key = json.loads((folder / "key.json").read_text(encoding="utf-8"))
    out: dict[str, dict[str, dict]] = {}
    for path in sorted(folder.glob(f"rater-{role}-c*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                r = json.loads(line)
                k = key[r["id"]]
                out.setdefault(k["bench"], {})[k["task"]] = r
    return out


def dabstep_scores() -> tuple[Score, int]:
    from knowledge_delivery import rows

    score: dict[str, Counter[str]] = {
        "contract": Counter(),
        "manual_resolved": Counter(),
    }
    score["manual_prompt"] = Counter()
    for model in DABSTEP_MODELS:
        for i in (1, 2, 3):
            for r in rows("dabstep", model, i, tuple(score)):
                score[r["arm"]][str(r["task_id"])] += r["verdict"] == "correct"
    return score, 3 * len(DABSTEP_MODELS)


def lsb_scores() -> tuple[Score, int]:
    from lsb_root_cause import scores

    score: dict[str, Counter[str]] = {}
    for model in LSB_MODELS:
        s, _ = scores(model)
        for arm, c in s.items():
            score.setdefault(arm, Counter()).update(c)
    return score, 3 * len(LSB_MODELS)


def lsb_groups() -> tuple[set[str], set[str]]:
    """(determined tasks, group-A tasks) of LiveSQLBench's primary set."""
    import lsb_kb_clarity as kc
    from lsb_root_cause import load_inputs, scores, task_group

    first, second = kc.all_ratings()
    determined = {
        t
        for t in first
        if t in second and kc.consensus(first[t], second[t]) == "determined"
    }
    mapping, entries, by_db = load_inputs()
    _, db = scores("gpt-6-luna")
    group_a = {
        t
        for t, d in db.items()
        if task_group(
            [entries[(d, e)] for e in mapping[t] if (d, e) in entries], by_db[d]
        )
        == "A"
    }
    return determined, group_a


def _test(label, score: Score, runs, contract, prose, load, tasks) -> float | None:
    hi, lo = split(load, tasks)
    print(f"-- {label}: {len(hi)} high-load, {len(lo)} other tasks")
    if not hi or not lo:
        print("   too few tasks to compare")
        return None
    arms = (prose, contract)
    for name, ts in (("high", hi), ("other", lo)):
        acc = "  ".join(
            f"{a} {sum(score[a][t] for t in ts) / (runs * len(ts)):.1%}" for a in arms
        )
        print(f"   {name:5s}: {acc}")
    gains = {t: gain(score, contract, prose, t, runs) for t in hi + lo}
    p = permutation_p(gains, hi, lo)
    rho = spearmanr([load[t] for t in hi + lo], [gains[t] for t in hi + lo])
    up = sum(score[contract][t] > score[prose][t] for t in hi)
    down = sum(score[contract][t] < score[prose][t] for t in hi)
    sign = binomtest(up, up + down, 0.5).pvalue if up + down else 1.0
    print(
        f"   mean gain high {sum(gains[t] for t in hi) / len(hi):+.3f}, other "
        f"{sum(gains[t] for t in lo) / len(lo):+.3f}; difference "
        f"{_diff(gains, hi, lo):+.3f}, one-sided permutation p = {p:.2g}"
    )
    print(
        f"   Spearman rho {rho.statistic:+.2f} (p = {rho.pvalue:.2g}); on high-load "
        f"tasks {contract} vs {prose} {up} / {down} (two-sided p = {sign:.2g})"
    )
    return p


def _describe(name: str, first: dict, second: dict, load: dict[str, float]) -> None:
    tasks = sorted(load)
    a = [first[t]["load"] for t in tasks]
    b = [second[t]["load"] for t in tasks]
    import lsb_kb_clarity as kc

    k2 = kc.kappa([x >= HIGH for x in a], [x >= HIGH for x in b])
    print(
        f"== {name}: {len(tasks)} tasks rated twice; weighted kappa "
        f"{weighted_kappa(a, b):.2f}, kappa on high {k2:.2f}"
    )
    bins = Counter(load[t] for t in tasks)
    print("   load: " + ", ".join(f"{k:g} {bins[k]}" for k in sorted(bins)))
    for field in ("rule_matching", "multi_grain", "branching", "direct"):
        n = sum(bool(first[t].get(field)) + bool(second[t].get(field)) for t in tasks)
        print(f"   {field}: {n / (2 * len(tasks)):.0%} of ratings")
    steps = sum(first[t].get("steps", 0) + second[t].get("steps", 0) for t in tasks)
    print(f"   steps: mean {steps / (2 * len(tasks)):.2f}")


def main() -> None:
    from counterfactuals import family_of

    first, second = read_ratings(RATINGS, "r1"), read_ratings(RATINGS, "r2")
    load = {b: loads(first[b], second[b]) for b in ("dabstep", "livesqlbench")}
    for b in load:
        _describe(b, first[b], second[b], load[b])

    high = {b: sum(is_high(v) for v in load[b].values()) for b in load}
    n = {b: len(load[b]) for b in load}
    p19 = fisher_exact(
        [
            [high["dabstep"], n["dabstep"] - high["dabstep"]],
            [high["livesqlbench"], n["livesqlbench"] - high["livesqlbench"]],
        ],
        alternative="greater",
    ).pvalue
    print(
        f"(19) high-load share: DABStep {high['dabstep']} of {n['dabstep']}, "
        f"LiveSQLBench {high['livesqlbench']} of {n['livesqlbench']}; "
        f"one-sided Fisher p = {p19:.2g}"
    )

    score, runs = dabstep_scores()
    tasks = set(score["contract"])
    fee = set(family_of()) & tasks
    print("== DABStep: contract vs manual_resolved")
    p16 = _test(
        "(16) all tasks",
        score,
        runs,
        "contract",
        "manual_resolved",
        load["dabstep"],
        tasks,
    )
    p17 = _test(
        "(17) without the fee-matching families",
        score,
        runs,
        "contract",
        "manual_resolved",
        load["dabstep"],
        tasks - fee,
    )

    score, runs = lsb_scores()
    tasks = set(score["contract"])
    determined, group_a = lsb_groups()
    print("== LiveSQLBench: contract vs manual_prompt")
    p18 = _test(
        "(18) determined tasks",
        score,
        runs,
        "contract",
        "manual_prompt",
        load["livesqlbench"],
        tasks & determined,
    )
    n18 = len(split(load["livesqlbench"], tasks & determined)[0])
    _test(
        "secondary: all primary tasks",
        score,
        runs,
        "contract",
        "manual_prompt",
        load["livesqlbench"],
        tasks,
    )
    _test(
        "secondary: determined, every needed metric faithful (group A)",
        score,
        runs,
        "contract",
        "manual_prompt",
        load["livesqlbench"],
        tasks & determined & group_a,
    )

    def verdict(p: float | None, alpha: float) -> str:
        return "no test" if p is None else ("holds" if p < alpha else "fails")

    print("== predictions")
    print(f"(16) {verdict(p16, 0.05)}")
    print(f"(17) {verdict(p17, 0.05)}")
    if n18 < 10:
        print(f"(18) {n18} high-load determined tasks: too few, counts neither way")
    else:
        print(f"(18) {verdict(p18, 0.10)}")
    print(f"(19) {'holds' if p19 < 0.05 else 'fails'}")


if __name__ == "__main__":
    main()
