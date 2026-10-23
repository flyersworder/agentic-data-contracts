"""Clustered inference and intervals: the second review round's checks.

Exploratory. Every p-value elsewhere in this harness treats tasks as
independent. They are not: DABStep's 450 tasks expand about 113 question
shapes (`templates.py`), and 176 of the 332 hard ones fall in 11 macro
families (`counterfactuals.family_of`); LiveSQLBench's tasks sit in 22
databases; experiment 4b's questions in five gap clusters; experiment 6's
in six knowledge pieces (a database's ten R or ten C questions). This
script re-reads the paired contrasts the paper reports and gives, for each:

- the effect in points (mean over tasks of a's correct-run share minus b's);
- a 95% percentile bootstrap interval over tasks;
- a 95% percentile bootstrap interval over clusters (resample whole
  clusters, ratio of summed differences to summed tasks), the cluster
  count, and a sign test over clusters (each cluster's summed difference;
  ties dropped; two-sided unless marked).

DABStep's primary clustering is the question shape; the macro family with
every other task its own cluster is the sensitivity column. Both bootstraps
use 10,000 resamples and seed 2026. A cluster interval over three to six
clusters is printed but is not a reliable 95% interval; the sign test over
so few clusters cannot reach p < 0.05 two-sided below six clusters.

Sections: the k=3 panels; the two facts (experiments 5, 5b, 5c) and the
one-fact gap-recovery shares; LiveSQLBench (knowledge gain, determinacy on
two scales, experiments 2 and 3's nulls); experiment 4b; experiment 6's
equivalence as an exact bound; the A/A drift between reruns of one arm.

Prints aggregates only. Reads the private ratings and LiveSQLBench files at
their default paths (`$PROCEDURAL_LOAD`, `$LSB_AUDIT`, `$LSB_EXP3`,
`$LSB_PRIVATE`).

Run:  uv run python analysis/clustered.py [--section NAME ...]
"""

from __future__ import annotations

import math
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path

import numpy as np
from scipy.stats import beta, binomtest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

RESAMPLES = 10_000
SEED = 2026
#: Rates are clipped this far from 0 and 1 before taking a logit.
LOGIT_EPS = 1e-3

Diffs = Mapping[str, float]
Clusters = Mapping[str, str]


# ── Pure functions ──────────────────────────────────────────────────────────


def diffs(
    a: Mapping[str, int], b: Mapping[str, int], tasks: Iterable[str], *, runs: int
) -> dict[str, float]:
    """task -> a's correct runs minus b's, as a fraction of `runs`; a task an
    arm lacks counts as zero correct there."""
    return {t: (a.get(t, 0) - b.get(t, 0)) / runs for t in tasks}


def _cluster_key(t: str, cluster_of: Clusters) -> str:
    return cluster_of.get(t, f"solo:{t}")


def _by_cluster(d: Diffs, cluster_of: Clusters) -> tuple[np.ndarray, np.ndarray]:
    sums: dict[str, float] = defaultdict(float)
    sizes: Counter[str] = Counter()
    for t, v in d.items():
        k = _cluster_key(t, cluster_of)
        sums[k] += v
        sizes[k] += 1
    keys = sorted(sums)
    return np.array([sums[k] for k in keys]), np.array([sizes[k] for k in keys])


def n_clusters(d: Diffs, cluster_of: Clusters) -> int:
    return len({_cluster_key(t, cluster_of) for t in d})


def _percentile(values: np.ndarray) -> tuple[float, float]:
    lo, hi = np.percentile(values, [2.5, 97.5])
    return float(lo), float(hi)


def task_interval(
    d: Diffs, *, resamples: int = RESAMPLES, seed: int = SEED
) -> tuple[float, float]:
    """95% percentile bootstrap interval of the mean difference, over tasks."""
    x = np.array([d[t] for t in sorted(d)])
    rng = np.random.default_rng(seed)
    return _percentile(x[rng.integers(0, len(x), (resamples, len(x)))].mean(axis=1))


def cluster_interval(
    d: Diffs, cluster_of: Clusters, *, resamples: int = RESAMPLES, seed: int = SEED
) -> tuple[float, float]:
    """95% percentile bootstrap interval of the mean difference, resampling
    whole clusters: each resample's estimate is its summed difference over
    its summed task count. A task without a cluster is its own."""
    sums, sizes = _by_cluster(d, cluster_of)
    rng = np.random.default_rng(seed)
    pick = rng.integers(0, len(sums), (resamples, len(sums)))
    return _percentile(sums[pick].sum(axis=1) / sizes[pick].sum(axis=1))


def cluster_sign(
    d: Diffs, cluster_of: Clusters, alternative: str = "two-sided"
) -> tuple[int, int, float]:
    """Clusters whose summed difference is positive, negative, and the exact
    binomial p over the non-tied ones."""
    sums, _ = _by_cluster(d, cluster_of)
    eps = 1e-9
    up, down = int((sums > eps).sum()), int((sums < -eps).sum())
    if not up + down:
        return up, down, 1.0
    return (
        up,
        down,
        float(binomtest(up, up + down, 0.5, alternative=alternative).pvalue),
    )


def clopper_pearson(k: float, n: float, alpha: float = 0.05) -> tuple[float, float]:
    """Exact (Clopper-Pearson) two-sided interval for k successes in n. Takes
    fractional counts, for an effective sample size."""
    lo = 0.0 if k <= 0 else float(beta.ppf(alpha / 2, k, n - k + 1))
    hi = 1.0 if k >= n else float(beta.ppf(1 - alpha / 2, k + 1, n - k))
    return lo, hi


def rule_of_three(n: int) -> float:
    """The approximate one-sided 95% upper bound for zero events in n."""
    return 3 / n


def logit(p: float) -> float:
    p = min(max(p, LOGIT_EPS), 1 - LOGIT_EPS)
    return math.log(p / (1 - p))


def logit_gain(before: float, after: float) -> float:
    """The gain from rate `before` to rate `after`, in log-odds."""
    return logit(after) - logit(before)


def ratio_interval(
    num: Mapping[str, float],
    den: Mapping[str, float],
    cluster_of: Clusters | None = None,
    *,
    resamples: int = RESAMPLES,
    seed: int = SEED,
) -> tuple[float | None, float | None, int]:
    """95% percentile interval of sum(num) / sum(den), resampling tasks (or
    whole clusters); a resample whose denominator is not positive has no
    ratio and is dropped, and the count of those is returned."""
    tasks = sorted(num)
    if cluster_of is None:
        n = np.array([num[t] for t in tasks])
        m = np.array([den[t] for t in tasks])
    else:
        n, _ = _by_cluster({t: num[t] for t in tasks}, cluster_of)
        m, _ = _by_cluster({t: den[t] for t in tasks}, cluster_of)
    rng = np.random.default_rng(seed)
    pick = rng.integers(0, len(n), (resamples, len(n)))
    top, bottom = n[pick].sum(axis=1), m[pick].sum(axis=1)
    ok = bottom > 0
    if not ok.any():
        return None, None, resamples
    lo, hi = _percentile(top[ok] / bottom[ok])
    return lo, hi, int((~ok).sum())


def design_effect(values: Mapping[str, float], cluster_of: Clusters) -> float:
    """The variance of the mean under cluster sampling (the between-cluster
    estimator of the ratio mean) over its variance under simple random
    sampling of tasks; 1 when the tasks do not vary."""
    x = np.array([values[t] for t in sorted(values)])
    n = len(x)
    srs = x.var(ddof=1) / n if n > 1 else 0.0
    if srs == 0:
        return 1.0
    sums, sizes = _by_cluster(values, cluster_of)
    g = len(sums)
    if g < 2:
        return float("inf")
    mean = sums.sum() / sizes.sum()
    resid = sums - mean * sizes
    clustered = g / (g - 1) * (resid**2).sum() / sizes.sum() ** 2
    return float(clustered / srs)


def contrast(
    a: Mapping[str, int],
    b: Mapping[str, int],
    tasks: Iterable[str],
    *,
    runs: int,
    clusterings: Mapping[str, Clusters],
) -> dict:
    """a against b over `tasks`: the effect, the task interval and sign
    counts, and per clustering its cluster count, interval and sign test."""
    d = diffs(a, b, sorted(tasks), runs=runs)
    out = {
        "n": len(d),
        "effect": sum(d.values()) / len(d),
        "task_ci": task_interval(d),
        "tasks": (sum(v > 0 for v in d.values()), sum(v < 0 for v in d.values())),
        "clusters": {},
    }
    for name, cluster_of in clusterings.items():
        out["clusters"][name] = {
            "n": n_clusters(d, cluster_of),
            "ci": cluster_interval(d, cluster_of),
            "sign": cluster_sign(d, cluster_of),
        }
    return out


def knowledge_piece(task_id: str) -> str:
    """Experiment 4/6 question id -> its knowledge piece: the database and
    whether it is one of the ten R or the ten C questions."""
    db, q = task_id.rsplit("_", 1)
    return f"{db}:{q[0]}"


def names_fees(question: str) -> bool:
    """The fee bucket of FINDINGS.md *Where the effect lives*: 294 of the 332
    hard tasks mention fees."""
    return "fee" in question.lower()


# ── Reporting ───────────────────────────────────────────────────────────────


def _pts(x: float) -> str:
    return f"{100 * x:+.1f}"


def _ci(ci: tuple[float | None, float | None], scale: float = 100.0) -> str:
    """An interval in points (scale 100, one decimal) or as is (two)."""
    lo, hi = ci
    if lo is None or hi is None:
        return "[none]"
    digits = 1 if scale == 100.0 else 2
    return f"[{scale * lo:+.{digits}f}, {scale * hi:+.{digits}f}]"


def _p(p: float) -> str:
    return f"{p:.2g}"


def report(label: str, r: dict) -> None:
    """One contrast on one line, then one line per clustering."""
    up, down = r["tasks"]
    print(
        f"   {label}: {_pts(r['effect'])} pts over {r['n']} tasks, "
        f"task CI {_ci(r['task_ci'])}, tasks {up}/{down}"
    )
    for name, c in r["clusters"].items():
        cu, cd, p = c["sign"]
        print(
            f"      by {name} ({c['n']} clusters): CI {_ci(c['ci'])}, "
            f"clusters {cu}/{cd}, two-sided p = {_p(p)}"
        )


# ── DABStep: the k=3 panels ─────────────────────────────────────────────────


def dabstep_clusterings() -> dict[str, dict[str, str]]:
    from counterfactuals import family_of
    from templates import shape_of

    return {"shape": shape_of(), "family": family_of()}


def panels(clusterings: dict[str, dict[str, str]]) -> None:
    import panel_decomposition as pd
    from templates import _tasks

    question = {str(t["task_id"]): t["question"] for t in _tasks()}
    print("== the k=3 panels: hard tasks, each task 0-3 correct runs")
    for model, pattern in pd.PANELS.items():
        official = None
        for view, e2e in (("official", False), ("end-to-end", True)):
            repeats = pd._repeats(pattern, e2e)
            hard = pd.scores(repeats, "hard")
            easy = pd.scores(repeats, "easy")
            if (hard, easy) == official:
                print(f"-- {model}, {view}: identical to official")
                continue
            official = (hard, easy)
            h = set().union(*(hard[a] for a in pd.ARMS))
            e = set().union(*(easy[a] for a in pd.ARMS))
            print(f"-- {model}, {view}")
            for a, b in (
                ("contract_hollow", "schema_only"),
                ("contract", "contract_hollow"),
                ("contract", "manual_prompt"),
                ("manual_prompt", "schema_only"),
            ):
                r = contrast(hard[a], hard[b], h, runs=3, clusterings=clusterings)
                report(f"{a} vs {b}, hard", r)
            nonfee = {t for t in h if not names_fees(question[t])}
            for label, score, among in (
                ("fee hard", hard, h - nonfee),
                ("non-fee hard", hard, nonfee),
                ("easy", easy, e),
            ):
                r = contrast(
                    score["contract"],
                    score["contract_hollow"],
                    among,
                    runs=3,
                    clusterings=clusterings,
                )
                report(f"content step (contract vs contract_hollow), {label}", r)


# ── DABStep: the two facts ──────────────────────────────────────────────────


def _rule_matching() -> set[str]:
    import procedural_load as pl
    from contract_text import rule_matching

    first = pl.read_ratings(pl.PUBLIC_RATINGS, "r1")["dabstep"]
    second = pl.read_ratings(pl.PUBLIC_RATINGS, "r2")["dabstep"]
    return rule_matching(first, second)


def _one_repeat(rows: list[dict]) -> dict[str, Counter[str]]:
    score: dict[str, Counter[str]] = {}
    for r in rows:
        score.setdefault(r["arm"], Counter())[str(r["task_id"])] += (
            r["verdict"] == "correct"
        )
    return score


def two_facts(clusterings: dict[str, dict[str, str]]) -> None:
    import contract_noted as cn
    import contract_text as ct
    import heldout_note as hn

    luna5 = hn.RUNS["gpt-6-luna"]["exp5"]
    rated = _rule_matching()
    print("== the two facts (experiments 5, 5b, 5c), end-to-end strict")
    for model, label28, label30 in (
        ("luna", "(28)", "(30)"),
        ("qwen", "(28Q)", "(30Q)"),
    ):
        s5 = ct.scores(model)
        s5c = cn.scores(ROOT / "results", cn.FILES[model], cn.ARMS)
        tasks = set(s5["contract"])
        rm = rated & tasks
        rest = tasks - rm
        seen = hn.inspected(hn._rows(luna5, 1), hn._rows(luna5, 2), among=rm)
        print(f"-- {model}: {len(rm)} rule-matching, {len(rest)} other")
        for label, score, a, b, among in (
            (
                f"{label28} manual_summed vs manual_resolved",
                s5,
                "manual_summed",
                "manual_resolved",
                rm,
            ),
            (
                "(29) contract_text vs manual_summed",
                s5,
                "contract_text",
                "manual_summed",
                rm,
            ),
            (
                f"{label30} contract_text_noted vs manual_summed",
                s5c,
                "contract_text_noted",
                "manual_summed",
                rm,
            ),
            (
                f"{label30} on the {len(rm - seen)} never read",
                s5c,
                "contract_text_noted",
                "manual_summed",
                rm - seen,
            ),
            (
                "tools: contract vs contract_text, rule-matching",
                s5,
                "contract",
                "contract_text",
                rm,
            ),
            (
                "tools: contract vs contract_text, other",
                s5,
                "contract",
                "contract_text",
                rest,
            ),
        ):
            report(
                label,
                contrast(score[a], score[b], among, runs=3, clusterings=clusterings),
            )
    selection(clusterings, rated)


def selection(clusterings: dict[str, dict[str, str]], rated: set[str]) -> None:
    """(28) on gpt-6-luna: the 34 inspected tasks were chosen on
    manual_resolved's r1 and r2 rows, which (28) reuses. Repeat r3 played no
    part, so it gives the selection-free comparison."""
    import contract_text as ct
    import heldout_note as hn

    luna5 = hn.RUNS["gpt-6-luna"]["exp5"]
    s5 = ct.scores("luna")
    rm = rated & set(s5["contract"])
    seen = hn.inspected(hn._rows(luna5, 1), hn._rows(luna5, 2), among=rm)
    unseen = rm - seen
    r3 = _one_repeat(hn._rows(luna5, 3))
    print("-- (28) on gpt-6-luna, the selection (M6)")
    for label, among in (
        (f"{len(seen)} inspected", seen),
        (f"{len(unseen)} never read", unseen),
        (f"all {len(rm)}", rm),
    ):
        report(
            f"(28) {label}, r3 only",
            contrast(
                r3["manual_summed"],
                r3["manual_resolved"],
                among,
                runs=1,
                clusterings=clusterings,
            ),
        )
    report(
        f"(28) {len(unseen)} never read, r1-r3",
        contrast(
            s5["manual_summed"],
            s5["manual_resolved"],
            unseen,
            runs=3,
            clusterings=clusterings,
        ),
    )
    per = [_one_repeat(hn._rows(luna5, i))["manual_resolved"] for i in (1, 2, 3)]
    for label, among in (
        (f"{len(unseen)} never read", unseen),
        (f"{len(seen)} inspected", seen),
    ):
        accs = ", ".join(
            f"r{i} {sum(p[t] for t in among) / len(among):.1%}"
            for i, p in zip((1, 2, 3), per, strict=True)
        )
        print(f"      manual_resolved on the {label}: {accs}")


def shares(clusterings: dict[str, dict[str, str]]) -> None:
    """Experiment 5's one fact: the share of the manual-to-contract gap that
    manual_resolved recovers, (W - M) / (C - M) over all 401 tasks."""
    import knowledge_delivery as kd

    arms = ("manual_prompt", "manual_resolved", "contract")
    print("== the one-fact gap-recovery share, all tasks, end-to-end strict")
    for model in ("gpt-6-sol", "gpt-6-luna", "qwen3.8-27b"):
        score: dict[str, Counter[str]] = {a: Counter() for a in arms}
        for i in kd.REPEATS:
            for r in kd.rows("dabstep", model, i, arms):
                score[r["arm"]][str(r["task_id"])] += r["verdict"] == "correct"
        tasks = sorted(set(score["contract"]))
        m, w, c = (score[a] for a in arms)
        num = {t: float(w[t] - m[t]) for t in tasks}
        den = {t: float(c[t] - m[t]) for t in tasks}
        point = sum(num.values()) / sum(den.values())
        lo, hi, dropped = ratio_interval(num, den)
        line = (
            f"   {model}: share {point:.2f} over {len(tasks)} tasks, "
            f"task CI [{lo:.2f}, {hi:.2f}]"
        )
        for name, cluster_of in clusterings.items():
            lo, hi, dropped = ratio_interval(num, den, cluster_of)
            line += f"; by {name} [{lo:.2f}, {hi:.2f}] ({dropped} dropped)"
        print(line)


# ── LiveSQLBench ────────────────────────────────────────────────────────────


def _group_gains(
    tasks: list[str],
    group: Mapping[str, str],
    know: Mapping[str, float],
    base: Mapping[str, float],
    cluster_of: Clusters | None,
    groups: tuple[str, ...],
) -> dict:
    """Per group: the knowledge arms' mean, schema_only's, the gain in points
    and in log-odds of the group means, each with a bootstrap interval
    (tasks, or whole clusters); and the first group's gains minus the last's.
    """
    keys = sorted({(cluster_of or {}).get(t, f"solo:{t}") for t in tasks})
    index = {k: i for i, k in enumerate(keys)}
    g = len(groups)
    k_sum = np.zeros((len(keys), g))
    b_sum = np.zeros((len(keys), g))
    n = np.zeros((len(keys), g))
    for t in tasks:
        if group[t] not in groups:
            continue
        i = index[(cluster_of or {}).get(t, f"solo:{t}")]
        j = groups.index(group[t])
        k_sum[i, j] += know[t]
        b_sum[i, j] += base[t]
        n[i, j] += 1
    rng = np.random.default_rng(SEED)
    pick = rng.integers(0, len(keys), (RESAMPLES, len(keys)))

    def stats(ks, bs, ns):
        with np.errstate(divide="ignore", invalid="ignore"):
            kn, bn = ks / ns, bs / ns
        lk = np.log(
            np.clip(kn, LOGIT_EPS, 1 - LOGIT_EPS)
            / (1 - np.clip(kn, LOGIT_EPS, 1 - LOGIT_EPS))
        )
        lb = np.log(
            np.clip(bn, LOGIT_EPS, 1 - LOGIT_EPS)
            / (1 - np.clip(bn, LOGIT_EPS, 1 - LOGIT_EPS))
        )
        return kn, bn, kn - bn, lk - lb

    point = stats(k_sum.sum(0), b_sum.sum(0), n.sum(0))
    boot = stats(k_sum[pick].sum(1), b_sum[pick].sum(1), n[pick].sum(1))
    ok = (n[pick].sum(1) > 0).all(axis=1)
    out = {"n": n.sum(0).astype(int), "know": point[0], "base": point[1]}
    for name, i in (("points", 2), ("logodds", 3)):
        out[name] = point[i]
        out[f"{name}_ci"] = [_percentile(boot[i][ok][:, j]) for j in range(g)]
        diff = boot[i][ok][:, 0] - boot[i][ok][:, -1]
        out[f"{name}_diff"] = point[i][0] - point[i][-1]
        out[f"{name}_diff_ci"] = _percentile(diff)
    return out


def _gains_report(label: str, r: dict, groups: tuple[str, ...], cluster: str) -> None:
    print(f"   {label}, intervals by {cluster}:")
    for j, gname in enumerate(groups):
        print(
            f"      {gname:12s} {r['n'][j]:3d} tasks  knowledge {r['know'][j]:.1%}, "
            f"schema_only {r['base'][j]:.1%}; gain {_pts(r['points'][j])} pts "
            f"{_ci(r['points_ci'][j])}, {r['logodds'][j]:+.2f} log-odds "
            f"{_ci(r['logodds_ci'][j], 1.0)}"
        )
    print(
        f"      {groups[0]} minus {groups[-1]}: {_pts(r['points_diff'])} pts "
        f"{_ci(r['points_diff_ci'])}; {r['logodds_diff']:+.2f} log-odds "
        f"{_ci(r['logodds_diff_ci'], 1.0)}"
    )


def livesqlbench() -> None:
    import lsb_corrected as lc
    import lsb_kb_clarity as kb
    import stronger_author as sa
    from dce.stats import load

    score, db_of = kb.main_runs()
    by_db = {"database": db_of}
    tasks = sorted(db_of)
    print(
        f"== LiveSQLBench: {len(tasks)} primary tasks in "
        f"{len(set(db_of.values()))} databases"
    )
    for model in kb.MAIN_RUNS:
        s = score[model]
        report(
            f"{model} knowledge gain, manual_prompt vs schema_only",
            contrast(
                s["manual_prompt"], s["schema_only"], tasks, runs=3, clusterings=by_db
            ),
        )

    first, second = kb.all_ratings()
    rated = sorted(set(first) & set(second) & set(tasks))
    know = {
        t: sum(score[m][a][t] for m in kb.MAIN_RUNS for a in kb.KNOWLEDGE_ARMS) / 18
        for t in rated
    }
    base = {t: sum(score[m]["schema_only"][t] for m in kb.MAIN_RUNS) / 6 for t in rated}
    print(
        "-- determinacy: the knowledge arms (both models, 18 runs a task) "
        "against schema_only (6)"
    )
    two = ("determined", "undetermined")
    three = ("determined", "split", "undetermined")
    consensus = {t: kb.consensus(first[t], second[t]) for t in rated}
    for label, group, groups in (
        ("consensus, split dropped", consensus, two),
        ("consensus, split as the middle group", consensus, three),
        ("first rater alone", {t: two[not kb.settled(first[t])] for t in rated}, two),
        ("second rater alone", {t: two[not kb.settled(second[t])] for t in rated}, two),
    ):
        for cluster, cluster_of in (("task", None), ("database", db_of)):
            _gains_report(
                label,
                _group_gains(rated, group, know, base, cluster_of, groups),
                groups,
                cluster,
            )

    print("-- experiment 2 (55 tasks, 3 databases), gpt-6-luna")
    exp = sa.EXPERIMENTS["livesqlbench"]
    repeats = [load(ROOT / "results" / exp["files"](i)) for i in sa.REPEATS]
    s2 = sa.task_scores(repeats, exp["subset"])
    t2 = set(s2[exp["arms"][0]])
    db2 = {r["task_id"]: r["db"] for rows in repeats for r in rows}
    for a, b in (("contract_opus", "contract"), ("contract_authored", "contract")):
        report(
            f"{a} vs {b}",
            contrast(s2[a], s2[b], t2, runs=3, clusterings={"database": db2}),
        )

    print("-- experiment 3, gpt-6-luna")
    repeats = [load(lc.RESULTS / f"exp3-luna-r{i}.jsonl") for i in (1, 2, 3)]
    import json

    split = json.loads(
        (ROOT / "results/livesqlbench/exp3-split.json").read_text(encoding="utf-8")
    )
    s3 = sa.task_scores(repeats, "primary")
    primary = set(s3["contract_opus"])
    db3 = {r["task_id"]: r["db"] for rows in repeats for r in rows}
    for label, among in (
        ("held out", primary & set(split["held_out"])),
        ("training (upper bound)", primary & set(split["train"])),
    ):
        for a, b in (
            ("manual_corrected", "manual_prompt"),
            ("contract_corrected", "contract_opus"),
        ):
            report(
                f"{label}: {a} vs {b}",
                contrast(s3[a], s3[b], among, runs=3, clusterings={"database": db3}),
            )


# ── Experiment 4b ───────────────────────────────────────────────────────────


def exp4b() -> None:
    import json

    import procedures_gap as pg

    doc = json.loads((pg.MATERIALS / "subsets.json").read_text(encoding="utf-8"))
    informed = pg.subsets(doc["questions"], doc["labels"])["data-informed"]
    gaps = pg.clusters(doc["questions"], informed)
    gap_of = {q: g for g, qs in gaps.items() for q in qs}
    by_model = {m: pg.scores(pg.RESULTS, pat) for m, pat in pg.FILES.items()}
    print(
        f"== experiment 4b: contract_gap vs manual_gap, {len(informed)} "
        f"data-informed questions, {len(gaps)} gap clusters"
    )
    for model, score, runs in (
        *((m, s, 3) for m, s in by_model.items()),
        ("pooled", pg.pool(by_model), 9),
    ):
        r = contrast(
            score["contract_gap"],
            score["manual_gap"],
            informed,
            runs=runs,
            clusterings={"gap": gap_of},
        )
        report(model, r)
        d = diffs(score["contract_gap"], score["manual_gap"], informed, runs=runs)
        per = ", ".join(
            f"{g} ({len(qs)}) {_pts(sum(d[q] for q in qs) / len(qs))}"
            for g, qs in gaps.items()
        )
        up, down, p = cluster_sign(d, gap_of, "greater")
        print(f"      per gap: {per}; one-sided cluster sign {up}/{down} p = {_p(p)}")


# ── Experiment 6 ────────────────────────────────────────────────────────────


def exp6() -> None:
    import json

    import disclosure_scale as ds
    import procedures_gap as pg

    fit = json.loads(ds.FIT.read_text(encoding="utf-8"))
    costs = {m: pg.run_costs(ds.RESULTS, pat) for m, pat in ds.FILES.items()}
    tasks = sorted(
        {t for by_arm in costs.values() for per in by_arm.values() for t in per}
    )
    ran = {m: ds.complete(by_arm, tasks, runs=3) for m, by_arm in costs.items()}
    by_model = {
        m: {a: v for a, v in pg.scores(ds.RESULTS, ds.FILES[m]).items() if a in ran[m]}
        for m in costs
    }
    pooled = pg.pool(by_model)
    runs = 3 * len(by_model)
    top = ds.common_top(fit)
    piece = {t: knowledge_piece(t) for t in tasks}
    print(
        f"== experiment 6: 1x against the common top {top}, {len(tasks)} tasks, "
        f"{runs} runs each, {len(set(piece.values()))} knowledge pieces"
    )
    for kind in ("contract_text_scaled", "contract_scaled"):
        a, b = f"{kind}_1x", f"{kind}_{top}"
        r = contrast(
            pooled[a], pooled[b], tasks, runs=runs, clusterings={"piece": piece}
        )
        report(f"{kind} drop (1x minus {top})", r)
        lost = {t: max(pooled[a][t] - pooled[b][t], 0) for t in tasks}
        k, n = sum(lost.values()), runs * len(tasks)
        lo, hi = clopper_pearson(k, n)
        deff = design_effect({t: v / runs for t, v in lost.items()}, piece)
        eff = max(deff, 1.0)
        _, hi_eff = clopper_pearson(k / eff, n / eff)
        where = Counter(piece[t] for t, v in lost.items() for _ in range(v))
        hit = sum(v > 0 for v in lost.values())
        print(
            f"      runs lost (tasks where 1x led, summed): {k} of {n}; exact 95% "
            f"[{100 * lo:.2f}, {100 * hi:.2f}]%; design effect over pieces {deff:.2f}, "
            f"effective upper {100 * hi_eff:.2f}%; tasks with a loss {hit} "
            f"of {len(tasks)}, upper {100 * clopper_pearson(hit, len(tasks))[1]:.1f}%; "
            f"by piece {dict(sorted(where.items()))}"
        )


# ── The A/A noise floor ─────────────────────────────────────────────────────


def drift(clusterings: dict[str, dict[str, str]]) -> None:
    import contract_noted as cn

    rated = _rule_matching()
    results = ROOT / "results"
    print("== A/A: a rerun arm in experiment 5c against its earlier files, end-to-end")
    for model in ("luna", "qwen"):
        now = cn.scores(results, cn.FILES[model], cn.ARMS)
        tasks = set(now["contract_text_noted"])
        rm = rated & tasks
        for arm, pattern in cn.EARLIER[model].items():
            before = cn.scores(results, pattern, (arm,))[arm]
            for label, among in (("rule-matching", rm), ("other", tasks - rm)):
                report(
                    f"{model} {arm} 5c minus {pattern.split('-')[0]}, {label}",
                    contrast(now[arm], before, among, runs=3, clusterings=clusterings),
                )


SECTIONS = ("panels", "two_facts", "shares", "livesqlbench", "exp4b", "exp6", "drift")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--section", nargs="*", choices=SECTIONS, default=list(SECTIONS)
    )
    chosen = parser.parse_args().section
    print(
        f"bootstrap: {RESAMPLES} resamples, seed {SEED}; points are percentage points"
    )
    dab = None
    for name in chosen:
        if name in ("panels", "two_facts", "shares", "drift") and dab is None:
            dab = dabstep_clusterings()
        fn = globals()[name]
        fn(dab) if name in ("panels", "two_facts", "shares", "drift") else fn()


if __name__ == "__main__":
    main()
