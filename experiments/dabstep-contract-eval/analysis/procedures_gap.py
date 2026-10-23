"""Experiment 4b: does an author with the data open recover what the
documentation leaves open, and is the contract's gain the author's knowledge?

Reproduces FINDINGS.md, *Gaps the documentation leaves open: predictions
(32)-(36)* (design: docs/superpowers/specs/2026-10-19-experiment-4b-design.md).
Experiment 4's three databases and 60 questions, each database's KB opened by
gaps (prep/procedures4b/gaps.json): G1, the rule table's wildcard stored as a
sentinel, in all three; G2, a silent encoding of a quantity C reads, in
reverse_logistics and solar_panel; G3, the precedence among matching rules
removed from reverse_logistics' KB. Blind raters labelled each gap
(prep/procedures4b/subsets.json): settled (solar G1, G2), split (labor G1,
reverse_logistics G1 and G2), convention-only (reverse_logistics G3); labor's
C questions have no gap. The data-informed subset is settled + split: 41
questions, five gap clusters.

Offline, from contracts-procedures4b/recovery.json alone (three draws with data
and one without per database):

  (32)  At least two thirds of the 15 data gap-draws (five data-informed
        gaps, three draws) are recovered.
  (32g) Recovery is highest on settled gaps, then split, then
        convention-only (a G3 draw "recovers" by choosing most specific
        wins); each step strictly lower.
  (32c) The no-data control recovers fewer data-informed gaps than the data
        draws (as a rate), and at most one of five.
  (33)  No data draw chooses most specific wins on G3.

On the data-informed subset, with gpt-6-luna, qwen3.8-27b and gpt-6-sol,
k=3, the six arms together in each repeat (a task scores 0-9 pooled):

  (34)  manual_full beats manual_gap.
  (35)  contract_gap beats manual_gap.
  (36)  The knowledge share, (manual_noted - manual_gap) / (contract_gap -
        manual_gap), is at least 0.5; falsified if its 95% interval's upper
        bound is below 0.5, inconclusive if the interval spans 0.5 with the
        point below it.

A directional prediction holds only if the pooled one-sided sign test over
tasks gives p < 0.05, the direction holds on each model, and it holds on a
majority of gap clusters. The share's interval resamples gap clusters
(10,000 resamples, seed 2026); a resample whose denominator is not positive
has no share and is dropped, and the count is printed.

Also reported: each label alone; the no-gap control (all knowledge arms
should tie); contract_text against manual_noted (form) and contract_gap
against contract_text (tools); the G3 subset descriptively; manual_full
against schema_only (manipulation check). Graded as LiveSQLBench is; there
is no separate end-to-end view for SQL answers.

Also reported, per model since prices differ: what the mean run spends --
turns, tool calls, the governed arm's lookups of contract text, fresh and
cached input, output and reasoning tokens, dollars, and dollars per correct
answer -- with sign tests over tasks on dollars and turns between the arms
that carry the same knowledge. qwen3.8-27b's route meters output only and
has no cache tier.

Run:  uv run python analysis/procedures_gap.py
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

import numpy as np
from scipy.stats import binomtest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dce.stats import load  # noqa: E402

RESULTS = ROOT / "results" / "procedures4b"
MATERIALS = ROOT / "prep" / "procedures4b"
RECOVERY = ROOT / "contracts-procedures4b"
FILES = {
    "gpt-6-luna": "exp4b-luna-r{}.jsonl",
    "qwen3.8-27b": "exp4b-qwen-r{}.jsonl",
    "gpt-6-sol": "exp4b-sol-r{}.jsonl",
}
ARMS = (
    "schema_only",
    "manual_gap",
    "manual_full",
    "contract_gap",
    "manual_noted",
    "contract_text",
)
DATA_DRAWS = ("draw-1", "draw-2", "draw-3")
DATA_INFORMED = ("settled", "split")
GRADIENT = ("settled", "split", "convention-only")
RESAMPLES = 10_000
SEED = 2026

# The governed arm's tools that fetch contract text on demand.
LOOKUPS = ("list_metrics", "lookup_metric", "lookup_domain", "lookup_relationships")
METRICS = ("turns", "calls", "lookups", "fresh", "cached", "output", "reasoning", "usd")

Score = dict[str, Counter[str]]
Runs = dict[str, dict[str, list[dict]]]


def label_of(tags: list[str], labels: dict[str, list[str]]) -> str:
    """A question's label: convention-only if any of its gaps is, else that
    of its gaps; 'no gap' when it has none."""
    if not tags:
        return "no gap"
    of = {g: label for label, gaps in labels.items() for g in gaps}
    found = {of[t] for t in tags}
    if "convention-only" in found:
        return "convention-only"
    if len(found) != 1:
        raise ValueError(f"a question mixes labels: {tags}")
    return found.pop()


def subsets(tags: dict[str, list[str]], labels: dict[str, list[str]]) -> dict:
    """label -> sorted question ids, plus 'data-informed' (settled + split)."""
    out: dict[str, list[str]] = {}
    for q, t in tags.items():
        out.setdefault(label_of(t, labels), []).append(q)
    out["data-informed"] = [q for label in DATA_INFORMED for q in out.get(label, [])]
    return {k: sorted(v) for k, v in out.items()}


def clusters(tags: dict[str, list[str]], among: list[str]) -> dict[str, list[str]]:
    """gap -> its questions, for questions carrying exactly one gap."""
    out: dict[str, list[str]] = {}
    for q in among:
        (gap,) = tags[q]
        out.setdefault(gap, []).append(q)
    return {g: sorted(qs) for g, qs in sorted(out.items())}


def scores(results: Path, pattern: str) -> Score:
    """arm -> task -> correct runs over the three repeats."""
    score: Score = {}
    for i in (1, 2, 3):
        for r in load(results / pattern.format(i)):
            score.setdefault(r["arm"], Counter())[str(r["task_id"])] += (
                r["verdict"] == "correct"
            )
    return score


def run_costs(results: Path, pattern: str) -> Runs:
    """arm -> task -> one entry per run over the three repeats: its cost
    (input split into fresh and cached) and whether it was correct."""
    out: Runs = {}
    for i in (1, 2, 3):
        for r in load(results / pattern.format(i)):
            calls = r["tool_calls"]
            cached = r.get("cached_tokens") or 0
            run = {
                "turns": r["turns"],
                "calls": len(calls),
                "lookups": sum(c in LOOKUPS for c in calls),
                "fresh": r["input_tokens"] - cached,
                "cached": cached,
                "output": r["output_tokens"],
                "reasoning": r.get("reasoning_tokens") or 0,
                "usd": r["usd"],
                "correct": r["verdict"] == "correct",
            }
            out.setdefault(r["arm"], {}).setdefault(str(r["task_id"]), []).append(run)
    return out


def efficiency(by_arm: Runs, tasks: list[str]) -> dict[str, dict]:
    """arm -> the mean run's METRICS over `tasks`, and the dollars spent per
    correct answer (None when no run was correct)."""
    out = {}
    for arm, per in by_arm.items():
        rs = [r for t in tasks for r in per.get(t, [])]
        correct = sum(r["correct"] for r in rs)
        out[arm] = {k: sum(r[k] for r in rs) / len(rs) for k in METRICS}
        out[arm]["usd_per_correct"] = (
            sum(r["usd"] for r in rs) / correct if correct else None
        )
    return out


def cheaper(by_arm: Runs, a: str, b: str, tasks, key: str = "usd"):
    """Tasks on which a's mean run spends less `key` than b's, tasks on
    which it spends more, and the two-sided sign test."""

    def mean(arm: str, t: str) -> float:
        return sum(r[key] for r in by_arm[arm][t]) / len(by_arm[arm][t])

    up = sum(mean(a, t) < mean(b, t) for t in tasks)
    down = sum(mean(a, t) > mean(b, t) for t in tasks)
    n = up + down
    return up, down, binomtest(up, n, 0.5).pvalue if n else 1.0


def pool(by_model: dict[str, Score]) -> Score:
    out: Score = {}
    for score in by_model.values():
        for arm, c in score.items():
            out.setdefault(arm, Counter()).update(c)
    return out


def _mean(score: Score, arm: str, tasks: list[str]) -> float:
    return sum(score[arm][t] for t in tasks) / len(tasks)


def _gap(score: Score, a: str, b: str, tasks: list[str]) -> float:
    """a minus b, in correct runs per task."""
    return _mean(score, a, tasks) - _mean(score, b, tasks)


def sign(score: Score, a: str, b: str, tasks, alternative: str):
    up = sum(score[a][t] > score[b][t] for t in tasks)
    down = sum(score[a][t] < score[b][t] for t in tasks)
    n = up + down
    p = binomtest(up, n, 0.5, alternative=alternative).pvalue if n else 1.0
    return up, down, p


def directional(
    by_model: dict[str, Score],
    a: str,
    b: str,
    tasks: list[str],
    gap_clusters: dict[str, list[str]],
) -> dict:
    """a beats b: the pooled sign test, the accuracy gap per model and per
    gap cluster (pooled over models), and the verdict."""
    pooled = pool(by_model)
    runs = 3 * len(by_model)
    up, down, p = sign(pooled, a, b, tasks, "greater")
    models = {m: _gap(s, a, b, tasks) / 3 for m, s in by_model.items()}
    per = {c: _gap(pooled, a, b, qs) / runs for c, qs in gap_clusters.items()}
    holds = (
        p < 0.05
        and all(g > 0 for g in models.values())
        and sum(g > 0 for g in per.values()) * 2 > len(per)
    )
    return {
        "up": up,
        "down": down,
        "p": p,
        "models": models,
        "clusters": per,
        "holds": holds,
    }


def share(score: Score, tasks: list[str]) -> float | None:
    """(manual_noted - manual_gap) / (contract_gap - manual_gap); None when
    the denominator is not positive."""
    den = _gap(score, "contract_gap", "manual_gap", tasks)
    if den <= 0:
        return None
    return _gap(score, "manual_noted", "manual_gap", tasks) / den


def share_interval(
    score: Score, gap_clusters: dict[str, list[str]], n: int = RESAMPLES
) -> tuple[float | None, float | None, int]:
    """95% percentile interval of the share, resampling gap clusters; and
    how many resamples had no share."""
    rng = np.random.default_rng(SEED)
    names = sorted(gap_clusters)
    values, dropped = [], 0
    for _ in range(n):
        pick = rng.integers(0, len(names), len(names))
        tasks = [t for i in pick for t in gap_clusters[names[i]]]
        s = share(score, tasks)
        if s is None:
            dropped += 1
        else:
            values.append(s)
    if not values:
        return None, None, dropped
    lo, hi = np.percentile(values, [2.5, 97.5])
    return float(lo), float(hi), dropped


def share_verdict(point, lo, hi) -> str:
    if point is None or hi is None:
        return "undefined"
    if hi < 0.5:
        return "falsified"
    return "holds" if point >= 0.5 else "inconclusive"


def recovery_rates(
    recovery: dict, labels: dict[str, list[str]]
) -> dict[str, dict[str, tuple[int, int]]]:
    """'data' and 'no-data' -> label -> (recovered, gap-draws). A G3 draw
    counts as recovered when it chose most specific wins."""
    out: dict[str, dict[str, list[int]]] = {"data": {}, "no-data": {}}
    for label, gaps in labels.items():
        for gap in gaps:
            db, g = gap.split(":")
            for draw, found in recovery[db].items():
                kind = "no-data" if draw == "no-data" else "data"
                entry = found[g]
                ok = (
                    entry["convention"] == "most_specific"
                    if g == "g3"
                    else entry["category"] == "recovered"
                )
                tally = out[kind].setdefault(label, [0, 0])
                tally[0] += ok
                tally[1] += 1
    return {k: {lb: (v[0], v[1]) for lb, v in d.items()} for k, d in out.items()}


def recovery_predictions(recovery: dict, labels: dict[str, list[str]]) -> dict:
    """label -> (holds, what was found), for (32), (32g), (32c) and (33)."""
    rates = recovery_rates(recovery, labels)

    def informed(kind: str) -> tuple[int, int]:
        pairs = [rates[kind][lb] for lb in DATA_INFORMED if lb in rates[kind]]
        return sum(p[0] for p in pairs), sum(p[1] for p in pairs)

    out = {}
    got, of = informed("data")
    out["(32)"] = (3 * got >= 2 * of, f"{got} of {of} data gap-draws recovered")
    steps = [rates["data"][lb][0] / rates["data"][lb][1] for lb in GRADIENT]
    out["(32g)"] = (
        all(x > y for x, y in zip(steps, steps[1:], strict=False)),
        ", ".join(f"{lb} {r:.0%}" for lb, r in zip(GRADIENT, steps, strict=True)),
    )
    nd_got, nd_of = informed("no-data")
    out["(32c)"] = (
        nd_got / nd_of < got / of and nd_got <= 1,
        f"no-data {nd_got} of {nd_of}, data draws {got} of {of}",
    )
    g3 = [g for g in labels.get("convention-only", []) if g.endswith(":g3")]
    chosen = Counter(
        recovery[g.split(":")[0]][d]["g3"]["convention"] for g in g3 for d in DATA_DRAWS
    )
    no_data = ", ".join(
        recovery[g.split(":")[0]]["no-data"]["g3"]["convention"] for g in g3
    )
    out["(33)"] = (
        chosen["most_specific"] == 0,
        "data draws: "
        + ", ".join(f"{c} {n}" for c, n in sorted(chosen.items()))
        + f"; no-data: {no_data}",
    )
    return out


def _row(score: Score, tasks: list[str], runs: int) -> str:
    return "  ".join(f"{a} {_mean(score, a, tasks) / runs:.1%}" for a in ARMS)


def main() -> None:
    labels_doc = json.loads((MATERIALS / "subsets.json").read_text(encoding="utf-8"))
    labels, tags = labels_doc["labels"], labels_doc["questions"]
    recovery = json.loads((RECOVERY / "recovery.json").read_text(encoding="utf-8"))

    print("== recovery (offline)")
    for kind, per in recovery_rates(recovery, labels).items():
        print(f"   {kind}: " + ", ".join(f"{lb} {a}/{b}" for lb, (a, b) in per.items()))
    for label, (holds, found) in recovery_predictions(recovery, labels).items():
        print(f"{label} {'holds' if holds else 'fails'}: {found}")

    by_model = {
        m: scores(RESULTS, pattern)
        for m, pattern in FILES.items()
        if (RESULTS / pattern.format(1)).exists()
    }
    if not by_model:
        print("no agent results yet")
        return
    pooled = pool(by_model)
    runs = 3 * len(by_model)
    sub = subsets(tags, labels)
    informed = sub["data-informed"]
    gaps = clusters(tags, informed)

    print(f"== accuracy, {', '.join(by_model)} pooled ({runs} runs a task)")
    for label in ("data-informed", *DATA_INFORMED, "convention-only", "no gap"):
        print(f"   {label} ({len(sub[label])}): {_row(pooled, sub[label], runs)}")
    for m, s in by_model.items():
        print(f"   {m}, data-informed: {_row(s, informed, 3)}")
    for g, qs in gaps.items():
        print(f"   cluster {g} ({len(qs)}): {_row(pooled, qs, runs)}")

    for label, a, b in (
        ("(34)", "manual_full", "manual_gap"),
        ("(35)", "contract_gap", "manual_gap"),
    ):
        r = directional(by_model, a, b, informed, gaps)
        print(
            f"{label} {'holds' if r['holds'] else 'fails'}: {a} vs {b}, "
            f"{r['up']} / {r['down']}, one-sided p = {r['p']:.2g}; gap "
            f"{_gap(pooled, a, b, informed) / runs:+.1%}; models "
            + ", ".join(f"{m} {g:+.1%}" for m, g in r["models"].items())
            + "; clusters "
            + ", ".join(f"{c} {g:+.1%}" for c, g in r["clusters"].items())
        )
        for lb in DATA_INFORMED:
            up, down, p = sign(pooled, a, b, sub[lb], "greater")
            print(
                f"      {lb}: {_gap(pooled, a, b, sub[lb]) / runs:+.1%}, "
                f"{up} / {down}, one-sided p = {p:.2g}"
            )

    point = share(pooled, informed)
    lo, hi, dropped = share_interval(pooled, gaps)
    shown = "undefined" if point is None else f"{point:.2f}"
    interval = "none" if lo is None else f"[{lo:.2f}, {hi:.2f}]"
    print(
        f"(36) {share_verdict(point, lo, hi)}: knowledge share {shown}, 95% "
        f"interval {interval} over gap clusters ({dropped} of {RESAMPLES} "
        "resamples without a share)"
    )
    for lb in DATA_INFORMED:
        s = share(pooled, sub[lb])
        print(f"      {lb}: {'undefined' if s is None else f'{s:.2f}'}")
    for m, s in by_model.items():
        v = share(s, informed)
        print(f"      {m}: {'undefined' if v is None else f'{v:.2f}'}")

    print("== reported, not predicted")
    for among, name in (
        (informed, "data-informed"),
        (sub["no gap"], "no gap"),
        (sub["convention-only"], "convention-only"),
    ):
        for a, b in (
            ("contract_text", "manual_noted"),
            ("contract_gap", "contract_text"),
            ("contract_gap", "manual_gap"),
            ("contract_gap", "manual_full"),
            ("manual_full", "schema_only"),
        ):
            up, down, p = sign(pooled, a, b, among, "two-sided")
            print(
                f"   {name}: {a} vs {b}: {_gap(pooled, a, b, among) / runs:+.1%}, "
                f"{up} / {down}, two-sided p = {p:.2g}"
            )

    print("== efficiency, per model (prices differ), the mean run")
    every = sorted(tags)
    for m, pattern in FILES.items():
        if m not in by_model:
            continue
        by_arm = run_costs(RESULTS, pattern)
        for among, name in ((informed, "data-informed"), (every, "all")):
            print(f"   {m}, {name} ({len(among)}):")
            for arm, e in efficiency(by_arm, among).items():
                per_correct = e["usd_per_correct"]
                print(
                    f"      {arm:14s} turns {e['turns']:4.1f}  calls "
                    f"{e['calls']:4.1f}  lookups {e['lookups']:3.1f}  fresh "
                    f"{e['fresh']:7,.0f}  cached {e['cached']:7,.0f}  output "
                    f"{e['output']:6,.0f}  reasoning {e['reasoning']:6,.0f}  "
                    f"m$ {1000 * e['usd']:6.2f}  m$/correct "
                    + ("-" if per_correct is None else f"{1000 * per_correct:.2f}")
                )
        for a, b in (
            ("contract_gap", "contract_text"),
            ("contract_gap", "manual_noted"),
            ("contract_text", "manual_noted"),
            ("contract_gap", "manual_full"),
        ):
            for key in ("usd", "turns"):
                up, down, p = cheaper(by_arm, a, b, informed, key)
                print(
                    f"      data-informed, {a} spends less {key} than {b}: "
                    f"{up} / {down}, two-sided p = {p:.2g}"
                )


if __name__ == "__main__":
    main()
