"""Cross-family blind ratings: agreement with the Opus raters, and the subset
results recomputed on each family's subsets (prep/cross-family-ratings/PLAN.md).

Each alternative uses the paper's rules. Rule-matching means both raters say
`rule_matching`. Determined means both say settled; undetermined means both
say not. "GPT alone" and "Gemini alone" use one rating as both raters;
"cross-family" pairs the two. The paper's own subsets (the two Opus raters)
run through the same code, as the reference each alternative is judged
against.

Inputs: the new raters' outputs (`$CROSS_FAMILY`, default
~/data/dce-held-results/cross-family/<model>/<chunk>.jsonl); the private load
key and the Opus load ratings (`$PROCEDURAL_LOAD`); the determinacy keys and
labels (`$LSB_AUDIT`). Prints aggregates only.

Run:  uv run python analysis/cross_family.py
"""

from __future__ import annotations

import json
import os
import sys
from collections import Counter
from itertools import combinations
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lsb_kb_clarity import kappa  # noqa: E402, F401  (re-exported for tests)

NEW = Path(
    os.environ.get("CROSS_FAMILY", Path.home() / "data/dce-held-results/cross-family")
)
FAMILIES = {"gpt": "gpt-6.1-sol", "gemini": "gemini-4-argon"}


def verdict(
    ref_effect: float,
    ref_ci: tuple | None,
    alt_effect: float,
    alt_ci: tuple | None,
    *,
    ref_p: float | None = None,
    alt_p: float | None = None,
) -> str:
    """The plan's section D. Significant means the interval excludes zero
    or, where there is no interval, p < 0.05."""

    def significant(ci, p) -> bool:
        if ci is not None:
            lo, hi = ci
            return lo is not None and hi is not None and (lo > 0 or hi < 0)
        return p is not None and p < 0.05

    if not significant(ref_ci, ref_p):
        return "no claim"
    same_sign = alt_effect != 0 and (alt_effect > 0) == (ref_effect > 0)
    return "robust" if same_sign and significant(alt_ci, alt_p) else "not robust"


def by_task(rows: list[dict], key: dict[str, dict]) -> dict[str, dict[str, dict]]:
    out: dict[str, dict[str, dict]] = {}
    for r in rows:
        k = key[r["id"]]
        out.setdefault(k["bench"], {})[k["task"]] = r
    return out


def det_by_task(rows: list[dict], key: dict[str, str]) -> dict[str, dict]:
    return {key[r["id"]]: r for r in rows}


def _jsonl(path: Path) -> list[dict]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def new_load(model: str) -> dict[str, dict[str, dict]]:
    import procedural_load as pl

    key = json.loads((pl.RATINGS / "key.json").read_text(encoding="utf-8"))
    rows = [
        r for p in sorted((NEW / model).glob("c[0-9][0-9].jsonl")) for r in _jsonl(p)
    ]
    return by_task(rows, key)


def new_det(model: str) -> dict[str, dict]:
    import lsb_kb_clarity as kb

    out: dict[str, dict] = {}
    for pattern, keyfile in (
        ("det-b.jsonl", "blind-key.json"),
        ("det-c*.jsonl", "blind302-key.json"),
    ):
        key = json.loads((kb.AUDIT / keyfile).read_text(encoding="utf-8"))
        for p in sorted((NEW / model).glob(pattern)):
            out.update(det_by_task(_jsonl(p), key))
    return out


def coverage() -> None:
    print("== coverage: items each new rater returned")
    for model in FAMILIES:
        load = new_load(model)
        det = new_det(model)
        counts = {b: len(t) for b, t in load.items()}
        print(f"   {FAMILIES[model]}: load {counts}, determinacy {len(det)}")


def agreement() -> None:
    import lsb_kb_clarity as kb
    import procedural_load as pl

    load = {
        "opus r1": pl.read_ratings(pl.RATINGS, "r1"),
        "opus r2": pl.read_ratings(pl.RATINGS, "r2"),
        **{FAMILIES[m]: new_load(m) for m in FAMILIES},
    }
    print(
        "== A. agreement: procedural load "
        "(weighted kappa on load; kappa on rule_matching)"
    )
    for bench in ("dabstep", "livesqlbench"):
        for x, y in combinations(load, 2):
            a, b = load[x].get(bench, {}), load[y].get(bench, {})
            common = sorted(set(a) & set(b))
            if not common:
                continue
            wk = pl.weighted_kappa(
                [a[t]["load"] for t in common], [b[t]["load"] for t in common]
            )
            rk = kappa(
                [a[t]["rule_matching"] for t in common],
                [b[t]["rule_matching"] for t in common],
            )
            print(
                f"   {bench:12s} {x:15s} / {y:15s} {len(common):3d} items: "
                f"load {wk:.2f}, rule_matching {rk:.2f}"
            )

    first, second = kb.all_ratings()
    det = {
        "opus first": first,
        "opus second": second,
        **{FAMILIES[m]: new_det(m) for m in FAMILIES},
    }
    exp2 = set(
        json.loads((kb.AUDIT / "blind-key.json").read_text(encoding="utf-8")).values()
    )
    print("== A. agreement: determinacy (kappa on settled; four-value agreement)")
    for label, among in (("all 302", None), ("experiment 2's 55", exp2)):
        for x, y in combinations(det, 2):
            common = sorted(set(det[x]) & set(det[y]) & (among or set(det[x])))
            if not common:
                continue
            a, b = det[x], det[y]
            k = kappa(
                [kb.settled(a[t]) for t in common], [kb.settled(b[t]) for t in common]
            )
            same = sum(a[t]["kb_clarity"] == b[t]["kb_clarity"] for t in common) / len(
                common
            )
            print(
                f"   {label:18s} {x:15s} / {y:15s} {len(common):3d} tasks: "
                f"kappa {k:.2f}, four values {same:.0%}"
            )


def _alternatives_load() -> dict[str, set[str]]:
    import procedural_load as pl
    from contract_text import rule_matching

    r1 = pl.read_ratings(pl.PUBLIC_RATINGS, "r1")["dabstep"]
    r2 = pl.read_ratings(pl.PUBLIC_RATINGS, "r2")["dabstep"]
    g = new_load("gpt").get("dabstep", {})
    m = new_load("gemini").get("dabstep", {})
    return {
        "paper (opus)": rule_matching(r1, r2),
        "gpt alone": rule_matching(g, g),
        "gemini alone": rule_matching(m, m),
        "cross-family": rule_matching(g, m),
    }


def _alternatives_det() -> dict[str, tuple[dict, dict]]:
    import lsb_kb_clarity as kb

    first, second = kb.all_ratings()
    g, m = new_det("gpt"), new_det("gemini")
    return {
        "paper (opus)": (first, second),
        "gpt alone": (g, g),
        "gemini alone": (m, m),
        "cross-family": (g, m),
    }


def _show(label: str, alt: str, effect: float, ci, v: str, extra: str = "") -> None:
    ci_s = "" if ci is None else f" [{100 * ci[0]:+.1f}, {100 * ci[1]:+.1f}]"
    print(f"   {label:52s} {alt:13s} {100 * effect:+6.1f}{ci_s} {extra:16s} {v}")


def dabstep(tally: Counter) -> None:
    import clustered as cl
    import contract_text as ct
    import heldout_note as hn

    clusterings = cl.dabstep_clusterings()
    alts = _alternatives_load()
    print("== B-C. DABStep subset results (points; interval over question shapes)")
    print("   " + ", ".join(f"{a}: {len(s)} rule-matching" for a, s in alts.items()))
    for model in ("luna", "qwen"):
        ref: dict[str, dict] = {}
        for alt, rated in alts.items():
            _, _, rows = cl.two_facts_contrasts(model, rated, clusterings)
            s5 = ct.scores(model)
            rm = rated & set(s5["contract"])
            luna5 = hn.RUNS["gpt-6-luna"]["exp5"]
            seen = hn.inspected(hn._rows(luna5, 1), hn._rows(luna5, 2), among=rm)
            for label, a, b in (
                (
                    "held out: manual_summed vs manual_resolved",
                    "manual_summed",
                    "manual_resolved",
                ),
                (
                    "held out: contract_text vs manual_summed",
                    "contract_text",
                    "manual_summed",
                ),
            ):
                rows.append(
                    (
                        label,
                        cl.contrast(
                            s5[a], s5[b], rm - seen, runs=3, clusterings=clusterings
                        ),
                    )
                )
            for label, r in rows:
                key = f"{model} {label.split(' on the ')[0]}"
                ci = r["clusters"]["shape"]["ci"]
                if alt == "paper (opus)":
                    ref[key] = r
                    v = "reference"
                else:
                    rr = ref[key]
                    v = verdict(
                        rr["effect"], rr["clusters"]["shape"]["ci"], r["effect"], ci
                    )
                    tally[(alt, v)] += 1
                _show(key, alt, r["effect"], ci, v, f"n={r['n']}")


def livesqlbench(tally: Counter) -> None:
    import clustered as cl
    import lsb_kb_clarity as kb
    import stronger_author as sa
    from dce.stats import load
    from scipy.stats import mannwhitneyu

    score, db_of = kb.main_runs()
    print("== B-C. LiveSQLBench determinacy (consensus, split dropped)")
    ref: dict[str, tuple] = {}
    for alt, (first, second) in _alternatives_det().items():
        gains = cl.determinacy_gains(first, second, score, db_of)
        r = next(
            g
            for label, g, _, unit in gains
            if label == "consensus, split dropped" and unit == "database"
        )
        rated = sorted(set(first) & set(second) & set(db_of))
        know = {
            t: sum(score[m][a][t] for m in kb.MAIN_RUNS for a in kb.KNOWLEDGE_ARMS) / 18
            for t in rated
        }
        group = {t: kb.consensus(first[t], second[t]) for t in rated}
        det = [know[t] for t in rated if group[t] == "determined"]
        und = [know[t] for t in rated if group[t] == "undetermined"]
        p = mannwhitneyu(det, und, alternative="greater").pvalue
        acc_gap = sum(det) / len(det) - sum(und) / len(und)
        results = {
            "accuracy, determined minus undetermined": (acc_gap, None, p),
            "gain in points, determined minus undetermined": (
                r["points_diff"],
                r["points_diff_ci"],
                None,
            ),
            "gain in log-odds, determined minus undetermined": (
                r["logodds_diff"] / 100,
                tuple(x / 100 for x in r["logodds_diff_ci"]),
                None,
            ),
        }
        counts = f"{len(det)}/{len(und)}/{len(rated) - len(det) - len(und)}"
        for label, (effect, ci, pv) in results.items():
            if alt == "paper (opus)":
                ref[label] = (effect, ci, pv)
                v = "reference"
            else:
                e0, c0, p0 = ref[label]
                v = verdict(e0, c0, effect, ci, ref_p=p0, alt_p=pv)
                tally[(alt, v)] += 1
            extra = f"d/u/s {counts}" + ("" if pv is None else f" p={pv:.1g}")
            _show(label, alt, effect, ci, v, extra)

    print("== B-C. experiment 2's 55 tasks, gpt-6-luna (four arms, three repeats)")
    exp = sa.EXPERIMENTS["livesqlbench"]
    repeats = [load(ROOT / "results" / exp["files"](i)) for i in sa.REPEATS]
    s2 = sa.task_scores(repeats, exp["subset"])
    exp2 = set(
        json.loads((kb.AUDIT / "blind-key.json").read_text(encoding="utf-8")).values()
    )
    ref55 = None
    for alt, (first, second) in _alternatives_det().items():
        tasks = sorted(exp2 & set(first) & set(second))
        acc = {t: sum(s2[a][t] for a in kb.ARMS) / 12 for t in tasks}
        group = {t: kb.consensus(first[t], second[t]) for t in tasks}
        det = [acc[t] for t in tasks if group[t] == "determined"]
        und = [acc[t] for t in tasks if group[t] == "undetermined"]
        if not det or not und:
            print(
                f"   {alt}: a group is empty "
                f"({len(det)} determined, {len(und)} undetermined)"
            )
            tally[(alt, "not robust")] += alt != "paper (opus)"
            continue
        p = mannwhitneyu(det, und, alternative="greater").pvalue
        gap = sum(det) / len(det) - sum(und) / len(und)
        if alt == "paper (opus)":
            ref55, v = (gap, p), "reference"
        else:
            assert ref55 is not None
            v = verdict(ref55[0], None, gap, None, ref_p=ref55[1], alt_p=p)
            tally[(alt, v)] += 1
        _show(
            "accuracy, determined minus undetermined",
            alt,
            gap,
            None,
            v,
            f"{len(det)}/{len(und)} p={p:.1g}",
        )


def main() -> None:
    coverage()
    agreement()
    tally: Counter = Counter()
    dabstep(tally)
    livesqlbench(tally)
    print("== D. robustness, per alternative (results significant in the paper)")
    for alt in ("gpt alone", "gemini alone", "cross-family"):
        print(
            f"   {alt:13s} robust {tally[(alt, 'robust')]}, not robust "
            f"{tally[(alt, 'not robust')]}, no claim {tally[(alt, 'no claim')]}"
        )


if __name__ == "__main__":
    main()
