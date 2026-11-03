"""Experiment 5e: the two facts written into the contract, looked up or pasted.

Design: docs/superpowers/specs/2026-11-03-experiment-5e-facts-in-the-contract-
design.md. Six arms together in each repeat, k=3, on gpt-6-luna and
qwen3.8-27b, the 401 golded DABStep tasks: `contract`, `contract_text`,
`contract_plain` and `contract_plain_text` (a 2x2 of route by wording: the
contract looked up or pasted; the two facts in the contract's original
wording, or written in place in the notes' words by `dce.plain`), and
`contract_noted` (5d) and `manual_summed` (5b), co-run.

Contrasts, each on the 199 rule-matching tasks end-to-end (primary) and on
all 401; the official and exact views (`exact_match.py`) are exploratory:
- R, route with the facts in place: `contract_plain` - `contract_plain_text`;
- P, plain wording, governed: `contract_plain` - `contract`;
- H, the headline co-run: `manual_summed` - `contract`;
- exploratory: R0 (`contract` - `contract_text`), the route x wording
  interaction RxW (R minus R0, task by task), `contract_plain` -
  `contract_noted` (facts placed against facts noted, both governed), and F
  (`contract_plain_text` - `manual_summed`).

Every contrast is read with the registered engine (`dce.inference`): the
task-weighted mean difference, the shape-level sign-flip p, 90% and 95%
intervals by test inversion, the shape bootstrap and task signs beside.

Registered, on the rule-matching tasks, end-to-end:
  (49)  luna R: the governed route does not lead by 5 points or more (the
        90% interval's upper end below +5);
  (49Q) Qwen R: the same;
  (50Q) Qwen P: `contract_plain` beats `contract`, shape-level p < 0.05 and
        a positive estimate;
  (51)  luna H: `manual_summed` is not worse than `contract` by 5 points or
        more (the 90% interval's lower end above -5).
  Holm over the four, at the shape level: (49) and (49Q) enter with their
  one-sided p at +5, (51) with its one-sided p at -5, (50Q) with its
  two-sided p.
  Estimates, no verdict: (50) luna P, (51Q) Qwen H.

Drift check: each rerun arm within 3 points of its earlier run on every
subset (`contract` and `contract_noted` against 5d, `contract_text` and
`manual_summed` against 5c); a larger drift is flagged, and that arm's
cross-run comparisons are not made.

Lookup log (exploratory; needs the transcripts): per governed run, whether
`lookup_domain("fees")`, `list_metrics` for the fees domain, and
`lookup_metric` of `total_transaction_fees` or `fee_rule_matches_transaction`
came before the first query naming the `fees` table; and the wrong governed
runs on the tasks the governed arm lost to its pasted twin, split into "never
fetched", "fetched after the first fee query" and "fetched, not used".

Scale (LiveSQLBench, experiment 6's 60 questions, Qwen, k=3):
  run health first: no overflow and no truncated request in any
  `manual_scaled_all` run (one that does is reported and counts wrong);
  (52) `manual_scaled_all` loses no more than 5 points against
       `manual_scaled_1x`: the exact 95% upper bound on runs lost, widened
       by the design effect over the six knowledge pieces, below 5%;
  (53) `contract_scaled_all` uses fewer input tokens per run (fresh plus
       cached) than `manual_scaled_all`: one-sided sign test over the 60
       questions' mean input tokens, p < 0.05. Dollars at Qwen's meter and at
       one fixed price beside.

Files (results/): `exp5e-luna-r{1,2,3}.jsonl`, `exp5e-qwen-r{1,2,3}.jsonl`;
the scale block `scale/exp5e-scale-qwen-r{1,2,3}.jsonl`. Transcripts, for
the lookup log: `exp5e-{luna,qwen}-r{1,2,3}-traces.tgz` in the directory
given to `--traces`, as the cluster job archives them.

Prints aggregates only; the rule-matching ratings are public
(`ratings/procedural-load`).

Run:  uv run python analysis/facts_placed.py [--traces DIR] [--views all]
"""

from __future__ import annotations

import gzip
import json
import re
import sys
import tarfile
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path

from scipy.stats import binomtest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dce import inference as inf  # noqa: E402
from dce.stats import _graded, load  # noqa: E402
from exact_match import VIEWS, scores_from  # noqa: E402

FILES = {"luna": "exp5e-luna-r{}.jsonl", "qwen": "exp5e-qwen-r{}.jsonl"}
SCALE_FILES = "exp5e-scale-qwen-r{}.jsonl"
TRACES = "exp5e-{model}-r{i}-traces.tgz"
REPEATS = (1, 2, 3)
RUNS = len(REPEATS)
ARMS = (
    "contract",
    "contract_text",
    "contract_plain",
    "contract_plain_text",
    "contract_noted",
    "manual_summed",
)
#: name -> (a, b): a's task mean minus b's.
CONTRASTS: dict[str, tuple[str, str]] = {
    "R": ("contract_plain", "contract_plain_text"),
    "P": ("contract_plain", "contract"),
    "H": ("manual_summed", "contract"),
    "R0": ("contract", "contract_text"),
    "placed-noted": ("contract_plain", "contract_noted"),
    "F": ("contract_plain_text", "manual_summed"),
}
#: The route x wording interaction: R minus R0, task by task.
INTERACTION = "RxW"
CONFIRMATORY = ("R", "P", "H")
EXPLORATORY = ("R0", INTERACTION, "placed-noted", "F")
#: label, model, contrast, kind. Holm runs over every kind but "estimate".
PREDICTIONS: tuple[tuple[str, str, str, str], ...] = (
    ("(49)", "luna", "R", "non-superiority"),
    ("(49Q)", "qwen", "R", "non-superiority"),
    ("(50Q)", "qwen", "P", "superiority"),
    ("(51)", "luna", "H", "non-inferiority"),
    ("(50)", "luna", "P", "estimate"),
    ("(51Q)", "qwen", "H", "estimate"),
)
#: Where each rerun arm ran before, for the drift check.
EARLIER = {
    m: {
        "contract": f"exp5d-{m}-r{{}}.jsonl",
        "contract_text": f"exp5c-{m}-r{{}}.jsonl",
        "contract_noted": f"exp5d-{m}-r{{}}.jsonl",
        "manual_summed": f"exp5c-{m}-r{{}}.jsonl",
    }
    for m in FILES
}
DRIFT_POINTS = 0.03
#: The end-to-end view first: it is the primary one.
VIEW_ORDER = ("end-to-end", "official", "exact, full message", "exact, final paragraph")

Score = Mapping[str, Mapping[str, int]]


# ── Contrasts and predictions ───────────────────────────────────────────────


def differences(
    score: Score, name: str, tasks: Iterable[str], runs: int = RUNS
) -> dict[str, float]:
    """Task differences of contrast `name` over `tasks`."""
    tasks = list(tasks)
    if name == INTERACTION:
        r = differences(score, "R", tasks, runs)
        r0 = differences(score, "R0", tasks, runs)
        return {t: r[t] - r0[t] for t in tasks}
    a, b = CONTRASTS[name]
    return inf.task_differences(score[a], score[b], tasks, runs=runs)


def _spec(label: str) -> tuple[str, str, str]:
    for lab, model, name, kind in PREDICTIONS:
        if lab == label:
            return model, name, kind
    raise KeyError(label)


def prediction(
    label: str, score: Score, tasks: Iterable[str], shapes: Mapping[str, str]
) -> dict:
    """One registered prediction on one model's scores: the contrast, the
    verdict (None for an estimate), and the p it enters Holm with."""
    model, name, kind = _spec(label)
    d = differences(score, name, sorted(tasks))
    c = inf.contrast(d, shapes)
    out: dict = {"label": label, "model": model, "contrast": name, "kind": kind}
    out["result"] = c
    if kind == "non-superiority":
        out["holds"] = inf.non_superior(c)
        out["family_p"] = inf.bound_p(d, shapes, inf.BOUND, "less")
    elif kind == "non-inferiority":
        out["holds"] = inf.non_inferior(c)
        out["family_p"] = inf.bound_p(d, shapes, -inf.BOUND, "greater")
    elif kind == "superiority":
        out["holds"] = c.p < 0.05 and c.estimate > 0
        out["family_p"] = c.p
    else:
        out["holds"] = None
    return out


def predictions(
    by_model: Mapping[str, Score], tasks: Iterable[str], shapes: Mapping[str, str]
) -> list[dict]:
    """Every registered prediction whose model ran, Holm-adjusted together."""
    tasks = sorted(tasks)
    out = [
        prediction(label, by_model[model], tasks, shapes)
        for label, model, _, _ in PREDICTIONS
        if model in by_model
    ]
    family = {o["label"]: o["family_p"] for o in out if "family_p" in o}
    for o, adjusted in ((o, inf.holm(family).get(o["label"])) for o in out):
        if adjusted is not None:
            o["holm_p"] = adjusted
    return out


def drift(now: Mapping[str, int], before: Mapping[str, int], among: set[str]) -> dict:
    """A rerun arm against its earlier run on `among`: the gap in accuracy,
    tasks up and down, and whether it exceeds `DRIFT_POINTS`."""
    acc_now = sum(now.get(t, 0) for t in among) / (RUNS * len(among))
    acc_before = sum(before.get(t, 0) for t in among) / (RUNS * len(among))
    gap = acc_now - acc_before
    return {
        "now": acc_now,
        "before": acc_before,
        "gap": gap,
        "up": sum(now.get(t, 0) > before.get(t, 0) for t in among),
        "down": sum(now.get(t, 0) < before.get(t, 0) for t in among),
        "flagged": round(abs(gap), 9) > DRIFT_POINTS,
    }


# ── The lookup log ──────────────────────────────────────────────────────────

GOVERNED = ("contract", "contract_plain", "contract_noted")
#: Each governed arm's pasted twin, for the lost tasks.
PASTED_TWIN = {"contract_plain": "contract_plain_text", "contract": "contract_text"}
SQL_TOOLS = ("run_query", "execute_sql")
FEE_METRICS = ("total_transaction_fees", "fee_rule_matches_transaction")
_FEES_TABLE = re.compile(r"(?<!\w)fees(?!\w)", re.IGNORECASE)


def is_fee_query(sql: str | None) -> bool:
    """The SQL names the `fees` table (bare or schema-qualified)."""
    return bool(sql) and bool(_FEES_TABLE.search(sql or ""))


def _args(part: dict) -> dict:
    args = part.get("args")
    if isinstance(args, dict):
        return args
    try:
        out = json.loads(args or "{}")
    except (TypeError, ValueError):
        return {}
    return out if isinstance(out, dict) else {}


def _fetch(name: str, args: dict) -> str | None:
    if name == "lookup_domain" and args.get("name") == "fees":
        return "domain"
    if name == "list_metrics" and args.get("domain") == "fees":
        return "list"
    if name == "lookup_metric" and args.get("metric_name") in FEE_METRICS:
        return "metric"
    return None


def lookup_log(trace: list[dict]) -> dict:
    """One governed run: which of the three fetches came before its first
    fee query (the whole run if it has none), and whether any came at all."""
    before: set[str] = set()
    ever: set[str] = set()
    fee_query = False
    for message in trace:
        for part in message.get("parts", []):
            if part.get("part_kind") != "tool-call":
                continue
            name, args = part.get("tool_name", ""), _args(part)
            if name in SQL_TOOLS and is_fee_query(args.get("sql")):
                fee_query = True
            kind = _fetch(name, args)
            if kind:
                ever.add(kind)
                if not fee_query:
                    before.add(kind)
    return {
        "fee_query": fee_query,
        "domain": "domain" in before,
        "list": "list" in before,
        "metric": "metric" in before,
        "fetched_before": bool(before),
        "fetched_ever": bool(ever),
    }


def loss_kind(log: dict) -> str:
    if not log["fetched_ever"]:
        return "never fetched"
    if not log["fetched_before"]:
        return "fetched after the first fee query"
    return "fetched, not used"


def read_logs(directory: Path, model: str) -> dict[tuple[int, str, str], dict]:
    """(repeat, task, arm) -> lookup log, for every governed run whose
    transcript is in the repeat archives."""
    out = {}
    for i in REPEATS:
        archive = directory / TRACES.format(model=model, i=i)
        if not archive.is_file():
            continue
        with tarfile.open(archive) as tar:
            for member in tar:
                if not member.isfile() or not member.name.endswith(".json.gz"):
                    continue
                task, arm, _ = Path(member.name).name.split("__")
                if arm not in GOVERNED:
                    continue
                handle = tar.extractfile(member)
                assert handle is not None
                out[(i, task, arm)] = lookup_log(
                    json.loads(gzip.decompress(handle.read()))
                )
    return out


# ── Scale ───────────────────────────────────────────────────────────────────

SCALE_ARMS = (
    "manual_scaled_1x",
    "manual_scaled_all",
    "contract_scaled_1x",
    "contract_scaled_all",
)


def scale_health(rows: Iterable[dict]) -> dict:
    """(52)'s precondition: no `manual_scaled_all` run overflowed the
    context or had its request truncated."""
    rs = [r for r in rows if r.get("arm") == "manual_scaled_all"]
    overflow = sum(r.get("verdict") == "overflow" for r in rs)
    truncated = sum(r.get("verdict") == "truncated" for r in rs)
    return {
        "runs": len(rs),
        "overflow": overflow,
        "truncated": truncated,
        "holds": overflow == 0 and truncated == 0,
    }


def no_drop(
    score: Score, tasks: list[str], piece: Mapping[str, str], *, runs: int
) -> dict:
    """(52): the runs `manual_scaled_all` lost against `manual_scaled_1x`
    (summed over the tasks where 1x led), with an exact 95% upper bound
    widened by the design effect over knowledge pieces."""
    from clustered import clopper_pearson, design_effect

    a, b = score["manual_scaled_1x"], score["manual_scaled_all"]
    lost = {t: max(a.get(t, 0) - b.get(t, 0), 0) for t in tasks}
    k, n = sum(lost.values()), runs * len(tasks)
    _, hi = clopper_pearson(k, n)
    deff = max(design_effect({t: v / runs for t, v in lost.items()}, piece), 1.0)
    _, hi_eff = clopper_pearson(k / deff, n / deff)
    up = sum(a.get(t, 0) > b.get(t, 0) for t in tasks)
    down = sum(a.get(t, 0) < b.get(t, 0) for t in tasks)
    return {
        "lost": k,
        "n": n,
        "upper": hi,
        "design_effect": deff,
        "upper_effective": hi_eff,
        "drop": (sum(a.get(t, 0) for t in tasks) - sum(b.get(t, 0) for t in tasks)) / n,
        "up": up,
        "down": down,
        "p": float(binomtest(up, up + down, 0.5).pvalue) if up + down else 1.0,
        "holds": hi_eff < inf.BOUND,
    }


def _mean_input(runs: list[dict]) -> float:
    return sum(r["fresh"] + r["cached"] for r in runs) / len(runs)


def fewer_tokens(
    costs: Mapping[str, Mapping[str, list[dict]]], tasks: list[str]
) -> dict:
    """(53): per question, `contract_scaled_all`'s mean input tokens against
    `manual_scaled_all`'s; one-sided sign test, ties dropped."""
    gov, prose = costs["contract_scaled_all"], costs["manual_scaled_all"]
    up = sum(_mean_input(gov[t]) < _mean_input(prose[t]) for t in tasks)
    down = sum(_mean_input(gov[t]) > _mean_input(prose[t]) for t in tasks)
    p = (
        float(binomtest(up, up + down, 0.5, alternative="greater").pvalue)
        if up + down
        else 1.0
    )
    return {
        "up": up,
        "down": down,
        "p": p,
        "holds": p < 0.05,
        "governed": sum(_mean_input(gov[t]) for t in tasks) / len(tasks),
        "prose": sum(_mean_input(prose[t]) for t in tasks) / len(tasks),
    }


# ── Reading the files ───────────────────────────────────────────────────────


def _repeats(results: Path, pattern: str) -> list[list[dict]] | None:
    paths = [results / pattern.format(i) for i in REPEATS]
    if not all(p.is_file() for p in paths):
        return None
    return [_graded(load(p)) for p in paths]


def _acc(score: Mapping[str, int], among: set[str]) -> float:
    return sum(score.get(t, 0) for t in among) / (RUNS * len(among))


def _print_prediction(o: dict) -> None:
    c = o["result"]
    verdict = {True: "holds", False: "fails", None: "estimate"}[o["holds"]]
    holm = (
        f"; Holm p = {inf.p_text(o['holm_p'])} (family p = {inf.p_text(o['family_p'])})"
        if "holm_p" in o
        else ""
    )
    print(
        f"{o['label']} {verdict} ({o['kind']}, {o['model']} {o['contrast']}): "
        f"{inf.points(c.estimate)} pts, 90% {inf.interval(c.ci90)}, "
        f"95% {inf.interval(c.ci95)}, shape p = {inf.p_text(c.p)}{holm}"
    )


def _lookup_section(
    logs: dict[tuple[int, str, str], dict],
    correct: dict[tuple[int, str, str], bool],
    score: Score,
    rm: set[str],
) -> None:
    for arm in GOVERNED:
        mine = {k: v for k, v in logs.items() if k[2] == arm}
        if not mine:
            continue
        n = len(mine)
        share = {
            key: sum(v[key] for v in mine.values()) / n
            for key in ("fee_query", "domain", "list", "metric", "fetched_before")
        }
        print(
            f"   {arm}: {n} runs; a fee query {share['fee_query']:.0%}; before it: "
            f"lookup_domain(fees) {share['domain']:.0%}, list_metrics(fees) "
            f"{share['list']:.0%}, fee metric {share['metric']:.0%}, any "
            f"{share['fetched_before']:.0%}"
        )
        twin = PASTED_TWIN.get(arm)
        if twin is None:
            continue
        lost = {t for t in rm if score[arm].get(t, 0) < score[twin].get(t, 0)}
        kinds = Counter(
            loss_kind(log)
            for (i, t, a), log in mine.items()
            if t in lost and not correct.get((i, t, a), False)
        )
        print(
            f"      rule-matching tasks lost to {twin}: {len(lost)}; their wrong "
            f"runs: {dict(sorted(kinds.items()))}"
        )


def report(
    results: Path,
    *,
    shapes: Mapping[str, str],
    rated: set[str],
    traces: Path | None = None,
    views: tuple[str, ...] = VIEW_ORDER,
) -> None:
    by_model: dict[str, dict] = {}
    repeats: dict[str, list[list[dict]]] = {}
    for model, pattern in FILES.items():
        got = _repeats(results, pattern)
        if got is not None:
            repeats[model] = got
            by_model[model] = {
                view: scores_from(got, ARMS, VIEWS[view]) for view in views
            }
    if not by_model:
        print("no experiment 5e results yet")
    common: set[str] | None = None
    for model, got in repeats.items():
        tasks = {str(r["task_id"]) for rows in got for r in rows}
        common = tasks if common is None else common & tasks
        rm = rated & tasks
        subsets = (("rule-matching", rm), ("rest", tasks - rm), ("all", tasks))
        e2e = by_model[model]["end-to-end"]
        print(f"== {model}: accuracy, end-to-end strict, k={RUNS}")
        for label, among in subsets:
            print(
                f"   {label:14s} {len(among):4d} tasks  "
                + "  ".join(f"{a} {_acc(e2e[a], among):.1%}" for a in ARMS)
            )
        for view in views:
            tag = "primary" if view == "end-to-end" else "exploratory"
            print(f"== {model}: contrasts, {view} ({tag})")
            for label, among in (("rule-matching", rm), ("all", tasks)):
                for name in CONFIRMATORY + EXPLORATORY:
                    d = differences(by_model[model][view], name, sorted(among))
                    print("   " + inf.line(f"{name}, {label}", inf.contrast(d, shapes)))

        print(f"== {model}: drift, each rerun arm against its earlier run (end-to-end)")
        for arm, pattern in EARLIER[model].items():
            earlier = _repeats(results, pattern)
            if earlier is None:
                print(f"   {arm}: no earlier files ({pattern.format('*')})")
                continue
            before = scores_from(earlier, (arm,), VIEWS["end-to-end"])[arm]
            for label, among in subsets:
                r = drift(e2e[arm], before, among)
                flag = "  DRIFT: cross-run comparisons not made" if r["flagged"] else ""
                print(
                    f"   {arm} {label}: now {r['now']:.1%}, before {r['before']:.1%}, "
                    f"{inf.points(r['gap'])} pts ({r['up']} / {r['down']}){flag}"
                )

        if traces is not None:
            logs = read_logs(traces, model)
            correct = {
                (i, str(r["task_id"]), r["arm"]): VIEWS["end-to-end"](r)
                for i, rows in zip(REPEATS, got, strict=True)
                for r in rows
            }
            print(f"== {model}: lookup log (exploratory), {len(logs)} governed runs")
            _lookup_section(logs, correct, e2e, rm)

    if by_model and common is not None:
        rm = rated & common
        print(
            f"== registered predictions, rule-matching ({len(rm)} tasks), end-to-end; "
            "Holm over (49), (49Q), (50Q), (51)"
        )
        e2e_by_model = {m: v["end-to-end"] for m, v in by_model.items()}
        for o in predictions(e2e_by_model, rm, shapes):
            _print_prediction(o)

    scale_report(results / "scale")


def scale_report(directory: Path) -> None:
    import disclosure_scale as ds
    import procedures_gap as pg
    from clustered import knowledge_piece

    paths = [directory / SCALE_FILES.format(i) for i in REPEATS]
    if not all(p.is_file() for p in paths):
        print("== scale: no experiment 5e scale results yet")
        return
    rows = [r for p in paths for r in load(p)]
    health = scale_health(rows)
    print(
        f"== scale, qwen3.8-27b: run health of manual_scaled_all: {health['runs']} "
        f"runs, {health['overflow']} overflowed, {health['truncated']} truncated: "
        f"precondition {'holds' if health['holds'] else 'FAILS (counted wrong)'}"
    )
    score = pg.scores(directory, SCALE_FILES)
    costs = pg.run_costs(directory, SCALE_FILES)
    tasks = sorted({t for per in costs.values() for t in per})
    for arm in SCALE_ARMS:
        if arm in score:
            print(f"   {arm:22s} {_acc(score[arm], set(tasks)):6.1%}")
    piece = {t: knowledge_piece(t) for t in tasks}
    r52 = no_drop(score, tasks, piece, runs=RUNS)
    print(
        f"(52) {'holds' if r52['holds'] else 'fails'}: manual_scaled_all lost "
        f"{r52['lost']} of {r52['n']} runs against 1x; exact 95% upper "
        f"{r52['upper']:.1%}, design effect {r52['design_effect']:.2f}, effective "
        f"upper {r52['upper_effective']:.1%}; mean drop {inf.points(r52['drop'])} pts; "
        f"tasks {r52['up']} / {r52['down']}, two-sided p = {inf.p_text(r52['p'])}"
    )
    r53 = fewer_tokens(costs, tasks)
    print(
        f"(53) {'holds' if r53['holds'] else 'fails'}: contract_scaled_all uses fewer "
        f"input tokens on {r53['up']} / {r53['down']} questions, one-sided "
        f"p = {inf.p_text(r53['p'])}; mean input {r53['governed'] / 1000:.0f}k "
        f"against {r53['prose'] / 1000:.0f}k a run"
    )
    print("   cost per run: route m$ (Qwen meters output only), fixed-price m$ cached")
    print("   / uncached")
    for arm in SCALE_ARMS:
        if arm not in costs:
            continue
        rs = [r for t in tasks for r in costs[arm].get(t, [])]
        route = 1000 * sum(r["usd"] for r in rs) / len(rs)
        cached = 1000 * sum(ds._fixed_usd(r, True) for r in rs) / len(rs)
        fresh = 1000 * sum(ds._fixed_usd(r, False) for r in rs) / len(rs)
        print(f"   {arm:22s} {route:7.2f}  {cached:7.2f} / {fresh:7.2f}")


def main() -> None:
    import argparse

    import procedural_load as pl
    from contract_text import rule_matching
    from templates import shape_of

    parser = argparse.ArgumentParser()
    parser.add_argument("--traces", type=Path, default=None)
    parser.add_argument(
        "--views",
        choices=("primary", "all"),
        default="all",
        help="end-to-end only, or the official and exact views beside it",
    )
    args = parser.parse_args()
    first = pl.read_ratings(pl.PUBLIC_RATINGS, "r1")["dabstep"]
    second = pl.read_ratings(pl.PUBLIC_RATINGS, "r2")["dabstep"]
    report(
        ROOT / "results",
        shapes=shape_of(),
        rated=rule_matching(first, second),
        traces=args.traces,
        views=VIEW_ORDER if args.views == "all" else ("end-to-end",),
    )


if __name__ == "__main__":
    main()
