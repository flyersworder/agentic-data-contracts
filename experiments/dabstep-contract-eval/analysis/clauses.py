"""Does the contract's vocabulary reach the SQL the agent actually runs?

`analysis/counterfactuals.py` shows that agent failures cannot be attributed to
identifiable missing semantics. This module asks the prior question — not why
an attempt failed, but whether the contract's clauses appear in the query at
all — by reading the SQL out of the transcripts rather than inferring anything
from the answers.

WHAT THIS MEASURES, AND WHAT IT DOES NOT. Between arms it is sharp: on tasks
that need the same six clauses, the contract arm writes them far more often
than an arm handed the same knowledge as prose. Within an arm it is null:
attempts that got the task right and attempts that got it wrong write the same
clauses (`--within` prints this, so the limitation travels with the tool).

The naive form of this analysis is badly confounded and looks great. Pooling
tasks, contract-arm attempts that wrote the NULL-wildcard clause were correct
90% of the time against 23% for those that did not — but different families
need different clauses, so "wrote the clause" partly encodes "drew an easier
family". Restricting to one required-clause set removes the confound and the
within-arm effect disappears entirely. Only the between-arm comparison below
holds tasks constant, and it is the only one to quote.

Run:  uv run python analysis/clauses.py [--within] [traces/<run> ...]
"""

from __future__ import annotations

import gzip
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from counterfactuals import family_of  # noqa: E402
from dce.stats import ANSWER_VERDICTS, load  # noqa: E402

ARMS = ["schema_only", "contract_hollow", "manual_prompt", "contract"]

# The ungoverned arms get a raw `execute_sql`; the governed arms get
# `run_query` / `inspect_query`. Miss either name and that arm reads as having
# written no SQL at all -- which looks like a dramatic finding rather than the
# bug it is.
SQL_TOOLS = frozenset({"run_query", "inspect_query", "execute_sql"})

# One detector per load-bearing clause of the compiled macro. Deliberately
# permissive: the question is whether the agent expressed the idea at all, so a
# detector that demands the contract's exact phrasing would measure copying
# rather than use.
CLAUSES: dict[str, str] = {
    "null_wildcard": r"IS\s+NULL\s+OR",
    "emptylist_wildcard": r"(len|array_length|cardinality)\s*\([^)]*\)\s*=\s*0",
    "capture_delay_band": r"'(<3|3-5|>5)'",
    "monthly_aggregate": r"(date_trunc|month)\b[\s\S]{0,400}?GROUP\s+BY",
    "natural_month": r"day_of_year\s*-\s*1|make_date",
    "fraud_volume": r"has_fraudulent_dispute",
}

# The families whose answer requires joining payments to fees, and therefore
# every clause above. The fee-table-only families (`avg_fee_*`,
# `fee_ids_by_at_aci`) need no monthly aggregate and are excluded, so every row
# compared here has the same clause requirement.
PAYMENTS_FAMILIES = frozenset(
    {
        "total_fees_day",
        "total_fees_month",
        "total_fees_year",
        "fee_ids_day",
        "fee_ids_month",
        "fee_ids_year",
        "merchants_for_fee",
    }
)


def sql_of(path: Path) -> str:
    """Every SQL string the agent submitted, concatenated.

    A model sometimes emits tool-call arguments that are not valid JSON. The
    raw string is kept in that case rather than dropped: the clause detectors
    are text patterns, and discarding a malformed call would undercount the
    SQL of exactly the arms that malform most -- a bias in the measurement
    dressed up as a result.
    """
    out: list[str] = []
    for message in json.load(gzip.open(path)):
        for part in message.get("parts", []):
            if part.get("part_kind") != "tool-call":
                continue
            if part.get("tool_name") not in SQL_TOOLS:
                continue
            args = part.get("args")
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except json.JSONDecodeError:
                    out.append(args)
                    continue
            if isinstance(args, dict) and args.get("sql"):
                out.append(str(args["sql"]))
    return "\n".join(out)


def clauses_in(sql: str) -> set[str]:
    return {n for n, rx in CLAUSES.items() if re.search(rx, sql, re.I)}


def trace_for(run: Path, task_id: str, arm: str) -> Path | None:
    hits = sorted(run.glob(f"{task_id}__{arm}__*.json.gz"))
    return hits[0] if hits else None


def collect(run: Path, results: Path) -> tuple[str, dict]:
    fam = family_of()
    rows = [
        r
        for r in load(results)
        if r.get("level") == "hard"
        and r.get("verdict") in ANSWER_VERDICTS
        and fam.get(str(r["task_id"])) in PAYMENTS_FAMILIES
    ]
    model = sorted({r["model"] for r in rows})[0].split("/")[-1] if rows else "?"
    # Complete cases only: a task must have an answer from every arm, or the
    # arms are no longer being compared on the same tasks.
    seen: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        seen[str(r["task_id"])].add(r["arm"])
    complete = {t for t, a in seen.items() if a >= set(ARMS)}

    stats: dict = {
        "by_arm": defaultdict(Counter),
        "by_arm_verdict": defaultdict(Counter),
        "missing": 0,
        "tasks": len(complete),
    }
    for r in rows:
        tid, arm = str(r["task_id"]), r["arm"]
        if tid not in complete:
            continue
        path = trace_for(run, tid, arm)
        if path is None:
            stats["missing"] += 1
            continue
        got = len(clauses_in(sql_of(path)))
        for key in (stats["by_arm"][arm], stats["by_arm_verdict"][arm, r["verdict"]]):
            key["n"] += 1
            key["clauses"] += got
            key["all"] += got == len(CLAUSES)
    return model, stats


def main(argv: list[str]) -> None:
    within = "--within" in argv
    runs = [a for a in argv if not a.startswith("--")] or ["glm-full", "dsflash-full"]
    try:
        from scipy.stats import fisher_exact
    except ImportError:  # pragma: no cover
        fisher_exact = None

    k = len(CLAUSES)
    for name in runs:
        run = ROOT / "traces" / name
        results = ROOT / "results" / f"{name}.jsonl"
        if not run.is_dir() or not results.exists():
            print(f"skipping {name}: no traces or no results file")
            continue
        model, s = collect(run, results)
        print(
            f"\n{model} — {s['tasks']} tasks needing all {k} clauses"
            + (f", {s['missing']} traces missing" if s["missing"] else "")
        )
        print(f"{'arm':18s}{'n':>5s}{'mean clauses':>14s}{'wrote all':>11s}")
        for arm in ARMS:
            c = s["by_arm"][arm]
            if not c["n"]:
                continue
            print(
                f"{arm:18s}{c['n']:5d}{c['clauses'] / c['n']:10.2f}/{k}"
                f"{100 * c['all'] / c['n']:10.0f}%"
            )
        if fisher_exact is not None:
            ct = s["by_arm"]["contract"]
            for arm in ARMS:
                if arm == "contract" or not s["by_arm"][arm]["n"]:
                    continue
                o = s["by_arm"][arm]
                _, p = fisher_exact(
                    [[ct["all"], ct["n"] - ct["all"]], [o["all"], o["n"] - o["all"]]]
                )
                print(f"  contract vs {arm:18s} wrote-all Fisher p={p:.2g}")
        if within:
            print(
                "\n  within-arm (THE NULL RESULT: correct and incorrect "
                "attempts write the same clauses)"
            )
            for arm in ARMS:
                a = s["by_arm_verdict"][arm, "correct"]
                b = s["by_arm_verdict"][arm, "incorrect"]
                if not a["n"] or not b["n"]:
                    continue
                line = (
                    f"  {arm:18s} correct {a['clauses'] / a['n']:.2f}/{k} "
                    f"(n={a['n']})   incorrect {b['clauses'] / b['n']:.2f}/{k} "
                    f"(n={b['n']})"
                )
                if fisher_exact is not None:
                    _, p = fisher_exact(
                        [[a["all"], a["n"] - a["all"]], [b["all"], b["n"] - b["all"]]]
                    )
                    line += f"   wrote-all p={p:.2g}"
                print(line)


if __name__ == "__main__":
    main(sys.argv[1:])
