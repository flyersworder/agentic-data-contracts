"""Knowledge or delivery: how much of the contract's lead is the one extra fact?

`manual_resolved` is `manual_prompt` plus the one fact the contract took from
the data (the list-typed `fees` fields store "all" as an empty list). This
script reproduces the numbers in FINDINGS.md, *Knowledge or delivery?* and
*... on a weaker model*, for both models:

1. accuracy per repeat and the share of the manual -> contract gap the note
   recovers (end-to-end strict, the headline score);
2. task-level sign tests over the three repeats, each task scored 0-3;
3. the same split over three task groups: the rule-set families, where only
   the right set of fee rules matters; the total-fee families; and the rest;
4. how often a run's SQL treats the empty list as "all", from the traces.
   The detector is a regular expression, so its rates are approximate.

gpt-6-sol's arms come from two files per repeat (the panel and the one-arm
`gpt6-resolved` run); Qwen 3.8 ran all three arms in one file per repeat.

Run:  uv run python analysis/knowledge_delivery.py
"""

from __future__ import annotations

import gzip
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

from scipy.stats import binomtest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from counterfactuals import family_of  # noqa: E402
from dce.stats import _as_e2e, _graded, load  # noqa: E402

ARMS = ("manual_prompt", "manual_resolved", "contract")
REPEATS = (1, 2, 3)
MODELS = {
    "gpt-6-sol": lambda i: [f"gpt6-panel-r{i}", f"gpt6-resolved-r{i}"],
    "qwen3.8-27b": lambda i: [f"qwen38-resolved-r{i}"],
}
RULE_SET = {"avg_fee_account", "avg_fee_account_mcc", "fee_ids_by_at_aci"}
TOTAL_FEE = {"total_fees_day", "total_fees_month", "total_fees_year"}
GROUPS = ("rule-set", "total-fee", "rest")
LIST_COLS = r"[\w.\"]*(account_type|aci|merchant_category_code)\"?"
EMPTY_LIST = re.compile(
    rf"(len|length|array_length|cardinality|list_count)\s*\(\s*{LIST_COLS}\s*\)"
    r"\s*(=|<|<=|>|!=|<>)\s*[01]\b"
    rf"|{LIST_COLS}\s*=\s*\[\s*\]",
    re.IGNORECASE,
)

FAMILY = family_of()


def group(task_id: str) -> str:
    family = FAMILY.get(str(task_id))
    if family in RULE_SET:
        return "rule-set"
    return "total-fee" if family in TOTAL_FEE else "rest"


def rows(model: str, repeat: int) -> list[dict]:
    """End-to-end rows of the three arms for one repeat, with their trace dir."""
    out = []
    for name in MODELS[model](repeat):
        for row in _as_e2e(_graded(load(ROOT / "results" / f"{name}.jsonl"))):
            if row["arm"] in ARMS:
                out.append({**row, "_traces": ROOT / "traces" / name})
    return out


def queries(row: dict) -> list[str]:
    """The SQL of every tool call in the run's trace, in order."""
    path = row["_traces"] / Path(row.get("trace_path") or "").name
    if not path.is_file():
        return []
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        messages = json.load(fh)
    out = []
    for message in messages:
        for part in message.get("parts") or []:
            if part.get("part_kind") != "tool-call":
                continue
            args = part.get("args")
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except json.JSONDecodeError:
                    continue
            if isinstance(args, dict) and args.get("sql"):
                out.append(args["sql"])
    return out


def sign_test(better: int, worse: int) -> str:
    n = better + worse
    p = binomtest(better, n, 0.5).pvalue if n else 1.0
    return f"{better} vs {worse}, p={p:.2g}"


def report(model: str) -> None:
    print(f"== {model}")
    per_repeat = {i: rows(model, i) for i in REPEATS}

    totals: Counter[str] = Counter()
    for i, repeat_rows in per_repeat.items():
        correct = Counter(r["arm"] for r in repeat_rows if r["verdict"] == "correct")
        totals.update(correct)
        lead = correct["contract"] - correct["manual_prompt"]
        note = correct["manual_resolved"] - correct["manual_prompt"]
        share = f"{note / lead:.0%}" if lead else "-"
        print(
            f"r{i}  "
            + "  ".join(f"{a} {correct[a]}" for a in ARMS)
            + f"  share {share}"
        )
    n = len(per_repeat[1]) // len(ARMS)
    lead = totals["contract"] - totals["manual_prompt"]
    note = totals["manual_resolved"] - totals["manual_prompt"]
    print(
        f"k={len(REPEATS)} "
        + "  ".join(f"{a} {totals[a] / (n * len(REPEATS)):.1%}" for a in ARMS)
        + f"  share {note}/{lead} = {note / lead:.0%}"
    )

    # Each task scored 0-3 over the repeats, per arm.
    score: dict[str, Counter[str]] = defaultdict(Counter)
    by_group: dict[str, Counter[str]] = defaultdict(Counter)
    for repeat_rows in per_repeat.values():
        for r in repeat_rows:
            ok = r["verdict"] == "correct"
            score[r["arm"]][r["task_id"]] += ok
            by_group[group(r["task_id"])][r["arm"]] += ok
    tasks = set(score["contract"])
    pairs = (("manual_resolved", "manual_prompt"), ("contract", "manual_resolved"))

    def sign(a: str, b: str, among: set[str]) -> str:
        better = sum(score[a][t] > score[b][t] for t in among)
        worse = sum(score[a][t] < score[b][t] for t in among)
        return sign_test(better, worse)

    for a, b in pairs:
        print(f"sign {a} vs {b}: {sign(a, b, tasks)}")

    print("group      task-runs  " + "  ".join(ARMS))
    for g in GROUPS:
        among = {t for t in tasks if group(t) == g}
        runs = len(among) * len(REPEATS)
        print(
            f"{g:10s} {runs:9d}  "
            + "  ".join(f"{by_group[g][a]:>{len(a)}d}" for a in ARMS)
        )
        for a, b in pairs:
            print(f"{'':12s}sign {a} vs {b}: {sign(a, b, among)}")

    # Empty list read as "all", fee families only.
    print("fee families: some query / last query treats the empty list as 'all'")
    for arm in ARMS:
        c: Counter[str] = Counter()
        for repeat_rows in per_repeat.values():
            for r in repeat_rows:
                if r["arm"] != arm or group(r["task_id"]) == "rest":
                    continue
                sql = queries(r)
                ok = r["verdict"] == "correct"
                any_hit = any(EMPTY_LIST.search(q) for q in sql)
                c["runs"] += 1
                c["any"] += any_hit
                c["any_ok"] += any_hit and ok
                if group(r["task_id"]) == "rule-set":
                    c["rule_runs"] += 1
                    c["rule_last"] += bool(sql) and bool(EMPTY_LIST.search(sql[-1]))
        if not c["runs"]:
            continue
        right = f"{c['any_ok'] / c['any']:.0%}" if c["any"] else "-"
        print(
            f"  {arm:15s} any {c['any']}/{c['runs']} ({c['any'] / c['runs']:.0%}),"
            f" correct when any {right};"
            f" rule-set last query {c['rule_last']}/{c['rule_runs']}"
        )
    print()


def main() -> None:
    for model in MODELS:
        report(model)


if __name__ == "__main__":
    main()
