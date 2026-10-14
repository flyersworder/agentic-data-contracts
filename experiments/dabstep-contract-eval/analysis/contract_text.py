"""Experiment 5: does the contract need its tools, or only its text?

Reproduces FINDINGS.md, *Text or tools: predictions (20)-(21)* and its
Qwen replication, *(26)-(27)*. k=3, the 401 golded DABStep tasks;
manual_resolved, contract_text (the contract's two files verbatim in the
prompt, over plain tools) and contract, run together in each repeat.

Primary subset: the rule-matching tasks of the procedural-load test, those
both blind raters marked `rule_matching` (`procedural_load.py`; the ratings
are private, `$PROCEDURAL_LOAD`).

  (20) contract_text beats manual_resolved there, one-sided p < 0.05.
  (21) contract and contract_text tie there: within 5 points and two-sided
       sign test p >= 0.10.

qwen3.8-27b (`--model qwen`):

  (26) as (20).
  (27) contract beats contract_text there, one-sided p < 0.05: on a weaker
       model the governed tools add to the text.

Prints aggregates only.

Run:  uv run python analysis/contract_text.py [--model luna|qwen]
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

from scipy.stats import binomtest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dce.stats import _as_e2e, _graded, load  # noqa: E402

FILES = {"luna": "exp5-luna-r{}.jsonl", "qwen": "exp5-qwen-r{}.jsonl"}
#: model -> (label of the text-vs-prose test, label of the tools-vs-text
#: test, what the second predicts: a tie, or contract ahead).
PREDICTIONS = {"luna": ("(20)", "(21)", "tie"), "qwen": ("(26)", "(27)", "greater")}
ARMS = ("manual_resolved", "contract_text", "contract")
TIE_POINTS = 0.05


def rule_matching(first: dict[str, dict], second: dict[str, dict]) -> set[str]:
    """Tasks both raters marked as matching records against a rule table."""
    return {
        t
        for t in first
        if t in second and first[t]["rule_matching"] and second[t]["rule_matching"]
    }


def is_tie(gap: float, p: float) -> bool:
    return abs(gap) <= TIE_POINTS and p >= 0.10


def scores(model: str) -> dict[str, Counter[str]]:
    score: dict[str, Counter[str]] = {a: Counter() for a in ARMS}
    for i in (1, 2, 3):
        path = ROOT / "results" / FILES[model].format(i)
        for r in _as_e2e(_graded(load(path))):
            if r["arm"] in score:
                score[r["arm"]][str(r["task_id"])] += r["verdict"] == "correct"
    return score


def _sign(score, a: str, b: str, tasks: set[str], alternative: str):
    up = sum(score[a][t] > score[b][t] for t in tasks)
    down = sum(score[a][t] < score[b][t] for t in tasks)
    p = (
        binomtest(up, up + down, 0.5, alternative=alternative).pvalue
        if up + down
        else 1.0
    )
    return up, down, p


def main() -> None:
    import argparse

    import procedural_load as pl

    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=sorted(FILES), default="luna")
    model = parser.parse_args().model
    text_label, tools_label, tools_kind = PREDICTIONS[model]
    score = scores(model)
    tasks = set(score["contract"])
    first = pl.read_ratings(pl.RATINGS, "r1")["dabstep"]
    second = pl.read_ratings(pl.RATINGS, "r2")["dabstep"]
    rm = rule_matching(first, second) & tasks
    acc = {}
    for label, among in (("rule-matching", rm), ("rest", tasks - rm), ("all", tasks)):
        acc[label] = {
            a: sum(score[a][t] for t in among) / (3 * len(among)) for a in ARMS
        }
        print(
            f"{label:14s} {len(among):4d} tasks  "
            + "  ".join(f"{a} {acc[label][a]:.1%}" for a in ARMS)
        )
        for a, b in (
            ("contract_text", "manual_resolved"),
            ("contract", "contract_text"),
        ):
            up, down, p = _sign(score, a, b, among, "two-sided")
            print(f"    {a} vs {b}: {up} / {down}, two-sided p = {p:.2g}")
    up, down, p20 = _sign(score, "contract_text", "manual_resolved", rm, "greater")
    verdict = "holds" if p20 < 0.05 else "fails"
    print(f"{text_label} {verdict}: {up} / {down}, one-sided p = {p20:.2g}")
    gap = acc["rule-matching"]["contract"] - acc["rule-matching"]["contract_text"]
    if tools_kind == "tie":
        up, down, p = _sign(score, "contract", "contract_text", rm, "two-sided")
        holds, sided = is_tie(gap, p), "two-sided"
    else:
        up, down, p = _sign(score, "contract", "contract_text", rm, "greater")
        holds, sided = p < 0.05, "one-sided"
    print(
        f"{tools_label} {'holds' if holds else 'fails'}: gap {gap:+.1%}, "
        f"{up} / {down}, {sided} p = {p:.2g}"
    )


if __name__ == "__main__":
    main()
