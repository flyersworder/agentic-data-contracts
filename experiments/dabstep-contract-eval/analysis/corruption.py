"""Corrupted working copies, governed arms against ungoverned, every DABStep run.

Reproduces the count in FINDINGS.md, *Governance: the ungoverned arms write
to the warehouse*, across every run since. Each run gets its own working copy
of the database, and the harness checks it afterwards (`db_corrupted`). A
governed arm is a contract behind the governed tools (`contract`,
`contract_hollow`, the compiled contracts). The contract's text in the prompt
(`contract_text*`) runs over the plain tools, so it counts as ungoverned.
Smoke files are not runs.

Then the paper's seven models split by model and by whether the arm's prompt
states the forbidden operations (review round 2). Every contract arm does:
the governed arms render the contract's constraints ("Forbidden operations:
DELETE, DROP, ...") into the system prompt, and the `contract_text` arms
paste contract.yml, `forbidden_operations` included. No schema or manual
arm does; the manual and the payments readme say nothing about writes.
Each cell gets an exact (Clopper-Pearson) 95% interval, and a zero cell its
rule-of-three bound beside it.

Run:  uv run python analysis/corruption.py
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

from scipy.stats import fisher_exact

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from clustered import clopper_pearson, rule_of_three  # noqa: E402
from dce.stats import load  # noqa: E402


def governed(arm: str) -> bool:
    return arm.startswith("contract") and not arm.startswith("contract_text")


def states_rules(arm: str) -> bool:
    """The arm's prompt lists the forbidden operations: every contract arm,
    governed or pasted as text."""
    return arm.startswith("contract")


def bound(k: int, n: int) -> tuple[float, float]:
    """The exact 95% interval of a rate; (0, 1) for an empty cell."""
    return (0.0, 1.0) if n == 0 else clopper_pearson(k, n)


def is_run(stem: str) -> bool:
    return "smoke" not in stem


#: The paper's seven models. Two more ran on DABStep and are not reported
#: there: a Qwen 3.6 sweep and a 62-row deepseek-v4-pro probe.
PAPER_MODELS = (
    "gpt-6-sol",
    "gpt-6-luna",
    "qwen3.8-27b",
    "claudesonnet5",
    "z-ai/glm-5.3-flash",
    "deepseek/deepseek-v4-flash",
    "openai/gpt-5.6-sol",
)


def in_paper(model: str) -> bool:
    return model.startswith(PAPER_MODELS)


def main() -> None:
    runs: Counter = Counter()
    bad: Counter = Counter()
    paper_runs: Counter = Counter()
    paper_bad: Counter = Counter()
    where: Counter = Counter()
    strata: Counter = Counter()
    strata_bad: Counter = Counter()
    models: dict[str, set] = {"governed": set(), "ungoverned": set()}
    for path in sorted((ROOT / "results").glob("*.jsonl")):
        if not is_run(path.stem):
            continue
        try:
            rows = load(path)
        except KeyError:  # a score export, not a run file
            continue
        for r in rows:
            if r.get("benchmark", "dabstep") != "dabstep":
                continue
            side = "governed" if governed(r["arm"]) else "ungoverned"
            runs[side] += 1
            models[side].add(r["model"])
            if in_paper(r["model"]):
                paper_runs[side] += 1
                paper_bad[side] += bool(r.get("db_corrupted"))
                cell = (r["model"], states_rules(r["arm"]))
                strata[cell] += 1
                strata_bad[cell] += bool(r.get("db_corrupted"))
                if states_rules(r["arm"]) and not governed(r["arm"]):
                    strata[(r["model"], "text")] += 1
                    strata_bad[(r["model"], "text")] += bool(r.get("db_corrupted"))
            if r.get("db_corrupted"):
                bad[side] += 1
                where[(r["model"], r["arm"])] += 1
    for side in ("governed", "ungoverned"):
        print(
            f"{side:10s} {bad[side]} corrupted of {runs[side]} runs, "
            f"{len(models[side])} models"
        )
    for side in ("governed", "ungoverned"):
        print(
            f"{side:10s} {paper_bad[side]} corrupted of {paper_runs[side]} runs, "
            "the paper's seven models"
        )
    for (model, arm), n in sorted(where.items()):
        print(f"   {model:20s} {arm:20s} {n}")
    stratified(strata, strata_bad)


def stratified(runs: Counter, bad: Counter) -> None:
    """The paper's seven models, by model and by whether the rules were
    stated, with exact bounds."""
    print("== the paper's seven models, by whether the prompt states the rules")
    print(
        f"   {'model':32s} {'stated':>7s} {'corrupted':>9s} {'runs':>6s} "
        f"{'rate':>7s}  95% exact interval   rule of 3"
    )
    totals: Counter = Counter()
    totals_bad: Counter = Counter()
    for model in sorted({m for m, s in runs if s != "text"}):
        for stated in (True, False):
            n, k = runs[(model, stated)], bad[(model, stated)]
            totals[stated] += n
            totals_bad[stated] += k
            print(_cell(model, stated, k, n))
        print(
            f"   {'':32s} Fisher exact, stated vs not: "
            f"p = {_fisher(runs, bad, model):.2g}"
        )
    for stated in (True, False):
        print(_cell("all seven", stated, totals_bad[stated], totals[stated]))
    text = [m for m, s in runs if s == "text"]
    k = sum(bad[(m, "text")] for m in text)
    n = sum(runs[(m, "text")] for m in text)
    lo, hi = bound(k, n)
    print(
        f"   of the stated: the contract's text over the plain tools, no "
        f"enforcement, {len(text)} models: {k} of {n} [{lo:.1e}, {hi:.1e}]"
        + "".join(f"; {m} {bad[(m, 'text')]} of {runs[(m, 'text')]}" for m in text)
    )


def _fisher(runs: Counter, bad: Counter, model: str) -> float:
    table = [
        [bad[(model, s)], runs[(model, s)] - bad[(model, s)]] for s in (True, False)
    ]
    return float(fisher_exact(table)[1])


def _cell(label: str, stated: bool, k: int, n: int) -> str:
    lo, hi = bound(k, n)
    three = f"{rule_of_three(n):.1e}" if k == 0 and n else "-"
    return (
        f"   {label:32s} {'yes' if stated else 'no':>7s} {k:9d} {n:6d} "
        f"{k / n if n else 0:7.2%}  [{lo:.1e}, {hi:.1e}]   {three}"
    )


if __name__ == "__main__":
    main()
