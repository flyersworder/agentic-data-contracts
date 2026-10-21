"""Corrupted working copies, governed arms against ungoverned, every DABStep run.

Reproduces the count in FINDINGS.md, *Governance: the ungoverned arms write
to the warehouse*, across every run since. Each run gets its own working copy
of the database, and the harness checks it afterwards (`db_corrupted`). A
governed arm is a contract behind the governed tools (`contract`,
`contract_hollow`, the compiled contracts). The contract's text in the prompt
(`contract_text*`) runs over the plain tools, so it counts as ungoverned.
Smoke files are not runs.

Run:  uv run python analysis/corruption.py
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dce.stats import load  # noqa: E402


def governed(arm: str) -> bool:
    return arm.startswith("contract") and not arm.startswith("contract_text")


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


if __name__ == "__main__":
    main()
