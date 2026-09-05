"""How much of each arm's accuracy rests on the scorer's tolerance?

The leaderboard submission (see `FINDINGS.md`) graded the contract arm's 450
answers against DABStep's withheld golds and disagreed with our reconstructed
golds on 19 of 401 tasks — every one of them lenient here and wrong
officially. The cause is a wrong reconstructed gold, not a lenient scorer:
Adyen grades with this same vendored file, and `analysis/gold_disagreement.py`
shows it calls all 19 correct against our golds. What the scorer's tolerance
did was let a wrong value pass a wrong gold.

That measures the bias on ONE arm and ONE model. This script bounds it on the
rest, without a second submission. Every one of the 19 failed *exact match*
against the reconstructed gold and needed the vendored scorer's tolerance to
be graded correct — a necessary condition for a bad gold to hide a bad answer
— so exact-match failure among locally-correct rows over-counts, but never
misses, an arm's exposure.

Exact match is deliberately crude: strip surrounding whitespace and Markdown
bold, compare the raw `answer` to `gold`. It is NOT `answer_normalized` —
that field applies the benchmark normaliser, which is the very tolerance
being measured, and using it would hide roughly a third of the exposure.

Calibration on the submitted file (`results/glm-all450.jsonl`, hard split):
33 flags, 19 of them real errors, so a flag converts to an error at 0.576 and
recall is 19/19. Both splits together give 40 flags at 0.475, which mixes an
easy split contributing 7 flags and no errors — use the hard-split rate.

Run:  uv run python analysis/leniency.py [results/*.jsonl ...]
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dce.stats import ANSWER_VERDICTS, load  # noqa: E402

ARMS = ["contract", "contract_hollow", "manual_prompt", "schema_only"]

#: Flags that convert to a real error, measured on the submitted file's hard
#: split. Applying it to another arm or model assumes it transfers, which is
#: exactly what a second submission would test and this script cannot.
CONVERSION = 19 / 33

_BOLD = re.compile(r"^\*+|\*+$")


def strip(value: str | None) -> str:
    """Whitespace and Markdown bold only — never the benchmark normaliser."""
    return _BOLD.sub("", (value or "").strip()).strip()


def flagged(row: dict) -> bool:
    """True if grading this row correct needed more than an exact match."""
    return strip(row.get("answer")) != strip(row.get("gold"))


def main(paths: list[str]) -> None:
    for path in paths:
        # `dce.stats.load` is the ONE deduplicating reader; a raw parse
        # double-counts the rows a resume re-ran.
        hard = [r for r in load(Path(path)) if r.get("level") == "hard"]
        if not hard:
            continue
        # Complete-case intersection, matching how FINDINGS.md reports run B:
        # a task counts only where every arm produced a graded answer, so the
        # four arms share one denominator and the contrasts stay paired.
        graded_ids = [
            {
                r["task_id"]
                for r in hard
                if r["arm"] == a and r["verdict"] in ANSWER_VERDICTS
            }
            for a in ARMS
        ]
        keep = set.intersection(*graded_ids)
        hard = [r for r in hard if r["task_id"] in keep]
        print(f"\n{path}  ({len(keep)} hard tasks, complete-case)")
        print("  arm                  n  correct  flags  exposure     raw  corrected")
        for arm in ARMS:
            graded = [
                r for r in hard if r["arm"] == arm and r["verdict"] in ANSWER_VERDICTS
            ]
            correct = [r for r in graded if r["verdict"] == "correct"]
            if not correct:
                continue
            total = len(keep)
            flags = [r for r in correct if flagged(r)]
            adjusted = len(correct) - CONVERSION * len(flags)
            print(
                f"  {arm:16s} {total:5d} {len(correct):8d} {len(flags):6d} "
                f"{100 * len(flags) / len(correct):8.1f}% "
                f"{100 * len(correct) / total:6.1f}% {100 * adjusted / total:9.1f}%"
            )


if __name__ == "__main__":
    args = sys.argv[1:] or [
        str(p) for p in sorted((ROOT / "results").glob("*-full.jsonl"))
    ]
    main(args)
