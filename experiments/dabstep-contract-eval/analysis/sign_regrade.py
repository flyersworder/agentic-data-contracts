"""Does the scorer's blindness to sign change any DABStep result?

DABStep's official scorer (`vendor/dabstep_scorer.py`, vendored verbatim)
reads the first number in an answer with a pattern that has no minus sign,
so `5.24` matches a gold of `-5.24` and the other way round. Nine questions
have a negative gold: the change in a merchant's fees under a what-if, which
is negative when the fees fall. An answer that gives the magnitude, or says
"the delta is 5.24", is credited.

This re-grades with the sign enforced. A DABStep row scored correct whose
graded number has the opposite sign to the gold's becomes incorrect, under
the official view and the end-to-end view alike. Each DABStep analysis then
runs twice in a subprocess, as is and re-graded, and only the lines that
differ are printed. Official verdicts stay the paper's headline; this
reports what the defect moves.

Reproduces FINDINGS.md, *The scorer ignores sign*.

Run:  uv run python analysis/sign_regrade.py
"""

from __future__ import annotations

import difflib
import re
import runpy
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import dce.grade  # noqa: E402
import dce.runner  # noqa: E402
import dce.stats  # noqa: E402

#: The scorer's own number pattern (`extract_numeric`), after it strips
#: commas and `$`.
NUMBER = re.compile(r"(\d*\.\d+|\d+\.?\d*)%?")
MINUS = ("-", "−")
ANALYSES = (
    ("analysis/panel_decomposition.py",),
    ("analysis/knowledge_delivery.py",),
    ("analysis/contract_text.py", "--model", "luna"),
    ("analysis/contract_text.py", "--model", "qwen"),
    ("analysis/contract_noted.py", "--model", "luna"),
    ("analysis/contract_noted.py", "--model", "qwen"),
    ("analysis/stronger_author.py",),
    ("analysis/procedural_load.py",),
    ("-m", "dce.stats", "results/glm-full.jsonl"),
    ("-m", "dce.stats", "results/dsflash-full.jsonl"),
    ("-m", "dce.stats", "results/sonnet5-full.jsonl"),
    ("-m", "dce.stats", "results/sol-full.jsonl"),
)

_load = dce.stats.load
_latest = dce.runner.latest_rows
_final = dce.grade.score_final_paragraph


def sign_of(text: str) -> int:
    """The sign of the first number the scorer would read: -1, 0 or 1."""
    t = str(text).replace(",", "").replace("$", "")
    m = NUMBER.search(t)
    if not m or float(m.group(1)) == 0:
        return 0
    return -1 if t[: m.start()].rstrip().endswith(MINUS) else 1


def _opposite(answer: str, gold: str) -> bool:
    a, g = sign_of(answer), sign_of(gold)
    return a != 0 and g != 0 and a != g


def demote(row: dict) -> dict:
    """A DABStep row scored correct on a number of the wrong sign, as
    incorrect; every other row unchanged."""
    if (
        row.get("benchmark", "dabstep") == "dabstep"
        and row.get("verdict") == "correct"
        and row.get("gold")
        and _opposite(row.get("answer", ""), row["gold"])
    ):
        return {**row, "verdict": "incorrect"}
    return row


def strict_final_paragraph(answer: str | None, gold: str) -> bool:
    """The end-to-end reading, with the sign enforced."""
    return _final(answer, gold) and not _opposite(
        dce.grade.final_paragraph(answer), gold
    )


def _strict_latest(path: Path) -> list[dict]:
    return [demote(r) for r in _latest(path)]


def _child(strict: bool, argv: list[str]) -> None:
    # `dce.stats.load` reads through `latest_rows`, and `python -m dce.stats`
    # re-imports it from `dce.runner`, so patching both reaches every reader.
    if strict:
        dce.runner.latest_rows = _strict_latest
        dce.stats.latest_rows = _strict_latest
        dce.grade.score_final_paragraph = strict_final_paragraph
    if argv[0] == "-m":
        sys.argv = [argv[1], *argv[2:]]
        runpy.run_module(argv[1], run_name="__main__", alter_sys=True)
    else:
        sys.argv = list(argv)
        runpy.run_path(str(ROOT / argv[0]), run_name="__main__")


def _run(strict: bool, argv: tuple[str, ...]) -> list[str]:
    cmd = [sys.executable, __file__, "--child", "strict" if strict else "as-is"]
    done = subprocess.run(
        [*cmd, *argv], cwd=ROOT, capture_output=True, text=True, encoding="utf-8"
    )
    if done.returncode:
        return [f"FAILED ({done.returncode}): {done.stderr.strip().splitlines()[-1]}"]
    return done.stdout.splitlines()


def main() -> None:
    demoted: Counter = Counter()
    tasks: set = set()
    for path in sorted((ROOT / "results").glob("*.jsonl")):
        try:
            rows = _load(path)
        except KeyError:  # not a run file (score exports have no arm)
            continue
        for r in rows:
            if demote(r) is not r:
                demoted[(path.stem, r["arm"])] += 1
                tasks.add(str(r["task_id"]))
    print(
        f"== official verdicts on the wrong sign: {sum(demoted.values())} rows, "
        f"{len(tasks)} tasks ({', '.join(sorted(tasks, key=int))})"
    )
    for (stem, arm), n in sorted(demoted.items()):
        print(f"   {stem:28s} {arm:22s} {n}")
    for argv in ANALYSES:
        before, after = _run(False, argv), _run(True, argv)
        diff = [
            line
            for line in difflib.unified_diff(before, after, lineterm="", n=0)
            if line[:1] in "+-" and line[:3] not in ("+++", "---")
        ]
        print(f"== {' '.join(argv)}: {len(diff) // 2 if diff else 0} lines change")
        for line in diff:
            print(f"   {line}")


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "--child":
        _child(sys.argv[2] == "strict", sys.argv[3:])
    else:
        main()
