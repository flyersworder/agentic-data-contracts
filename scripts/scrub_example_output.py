"""Scrub an example's stdout of values that change without any code change.

Reads stdin, writes stdout. Shared by ``scripts/regen_examples.sh`` (writing a
golden file) and the ``examples`` CI job (diffing against one), so both apply
the same rules. Add a rule only for a value that moves on its own; anything
else moving is a real diff.

- ``Sample database created at <abs path>`` prints only when the generated
  .duckdb is absent (a fresh CI checkout) and carries a machine path. It is
  setup noise, not a demonstrated behaviour.
- ``age_days`` is measured against today's date.
- ``elapsed_seconds`` in a session report is wall-clock time since the
  session's first call: 0.0 locally, but a slow runner crosses 0.05s.
- ``seconds_remaining`` is the configured ``max_duration_seconds`` minus that
  time. It is rewritten as ``<budget>-T``, the budget recovered as the sum of
  the two values in the same report (each rounded to 0.1s, so the sum is
  within 0.1s of it, however slow the runner), to the whole second: the
  configured budget stays in the diff, and nothing else about timing does.
  Whole seconds is the precision that error allows, so a budget that differs
  from another by less than a second (30 vs 30.5) is not told apart; every
  example declares a whole number.

The keys are matched in either quote style, since an example may print the
report as JSON or as a Python dict.
"""

from __future__ import annotations

import re
import sys

_NUMBER = r"-?[0-9]+(?:\.[0-9]+)?"
_Q = r"[\"']"
# elapsed_seconds and seconds_remaining in one report: no brace between them.
_REPORT = re.compile(
    rf"({_Q})elapsed_seconds\1: ({_NUMBER})"
    rf"([^{{}}]*?)\1seconds_remaining\1: ({_NUMBER})"
)


def _budget(m: re.Match[str]) -> str:
    q, elapsed, between, remaining = m.groups()
    budget = round(float(elapsed) + float(remaining))
    return f"{q}elapsed_seconds{q}: T{between}{q}seconds_remaining{q}: {budget}-T"


def scrub(text: str) -> str:
    text = re.sub(r"(?m)^Sample database created at .*(?:\n|\Z)", "", text)
    text = re.sub(r"age_days=[0-9]+", "age_days=N", text)
    text = _REPORT.sub(_budget, text)
    # A report without both keys has no budget to recover: mask what is left.
    return re.sub(
        rf"({_Q})(elapsed_seconds|seconds_remaining)\1: {_NUMBER}(?![0-9.]|-T)",
        r"\1\2\1: T",
        text,
    )


if __name__ == "__main__":
    sys.stdout.write(scrub(sys.stdin.read()))
