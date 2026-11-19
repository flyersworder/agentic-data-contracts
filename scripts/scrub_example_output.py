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
  elapsed time, so it is rounded to whole seconds rather than masked: the
  configured budget stays in the diff, and a session under half a second old
  rounds back to it exactly. It goes negative once a session overruns.
"""

from __future__ import annotations

import re
import sys

_NUMBER = r"-?[0-9]+(?:\.[0-9]+)?"


def scrub(text: str) -> str:
    text = re.sub(r"(?m)^Sample database created at .*\n", "", text)
    text = re.sub(r"age_days=[0-9]+", "age_days=N", text)
    text = re.sub(rf'"elapsed_seconds": {_NUMBER}', '"elapsed_seconds": T', text)
    return re.sub(
        rf'"seconds_remaining": ({_NUMBER})',
        lambda m: f'"seconds_remaining": ~{round(float(m.group(1)))}',
        text,
    )


if __name__ == "__main__":
    sys.stdout.write(scrub(sys.stdin.read()))
