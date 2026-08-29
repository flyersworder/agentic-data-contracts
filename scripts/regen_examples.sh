#!/usr/bin/env bash
# Regenerate the golden output files the `examples` CI job diffs against.
#
# Run this whenever an example's printed output changes on purpose, and commit
# the result: the diff IS the review artifact. A reviewer who reads it sees
# exactly what the executable documentation now prints — including a section
# that quietly stopped printing anything, which is the failure a marker-grep
# gate cannot see.
#
# The scrub below removes the one value that drifts without anyone changing
# code: `age_days` is measured against today's date. Add a scrub only for
# values that move on their own; anything else moving is a real diff.
set -euo pipefail
cd "$(dirname "$0")/.."

for d in revenue_agent growth_agent ops_agent; do
  # Redirected, not piped: a pipeline would mask the interpreter's exit status.
  uv run python "examples/$d/agent.py" > /tmp/example_raw.txt
  sed -E 's/age_days=[0-9]+/age_days=N/' /tmp/example_raw.txt \
    > "examples/$d/expected_output.txt"
  echo "regenerated examples/$d/expected_output.txt"
done
