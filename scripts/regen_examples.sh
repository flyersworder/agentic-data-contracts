#!/usr/bin/env bash
# Regenerate the golden output files the `examples` CI job diffs against.
#
# Run this whenever an example's printed output changes on purpose, and commit
# the result: the diff IS the review artifact. A reviewer who reads it sees
# exactly what the executable documentation now prints — including a section
# that quietly stopped printing anything, which is the failure a marker-grep
# gate cannot see.
#
# Each example's stdout is passed through scripts/scrub_example_output.py,
# which the CI job also uses, so the golden file and the diff apply the same
# rules. Its docstring lists what is scrubbed and why.
set -euo pipefail
cd "$(dirname "$0")/.."

raw=$(mktemp)
trap 'rm -f "$raw"' EXIT
for d in revenue_agent growth_agent ops_agent; do
  # Redirected, not piped: a pipeline would mask the interpreter's exit status.
  uv run python "examples/$d/agent.py" > "$raw"
  uv run python scripts/scrub_example_output.py < "$raw" \
    > "examples/$d/expected_output.txt"
  echo "regenerated examples/$d/expected_output.txt"
done

# check_drift.py is diffed the same way rather than grepped. Its output is a
# report, and a report that quietly stops naming a finding is exactly what a
# marker grep cannot see -- the same reasoning as above, and the same O(1)
# maintenance: a new drift kind is covered the moment this file is regenerated.
# It exits non-zero when the contract drifts, and the clean contract must not,
# so no `|| true` here: a failure is a real one.
uv run python examples/revenue_agent/check_drift.py \
  > examples/revenue_agent/expected_drift_output.txt
echo "regenerated examples/revenue_agent/expected_drift_output.txt"
