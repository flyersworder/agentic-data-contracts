"""Validate an external verified-examples corpus against the revenue contract.

Run:
    uv run python examples/revenue_agent/setup_db.py        # once — builds the DuckDB
    uv run python examples/revenue_agent/verify_examples.py

What this shows
---------------
``verified_examples.yml`` is an *external* corpus of question -> SQL pairs — the
kind your analytics agent's lessons-learned MR flow produces. The framework never
owns that file; ``validate_examples`` only re-checks each ``sql`` against the
contract, using the SAME two-layer Validator that gates live agent queries:

  * Layer 1 (static)  — allowed tables, forbidden ops, required filters, no SELECT *
  * Layer 2 (dry run) — a live DuckDB ``EXPLAIN``, so schema drift (a dropped or
                        renamed column) is caught even when the static contract
                        cannot see it

Each example lands in exactly one status — ``valid`` (contract-checked and
passed), ``violation`` (a check rejected it), ``unverified`` (engine-planned but
policy not statically checked; see the note below), or ``unchecked`` (no verdict)
— with two flags, ``contract_checked`` and ``engine_checked``, recording *what*
was verified. ``report.ok`` is True only when every example is ``valid``. Extra
YAML keys (``type``, ``verified_by``, ``last_verified``) are preserved untouched
in ``.metadata``.

Two real uses of the same call:
  * MR gate — validate the corpus in CI before a human reviews; ``sys.exit(1)``
    when ``not report.ok``.
  * Drift sweep — re-run against a changed contract; ``report.violations`` are the
    examples the change (or schema drift) just broke.

Note: the decision-B engine fallback (asking the engine to parse SQL sqlglot
cannot) never triggers here — DuckDB and sqlglot both parse standard SQL. It
earns its keep on engines sqlglot does not model, e.g. Denodo/VDP.

Compliance is not correctness — a second pass
-----------------------------------------------
The first pass only proves an example is *allowed*: the right tables, an
explicit column list, a tenant filter, a schema that still matches. None of
that says the query still returns the right *number*. A handful of rows below
also carry an ``expected`` value — the certified answer — and
``check_example_answers`` executes exactly those rows (and only the ones that
came back ``valid`` from the first pass; a contract violation is never run)
and compares the live result against it within tolerance. It takes the first
pass's *report*, not the raw examples, so there is no way to hand it SQL that
was never cleared — an example that fails the tenant-filter rule is precisely
the kind of query that must not be sent to the warehouse to see what it
returns. The result is a distinct verdict for a distinct failure mode: a
`mismatch` on an otherwise-compliant row is a wrong number, not a policy
violation.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

from agentic_data_contracts import DataContract
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.validation import (
    VerifiedExample,
    check_example_answers,
    validate_examples,
)

EXAMPLE_DIR = Path(__file__).parent


def main() -> None:
    contract = DataContract.from_yaml(EXAMPLE_DIR / "contract.yml")
    semantic = YamlSource(EXAMPLE_DIR / "semantic.yml")

    db_path = EXAMPLE_DIR / "sample_data.duckdb"
    sys.path.insert(0, str(EXAMPLE_DIR))
    from setup_db import ensure_sample_db  # type: ignore[import]

    ensure_sample_db(str(db_path))
    sys.path.pop(0)
    adapter = DuckDBAdapter(str(db_path))

    # You own this load step — the framework never reads your corpus for you.
    raw = yaml.safe_load((EXAMPLE_DIR / "verified_examples.yml").read_text())
    examples = [VerifiedExample.from_dict(row) for row in raw]

    report = validate_examples(
        examples,
        contract,
        dialect=adapter.dialect,  # so Layer 1 parses in the engine's dialect
        explain_adapter=adapter,  # enables the live DuckDB EXPLAIN dry run
        semantic_source=semantic,
    )

    print("=== Verified-examples validation (live, DuckDB EXPLAIN) ===\n")
    for r in report.results:
        label = r.example.id or r.example.question or "<unnamed>"
        flags = (
            f"contract_checked={r.contract_checked} engine_checked={r.engine_checked}"
        )
        print(f"[{r.status.upper():9}] {label}  ({flags})")
        for reason in r.reasons:
            print(f"            reason:  {reason}")
        for warning in r.warnings:
            print(f"            warning: {warning}")

    print("\n--- report.summary() (ready to post as an MR comment) ---")
    print(report.summary())

    print(
        f"\nGate: ok={report.ok}  "
        f"({len(report.valid)} valid, {len(report.violations)} violation(s), "
        f"{len(report.unchecked)} unchecked, "
        f"{len(report.unverified_compliance)} plannable-but-unverified)"
    )

    # === Second pass: does the compliant SQL return the right number? ===
    # Consumes `report`, not `examples` — a violation can never be executed.
    # Only rows that are `valid` AND carry an `expected` produce a result.
    print("\n=== Answer checks (does the compliant SQL return the right number?) ===\n")
    answers = check_example_answers(report, adapter=adapter)
    for r in answers.results:
        # r.label, not a recomputed one: the field exists precisely so this
        # listing and the answers.summary() block printed below cannot disagree
        # about what to call an unnamed row.
        print(f"[{r.status.upper():12}] {r.label}")
        if r.status in ("match", "mismatch"):
            print(f"             expected={r.expected}  actual={r.actual}")
        if r.reason:
            print(f"             reason:  {r.reason}")

    print("\n--- answers.summary() (ready to post as an MR comment) ---")
    print(answers.summary())

    print(
        f"\nGate: ok={answers.ok}  "
        f"({len(answers.matches)} match, {len(answers.mismatches)} mismatch(es), "
        f"{len(answers.unassertable)} unassertable, {len(answers.errors)} error(s))"
    )
    # This demo deliberately includes a mismatch (stale certified answer) and an
    # unassertable row (a relative time window) — that is the whole point of
    # the second pass. Like the contract-violation rows above, we REPORT the
    # outcome rather than sys.exit(1); a real CI gate composes both reports:
    #     if not (report.ok and answers.ok): sys.exit(1)


if __name__ == "__main__":
    main()
