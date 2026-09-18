"""Does the query derive what the contract says it depends on?

Run:
    uv run python examples/revenue_agent/setup_db.py       # once
    uv run python examples/revenue_agent/check_sensitivity.py

What this shows
---------------
The validator sees *policy* (allowed tables, forbidden operations, required
filters) and *plannability* (a live EXPLAIN). Neither sees a query that is
authorized, parseable, plannable -- and computes the wrong thing.

Both queries below are allowed and both run. One of them forgot
``status = 'completed'``, which the contract states in the metric's ``filters``
and again in its ``sensitivity`` property. Nothing downstream disagrees with
it: the number is plausible, the query is legal, and the agent moves on.

``check_sensitivity`` shadows ``analytics.orders`` with a copy carrying one
extra *pending* order per customer and re-runs each query. A query that filters
to completed orders cannot move. One that does not, moves -- and that is the
violation.

Nothing is written. The shadow is a SELECT spliced in as a CTE; the database is
never modified, and no DDL runs.

This is the fourth validation verb the library contributes:

  * ``validate_examples``     -- is this SQL still *allowed* and *plannable*?
  * ``check_example_answers`` -- does it still return the *right number*?
  * ``check_schema_drift``    -- do the *declarations* still describe reality?
  * ``check_sensitivity``     -- does the query *derive* what it depends on?

A passing property never says the answer is right. It says the query responded
to an input the contract says it depends on.
"""

from __future__ import annotations

import sys
from pathlib import Path

from agentic_data_contracts import DataContract
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.validation import (
    check_sensitivity,
    validate_sensitivity_tables,
)

HERE = Path(__file__).parent

CORRECT = """
SELECT SUM(amount) AS revenue
FROM analytics.orders
WHERE status = 'completed' AND tenant_id = 'acme'
"""

DEFECTIVE = """
SELECT SUM(amount) AS revenue
FROM analytics.orders
WHERE tenant_id = 'acme'
"""


def main() -> int:
    contract = DataContract.from_yaml(HERE / "contract.yml")
    source = YamlSource(str(HERE / "semantic.yml"))
    adapter = DuckDBAdapter(str(HERE / "sample_data.duckdb"))

    # CI-time gate: a shadow reading a table the contract does not govern is a
    # governance hole, and this is where it is caught before anything runs.
    # `dialect=adapter.dialect` so this parses shadows the same way
    # `check_sensitivity` below will.
    problems = validate_sensitivity_tables(
        contract, source.get_metrics(), dialect=adapter.dialect
    )
    if problems:
        print("Shadow reads an ungoverned table:")
        for p in problems:
            print(f"  - {p}")
        return 1

    metric = source.get_metric("total_revenue")
    assert metric is not None

    failed = False
    for label, sql in (
        ("filters to completed", CORRECT),
        ("no status filter", DEFECTIVE),
    ):
        report = check_sensitivity(metric, sql, contract=contract, adapter=adapter)
        print(f"\n{label}:")
        print(report.summary())
        failed = failed or not report.ok

    print("\nThe second query is legal, plannable and wrong. Only the")
    print("behavioural check disagrees with it.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
