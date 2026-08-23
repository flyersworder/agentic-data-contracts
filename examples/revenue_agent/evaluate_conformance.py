"""Pass 3 over the revenue corpus: can an agent reach the certified answer?

``verify_examples.py`` (in this same directory) checks whether a corpus of
question -> SQL pairs is *contract-compliant* and, for rows with an
``expected`` value, whether the SQL *returns the right number*. Neither of
those passes says anything about the AGENT: did it consult the governed
metric definition before writing SQL, and did it stay inside the contract
while doing so? That is what ``evaluate_conformance`` scores, from a
recorded ``ContractSession`` rather than from the raw SQL.

Run:
    uv run python examples/revenue_agent/setup_db.py        # once — builds the DuckDB
    uv run python examples/revenue_agent/evaluate_conformance.py

Runs with no API key and no network. The "agent" here is a scripted function
(``_scripted_agent`` below) that, for each corpus row, calls ``lookup_metric``
for every name the row declares under ``expects_metrics`` and then calls
``run_query`` with the row's certified SQL. The point is to show the harness
end to end — how an ``Attempt`` is built from a session's recorded tool
calls, and how ``evaluate_conformance`` scores it — not to benchmark a model.
Swap ``_scripted_agent`` for a real agent loop to get a real measurement.

Only one corpus row declares ``expects_metrics`` (``acme-completed-revenue``,
whose SQL is exactly the governed ``total_revenue`` definition — see
``verified_examples.yml``); rows that name no metric still make a valid
``Attempt`` — ``evaluate_conformance`` simply has nothing to check against, so
they score on protocol alone. A row whose ``question`` is empty cannot be
evaluated (there is nothing for an agent to have been asked) and is skipped;
this demo's corpus does not have any, but the skip count is still reported so
an unevaluatable corpus is visible rather than silently shrinking the run.

Like ``verify_examples.py``, this script does NOT ``sys.exit(1)`` when the
report is not clean — it prints ``report.ok`` and returns. The shipped
corpus deliberately contains two contract violations (a missing ``tenant_id``
filter and a ``SELECT *``), so a real agent run over it would legitimately
score short of 100%; a demo that treats that as a crash would look broken
while the harness is doing exactly its job. A real CI gate composes the
verdict itself, e.g.:
    if not report.ok: sys.exit(1)
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import yaml

from agentic_data_contracts import DataContract
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools
from agentic_data_contracts.validation import (
    Attempt,
    ToolRecorder,
    VerifiedExample,
    evaluate_conformance,
)

EXAMPLE_DIR = Path(__file__).parent


async def _scripted_agent(example: VerifiedExample, tools: dict) -> str:
    """Stand-in for a model: consult declared metrics, then run the query."""
    for metric in example.expects_metrics:
        await tools["lookup_metric"]({"metric_name": metric})
    await tools["run_query"]({"sql": example.sql})
    return "answered"


async def main() -> None:
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
    all_examples = [VerifiedExample.from_dict(row) for row in raw]

    # A row with no question cannot be evaluated — there is nothing an agent
    # was asked. Report the skip rather than let the corpus silently shrink.
    corpus = [ex for ex in all_examples if ex.question]
    skipped = len(all_examples) - len(corpus)

    attempts = []
    for example in corpus:
        # One recorder per row: a ToolRecorder serves exactly one attempt and
        # refuses a second read, so the session is rebuilt for each example.
        session = ContractSession(contract, recorder=ToolRecorder())
        tools = {
            t.name: t.callable
            for t in create_tools(
                contract,
                adapter=adapter,
                semantic_source=semantic,
                session=session,
            )
        }
        final_text = await _scripted_agent(example, tools)
        attempts.append(Attempt.from_session(example, session, final_text=final_text))

    print("=== Conformance evaluation (scripted agent, no API key) ===\n")
    print(
        f"Evaluated {len(attempts)} attempt(s); skipped {skipped} row(s) with no question.\n"
    )

    report = evaluate_conformance(attempts)
    print(report.summary())

    print(
        f"\nGate: ok={report.ok}  ({len(report.passed)}/{len(report.results)} passed)"
    )
    # This demo deliberately does NOT sys.exit(1) when report.ok is False. The
    # shipped corpus intentionally includes contract violations (see the
    # module docstring), so a scripted agent run over it correctly scores
    # short of 100% — that is the harness working, not the demo being broken.
    # A real CI gate composes the verdict itself:
    #     if not report.ok: sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
