"""Would a metric-level certified example have caught the broken fifth arm?

The first version of `contract_uninterpreted` (commit fb2579c) rewrote
`fee_rule_matches_transaction` to the manual's literal "null means all". That
predicate is valid, plannable SQL over columns that exist, so the schema-drift
preflight, `validate_examples` on hand-written SQL and every other structural
check passed it, and gpt-6-sol scored 16% on hard tasks with it -- below the
bare-schema arm.

v0.56.0 lets a certified example execute a metric's own SQL through a
`{{ metric:NAME }}` placeholder. This script certifies the DABStep fee-ID
tasks that way and runs the check against three contracts: the frozen
contract, the literal-rule fifth arm (fb2579c) and the fifth arm as run
(matching SQL emptied).

THE ANSWERS ARE THE BENCHMARK'S GOLDS, NOT THE CONTRACT'S OUTPUT. Certifying
with the contract under test would make the check circular. Each example is a
`fee_ids_day` or `fee_ids_month` task: the fee IDs that apply to one
merchant's transactions on a day or in a month. Only the transaction-level
predicate is a placeholder; the two monthly bands are written out by hand,
because `fee_rule_matches_merchant_month` takes `:volume`/`:fraud_ratio`
parameters a placeholder cannot bind, and it is identical in all three
contracts anyway.

Run:  uv run python analysis/certify_metrics.py
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from clauses import MONTH_ENDS, MONTH_STARTS  # noqa: E402
from counterfactuals import family_of  # noqa: E402

from agentic_data_contracts import DataContract  # noqa: E402
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter  # noqa: E402
from agentic_data_contracts.validation.examples import (  # noqa: E402
    VerifiedExample,
    check_example_answers,
    validate_examples,
)

LITERAL_COMMIT = "fb2579c"
MONTHS = [
    "January", "February", "March", "April", "May", "June", "July",
    "August", "September", "October", "November", "December",
]  # fmt: skip
_DAY = re.compile(r"For the (\d+)\w* of the year 2023, .* applicable to (\w+)\?")
_MONTH = re.compile(r"applicable Fee IDs for (\w+) in (\w+) 2023\?")

# The manual's bands, boundaries inclusive on both sides as the manual's
# "between A and B" licenses -- the same reading as the contract's own
# fee_rule_matches_merchant_month, spelled out for a bound value.
MONTHLY_BANDS = """
      AND (f.monthly_volume IS NULL OR CASE f.monthly_volume
             WHEN '<100k'   THEN mm.volume <  100000
             WHEN '100k-1m' THEN mm.volume >= 100000  AND mm.volume <= 1000000
             WHEN '1m-5m'   THEN mm.volume >= 1000000 AND mm.volume <= 5000000
             WHEN '>5m'     THEN mm.volume >  5000000 END)
      AND (f.monthly_fraud_level IS NULL OR CASE f.monthly_fraud_level
             WHEN '<7.2%'     THEN 100 * mm.fraud <  7.2
             WHEN '7.2%-7.7%' THEN 100 * mm.fraud >= 7.2 AND 100 * mm.fraud <= 7.7
             WHEN '7.7%-8.3%' THEN 100 * mm.fraud >= 7.7 AND 100 * mm.fraud <= 8.3
             WHEN '>8.3%'     THEN 100 * mm.fraud >  8.3 END)"""


def _month_of(day: int) -> tuple[int, int]:
    return next((s, e) for s, e in zip(MONTH_STARTS, MONTH_ENDS) if s <= day <= e)


def example_sql(merchant: str, days: tuple[int, int], month: tuple[int, int]) -> str:
    """Fee IDs matching any of `merchant`'s transactions in `days`, with the
    monthly bands measured over the natural month `month`."""
    lo, hi = days
    mlo, mhi = month
    return f"""
    WITH mm AS (
      SELECT SUM(eur_amount) AS volume,
             SUM(eur_amount) FILTER (WHERE has_fraudulent_dispute)
               / SUM(eur_amount) AS fraud
      FROM main.payments
      WHERE merchant = '{merchant}' AND year = 2023
        AND day_of_year BETWEEN {mlo} AND {mhi}
    )
    SELECT DISTINCT f.ID
    FROM main.payments p
    JOIN main.merchant_data m ON m.merchant = p.merchant
    CROSS JOIN main.fees f
    CROSS JOIN mm
    WHERE p.merchant = '{merchant}' AND p.year = 2023
      AND p.day_of_year BETWEEN {lo} AND {hi}
      AND {{{{ metric:fee_rule_matches_transaction }}}}{MONTHLY_BANDS}
    ORDER BY f.ID"""


def corpus() -> list[VerifiedExample]:
    fam = family_of()
    tasks = json.loads((ROOT / "data" / "tasks.json").read_text(encoding="utf-8"))
    golds = json.loads((ROOT / "data" / "golds.json").read_text(encoding="utf-8"))
    golds = golds["golds"]
    out = []
    for task in tasks:
        tid, question = task["task_id"], task["question"]
        family = fam.get(tid)
        gold = golds.get(tid, "")
        if family not in ("fee_ids_day", "fee_ids_month") or not gold.strip():
            continue
        if family == "fee_ids_day":
            m = _DAY.search(question)
            if not m:
                continue
            day, merchant = int(m.group(1)), m.group(2)
            days, month = (day, day), _month_of(day)
        else:
            m = _MONTH.search(question)
            if not m or m.group(2) not in MONTHS:
                continue
            merchant = m.group(1)
            i = MONTHS.index(m.group(2))
            month = (MONTH_STARTS[i], MONTH_ENDS[i])
            days = month
        ids = sorted(int(x) for x in gold.split(","))
        out.append(
            VerifiedExample(
                sql=example_sql(merchant, days, month),
                question=question,
                id=f"task-{tid}",
                expected_rows=[[i] for i in ids],
                ordered=True,
            )
        )
    return out


def _literal_contract(tmp: Path) -> Path:
    """The literal-rule fifth arm, exactly as committed at fb2579c."""
    out = tmp / "literal"
    out.mkdir()
    for name in ("contract.yml", "semantic.yml"):
        blob = subprocess.run(
            [
                "git",
                "show",
                f"{LITERAL_COMMIT}:experiments/dabstep-contract-eval/"
                f"contract_uninterpreted/{name}",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
            encoding="utf-8",
        ).stdout
        (out / name).write_text(blob, encoding="utf-8")
    return out / "contract.yml"


def check(label: str, path: Path, examples: list[VerifiedExample], db: Path) -> None:
    contract = DataContract.from_yaml(path)
    source = contract.load_semantic_source()
    adapter = DuckDBAdapter(str(db))
    try:
        report = validate_examples(
            examples, contract, explain_adapter=adapter, semantic_source=source
        )
    except ValueError as exc:
        print(f"{label:34s} REFUSED before running anything: {exc}")
        return
    answers = check_example_answers(report, adapter=adapter)
    statuses = [r.status for r in answers.results]
    counts = {s: statuses.count(s) for s in sorted(set(statuses))}
    print(
        f"{label:34s} validated {len(report.valid)}/{len(examples)}, answers {counts}"
    )
    for r in answers.results:
        if r.status != "match":
            diff = "; ".join(r.row_differences[:2]) if r.row_differences else ""
            print(f"    {r.example.id}: {r.status} {diff[:160]}")


def main() -> None:
    examples = corpus()
    print(f"{len(examples)} certified fee-ID examples, answers from the golds\n")
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        db = tmp / "dabstep.duckdb"
        shutil.copy(ROOT / "data" / "dabstep.duckdb", db)
        for label, path in [
            ("contract (frozen)", ROOT / "contract" / "contract.yml"),
            (f"fifth arm, literal rule ({LITERAL_COMMIT})", _literal_contract(tmp)),
            (
                "fifth arm, SQL emptied (as run)",
                ROOT / "contract_uninterpreted" / "contract.yml",
            ),
        ]:
            check(label, path, examples, db)


if __name__ == "__main__":
    main()
