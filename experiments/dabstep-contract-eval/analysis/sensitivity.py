"""Does the agent's query DERIVE what the contract says it must, or freeze it?

`analysis/clauses.py` reads the SQL and asks whether the contract's clauses
appear in it. `analysis/counterfactuals.py` reads the answer and asks which
named convention would produce it. Both are text instruments, and within the
`contract` arm both are null: attempts that got the task right and attempts
that got it wrong look the same (`clauses.py --within`, every Fisher p >= 0.54;
a smell detector for band literals, p ~ 1 on all four models).

This module reads BEHAVIOUR instead. It mutates the data in a way the contract
itself dictates, re-runs the agent's own query, and asks whether the answer
MOVED. It never computes a correct answer, so it is not the compiled macro in
disguise -- it cannot answer a task, only observe whether a query responds to a
change the contract says it must respond to.

WHAT IT FINDS. `volume_x10` scales every transaction amount by ten, moving
every merchant-month across a `monthly_volume` band boundary. A query that
derives the band tracks the move; a query that computed the volume in an
earlier turn, read the number, decided the band itself and pasted it in as a
string literal (`f.monthly_volume = '100k-1m'`) does not. That construction is
sanctioned by the contract's own prose -- `fee_rule_matches_merchant_month`
says to substitute `:volume` with "a number of euros, OR that metric's SQL" --
and only one of the two branches is correct, because a value on a band boundary
satisfies BOTH adjacent bands and a materialised number can only pick one.

WHAT IT MEASURES THAT DABSTEP CANNOT. Freezing the band gives the right answer
on this data and the wrong answer one boundary away, so the benchmark scores it
correct: FINDINGS records that the `exclusive_bands` lesion is diagnostic on no
golded task "because no golded task sits exactly on a band boundary". Graded
against DABStep's labels the detector looks terrible (precision 0.24 on Sonnet
5). Graded against the compiled macro ON THE MUTATED DATA -- the same
instrument, 176/176 on base -- every insensitive query that DABStep scored
correct is wrong: 0 of 31, against 103 of 112 for the sensitive ones, Fisher
exact p = 1.2e-23. The yardstick was the benchmark's, and the benchmark is
blind here.

WHAT IT DOES NOT MEASURE. Sensitivity is necessary, not sufficient: 9 of the
112 sensitive queries also fail on the mutant. And `volume_x10` shows a query
does not track the band AT ALL, which is stronger than a boundary failure but
is not the same test. The `null_kill` / `empty_kill` mutations are reported for
completeness and are near-saturated on the `contract` arm (98-100% move), which
independently replicates counterfactuals.py's "the contract eliminates
precisely the misconceptions it documents" by a behavioural route.

Mutants are built into a temporary directory from a copy of
`data/dabstep.duckdb`; the source database is opened read-only and never
written. Each mutation asserts the exact number of rows it must touch, so a
change to the data fails loudly here rather than silently mutating nothing.

Run:  uv run python analysis/sensitivity.py [--arms] [--keep DIR] [run ...]
"""

from __future__ import annotations

import gzip
import json
import re
import shutil
import sys
import tempfile
import threading
from collections import Counter, defaultdict
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import coverage  # noqa: E402
from clauses import ARMS, SQL_TOOLS, trace_for  # noqa: E402
from counterfactuals import family_of  # noqa: E402
from coverage import DB, FAMILIES, PAYMENTS_FAMILIES  # noqa: E402
from dce.stats import ANSWER_VERDICTS, load  # noqa: E402

#: A query that never finishes is a measurement problem, not a finding. 25s is
#: ~50x the median; every timeout observed was an unbounded cross join the agent
#: also failed to get an answer from.
TIMEOUT = 25.0

#: The three fee-ID families are the only ones this analysis grades against the
#: macro on mutated data. Their answer is a SET OF FEE IDS, so scaling
#: `eur_amount` cannot leak into the answer through the fee arithmetic -- the
#: only channel by which the answer can move is band membership, which is
#: exactly the property under test. `total_fees_*` answers are euro amounts and
#: move under `volume_x10` for the trivial reason that the amounts changed.
FEE_ID_FAMILIES = frozenset({"fee_ids_day", "fee_ids_month", "fee_ids_year"})

#: (statement, exact rows it must affect). Each is derived from one sentence of
#: the frozen contract, named in the comment; the count is asserted so a change
#: to `data/dabstep.duckdb` fails loudly rather than producing a mutant that
#: mutates nothing and therefore reports every query as insensitive.
MUTATIONS: dict[str, list[tuple[str, int]]] = {
    # `fee_rule_matches_transaction`: "if a field is set to null it means that
    # it applies to all possible values of that field". Replace NULL with a
    # sentinel no transaction can match: a query reading NULL as a wildcard
    # LOSES matches, one reading it as "matches nothing" was already excluding
    # them and does not move.
    "null_kill": [
        (
            "UPDATE main.fees SET capture_delay = '__none__'"
            " WHERE capture_delay IS NULL",
            500,
        ),
        (
            "UPDATE main.fees SET monthly_volume = '__none__'"
            " WHERE monthly_volume IS NULL",
            800,
        ),
        (
            "UPDATE main.fees SET monthly_fraud_level = '__none__'"
            " WHERE monthly_fraud_level IS NULL",
            900,
        ),
        ("UPDATE main.fees SET intracountry = -1.0 WHERE intracountry IS NULL", 561),
    ],
    # Same sentence, plus `fee_rule_matches_transaction`'s INTERPRETATION note:
    # the list-typed fields "express 'applies to all values' as an empty list".
    "empty_kill": [
        (
            "UPDATE main.fees SET aci = ['__none__']"
            " WHERE aci IS NOT NULL AND len(aci) = 0",
            112,
        ),
        (
            "UPDATE main.fees SET account_type = ['__none__']"
            " WHERE account_type IS NOT NULL AND len(account_type) = 0",
            720,
        ),
        (
            "UPDATE main.fees SET merchant_category_code = [-1]"
            " WHERE merchant_category_code IS NOT NULL"
            " AND len(merchant_category_code) = 0",
            127,
        ),
    ],
    # `merchant_monthly_volume` feeding `fee_rule_matches_merchant_month`'s
    # `:volume` placeholder. x10 moves every merchant-month across a band
    # boundary; fraud level is a ratio and is scale-invariant, so the volume
    # band is the only dimension that changes.
    "volume_x10": [("UPDATE main.payments SET eur_amount = eur_amount * 10", 138236)],
}
MUTANTS = ("base", *MUTATIONS)


def build_mutants(out: Path) -> dict[str, Path]:
    """Copy the frozen database once per mutant and apply its statements."""
    paths: dict[str, Path] = {}
    for name in MUTANTS:
        dst = out / f"{name}.duckdb"
        shutil.copy(DB, dst)
        con = duckdb.connect(str(dst))
        try:
            for stmt, expected in MUTATIONS.get(name, []):
                changed = con.execute(stmt).fetchone()[0]
                if changed != expected:
                    raise AssertionError(
                        f"mutation {name!r} touched {changed} rows, expected "
                        f"{expected}: {stmt}"
                    )
            con.execute("CHECKPOINT")
        finally:
            con.close()
        paths[name] = dst
    return paths


def fee_query(path: Path) -> tuple[str, bool]:
    """The last submitted query that references `fees` -- the one that computes
    the answer -- and whether such a query exists at all.

    Falls back to the last query of any kind. A trace whose final SQL never
    touches `fees` did not derive the fee, and that is reported as its own
    status rather than scored as insensitive: a query that cannot move is not
    evidence about whether the agent would have derived the band.
    """
    calls: list[str] = []
    for message in json.load(gzip.open(path)):
        for part in message.get("parts", []):
            if part.get("part_kind") != "tool-call":
                continue
            if part.get("tool_name") not in SQL_TOOLS:
                continue
            args = part.get("args")
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except json.JSONDecodeError:
                    continue
            if isinstance(args, dict) and args.get("sql"):
                calls.append(str(args["sql"]))
    fees = [q for q in calls if "fees" in q.lower()]
    if fees:
        return fees[-1], True
    return (calls[-1] if calls else ""), False


def _norm(rows: list[tuple]) -> list[tuple]:
    """Order- and float-noise-insensitive form, so 'the answer moved' means the
    values moved and not that the engine returned them in another order."""
    out = [
        tuple(round(v, 4) if isinstance(v, float) else v for v in row) for row in rows
    ]
    return sorted(out, key=repr)


def _run(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple] | None:
    """The query's result, or None if it raised or timed out."""
    timer = threading.Timer(TIMEOUT, con.interrupt)
    timer.start()
    try:
        return _norm(con.execute(sql).fetchall())
    except Exception:  # noqa: BLE001 - any engine failure is "no result"
        return None
    finally:
        timer.cancel()


def _id_set(rows: list[tuple]) -> frozenset[int]:
    """The fee-ID column of an arbitrary agent result.

    Agents return the IDs under many shapes (one column, or IDs beside a label
    or a count), so the first all-integer column is taken rather than assuming
    a position.
    """
    if not rows:
        return frozenset()
    for col in range(len(rows[0])):
        values = [row[col] for row in rows]
        if all(isinstance(v, int) for v in values):
            return frozenset(values)
    return frozenset()


def _complete_cases(rows: list[dict]) -> set[str]:
    """Tasks answered by every arm. Comparing arms on different tasks is not a
    comparison."""
    seen: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        seen[str(r["task_id"])].add(r["arm"])
    return {t for t, arms in seen.items() if arms >= set(ARMS)}


def _rows_for(name: str) -> tuple[str, list[dict], set[str]]:
    fam = family_of()
    rows = [
        r
        for r in load(ROOT / "results" / f"{name}.jsonl")
        if r.get("level") == "hard"
        and r.get("verdict") in ANSWER_VERDICTS
        and fam.get(str(r["task_id"])) in PAYMENTS_FAMILIES
    ]
    model = sorted({r["model"] for r in rows})[0].split("/")[-1] if rows else "?"
    return model, rows, _complete_cases(rows)


def measure(name: str, cons: dict[str, duckdb.DuckDBPyConnection]) -> dict:
    """For every stored query: did its answer move under each mutation?"""
    model, rows, complete = _rows_for(name)
    fam = family_of()
    run_dir = ROOT / "traces" / name
    out: list[dict] = []
    for r in rows:
        tid, arm = str(r["task_id"]), r["arm"]
        if tid not in complete:
            continue
        path = trace_for(run_dir, tid, arm)
        if path is None:
            continue
        sql, touches_fees = fee_query(path)
        rec = {
            "task": tid,
            "arm": arm,
            "verdict": r["verdict"],
            "family": fam.get(tid),
            "sql": sql,
        }
        base = _run(cons["base"], sql) if sql else None
        if base is None:
            rec["status"] = "no_sql" if not sql else "unrunnable"
            out.append(rec)
            continue
        rec["status"] = "ok" if touches_fees else "no_fees"
        for mutation in MUTATIONS:
            result = _run(cons[mutation], sql)
            rec[mutation] = None if result is None else (result != base)
        out.append(rec)
    return {"model": model, "rows": out}


def report_arms(measured: dict) -> None:
    """How often each arm's query responds to each mutation. Between arms this
    is sharp; within an arm it is not the question -- see `report_latent`."""
    rows = [r for r in measured["rows"] if r["status"] == "ok"]
    print(f"\n{measured['model']} — {len(rows)} runnable fee queries")
    header = "".join(f"{m:>13s}" for m in MUTATIONS)
    print(f"{'arm':18s}{'n':>5s}{header}")
    for arm in ARMS:
        arm_rows = [r for r in rows if r["arm"] == arm]
        if not arm_rows:
            continue
        moved = [sum(1 for r in arm_rows if r.get(m) is True) for m in MUTATIONS]
        cells = "".join(f"{100 * k / len(arm_rows):11.0f}% " for k in moved)
        print(f"{arm:18s}{len(arm_rows):5d}{cells}")


def report_latent(
    measured: dict, name: str, macro_con, raw_con, match: dict
) -> tuple[int, int, int, int] | None:
    """THE RESULT: among contract-arm queries DABStep scored CORRECT, does
    insensitivity to `volume_x10` predict being wrong in the mutated world?

    Graded against the compiled macro on the mutated data, because DABStep has
    no gold for a world its tasks do not describe.
    """
    run_dir = ROOT / "traces" / name
    cells: Counter = Counter()
    for r in measured["rows"]:
        if r["arm"] != "contract" or r["status"] != "ok":
            continue
        if r["verdict"] != "correct" or r["task"] not in match:
            continue
        key, m = match[r["task"]]
        expected = frozenset(
            int(x) for x in coverage.answer(macro_con, key, m).split(", ") if x.strip()
        )
        path = trace_for(run_dir, r["task"], "contract")
        rows = _run(raw_con, fee_query(path)[0])
        if rows is None:
            cells["unrunnable"] += 1
            continue
        cells[(bool(r.get("volume_x10")), _id_set(rows) == expected)] += 1

    moved, still = cells[(True, True)] + cells[(True, False)], cells[(True, True)]
    froze, froze_ok = cells[(False, True)] + cells[(False, False)], cells[(False, True)]
    total = moved + froze
    if not total:
        return None
    print(
        f"\n{measured['model']} — {total} contract-arm queries scored CORRECT on"
        " base data (fee-ID families)"
    )
    print(
        f"  responded to volume_x10 : {moved:3d}   still correct on the"
        f" mutated world: {still:3d}"
        f" ({100 * still / max(moved, 1):.0f}%)"
    )
    print(
        f"  did NOT respond         : {froze:3d}   still correct on the"
        f" mutated world: {froze_ok:3d}"
        f" ({100 * froze_ok / max(froze, 1):.0f}%)"
        "   <- latent defects the benchmark scores as correct"
    )
    if cells["unrunnable"]:
        print(f"  ({cells['unrunnable']} unrunnable on the mutant)")
    return (moved, still, froze, froze_ok)


def _fee_id_matches() -> dict[str, tuple[str, re.Match]]:
    """task_id -> (family key, the parsed question), for the fee-ID families."""
    golded, _ = coverage.load_golded()
    out: dict[str, tuple[str, re.Match]] = {}
    for task in golded:
        question = task["question"].strip()
        for key, _, pattern in FAMILIES:
            if key not in FEE_ID_FAMILIES:
                continue
            m = re.match(pattern, question)
            if m:
                out[str(task["task_id"])] = (key, m)
                break
    return out


def main(argv: list[str]) -> None:
    show_arms = "--arms" in argv
    keep = None
    if "--keep" in argv:
        keep = Path(argv[argv.index("--keep") + 1])
    runs = [
        a
        for i, a in enumerate(argv)
        if not a.startswith("--") and (i == 0 or argv[i - 1] != "--keep")
    ] or ["glm-full", "dsflash-full", "sonnet5-full", "sol-full"]

    tmp = Path(tempfile.mkdtemp(prefix="adc-sensitivity-")) if keep is None else keep
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        paths = build_mutants(tmp)
        cons = {n: duckdb.connect(str(p), read_only=True) for n, p in paths.items()}
        # The macro compiled over the mutated data: the oracle for a world
        # DABStep has no gold for. `coverage.connect()` reads a module global.
        original_db, coverage.DB = coverage.DB, str(paths["volume_x10"])
        try:
            macro_con = coverage.connect()
        finally:
            coverage.DB = original_db
        match = _fee_id_matches()

        totals = [0, 0, 0, 0]
        for name in runs:
            if not (ROOT / "results" / f"{name}.jsonl").exists():
                print(f"skipping {name}: no results file")
                continue
            measured = measure(name, cons)
            if show_arms:
                report_arms(measured)
            got = report_latent(measured, name, macro_con, cons["volume_x10"], match)
            if got:
                totals = [a + b for a, b in zip(totals, got)]

        moved, still, froze, froze_ok = totals
        if moved or froze:
            print(
                f"\npooled: {still}/{moved} sensitive queries still correct;"
                f" {froze_ok}/{froze} insensitive ones"
            )
            try:
                from scipy.stats import fisher_exact

                _, p = fisher_exact(
                    [[still, moved - still], [froze_ok, froze - froze_ok]]
                )
                print(f"        Fisher exact p = {p:.3g}")
            except ImportError:  # pragma: no cover
                pass
        for con in cons.values():
            con.close()
        macro_con.close()
    finally:
        if keep is None:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main(sys.argv[1:])
