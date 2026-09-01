"""Two measurements the compiled macro (`analysis/macro.sql`) makes possible,
neither of which requires running a new experimental arm.

A. CONTRACT SUFFICIENCY. The macro is a mechanical composition of the frozen
   contract's own `sql_expression` fields. Where it reproduces the benchmark's
   gold answer exactly, the contract demonstrably CONTAINS the information that
   task needs — so the corresponding gap in an agent arm is a failure to derive
   from present knowledge, not a consequence of absent knowledge.

C. MACRO COVERAGE. How much of DABStep a pre-computed view/macro layer can
   answer outright, versus how much still requires derivation on top of it.
   This bounds what the "pre-computed artifacts" component of a context layer
   can buy on this benchmark.

Run:  uv run python analysis/coverage.py

Reads the frozen DuckDB read-only (ATTACH ... READ_ONLY) and builds the views
in an in-memory database, so `data/` is never written to.
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dce.grade import active_scorer, score  # noqa: E402

DB = ROOT / "data" / "dabstep.duckdb"
TASKS = ROOT / "data" / "tasks.json"
GOLDS = ROOT / "data" / "golds.json"
MACRO = Path(__file__).resolve().parent / "macro.sql"

MONTHS = {
    m: i
    for i, m in enumerate(
        "January February March April May June July August September "
        "October November December".split(),
        start=1,
    )
}
BASE_TABLES = (
    "payments merchant_data fees merchant_category_codes acquirer_countries"
).split()


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"ATTACH '{DB}' AS src (READ_ONLY)")
    for t in BASE_TABLES:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM src.{t}")
    con.execute(MACRO.read_text())
    return con


def num(con, sql: str, digits: int) -> str:
    v = con.execute(sql).fetchone()[0]
    return "Not Applicable" if v is None else f"{float(v):.{digits}f}"


def lst(con, sql: str) -> str:
    return ", ".join(str(r[0]) for r in con.execute(sql).fetchall())


def month_bounds(month: str, year: int) -> tuple[str, str]:
    """A natural-month filter expressed on `month`, the contract's
    `transaction_natural_month`."""
    return f"month = DATE '{year}-{MONTHS[month]:02d}-01'", ""


# ── Family handlers ─────────────────────────────────────────────────────────
# Each returns the macro's answer for one task, formatted per the task's
# stated guidelines (2dp for fee totals, 6dp for average fees, comma-separated
# for lists).

FEE_SUM = "SELECT SUM(fee) FROM transaction_fee_matches WHERE {w}"
FEE_IDS = (
    "SELECT DISTINCT fee_id FROM transaction_fee_matches WHERE {w} ORDER BY fee_id"
)

# Wildcard-aware membership, transcribed from `fee_rule_matches_transaction`.
AT = (
    "(account_type IS NULL OR len(account_type) = 0"
    " OR list_contains(account_type, '{v}'))"
)
ACI = "(aci IS NULL OR len(aci) = 0 OR list_contains(aci, '{v}'))"
MCC = (
    "(merchant_category_code IS NULL OR len(merchant_category_code) = 0"
    " OR list_contains(merchant_category_code, {v}))"
)
CREDIT = "(is_credit IS NULL OR is_credit = {v})"
AVG_FEE = "SELECT AVG(fixed_amount + rate * {amt} / 10000) FROM fees WHERE {w}"

FAMILIES: list[tuple[str, str, str]] = [
    # (family key, category, regex)
    (
        "total_fees_day",
        "macro",
        r"^For the (\d+)(?:st|nd|rd|th) of the year (\d+), what is the total fees "
        r"\(in euros\) that (\w+) should pay\?$",
    ),
    (
        "total_fees_year",
        "macro",
        r"^What are the total fees \(in euros\) that (\w+) paid in (\d{4})\?$",
    ),
    (
        "total_fees_month",
        "macro",
        r"^What are the total fees \(in euros\) that (\w+) paid in (\w+) (\d{4})\?$",
    ),
    (
        "fee_ids_day",
        "macro",
        r"^For the (\d+)(?:st|nd|rd|th) of the year (\d+), what are the Fee IDs "
        r"applicable to (\w+)\?$",
    ),
    (
        "fee_ids_year",
        "macro",
        r"^What are the applicable fee IDs for (\w+) in (\d{4})\?$",
    ),
    (
        "fee_ids_month",
        "macro",
        r"^What were the applicable Fee IDs for (\w+) in (\w+) (\d{4})\?$",
    ),
    (
        "merchants_for_fee",
        "macro",
        r"^In (\d{4}), which merchants were affected by the Fee with ID (\d+)\?$",
    ),
    (
        "avg_fee_credit",
        "rules",
        r"^For credit transactions, what would be the average fee that the card "
        r"scheme (\w+) would charge for a transaction value of (\d+) EUR\?",
    ),
    (
        "avg_fee_account",
        "rules",
        r"^For account type (\w+), what would be the average fee that the card "
        r"scheme (\w+) would charge for a transaction value of (\d+) EUR\?",
    ),
    (
        "avg_fee_account_mcc",
        "rules",
        r"^For account type (\w+) and the MCC description: (.+?), what would be the "
        r"average fee that the card scheme (\w+) would charge for a transaction "
        r"value of (\d+) EUR\?",
    ),
    (
        "fee_ids_by_at_aci",
        "rules",
        r"^What is the fee ID or IDs that apply to account_type = (\w+) and "
        r"aci = (\w+)\?$",
    ),
]

# The macro families whose answer requires joining `payments` to `fees`, and
# therefore exercise every clause of `analysis/macro.sql`. The other four
# families resolve straight off `fees` / `merchant_category_codes`, so any
# analysis that lesions or inspects the macro's own predicates MUST restrict
# itself to these -- a lesion cannot change an answer that never touched the
# lesioned views, and counting those tasks as "undiagnosed" silently conflates
# "no lesion matched" with "no lesion could apply".
PAYMENTS_FAMILIES = frozenset(
    {
        "total_fees_day",
        "total_fees_month",
        "total_fees_year",
        "fee_ids_day",
        "fee_ids_month",
        "fee_ids_year",
        "merchants_for_fee",
    }
)

# Questions that USE fee semantics but ask a counterfactual or an optimisation
# on top of them: the macro is a building block, not the answer.
DERIVED = [
    r"what amount delta will it have to pay in fees",
    r"what delta would .* pay if the relative fee",
    r"move the fraudulent transactions towards a different Authorization",
    r"steer traffic",
    r"imagine if the Fee with ID .* was only applied to account type",
    r"most expensive Authorization Characteristics Indicator",
    r"most expensive MCC for a transaction",
    r"which card scheme would provide the (cheapest|most expensive) fee",
]


def answer(con, key: str, m: re.Match) -> str:
    g = m.groups()
    if key == "total_fees_day":
        day, year, mer = int(g[0]), int(g[1]), g[2]
        return num(
            con,
            FEE_SUM.format(
                w=f"merchant = '{mer}' AND year = {year} AND day_of_year = {day}"
            ),
            2,
        )
    if key == "total_fees_year":
        mer, year = g[0], int(g[1])
        return num(con, FEE_SUM.format(w=f"merchant = '{mer}' AND year = {year}"), 2)
    if key == "total_fees_month":
        mer, mon, year = g[0], g[1], int(g[2])
        w, _ = month_bounds(mon, year)
        return num(con, FEE_SUM.format(w=f"merchant = '{mer}' AND {w}"), 2)
    if key == "fee_ids_day":
        day, year, mer = int(g[0]), int(g[1]), g[2]
        return lst(
            con,
            FEE_IDS.format(
                w=f"merchant = '{mer}' AND year = {year} AND day_of_year = {day}"
            ),
        )
    if key == "fee_ids_year":
        mer, year = g[0], int(g[1])
        return lst(con, FEE_IDS.format(w=f"merchant = '{mer}' AND year = {year}"))
    if key == "fee_ids_month":
        mer, mon, year = g[0], g[1], int(g[2])
        w, _ = month_bounds(mon, year)
        return lst(con, FEE_IDS.format(w=f"merchant = '{mer}' AND {w}"))
    if key == "merchants_for_fee":
        year, fid = int(g[0]), int(g[1])
        return lst(
            con,
            "SELECT DISTINCT merchant FROM transaction_fee_matches "
            f"WHERE year = {year} AND fee_id = {fid} ORDER BY merchant",
        )
    if key == "avg_fee_credit":
        scheme, amt = g[0], int(g[1])
        w = f"card_scheme = '{scheme}' AND " + CREDIT.format(v="true")
        return num(con, AVG_FEE.format(amt=amt, w=w), 6)
    if key == "avg_fee_account":
        at, scheme, amt = g[0], g[1], int(g[2])
        w = f"card_scheme = '{scheme}' AND " + AT.format(v=at)
        return num(con, AVG_FEE.format(amt=amt, w=w), 6)
    if key == "avg_fee_account_mcc":
        at, desc, scheme, amt = g[0], g[1], g[2], int(g[3])
        code = con.execute(
            "SELECT mcc FROM merchant_category_codes WHERE description = ?", [desc]
        ).fetchone()
        if code is None:
            return "Not Applicable"
        w = (
            f"card_scheme = '{scheme}' AND "
            + AT.format(v=at)
            + " AND "
            + MCC.format(v=code[0])
        )
        return num(con, AVG_FEE.format(amt=amt, w=w), 6)
    if key == "fee_ids_by_at_aci":
        at, aci = g[0], g[1]
        w = AT.format(v=at) + " AND " + ACI.format(v=aci)
        return lst(con, f"SELECT ID FROM fees WHERE {w} ORDER BY ID")
    raise KeyError(key)


def load_golded() -> tuple[list[dict], dict]:
    tasks = json.loads(TASKS.read_text())
    tasks = tasks if isinstance(tasks, list) else tasks.get("tasks", tasks)
    golds = json.loads(GOLDS.read_text())["golds"]
    return [t for t in tasks if str(t["task_id"]) in golds], golds


def classify() -> dict[str, str]:
    """task_id -> one of `macro` (the compiled macro answers it outright),
    `derived` (fee semantics are needed, but the question asks a
    counterfactual or an optimisation on top of them), `unrelated`.

    Purely syntactic: it reads the question, never the gold answer and never
    any arm's output, so it cannot be tuned to make a result come out.
    """
    golded, _ = load_golded()
    out: dict[str, str] = {}
    for t in golded:
        q = t["question"].strip()
        if any(re.match(rx, q) for _, _, rx in FAMILIES):
            out[str(t["task_id"])] = "macro"
        elif any(re.search(d, q) for d in DERIVED):
            out[str(t["task_id"])] = "derived"
        else:
            out[str(t["task_id"])] = "unrelated"
    return out


def main() -> None:
    golded, golds = load_golded()

    con = connect()
    per_family: dict[str, Counter] = {}
    misses: list[tuple[str, str, str, str]] = []
    derived = unrelated = 0
    derived_hard = unrelated_hard = 0

    for t in golded:
        q, tid, lvl = t["question"].strip(), str(t["task_id"]), t["level"]
        gold = str(golds[tid])
        for key, _cat, rx in FAMILIES:
            m = re.match(rx, q)
            if not m:
                continue
            got = answer(con, key, m)
            ok = score(got, gold)
            c = per_family.setdefault(key, Counter())
            c["n"] += 1
            c["hard"] += lvl == "hard"
            c["ok"] += ok
            if not ok:
                misses.append((tid, key, gold[:70], got[:70]))
            break
        else:
            if any(re.search(d, q) for d in DERIVED):
                derived += 1
                derived_hard += lvl == "hard"
            else:
                unrelated += 1
                unrelated_hard += lvl == "hard"

    print(f"scorer: {active_scorer()}   golded tasks: {len(golded)}")
    print()
    print(f"{'family':22s} {'cat':6s} {'n':>4s} {'hard':>5s} {'exact':>6s}")
    tot = totok = tothard = 0
    for key, cat, _ in FAMILIES:
        c = per_family.get(key)
        if not c:
            continue
        tot += c["n"]
        totok += c["ok"]
        tothard += c["hard"]
        print(
            f"{key:22s} {cat:6s} {c['n']:4d} {c['hard']:5d} "
            f"{c['ok']:4d}/{c['n']:<4d} {100 * c['ok'] / c['n']:5.1f}%"
        )
    print(
        f"{'TOTAL macro-answerable':22s} {'':6s} {tot:4d} {tothard:5d} "
        f"{totok:4d}/{tot:<4d} {100 * totok / tot:5.1f}%"
    )
    print()
    print(f"needs derivation on top of the macro : {derived:4d}  ({derived_hard} hard)")
    print(
        f"unrelated to fee semantics           : {unrelated:4d}"
        f"  ({unrelated_hard} hard)"
    )
    if misses:
        print(f"\nmisses ({len(misses)}):")
        for tid, key, gold, got in misses[:25]:
            print(f"  {tid:>5s} {key:22s} gold={gold!r} got={got!r}")


if __name__ == "__main__":
    main()
