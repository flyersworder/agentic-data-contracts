"""Diagnose WHICH domain convention an agent got wrong, mechanically.

`analysis/macro.sql` compiles the frozen contract and reproduces gold on 176
hard tasks. Lesion exactly one convention in it and you get a macro that is
wrong in one specific, nameable way. Run the lesioned macro over the same
tasks and you get the number an agent would produce if it held that single
misconception and nothing else.

Match an agent's WRONG answer against those, and the misconception is
identified — no LLM judge, no hand labelling, no rubric. An agent that answers
9211.73 where gold is 15237.49 charged one fee rule per transaction instead of
summing every matching pair, and we can say so because the `first_match`
lesion produces exactly 9211.73.

Each lesion is a textual substitution on `macro.sql` guarded by an exact-count
assertion, so a change to the macro fails loudly here rather than silently
producing a different lesion.

Run:  uv run python analysis/counterfactuals.py [results/*.jsonl ...]
"""

from __future__ import annotations

import itertools
import math
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from coverage import (  # noqa: E402
    BASE_TABLES,
    DB,
    FAMILIES,
    MACRO,
    PAYMENTS_FAMILIES,
    answer,
    classify,
    load_golded,
)
from dce.grade import score  # noqa: E402
from dce.stats import ANSWER_VERDICTS, load  # noqa: E402

ARMS = ["schema_only", "contract_hollow", "manual_prompt", "contract"]

# ── The lesions ─────────────────────────────────────────────────────────────
# name -> (misconception, [(exact text in macro.sql, replacement, how many
# occurrences that text MUST have)]). The count is asserted so a change to the
# macro fails loudly instead of silently lesioning a different amount of it.
LESIONS: dict[str, tuple[str, list[tuple[str, str, int]]]] = {
    "null_not_wildcard": (
        "reads a NULL fee-rule field as 'matches nothing' rather than "
        "'applies to all values'",
        [
            (
                "(f.card_scheme IS NULL OR f.card_scheme = p.card_scheme)",
                "(f.card_scheme = p.card_scheme)",
                1,
            ),
            (
                "(f.is_credit   IS NULL OR f.is_credit   = p.is_credit)",
                "(f.is_credit = p.is_credit)",
                1,
            ),
            (
                "(f.intracountry IS NULL\n       OR (f.intracountry = 1) = "
                "(p.issuing_country = p.acquirer_country))",
                "((f.intracountry = 1) = (p.issuing_country = p.acquirer_country))",
                1,
            ),
        ],
    ),
    "empty_list_not_wildcard": (
        "reads an empty list-typed fee-rule field as 'matches nothing' rather "
        "than 'applies to all values'",
        [
            (
                "(f.aci IS NULL OR len(f.aci) = 0 OR list_contains(f.aci, p.aci))",
                "(list_contains(f.aci, p.aci))",
                1,
            ),
            (
                "(f.account_type IS NULL OR len(f.account_type) = 0\n       "
                "OR list_contains(f.account_type, m.account_type))",
                "(list_contains(f.account_type, m.account_type))",
                1,
            ),
        ],
    ),
    "raw_capture_delay": (
        "compares the merchant's raw capture_delay ('7') against the fee "
        "table's bands ('>5') without mapping days to a band",
        [
            (
                """(f.capture_delay IS NULL
       OR f.capture_delay = CASE
            WHEN m.capture_delay IN ('immediate', 'manual') THEN m.capture_delay
            WHEN TRY_CAST(m.capture_delay AS INTEGER) < 3 THEN '<3'
            WHEN TRY_CAST(m.capture_delay AS INTEGER) BETWEEN 3 AND 5 THEN '3-5'
            WHEN TRY_CAST(m.capture_delay AS INTEGER) > 5 THEN '>5'
          END)""",
                "(f.capture_delay IS NULL OR f.capture_delay = m.capture_delay)",
                1,
            ),
        ],
    ),
    "fraud_by_count": (
        "computes monthly fraud level as a share of transaction COUNT rather "
        "than of euro volume",
        [
            (
                "SUM(p.eur_amount) FILTER (WHERE p.has_fraudulent_dispute)\n"
                "         / NULLIF(SUM(p.eur_amount), 0)                           "
                "AS fraud_ratio",
                "COUNT(*) FILTER (WHERE p.has_fraudulent_dispute)\n"
                "         / NULLIF(COUNT(*), 0)::DOUBLE                            "
                "AS fraud_ratio",
                1,
            ),
        ],
    ),
    "volume_by_count": (
        "computes monthly volume as a transaction COUNT rather than a euro sum",
        [
            (
                "SUM(p.eur_amount)                                          AS volume",
                "COUNT(*)::DOUBLE                                           AS volume",
                1,
            )
        ],
    ),
    "exclusive_bands": (
        "reads the manual's 'between A and B' bands as exclusive at both ends",
        [
            (
                "WHEN '100k-1m' THEN mm.volume >= 100000  AND mm.volume <= 1000000",
                "WHEN '100k-1m' THEN mm.volume >  100000  AND mm.volume <  1000000",
                1,
            ),
            (
                "WHEN '1m-5m'   THEN mm.volume >= 1000000 AND mm.volume <= 5000000",
                "WHEN '1m-5m'   THEN mm.volume >  1000000 AND mm.volume <  5000000",
                1,
            ),
            (
                "WHEN '7.2%-7.7%' THEN 100 * mm.fraud_ratio >= 7.2 AND "
                "100 * mm.fraud_ratio <= 7.7",
                "WHEN '7.2%-7.7%' THEN 100 * mm.fraud_ratio >  7.2 AND "
                "100 * mm.fraud_ratio <  7.7",
                1,
            ),
            (
                "WHEN '7.7%-8.3%' THEN 100 * mm.fraud_ratio >= 7.7 AND "
                "100 * mm.fraud_ratio <= 8.3",
                "WHEN '7.7%-8.3%' THEN 100 * mm.fraud_ratio >  7.7 AND "
                "100 * mm.fraud_ratio <  8.3",
                1,
            ),
        ],
    ),
    "day_of_year_off_by_one": (
        "reconstructs the calendar date from day_of_year without the -1 "
        "offset, shifting month boundaries by a day",
        [("CAST(p.day_of_year - 1 AS INTEGER)", "CAST(p.day_of_year AS INTEGER)", 3)],
    ),
}

# Lesions applied after the views exist, by redefining `transaction_fee_matches`
# from a materialised copy of itself (a view cannot reference itself).
POST_LESIONS: dict[str, tuple[str, str]] = {
    "first_match": (
        "charges ONE fee rule per transaction (the lowest ID) instead of "
        "summing over every matching (transaction, rule) pair",
        "row_number() OVER (PARTITION BY psp_reference ORDER BY fee_id) = 1",
    ),
    "highest_fee_match": (
        "charges only the most expensive matching rule per transaction",
        "row_number() OVER (PARTITION BY psp_reference ORDER BY fee DESC) = 1",
    ),
}


def build(*lesions: str) -> duckdb.DuckDBPyConnection:
    """A macro with every named lesion applied. No lesions = the true macro."""
    sql = MACRO.read_text()
    for lesion in lesions:
        if lesion not in LESIONS:
            continue
        for old, new, expected in LESIONS[lesion][1]:
            n = sql.count(old)
            if n != expected:
                raise AssertionError(
                    f"lesion {lesion!r}: expected {expected} occurrence(s) of\n"
                    f"{old!r}\nin macro.sql, found {n}. The macro changed; "
                    f"update the lesion."
                )
            sql = sql.replace(old, new)
    con = duckdb.connect()
    con.execute(f"ATTACH '{DB}' AS src (READ_ONLY)")
    for t in BASE_TABLES:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM src.{t}")
    con.execute(sql)
    post = [name for name in lesions if name in POST_LESIONS]
    if len(post) > 1:
        raise ValueError(
            f"two post-lesions requested ({post}); both redefine the same view, "
            "so applying both silently drops one -- pass one"
        )
    if post:
        con.execute("CREATE TABLE tfm_base AS SELECT * FROM transaction_fee_matches")
        con.execute(
            "CREATE OR REPLACE VIEW transaction_fee_matches AS "
            "SELECT * EXCLUDE (rn) FROM (SELECT *, "
            f"{POST_LESIONS[post[0]][1]} AS keep, "
            "row_number() OVER () AS rn FROM tfm_base) WHERE keep"
        )
    return con


# Families whose answer is a single number, so an agent's error has a
# meaningful MAGNITUDE relative to gold (the list-valued families do not).
NUMERIC_FAMILIES = frozenset(
    {
        "total_fees_day",
        "total_fees_month",
        "total_fees_year",
        "avg_fee_credit",
        "avg_fee_account",
        "avg_fee_account_mcc",
    }
)
BANDS = ("<2x", "2-10x", "10-100x", ">100x")


def as_number(value: str) -> float | None:
    m = re.search(r"-?\d[\d,]*\.?\d*", str(value).replace(",", ""))
    return float(m.group()) if m else None


def band(ratio: float) -> str:
    lr = abs(math.log10(ratio))
    if lr < math.log10(2):
        return BANDS[0]
    if lr < 1:
        return BANDS[1]
    return BANDS[2] if lr < 2 else BANDS[3]


def describe(lesion: str) -> str:
    if lesion in LESIONS:
        return LESIONS[lesion][0]
    return POST_LESIONS[lesion][0]


def family_of() -> dict[str, str]:
    """task_id -> the macro family that answers it (macro-bucket tasks only)."""
    return {tid: key for tid, key, _ in macro_tasks()}


def macro_tasks(payments_only: bool = False) -> list[tuple[str, str, re.Match]]:
    """(task_id, family_key, match) for every task the true macro answers.

    `payments_only` keeps just the families that join `payments` to `fees` --
    the only ones a lesion of `macro.sql` can affect at all.
    """
    golded, _ = load_golded()
    out = []
    for t in golded:
        q = t["question"].strip()
        for key, _cat, rx in FAMILIES:
            m = re.match(rx, q)
            if m:
                if not payments_only or key in PAYMENTS_FAMILIES:
                    out.append((str(t["task_id"]), key, m))
                break
    return out


def main(paths: list[str]) -> None:
    tasks = macro_tasks(payments_only=True)
    _, golds = load_golded()
    names = list(LESIONS) + list(POST_LESIONS)

    # Every lesion's answer for every macro-answerable task.
    print(f"building {len(names)} lesioned macros over {len(tasks)} tasks ...")
    preds: dict[str, dict[str, str]] = {}
    for name in names:
        con = build(name)
        preds[name] = {}
        for tid, key, m in tasks:
            try:
                preds[name][tid] = answer(con, key, m)
            except duckdb.Error:
                preds[name][tid] = ""
        con.close()

    # A lesion that still lands on gold is not diagnostic for that task.
    # `preds[...][tid]` is "" when a lesioned macro raised. An empty prediction
    # is not a diagnosis: without this guard it inflates the diagnostic count
    # and lets any empty agent answer be attributed to that misconception. The
    # paired branch below has always guarded it.
    diagnostic = {
        name: {
            tid
            for tid in preds[name]
            if preds[name][tid] and not score(preds[name][tid], str(golds[tid]))
        }
        for name in names
    }
    print()
    print(f"{'lesion':26s} {'diagnostic on':>13s}  misconception")
    for name in names:
        print(
            f"{name:26s} {len(diagnostic[name]):5d}/{len(tasks):<7d} "
            f"{describe(name)[:70]}"
        )

    # Pairs, for errors that are two conventions deep rather than one.
    pairs = [
        (a, b)
        for a, b in itertools.combinations(names, 2)
        if not (a in POST_LESIONS and b in POST_LESIONS)
    ]
    print(f"\nbuilding {len(pairs)} paired lesions ...")
    for pair in pairs:
        con = build(*pair)
        preds[pair] = {}
        for tid, key, m in tasks:
            try:
                preds[pair][tid] = answer(con, key, m)
            except duckdb.Error:
                preds[pair][tid] = ""
        con.close()
        diagnostic[pair] = {
            tid
            for tid in preds[pair]
            if preds[pair][tid] and not score(preds[pair][tid], str(golds[tid]))
        }

    cat = classify()
    fam = family_of()
    for path in paths:
        rows = [r for r in load(Path(path)) if r.get("level") == "hard"]
        if not rows:
            continue
        model = sorted({r["model"] for r in rows})[0].split("/")[-1]
        seen: dict[str, set[str]] = defaultdict(set)
        for r in rows:
            if r.get("verdict") in ANSWER_VERDICTS:
                seen[str(r["task_id"])].add(r["arm"])
        complete = {t for t, a in seen.items() if a >= set(ARMS)}
        # ONLY the payments-joined families. The other four macro families
        # resolve straight off `fees` / `merchant_category_codes`, so no lesion
        # of `macro.sql` can change their answer -- every one of their wrong
        # answers lands in `undiagnosed` by construction. Including them does
        # not merely dilute: the share differs sharply by arm (71% of
        # contract-arm errors against 52% elsewhere), so it biases the very
        # comparison this table exists to make.
        wrong = [
            r
            for r in rows
            if r["verdict"] == "incorrect"
            and str(r["task_id"]) in complete
            and cat.get(str(r["task_id"])) == "macro"
            and fam.get(str(r["task_id"])) in PAYMENTS_FAMILIES
        ]

        tally: dict[str, Counter] = defaultdict(Counter)
        for r in wrong:
            tid, arm, ans = str(r["task_id"]), r["arm"], r["answer_normalized"]
            tally[arm]["wrong"] += 1
            if not ans.strip():
                tally[arm]["no answer"] += 1
                continue
            if any(tid in diagnostic[n] and score(ans, preds[n][tid]) for n in names):
                tally[arm]["single"] += 1
            elif any(tid in diagnostic[p] and score(ans, preds[p][tid]) for p in pairs):
                tally[arm]["paired"] += 1
            else:
                tally[arm]["undiagnosed"] += 1

        print(f"\n{model} — wrong answers on macro-bucket hard tasks")
        cols = ["wrong", "single", "paired", "no answer", "undiagnosed"]
        print(f"{'arm':18s}" + "".join(f"{c:>12s}" for c in cols))
        for arm in ARMS:
            t = tally[arm]
            print(f"{arm:18s}" + "".join(f"{t[c]:>12d}" for c in cols))

        # How wrong, in orders of magnitude, on the single-number families.
        print(f"\n{model} — magnitude of wrong numeric answers (agent / gold)")
        head = "".join(f"{b:>9s}" for b in BANDS)
        print(f"{'arm':18s}{'n':>5s}{'median':>9s}{head}{'over':>7s}")
        for arm in ARMS:
            ratios: list[float] = []
            dropped = 0
            for r in wrong:
                if r["arm"] != arm:
                    continue
                tid = str(r["task_id"])
                if fam.get(tid) not in NUMERIC_FAMILIES:
                    continue
                got, gold = as_number(r["answer_normalized"]), as_number(golds[tid])
                # `gold <= 0` is guarded symmetrically with `got`: a negative
                # gold would make the ratio negative and `band()` would raise
                # in `math.log10`. Today's fee golds are all positive.
                if got is None or gold is None or gold <= 0:
                    continue
                # An answer of exactly 0 is a plausible wrong answer for a fee
                # total but has no magnitude relative to gold. Count and report
                # it rather than dropping it silently, so `n` reconciles with
                # the `wrong` column above.
                if got <= 0:
                    dropped += 1
                    continue
                ratios.append(got / gold)
            if not ratios:
                continue
            b = Counter(band(x) for x in ratios)
            med = sorted(ratios)[len(ratios) // 2]
            over = sum(1 for x in ratios if x > 1)
            cells = "".join(f"{100 * b[k] / len(ratios):8.0f}%" for k in BANDS)
            note = f"   ({dropped} non-positive omitted)" if dropped else ""
            print(
                f"{arm:18s}{len(ratios):5d}{med:9.2f}{cells}"
                f"{100 * over / len(ratios):6.0f}%{note}"
            )


if __name__ == "__main__":
    args = sys.argv[1:] or [
        str(ROOT / "results" / "glm-full.jsonl"),
        str(ROOT / "results" / "dsflash-full.jsonl"),
        str(ROOT / "results" / "sol-full.jsonl"),
    ]
    main(args)
