"""How many distinct question shapes does DABStep's task set contain?

The paper leaned on "450 tasks generated from 26 templates" -- a number with no
source in this repository and no basis in the benchmark's own paper, which
describes 450 questions expanded from 95 core questions. This script replaces
it with a figure we compute and can defend.

The method is deliberately crude and stated rather than tuned: replace the
entities that DABStep varies between sibling tasks (merchant, card scheme,
month, account type, MCC description, and every number) and count what remains.
That is an upper bound on distinct SHAPES -- two shapes that differ only in
wording we have not normalised are counted separately.

Run:  uv run python analysis/templates.py
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

MONTHS = (
    "January|February|March|April|May|June|July|August|September|October"
    "|November|December"
)
SCHEMES = "NexPay|GlobalCard|TransactPlus|SwiftCharge"


def normalise(question: str, merchants: list[str], mcc_descriptions: list[str]) -> str:
    q = question.strip()
    # Longest first: several MCC descriptions are prefixes of others.
    for d in sorted(mcc_descriptions, key=len, reverse=True):
        if d and d in q:
            q = q.replace(d, "MCCDESC")
    for m in merchants:
        q = q.replace(m, "MERCHANT")
    q = re.sub(rf"\b({SCHEMES})\b", "SCHEME", q)
    q = re.sub(rf"\b({MONTHS})\b", "MONTH", q)
    q = re.sub(r"account_type = [A-Z]", "account_type = A", q)
    q = re.sub(r"\baci = [A-Z]\b", "aci = X", q)
    q = re.sub(r"account type [A-Z]\b", "account type A", q)
    q = re.sub(r"\b\d+(\.\d+)?%?\b", "N", q)
    return re.sub(r"\s+", " ", q).strip()


def main() -> None:
    con = duckdb.connect()
    con.execute(f"ATTACH '{ROOT / 'data' / 'dabstep.duckdb'}' AS src (READ_ONLY)")
    merchants = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT merchant FROM src.merchant_data"
        ).fetchall()
    ]
    descriptions = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT description FROM src.merchant_category_codes"
        ).fetchall()
    ]

    tasks = json.loads((ROOT / "data" / "tasks.json").read_text())
    tasks = tasks if isinstance(tasks, list) else tasks.get("tasks", tasks)
    golds = json.loads((ROOT / "data" / "golds.json").read_text())["golds"]
    golded = [t for t in tasks if str(t["task_id"]) in golds]

    for label, rows in (("all 450 tasks", tasks), ("401 golded", golded)):
        shapes = Counter(
            normalise(t["question"], merchants, descriptions) for t in rows
        )
        cum, n_for_half, n_for_three_quarters = 0, None, None
        for i, (_, c) in enumerate(shapes.most_common(), start=1):
            cum += c
            if n_for_half is None and cum >= len(rows) * 0.5:
                n_for_half = i
            if n_for_three_quarters is None and cum >= len(rows) * 0.75:
                n_for_three_quarters = i
        singletons = sum(1 for c in shapes.values() if c == 1)
        print(f"{label}: {len(rows)} questions, {len(shapes)} distinct shapes")
        print(
            f"   {n_for_half} shapes cover half of them, "
            f"{n_for_three_quarters} cover three quarters"
        )
        print(f"   {singletons} shapes occur exactly once")
        print(f"   largest shape: {shapes.most_common(1)[0][1]} questions")


if __name__ == "__main__":
    main()
