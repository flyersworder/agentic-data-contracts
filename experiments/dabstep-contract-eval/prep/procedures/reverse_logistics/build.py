"""Build the procedures variant of LiveSQLBench's reverse_logistics database.

Reads the pristine files from LSB_DATA (default ~/data/livesqlbench) and
writes to PROC_DATA (default ~/data/procedures):

* PROC_DATA/duckdb/reverse_logistics.duckdb -- a fresh copy of the pristine
  database plus the rule table `return_charge_rules` (and its `public` view,
  as every pristine table has one);
* PROC_DATA/hf-full-v1/reverse_logistics/ -- the KB (original entries plus
  knowledge.json without "piece"), the schema text (plus the new table) and
  the column meanings (plus the new columns).

Deterministic (fixed seed) and idempotent: every run starts again from the
pristine files and overwrites the outputs.

    UV_FROZEN=1 uv run python prep/procedures/reverse_logistics/build.py
"""

from __future__ import annotations

import json
import os
import random
import shutil
from pathlib import Path

import duckdb

DB = "reverse_logistics"
HERE = Path(__file__).resolve().parent
LSB_DATA = Path(os.environ.get("LSB_DATA", "~/data/livesqlbench")).expanduser()
PROC_DATA = Path(os.environ.get("PROC_DATA", "~/data/procedures")).expanduser()

SEED = 20261014
N_RULES = 400

TABLE = "return_charge_rules"

SEGMENTS = ["Wholesale", "Retail", "Individual"]
CHANNELS = ["Store", "Collection Point", "Mail", "Courier"]
CATEGORIES = ["Electronics", "Accessories", "Home Goods", "Apparel"]
REASONS = ["Wrong Item", "Quality Issue", "Size/Fit", "Changed Mind"]
CONDITIONS = ["New", "Like New", "Used", "Damaged"]
# Bands of the customer's number of returns in the calendar month; both
# bounds inclusive, None = unbounded above.
BANDS = [(1, 2), (3, 4), (5, 7), (8, None)]

# Criteria a rule may specify, with how often each is chosen.
FIELDS = ["segment", "channel", "category", "reason", "condition", "band"]
FIELD_WEIGHTS = [1.0, 1.1, 1.0, 1.0, 1.2, 1.7]
# How many criteria a rule specifies (index = count): two to five, so no
# rule is a near-blanket rule and some returns match none.
K_WEIGHTS = [0.0, 0.0, 0.04, 0.26, 0.40, 0.30]

# The pricing model behind the generated rates: a percentage of the item's
# unit value driven by condition, reason, category and monthly volume, and a
# flat amount driven by channel. A criterion left NULL takes the average.
PCT_CONDITION = {"New": 2.0, "Like New": 5.0, "Used": 10.0, "Damaged": 17.0}
PCT_REASON_MULT = {
    "Wrong Item": 0.0,
    "Quality Issue": 0.35,
    "Size/Fit": 0.9,
    "Changed Mind": 1.3,
}
PCT_CATEGORY = {
    "Electronics": 3.0,
    "Accessories": 0.0,
    "Home Goods": 1.0,
    "Apparel": -1.0,
}
# Volume effect per band: serial returners pay more, wholesale less.
PCT_BAND_CONSUMER = [0.0, 1.5, 4.0, 7.0]
PCT_BAND_WHOLESALE = [0.0, -1.0, -2.0, -3.0]
FLAT_CHANNEL = {"Store": 0.0, "Collection Point": 4.0, "Mail": 7.5, "Courier": 12.0}
FLAT_REASON = {
    "Wrong Item": 0.0,
    "Quality Issue": 0.5,
    "Size/Fit": 1.0,
    "Changed Mind": 1.0,
}


def _mean(values) -> float:
    values = list(values)
    return sum(values) / len(values)


def _rule_amounts(rule: dict, rng: random.Random) -> tuple[float, float]:
    cond = rule["condition"]
    pct = PCT_CONDITION[cond] if cond else _mean(PCT_CONDITION.values())
    reason = rule["reason"]
    pct *= PCT_REASON_MULT[reason] if reason else _mean(PCT_REASON_MULT.values())
    cat = rule["category"]
    pct += PCT_CATEGORY[cat] if cat else _mean(PCT_CATEGORY.values())
    band = rule["band"]
    if band is not None:
        i = BANDS.index(band)
        if rule["segment"] == "Wholesale":
            pct += PCT_BAND_WHOLESALE[i]
        elif rule["segment"] is None:
            pct += (PCT_BAND_CONSUMER[i] + PCT_BAND_WHOLESALE[i]) / 2
        else:
            pct += PCT_BAND_CONSUMER[i]
    if reason == "Wrong Item":
        # The retailer's error: no percentage charge whatever else applies.
        pct = 0.0
    pct = max(0.0, pct * rng.uniform(0.85, 1.15))
    pct = round(round(pct * 2) / 2, 2)

    chan = rule["channel"]
    flat = FLAT_CHANNEL[chan] if chan else _mean(FLAT_CHANNEL.values())
    flat += FLAT_REASON[reason] if reason else _mean(FLAT_REASON.values())
    if rule["segment"] == "Wholesale":
        flat *= 0.5
    flat = max(0.0, flat + rng.uniform(-1.5, 1.5))
    if reason == "Wrong Item":
        flat = 0.0
    flat = round(round(flat * 4) / 4, 2)
    return flat, pct


def generate_rules() -> list[tuple]:
    rng = random.Random(SEED)
    values = {
        "segment": SEGMENTS,
        "channel": CHANNELS,
        "category": CATEGORIES,
        "reason": REASONS,
        "condition": CONDITIONS,
        "band": BANDS,
    }
    seen: set[tuple] = set()
    rules: list[tuple] = []
    while len(rules) < N_RULES:
        k = rng.choices(range(len(K_WEIGHTS)), weights=K_WEIGHTS)[0]
        chosen: list[str] = []
        pool = list(FIELDS)
        weights = list(FIELD_WEIGHTS)
        for _ in range(k):
            i = rng.choices(range(len(pool)), weights=weights)[0]
            chosen.append(pool.pop(i))
            weights.pop(i)
        rule = {f: (rng.choice(values[f]) if f in chosen else None) for f in FIELDS}
        key = tuple(rule[f] for f in FIELDS)
        if key in seen:
            continue
        seen.add(key)
        flat, pct = _rule_amounts(rule, rng)
        band = rule["band"]
        lo, hi = band if band is not None else (None, None)
        rules.append(
            (
                len(rules) + 1,
                rule["segment"],
                rule["channel"],
                rule["category"],
                rule["reason"],
                rule["condition"],
                lo,
                hi,
                flat,
                pct,
            )
        )
    return rules


NULL_SPEC = "**NULL means no {} is specified for the rule.**."

COLUMNS = [
    # name, DuckDB type, schema-text type, meaning
    (
        "rule_id",
        "BIGINT",
        "bigint",
        "BIGINT. Unique identifier of the return handling charge rule. PK. "
        "Example: 17.",
    ),
    (
        "customer_segment",
        "VARCHAR",
        "character varying",
        "VARCHAR(50). Customer segment the rule is written for. "
        + NULL_SPEC.format("segment")
        + " Possible values: Individual, Retail, Wholesale.",
    ),
    (
        "return_channel",
        "VARCHAR",
        "character varying",
        "VARCHAR(50). Return channel the rule is written for. "
        + NULL_SPEC.format("channel")
        + " Possible values: Collection Point, Courier, Mail, Store.",
    ),
    (
        "item_category",
        "VARCHAR",
        "character varying",
        "VARCHAR(50). Product category the rule is written for. "
        + NULL_SPEC.format("category")
        + " Possible values: Accessories, Apparel, Electronics, Home Goods.",
    ),
    (
        "primary_reason",
        "VARCHAR",
        "character varying",
        "VARCHAR(50). Primary return reason the rule is written for. "
        + NULL_SPEC.format("reason")
        + " Possible values: Changed Mind, Quality Issue, Size/Fit, Wrong Item.",
    ),
    (
        "item_condition",
        "VARCHAR",
        "character varying",
        "VARCHAR(50). Inspected item condition the rule is written for. "
        + NULL_SPEC.format("condition")
        + " Possible values: Damaged, Like New, New, Used.",
    ),
    (
        "min_monthly_returns",
        "BIGINT",
        "bigint",
        "BIGINT. Lower bound of the customer's monthly return count the rule is "
        "written for. **NULL means no lower bound is specified.**. Example: 3.",
    ),
    (
        "max_monthly_returns",
        "BIGINT",
        "bigint",
        "BIGINT. Upper bound of the customer's monthly return count the rule is "
        "written for. **NULL means no upper bound is specified.**. Example: 4.",
    ),
    (
        "flat_charge",
        "DOUBLE",
        "double precision",
        "DOUBLE. Fixed handling charge of the rule, in the currency units of the "
        "product's unit value. Example: 7.25.",
    ),
    (
        "charge_pct",
        "DOUBLE",
        "double precision",
        "DOUBLE. Handling charge of the rule as a percentage of the product's "
        "unit value. Example: 4.5.",
    ),
]


def build_database(rules: list[tuple]) -> Path:
    src = LSB_DATA / "duckdb" / (DB + ".duckdb")
    dst = PROC_DATA / "duckdb" / (DB + ".duckdb")
    dst.parent.mkdir(parents=True, exist_ok=True)
    for stale in (dst, dst.with_name(dst.name + ".wal")):
        if stale.exists():
            stale.unlink()
    shutil.copyfile(src, dst)
    con = duckdb.connect(str(dst))
    try:
        cols = ",\n  ".join(f"{name} {dtype}" for name, dtype, _, _ in COLUMNS)
        con.execute(
            f"CREATE TABLE main.{TABLE} (\n  {cols},\n  PRIMARY KEY (rule_id)\n)"
        )
        con.executemany(
            f"INSERT INTO main.{TABLE} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rules
        )
        con.execute(f"CREATE VIEW public.{TABLE} AS SELECT * FROM main.{TABLE}")
        con.execute("CHECKPOINT")
    finally:
        con.close()
    return dst


def _fmt(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:g}" if value != int(value) else f"{value:.0f}"
    return str(value)


def sample_rows_text(rules: list[tuple], n: int = 3) -> str:
    """The rows in the same plain-table layout as the pristine schema file:
    text left-aligned, numbers right-aligned, NULL blank."""
    names = [c[0] for c in COLUMNS]
    numeric = [c[1] in ("BIGINT", "DOUBLE") for c in COLUMNS]
    rows = [[_fmt(v) for v in r] for r in rules[:n]]
    widths = [max(len(names[i]), *(len(r[i]) for r in rows)) for i in range(len(names))]

    def line(cells: list[str]) -> str:
        out = []
        for i, cell in enumerate(cells):
            out.append(cell.rjust(widths[i]) if numeric[i] else cell.ljust(widths[i]))
        return "  ".join(out).rstrip()

    lines = [line(names), "  ".join("-" * w for w in widths)]
    lines += [line(r) for r in rows]
    return "\n".join(lines)


def schema_block(rules: list[tuple]) -> str:
    body = ",\n".join(
        f"{name} {ttype} NULL" if name != "rule_id" else f"{name} {ttype} NOT NULL"
        for name, _, ttype, _ in COLUMNS
    )
    return (
        f'CREATE TABLE "{TABLE}" (\n{body},\n    PRIMARY KEY (rule_id)\n);\n\n'
        f"First 3 rows:\n{sample_rows_text(rules)}\n...\n"
    )


def build_text_files(rules: list[tuple]) -> Path:
    src = LSB_DATA / "hf-full-v1" / DB
    dst = PROC_DATA / "hf-full-v1" / DB
    dst.mkdir(parents=True, exist_ok=True)

    # KB: the original lines verbatim, then the new entries without "piece".
    kb_lines = (src / (DB + "_kb.jsonl")).read_text(encoding="utf-8").splitlines()
    kb_lines = [ln for ln in kb_lines if ln.strip()]
    max_id = max(json.loads(ln)["id"] for ln in kb_lines)
    new = json.loads((HERE / "knowledge.json").read_text(encoding="utf-8"))
    ids = [e["id"] for e in new]
    if ids != list(range(max_id + 1, max_id + 1 + len(new))):
        raise SystemExit(f"knowledge.json ids must continue after {max_id}: {ids}")
    keys = [
        "id",
        "knowledge",
        "description",
        "definition",
        "type",
        "children_knowledge",
    ]
    for entry in new:
        kb_lines.append(json.dumps({k: entry[k] for k in keys}, ensure_ascii=False))
    (dst / (DB + "_kb.jsonl")).write_text("\n".join(kb_lines), encoding="utf-8")

    # Schema text: the original, then the new table in the same layout.
    schema = (src / (DB + "_schema.txt")).read_text(encoding="utf-8")
    schema = schema.rstrip("\n") + "\n\n\n" + schema_block(rules)
    (dst / (DB + "_schema.txt")).write_text(schema, encoding="utf-8")

    # Column meanings: the original, plus one key per new column.
    meanings = json.loads(
        (src / (DB + "_column_meaning_base.json")).read_text(encoding="utf-8")
    )
    for name, _, _, meaning in COLUMNS:
        meanings[f"{DB}|{TABLE}|{name}"] = meaning
    (dst / (DB + "_column_meaning_base.json")).write_text(
        json.dumps(meanings, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return dst


def main() -> None:
    rules = generate_rules()
    db = build_database(rules)
    txt = build_text_files(rules)
    print(f"wrote {db} ({len(rules)} rules)")
    print(f"wrote {txt}")


if __name__ == "__main__":
    main()
