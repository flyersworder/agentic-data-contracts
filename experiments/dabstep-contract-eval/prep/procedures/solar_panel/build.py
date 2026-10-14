"""Build the procedures variant of the solar_panel LiveSQLBench database.

Copies the pristine DuckDB file and the hf-full-v1 public files from LSB_DATA
(default ~/data/livesqlbench) to PROC_DATA (default ~/data/procedures), adds
the module warranty settlement rules table (piece R) to the database, and
extends the KB, the schema text and the column meanings.

Deterministic (fixed seed) and idempotent: every run starts again from the
pristine files and overwrites the outputs.

    UV_FROZEN=1 uv run python prep/procedures/solar_panel/build.py
"""

import json
import os
import random
import shutil
from pathlib import Path

import duckdb

DB = "solar_panel"
SEED = 20261014
N_RULES = 520
TABLE = "warranty_settle_rules"

HERE = Path(__file__).resolve().parent
LSB_DATA = Path(os.environ.get("LSB_DATA", "~/data/livesqlbench")).expanduser()
PROC_DATA = Path(os.environ.get("PROC_DATA", "~/data/procedures")).expanduser()

MAKERS = ["Canadian Solar", "JA Solar", "JinkoSolar", "Longi", "Trina"]
KINDS = ["Bifacial", "HJT", "Mono-PERC", "Poly-PERC", "TOPCon"]
ALERTS = ["Critical", "Warning"]
WARRANTY = ["Active", "Claimed", "Expired"]
CAP_BANDS = ["Small", "Mid", "Large"]
VOL_TIERS = ["Single", "Moderate", "Bulk"]

# criterion column -> (domain, weight for being specified in a rule)
CRITERIA = {
    "maker_tag": (MAKERS, 0.50),
    "pnl_kind": (KINDS, 0.40),
    "alrt_state": (ALERTS, 0.50),
    "warr_state": (WARRANTY, 0.60),
    "cap_band": (CAP_BANDS, 0.45),
    "vol_tier": (VOL_TIERS, 0.80),
}
# number of specified criteria -> weight
K_WEIGHTS = {2: 0.06, 3: 0.22, 4: 0.33, 5: 0.26, 6: 0.13}

COVER_BASE = {
    "Active": (11.0, 18.0),
    "Claimed": (7.0, 12.0),
    "Expired": (0.0, 2.5),
    None: (5.0, 11.0),
}
COVER_ALERT = {"Critical": (2.0, 5.0), "Warning": (0.5, 2.0), None: (0.0, 0.0)}
COVER_TIER = {
    "Single": (0.0, 0.0),
    "Moderate": (-1.5, 0.0),
    "Bulk": (-4.0, -2.0),
    None: (-1.0, 0.0),
}
COVER_CAP = {"Small": 1.0, "Mid": 0.0, "Large": -1.0, None: 0.0}
COVER_MAKER = {
    "Canadian Solar": -1.0,
    "JA Solar": 1.0,
    "JinkoSolar": 0.0,
    "Longi": 0.5,
    "Trina": -0.5,
    None: 0.0,
}
COVER_KIND = {
    "Bifacial": 0.0,
    "HJT": 0.5,
    "Mono-PERC": 0.0,
    "Poly-PERC": -1.0,
    "TOPCon": 0.5,
    None: 0.0,
}
FEE_TIER = {
    "Single": (250.0, 600.0),
    "Moderate": (150.0, 400.0),
    "Bulk": (50.0, 250.0),
    None: (100.0, 450.0),
}


def weighted_sample(rng: random.Random, weights: dict[str, float], k: int) -> list[str]:
    """k distinct keys drawn without replacement, proportional to weight."""
    pool = dict(weights)
    chosen = []
    for _ in range(k):
        total = sum(pool.values())
        x = rng.random() * total
        for key, w in pool.items():
            x -= w
            if x <= 0:
                break
        chosen.append(key)
        del pool[key]
    return chosen


def generate_rules() -> list[dict]:
    rng = random.Random(SEED)
    rules: list[dict] = []
    seen: set[tuple] = set()
    ks = list(K_WEIGHTS)
    kw = list(K_WEIGHTS.values())
    while len(rules) < N_RULES:
        k = rng.choices(ks, weights=kw)[0]
        picked = weighted_sample(rng, {c: w for c, (_, w) in CRITERIA.items()}, k)
        crit = {
            c: (rng.choice(CRITERIA[c][0]) if c in picked else None) for c in CRITERIA
        }
        key = tuple(crit[c] for c in CRITERIA)
        if key in seen:
            continue
        seen.add(key)
        cover = rng.uniform(*COVER_BASE[crit["warr_state"]])
        cover += rng.uniform(*COVER_ALERT[crit["alrt_state"]])
        cover += rng.uniform(*COVER_TIER[crit["vol_tier"]])
        cover += (
            COVER_CAP[crit["cap_band"]]
            + COVER_MAKER[crit["maker_tag"]]
            + COVER_KIND[crit["pnl_kind"]]
        )
        cover = min(max(cover, 0.0), 25.0)
        fee = rng.uniform(*FEE_TIER[crit["vol_tier"]])
        if crit["cap_band"] == "Large":
            fee += 100.0
        if crit["warr_state"] == "Expired":
            fee = rng.uniform(0.0, 150.0)
        rules.append(
            {
                "rule_id": len(rules) + 1,
                **crit,
                "fixed_fee_usd": round(fee, 2),
                "cover_pct": round(cover, 2),
            }
        )
    return rules


COLUMNS = [
    ("rule_id", "INTEGER NOT NULL", "integer NOT NULL"),
    ("maker_tag", "VARCHAR", "text NULL"),
    ("pnl_kind", "VARCHAR", "text NULL"),
    ("alrt_state", "VARCHAR", "text NULL"),
    ("warr_state", "VARCHAR", "text NULL"),
    ("cap_band", "VARCHAR", "text NULL"),
    ("vol_tier", "VARCHAR", "text NULL"),
    ("fixed_fee_usd", "DOUBLE", "double precision NULL"),
    ("cover_pct", "DOUBLE", "double precision NULL"),
]

COLUMN_MEANINGS = {
    "rule_id": (
        "INTEGER. Unique identifier of the warranty settlement rule. Example: 17."
    ),
    "maker_tag": (
        "TEXT. Panel manufacturer the rule applies to. NULL means the rule is not "
        "restricted by manufacturer. Possible values: Canadian Solar, JA Solar, "
        "JinkoSolar, Longi, Trina."
    ),
    "pnl_kind": (
        "TEXT. Panel technology the rule applies to. NULL means the rule is not "
        "restricted by technology. Possible values: Bifacial, HJT, Mono-PERC, "
        "Poly-PERC, TOPCon."
    ),
    "alrt_state": (
        "TEXT. Snapshot alert status the rule applies to. NULL means the rule is "
        "not restricted by alert status. Possible values: Critical, Warning."
    ),
    "warr_state": (
        "TEXT. Plant equipment warranty status the rule applies to. NULL means the "
        "rule is not restricted by warranty status. Possible values: Active, "
        "Claimed, Expired."
    ),
    "cap_band": (
        "TEXT. Plant capacity band the rule applies to. NULL means the rule is not "
        "restricted by capacity band. Possible values: Small, Mid, Large."
    ),
    "vol_tier": (
        "TEXT. Manufacturer monthly claim volume tier the rule applies to. NULL "
        "means the rule is not restricted by volume tier. Possible values: Single, "
        "Moderate, Bulk."
    ),
    "fixed_fee_usd": (
        "DOUBLE. Fixed handling fee per settled snapshot, in US dollars. "
        "Example: 312.45."
    ),
    "cover_pct": (
        "DOUBLE. Coverage percentage applied to the snapshot's component replacement "
        "cost. Example: 12.8."
    ),
}


def fmt_cell(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.2f}".rstrip("0").rstrip(".") if v != int(v) else str(int(v))
    return str(v)


def sample_table(rows: list[dict]) -> str:
    """The first rows in the plain-table style of the original schema file:
    two-space column gaps, dashed header rule, numbers right-aligned."""
    names = [c for c, _, _ in COLUMNS]
    cells = [[fmt_cell(r[c]) for c in names] for r in rows]
    numeric = [
        isinstance(rows[0][c], (int, float)) and not isinstance(rows[0][c], bool)
        for c in names
    ]
    widths = [
        max(len(names[i]), *(len(row[i]) for row in cells)) for i in range(len(names))
    ]

    def line(vals: list[str]) -> str:
        out = []
        for i, v in enumerate(vals):
            out.append(v.rjust(widths[i]) if numeric[i] else v.ljust(widths[i]))
        return "  ".join(out)

    lines = [line(names), "  ".join("-" * w for w in widths)]
    lines += [line(row) for row in cells]
    return "\n".join(lines)


def schema_block(rules: list[dict]) -> str:
    body = ",\n".join(f"{c} {pg}" for c, _, pg in COLUMNS)
    ddl = f'CREATE TABLE "{TABLE}" (\n{body},\n    PRIMARY KEY (rule_id)\n);'
    return f"{ddl}\n\nFirst 3 rows:\n{sample_table(rules[:3])}\n...\n"


def build_db(rules: list[dict]) -> Path:
    src = LSB_DATA / "duckdb" / f"{DB}.duckdb"
    dst = PROC_DATA / "duckdb" / f"{DB}.duckdb"
    dst.parent.mkdir(parents=True, exist_ok=True)
    for stale in (dst, dst.with_name(dst.name + ".wal")):
        if stale.exists():
            stale.unlink()
    shutil.copyfile(src, dst)
    con = duckdb.connect(str(dst))
    try:
        cols = ", ".join(f"{c} {duck}" for c, duck, _ in COLUMNS)
        con.execute(f"CREATE TABLE {TABLE} ({cols}, PRIMARY KEY (rule_id))")
        names = [c for c, _, _ in COLUMNS]
        marks = ", ".join("?" for _ in names)
        con.executemany(
            f"INSERT INTO {TABLE} VALUES ({marks})",
            [[r[c] for c in names] for r in rules],
        )
        # The pristine file mirrors every table as a view in schema public
        # (PostgreSQL-style names); the new table gets the same.
        has_public = con.execute(
            "SELECT count(*) FROM duckdb_views() "
            "WHERE schema_name = 'public' AND NOT internal"
        ).fetchone()
        if has_public and has_public[0]:
            con.execute(f"CREATE VIEW public.{TABLE} AS SELECT * FROM main.{TABLE}")
        con.execute("CHECKPOINT")
    finally:
        con.close()
    return dst


def build_public_files(rules: list[dict]) -> Path:
    src = LSB_DATA / "hf-full-v1" / DB
    dst = PROC_DATA / "hf-full-v1" / DB
    dst.mkdir(parents=True, exist_ok=True)

    knowledge = json.loads((HERE / "knowledge.json").read_text(encoding="utf-8"))
    kb_lines = (src / f"{DB}_kb.jsonl").read_text(encoding="utf-8").splitlines()
    kb_lines = [ln for ln in kb_lines if ln.strip()]
    max_id = max(json.loads(ln)["id"] for ln in kb_lines)
    new_ids = [k["id"] for k in knowledge]
    if new_ids != list(range(max_id + 1, max_id + 1 + len(knowledge))):
        raise SystemExit(f"knowledge.json ids must continue after {max_id}: {new_ids}")
    for k in knowledge:
        entry = {
            key: k[key]
            for key in (
                "id",
                "knowledge",
                "description",
                "definition",
                "type",
                "children_knowledge",
            )
        }
        kb_lines.append(json.dumps(entry, ensure_ascii=False))
    (dst / f"{DB}_kb.jsonl").write_text("\n".join(kb_lines), encoding="utf-8")

    schema = (src / f"{DB}_schema.txt").read_text(encoding="utf-8")
    schema = schema.rstrip("\n") + "\n\n\n"
    (dst / f"{DB}_schema.txt").write_text(
        schema + schema_block(rules), encoding="utf-8"
    )

    meanings = json.loads(
        (src / f"{DB}_column_meaning_base.json").read_text(encoding="utf-8")
    )
    for col, meaning in COLUMN_MEANINGS.items():
        meanings[f"{DB}|{TABLE}|{col}"] = meaning
    (dst / f"{DB}_column_meaning_base.json").write_text(
        json.dumps(meanings, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return dst


def main() -> None:
    rules = generate_rules()
    db = build_db(rules)
    files = build_public_files(rules)
    print(f"wrote {db} ({len(rules)} rules in {TABLE})")
    print(f"wrote {files}")


if __name__ == "__main__":
    main()
