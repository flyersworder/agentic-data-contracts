"""Build the procedures variant of labor_certification_applications.

Copies the pristine DuckDB from LSB_DATA to PROC_DATA and adds the
`review_schedule` table (the Case Review Effort Schedule, knowledge piece R),
then writes the extended public files: the KB (original entries plus
knowledge.json without its "piece" field), the schema text (original plus the
new table) and the column meanings (original plus the new columns).

Deterministic (fixed seed, rules anchored on cases taken in filekey order) and
idempotent: every run starts again from the pristine files.

Run: UV_FROZEN=1 uv run python prep/procedures/labor_certification_applications/build.py
"""

import json
import os
import random
import shutil
from decimal import Decimal
from pathlib import Path

import duckdb

DB = "labor_certification_applications"
TABLE = "review_schedule"
SEED = 11
HERE = Path(__file__).resolve().parent
LSB_DATA = Path(os.environ.get("LSB_DATA", Path.home() / "data" / "livesqlbench"))
PROC_DATA = Path(os.environ.get("PROC_DATA", Path.home() / "data" / "procedures"))

FIELDS = (
    "visa_class",
    "wage_level",
    "h1b_dependent",
    "soc_major_group",
    "worksite_state",
    "monthly_volume_band",
)

# How often each criterion is specified when a rule restricts k of them.
FIELD_WEIGHT = {
    "visa_class": 0.6,
    "wage_level": 1.0,
    "h1b_dependent": 0.8,
    "soc_major_group": 1.0,
    "worksite_state": 0.7,
    "monthly_volume_band": 1.3,
}

STATES = (
    "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS "
    "MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY"
).split()

# Values a free (not anchored) rule draws from, uniformly. Uniform draws keep
# free rules narrow on this data (97% of cases are H-1B), so some cases match
# no rule and fall to the default schedule.
DOMAIN = {
    "visa_class": ["H-1B", "E-3 Australian", "H-1B1 Singapore", "H-1B1 Chile"],
    "wage_level": ["I", "II", "III", "IV"],
    "h1b_dependent": ["Yes", "No"],
    "soc_major_group": [
        "11", "13", "15", "17", "19", "21", "23", "25", "27", "29",
        "31", "33", "35", "41", "43", "47", "49", "51", "53",
    ],
    "worksite_state": STATES,
    "monthly_volume_band": ["1", "2-4", "5-14", "15+"],
}  # fmt: skip

# Broad single-criterion rules every schedule carries.
BROAD_RULES = (
    {"visa_class": "E-3 Australian"},
    {"visa_class": "H-1B1 Singapore"},
    {"visa_class": "H-1B1 Chile"},
    {"wage_level": "I"},
    {"wage_level": "IV"},
    {"monthly_volume_band": "15+"},
)

# Number of rules restricting k criteria, and the share anchored on a real case.
PLAN = {2: (30, 0.0), 3: (150, 0.0), 4: (180, 0.25), 5: (100, 0.45), 6: (30, 0.5)}

# Effort model the hours are drawn around (reviewer-hours).
ADJ = {
    "visa_class": {
        "H-1B": 0.0, "E-3 Australian": 0.5,
        "H-1B1 Singapore": 0.75, "H-1B1 Chile": 0.75,
    },
    "wage_level": {"I": 0.75, "II": 0.25, "III": 0.0, "IV": -0.25},
    "h1b_dependent": {"Yes": 1.5, "No": 0.0},
    "soc_major_group": {"29": 1.0, "25": 0.5, "21": 0.5, "15": -0.25},
    "worksite_state": {"CA": 0.25, "NY": 0.25, "NJ": 0.25, "MA": 0.25},
    "monthly_volume_band": {"1": 1.25, "2-4": 0.5, "5-14": -0.25, "15+": -0.75},
}  # fmt: skip

# Attributes each case is matched on (wage level and H-1B dependency may be NULL).
CASE_ATTRIBUTES_SQL = """
WITH base AS (
    SELECT c.filekey, c.homefirm,
           strftime(strptime(c.beginday, '%d/%m/%Y'), '%Y-%m') AS start_month,
           CAST(c.visacls AS VARCHAR) AS visa_class,
           p.wage_details->'prevailing_wage'->>'level' AS wage_level,
           CAST(c.h1bdep AS VARCHAR) AS h1b_dependent,
           substr(c.soccd, 1, 2) AS soc_major_group,
           cw.wsstate AS worksite_state
    FROM cases c
    JOIN case_worksite cw ON cw.dockkey = c.filekey
    JOIN prevailing_wage p ON p.trackno = cw.wagetrack
), vol AS (
    SELECT *, count(*) OVER (PARTITION BY homefirm, start_month) AS emcv FROM base
)
SELECT filekey, visa_class, wage_level, h1b_dependent, soc_major_group, worksite_state,
       CASE WHEN emcv = 1 THEN '1' WHEN emcv <= 4 THEN '2-4'
            WHEN emcv <= 14 THEN '5-14' ELSE '15+' END AS monthly_volume_band
FROM vol ORDER BY filekey
"""


def pick_fields(rng: random.Random, k: int, allowed: list[str]) -> list[str]:
    chosen: list[str] = []
    pool = list(allowed)
    while len(chosen) < k:
        f = rng.choices(pool, weights=[FIELD_WEIGHT[x] for x in pool], k=1)[0]
        chosen.append(f)
        pool.remove(f)
    return chosen


def hours(rng: random.Random, crit: dict[str, str]) -> tuple[Decimal, Decimal]:
    base = 2.5 + sum(ADJ[f].get(v, 0.0) for f, v in crit.items())
    base += rng.choice((-0.5, -0.25, 0.0, 0.25, 0.5))
    base = min(max(base, 1.0), 9.0)
    per = 0.25 + 0.05 * rng.randint(0, 12)
    if crit.get("h1b_dependent") == "Yes":
        per += 0.25
    return Decimal(f"{round(base * 4) / 4:.2f}"), Decimal(f"{per:.2f}")


def generate_rules(cases: list[dict]) -> list[tuple]:
    rng = random.Random(SEED)
    seen: set[tuple] = set()
    crits: list[dict[str, str]] = []

    def add(crit: dict[str, str]) -> bool:
        key = tuple(crit.get(f) for f in FIELDS)
        if key in seen:
            return False
        seen.add(key)
        crits.append(crit)
        return True

    for crit in BROAD_RULES:
        add(dict(crit))
    for k, (count, anchored_share) in PLAN.items():
        made = 0
        while made < count:
            if rng.random() < anchored_share:
                case = rng.choice(cases)
                allowed = [f for f in FIELDS if case[f] is not None]
                if len(allowed) < k:
                    continue
                crit = {f: case[f] for f in pick_fields(rng, k, allowed)}
            else:
                fields = pick_fields(rng, k, list(FIELDS))
                crit = {f: rng.choice(DOMAIN[f]) for f in fields}
            if add(crit):
                made += 1

    rows = []
    for crit in crits:
        base, per = hours(rng, crit)
        rows.append([crit.get(f) for f in FIELDS] + [base, per])
    rng.shuffle(rows)
    return [(i + 1, *row) for i, row in enumerate(rows)]


def build_db() -> list[tuple]:
    src = LSB_DATA / "duckdb" / f"{DB}.duckdb"
    dst = PROC_DATA / "duckdb" / f"{DB}.duckdb"
    dst.parent.mkdir(parents=True, exist_ok=True)
    wal = dst.with_name(dst.name + ".wal")
    if wal.exists():
        wal.unlink()
    shutil.copyfile(src, dst)
    con = duckdb.connect(str(dst))
    try:
        cur = con.execute(CASE_ATTRIBUTES_SQL)
        cols = [d[0] for d in cur.description]
        cases = [dict(zip(cols, r)) for r in cur.fetchall()]
        rules = generate_rules(cases)
        con.execute(
            f"""CREATE TABLE {TABLE} (
                rule_id BIGINT PRIMARY KEY,
                visa_class VARCHAR,
                wage_level VARCHAR,
                h1b_dependent VARCHAR,
                soc_major_group VARCHAR,
                worksite_state VARCHAR,
                monthly_volume_band VARCHAR,
                base_hours DECIMAL(5,2) NOT NULL,
                position_hours DECIMAL(5,2) NOT NULL
            )"""
        )
        con.executemany(
            f"INSERT INTO {TABLE} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rules
        )
        # The pristine file mirrors every table as a view in schema public;
        # the new table gets one too, as in the other two databases.
        con.execute(f"CREATE VIEW public.{TABLE} AS SELECT * FROM main.{TABLE}")
        con.execute("CHECKPOINT")
    finally:
        con.close()
    return rules


def format_rows(header: list[str], rows: list[tuple]) -> str:
    """Render rows as the schema file does (tabulate's 'simple' format)."""

    def cell(v) -> str:
        return "" if v is None else str(v)

    numeric = [
        all(isinstance(r[i], (int, Decimal)) for r in rows if r[i] is not None)
        for i in range(len(header))
    ]
    widths = [
        max([len(header[i])] + [len(cell(r[i])) for r in rows])
        for i in range(len(header))
    ]

    def line(values: list[str]) -> str:
        parts = [
            v.rjust(w) if num else v.ljust(w)
            for v, w, num in zip(values, widths, numeric)
        ]
        return "  ".join(parts).rstrip()

    out = [line(header), line(["-" * w for w in widths])]
    out += [line([cell(v) for v in r]) for r in rows]
    return "\n".join(out)


def schema_block(rules: list[tuple]) -> str:
    ddl = (
        f'CREATE TABLE "{TABLE}" (\n'
        "rule_id bigint NOT NULL,\n"
        "visa_class text NULL,\n"
        "wage_level text NULL,\n"
        "h1b_dependent text NULL,\n"
        "soc_major_group text NULL,\n"
        "worksite_state text NULL,\n"
        "monthly_volume_band text NULL,\n"
        "base_hours numeric NOT NULL,\n"
        "position_hours numeric NOT NULL,\n"
        "    PRIMARY KEY (rule_id)\n"
        ");"
    )
    header = ["rule_id", *FIELDS, "base_hours", "position_hours"]
    sample = format_rows(header, rules[:3])
    return ddl + "\n\nFirst 3 rows:\n" + sample + "\n...\n"


def column_meanings() -> dict[str, str]:
    p = f"{DB}|{TABLE}|"
    return {
        p + "rule_id": "BIGINT. Unique identifier of the case review effort rule. PK.",
        p + "visa_class": (
            "TEXT. Visa class criterion of the rule. **NULL means no visa class "
            "specified.**. Possible values: E-3 Australian, H-1B, H-1B1 Chile, "
            "H-1B1 Singapore."
        ),
        p + "wage_level": (
            "TEXT. Prevailing wage level criterion of the rule. **NULL means no "
            "wage level specified.**. Possible values: I, II, III, IV."
        ),
        p + "h1b_dependent": (
            "TEXT. H-1B dependency criterion of the rule. **NULL means no "
            "dependency status specified.**. Possible values: No, Yes."
        ),
        p + "soc_major_group": (
            "TEXT. SOC major group criterion of the rule (the first two digits "
            "of an SOC code). **NULL means no SOC major group specified.**. "
            "Example: 15."
        ),
        p + "worksite_state": (
            "TEXT. Worksite state criterion of the rule. **NULL means no "
            "worksite state specified.**. Example: CA."
        ),
        p + "monthly_volume_band": (
            "TEXT. Employer monthly case volume band criterion of the rule. "
            "**NULL means no volume band specified.**. Possible values: 1, "
            "15+, 2-4, 5-14."
        ),
        p + "base_hours": (
            "DECIMAL(5,2). Reviewer-hours the rule charges once per case. "
            "Example: 3.25."
        ),
        p + "position_hours": (
            "DECIMAL(5,2). Reviewer-hours the rule charges per worker position "
            "requested. Example: 0.50."
        ),
    }


def build_files(rules: list[tuple]) -> None:
    src = LSB_DATA / "hf-full-v1" / DB
    dst = PROC_DATA / "hf-full-v1" / DB
    dst.mkdir(parents=True, exist_ok=True)

    kb_name = f"{DB}_kb.jsonl"
    kb_text = (src / kb_name).read_text(encoding="utf-8").rstrip("\n")
    new = json.loads((HERE / "knowledge.json").read_text(encoding="utf-8"))
    ids = [json.loads(line)["id"] for line in kb_text.split("\n") if line]
    assert [e["id"] for e in new] == list(range(max(ids) + 1, max(ids) + 1 + len(new)))
    lines = [
        json.dumps({k: v for k, v in e.items() if k != "piece"}, ensure_ascii=False)
        for e in new
    ]
    (dst / kb_name).write_text(kb_text + "\n" + "\n".join(lines), encoding="utf-8")

    schema_name = f"{DB}_schema.txt"
    schema = (src / schema_name).read_text(encoding="utf-8")
    (dst / schema_name).write_text(
        schema + "\n\n" + schema_block(rules), encoding="utf-8"
    )

    cm_name = f"{DB}_column_meaning_base.json"
    cm = json.loads((src / cm_name).read_text(encoding="utf-8"))
    cm.update(column_meanings())
    (dst / cm_name).write_text(
        json.dumps(cm, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def main() -> None:
    rules = build_db()
    build_files(rules)
    print("wrote " + str(len(rules)) + " rules to " + str(PROC_DATA))


if __name__ == "__main__":
    main()
