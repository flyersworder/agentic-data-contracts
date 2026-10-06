"""A compiled contract from its compile records: `contract.yml` and
`semantic.yml`, built from the records alone so a log replays to the same
files. Domains appear in entry-id order of first use; metrics in entry-id
order; tables and columns as DuckDB lists them, each column's description
its meaning plus any illustration placed on it."""

from __future__ import annotations

from pathlib import Path

import yaml

from dce.compile.inputs import CompileInputs
from dce.compile.session import COMPILER_MODEL, NOTE_MARKER, EntryRecord

#: As the hand-authored DABStep contract forbids.
FORBIDDEN_OPERATIONS = [
    "DELETE",
    "DROP",
    "TRUNCATE",
    "UPDATE",
    "INSERT",
    "CREATE",
    "ALTER",
    "MERGE",
    "GRANT",
    "REVOKE",
    "COPY",
]

#: The hand-authored contract's two rules, worded for any benchmark.
RULES = [
    {
        "name": "no_select_star",
        "description": "Queries must name their columns; use preview_table to explore",
        "enforcement": "block",
        "query_check": {"no_select_star": True},
    },
    {
        "name": "use_declared_definitions",
        "description": (
            "Computations must follow the metric definitions in the semantic "
            "source rather than an ad-hoc formula."
        ),
        "enforcement": "warn",
    },
]

KIND_DOMAINS: dict[str, tuple[str, str]] = {
    "calculations": (
        "Computed measures defined in the knowledge base",
        "Calculations from the knowledge base, each a metric. Look a metric up "
        "by name for its definition and SQL.",
    ),
    "definitions": (
        "Business conditions defined in the knowledge base",
        "Definitions from the knowledge base, each a metric whose SQL is a "
        "boolean condition over the rows of its table.",
    ),
    "reference": (
        "Value explanations that belong to no single table or column",
        "Explanations of values from the knowledge base.",
    ),
}


def _join(*parts: str) -> str:
    return "\n\n".join(p for p in parts if p)


def build(
    inputs: CompileInputs,
    records: list[EntryRecord],
    tables: dict[str, list[tuple[str, str]]],
) -> tuple[dict, dict]:
    records = sorted(records, key=lambda r: r.entry_id)
    domains: dict[str, dict] = {}
    table_desc = {t.lower(): d for t, d in inputs.table_descriptions.items()}
    column_desc = dict(inputs.column_descriptions)
    metrics = []

    def domain(record: EntryRecord) -> dict:
        if record.domain not in domains:
            summary, description = KIND_DOMAINS.get(record.domain, (record.name, ""))
            domains[record.domain] = {
                "name": record.domain,
                "summary": summary,
                "description": description,
            }
        return domains[record.domain]

    for record in records:
        for metric in record.metrics:
            domain(record)
            out = {
                k: metric[k]
                for k in ("name", "description", "sql_expression", "source_model")
            }
            out["domains"] = [record.domain]
            if metric.get("untranslated"):
                out["untranslated"] = metric["untranslated"]
            metrics.append(out)
        for prose in record.prose:
            if prose["target"] == "column":
                key = (prose["table"].lower(), prose["column"].lower())
                column_desc[key] = _join(column_desc.get(key, ""), prose["text"])
            elif prose["target"] == "table":
                key = prose["table"].lower()
                table_desc[key] = _join(table_desc.get(key, ""), prose["text"])
            else:
                d = domain(record)
                d["description"] = _join(d["description"], prose["text"])

    contract = {
        "version": "1.0",
        "name": f"{inputs.benchmark}-{inputs.db_name}",
        "semantic": {
            "source": {"type": "yaml", "path": "./semantic.yml", "expected_extras": []},
            "allowed_tables": [
                {"schema": "main", "preferred": True, "tables": sorted(tables)}
            ],
            "forbidden_operations": FORBIDDEN_OPERATIONS,
            "domains": list(domains.values()),
            "rules": RULES,
        },
    }
    semantic_tables = []
    for table in sorted(tables):
        entry: dict = {"schema": "main", "table": table}
        if table_desc.get(table.lower()):
            entry["description"] = table_desc[table.lower()]
        entry["columns"] = [
            {
                "name": column,
                "type": data_type,
                "description": column_desc.get((table.lower(), column.lower()), ""),
            }
            for column, data_type in tables[table]
        ]
        semantic_tables.append(entry)
    return contract, {"metrics": metrics, "tables": semantic_tables}


HEADER = (
    "# Compiled by dce.compile -- do not edit; recompile instead.\n"
    "# benchmark: {benchmark}, database: {db}, compiler: {model}\n"
    "# inputs: {inputs}\n"
)


def write(out_dir: Path, contract: dict, semantic: dict, inputs: CompileInputs) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    header = HEADER.format(
        benchmark=inputs.benchmark,
        db=inputs.db_name,
        model=COMPILER_MODEL,
        inputs=", ".join(p.name for p in inputs.sources),
    )
    for name, data in (("contract.yml", contract), ("semantic.yml", semantic)):
        body = yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=100)
        (out_dir / name).write_text(header + body, encoding="utf-8")


def _notes(text: str | None) -> list[str]:
    return [
        p[len(NOTE_MARKER) :].strip()
        for p in (text or "").split("\n\n")
        if p.startswith(NOTE_MARKER)
    ]


def render_compiled(contract_dir: Path) -> str:
    """`manual_compiled`'s appendix: each translated metric's name, table and
    SQL exactly as the contract holds them, then every compiler note on a
    table, column or domain."""
    semantic = yaml.safe_load(
        (contract_dir / "semantic.yml").read_text(encoding="utf-8")
    )
    contract = yaml.safe_load(
        (contract_dir / "contract.yml").read_text(encoding="utf-8")
    )
    blocks = []
    for metric in semantic.get("metrics") or []:
        if not metric.get("sql_expression"):
            continue
        block = (
            f"### {metric['name']}\n\nOver `{metric['source_model']}`:\n\n"
            f"```sql\n{metric['sql_expression']}\n```"
        )
        for note in _notes(metric.get("description")):
            block += f"\n\n{NOTE_MARKER}{note}"
        blocks.append(block)
    notes = []
    for table in semantic.get("tables") or []:
        notes += [f"- {table['table']}: {n}" for n in _notes(table.get("description"))]
        for column in table.get("columns") or []:
            notes += [
                f"- {table['table']}.{column['name']}: {n}"
                for n in _notes(column.get("description"))
            ]
    for d in contract["semantic"].get("domains") or []:
        notes += [f"- {d['name']}: {n}" for n in _notes(d.get("description"))]
    if notes:
        blocks.append("### Notes on the data\n\n" + "\n".join(notes))
    return "\n\n".join(blocks)
