"""One KB entry's compile: the prompt, the two tools, the submission rules.

The model gets `run_sql` (read-only, at most 50 rows, every call logged)
and `submit_entry`, whose parameters depend on the entry's kind. A
submission is validated before it is accepted -- a metric's SQL must run
over its source table, an illustration's target must exist -- and the model
has `MAX_ATTEMPTS` tries. An entry that never validates is KEPT: a
calculation or predicate becomes a metric with an empty `sql_expression`
and `untranslated` set to the last error, so its KB text still reaches the
contract arm (the library carries `untranslated` for exactly this).

The driver -- the model loop -- is injected. Tests script the tool calls;
`dce.compile.run.claude_driver` is the only code that spends money.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Literal

from pydantic import BaseModel

from dce.compile.db import CompileDB
from dce.compile.entries import KBEntry, domain_of, metric_names, slug
from dce.compile.inputs import CompileInputs

COMPILER_MODEL = "claudesonnet5"
MAX_ATTEMPTS = 3
#: Starts the paragraph that carries what the compiler learned from the
#: data. `manual_compiled` collects every such paragraph from the contract,
#: so both arms see the compiler's knowledge (spec, Compiler: Notes).
NOTE_MARKER = "Compiler note: "

SYSTEM_PROMPT = """\
You compile a database's written documentation into a data contract, one \
entry at a time. An analyst agent will later answer questions with the \
contract: each metric you write is a SQL expression it can look up by name \
and reuse.

Rules:
- Write DuckDB SQL. {dialect_note}
- A calculation's sql_expression is evaluated as \
`SELECT <sql_expression> FROM <source_model>`; a predicate's as \
`SELECT * FROM <source_model> WHERE <sql_expression>`. Use only columns of \
source_model; reach other tables through scalar subqueries.
- Encode what the entry states. Where the data forces a choice the entry \
does not make (how a value is stored, a JSON path, a unit), check it with \
run_sql, encode it, and say so in `note`. Add nothing else.
- run_sql is read-only and returns at most 50 rows.
- Call submit_entry when you are done. If it returns an error, fix the \
submission and call it again; you have {attempts} attempts. When it \
returns "Recorded.", reply "done".

## Database schema

{ddl}

## Column meanings

{meanings}
"""

INSTRUCTIONS: dict[str, str] = {
    "calculation": (
        "Compile this calculation into one metric: a SQL expression computing "
        "the value over one table. Call submit_entry(sql_expression, "
        "source_model, note)."
    ),
    "predicate": (
        "Compile this definition into one metric whose SQL expression is a "
        "boolean condition over the rows of one table. Call "
        "submit_entry(sql_expression, source_model, note)."
    ),
    "illustration": (
        "This entry explains values. Find the table or column it describes and "
        "call submit_entry(target, note), with target 'table' or "
        "'table.column', or '' if it describes no single one."
    ),
    "section": (
        "This is one section of the manual. Submit every rule in it that can be "
        "computed over the database as a metric (a calculation, or a predicate "
        "over rows), or no metrics if the section is prose only. Call "
        "submit_entry(metrics, note)."
    ),
}


class SectionMetric(BaseModel):
    name: str
    kind: Literal["calculation", "predicate"]
    sql_expression: str
    source_model: str
    description: str


@dataclass
class DriverResult:
    messages: list
    usage: dict


#: (*, system_prompt, user_prompt, tools) -> DriverResult
Driver = Callable[..., DriverResult]


@dataclass
class EntryRecord:
    entry_id: int
    kind: str
    name: str
    domain: str
    status: str  # "metric" | "untranslated" | "prose"
    metrics: list[dict]
    prose: list[dict]
    note: str
    attempts: int
    error: str | None
    queries: list[dict] = field(default_factory=list)
    usage: dict = field(default_factory=dict)
    messages: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"type": "entry", **asdict(self)}

    @classmethod
    def from_dict(cls, data: dict) -> EntryRecord:
        return cls(**{k: v for k, v in data.items() if k != "type"})


def system_prompt(inputs: CompileInputs) -> str:
    return SYSTEM_PROMPT.format(
        dialect_note=inputs.dialect_note,
        attempts=MAX_ATTEMPTS,
        ddl=inputs.ddl,
        meanings=inputs.column_meanings or "(none)",
    )


def _one_paragraph(note: str) -> str:
    # A note is one paragraph, so `manual_compiled` can find it by its marker.
    return " ".join(note.split())


class _EntrySession:
    def __init__(self, entry, inputs, db, *, prior, tables) -> None:
        self.entry = entry
        self.inputs = inputs
        self.db = db
        self.prior = prior
        self.tables = tables
        self.by_id = {e.id: e for e in inputs.entries}
        self.names = metric_names(inputs.entries)
        self.taken = set(self.names.values()) | {
            m["name"] for r in prior.values() for m in r.metrics
        }
        self.attempts = 0
        self.error: str | None = None
        self.accepted: dict | None = None
        self.queries: list[dict] = []

    # -- resolution ---------------------------------------------------------
    def _table(self, name: str) -> str | None:
        want = name.strip().strip('"').lower().removeprefix("main.")
        return next((t for t in self.tables if t.lower() == want), None)

    def _column(self, table: str, name: str) -> str | None:
        want = name.strip().strip('"').lower()
        return next((c for c, _ in self.tables[table] if c.lower() == want), None)

    def _metric(self, expr: str, source: str, *, predicate: bool):
        table = self._table(source)
        if table is None:
            return None, f"no table {source!r}; tables are {sorted(self.tables)}"
        error = self.db.check_metric(expr, f"main.{table}", predicate=predicate)
        if error is not None:
            return None, error
        return {"sql_expression": expr.strip(), "source_model": f"main.{table}"}, None

    # -- the submission rule --------------------------------------------------
    def _submit(self, check: Callable[[], tuple[dict | None, str | None]]) -> str:
        if self.accepted is not None:
            return "Already recorded."
        if self.attempts >= MAX_ATTEMPTS:
            return "No attempts left; the entry is kept as prose. Stop."
        self.attempts += 1
        accepted, error = check()
        if error is None and accepted is not None:
            self.accepted, self.error = accepted, None
            return "Recorded."
        self.error = error
        left = MAX_ATTEMPTS - self.attempts
        if left:
            return (
                f"ERROR: {error}\nFix it and call submit_entry again "
                f"({left} attempt(s) left)."
            )
        return f"ERROR: {error}\nNo attempts left; the entry is kept as prose. Stop."

    # -- tools ----------------------------------------------------------------
    def tools(self) -> list[Callable]:
        db, queries = self.db, self.queries

        def run_sql(sql: str) -> str:
            """Run a read-only SQL query on the database; at most 50 rows come back."""
            out = db.run_sql(sql)
            queries.append({"sql": sql, "error": out.startswith("ERROR:")})
            return out

        kind = self.entry.kind
        if kind in ("calculation", "predicate"):
            predicate = kind == "predicate"

            def submit_entry(
                sql_expression: str, source_model: str, note: str = ""
            ) -> str:
                """Submit this entry's metric: a SQL expression over one table, and
                a note on anything you checked in the data to write it."""

                def check():
                    accepted, error = self._metric(
                        sql_expression, source_model, predicate=predicate
                    )
                    return (accepted and {**accepted, "note": note}), error

                return self._submit(check)

        elif kind == "illustration":

            def submit_entry(target: str, note: str = "") -> str:  # type: ignore[no-redef]
                """Submit where this entry belongs: 'table', 'table.column', or ''
                for neither, and a note on anything you checked in the data."""

                def check():
                    if not target.strip():
                        return {"target": "domain", "note": note}, None
                    table_part, _, column_part = target.partition(".")
                    table = self._table(table_part)
                    if table is None:
                        return None, f"no table {table_part!r}"
                    if not column_part:
                        return {"target": "table", "table": table, "note": note}, None
                    column = self._column(table, column_part)
                    if column is None:
                        return None, f"no column {column_part!r} in {table}"
                    return {
                        "target": "column",
                        "table": table,
                        "column": column,
                        "note": note,
                    }, None

                return self._submit(check)

        else:

            def submit_entry(metrics: list[SectionMetric], note: str = "") -> str:  # type: ignore[no-redef]
                """Submit the section's metrics (possibly none), and a note on
                anything you checked in the data to write them."""

                def check():
                    out, seen = [], set()
                    for raw in metrics:
                        m = SectionMetric.model_validate(raw)
                        name = slug(m.name)
                        if name in self.taken or name in seen:
                            return None, f"metric name {name!r} is already used"
                        seen.add(name)
                        accepted, error = self._metric(
                            m.sql_expression,
                            m.source_model,
                            predicate=m.kind == "predicate",
                        )
                        if error is not None or accepted is None:
                            return None, f"metric {name!r}: {error}"
                        out.append(
                            {"name": name, "description": m.description, **accepted}
                        )
                    return {"metrics": out, "note": note}, None

                return self._submit(check)

        return [run_sql, submit_entry]

    # -- the record -----------------------------------------------------------
    def _ref(self, child: int) -> str:
        return self.names.get(child, self.by_id[child].name)

    def _with_note(self, text: str, note: str) -> str:
        note = _one_paragraph(note)
        return f"{text}\n\n{NOTE_MARKER}{note}" if note else text

    def user_prompt(self) -> str:
        entry = self.entry
        parts = [INSTRUCTIONS[entry.kind], f"## Entry: {entry.name}\n\n{entry.text}"]
        built = [
            f"- {m['name']} over {m['source_model']}: {m['sql_expression']}"
            for child in entry.children
            for m in (self.prior[child].metrics if child in self.prior else [])
            if m["sql_expression"]
        ]
        if built:
            parts.append("## Compiled entries it builds on\n\n" + "\n".join(built))
        return "\n\n".join(parts)

    def record(self, result: DriverResult) -> EntryRecord:
        entry, got = self.entry, self.accepted
        note = _one_paragraph(got.get("note", "")) if got else ""
        metrics: list[dict] = []
        prose: list[dict] = []
        if entry.kind in ("calculation", "predicate"):
            text = entry.text
            if entry.children:
                text += (
                    "\n\nBuilt on: "
                    + ", ".join(self._ref(c) for c in entry.children)
                    + "."
                )
            if got:
                status = "metric"
                metrics.append(
                    {
                        "name": self.names[entry.id],
                        "description": self._with_note(text, note),
                        "sql_expression": got["sql_expression"],
                        "source_model": got["source_model"],
                    }
                )
            else:
                status = "untranslated"
                metrics.append(
                    {
                        "name": self.names[entry.id],
                        "description": text,
                        "sql_expression": "",
                        "source_model": "",
                        "untranslated": self.error or "no submission",
                    }
                )
        elif entry.kind == "illustration":
            status = "prose"
            target = got or {"target": "domain"}
            prose.append(
                {
                    "target": target["target"],
                    "table": target.get("table"),
                    "column": target.get("column"),
                    "text": self._with_note(f"{entry.name}: {entry.text}", note),
                }
            )
        else:
            for m in got["metrics"] if got else []:
                metrics.append(
                    {
                        "name": m["name"],
                        "description": f"{m['description'].strip()}\n\n"
                        f'From the manual section "{entry.name}".',
                        "sql_expression": m["sql_expression"],
                        "source_model": m["source_model"],
                    }
                )
            status = "metric" if metrics else "prose"
            prose.append(
                {
                    "target": "domain",
                    "table": None,
                    "column": None,
                    "text": self._with_note(entry.text, note),
                }
            )
        return EntryRecord(
            entry_id=entry.id,
            kind=entry.kind,
            name=entry.name,
            domain=domain_of(entry),
            status=status,
            metrics=metrics,
            prose=prose,
            note=note,
            attempts=self.attempts,
            error=self.error,
            queries=self.queries,
            usage=result.usage,
            messages=result.messages,
        )


def compile_entry(
    entry: KBEntry,
    inputs: CompileInputs,
    db: CompileDB,
    driver: Driver,
    *,
    prior: dict[int, EntryRecord],
    tables: dict[str, list[tuple[str, str]]],
) -> EntryRecord:
    session = _EntrySession(entry, inputs, db, prior=prior, tables=tables)
    result = driver(
        system_prompt=system_prompt(inputs),
        user_prompt=session.user_prompt(),
        tools=session.tools(),
    )
    return session.record(result)
