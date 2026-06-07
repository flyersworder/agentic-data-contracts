from __future__ import annotations

SYSTEM = (
    "You connect database tables. Given a schema and a set of tables, output ONLY the "
    "JOIN conditions needed to connect those tables into one query. Use only columns "
    "that appear in the schema. Respond with a JSON array of strings like "
    '["ta.cx = tb.cy", ...] and nothing else.'
)

USER_TEMPLATE = (
    "Schema:\n{rendering}\n\n"
    "Connect these tables into a single joined query: {tables}.\n"
    "Return the minimal set of join conditions as a JSON array of "
    '"ta.cx = tb.cy" strings.'
)


def build_messages(rendering: str, endpoint_tables: list[str]) -> list[dict]:
    tables = ", ".join(endpoint_tables)
    return [
        {"role": "system", "content": SYSTEM},
        {
            "role": "user",
            "content": USER_TEMPLATE.format(rendering=rendering, tables=tables),
        },
    ]
