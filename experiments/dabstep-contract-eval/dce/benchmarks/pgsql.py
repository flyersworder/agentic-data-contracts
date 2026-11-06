"""Read PostgreSQL SQL files without a server, for experiment 7's build.

DataAgentBench ships its PostgreSQL databases as SQL files of two kinds: plain
`pg_dump` output (CREATE TABLE, then `COPY ... FROM stdin` blocks in COPY's
text format) and plain scripts of CREATE TABLE and `INSERT ... VALUES`
statements (crmarenapro's `support.sql`). `read_sql` reads both into
tables of text values exactly as PostgreSQL would store them:

- COPY text format: fields split on tabs, `\\N` is NULL, and the backslash
  escapes (`\\b \\f \\n \\r \\t \\v \\\\`, octal `\\ooo`, hex `\\xhh`, any
  other escaped character standing for itself) are undone at the byte level,
  then decoded as UTF-8.
- INSERT literals: standard-conforming strings (`''` is a quote, a backslash
  is a backslash, as with `standard_conforming_strings = on`), `NULL`, and
  numbers kept as their literal text, which is what a text column stores.
- Identifiers fold as PostgreSQL folds them: unquoted to lower case, quoted
  ones kept. The databases' descriptions name the folded forms.

Statements that define no data (SET, set_config, ALTER ... OWNER, comments)
are skipped; anything else stops the read, so nothing is dropped silently.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

_TOKEN = re.compile(
    r"""
    (?P<ws>\s+)
  | (?P<comment>--[^\n]*)
  | (?P<estring>[Ee]'(?:[^'\\]|\\.|'')*')
  | (?P<string>'(?:[^']|'')*')
  | (?P<qident>"(?:[^"]|"")*")
  | (?P<number>-?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?)
  | (?P<word>[A-Za-z_][A-Za-z0-9_$]*)
  | (?P<punct>[;(),.=\[\]*:+-])
    """,
    re.VERBOSE | re.DOTALL,
)

#: Statement openings that carry no data and are skipped.
_SKIPPED = (
    ("SET",),
    ("SELECT", "pg_catalog"),
    ("ALTER", "TABLE"),
    ("ALTER", "SCHEMA"),
    ("COMMENT", "ON"),
    ("CREATE", "INDEX"),
    ("CREATE", "UNIQUE", "INDEX"),
)

#: PostgreSQL types read so far, and their DuckDB types. A type is added
#: here only once its values are known to round-trip; any other stops the
#: build with its name (experiment 7's spec leaves unmatched types to the
#: builder, and none occurs in phase 1).
_TYPES: dict[str, str] = {
    "text": "VARCHAR",
    "character varying": "VARCHAR",
    "varchar": "VARCHAR",
    "bigint": "BIGINT",
    "int8": "BIGINT",
    "integer": "INTEGER",
    "int": "INTEGER",
    "int4": "INTEGER",
    "smallint": "SMALLINT",
    "int2": "SMALLINT",
    "double precision": "DOUBLE",
    "float8": "DOUBLE",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
}


def _base_type(pg_type: str) -> str:
    """`character varying(255)` -> `character varying`."""
    return re.sub(r"\s*\(.*\)\s*$", "", pg_type.strip().lower())


def duckdb_type(pg_type: str) -> str:
    base = _base_type(pg_type)
    if base not in _TYPES:
        raise ValueError(
            f"PostgreSQL type {pg_type!r} has no checked DuckDB mapping; "
            "add one to dce.benchmarks.pgsql._TYPES with a test"
        )
    return _TYPES[base]


def python_value(pg_type: str, text: str | None):
    """The value a DuckDB column of `duckdb_type(pg_type)` returns to Python
    for this PostgreSQL text, for the build's per-column value check."""
    if text is None:
        return None
    duck = duckdb_type(pg_type)
    if duck in ("BIGINT", "INTEGER", "SMALLINT"):
        return int(text)
    if duck == "DOUBLE":
        return float(text)
    if duck == "BOOLEAN":
        return text.lower() in ("t", "true", "1", "y", "yes", "on")
    return text


# -- COPY text format ---------------------------------------------------------

_SIMPLE_ESCAPES = {
    ord("b"): 0x08,
    ord("f"): 0x0C,
    ord("n"): 0x0A,
    ord("r"): 0x0D,
    ord("t"): 0x09,
    ord("v"): 0x0B,
}
_OCTAL = b"01234567"
_HEX = b"0123456789abcdefABCDEF"


def _unescape(raw: bytes) -> str:
    if b"\\" not in raw:
        return raw.decode("utf-8")
    out = bytearray()
    i, n = 0, len(raw)
    while i < n:
        c = raw[i]
        if c != 0x5C or i + 1 == n:
            out.append(c)
            i += 1
            continue
        nxt = raw[i + 1]
        if nxt in _SIMPLE_ESCAPES:
            out.append(_SIMPLE_ESCAPES[nxt])
            i += 2
        elif nxt in _OCTAL:
            j = i + 1
            while j < n and j < i + 4 and raw[j] in _OCTAL:
                j += 1
            out.append(int(raw[i + 1 : j], 8) & 0xFF)
            i = j
        elif nxt == ord("x") and i + 2 < n and raw[i + 2] in _HEX:
            j = i + 2
            while j < n and j < i + 4 and raw[j] in _HEX:
                j += 1
            out.append(int(raw[i + 2 : j], 16))
            i = j
        else:
            out.append(nxt)
            i += 2
    return out.decode("utf-8")


def parse_copy_line(line: bytes) -> list[str | None]:
    """One data line of COPY's text format, without its newline."""
    return [None if f == b"\\N" else _unescape(f) for f in line.split(b"\t")]


# -- statements ---------------------------------------------------------------


@dataclass
class Table:
    name: str
    columns: list[tuple[str, str]]  # (column, PostgreSQL type)
    rows: list[list[str | None]] = field(default_factory=list)


def _fold(token: tuple[str, str]) -> str:
    kind, text = token
    if kind == "qident":
        return text[1:-1].replace('""', '"')
    if kind == "word":
        return text.lower()
    raise ValueError(f"expected an identifier, found {text!r}")


class _Reader:
    def __init__(self, text: str) -> None:
        self.text = text
        self.pos = 0

    def token(self) -> tuple[str, str] | None:
        while self.pos < len(self.text):
            m = _TOKEN.match(self.text, self.pos)
            if m is None:
                raise ValueError(
                    f"unsupported text at offset {self.pos}: "
                    f"{self.text[self.pos : self.pos + 40]!r}"
                )
            self.pos = m.end()
            kind = m.lastgroup
            assert kind is not None
            if kind in ("ws", "comment"):
                continue
            return kind, m.group()
        return None

    def statement(self) -> list[tuple[str, str]] | None:
        """The tokens up to the next `;` (not included), or None at the end."""
        tokens = []
        while True:
            tok = self.token()
            if tok is None:
                if tokens:
                    raise ValueError(f"unterminated statement: {tokens[:6]}")
                return None
            if tok == ("punct", ";"):
                return tokens
            tokens.append(tok)

    def copy_lines(self) -> list[bytes]:
        """COPY's data: the lines after the statement, up to `\\.`."""
        end = self.text.index("\n", self.pos) + 1
        stop = self.text.find("\n\\.\n", end - 1)
        if stop == -1:
            raise ValueError("COPY data without its \\. terminator")
        block = self.text[end : stop + 1]
        self.pos = stop + 4
        if not block:
            return []
        return [line.encode("utf-8") for line in block[:-1].split("\n")]


def _qualified_name(tokens: list[tuple[str, str]], i: int) -> tuple[str, int]:
    """A possibly schema-qualified name at `tokens[i]`: the table's own name
    (the schema is dropped; each file holds one database) and the next index."""
    name = _fold(tokens[i])
    i += 1
    while i < len(tokens) and tokens[i] == ("punct", "."):
        name = _fold(tokens[i + 1])
        i += 2
    return name, i


def _split_commas(tokens: list[tuple[str, str]]) -> list[list[tuple[str, str]]]:
    """Split a parenthesised list's inside on its top-level commas."""
    parts, depth, current = [], 0, []
    for tok in tokens:
        if tok == ("punct", "(") or tok == ("punct", "["):
            depth += 1
        elif tok == ("punct", ")") or tok == ("punct", "]"):
            depth -= 1
        if tok == ("punct", ",") and depth == 0:
            parts.append(current)
            current = []
        else:
            current.append(tok)
    if current:
        parts.append(current)
    return parts


def _paren_group(tokens: list[tuple[str, str]], i: int) -> tuple[list, int]:
    """The tokens inside the parentheses opening at `tokens[i]`, and the
    index after the closing one."""
    if tokens[i] != ("punct", "("):
        raise ValueError(f"expected '(' at {tokens[i]}")
    depth = 0
    for j in range(i, len(tokens)):
        if tokens[j] == ("punct", "("):
            depth += 1
        elif tokens[j] == ("punct", ")"):
            depth -= 1
            if depth == 0:
                return tokens[i + 1 : j], j + 1
    raise ValueError("unbalanced parentheses")


_CONSTRAINT_WORDS = {"constraint", "primary", "unique", "check", "foreign", "exclude"}
_MODIFIER_WORDS = {"not", "null", "default", "collate", "references", "generated"}


def _create_table(tokens: list[tuple[str, str]]) -> Table:
    name, i = _qualified_name(tokens, 2)
    inner, _ = _paren_group(tokens, i)
    columns = []
    for part in _split_commas(inner):
        if part[0][0] == "word" and part[0][1].lower() in _CONSTRAINT_WORDS:
            continue  # a table constraint: no data
        type_tokens = []
        for kind, text in part[1:]:
            if kind == "word" and text.lower() in _MODIFIER_WORDS:
                break
            type_tokens.append(text)
        # Type names are case-insensitive; written lower case, as pg_dump does.
        pg_type = re.sub(r"\s*\(\s*", "(", " ".join(type_tokens).lower())
        pg_type = re.sub(r"\s*\)", ")", re.sub(r"\s*,\s*", ",", pg_type))
        columns.append((_fold(part[0]), pg_type))
    return Table(name, columns)


def _literal(tokens: list[tuple[str, str]]) -> str | None:
    if len(tokens) == 2 and tokens[0] == ("punct", "-") and tokens[1][0] == "number":
        return "-" + tokens[1][1]
    if len(tokens) != 1:
        raise ValueError(f"unsupported VALUES item: {tokens}")
    kind, text = tokens[0]
    if kind == "string":
        return text[1:-1].replace("''", "'")
    if kind == "number":
        return text
    if kind == "word" and text.upper() == "NULL":
        return None
    if kind == "word" and text.upper() in ("TRUE", "FALSE"):
        return text.lower()
    raise ValueError(f"unsupported VALUES item: {text!r}")


def _insert(tokens: list[tuple[str, str]], tables: dict[str, Table]) -> None:
    name, i = _qualified_name(tokens, 2)
    table = tables[name]
    if tokens[i] == ("punct", "("):
        cols, i = _paren_group(tokens, i)
        named = [_fold(part[0]) for part in _split_commas(cols)]
        if named != [c for c, _ in table.columns]:
            raise ValueError(f"INSERT into {name}: column list {named} differs")
    if tokens[i][1].upper() != "VALUES":
        raise ValueError(f"unsupported INSERT into {name}: {tokens[i : i + 3]}")
    i += 1
    width = len(table.columns)
    while i < len(tokens):
        group, i = _paren_group(tokens, i)
        row = [_literal(item) for item in _split_commas(group)]
        if len(row) != width:
            raise ValueError(f"INSERT into {name}: {len(row)} values for {width}")
        table.rows.append(row)
        if i < len(tokens):
            if tokens[i] != ("punct", ","):
                raise ValueError(f"unsupported INSERT into {name}: {tokens[i]}")
            i += 1


def _copy(tokens: list[tuple[str, str]], tables: dict[str, Table], reader: _Reader):
    name, i = _qualified_name(tokens, 1)
    table = tables[name]
    cols, i = _paren_group(tokens, i)
    named = [_fold(part[0]) for part in _split_commas(cols)]
    if named != [c for c, _ in table.columns]:
        raise ValueError(f"COPY {name}: column list {named} differs from the table's")
    if [t[1].upper() for t in tokens[i:]] != ["FROM", "STDIN"]:
        raise ValueError(f"unsupported COPY {name}: {tokens[i:]}")
    width = len(table.columns)
    for n, line in enumerate(reader.copy_lines(), start=1):
        row = parse_copy_line(line)
        if len(row) != width:
            raise ValueError(f"COPY {name} line {n}: {len(row)} fields for {width}")
        table.rows.append(row)


def read_sql(text: str) -> dict[str, Table]:
    """Every table the SQL text defines, with its rows, in definition order."""
    reader = _Reader(text)
    tables: dict[str, Table] = {}
    while (tokens := reader.statement()) is not None:
        if not tokens:
            continue
        head = tuple(t[1].upper() for t in tokens[:3])
        if head[:2] == ("CREATE", "TABLE"):
            table = _create_table(tokens)
            if table.name in tables:
                raise ValueError(f"table {table.name} defined twice")
            tables[table.name] = table
        elif head[:2] == ("INSERT", "INTO"):
            _insert(tokens, tables)
        elif head[0] == "COPY":
            _copy(tokens, tables, reader)
        elif any(head[: len(s)] == tuple(w.upper() for w in s) for s in _SKIPPED):
            continue
        else:
            raise ValueError(f"unsupported statement: {' '.join(head)} ...")
    return tables


_RAW_NAME = r'((?:\w+\.)?(?:"(?:[^"]|"")+"|\w+))'
_RAW_COPY = re.compile(r"^COPY " + _RAW_NAME + r" \(.*?\) FROM stdin;\n", re.M)
_RAW_INSERT = re.compile(r"^INSERT INTO " + _RAW_NAME + r" ", re.M)


def _raw_fold(name: str) -> str:
    name = re.sub(r'^\w+\.(?=")|^\w+\.(?=\w)', "", name)
    if name.startswith('"'):
        return name[1:-1].replace('""', '"')
    return name.lower()


def count_rows(text: str) -> dict[str, int]:
    """Rows per table, counted from the raw text without `read_sql`'s
    tokenizer, for the build's row-count check: COPY data lines up to `\\.`,
    and INSERT statements starting a line, one row each. A multi-row INSERT
    or an INSERT inside a string literal therefore disagrees with `read_sql`,
    which stops the build rather than passing it."""
    counts: dict[str, int] = {}
    for m in _RAW_COPY.finditer(text):
        end = text.index("\n\\.\n", m.end() - 1)
        block = text[m.end() : end + 1]
        counts[_raw_fold(m[1])] = block.count("\n")
    for m in _RAW_INSERT.finditer(text):
        name = _raw_fold(m[1])
        counts[name] = counts.get(name, 0) + 1
    return counts
