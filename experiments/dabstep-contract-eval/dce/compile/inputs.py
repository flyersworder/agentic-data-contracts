"""What one compile reads: the entries, the context a data engineer would
have (DDL, column meanings), and the database. Built by each benchmark's
adapter; `sources` lists every file the adapter read, for the log header."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from dce.compile.entries import KBEntry

#: The frozen compiler's model; `--model` picks another.
COMPILER_MODEL = "claudesonnet5"


@dataclass(frozen=True)
class CompileInputs:
    benchmark: str
    db_name: str
    entries: tuple[KBEntry, ...]
    ddl: str
    column_meanings: str  # rendered for the compiler's prompt
    dialect_note: str
    db_path: Path
    init_sql: tuple[str, ...]
    sources: tuple[Path, ...]
    out_subdir: str  # under the contracts directory
    column_descriptions: dict[tuple[str, str], str] = field(default_factory=dict)
    table_descriptions: dict[str, str] = field(default_factory=dict)
    #: Let a metric declare the FROM it is written against (`from_sql`, with
    #: its `grain`). Off, the compiler is the frozen one, prompt and tools
    #: unchanged.
    joins: bool = False
    #: The model that compiles; the log and the contract header record it.
    model: str = COMPILER_MODEL
