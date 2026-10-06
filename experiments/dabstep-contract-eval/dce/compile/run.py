"""Compile one database: resumable, logged, capped.

Every finished entry is appended to `compile-log.jsonl` (flushed and
fsynced) before the next starts, so a crash costs only the entry in
progress. A rerun reads the log, drops a torn last line, refuses if the
inputs' hashes changed, and continues. When every entry is done the
contract is written and the log gzipped to `compile-log.jsonl.gz`; a
database with that file and no plain log is finished and skipped.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path

from dce.compile.db import CompileDB
from dce.compile.emit import build, write
from dce.compile.entries import compile_order
from dce.compile.inputs import CompileInputs
from dce.compile.session import (
    COMPILER_MODEL,
    MAX_ATTEMPTS,
    DriverResult,
    EntryRecord,
    compile_entry,
)

LOG = "compile-log.jsonl"
FINAL_LOG = "compile-log.jsonl.gz"
#: Model requests per entry. An entry takes a handful (a few queries, one
#: or two submissions); the cap stops a model that loops.
COMPILE_REQUEST_LIMIT = 25


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _header(inputs: CompileInputs) -> dict:
    from dce.agent import _commit_sha

    return {
        "type": "header",
        "benchmark": inputs.benchmark,
        "db": inputs.db_name,
        "model": COMPILER_MODEL,
        "commit_sha": _commit_sha(),
        "inputs": {p.name: _sha256(p) for p in inputs.sources},
    }


def read_log(path: Path) -> tuple[dict | None, list[EntryRecord], bool]:
    """(header, records, torn). Only the LAST line may be torn -- it is the
    one a crash interrupts; a bad line anywhere else is corruption."""
    lines = path.read_text(encoding="utf-8").splitlines()
    header, records, torn = None, [], False
    for i, line in enumerate(lines):
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            if i == len(lines) - 1:
                torn = True
                break
            raise SystemExit(f"{path}: line {i + 1} is not JSON") from None
        if data.get("type") == "header":
            header = data
        else:
            records.append(EntryRecord.from_dict(data))
    return header, records, torn


def _gz_records(path: Path) -> list[dict]:
    lines = gzip.decompress(path.read_bytes()).decode("utf-8").splitlines()
    return [json.loads(line) for line in lines]


def log_spend(directory: Path) -> float:
    """What the logs under `directory` record as spent, finished or not."""
    total = 0.0
    for path in directory.rglob(LOG):
        total += sum(r.usage.get("usd") or 0.0 for r in read_log(path)[1])
    for path in directory.rglob(FINAL_LOG):
        total += sum(
            (r.get("usage") or {}).get("usd") or 0.0
            for r in _gz_records(path)
            if r.get("type") == "entry"
        )
    return total


def _append(path: Path, data: dict) -> None:
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(data, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def compile_database(
    inputs: CompileInputs,
    out_dir: Path,
    driver,
    *,
    spend_cap: float,
    spent: float = 0.0,
) -> tuple[str, float]:
    log, final = out_dir / LOG, out_dir / FINAL_LOG
    if final.exists() and not log.exists():
        return "skipped", spent
    out_dir.mkdir(parents=True, exist_ok=True)
    header = _header(inputs)
    records: list[EntryRecord] = []
    if log.exists():
        old, records, torn = read_log(log)
        if old is None or old["inputs"] != header["inputs"]:
            raise SystemExit(
                f"{log}: inputs changed since this compile started; delete the "
                "log to start over"
            )
        if torn:
            # Rewrite without the torn line, so the next append starts clean.
            log.write_text(
                "".join(
                    json.dumps(d, ensure_ascii=False) + "\n"
                    for d in [old] + [r.to_dict() for r in records]
                ),
                encoding="utf-8",
            )
    else:
        _append(log, header)
    prior = {r.entry_id: r for r in records}
    db = CompileDB(inputs.db_path, init_sql=inputs.init_sql)
    try:
        tables = db.tables()
        for entry in compile_order(inputs.entries):
            if entry.id in prior:
                continue
            if spent >= spend_cap:
                return "capped", spent
            record = compile_entry(
                entry, inputs, db, driver, prior=prior, tables=tables
            )
            _append(log, record.to_dict())
            prior[entry.id] = record
            spent += record.usage.get("usd") or 0.0
    finally:
        db.close()
    write(out_dir, *build(inputs, list(prior.values()), tables), inputs)
    final.write_bytes(gzip.compress(log.read_bytes(), mtime=0))
    log.unlink()
    return "done", spent


def claude_driver(*, system_prompt: str, user_prompt: str, tools) -> DriverResult:
    """The compiler model's loop, through the harness's own agent factory
    (route, reasoning effort and caching as for every agent run). Spends
    money; tests replace it."""
    from pydantic_ai import Tool
    from pydantic_ai.exceptions import UsageLimitExceeded
    from pydantic_ai.messages import ModelMessagesTypeAdapter
    from pydantic_ai.usage import UsageLimits

    from dce.agent import _default_agent_factory
    from dce.pricing import cost

    agent = _default_agent_factory(
        model=COMPILER_MODEL,
        system_prompt=system_prompt,
        tools=[Tool(t) for t in tools],
        retries=MAX_ATTEMPTS + 2,
    )
    try:
        result = agent.run_sync(
            user_prompt, usage_limits=UsageLimits(request_limit=COMPILE_REQUEST_LIMIT)
        )
    except UsageLimitExceeded as exc:
        # The entry keeps whatever it submitted before the cap; its cost is
        # unknown here, so it is recorded as such rather than as zero.
        return DriverResult(messages=[], usage={"usd": None, "error": str(exc)})
    usage = result.usage()
    in_tok = usage.input_tokens or 0
    out_tok = usage.output_tokens or 0
    cached = getattr(usage, "cache_read_tokens", 0) or 0
    return DriverResult(
        messages=ModelMessagesTypeAdapter.dump_python(
            result.all_messages(), mode="json"
        ),
        usage={
            "input_tokens": in_tok,
            "output_tokens": out_tok,
            "cached_tokens": cached,
            "usd": cost(COMPILER_MODEL, in_tok, out_tok, cached),
        },
    )
