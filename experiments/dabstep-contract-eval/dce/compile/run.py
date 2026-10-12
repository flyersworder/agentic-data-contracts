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
import time
from pathlib import Path

from dce.compile.db import CompileDB
from dce.compile.emit import build, write
from dce.compile.entries import compile_order
from dce.compile.inputs import COMPILER_MODEL, CompileInputs
from dce.compile.session import (
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
#: The provider's "busy, try later" answers. Measured: two 503s from the
#: compiler model in the first 20 minutes of the full compile, each stopping
#: it. Retried with the sweep's 429 waits (`dce.agent.RATE_LIMIT_BACKOFF_S`);
#: anything else still stops the compile.
TRANSIENT_STATUS = frozenset({429, 500, 502, 503, 504, 529})
TRANSIENT_BACKOFF_S: tuple[float, ...] = (20.0, 60.0, 180.0, 420.0)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _header(inputs: CompileInputs) -> dict:
    from dce.agent import _commit_sha

    header = {
        "type": "header",
        "benchmark": inputs.benchmark,
        "db": inputs.db_name,
        "model": inputs.model,
        "commit_sha": _commit_sha(),
        "inputs": {p.name: _sha256(p) for p in inputs.sources},
    }
    if inputs.joins:  # absent otherwise, so the frozen logs stay as they are
        header["joins"] = True
    return header


def _same_compiler(old: dict, inputs: CompileInputs) -> None:
    if old.get("model", COMPILER_MODEL) != inputs.model:
        raise SystemExit(
            f"{inputs.db_name}: compiled with {old.get('model')}, now "
            f"{inputs.model}; use another --out"
        )
    if old.get("joins", False) != inputs.joins:
        raise SystemExit(
            f"{inputs.db_name}: compiled with joins={old.get('joins', False)}, "
            f"now joins={inputs.joins}; use another --out"
        )


def _log_lines(path: Path) -> tuple[list[dict], bool]:
    """Every line of a working log, and whether its last line was torn. Only
    the LAST line may be torn -- it is the one a crash interrupts; a bad
    line anywhere else is corruption."""
    lines = path.read_text(encoding="utf-8").splitlines()
    out = []
    for i, line in enumerate(lines):
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            if i == len(lines) - 1:
                return out, True
            raise SystemExit(f"{path}: line {i + 1} is not JSON") from None
    return out, False


def read_log(path: Path) -> tuple[dict | None, list[EntryRecord], bool]:
    """(header, records, torn). A `spend` line -- an entry whose model call
    failed, billed but not finished -- is not a record: the entry reruns."""
    lines, torn = _log_lines(path)
    header = next((d for d in lines if d.get("type") == "header"), None)
    records = [EntryRecord.from_dict(d) for d in lines if d.get("type") == "entry"]
    return header, records, torn


def _gz_records(path: Path) -> list[dict]:
    lines = gzip.decompress(path.read_bytes()).decode("utf-8").splitlines()
    return [json.loads(line) for line in lines]


def _usd(lines: list[dict]) -> float:
    return sum(
        (d.get("usage") or {}).get("usd") or 0.0
        for d in lines
        if d.get("type") in ("entry", "spend")
    )


def log_spend(directory: Path) -> float:
    """What the logs under `directory` record as spent, finished or not,
    failed entries included."""
    total = 0.0
    for path in directory.rglob(LOG):
        total += _usd(_log_lines(path)[0])
    for path in directory.rglob(FINAL_LOG):
        total += _usd(_gz_records(path))
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
    header = _header(inputs)
    if final.exists() and not log.exists():
        done = next((d for d in _gz_records(final) if d.get("type") == "header"), None)
        if done is not None:
            _same_compiler(done, inputs)
        if done is None or done["inputs"] != header["inputs"]:
            raise SystemExit(
                f"{final}: inputs changed since this database was compiled; "
                "delete its directory to recompile"
            )
        return "skipped", spent
    out_dir.mkdir(parents=True, exist_ok=True)
    records: list[EntryRecord] = []
    if log.exists():
        old, records, torn = read_log(log)
        if old is not None:
            _same_compiler(old, inputs)
        if old is None or old["inputs"] != header["inputs"]:
            raise SystemExit(
                f"{log}: inputs changed since this compile started; delete the "
                "log to start over"
            )
        if torn:
            # Rewrite without the torn line, so the next append starts clean;
            # spend lines are kept, so a failed entry's cost is not forgotten.
            log.write_text(
                "".join(
                    json.dumps(d, ensure_ascii=False) + "\n" for d in _log_lines(log)[0]
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
            if record.usage.get("fatal"):
                # The call failed outside the model (the gateway, say). Its
                # cost is logged so the cap counts it, the entry is left to
                # rerun, and the compile stops rather than burn the cap on a
                # route that is down.
                _append(
                    log,
                    {"type": "spend", "entry_id": entry.id, "usage": record.usage},
                )
                raise SystemExit(
                    f"{inputs.db_name}: entry {entry.id} failed: "
                    f"{record.usage.get('error')}; rerun to resume"
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


def reopen(out_dir: Path, inputs: CompileInputs, entry_ids) -> list[int]:
    """Make a finished database recompile `entry_ids` and every entry built on
    them, on the next `compile_database`. Their records become `spend` lines
    that keep the whole first try under `replaced`, so what it said and cost
    stays in the log, and a `reopen` line records which entries were redone
    and at what commit. Returns the reopened ids."""
    from dce.agent import _commit_sha

    final = out_dir / FINAL_LOG
    if not final.exists() or (out_dir / LOG).exists():
        raise SystemExit(f"{out_dir}: not finished; resume it instead")
    by_id = {e.id: e for e in inputs.entries}
    unknown = sorted(set(entry_ids) - set(by_id))
    if unknown:
        raise SystemExit(f"{inputs.db_name}: no entry {', '.join(map(str, unknown))}")
    ids = set(entry_ids)
    grew = True
    while grew:
        parents = {e.id for e in inputs.entries if ids & set(e.children)}
        grew = not parents <= ids
        ids |= parents
    lines = [
        {
            "type": "spend",
            "entry_id": d["entry_id"],
            "usage": d["usage"],
            "replaced": {k: v for k, v in d.items() if k != "type"},
        }
        if d.get("type") == "entry" and d["entry_id"] in ids
        else d
        for d in _gz_records(final)
    ]
    lines.append(
        {"type": "reopen", "entry_ids": sorted(ids), "commit_sha": _commit_sha()}
    )
    (out_dir / LOG).write_text(
        "".join(json.dumps(d, ensure_ascii=False) + "\n" for d in lines),
        encoding="utf-8",
    )
    final.unlink()
    return sorted(ids)


def claude_driver(
    *, system_prompt: str, user_prompt: str, tools, model: str = COMPILER_MODEL
) -> DriverResult:
    """The compiler model's loop, through the harness's own agent factory
    (route, reasoning effort and caching as for every agent run). Spends
    money; tests replace it."""
    from pydantic_ai import Tool, capture_run_messages
    from pydantic_ai.exceptions import (
        ModelHTTPError,
        UnexpectedModelBehavior,
        UsageLimitExceeded,
    )
    from pydantic_ai.messages import ModelMessagesTypeAdapter
    from pydantic_ai.usage import RunUsage, UsageLimits

    from dce import agent as agent_module
    from dce.pricing import cost

    agent = agent_module._default_agent_factory(
        model=model,
        system_prompt=system_prompt,
        tools=[Tool(t) for t in tools],
        retries=MAX_ATTEMPTS + 2,
    )
    # Filled in place even when the run raises, as `dce.agent.run_task` relies
    # on: an entry that loops to the request cap is the most expensive kind,
    # and must not be counted as free.
    usage = RunUsage()
    error, fatal = None, False
    with capture_run_messages() as messages:
        try:
            # A retry re-runs the entry on the same session: a submission
            # already accepted stays ("Already recorded."), and `usage` keeps
            # what the failed attempt spent.
            for wait in (*TRANSIENT_BACKOFF_S, None):
                try:
                    agent.run_sync(
                        user_prompt,
                        usage_limits=UsageLimits(request_limit=COMPILE_REQUEST_LIMIT),
                        usage=usage,
                    )
                    break
                except ModelHTTPError as exc:
                    if wait is None or exc.status_code not in TRANSIENT_STATUS:
                        raise
                    time.sleep(wait)
        except (UsageLimitExceeded, UnexpectedModelBehavior) as exc:
            # The model's own doing: the entry keeps whatever it submitted.
            error = f"{type(exc).__name__}: {exc}"
        except Exception as exc:
            # Anything else (the gateway, the network) stops the compile.
            error, fatal = f"{type(exc).__name__}: {exc}", True
    in_tok = usage.input_tokens or 0
    out_tok = usage.output_tokens or 0
    cached = getattr(usage, "cache_read_tokens", 0) or 0
    out = {
        "input_tokens": in_tok,
        "output_tokens": out_tok,
        "cached_tokens": cached,
        "usd": cost(model, in_tok, out_tok, cached),
    }
    if error is not None:
        out["error"] = error
    if fatal:
        out["fatal"] = True
    return DriverResult(
        messages=ModelMessagesTypeAdapter.dump_python(messages, mode="json"),
        usage=out,
    )
