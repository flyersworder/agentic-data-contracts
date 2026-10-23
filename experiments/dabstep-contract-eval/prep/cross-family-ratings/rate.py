"""Run the cross-family blind raters (see PLAN.md in this folder).

One fresh Copilot CLI session per (model, chunk), inside `sandbox.sh`, with a
work folder holding only that chunk's rubric and packet or dossiers. The
session writes `out.jsonl`; a valid one is copied to
~/data/dce-held-results/cross-family/<model>/<chunk>.jsonl with a `.meta.json`
beside it. An invalid or incomplete output is rerun once in a fresh session.

Run:  uv run python prep/cross-family-ratings/rate.py [--models gpt gemini]
          [--chunks c01 det-b ...] [--jobs 4]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
HELD = Path.home() / "data/dce-held-results"
LOAD = HELD / "procedural-load"
AUDIT = HELD / "failure-audit"
OUT = HELD / "cross-family"
SANDBOX = Path.home() / ".cache/rating-sandbox"
#: Holds only the Copilot login (a copy of ~/.copilot/config.json). Every
#: session gets its own copy of it, so no session can read another's state.
LOGIN = SANDBOX / "copilot-home" / "config.json"

MODELS = {"gpt": "gpt-6.1-sol", "gemini": "gemini-4-argon"}
SESSION_SECONDS = 90 * 60
ATTEMPTS = 2

LOAD_FIELDS = {
    "needed": "list",
    "steps": "count",
    "rule_matching": "bool",
    "multi_grain": "bool",
    "branching": "bool",
    "direct": "bool",
    "load": "level",
    "notes": "str",
}
DET_FIELDS = {
    "kb_clarity": "clarity",
    "undetermined": "list",
    "decisive": "bool",
    "question_states_it": "bool",
    "mapping_ok": "bool",
    "notes": "str",
}
CLARITY = {"clear", "ambiguous", "misleading", "absent"}


def _ok(kind: str, value) -> bool:
    if kind == "bool":
        return isinstance(value, bool)
    if kind == "str":
        return isinstance(value, str)
    if kind == "list":
        return isinstance(value, list) and all(isinstance(v, str) for v in value)
    if kind in ("count", "level"):
        if not isinstance(value, int) or isinstance(value, bool):
            return False
        return value >= 0 and (kind == "count" or value <= 3)
    if kind == "clarity":
        return value in CLARITY
    raise ValueError(kind)


def validate(kind: str, rows: list[dict], ids: list[str]) -> list[str]:
    """Every way `rows` falls short of one valid rating per id in `ids`."""
    fields = LOAD_FIELDS if kind == "load" else DET_FIELDS
    errors: list[str] = []
    seen: set[str] = set()
    for i, row in enumerate(rows):
        if "_unparsed" in row:
            errors.append(f"line {i + 1}: not JSON: {row['_unparsed'][:60]!r}")
            continue
        rid = row.get("id")
        if rid not in ids:
            errors.append(f"{rid!r}: unexpected id")
            continue
        if rid in seen:
            errors.append(f"{rid}: duplicate")
            continue
        seen.add(rid)
        for field, ftype in fields.items():
            if field not in row:
                errors.append(f"{rid}: {field} missing")
            elif not _ok(ftype, row[field]):
                errors.append(f"{rid}: {field} = {row[field]!r} is not a valid {ftype}")
    errors += [f"{i}: missing" for i in ids if i not in seen]
    return errors


def parse_jsonl(text: str) -> list[dict]:
    rows: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("```"):
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            rows.append({"_unparsed": line})
    return rows


def packet_ids(text: str) -> list[str]:
    return re.findall(r"^## (P\d+)\b", text, flags=re.M)


#: The blind rubric's three location-specific passages and their sandbox
#: wording. Everything else in the rubric is copied unchanged.
_MOVES = (
    (
        r"Read ONLY the files in `dossiers-blind/` and this rubric\. Do not open any\s+"
        r"other file or directory under `failure-audit/` \(other verdicts, keys or\s+"
        r"dossiers\): this pass must not know how any agent did\.",
        "Read ONLY the dossier files in your working folder and this rubric: this\n"
        "pass must not know how any agent did.",
    ),
    (r"Each `dossiers-blind/Bnn\.md` holds", "Each dossier file holds"),
    (
        r"You may check the data read-only, from\s+`~/projects/[^`]*`:\s+"
        r"UV_FROZEN=1 uv run[^\n]*\n",
        "You may check the data read-only, from your working folder:\n"
        '  ./q <db> "<sql>"\n',
    ),
    (
        r"Output: one JSON line per task, all 55, to the file named in your "
        r"instructions:",
        "Output: one JSON line per task, every task in your folder, to `out.jsonl` in\n"
        "your working folder:",
    ),
)


def adapt_blind_rubric(text: str) -> str:
    for pattern, replacement in _MOVES:
        text, n = re.subn(pattern, lambda _m, r=replacement: r, text)
        if n != 1:
            raise ValueError(f"rubric passage not found exactly once: {pattern[:50]}")
    return text


@dataclass(frozen=True)
class Chunk:
    name: str
    kind: str  # "load" or "determinacy"
    files: dict[str, Path]  # name in the work folder -> source
    ids: list[str]
    prompt: str


def _prompt(template: str, **values: str) -> str:
    text = (HERE / "prompts" / template).read_text(encoding="utf-8")
    for k, v in values.items():
        text = text.replace("{" + k + "}", v)
    return text


def chunks() -> list[Chunk]:
    out: list[Chunk] = []
    for packet in sorted((LOAD / "packets").glob("c*.md")):
        ids = packet_ids(packet.read_text(encoding="utf-8"))
        files = {"RUBRIC.md": LOAD / "RUBRIC.md", "manual.md": LOAD / "manual.md"}
        files[packet.name] = packet
        out.append(
            Chunk(
                packet.stem, "load", files, ids, _prompt("load.md", packet=packet.name)
            )
        )
    det = [("det-b", sorted(p.stem for p in (AUDIT / "dossiers-blind").glob("B*.md")))]
    for path in sorted(AUDIT.glob("blind302-r1-c*.jsonl")):
        ids = [
            json.loads(line)["id"]
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        det.append((f"det-{path.stem.rsplit('-', 1)[1]}", ids))
    for name, ids in det:
        folder = AUDIT / ("dossiers-blind" if name == "det-b" else "dossiers-blind302")
        files = {f"{i}.md": folder / f"{i}.md" for i in ids}
        prompt = _prompt(
            "determinacy.md", first=ids[0], last=ids[-1], count=str(len(ids))
        )
        out.append(Chunk(name, "determinacy", files, ids, prompt))
    return out


def fresh_copilot_home(home: Path, login: Path) -> Path:
    """An empty Copilot state folder holding only the login."""
    if home.exists():
        shutil.rmtree(home)
    home.mkdir(parents=True, mode=0o700)
    shutil.copyfile(login, home / "config.json")
    (home / "config.json").chmod(0o600)
    return home


def _copilot_version() -> str:
    return subprocess.run(
        ["copilot", "--version"], capture_output=True, text=True, check=False
    ).stdout.splitlines()[0]


def run_session(model_key: str, chunk: Chunk, attempt: int) -> tuple[list[str], dict]:
    work = SANDBOX / "runs" / model_key / f"{chunk.name}-a{attempt}"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    for name, src in chunk.files.items():
        if name == "RUBRIC-BLIND.md":
            continue
        shutil.copyfile(src, work / name)
    tools = ["--allow-tool", "write"]
    server = None
    if chunk.kind == "determinacy":
        rubric = adapt_blind_rubric(
            (AUDIT / "RUBRIC-BLIND.md").read_text(encoding="utf-8")
        )
        (work / "RUBRIC-BLIND.md").write_text(rubric, encoding="utf-8")
        shutil.copyfile(HERE / "q", work / "q")
        (work / "q").chmod(0o755)
        server = subprocess.Popen(
            [sys.executable, str(HERE / "qserver.py"), str(work / "q.sock")],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for _ in range(100):
            if (work / "q.sock").exists():
                break
            time.sleep(0.1)
        tools += ["--allow-tool", "shell(./q:*)"]
    cmd = [
        str(HERE / "sandbox.sh"), str(work),
        "copilot", "-p", chunk.prompt, "--model", MODELS[model_key],
        "--reasoning-effort", "high", "-s", "--no-custom-instructions",
        "--disable-builtin-mcps", "--no-ask-user", "--no-auto-update",
        "--autopilot", "--max-autopilot-continues", "40", *tools,
    ]  # fmt: skip
    home = fresh_copilot_home(SANDBOX / "homes" / model_key / work.name, LOGIN)
    env = dict(os.environ, RATER_COPILOT_HOME=str(home))
    started = time.time()
    try:
        proc = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=SESSION_SECONDS,
            check=False,
        )
        code, reply = proc.returncode, proc.stdout[-4000:]
    except subprocess.TimeoutExpired:
        code, reply = "timeout", ""
    finally:
        if server is not None:
            server.terminate()
            server.wait()
    out = work / "out.jsonl"
    rows = parse_jsonl(out.read_text(encoding="utf-8")) if out.exists() else []
    errors = validate(chunk.kind, rows, chunk.ids) if rows else ["no out.jsonl"]
    meta = {
        "model": MODELS[model_key],
        "copilot": _copilot_version(),
        "chunk": chunk.name,
        "attempt": attempt,
        "prompt_sha256": hashlib.sha256(chunk.prompt.encode()).hexdigest(),
        "started": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(started)),
        "seconds": round(time.time() - started),
        "exit": code,
        "errors": errors[:50],
        "n_errors": len(errors),
        "reply_tail": reply,
    }
    return errors, meta


def rate(model_key: str, chunk: Chunk) -> str:
    dest = OUT / model_key
    dest.mkdir(parents=True, exist_ok=True)
    if (dest / f"{chunk.name}.jsonl").exists():
        return f"{model_key} {chunk.name}: done already"
    errors: list[str] = []
    for attempt in range(1, ATTEMPTS + 1):
        errors, meta = run_session(model_key, chunk, attempt)
        work = SANDBOX / "runs" / model_key / f"{chunk.name}-a{attempt}"
        (dest / f"{chunk.name}.a{attempt}.meta.json").write_text(
            json.dumps(meta, indent=1) + "\n", encoding="utf-8"
        )
        if (work / "out.jsonl").exists():
            shutil.copyfile(work / "out.jsonl", dest / f"{chunk.name}.a{attempt}.jsonl")
        if not errors:
            shutil.copyfile(work / "out.jsonl", dest / f"{chunk.name}.jsonl")
            return f"{model_key} {chunk.name}: valid on attempt {attempt}"
    n = len(errors)
    return f"{model_key} {chunk.name}: INVALID after {ATTEMPTS} attempts ({n} errors)"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", nargs="+", default=list(MODELS), choices=list(MODELS))
    ap.add_argument("--chunks", nargs="+", default=None)
    ap.add_argument("--jobs", type=int, default=4)
    args = ap.parse_args()
    if not LOGIN.exists():
        sys.exit(f"no Copilot login at {LOGIN}; copy ~/.copilot/config.json there")
    todo = [c for c in chunks() if args.chunks is None or c.name in args.chunks]
    jobs = [(m, c) for m in args.models for c in todo]
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        for line in pool.map(lambda mc: rate(*mc), jobs):
            print(line, flush=True)


if __name__ == "__main__":
    main()
