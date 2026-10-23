"""Experiment 4b's notes for `manual_noted` (design, "Arms").

One model (WRITER) reads a database's gapped documentation and the authored
contract (draw 1, after any correction) and writes, in words and without
SQL, every way the contract departs from or adds to the documentation. A
second model (CHECKER) reads the same and the notes and says whether every
resolution is there and nothing else is; what it names goes back to the
writer, for at most MAX_ROUNDS drafts. Neither sees a question, gold or
result, nor the database.
The notes carry what the author settled, right or wrong.

    set -a; . ./.env; set +a
    UV_FROZEN=1 uv run python prep/procedures4b/notes.py --max-spend 10

Writes contracts-procedures4b/draw-1/livesqlbench/<db>/notes.md and a log.
"""

from __future__ import annotations

import argparse
import functools
import gzip
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE))

WRITER = "gpt-6-sol"
CHECKER = "claudeopus5.5"
REQUEST_LIMIT = 30
MAX_ROUNDS = 3
DRAW = ROOT / "contracts-procedures4b" / "draw-1" / "livesqlbench"

WRITER_PROMPT = """\
You read a database's documentation and a data contract an engineer wrote \
from it after looking at the data. Write notes for a reader who has the \
documentation but not the contract: every way the contract departs from or \
adds to the documentation about how the data is stored or how a definition \
is computed -- each resolution the contract makes that the documentation \
does not state, or states differently. Use the contract's own words where \
it states the resolution; where only its SQL shows it, say in plain words \
what the SQL does. No SQL in the notes. Add nothing the contract does not \
say or compute, and do not judge whether it is right. One short paragraph \
per resolution, most consequential first. Submit with submit_notes(notes); \
when it returns "Accepted.", reply "done".
"""

CHECKER_PROMPT = """\
You read a database's documentation, a data contract an engineer wrote from \
it, and notes meant to carry, in words, every way the contract departs from \
or adds to the documentation about how the data is stored or how a \
definition is computed. Check the notes against the contract: is every such \
resolution there, stated as the contract states or computes it, and is \
there nothing the contract does not say or compute? Submit with \
submit_verdict(verdict): {"complete": true or false, "missing": [each \
resolution the notes lack, in words], "extra": [each statement the contract \
does not support]}. When it returns "Accepted.", reply "done".
"""

USER_PROMPT = """\
Database `{db}`.

## Documentation ({n} entries)

{kb}

## contract.yml

{contract_yml}

## semantic.yml

{semantic_yml}
"""

_SQL = re.compile(r"\b(SELECT|FROM|WHERE|JOIN|GROUP BY|CASE WHEN)\b")


def user_prompt(
    *, db: str, kb: list[dict], contract_yml: str, semantic_yml: str
) -> str:
    return USER_PROMPT.format(
        db=db,
        n=len(kb),
        kb="\n".join(
            f"[{e['id']}] {e['knowledge']}: "
            + " ".join(f"{e['description']} {e['definition']}".split())
            for e in sorted(kb, key=lambda e: e["id"])
        ),
        contract_yml=contract_yml.strip(),
        semantic_yml=semantic_yml.strip(),
    )


def check_notes(text) -> list[str]:
    if not isinstance(text, str) or not text.strip():
        return ["the notes are empty"]
    if len(_SQL.findall(text)) >= 2:
        return ["the notes contain SQL; say in words what it does"]
    return []


def check_verdict(verdict) -> list[str]:
    if not isinstance(verdict, dict):
        return ["the verdict must be an object"]
    missing = [k for k in ("complete", "missing", "extra") if k not in verdict]
    return [f"the verdict lacks {', '.join(missing)}"] if missing else []


def _session(driver, system: str, user: str, tool_name: str, check):
    state: dict = {"value": None, "errors": []}

    def accept(value) -> str:
        problems = check(value)
        if problems:
            state["errors"].append(problems)
            return "ERROR:\n" + "\n".join(f"- {p}" for p in problems)
        state["value"] = value
        return "Accepted."

    # Typed and named as the prompts say: an untyped parameter gives a tool
    # schema without a type, which some providers reject.
    def submit_notes(notes: str) -> str:
        """Submit the notes."""
        return accept(notes)

    def submit_verdict(verdict: dict) -> str:
        """Submit the verdict."""
        return accept(verdict)

    tool = {"submit_notes": submit_notes, "submit_verdict": submit_verdict}[tool_name]
    result = driver(system_prompt=system, user_prompt=user, tools=[tool])
    return state, result


def write_notes(db: str, kb: list[dict], folder: Path, writer, checker) -> dict:
    base = user_prompt(
        db=db,
        kb=kb,
        contract_yml=(folder / "contract.yml").read_text(encoding="utf-8"),
        semantic_yml=(folder / "semantic.yml").read_text(encoding="utf-8"),
    )
    log: dict = {"rounds": []}
    feedback = ""
    for _ in range(MAX_ROUNDS):
        w, wres = _session(
            writer, WRITER_PROMPT, base + feedback, "submit_notes", check_notes
        )
        if w["value"] is None:
            raise SystemExit(
                f"{db}: the writer submitted no notes "
                f"({(wres.usage or {}).get('error')})"
            )
        text = w["value"]
        c, cres = _session(
            checker,
            CHECKER_PROMPT,
            base + "\n## Notes\n\n" + text,
            "submit_verdict",
            check_verdict,
        )
        if c["value"] is None:
            raise SystemExit(
                f"{db}: the checker gave no verdict ({(cres.usage or {}).get('error')})"
            )
        verdict = c["value"]
        log["rounds"].append(
            {
                "notes": text,
                "verdict": verdict,
                "usage": [wres.usage, cres.usage],
                "messages": [wres.messages, cres.messages],
            }
        )
        if verdict.get("complete") and not verdict.get("extra"):
            break
        feedback = "\n## A reviewer's findings on an earlier draft\n\n" + json.dumps(
            {k: verdict.get(k) for k in ("missing", "extra")}, indent=1
        )
    log["final"] = log["rounds"][-1]
    (folder / "notes.md").write_text(
        log["final"]["notes"].strip() + "\n", encoding="utf-8"
    )
    (folder / "notes-log.jsonl.gz").write_bytes(
        gzip.compress(
            (json.dumps(log, ensure_ascii=False) + "\n").encode("utf-8"), mtime=0
        )
    )
    return log


def main() -> None:
    from build import PROC4B_DATA
    from dce.compile import run as run_module

    parser = argparse.ArgumentParser()
    parser.add_argument("--max-spend", type=float, required=True)
    args = parser.parse_args()
    spent = 0.0
    for folder in sorted(p for p in DRAW.iterdir() if p.is_dir()):
        db = folder.name
        if (folder / "notes.md").exists():
            print(f"{db}: notes exist")
            continue
        if spent >= args.max_spend:
            raise SystemExit(f"stopped at the ${args.max_spend:.2f} cap")
        kb_path = PROC4B_DATA / "hf-full-v1" / db / f"{db}_kb.jsonl"
        kb = [
            json.loads(x) for x in kb_path.read_text(encoding="utf-8").splitlines() if x
        ]
        log = write_notes(
            db,
            kb,
            folder,
            functools.partial(
                run_module.claude_driver, model=WRITER, request_limit=REQUEST_LIMIT
            ),
            functools.partial(
                run_module.claude_driver, model=CHECKER, request_limit=REQUEST_LIMIT
            ),
        )
        spent += sum(
            (u or {}).get("usd") or 0.0 for r in log["rounds"] for u in r["usage"]
        )
        final = log["final"]["verdict"]
        print(
            f"{db}: {len(log['rounds'])} round(s); complete={final.get('complete')}; "
            f"missing={len(final.get('missing') or [])}, "
            f"extra={len(final.get('extra') or [])} (spent ${spent:.2f})"
        )


if __name__ == "__main__":
    main()
