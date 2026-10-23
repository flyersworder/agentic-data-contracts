"""Experiment 6's mechanism: how often each arm reaches into padding.

Reproduces the mechanism counts in FINDINGS.md, *Disclosure at scale:
predictions (37)-(40)*. Padding is namespaced into its own database's name
(`solar_panel.yield`, `solar_panel.panels`), so a tool call reaches padding
when its arguments name a padding database followed by a dot, or name the
database itself. Per run: the governed lookups, those that reach padding,
the lookup results that name padding, the other tool calls (SQL, schema)
that reach padding, and the largest tool result in characters.

Traces are not in the repository; they are released with the transcripts.

Run:  uv run python analysis/disclosure_mechanism.py <dir of exp6-*-traces.tgz>
"""

from __future__ import annotations

import gzip
import json
import re
import statistics
import sys
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEVELS_FILE = ROOT / "contracts-scale" / "levels.json"
LOOKUPS = ("list_metrics", "lookup_metric", "lookup_domain", "lookup_relationships")
MODELS = {"luna": "gpt-6-luna", "sol": "gpt-6-sol", "qwen": "qwen3.8-27b"}


def _names(padding: list[str]) -> re.Pattern | None:
    if not padding:
        return None
    alt = "|".join(re.escape(p) for p in padding)
    return re.compile(rf"(?<![\w.])(?:{alt})(?:\.|(?![\w.]))")


def reach(trace: list[dict], padding: list[str]) -> dict:
    """One run's lookups, the lookups and other calls that reach padding,
    the lookup results that name padding (an unfiltered `list_metrics`
    returns every domain's metrics), its largest tool result, and whether
    the trace was cut at the cap."""
    pattern = _names(padding)
    out = {
        "lookups": 0,
        "padding_lookups": 0,
        "padding_sql": 0,
        "padding_results": 0,
        "max_result": 0,
        "truncated": False,
    }
    for message in trace:
        if message.get("__truncated__"):
            out["truncated"] = True
        for part in message.get("parts", []):
            kind = part.get("part_kind")
            if kind == "tool-return":
                content = str(part["content"])
                out["max_result"] = max(out["max_result"], len(content))
                if part["tool_name"] in LOOKUPS and pattern:
                    out["padding_results"] += bool(pattern.search(content))
            if kind != "tool-call":
                continue
            args = part.get("args")
            text = args if isinstance(args, str) else json.dumps(args)
            hit = bool(pattern and pattern.search(text))
            if part["tool_name"] in LOOKUPS:
                out["lookups"] += 1
                out["padding_lookups"] += hit
            else:
                out["padding_sql"] += hit
    return out


def split(name: str) -> tuple[str, str, str]:
    """A trace file's base database, arm and level."""
    task, arm, _ = name.split("__")
    return task.rsplit("_", 1)[0], arm, arm.rsplit("_", 1)[1]


def runs(archive: Path, levels: dict) -> list[tuple[str, dict]]:
    """(arm, reach) for every trace in one repeat's archive."""
    out = []
    with tarfile.open(archive) as tar:
        for member in tar:
            if not member.isfile() or not member.name.endswith(".json.gz"):
                continue
            base, arm, level = split(Path(member.name).name)
            handle = tar.extractfile(member)
            assert handle is not None
            trace = json.loads(gzip.decompress(handle.read()))
            out.append((arm, reach(trace, levels[base][level])))
    return out


def main(directory: Path) -> None:
    levels = json.loads(LEVELS_FILE.read_text(encoding="utf-8"))
    print(
        "== per arm-level: runs; lookups per run; runs with a padding lookup; "
        "runs with SQL into padding; runs with a lookup result naming padding; "
        "largest tool result, median / max chars; truncated traces"
    )
    for short, model in MODELS.items():
        by_arm: dict[str, list[dict]] = {}
        for archive in sorted(directory.glob(f"exp6-{short}-r*-traces.tgz")):
            for arm, r in runs(archive, levels):
                by_arm.setdefault(arm, []).append(r)
        if not by_arm:
            continue
        print(f"   {model}")
        order = ("1x", "2x", "4x", "8x", "all")
        for arm in sorted(by_arm, key=lambda a: (order.index(a.rsplit("_", 1)[1]), a)):
            rs = by_arm[arm]
            n = len(rs)
            sizes = [r["max_result"] for r in rs]
            print(
                f"      {arm:26s} {n:4d}  {sum(r['lookups'] for r in rs) / n:4.1f}  "
                f"{sum(r['padding_lookups'] > 0 for r in rs) / n:6.1%}  "
                f"{sum(r['padding_sql'] > 0 for r in rs) / n:6.1%}  "
                f"{sum(r['padding_results'] > 0 for r in rs) / n:6.1%}  "
                f"{statistics.median(sizes):8.0f} / {max(sizes):8d}  "
                f"{sum(r['truncated'] for r in rs)}"
            )


if __name__ == "__main__":
    main(Path(sys.argv[1]))
