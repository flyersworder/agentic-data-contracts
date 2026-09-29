"""Trim a dbt manifest.json to what DbtSource reads, for a test fixture.

A full manifest from `dbt parse` is ~600 KB, almost all of it macros. This
keeps `metadata`, the model and test `nodes`, `semantic_models` and `metrics`
verbatim -- each kept object is exactly what dbt wrote, so the fixture stays a
real manifest rather than a hand-written approximation of one.

Usage: uv run python scripts/trim_dbt_manifest.py <manifest.json> <out.json>
"""

import json
import sys
from pathlib import Path

KEEP = ("metadata", "semantic_models", "metrics")


def main(src: str, dst: str) -> None:
    manifest = json.loads(Path(src).read_text())
    trimmed = {key: manifest[key] for key in KEEP}
    # dbt's anonymous telemetry id for whoever ran `dbt parse`: not the
    # project's, and nothing DbtSource reads.
    trimmed["metadata"]["user_id"] = None
    trimmed["nodes"] = {
        key: node
        for key, node in manifest["nodes"].items()
        if node.get("resource_type") in ("model", "test")
    }
    Path(dst).write_text(json.dumps(trimmed, indent=1, sort_keys=True) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:])
