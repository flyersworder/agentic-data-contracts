import ast
import json
import re
from pathlib import Path

import duckdb
from dce.data import DATASET_REVISION, build_duckdb
from dce.prepare import write_golds


def test_build_duckdb_creates_one_table_per_source(tmp_path: Path):
    csv = tmp_path / "acquirer_countries.csv"
    csv.write_text("acquirer,country\nbank_a,NL\nbank_b,BE\n")
    js = tmp_path / "merchant_data.json"
    js.write_text(json.dumps([{"merchant": "Crossfit_Hanna", "mcc": 7997}]))

    db = tmp_path / "dabstep.duckdb"
    build_duckdb({"acquirer_countries": csv, "merchant_data": js}, db)

    con = duckdb.connect(str(db))
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert tables == {"acquirer_countries", "merchant_data"}
    assert con.execute("SELECT count(*) FROM acquirer_countries").fetchone()[0] == 2
    assert con.execute("SELECT merchant FROM merchant_data").fetchone()[0] == (
        "Crossfit_Hanna"
    )


def test_build_duckdb_is_idempotent(tmp_path: Path):
    # Re-running the loader must not double the rows; the sweep may restart.
    csv = tmp_path / "acquirer_countries.csv"
    csv.write_text("acquirer,country\nbank_a,NL\n")
    db = tmp_path / "dabstep.duckdb"

    build_duckdb({"acquirer_countries": csv}, db)
    build_duckdb({"acquirer_countries": csv}, db)

    con = duckdb.connect(str(db))
    assert con.execute("SELECT count(*) FROM acquirer_countries").fetchone()[0] == 1


# The Hub functions that accept a `revision`. Method names cover HfApi instance
# calls (`api.repo_info(...)`) as well as the module-level helpers, since the
# check below matches on the attribute name.
_HUB_CALLS = {
    "hf_hub_download",
    "snapshot_download",
    "list_repo_files",
    "repo_info",
    "dataset_info",
    "load_dataset",
}


def test_the_pinned_revision_is_a_full_length_commit_sha():
    # A short sha or a branch name would silently resolve to a moving target.
    assert re.fullmatch(r"[0-9a-f]{40}", DATASET_REVISION), DATASET_REVISION


def test_every_hub_call_pins_the_dataset_revision():
    # A whole-package property rather than a per-call-site assertion: it covers
    # Hub calls added later, and it fails if someone "fixes" a call by dropping
    # the kwarg or hardcoding a branch. Unpinned reads would reconstruct a
    # different gold set on a later run and score arms against different golds.
    package = Path(__file__).resolve().parent.parent / "dce"
    offenders = []
    for path in sorted(package.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = (
                func.attr
                if isinstance(func, ast.Attribute)
                else getattr(func, "id", "")
            )
            if name not in _HUB_CALLS:
                continue
            pinned = [kw for kw in node.keywords if kw.arg == "revision"]
            if not pinned:
                offenders.append(f"{path.name} line {node.lineno}: {name} is unpinned")
            elif not (
                isinstance(pinned[0].value, ast.Name)
                and pinned[0].value.id == "DATASET_REVISION"
            ):
                offenders.append(
                    f"{path.name} line {node.lineno}: {name} pins something "
                    "other than DATASET_REVISION"
                )
    assert offenders == [], offenders


def test_at_least_one_download_helper_is_covered_by_that_check():
    # Guards the guard: an empty scan would pass vacuously if the Hub call
    # names ever changed or the package moved.
    package = Path(__file__).resolve().parent.parent / "dce"
    found = sum(
        1
        for path in package.glob("*.py")
        for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, ast.Call)
        and (
            node.func.attr
            if isinstance(node.func, ast.Attribute)
            else getattr(node.func, "id", "")
        )
        in _HUB_CALLS
    )
    assert found >= 3, f"expected the download helpers to be scanned, saw {found}"


def test_the_gold_file_describes_the_revision_and_threshold_it_came_from(
    tmp_path: Path,
):
    # Task 8's runner reads this file; a bare mapping could not tell it whether
    # two arms were scored against the same ground truth.
    path = tmp_path / "golds.json"
    envelope = write_golds(path, {"1": "0.12", "2": "NL"}, threshold=0.75)
    assert envelope == json.loads(path.read_text())
    assert envelope["revision"] == DATASET_REVISION
    assert envelope["threshold"] == 0.75
    assert envelope["count"] == 2
    assert envelope["golds"] == {"1": "0.12", "2": "NL"}
