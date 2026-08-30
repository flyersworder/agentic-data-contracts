import ast
import json
import re
from pathlib import Path

import duckdb
from dce.data import DATASET_REVISION, build_duckdb
from dce.golds import PLURALITY_THRESHOLD, golds_sha256
from dce.prepare import (
    Corpus,
    _coverage_by_level,
    _golds_path,
    write_golds,
)


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


def test_build_duckdb_does_not_ingest_a_leading_index_header_as_data(tmp_path: Path):
    # Real DABStep annex files (acquirer_countries.csv, merchant_category_codes.csv)
    # carry a leading unnamed pandas-index column. Its header cell is blank, which
    # can defeat DuckDB's header sniffer: the blank header vs. an integer index
    # doesn't type-differ enough for confident detection, so it falls back to
    # "no header" and ingests the header row itself as data (column0/column1/
    # column2 names, header text as the first row).
    csv = tmp_path / "acquirer_countries.csv"
    csv.write_text(
        ",acquirer,country_code\n0,gringotts,GB\n1,the_savings_and_loan_bank,US\n"
    )
    db = tmp_path / "dabstep.duckdb"
    build_duckdb({"acquirer_countries": csv}, db)

    con = duckdb.connect(str(db))
    columns = [r[0] for r in con.execute("DESCRIBE acquirer_countries").fetchall()]
    assert columns[1:] == ["acquirer", "country_code"]
    assert con.execute("SELECT count(*) FROM acquirer_countries").fetchone()[0] == 2


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
    "list_repo_tree",
    "get_paths_info",
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
    for path in sorted(package.rglob("*.py")):
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
        for path in package.rglob("*.py")
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


def test_the_gold_file_records_which_corpus_produced_it(tmp_path: Path):
    # Same revision, different filesystem, different corpus: the envelope has to
    # make that visible rather than let two unequal gold sets look comparable.
    corpus = Corpus(
        submissions={},
        scores={},
        expected=2205,
        consumed=2199,
        manifest_sha256="deadbeef" * 8,
        missing=[],
        shadowed=[],
    )
    envelope = write_golds(tmp_path / "golds.json", {"1": "0.12"}, corpus=corpus)
    assert envelope["submissions_expected"] == 2205
    assert envelope["submissions_consumed"] == 2199
    assert envelope["manifest_sha256"] == "deadbeef" * 8


def test_the_gold_file_fingerprints_the_golds_not_only_the_corpus(tmp_path: Path):
    """Two gold sets reconstructed from ONE corpus at two thresholds differ
    in content and share an identical `manifest_sha256`. Ruling 8 requires
    exactly those re-runs, and `data/` is gitignored, so nothing else could
    catch an in-place overwrite. `golds_sha256` is what a result row's
    `golds_hash` must be."""
    corpus = Corpus(
        submissions={},
        scores={},
        expected=2205,
        consumed=2199,
        manifest_sha256="deadbeef" * 8,
        missing=[],
        shadowed=[],
    )
    loose = write_golds(
        tmp_path / "loose.json", {"1": "0.12", "2": "NL"}, threshold=0.60, corpus=corpus
    )
    tight = write_golds(
        tmp_path / "tight.json", {"1": "0.12"}, threshold=0.75, corpus=corpus
    )
    assert loose["manifest_sha256"] == tight["manifest_sha256"]
    assert loose["golds_sha256"] != tight["golds_sha256"]
    assert loose["golds_sha256"] == golds_sha256({"1": "0.12", "2": "NL"})


def test_a_sensitivity_threshold_never_overwrites_the_primary_gold_file(
    tmp_path: Path,
):
    assert _golds_path(tmp_path, PLURALITY_THRESHOLD).name == "golds.json"
    assert _golds_path(tmp_path, 0.60).name == "golds_threshold_0.6.json"
    assert _golds_path(tmp_path, 0.90).name == "golds_threshold_0.9.json"


def test_gold_coverage_is_broken_down_by_level():
    tasks = [
        {"task_id": "1", "level": "easy"},
        {"task_id": "2", "level": "easy"},
        {"task_id": "3", "level": "hard"},
    ]
    coverage = _coverage_by_level(tasks, {"1": "a", "3": "b"})
    assert coverage == {"easy": (1, 2), "hard": (1, 1)}
