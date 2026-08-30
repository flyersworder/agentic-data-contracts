import json
from pathlib import Path

import duckdb
from dce.data import build_duckdb


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
