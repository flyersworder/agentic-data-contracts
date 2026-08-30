import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
from dce.arms import ARMS, build_arm, check_and_restore, make_working_copy
from pydantic_ai import ModelRetry

DOCS = {"manual": "FEE RULE ALPHA: match on card_scheme.", "payments_readme": "cols"}

# A stand-in `RunContext`: the pydantic-ai wrapper around governed tools only
# reads `ctx.usage.total_tokens` (to observe spend) and `ctx.run_id` (to scope
# that observation) before dispatching, so a duck-typed object with just those
# two attributes is enough to call a governed tool's `.function` directly in a
# test, without an `Agent` run around it.
_CTX = SimpleNamespace(usage=SimpleNamespace(total_tokens=0), run_id=None)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    path = tmp_path / "t.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE payments AS SELECT 1 AS psp_reference")
    con.close()
    return path


def _tool(setup, name: str):
    return next(t for t in setup.tools if t.name == name)


def test_three_arms_with_the_spec_names():
    assert ARMS == ("schema_only", "manual_prompt", "contract")


def test_max_rows_is_capped_well_below_ten_thousand():
    """Pins the lowered `MAX_ROWS` (dce/agent.py review, N2): a 10,000-row
    tool return serializes to roughly 429k tokens and becomes the next
    request's input in full — useless to a model and expensive to ship. 1,000
    rows is still far beyond any DABStep answer's needs; a future edit
    creeping this back toward 10,000 should fail here, loudly, rather than
    only show up as an unexplained cost spike in a live sweep.
    """
    import dce.arms as arms_mod

    assert arms_mod.MAX_ROWS == 1_000


def test_schema_only_prompt_contains_no_manual_text(db):
    setup = build_arm("schema_only", db, DOCS)
    assert "FEE RULE ALPHA" not in setup.system_prompt


def test_manual_prompt_contains_the_manual_verbatim(db):
    setup = build_arm("manual_prompt", db, DOCS)
    assert "FEE RULE ALPHA: match on card_scheme." in setup.system_prompt


def test_contract_arm_prompt_contains_no_verbatim_manual_text(db):
    # Arm C must carry the manual's knowledge as structure, not as prose.
    setup = build_arm("contract", db, DOCS)
    assert "FEE RULE ALPHA" not in setup.system_prompt


def test_only_the_contract_arm_gets_the_governed_tools(db):
    names = {t.name for t in build_arm("contract", db, DOCS).tools}
    assert {"inspect_query", "run_query", "lookup_metric"} <= names

    for arm in ("schema_only", "manual_prompt"):
        ungoverned = {t.name for t in build_arm(arm, db, DOCS).tools}
        assert "inspect_query" not in ungoverned
        assert "lookup_metric" not in ungoverned


def test_ungoverned_arms_still_execute_sql(db):
    setup = build_arm("schema_only", db, DOCS)
    assert "execute_sql" in {t.name for t in setup.tools}


def test_only_the_contract_arm_carries_a_session(db):
    assert build_arm("contract", db, DOCS).session is not None
    assert build_arm("schema_only", db, DOCS).session is None


def test_unknown_arm_raises(db):
    with pytest.raises(ValueError):
        build_arm("some_other_arm", db, DOCS)


def test_all_three_arms_share_one_process_without_connection_conflict(db):
    """C1 regression guard.

    Arm `contract`'s `DuckDBAdapter` opens a persistent read-write connection
    and never closes it until `.close()` is called. Task 7's runner builds
    the arm map up front, in one process, so arms A and B must still be able
    to open and use `db` afterward — previously they opened `read_only=True`,
    which DuckDB refuses once a differently-configured connection to the same
    file exists, crashing every A/B tool call rather than returning a
    recoverable error.
    """
    setups = [build_arm(arm, db, DOCS) for arm in ARMS]
    try:
        out_a = _tool(setups[0], "list_tables").function()
        assert "payments" in out_a

        out_b = _tool(setups[1], "list_tables").function()
        assert "payments" in out_b

        out_c = asyncio.run(
            _tool(setups[2], "describe_table").function(
                _CTX, schema="main", table="payments"
            )
        )
        assert "psp_reference" in out_c
    finally:
        for setup in setups:
            setup.close()


def test_row_cap_parity_between_ungoverned_and_governed_arms(db, monkeypatch):
    import dce.arms as arms_mod

    monkeypatch.setattr(arms_mod, "MAX_ROWS", 5)

    con = duckdb.connect(str(db))
    con.execute(
        "CREATE OR REPLACE TABLE payments AS "
        "SELECT range AS psp_reference FROM range(20)"
    )
    con.close()

    setup_a = build_arm("schema_only", db, DOCS)
    out_a = _tool(setup_a, "execute_sql").function(
        "SELECT psp_reference FROM payments ORDER BY psp_reference"
    )
    rows_a = [
        line
        for line in out_a.splitlines()[1:]  # drop the CSV header row
        if not line.startswith("-- truncated")
    ]

    setup_c = build_arm("contract", db, DOCS)
    out_c = asyncio.run(
        _tool(setup_c, "run_query").function(
            _CTX, sql="SELECT psp_reference FROM main.payments ORDER BY psp_reference"
        )
    )
    blob = out_c[out_c.index("{") :].split("\n-- truncated")[0]
    data = json.loads(blob)

    assert len(rows_a) == 5
    assert len(data["rows"]) == 5

    setup_a.close()
    setup_c.close()


def test_bad_query_returns_something_the_model_can_act_on_every_arm(db):
    setup_a = build_arm("schema_only", db, DOCS)
    out_a = _tool(setup_a, "execute_sql").function("SELECT * FROM nonexistent_table")
    assert out_a.startswith("ERROR:")

    setup_c = build_arm("contract", db, DOCS)
    # `SELECT *` is blocked by `no_select_star` before execution — a
    # recoverable validation block, surfaced as `ModelRetry` (not a bare
    # exception) so the model can rewrite its query and try again.
    with pytest.raises(ModelRetry):
        asyncio.run(
            _tool(setup_c, "run_query").function(
                _CTX, sql="SELECT * FROM main.payments"
            )
        )

    setup_a.close()
    setup_c.close()


def test_csv_rendering_survives_a_comma_bearing_cell(db):
    con = duckdb.connect(str(db))
    con.execute(
        "CREATE OR REPLACE TABLE payments AS SELECT 'Restaurants, cafes' AS description"
    )
    con.close()

    setup = build_arm("schema_only", db, DOCS)
    out = _tool(setup, "execute_sql").function("SELECT description FROM payments")

    import csv
    import io

    rows = list(csv.reader(io.StringIO(out)))
    assert rows[0] == ["description"]
    assert rows[1] == ["Restaurants, cafes"]

    setup.close()


def test_truncation_marker_present_when_a_result_is_cut(db, monkeypatch):
    import dce.arms as arms_mod

    monkeypatch.setattr(arms_mod, "MAX_ROWS", 3)

    con = duckdb.connect(str(db))
    con.execute(
        "CREATE OR REPLACE TABLE payments AS "
        "SELECT range AS psp_reference FROM range(10)"
    )
    con.close()

    setup_a = build_arm("schema_only", db, DOCS)
    out_a = _tool(setup_a, "execute_sql").function("SELECT psp_reference FROM payments")
    # The marker carries the true total too, not just that a cut happened:
    # arm C keeps `row_count` regardless of truncation, so arm A must learn
    # the same thing about the rows it didn't see.
    assert "-- truncated at 3 rows (10 total)" in out_a

    setup_c = build_arm("contract", db, DOCS)
    out_c = asyncio.run(
        _tool(setup_c, "run_query").function(
            _CTX, sql="SELECT psp_reference FROM main.payments"
        )
    )
    assert "-- truncated at 3 rows (10 total)" in out_c

    setup_a.close()
    setup_c.close()


def test_check_and_restore_detects_a_mutation_made_while_a_governed_arm_is_open(
    tmp_path,
):
    """C1b regression test.

    Reproduces the reported sequence: arm `contract`'s connection stays open
    (as it does for the arm's whole lifetime) while an ungoverned arm mutates
    the shared database underneath it. DuckDB keeps that mutation in a `.wal`
    sidecar until something checkpoints, so a check that only digests the
    main file reports a false `corrupted=False` right when the database is
    being destroyed — this is the reported false negative. It must now report
    `corrupted=True` regardless of whether the connection that caused it is
    still open, and — once every arm has actually been closed, the only valid
    sequence (see the module docstring) — the repair must be genuine and
    durable, verified by reopening the file fresh.
    """
    pristine = tmp_path / "pristine.duckdb"
    con = duckdb.connect(str(pristine))
    con.execute("CREATE TABLE payments AS SELECT 1 AS psp_reference")
    con.close()

    working = make_working_copy(pristine, tmp_path / "working.duckdb")

    setup_c = build_arm("contract", working, DOCS)  # holds a connection open
    setup_a = build_arm("schema_only", working, DOCS)
    out = _tool(setup_a, "execute_sql").function("DROP TABLE payments")
    assert not out.startswith("ERROR")  # the mutation succeeds -- by design

    # The reported false negative happened exactly here: checking while a
    # governed arm's connection is still open.
    mid_flight = check_and_restore(working, pristine)
    assert mid_flight.corrupted is True
    assert mid_flight.wal_present is True

    # The documented, only-valid sequence: close every arm for the task
    # first. DuckDB checkpoints a live connection's own state onto the main
    # file when it finally closes -- independent of whatever this module did
    # to the file while it was open -- so the mid-flight restore above is not
    # guaranteed to have survived. A check performed only now is genuinely
    # valid, and must still catch and repair whatever state that leaves.
    setup_a.close()
    setup_c.close()

    result = check_and_restore(working, pristine)
    assert result.corrupted is True

    con = duckdb.connect(str(working))
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    con.close()
    assert "payments" in tables
