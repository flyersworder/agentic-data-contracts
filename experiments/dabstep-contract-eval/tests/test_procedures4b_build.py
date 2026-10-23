"""Experiment 4b's builder: the gaps as transformations of experiment 4's
databases, the literal and alternative readings used by the checks, and the
gapped and full documentation."""

import json
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "prep" / "procedures4b"))

import build as b4  # noqa: E402

QUALIFY = (
    "QUALIFY ROW_NUMBER() OVER (PARTITION BY casenum ORDER BY specificity DESC, "
    "rule_id ASC) = 1"
)
SQL = (
    """WITH matches AS (
    SELECT x.casenum, k.rule_id, k.flat_charge, k.charge_pct,
        CAST(k.customer_segment IS NOT NULL AS INTEGER) AS specificity
    FROM rec x
    JOIN return_charge_rules k
      ON (k.customer_segment IS NULL OR k.customer_segment = x.seg)
),
best AS (
    SELECT *
    FROM matches
    """
    + QUALIFY
    + """
)
SELECT * FROM best"""
)


def test_g1_adapts_only_the_rule_tables_alias():
    out = b4.adapt_g1(
        SQL + " WHERE x.seg IS NULL",
        "return_charge_rules",
        ("customer_segment",),
        "ANY",
    )
    assert "(k.customer_segment IS NULL OR k.customer_segment = 'ANY')" in out
    assert "(k.customer_segment IS NOT NULL AND k.customer_segment <> 'ANY')" in out
    assert "x.seg IS NULL" in out


def test_g3_lowest_id_and_sum_variants():
    low = b4.g3_variant(SQL, "lowest_id")
    assert "ORDER BY rule_id ASC) = 1" in low
    assert "specificity DESC" not in low
    total = b4.g3_variant(SQL, "sum")
    assert "SUM(flat_charge) AS flat_charge" in total
    assert "GROUP BY casenum" in total
    assert "QUALIFY" not in total


def test_g3_sum_is_the_sum_over_matching_rules():
    con = duckdb.connect()
    con.execute("CREATE TABLE rec AS SELECT 'a' AS casenum, 'R' AS seg")
    con.execute(
        "CREATE TABLE return_charge_rules AS SELECT * FROM (VALUES "
        "(1, NULL, 1.0, 10.0), (2, 'R', 2.0, 20.0)) "
        "t(rule_id, customer_segment, flat_charge, charge_pct)"
    )
    row = con.execute(b4.g3_variant(SQL, "sum")).fetchone()
    assert row is not None and tuple(float(x) for x in row[3:5]) == (3.0, 30.0)


def test_apply_g2_rewrites_one_json_leaf_and_keeps_the_rest():
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE q AS SELECT 'x' AS k, "
        """'{"results": {"aesthetic_score": 5, "qa": "Pass"}}' AS s"""
    )
    cand = {
        "table": "q",
        "column": "s",
        "json_path": "$.results.aesthetic_score",
        "stored": "v - 1",
        "recover": "v + 1",
    }
    b4.apply_g2(con, cand, "stored")
    value = json.loads(con.execute("SELECT s FROM q").fetchone()[0])
    assert value == {"results": {"aesthetic_score": 4, "qa": "Pass"}}
    b4.apply_g2(con, cand, "recover")
    value = json.loads(con.execute("SELECT s FROM q").fetchone()[0])
    assert value["results"]["aesthetic_score"] == 5


def test_numbers_keep_their_kind():
    assert b4.number(4.0, was_int=True) == 4
    assert isinstance(b4.number(4.0, was_int=True), int)
    assert b4.number(0.9773, was_int=False) == 0.9773


def test_sample_text_transforms_the_displayed_leaf():
    text = "{'results': {'qa_result': 'Fail', 'aesthetic_score': 5}, 'x': 1}"
    cand = {"json_path": "$.results.aesthetic_score", "stored": "v - 1"}
    assert b4.transform_sample_text(text, cand) == (
        "{'results': {'qa_result': 'Fail', 'aesthetic_score': 4}, 'x': 1}"
    )


def test_r06_becomes_which_rules_match():
    question = {
        "id": "reverse_logistics_R06",
        "query": "Which rule applies to return RL859197?",
        "gold_sql": SQL.replace("SELECT * FROM best", "SELECT rule_id FROM best"),
        "order": "False",
    }
    new = b4.rewrite_r06(question)
    assert "match" in new["query"] and "RL859197" in new["query"]
    assert new["order"] == "True"
    assert "best AS" not in new["gold_sql"]
    assert (
        new["gold_sql"]
        .rstrip()
        .endswith(
            "SELECT rule_id FROM matches WHERE casenum = 'RL859197' ORDER BY rule_id"
        )
    )


def test_doc_edits_reach_both_documentations_and_must_match_once():
    import pytest

    rows = [
        {"id": 32, "definition": "a non-NULL criterion never matches."},
        {"id": 36, "definition": "A is 1 to 10."},
    ]
    gaps = {
        "doc_edits": [
            {
                "entry_id": 32,
                "old": "a non-NULL criterion",
                "new": "a criterion that places a restriction",
            }
        ],
        "g2": {"entry_id": 36, "full_note": "Stored 0 to 9."},
    }
    for out in (b4.gapped_kb(rows, gaps), b4.full_kb(rows, gaps)):
        assert (
            out[0]["definition"]
            == "a criterion that places a restriction never matches."
        )
    assert b4.full_kb(rows, gaps)[1]["definition"] == "A is 1 to 10. Stored 0 to 9."
    bad = {"doc_edits": [{"entry_id": 32, "old": "absent", "new": "x"}]}
    with pytest.raises(SystemExit):
        b4.gapped_kb(rows, bad)
