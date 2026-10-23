import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import lookup_fidelity as lf  # noqa: E402

EXPR = "SUM(e.kwh / NULLIF(e.avail::DOUBLE / 100, 0))"


def _call(tool, args):
    return {"part_kind": "tool-call", "tool_name": tool, "args": args}


def _ret(tool, content):
    return {"part_kind": "tool-return", "tool_name": tool, "content": content}


def _metric(name, expr):
    return json.dumps({"name": name, "sql_expression": expr})


def test_the_metric_inside_the_final_sql_under_other_aliases_is_exact():
    sql = (
        "SELECT ROUND(SUM(x.kwh / NULLIF(CAST(x.avail AS DOUBLE) / 100, 0)), 2) "
        "FROM t x"
    )
    assert lf.uses(EXPR, sql) == "exact"


def test_the_metric_columns_without_its_formula_is_columns():
    sql = "SELECT SUM(kwh) / AVG(avail) FROM t"
    assert lf.uses(EXPR, sql) == "columns"


def test_sql_without_the_metric_columns_is_none():
    assert lf.uses(EXPR, "SELECT COUNT(*) FROM t") == "none"


def test_an_unparseable_expression_falls_back_to_whitespace_normalized_text():
    assert lf.uses("a ->>> b", "SELECT a   ->>> b FROM t") == "exact"
    assert lf.uses("a ->>> b", "SELECT 1") == "none"


def test_a_run_reads_its_looked_up_metrics_and_its_last_executed_sql():
    trace = [
        {
            "parts": [
                _call("lookup_metric", {"metric_name": "m1"}),
                _ret("lookup_metric", _metric("m1", EXPR)),
                _call("lookup_metric", {"metric_name": "m2"}),
                _ret("lookup_metric", {"name": "m2", "sql_expression": "COUNT(*)"}),
                _ret("lookup_metric", "metric not found"),
                _call("run_query", {"sql": "SELECT 1"}),
                _call("run_query", '{"sql": "SELECT COUNT(*) FROM t"}'),
            ]
        }
    ]
    assert lf.looked_up(trace) == {"m1": EXPR, "m2": "COUNT(*)"}
    assert lf.final_sql(trace) == "SELECT COUNT(*) FROM t"


def test_a_run_is_as_faithful_as_its_best_looked_up_metric():
    assert lf.run_status({"m1": EXPR, "m2": "COUNT(*)"}, "SELECT COUNT(*) FROM t") == (
        "exact"
    )
    assert lf.run_status({"m1": EXPR}, "SELECT kwh, avail FROM t") == "columns"
    assert lf.run_status({"m1": EXPR}, "SELECT kwh FROM t") == "none"
    assert lf.run_status({}, "SELECT 1") == "no lookup"
    assert lf.run_status({"m1": EXPR}, None) == "no sql"
