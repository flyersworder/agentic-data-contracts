import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import disclosure_mechanism as dm  # noqa: E402

PAD = ["reverse_logistics", "solar_panel"]


def _call(tool, args):
    return {"part_kind": "tool-call", "tool_name": tool, "args": args}


def _ret(tool, content):
    return {"part_kind": "tool-return", "tool_name": tool, "content": content}


def test_a_lookup_naming_a_padding_domain_or_metric_reaches_padding():
    trace = [
        {
            "parts": [
                _call("lookup_domain", {"name": "energy_loss"}),
                _call("lookup_metric", '{"metric_name":"solar_panel.yield"}'),
                _call("lookup_domain", {"name": "reverse_logistics"}),
                _ret("lookup_metric", "x" * 40),
            ]
        }
    ]
    r = dm.reach(trace, PAD)
    assert r["lookups"] == 3
    assert r["padding_lookups"] == 2
    assert r["padding_sql"] == 0
    assert r["max_result"] == 40


def test_sql_naming_a_padding_schema_reaches_padding_but_a_lookalike_does_not():
    trace = [
        {
            "parts": [
                _call("run_query", {"sql": "SELECT * FROM solar_panel.panels"}),
                _call("run_query", {"sql": "SELECT my_solar_panel.x FROM t"}),
                _call("run_query", {"sql": "SELECT 1"}),
            ]
        }
    ]
    r = dm.reach(trace, PAD)
    assert r["lookups"] == 0
    assert r["padding_sql"] == 1


def test_no_padding_at_1x_means_no_reach():
    trace = [{"parts": [_call("run_query", {"sql": "SELECT * FROM solar_panel.p"})]}]
    assert dm.reach(trace, [])["padding_sql"] == 0


def test_a_truncated_trace_is_flagged():
    assert dm.reach([{"__truncated__": True}], PAD)["truncated"]
    assert not dm.reach([{"parts": []}], PAD)["truncated"]


def test_a_trace_name_splits_into_task_arm_and_level():
    assert dm.split("solar_panel_C02__contract_scaled_4x__gpt-6-luna.json.gz") == (
        "solar_panel",
        "contract_scaled_4x",
        "4x",
    )
    assert dm.split(
        "labor_certification_applications_R01__manual_scaled_all__x.json.gz"
    ) == ("labor_certification_applications", "manual_scaled_all", "all")


def test_a_lookup_whose_result_names_padding_counts_even_when_its_call_does_not():
    trace = [
        {
            "parts": [
                _call("list_metrics", {}),
                _ret("list_metrics", "solar_panel.yield, energy_loss"),
                _call("lookup_domain", {"name": "energy_loss"}),
                _ret("lookup_domain", "energy_loss: ..."),
                _ret("run_query", "solar_panel.x"),
            ]
        }
    ]
    r = dm.reach(trace, PAD)
    assert r["padding_lookups"] == 0
    assert r["padding_results"] == 1
