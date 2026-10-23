import gzip
import json
import sys
from pathlib import Path

import duckdb
import pytest
from dce.benchmarks import dabstep, livesqlbench
from dce.compile import run as run_module
from dce.compile.run import (
    FINAL_LOG,
    LOG,
    compile_database,
    log_spend,
    read_log,
    reopen,
)
from dce.compile.session import DriverResult

from agentic_data_contracts import DataContract

USAGE = {"input_tokens": 10, "output_tokens": 5, "cached_tokens": 0, "usd": 0.25}

# The shop fixture's six entries, as a model that knows the answers would
# submit them.
ANSWERS = {
    "Customer Tier": {"target": "customers.tier"},
    "Shipped Order": {"sql_expression": "status = 'shipped'", "source_model": "orders"},
    "Shipped Revenue": {
        "sql_expression": "SUM(CASE WHEN status = 'shipped' THEN amount END)",
        "source_model": "orders",
    },
    "Basket Size": {"sql_expression": "SUM(qty) / COUNT(*)", "source_model": "orders"},
    "Gold Customer": {"sql_expression": "tier = 'gold'", "source_model": "customers"},
    "Order Status": {"target": "orders.status"},
}


def shop_driver(calls: list):
    def driver(*, system_prompt, user_prompt, tools, model=None):
        name = user_prompt.split("## Entry: ", 1)[1].split("\n", 1)[0]
        calls.append(name)
        submit = next(t for t in tools if t.__name__ == "submit_entry")
        submit(**ANSWERS[name])
        return DriverResult(messages=[{"entry": name}], usage=dict(USAGE))

    return driver


def test_a_database_compiles_to_a_loadable_contract_and_a_gzipped_log(
    lsb_data, tmp_path
):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    out = tmp_path / "contracts" / inputs.out_subdir
    calls: list = []
    status, spent = compile_database(inputs, out, shop_driver(calls), spend_cap=10.0)
    assert status == "done" and spent == pytest.approx(1.5)
    assert calls == [
        "Customer Tier",
        "Shipped Order",
        "Shipped Revenue",
        "Basket Size",
        "Gold Customer",
        "Order Status",
    ]
    assert not (out / LOG).exists() and (out / FINAL_LOG).exists()
    lines = gzip.decompress((out / FINAL_LOG).read_bytes()).decode("utf-8").splitlines()
    header = json.loads(lines[0])
    assert header["type"] == "header" and header["model"] == "claudesonnet5"
    assert set(header["inputs"]) == {
        "shop_kb.jsonl",
        "shop_column_meaning_base.json",
        "shop_schema.txt",
        "shop.duckdb",
    }
    contract = DataContract.from_yaml(out / "contract.yml")
    assert contract.name == "livesqlbench-shop"
    assert log_spend(tmp_path / "contracts") == pytest.approx(1.5)
    # A finished database is skipped, and nothing is called.
    again: list = []
    assert (
        compile_database(inputs, out, shop_driver(again), spend_cap=10.0)[0]
        == "skipped"
    )
    assert again == []


def test_a_capped_run_resumes_where_it_stopped_without_rebilling(lsb_data, tmp_path):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    out = tmp_path / "c"
    first: list = []
    status, spent = compile_database(inputs, out, shop_driver(first), spend_cap=0.5)
    assert status == "capped" and first == ["Customer Tier", "Shipped Order"]
    # A torn last line, as a crash mid-write leaves it, is dropped.
    with (out / LOG).open("a", encoding="utf-8") as fh:
        fh.write('{"type": "entry", "entry_id": 3, "kin')
    header, records, torn = read_log(out / LOG)
    assert torn and [r.entry_id for r in records] == [1, 2]
    second: list = []
    status, spent = compile_database(
        inputs, out, shop_driver(second), spend_cap=10.0, spent=spent
    )
    assert status == "done"
    assert second == ["Shipped Revenue", "Basket Size", "Gold Customer", "Order Status"]
    assert spent == pytest.approx(1.5)


def test_a_resume_with_changed_inputs_is_refused(lsb_data, tmp_path):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    out = tmp_path / "c"
    compile_database(inputs, out, shop_driver([]), spend_cap=0.3)
    kb = lsb_data / "hf-full-v1" / "shop" / "shop_kb.jsonl"
    kb.write_text(
        kb.read_text(encoding="utf-8").replace("loyalty", "LOYALTY"), encoding="utf-8"
    )
    changed = livesqlbench.compile_inputs(lsb_data, "shop")
    with pytest.raises(SystemExit, match="inputs changed"):
        compile_database(changed, out, shop_driver([]), spend_cap=10.0)


_OPENED: list[str] | None = None


def _audit(event, args):
    if (
        event == "open"
        and _OPENED is not None
        and isinstance(args[0], (str, bytes, Path))
    ):
        _OPENED.append(str(args[0]))


sys.addaudithook(_audit)


def test_the_compiler_opens_no_task_gold_or_test_case_file(
    lsb_data, tmp_path, monkeypatch
):
    global _OPENED
    monkeypatch.setattr(run_module, "claude_driver", shop_driver([]))
    forbidden = {
        str(lsb_data / "gold_results_full_v1.json"),
        str(lsb_data / "tasks_frozen_full_v1.json"),
        str(lsb_data / "gold_sql.json"),
        str(lsb_data / "hf-full-v1" / "livesqlbench_data.jsonl"),
    }
    _OPENED = []
    try:
        from dce.compile.__main__ import main

        main(
            [
                "--benchmark",
                "livesqlbench",
                "--db",
                "shop",
                "--lsb-data",
                str(lsb_data),
                "--out",
                str(tmp_path / "out"),
                "--max-spend",
                "10",
            ]
        )
        opened = set(_OPENED)
    finally:
        _OPENED = None
    assert (tmp_path / "out" / "livesqlbench" / "shop" / "contract.yml").exists()
    assert not opened & forbidden, opened & forbidden
    assert any(p.endswith("shop_kb.jsonl") for p in opened)  # the hook does see opens


def test_dabstep_compiles_without_opening_tasks_or_golds(tmp_path):
    global _OPENED
    context = tmp_path / "context"
    context.mkdir()
    (context / "manual.md").write_text(
        "# Guide\n\n## 1. Fees\nfee = amount * 2\n", encoding="utf-8"
    )
    (context / "payments-readme.md").write_text("amount: euros.", encoding="utf-8")
    db = tmp_path / "d.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE payments AS SELECT 1.5 AS amount")
    con.close()
    for name in ("tasks.json", "golds.json"):
        (tmp_path / name).write_text("{}", encoding="utf-8")

    def driver(*, system_prompt, user_prompt, tools):
        submit = next(t for t in tools if t.__name__ == "submit_entry")
        metrics = []
        if "## 1. Fees" in user_prompt:
            metrics = [
                {
                    "name": "fee",
                    "kind": "calculation",
                    "description": "The fee.",
                    "sql_expression": "amount * 2",
                    "source_model": "payments",
                }
            ]
        submit(metrics=metrics)
        return DriverResult(messages=[], usage=dict(USAGE))

    _OPENED = []
    try:
        inputs = dabstep.compile_inputs(context, db)
        status, _ = compile_database(inputs, tmp_path / "out", driver, spend_cap=10.0)
        opened = set(_OPENED)
    finally:
        _OPENED = None
    assert status == "done"
    assert not opened & {str(tmp_path / "tasks.json"), str(tmp_path / "golds.json")}
    contract = DataContract.from_yaml(tmp_path / "out" / "contract.yml")
    assert "1_fees" in contract.to_system_prompt()


def _function_factory(fn):
    from pydantic_ai import Agent
    from pydantic_ai.models.function import FunctionModel

    def factory(*, model, system_prompt, tools, retries):
        return Agent(FunctionModel(fn), tools=tools, retries=retries)

    return factory


def _looping_model(messages, info):
    from pydantic_ai.messages import ModelResponse, ToolCallPart
    from pydantic_ai.usage import RequestUsage

    return ModelResponse(
        parts=[ToolCallPart(tool_name="run_sql", args={"sql": "SELECT 1"})],
        usage=RequestUsage(input_tokens=100_000, output_tokens=100),
    )


def test_an_entry_that_hits_the_request_cap_is_priced_not_free(monkeypatch):
    import dce.agent

    monkeypatch.setattr(
        dce.agent, "_default_agent_factory", _function_factory(_looping_model)
    )

    def run_sql(sql: str) -> str:
        """Run SQL."""
        return "x\n1"

    result = run_module.claude_driver(
        system_prompt="s", user_prompt="u", tools=[run_sql]
    )
    assert result.usage["usd"] > 0 and "error" in result.usage
    assert not result.usage.get("fatal")
    assert result.messages  # the turns up to the cap are kept


def test_a_transport_error_is_priced_and_marked_fatal(monkeypatch):
    import dce.agent

    def broken(messages, info):
        raise RuntimeError("529 overloaded")

    monkeypatch.setattr(dce.agent, "_default_agent_factory", _function_factory(broken))
    result = run_module.claude_driver(system_prompt="s", user_prompt="u", tools=[])
    assert result.usage["fatal"] and "529" in result.usage["error"]
    assert result.usage["usd"] == 0.0


def _overloaded_then(answers: int, status: int):
    """A model whose first `answers` requests fail with `status`, then
    answers 'done'."""
    from pydantic_ai.exceptions import ModelHTTPError
    from pydantic_ai.messages import ModelResponse, TextPart

    def fn(messages, info):
        fn.calls += 1
        if fn.calls <= answers:
            raise ModelHTTPError(status, "claudesonnet5", {"error": "overloaded"})
        return ModelResponse(parts=[TextPart("done")])

    fn.calls = 0
    return fn


def test_a_transient_provider_error_is_retried_not_fatal(monkeypatch):
    import dce.agent

    waits: list = []
    monkeypatch.setattr(run_module.time, "sleep", waits.append)
    model = _overloaded_then(2, 503)
    monkeypatch.setattr(dce.agent, "_default_agent_factory", _function_factory(model))
    result = run_module.claude_driver(system_prompt="s", user_prompt="u", tools=[])
    assert "error" not in result.usage and not result.usage.get("fatal")
    assert model.calls == 3 and len(waits) == 2


def test_transient_errors_stop_after_the_last_backoff(monkeypatch):
    import dce.agent

    monkeypatch.setattr(run_module.time, "sleep", lambda s: None)
    model = _overloaded_then(99, 529)
    monkeypatch.setattr(dce.agent, "_default_agent_factory", _function_factory(model))
    result = run_module.claude_driver(system_prompt="s", user_prompt="u", tools=[])
    assert result.usage["fatal"] and "529" in result.usage["error"]
    assert model.calls == len(run_module.TRANSIENT_BACKOFF_S) + 1


def test_a_client_error_is_not_retried(monkeypatch):
    import dce.agent

    monkeypatch.setattr(run_module.time, "sleep", lambda s: None)
    model = _overloaded_then(99, 400)
    monkeypatch.setattr(dce.agent, "_default_agent_factory", _function_factory(model))
    result = run_module.claude_driver(system_prompt="s", user_prompt="u", tools=[])
    assert result.usage["fatal"] and model.calls == 1


def test_a_fatal_entry_logs_its_spend_stops_and_is_retried(lsb_data, tmp_path):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    out = tmp_path / "c"
    calls: list = []
    good = shop_driver(calls)

    def flaky(*, system_prompt, user_prompt, tools):
        if "## Entry: Shipped Order" in user_prompt and not flaky.failed:
            flaky.failed = True
            return DriverResult(
                messages=[], usage={"usd": 0.4, "error": "529", "fatal": True}
            )
        return good(system_prompt=system_prompt, user_prompt=user_prompt, tools=tools)

    flaky.failed = False
    with pytest.raises(SystemExit, match="529"):
        compile_database(inputs, out, flaky, spend_cap=10.0)
    assert log_spend(out) == pytest.approx(0.25 + 0.4)
    header, records, torn = read_log(out / LOG)
    assert [r.entry_id for r in records] == [1]
    status, spent = compile_database(inputs, out, flaky, spend_cap=10.0)
    assert status == "done" and "Shipped Order" in calls
    assert log_spend(out) == pytest.approx(1.5 + 0.4)


def test_a_finished_database_whose_inputs_changed_is_refused(lsb_data, tmp_path):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    out = tmp_path / "c"
    compile_database(inputs, out, shop_driver([]), spend_cap=10.0)
    kb = lsb_data / "hf-full-v1" / "shop" / "shop_kb.jsonl"
    kb.write_text(
        kb.read_text(encoding="utf-8").replace("loyalty", "LOYALTY"), encoding="utf-8"
    )
    changed = livesqlbench.compile_inputs(lsb_data, "shop")
    with pytest.raises(SystemExit, match="inputs changed"):
        compile_database(changed, out, shop_driver([]), spend_cap=10.0)


def test_reopen_recompiles_entries_and_their_parents_keeping_their_cost(
    lsb_data, tmp_path
):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    out = tmp_path / "contracts" / inputs.out_subdir
    compile_database(inputs, out, shop_driver([]), spend_cap=10.0)
    # Shipped Revenue (3) is built on Shipped Order (2), so it goes too.
    assert reopen(out, inputs, [2]) == [2, 3]
    assert (out / LOG).exists() and not (out / FINAL_LOG).exists()
    # The first try is kept whole, not only its cost.
    replaced = [
        d
        for d in map(json.loads, (out / LOG).read_text().splitlines())
        if d["type"] == "spend"
    ]
    assert [d["replaced"]["name"] for d in replaced] == [
        "Shipped Order",
        "Shipped Revenue",
    ]
    assert replaced[0]["replaced"]["metrics"][0]["sql_expression"]
    assert log_spend(tmp_path / "contracts") == pytest.approx(1.5)
    calls: list = []
    status, spent = compile_database(inputs, out, shop_driver(calls), spend_cap=10.0)
    assert status == "done" and calls == ["Shipped Order", "Shipped Revenue"]
    lines = [
        json.loads(line)
        for line in gzip.decompress((out / FINAL_LOG).read_bytes())
        .decode("utf-8")
        .splitlines()
    ]
    [marker] = [d for d in lines if d["type"] == "reopen"]
    assert marker["entry_ids"] == [2, 3] and marker["commit_sha"]
    assert log_spend(tmp_path / "contracts") == pytest.approx(2.0)
    assert DataContract.from_yaml(out / "contract.yml")


def test_reopen_refuses_an_unknown_entry_or_an_unfinished_database(lsb_data, tmp_path):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    out = tmp_path / "c"
    with pytest.raises(SystemExit, match="not finished"):
        reopen(out, inputs, [2])
    compile_database(inputs, out, shop_driver([]), spend_cap=10.0)
    with pytest.raises(SystemExit, match="no entry 99"):
        reopen(out, inputs, [99])


def test_the_cli_recompiles_named_entries_of_one_database(
    lsb_data, tmp_path, monkeypatch
):
    from dce.compile.__main__ import main

    calls: list = []
    monkeypatch.setattr(run_module, "claude_driver", shop_driver(calls))
    args = ["--benchmark", "livesqlbench", "--lsb-data", str(lsb_data)]
    args += ["--out", str(tmp_path), "--max-spend", "10"]
    main([*args, "--db", "shop"])
    calls.clear()
    main([*args, "--db", "shop", "--recompile", "2"])
    assert calls == ["Shipped Order", "Shipped Revenue"]
    with pytest.raises(SystemExit, match="one database"):
        main([*args, "--all", "--recompile", "2"])
