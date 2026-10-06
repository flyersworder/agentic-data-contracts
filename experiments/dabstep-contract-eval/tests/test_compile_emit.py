from pathlib import Path

from dce.benchmarks import livesqlbench
from dce.compile.emit import build, render_compiled, write
from dce.compile.session import NOTE_MARKER, EntryRecord

from agentic_data_contracts import DataContract

TABLES = {
    "customers": [("custid", "BIGINT"), ("tier", "VARCHAR")],
    "orders": [("orderid", "BIGINT"), ("amount", "DOUBLE"), ("status", "VARCHAR")],
}


def _r(entry_id, kind, name, domain, status, metrics=(), prose=()):
    return EntryRecord(
        entry_id, kind, name, domain, status, list(metrics), list(prose), "", 1, None
    )


def records():
    return [
        _r(
            3,
            "calculation",
            "Shipped Revenue",
            "calculations",
            "metric",
            [
                {
                    "name": "shipped_revenue",
                    "description": f"Revenue.\n\n{NOTE_MARKER}Lower-case statuses.",
                    "sql_expression": "SUM(amount)",
                    "source_model": "main.orders",
                }
            ],
        ),
        _r(
            4,
            "calculation",
            "Basket Size",
            "calculations",
            "untranslated",
            [
                {
                    "name": "basket_size",
                    "description": "Items per order.",
                    "sql_expression": "",
                    "source_model": "",
                    "untranslated": "bad SQL",
                }
            ],
        ),
        _r(
            2,
            "predicate",
            "Shipped Order",
            "definitions",
            "metric",
            [
                {
                    "name": "shipped_order",
                    "description": "Shipped.",
                    "sql_expression": "status = 'shipped'",
                    "source_model": "main.orders",
                }
            ],
        ),
        _r(
            1,
            "illustration",
            "Customer Tier",
            "reference",
            "prose",
            prose=[
                {
                    "target": "column",
                    "table": "customers",
                    "column": "tier",
                    "text": "Customer Tier: gold or silver.\n\n"
                    f"{NOTE_MARKER}Two values.",
                }
            ],
        ),
        _r(
            6,
            "illustration",
            "Order Status",
            "reference",
            "prose",
            prose=[
                {
                    "target": "domain",
                    "table": None,
                    "column": None,
                    "text": "Order Status: pending, shipped, returned.",
                }
            ],
        ),
    ]


def test_build_and_write_a_contract_the_library_loads(lsb_data, tmp_path: Path):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    contract, semantic = build(inputs, records(), TABLES)
    # In entry-id order of first use. Entry 1's illustration lands on a
    # column, so `reference` first appears with entry 6.
    assert [d["name"] for d in contract["semantic"]["domains"]] == [
        "definitions",
        "calculations",
        "reference",
    ]
    assert [m["name"] for m in semantic["metrics"]] == [
        "shipped_order",
        "shipped_revenue",
        "basket_size",
    ]
    assert semantic["metrics"][2]["untranslated"] == "bad SQL"
    tier = next(
        c
        for t in semantic["tables"]
        if t["table"] == "customers"
        for c in t["columns"]
        if c["name"] == "tier"
    )
    assert tier["description"].startswith(
        "Loyalty tier.\n\nCustomer Tier: gold or silver."
    )
    out = tmp_path / "c"
    write(out, contract, semantic, inputs)
    head = (out / "contract.yml").read_text(encoding="utf-8").splitlines()[0]
    assert head.startswith("# Compiled by dce.compile")
    loaded = DataContract.from_yaml(out / "contract.yml")
    assert loaded.name == "livesqlbench-shop"
    prompt = loaded.to_system_prompt()
    assert "calculations" in prompt and "main.orders" in prompt
    reference = next(
        d for d in contract["semantic"]["domains"] if d["name"] == "reference"
    )
    assert "Order Status: pending, shipped, returned." in reference["description"]


def test_render_compiled_lists_translated_metrics_and_every_note(
    lsb_data, tmp_path: Path
):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    out = tmp_path / "c"
    write(out, *build(inputs, records(), TABLES), inputs)
    text = render_compiled(out)
    assert text.startswith(
        "### shipped_order\n\nOver `main.orders`:\n\n```sql\nstatus = 'shipped'\n```"
    )
    assert (
        "### shipped_revenue" in text and f"{NOTE_MARKER}Lower-case statuses." in text
    )
    assert "basket_size" not in text  # untranslated: no SQL to show
    assert "- customers.tier: Two values." in text
