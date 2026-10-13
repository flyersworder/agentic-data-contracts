from pathlib import Path

import duckdb
from dce.benchmarks import dabstep, livesqlbench
from dce.benchmarks.lsb_grade import PG_COMPAT
from dce.compile.entries import KBEntry


def test_livesqlbench_adapter_maps_kinds_text_and_children(lsb_data):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    assert inputs.benchmark == "livesqlbench" and inputs.db_name == "shop"
    by_id = {e.id: e for e in inputs.entries}
    assert [by_id[i].kind for i in range(1, 7)] == [
        "illustration",
        "predicate",
        "calculation",
        "calculation",
        "predicate",
        "illustration",
    ]
    assert by_id[3] == KBEntry(
        3,
        "calculation",
        "Shipped Revenue",
        "Revenue a customer has realised.\n\n"
        "The sum of amount over the customer's Shipped Orders.",
        (2,),
    )
    assert by_id[1].children == ()  # -1 in the file means none
    assert inputs.ddl.startswith('CREATE TABLE "customers"')
    assert "- orders.amount: Order value; null when not yet priced." in (
        inputs.column_meanings
    )
    assert inputs.column_descriptions[("orders", "amount")] == (
        "Order value; null when not yet priced."
    )
    assert inputs.init_sql == PG_COMPAT
    assert inputs.db_path == lsb_data / "duckdb" / "shop.duckdb"
    assert inputs.out_subdir == "livesqlbench/shop"
    assert {p.name for p in inputs.sources} == {
        "shop_kb.jsonl",
        "shop_column_meaning_base.json",
        "shop_schema.txt",
        "shop.duckdb",
    }


MANUAL = """# A Guide

## Table of Contents
1. Intro

## 1. Introduction
Hello.

## 2. Fees
fee = fixed + rate * value / 10000

### Pricing Overview
More about fees.

####  2.1. Local Acquiring
Same country is cheaper.
"""


def test_manual_sections_split_at_numbered_headings():
    sections = dabstep.manual_sections(MANUAL)
    assert [(s.id, s.kind, s.name) for s in sections] == [
        (0, "section", "A Guide"),
        (1, "section", "1 Introduction"),
        (2, "section", "2 Fees"),
        (3, "section", "2.1 Local Acquiring"),
    ]
    assert sections[2].text == (
        "## 2. Fees\nfee = fixed + rate * value / 10000\n\n"
        "### Pricing Overview\nMore about fees."
    )
    assert "".join(s.text for s in sections).replace("\n", "") == (
        MANUAL.replace("\n", "")
    )


def test_dabstep_adapter(tmp_path: Path):
    context = tmp_path / "context"
    context.mkdir()
    (context / "manual.md").write_text(MANUAL, encoding="utf-8")
    (context / "payments-readme.md").write_text(
        "Columns: psp_reference.", encoding="utf-8"
    )
    db = tmp_path / "d.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE payments (psp_reference BIGINT)")
    con.close()
    inputs = dabstep.compile_inputs(context, db)
    assert inputs.benchmark == "dabstep" and inputs.out_subdir == "dabstep"
    assert [e.name for e in inputs.entries][1:] == [
        "1 Introduction",
        "2 Fees",
        "2.1 Local Acquiring",
    ]
    assert inputs.column_meanings == "Columns: psp_reference."
    assert inputs.table_descriptions == {"payments": "Columns: psp_reference."}
    assert "CREATE TABLE payments" in inputs.ddl
    assert inputs.init_sql == ()
    assert {p.name for p in inputs.sources} == {
        "manual.md",
        "payments-readme.md",
        "d.duckdb",
    }
