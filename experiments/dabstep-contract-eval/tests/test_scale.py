import duckdb
import yaml
from dce import scale
from dce.frozen import load_contract_at


def _contract(db: str, *, tables=("orders", "customers"), rules=None):
    contract = {
        "version": "0.1",
        "name": db,
        "semantic": {
            "source": {"type": "yaml", "path": "./semantic.yml"},
            "allowed_tables": [
                {
                    "schema": "main",
                    "description": f"{db} data",
                    "preferred": True,
                    "tables": list(tables),
                }
            ],
            "forbidden_operations": ["DELETE", "DROP"],
            "domains": [
                {
                    "name": "spend",
                    "summary": "What customers spend",
                    "description": "Use order_total over main.orders.",
                    "tables": [f"main.{tables[0]}"],
                }
            ],
            "rules": rules
            if rules is not None
            else [
                {
                    "name": "no_select_star",
                    "description": "Name the columns",
                    "enforcement": "block",
                    "query_check": {"no_select_star": True},
                },
                {
                    "name": "round_late",
                    "description": "Round order_total only when reported.",
                    "enforcement": "warn",
                },
            ],
        },
    }
    semantic = {
        "metrics": [
            {
                "name": "order_total",
                "description": "Sum of amounts; see order_count.",
                "sql_expression": "SUM(o.amount)",
                "source_model": f"main.{tables[0]}",
                "from_sql": f"main.{tables[0]} o JOIN main.{tables[1]} c "
                "ON o.cust = c.id",
                "grain": "one row per order",
                "domains": ["spend"],
            },
            {
                "name": "order_count",
                "description": "Number of orders.",
                "sql_expression": "COUNT(*)",
                "source_model": f"main.{tables[0]}",
                "from_sql": f"{tables[0]}",
                "grain": "one row per order",
                "domains": ["spend"],
            },
        ],
        "tables": [
            {
                "schema": "main",
                "table": t,
                "columns": [{"name": "id", "type": "INT", "description": "Key."}],
            }
            for t in tables
        ],
        "relationships": [
            {
                "from": f"main.{tables[0]}.cust",
                "to": f"main.{tables[1]}.id",
                "type": "many_to_one",
            }
        ],
    }
    return contract, semantic


def test_qualify_moves_main_and_bare_tables_into_the_schema():
    sql = "SELECT o.amount FROM main.orders o JOIN customers c ON o.cust = c.id"
    out = scale.qualify(sql, "shop", {"orders", "customers"})
    assert "shop.orders" in out and "shop.customers" in out
    assert "main." not in out
    # A FROM fragment, as from_sql holds it.
    frag = scale.qualify_from(
        "main.orders o JOIN customers c ON o.cust = c.id",
        "shop",
        {"orders", "customers"},
    )
    assert frag.startswith("shop.orders")
    assert "shop.customers" in frag


def test_qualify_leaves_other_tables_and_columns_alone():
    out = scale.qualify("SELECT orders.amount FROM orders, ref", "shop", {"orders"})
    assert "shop.orders" in out
    assert "shop.ref" not in out


def test_namespace_prefixes_names_and_qualifies_tables():
    contract, semantic = scale.namespace(*_contract("shop"), "shop")
    sem = contract["semantic"]
    assert sem["allowed_tables"][0]["schema"] == "shop"
    assert sem["domains"][0]["name"] == "shop.spend"
    assert sem["domains"][0]["tables"] == ["shop.orders"]
    assert "shop.orders" in sem["domains"][0]["description"]
    assert "shop.order_total" in sem["domains"][0]["description"]
    m = semantic["metrics"][0]
    assert m["name"] == "shop.order_total"
    assert m["domains"] == ["shop.spend"]
    assert m["source_model"] == "shop.orders"
    assert "shop.order_count" in m["description"]
    assert m["from_sql"].startswith("shop.orders")
    assert semantic["metrics"][1]["from_sql"] == "shop.orders"
    assert semantic["tables"][0]["schema"] == "shop"
    assert semantic["relationships"][0]["from"] == "shop.orders.cust"


def test_namespace_drops_enforcing_rules_and_keeps_prose_rules():
    contract, _ = scale.namespace(*_contract("shop"), "shop")
    rules = contract["semantic"]["rules"]
    assert [r["name"] for r in rules] == ["shop.round_late"]
    assert "shop.order_total" in rules[0]["description"]


def test_padding_order_puts_the_near_databases_first_and_is_seeded():
    pool = [f"p{i}" for i in range(10)]
    one = scale.padding_order("base", ["n2", "n1"], pool, seed=2026)
    assert one[:2] == ["n1", "n2"]
    assert sorted(one[2:]) == pool
    assert one == scale.padding_order("base", ["n2", "n1"], pool, seed=2026)
    assert one != scale.padding_order("other", ["n2", "n1"], pool, seed=2026)


def test_level_padding_takes_the_shortest_prefix_that_reaches_the_target():
    order = ["a", "b", "c", "d"]
    sizes = {"base": 10, "a": 10, "b": 15, "c": 30, "d": 50}
    assert scale.level_padding(order, sizes, "base", 1) == []
    assert scale.level_padding(order, sizes, "base", 2) == ["a"]
    assert scale.level_padding(order, sizes, "base", 4) == ["a", "b", "c"]
    assert scale.level_padding(order, sizes, "base", None) == order


def test_insert_mid_splits_the_padding_by_size():
    pad = [{"n": "x" * 10}, {"n": "y" * 10}, {"n": "z" * 10}, {"n": "w" * 10}]
    out = scale.insert_mid(pad, [{"n": "B"}])
    assert [e["n"][0] for e in out] == ["x", "y", "B", "z", "w"]
    assert scale.insert_mid([], [{"n": "B"}]) == [{"n": "B"}]
    assert scale.insert_mid(pad, [{"n": "B"}], first=True)[0] == {"n": "B"}


def test_merge_keeps_the_base_unchanged_and_in_the_middle(tmp_path):
    base = _contract("base", tables=("orders", "customers"))
    pads = [scale.namespace(*_contract(p), p) for p in ("p1", "p2")]
    contract, semantic = scale.merge(base, pads)
    domains = [d["name"] for d in contract["semantic"]["domains"]]
    assert domains == ["p1.spend", "spend", "p2.spend"]
    assert contract["name"] == "base"
    base_metrics = [m for m in semantic["metrics"] if "." not in m["name"]]
    assert base_metrics == base[1]["metrics"]
    rules = [r["name"] for r in contract["semantic"]["rules"]]
    assert rules == ["p1.round_late", "no_select_star", "round_late", "p2.round_late"]
    out = scale.write(tmp_path, contract, semantic)
    loaded = load_contract_at(out / "contract.yml")
    assert loaded.name == "base"


def test_merge_with_no_padding_is_the_base_contract():
    base = _contract("base")
    contract, semantic = scale.merge(base, [])
    assert (contract, semantic) == base


def test_working_db_copies_other_databases_into_schemas(tmp_path):
    def make(path, table):
        con = duckdb.connect(str(path))
        con.execute(f"CREATE TABLE {table} (id INT)")
        con.execute(f"INSERT INTO {table} VALUES (1), (2)")
        con.close()

    make(tmp_path / "base.duckdb", "orders")
    make(tmp_path / "shop.duckdb", "orders")
    out = scale.working_db(
        tmp_path / "base.duckdb",
        {"shop": tmp_path / "shop.duckdb"},
        tmp_path / "w.duckdb",
    )
    con = duckdb.connect(str(out), read_only=True)
    assert con.sql("SELECT count(*) FROM main.orders").fetchone() == (2,)
    assert con.sql("SELECT count(*) FROM shop.orders").fetchone() == (2,)
    cols = con.sql(
        "SELECT count(*) FROM information_schema.columns "
        "WHERE table_schema = 'main' AND table_name = 'orders'"
    ).fetchone()
    assert cols == (1,)
    con.close()


def test_write_round_trips(tmp_path):
    contract, semantic = _contract("base")
    out = scale.write(tmp_path / "x", contract, semantic)
    assert (
        yaml.safe_load((out / "semantic.yml").read_text(encoding="utf-8")) == semantic
    )
