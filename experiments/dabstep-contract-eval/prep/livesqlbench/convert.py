"""Port every LiveSQLBench PostgreSQL database to a DuckDB file.

Run: LSB_DATA=... LSB_PG_DSN=... uv run --with psycopg2-binary \
         python prep/livesqlbench/convert.py

Reads the benchmark's PostgreSQL databases (the official container) and writes
LSB_DATA/duckdb/<db>.duckdb, one per database, copying every public table and
casting json/jsonb columns to DuckDB JSON. Prints each database's row count and
any table whose row count differs between the engines.
"""

import collections
import os
from pathlib import Path

import duckdb

PG_DSN = os.environ.get("LSB_PG_DSN")  # e.g. "host=... port=... user=... password=..."
if not PG_DSN:
    raise SystemExit("set LSB_PG_DSN to the LiveSQLBench PostgreSQL connection string")

DATA = Path(os.environ.get("LSB_DATA", Path.home() / "data" / "livesqlbench"))
OUT = DATA / "duckdb"
PG = PG_DSN + " dbname="

meta = duckdb.connect()
meta.execute("INSTALL postgres; LOAD postgres;")
meta.execute("ATTACH '" + PG + "postgres' AS pgm (TYPE postgres, READ_ONLY)")
dbs = [
    r[0]
    for r in meta.execute(
        "SELECT * FROM postgres_query('pgm', 'SELECT datname FROM pg_database "
        "WHERE NOT datistemplate AND datname NOT IN "
        "(''postgres'',''root'',''sql_test_template'')')"
    ).fetchall()
]
print(len(dbs), "databases")
types = collections.Counter()
bad = 0
for db in sorted(dbs):
    path = OUT / (db + ".duckdb")
    path.unlink(missing_ok=True)
    con = duckdb.connect(str(path))
    con.execute("LOAD postgres")
    con.execute("ATTACH '" + PG + db + "' AS pg (TYPE postgres, READ_ONLY)")
    tables = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE "
            "table_catalog='pg' AND table_schema='public' "
            "AND table_type='BASE TABLE'"
        ).fetchall()
    ]
    mism = []
    pgtypes = con.execute(
        "SELECT * FROM postgres_query('pg', 'SELECT table_name, column_name, "
        "data_type, udt_name FROM information_schema.columns WHERE "
        "table_schema=''public'' ORDER BY table_name, ordinal_position')"
    ).fetchall()
    cols = collections.defaultdict(list)
    for tn, cn, dt, udt in pgtypes:
        cols[tn].append((cn, dt, udt))
        types["pg:" + dt] += 1
    for t in tables:
        sel = ", ".join(
            f'CAST("{c}" AS JSON) AS "{c}"' if dt in ("json", "jsonb") else f'"{c}"'
            for c, dt, _ in cols[t]
        )
        con.execute(f'CREATE TABLE main."{t}" AS SELECT {sel} FROM pg.public."{t}"')
        n_pg = con.execute(f'SELECT count(*) FROM pg.public."{t}"').fetchone()[0]
        n_dk = con.execute(f'SELECT count(*) FROM main."{t}"').fetchone()[0]
        if n_pg != n_dk:
            mism.append((t, n_pg, n_dk))
    for (dt,) in con.execute(
        "SELECT data_type FROM information_schema.columns WHERE "
        "table_catalog=current_database() AND table_schema='main'"
    ).fetchall():
        types[dt.split("(")[0] if not dt.startswith("ENUM") else "ENUM"] += 1
    rows = sum(
        con.execute(f'SELECT count(*) FROM main."{t}"').fetchone()[0] for t in tables
    )
    con.execute("DETACH pg")
    con.close()
    bad += bool(mism)
    status = f"MISMATCH {mism}" if mism else "ok"
    print(f"{db:<36s} tables={len(tables):2d} rows={rows:7d} {status}")
print("column types:", dict(types.most_common()))
print("dbs with mismatches:", bad)
