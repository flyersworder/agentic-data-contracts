"""Read-only LiveSQLBench queries for a sandboxed rater, over a Unix socket.

The rater's `./q <db> "<sql>"` client sends one request and prints the reply.
This server runs outside the sandbox. It opens the database the grader's way
(`connect_readonly`: read-only, external file access disabled), so a query
cannot write and cannot read any file but that database. Only names of
existing databases are accepted, at most `MAX_ROWS` rows are returned, and a
query is interrupted after `SECONDS`.

Run:  uv run python prep/cross-family-ratings/qserver.py <socket path>
"""

from __future__ import annotations

import json
import re
import socketserver
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dce.benchmarks.lsb_grade import connect_readonly  # noqa: E402

LSB_DUCKDB = Path.home() / "data/livesqlbench/duckdb"
MAX_ROWS = 200
FETCH_LIMIT = 100_000
SECONDS = 60.0
_NAME = re.compile(r"[a-z0-9_]+")


def handle(lsb_dir: Path, db: str, sql: str) -> str:
    """The text the rater sees for one query."""
    path = lsb_dir / f"{db}.duckdb"
    if not _NAME.fullmatch(db) or not path.is_file():
        return f"error: unknown database {db!r}"
    try:
        con = connect_readonly(path)
    except Exception as e:  # noqa: BLE001 - the rater sees the message
        return f"error: {e}"
    timer = threading.Timer(SECONDS, con.interrupt)
    timer.start()
    try:
        cur = con.execute(sql)
        names = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(FETCH_LIMIT + 1)
    except Exception as e:  # noqa: BLE001
        return f"error: {e}"
    finally:
        timer.cancel()
        con.close()
    total = f"more than {FETCH_LIMIT}" if len(rows) > FETCH_LIMIT else str(len(rows))
    lines = [f"columns: {', '.join(names)}"]
    lines += [repr(tuple(r)) for r in rows[:MAX_ROWS]]
    if len(rows) > MAX_ROWS:
        lines.append(f"(first {MAX_ROWS} of {total} rows)")
    else:
        lines.append(f"({total} rows)")
    return "\n".join(lines)


class _Handler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        try:
            req = json.loads(self.rfile.readline())
            reply = handle(LSB_DUCKDB, str(req["db"]), str(req["sql"]))
        except Exception as e:  # noqa: BLE001
            reply = f"error: bad request ({e})"
        self.wfile.write(reply.encode("utf-8"))


def main() -> None:
    sock = Path(sys.argv[1])
    sock.unlink(missing_ok=True)
    with socketserver.ThreadingUnixStreamServer(str(sock), _Handler) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
