import shutil
from pathlib import Path

import duckdb
import pytest

LSB_FIXTURE = Path(__file__).parent / "fixtures" / "lsb"


@pytest.fixture
def lsb_data(tmp_path: Path) -> Path:
    """The LiveSQLBench fixture laid out as `LSB_DATA`, its DuckDB built."""
    root = tmp_path / "lsb"
    shutil.copytree(LSB_FIXTURE, root)
    (root / "duckdb").mkdir()
    con = duckdb.connect(str(root / "duckdb" / "shop.duckdb"))
    con.execute((LSB_FIXTURE / "shop.sql").read_text(encoding="utf-8"))
    con.close()
    return root
