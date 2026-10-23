import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import lsb_corrected as lc  # noqa: E402


def test_a_task_is_touched_when_its_entries_meet_a_correction():
    uses = {"d_1": ("d", {0, 1}), "d_2": ("d", {2}), "e_1": ("e", {0})}
    corrected = {"d": {1}}
    assert lc.touched(uses, corrected, ["d_1", "d_2", "e_1"]) == {"d_1"}


def test_corrected_entries_reads_every_correction(tmp_path):
    (tmp_path / "d.json").write_text(
        '[{"entries": [3, "4"]}, {"entries": []}]', encoding="utf-8"
    )
    assert lc.corrected_entries(tmp_path) == {"d": {3, 4}}
