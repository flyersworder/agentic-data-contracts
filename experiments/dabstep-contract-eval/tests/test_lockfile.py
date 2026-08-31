"""A sweep's single-instance lock.

The failure this exists to prevent was reproduced by accident: two sweeps
started against one results file duplicated 72 units of paid work. The
results file itself survived — every row landed, nothing was torn, and
`latest_rows` dedupes — but two things do not survive it. The money is spent
twice, and both processes share `<db>.working`, so one instance's
`make_working_copy` copies the pristine database over a file the other holds
a live DuckDB connection on. That produces false `db_corrupted` readings on
the experiment's headline governance metric.

An auto-restarting supervisor, or an operator reconnecting to a VPS and
starting the sweep again because the old session looked dead, is exactly how
that happens.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from dce.lockfile import (
    EMPTY_LOCK_GRACE_S,
    SweepLockedError,
    lock_path_for,
    sweep_lock,
)


def _write_lock(path: Path, **fields) -> None:
    payload = {
        "pid": os.getpid(),
        "host": "somewhere-else",
        "started": "2026-08-31T00:00:00+00:00",
        "argv": ["dce.runner"],
    }
    payload.update(fields)
    path.write_text(json.dumps(payload))


def test_lock_path_sits_beside_the_results_file(tmp_path: Path):
    out = tmp_path / "results" / "glm-full.jsonl"
    assert lock_path_for(out) == tmp_path / "results" / "glm-full.jsonl.lock"


def test_acquires_and_releases(tmp_path: Path):
    out = tmp_path / "r.jsonl"
    with sweep_lock(out) as held:
        assert held.exists()
        assert json.loads(held.read_text())["pid"] == os.getpid()
    assert not lock_path_for(out).exists()


def test_a_second_holder_is_refused_while_the_first_is_alive(tmp_path: Path):
    out = tmp_path / "r.jsonl"
    with sweep_lock(out):
        with pytest.raises(SweepLockedError) as exc:
            with sweep_lock(out):
                pass
    # The message has to be actionable: a sweep refusing to start on a VPS
    # at 3am is read by someone who needs to know WHICH process holds it.
    assert str(os.getpid()) in str(exc.value)
    assert str(lock_path_for(out)) in str(exc.value)


def test_the_lock_is_released_even_when_the_body_raises(tmp_path: Path):
    out = tmp_path / "r.jsonl"
    with pytest.raises(ValueError):
        with sweep_lock(out):
            raise ValueError("boom")
    assert not lock_path_for(out).exists()


def test_a_lock_held_by_a_dead_pid_on_this_host_is_stolen(tmp_path: Path):
    """The path that makes an auto-restarting supervisor work at all: a
    SIGKILLed sweep leaves its lockfile behind, and the restart must not be
    blocked by its own corpse."""
    out = tmp_path / "r.jsonl"
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    _write_lock(lock_path_for(out), pid=dead.pid, host=os.uname().nodename)
    with sweep_lock(out) as held:
        assert json.loads(held.read_text())["pid"] == os.getpid()


def test_a_lock_from_another_host_is_never_stolen(tmp_path: Path):
    """`os.kill(pid, 0)` answers a question about THIS machine. A lockfile
    written elsewhere — a restored disk image, a shared volume — whose pid
    happens to exist locally would be declared alive or dead by pure
    coincidence, so it is neither: refuse, and say who to ask."""
    out = tmp_path / "r.jsonl"
    _write_lock(lock_path_for(out), pid=999_999, host="other-box")
    with pytest.raises(SweepLockedError, match="other-box"):
        with sweep_lock(out):
            pass


def test_an_unreadable_lockfile_is_stale_once_it_is_old_enough(tmp_path: Path):
    out = tmp_path / "r.jsonl"
    path = lock_path_for(out)
    path.write_text("{not json")
    old = time.time() - (EMPTY_LOCK_GRACE_S + 5)
    os.utime(path, (old, old))
    with sweep_lock(out) as held:
        assert json.loads(held.read_text())["pid"] == os.getpid()


def test_a_freshly_created_empty_lockfile_is_treated_as_live(tmp_path: Path):
    """There is a microsecond between `O_EXCL` creating the file and the
    payload being written. A competitor observing that window sees an empty
    file, and must not read "empty" as "abandoned" — otherwise two sweeps
    can both win the race the lock exists to settle."""
    out = tmp_path / "r.jsonl"
    lock_path_for(out).write_text("")
    with pytest.raises(SweepLockedError):
        with sweep_lock(out):
            pass


def test_release_does_not_delete_a_lock_someone_else_now_holds(
    tmp_path: Path, monkeypatch
):
    """If this process is wrongly declared stale and a successor takes the
    lock, this process exiting must not remove the successor's lock and
    leave the file unguarded."""
    out = tmp_path / "r.jsonl"
    with sweep_lock(out):
        _write_lock(lock_path_for(out), pid=424_242, host="successor-box")
    assert lock_path_for(out).exists()
    assert json.loads(lock_path_for(out).read_text())["pid"] == 424_242


def test_the_payload_names_the_command_that_holds_it(tmp_path: Path):
    out = tmp_path / "r.jsonl"
    with sweep_lock(out) as held:
        payload = json.loads(held.read_text())
    assert payload["host"] == os.uname().nodename
    assert payload["argv"]
    assert payload["started"]
