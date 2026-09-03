"""Single-instance lock for a sweep's results file.

WHAT THIS PREVENTS, MEASURED. Two sweeps started against one results file
duplicated 72 units of paid work in a probe here. The results FILE survived
that intact — every row landed, no line was torn, and `latest_rows` dedupes —
which is precisely why the failure is dangerous: nothing about the output says
it happened. Two things do not survive it:

  * The money is spent twice.
  * Both instances share `<db>.working`. One's `make_working_copy` copies the
    pristine database over a file the other holds a live DuckDB connection on,
    so `check_and_restore` reports corruption that no arm caused — a false
    positive on this experiment's headline governance metric.

An auto-restarting supervisor, or an operator reconnecting to a VPS and
starting the sweep again because the old session looked dead, is exactly how
two instances happen.

THE STALENESS RULE, AND WHY IT ERRS THE WAY IT DOES. A lock is stolen only
when this host can positively establish that its holder is gone. Anything
else refuses to start and names what to look at, because the two errors are
not symmetric: a false "locked" stalls a sweep until an operator reads one
line and deletes one file, while a false "stale" resumes the exact
double-instance failure above, silently, on a machine nobody is watching.

Three cases, in that spirit:

  * SAME HOST, PID NOT ALIVE -> stale, steal it. This is the case that makes
    an auto-restarting supervisor work at all: a SIGKILLed sweep leaves its
    lockfile behind, and the restart must not be blocked by its own corpse.
  * ANOTHER HOST -> never stale. `os.kill(pid, 0)` answers a question about
    THIS machine; against a lockfile from a restored image or a shared volume
    it would declare liveness by coincidence.
  * EMPTY OR UNPARSEABLE -> stale only once it is older than
    `EMPTY_LOCK_GRACE_S`. `O_EXCL` creates the file before the payload is
    written, so a competitor can observe a legitimately-held lock in that
    window; reading "empty" as "abandoned" would let both sweeps win the race
    this module exists to settle.

PID reuse is NOT defended against, deliberately. A recycled pid can only make
a dead holder look alive, which is the stalling direction, and the refusal
message names the file to delete. Defending it properly needs a process start
time, which has no portable source without a new dependency — a real cost for
a failure whose remedy is already one documented command.
"""

from __future__ import annotations

import json
import os
import socket
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

#: How long a lockfile with no readable payload is presumed to belong to a
#: live process still in the window between `O_EXCL` and its first write.
#: Generous by design: the cost of waiting is one refused start, and the cost
#: of being wrong is two concurrent sweeps.
EMPTY_LOCK_GRACE_S: float = 30.0


class SweepLockedError(RuntimeError):
    """Another sweep holds this results file."""


def lock_path_for(out: Path) -> Path:
    """Beside the results file, not in a temp dir: the lock protects THAT
    file, and the two must travel together across a VPS rebuild or a `scp`.
    """
    return out.with_name(out.name + ".lock")


def _pid_alive(pid: int) -> bool:
    """`os.kill(pid, 0)`: `ESRCH` means gone, `EPERM` means alive but owned by
    another user (which still counts as alive)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        # Anything unexpected reads as "cannot establish death" — see the
        # module docstring on which direction to err in.
        return True
    return True


def _payload() -> dict:
    return {
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "started": datetime.now(UTC).isoformat(),
        "argv": sys.argv,
    }


def _describe(path: Path, existing: dict | None) -> str:
    if existing is None:
        return (
            f"{path} exists but carries no readable holder. If no sweep is "
            f"running, delete it: rm {path}"
        )
    return (
        f"{path} is held by pid {existing.get('pid')} on host "
        f"{existing.get('host')!r}, started {existing.get('started')} "
        f"({existing.get('argv')}). If that process is gone, delete the "
        f"lock: rm {path}"
    )


def _is_stale(path: Path) -> tuple[bool, dict | None]:
    try:
        raw = path.read_text()
    except FileNotFoundError:
        # Vanished between our failed create and this read: the holder
        # released it. Caller retries.
        return True, None
    try:
        existing = json.loads(raw)
        if not isinstance(existing, dict):
            raise ValueError("lock payload is not an object")
    except Exception:
        existing = None

    if existing is None:
        try:
            age = time.time() - path.stat().st_mtime
        except FileNotFoundError:
            return True, None
        return age > EMPTY_LOCK_GRACE_S, None

    if existing.get("host") != socket.gethostname():
        return False, existing
    pid = existing.get("pid")
    if not isinstance(pid, int):
        return False, existing
    return not _pid_alive(pid), existing


def _create(path: Path, payload: dict) -> bool:
    """`O_EXCL` create + write. Returns False iff someone else got there
    first."""
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        return False
    with os.fdopen(fd, "w") as fh:
        json.dump(payload, fh)
        fh.flush()
        os.fsync(fh.fileno())
    return True


@contextmanager
def sweep_lock(out: Path) -> Iterator[Path]:
    """Hold the single-instance lock for `out`, or raise `SweepLockedError`.

    Yields the lockfile's path so a caller can report it.
    """
    path = lock_path_for(out)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _payload()

    if not _create(path, payload):
        stale, existing = _is_stale(path)
        if not stale:
            raise SweepLockedError(_describe(path, existing))
        # Steal it, then try exactly once more. A second failure means a
        # competitor won the race for the same abandoned lock — refuse
        # rather than loop, since looping is how both of them end up
        # believing they won.
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        if not _create(path, payload):
            _, existing = _is_stale(path)
            raise SweepLockedError(
                "lost the race to replace an abandoned lock: "
                + _describe(path, existing)
            )

    try:
        yield path
    finally:
        # Only remove OUR lock. If this process was wrongly declared stale
        # and a successor took over, deleting its lock here would leave the
        # results file unguarded — the very state this module exists to
        # prevent, reached by way of the cleanup path.
        try:
            current = json.loads(path.read_text())
        except Exception:
            current = None
        if isinstance(current, dict) and (
            current.get("pid") == payload["pid"]
            and current.get("host") == payload["host"]
        ):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
