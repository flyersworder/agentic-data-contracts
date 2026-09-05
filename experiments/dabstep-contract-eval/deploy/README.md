# Running the sweep unattended

The sweep is 1,203 runs at ~3 minutes each. At `--workers 4` that is roughly
15 hours, which is long enough that the machine will probably do something
unhelpful at least once.

## What survives what

| event | survives? | cost |
|---|---|---|
| the sweep process is SIGKILLed | yes | the runs in flight (≤ `workers` × arms) |
| a torn final line in the results file | yes | nothing — repaired before the next append |
| the machine reboots | yes, **if something restarts the sweep** | as above |
| power loss mid-write | yes | nothing — every row is `fsync`ed before the next |
| two sweeps started at once | refused | nothing |

All but the last two were verified by probe, not assumed: a `SIGKILL` at
142/180 rows, and a hand-torn tail plus a crash loop, both resumed to exactly
180 rows with nothing missing, nothing re-run, and no unparseable line.

Resume works because every completed unit has a row on disk, and a restart
reads that file for four things: which units are done, how much has been
spent (so **`--max-spend` bounds the experiment, not the process** — a
restart loop cannot blow the budget), what each model has cost so far, and
whether the last line was torn.

## The three guards

**Single-instance lock** (`results/<name>.jsonl.lock`). Two sweeps against one
results file duplicate paid work — and, worse, share `<db>.working`, so one
instance's `make_working_copy` copies the pristine database over a file the
other holds a live DuckDB connection on, producing `db_corrupted` readings no
arm caused. The results file gives no sign this happened, which is why the
lock exists rather than a warning. A lock left behind by a dead process on
this host is stolen automatically, so a restart is never blocked by its own
corpse; a lock from another host is never stolen, and the refusal message
names the file to delete.

**Clear the working copies before resuming a KILLED sweep.** The lock is
stolen from a dead process, which is what you want — but stealing it is all
that happens. The dead sweep's `data/<db>.working*` files are left behind, and
the restart's workers re-derive copies over files whose previous owner died
holding a live DuckDB connection on them. That is the same race the lock
prevents *between* concurrent sweeps, reappearing *across* a kill and a
resume:

```bash
rm -f data/*.working*      # ONLY when no sweep is running
```

Measured on 2026-09-05: a sweep killed mid-flight and resumed into the same
results file produced **3 `db_corrupted` rows in 450** (`manual_prompt`, glm),
against 0-1 in 1,604 for each of the four ablation runs — which used the same
`--workers 6` but were never killed and resumed. Width is not the variable;
the restart is. The corrupted rows are recoverable (re-run them into a
separate results file and let `dce.submit`'s later-file-wins rule merge them),
but they are silent: nothing fails, the rows just carry the flag. Check for
it after any resumed sweep:

```bash
uv run python -c "import json,sys; sys.path.insert(0,'.'); from dce.runner import latest_rows; from pathlib import Path; rows=latest_rows(Path('results/<name>.jsonl')); print(sum(1 for r in rows if r.get('db_corrupted')), 'of', len(rows))"
```

**`fsync` per row.** `flush()` hands bytes to the OS page cache: that survives
the process dying but not the machine dying. Rows arrive about once a minute
per worker, so the guarantee is bought outright rather than batched. (On Linux
`fsync` reaches the device. macOS needs `F_FULLFSYNC`, so a laptop run keeps
the weaker promise.)

**Snapshot** (`results/<name>.jsonl.snapshot`), every 25 rows and once more
after the pool drains. This is not for process or machine death — `fsync`
covers those. It is for the losses that reach the file itself: operator error,
a bad disk, a bug that truncates it. Written to a temp name and `os.replace`d,
because a torn backup is worse than none: it gets trusted.

## Setup on the box

```bash
git clone <repo> /opt/agentic-data-contracts
cd /opt/agentic-data-contracts/experiments/dabstep-contract-eval
uv sync
uv run pytest -q                  # offline, deterministic — run it before spending
uv run python -m dce.prepare      # downloads DABStep, builds the DuckDB, rebuilds golds
```

`data/` is gitignored and nothing under it is committed, so `dce.prepare` has
to run on every fresh checkout.

The API key goes in a file **outside the repository**, never in the unit file
(`systemctl show` prints `Environment=` to any user):

```bash
sudo install -d -m 0755 /etc/dce
printf 'OPENROUTER_API_KEY=%s\n' "$KEY" | sudo tee /etc/dce/env > /dev/null
sudo chmod 0600 /etc/dce/env
```

The tree must be clean: `commit_sha` on every row is the only pin on the
library under test, and `assert_clean_tree` refuses to start otherwise. The
results file itself is exempt.

## Starting it

```bash
sudo cp deploy/dce-sweep.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now dce-sweep     # `enable` is what survives a reboot
journalctl -u dce-sweep -f
```

Without systemd, `deploy/supervise.sh` takes the same flags and applies the
same exit-code policy — but it survives only the sweep dying, not the machine
rebooting.

## Reading the exit code

| code | meaning | restart? |
|---:|---|---|
| 0 | completed | no |
| 1 | uncaught exception | yes, could be transient |
| 2 | spend cap reached | **no** — a human raises the cap |
| 3 | circuit breaker: systemic failure | **no** — fix the cause first |
| 4 | DuckDB connection leaked | yes — a fresh process is the documented recovery |

Codes 2 and 3 are the ones an eager supervisor gets wrong. Restarting on 2
re-hits the cap every 30 seconds forever; restarting on 3 writes thousands of
phantom-priced rows against a problem no restart can fix. Both units above
refuse to restart on either.

## Watching it

```bash
wc -l results/glm-full.jsonl                  # rows done, of 1203
python -m dce.stats results/glm-full.jsonl    # accuracy + McNemar so far
journalctl -u dce-sweep --since '1 hour ago' | grep -i stopping
```

Start at `--workers 4`, not 10. Every call is pinned to one provider endpoint
with `allow_fallbacks: false`, so a rate limit there surfaces as an error
rather than a re-route. Step up once the error rate looks flat.

`--max-spend` also caps concurrency, because reservations are held while in
flight: at $12 only four worst-case groups fit at once. The runner prints a
note when the cap admits fewer groups than you asked for workers — the
symptom otherwise looks like a broken pool.
