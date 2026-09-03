#!/usr/bin/env bash
# Portable stand-in for the systemd unit, for a box without systemd or for a
# run inside tmux/screen. Same exit-code policy; see dce-sweep.service for
# why each code decides the way it does.
#
#   tmux new -s sweep
#   ./deploy/supervise.sh --models z-ai/glm-5.3-flash --workers 4 \
#       --max-spend 12 --out results/glm-full.jsonl
#
# This survives the SWEEP dying. It does not survive the MACHINE rebooting —
# for that you need the systemd unit (or an @reboot cron entry calling this).

set -uo pipefail

MAX_RESTARTS=${MAX_RESTARTS:-20}
BACKOFF=${BACKOFF:-30}
restarts=0

while :; do
    uv run python -m dce.runner "$@"
    code=$?
    case $code in
        0) echo "supervise: sweep completed"; exit 0 ;;
        2) echo "supervise: stopped at the spend cap — raise --max-spend to continue" >&2; exit 2 ;;
        3) echo "supervise: circuit breaker tripped — fix the underlying problem" >&2; exit 3 ;;
    esac
    restarts=$((restarts + 1))
    if [ "$restarts" -ge "$MAX_RESTARTS" ]; then
        echo "supervise: giving up after $restarts restarts (last exit $code)" >&2
        exit "$code"
    fi
    echo "supervise: exit $code — restarting in ${BACKOFF}s (${restarts}/${MAX_RESTARTS})" >&2
    sleep "$BACKOFF"
done
