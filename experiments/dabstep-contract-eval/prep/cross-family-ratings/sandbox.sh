#!/bin/bash
# Run a command where the only visible parts of $HOME are the Copilot install,
# a fresh Copilot state folder ($RATER_COPILOT_HOME, mounted at ~/.copilot, so
# no earlier session is readable) and one work folder (mounted at ~/work). /tmp is empty, other
# processes are invisible, and /mnt is hidden except /mnt/wsl (WSL keeps
# resolv.conf there; the Windows drives stay hidden).
# Usage: sandbox.sh <workdir> <command> [args...]
set -euo pipefail
WORK=$(readlink -f "$1"); shift
CPHOME=$(readlink -f "${RATER_COPILOT_HOME:?set RATER_COPILOT_HOME}")
exec unshare --user --map-root-user --mount --pid --fork --mount-proc --propagation private bash -c '
set -euo pipefail
H="$HOME"; WORK="$1"; CPHOME="$2"; UID_OUT="$3"; GID_OUT="$4"; shift 4
mount -t tmpfs none /tmp
mkdir /tmp/old /tmp/work /tmp/wsl /tmp/cphome
mount --rbind "$H" /tmp/old
mount --bind "$WORK" /tmp/work
mount --bind "$CPHOME" /tmp/cphome
mount --rbind /mnt/wsl /tmp/wsl
mount -t tmpfs none "$H"
mkdir -p "$H/.copilot" "$H/.npm-global" "$H/work"
mount --bind /tmp/cphome "$H/.copilot"
mount --bind /tmp/old/.npm-global "$H/.npm-global"
mount --bind /tmp/work "$H/work"
mount -t tmpfs none /mnt
mkdir /mnt/wsl && mount --rbind /tmp/wsl /mnt/wsl
umount -l /tmp/old /tmp/work /tmp/wsl /tmp/cphome && rmdir /tmp/old /tmp/work /tmp/wsl /tmp/cphome
cd "$H/work"
exec unshare --user --map-user=$UID_OUT --map-group=$GID_OUT "$@"
' _ "$WORK" "$CPHOME" "$(id -u)" "$(id -g)" "$@"
