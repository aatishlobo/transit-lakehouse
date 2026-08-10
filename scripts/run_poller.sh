#!/usr/bin/env bash
#
# Supervised poller for unattended runs.
#
# Two jobs:
#
#   1. Restart the poller if it dies. It already survives HTTP errors per-poll,
#      but an unexpected write failure would end the process silently and the
#      loss would only surface as a gap in the archive hours later. GTFS-RT
#      history is not re-fetchable (pitfall 0.1), so a crash at 2am costs real
#      training data.
#
#   2. Enforce single-instance MECHANICALLY, not by convention. CLAUDE.md §3.10
#      says the poller must never run as more than one replica: it is not a
#      Kafka consumer, has no lag to divide, and N instances means N x the API
#      calls against a 60/hour budget -- which throttles the token and stops
#      the archive outright. A documented rule that is easy to violate by
#      double-clicking is not a control. The lock below is.
#
# Usage:
#   scripts/run_poller.sh              # supervise until stopped
#   scripts/run_poller.sh --status
#   scripts/run_poller.sh --stop
#
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
ROOT="$PWD"
LOCK="$ROOT/.poller.lock"
LOG="$ROOT/poller.log"
PY="$ROOT/.venv/bin/python"

# Backoff bounds. The floor is deliberately non-zero: a poller that crashes
# instantly and restarts instantly becomes an unthrottled request loop against
# a rate-limited API, which is the exact failure the budget exists to prevent.
BACKOFF_MIN=5
BACKOFF_MAX=300

_running_pid() {
    [ -f "$LOCK" ] || return 1
    local pid
    pid="$(cat "$LOCK" 2>/dev/null)"
    [ -n "$pid" ] || return 1
    # Confirm the pid is actually our supervisor and not a recycled pid that
    # now belongs to something unrelated.
    ps -p "$pid" -o command= 2>/dev/null | grep -q "run_poller.sh" || return 1
    echo "$pid"
}

case "${1:-}" in
    --status)
        if pid="$(_running_pid)"; then
            echo "supervisor RUNNING (pid $pid)"
            pgrep -f "ingest.poller.poller" >/dev/null \
                && echo "poller     RUNNING (pid $(pgrep -f 'ingest.poller.poller' | tr '\n' ' '))" \
                || echo "poller     not running (supervisor may be backing off)"
            echo "polls archived: $(find "$ROOT/data/raw" -name '*.pb.gz' 2>/dev/null | wc -l | tr -d ' ')"
            tail -3 "$LOG" 2>/dev/null
        else
            echo "supervisor not running"
        fi
        exit 0
        ;;
    --stop)
        if pid="$(_running_pid)"; then
            kill "$pid" 2>/dev/null
            echo "stopped supervisor (pid $pid)"
        else
            echo "supervisor not running"
        fi
        # SIGTERM lets the poller finish its current cycle before exiting.
        pkill -f "ingest.poller.poller" 2>/dev/null && echo "signalled poller"
        rm -f "$LOCK"
        exit 0
        ;;
esac

if pid="$(_running_pid)"; then
    echo "refusing to start: supervisor already running (pid $pid)." >&2
    echo "The poller is single-instance by design -- see CLAUDE.md 3.10." >&2
    exit 1
fi

# Stale lock from a previous run that was killed without cleanup.
[ -f "$LOCK" ] && rm -f "$LOCK"
echo $$ > "$LOCK"
trap 'rm -f "$LOCK"; pkill -f "ingest.poller.poller" 2>/dev/null; exit 0' INT TERM

if [ ! -f "$ROOT/.env" ]; then
    echo "no .env -- copy .env.example and add your 511 key" >&2
    rm -f "$LOCK"
    exit 2
fi
set -a; . "$ROOT/.env"; set +a

backoff=$BACKOFF_MIN
echo "$(date -u '+%Y-%m-%d %H:%M:%S') SUPERVISOR start (pid $$)" >> "$LOG"

while true; do
    started=$(date +%s)
    "$PY" -m ingest.poller.poller >> "$LOG" 2>&1
    rc=$?
    ran=$(( $(date +%s) - started ))

    # A clean exit means someone asked it to stop (SIGTERM handler in the
    # poller returns 0 after finishing the cycle). Don't fight the operator.
    if [ $rc -eq 0 ]; then
        echo "$(date -u '+%Y-%m-%d %H:%M:%S') SUPERVISOR poller exited cleanly, stopping" >> "$LOG"
        break
    fi

    # Reset backoff only if it stayed up long enough to count as healthy;
    # otherwise a crash-loop would restart every BACKOFF_MIN forever.
    if [ $ran -ge 120 ]; then
        backoff=$BACKOFF_MIN
    else
        backoff=$(( backoff * 2 ))
        [ $backoff -gt $BACKOFF_MAX ] && backoff=$BACKOFF_MAX
    fi

    echo "$(date -u '+%Y-%m-%d %H:%M:%S') SUPERVISOR poller exited rc=$rc after ${ran}s, restarting in ${backoff}s" >> "$LOG"
    sleep $backoff
done

rm -f "$LOCK"
