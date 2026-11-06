#!/bin/bash
# Supervisor for the GitHub issue relay + pi session agent on the Allianz MacBook.
#
# Portable for bash 3.2 (the macOS default): watch both children by PID and
# exit as soon as ONE of them dies, so launchd (KeepAlive) restarts the whole
# bundle instead of leaving a half-dead relay behind.
#
# NOTE: `wait -n` would be the obvious way to write this, but it needs bash 4.3+
# and macOS ships bash 3.2 ("wait: -n: invalid option"). The PID watchdog below
# does the same thing on every bash.
export GH_TOKEN="$(cat "$HOME/.mbp-relay/token")"
export RELAY_INTERVAL=1
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$HOME/.local/bin:$PATH"
cd "$HOME/mbp-relay"
# Keep the laptop awake so the relay keeps polling while idle.
caffeinate -i -d -w $$ &
node poller.mjs >> "$HOME/mbp-relay/poller.log" 2>&1 &
POLLER_PID=$!
node pisess.mjs >> "$HOME/mbp-relay/pisess.log" 2>&1 &
PISESS_PID=$!
while kill -0 "$POLLER_PID" 2>/dev/null && kill -0 "$PISESS_PID" 2>/dev/null; do
  sleep 5
done
# One of them died: bring the whole thing down so launchd restarts it cleanly.
kill "$POLLER_PID" "$PISESS_PID" 2>/dev/null
exit 1
