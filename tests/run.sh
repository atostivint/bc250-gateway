#!/usr/bin/env bash
# End-to-end test: a fake backend that can be "powered off", the gateway in front
# of it, and a scripted power on/power off. No hardware, no network.
#
#   bash tests/run.sh
#   PYTHON=/path/to/venv/bin/python bash tests/run.sh   # if deps are in a venv
#
# Checks the whole contract: wake on request, SSE keep-alives during boot, cached
# /v1/models, two-stage shutdown, the wake-failure guard.
set -euo pipefail

cd "$(dirname "$0")"
ROOT="$(cd .. && pwd)"
RUN="$PWD/run"
PYTHON="${PYTHON:-python3}"
GATEWAY_BIN="${GATEWAY_BIN:-$ROOT/gateway.py}"
GW_PORT="${GW_PORT:-8199}"
API_PORT="${API_PORT:-9111}"

# A stale gateway on the test port would make this suite test the wrong process.
for port in "$GW_PORT" "$API_PORT"; do
  if ss -ltn "sport = :$port" 2>/dev/null | grep -q LISTEN; then
    echo "port $port already in use — stop that process first (ss -ltnp | grep :$port)" >&2
    exit 1
  fi
done

rm -rf "$RUN"; mkdir -p "$RUN"
chmod +x fake_actions.sh

cat > "$RUN/config.toml" <<EOF
[gateway]
listen = "127.0.0.1"
port = $GW_PORT
api_key = ""

[backend]
base_url = "http://127.0.0.1:$API_PORT"
wake_timeout = 20.0
poll_interval = 0.3
models_cache = "$RUN/models.json"
monitor_interval = 1.0
cycle_delay = 1.0

[wake]
command = ["$PWD/fake_actions.sh", "wake"]
cycle_after_failures = 2

[shutdown]
enabled = true
idle_timeout = 6.0
min_uptime = 1.0
# no pre_sleep_command here: the occupancy guard is optional, and a command that
# exits non-zero vetoes the shutdown (which is what 409 means below).
poweroff_command = ["$PWD/fake_actions.sh", "poweroff"]
plug_off_command = ["$PWD/fake_actions.sh", "plug-off"]

[keepawake]
flag_file = "$RUN/keep-awake"
EOF

export FAKE_BASE="$RUN" FAKE_PORT="$API_PORT"
$PYTHON fake_backend.py & BACK=$!
BC250_GATEWAY_CONFIG="$RUN/config.toml" $PYTHON "$GATEWAY_BIN" & GW=$!
cleanup() {
  kill $BACK $GW 2>/dev/null || true
  # give uvicorn time to release the port, else the next run trips the guard above
  for _ in $(seq 1 25); do
    ss -ltn "sport = :$GW_PORT" 2>/dev/null | grep -q LISTEN || break
    sleep 0.2
  done
  kill -9 $BACK $GW 2>/dev/null || true
}
trap cleanup EXIT
sleep 2

GW_URL="http://127.0.0.1:$GW_PORT" $PYTHON test_e2e.py
