#!/usr/bin/env bash
# Test doubles for the three external actions the gateway takes.
# Real deployment replaces these with the Home Assistant plug call and a
# graceful `ssh poweroff@10.0.0.10 sudo systemctl poweroff`.
set -u
BASE="${FAKE_BASE:?}"
cmd="${1:-}"

case "$cmd" in
  wake)
    echo "plug_on $(date +%s)" >> "$BASE/events.log"
    # $BASE/never_boot simulates a machine that is powered but never answers:
    # the socket is on, the OS does not come up.
    [ -e "$BASE/never_boot" ] && exit 0
    # a cold start is ~49 s on the real card; keep the test fast
    ( sleep "${FAKE_BOOT_DELAY:-3}"; touch "$BASE/up" ) &
    ;;
  wake_noop)
    echo "plug_on $(date +%s)" >> "$BASE/events.log"
    ;;
  poweroff)
    echo "poweroff $(date +%s)" >> "$BASE/events.log"
    ( sleep "${FAKE_POWEROFF_DELAY:-1}"; rm -f "$BASE/up" ) &
    ;;
  plug-off)
    echo "plug_off $(date +%s)" >> "$BASE/events.log"
    ;;
  fail)
    echo "boom" >&2
    exit 1
    ;;
  *)
    echo "usage: $0 wake|poweroff|plug-off|fail" >&2
    exit 2
    ;;
esac
