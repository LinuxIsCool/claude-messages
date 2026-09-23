#!/usr/bin/env bash
set -euo pipefail

XVFB_BIN="${XVFB_BIN:-}"
SIGNAL_DESKTOP_BIN="${SIGNAL_DESKTOP_BIN:-/usr/bin/signal-desktop}"
DISPLAY_NUMBER="${SIGNAL_DISPLAY_NUMBER:-91}"

if [[ -z "$XVFB_BIN" ]]; then
  if command -v Xvfb >/dev/null 2>&1; then
    XVFB_BIN="$(command -v Xvfb)"
  else
    XVFB_BIN="${HOME}/.local/opt/signal-headless/usr/bin/Xvfb"
  fi
fi

if [[ ! -x "$XVFB_BIN" ]]; then
  echo "Xvfb is not executable at $XVFB_BIN" >&2
  exit 2
fi
if [[ ! -x "$SIGNAL_DESKTOP_BIN" ]]; then
  echo "Signal Desktop is not executable at $SIGNAL_DESKTOP_BIN" >&2
  exit 2
fi
if [[ ! -f "${HOME}/.config/Signal/config.json" ]]; then
  echo "Signal Desktop profile is absent. Link it in a graphical session first." >&2
  exit 3
fi

DISPLAY=":${DISPLAY_NUMBER}"
export DISPLAY

"$XVFB_BIN" "$DISPLAY" -screen 0 1280x720x24 -nolisten tcp -ac &
xvfb_pid=$!
signal_pid=''

cleanup() {
  if [[ -n "$signal_pid" ]]; then
    kill "$signal_pid" 2>/dev/null || true
  fi
  kill "$xvfb_pid" 2>/dev/null || true
  wait "$signal_pid" 2>/dev/null || true
  wait "$xvfb_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for _ in $(seq 1 100); do
  [[ -S "/tmp/.X11-unix/X${DISPLAY_NUMBER}" ]] && break
  kill -0 "$xvfb_pid" 2>/dev/null || {
    echo "Xvfb exited before display $DISPLAY became ready" >&2
    exit 4
  }
  sleep 0.1
done

if [[ ! -S "/tmp/.X11-unix/X${DISPLAY_NUMBER}" ]]; then
  echo "Xvfb display $DISPLAY did not become ready" >&2
  exit 4
fi

# KWallet is D-Bus activated outside this process tree. Publish the private
# display to the user bus before Electron requests kwalletd6, otherwise the
# wallet daemon starts without DISPLAY and the existing encrypted key is
# unreadable.
dbus-update-activation-environment --systemd DISPLAY

"$SIGNAL_DESKTOP_BIN" \
  --password-store=kwallet6 \
  --disable-gpu \
  --disable-dev-shm-usage &
signal_pid=$!
wait "$signal_pid"
