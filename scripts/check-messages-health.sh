#!/usr/bin/env bash
# check-messages-health.sh — Read health.json, alert on stale adapters
# Called by systemd timer every 5 minutes.
#
# Exit codes:
#   0 = all adapters healthy
#   1 = one or more adapters stale
#   2 = health.json missing or unreadable (daemon likely down)

set -euo pipefail

HEALTH_FILE="${HOME}/.claude/local/messages/health.json"
ALERT_LOG="${HOME}/.claude/local/messages/health-alerts.log"

# Staleness thresholds per tier (seconds)
# NOTE: Tier-0 adapters sync in <1s but the full cycle takes ~8 min (Telegram scans 881 dialogs).
# Threshold must exceed: cycle_duration + poll_interval + margin.
TIER_0_THRESHOLD=1200   # 20 min — ~2.5 full cycles (Telegram scan ~8min + poll ~60s + variance)
TIER_2_THRESHOLD=7200   # 2 hr  — network adapters (Telegram, Email, Slack)

# Daemon-level staleness: if last_cycle is older than this, daemon is stuck/dead
DAEMON_THRESHOLD=1200   # 20 min (cycle ~8min + poll ~60s + generous margin)

NOW_EPOCH=$(date +%s)

# --- Check health.json exists ---
if [[ ! -f "$HEALTH_FILE" ]]; then
  MSG="legion-messages health.json missing — daemon may not be running"
  echo "$(date -Iseconds) CRITICAL $MSG" >> "$ALERT_LOG"
  notify-send -u critical "Messages Daemon" "$MSG" 2>/dev/null || true
  exit 2
fi

# --- Parse with jq ---
HEALTH=$(cat "$HEALTH_FILE")
LAST_CYCLE=$(echo "$HEALTH" | jq -r '.last_cycle')
DAEMON_NAME=$(echo "$HEALTH" | jq -r '.daemon')

# Convert last_cycle to epoch
LAST_CYCLE_EPOCH=$(date -d "$LAST_CYCLE" +%s 2>/dev/null || echo 0)
DAEMON_AGE=$((NOW_EPOCH - LAST_CYCLE_EPOCH))

# --- Check daemon-level staleness ---
if [[ $DAEMON_AGE -gt $DAEMON_THRESHOLD ]]; then
  MSG="${DAEMON_NAME} last cycle ${DAEMON_AGE}s ago (threshold: ${DAEMON_THRESHOLD}s) — daemon may be stuck"
  echo "$(date -Iseconds) CRITICAL $MSG" >> "$ALERT_LOG"
  notify-send -u critical "Messages Daemon" "$MSG" 2>/dev/null || true
  exit 1
fi

# --- Check per-adapter staleness ---
STALE_COUNT=0
STALE_ADAPTERS=""

for PLATFORM in $(echo "$HEALTH" | jq -r '.adapters | keys[]'); do
  ADAPTER=$(echo "$HEALTH" | jq ".adapters[\"$PLATFORM\"]")
  TIER=$(echo "$ADAPTER" | jq -r '.tier')
  LAST_SUCCESS=$(echo "$ADAPTER" | jq -r '.last_success')
  CONSECUTIVE_FAILURES=$(echo "$ADAPTER" | jq -r '.consecutive_failures')

  # Determine threshold
  if [[ "$TIER" -eq 0 ]]; then
    THRESHOLD=$TIER_0_THRESHOLD
  else
    THRESHOLD=$TIER_2_THRESHOLD
  fi

  # Check if adapter has ever succeeded
  if [[ "$LAST_SUCCESS" == "null" ]]; then
    # Never succeeded — only alert if daemon has been up > threshold
    if [[ $DAEMON_AGE -gt $THRESHOLD ]]; then
      STALE_COUNT=$((STALE_COUNT + 1))
      STALE_ADAPTERS="${STALE_ADAPTERS} ${PLATFORM}(never-synced)"
    fi
    continue
  fi

  # Check staleness
  SUCCESS_EPOCH=$(date -d "$LAST_SUCCESS" +%s 2>/dev/null || echo 0)
  AGE=$((NOW_EPOCH - SUCCESS_EPOCH))

  if [[ $AGE -gt $THRESHOLD ]]; then
    STALE_COUNT=$((STALE_COUNT + 1))
    STALE_ADAPTERS="${STALE_ADAPTERS} ${PLATFORM}(${AGE}s,tier${TIER},${CONSECUTIVE_FAILURES}fails)"
  fi
done

# --- Report ---
if [[ $STALE_COUNT -gt 0 ]]; then
  MSG="${STALE_COUNT} stale adapter(s):${STALE_ADAPTERS}"
  echo "$(date -Iseconds) WARNING $MSG" >> "$ALERT_LOG"
  notify-send -u normal "Messages Sync" "$MSG" 2>/dev/null || true
  exit 1
fi

# All healthy — log silently (no notification)
echo "$(date -Iseconds) OK all adapters healthy (cycle ${DAEMON_AGE}s ago)" >> "$ALERT_LOG"
exit 0
